// scraper/src/research.ts
// Main research orchestration module.
// Implements researchClient() and researchAll() for podcast market research pipeline.

import Anthropic from '@anthropic-ai/sdk';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

import {
  query,
  upsertResearch,
  snapshotResearch,
  getGroupsNeedingResearch,
  getGuestHistory,
  updateGroupCategory,
  updateResearchBrief,
  setFirstResearchCompleted,
  upsertGuestHistory,
  getEpisodeTitlesForGroup,
  getResearchForGroup,
  logResearchAudit,
} from './db.js';
import { extractGuests } from './guest-extractor.js';
import { safeValidateResearchData, VALID_RESEARCH_TYPES } from './research-schemas.js';
import { serperSearch } from './serper.js';
import { listenNotesSearch, listenNotesBestPodcasts } from './listen-notes.js';
import { newsApiTopHeadlines, newsApiEverything } from './newsapi.js';
import {
  trendInterestOverTime,
  trendRelatedQueries,
  trendDailyTrends,
} from './google-trends.js';
import {
  notifyAdmin,
  researchStartMessage,
  researchProgressMessage,
  researchDoneMessage,
  researchFailMessage,
} from './notify.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const HAIKU = 'claude-haiku-4-5-20250514';

// Canonical source: src/config/analytics.ts — keep in sync
// (Scraper is a separate deploy and cannot import from the main bot source)
const CATEGORY_TAXONOMY = [
  'טכנולוגיה',
  'עסקים ויזמות',
  'בריאות ורפואה',
  'חינוך ותרבות',
  'ספורט ואורח חיים',
  'כלכלה ופיננסים',
  'פסיכולוגיה ומנטליות',
  'קריירה ועבודה',
  'יחסים ומשפחה',
  'פוליטיקה וחברה',
  'בידור ותרבות פופ',
  'מדע וחקר',
  'אחר',
];

// ---------------------------------------------------------------------------
// Skill prompt loader
// ---------------------------------------------------------------------------

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Read the research skill prompt from research-skill-content.txt */
export function readResearchSkillPrompt(): string {
  const skillPath = join(__dirname, 'research-skill-content.txt');
  return readFileSync(skillPath, 'utf-8');
}

// Cache the prompt at module load time (avoid repeated disk reads)
const RESEARCH_SKILL_PROMPT = readResearchSkillPrompt();

// ---------------------------------------------------------------------------
// Return types
// ---------------------------------------------------------------------------

export interface ResearchClientResult {
  success: boolean;
  typesCompleted: string[];
  typesFailed: string[];
  briefGenerated: boolean;
}

export interface ResearchAllResult {
  total: number;
  completed: number;
  failed: number;
  failures: Array<{ client_name: string; error: string }>;
}

// ---------------------------------------------------------------------------
// Helper: parse LLM JSON response (strip markdown fences)
// ---------------------------------------------------------------------------

function parseLlmJson(text: string): unknown {
  const raw = text.trim().replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
  return JSON.parse(raw);
}

// ---------------------------------------------------------------------------
// Helper: LLM call with audit logging
// ---------------------------------------------------------------------------

interface LlmCallOptions {
  groupId: string;
  auditType: string;
  systemPrompt: string;
  userMessage: string;
  maxTokens?: number;
}

interface LlmCallResult {
  text: string;
  inputTokens: number;
  outputTokens: number;
  durationMs: number;
}

async function callHaiku(
  client: Anthropic,
  opts: LlmCallOptions
): Promise<LlmCallResult> {
  const start = Date.now();
  let response: Anthropic.Message;
  try {
    response = await client.messages.create({
      model: HAIKU,
      max_tokens: opts.maxTokens ?? 4096,
      system: opts.systemPrompt,
      messages: [{ role: 'user', content: opts.userMessage }],
    });
  } catch (err: any) {
    const durationMs = Date.now() - start;
    logResearchAudit({
      groupId: opts.groupId,
      auditType: opts.auditType,
      model: HAIKU,
      durationMs,
      success: false,
      errorMessage: err.message,
    });
    throw err;
  }

  const durationMs = Date.now() - start;
  const textBlock = response.content.find(
    (b): b is Anthropic.TextBlock => b.type === 'text'
  );
  const text = textBlock?.text ?? '';

  logResearchAudit({
    groupId: opts.groupId,
    auditType: opts.auditType,
    model: HAIKU,
    inputTokens: response.usage.input_tokens,
    outputTokens: response.usage.output_tokens,
    promptSummary: opts.userMessage.slice(0, 300),
    responseSummary: text.slice(0, 300),
    durationMs,
    success: true,
  });

  return {
    text,
    inputTokens: response.usage.input_tokens,
    outputTokens: response.usage.output_tokens,
    durationMs,
  };
}

// ---------------------------------------------------------------------------
// Step 1: Resolve podcast category
// ---------------------------------------------------------------------------

async function resolveCategory(
  client: Anthropic,
  groupId: string,
  showName: string,
  existingCategory: string | null
): Promise<string> {
  if (existingCategory) {
    return existingCategory;
  }

  // Fetch episode titles to help classify
  const episodeRows = await getEpisodeTitlesForGroup(groupId);
  const episodeTitles = episodeRows.map((r) => r.episode_name).slice(0, 30);

  const systemPrompt = `You are a podcast classification expert. Classify Hebrew podcasts into exactly one category from the given taxonomy. Return ONLY the category name as a JSON string (e.g. "טכנולוגיה"). No explanation.`;

  const userMessage = `Podcast name: ${showName}
Episode titles:
${episodeTitles.length > 0 ? episodeTitles.map((t, i) => `${i + 1}. ${t}`).join('\n') : '(no episode data)'}

Choose ONE category from this list:
${CATEGORY_TAXONOMY.join(', ')}

Return just the category name as a plain JSON string.`;

  let category = 'אחר';
  try {
    const result = await callHaiku(client, {
      groupId,
      auditType: 'category_classification',
      systemPrompt,
      userMessage,
      maxTokens: 64,
    });

    const raw = result.text.trim().replace(/^"|"$/g, '').trim();
    if (CATEGORY_TAXONOMY.includes(raw)) {
      category = raw;
    } else {
      console.warn(`[research] Category "${raw}" not in taxonomy for ${showName}, defaulting to אחר`);
    }
  } catch (err: any) {
    console.error(`[research] Category classification failed for ${showName}:`, err.message);
  }

  await updateGroupCategory(groupId, category);
  console.log(`[research] Category for "${showName}": ${category}`);
  return category;
}

// ---------------------------------------------------------------------------
// Step 3: Craft search queries for a research type
// ---------------------------------------------------------------------------

async function craftSearchQueries(
  client: Anthropic,
  groupId: string,
  researchType: string,
  showName: string,
  category: string
): Promise<string[]> {
  const systemPrompt = `You are a podcast market research strategist. Generate search queries for researching a Hebrew podcast.
Return ONLY a valid JSON array of strings (the queries). No markdown, no explanation.`;

  const queryGuide: Record<string, string> = {
    competitor: `Generate 4 search queries to find Hebrew podcasts competing in the "${category}" niche, and 2 English queries for global competitors.`,
    keyword: `Generate 4 search queries to find high-demand keywords for a "${category}" Hebrew podcast. Include both Hebrew and English queries.`,
    trend: `Generate 3 Hebrew search queries for current trends in "${category}" in Israel, and 2 English queries for global trends.`,
    guest: `Generate 3 Hebrew queries to find Israeli experts in "${category}" suitable as podcast guests, and 2 English queries for international thought leaders.`,
    related_global: `Generate 4 English search queries to find the top global podcasts in the "${category}" category.`,
    episode_topics: `Generate 4 search queries (Hebrew + English) to find trending episode topic ideas for a "${category}" podcast.`,
    benchmarks: `Generate 3 search queries to find podcast industry benchmarks and statistics for the "${category}" category.`,
    youtube_landscape: `Generate 3 search queries to find popular YouTube channels in the "${category}" category in Hebrew/Israel. Include 2 English queries for global YouTube podcasters in the same niche.`,
  };

  const guide = queryGuide[researchType] ?? `Generate 4 search queries for "${researchType}" research on a "${category}" podcast.`;

  const userMessage = `Podcast: "${showName}"
Category: ${category}
Research type: ${researchType}

${guide}

Return a JSON array of query strings, e.g. ["query 1", "query 2"].`;

  try {
    const result = await callHaiku(client, {
      groupId,
      auditType: `query_craft_${researchType}`,
      systemPrompt,
      userMessage,
      maxTokens: 512,
    });

    const parsed = parseLlmJson(result.text);
    if (Array.isArray(parsed) && parsed.every((q) => typeof q === 'string')) {
      return parsed as string[];
    }
  } catch (err: any) {
    console.error(`[research] Query crafting failed for ${researchType}:`, err.message);
  }

  // Fallback: basic queries
  return [`${showName} ${category} podcast`, `${category} podcast Israel`];
}

// ---------------------------------------------------------------------------
// Step 3: Execute API calls for a research type
// ---------------------------------------------------------------------------

interface ApiResults {
  serper_he?: any[];
  serper_en?: any[];
  listen_notes?: any;
  newsapi?: any[];
  google_trends?: Record<string, any>;
}

async function executeApiCalls(
  researchType: string,
  queries: string[],
  showName: string
): Promise<ApiResults> {
  const results: ApiResults = {};

  // Helper: safe call that returns null on circuit-breaker trip or error
  async function safeCall<T>(label: string, fn: () => Promise<T>): Promise<T | null> {
    try {
      return await fn();
    } catch (err: any) {
      const msg: string = err.message ?? '';
      if (msg.includes('Circuit breaker open') || msg.includes('quota exhausted')) {
        console.warn(`[research] ${label} skipped — circuit breaker tripped`);
      } else {
        console.warn(`[research] ${label} failed:`, msg.slice(0, 200));
      }
      return null;
    }
  }

  // Determine which queries are Hebrew vs English (simple heuristic: Hebrew Unicode range)
  const isHebrew = (q: string) => /[\u0590-\u05FF]/.test(q);
  const hebrewQueries = queries.filter(isHebrew);
  const englishQueries = queries.filter((q) => !isHebrew(q));

  switch (researchType) {
    case 'competitor': {
      // Serper Hebrew + English, Listen Notes
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const heQ = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('serper_he', () =>
            serperSearch(heQ, { gl: 'il', hl: 'iw' })
          );
          if (r) results.serper_he = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const enQ = englishQueries[0] ?? queries[1] ?? `${showName} podcast category`;
          const r = await safeCall('serper_en', () =>
            serperSearch(enQ, { gl: 'us', hl: 'en' })
          );
          if (r) results.serper_en = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('listen_notes', () =>
            listenNotesSearch({ q, type: 'podcast' })
          );
          if (r) results.listen_notes = r;
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    case 'keyword': {
      // Serper + Google Trends relatedQueries
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('serper_he', () =>
            serperSearch(q, { gl: 'il', hl: 'iw' })
          );
          if (r) results.serper_he = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const q = englishQueries[0] ?? queries[1] ?? `${showName} podcast`;
          const r = await safeCall('serper_en', () =>
            serperSearch(q, { gl: 'us', hl: 'en' })
          );
          if (r) results.serper_en = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const keyword = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('google_trends_relatedQueries', () =>
            trendRelatedQueries(keyword, 'IL')
          );
          if (r) {
            results.google_trends = results.google_trends ?? {};
            results.google_trends.relatedQueries = r;
          }
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    case 'trend': {
      // NewsAPI, Google Trends (dailyTrends, interestOverTime), Serper
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('newsapi_everything', () =>
            newsApiEverything({ q, language: 'he', sortBy: 'publishedAt' })
          );
          if (r) results.newsapi = r.articles;
        })()
      );

      calls.push(
        (async () => {
          const r = await safeCall('google_trends_daily', () =>
            trendDailyTrends('IL')
          );
          if (r) {
            results.google_trends = results.google_trends ?? {};
            results.google_trends.dailyTrends = r;
          }
        })()
      );

      calls.push(
        (async () => {
          const keyword = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('google_trends_interestOverTime', () =>
            trendInterestOverTime(keyword, 'IL')
          );
          if (r) {
            results.google_trends = results.google_trends ?? {};
            results.google_trends.interestOverTime = r;
          }
        })()
      );

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('serper_he', () =>
            serperSearch(q, { gl: 'il', hl: 'iw' })
          );
          if (r) results.serper_he = r.organic;
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    case 'guest': {
      // Serper Hebrew (Israeli guests) + English (international), Listen Notes
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('serper_he', () =>
            serperSearch(q, { gl: 'il', hl: 'iw' })
          );
          if (r) results.serper_he = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const q = englishQueries[0] ?? queries[1] ?? `${showName} podcast guest`;
          const r = await safeCall('serper_en', () =>
            serperSearch(q, { gl: 'us', hl: 'en' })
          );
          if (r) results.serper_en = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const q = englishQueries[0] ?? queries[0];
          const r = await safeCall('listen_notes', () =>
            listenNotesSearch({ q, type: 'episode' })
          );
          if (r) results.listen_notes = r;
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    case 'related_global': {
      // Listen Notes best podcasts + Serper English
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const r = await safeCall('listen_notes_best', () =>
            listenNotesBestPodcasts({ language: 'English' })
          );
          if (r) results.listen_notes = r;
        })()
      );

      calls.push(
        (async () => {
          const q = englishQueries[0] ?? queries[0];
          const r = await safeCall('serper_en', () =>
            serperSearch(q, { gl: 'us', hl: 'en' })
          );
          if (r) results.serper_en = r.organic;
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    case 'episode_topics': {
      // Serper, NewsAPI, Google Trends
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('serper_he', () =>
            serperSearch(q, { gl: 'il', hl: 'iw' })
          );
          if (r) results.serper_he = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('newsapi', () =>
            newsApiEverything({ q, language: 'he', sortBy: 'relevancy' })
          );
          if (r) results.newsapi = r.articles;
        })()
      );

      calls.push(
        (async () => {
          const keyword = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('google_trends_relatedQueries', () =>
            trendRelatedQueries(keyword, 'IL')
          );
          if (r) {
            results.google_trends = results.google_trends ?? {};
            results.google_trends.relatedQueries = r;
          }
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    case 'benchmarks': {
      // Google Trends interestOverTime, Serper
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const keyword = showName;
          const r = await safeCall('google_trends_showName', () =>
            trendInterestOverTime(keyword, 'IL')
          );
          if (r) {
            results.google_trends = results.google_trends ?? {};
            results.google_trends.interestOverTime_showName = r;
          }
        })()
      );

      calls.push(
        (async () => {
          const keyword = englishQueries[0] ?? queries[0];
          const r = await safeCall('google_trends_category', () =>
            trendInterestOverTime(keyword, 'IL')
          );
          if (r) {
            results.google_trends = results.google_trends ?? {};
            results.google_trends.interestOverTime_category = r;
          }
        })()
      );

      calls.push(
        (async () => {
          const q = englishQueries[0] ?? queries[0];
          const r = await safeCall('serper_en', () =>
            serperSearch(q, { gl: 'us', hl: 'en' })
          );
          if (r) results.serper_en = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('serper_he', () =>
            serperSearch(q, { gl: 'il', hl: 'iw' })
          );
          if (r) results.serper_he = r.organic;
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    case 'youtube_landscape': {
      // YouTube landscape: Serper web search (Hebrew + English) for YouTube channels
      // Note: YouTube Data API search.list costs 100 units — use Serper as proxy to avoid quota burn
      const calls: Promise<void>[] = [];

      calls.push(
        (async () => {
          const q = hebrewQueries[0] ?? queries[0];
          const r = await safeCall('serper_he_youtube', () =>
            serperSearch(`site:youtube.com ${q}`, { gl: 'il', hl: 'iw' })
          );
          if (r) results.serper_he = r.organic;
        })()
      );

      calls.push(
        (async () => {
          const q = englishQueries[0] ?? queries[1] ?? `${showName} YouTube podcast channel`;
          const r = await safeCall('serper_en_youtube', () =>
            serperSearch(`site:youtube.com ${q}`, { gl: 'us', hl: 'en' })
          );
          if (r) results.serper_en = r.organic;
        })()
      );

      await Promise.allSettled(calls);
      break;
    }

    default:
      console.warn(`[research] Unknown research type: ${researchType}`);
  }

  return results;
}

// ---------------------------------------------------------------------------
// Step 3: Parse raw results via Haiku
// ---------------------------------------------------------------------------

async function parseRawResults(
  client: Anthropic,
  groupId: string,
  researchType: string,
  showName: string,
  category: string,
  apiResults: ApiResults,
  previousSnapshot: any | null,
  guestHistory: string[] | null
): Promise<any> {
  // Derive a rough English category name for the LLM
  const categoryEnglishMap: Record<string, string> = {
    'טכנולוגיה': 'Technology',
    'עסקים ויזמות': 'Business & Entrepreneurship',
    'בריאות ורפואה': 'Health & Medicine',
    'חינוך ותרבות': 'Education & Culture',
    'ספורט ואורח חיים': 'Sports & Lifestyle',
    'כלכלה ופיננסים': 'Economics & Finance',
    'פסיכולוגיה ומנטליות': 'Psychology & Mindset',
    'קריירה ועבודה': 'Career & Work',
    'יחסים ומשפחה': 'Relationships & Family',
    'פוליטיקה וחברה': 'Politics & Society',
    'בידור ותרבות פופ': 'Entertainment & Pop Culture',
    'מדע וחקר': 'Science & Research',
    'אחר': 'Other',
  };
  const categoryEnglish = categoryEnglishMap[category] ?? category;

  const inputPayload = {
    research_type: researchType,
    show_name: showName,
    category,
    category_english: categoryEnglish,
    api_results: apiResults,
    previous_snapshot: previousSnapshot ?? null,
    guest_history: guestHistory,
  };

  const userMessage = JSON.stringify(inputPayload);

  const result = await callHaiku(client, {
    groupId,
    auditType: `parse_${researchType}`,
    systemPrompt: RESEARCH_SKILL_PROMPT,
    userMessage,
    maxTokens: 4096,
  });

  const parsed = parseLlmJson(result.text);
  return parsed;
}

// ---------------------------------------------------------------------------
// Step 4: Generate research brief
// ---------------------------------------------------------------------------

async function generateResearchBrief(
  client: Anthropic,
  groupId: string,
  showName: string,
  category: string,
  allResearch: any[],
  failedTypes: string[] = []
): Promise<string> {
  const summaries = allResearch
    .map((r: any) => `## ${r.research_type}\n${r.research_data?.summary ?? ''}`)
    .join('\n\n');

  const missingNote = failedTypes.length > 0
    ? `\n\nNote: The following research types failed and are missing from this brief: ${failedTypes.join(', ')}. Do not mention these gaps to the reader — just work with the available data.`
    : '';

  const systemPrompt = `You are a podcast strategy consultant. Condense research summaries into a concise Hebrew brief (~500 tokens / ~350 words) for a podcast creator. Focus on actionable insights. Write in Hebrew.`;

  const userMessage = `Podcast: "${showName}"
Category: ${category}

Research summaries:
${summaries}${missingNote}

Write a ~350 word Hebrew brief covering the available research. Be specific and actionable.`;

  const result = await callHaiku(client, {
    groupId,
    auditType: 'research_brief',
    systemPrompt,
    userMessage,
    maxTokens: 1024,
  });

  // If some sources failed, prepend a note to the brief
  let brief = result.text.trim();
  if (failedTypes.length > 0) {
    brief = `⚠️ הערה: חלק ממקורות המחקר לא היו זמינים (${failedTypes.join(', ')}). הסיכום מבוסס על המידע שנאסף.\n\n${brief}`;
  }

  return brief;
}

// ---------------------------------------------------------------------------
// Main export: researchClient()
// ---------------------------------------------------------------------------

export async function researchClient(groupId: string): Promise<ResearchClientResult> {
  const client = new Anthropic();
  const typesCompleted: string[] = [];
  const typesFailed: string[] = [];

  // Load group info
  const groupRows = await query<{ id: string; client_name: string; podcast_category: string | null }>(
    `SELECT id, client_name, podcast_category FROM groups WHERE id = $1`,
    [groupId]
  );

  if (groupRows.length === 0) {
    console.error(`[research] Group not found: ${groupId}`);
    return { success: false, typesCompleted: [], typesFailed: VALID_RESEARCH_TYPES, briefGenerated: false };
  }

  const group = groupRows[0];
  const showName = group.client_name;

  console.log(`[research] Starting research for "${showName}" (${groupId})`);

  // ---- Step 1: Resolve category ----
  const category = await resolveCategory(client, groupId, showName, group.podcast_category);

  // ---- Step 2: Extract guest history ----
  try {
    const episodeRows = await getEpisodeTitlesForGroup(groupId);
    const episodeTitles = episodeRows.map((r) => r.episode_name);

    if (episodeTitles.length > 0) {
      const guestExtractions = await extractGuests(episodeTitles);
      await upsertGuestHistory(groupId, guestExtractions);
      console.log(`[research] Upserted ${guestExtractions.length} guest history entries for "${showName}"`);
    }
  } catch (err: any) {
    console.error(`[research] Guest extraction failed for "${showName}":`, err.message);
    // Non-fatal — continue with research
  }

  // Build guest history list for LLM context
  let guestHistory: string[] | null = null;
  try {
    const ghRows = await getGuestHistory(groupId);
    const names = ghRows
      .filter((r: any) => r.guest_name)
      .map((r: any) => r.guest_name as string);
    guestHistory = names.length > 0 ? names : null;
  } catch {
    // ignore
  }

  // ---- Step 3: Run 7 research types sequentially ----
  for (const researchType of VALID_RESEARCH_TYPES) {
    console.log(`[research] Running type "${researchType}" for "${showName}"`);

    try {
      // Craft search queries
      const queries = await craftSearchQueries(client, groupId, researchType, showName, category);

      // Execute API calls (parallel within type)
      const apiResults = await executeApiCalls(researchType, queries, showName);

      // Get previous research data for comparison
      let previousSnapshot: any | null = null;
      try {
        const existing = await getResearchForGroup(groupId);
        const prev = existing.find((r: any) => r.research_type === researchType);
        if (prev) {
          previousSnapshot = prev.research_data;
          // Snapshot old data to history before overwriting
          await snapshotResearch(prev.id);
        }
      } catch (err: any) {
        console.warn(`[research] Could not load previous snapshot for ${researchType}:`, err.message);
      }

      // Parse raw results via Haiku
      const parsedData = await parseRawResults(
        client,
        groupId,
        researchType,
        showName,
        category,
        apiResults,
        previousSnapshot,
        guestHistory
      );

      // Validate against Zod schema
      let dataToStore = parsedData;
      const validation = safeValidateResearchData(researchType, parsedData);
      if (!validation.success) {
        console.warn(`[research] Schema validation warning for ${researchType} (${showName}):`, validation.error);
        // Continue with raw data
      } else {
        dataToStore = validation.data;
      }

      // Upsert to podcast_research
      await upsertResearch(groupId, researchType, dataToStore, queries);

      typesCompleted.push(researchType);
      console.log(`[research] Completed type "${researchType}" for "${showName}"`);
    } catch (err: any) {
      console.error(`[research] Type "${researchType}" failed for "${showName}":`, err.message);
      typesFailed.push(researchType);
    }
  }

  // ---- Step 4: Generate research brief (if at least 5 of 7 types succeeded) ----
  let briefGenerated = false;
  const MIN_TYPES_FOR_BRIEF = 5;
  if (typesCompleted.length >= MIN_TYPES_FOR_BRIEF) {
    try {
      const allResearch = await getResearchForGroup(groupId);
      const brief = await generateResearchBrief(client, groupId, showName, category, allResearch, typesFailed);
      await updateResearchBrief(groupId, brief);
      briefGenerated = true;
      if (typesFailed.length > 0) {
        console.log(
          `[research] Partial research brief generated for "${showName}" — ${typesCompleted.length}/${VALID_RESEARCH_TYPES.length} types, missing: ${typesFailed.join(', ')}`
        );
      } else {
        console.log(`[research] Research brief generated for "${showName}"`);
      }
    } catch (err: any) {
      console.error(`[research] Brief generation failed for "${showName}":`, err.message);
    }
  } else {
    console.log(
      `[research] Skipping brief for "${showName}" — only ${typesCompleted.length}/${VALID_RESEARCH_TYPES.length} types succeeded (need at least ${MIN_TYPES_FOR_BRIEF})`
    );
  }

  // ---- Step 5: Mark first research completed (idempotent) ----
  await setFirstResearchCompleted(groupId).catch((err: any) => {
    console.error(`[research] setFirstResearchCompleted failed:`, err.message);
  });

  // success = brief was generated (at least 5/7 types succeeded)
  const success = briefGenerated;
  console.log(
    `[research] Done for "${showName}": ${typesCompleted.length} completed, ${typesFailed.length} failed, brief=${briefGenerated}`
  );

  return { success, typesCompleted, typesFailed, briefGenerated };
}

// ---------------------------------------------------------------------------
// Main export: researchAll()
// ---------------------------------------------------------------------------

export async function researchAll(
  groups?: Array<{ id: string; client_name: string }>
): Promise<ResearchAllResult> {
  const targetGroups = groups ?? (await getGroupsNeedingResearch());

  const total = targetGroups.length;
  const failures: Array<{ client_name: string; error: string }> = [];
  let completed = 0;

  console.log(`[research] researchAll: ${total} groups to process`);
  await notifyAdmin(researchStartMessage('all'));

  for (let i = 0; i < targetGroups.length; i++) {
    const group = targetGroups[i];

    // Progress notification every 10 clients
    if (i > 0 && i % 10 === 0) {
      await notifyAdmin(researchProgressMessage(i, total, group.client_name));
    }

    try {
      const result = await researchClient(group.id);
      if (result.success) {
        completed++;
        // If brief was generated but some types failed, note it
        if (result.typesFailed.length > 0) {
          const partialMsg = `⚠️ ${group.client_name}: סיכום חלקי — מקורות שנכשלו: ${result.typesFailed.join(', ')}`;
          await notifyAdmin(partialMsg);
        }
      } else {
        failures.push({
          client_name: group.client_name,
          error: `Failed types (${result.typesFailed.length}/${VALID_RESEARCH_TYPES.length}): ${result.typesFailed.join(', ')}`,
        });
      }
    } catch (err: any) {
      console.error(`[research] researchAll: Fatal error for "${group.client_name}":`, err.message);
      failures.push({
        client_name: group.client_name,
        error: err.message ?? 'Unknown error',
      });
    }
  }

  const failed = failures.length;

  // Done notification — calculate approximate duration from start (not tracked here, use placeholder)
  const doneMsg = researchDoneMessage(completed, failed, `${total} לקוחות`);
  await notifyAdmin(doneMsg);

  // Per-failure notifications
  for (const f of failures) {
    await notifyAdmin(researchFailMessage(f.client_name, f.error));
  }

  return { total, completed, failed, failures };
}
