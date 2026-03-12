// scraper/src/orchestrator.ts
// LLM-orchestrated Spotify scraper using Claude API with tool_use

import Anthropic from '@anthropic-ai/sdk';
import { execSync } from 'child_process';
import { readFileSync, existsSync } from 'fs';
import { SKILL_CONTENT } from './skill-prompt.js';

const HAIKU = 'claude-haiku-4-5-20251001';
const SONNET = 'claude-sonnet-4-6';
const MAX_TURNS = 120;
const COMMAND_TIMEOUT = 60_000;

export interface ScrapeResult {
  overview?: {
    on_spotify?: Record<string, any>;
    all_platforms?: Record<string, any>;
  };
  discovery?: any;
  audience?: any;
  episode_rankings?: any[];
  scan_status: 'completed' | 'partial' | 'failed';
  notes?: string;
}

const tools: Anthropic.Tool[] = [
  {
    name: 'run_command',
    description:
      'Execute a shell command (agent-browser CLI or other). Returns stdout/stderr. ' +
      'If the command takes a screenshot, the image will be analyzed by a vision model and the analysis included in the result.',
    input_schema: {
      type: 'object' as const,
      properties: {
        command: { type: 'string', description: 'The shell command to execute' },
      },
      required: ['command'],
    },
  },
  {
    name: 'report_data',
    description:
      'Report the final scraped analytics data. Call this when all phases are complete (or when you need to stop early).',
    input_schema: {
      type: 'object' as const,
      properties: {
        data: {
          type: 'object',
          description: 'The complete analytics JSON matching the output schema',
        },
        status: {
          type: 'string',
          enum: ['completed', 'partial', 'failed'],
          description: 'completed = all sections OK, partial = some sections failed, failed = critical error',
        },
        notes: {
          type: 'string',
          description: 'Any notes about what succeeded/failed',
        },
      },
      required: ['data', 'status'],
    },
  },
];

function buildSystemPrompt(showName: string, showUrl: string | null, authPath: string): string {
  const skillContent = SKILL_CONTENT
    .replace(/<AUTH_PATH>/g, authPath);

  return `You are a Spotify for Creators analytics scraper bot. You have two tools:
- run_command: execute agent-browser CLI commands (or any shell command)
- report_data: submit the final structured analytics JSON when done

Your task: scrape ALL analytics data for the podcast "${showName}" from Spotify for Creators.

## Environment
- Running headless in a Docker container. Do NOT use --headed flag.
- Auth state path: ${authPath}
- Credentials available as env vars: SPOTIFY_CREATORS_EMAIL, SPOTIFY_CREATORS_PASSWORD
${showUrl ? `- Show URL: ${showUrl}` : '- No show URL — search for the show by name.'}

## Rules
- Execute ONE command per tool call. Wait for the result before deciding next step.
- After every click/fill/navigation: re-snapshot with \`agent-browser snapshot -i\`
- After dropdown/tab changes: \`agent-browser wait 3000\` then snapshot
- For Plays counter and Episode Rankings: ALWAYS use \`agent-browser screenshot\` (CSS animation bug)
- When done (or on critical failure): call report_data with the complete JSON

## Scraping Instructions

${skillContent}

## Output Schema

When calling report_data, use this JSON structure for the data field:
{
  "overview": {
    "on_spotify": {
      "7d": { "plays": N|null, "consumption_hours": N|null, "followers_delta": N|null, "date_range": "..." },
      "30d": { ... }, "90d": { ... }, "all_time": { ... }
    },
    "all_platforms": {
      "all_time": { "total_streams": N|null, "per_episode_avg": N|null, "date_range": null }
    }
  },
  "discovery": {
    "date_range": "...",
    "funnel": { "reached": N, "interested": N, "consumed": N, "reach_to_interest_pct": 0.NNN, "interest_to_consumed_pct": 0.NNN },
    "key_stats": { "headline": "...", "hours_per_person": N.N, "follow_pct": 0.NNN },
    "traffic": { "impressions_total": N, "source_home": N, "source_search": N, "source_library": N, "source_other": N },
    "clips": [{ "rank": N, "name": "...", "date": "...", "duration_seconds": N, "impressions": N, "plays_from_clips": N }]
  },
  "audience": {
    "demographics": {
      "7d": { "gender": { "male": 0.NNN, ... }, "age": { "0_17": 0.NNN, ... } },
      "30d": { ... }, "90d": { ... }, "all_time": { ... }
    },
    "geographic": [{ "rank": N, "country": "...", "pct": 0.NNN }],
    "platforms": [{ "name": "...", "pct": 0.NNN }]
  },
  "episode_rankings": [{ "rank": N, "episode_name": "...", "episode_number": N|null, "streams": N }]
}

All percentages as decimals (0.652 not 65.2%). Integers without commas. null for unavailable data.`;
}

/** Use Sonnet vision to extract data from a screenshot */
async function extractFromScreenshot(
  client: Anthropic,
  imagePath: string,
  showName: string
): Promise<string> {
  if (!existsSync(imagePath)) {
    return `[Screenshot file not found: ${imagePath}]`;
  }

  const imageBuffer = readFileSync(imagePath);
  const base64 = imageBuffer.toString('base64');
  const mediaType: 'image/png' | 'image/jpeg' = imagePath.endsWith('.png') ? 'image/png' : 'image/jpeg';

  const response = await client.messages.create({
    model: SONNET,
    max_tokens: 2048,
    messages: [
      {
        role: 'user',
        content: [
          {
            type: 'image',
            source: { type: 'base64', media_type: mediaType, data: base64 },
          },
          {
            type: 'text',
            text: `Extract all visible numbers and data from this Spotify for Creators analytics screenshot for "${showName}".

For Overview screenshots: extract Plays (integer), Consumption hours (integer), Followers delta (+/- integer), and the date range text.
For Episode Rankings: extract each episode's rank (top to bottom), full name, episode number (from "פרק N" pattern), and streams count (integer to the right of each bar).
For All Platforms: extract Total streams and Per episode average.

Return the data as clearly labeled key-value pairs. Strip commas from numbers. Be precise — these numbers go into a database.`,
          },
        ],
      },
    ],
  });

  return response.content
    .filter((b): b is Anthropic.TextBlock => b.type === 'text')
    .map((b) => b.text)
    .join('\n');
}

export async function orchestrateScrape(
  showName: string,
  showUrl: string | null,
  scanId: string,
  authPath: string
): Promise<ScrapeResult> {
  const client = new Anthropic();
  const systemPrompt = buildSystemPrompt(showName, showUrl, authPath);

  const messages: Anthropic.MessageParam[] = [
    {
      role: 'user',
      content: `Begin scraping analytics for "${showName}". Start with Phase 0 (Setup & Auth).`,
    },
  ];

  for (let turn = 0; turn < MAX_TURNS; turn++) {
    let response: Anthropic.Message;
    try {
      response = await client.messages.create({
        model: HAIKU,
        max_tokens: 4096,
        system: systemPrompt,
        tools,
        messages,
      });
    } catch (err: any) {
      console.error(`Claude API error on turn ${turn}:`, err.message);
      return { scan_status: 'failed', notes: `Claude API error: ${err.message}` };
    }

    // Append assistant response to conversation
    messages.push({ role: 'assistant', content: response.content });

    // Find tool_use blocks
    const toolBlocks = response.content.filter(
      (b): b is Anthropic.ToolUseBlock => b.type === 'tool_use'
    );

    // No tool calls = LLM thinks it's done (or stuck)
    if (toolBlocks.length === 0) {
      if (response.stop_reason === 'end_turn') {
        return { scan_status: 'partial', notes: 'LLM finished without calling report_data' };
      }
      continue;
    }

    // Process each tool call — collect all results before returning
    const toolResults: Anthropic.ToolResultBlockParam[] = [];
    let reportPayload: { data: any; status: string; notes?: string } | null = null;

    for (const block of toolBlocks) {
      if (block.name === 'report_data') {
        reportPayload = block.input as { data: any; status: string; notes?: string };
        toolResults.push({
          type: 'tool_result',
          tool_use_id: block.id,
          content: 'Acknowledged.',
        });
        continue;
      }

      if (block.name === 'run_command') {
        const { command } = block.input as { command: string };
        console.log(`  [turn ${turn}] $ ${command}`);

        try {
          const stdout = execSync(command, {
            timeout: COMMAND_TIMEOUT,
            encoding: 'utf-8',
            env: { ...process.env },
          });

          // Check if this was a screenshot command → analyze with Sonnet
          const screenshotMatch = command.match(/screenshot\s+(\S+\.(?:png|jpg|jpeg))/i);
          if (screenshotMatch) {
            const imagePath = screenshotMatch[1];
            console.log(`  [turn ${turn}] Analyzing screenshot: ${imagePath}`);
            const analysis = await extractFromScreenshot(client, imagePath, showName);
            toolResults.push({
              type: 'tool_result',
              tool_use_id: block.id,
              content: `Command succeeded.\nstdout: ${stdout.trim()}\n\n[Vision analysis of screenshot]:\n${analysis}`,
            });
          } else {
            toolResults.push({
              type: 'tool_result',
              tool_use_id: block.id,
              content: stdout.slice(0, 15_000) || '(no output)',
            });
          }
        } catch (err: any) {
          const errMsg = err.stderr?.slice(0, 3000) || err.message?.slice(0, 3000) || 'Unknown error';
          console.error(`  [turn ${turn}] Command failed: ${errMsg.slice(0, 200)}`);
          toolResults.push({
            type: 'tool_result',
            tool_use_id: block.id,
            content: `Command failed.\nstderr: ${errMsg}`,
            is_error: true,
          });
        }
      }
    }

    // If report_data was called, return the payload after processing all tool calls
    if (reportPayload) {
      return {
        ...reportPayload.data,
        scan_status: reportPayload.status as ScrapeResult['scan_status'],
        notes: reportPayload.notes,
      };
    }

    // Append tool results as user message
    if (toolResults.length > 0) {
      messages.push({ role: 'user', content: toolResults });
    }

    // Progress logging
    if (turn > 0 && turn % 20 === 0) {
      console.warn(`[orchestrator] Turn ${turn}/${MAX_TURNS} — still running for ${showName}`);
    }
  }

  return { scan_status: 'failed', notes: `Exceeded max turns (${MAX_TURNS})` };
}
