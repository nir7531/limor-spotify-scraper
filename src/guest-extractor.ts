// scraper/src/guest-extractor.ts
// LLM-based guest name extraction from Hebrew podcast episode titles.
// Used during the guest research pipeline (step 2 of research flow per client).

import Anthropic from '@anthropic-ai/sdk';

const HAIKU = 'claude-haiku-4-5-20250514';

const anthropicClient = new Anthropic();

export interface GuestExtractionResult {
  episode_name: string;
  guest_name: string | null;
  episode_type: 'interview' | 'solo' | 'panel' | 'unknown';
}

const SYSTEM_PROMPT = `You are an expert at parsing Hebrew podcast episode titles to identify guests and episode formats.

You will receive a list of Hebrew episode titles and must classify each one.

## Hebrew episode title patterns

**Interview episodes** (episode_type: "interview"):
- "פרק N - [שם אורח] על [נושא]" → guest is the name between the dash and "על"
- "[שם אורח]: [נושא]" → guest is before the colon
- "ראיון עם [שם אורח]" → guest follows "ראיון עם"
- "אורח: [שם אורח]" → guest follows "אורח:"
- "[שם אורח] - [נושא]" → may be a guest if the name sounds like a person (Israeli or English name)

**Solo episodes** (episode_type: "solo", guest_name: null):
- Starts with "סולו:" or "סולו פרק"
- Contains "ביחד עם עצמי", "מחשבות", "סכום", "סיכום", "עדכון"
- No person name visible

**Panel episodes** (episode_type: "panel"):
- "עם [name1] ו[name2]" → 2+ guests
- "פאנל:", "שולחן עגול"

**Unknown** (episode_type: "unknown", guest_name: null):
- Title is ambiguous or only contains a topic with no clear person name

## Name detection rules
- Israeli names: common Hebrew first names (שרה, דוד, אורן, יעל, גיל, נועה, ליאור, עמית, רן, שי, אלי, ורד, נתן, אדם, רוני, תמי, גלי, עמי, בן, דן, תום, עידן, מאיה, etc.)
- International names: capitalized English names (Elon, Sam, Reid, Tim, etc.)
- Titles: ד"ר, פרופ', מר, גב' — indicate a person follows
- Company names or pure topics are NOT guests

## Research instruction
For Israeli experts, always search in Hebrew.
For international figures, search in English.

## Output format
Return ONLY a valid JSON array. No markdown, no explanation. Each element:
{
  "episode_name": "<exact title as given>",
  "guest_name": "<first and last name, or null>",
  "episode_type": "interview" | "solo" | "panel" | "unknown"
}`;

/**
 * Extract guest names and episode types from a list of Hebrew episode titles.
 * Uses Haiku for cost efficiency.
 *
 * @param episodeTitles - Array of Hebrew episode title strings (e.g. from podcast_episode_rankings)
 * @returns Array of extraction results, one per title, preserving order
 */
export async function extractGuests(episodeTitles: string[]): Promise<GuestExtractionResult[]> {
  if (episodeTitles.length === 0) {
    return [];
  }

  // Format titles as a numbered list for the LLM
  const titlesText = episodeTitles
    .map((title, idx) => `${idx + 1}. ${title}`)
    .join('\n');

  const userMessage = `Extract guest information from these ${episodeTitles.length} Hebrew podcast episode titles:

${titlesText}

Return a JSON array with exactly ${episodeTitles.length} elements (one per title, in the same order).`;

  const response = await anthropicClient.messages.create({
    model: HAIKU,
    max_tokens: 2048,
    system: SYSTEM_PROMPT,
    messages: [
      {
        role: 'user',
        content: userMessage,
      },
    ],
  });

  // Extract the text content from the response
  const textBlock = response.content.find(
    (block): block is Anthropic.TextBlock => block.type === 'text'
  );

  if (!textBlock) {
    throw new Error('guest-extractor: No text response from Haiku');
  }

  let parsed: unknown;
  try {
    // Strip potential markdown code fences (```json ... ```)
    const raw = textBlock.text.trim().replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(`guest-extractor: Failed to parse LLM JSON response: ${err}`);
  }

  if (!Array.isArray(parsed)) {
    throw new Error('guest-extractor: Expected JSON array from LLM');
  }

  // Validate and normalise each result
  const results: GuestExtractionResult[] = parsed.map((item: unknown, idx: number) => {
    if (typeof item !== 'object' || item === null) {
      return {
        episode_name: episodeTitles[idx] ?? `episode_${idx}`,
        guest_name: null,
        episode_type: 'unknown' as const,
      };
    }

    const obj = item as Record<string, unknown>;
    const episodeName = typeof obj.episode_name === 'string'
      ? obj.episode_name
      : (episodeTitles[idx] ?? `episode_${idx}`);

    const guestName = typeof obj.guest_name === 'string' && obj.guest_name.length > 0
      ? obj.guest_name
      : null;

    const rawType = typeof obj.episode_type === 'string' ? obj.episode_type : 'unknown';
    const episodeType: GuestExtractionResult['episode_type'] =
      rawType === 'interview' || rawType === 'solo' || rawType === 'panel'
        ? rawType
        : 'unknown';

    return { episode_name: episodeName, guest_name: guestName, episode_type: episodeType };
  });

  // Ensure array length matches input (guard against hallucinated extras or truncation)
  if (results.length !== episodeTitles.length) {
    // Pad with unknowns or truncate
    while (results.length < episodeTitles.length) {
      const idx = results.length;
      results.push({ episode_name: episodeTitles[idx], guest_name: null, episode_type: 'unknown' });
    }
    return results.slice(0, episodeTitles.length);
  }

  return results;
}
