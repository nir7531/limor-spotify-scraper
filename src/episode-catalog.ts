// scraper/src/episode-catalog.ts
// Spotify internal API client for full episode catalog tracking
// Uses cookies from spotify-auth.json (Playwright storageState format) — no browser needed

import { readFileSync } from 'fs';
import { query, queryOne } from './db.js';

const logger = {
  info: (msg: string, ...args: any[]) => console.log(`[episode-catalog] ${msg}`, ...args),
  warn: (msg: string, ...args: any[]) => console.warn(`[episode-catalog] WARN ${msg}`, ...args),
  error: (msg: string, ...args: any[]) => console.error(`[episode-catalog] ERROR ${msg}`, ...args),
};

// ---- Auth cookie extraction ----

/**
 * Extract sp_dc and sp_key cookies from a Playwright storageState file.
 * spotify-auth.json is written by agent-browser after each login.
 * Format: { cookies: [{ name, value, ... }], origins: [...] }
 */
export function extractSpotifyCookies(authPath: string): { spDc: string; spKey: string } | null {
  try {
    const raw = readFileSync(authPath, 'utf-8');
    const state = JSON.parse(raw);
    const cookies: Array<{ name: string; value: string }> = state.cookies ?? [];
    const spDc = cookies.find((c) => c.name === 'sp_dc')?.value;
    const spKey = cookies.find((c) => c.name === 'sp_key')?.value;
    if (!spDc || !spKey) {
      logger.warn('sp_dc or sp_key not found in spotify-auth.json');
      return null;
    }
    return { spDc, spKey };
  } catch (err) {
    logger.error('Failed to read spotify-auth.json:', err);
    return null;
  }
}

// ---- Token exchange ----

const SPOTIFY_CLIENT_ID = '05a1371ee5194c27860b3ff3ff3979d2';

/**
 * Exchange sp_dc / sp_key cookies for a short-lived bearer token.
 * Token is valid for ~1 hour — sufficient for paginated episode API calls.
 */
export async function exchangeForBearerToken(spDc: string, spKey: string): Promise<string> {
  const url = `https://generic.wg.spotify.com/creator-auth-proxy/v1/web/token?client_id=${SPOTIFY_CLIENT_ID}`;
  const response = await fetch(url, {
    headers: { Cookie: `sp_dc=${spDc}; sp_key=${spKey}` },
  });

  if (!response.ok) {
    throw new Error(`Token exchange failed: ${response.status} ${response.statusText}`);
  }

  const data = await response.json() as { access_token?: string };
  if (!data.access_token) {
    throw new Error('Token exchange returned no access_token');
  }
  return data.access_token;
}

// ---- Episode types ----

export interface SpotifyEpisodeRaw {
  id: string;
  name: string;
  publishDate?: string;         // ISO date string
  durationMs?: number;
  plays?: number;
  streams?: number;
  listeners?: number;
}

// ---- Paginated episode fetch ----

/**
 * Fetch all episodes for a show via Spotify internal API.
 * Fetches ALL historical episodes (start = 2020-01-01).
 * Paginates until the last page (<50 results) or API error.
 */
export async function fetchEpisodeCatalog(
  showId: string,
  authToken: string
): Promise<SpotifyEpisodeRaw[]> {
  const episodes: SpotifyEpisodeRaw[] = [];
  let page = 1;
  const startDate = '2020-01-01';
  const endDate = new Date().toISOString().split('T')[0];

  while (true) {
    const url =
      `https://generic.wg.spotify.com/podcasters/v0/shows/${showId}/episodes` +
      `?start=${startDate}&end=${endDate}&page=${page}&size=50&sortBy=releaseDate&sortOrder=descending`;

    let response: Response;
    try {
      response = await fetch(url, {
        headers: { Authorization: `Bearer ${authToken}` },
      });
    } catch (err) {
      logger.warn(`Network error fetching page ${page}:`, err);
      break;
    }

    if (!response.ok) {
      logger.warn(`Episode catalog API returned ${response.status} on page ${page}`);
      break;
    }

    const data = await response.json() as { episodes?: SpotifyEpisodeRaw[] };
    const pageEpisodes = data.episodes ?? [];

    if (pageEpisodes.length === 0) break;

    episodes.push(...pageEpisodes);

    if (pageEpisodes.length < 50) break; // Last page
    page++;
  }

  logger.info(`Fetched ${episodes.length} episodes across ${page} pages for show ${showId}`);
  return episodes;
}

// ---- Show ID extraction ----

/**
 * Parse the Spotify show ID from a spotify_show_url.
 * Example: "https://creators.spotify.com/pod/show/abc123" → "abc123"
 */
export function parseShowIdFromUrl(spotifyShowUrl: string | null): string | null {
  if (!spotifyShowUrl) return null;
  try {
    const url = new URL(spotifyShowUrl);
    const segments = url.pathname.split('/').filter(Boolean);
    return segments[segments.length - 1] ?? null;
  } catch {
    logger.warn(`Could not parse show ID from URL: ${spotifyShowUrl}`);
    return null;
  }
}

// ---- Episode number parsing ----

/**
 * Parse episode number from Hebrew episode name patterns like "פרק 42" or "פרק 42 - שם הפרק".
 */
function parseEpisodeNumber(episodeName: string): number | null {
  const match = episodeName.match(/פרק\s+(\d+)/i);
  return match ? parseInt(match[1], 10) : null;
}

// ---- DB upsert functions ----

/**
 * Upsert episodes into podcast_episodes.
 * Returns a map of spotify_episode_id → UUID (row id).
 */
export async function upsertEpisodes(
  groupId: string,
  episodes: SpotifyEpisodeRaw[]
): Promise<Map<string, string>> {
  const idMap = new Map<string, string>();

  if (episodes.length === 0) return idMap;

  // Batch upsert in chunks of 100 to avoid giant parameter lists
  const CHUNK_SIZE = 100;
  for (let i = 0; i < episodes.length; i += CHUNK_SIZE) {
    const chunk = episodes.slice(i, i + CHUNK_SIZE);
    const params: any[] = [];
    const valueClauses = chunk.map((ep, idx) => {
      const base = idx * 6;
      params.push(
        groupId,
        ep.id,
        ep.name,
        parseEpisodeNumber(ep.name),
        ep.publishDate ? ep.publishDate.split('T')[0] : null,
        ep.durationMs ?? null
      );
      return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`;
    });

    const rows = await query<{ id: string; spotify_episode_id: string }>(
      `INSERT INTO podcast_episodes
         (group_id, spotify_episode_id, episode_name, episode_number, published_at, duration_ms)
       VALUES ${valueClauses.join(', ')}
       ON CONFLICT (group_id, spotify_episode_id) DO UPDATE SET
         episode_name = EXCLUDED.episode_name,
         episode_number = COALESCE(EXCLUDED.episode_number, podcast_episodes.episode_number),
         published_at = COALESCE(EXCLUDED.published_at, podcast_episodes.published_at),
         duration_ms = COALESCE(EXCLUDED.duration_ms, podcast_episodes.duration_ms)
       RETURNING id, spotify_episode_id`,
      params
    );

    for (const row of rows) {
      idMap.set(row.spotify_episode_id, row.id);
    }
  }

  return idMap;
}

/**
 * Insert per-scan metrics for all cataloged episodes.
 */
export async function insertEpisodeMetrics(
  scanId: string,
  episodeIdMap: Map<string, string>,
  episodes: SpotifyEpisodeRaw[]
): Promise<void> {
  if (episodeIdMap.size === 0) return;

  const params: any[] = [];
  const valueClauses: string[] = [];

  let idx = 0;
  for (const ep of episodes) {
    const episodeId = episodeIdMap.get(ep.id);
    if (!episodeId) continue; // Should not happen after upsertEpisodes

    const base = idx * 5;
    params.push(
      scanId,
      episodeId,
      ep.plays ?? null,
      ep.streams ?? null,
      ep.listeners ?? null
    );
    valueClauses.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
    idx++;
  }

  if (valueClauses.length === 0) return;

  // Batch in chunks of 100
  const CHUNK_SIZE = 100;
  for (let i = 0; i < valueClauses.length; i += CHUNK_SIZE) {
    const chunkClauses = valueClauses.slice(i, i + CHUNK_SIZE);
    const chunkParams = params.slice(i * 5, (i + CHUNK_SIZE) * 5);
    await query(
      `INSERT INTO podcast_episode_metrics (scan_id, episode_id, plays, streams, listeners)
       VALUES ${chunkClauses.join(', ')}
       ON CONFLICT (scan_id, episode_id) DO NOTHING`,
      chunkParams
    );
  }
}

// ---- Episode tagging — link research sessions ----

/**
 * After upserting episodes, tag each episode with its research session if one exists.
 * Matches by folder_name in episode_research_sessions against episode_name in podcast_episodes.
 */
export async function tagResearchedEpisodes(groupId: string): Promise<void> {
  // Find research sessions for this group with folder names
  const sessions = await query<{ id: string; folder_name: string }>(
    `SELECT id, folder_name FROM episode_research_sessions
     WHERE group_id = $1 AND folder_name IS NOT NULL`,
    [groupId]
  );

  if (sessions.length === 0) return;

  let tagged = 0;
  for (const session of sessions) {
    // Exact match by episode name (episode_name stored by Spotify) vs folder_name
    const result = await query<{ id: string }>(
      `UPDATE podcast_episodes
       SET research_session_id = $1
       WHERE group_id = $2
         AND episode_name = $3
         AND research_session_id IS NULL
       RETURNING id`,
      [session.id, groupId, session.folder_name]
    );
    tagged += result.length;
  }

  if (tagged > 0) {
    logger.info(`Tagged ${tagged} episodes with research sessions for group ${groupId}`);
  }
}

// ---- Main orchestration function ----

/**
 * Run the full episode catalog pipeline for one group+scan:
 * 1. Extract cookies from auth file
 * 2. Exchange for bearer token
 * 3. Fetch all episodes via internal API
 * 4. Upsert episodes + insert per-scan metrics
 * 5. Tag episodes with research sessions
 */
export async function runEpisodeCatalog(
  groupId: string,
  scanId: string,
  spotifyShowUrl: string | null,
  authPath: string
): Promise<void> {
  const showId = parseShowIdFromUrl(spotifyShowUrl);
  if (!showId) {
    logger.warn(`No show ID parseable for group ${groupId} (url=${spotifyShowUrl ?? 'null'})`);
    return;
  }

  const cookies = extractSpotifyCookies(authPath);
  if (!cookies) {
    logger.warn(`Could not extract Spotify cookies for group ${groupId}`);
    return;
  }

  let token: string;
  try {
    token = await exchangeForBearerToken(cookies.spDc, cookies.spKey);
  } catch (err) {
    logger.error(`Token exchange failed for group ${groupId}:`, err);
    return;
  }

  let episodes: SpotifyEpisodeRaw[];
  try {
    episodes = await fetchEpisodeCatalog(showId, token);
  } catch (err) {
    logger.error(`Episode fetch failed for group ${groupId}:`, err);
    return;
  }

  if (episodes.length === 0) {
    logger.warn(`No episodes returned for group ${groupId} (show ${showId})`);
    return;
  }

  try {
    const episodeIdMap = await upsertEpisodes(groupId, episodes);
    await insertEpisodeMetrics(scanId, episodeIdMap, episodes);
    await tagResearchedEpisodes(groupId);
    logger.info(`Cataloged ${episodes.length} episodes for group ${groupId}`);
  } catch (err) {
    logger.error(`DB operations failed for group ${groupId}:`, err);
  }
}
