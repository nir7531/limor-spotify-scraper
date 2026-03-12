// scraper/src/listen-notes.ts

import { CircuitBreaker } from './circuit-breaker.js';

const circuitBreaker = new CircuitBreaker('listen-notes', 3);

const BASE_URL = 'https://listen-api.listennotes.com/api/v2';

async function listenNotesGet(path: string, params: Record<string, any>): Promise<any> {
  if (circuitBreaker.tripped) {
    throw new Error(`[listen-notes] Circuit breaker open${circuitBreaker.quotaExhausted ? ' (quota exhausted)' : ''}`);
  }

  const apiKey = process.env.LISTEN_NOTES_API_KEY;
  if (!apiKey) throw new Error('[listen-notes] Missing LISTEN_NOTES_API_KEY');

  const url = new URL(`${BASE_URL}${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, String(v));
  }

  const start = Date.now();
  let res: Response;
  try {
    res = await fetch(url.toString(), {
      headers: { 'X-ListenAPI-Key': apiKey },
      signal: AbortSignal.timeout(15000),
    });
  } catch (err) {
    circuitBreaker.recordFailure();
    throw err;
  }

  const ms = Date.now() - start;
  console.log(`[listen-notes] ${path} (${ms}ms)`);

  if (res.status === 429) {
    circuitBreaker.recordFailure(true);
    throw new Error('[listen-notes] Rate limited (429)');
  }

  if (!res.ok) {
    circuitBreaker.recordFailure();
    throw new Error(`[listen-notes] HTTP ${res.status}`);
  }

  const data = await res.json();
  circuitBreaker.recordSuccess();
  return data;
}

export async function listenNotesSearch(params: {
  q: string;
  language?: string;
  type?: 'podcast' | 'episode';
}): Promise<any> {
  return listenNotesGet('/search', {
    q: params.q,
    language: params.language,
    type: params.type,
  });
}

export async function listenNotesBestPodcasts(params: {
  language?: string;
  region?: string;
  genre_id?: number;
}): Promise<any> {
  return listenNotesGet('/best_podcasts', {
    language: params.language,
    region: params.region,
    genre_id: params.genre_id,
  });
}
