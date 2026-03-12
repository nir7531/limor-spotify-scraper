// scraper/src/serper.ts

import { CircuitBreaker } from './circuit-breaker.js';

const circuitBreaker = new CircuitBreaker('serper', 5);

export interface SerperResult {
  organic: Array<{ title: string; link: string; snippet: string }>;
  searchParameters: any;
}

export async function serperSearch(
  query: string,
  options?: { gl?: string; hl?: string; type?: 'search' | 'news' }
): Promise<SerperResult> {
  if (circuitBreaker.tripped) {
    throw new Error(`[serper] Circuit breaker open${circuitBreaker.quotaExhausted ? ' (quota exhausted)' : ''}`);
  }

  const apiKey = process.env.SERPER_API_KEY;
  if (!apiKey) throw new Error('[serper] Missing SERPER_API_KEY');

  const endpoint = options?.type === 'news'
    ? 'https://google.serper.dev/news'
    : 'https://google.serper.dev/search';

  const body: Record<string, any> = { q: query, num: 10 };
  if (options?.gl) body.gl = options.gl;
  if (options?.hl) body.hl = options.hl;

  const start = Date.now();
  let res: Response;
  try {
    res = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'X-API-KEY': apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15000),
    });
  } catch (err) {
    circuitBreaker.recordFailure();
    throw err;
  }

  const ms = Date.now() - start;
  console.log(`[serper] Query: "${query}" (${ms}ms)`);

  if (res.status === 429) {
    circuitBreaker.recordFailure(true);
    throw new Error('[serper] Rate limited (429)');
  }

  if (!res.ok) {
    circuitBreaker.recordFailure();
    throw new Error(`[serper] HTTP ${res.status}`);
  }

  const data = await res.json();
  circuitBreaker.recordSuccess();

  return {
    organic: (data.organic ?? []).map((item: any) => ({
      title: item.title ?? '',
      link: item.link ?? '',
      snippet: item.snippet ?? '',
    })),
    searchParameters: data.searchParameters ?? {},
  };
}
