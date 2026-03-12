// scraper/src/newsapi.ts

import { CircuitBreaker } from './circuit-breaker.js';

const circuitBreaker = new CircuitBreaker('newsapi', 3);

export interface NewsApiArticle {
  title: string;
  description: string;
  url: string;
  source: { name: string };
  publishedAt: string;
}

export interface NewsApiResult {
  articles: NewsApiArticle[];
}

const BASE_URL = 'https://newsapi.org/v2';

async function newsApiGet(path: string, params: Record<string, any>): Promise<NewsApiResult> {
  if (circuitBreaker.tripped) {
    throw new Error(`[newsapi] Circuit breaker open${circuitBreaker.quotaExhausted ? ' (quota exhausted)' : ''}`);
  }

  const apiKey = process.env.NEWSAPI_API_KEY;
  if (!apiKey) throw new Error('[newsapi] Missing NEWSAPI_API_KEY');

  const url = new URL(`${BASE_URL}${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, String(v));
  }

  const start = Date.now();
  let res: Response;
  try {
    res = await fetch(url.toString(), {
      headers: { 'X-Api-Key': apiKey },
      signal: AbortSignal.timeout(15000),
    });
  } catch (err) {
    circuitBreaker.recordFailure();
    throw err;
  }

  const ms = Date.now() - start;
  console.log(`[newsapi] ${path} (${ms}ms)`);

  if (res.status === 429) {
    circuitBreaker.recordFailure(true);
    throw new Error('[newsapi] Rate limited (429)');
  }

  if (!res.ok) {
    circuitBreaker.recordFailure();
    throw new Error(`[newsapi] HTTP ${res.status}`);
  }

  const data = await res.json();
  circuitBreaker.recordSuccess();

  return {
    articles: (data.articles ?? []).map((a: any) => ({
      title: a.title ?? '',
      description: a.description ?? '',
      url: a.url ?? '',
      source: { name: a.source?.name ?? '' },
      publishedAt: a.publishedAt ?? '',
    })),
  };
}

export async function newsApiTopHeadlines(params: {
  country?: string;
  category?: string;
  language?: string;
}): Promise<NewsApiResult> {
  return newsApiGet('/top-headlines', {
    country: params.country,
    category: params.category,
    language: params.language,
  });
}

export async function newsApiEverything(params: {
  q: string;
  language?: string;
  sortBy?: string;
}): Promise<NewsApiResult> {
  return newsApiGet('/everything', {
    q: params.q,
    language: params.language,
    sortBy: params.sortBy,
  });
}
