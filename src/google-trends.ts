// scraper/src/google-trends.ts

import googleTrends from 'google-trends-api';
import { CircuitBreaker } from './circuit-breaker.js';

const circuitBreaker = new CircuitBreaker('google-trends', 3);

const DEFAULT_GEO = 'IL';

function delay(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

async function withBreaker<T>(label: string, fn: () => Promise<string>): Promise<T> {
  if (circuitBreaker.tripped) {
    throw new Error(`[google-trends] Circuit breaker open`);
  }

  const start = Date.now();
  let raw: string;
  try {
    raw = await fn();
  } catch (err) {
    circuitBreaker.recordFailure();
    throw err;
  }

  const ms = Date.now() - start;
  console.log(`[google-trends] ${label} (${ms}ms)`);

  let parsed: T;
  try {
    parsed = JSON.parse(raw) as T;
  } catch {
    circuitBreaker.recordFailure();
    throw new Error(`[google-trends] Failed to parse response for ${label}`);
  }

  circuitBreaker.recordSuccess();
  return parsed;
}

export async function trendInterestOverTime(keyword: string, geo = DEFAULT_GEO): Promise<any> {
  await delay(2500);
  const result = await withBreaker<any>(`interestOverTime("${keyword}")`, () =>
    googleTrends.interestOverTime({ keyword, geo })
  );
  const timelineData = result?.default?.timelineData ?? [];
  if (timelineData.length === 0) circuitBreaker.recordEmpty();
  return result;
}

export async function trendRelatedQueries(keyword: string, geo = DEFAULT_GEO): Promise<any> {
  await delay(2500);
  const result = await withBreaker<any>(`relatedQueries("${keyword}")`, () =>
    googleTrends.relatedQueries({ keyword, geo })
  );
  const items = result?.default?.rankedList ?? [];
  if (items.length === 0) circuitBreaker.recordEmpty();
  return result;
}

export async function trendRelatedTopics(keyword: string, geo = DEFAULT_GEO): Promise<any> {
  await delay(2500);
  const result = await withBreaker<any>(`relatedTopics("${keyword}")`, () =>
    googleTrends.relatedTopics({ keyword, geo })
  );
  const items = result?.default?.rankedList ?? [];
  if (items.length === 0) circuitBreaker.recordEmpty();
  return result;
}

export async function trendDailyTrends(geo = DEFAULT_GEO): Promise<any> {
  await delay(2500);
  const result = await withBreaker<any>(`dailyTrends(${geo})`, () =>
    googleTrends.dailyTrends({ geo })
  );
  const trends = result?.default?.trendingSearchesDays ?? [];
  if (trends.length === 0) circuitBreaker.recordEmpty();
  return result;
}
