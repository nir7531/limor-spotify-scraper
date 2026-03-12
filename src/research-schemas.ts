// scraper/src/research-schemas.ts
// Zod validation schemas for all 7 research types' research_data JSONB.
// These schemas define the contract that v1.1.10 and other consumers rely on.

import { z } from 'zod';

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const nonEmptyString = z.string().min(1);
const optionalUrl = z.string().url().optional();
const trendDirection = z.enum(['rising', 'falling', 'stable', 'breakout', 'declining']).optional();
const searchVolumeHint = z.enum(['very_high', 'high', 'medium', 'low', 'very_low']).optional();

// Common wrapper fields that every schema must include
const withCommonFields = <T extends z.ZodRawShape>(shape: T) =>
  z.object({
    ...shape,
    summary: nonEmptyString,
    changes_from_previous: z.string().nullable(),
    data_sources: z.array(z.string()),
  });

// ---------------------------------------------------------------------------
// competitor
// ---------------------------------------------------------------------------

const CompetitorFinding = z.object({
  name: nonEmptyString,
  description: nonEmptyString,
  url: optionalUrl,
  relevance: z.string().optional(),
  market: z.enum(['hebrew', 'global', 'english']).optional(),
  google_trends_interest: z.number().min(0).max(100).optional(),
  interest_trend: trendDirection,
});

const GlobalCompetitorFinding = z.object({
  name: nonEmptyString,
  description: nonEmptyString,
  url: optionalUrl,
  relevance: z.string().optional(),
});

export const CompetitorSchema = withCommonFields({
  findings: z.array(CompetitorFinding).min(1),
  global_competitors: z.array(GlobalCompetitorFinding).optional(),
});

// ---------------------------------------------------------------------------
// keyword
// ---------------------------------------------------------------------------

const KeywordFinding = z.object({
  keyword: nonEmptyString,
  search_volume_hint: searchVolumeHint,
  google_trends_interest: z.number().min(0).max(100).optional(),
  interest_trend: trendDirection,
  related_queries: z.array(z.string()).optional(),
  suggestion: z.string().optional(),
});

const LongTailKeyword = z.object({
  keyword: nonEmptyString,
  interest: z.number().min(0).max(100).optional(),
  competition: z.enum(['high', 'medium', 'low']).optional(),
});

export const KeywordSchema = withCommonFields({
  findings: z.array(KeywordFinding).min(1),
  long_tail_keywords: z.array(LongTailKeyword).optional(),
});

// ---------------------------------------------------------------------------
// trend
// ---------------------------------------------------------------------------

const TrendFinding = z.object({
  topic: nonEmptyString,
  description: z.string().optional(),
  source: z.string().optional(),
  google_trends_interest: z.number().min(0).max(100).optional(),
  interest_trend: trendDirection,
  relevance: z.string().optional(),
});

const GlobalTrend = z.object({
  topic: nonEmptyString,
  description: z.string().optional(),
  google_trends_interest: z.number().min(0).max(100).optional(),
  estimated_arrival_in_israel: z.string().optional(),
});

export const TrendSchema = withCommonFields({
  findings: z.array(TrendFinding).min(1),
  global_trends: z.array(GlobalTrend).optional(),
  daily_trending_israel: z.array(z.string()).optional(),
});

// ---------------------------------------------------------------------------
// guest
// ---------------------------------------------------------------------------

const GuestFinding = z.object({
  category: z.enum(['israeli_expert', 'global_leader', 'rising_name']),
  name: nonEmptyString,
  field: z.string().optional(),
  description: z.string().optional(),
  source: z.string().optional(),
  url: optionalUrl,
  appeared_on: z.array(z.string()).optional(),
  podcast_appearances: z.number().optional(),
  google_trends_interest: z.number().min(0).max(100).optional(),
  interest_trend: trendDirection,
  relevance: z.string().optional(),
  why_now: z.string().optional(),
});

export const GuestSchema = withCommonFields({
  findings: z.array(GuestFinding).min(1),
  already_hosted: z.array(z.string()).optional(),
  top_performing_guest_type: z.string().optional(),
});

// ---------------------------------------------------------------------------
// related_global
// ---------------------------------------------------------------------------

const RelatedGlobalFinding = z.object({
  name: nonEmptyString,
  category: z.string().optional(),
  description: z.string().optional(),
  url: optionalUrl,
  listen_score: z.number().min(0).max(100).optional(),
  what_theyre_doing: z.string().optional(),
  actionable_for_client: z.string().optional(),
});

export const RelatedGlobalSchema = withCommonFields({
  findings: z.array(RelatedGlobalFinding).min(1),
  global_category_trends: z.array(z.string()).optional(),
});

// ---------------------------------------------------------------------------
// episode_topics
// ---------------------------------------------------------------------------

const EpisodeTopicFinding = z.object({
  topic: nonEmptyString,
  description: z.string().optional(),
  search_demand: searchVolumeHint,
  google_trends_interest: z.number().min(0).max(100).optional(),
  competition_in_hebrew: z.enum(['high', 'medium', 'low']).optional(),
  competitor_coverage: z.string().optional(),
  suggested_angle: z.string().optional(),
});

const SeasonalTopic = z.object({
  topic: nonEmptyString,
  peak_month: z.string().optional(),
  interest: z.number().min(0).max(100).optional(),
});

export const EpisodeTopicsSchema = withCommonFields({
  findings: z.array(EpisodeTopicFinding).min(1),
  seasonal_topics: z.array(SeasonalTopic).optional(),
  topics_client_already_covered: z.array(z.string()).optional(),
});

// ---------------------------------------------------------------------------
// benchmarks
// ---------------------------------------------------------------------------

const BenchmarkFinding = z.discriminatedUnion('category', [
  z.object({
    category: z.literal('category_benchmarks'),
    median_plays_per_episode: z.number().optional(),
    top_10_pct_threshold: z.number().optional(),
    avg_episodes_per_month: z.number().optional(),
    avg_episode_length_min: z.number().optional(),
  }),
  z.object({
    category: z.literal('israeli_market'),
    spotify_share: z.string().optional(),
    audience_skew: z.string().optional(),
    peak_listening: z.string().optional(),
  }),
  z.object({
    category: z.literal('brand_tracking'),
    show_name_interest: z.number().min(0).max(100).optional(),
    interest_trend: trendDirection,
    interest_history: z.array(z.number()).optional(),
  }),
  z.object({
    category: z.literal('category_growth'),
    interest_one_year_ago: z.number().min(0).max(100).optional(),
    interest_now: z.number().min(0).max(100).optional(),
    growth_pct: z.number().optional(),
    trend: z.enum(['growing', 'stable', 'shrinking']).optional(),
  }),
]);

export const BenchmarksSchema = withCommonFields({
  findings: z.array(BenchmarkFinding).min(1),
});

// ---------------------------------------------------------------------------
// Schema registry
// ---------------------------------------------------------------------------

const SCHEMA_MAP: Record<string, z.ZodTypeAny> = {
  competitor: CompetitorSchema,
  keyword: KeywordSchema,
  trend: TrendSchema,
  guest: GuestSchema,
  related_global: RelatedGlobalSchema,
  episode_topics: EpisodeTopicsSchema,
  benchmarks: BenchmarksSchema,
};

export type ResearchType = keyof typeof SCHEMA_MAP;

export const VALID_RESEARCH_TYPES = Object.keys(SCHEMA_MAP) as ResearchType[];

/**
 * Validate research_data JSONB against the Zod schema for the given research type.
 * Throws a ZodError if validation fails.
 *
 * @param type - One of the 7 research type identifiers
 * @param data - Raw parsed JSON from the LLM / API
 * @returns The validated and typed data
 */
export function validateResearchData(type: string, data: unknown): unknown {
  const schema = SCHEMA_MAP[type];
  if (!schema) {
    throw new Error(`Unknown research type: "${type}". Valid types: ${VALID_RESEARCH_TYPES.join(', ')}`);
  }
  return schema.parse(data);
}

/**
 * Safe variant — returns { success, data, error } instead of throwing.
 */
export function safeValidateResearchData(
  type: string,
  data: unknown
): { success: true; data: unknown } | { success: false; error: string } {
  const schema = SCHEMA_MAP[type];
  if (!schema) {
    return { success: false, error: `Unknown research type: "${type}"` };
  }
  const result = schema.safeParse(data);
  if (result.success) {
    return { success: true, data: result.data };
  }
  return { success: false, error: result.error.message };
}
