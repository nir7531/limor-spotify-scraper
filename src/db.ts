// scraper/src/db.ts
import pg from 'pg';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

export async function query<T = any>(sql: string, params?: any[]): Promise<T[]> {
  const { rows } = await pool.query(sql, params);
  return rows;
}

export async function queryOne<T = any>(sql: string, params?: any[]): Promise<T | null> {
  const rows = await query<T>(sql, params);
  return rows[0] ?? null;
}

export async function closePool() {
  await pool.end();
}

// ---- Insert functions for analytics tables ----

export async function insertOverviewSpotify(
  scanId: string,
  period: string,
  data: { plays?: number | null; consumption_hours?: number | null; followers_delta?: number | null; date_range?: string | null }
): Promise<void> {
  await query(
    `INSERT INTO podcast_overview_spotify (scan_id, period, date_range, plays, consumption_hours, followers_delta)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (scan_id, period) DO UPDATE SET
       date_range = EXCLUDED.date_range, plays = EXCLUDED.plays,
       consumption_hours = EXCLUDED.consumption_hours, followers_delta = EXCLUDED.followers_delta`,
    [scanId, period, data.date_range ?? null, data.plays ?? null, data.consumption_hours ?? null, data.followers_delta ?? null]
  );
}

export async function insertOverviewAllPlatforms(
  scanId: string,
  period: string,
  data: { total_streams?: number | null; per_episode_avg?: number | null; date_range?: string | null }
): Promise<void> {
  await query(
    `INSERT INTO podcast_overview_all_platforms (scan_id, period, date_range, total_streams, per_episode_avg)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (scan_id, period) DO UPDATE SET
       date_range = EXCLUDED.date_range, total_streams = EXCLUDED.total_streams, per_episode_avg = EXCLUDED.per_episode_avg`,
    [scanId, period, data.date_range ?? null, data.total_streams ?? null, data.per_episode_avg ?? null]
  );
}

export async function insertDiscovery(
  scanId: string,
  data: {
    date_range?: string | null;
    funnel_reached?: number | null; funnel_interested?: number | null; funnel_consumed?: number | null;
    funnel_reach_to_interest_pct?: number | null; funnel_interest_to_consumed_pct?: number | null;
    key_stats_headline?: string | null; key_stats_hours_per_person?: number | null; key_stats_follow_pct?: number | null;
    traffic_impressions_total?: number | null; traffic_source_home?: number | null;
    traffic_source_search?: number | null; traffic_source_library?: number | null; traffic_source_other?: number | null;
  }
): Promise<void> {
  await query(
    `INSERT INTO podcast_discovery (
       scan_id, date_range,
       funnel_reached, funnel_interested, funnel_consumed,
       funnel_reach_to_interest_pct, funnel_interest_to_consumed_pct,
       key_stats_headline, key_stats_hours_per_person, key_stats_follow_pct,
       traffic_impressions_total, traffic_source_home, traffic_source_search, traffic_source_library, traffic_source_other
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     ON CONFLICT (scan_id) DO UPDATE SET
       date_range=EXCLUDED.date_range, funnel_reached=EXCLUDED.funnel_reached,
       funnel_interested=EXCLUDED.funnel_interested, funnel_consumed=EXCLUDED.funnel_consumed,
       funnel_reach_to_interest_pct=EXCLUDED.funnel_reach_to_interest_pct,
       funnel_interest_to_consumed_pct=EXCLUDED.funnel_interest_to_consumed_pct,
       key_stats_headline=EXCLUDED.key_stats_headline, key_stats_hours_per_person=EXCLUDED.key_stats_hours_per_person,
       key_stats_follow_pct=EXCLUDED.key_stats_follow_pct,
       traffic_impressions_total=EXCLUDED.traffic_impressions_total, traffic_source_home=EXCLUDED.traffic_source_home,
       traffic_source_search=EXCLUDED.traffic_source_search, traffic_source_library=EXCLUDED.traffic_source_library,
       traffic_source_other=EXCLUDED.traffic_source_other`,
    [
      scanId, data.date_range ?? null,
      data.funnel_reached ?? null, data.funnel_interested ?? null, data.funnel_consumed ?? null,
      data.funnel_reach_to_interest_pct ?? null, data.funnel_interest_to_consumed_pct ?? null,
      data.key_stats_headline ?? null, data.key_stats_hours_per_person ?? null, data.key_stats_follow_pct ?? null,
      data.traffic_impressions_total ?? null, data.traffic_source_home ?? null,
      data.traffic_source_search ?? null, data.traffic_source_library ?? null, data.traffic_source_other ?? null,
    ]
  );
}

export async function insertDiscoveryClip(
  scanId: string,
  clip: {
    clip_rank: number; clip_name: string; clip_date?: string | null;
    clip_duration_seconds?: number | null; impressions?: number | null; plays_from_clips?: number | null;
  }
): Promise<void> {
  await query(
    `INSERT INTO podcast_discovery_clips (scan_id, clip_rank, clip_name, clip_date, clip_duration_seconds, impressions, plays_from_clips)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     ON CONFLICT (scan_id, clip_rank) DO UPDATE SET
       clip_name=EXCLUDED.clip_name, clip_date=EXCLUDED.clip_date,
       clip_duration_seconds=EXCLUDED.clip_duration_seconds, impressions=EXCLUDED.impressions,
       plays_from_clips=EXCLUDED.plays_from_clips`,
    [scanId, clip.clip_rank, clip.clip_name, clip.clip_date ?? null, clip.clip_duration_seconds ?? null, clip.impressions ?? null, clip.plays_from_clips ?? null]
  );
}

export async function insertAudienceDemographics(
  scanId: string,
  period: string,
  data: {
    gender_male?: number | null; gender_female?: number | null;
    gender_non_binary?: number | null; gender_not_specified?: number | null;
    age_0_17?: number | null; age_18_22?: number | null; age_23_27?: number | null;
    age_28_34?: number | null; age_35_44?: number | null; age_45_59?: number | null;
    age_60_plus?: number | null; age_unknown?: number | null;
  }
): Promise<void> {
  await query(
    `INSERT INTO podcast_audience_demographics (
       scan_id, period,
       gender_male, gender_female, gender_non_binary, gender_not_specified,
       age_0_17, age_18_22, age_23_27, age_28_34, age_35_44, age_45_59, age_60_plus, age_unknown
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
     ON CONFLICT (scan_id, period) DO UPDATE SET
       gender_male=EXCLUDED.gender_male, gender_female=EXCLUDED.gender_female,
       gender_non_binary=EXCLUDED.gender_non_binary, gender_not_specified=EXCLUDED.gender_not_specified,
       age_0_17=EXCLUDED.age_0_17, age_18_22=EXCLUDED.age_18_22, age_23_27=EXCLUDED.age_23_27,
       age_28_34=EXCLUDED.age_28_34, age_35_44=EXCLUDED.age_35_44, age_45_59=EXCLUDED.age_45_59,
       age_60_plus=EXCLUDED.age_60_plus, age_unknown=EXCLUDED.age_unknown`,
    [
      scanId, period,
      data.gender_male ?? null, data.gender_female ?? null,
      data.gender_non_binary ?? null, data.gender_not_specified ?? null,
      data.age_0_17 ?? null, data.age_18_22 ?? null, data.age_23_27 ?? null,
      data.age_28_34 ?? null, data.age_35_44 ?? null, data.age_45_59 ?? null,
      data.age_60_plus ?? null, data.age_unknown ?? null,
    ]
  );
}

export async function insertAudienceGeo(
  scanId: string,
  geo: { rank: number; country: string; percentage: number }
): Promise<void> {
  await query(
    `INSERT INTO podcast_audience_geo (scan_id, rank, country, percentage)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (scan_id, rank) DO UPDATE SET country=EXCLUDED.country, percentage=EXCLUDED.percentage`,
    [scanId, geo.rank, geo.country, geo.percentage]
  );
}

export async function insertAudiencePlatform(
  scanId: string,
  platform: { platform_name: string; percentage: number }
): Promise<void> {
  await query(
    `INSERT INTO podcast_audience_platforms (scan_id, platform_name, percentage)
     VALUES ($1,$2,$3)
     ON CONFLICT (scan_id, platform_name) DO UPDATE SET percentage=EXCLUDED.percentage`,
    [scanId, platform.platform_name, platform.percentage]
  );
}

export async function insertEpisodeRanking(
  scanId: string,
  episode: { rank: number; episode_name: string; episode_number?: number | null; streams: number }
): Promise<void> {
  await query(
    `INSERT INTO podcast_episode_rankings (scan_id, rank, episode_name, episode_number, streams)
     VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT (scan_id, rank) DO UPDATE SET
       episode_name=EXCLUDED.episode_name, episode_number=EXCLUDED.episode_number, streams=EXCLUDED.streams`,
    [scanId, episode.rank, episode.episode_name, episode.episode_number ?? null, episode.streams]
  );
}
