// scraper/src/youtube-db.ts
// DB insert functions for YouTube analytics tables
// Follows existing pattern from db.ts: INSERT ... ON CONFLICT DO NOTHING

import { query } from './db.js';
import type {
  ChannelOverview,
  ShortsOverview,
  EpisodeStats,
  Demographics,
  TrafficSources,
  GeoEntry,
  DeviceTypes,
  YouTubeScanResult,
} from './youtube.js';

export async function insertOverviewYouTube(
  scanId: string,
  period: string,
  data: ChannelOverview
): Promise<void> {
  await query(
    `INSERT INTO podcast_overview_youtube (
       scan_id, period, date_range,
       views, watch_hours, subscribers_delta,
       impressions, ctr, avg_view_duration_seconds, avg_view_pct,
       subscriber_views_pct, non_subscriber_views_pct,
       new_viewers_pct, returning_viewers_pct,
       likes, comments
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
     ON CONFLICT (scan_id, period) DO NOTHING`,
    [
      scanId, period, data.date_range,
      data.views, data.watch_hours, data.subscribers_delta,
      data.impressions, data.ctr, data.avg_view_duration_seconds, data.avg_view_pct,
      data.subscriber_views_pct, data.non_subscriber_views_pct,
      data.new_viewers_pct, data.returning_viewers_pct,
      data.likes, data.comments,
    ]
  );
}

export async function insertShortsYouTube(
  scanId: string,
  period: string,
  data: ShortsOverview
): Promise<void> {
  await query(
    `INSERT INTO podcast_shorts_youtube (
       scan_id, period, date_range,
       views, engaged_views, watch_hours,
       avg_view_duration_seconds, likes, comments
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (scan_id, period) DO NOTHING`,
    [
      scanId, period, data.date_range,
      data.views, data.engaged_views, data.watch_hours,
      data.avg_view_duration_seconds, data.likes, data.comments,
    ]
  );
}

export async function insertEpisodeYouTubeStats(
  scanId: string,
  episode: EpisodeStats
): Promise<void> {
  await query(
    `INSERT INTO podcast_episode_youtube_stats (
       scan_id, video_id, title, published_at,
       views, watch_hours, likes, comments, rank
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (scan_id, video_id) DO NOTHING`,
    [
      scanId, episode.video_id, episode.title,
      episode.published_at || null,
      episode.views, episode.watch_hours,
      episode.likes, episode.comments, episode.rank,
    ]
  );
}

export async function insertDemographicsYouTube(
  scanId: string,
  period: string,
  data: Demographics
): Promise<void> {
  await query(
    `INSERT INTO podcast_audience_demographics_youtube (
       scan_id, period,
       male_pct, female_pct,
       age_13_17, age_18_24, age_25_34, age_35_44,
       age_45_54, age_55_64, age_65_plus
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT (scan_id, period) DO NOTHING`,
    [
      scanId, period,
      data.male_pct, data.female_pct,
      data.age_13_17, data.age_18_24, data.age_25_34, data.age_35_44,
      data.age_45_54, data.age_55_64, data.age_65_plus,
    ]
  );
}

export async function insertTrafficSourcesYouTube(
  scanId: string,
  period: string,
  data: TrafficSources
): Promise<void> {
  await query(
    `INSERT INTO podcast_traffic_sources_youtube (
       scan_id, period,
       search_pct, suggested_pct, browse_pct,
       external_pct, playlist_pct, other_pct
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (scan_id, period) DO NOTHING`,
    [
      scanId, period,
      data.search_pct, data.suggested_pct, data.browse_pct,
      data.external_pct, data.playlist_pct, data.other_pct,
    ]
  );
}

export async function insertGeoYouTube(
  scanId: string,
  geo: GeoEntry
): Promise<void> {
  // Geo percentage stored as 0-1 decimal (divide API's 0-100 by 100)
  // Note: fetchGeo already divides by total, so percentage is 0-1 range
  await query(
    `INSERT INTO podcast_audience_geo_youtube (scan_id, rank, country, percentage)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (scan_id, rank) DO NOTHING`,
    [scanId, geo.rank, geo.country, geo.percentage]
  );
}

export async function insertDeviceYouTube(
  scanId: string,
  period: string,
  data: DeviceTypes
): Promise<void> {
  await query(
    `INSERT INTO podcast_device_youtube (
       scan_id, period,
       mobile_pct, desktop_pct, tv_pct, tablet_pct
     ) VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (scan_id, period) DO NOTHING`,
    [scanId, period, data.mobile_pct, data.desktop_pct, data.tv_pct, data.tablet_pct]
  );
}

export async function updateAllPlatformsYouTube(
  scanId: string,
  period: string,
  views: number,
  watchHours: number
): Promise<void> {
  await query(
    `UPDATE podcast_overview_all_platforms
     SET youtube_views = $3, youtube_watch_hours = $4
     WHERE scan_id = $1 AND period = $2`,
    [scanId, period, views, watchHours]
  );
}

export async function insertYouTubeScanData(
  scanId: string,
  result: YouTubeScanResult
): Promise<void> {
  // Overview for each period
  for (const [period, data] of Object.entries(result.overview)) {
    await insertOverviewYouTube(scanId, period, data);
    // Also update all_platforms table with YouTube views
    await updateAllPlatformsYouTube(scanId, period, data.views, data.watch_hours);
  }

  // Shorts for each period
  for (const [period, data] of Object.entries(result.shorts)) {
    await insertShortsYouTube(scanId, period, data);
  }

  // Episode stats
  for (const episode of result.episodes) {
    await insertEpisodeYouTubeStats(scanId, episode);
  }

  // Demographics (7d/30d/90d only)
  for (const [period, data] of Object.entries(result.demographics)) {
    await insertDemographicsYouTube(scanId, period, data);
  }

  // Traffic sources
  for (const [period, data] of Object.entries(result.trafficSources)) {
    await insertTrafficSourcesYouTube(scanId, period, data);
  }

  // Geo (top 5 countries)
  for (const geo of result.geo) {
    await insertGeoYouTube(scanId, geo);
  }

  // Device types
  for (const [period, data] of Object.entries(result.deviceTypes)) {
    await insertDeviceYouTube(scanId, period, data);
  }
}
