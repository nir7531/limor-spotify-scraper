// scraper/src/scraper.ts
// Main scraper: queries groups with spotify_show_name, runs LLM-orchestrated scrape per show

import {
  query, queryOne, closePool,
  insertOverviewSpotify, insertOverviewAllPlatforms,
  insertDiscovery, insertDiscoveryClip,
  insertAudienceDemographics, insertAudienceGeo, insertAudiencePlatform,
  insertEpisodeRanking,
} from './db.js';
import { notifyAdmin } from './notify.js';
import { orchestrateScrape, ScrapeResult } from './orchestrator.js';

interface ShowToScrape {
  id: string;
  spotify_show_name: string;
  spotify_show_url: string | null;
}

/** Random delay between min and max ms */
function randomDelay(minMs: number, maxMs: number): Promise<void> {
  const delay = minMs + Math.random() * (maxMs - minMs);
  return new Promise(resolve => setTimeout(resolve, delay));
}

/** Shuffle array in place */
function shuffle<T>(arr: T[]): T[] {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

async function getNextScanNumber(groupId: string): Promise<number> {
  const row = await queryOne<{ next: number }>(
    `SELECT COALESCE(MAX(scan_number), 0) + 1 AS next FROM podcast_scans WHERE group_id = $1`,
    [groupId]
  );
  return row?.next ?? 1;
}

/** Insert all scraped data into the analytics tables */
async function insertScrapeData(scanId: string, result: ScrapeResult): Promise<void> {
  // Overview — On Spotify
  if (result.overview?.on_spotify) {
    for (const [period, data] of Object.entries(result.overview.on_spotify)) {
      if (data) await insertOverviewSpotify(scanId, period, data);
    }
  }

  // Overview — All Platforms
  if (result.overview?.all_platforms) {
    for (const [period, data] of Object.entries(result.overview.all_platforms)) {
      if (data) await insertOverviewAllPlatforms(scanId, period, data);
    }
  }

  // Discovery
  if (result.discovery) {
    const d = result.discovery;
    await insertDiscovery(scanId, {
      date_range: d.date_range,
      funnel_reached: d.funnel?.reached,
      funnel_interested: d.funnel?.interested,
      funnel_consumed: d.funnel?.consumed,
      funnel_reach_to_interest_pct: d.funnel?.reach_to_interest_pct,
      funnel_interest_to_consumed_pct: d.funnel?.interest_to_consumed_pct,
      key_stats_headline: d.key_stats?.headline,
      key_stats_hours_per_person: d.key_stats?.hours_per_person,
      key_stats_follow_pct: d.key_stats?.follow_pct,
      traffic_impressions_total: d.traffic?.impressions_total,
      traffic_source_home: d.traffic?.source_home,
      traffic_source_search: d.traffic?.source_search,
      traffic_source_library: d.traffic?.source_library,
      traffic_source_other: d.traffic?.source_other,
    });

    // Discovery clips
    if (Array.isArray(d.clips)) {
      for (const clip of d.clips) {
        await insertDiscoveryClip(scanId, {
          clip_rank: clip.rank,
          clip_name: clip.name,
          clip_date: clip.date,
          clip_duration_seconds: clip.duration_seconds,
          impressions: clip.impressions,
          plays_from_clips: clip.plays_from_clips,
        });
      }
    }
  }

  // Audience demographics
  if (result.audience?.demographics) {
    for (const [period, data] of Object.entries(result.audience.demographics) as [string, any][]) {
      if (!data?.gender && !data?.age) continue;
      await insertAudienceDemographics(scanId, period, {
        gender_male: data.gender?.male,
        gender_female: data.gender?.female,
        gender_non_binary: data.gender?.non_binary,
        gender_not_specified: data.gender?.not_specified,
        age_0_17: data.age?.['0_17'],
        age_18_22: data.age?.['18_22'],
        age_23_27: data.age?.['23_27'],
        age_28_34: data.age?.['28_34'],
        age_35_44: data.age?.['35_44'],
        age_45_59: data.age?.['45_59'],
        age_60_plus: data.age?.['60_plus'],
        age_unknown: data.age?.unknown,
      });
    }
  }

  // Audience geo
  if (Array.isArray(result.audience?.geographic)) {
    for (const geo of result.audience.geographic) {
      await insertAudienceGeo(scanId, {
        rank: geo.rank,
        country: geo.country,
        percentage: geo.pct,
      });
    }
  }

  // Audience platforms
  if (Array.isArray(result.audience?.platforms)) {
    for (const plat of result.audience.platforms) {
      await insertAudiencePlatform(scanId, {
        platform_name: plat.name,
        percentage: plat.pct,
      });
    }
  }

  // Episode rankings
  if (Array.isArray(result.episode_rankings)) {
    for (const ep of result.episode_rankings) {
      await insertEpisodeRanking(scanId, {
        rank: ep.rank,
        episode_name: ep.episode_name,
        episode_number: ep.episode_number,
        streams: ep.streams,
      });
    }
  }
}

export async function scrapeShow(show: ShowToScrape): Promise<'completed' | 'partial' | 'failed'> {
  const scanNumber = await getNextScanNumber(show.id);
  const authPath = process.env.SPOTIFY_AUTH_PATH ?? './spotify-auth.json';

  // Create scan record
  const scan = await queryOne<{ id: string }>(
    `INSERT INTO podcast_scans (group_id, scan_number, show_name, scan_status)
     VALUES ($1, $2, $3, 'in_progress') RETURNING id`,
    [show.id, scanNumber, show.spotify_show_name]
  );
  if (!scan) throw new Error('Failed to create scan record');
  const scanId = scan.id;

  console.log(`Scraping: ${show.spotify_show_name} (scan #${scanNumber}, id=${scanId})`);

  try {
    const result = await orchestrateScrape(
      show.spotify_show_name,
      show.spotify_show_url,
      scanId,
      authPath
    );

    // Insert data into analytics tables
    if (result.scan_status !== 'failed') {
      await insertScrapeData(scanId, result);
    }

    // Update scan status
    await query(
      `UPDATE podcast_scans SET scan_status = $2, notes = $3 WHERE id = $1`,
      [scanId, result.scan_status, result.notes?.slice(0, 500) ?? null]
    );

    console.log(`  Result: ${result.scan_status}${result.notes ? ` — ${result.notes}` : ''}`);
    return result.scan_status;
  } catch (err: any) {
    console.error(`  Fatal error scraping ${show.spotify_show_name}:`, err.message);
    await query(
      `UPDATE podcast_scans SET scan_status = 'failed', notes = $2 WHERE id = $1`,
      [scanId, err.message?.slice(0, 500) ?? 'Unknown error']
    );
    return 'failed';
  }
}

export async function scrapeAll(): Promise<{ completed: number; partial: number; failed: number; failures: string[] }> {
  const shows = await query<ShowToScrape>(
    `SELECT id, spotify_show_name, spotify_show_url FROM groups
     WHERE spotify_show_name IS NOT NULL AND is_activated = true`
  );

  if (shows.length === 0) {
    console.log('No shows to scrape');
    return { completed: 0, partial: 0, failed: 0, failures: [] };
  }

  shuffle(shows);
  console.log(`Scraping ${shows.length} shows...`);

  let completed = 0;
  let partial = 0;
  let failed = 0;
  const failures: string[] = [];

  for (const show of shows) {
    const result = await scrapeShow(show);
    if (result === 'completed') completed++;
    else if (result === 'partial') partial++;
    else {
      failed++;
      failures.push(show.spotify_show_name);
    }

    // Random delay 2-5 minutes between shows
    if (shows.indexOf(show) < shows.length - 1) {
      await randomDelay(2 * 60 * 1000, 5 * 60 * 1000);
    }
  }

  return { completed, partial, failed, failures };
}

// Note: This module only exports functions. For standalone execution, use run-scraper.ts.
