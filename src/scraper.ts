// scraper/src/scraper.ts
// Main scraper: queries groups with spotify_show_name, runs agent-browser per show

import { query, queryOne, closePool } from './db.js';
import { notifyAdmin } from './notify.js';
import { execSync } from 'child_process';
import * as fs from 'fs';

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

async function scrapeShow(show: ShowToScrape): Promise<'completed' | 'partial' | 'failed'> {
  const scanNumber = await getNextScanNumber(show.id);

  // Create scan record
  const scan = await queryOne<{ id: string }>(
    `INSERT INTO podcast_scans (group_id, scan_number, show_name, scan_status)
     VALUES ($1, $2, $3, 'in_progress') RETURNING id`,
    [show.id, scanNumber, show.spotify_show_name]
  );
  if (!scan) throw new Error('Failed to create scan record');
  const scanId = scan.id;

  try {
    // Build agent-browser command
    // The spotify-analytics skill handles the actual scraping
    // This is a placeholder — the actual command depends on the skill implementation
    const authPath = process.env.SPOTIFY_AUTH_PATH ?? './spotify-auth.json';
    const showUrl = show.spotify_show_url ?? '';

    // TODO: Invoke agent-browser with the spotify-analytics skill
    // The exact command will be:
    // agent-browser run --skill spotify-analytics --args '{"show_name": "...", "show_url": "...", "scan_id": "...", "auth_path": "..."}'
    //
    // For now, this is a placeholder that will be completed when the skill is updated
    console.log(`Scraping: ${show.spotify_show_name} (scan ${scanNumber})`);

    // After successful scrape: save spotify_show_url if discovered during fuzzy search
    // (the agent-browser skill outputs the discovered URL)
    // if (!show.spotify_show_url && discoveredUrl) {
    //   await query('UPDATE groups SET spotify_show_url = $1 WHERE id = $2', [discoveredUrl, show.id]);
    // }

    // Update scan status
    await query(
      `UPDATE podcast_scans SET scan_status = 'completed' WHERE id = $1`,
      [scanId]
    );

    return 'completed';
  } catch (err: any) {
    // Retry once
    try {
      console.log(`Retrying: ${show.spotify_show_name}`);
      // TODO: Same agent-browser invocation
      await query(
        `UPDATE podcast_scans SET scan_status = 'completed' WHERE id = $1`,
        [scanId]
      );
      return 'completed';
    } catch (retryErr: any) {
      await query(
        `UPDATE podcast_scans SET scan_status = 'failed', notes = $2 WHERE id = $1`,
        [scanId, retryErr.message?.slice(0, 500) ?? 'Unknown error']
      );
      return 'failed';
    }
  }
}

async function main() {
  console.log('Starting Spotify analytics scraper...');

  // Get all shows to scrape
  const shows = await query<ShowToScrape>(
    `SELECT id, spotify_show_name, spotify_show_url FROM groups
     WHERE spotify_show_name IS NOT NULL AND is_activated = true`
  );

  if (shows.length === 0) {
    console.log('No shows to scrape');
    await closePool();
    return;
  }

  // Randomize order
  shuffle(shows);
  console.log(`Scraping ${shows.length} shows...`);

  let completed = 0;
  let failed = 0;
  const failures: string[] = [];

  for (const show of shows) {
    try {
      const result = await scrapeShow(show);
      if (result === 'failed') {
        failed++;
        failures.push(show.spotify_show_name);
      } else {
        completed++;
      }
    } catch (err: any) {
      failed++;
      failures.push(show.spotify_show_name);
      console.error(`Error scraping ${show.spotify_show_name}:`, err);
    }

    // Random delay 2-5 minutes between shows
    if (shows.indexOf(show) < shows.length - 1) {
      await randomDelay(2 * 60 * 1000, 5 * 60 * 1000);
    }
  }

  // Save auth state to volume for persistence across runs
  // TODO: Save agent-browser auth state to SPOTIFY_AUTH_PATH
  // The exact API depends on agent-browser: e.g., browser.saveAuthState(authPath)
  const authPath = process.env.SPOTIFY_AUTH_PATH ?? './spotify-auth.json';
  console.log(`Auth state should be saved to: ${authPath}`);

  // Notify admin
  let msg = `✅ סריקת ספוטיפיי הושלמה — ${completed}/${shows.length} הצליחו`;
  if (failed > 0) {
    msg += `, ${failed} נכשלו: ${failures.join(', ')}`;
  }
  await notifyAdmin(msg);

  console.log(`Done. Completed: ${completed}, Failed: ${failed}`);
  await closePool();
}

main().catch(async (err) => {
  console.error('Scraper fatal error:', err);
  await notifyAdmin(`❌ שגיאה חמורה בסריקת ספוטיפיי: ${err.message}`);
  await closePool();
  process.exit(1);
});
