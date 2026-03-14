// scraper/src/run-scraper.ts
// Standalone entry point for running the scraper (e.g., via cron job)

import { scrapeAll } from './scraper.js';
import { researchAll } from './research.js';
import { getGroupsNeedingResearch, getYouTubeGroups, markCombinedScans, closePool, query } from './db.js';
import { notifyAdmin } from './notify.js';
import { acquireLock, releaseLock } from './job-lock.js';
import { runEpisodeCatalog } from './episode-catalog.js';
import { scanAllYouTube } from './youtube.js';

async function main() {
  console.log('Starting Spotify analytics scraper...');

  const acquired = await acquireLock('scrape', 'cron', 360);
  if (!acquired) {
    console.log('Another job is running, skipping cron scrape');
    await closePool();
    return;
  }

  try {
    const { completed, partial, failed, failures } = await scrapeAll();

    const total = completed + partial + failed;
    let msg = `✅ סריקת ספוטיפיי הושלמה — ${completed}/${total} הצליחו`;
    if (partial > 0) msg += `, ${partial} חלקי`;
    if (failed > 0) msg += `, ${failed} נכשלו: ${failures.join(', ')}`;
    await notifyAdmin(msg);

    console.log(`Done. Completed: ${completed}, Partial: ${partial}, Failed: ${failed}`);

    // Episode catalog: fetch for all recently completed scans
    try {
      const authPath = process.env.SPOTIFY_AUTH_PATH ?? './spotify-auth.json';
      const recentScans = await query<{ id: string; group_id: string; spotify_show_url: string | null }>(
        `SELECT ps.id, ps.group_id, g.spotify_show_url
         FROM podcast_scans ps
         JOIN groups g ON ps.group_id = g.id
         WHERE ps.scan_status IN ('completed', 'partial')
           AND ps.created_at > NOW() - INTERVAL '2 hours'
         ORDER BY ps.created_at DESC`
      );
      for (const scan of recentScans) {
        await runEpisodeCatalog(scan.group_id, scan.id, scan.spotify_show_url, authPath);
      }
    } catch (err) {
      console.error('[episode-catalog] Error running episode catalog:', err);
    }

    // Auto-chain: YouTube scan for groups with YouTube configured
    // Order: Spotify → Episode Catalog → YouTube → Research
    try {
      const ytGroups = await getYouTubeGroups();
      if (ytGroups.length > 0) {
        console.log(`[auto-chain] ${ytGroups.length} groups have YouTube, starting YouTube scan...`);
        const ytResult = await scanAllYouTube(ytGroups);
        if (ytResult.success > 0) {
          await markCombinedScans(ytGroups.map(g => g.id));
        }
        const ytMsg = `▶️ YouTube scan הושלם — ${ytResult.success}/${ytGroups.length} הצליחו${ytResult.failedGroups.length > 0 ? `, נכשלו: ${ytResult.failedGroups.join(', ')}` : ''}`;
        await notifyAdmin(ytMsg);
      } else {
        console.log('[auto-chain] No groups with YouTube configured');
      }
    } catch (err) {
      console.error('[auto-chain] YouTube scan error:', err);
    }

    // Auto-chain: check if any groups need research after scraping
    try {
      const groupsNeedingResearch = await getGroupsNeedingResearch();
      if (groupsNeedingResearch.length > 0) {
        console.log(`[auto-chain] ${groupsNeedingResearch.length} groups need research, starting...`);
        const researchAcquired = await acquireLock('research', 'auto-chain', 60);
        if (researchAcquired) {
          try {
            await researchAll(groupsNeedingResearch);
          } finally {
            await releaseLock('auto-chain');
          }
        } else {
          console.log('[auto-chain] Could not acquire research lock — skipping');
        }
      } else {
        console.log('[auto-chain] No groups need research');
      }
    } catch (err) {
      console.error('[auto-chain] Error checking research eligibility:', err);
    }
  } finally {
    await releaseLock('cron');
    await closePool();
  }
}

main().catch(async (err) => {
  console.error('Scraper fatal error:', err);
  await notifyAdmin(`❌ שגיאה חמורה בסריקת ספוטיפיי: ${err.message}`);
  await closePool();
  process.exit(1);
});
