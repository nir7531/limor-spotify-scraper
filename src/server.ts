// scraper/src/server.ts
// Lightweight HTTP server for on-demand scrape triggers from Limor admin menu

import express from 'express';
import { query, queryOne, getGroupsNeedingResearch } from './db.js';
import { scrapeShow, scrapeAll } from './scraper.js';
import { researchClient, researchAll } from './research.js';
import { notifyAdmin } from './notify.js';
import { acquireLock, releaseLock, getLockInfo } from './job-lock.js';

const app = express();
app.use(express.json());

const API_KEY = process.env.SCRAPER_API_KEY;

// Auth middleware
app.use('/api', (req, res, next) => {
  const auth = req.headers.authorization;
  if (!API_KEY || auth !== `Bearer ${API_KEY}`) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  next();
});

app.post('/api/scrape', async (req, res) => {
  const { group_id } = req.body;

  const acquired = await acquireLock('scrape', 'api', 240);
  if (!acquired) {
    const lock = await getLockInfo();
    res.status(409).json({
      error: lock?.job_type === 'research' ? 'מחקר בתהליך, נסו שוב מאוחר יותר' : 'סריקה כבר רצה'
    });
    return;
  }

  // Respond immediately, run scrape in background
  res.json({ status: 'started', target: group_id ?? 'all' });

  try {
    if (group_id) {
      // Scrape specific group
      const group = await queryOne<{ id: string; spotify_show_name: string; spotify_show_url: string | null }>(
        `SELECT id, spotify_show_name, spotify_show_url FROM groups WHERE id = $1 AND spotify_show_name IS NOT NULL`,
        [group_id]
      );

      if (!group) {
        console.error(`Group ${group_id} not found or has no spotify_show_name`);
        await notifyAdmin(`❌ סריקה נכשלה — קבוצה ${group_id} לא נמצאה`);
        return;
      }

      const result = await scrapeShow(group);
      const msg = result === 'failed'
        ? `❌ סריקת ${group.spotify_show_name} נכשלה`
        : `✅ סריקת ${group.spotify_show_name} הושלמה (${result})`;
      await notifyAdmin(msg);
    } else {
      // Scrape all groups
      const { completed, partial, failed, failures } = await scrapeAll();
      const total = completed + partial + failed;
      let msg = `✅ סריקת ספוטיפיי הושלמה — ${completed}/${total} הצליחו`;
      if (partial > 0) msg += `, ${partial} חלקי`;
      if (failed > 0) msg += `, ${failed} נכשלו: ${failures.join(', ')}`;
      await notifyAdmin(msg);

      // Auto-chain: check if any groups need research after scraping
      try {
        const groupsNeedingResearch = await getGroupsNeedingResearch();
        if (groupsNeedingResearch.length > 0) {
          console.log(`[auto-chain] ${groupsNeedingResearch.length} groups need research, starting...`);
          // Acquire research lock — do NOT release 'api' lock here; let the outer finally handle it
          const researchAcquired = await acquireLock('research', 'auto-chain', 60);
          if (researchAcquired) {
            try {
              await researchAll(groupsNeedingResearch);
            } finally {
              await releaseLock('auto-chain');
            }
          }
        }
      } catch (err) {
        console.error('[auto-chain] Error checking research eligibility:', err);
      }
    }
  } catch (err: any) {
    console.error('Scrape error:', err);
    await notifyAdmin(`❌ שגיאה בסריקה: ${err.message}`);
  } finally {
    await releaseLock('api');
  }
});

app.post('/api/research', async (req, res) => {
  const { group_id } = req.body || {};

  const acquired = await acquireLock('research', 'api', 60);
  if (!acquired) {
    const lock = await getLockInfo();
    res.status(409).json({
      error: lock?.job_type === 'scrape' ? 'סריקה בתהליך, נסו שוב מאוחר יותר' : 'מחקר כבר רץ'
    });
    return;
  }

  res.json({ status: 'started', target: group_id || 'all' });

  // Run async in background
  (async () => {
    try {
      if (group_id) {
        await researchClient(group_id);
      } else {
        await researchAll();
      }
    } catch (err) {
      console.error('[research] Error:', err);
      await notifyAdmin(`❌ שגיאה במחקר: ${(err as any)?.message ?? 'Unknown error'}`);
    } finally {
      await releaseLock('api');
    }
  })();
});

app.get('/api/health', async (req, res) => {
  const lock = await getLockInfo();
  res.json({ status: 'ok', scraping: !!lock, timestamp: new Date().toISOString() });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Scraper server listening on port ${PORT}`);
});
