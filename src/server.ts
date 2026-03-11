// scraper/src/server.ts
// Lightweight HTTP server for on-demand scrape triggers from Limor admin menu

import express from 'express';
import { query, queryOne } from './db.js';
import { scrapeShow, scrapeAll } from './scraper.js';
import { notifyAdmin } from './notify.js';

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

// Track running scrapes to prevent concurrent runs
let scrapeInProgress = false;

app.post('/api/scrape', async (req, res) => {
  const { group_id } = req.body;

  if (scrapeInProgress) {
    res.status(409).json({ error: 'A scrape is already in progress' });
    return;
  }

  scrapeInProgress = true;

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
    }
  } catch (err: any) {
    console.error('Scrape error:', err);
    await notifyAdmin(`❌ שגיאה בסריקה: ${err.message}`);
  } finally {
    scrapeInProgress = false;
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', scrapeInProgress, timestamp: new Date().toISOString() });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Scraper server listening on port ${PORT}`);
});
