// scraper/src/server.ts
// Lightweight HTTP server for on-demand scrape triggers from Limor admin menu

import express from 'express';
import { query, queryOne, getGroupsNeedingResearch, getYouTubeGroups, getLastYouTubeScan, getYouTubeGroupCount } from './db.js';
import { scrapeShow, scrapeAll } from './scraper.js';
import { researchClient, researchAll } from './research.js';
import { notifyAdmin } from './notify.js';
import { acquireLock, releaseLock, getLockInfo } from './job-lock.js';
import { runEpisodeCatalog } from './episode-catalog.js';
import { scanAllYouTube, validateYouTubeChannel } from './youtube.js';
import { getQuotaStatus } from './quota.js';
import { saveOAuthToken, getOAuth2Client } from './youtube-auth.js';

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

  const acquired = await acquireLock('scrape', 'api', 360);
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

      // Episode catalog: fetch after successful scrape
      if (result !== 'failed') {
        const scanRow = await queryOne<{ id: string }>(
          `SELECT id FROM podcast_scans WHERE group_id = $1 ORDER BY created_at DESC LIMIT 1`,
          [group.id]
        );
        if (scanRow) {
          const authPath = process.env.SPOTIFY_AUTH_PATH ?? './spotify-auth.json';
          await runEpisodeCatalog(group.id, scanRow.id, group.spotify_show_url, authPath);
        }
      }
    } else {
      // Scrape all groups
      const { completed, partial, failed, failures } = await scrapeAll();
      const total = completed + partial + failed;
      let msg = `✅ סריקת ספוטיפיי הושלמה — ${completed}/${total} הצליחו`;
      if (partial > 0) msg += `, ${partial} חלקי`;
      if (failed > 0) msg += `, ${failed} נכשלו: ${failures.join(', ')}`;
      await notifyAdmin(msg);

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
            // Mark scans that had both Spotify and YouTube as 'combined'
            const { markCombinedScans } = await import('./db.js');
            await markCombinedScans(ytGroups.map(g => g.id));
          }
          const ytMsg = `▶️ YouTube scan הושלם — ${ytResult.success}/${ytGroups.length} הצליחו${ytResult.failedGroups.length > 0 ? `, נכשלו: ${ytResult.failedGroups.join(', ')}` : ''}`;
          await notifyAdmin(ytMsg);
        }
      } catch (err) {
        console.error('[auto-chain] YouTube scan error:', err);
      }

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

app.post('/api/youtube-scan', async (req, res) => {
  const { group_id } = req.body || {};

  // Use a separate lock key to avoid blocking Spotify scans
  const locked = await acquireLock('scrape', 'youtube_api', 120);
  if (!locked) {
    res.status(409).json({ status: 'error', message: 'YouTube scan already in progress' });
    return;
  }

  const groups = await getYouTubeGroups(group_id);
  if (groups.length === 0) {
    await releaseLock('youtube_api');
    res.status(404).json({ status: 'error', message: 'No YouTube-configured groups found' });
    return;
  }

  res.json({ status: 'started', groups: groups.length });

  (async () => {
    try {
      const result = await scanAllYouTube(groups);
      const msg = `✅ YouTube scan הושלם — ${result.success}/${groups.length} הצליחו${result.failedGroups.length > 0 ? `, נכשלו: ${result.failedGroups.join(', ')}` : ''}`;
      await notifyAdmin(msg);
    } catch (err) {
      console.error('[youtube-scan] Error:', err);
      await notifyAdmin(`❌ שגיאה ב-YouTube scan: ${(err as any)?.message ?? 'Unknown error'}`);
    } finally {
      await releaseLock('youtube_api');
    }
  })();
});

app.get('/api/youtube-health', async (_req, res) => {
  const quota = getQuotaStatus();
  const lastScan = await getLastYouTubeScan();
  const groupCount = await getYouTubeGroupCount();
  res.json({
    quota_used: quota.used,
    quota_limit: quota.limit,
    quota_pct: quota.pct,
    last_scan_at: lastScan?.scan_date ?? null,
    groups_with_youtube: groupCount,
    last_scan_success: lastScan?.successCount ?? 0,
    last_scan_failed: lastScan?.failCount ?? 0,
    failed_groups: lastScan?.failures ?? [],
  });
});

app.get('/api/youtube-validate', async (req, res) => {
  const { channel_url } = req.query;
  if (!channel_url) {
    res.status(400).json({ error: 'channel_url required' });
    return;
  }
  try {
    const { channelId, channelTitle, playlistId } = await validateYouTubeChannel(String(channel_url));
    res.json({ channel_id: channelId, channel_title: channelTitle, podcast_playlist_id: playlistId });
  } catch (err) {
    res.status(400).json({ error: (err as Error).message });
  }
});

app.get('/api/health', async (req, res) => {
  const lock = await getLockInfo();
  res.json({ status: 'ok', scraping: !!lock, timestamp: new Date().toISOString() });
});

// OAuth callback (outside /api prefix — no auth middleware)
app.get('/auth/youtube/callback', async (req, res) => {
  const { code, state, error } = req.query;

  if (error) {
    res.status(400).send(`OAuth error: ${error}`);
    return;
  }

  if (!code || !state) {
    res.status(400).send('Missing code or state parameter');
    return;
  }

  try {
    // State = groupId (validate it exists)
    const groupId = String(state);
    const group = await queryOne<{ id: string; client_name: string }>(
      `SELECT id, client_name FROM groups WHERE id = $1`,
      [groupId]
    );
    if (!group) {
      res.status(400).send('Invalid state parameter');
      return;
    }

    // Exchange code for tokens
    const oauthClient = getOAuth2Client();
    const { tokens } = await oauthClient.getToken(String(code));
    if (!tokens.refresh_token) {
      res.status(400).send('No refresh token returned — ensure offline access was requested');
      return;
    }

    // Get channel info
    oauthClient.setCredentials(tokens);
    const { google: googleLib } = await import('googleapis');
    const yt = googleLib.youtube({ version: 'v3', auth: oauthClient });
    const channelRes = await yt.channels.list({ part: ['snippet'], mine: true });
    const channelTitle = channelRes.data.items?.[0]?.snippet?.title ?? group.client_name;

    await saveOAuthToken(groupId, tokens.refresh_token, channelTitle);
    res.send(`<html><body><p>✅ YouTube מחובר בהצלחה ל-${channelTitle}. ניתן לסגור חלון זה.</p></body></html>`);
  } catch (err) {
    console.error('[youtube-oauth] Callback error:', err);
    res.status(500).send('OAuth callback failed');
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Scraper server listening on port ${PORT}`);
});
