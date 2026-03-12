// scraper/src/run-scraper.ts
// Standalone entry point for running the scraper (e.g., via cron job)

import { scrapeAll } from './scraper.js';
import { notifyAdmin } from './notify.js';
import { closePool } from './db.js';
import { acquireLock, releaseLock } from './job-lock.js';

async function main() {
  console.log('Starting Spotify analytics scraper...');

  const acquired = await acquireLock('scrape', 'cron', 240);
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
