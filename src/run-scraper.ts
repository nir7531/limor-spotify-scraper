// scraper/src/run-scraper.ts
// Standalone entry point for running the scraper (e.g., via cron job)

import { scrapeAll } from './scraper.js';
import { notifyAdmin } from './notify.js';
import { closePool } from './db.js';

async function main() {
  console.log('Starting Spotify analytics scraper...');

  const { completed, partial, failed, failures } = await scrapeAll();

  const total = completed + partial + failed;
  let msg = `✅ סריקת ספוטיפיי הושלמה — ${completed}/${total} הצליחו`;
  if (partial > 0) msg += `, ${partial} חלקי`;
  if (failed > 0) msg += `, ${failed} נכשלו: ${failures.join(', ')}`;
  await notifyAdmin(msg);

  console.log(`Done. Completed: ${completed}, Partial: ${partial}, Failed: ${failed}`);
  await closePool();
}

main().catch(async (err) => {
  console.error('Scraper fatal error:', err);
  await notifyAdmin(`❌ שגיאה חמורה בסריקת ספוטיפיי: ${err.message}`);
  await closePool();
  process.exit(1);
});
