// scraper/src/job-lock.ts
import { query, queryOne } from './db.js';

const LOCK_NAME = 'scraper_job';

export async function acquireLock(
  jobType: 'scrape' | 'research',
  lockedBy: string,
  ttlMinutes: number
): Promise<boolean> {
  // Clean up expired locks first
  await query(
    `DELETE FROM job_locks WHERE lock_name = $1 AND expires_at < now()`,
    [LOCK_NAME]
  );

  // Try to acquire
  const result = await query(
    `INSERT INTO job_locks (lock_name, job_type, locked_by, locked_at, expires_at)
     VALUES ($1, $2, $3, now(), now() + interval '1 minute' * $4)
     ON CONFLICT (lock_name) DO NOTHING
     RETURNING lock_name`,
    [LOCK_NAME, jobType, lockedBy, ttlMinutes]
  );

  return result.length > 0;
}

export async function releaseLock(lockedBy: string): Promise<void> {
  await query(
    `DELETE FROM job_locks WHERE lock_name = $1 AND locked_by = $2`,
    [LOCK_NAME, lockedBy]
  );
}

export async function getLockInfo(): Promise<{
  job_type: string;
  locked_by: string;
  locked_at: Date;
  expires_at: Date;
} | null> {
  return queryOne(
    `SELECT job_type, locked_by, locked_at, expires_at FROM job_locks WHERE lock_name = $1`,
    [LOCK_NAME]
  );
}
