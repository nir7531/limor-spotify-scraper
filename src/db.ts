// scraper/src/db.ts
import pg from 'pg';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

export async function query<T = any>(sql: string, params?: any[]): Promise<T[]> {
  const { rows } = await pool.query(sql, params);
  return rows;
}

export async function queryOne<T = any>(sql: string, params?: any[]): Promise<T | null> {
  const rows = await query<T>(sql, params);
  return rows[0] ?? null;
}

export async function closePool() {
  await pool.end();
}
