// scraper/src/youtube-auth.ts
// YouTube OAuth2 authentication with AES-256-GCM token encryption

import { google } from 'googleapis';
import crypto from 'crypto';
import { query } from './db.js';

const ALGORITHM = 'aes-256-gcm';

const logger = {
  info: (msg: string, ...args: any[]) => console.log(`[youtube-auth] ${msg}`, ...args),
  warn: (msg: string, ...args: any[]) => console.warn(`[youtube-auth] WARN ${msg}`, ...args),
  error: (msg: string, ...args: any[]) => console.error(`[youtube-auth] ERROR ${msg}`, ...args),
};

function getEncryptionKey(): Buffer {
  const key = process.env.OAUTH_ENCRYPTION_KEY;
  if (!key) throw new Error('OAUTH_ENCRYPTION_KEY not set');
  const buf = Buffer.from(key, 'hex');
  if (buf.length !== 32) throw new Error('OAUTH_ENCRYPTION_KEY must be 64 hex chars (32 bytes)');
  return buf;
}

export function encryptToken(token: string): string {
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  let encrypted = cipher.update(token, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  const tag = cipher.getAuthTag().toString('hex');
  return `${iv.toString('hex')}:${tag}:${encrypted}`;
}

export function decryptToken(encrypted: string): string {
  const key = getEncryptionKey();
  const parts = encrypted.split(':');
  if (parts.length < 3) throw new Error('Invalid encrypted token format');
  const [ivHex, tagHex, ...rest] = parts;
  const data = rest.join(':');
  const iv = Buffer.from(ivHex, 'hex');
  const tag = Buffer.from(tagHex, 'hex');
  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(tag);
  let decrypted = decipher.update(data, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  return decrypted;
}

export function getOAuth2Client(): InstanceType<typeof google.auth.OAuth2> {
  const client = new google.auth.OAuth2(
    process.env.YOUTUBE_CLIENT_ID,
    process.env.YOUTUBE_CLIENT_SECRET
  );
  if (process.env.YOUTUBE_OAUTH_REFRESH_TOKEN) {
    client.setCredentials({ refresh_token: process.env.YOUTUBE_OAUTH_REFRESH_TOKEN });
  }
  return client;
}

export async function getAuthForChannel(groupId: string): Promise<InstanceType<typeof google.auth.OAuth2>> {
  // Check for per-channel token first
  const rows = await query<{ encrypted_refresh_token: string }>(
    'SELECT encrypted_refresh_token FROM youtube_oauth_tokens WHERE group_id = $1',
    [groupId]
  );
  if (rows.length > 0) {
    try {
      const refreshToken = decryptToken(rows[0].encrypted_refresh_token);
      const client = new google.auth.OAuth2(
        process.env.YOUTUBE_CLIENT_ID,
        process.env.YOUTUBE_CLIENT_SECRET
      );
      client.setCredentials({ refresh_token: refreshToken });
      logger.info(`Using per-channel OAuth for group ${groupId}`);
      return client;
    } catch (err) {
      logger.warn(`Failed to decrypt per-channel token for group ${groupId}, falling back to studio account:`, err);
    }
  }
  // Fall back to studio account
  return getOAuth2Client();
}

export async function saveOAuthToken(
  groupId: string,
  refreshToken: string,
  channelTitle: string
): Promise<void> {
  const encrypted = encryptToken(refreshToken);
  await query(
    `INSERT INTO youtube_oauth_tokens (group_id, encrypted_refresh_token, channel_title)
     VALUES ($1, $2, $3)
     ON CONFLICT (group_id) DO UPDATE SET
       encrypted_refresh_token = $2, channel_title = $3, updated_at = now()`,
    [groupId, encrypted, channelTitle]
  );
  logger.info(`Saved OAuth token for group ${groupId} (${channelTitle})`);
}
