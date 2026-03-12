// scraper/src/notify.ts

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const ADMIN_CHAT_ID = process.env.ADMIN_CHAT_ID;

export async function notifyAdmin(message: string): Promise<void> {
  if (!BOT_TOKEN || !ADMIN_CHAT_ID) return;
  try {
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`;
    await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: ADMIN_CHAT_ID, text: message }),
    });
  } catch (err) {
    console.error('Failed to notify admin:', err);
  }
}

// ---------------------------------------------------------------------------
// Research notification string builders
// ---------------------------------------------------------------------------

export function researchStartMessage(target: string): string {
  return `🔍 מתחיל מחקר שוק${target === 'all' ? ' לכל הלקוחות' : ` ל-${target}`}...`;
}

export function researchProgressMessage(current: number, total: number, clientName: string): string {
  return `🔍 מחקר ${current}/${total}: ${clientName}`;
}

export function researchDoneMessage(count: number, failures: number, duration: string): string {
  const failText = failures > 0 ? ` | ${failures} נכשלו` : '';
  return `✅ מחקר שוק הושלם: ${count} לקוחות${failText} (${duration})`;
}

export function researchFailMessage(client: string, error: string): string {
  return `❌ מחקר נכשל ל-${client}: ${error}`;
}

export function circuitBreakerMessage(apiName: string, isQuota: boolean): string {
  return isQuota
    ? `⚠️ מכסת API של ${apiName} נגמרה במהלך מחקר. המחקר ימשיך ללא נתוני ${apiName}.`
    : `⚠️ API ${apiName} לא זמין. המחקר ימשיך ללא נתוני ${apiName}.`;
}

export function batchFailureWarning(failureRate: number): string | null {
  if (failureRate > 0.6) {
    return '🔴 רוב הסריקות נכשלו — ייתכן שממשק Spotify השתנה. בדוק ידנית.';
  }
  return null;
}
