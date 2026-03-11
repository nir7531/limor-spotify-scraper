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
