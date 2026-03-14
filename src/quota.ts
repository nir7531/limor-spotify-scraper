// scraper/src/quota.ts
// YouTube API quota tracker with daily reset and hard-stop threshold

const DAILY_LIMIT = 10_000;
const WARNING_PCT = 0.80;
const HARD_STOP_PCT = 0.95;

let unitsUsed = 0;
let lastResetDate = new Date().toDateString();

function maybeResetDaily(): void {
  const today = new Date().toDateString();
  if (today !== lastResetDate) {
    unitsUsed = 0;
    lastResetDate = today;
  }
}

export function trackUnits(units: number): void {
  maybeResetDaily();
  const before = unitsUsed;
  unitsUsed += units;
  // Log warning once when crossing the 80% threshold
  if (unitsUsed >= DAILY_LIMIT * WARNING_PCT && before < DAILY_LIMIT * WARNING_PCT) {
    console.warn(`[quota] YouTube API quota at ${Math.round(unitsUsed / DAILY_LIMIT * 100)}% (${unitsUsed}/${DAILY_LIMIT})`);
  }
}

export function canProceed(): boolean {
  maybeResetDaily();
  return unitsUsed < DAILY_LIMIT * HARD_STOP_PCT;
}

export function getQuotaStatus(): { used: number; limit: number; pct: number } {
  maybeResetDaily();
  return {
    used: unitsUsed,
    limit: DAILY_LIMIT,
    pct: Math.round(unitsUsed / DAILY_LIMIT * 100),
  };
}

/** Reset quota counter (for testing) */
export function resetQuota(): void {
  unitsUsed = 0;
  lastResetDate = new Date().toDateString();
}
