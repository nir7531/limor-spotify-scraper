// scraper/src/circuit-breaker.ts

export class CircuitBreaker {
  private failureCount = 0;
  private emptyResponseCount = 0;
  private isOpen = false;
  quotaExhausted = false;
  readonly name: string;
  private readonly threshold: number;
  private readonly emptyThreshold: number;

  constructor(name: string, threshold: number, emptyThreshold = 3) {
    this.name = name;
    this.threshold = threshold;
    this.emptyThreshold = emptyThreshold;
  }

  get tripped(): boolean { return this.isOpen; }

  recordSuccess(): void {
    this.failureCount = 0;
    this.emptyResponseCount = 0;
  }

  recordFailure(quota = false): void {
    if (quota) {
      this.quotaExhausted = true;
    }
    this.failureCount++;
    if (this.failureCount >= this.threshold) {
      this.isOpen = true;
      console.warn(`[circuit-breaker] ${this.name} tripped after ${this.failureCount} consecutive failures${this.quotaExhausted ? ' (quota exhausted)' : ''}`);
    }
  }

  recordEmpty(): void {
    this.emptyResponseCount++;
    if (this.emptyResponseCount >= this.emptyThreshold) {
      this.recordFailure();
    }
  }

  reset(): void {
    this.failureCount = 0;
    this.emptyResponseCount = 0;
    this.isOpen = false;
    this.quotaExhausted = false;
  }
}
