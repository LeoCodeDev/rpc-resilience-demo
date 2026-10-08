export interface BackoffOptions {
  baseDelayMs: number;
  maxDelayMs: number;
  /** Injectable for deterministic tests. Must return a value in [0, 1). */
  random?: () => number;
}

/**
 * Exponential backoff with "full jitter":
 *   delay = random(0, min(maxDelay, base * 2^(attempt-1)))
 * Full jitter spreads retries uniformly, avoiding thundering-herd spikes when
 * many workers hit the same rate-limited provider at once.
 *
 * @param attempt 1-based number of the attempt that just failed.
 */
export function computeBackoffDelay(attempt: number, opts: BackoffOptions): number {
  const random = opts.random ?? Math.random;
  const ceiling = Math.min(opts.maxDelayMs, opts.baseDelayMs * 2 ** (attempt - 1));
  return Math.floor(random() * ceiling);
}
