/**
 * Typed error hierarchy. The `retryable` flag lets the resilience logic
 * decide what to do without brittle `instanceof` chains or string matching.
 */
export abstract class RpcError extends Error {
  abstract readonly retryable: boolean;

  constructor(
    message: string,
    readonly node: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class RpcTimeoutError extends RpcError {
  readonly retryable = true;
  constructor(node: string, timeoutMs: number) {
    super(`[${node}] request timed out after ${timeoutMs}ms`, node);
  }
}

export class RpcRateLimitError extends RpcError {
  readonly retryable = true;
  constructor(
    node: string,
    /** Value of the `Retry-After` header, when the node provides one. */
    readonly retryAfterMs?: number,
  ) {
    super(`[${node}] HTTP 429 rate limited`, node);
  }
}

export class RpcServerError extends RpcError {
  readonly retryable = true;
  constructor(node: string, readonly status: number) {
    super(`[${node}] HTTP ${status} server error`, node);
  }
}

/** Node answered, but the payload failed schema validation. Retrying the same node is pointless. */
export class RpcInvalidResponseError extends RpcError {
  readonly retryable = false;
  constructor(node: string, detail: string) {
    super(`[${node}] invalid response: ${detail}`, node);
  }
}

/** Every configured node failed. Carries each node's last error for diagnostics. */
export class AllProvidersFailedError extends Error {
  constructor(
    readonly txHash: string,
    readonly causes: readonly RpcError[],
  ) {
    super(`All RPC providers failed for ${txHash}: ${causes.map((c) => c.message).join(" | ")}`);
    this.name = "AllProvidersFailedError";
  }
}
