import {
  AllProvidersFailedError,
  RpcError,
  RpcInvalidResponseError,
  RpcRateLimitError,
  RpcTimeoutError,
} from "../errors/rpc.errors.js";
import { RpcTransactionResponseSchema, type RpcTransactionResponse } from "../schemas/transaction.schema.js";
import { sleep } from "../utils/async.js";
import { computeBackoffDelay } from "../utils/backoff.js";
import type { ILogger } from "../utils/logger.js";

/** Public port consumed by the application layer. */
export interface IRPCProvider {
  getTransaction(txHash: string): Promise<RpcTransactionResponse>;
}

/**
 * Low-level transport for a single RPC endpoint (HTTP client, ethers provider,
 * simulator...). Must honour `signal` so timeouts really cancel the request.
 */
export interface IRpcNode {
  readonly name: string;
  request(txHash: string, signal: AbortSignal): Promise<unknown>;
}

export interface ResilientProviderOptions {
  /** Per-request deadline. */
  timeoutMs: number;
  /** Attempts per node (including the first) before failing over. */
  maxAttemptsPerNode: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

/**
 * Decorates an ordered list of nodes (primary first, then fallbacks) with:
 *  - per-request timeouts,
 *  - retries with exponential backoff + full jitter on transient errors,
 *  - automatic failover to the next node when a node keeps timing out / 429s / 5xxs.
 */
export class ResilientRPCProvider implements IRPCProvider {
  constructor(
    private readonly nodes: readonly [IRpcNode, ...IRpcNode[]],
    private readonly options: ResilientProviderOptions,
    private readonly logger: ILogger,
  ) {}

  async getTransaction(txHash: string): Promise<RpcTransactionResponse> {
    const failures: RpcError[] = [];

    for (const [index, node] of this.nodes.entries()) {
      try {
        return await this.queryWithRetries(node, txHash);
      } catch (err) {
        if (!(err instanceof RpcError)) throw err; // programmer error: don't mask it
        failures.push(err);
        const next = this.nodes[index + 1];
        if (next) this.logger.warn(`Failover: ${node.name} -> ${next.name} (${err.message})`);
      }
    }
    throw new AllProvidersFailedError(txHash, failures);
  }

  private async queryWithRetries(node: IRpcNode, txHash: string): Promise<RpcTransactionResponse> {
    const { maxAttemptsPerNode } = this.options;

    for (let attempt = 1; ; attempt++) {
      try {
        const raw = await this.requestWithTimeout(node, txHash);
        return this.parse(node, raw);
      } catch (err) {
        if (!(err instanceof RpcError) || !err.retryable || attempt >= maxAttemptsPerNode) throw err;

        let delay = computeBackoffDelay(attempt, this.options);
        // Respect the server's own back-pressure hint when it's longer than ours.
        if (err instanceof RpcRateLimitError && err.retryAfterMs) delay = Math.max(delay, err.retryAfterMs);

        this.logger.warn(`${err.message} — retry ${attempt}/${maxAttemptsPerNode - 1} in ${delay}ms`);
        await sleep(delay);
      }
    }
  }

  /** Enforces the deadline by aborting the in-flight request, not just ignoring it. */
  private async requestWithTimeout(node: IRpcNode, txHash: string): Promise<unknown> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.options.timeoutMs);
    try {
      return await node.request(txHash, controller.signal);
    } catch (err) {
      if (controller.signal.aborted) throw new RpcTimeoutError(node.name, this.options.timeoutMs);
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  /** Never trust a third party: validate the response shape at the boundary. */
  private parse(node: IRpcNode, raw: unknown): RpcTransactionResponse {
    const result = RpcTransactionResponseSchema.safeParse(raw);
    if (!result.success) throw new RpcInvalidResponseError(node.name, result.error.message);
    return result.data;
  }
}
