import { RpcRateLimitError, RpcServerError } from "../errors/rpc.errors.js";
import { sleep } from "../utils/async.js";
import type { IRpcNode } from "./rpc-provider.service.js";

/** Scripted behaviour for one simulated request. */
export type NodeBehavior =
  | { kind: "ok"; latencyMs: number }
  | { kind: "rate-limit"; retryAfterMs?: number }
  | { kind: "server-error"; status: number }
  /** Hangs until the caller aborts — exercises the provider's timeout path. */
  | { kind: "hang" };

export type BehaviorScript = (txHash: string, callIndex: number) => NodeBehavior;

/** In-memory fake of a JSON-RPC node, driven by a per-request behaviour script. */
export class SimulatedRpcNode implements IRpcNode {
  private calls = 0;

  constructor(
    readonly name: string,
    private readonly script: BehaviorScript,
  ) {}

  async request(txHash: string, signal: AbortSignal): Promise<unknown> {
    const behavior = this.script(txHash, this.calls++);
    switch (behavior.kind) {
      case "ok":
        await sleep(behavior.latencyMs, signal);
        return {
          hash: txHash,
          blockNumber: 19_000_000 + (this.calls % 1000),
          status: "confirmed",
          confirmations: 12,
          gasUsed: 21_000,
        };
      case "rate-limit":
        throw new RpcRateLimitError(this.name, behavior.retryAfterMs);
      case "server-error":
        throw new RpcServerError(this.name, behavior.status);
      case "hang":
        await sleep(60_000, signal); // rejected by the provider's AbortController
        throw new Error("unreachable");
    }
  }
}
