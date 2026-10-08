import { InMemoryDeadLetterQueue, TransactionProcessor } from "./queues/transaction-processor.js";
import { ResilientRPCProvider } from "./services/rpc-provider.service.js";
import { SimulatedRpcNode, type NodeBehavior } from "./services/simulated-rpc-node.js";
import { consoleLogger as log } from "./utils/logger.js";

const txHash = (n: number): string => `0x${n.toString(16).padStart(64, "0")}`;
const address = (n: number): string => `0x${n.toString(16).padStart(40, "0")}`;
const task = (txId: string) => ({ txId, from: address(1), to: address(2), valueWei: "1000000000000000000", chainId: 1 });

const TX_OK = txHash(0xa1);
const TX_FAILOVER = txHash(0xb2);
const TX_DLQ = txHash(0xc3);

const ok: NodeBehavior = { kind: "ok", latencyMs: 40 };

// Primary: healthy, except it rate-limits TX_FAILOVER and hangs on TX_DLQ.
const primary = new SimulatedRpcNode("primary", (tx): NodeBehavior => {
  if (tx === TX_FAILOVER) return { kind: "rate-limit", retryAfterMs: 50 };
  if (tx === TX_DLQ) return { kind: "hang" };
  return ok;
});
// Secondary: healthy, except it returns 503 for TX_DLQ (so everything fails).
const secondary = new SimulatedRpcNode("secondary", (tx): NodeBehavior =>
  tx === TX_DLQ ? { kind: "server-error", status: 503 } : ok,
);

const provider = new ResilientRPCProvider(
  [primary, secondary],
  { timeoutMs: 250, maxAttemptsPerNode: 2, baseDelayMs: 100, maxDelayMs: 800 },
  log,
);
const dlq = new InMemoryDeadLetterQueue();
const processor = new TransactionProcessor(
  provider,
  dlq,
  { maxAttempts: 3, concurrency: 2, retryBaseDelayMs: 200, retryMaxDelayMs: 1_000 },
  log,
);

const section = (title: string): void => console.log(`\n\x1b[1m━━ ${title} ━━\x1b[0m`);

async function main(): Promise<void> {
  section("1. Success path (+ idempotent replay)");
  processor.enqueue(task(TX_OK));
  processor.enqueue(task(TX_OK.toUpperCase().replace("0X", "0x"))); // same tx, different casing
  await processor.onIdle();

  section("2. Failover: primary returns HTTP 429 -> secondary");
  processor.enqueue(task(TX_FAILOVER));
  await processor.onIdle();

  section("3. Both nodes down -> retries -> Dead Letter Queue");
  processor.enqueue(task(TX_DLQ));
  await processor.onIdle();

  section("4. Invalid payload -> rejected at the boundary");
  processor.enqueue({ txId: "not-a-hash" });

  section("Dead Letter Queue contents");
  for (const e of dlq.list()) {
    console.log(`- reason=${e.reason} attempts=${e.attempts}\n  lastError=${e.lastError.split("\n")[0]?.slice(0, 140)}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
