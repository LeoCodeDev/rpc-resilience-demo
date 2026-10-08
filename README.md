# rpc-resilience-demo

A small, dependency-light TypeScript module showing how to talk to **rate-limited third-party RPC providers** safely: automatic failover, retries with exponential backoff and jitter, idempotent queue processing, and a Dead Letter Queue (DLQ). Everything is simulated in-memory, so it runs with zero infrastructure.

## Run it

```bash
npm install && npm start
```

Requires Node.js >= 18.17. Other scripts: `npm run typecheck`, `npm run build`.

The demo runs four scenarios:

1. **Success path**, plus an idempotent replay of the same `txId` (dropped).
2. **Failover**: the primary node returns HTTP 429, so the request is served by the secondary.
3. **DLQ**: both nodes fail (timeout / 503). The job is retried, then dead-lettered.
4. **Validation**: a malformed payload is rejected at the boundary.

## Project layout

```
src/
├── schemas/transaction.schema.ts     Zod schemas + inferred types (input DTO, RPC response)
├── errors/rpc.errors.ts              Typed error hierarchy with a `retryable` flag
├── services/
│   ├── rpc-provider.service.ts       IRPCProvider port + ResilientRPCProvider (timeout, retry, failover)
│   └── simulated-rpc-node.ts         Scriptable fake RPC node (ok / 429 / 5xx / hang)
├── queues/transaction-processor.ts   Async worker: idempotency, retries, DLQ
├── utils/                            backoff (full jitter), abortable sleep, logger
└── index.ts                          Composition root + console simulation
```

## Architecture decisions

**Clean Architecture / ports & adapters.**
- The queue (application layer) depends only on the `IRPCProvider` interface. It knows nothing about nodes, HTTP or retries.
- `ResilientRPCProvider` is a decorator over low-level `IRpcNode` transports. Replacing the simulator with a real `fetch`/ethers transport changes one class.
- `IDeadLetterQueue` is a port as well, so the in-memory DLQ can become BullMQ, SQS or a database table.
- `index.ts` is the only place where concrete classes are wired together.

**Resilience patterns.**
- **Timeouts that really cancel.** Each request gets an `AbortController`. The in-flight call is aborted, not just ignored.
- **Exponential backoff with full jitter.** `delay = random(0, min(max, base·2^n))` avoids thundering-herd retries against a provider that is already throttling. A server `Retry-After` hint is honoured when it is longer.
- **Failover.** A node is tried up to `maxAttemptsPerNode` times. If it still times out, returns 429 or returns 5xx, the next node is used. If every node fails, `AllProvidersFailedError` carries each node's error.
- **Fail fast on non-transient errors.** Errors carry a `retryable` flag. A schema-invalid response is not retried on the same node, but still triggers failover.
- **Two retry levels.** The provider handles fast, transient retries. The queue handles slower job-level retries, up to `maxAttempts`, then routes the job to the DLQ.

**Idempotency.** `txId` (normalised to lowercase) is the idempotency key. Replays are dropped while a job is queued, running or completed. The key is released when a job is dead-lettered, so it can be replayed after the cause is fixed.

**Strict typing and validation.** `strict`, `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes` are enabled. Zod validates both the inbound payload and the third-party RPC response, because external data is never trusted. Types are inferred from the schemas, so there is a single source of truth.

## Production notes

- The in-memory queue and idempotency set are single-process. In production use Redis (BullMQ) with `SET NX` or a unique DB constraint for idempotency.
- Add a circuit breaker so a known-bad primary is skipped for a cool-down period instead of being retried first on every request.
- `computeBackoffDelay` accepts an injectable `random`, and the `IRpcNode` and `ILogger` seams make unit testing straightforward. No test suite is included, to keep the demo focused.
