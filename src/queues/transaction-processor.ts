import { TransactionTaskSchema, type RpcTransactionResponse, type TransactionTask } from "../schemas/transaction.schema.js";
import type { IRPCProvider } from "../services/rpc-provider.service.js";
import { computeBackoffDelay } from "../utils/backoff.js";
import type { ILogger } from "../utils/logger.js";

export interface DeadLetterEntry {
  /** Parsed task, or the raw payload when it failed validation. */
  payload: TransactionTask | unknown;
  reason: "validation-failed" | "max-retries-exceeded";
  attempts: number;
  lastError: string;
  failedAt: Date;
}

/** Port for the DLQ; in production this would be a BullMQ/SQS queue. */
export interface IDeadLetterQueue {
  push(entry: DeadLetterEntry): void;
  list(): readonly DeadLetterEntry[];
}

export class InMemoryDeadLetterQueue implements IDeadLetterQueue {
  private readonly entries: DeadLetterEntry[] = [];
  push(entry: DeadLetterEntry): void {
    this.entries.push(entry);
  }
  list(): readonly DeadLetterEntry[] {
    return this.entries;
  }
}

export interface ProcessorOptions {
  /** Total tries per job (including the first) before it is dead-lettered. */
  maxAttempts: number;
  concurrency: number;
  retryBaseDelayMs: number;
  retryMaxDelayMs: number;
}

export type EnqueueOutcome = "accepted" | "duplicate" | "rejected";

interface Job {
  task: TransactionTask;
  attempts: number;
}

/**
 * In-memory async queue worker. The structure mirrors a BullMQ worker
 * (enqueue / concurrency / attempts / backoff / DLQ) so swapping in Redis
 * only touches this file.
 *
 * Idempotency: `txId` is the idempotency key. A key is held while the job is
 * queued/running and kept after success, so replays are dropped. It is
 * released when the job lands in the DLQ so it can be resubmitted after a fix.
 */
export class TransactionProcessor {
  private readonly queue: Job[] = [];
  private readonly seen = new Set<string>();
  private readonly results = new Map<string, RpcTransactionResponse>();
  private active = 0;
  private pendingRetries = 0;
  private idleWaiters: Array<() => void> = [];

  constructor(
    private readonly provider: IRPCProvider,
    private readonly dlq: IDeadLetterQueue,
    private readonly options: ProcessorOptions,
    private readonly logger: ILogger,
  ) {}

  /** Validates, de-duplicates and enqueues a raw payload. Never throws. */
  enqueue(payload: unknown): EnqueueOutcome {
    const parsed = TransactionTaskSchema.safeParse(payload);
    if (!parsed.success) {
      this.logger.error(`Rejected invalid payload: ${parsed.error.issues.map((i) => i.message).join("; ")}`);
      this.dlq.push({
        payload,
        reason: "validation-failed",
        attempts: 0,
        lastError: parsed.error.issues.map((i) => `${i.path.join(".") || "payload"}: ${i.message}`).join("; "),
        failedAt: new Date(),
      });
      return "rejected";
    }

    const task = parsed.data;
    if (this.seen.has(task.txId)) {
      this.logger.info(`Duplicate ${short(task.txId)} ignored (idempotent)`);
      return "duplicate";
    }

    this.seen.add(task.txId);
    this.queue.push({ task, attempts: 0 });
    this.pump();
    return "accepted";
  }

  getResult(txId: string): RpcTransactionResponse | undefined {
    return this.results.get(txId.toLowerCase());
  }

  /** Resolves once the queue is drained, including jobs waiting on a retry timer. */
  onIdle(): Promise<void> {
    if (this.isIdle()) return Promise.resolve();
    return new Promise((resolve) => this.idleWaiters.push(resolve));
  }

  private isIdle(): boolean {
    return this.active === 0 && this.queue.length === 0 && this.pendingRetries === 0;
  }

  private pump(): void {
    while (this.active < this.options.concurrency) {
      const job = this.queue.shift();
      if (!job) break;
      this.active++;
      void this.run(job).finally(() => {
        this.active--;
        this.pump();
      });
    }
    if (this.isIdle()) {
      this.idleWaiters.splice(0).forEach((resolve) => resolve());
    }
  }

  private async run(job: Job): Promise<void> {
    const { txId } = job.task;
    job.attempts++;
    try {
      const result = await this.provider.getTransaction(txId);
      this.results.set(txId, result);
      this.logger.info(`Processed ${short(txId)} -> ${result.status} @ block ${result.blockNumber}`);
    } catch (err) {
      this.handleFailure(job, err);
    }
  }

  private handleFailure(job: Job, err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    const { txId } = job.task;

    if (job.attempts >= this.options.maxAttempts) {
      this.logger.error(`${short(txId)} exhausted ${job.attempts} attempts -> Dead Letter Queue`);
      this.dlq.push({
        payload: job.task,
        reason: "max-retries-exceeded",
        attempts: job.attempts,
        lastError: message,
        failedAt: new Date(),
      });
      this.seen.delete(txId); // allow a manual replay later
      return;
    }

    const delay = computeBackoffDelay(job.attempts, {
      baseDelayMs: this.options.retryBaseDelayMs,
      maxDelayMs: this.options.retryMaxDelayMs,
    });
    this.logger.warn(`Job ${short(txId)} failed (attempt ${job.attempts}/${this.options.maxAttempts}), requeue in ${delay}ms`);
    this.pendingRetries++;
    setTimeout(() => {
      this.pendingRetries--;
      this.queue.push(job);
      this.pump();
    }, delay);
  }
}

const short = (txId: string): string => `${txId.slice(0, 6)}…${txId.slice(-4)}`;
