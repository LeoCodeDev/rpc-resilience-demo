import { z } from "zod";

/**
 * Zod schemas are the single source of truth for both runtime validation
 * and static types (via `z.infer`). Nothing crosses a boundary un-parsed.
 */

/** 32-byte hex hash, normalised to lowercase so idempotency keys are stable. */
export const TxHashSchema = z
  .string()
  .regex(/^0x[a-fA-F0-9]{64}$/, "txId must be a 0x-prefixed 32-byte hex string")
  .transform((s) => s.toLowerCase());

export const AddressSchema = z
  .string()
  .regex(/^0x[a-fA-F0-9]{40}$/, "invalid address");

/** Incoming queue payload (DTO). */
export const TransactionTaskSchema = z.object({
  txId: TxHashSchema,
  from: AddressSchema,
  to: AddressSchema,
  /** Wei amount as a decimal string — avoids JS number precision loss. */
  valueWei: z.string().regex(/^\d+$/, "valueWei must be a non-negative integer string"),
  chainId: z.number().int().positive(),
});
export type TransactionTask = z.infer<typeof TransactionTaskSchema>;

/** What we expect an RPC node to return for a transaction lookup. */
export const RpcTransactionResponseSchema = z.object({
  hash: TxHashSchema,
  blockNumber: z.number().int().nonnegative(),
  status: z.enum(["confirmed", "reverted"]),
  confirmations: z.number().int().nonnegative(),
  gasUsed: z.number().int().nonnegative(),
});
export type RpcTransactionResponse = z.infer<typeof RpcTransactionResponseSchema>;
