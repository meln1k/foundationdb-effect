import { Effect } from "effect";
import type { Subspace } from "../tuple/mod.ts";

/** The mailbox's physical key layout. Callers supply only keys within a family. */
export const makeMessageSubspaces = Effect.fnUntraced(
  function* (root: Subspace) {
    return {
      messages: yield* root.subspace(["message"]),
      envelopes: yield* root.subspace(["envelope"]),
      requestStates: yield* root.subspace(["state"]),
      primaryKeys: yield* root.subspace(["primary"]),
      replies: yield* root.subspace(["reply"]),
      replyMetadata: yield* root.subspace(["reply-meta"]),
      replyIds: yield* root.subspace(["replies"]),
      uniqueReplies: yield* root.subspace(["reply-unique"]),
      messagesByShard: yield* root.subspace(["shard"]),
      messagesByAddress: yield* root.subspace(["address"]),
      messagesByRequest: yield* root.subspace(["request"]),
      readyByShard: yield* root.subspace(["ready", "shard"]),
      readyByAddress: yield* root.subspace(["ready", "address"]),
      scheduledByShard: yield* root.subspace(["scheduled", "shard"]),
      scheduledByAddress: yield* root.subspace(["scheduled", "address"]),
      // Retained after completion so reopening preserves original enqueue order.
      orders: yield* root.subspace(["order"]),
    };
  },
);

export type MessageSubspaces = Effect.Success<
  ReturnType<typeof makeMessageSubspaces>
>;
