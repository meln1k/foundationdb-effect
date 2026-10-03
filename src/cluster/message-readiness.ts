/** Transactional membership of the ready and time-ordered polling indexes. */
import { Effect, Schema } from "effect";
import * as ShardId from "effect/cluster/ShardId";
import type { FoundationDbTransaction } from "../FoundationDb.ts";
import type { Tuple } from "../tuple/mod.ts";
import { setMessageOrderIndexed } from "./message-order.ts";
import type { MessageSubspaces } from "./message-subspaces.ts";

export const claimExpirationMillis = 10 * 60 * 1_000;
const Polling = Schema.Union([
  Schema.Literals(["ready", "blocked"]),
  Schema.Number,
]);
const Address = Schema.Struct({
  shardId: Schema.Struct({ group: Schema.String, id: Schema.Int }),
  entityType: Schema.String,
  entityId: Schema.String,
});
export const MessageMeta = Schema.Struct({
  id: Schema.String,
  requestId: Schema.String,
  kind: Schema.Literals(["Request", "AckChunk", "Interrupt"]),
  address: Address,
  primaryKey: Schema.NullOr(Schema.String),
  deliverAt: Schema.NullOr(Schema.Number),
  claimedAt: Schema.NullOr(Schema.Number),
  processed: Schema.Boolean,
  // A number is the next eligibility time, shared by delivery and claim timers.
  polling: Polling,
});
export type MessageMeta = typeof MessageMeta.Type;
export const RequestState = Schema.Struct({
  lastReplyId: Schema.NullOr(Schema.String),
  exitId: Schema.NullOr(Schema.String),
  latestAckId: Schema.NullOr(Schema.String),
  unacked: Schema.Int,
});
export type RequestState = typeof RequestState.Type;

export const addressKey = (address: typeof Address.Type): Tuple => [
  address.entityType,
  address.entityId,
  ShardId.toString(address.shardId),
];
export const readyIndexes = (spaces: MessageSubspaces, meta: MessageMeta) =>
  Effect.all([
    spaces.readyByShard.subspace([ShardId.toString(meta.address.shardId)]),
    spaces.readyByAddress.subspace(addressKey(meta.address)),
  ]);
const scheduledIndexes = (spaces: MessageSubspaces, meta: MessageMeta) =>
  Effect.all([
    spaces.scheduledByShard.subspace([ShardId.toString(meta.address.shardId)]),
    spaces.scheduledByAddress.subspace(addressKey(meta.address)),
  ]);

// No wall clock at write time: the polling API supplies `now`. Even a timer
// already in the past is promoted by polling, preserving deterministic clocks.
export const pollingState = (
  meta: MessageMeta,
  state: RequestState,
): typeof Polling.Type => {
  if (
    meta.processed || state.exitId !== null || state.unacked > 0 ||
    (meta.kind === "AckChunk" && state.latestAckId !== meta.id)
  ) return "blocked";
  if (meta.claimedAt !== null) {
    return Math.max(
      meta.deliverAt ?? -Infinity,
      meta.claimedAt + claimExpirationMillis,
    );
  }
  return meta.deliverAt ?? "ready";
};

interface Store {
  readonly transaction: FoundationDbTransaction["Service"];
  readonly spaces: MessageSubspaces;
}
const empty = new Uint8Array();

export const clearMessageTimer = Effect.fnUntraced(function* (
  store: Store,
  meta: MessageMeta,
) {
  if (typeof meta.polling !== "number") return;
  for (const index of yield* scheduledIndexes(store.spaces, meta)) {
    yield* store.transaction.clear(yield* index.pack([meta.polling, meta.id]));
  }
});

/** Returns the metadata to persist alongside these index mutations. */
export const setMessagePolling = Effect.fnUntraced(function* (
  store: Store,
  meta: MessageMeta,
  polling: typeof Polling.Type,
) {
  if (meta.polling === polling) return meta;
  yield* clearMessageTimer(store, meta);
  if (meta.polling === "ready" || polling === "ready") {
    yield* setMessageOrderIndexed(
      store,
      meta.id,
      yield* readyIndexes(store.spaces, meta),
      polling === "ready",
    );
  }
  if (typeof polling === "number") {
    for (const index of yield* scheduledIndexes(store.spaces, meta)) {
      yield* store.transaction.set(
        yield* index.pack([polling, meta.id]),
        empty,
      );
    }
  }
  return { ...meta, polling };
});
