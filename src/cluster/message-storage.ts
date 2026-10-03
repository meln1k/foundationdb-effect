/** Durable Effect Cluster mailbox storage in a FoundationDB directory. */
import { Effect, Layer, Option, Schema, Stream } from "effect";
import * as ClusterError from "effect/cluster/ClusterError";
import type * as EntityAddress from "effect/cluster/EntityAddress";
import * as Envelope from "effect/cluster/Envelope";
import * as MessageStorage from "effect/cluster/MessageStorage";
import type * as Reply from "effect/cluster/Reply";
import * as ShardId from "effect/cluster/ShardId";
import * as Snowflake from "effect/cluster/Snowflake";
import { FoundationDb, FoundationDbTransaction } from "../FoundationDb.ts";
import { isFoundationDbError } from "../errors.ts";
import type { FoundationDbError } from "../errors.ts";
import {
  clearChunkedValue,
  readChunkedValue,
  writeChunkedValue,
} from "../internal/chunked-value.ts";
import { keyRange } from "../model.ts";
import { makeDirectoryStore } from "../persistence/internal.ts";
import type { DirectoryStoreOptions } from "../persistence/internal.ts";
import { compareBytes } from "../tuple/bytes.ts";
import type { Subspace, Tuple } from "../tuple/mod.ts";
import { DuplicateReplyError, MissingMessageRecordError } from "./errors.ts";
import {
  insertMessageOrder,
  messageOrder,
  removeMessageOrder,
  visitMessageOrder,
} from "./message-order.ts";
import {
  addressKey,
  claimExpirationMillis,
  clearMessageTimer,
  MessageMeta,
  pollingState,
  readyIndexes,
  RequestState,
  setMessagePolling,
} from "./message-readiness.ts";
import { makeMessageSubspaces } from "./message-subspaces.ts";
import type { MessageSubspaces } from "./message-subspaces.ts";

/** Directory and retry settings shared by all instances of this mailbox. */
export interface MessageStorageOptions extends DirectoryStoreOptions {}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const pageSize = 64;
const empty = new Uint8Array();

const initialState: RequestState = {
  lastReplyId: null,
  exitId: null,
  latestAckId: null,
  unacked: 0,
};
const ReplyMeta = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["Chunk", "WithExit"]),
  sequence: Schema.NullOr(Schema.Int),
  acked: Schema.Boolean,
});
const ReplySchema: Schema.Codec<Reply.Encoded, unknown> = Schema.Union([
  Schema.TaggedStruct("Chunk", {
    id: Schema.String,
    requestId: Schema.String,
    sequence: Schema.Int,
    values: Schema.NonEmptyArray(Schema.Unknown),
  }),
  Schema.TaggedStruct("WithExit", {
    id: Schema.String,
    requestId: Schema.String,
    exit: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Unknown }),
      Schema.TaggedStruct("Failure", {
        cause: Schema.Array(Schema.Union([
          Schema.TaggedStruct("Fail", {
            error: Schema.Unknown,
          }),
          Schema.TaggedStruct("Die", {
            defect: Schema.Unknown,
          }),
          Schema.TaggedStruct("Interrupt", {
            fiberId: Schema.UndefinedOr(Schema.NullOr(Schema.Number)).pipe(
              Schema.withDecodingDefault(Effect.sync(() => undefined)),
            ),
          }),
        ])),
      }),
    ]),
  }),
]);

interface Store {
  readonly transaction: FoundationDbTransaction["Service"];
  readonly spaces: MessageSubspaces;
}

// Metadata is stored at point keys for getMany. Only payloads need chunking.
const record = <A, I>(
  namespace: "messages" | "requestStates" | "replyMetadata",
  schema: Schema.Codec<A, I>,
) => {
  const json = Schema.fromJsonString(schema);
  const decode = Schema.decodeUnknownEffect(json);
  const encode = Schema.encodeEffect(json);
  const decodeValue = Effect.fnUntraced(
    function* (bytes: Uint8Array | undefined) {
      return bytes === undefined
        ? undefined
        : yield* decode(decoder.decode(bytes));
    },
  );
  return {
    read: Effect.fnUntraced(function* (store: Store, path: Tuple) {
      return yield* decodeValue(
        yield* store.transaction.get(yield* store.spaces[namespace].pack(path)),
      );
    }),
    readMany: Effect.fnUntraced(
      function* (store: Store, paths: ReadonlyArray<Tuple>) {
        if (paths.length === 0) return [];
        const keys = yield* Effect.forEach(
          paths,
          (path) => store.spaces[namespace].pack(path),
        );
        return yield* Effect.forEach(
          yield* store.transaction.getMany(keys),
          decodeValue,
        );
      },
    ),
    write: Effect.fnUntraced(function* (store: Store, path: Tuple, value: A) {
      yield* store.transaction.set(
        yield* store.spaces[namespace].pack(path),
        encoder.encode(yield* encode(value)),
      );
    }),
    clear: Effect.fnUntraced(function* (store: Store, path: Tuple) {
      yield* store.transaction.clear(yield* store.spaces[namespace].pack(path));
    }),
  };
};
const chunkedRecord = <A, I>(
  namespace: keyof MessageSubspaces,
  schema: Schema.Codec<A, I>,
) => {
  const json = Schema.fromJsonString(schema);
  const decode = Schema.decodeUnknownEffect(json);
  const encode = Schema.encodeEffect(json);
  return {
    read: Effect.fnUntraced(function* (store: Store, path: Tuple) {
      const bytes = yield* readChunkedValue(
        store.transaction,
        store.spaces[namespace],
        path,
      );
      return bytes === undefined
        ? undefined
        : yield* decode(decoder.decode(bytes));
    }),
    write: Effect.fnUntraced(function* (store: Store, path: Tuple, value: A) {
      yield* writeChunkedValue(
        store.transaction,
        store.spaces[namespace],
        path,
        encoder.encode(yield* encode(value)),
      );
    }),
    clear: (store: Store, path: Tuple) =>
      clearChunkedValue(store.transaction, store.spaces[namespace], path),
  };
};
const messages = record("messages", MessageMeta);
const envelopes = chunkedRecord(
  "envelopes",
  Schema.toEncoded(Envelope.PartialJson),
);
const states = record("requestStates", RequestState);
const replyMetadata = record("replyMetadata", ReplyMeta);
const replies = chunkedRecord("replies", ReplySchema);
const readState = Effect.fnUntraced(function* (store: Store, id: string) {
  return (yield* states.read(store, [id])) ?? initialState;
});
const writeMessage = Effect.fnUntraced(function* (
  store: Store,
  meta: MessageMeta,
  state: RequestState,
) {
  yield* messages.write(
    store,
    [meta.id],
    yield* setMessagePolling(store, meta, pollingState(meta, state)),
  );
});
const messageIndexes = (spaces: MessageSubspaces, meta: MessageMeta) =>
  Effect.all([
    spaces.messagesByShard.subspace([ShardId.toString(meta.address.shardId)]),
    spaces.messagesByAddress.subspace(addressKey(meta.address)),
    spaces.messagesByRequest.subspace([meta.requestId]),
  ]);
const removeMessage = Effect.fnUntraced(
  function* (store: Store, meta: MessageMeta) {
    yield* clearMessageTimer(store, meta);
    yield* removeMessageOrder(
      store,
      meta.id,
      yield* readyIndexes(store.spaces, meta),
    );
    for (const index of yield* messageIndexes(store.spaces, meta)) {
      yield* store.transaction.clear(
        yield* index.pack([BigInt(meta.id)]),
      );
    }
    if (meta.primaryKey !== null) {
      yield* store.transaction.clear(
        yield* store.spaces.primaryKeys.pack([meta.primaryKey]),
      );
    }
    yield* messages.clear(store, [meta.id]);
    yield* envelopes.clear(store, [meta.id]);
  },
);

// Cursor reads are confined to the requested shard/address/request. In
// particular a bounded claim does not materialize the entire mailbox index.
const visitIndex = Effect.fnUntraced(function* <E>(
  store: Store,
  index: Subspace,
  visit: (id: string) => Effect.Effect<boolean, E>,
) {
  const [start, end] = yield* index.range();
  let begin = start;
  while (true) {
    const rows = yield* Stream.runCollect(
      store.transaction.getRange(keyRange(begin, end, { limit: pageSize })),
    );
    for (const row of rows) {
      const tuple = yield* index.unpack(row.key);
      if (!(yield* visit(String(tuple[tuple.length - 1])))) return;
    }
    if (rows.length < pageSize) return;
    const last = rows[rows.length - 1].key;
    begin = new Uint8Array(last.length + 1);
    begin.set(last);
  }
});

const primaryHash = (value: string) =>
  Effect.tryPromise(async () => {
    const hash = await crypto.subtle.digest("SHA-256", encoder.encode(value));
    return Array.from(new Uint8Array(hash), (byte) =>
      byte.toString(16).padStart(2, "0")).join("");
  });

/** Low-level encoded driver, also useful for testing storage boundaries. */
export const makeEncodedMessageStorage = Effect.fnUntraced(function* (
  options: MessageStorageOptions = {},
) {
  const database = yield* FoundationDb;
  const directory = yield* makeDirectoryStore(
    options,
    ["effect-foundationdb", "message-storage"],
    "effect-foundationdb/message-storage",
  );

  const transact = Effect.fnUntraced(function* <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<
    A,
    E | FoundationDbError,
    Exclude<R, FoundationDbTransaction>
  > {
    const transaction = yield* Effect.serviceOption(FoundationDbTransaction);
    if (Option.isSome(transaction)) {
      return yield* Effect.provideService(
        effect,
        FoundationDbTransaction,
        transaction.value,
      );
    }
    return yield* database.withTransaction(
      effect,
      {
        ...directory.transactionOptions,
        // Replaying an ambiguously committed claim would return a different
        // batch and strand the original claims until expiration.
        retryOnMaybeCommitted: false,
      },
    );
  });

  const run = <A, E>(f: (store: Store) => Effect.Effect<A, E>) =>
    transact(Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      const spaces = yield* makeMessageSubspaces(yield* directory.root());
      return yield* f({ transaction, spaces });
    })).pipe(ClusterError.PersistenceError.refail);

  const updateRequest = Effect.fnUntraced(function* (
    store: Store,
    id: string,
    update: (meta: MessageMeta) => Effect.Effect<void, unknown>,
  ) {
    yield* visitIndex(
      store,
      yield* store.spaces.messagesByRequest.subspace([id]),
      Effect.fnUntraced(function* (messageId) {
        const meta = yield* messages.read(store, [messageId]);
        if (meta !== undefined) yield* update(meta);
        return true;
      }),
    );
  });

  const lastReply = Effect.fnUntraced(
    function* (store: Store, id: string, state: RequestState) {
      return state.lastReplyId === null
        ? Option.none<Reply.Encoded>()
        : Option.fromNullishOr(
          yield* replies.read(store, [id, state.lastReplyId]),
        );
    },
  );

  const readMessages = Effect.fnUntraced(function* (
    store: Store,
    ids: ReadonlyArray<string>,
    now: number,
    claim: boolean,
    limit: number,
  ) {
    const metadata = yield* messages.readMany(store, ids.map((id) => [id]));
    const candidates = metadata.filter((meta): meta is MessageMeta =>
      meta !== undefined && !meta.processed &&
      (meta.deliverAt === null || meta.deliverAt <= now) &&
      (!claim || meta.claimedAt === null ||
        meta.claimedAt <= now - claimExpirationMillis)
    );
    const requestIds = [...new Set(candidates.map((meta) => meta.requestId))];
    const requestStates = yield* states.readMany(
      store,
      requestIds.map((id) => [id]),
    );
    const byRequest = new Map(
      requestIds.map((id, i) => [id, requestStates[i] ?? initialState]),
    );
    const selected: Array<{ meta: MessageMeta; state: RequestState }> = [];
    for (const meta of candidates) {
      const state = byRequest.get(meta.requestId)!;
      if (
        state.exitId !== null || state.unacked > 0 ||
        (meta.kind === "AckChunk" && state.latestAckId !== meta.id)
      ) continue;
      selected.push({ meta, state });
      if (selected.length === limit) break;
    }
    // forEach preserves input order even when payload reads finish out of order.
    // Prefetching metadata never claims or loads payloads beyond the batch limit.
    const loaded = yield* Effect.forEach(
      selected,
      Effect.fnUntraced(function* ({ meta, state }) {
        const envelope = yield* envelopes.read(store, [meta.id]);
        if (envelope === undefined) {
          return yield* new MissingMessageRecordError({
            record: "envelope",
            id: meta.id,
          });
        }
        return {
          envelope,
          lastSentReply: meta.kind === "Request"
            ? yield* lastReply(store, meta.requestId, state)
            : Option.none<Reply.Encoded>(),
        };
      }),
      { concurrency: 8 },
    );
    if (claim) {
      for (const { meta, state } of selected) {
        yield* writeMessage(store, { ...meta, claimedAt: now }, state);
      }
    }
    return loaded;
  });

  // Only due keys in the selected shard/address are visited. Promote every due
  // page before the versionstamp merge: stopping early could miss an older
  // message or make a short result falsely imply the mailbox was exhausted.
  const promoteDue = Effect.fnUntraced(function* (
    store: Store,
    index: Subspace,
    now: number,
  ) {
    const [begin] = yield* index.range();
    const [, end] = yield* index.range([now]);
    let cursor = begin;
    while (true) {
      const rows = yield* Stream.runCollect(
        store.transaction.getRange(keyRange(cursor, end, { limit: pageSize })),
      );
      const paths = yield* Effect.forEach(
        rows,
        (row) =>
          Effect.map(
            index.unpack(row.key, Schema.Tuple([Schema.Number, Schema.String])),
            ([, id]) => [id],
          ),
      );
      for (const meta of yield* messages.readMany(store, paths)) {
        if (meta !== undefined) {
          yield* messages.write(
            store,
            [meta.id],
            yield* setMessagePolling(store, meta, "ready"),
          );
        }
      }
      if (rows.length < pageSize) return;
      cursor = new Uint8Array([...rows[rows.length - 1].key, 0]);
    }
  });

  const readReplies = (ids: ReadonlyArray<string>, filtered: boolean) =>
    run(Effect.fnUntraced(function* (store) {
      const result: Array<Reply.Encoded> = [];
      for (const id of new Set(ids)) {
        yield* visitIndex(
          store,
          yield* store.spaces.replyIds.subspace([id]),
          Effect.fnUntraced(function* (replyId) {
            const meta = yield* replyMetadata.read(store, [
              id,
              replyId,
            ]);
            if (
              meta !== undefined &&
              (!filtered || meta.kind === "WithExit" || !meta.acked)
            ) {
              const reply = yield* replies.read(store, [id, replyId]);
              if (reply === undefined) {
                return yield* new MissingMessageRecordError({
                  record: "reply",
                  id: replyId,
                });
              }
              result.push(reply);
            }
            return true;
          }),
        );
      }
      return result;
    }));

  const resetIndex = (
    namespace: "messagesByAddress" | "messagesByShard",
    keys: ReadonlyArray<Tuple>,
  ) =>
    run(Effect.fnUntraced(function* (store) {
      for (const key of keys) {
        yield* visitIndex(
          store,
          yield* store.spaces[namespace].subspace(key),
          Effect.fnUntraced(function* (id) {
            const meta = yield* messages.read(store, [id]);
            if (
              meta !== undefined && !meta.processed && meta.claimedAt !== null
            ) {
              yield* writeMessage(store, {
                ...meta,
                claimedAt: null,
              }, yield* readState(store, meta.requestId));
            }
            return true;
          }),
        );
      }
    }));

  const encoded: MessageStorage.Encoded = {
    saveEnvelope: ({ envelope, primaryKey, deliverAt }) =>
      run(Effect.fnUntraced(function* (store) {
        const hash = primaryKey === null
          ? null
          : yield* primaryHash(primaryKey);
        const id = envelope._tag === "Request"
          ? envelope.requestId
          : envelope.id;
        const primary = hash === null
          ? undefined
          : yield* store.transaction.get(
            yield* store.spaces.primaryKeys.pack([hash]),
          );
        const originalId = primary === undefined ? id : decoder.decode(primary);
        const existing = yield* messages.read(store, [originalId]);
        if (existing !== undefined) {
          return MessageStorage.SaveResultEncoded.Duplicate({
            originalId: Snowflake.Snowflake(originalId),
            lastReceivedReply: yield* lastReply(
              store,
              existing.requestId,
              yield* readState(store, existing.requestId),
            ),
          });
        }
        // Late controls (or a request arriving after its reply) must not put
        // completed work back into the polling indexes. This read also conflicts
        // with a concurrent completion so both transitions stay atomic.
        let state = yield* readState(store, envelope.requestId);
        const meta: MessageMeta = {
          id,
          requestId: envelope.requestId,
          kind: envelope._tag,
          address: envelope.address,
          primaryKey: hash,
          deliverAt,
          claimedAt: null,
          processed: state.exitId !== null,
          polling: "blocked",
        };
        if (envelope._tag === "AckChunk") {
          const replyMeta = yield* replyMetadata.read(store, [
            envelope.requestId,
            envelope.replyId,
          ]);
          const acknowledge = replyMeta?.kind === "Chunk" && !replyMeta.acked;
          if (acknowledge) {
            yield* replyMetadata.write(store, [
              envelope.requestId,
              envelope.replyId,
            ], { ...replyMeta, acked: true });
          }
          state = {
            ...state,
            latestAckId: id,
            unacked: state.unacked - (acknowledge ? 1 : 0),
          };
          yield* states.write(store, [envelope.requestId], state);
        }
        yield* envelopes.write(store, [id], envelope);
        yield* insertMessageOrder(
          store,
          id,
          yield* readyIndexes(store.spaces, meta),
          false,
        );
        yield* writeMessage(store, meta, state);
        for (const index of yield* messageIndexes(store.spaces, meta)) {
          yield* store.transaction.set(
            yield* index.pack([BigInt(id)]),
            empty,
          );
        }
        if (envelope._tag === "AckChunk") {
          // ACKs can unblock all siblings and supersede an older ACK, including
          // controls at a different address/shard. Recompute both index families.
          yield* updateRequest(
            store,
            meta.requestId,
            (meta) => writeMessage(store, meta, state),
          );
        }
        if (hash !== null) {
          yield* store.transaction.set(
            yield* store.spaces.primaryKeys.pack([hash]),
            encoder.encode(id),
          );
        }
        return MessageStorage.SaveResultEncoded.Success();
      })),

    saveReply: (reply) =>
      run(Effect.fnUntraced(function* (store) {
        const id = reply.requestId;
        if (
          (yield* replyMetadata.read(store, [id, reply.id])) !==
            undefined
        ) return;
        const state = yield* readState(store, id);
        const sequence = reply._tag === "Chunk" ? reply.sequence : null;
        const unique = yield* store.spaces.uniqueReplies.pack([
          id,
          sequence === null ? "exit" : BigInt(sequence),
        ]);
        if ((yield* store.transaction.get(unique)) !== undefined) {
          return yield* new DuplicateReplyError({
            requestId: id,
            replyId: reply.id,
            sequence,
          });
        }
        yield* replies.write(store, [id, reply.id], reply);
        yield* replyMetadata.write(store, [id, reply.id], {
          id: reply.id,
          kind: reply._tag,
          sequence,
          acked: false,
        });
        yield* store.transaction.set(
          yield* store.spaces.replyIds.pack([id, BigInt(reply.id)]),
          empty,
        );
        yield* store.transaction.set(unique, encoder.encode(reply.id));
        const nextState: RequestState = {
          ...state,
          lastReplyId: reply.id,
          exitId: reply._tag === "WithExit" ? reply.id : state.exitId,
          unacked: state.unacked + (reply._tag === "Chunk" ? 1 : 0),
        };
        yield* states.write(store, [id], nextState);
        if (reply._tag === "WithExit" || state.unacked === 0) {
          yield* updateRequest(
            store,
            id,
            (meta) =>
              writeMessage(store, {
                ...meta,
                processed: meta.processed || reply._tag === "WithExit",
              }, nextState),
          );
        }
      })),

    // Match SQL storage: retain streaming history, remove exits; unconditional
    // clears also discard interrupts, whereas a CAS clear preserves them.
    clearReplies: (requestId, options) =>
      run(Effect.fnUntraced(function* (store) {
        const id = String(requestId);
        const state = yield* readState(store, id);
        if (
          options?.expectedReplyId !== undefined &&
          state.lastReplyId !== String(options.expectedReplyId)
        ) return;
        if (state.exitId !== null) {
          yield* replies.clear(store, [id, state.exitId]);
          yield* replyMetadata.clear(store, [id, state.exitId]);
          yield* store.transaction.clear(
            yield* store.spaces.replyIds.pack([id, BigInt(state.exitId)]),
          );
          yield* store.transaction.clear(
            yield* store.spaces.uniqueReplies.pack([id, "exit"]),
          );
        }
        const nextState: RequestState = {
          ...state,
          lastReplyId: null,
          exitId: null,
        };
        yield* states.write(store, [id], nextState);
        yield* updateRequest(
          store,
          id,
          Effect.fnUntraced(function* (meta) {
            if (
              meta.kind === "Interrupt" &&
              options?.expectedReplyId === undefined
            ) {
              return yield* removeMessage(store, meta);
            }
            yield* writeMessage(store, {
              ...meta,
              processed: false,
              claimedAt: null,
            }, nextState);
          }),
        );
      })),

    requestIdForPrimaryKey: (primaryKey) =>
      run(Effect.fnUntraced(function* (store) {
        const bytes = yield* store.transaction.get(
          yield* store.spaces.primaryKeys.pack([
            yield* primaryHash(primaryKey),
          ]),
        );
        return bytes === undefined
          ? Option.none()
          : Option.some(Snowflake.Snowflake(decoder.decode(bytes)));
      })),
    repliesFor: (ids) => readReplies(ids, true),
    repliesForUnfiltered: (ids) => readReplies(ids, false),
    unprocessedMessages: (shards, now, options) =>
      run(Effect.fnUntraced(function* (store) {
        const result: Array<
          {
            envelope: Envelope.Encoded;
            lastSentReply: Option.Option<Reply.Encoded>;
          }
        > = [];
        const limit = options?.limit === undefined
          ? Infinity
          : Math.max(0, Math.floor(options.limit));
        if (!(limit > 0)) return result;
        const selectors: ReadonlyArray<Tuple> = options?.addresses === undefined
          ? [...new Set(shards)].map((shard) => [shard])
          : options.addresses.filter((address) =>
            shards.includes(ShardId.toString(address.shardId))
          ).map(addressKey);
        const ready = options?.addresses === undefined
          ? store.spaces.readyByShard
          : store.spaces.readyByAddress;
        const scheduled = options?.addresses === undefined
          ? store.spaces.scheduledByShard
          : store.spaces.scheduledByAddress;
        const indexes = yield* Effect.forEach(
          selectors,
          (key) => ready.subspace(key),
        );
        for (const key of selectors) {
          yield* promoteDue(store, yield* scheduled.subspace(key), now);
        }
        yield* visitMessageOrder(
          store,
          indexes,
          Effect.fnUntraced(function* (ids) {
            result.push(
              ...yield* readMessages(
                store,
                ids,
                now,
                true,
                limit - result.length,
              ),
            );
            return result.length < limit;
          }),
        );
        return result;
      })),
    unprocessedMessagesById: (ids, now) =>
      run(Effect.fnUntraced(function* (store) {
        const result = [];
        const unique = [...new Set(ids)].map(String);
        for (let offset = 0; offset < unique.length; offset += pageSize) {
          const batch = yield* readMessages(
            store,
            unique.slice(offset, offset + pageSize),
            now,
            false,
            Infinity,
          );
          for (const message of batch) {
            const envelope = message.envelope;
            const id = envelope._tag === "Request"
              ? envelope.requestId
              : envelope.id;
            result.push({
              message,
              stamp: yield* messageOrder(store, id),
            });
          }
        }
        result.sort((left, right) =>
          compareBytes(left.stamp.bytes, right.stamp.bytes)
        );
        return result.map(({ message }) => message);
      })),
    resetRequests: (ids) =>
      run(Effect.fnUntraced(function* (store) {
        for (const id of ids) {
          const meta = yield* messages.read(store, [String(id)]);
          if (meta !== undefined && !meta.processed) {
            yield* writeMessage(store, {
              ...meta,
              claimedAt: null,
            }, yield* readState(store, meta.requestId));
          }
        }
      })),
    resetAddresses: (addresses) =>
      resetIndex("messagesByAddress", addresses.map(addressKey)),
    resetShards: (shards) =>
      resetIndex("messagesByShard", shards.map((shard) => [shard])),
    clearAddress: (address: EntityAddress.EntityAddress) =>
      run(Effect.fnUntraced(function* (store) {
        // Like upstream, entity identity is independent of its current shard.
        yield* visitIndex(
          store,
          yield* store.spaces.messagesByAddress.subspace([
            address.entityType,
            address.entityId,
          ]),
          Effect.fnUntraced(function* (id) {
            const meta = yield* messages.read(store, [id]);
            if (meta === undefined) {
              return true;
            }
            if (meta.kind === "Request") {
              yield* replies.clear(store, [id]);
              yield* store.transaction.clearRange(
                ...yield* store.spaces.replyMetadata.range([id]),
              );
              yield* states.clear(store, [id]);
              yield* store.transaction.clearRange(
                ...yield* store.spaces.replyIds.range([id]),
              );
              yield* store.transaction.clearRange(
                ...yield* store.spaces.uniqueReplies.range([id]),
              );
            }
            yield* removeMessage(store, meta);
            return true;
          }),
        );
      })),
    // The upstream signature cannot expose commit errors. As with SQL storage,
    // transaction infrastructure errors defect; user failures still roll back.
    withTransaction: (effect) =>
      transact(effect).pipe(Effect.catchIf(isFoundationDbError, Effect.die)),
  };
  return encoded;
});

/** Creates the decoded adapter, using the caller's Snowflake.Generator. */
export const makeMessageStorage = (options?: MessageStorageOptions) =>
  Effect.flatMap(
    makeEncodedMessageStorage(options),
    MessageStorage.makeEncoded,
  );

/** Provides the mailbox and the standard Snowflake generator. */
export const layerMessageStorage = (options?: MessageStorageOptions) =>
  Layer.effect(MessageStorage.MessageStorage, makeMessageStorage(options)).pipe(
    Layer.provide(Snowflake.layerGenerator),
  );
