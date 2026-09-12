/** Durable FoundationDB repository for Effect's persisted queue. */
import {
  Cause,
  Clock,
  Duration,
  Effect,
  Exit,
  Predicate,
  Schema,
  Stream,
} from "effect";
import { DirectoryLayer, DirectorySubspace } from "../directory/mod.ts";
import type { DirectoryOutput } from "../directory/mod.ts";
import { FoundationDb, FoundationDbTransaction } from "../FoundationDb.ts";
import type { FoundationDbFuture } from "../FoundationDb.ts";
import {
  clearChunkedValue,
  readChunkedValue,
  writeChunkedValue,
} from "../internal/chunked-value.ts";
import { keyRange, MutationType } from "../model.ts";
import type { Bytes, KeyValue, TransactionOptions } from "../model.ts";
import { pack, Subspace, unpack } from "../tuple/mod.ts";
import type { TupleError } from "../tuple/mod.ts";
import type { PersistedQueueStoreOptions } from "./model.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const defaultDirectoryPath = ["effect-foundationdb", "persisted-queue"];
const storeLayer = encoder.encode("effect-foundationdb/persisted-queue");
const queueLayer = encoder.encode("effect-foundationdb/persisted-queue/queue");
const indexBatchSize = 100;
const deadLetterTag = "~effect/persistence/PersistedQueue/DeadLetter";
const signalIncrement = new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]);

const QueueEntryMetadataFields = {
  id: Schema.String,
  attempts: Schema.Int,
  state: Schema.Literals(["pending", "processing", "completed", "failed"]),
  visibleAt: Schema.Number,
  acquiredAt: Schema.NullOr(Schema.Number),
  acquiredBy: Schema.NullOr(Schema.String),
  updatedAt: Schema.Number,
} as const;
const QueueEntrySchema = Schema.Struct({
  ...QueueEntryMetadataFields,
  element: Schema.Json,
});
const QueueEntryMetadataSchema = Schema.Struct(QueueEntryMetadataFields);
const QueueEntryMetadataJson = Schema.fromJsonString(QueueEntryMetadataSchema);
const QueueElementJson = Schema.fromJsonString(Schema.Json);
const decodeQueueEntryMetadata = Schema.decodeUnknownEffect(
  QueueEntryMetadataJson,
);
const encodeQueueEntryMetadata = Schema.encodeUnknownEffect(
  QueueEntryMetadataJson,
);
const decodeQueueElement = Schema.decodeUnknownEffect(QueueElementJson);
const encodeQueueElement = Schema.encodeUnknownEffect(QueueElementJson);
const decodeJson = Schema.decodeUnknownEffect(Schema.Json);

type QueueEntry = typeof QueueEntrySchema.Type;
type QueueEntryMetadata = typeof QueueEntryMetadataSchema.Type;
type QueueState = QueueEntry["state"];

class QueueStoreFailure extends Schema.TaggedError<QueueStoreFailure>()(
  "QueueStoreFailure",
  { reason: Schema.String },
) {}

interface QueueKeys {
  readonly counterKey: Uint8Array;
  readonly signalKey: Uint8Array;
  readonly entries: Subspace;
  readonly ids: Subspace;
  readonly pending: Subspace;
  readonly locks: Subspace;
  readonly completed: Subspace;
  readonly failed: Subspace;
  readonly claims: Subspace;
}

interface ClaimedEntry {
  readonly claimId: string;
  readonly sequence: bigint;
  readonly entry: QueueEntry;
}

interface ClaimWait {
  readonly _tag: "ClaimWait";
  readonly wake: FoundationDbFuture<void>;
}

const encodeSequence = (value: bigint): Effect.Effect<Uint8Array, TupleError> =>
  pack([value]);

const decodeSequence = (
  value: Bytes,
): Effect.Effect<bigint, TupleError | QueueStoreFailure> =>
  unpack(value).pipe(
    Effect.flatMap((tuple) =>
      tuple.length === 1 && typeof tuple[0] === "bigint" && tuple[0] >= 0n
        ? Effect.succeed(tuple[0])
        : Effect.fail(
          new QueueStoreFailure({
            reason: "expected one non-negative tuple integer",
          }),
        )
    ),
  );

const timestamp = (value: number): bigint =>
  BigInt(Math.max(
    0,
    Math.min(Number.MAX_SAFE_INTEGER, Math.floor(value)),
  ));

const hasDeadLetter = (cause: Cause.Cause<unknown>): boolean => {
  for (const reason of cause.reasons) {
    if (
      Cause.isFailReason(reason) &&
      Predicate.isTagged(reason.error, deadLetterTag)
    ) {
      return true;
    }
  }
  return false;
};

const regularDirectory = (
  output: DirectoryOutput,
): Effect.Effect<DirectorySubspace, QueueStoreFailure> =>
  output instanceof DirectorySubspace ? Effect.succeed(output) : Effect.fail(
    new QueueStoreFailure({
      reason: "persisted queue directory cannot be a partition",
    }),
  );

const queueKeys = Effect.fnUntraced(function* (
  directory: DirectorySubspace,
) {
  return {
    counterKey: yield* directory.pack(["counter"]),
    signalKey: yield* directory.pack(["signal"]),
    entries: yield* directory.subspace(["entries"]),
    ids: yield* directory.subspace(["ids"]),
    pending: yield* directory.subspace(["pending"]),
    locks: yield* directory.subspace(["locks"]),
    completed: yield* directory.subspace(["completed"]),
    failed: yield* directory.subspace(["failed"]),
    claims: yield* directory.subspace(["claims"]),
  } satisfies QueueKeys;
});

const decodeUtf8 = (value: Uint8Array) =>
  Effect.try({
    try: () => decoder.decode(value),
    catch: (cause) =>
      new QueueStoreFailure({ reason: `invalid UTF-8: ${String(cause)}` }),
  });

const readEntryMetadata = Effect.fnUntraced(function* (
  entries: Subspace,
  sequence: bigint,
): Effect.fn.Return<
  QueueEntryMetadata | undefined,
  unknown,
  FoundationDbTransaction
> {
  const transaction = yield* FoundationDbTransaction;
  const metadataKey = yield* entries.pack([sequence, "metadata"]);
  const encoded = yield* transaction.get(metadataKey);
  if (encoded === undefined) {
    return undefined;
  }
  return yield* decodeQueueEntryMetadata(yield* decodeUtf8(encoded));
});

const readEntry = Effect.fnUntraced(function* (
  entries: Subspace,
  sequence: bigint,
): Effect.fn.Return<
  QueueEntry | undefined,
  unknown,
  FoundationDbTransaction
> {
  const transaction = yield* FoundationDbTransaction;
  const [metadata, encodedElement] = yield* Effect.all([
    readEntryMetadata(entries, sequence),
    readChunkedValue(transaction, entries, [sequence, "element"]),
  ]);
  if (metadata === undefined && encodedElement === undefined) {
    return undefined;
  }
  if (metadata === undefined || encodedElement === undefined) {
    return yield* new QueueStoreFailure({
      reason: "incomplete persisted queue entry",
    });
  }
  const element = yield* decodeQueueElement(yield* decodeUtf8(encodedElement));
  return { ...metadata, element };
});

const writeEntryMetadata = Effect.fnUntraced(function* (
  entries: Subspace,
  sequence: bigint,
  entry: QueueEntryMetadata,
): Effect.fn.Return<void, unknown, FoundationDbTransaction> {
  const transaction = yield* FoundationDbTransaction;
  const metadata = yield* encodeQueueEntryMetadata(entry);
  yield* transaction.set(
    yield* entries.pack([sequence, "metadata"]),
    encoder.encode(metadata),
  );
});

const createEntry = Effect.fnUntraced(function* (
  entries: Subspace,
  sequence: bigint,
  entry: QueueEntry,
): Effect.fn.Return<void, unknown, FoundationDbTransaction> {
  const transaction = yield* FoundationDbTransaction;
  yield* writeEntryMetadata(entries, sequence, entry);
  yield* writeChunkedValue(
    transaction,
    entries,
    [sequence, "element"],
    encoder.encode(yield* encodeQueueElement(entry.element)),
  );
});

const clearEntry = Effect.fnUntraced(function* (
  entries: Subspace,
  sequence: bigint,
): Effect.fn.Return<void, unknown, FoundationDbTransaction> {
  const transaction = yield* FoundationDbTransaction;
  yield* clearChunkedValue(transaction, entries, [sequence]);
});

const indexBefore = Effect.fnUntraced(function* (
  subspace: Subspace,
  timestamp: number,
): Effect.fn.Return<
  ReadonlyArray<KeyValue>,
  unknown,
  FoundationDbTransaction
> {
  const transaction = yield* FoundationDbTransaction;
  const [begin, rangeEnd] = yield* subspace.range();
  const end = timestamp < 0
    ? begin
    : timestamp >= Number.MAX_SAFE_INTEGER
    ? rangeEnd
    : yield* subspace.pack([BigInt(Math.floor(timestamp)) + 1n]);
  return yield* Stream.runCollect(transaction.getRange(keyRange(
    begin,
    end,
    { limit: indexBatchSize },
  )));
});

const addDelay = (now: number, delay: Duration.Duration): number => {
  const milliseconds = Duration.toMillis(delay);
  return Number.isFinite(milliseconds)
    ? Math.ceil(
      Math.min(Number.MAX_SAFE_INTEGER, now + Math.max(0, milliseconds)),
    )
    : Number.MAX_SAFE_INTEGER;
};

export const makeQueueRepository = Effect.fnUntraced(function* (
  options: PersistedQueueStoreOptions = {},
) {
  const database = yield* FoundationDb;
  const clock = yield* Clock.Clock;
  const directory = options.directory ??
    (yield* DirectoryLayer.make().pipe(Effect.orDie));
  const directoryPath = (options.directoryPath ?? defaultDirectoryPath).slice();
  const lockExpiration = Duration.max(
    Duration.fromInputUnsafe(options.lockExpiration ?? "2 minutes"),
    Duration.millis(1),
  );
  const transactionOptions: TransactionOptions = {
    ...options.transactionOptions,
    timeoutMs: options.transactionOptions?.timeoutMs ?? 5_000,
    retryLimit: options.transactionOptions?.retryLimit ?? 10,
    maxRetryDelayMs: options.transactionOptions?.maxRetryDelayMs ?? 1_000,
  };
  const rootDirectory = Effect.fnUntraced(function* () {
    return yield* regularDirectory(
      yield* directory.createOrOpen(
        directoryPath,
        { layer: storeLayer },
      ),
    );
  });
  const keysForQueue = Effect.fnUntraced(function* (name: string) {
    const root = yield* rootDirectory();
    const queue = yield* regularDirectory(
      yield* root.createOrOpen(
        [name],
        { layer: queueLayer },
      ),
    );
    return yield* queueKeys(queue);
  });

  const offer = Effect.fnUntraced(function* (offerOptions: {
    readonly name: string;
    readonly id: string;
    readonly element: unknown;
  }) {
    const element = yield* decodeJson(offerOptions.element);
    const now = clock.currentTimeMillisUnsafe();
    yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const keys = yield* keysForQueue(offerOptions.name);
        const idKey = yield* keys.ids.pack([offerOptions.id]);
        if ((yield* transaction.get(idKey)) !== undefined) {
          return;
        }
        const encodedSequence = yield* transaction.get(keys.counterKey);
        const sequence = encodedSequence === undefined
          ? 0n
          : yield* decodeSequence(encodedSequence);
        const entry: QueueEntry = {
          id: offerOptions.id,
          element,
          attempts: 0,
          state: "pending",
          visibleAt: now,
          acquiredAt: null,
          acquiredBy: null,
          updatedAt: now,
        };
        const sequenceValue = yield* encodeSequence(sequence);
        yield* createEntry(keys.entries, sequence, entry);
        yield* transaction.set(idKey, sequenceValue);
        yield* transaction.set(
          yield* keys.pending.pack([timestamp(now), sequence]),
          sequenceValue,
        );
        yield* transaction.set(
          keys.counterKey,
          yield* encodeSequence(sequence + 1n),
        );
        yield* transaction.atomicOp(
          keys.signalKey,
          signalIncrement,
          MutationType.Add,
        );
      }),
      transactionOptions,
    );
  });

  const claim = Effect.fnUntraced(function* (
    name: string,
    claimId: string,
    maxAttempts: number,
  ) {
    const now = clock.currentTimeMillisUnsafe();
    return yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const keys = yield* keysForQueue(name);
        const claimKey = yield* keys.claims.pack([claimId]);
        const previousClaim = yield* transaction.get(claimKey);
        if (previousClaim !== undefined) {
          const sequence = yield* decodeSequence(previousClaim);
          const entry = yield* readEntry(keys.entries, sequence);
          if (
            entry !== undefined && entry.state === "processing" &&
            entry.acquiredBy === claimId
          ) {
            return { claimId, sequence, entry } satisfies ClaimedEntry;
          }
          yield* transaction.clear(claimKey);
        }

        const expiredAt = now - Duration.toMillis(lockExpiration);
        const expired = yield* indexBefore(keys.locks, expiredAt);
        let progressed = expired.length > 0;
        let recoveredPending = false;
        for (const lock of expired) {
          const sequence = yield* decodeSequence(lock.value);
          const entry = yield* readEntryMetadata(keys.entries, sequence);
          if (
            entry === undefined || entry.state !== "processing" ||
            entry.acquiredAt === null ||
            entry.acquiredAt > expiredAt
          ) {
            yield* transaction.clear(lock.key);
            continue;
          }
          yield* transaction.clear(lock.key);
          if (entry.acquiredBy !== null) {
            yield* transaction.clear(
              yield* keys.claims.pack([entry.acquiredBy]),
            );
          }
          const recovered: QueueEntryMetadata = entry.attempts >= maxAttempts
            ? {
              ...entry,
              state: "failed",
              acquiredAt: null,
              acquiredBy: null,
              updatedAt: now,
            }
            : {
              ...entry,
              state: "pending",
              visibleAt: now,
              acquiredAt: null,
              acquiredBy: null,
              updatedAt: now,
            };
          yield* writeEntryMetadata(keys.entries, sequence, recovered);
          yield* transaction.set(
            yield* (recovered.state === "failed" ? keys.failed : keys.pending)
              .pack([timestamp(now), sequence]),
            yield* encodeSequence(sequence),
          );
          recoveredPending = recoveredPending || recovered.state === "pending";
        }
        if (recoveredPending) {
          yield* transaction.atomicOp(
            keys.signalKey,
            signalIncrement,
            MutationType.Add,
          );
        }

        const pending = yield* indexBefore(keys.pending, now);
        progressed = progressed || pending.length > 0;
        for (const candidate of pending) {
          const sequence = yield* decodeSequence(candidate.value);
          const entry = yield* readEntry(keys.entries, sequence);
          if (entry === undefined || entry.state !== "pending") {
            yield* transaction.clear(candidate.key);
            continue;
          }
          if (entry.visibleAt > now) {
            yield* transaction.clear(candidate.key);
            yield* transaction.set(
              yield* keys.pending.pack([
                timestamp(Math.ceil(entry.visibleAt)),
                sequence,
              ]),
              yield* encodeSequence(sequence),
            );
            continue;
          }
          if (entry.attempts >= maxAttempts) {
            const failed: QueueEntry = {
              ...entry,
              state: "failed",
              updatedAt: now,
            };
            yield* transaction.clear(candidate.key);
            yield* writeEntryMetadata(keys.entries, sequence, failed);
            yield* transaction.set(
              yield* keys.failed.pack([timestamp(now), sequence]),
              yield* encodeSequence(sequence),
            );
            continue;
          }

          const processing: QueueEntry = {
            ...entry,
            attempts: entry.attempts + 1,
            state: "processing",
            acquiredAt: now,
            acquiredBy: claimId,
            updatedAt: now,
          };
          yield* transaction.clear(candidate.key);
          yield* writeEntryMetadata(keys.entries, sequence, processing);
          yield* transaction.set(
            yield* keys.locks.pack([timestamp(now), sequence]),
            yield* encodeSequence(sequence),
          );
          yield* transaction.set(
            claimKey,
            yield* encodeSequence(sequence),
          );
          return {
            claimId,
            sequence,
            entry: processing,
          } satisfies ClaimedEntry;
        }
        if (progressed) {
          return true;
        }
        return {
          _tag: "ClaimWait",
          wake: yield* transaction.watch(keys.signalKey),
        } satisfies ClaimWait;
      }),
      transactionOptions,
    );
  });

  const refreshClaim = Effect.fnUntraced(function* (
    name: string,
    claimed: ClaimedEntry,
  ) {
    const now = clock.currentTimeMillisUnsafe();
    yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const keys = yield* keysForQueue(name);
        const entry = yield* readEntryMetadata(keys.entries, claimed.sequence);
        if (
          entry === undefined || entry.state !== "processing" ||
          entry.acquiredBy !== claimed.claimId || entry.acquiredAt === null
        ) {
          return;
        }
        yield* transaction.clear(
          yield* keys.locks.pack([
            timestamp(entry.acquiredAt),
            claimed.sequence,
          ]),
        );
        yield* writeEntryMetadata(keys.entries, claimed.sequence, {
          ...entry,
          acquiredAt: now,
        });
        yield* transaction.set(
          yield* keys.locks.pack([timestamp(now), claimed.sequence]),
          yield* encodeSequence(claimed.sequence),
        );
      }),
      transactionOptions,
    );
  });

  const finalizeClaim = Effect.fnUntraced(function* (
    name: string,
    claimed: ClaimedEntry,
    maxAttempts: number,
    retryDelay: (attempts: number) => Effect.Effect<Duration.Duration>,
    exit: Exit.Exit<unknown, unknown>,
  ) {
    const now = clock.currentTimeMillisUnsafe();
    let state: QueueState;
    let visibleAt = now;
    let decrementAttempt = false;
    if (exit._tag === "Success") {
      state = "completed";
    } else if (hasDeadLetter(exit.cause)) {
      state = "failed";
    } else if (Cause.hasInterruptsOnly(exit.cause)) {
      state = "pending";
      decrementAttempt = true;
    } else if (claimed.entry.attempts >= maxAttempts) {
      state = "failed";
    } else {
      state = "pending";
      visibleAt = addDelay(now, yield* retryDelay(claimed.entry.attempts));
    }

    yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const keys = yield* keysForQueue(name);
        const entry = yield* readEntryMetadata(keys.entries, claimed.sequence);
        if (
          entry === undefined || entry.state !== "processing" ||
          entry.acquiredBy !== claimed.claimId || entry.acquiredAt === null
        ) {
          return;
        }
        yield* transaction.clear(
          yield* keys.locks.pack([
            timestamp(entry.acquiredAt),
            claimed.sequence,
          ]),
        );
        yield* transaction.clear(
          yield* keys.claims.pack([claimed.claimId]),
        );
        const updated: QueueEntryMetadata = {
          ...entry,
          attempts: decrementAttempt
            ? Math.max(0, entry.attempts - 1)
            : entry.attempts,
          state,
          visibleAt,
          acquiredAt: null,
          acquiredBy: null,
          updatedAt: now,
        };
        yield* writeEntryMetadata(keys.entries, claimed.sequence, updated);
        if (state === "pending") {
          yield* transaction.set(
            yield* keys.pending.pack([
              timestamp(visibleAt),
              claimed.sequence,
            ]),
            yield* encodeSequence(claimed.sequence),
          );
          yield* transaction.atomicOp(
            keys.signalKey,
            signalIncrement,
            MutationType.Add,
          );
        } else {
          yield* transaction.set(
            yield* (state === "completed" ? keys.completed : keys.failed).pack([
              timestamp(now),
              claimed.sequence,
            ]),
            yield* encodeSequence(claimed.sequence),
          );
        }
      }),
      transactionOptions,
    );
    return state;
  });

  const cleanupState = Effect.fnUntraced(function* (
    name: string,
    state: "completed" | "failed",
    cutoff: number,
  ) {
    while (true) {
      const count = yield* database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const keys = yield* keysForQueue(name);
          const index = state === "completed" ? keys.completed : keys.failed;
          const entries = yield* indexBefore(index, cutoff);
          for (const indexed of entries) {
            const sequence = yield* decodeSequence(indexed.value);
            const entry = yield* readEntryMetadata(keys.entries, sequence);
            if (
              entry !== undefined && entry.state === state &&
              entry.updatedAt <= cutoff
            ) {
              yield* clearEntry(keys.entries, sequence);
              yield* transaction.clear(yield* keys.ids.pack([entry.id]));
            }
            yield* transaction.clear(indexed.key);
          }
          return entries.length;
        }),
        transactionOptions,
      );
      if (count < indexBatchSize) {
        return;
      }
      yield* Effect.yieldNow;
    }
  });

  const cleanup = Effect.fnUntraced(function* (cleanupOptions: {
    readonly timeToLive: Duration.Duration;
    readonly failedTimeToLive: Duration.Duration | undefined;
  }) {
    const now = clock.currentTimeMillisUnsafe();
    const queues = yield* database.withTransaction(
      Effect.flatMap(rootDirectory(), (root) => root.list()),
      transactionOptions,
    );
    for (const name of queues) {
      yield* cleanupState(
        name,
        "completed",
        now - Duration.toMillis(cleanupOptions.timeToLive),
      );
      if (cleanupOptions.failedTimeToLive !== undefined) {
        yield* cleanupState(
          name,
          "failed",
          now - Duration.toMillis(cleanupOptions.failedTimeToLive),
        );
      }
    }
  });

  return { offer, claim, refreshClaim, finalizeClaim, cleanup } as const;
});
