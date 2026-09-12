/** Effect BackingPersistence backed by FoundationDB. */
import {
  Clock,
  Duration,
  Effect,
  Layer,
  Schedule,
  Stream,
  SynchronizedRef,
} from "effect";
import { Persistence } from "effect/unstable/persistence";
import { FoundationDb, FoundationDbTransaction } from "../FoundationDb.ts";
import {
  clearChunkedValue,
  readChunkedValue,
  splitChunkedValue,
} from "../internal/chunked-value.ts";
import { keyRange } from "../model.ts";
import type { Bytes } from "../model.ts";
import type { Subspace } from "../tuple/mod.ts";
import { makeDirectoryStore } from "./internal.ts";
import type { BackingPersistenceOptions } from "./model.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const emptyValue = new Uint8Array();
const defaultDirectoryPath = ["effect-foundationdb", "persistence"];
const cleanupBatchSize = 100;
const cleanupInterval = Duration.minutes(5);
const cleanupBatchDelay = Duration.millis(10);
const stagingGarbageDelay = Duration.hours(24);
const transactionByteBudget = 8_000_000;
const operationByteOverhead = 64;
const rangeReadConcurrency = 16;

interface EntryManifest {
  readonly identity: string;
  readonly expires: number | null;
}

interface PersistenceKeys {
  readonly entries: Subspace;
  readonly values: Subspace;
  readonly expires: Subspace;
  readonly garbage: Subspace;
  readonly metadata: Subspace;
}

interface StoreEpoch {
  readonly value: Uint8Array | undefined;
}

interface Mutation {
  readonly key: Uint8Array;
  readonly value: Uint8Array;
}

interface PreparedEntry {
  readonly key: string;
  readonly identity: string;
  readonly expires: number | null;
  readonly manifestKey: Uint8Array;
  readonly manifestValue: Uint8Array;
  readonly expirationKey: Uint8Array | undefined;
  readonly identityValue: Uint8Array;
  readonly stagingGarbageKey: Uint8Array;
  readonly chunks: ReadonlyArray<Mutation>;
  readonly directByteEstimate: number;
  readonly stagedPublicationByteEstimate: number;
}

type WriteEntry = readonly [
  key: string,
  value: object,
  ttl: Duration.Duration | undefined,
];

class StoreEpochChanged {
  readonly _tag = "StoreEpochChanged" as const;
}

const persistenceError = (
  method: string,
  key: string | undefined,
  cause: unknown,
) =>
  cause instanceof Persistence.PersistenceError
    ? cause
    : new Persistence.PersistenceError({
      message: key === undefined
        ? `FoundationDB persistence ${method} failed`
        : `FoundationDB persistence ${method} failed for key ${key}`,
      cause,
    });

const transactionLimitError = () =>
  new Persistence.PersistenceError({
    message:
      "FoundationDB persistence batch metadata exceeds the safe transaction size",
  });

const timestamp = (value: number): bigint =>
  BigInt(Math.max(
    0,
    Math.min(Number.MAX_SAFE_INTEGER, Math.floor(value)),
  ));

const equalBytes = (
  left: Bytes | undefined,
  right: Bytes | undefined,
): boolean => {
  if (left === undefined || right === undefined) {
    return left === right;
  }
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let index = 0; index < left.byteLength; index++) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
};

const mutationBytes = (mutation: Mutation): number =>
  mutation.key.byteLength + mutation.value.byteLength + operationByteOverhead;

const clearBytes = (key: Bytes): number =>
  key.byteLength + operationByteOverhead;

const readBytes = (key: Bytes): number =>
  key.byteLength * 2 + operationByteOverhead;

const encodeValue = (
  key: string,
  value: object,
): Effect.Effect<Uint8Array, Persistence.PersistenceError> =>
  Effect.try({
    try: () => {
      const json = JSON.stringify(value);
      if (json === undefined) {
        throw new Error("persisted value is not JSON serializable");
      }
      return encoder.encode(json);
    },
    catch: (cause) => persistenceError("set", key, cause),
  });

const decodeValue = (
  key: string,
  encoded: Uint8Array,
): Effect.Effect<object, Persistence.PersistenceError> =>
  Effect.try({
    try: () => {
      const parsed: unknown = JSON.parse(decoder.decode(encoded));
      if (parsed === null || typeof parsed !== "object") {
        throw new Error("invalid persisted value");
      }
      return parsed;
    },
    catch: (cause) => persistenceError("get", key, cause),
  });

const encodeManifest = (manifest: EntryManifest): Uint8Array =>
  encoder.encode(JSON.stringify([manifest.identity, manifest.expires]));

const decodeManifest = (
  key: string,
  encoded: Uint8Array,
): Effect.Effect<EntryManifest, Persistence.PersistenceError> =>
  Effect.try({
    try: () => {
      const parsed: unknown = JSON.parse(decoder.decode(encoded));
      if (
        !Array.isArray(parsed) || parsed.length !== 2 ||
        typeof parsed[0] !== "string" || parsed[0].length === 0 ||
        (parsed[1] !== null &&
          (typeof parsed[1] !== "number" || !Number.isFinite(parsed[1])))
      ) {
        throw new Error("invalid persisted entry manifest");
      }
      return { identity: parsed[0], expires: parsed[1] };
    },
    catch: (cause) => persistenceError("get", key, cause),
  });

const expirationFor = (
  ttl: Duration.Duration | undefined,
  now: number,
): number | null =>
  ttl === undefined || !Duration.isFinite(ttl) ? null : Math.ceil(Math.min(
    Number.MAX_SAFE_INTEGER,
    now + Math.max(0, Duration.toMillis(ttl)),
  ));

const latestEntries = (
  entries: ReadonlyArray<WriteEntry>,
): ReadonlyArray<WriteEntry> => {
  const latest = new Map<string, WriteEntry>();
  for (const entry of entries) {
    latest.set(entry[0], entry);
  }
  return Array.from(latest.values());
};

const sameManifest = (
  manifest: EntryManifest | undefined,
  entry: PreparedEntry,
): boolean =>
  manifest?.identity === entry.identity && manifest.expires === entry.expires;

/** Creates Effect's BackingPersistence service using FoundationDB. */
export const makeBackingPersistence = Effect.fnUntraced(function* (
  options: BackingPersistenceOptions = {},
) {
  const database = yield* FoundationDb;
  const clock = yield* Clock.Clock;
  const { root, transactionOptions } = yield* makeDirectoryStore(
    options,
    defaultDirectoryPath,
    "effect-foundationdb/persistence",
  );
  const keyCache = SynchronizedRef.makeUnsafe<PersistenceKeys | undefined>(
    undefined,
  );

  const initializeKeys = Effect.fnUntraced(function* () {
    const directory = yield* root();
    return {
      entries: yield* directory.subspace(["entries"]),
      values: yield* directory.subspace(["values"]),
      expires: yield* directory.subspace(["expires"]),
      garbage: yield* directory.subspace(["garbage"]),
      metadata: yield* directory.subspace(["metadata"]),
    } satisfies PersistenceKeys;
  });

  const keys = Effect.fnUntraced(function* () {
    const cached = SynchronizedRef.getUnsafe(keyCache);
    if (cached !== undefined) {
      return cached;
    }
    return yield* SynchronizedRef.modifyEffect(
      keyCache,
      (current) =>
        current !== undefined
          ? Effect.succeed([current, current] as const)
          : database.withTransaction(
            initializeKeys(),
            transactionOptions,
          ).pipe(
            Effect.map((initialized) => [initialized, initialized] as const),
          ),
    );
  });

  const epochKey = (
    storeKeys: PersistenceKeys,
    storeId: string,
  ) => storeKeys.metadata.pack([storeId, "epoch"]);

  const clearExpiration = Effect.fnUntraced(function* (
    transaction: FoundationDbTransaction["Service"],
    storeKeys: PersistenceKeys,
    storeId: string,
    key: string,
    expires: number | null,
  ) {
    if (expires !== null) {
      yield* transaction.clear(
        yield* storeKeys.expires.pack([storeId, timestamp(expires), key]),
      );
    }
  });

  const clearStoredValue = Effect.fnUntraced(function* (
    transaction: FoundationDbTransaction["Service"],
    storeKeys: PersistenceKeys,
    storeId: string,
    identity: string,
  ) {
    yield* clearChunkedValue(
      transaction,
      storeKeys.values,
      [storeId, identity],
    );
  });

  const readManifest = Effect.fnUntraced(function* (
    transaction: FoundationDbTransaction["Service"],
    storeKeys: PersistenceKeys,
    storeId: string,
    key: string,
  ) {
    const encoded = yield* transaction.get(
      yield* storeKeys.entries.pack([storeId, key]),
    );
    return encoded === undefined
      ? undefined
      : yield* decodeManifest(key, encoded);
  });

  const readStoredValue = Effect.fnUntraced(function* (
    transaction: FoundationDbTransaction["Service"],
    storeKeys: PersistenceKeys,
    storeId: string,
    key: string,
    manifest: EntryManifest,
  ) {
    const encoded = yield* readChunkedValue(
      transaction,
      storeKeys.values,
      [storeId, manifest.identity],
    );
    if (encoded === undefined) {
      return yield* new Persistence.PersistenceError({
        message: `Incomplete FoundationDB persistence value for key ${key}`,
      });
    }
    return yield* decodeValue(key, encoded);
  });

  const resolveEntry = Effect.fnUntraced(function* (
    transaction: FoundationDbTransaction["Service"],
    storeKeys: PersistenceKeys,
    storeId: string,
    key: string,
  ) {
    const manifest = yield* readManifest(
      transaction,
      storeKeys,
      storeId,
      key,
    );
    if (manifest === undefined) {
      return undefined;
    }
    if (
      manifest.expires !== null &&
      manifest.expires <= clock.currentTimeMillisUnsafe()
    ) {
      yield* transaction.clear(yield* storeKeys.entries.pack([storeId, key]));
      yield* clearExpiration(
        transaction,
        storeKeys,
        storeId,
        key,
        manifest.expires,
      );
      yield* clearStoredValue(
        transaction,
        storeKeys,
        storeId,
        manifest.identity,
      );
      return undefined;
    }
    return yield* readStoredValue(
      transaction,
      storeKeys,
      storeId,
      key,
      manifest,
    );
  });

  const prepareEntries = Effect.fnUntraced(function* (
    storeKeys: PersistenceKeys,
    storeId: string,
    entries: ReadonlyArray<WriteEntry>,
  ) {
    const now = clock.currentTimeMillisUnsafe();
    const garbageAt = Math.ceil(Math.min(
      Number.MAX_SAFE_INTEGER,
      now + Duration.toMillis(stagingGarbageDelay),
    ));
    const prepared = [] as Array<PreparedEntry>;

    for (const [key, value, ttl] of latestEntries(entries)) {
      const identity = crypto.randomUUID();
      const identityValue = encoder.encode(identity);
      const expires = expirationFor(ttl, now);
      const manifestKey = yield* storeKeys.entries.pack([storeId, key]);
      const manifestValue = encodeManifest({ identity, expires });
      const expirationKey = expires === null
        ? undefined
        : yield* storeKeys.expires.pack([storeId, timestamp(expires), key]);
      const previousExpirationKey = yield* storeKeys.expires.pack([
        storeId,
        timestamp(Number.MAX_SAFE_INTEGER),
        key,
      ]);
      const previousGarbageKey = yield* storeKeys.garbage.pack([
        storeId,
        timestamp(Number.MAX_SAFE_INTEGER),
        identity,
      ]);
      const stagingGarbageKey = yield* storeKeys.garbage.pack([
        storeId,
        timestamp(garbageAt),
        identity,
      ]);
      const encoded = yield* encodeValue(key, value);
      const values = splitChunkedValue(encoded);
      const chunks = [] as Array<Mutation>;
      for (let index = 0; index < values.length; index++) {
        chunks.push({
          key: yield* storeKeys.values.pack([
            storeId,
            identity,
            BigInt(index),
          ]),
          value: values[index],
        });
      }

      const manifestMutation = { key: manifestKey, value: manifestValue };
      const expirationMutation = expirationKey === undefined
        ? undefined
        : { key: expirationKey, value: identityValue };
      const publicationByteEstimate = readBytes(manifestKey) +
        mutationBytes(manifestMutation) + clearBytes(previousExpirationKey) +
        mutationBytes({ key: previousGarbageKey, value: emptyValue }) +
        (expirationMutation === undefined
          ? 0
          : mutationBytes(expirationMutation));
      const chunkBytes = chunks.reduce(
        (total, mutation) => total + mutationBytes(mutation),
        0,
      );
      prepared.push({
        key,
        identity,
        expires,
        manifestKey,
        manifestValue,
        expirationKey,
        identityValue,
        stagingGarbageKey,
        chunks,
        directByteEstimate: publicationByteEstimate + chunkBytes,
        stagedPublicationByteEstimate: publicationByteEstimate +
          readBytes(stagingGarbageKey) + clearBytes(stagingGarbageKey),
      });
    }
    return prepared;
  });

  const writeStageBatch = Effect.fnUntraced(function* (
    transactionEpochKey: Uint8Array,
    mutations: ReadonlyArray<Mutation>,
    expectedEpoch: StoreEpoch | undefined,
  ) {
    const observedEpoch = yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const currentEpoch = yield* transaction.get(transactionEpochKey);
        if (
          expectedEpoch !== undefined &&
          !equalBytes(expectedEpoch.value, currentEpoch)
        ) {
          return yield* Effect.fail(new StoreEpochChanged());
        }
        for (const mutation of mutations) {
          yield* transaction.set(mutation.key, mutation.value);
        }
        return currentEpoch;
      }),
      transactionOptions,
    );
    return expectedEpoch ?? { value: observedEpoch };
  });

  const stageEntries = Effect.fnUntraced(function* (
    transactionEpochKey: Uint8Array,
    entries: ReadonlyArray<PreparedEntry>,
  ) {
    const batchBaseBytes = readBytes(transactionEpochKey);
    let batchBytes = batchBaseBytes;
    let batch = [] as Array<Mutation>;
    let epoch: StoreEpoch | undefined;

    const flush = Effect.fnUntraced(function* () {
      epoch = yield* writeStageBatch(transactionEpochKey, batch, epoch);
      batch = [];
      batchBytes = batchBaseBytes;
    });

    for (const entry of entries) {
      const stagingMutations = [
        { key: entry.stagingGarbageKey, value: emptyValue },
        ...entry.chunks,
      ];
      for (const mutation of stagingMutations) {
        const bytes = mutationBytes(mutation);
        if (batch.length > 0 && batchBytes + bytes > transactionByteBudget) {
          yield* flush();
        }
        if (batchBytes + bytes > transactionByteBudget) {
          return yield* transactionLimitError();
        }
        batch.push(mutation);
        batchBytes += bytes;
      }
    }
    if (batch.length > 0) {
      yield* flush();
    }
    return epoch!;
  });

  const publishEntries = Effect.fnUntraced(function* (
    storeKeys: PersistenceKeys,
    storeId: string,
    transactionEpochKey: Uint8Array,
    entries: ReadonlyArray<PreparedEntry>,
    expectedEpoch: StoreEpoch | undefined,
  ) {
    const staged = expectedEpoch !== undefined;
    const replacedAt = clock.currentTimeMillisUnsafe();
    yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const currentEpoch = yield* transaction.get(transactionEpochKey);
        if (
          expectedEpoch !== undefined &&
          !equalBytes(expectedEpoch.value, currentEpoch)
        ) {
          return yield* Effect.fail(new StoreEpochChanged());
        }

        const readKeys = staged
          ? [
            ...entries.map((entry) => entry.stagingGarbageKey),
            ...entries.map((entry) => entry.manifestKey),
          ]
          : entries.map((entry) => entry.manifestKey);
        const encoded = yield* transaction.getMany(readKeys);
        const markerValues = staged ? encoded.slice(0, entries.length) : [];
        const manifestValues = staged ? encoded.slice(entries.length) : encoded;
        const previous = [] as Array<EntryManifest | undefined>;
        for (let index = 0; index < entries.length; index++) {
          const value = manifestValues[index];
          previous.push(
            value === undefined
              ? undefined
              : yield* decodeManifest(entries[index].key, value),
          );
        }

        if (
          staged &&
          entries.every((entry, index) => sameManifest(previous[index], entry))
        ) {
          for (let index = 0; index < markerValues.length; index++) {
            if (markerValues[index] !== undefined) {
              yield* transaction.clear(entries[index].stagingGarbageKey);
            }
          }
          return;
        }
        if (staged && markerValues.some((value) => value === undefined)) {
          return yield* Effect.fail(new StoreEpochChanged());
        }

        if (!staged) {
          for (const entry of entries) {
            for (const mutation of entry.chunks) {
              yield* transaction.set(mutation.key, mutation.value);
            }
          }
        }
        for (let index = 0; index < entries.length; index++) {
          const entry = entries[index];
          const old = previous[index];
          if (old !== undefined) {
            yield* clearExpiration(
              transaction,
              storeKeys,
              storeId,
              entry.key,
              old.expires,
            );
            if (old.identity !== entry.identity) {
              yield* transaction.set(
                yield* storeKeys.garbage.pack([
                  storeId,
                  timestamp(replacedAt),
                  old.identity,
                ]),
                emptyValue,
              );
            }
          }
          yield* transaction.set(entry.manifestKey, entry.manifestValue);
          if (staged) {
            yield* transaction.clear(entry.stagingGarbageKey);
          }
          if (entry.expirationKey !== undefined) {
            yield* transaction.set(entry.expirationKey, entry.identityValue);
          }
        }
      }),
      transactionOptions,
    );
  });

  const writeOnce = Effect.fnUntraced(function* (
    storeId: string,
    entries: ReadonlyArray<WriteEntry>,
  ) {
    const storeKeys = yield* keys();
    const prepared = yield* prepareEntries(storeKeys, storeId, entries);
    const transactionEpochKey = yield* epochKey(storeKeys, storeId);
    const epochBytes = readBytes(transactionEpochKey);
    const directBytes = epochBytes + prepared.reduce(
      (total, entry) => total + entry.directByteEstimate,
      0,
    );
    if (directBytes <= transactionByteBudget) {
      return yield* publishEntries(
        storeKeys,
        storeId,
        transactionEpochKey,
        prepared,
        undefined,
      );
    }
    const publicationBytes = epochBytes + prepared.reduce(
      (total, entry) => total + entry.stagedPublicationByteEstimate,
      0,
    );
    if (publicationBytes > transactionByteBudget) {
      return yield* transactionLimitError();
    }
    const epoch = yield* stageEntries(transactionEpochKey, prepared);
    return yield* publishEntries(
      storeKeys,
      storeId,
      transactionEpochKey,
      prepared,
      epoch,
    );
  });

  const writeEntries = (
    storeId: string,
    entries: ReadonlyArray<WriteEntry>,
  ): Effect.Effect<void, unknown> =>
    Effect.matchEffect(writeOnce(storeId, entries), {
      onFailure: (error) =>
        error instanceof StoreEpochChanged
          ? writeEntries(storeId, entries)
          : Effect.fail(error),
      onSuccess: Effect.succeed,
    });

  const cleanupBatch = (storeId: string) =>
    keys().pipe(
      Effect.flatMap((storeKeys) =>
        database.withTransaction(
          Effect.gen(function* () {
            const transaction = yield* FoundationDbTransaction;
            const now = clock.currentTimeMillisUnsafe();
            const [expiresBegin] = yield* storeKeys.expires.range([storeId]);
            const expiresEnd = yield* storeKeys.expires.pack([
              storeId,
              timestamp(now) + 1n,
            ]);
            const [garbageBegin] = yield* storeKeys.garbage.range([storeId]);
            const garbageEnd = yield* storeKeys.garbage.pack([
              storeId,
              timestamp(now) + 1n,
            ]);
            const [expired, garbage] = yield* Effect.all([
              Stream.runCollect(transaction.getRange(keyRange(
                expiresBegin,
                expiresEnd,
                { limit: cleanupBatchSize },
              ))),
              Stream.runCollect(transaction.getRange(keyRange(
                garbageBegin,
                garbageEnd,
                { limit: cleanupBatchSize },
              ))),
            ]);

            for (const item of expired) {
              const tuple = yield* storeKeys.expires.unpack(item.key);
              if (
                tuple.length !== 3 || typeof tuple[1] !== "bigint" ||
                typeof tuple[2] !== "string"
              ) {
                return yield* new Persistence.PersistenceError({
                  message: "Invalid FoundationDB persistence expiration index",
                });
              }
              const identity = decoder.decode(item.value);
              const manifest = yield* readManifest(
                transaction,
                storeKeys,
                storeId,
                tuple[2],
              );
              if (
                manifest !== undefined && manifest.identity === identity &&
                manifest.expires !== null && manifest.expires <= now
              ) {
                yield* transaction.clear(
                  yield* storeKeys.entries.pack([storeId, tuple[2]]),
                );
                yield* clearStoredValue(
                  transaction,
                  storeKeys,
                  storeId,
                  identity,
                );
              }
              yield* transaction.clear(item.key);
            }

            for (const item of garbage) {
              const tuple = yield* storeKeys.garbage.unpack(item.key);
              if (
                tuple.length !== 3 || typeof tuple[1] !== "bigint" ||
                typeof tuple[2] !== "string"
              ) {
                return yield* new Persistence.PersistenceError({
                  message: "Invalid FoundationDB persistence garbage index",
                });
              }
              yield* clearStoredValue(
                transaction,
                storeKeys,
                storeId,
                tuple[2],
              );
              yield* transaction.clear(item.key);
            }
            return Math.max(expired.length, garbage.length);
          }),
          transactionOptions,
        )
      ),
      Effect.mapError((cause) => persistenceError("cleanup", undefined, cause)),
    );

  const cleanupStore = (storeId: string) =>
    Effect.gen(function* () {
      while (true) {
        const count = yield* cleanupBatch(storeId);
        if (count < cleanupBatchSize) {
          return;
        }
        yield* Effect.sleep(cleanupBatchDelay);
      }
    });

  return Persistence.BackingPersistence.of({
    make: Effect.fnUntraced(function* (storeId: string) {
      yield* cleanupStore(storeId).pipe(
        Effect.matchEffect({
          onFailure: (error) => Effect.logWarning(error),
          onSuccess: () => Effect.void,
        }),
        Effect.repeat(Schedule.spaced(cleanupInterval)),
        Effect.forkScoped,
      );

      return {
        get: (key) =>
          keys().pipe(
            Effect.flatMap((storeKeys) =>
              database.withTransaction(
                Effect.gen(function* () {
                  const transaction = yield* FoundationDbTransaction;
                  return yield* resolveEntry(
                    transaction,
                    storeKeys,
                    storeId,
                    key,
                  );
                }),
                transactionOptions,
              )
            ),
            Effect.mapError((cause) => persistenceError("get", key, cause)),
          ),
        getMany: (requestedKeys) =>
          keys().pipe(
            Effect.flatMap((storeKeys) =>
              Effect.gen(function* () {
                const uniqueKeys = Array.from(new Set(requestedKeys));
                const manifestKeys = [] as Array<Uint8Array>;
                for (const key of uniqueKeys) {
                  manifestKeys.push(
                    yield* storeKeys.entries.pack([storeId, key]),
                  );
                }
                return yield* database.withTransaction(
                  Effect.gen(function* () {
                    const transaction = yield* FoundationDbTransaction;
                    const encoded = yield* transaction.getMany(manifestKeys);
                    const now = clock.currentTimeMillisUnsafe();
                    const values = new Map<string, object | undefined>();
                    const live = [] as Array<{
                      readonly key: string;
                      readonly manifest: EntryManifest;
                    }>;
                    for (let index = 0; index < uniqueKeys.length; index++) {
                      const key = uniqueKeys[index];
                      const item = encoded[index];
                      if (item === undefined) {
                        values.set(key, undefined);
                        continue;
                      }
                      const manifest = yield* decodeManifest(key, item);
                      if (
                        manifest.expires !== null && manifest.expires <= now
                      ) {
                        yield* transaction.clear(manifestKeys[index]);
                        yield* clearExpiration(
                          transaction,
                          storeKeys,
                          storeId,
                          key,
                          manifest.expires,
                        );
                        yield* clearStoredValue(
                          transaction,
                          storeKeys,
                          storeId,
                          manifest.identity,
                        );
                        values.set(key, undefined);
                      } else {
                        live.push({ key, manifest });
                      }
                    }
                    const loaded = yield* Effect.forEach(
                      live,
                      ({ key, manifest }) =>
                        readStoredValue(
                          transaction,
                          storeKeys,
                          storeId,
                          key,
                          manifest,
                        ),
                      { concurrency: rangeReadConcurrency },
                    );
                    for (let index = 0; index < live.length; index++) {
                      values.set(live[index].key, loaded[index]);
                    }
                    return requestedKeys.map((key) => values.get(key)) as [
                      object | undefined,
                      ...Array<object | undefined>,
                    ];
                  }),
                  transactionOptions,
                );
              })
            ),
            Effect.mapError((cause) =>
              persistenceError("getMany", undefined, cause)
            ),
          ),
        set: (key, value, ttl) =>
          writeEntries(storeId, [[key, value, ttl]]).pipe(
            Effect.mapError((cause) => persistenceError("set", key, cause)),
          ),
        setMany: (entries) =>
          writeEntries(storeId, entries).pipe(
            Effect.mapError((cause) =>
              persistenceError("setMany", undefined, cause)
            ),
          ),
        remove: (key) =>
          keys().pipe(
            Effect.flatMap((storeKeys) =>
              database.withTransaction(
                Effect.gen(function* () {
                  const transaction = yield* FoundationDbTransaction;
                  const manifest = yield* readManifest(
                    transaction,
                    storeKeys,
                    storeId,
                    key,
                  );
                  if (manifest !== undefined) {
                    yield* clearExpiration(
                      transaction,
                      storeKeys,
                      storeId,
                      key,
                      manifest.expires,
                    );
                    yield* clearStoredValue(
                      transaction,
                      storeKeys,
                      storeId,
                      manifest.identity,
                    );
                  }
                  yield* transaction.clear(
                    yield* storeKeys.entries.pack([storeId, key]),
                  );
                }),
                transactionOptions,
              )
            ),
            Effect.mapError((cause) => persistenceError("remove", key, cause)),
          ),
        clear: Effect.suspend(() => {
          const nextEpoch = encoder.encode(crypto.randomUUID());
          return keys().pipe(
            Effect.flatMap((storeKeys) =>
              database.withTransaction(
                Effect.gen(function* () {
                  const transaction = yield* FoundationDbTransaction;
                  const [entriesBegin, entriesEnd] = yield* storeKeys.entries
                    .range([storeId]);
                  const [valuesBegin, valuesEnd] = yield* storeKeys.values
                    .range([
                      storeId,
                    ]);
                  const [expiresBegin, expiresEnd] = yield* storeKeys.expires
                    .range([storeId]);
                  const [garbageBegin, garbageEnd] = yield* storeKeys.garbage
                    .range([storeId]);
                  yield* transaction.clearRange(entriesBegin, entriesEnd);
                  yield* transaction.clearRange(valuesBegin, valuesEnd);
                  yield* transaction.clearRange(expiresBegin, expiresEnd);
                  yield* transaction.clearRange(garbageBegin, garbageEnd);
                  yield* transaction.set(
                    yield* epochKey(storeKeys, storeId),
                    nextEpoch,
                  );
                }),
                transactionOptions,
              )
            ),
          );
        }).pipe(
          Effect.mapError((cause) =>
            persistenceError("clear", undefined, cause)
          ),
        ),
      } satisfies Persistence.BackingPersistenceStore;
    }),
  });
});

/** Provides Effect's BackingPersistence using FoundationDB. */
export const layerBackingPersistence = (
  options?: BackingPersistenceOptions,
): Layer.Layer<Persistence.BackingPersistence, never, FoundationDb> =>
  Layer.effect(
    Persistence.BackingPersistence,
    makeBackingPersistence(options),
  );
