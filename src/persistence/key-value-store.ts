/** Effect KeyValueStore backed by FoundationDB. */
import { Effect, Encoding, Layer } from "effect";
import { KeyValueStore } from "effect/unstable/persistence";
import { FoundationDb, FoundationDbTransaction } from "../FoundationDb.ts";
import {
  clearChunkedValue,
  hasChunkedValue,
  readChunkedValue,
  writeChunkedValue,
} from "../internal/chunked-value.ts";
import { MutationType } from "../model.ts";
import type { Subspace } from "../tuple/mod.ts";
import { makeDirectoryStore } from "./internal.ts";
import type { KeyValueStoreOptions } from "./model.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const defaultDirectoryPath = ["effect-foundationdb", "key-value-store"];
const valueTypeString = 0;
const valueTypeUint8Array = 1;

interface StoreKeys {
  readonly size: Uint8Array;
  readonly entries: Subspace;
}

class KeyValueStoreDataError extends Error {
  readonly _tag = "KeyValueStoreDataError" as const;
}

const counterDelta = (value: bigint): Uint8Array => {
  const encoded = new Uint8Array(8);
  new DataView(encoded.buffer).setBigInt64(0, value, true);
  return encoded;
};

const incrementSize = counterDelta(1n);
const decrementSize = counterDelta(-1n);
const emptySize = counterDelta(0n);

const storeError = (method: string, key: string | undefined, cause: unknown) =>
  new KeyValueStore.KeyValueStoreError({
    method,
    ...(key === undefined ? {} : { key }),
    message: key === undefined
      ? `FoundationDB KeyValueStore ${method} failed`
      : `FoundationDB KeyValueStore ${method} failed for key ${key}`,
    cause,
  });

const encodeValue = (value: string | Uint8Array): Uint8Array => {
  const payload = typeof value === "string" ? encoder.encode(value) : value;
  const encoded = new Uint8Array(payload.byteLength + 1);
  encoded[0] = typeof value === "string"
    ? valueTypeString
    : valueTypeUint8Array;
  encoded.set(payload, 1);
  return encoded;
};

const decodeString = (
  value: Uint8Array,
): Effect.Effect<string, KeyValueStoreDataError> => {
  if (value[0] === valueTypeString) {
    return Effect.succeed(decoder.decode(value.subarray(1)));
  }
  if (value[0] === valueTypeUint8Array) {
    return Effect.succeed(Encoding.encodeBase64(value.subarray(1)));
  }
  return Effect.fail(
    new KeyValueStoreDataError("invalid KeyValueStore value type"),
  );
};

const decodeBytes = (
  value: Uint8Array,
): Effect.Effect<Uint8Array, KeyValueStoreDataError> => {
  if (value[0] !== valueTypeString && value[0] !== valueTypeUint8Array) {
    return Effect.fail(
      new KeyValueStoreDataError("invalid KeyValueStore value type"),
    );
  }
  return Effect.succeed(value.slice(1));
};

const decodeSize = (
  value: Uint8Array | undefined,
): Effect.Effect<number, KeyValueStoreDataError> =>
  Effect.try({
    try: () => {
      if (value === undefined) {
        return 0;
      }
      if (value.byteLength !== 8) {
        throw new KeyValueStoreDataError(
          "invalid KeyValueStore size metadata",
        );
      }
      const count = new DataView(
        value.buffer,
        value.byteOffset,
        value.byteLength,
      ).getBigInt64(0, true);
      if (count < 0n || count > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new KeyValueStoreDataError(
          "KeyValueStore size metadata is out of range",
        );
      }
      return Number(count);
    },
    catch: (cause) =>
      cause instanceof KeyValueStoreDataError
        ? cause
        : new KeyValueStoreDataError("invalid KeyValueStore size metadata", {
          cause,
        }),
  });

/** Creates Effect's KeyValueStore backed by a FoundationDB subspace. */
export const makeKeyValueStore = Effect.fnUntraced(function* (
  options: KeyValueStoreOptions = {},
) {
  const database = yield* FoundationDb;
  const { root, transactionOptions } = yield* makeDirectoryStore(
    options,
    defaultDirectoryPath,
    "effect-foundationdb/key-value-store",
  );

  const keys = Effect.fnUntraced(function* () {
    const directory = yield* root();
    return {
      size: yield* directory.pack(["size"]),
      entries: yield* directory.subspace(["entries"]),
    } satisfies StoreKeys;
  });

  const read = (key: string) =>
    database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        return yield* readChunkedValue(
          transaction,
          (yield* keys()).entries,
          [key],
        );
      }),
      transactionOptions,
    );

  const set = (key: string, value: string | Uint8Array) =>
    database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const storeKeys = yield* keys();
        if (!(yield* hasChunkedValue(transaction, storeKeys.entries, [key]))) {
          yield* transaction.atomicOp(
            storeKeys.size,
            incrementSize,
            MutationType.Add,
          );
        }
        yield* writeChunkedValue(
          transaction,
          storeKeys.entries,
          [key],
          encodeValue(value),
        );
      }),
      transactionOptions,
    );

  const modify = <A extends string | Uint8Array>(
    method: "modify" | "modifyUint8Array",
    key: string,
    decode: (value: Uint8Array) => Effect.Effect<A, unknown>,
    f: (value: A) => A,
  ) =>
    database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const storeKeys = yield* keys();
        const encoded = yield* readChunkedValue(
          transaction,
          storeKeys.entries,
          [key],
        );
        if (encoded === undefined) {
          return undefined;
        }
        const updated = f(yield* decode(encoded));
        yield* writeChunkedValue(
          transaction,
          storeKeys.entries,
          [key],
          encodeValue(updated),
        );
        return updated;
      }),
      { ...transactionOptions, retryOnMaybeCommitted: false },
    ).pipe(Effect.mapError((cause) => storeError(method, key, cause)));

  const size = database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      const storeKeys = yield* keys();
      return yield* decodeSize(yield* transaction.get(storeKeys.size));
    }),
    transactionOptions,
  ).pipe(Effect.mapError((cause) => storeError("size", undefined, cause)));

  return KeyValueStore.make({
    get: (key) =>
      read(key).pipe(
        Effect.flatMap((value) =>
          value === undefined ? Effect.undefined : decodeString(value)
        ),
        Effect.mapError((cause) => storeError("get", key, cause)),
      ),
    getUint8Array: (key) =>
      read(key).pipe(
        Effect.flatMap((value) =>
          value === undefined ? Effect.undefined : decodeBytes(value)
        ),
        Effect.mapError((cause) => storeError("getUint8Array", key, cause)),
      ),
    set: (key, value) =>
      set(key, value).pipe(
        Effect.mapError((cause) => storeError("set", key, cause)),
      ),
    remove: (key) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const storeKeys = yield* keys();
          if (
            yield* hasChunkedValue(transaction, storeKeys.entries, [key])
          ) {
            yield* clearChunkedValue(transaction, storeKeys.entries, [key]);
            yield* transaction.atomicOp(
              storeKeys.size,
              decrementSize,
              MutationType.Add,
            );
          }
        }),
        transactionOptions,
      ).pipe(
        Effect.mapError((cause) => storeError("remove", key, cause)),
      ),
    clear: database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const storeKeys = yield* keys();
        const [begin, end] = yield* storeKeys.entries.range();
        yield* transaction.clearRange(begin, end);
        yield* transaction.set(storeKeys.size, emptySize);
      }),
      transactionOptions,
    ).pipe(Effect.mapError((cause) => storeError("clear", undefined, cause))),
    size,
    has: (key) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          return yield* hasChunkedValue(
            transaction,
            (yield* keys()).entries,
            [key],
          );
        }),
        transactionOptions,
      ).pipe(
        Effect.mapError((cause) => storeError("has", key, cause)),
      ),
    isEmpty: size.pipe(Effect.map((count) => count === 0)),
    modify: (key, f) => modify("modify", key, decodeString, f),
    modifyUint8Array: (key, f) =>
      modify("modifyUint8Array", key, decodeBytes, f),
  });
});

/** Provides Effect's KeyValueStore using FoundationDB. */
export const layerFoundationDB = (
  options?: KeyValueStoreOptions,
): Layer.Layer<KeyValueStore.KeyValueStore, never, FoundationDb> =>
  Layer.effect(KeyValueStore.KeyValueStore, makeKeyValueStore(options));
