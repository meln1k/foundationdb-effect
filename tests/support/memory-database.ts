import { Deferred, Effect, Semaphore, Stream } from "effect";
import { FoundationDbTransaction } from "../../src/FoundationDb.ts";
import type {
  FoundationDbFuture,
  FoundationDbShape,
  FoundationDbTransactionShape,
} from "../../src/FoundationDb.ts";
import { MutationType } from "../../src/model.ts";
import type { Bytes, KeyValue, RangeOptions } from "../../src/model.ts";

interface StoredValue {
  readonly key: Uint8Array;
  readonly value: Uint8Array;
}

interface MemoryWatch {
  readonly key: string;
  readonly deferred: Deferred.Deferred<void>;
  cancelled: boolean;
}

const copy = (value: Bytes): Uint8Array => value.slice();

const compare = (left: Bytes, right: Bytes): number => {
  const length = Math.min(left.byteLength, right.byteLength);
  for (let index = 0; index < length; index++) {
    const difference = left[index] - right[index];
    if (difference !== 0) {
      return difference;
    }
  }
  return left.byteLength - right.byteLength;
};

const keyId = (key: Bytes): string => {
  let output = "";
  for (const value of key) {
    output += value.toString(16).padStart(2, "0");
  }
  return output;
};

const bytesEqual = (left: Bytes, right: Bytes): boolean =>
  left.byteLength === right.byteLength &&
  left.every((value, index) => value === right[index]);

const addLittleEndian = (
  current: Bytes | undefined,
  operand: Bytes,
): Uint8Array => {
  const output = new Uint8Array(operand.byteLength);
  let carry = 0;
  for (let index = 0; index < operand.byteLength; index++) {
    const sum = (current?.[index] ?? 0) + operand[index] + carry;
    output[index] = sum & 0xff;
    carry = sum >>> 8;
  }
  return output;
};

const entries = (store: Map<string, StoredValue>): Array<StoredValue> =>
  Array.from(store.values()).sort((left, right) =>
    compare(left.key, right.key)
  );

const selectRange = (
  store: Map<string, StoredValue>,
  options: RangeOptions,
): Array<KeyValue> => {
  let selected = entries(store).filter(({ key }) =>
    compare(key, options.begin.key) >= 0 && compare(key, options.end.key) < 0
  );
  if (options.reverse === true) {
    selected = selected.reverse();
  }
  if (options.limit !== undefined && options.limit > 0) {
    selected = selected.slice(0, options.limit);
  }
  return selected.map(({ key, value }) => ({
    key: copy(key),
    value: copy(value),
  }));
};

const makeTransaction = (
  store: Map<string, StoredValue>,
  pendingWatches: Array<MemoryWatch>,
  removeWatch: (watch: MemoryWatch) => void,
): FoundationDbTransactionShape => ({
  attempt: 1,
  maybeCommitted: false,
  conflictingKeyRanges: [],
  get: (key) =>
    Effect.sync(() => {
      const value = store.get(keyId(key));
      return value === undefined ? undefined : copy(value.value);
    }),
  getMany: (keys) =>
    Effect.sync(() =>
      keys.map((key) => {
        const value = store.get(keyId(key));
        return value === undefined ? undefined : copy(value.value);
      })
    ),
  getKey: (selector) =>
    Effect.sync(() => {
      const all = entries(store);
      const index = selector.offset - 1 + all.findIndex(({ key }) =>
        selector.orEqual
          ? compare(key, selector.key) > 0
          : compare(key, selector.key) >= 0
      );
      return index >= 0 && index < all.length
        ? copy(all[index].key)
        : new Uint8Array();
    }),
  set: (key, value) =>
    Effect.sync(() => {
      store.set(keyId(key), { key: copy(key), value: copy(value) });
    }),
  atomicOp: (key, value, mutationType) =>
    mutationType === MutationType.Add
      ? Effect.sync(() => {
        const id = keyId(key);
        store.set(id, {
          key: copy(key),
          value: addLittleEndian(store.get(id)?.value, value),
        });
      })
      : Effect.die(
        new Error(
          "the in-memory test database only implements MutationType.Add",
        ),
      ),
  setWithoutWriteConflict: (key, value) =>
    Effect.sync(() => {
      store.set(keyId(key), { key: copy(key), value: copy(value) });
    }),
  clear: (key) =>
    Effect.sync(() => {
      store.delete(keyId(key));
    }),
  clearRange: (begin, end) =>
    Effect.sync(() => {
      for (const entry of entries(store)) {
        if (compare(entry.key, begin) >= 0 && compare(entry.key, end) < 0) {
          store.delete(keyId(entry.key));
        }
      }
    }),
  clearRangeWithoutWriteConflict: (begin, end) =>
    Effect.sync(() => {
      for (const entry of entries(store)) {
        if (compare(entry.key, begin) >= 0 && compare(entry.key, end) < 0) {
          store.delete(keyId(entry.key));
        }
      }
    }),
  addWriteConflictRange: () => Effect.void,
  addReadConflictRange: () => Effect.void,
  addConflictRange: () => Effect.void,
  getReadVersion: () => Effect.succeed(0n),
  setReadVersion: () => Effect.void,
  getApproximateSize: () => Effect.succeed(0n),
  watch: (key) =>
    Effect.sync(() => {
      const watch: MemoryWatch = {
        key: keyId(key),
        deferred: Deferred.makeUnsafe(),
        cancelled: false,
      };
      pendingWatches.push(watch);
      return {
        await: Deferred.await(watch.deferred),
        cancel: Effect.sync(() => {
          watch.cancelled = true;
          removeWatch(watch);
          Deferred.doneUnsafe(watch.deferred, Effect.interrupt);
        }),
      } satisfies FoundationDbFuture<void>;
    }),
  getVersionstamp: () =>
    Effect.succeed<FoundationDbFuture<Uint8Array>>({
      await: Effect.succeed(new Uint8Array(10)),
      cancel: Effect.void,
    }),
  getRange: (options) =>
    Stream.suspend(() => Stream.fromIterable(selectRange(store, options))),
});

export const makeMemoryFoundationDb = (): {
  readonly database: FoundationDbShape;
  readonly entries: () => ReadonlyArray<StoredValue>;
  readonly activeWatches: () => number;
} => {
  let store = new Map<string, StoredValue>();
  let committedVersion = 0n;
  const watches = new Map<string, Set<MemoryWatch>>();
  const transactionMutex = Semaphore.makeUnsafe(1);
  const removeWatch = (watch: MemoryWatch): void => {
    const registered = watches.get(watch.key);
    registered?.delete(watch);
    if (registered?.size === 0) {
      watches.delete(watch.key);
    }
  };

  const withTransactionResult: FoundationDbShape["withTransactionResult"] = (
    effect,
  ) =>
    transactionMutex.withPermits(1)(Effect.suspend(() => {
      const pendingWatches: Array<MemoryWatch> = [];
      const working = new Map(
        Array.from(store, ([id, entry]) => [id, {
          key: copy(entry.key),
          value: copy(entry.value),
        }]),
      );
      return Effect.scoped(Effect.provideService(
        effect,
        FoundationDbTransaction,
        makeTransaction(working, pendingWatches, removeWatch),
      )).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            const changedKeys = new Set([...store.keys(), ...working.keys()]);
            for (const key of changedKeys) {
              const previous = store.get(key);
              const next = working.get(key);
              if (
                previous !== undefined && next !== undefined &&
                bytesEqual(previous.value, next.value)
              ) {
                changedKeys.delete(key);
              }
            }
            store = working;
            committedVersion += 1n;
            for (const key of changedKeys) {
              const registered = watches.get(key);
              watches.delete(key);
              if (registered !== undefined) {
                for (const watch of registered) {
                  Deferred.doneUnsafe(watch.deferred, Effect.void);
                }
              }
            }
            for (const watch of pendingWatches) {
              if (!watch.cancelled) {
                let registered = watches.get(watch.key);
                if (registered === undefined) {
                  registered = new Set();
                  watches.set(watch.key, registered);
                }
                registered.add(watch);
              }
            }
          })
        ),
        Effect.map((value) => ({ value, committedVersion })),
      );
    }));

  const withTransaction: FoundationDbShape["withTransaction"] = (
    effect,
    options,
  ) =>
    withTransactionResult(effect, options).pipe(
      Effect.map((result) => result.value),
    );

  const database: FoundationDbShape = {
    withTransactionResult,
    withTransaction,
    get: (key, options) =>
      withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          transaction.get(key, options)),
        options?.transaction,
      ),
    getMany: (keys, options) =>
      withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          transaction.getMany(keys, options)),
        options?.transaction,
      ),
    set: (key, value, options) =>
      withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          transaction.set(key, value)),
        options,
      ),
    clear: (key, options) =>
      withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          transaction.clear(key)),
        options,
      ),
    clearRange: (begin, end, options) =>
      withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          transaction.clearRange(begin, end)),
        options,
      ),
    getRange: (options, transactionOptions) =>
      withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          Stream.runCollect(transaction.getRange(options))),
        transactionOptions,
      ),
    watch: (key, options) =>
      withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          transaction.watch(key)),
        options,
      ).pipe(
        Effect.flatMap((future) =>
          future.await.pipe(
            Effect.onInterrupt(() =>
              future.cancel
            ),
          )
        ),
      ),
  };

  return {
    database,
    entries: () => entries(store),
    activeWatches: () =>
      Array.from(watches.values()).reduce(
        (count, registered) => count + registered.size,
        0,
      ),
  };
};
