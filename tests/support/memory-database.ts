import { Effect, Stream } from "effect";
import { FoundationDbTransaction } from "../../src/FoundationDb.ts";
import type {
  FoundationDbShape,
  FoundationDbTransactionShape,
} from "../../src/FoundationDb.ts";
import type { Bytes, KeyValue, RangeOptions } from "../../src/model.ts";

interface StoredValue {
  readonly key: Uint8Array;
  readonly value: Uint8Array;
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
): FoundationDbTransactionShape => ({
  attempt: 1,
  maybeCommitted: false,
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
  atomicAdd: (key, value) =>
    Effect.sync(() => {
      const id = keyId(key);
      store.set(id, {
        key: copy(key),
        value: addLittleEndian(store.get(id)?.value, value),
      });
    }),
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
  getRange: (options) =>
    Stream.suspend(() => Stream.fromIterable(selectRange(store, options))),
});

export const makeMemoryFoundationDb = (): {
  readonly database: FoundationDbShape;
  readonly entries: () => ReadonlyArray<StoredValue>;
} => {
  let store = new Map<string, StoredValue>();

  const withTransaction: FoundationDbShape["withTransaction"] = (effect) =>
    Effect.suspend(() => {
      const working = new Map(
        Array.from(store, ([id, entry]) => [id, {
          key: copy(entry.key),
          value: copy(entry.value),
        }]),
      );
      return Effect.scoped(Effect.provideService(
        effect,
        FoundationDbTransaction,
        makeTransaction(working),
      )).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            store = working;
          })
        ),
      );
    });

  const database: FoundationDbShape = {
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
  };

  return {
    database,
    entries: () => entries(store),
  };
};
