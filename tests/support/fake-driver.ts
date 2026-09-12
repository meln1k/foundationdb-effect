import { Effect } from "effect";
import { FoundationDbError } from "../../src/errors.ts";
import type {
  DatabaseHandle,
  NativeDriverShape,
  RangeBatch,
  RangeHandle,
  TransactionHandle,
} from "../../src/internal/native.ts";
import type { Bytes, KeySelector, RangeOptions } from "../../src/model.ts";

export const fdbError = (
  operation: string,
  overrides: Partial<Omit<FoundationDbError, "_tag" | "operation">> = {},
): FoundationDbError =>
  new FoundationDbError({
    operation,
    code: overrides.code ?? 1007,
    message: overrides.message ?? "transaction_too_old",
    retryable: overrides.retryable ?? true,
    maybeCommitted: overrides.maybeCommitted ?? false,
    retryableNotCommitted: overrides.retryableNotCommitted ?? true,
  });

export interface FakeDriverState {
  readonly openDatabase: Array<string | undefined>;
  readonly closeDatabase: Array<DatabaseHandle>;
  readonly openTransaction: Array<DatabaseHandle>;
  readonly closeTransaction: Array<TransactionHandle>;
  readonly options: Array<
    readonly [
      TransactionHandle,
      "timeout" | "retryLimit" | "maxRetryDelay",
      number,
    ]
  >;
  readonly gets: Array<readonly [TransactionHandle, Bytes, boolean]>;
  readonly getMany: Array<
    readonly [TransactionHandle, ReadonlyArray<Bytes>, boolean]
  >;
  readonly getKeys: Array<readonly [TransactionHandle, KeySelector, boolean]>;
  readonly sets: Array<readonly [TransactionHandle, Bytes, Bytes]>;
  readonly atomicAdds: Array<readonly [TransactionHandle, Bytes, Bytes]>;
  readonly setsWithoutWriteConflict: Array<
    readonly [TransactionHandle, Bytes, Bytes]
  >;
  readonly clears: Array<readonly [TransactionHandle, Bytes]>;
  readonly clearRanges: Array<readonly [TransactionHandle, Bytes, Bytes]>;
  readonly clearRangesWithoutWriteConflict: Array<
    readonly [TransactionHandle, Bytes, Bytes]
  >;
  readonly writeConflictRanges: Array<
    readonly [TransactionHandle, Bytes, Bytes]
  >;
  readonly openRanges: Array<readonly [TransactionHandle, RangeOptions]>;
  readonly nextRanges: Array<RangeHandle>;
  readonly closeRanges: Array<RangeHandle>;
  readonly commits: Array<TransactionHandle>;
  readonly onErrors: Array<readonly [TransactionHandle, FoundationDbError]>;
  getResults: Array<Uint8Array | undefined | FoundationDbError>;
  getManyResults: Array<
    ReadonlyArray<Uint8Array | undefined> | FoundationDbError
  >;
  getKeyResults: Array<Uint8Array | FoundationDbError>;
  rangeBatches: Array<RangeBatch | FoundationDbError>;
  commitFailures: Array<FoundationDbError>;
  onErrorFailures: Array<FoundationDbError>;
}

const result = <A>(
  value: A | FoundationDbError,
): Effect.Effect<A, FoundationDbError> =>
  value instanceof FoundationDbError
    ? Effect.fail(value)
    : Effect.succeed(value);

export const makeFakeDriver = (): {
  readonly driver: NativeDriverShape;
  readonly state: FakeDriverState;
} => {
  const database = 1n;
  const transaction = 2n;
  const range = 3n;
  const state: FakeDriverState = {
    openDatabase: [],
    closeDatabase: [],
    openTransaction: [],
    closeTransaction: [],
    options: [],
    gets: [],
    getMany: [],
    getKeys: [],
    sets: [],
    atomicAdds: [],
    setsWithoutWriteConflict: [],
    clears: [],
    clearRanges: [],
    clearRangesWithoutWriteConflict: [],
    writeConflictRanges: [],
    openRanges: [],
    nextRanges: [],
    closeRanges: [],
    commits: [],
    onErrors: [],
    getResults: [],
    getManyResults: [],
    getKeyResults: [],
    rangeBatches: [],
    commitFailures: [],
    onErrorFailures: [],
  };

  const driver: NativeDriverShape = {
    openDatabase: (clusterFile) =>
      Effect.sync(() => {
        state.openDatabase.push(clusterFile);
        return database;
      }),
    closeDatabase: (handle) =>
      Effect.sync(() => {
        state.closeDatabase.push(handle);
      }),
    openTransaction: (handle) =>
      Effect.sync(() => {
        state.openTransaction.push(handle);
        return transaction;
      }),
    closeTransaction: (handle) =>
      Effect.sync(() => {
        state.closeTransaction.push(handle);
      }),
    setTransactionOption: (handle, option, value) =>
      Effect.sync(() => {
        state.options.push([handle, option, value]);
      }),
    get: (handle, key, snapshot) =>
      Effect.suspend(() => {
        state.gets.push([handle, key, snapshot]);
        return result(state.getResults.shift());
      }),
    getMany: (handle, keys, snapshot) =>
      Effect.suspend(() => {
        state.getMany.push([handle, keys, snapshot]);
        return result(state.getManyResults.shift() ?? []);
      }),
    getKey: (handle, selector, snapshot) =>
      Effect.suspend(() => {
        state.getKeys.push([handle, selector, snapshot]);
        return result(state.getKeyResults.shift() ?? new Uint8Array());
      }),
    set: (handle, key, value) =>
      Effect.sync(() => {
        state.sets.push([handle, key, value]);
      }),
    atomicAdd: (handle, key, value) =>
      Effect.sync(() => {
        state.atomicAdds.push([handle, key, value]);
      }),
    setWithoutWriteConflict: (handle, key, value) =>
      Effect.sync(() => {
        state.setsWithoutWriteConflict.push([handle, key, value]);
      }),
    clear: (handle, key) =>
      Effect.sync(() => {
        state.clears.push([handle, key]);
      }),
    clearRange: (handle, begin, end) =>
      Effect.sync(() => {
        state.clearRanges.push([handle, begin, end]);
      }),
    clearRangeWithoutWriteConflict: (handle, begin, end) =>
      Effect.sync(() => {
        state.clearRangesWithoutWriteConflict.push([handle, begin, end]);
      }),
    addWriteConflictRange: (handle, begin, end) =>
      Effect.sync(() => {
        state.writeConflictRanges.push([handle, begin, end]);
      }),
    openRange: (handle, options) =>
      Effect.sync(() => {
        state.openRanges.push([handle, options]);
        return range;
      }),
    nextRange: (handle) =>
      Effect.suspend(() => {
        state.nextRanges.push(handle);
        return result(
          state.rangeBatches.shift() ?? { values: [], more: false },
        );
      }),
    closeRange: (handle) =>
      Effect.sync(() => {
        state.closeRanges.push(handle);
      }),
    commit: (handle) =>
      Effect.suspend(() => {
        state.commits.push(handle);
        const failure = state.commitFailures.shift();
        return failure === undefined ? Effect.void : Effect.fail(failure);
      }),
    onError: (handle, error) =>
      Effect.suspend(() => {
        state.onErrors.push([handle, error]);
        const failure = state.onErrorFailures.shift();
        return failure === undefined ? Effect.void : Effect.fail(failure);
      }),
  };

  return { driver, state };
};
