import { Effect } from "effect";
import { FoundationDbError } from "../../src/errors.ts";
import type {
  DatabaseHandle,
  NativeDriverShape,
  RangeBatch,
  RangeHandle,
  TransactionHandle,
} from "../../src/internal/native.ts";
import type {
  Bytes,
  ConflictRange,
  ConflictRangeType,
  KeySelector,
  MutationType as MutationTypeValue,
  RangeOptions,
} from "../../src/model.ts";

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
      | "timeout"
      | "retryLimit"
      | "maxRetryDelay"
      | "reportConflictingKeys",
      number,
    ]
  >;
  readonly gets: Array<readonly [TransactionHandle, Bytes, boolean]>;
  readonly getMany: Array<
    readonly [TransactionHandle, ReadonlyArray<Bytes>, boolean]
  >;
  readonly getKeys: Array<readonly [TransactionHandle, KeySelector, boolean]>;
  readonly sets: Array<readonly [TransactionHandle, Bytes, Bytes]>;
  readonly atomicOps: Array<
    readonly [TransactionHandle, Bytes, Bytes, MutationTypeValue]
  >;
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
  readonly conflictRanges: Array<
    readonly [TransactionHandle, Bytes, Bytes, ConflictRangeType]
  >;
  readonly getReadVersions: Array<TransactionHandle>;
  readonly setReadVersions: Array<readonly [TransactionHandle, bigint]>;
  readonly getApproximateSizes: Array<TransactionHandle>;
  readonly watches: Array<readonly [TransactionHandle, Bytes]>;
  readonly getVersionstamps: Array<TransactionHandle>;
  readonly getConflictingKeyRanges: Array<TransactionHandle>;
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
  readVersionResults: Array<bigint | FoundationDbError>;
  approximateSizeResults: Array<bigint | FoundationDbError>;
  watchResults: Array<void | FoundationDbError>;
  versionstampResults: Array<Uint8Array | FoundationDbError>;
  conflictingKeyRangeResults: Array<
    ReadonlyArray<ConflictRange> | FoundationDbError
  >;
  committedVersionResults: Array<bigint>;
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
    atomicOps: [],
    setsWithoutWriteConflict: [],
    clears: [],
    clearRanges: [],
    clearRangesWithoutWriteConflict: [],
    writeConflictRanges: [],
    conflictRanges: [],
    getReadVersions: [],
    setReadVersions: [],
    getApproximateSizes: [],
    watches: [],
    getVersionstamps: [],
    getConflictingKeyRanges: [],
    openRanges: [],
    nextRanges: [],
    closeRanges: [],
    commits: [],
    onErrors: [],
    getResults: [],
    getManyResults: [],
    getKeyResults: [],
    rangeBatches: [],
    readVersionResults: [],
    approximateSizeResults: [],
    watchResults: [],
    versionstampResults: [],
    conflictingKeyRangeResults: [],
    committedVersionResults: [],
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
    atomicOp: (handle, key, value, mutationType) =>
      Effect.sync(() => {
        state.atomicOps.push([handle, key, value, mutationType]);
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
    addConflictRange: (handle, begin, end, conflictType) =>
      Effect.sync(() => {
        state.conflictRanges.push([handle, begin, end, conflictType]);
        if (conflictType === 1) {
          state.writeConflictRanges.push([handle, begin, end]);
        }
      }),
    getReadVersion: (handle) =>
      Effect.suspend(() => {
        state.getReadVersions.push(handle);
        return result(state.readVersionResults.shift() ?? 0n);
      }),
    setReadVersion: (handle, version) =>
      Effect.sync(() => {
        state.setReadVersions.push([handle, version]);
      }),
    getApproximateSize: (handle) =>
      Effect.suspend(() => {
        state.getApproximateSizes.push(handle);
        return result(state.approximateSizeResults.shift() ?? 0n);
      }),
    watch: (handle, key) =>
      Effect.suspend(() => {
        state.watches.push([handle, key]);
        return result(state.watchResults.shift());
      }),
    getVersionstamp: (handle) =>
      Effect.suspend(() => {
        state.getVersionstamps.push(handle);
        return result(
          state.versionstampResults.shift() ?? new Uint8Array(10),
        );
      }),
    getConflictingKeyRanges: (handle) =>
      Effect.suspend(() => {
        state.getConflictingKeyRanges.push(handle);
        return result(state.conflictingKeyRangeResults.shift() ?? []);
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
        return failure === undefined
          ? Effect.succeed(state.committedVersionResults.shift() ?? 1n)
          : Effect.fail(failure);
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
