import { Context, Effect } from "effect";
import type { FoundationDbError } from "../errors.ts";
import type { Bytes, KeySelector, KeyValue, RangeOptions } from "../model.ts";

export type DatabaseHandle = bigint;
export type TransactionHandle = bigint;
export type RangeHandle = bigint;

export interface RangeBatch {
  readonly values: ReadonlyArray<KeyValue>;
  readonly more: boolean;
}

export interface NativeDriverShape {
  readonly openDatabase: (
    clusterFile?: string,
  ) => Effect.Effect<DatabaseHandle, FoundationDbError>;
  readonly closeDatabase: (
    handle: DatabaseHandle,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly openTransaction: (
    database: DatabaseHandle,
  ) => Effect.Effect<TransactionHandle, FoundationDbError>;
  readonly closeTransaction: (
    handle: TransactionHandle,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly setTransactionOption: (
    handle: TransactionHandle,
    option: "timeout" | "retryLimit" | "maxRetryDelay",
    value: number,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly get: (
    handle: TransactionHandle,
    key: Bytes,
    snapshot: boolean,
  ) => Effect.Effect<Uint8Array | undefined, FoundationDbError>;
  readonly getMany: (
    handle: TransactionHandle,
    keys: ReadonlyArray<Bytes>,
    snapshot: boolean,
  ) => Effect.Effect<
    ReadonlyArray<Uint8Array | undefined>,
    FoundationDbError
  >;
  readonly getKey: (
    handle: TransactionHandle,
    selector: KeySelector,
    snapshot: boolean,
  ) => Effect.Effect<Uint8Array, FoundationDbError>;
  readonly set: (
    handle: TransactionHandle,
    key: Bytes,
    value: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly atomicAdd: (
    handle: TransactionHandle,
    key: Bytes,
    value: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly setWithoutWriteConflict: (
    handle: TransactionHandle,
    key: Bytes,
    value: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly clear: (
    handle: TransactionHandle,
    key: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly clearRange: (
    handle: TransactionHandle,
    begin: Bytes,
    end: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly clearRangeWithoutWriteConflict: (
    handle: TransactionHandle,
    begin: Bytes,
    end: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly addWriteConflictRange: (
    handle: TransactionHandle,
    begin: Bytes,
    end: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly openRange: (
    handle: TransactionHandle,
    options: RangeOptions,
  ) => Effect.Effect<RangeHandle, FoundationDbError>;
  readonly nextRange: (
    handle: RangeHandle,
  ) => Effect.Effect<RangeBatch, FoundationDbError>;
  readonly closeRange: (
    handle: RangeHandle,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly commit: (
    handle: TransactionHandle,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly onError: (
    handle: TransactionHandle,
    error: FoundationDbError,
  ) => Effect.Effect<void, FoundationDbError>;
}

export class NativeDriver
  extends Context.Service<NativeDriver, NativeDriverShape>()(
    "@effect-foundationdb/NativeDriver",
  ) {}
