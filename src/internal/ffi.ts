import { Effect, Layer, Scope } from "effect";
import { FoundationDbError } from "../errors.ts";
import { ConflictRange, KeyValue, StreamingMode } from "../model.ts";
import type { Bytes } from "../model.ts";
import { NativeDriver } from "./native.ts";
import type { NativeDriverShape, RangeBatch } from "./native.ts";

const encoder = new TextEncoder();
const RESULT_PRESENT = 1;
const RESULT_MORE = 2;
const FFI_FAILURE = 90_007;

const asFfiBuffer = (bytes: Bytes): Uint8Array<ArrayBuffer> =>
  bytes.buffer instanceof ArrayBuffer
    ? bytes as Uint8Array<ArrayBuffer>
    : new Uint8Array(bytes);

const symbols = {
  fdb_rs_init: { parameters: ["i32"], result: "i32" },
  fdb_rs_shutdown: { parameters: [], result: "i32", nonblocking: true },
  fdb_rs_database_open: {
    parameters: ["buffer", "usize", "u8"],
    result: "pointer",
  },
  fdb_rs_database_close: { parameters: ["u64"], result: "i32" },
  fdb_rs_transaction_open: { parameters: ["u64"], result: "pointer" },
  fdb_rs_transaction_close: { parameters: ["u64"], result: "i32" },
  fdb_rs_transaction_set_option: {
    parameters: ["u64", "u8", "i32"],
    result: "i32",
  },
  fdb_rs_transaction_get: {
    parameters: ["u64", "buffer", "usize", "u8", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_transaction_get_many: {
    parameters: ["u64", "buffer", "usize", "u8", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_transaction_get_key: {
    parameters: [
      "u64",
      "buffer",
      "usize",
      "u8",
      "i32",
      "u8",
      "pointer",
      "u64",
    ],
    result: "i32",
  },
  fdb_rs_transaction_set: {
    parameters: ["u64", "buffer", "usize", "buffer", "usize"],
    result: "i32",
  },
  fdb_rs_transaction_atomic_op: {
    parameters: ["u64", "buffer", "usize", "buffer", "usize", "i32"],
    result: "i32",
  },
  fdb_rs_transaction_set_without_write_conflict: {
    parameters: ["u64", "buffer", "usize", "buffer", "usize"],
    result: "i32",
  },
  fdb_rs_transaction_clear: {
    parameters: ["u64", "buffer", "usize"],
    result: "i32",
  },
  fdb_rs_transaction_clear_range: {
    parameters: ["u64", "buffer", "usize", "buffer", "usize"],
    result: "i32",
  },
  fdb_rs_transaction_clear_range_without_write_conflict: {
    parameters: ["u64", "buffer", "usize", "buffer", "usize"],
    result: "i32",
  },
  fdb_rs_transaction_add_conflict_range: {
    parameters: ["u64", "buffer", "usize", "buffer", "usize", "u8"],
    result: "i32",
  },
  fdb_rs_transaction_get_read_version: {
    parameters: ["u64", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_transaction_set_read_version: {
    parameters: ["u64", "i64"],
    result: "i32",
  },
  fdb_rs_transaction_get_approximate_size: {
    parameters: ["u64", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_transaction_watch: {
    parameters: ["u64", "buffer", "usize", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_transaction_get_versionstamp: {
    parameters: ["u64", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_transaction_get_conflicting_key_ranges: {
    parameters: ["u64"],
    result: "pointer",
  },
  fdb_rs_range_open: {
    parameters: [
      "u64",
      "buffer",
      "usize",
      "u8",
      "i32",
      "buffer",
      "usize",
      "u8",
      "i32",
      "i32",
      "i32",
      "i32",
      "u8",
      "u8",
    ],
    result: "pointer",
  },
  fdb_rs_range_next: {
    parameters: ["u64", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_range_close: { parameters: ["u64"], result: "i32" },
  fdb_rs_transaction_commit: {
    parameters: ["u64", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_transaction_on_error: {
    parameters: ["u64", "i32", "pointer", "u64"],
    result: "i32",
  },
  fdb_rs_async_take: { parameters: ["u64"], result: "pointer" },
  fdb_rs_async_cancel: { parameters: ["u64"], result: "i32" },
  fdb_rs_result_code: { parameters: ["pointer"], result: "i32" },
  fdb_rs_result_flags: { parameters: ["pointer"], result: "u32" },
  fdb_rs_result_handle: { parameters: ["pointer"], result: "u64" },
  fdb_rs_result_data: { parameters: ["pointer"], result: "pointer" },
  fdb_rs_result_length: { parameters: ["pointer"], result: "usize" },
  fdb_rs_result_free: { parameters: ["pointer"], result: "void" },
  fdb_rs_error_message: { parameters: ["i32"], result: "pointer" },
  fdb_rs_string_free: { parameters: ["pointer"], result: "void" },
  fdb_rs_error_is_retryable: { parameters: ["i32"], result: "u8" },
  fdb_rs_error_is_maybe_committed: { parameters: ["i32"], result: "u8" },
  fdb_rs_error_is_retryable_not_committed: {
    parameters: ["i32"],
    result: "u8",
  },
} as const satisfies Deno.ForeignLibraryInterface;

type Library = Deno.DynamicLibrary<typeof symbols>;
type NativeResult = Deno.PointerObject<unknown>;
let nextRequestId = 1n;

export interface FfiOptions {
  readonly libraryPath: string;
  readonly apiVersion?: number;
}

const fallbackError = (operation: string, cause: unknown): FoundationDbError =>
  new FoundationDbError({
    operation,
    code: FFI_FAILURE,
    message: cause instanceof Error ? cause.message : String(cause),
    retryable: false,
    maybeCommitted: false,
    retryableNotCommitted: false,
  });

const encodeKeys = (
  keys: ReadonlyArray<Bytes>,
): Uint8Array<ArrayBuffer> => {
  if (keys.length > 0xffff_ffff) {
    throw new Error("native key batch contains too many keys");
  }
  let length = 4;
  for (const key of keys) {
    if (key.byteLength > 0xffff_ffff) {
      throw new Error("native key batch contains an oversized key");
    }
    length += 4 + key.byteLength;
  }

  const bytes = new Uint8Array(length);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, keys.length, true);
  let offset = 4;
  for (const key of keys) {
    view.setUint32(offset, key.byteLength, true);
    offset += 4;
    bytes.set(key, offset);
    offset += key.byteLength;
  }
  return bytes;
};

const decodeOptionalValues = (
  bytes: Uint8Array,
): ReadonlyArray<Uint8Array | undefined> => {
  if (bytes.byteLength < 4) {
    throw new Error("native value batch is truncated");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(0, true);
  const values: Array<Uint8Array | undefined> = [];
  let offset = 4;
  for (let index = 0; index < count; index++) {
    if (offset + 4 > bytes.byteLength) {
      throw new Error("native value batch is truncated");
    }
    const length = view.getUint32(offset, true);
    offset += 4;
    if (length === 0xffff_ffff) {
      values.push(undefined);
      continue;
    }
    if (offset + length > bytes.byteLength) {
      throw new Error("native value batch is truncated");
    }
    values.push(bytes.slice(offset, offset + length));
    offset += length;
  }
  if (offset !== bytes.byteLength) {
    throw new Error("native value batch has trailing bytes");
  }
  return values;
};

const decodeKeyValues = (bytes: Uint8Array): ReadonlyArray<KeyValue> => {
  if (bytes.byteLength < 4) {
    throw new Error("native range response is truncated");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(0, true);
  const values: Array<KeyValue> = [];
  let offset = 4;
  for (let index = 0; index < count; index++) {
    if (offset + 8 > bytes.byteLength) {
      throw new Error("native range response is truncated");
    }
    const keyLength = view.getUint32(offset, true);
    const valueLength = view.getUint32(offset + 4, true);
    offset += 8;
    if (offset + keyLength + valueLength > bytes.byteLength) {
      throw new Error("native range response is truncated");
    }
    const key = bytes.slice(offset, offset + keyLength);
    offset += keyLength;
    const value = bytes.slice(offset, offset + valueLength);
    offset += valueLength;
    values.push(new KeyValue({ key, value }));
  }
  if (offset !== bytes.byteLength) {
    throw new Error("native range response has trailing bytes");
  }
  return values;
};

const decodeI64 = (bytes: Uint8Array): bigint => {
  if (bytes.byteLength !== 8) {
    throw new Error("native 64-bit integer has an invalid length");
  }
  return new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength,
  ).getBigInt64(0, true);
};

const decodeConflictRanges = (
  bytes: Uint8Array,
): ReadonlyArray<ConflictRange> => {
  if (bytes.byteLength < 4) {
    throw new Error("native conflict range response is truncated");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(0, true);
  const ranges: Array<ConflictRange> = [];
  let offset = 4;
  for (let index = 0; index < count; index++) {
    if (offset + 8 > bytes.byteLength) {
      throw new Error("native conflict range response is truncated");
    }
    const beginLength = view.getUint32(offset, true);
    const endLength = view.getUint32(offset + 4, true);
    offset += 8;
    if (offset + beginLength + endLength > bytes.byteLength) {
      throw new Error("native conflict range response is truncated");
    }
    const begin = bytes.slice(offset, offset + beginLength);
    offset += beginLength;
    const end = bytes.slice(offset, offset + endLength);
    offset += endLength;
    ranges.push(new ConflictRange({ begin, end }));
  }
  if (offset !== bytes.byteLength) {
    throw new Error("native conflict range response has trailing bytes");
  }
  return ranges;
};

const makeDriver = (
  library: Library,
): {
  readonly driver: NativeDriverShape;
  readonly drain: () => Promise<void>;
  readonly close: () => void;
} => {
  const error = (operation: string, code: number): FoundationDbError => {
    const pointer = library.symbols.fdb_rs_error_message(code);
    let message = `FoundationDB error ${code}`;
    if (pointer !== null) {
      try {
        message = new Deno.UnsafePointerView(pointer).getCString();
      } finally {
        library.symbols.fdb_rs_string_free(pointer);
      }
    }
    return new FoundationDbError({
      operation,
      code,
      message,
      retryable: library.symbols.fdb_rs_error_is_retryable(code) !== 0,
      maybeCommitted:
        library.symbols.fdb_rs_error_is_maybe_committed(code) !== 0,
      retryableNotCommitted:
        library.symbols.fdb_rs_error_is_retryable_not_committed(code) !== 0,
    });
  };

  const checkCode = (
    operation: string,
    value: number,
  ): Effect.Effect<void, FoundationDbError> =>
    value === 0 ? Effect.void : Effect.fail(error(operation, value));

  const syncCode = (
    operation: string,
    evaluate: () => number,
  ): Effect.Effect<void, FoundationDbError> =>
    Effect.try({
      try: evaluate,
      catch: (cause) => fallbackError(operation, cause),
    }).pipe(
      Effect.flatMap((value) => checkCode(operation, value)),
    );

  const readResult = (
    operation: string,
    pointer: NativeResult,
  ): {
    readonly flags: number;
    readonly handle: bigint;
    readonly data: Uint8Array;
  } => {
    try {
      const code = library.symbols.fdb_rs_result_code(pointer);
      if (code !== 0) {
        throw error(operation, code);
      }
      const flags = library.symbols.fdb_rs_result_flags(pointer);
      const handle = library.symbols.fdb_rs_result_handle(pointer);
      const length = Number(library.symbols.fdb_rs_result_length(pointer));
      const dataPointer = library.symbols.fdb_rs_result_data(pointer);
      const data = length === 0 ? new Uint8Array() : new Uint8Array(
        new Deno.UnsafePointerView(dataPointer!).getArrayBuffer(length),
      ).slice();
      return { flags, handle, data };
    } finally {
      library.symbols.fdb_rs_result_free(pointer);
    }
  };

  const syncResult = <A>(
    operation: string,
    evaluate: () => Deno.PointerValue<unknown>,
    project: (result: ReturnType<typeof readResult>) => A,
  ): Effect.Effect<A, FoundationDbError> =>
    Effect.try({
      try: () => {
        const pointer = evaluate();
        if (pointer === null) {
          throw new Error("native bridge returned a null result");
        }
        return project(readResult(operation, pointer));
      },
      catch: (cause) =>
        cause instanceof FoundationDbError
          ? cause
          : fallbackError(operation, cause),
    });

  interface PendingOperation {
    cancelled: boolean;
    settled: boolean;
    readonly done: Promise<void>;
    readonly complete: () => void;
  }

  let closing = false;
  const pending = new Map<bigint, PendingOperation>();
  const completionCallback = Deno.UnsafeCallback.threadSafe(
    { parameters: ["u64"], result: "void" } as const,
    (requestId: bigint) =>
      queueMicrotask(() => pending.get(requestId)?.complete()),
  );
  completionCallback.unref();

  const asyncResult = <A>(
    operation: string,
    start: (
      callback: Deno.PointerObject<unknown>,
      requestId: bigint,
    ) => number,
    project: (result: ReturnType<typeof readResult>) => A,
  ): Effect.Effect<A, FoundationDbError> =>
    Effect.callback((resume) => {
      if (closing) {
        resume(
          Effect.fail(fallbackError(operation, "native driver is closed")),
        );
        return Effect.void;
      }
      const requestId = nextRequestId++;
      let resolveDone!: () => void;
      const done = new Promise<void>((resolve) => {
        resolveDone = resolve;
      });

      const finish = (
        pendingOperation: PendingOperation,
        effect: Effect.Effect<A, FoundationDbError>,
      ) => {
        if (pendingOperation.settled) {
          return;
        }
        pendingOperation.settled = true;
        const removed = pending.delete(requestId);
        if (removed && pending.size === 0) {
          completionCallback.unref();
        }
        resolveDone();
        if (!pendingOperation.cancelled) {
          resume(effect);
        }
      };

      const pendingOperation: PendingOperation = {
        cancelled: false,
        settled: false,
        done,
        complete: () => {
          const pointer = library.symbols.fdb_rs_async_take(requestId);
          if (pointer === null) {
            return;
          }
          let effect: Effect.Effect<A, FoundationDbError>;
          try {
            effect = Effect.succeed(project(readResult(operation, pointer)));
          } catch (cause) {
            effect = Effect.fail(
              cause instanceof FoundationDbError
                ? cause
                : fallbackError(operation, cause),
            );
          }
          finish(pendingOperation, effect);
        },
      };

      const shouldRefCallback = pending.size === 0;
      pending.set(requestId, pendingOperation);
      if (shouldRefCallback) {
        completionCallback.ref();
      }
      try {
        const code = start(completionCallback.pointer, requestId);
        if (code !== 0) {
          finish(pendingOperation, Effect.fail(error(operation, code)));
        }
      } catch (cause) {
        finish(
          pendingOperation,
          Effect.fail(fallbackError(operation, cause)),
        );
      }

      return Effect.promise(() => {
        if (!pendingOperation.settled) {
          pendingOperation.cancelled = true;
          library.symbols.fdb_rs_async_cancel(requestId);
        }
        return pendingOperation.done;
      });
    });

  const asyncCode = (
    operation: string,
    start: (
      callback: Deno.PointerObject<unknown>,
      requestId: bigint,
    ) => number,
  ): Effect.Effect<void, FoundationDbError> =>
    asyncResult(operation, start, () => undefined);

  const driver: NativeDriverShape = {
    openDatabase: (clusterFile) => {
      const path = clusterFile === undefined
        ? new Uint8Array()
        : encoder.encode(clusterFile);
      return syncResult(
        "Database.open",
        () =>
          library.symbols.fdb_rs_database_open(
            path,
            BigInt(path.length),
            clusterFile === undefined ? 0 : 1,
          ),
        ({ handle }) => handle,
      );
    },
    closeDatabase: (handle) =>
      syncCode(
        "Database.close",
        () => library.symbols.fdb_rs_database_close(handle),
      ),
    openTransaction: (database) =>
      syncResult(
        "Transaction.open",
        () => library.symbols.fdb_rs_transaction_open(database),
        ({ handle }) => handle,
      ),
    closeTransaction: (handle) =>
      syncCode(
        "Transaction.close",
        () => library.symbols.fdb_rs_transaction_close(handle),
      ),
    setTransactionOption: (handle, option, value) => {
      const code = option === "timeout"
        ? 0
        : option === "retryLimit"
        ? 1
        : option === "maxRetryDelay"
        ? 2
        : 3;
      return syncCode(
        `Transaction.${option}`,
        () =>
          library.symbols.fdb_rs_transaction_set_option(handle, code, value),
      );
    },
    get: (handle, key, snapshot) =>
      asyncResult(
        "Transaction.get",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_get(
            handle,
            asFfiBuffer(key),
            BigInt(key.length),
            snapshot ? 1 : 0,
            callback,
            requestId,
          ),
        ({ data, flags }) => (flags & RESULT_PRESENT) === 0 ? undefined : data,
      ),
    getMany: (handle, keys, snapshot) =>
      Effect.try({
        try: () => encodeKeys(keys),
        catch: (cause) => fallbackError("Transaction.getMany", cause),
      }).pipe(
        Effect.flatMap((encoded) =>
          asyncResult(
            "Transaction.getMany",
            (callback, requestId) =>
              library.symbols.fdb_rs_transaction_get_many(
                handle,
                encoded,
                BigInt(encoded.length),
                snapshot ? 1 : 0,
                callback,
                requestId,
              ),
            ({ data }) => decodeOptionalValues(data),
          )
        ),
      ),
    getKey: (handle, selector, snapshot) =>
      asyncResult(
        "Transaction.getKey",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_get_key(
            handle,
            asFfiBuffer(selector.key),
            BigInt(selector.key.length),
            selector.orEqual ? 1 : 0,
            selector.offset,
            snapshot ? 1 : 0,
            callback,
            requestId,
          ),
        ({ data }) => data,
      ),
    set: (handle, key, value) =>
      syncCode(
        "Transaction.set",
        () =>
          library.symbols.fdb_rs_transaction_set(
            handle,
            asFfiBuffer(key),
            BigInt(key.length),
            asFfiBuffer(value),
            BigInt(value.length),
          ),
      ),
    atomicOp: (handle, key, value, mutationType) =>
      syncCode(
        "Transaction.atomicOp",
        () =>
          library.symbols.fdb_rs_transaction_atomic_op(
            handle,
            asFfiBuffer(key),
            BigInt(key.length),
            asFfiBuffer(value),
            BigInt(value.length),
            mutationType,
          ),
      ),
    setWithoutWriteConflict: (handle, key, value) =>
      syncCode(
        "Transaction.setWithoutWriteConflict",
        () =>
          library.symbols.fdb_rs_transaction_set_without_write_conflict(
            handle,
            asFfiBuffer(key),
            BigInt(key.length),
            asFfiBuffer(value),
            BigInt(value.length),
          ),
      ),
    clear: (handle, key) =>
      syncCode(
        "Transaction.clear",
        () =>
          library.symbols.fdb_rs_transaction_clear(
            handle,
            asFfiBuffer(key),
            BigInt(key.length),
          ),
      ),
    clearRange: (handle, begin, end) =>
      syncCode(
        "Transaction.clearRange",
        () =>
          library.symbols.fdb_rs_transaction_clear_range(
            handle,
            asFfiBuffer(begin),
            BigInt(begin.length),
            asFfiBuffer(end),
            BigInt(end.length),
          ),
      ),
    clearRangeWithoutWriteConflict: (handle, begin, end) =>
      syncCode(
        "Transaction.clearRangeWithoutWriteConflict",
        () =>
          library.symbols.fdb_rs_transaction_clear_range_without_write_conflict(
            handle,
            asFfiBuffer(begin),
            BigInt(begin.length),
            asFfiBuffer(end),
            BigInt(end.length),
          ),
      ),
    addConflictRange: (handle, begin, end, conflictType) =>
      syncCode(
        "Transaction.addConflictRange",
        () =>
          library.symbols.fdb_rs_transaction_add_conflict_range(
            handle,
            asFfiBuffer(begin),
            BigInt(begin.length),
            asFfiBuffer(end),
            BigInt(end.length),
            conflictType,
          ),
      ),
    getReadVersion: (handle) =>
      asyncResult(
        "Transaction.getReadVersion",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_get_read_version(
            handle,
            callback,
            requestId,
          ),
        ({ data }) => decodeI64(data),
      ),
    setReadVersion: (handle, version) =>
      syncCode(
        "Transaction.setReadVersion",
        () =>
          library.symbols.fdb_rs_transaction_set_read_version(handle, version),
      ),
    getApproximateSize: (handle) =>
      asyncResult(
        "Transaction.getApproximateSize",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_get_approximate_size(
            handle,
            callback,
            requestId,
          ),
        ({ data }) => decodeI64(data),
      ),
    watch: (handle, key) =>
      asyncCode(
        "Transaction.watch",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_watch(
            handle,
            asFfiBuffer(key),
            BigInt(key.length),
            callback,
            requestId,
          ),
      ),
    getVersionstamp: (handle) =>
      asyncResult(
        "Transaction.getVersionstamp",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_get_versionstamp(
            handle,
            callback,
            requestId,
          ),
        ({ data }) => data,
      ),
    getConflictingKeyRanges: (handle) =>
      syncResult(
        "Transaction.getConflictingKeyRanges",
        () =>
          library.symbols.fdb_rs_transaction_get_conflicting_key_ranges(handle),
        ({ data }) => decodeConflictRanges(data),
      ),
    openRange: (handle, options) =>
      syncResult(
        "Transaction.getRange.open",
        () =>
          library.symbols.fdb_rs_range_open(
            handle,
            asFfiBuffer(options.begin.key),
            BigInt(options.begin.key.length),
            options.begin.orEqual ? 1 : 0,
            options.begin.offset,
            asFfiBuffer(options.end.key),
            BigInt(options.end.key.length),
            options.end.orEqual ? 1 : 0,
            options.end.offset,
            options.limit ?? 0,
            options.targetBytes ?? 0,
            options.mode ?? StreamingMode.Iterator,
            options.reverse === true ? 1 : 0,
            options.snapshot === true ? 1 : 0,
          ),
        ({ handle }) => handle,
      ),
    nextRange: (handle): Effect.Effect<RangeBatch, FoundationDbError> =>
      asyncResult(
        "Transaction.getRange.next",
        (callback, requestId) =>
          library.symbols.fdb_rs_range_next(handle, callback, requestId),
        ({ data, flags }) => ({
          values: decodeKeyValues(data),
          more: (flags & RESULT_MORE) !== 0,
        }),
      ),
    closeRange: (handle) =>
      syncCode(
        "Transaction.getRange.close",
        () => library.symbols.fdb_rs_range_close(handle),
      ),
    commit: (handle) =>
      asyncResult(
        "Transaction.commit",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_commit(
            handle,
            callback,
            requestId,
          ),
        ({ data }) => decodeI64(data),
      ),
    onError: (handle, failure) =>
      asyncCode(
        "Transaction.onError",
        (callback, requestId) =>
          library.symbols.fdb_rs_transaction_on_error(
            handle,
            failure.code,
            callback,
            requestId,
          ),
      ),
  };

  const drain = async (): Promise<void> => {
    closing = true;
    const active = Array.from(pending, ([requestId, operation]) => ({
      requestId,
      operation,
    }));
    for (const { requestId, operation } of active) {
      if (!operation.settled) {
        library.symbols.fdb_rs_async_cancel(requestId);
      }
    }
    await Promise.all(active.map(({ operation }) => operation.done));
  };

  return { driver, drain, close: () => completionCallback.close() };
};

const acquireDriverLifecycle = <A, E, R>(
  initialize: Effect.Effect<void, E, R>,
  shutdown: Effect.Effect<void>,
  open: Effect.Effect<A, E, R>,
  drain: (resource: A) => Effect.Effect<void>,
  close: (resource: A) => Effect.Effect<void>,
): Effect.Effect<A, E, R | Scope.Scope> =>
  Effect.gen(function* () {
    const resource = yield* Effect.acquireRelease(open, close);
    yield* Effect.acquireRelease(initialize, () => shutdown);
    yield* Effect.acquireRelease(Effect.void, () => drain(resource));
    return resource;
  });

export const ffiLayer = (
  options: FfiOptions,
): Layer.Layer<NativeDriver, FoundationDbError> =>
  Layer.effect(
    NativeDriver,
    Effect.gen(function* () {
      const library = yield* Effect.acquireRelease(
        Effect.try({
          try: () => Deno.dlopen(options.libraryPath, symbols),
          catch: (cause) => fallbackError("NativeLibrary.open", cause),
        }),
        (library) => Effect.sync(() => library.close()),
      );
      const driverResource = yield* acquireDriverLifecycle(
        syncInit(library, options.apiVersion ?? 740),
        asyncCodeForLibrary("FoundationDb.shutdown", () =>
          library.symbols.fdb_rs_shutdown()).pipe(Effect.orDie),
        Effect.try({
          try: () =>
            makeDriver(library),
          catch: (cause) => fallbackError("NativeCallback.open", cause),
        }),
        (resource) => Effect.promise(resource.drain),
        (resource) => Effect.sync(resource.close),
      );
      return driverResource.driver;
    }),
  );

const syncInit = (
  library: Library,
  apiVersion: number,
): Effect.Effect<void, FoundationDbError> =>
  Effect.try({
    try: () => library.symbols.fdb_rs_init(apiVersion),
    catch: (cause) => fallbackError("FoundationDb.init", cause),
  }).pipe(
    Effect.flatMap((code) =>
      code === 0 ? Effect.void : Effect.fail(
        new FoundationDbError({
          operation: "FoundationDb.init",
          code,
          message: `FoundationDB initialization failed (${code})`,
          retryable: false,
          maybeCommitted: false,
          retryableNotCommitted: false,
        }),
      )
    ),
  );

const asyncCodeForLibrary = (
  operation: string,
  evaluate: () => Promise<number>,
): Effect.Effect<void, FoundationDbError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => fallbackError(operation, cause),
  }).pipe(
    Effect.uninterruptible,
    Effect.flatMap((code) =>
      code === 0
        ? Effect.void
        : Effect.fail(fallbackError(operation, `native error ${code}`))
    ),
  );

export const internal = {
  acquireDriverLifecycle,
  decodeConflictRanges,
  decodeI64,
  decodeKeyValues,
  decodeOptionalValues,
  encodeKeys,
};
