import { Cause, Context, Effect, Layer, Option, Scope, Stream } from "effect";
import { FoundationDbError, isFoundationDbError } from "./errors.ts";
import { ffiLayer } from "./internal/ffi.ts";
import type { FfiOptions } from "./internal/ffi.ts";
import { NativeDriver } from "./internal/native.ts";
import type {
  DatabaseHandle,
  NativeDriverShape,
  TransactionHandle,
} from "./internal/native.ts";
import type {
  Bytes,
  KeySelector,
  KeyValue,
  RangeOptions,
  TransactionAttempt,
  TransactionOptions,
} from "./model.ts";

export interface FoundationDbTransactionShape extends TransactionAttempt {
  readonly get: (
    key: Bytes,
    options?: { readonly snapshot?: boolean },
  ) => Effect.Effect<Uint8Array | undefined, FoundationDbError>;
  /** Reads all keys concurrently through one native bridge operation. */
  readonly getMany: (
    keys: ReadonlyArray<Bytes>,
    options?: { readonly snapshot?: boolean },
  ) => Effect.Effect<
    ReadonlyArray<Uint8Array | undefined>,
    FoundationDbError
  >;
  readonly getKey: (
    selector: KeySelector,
    options?: { readonly snapshot?: boolean },
  ) => Effect.Effect<Uint8Array, FoundationDbError>;
  readonly set: (
    key: Bytes,
    value: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly atomicAdd: (
    key: Bytes,
    value: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly setWithoutWriteConflict: (
    key: Bytes,
    value: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly clear: (key: Bytes) => Effect.Effect<void, FoundationDbError>;
  readonly clearRange: (
    begin: Bytes,
    end: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly clearRangeWithoutWriteConflict: (
    begin: Bytes,
    end: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly addWriteConflictRange: (
    begin: Bytes,
    end: Bytes,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly getRange: (
    options: RangeOptions,
  ) => Stream.Stream<KeyValue, FoundationDbError>;
}

export class FoundationDbTransaction extends Context.Service<
  FoundationDbTransaction,
  FoundationDbTransactionShape
>()(
  "@effect-foundationdb/FoundationDbTransaction",
) {}

type TransactionRequirements<R> = Exclude<
  Exclude<R, FoundationDbTransaction>,
  Scope.Scope
>;

export interface FoundationDbShape {
  readonly withTransaction: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    options?: TransactionOptions,
  ) => Effect.Effect<
    A,
    E | FoundationDbError,
    TransactionRequirements<R>
  >;
  readonly get: (
    key: Bytes,
    options?: {
      readonly snapshot?: boolean;
      readonly transaction?: TransactionOptions;
    },
  ) => Effect.Effect<Uint8Array | undefined, FoundationDbError>;
  /** Reads all keys in one retried transaction and one native bridge operation. */
  readonly getMany: (
    keys: ReadonlyArray<Bytes>,
    options?: {
      readonly snapshot?: boolean;
      readonly transaction?: TransactionOptions;
    },
  ) => Effect.Effect<
    ReadonlyArray<Uint8Array | undefined>,
    FoundationDbError
  >;
  readonly set: (
    key: Bytes,
    value: Bytes,
    options?: TransactionOptions,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly clear: (
    key: Bytes,
    options?: TransactionOptions,
  ) => Effect.Effect<void, FoundationDbError>;
  readonly clearRange: (
    begin: Bytes,
    end: Bytes,
    options?: TransactionOptions,
  ) => Effect.Effect<void, FoundationDbError>;
  /**
   * Runs and retries the complete range read and returns its buffered result.
   * Use `FoundationDbTransaction.getRange` inside `withTransaction` when rows
   * must be processed incrementally.
   */
  readonly getRange: (
    options: RangeOptions,
    transactionOptions?: TransactionOptions,
  ) => Effect.Effect<ReadonlyArray<KeyValue>, FoundationDbError>;
}

export interface FoundationDbOptions extends FfiOptions {
  readonly clusterFile?: string;
  /** Applied to every transaction unless a call overrides individual fields. */
  readonly transactionDefaults?: TransactionOptions;
}

const releaseOrDie = (
  effect: Effect.Effect<void, FoundationDbError>,
): Effect.Effect<void> => effect.pipe(Effect.orDie);

const singleFoundationDbFailure = <E>(
  cause: Cause.Cause<E | FoundationDbError>,
): FoundationDbError | undefined => {
  if (cause.reasons.length !== 1) {
    return undefined;
  }
  const reason = cause.reasons[0];
  return Cause.isFailReason(reason) && isFoundationDbError(reason.error)
    ? reason.error
    : undefined;
};

const preserveMaybeCommitted = (
  error: FoundationDbError,
  maybeCommitted: boolean,
): FoundationDbError =>
  !maybeCommitted || error.maybeCommitted ? error : new FoundationDbError({
    operation: error.operation,
    code: error.code,
    message: error.message,
    retryable: error.retryable,
    maybeCommitted: true,
    retryableNotCommitted: false,
  });

interface TransactionTelemetry {
  attempts: number;
  maybeCommitted: boolean;
  mutationCount: number;
  mutationBytes: number;
}

interface AttemptTelemetry {
  mutationCount: number;
  mutationBytes: number;
}

const traceClientOperation = <A, E, R>(
  name: string,
  operation: string,
  effect: Effect.Effect<A, E, R>,
  attributes: Record<string, unknown> = {},
): Effect.Effect<A, E, R> =>
  Effect.withSpan(effect, name, {
    kind: "client",
    captureStackTrace: false,
    attributes: {
      "db.system.name": "foundationdb",
      "db.operation.name": operation,
      ...attributes,
    },
  });

const makeTransaction = (
  driver: NativeDriverShape,
  handle: TransactionHandle,
  context: TransactionAttempt,
  attemptTelemetry: AttemptTelemetry,
  transactionTelemetry: TransactionTelemetry,
): FoundationDbTransactionShape => {
  const mutation = (
    effect: Effect.Effect<void, FoundationDbError>,
    bytes: number,
  ): Effect.Effect<void, FoundationDbError> =>
    Effect.tap(effect, () =>
      Effect.sync(() => {
        attemptTelemetry.mutationCount += 1;
        attemptTelemetry.mutationBytes += bytes;
        transactionTelemetry.mutationCount += 1;
        transactionTelemetry.mutationBytes += bytes;
      }));

  return {
    ...context,
    get: (key, options) =>
      traceClientOperation(
        "FoundationDb.Transaction.get",
        "get",
        driver.get(handle, key, options?.snapshot === true),
        { "db.foundationdb.read.snapshot": options?.snapshot === true },
      ),
    getMany: (keys, options) =>
      traceClientOperation(
        "FoundationDb.Transaction.getMany",
        "get_many",
        driver.getMany(handle, keys, options?.snapshot === true),
        {
          "db.foundationdb.read.snapshot": options?.snapshot === true,
          "db.foundationdb.read.key_count": keys.length,
        },
      ),
    getKey: (selector, options) =>
      traceClientOperation(
        "FoundationDb.Transaction.getKey",
        "get_key",
        driver.getKey(handle, selector, options?.snapshot === true),
        { "db.foundationdb.read.snapshot": options?.snapshot === true },
      ),
    set: (key, value) =>
      mutation(
        driver.set(handle, key, value),
        key.byteLength + value.byteLength,
      ),
    atomicAdd: (key, value) =>
      mutation(
        driver.atomicAdd(handle, key, value),
        key.byteLength + value.byteLength,
      ),
    setWithoutWriteConflict: (key, value) =>
      mutation(
        driver.setWithoutWriteConflict(handle, key, value),
        key.byteLength + value.byteLength,
      ),
    clear: (key) => mutation(driver.clear(handle, key), key.byteLength),
    clearRange: (begin, end) =>
      mutation(
        driver.clearRange(handle, begin, end),
        begin.byteLength + end.byteLength,
      ),
    clearRangeWithoutWriteConflict: (begin, end) =>
      mutation(
        driver.clearRangeWithoutWriteConflict(handle, begin, end),
        begin.byteLength + end.byteLength,
      ),
    addWriteConflictRange: (begin, end) =>
      driver.addWriteConflictRange(handle, begin, end),
    getRange: (options) =>
      Stream.unwrap(
        Effect.acquireRelease(
          driver.openRange(handle, options),
          (range) => releaseOrDie(driver.closeRange(range)),
        ).pipe(
          Effect.map((range) => {
            let page = 0;
            return Stream.paginate(true, (more) => {
              if (!more) {
                return Effect.succeed([[], Option.none<boolean>()] as const);
              }
              page += 1;
              return traceClientOperation(
                "FoundationDb.Transaction.getRangePage",
                "get_range",
                driver.nextRange(range),
                { "db.foundationdb.range.page": page },
              ).pipe(
                Effect.map((batch) =>
                  [batch.values, Option.some(batch.more)] as const
                ),
              );
            });
          }),
        ),
      ),
  };
};

const setOptions = (
  driver: NativeDriverShape,
  handle: TransactionHandle,
  options: TransactionOptions,
): Effect.Effect<void, FoundationDbError> =>
  Effect.gen(function* () {
    if (options.timeoutMs !== undefined) {
      yield* driver.setTransactionOption(handle, "timeout", options.timeoutMs);
    }
    if (options.retryLimit !== undefined) {
      yield* driver.setTransactionOption(
        handle,
        "retryLimit",
        options.retryLimit,
      );
    }
    if (options.maxRetryDelayMs !== undefined) {
      yield* driver.setTransactionOption(
        handle,
        "maxRetryDelay",
        options.maxRetryDelayMs,
      );
    }
  });

const makeDatabase = (
  driver: NativeDriverShape,
  database: DatabaseHandle,
  transactionDefaults: TransactionOptions = {},
): FoundationDbShape => {
  const withTransaction: FoundationDbShape["withTransaction"] = Effect.fn(
    "FoundationDb.withTransaction",
  )(
    <A, E, R>(
      effect: Effect.Effect<A, E, R>,
      options: TransactionOptions = {},
    ): Effect.Effect<
      A,
      E | FoundationDbError,
      TransactionRequirements<R>
    > => {
      const transactionTelemetry: TransactionTelemetry = {
        attempts: 0,
        maybeCommitted: false,
        mutationCount: 0,
        mutationBytes: 0,
      };

      return Effect.acquireUseRelease(
        driver.openTransaction(database),
        (handle) =>
          Effect.gen(function* () {
            yield* setOptions(driver, handle, {
              ...transactionDefaults,
              ...options,
            });

            const attempt = (
              attemptNumber: number,
              maybeCommitted: boolean,
            ): Effect.Effect<
              A,
              E | FoundationDbError,
              TransactionRequirements<R>
            > =>
              Effect.suspend(() => {
                transactionTelemetry.attempts = attemptNumber;
                transactionTelemetry.maybeCommitted = maybeCommitted;
                const attemptTelemetry: AttemptTelemetry = {
                  mutationCount: 0,
                  mutationBytes: 0,
                };

                const runAttempt = Effect.scoped(Effect.provideService(
                  effect,
                  FoundationDbTransaction,
                  makeTransaction(
                    driver,
                    handle,
                    {
                      attempt: attemptNumber,
                      maybeCommitted,
                    },
                    attemptTelemetry,
                    transactionTelemetry,
                  ),
                )).pipe(
                  Effect.flatMap((value) =>
                    traceClientOperation(
                      "FoundationDb.Transaction.commit",
                      "commit",
                      driver.commit(handle),
                      {
                        "db.foundationdb.transaction.attempt": attemptNumber,
                      },
                    ).pipe(Effect.as(value))
                  ),
                  Effect.tapCause((cause) => {
                    const failure = singleFoundationDbFailure(cause);
                    return failure === undefined
                      ? Effect.void
                      : Effect.annotateCurrentSpan({
                        "error.type": String(failure.code),
                        "db.foundationdb.error.code": failure.code,
                        "db.foundationdb.error.retryable": failure.retryable,
                        "db.foundationdb.error.maybe_committed":
                          failure.maybeCommitted,
                      });
                  }),
                  Effect.ensuring(Effect.suspend(() =>
                    Effect.annotateCurrentSpan({
                      "db.foundationdb.mutation.count":
                        attemptTelemetry.mutationCount,
                      "db.foundationdb.mutation.bytes":
                        attemptTelemetry.mutationBytes,
                    })
                  )),
                  Effect.withSpan(
                    "FoundationDb.Transaction.attempt",
                    {
                      attributes: {
                        "db.system.name": "foundationdb",
                        "db.operation.name": "transaction",
                        "db.foundationdb.transaction.attempt": attemptNumber,
                        "db.foundationdb.transaction.maybe_committed":
                          maybeCommitted,
                      },
                    },
                    { captureStackTrace: false },
                  ),
                );

                return Effect.matchCauseEffect(runAttempt, {
                  onFailure: (cause) => {
                    const failure = singleFoundationDbFailure(cause);
                    if (failure === undefined) {
                      return Effect.failCause(cause);
                    }
                    const nextMaybeCommitted = maybeCommitted ||
                      failure.maybeCommitted;
                    transactionTelemetry.maybeCommitted = nextMaybeCommitted;
                    return traceClientOperation(
                      "FoundationDb.Transaction.onError",
                      "on_error",
                      driver.onError(handle, failure),
                      {
                        "db.foundationdb.transaction.attempt": attemptNumber,
                        "db.foundationdb.retry.error_code": failure.code,
                        "db.foundationdb.retry.maybe_committed":
                          nextMaybeCommitted,
                      },
                    ).pipe(
                      Effect.mapError((error) =>
                        preserveMaybeCommitted(error, nextMaybeCommitted)
                      ),
                      Effect.andThen(
                        attempt(
                          attemptNumber + 1,
                          nextMaybeCommitted,
                        ),
                      ),
                    );
                  },
                  onSuccess: Effect.succeed,
                });
              });

            return yield* attempt(1, false);
          }),
        (handle) => releaseOrDie(driver.closeTransaction(handle)),
      ).pipe(
        Effect.ensuring(Effect.suspend(() =>
          Effect.annotateCurrentSpan({
            "db.system.name": "foundationdb",
            "db.operation.name": "transaction",
            "db.foundationdb.transaction.attempts":
              transactionTelemetry.attempts,
            "db.foundationdb.transaction.retries": Math.max(
              transactionTelemetry.attempts - 1,
              0,
            ),
            "db.foundationdb.transaction.maybe_committed":
              transactionTelemetry.maybeCommitted,
            "db.foundationdb.mutation.count":
              transactionTelemetry.mutationCount,
            "db.foundationdb.mutation.bytes":
              transactionTelemetry.mutationBytes,
          })
        )),
      );
    },
  );

  const get: FoundationDbShape["get"] = Effect.fnUntraced(function* (
    key,
    options,
  ) {
    return yield* withTransaction(
      Effect.flatMap(FoundationDbTransaction, (transaction) =>
        transaction.get(
          key,
          options?.snapshot === undefined
            ? undefined
            : { snapshot: options.snapshot },
        )),
      options?.transaction,
    );
  });
  const getMany: FoundationDbShape["getMany"] = Effect.fnUntraced(function* (
    keys,
    options,
  ) {
    return yield* withTransaction(
      Effect.flatMap(
        FoundationDbTransaction,
        (transaction) =>
          transaction.getMany(
            keys,
            options?.snapshot === undefined
              ? undefined
              : { snapshot: options.snapshot },
          ),
      ),
      options?.transaction,
    );
  });
  const set: FoundationDbShape["set"] = Effect.fnUntraced(function* (
    key,
    value,
    options,
  ) {
    return yield* withTransaction(
      Effect.flatMap(
        FoundationDbTransaction,
        (transaction) => transaction.set(key, value),
      ),
      options,
    );
  });
  const clear: FoundationDbShape["clear"] = Effect.fnUntraced(function* (
    key,
    options,
  ) {
    return yield* withTransaction(
      Effect.flatMap(
        FoundationDbTransaction,
        (transaction) => transaction.clear(key),
      ),
      options,
    );
  });
  const clearRange: FoundationDbShape["clearRange"] = Effect.fnUntraced(
    function* (begin, end, options) {
      return yield* withTransaction(
        Effect.flatMap(
          FoundationDbTransaction,
          (transaction) => transaction.clearRange(begin, end),
        ),
        options,
      );
    },
  );
  const getRange: FoundationDbShape["getRange"] = (
    options,
    transactionOptions,
  ) =>
    withTransaction(
      Effect.flatMap(FoundationDbTransaction, (transaction) =>
        Stream.runCollect(transaction.getRange(options))),
      transactionOptions,
    );

  return { withTransaction, get, getMany, set, clear, clearRange, getRange };
};

export class FoundationDb
  extends Context.Service<FoundationDb, FoundationDbShape>()(
    "@effect-foundationdb/FoundationDb",
  ) {
  static layerFromNative(
    clusterFile?: string,
    transactionDefaults: TransactionOptions = {},
  ): Layer.Layer<FoundationDb, FoundationDbError, NativeDriver> {
    return Layer.effect(
      FoundationDb,
      Effect.gen(function* () {
        const driver = yield* NativeDriver;
        const database = yield* Effect.acquireRelease(
          driver.openDatabase(clusterFile),
          (handle) => releaseOrDie(driver.closeDatabase(handle)),
        );
        return makeDatabase(driver, database, transactionDefaults);
      }),
    );
  }

  static layer(
    options: FoundationDbOptions,
  ): Layer.Layer<FoundationDb, FoundationDbError> {
    return FoundationDb.layerFromNative(
      options.clusterFile,
      options.transactionDefaults,
    ).pipe(
      Layer.provide(ffiLayer(options)),
    );
  }
}
