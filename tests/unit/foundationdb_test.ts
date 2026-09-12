import { assert, assertEquals } from "@std/assert";
import { Cause, Effect, Exit, Layer, Option, Stream, Tracer } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  KeySelector,
  KeyValue,
  StreamingMode,
} from "../../mod.ts";
import { NativeDriver } from "../../src/internal/native.ts";
import type { NativeDriverShape } from "../../src/internal/native.ts";
import type { FoundationDbShape } from "../../src/FoundationDb.ts";
import type { FoundationDbError } from "../../src/errors.ts";
import type { TransactionAttempt } from "../../src/model.ts";
import { fdbError, makeFakeDriver } from "../support/fake-driver.ts";

const bytes = (...values: number[]): Uint8Array => new Uint8Array(values);

const layerFor = (
  driver: NativeDriverShape,
  clusterFile = "test.cluster",
  transactionDefaults: Parameters<typeof FoundationDb.layerFromNative>[1] = {},
) =>
  FoundationDb.layerFromNative(clusterFile, transactionDefaults).pipe(
    Layer.provide(Layer.succeed(NativeDriver, driver)),
  );

const runWith = <A, E>(
  driver: NativeDriverShape,
  effect: Effect.Effect<A, E, FoundationDb>,
): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(layerFor(driver))));

const runWithTracer = <A, E>(
  driver: NativeDriverShape,
  effect: Effect.Effect<A, E, FoundationDb>,
  tracer: Tracer.Tracer,
): Promise<A> =>
  Effect.runPromise(effect.pipe(
    Effect.provide(layerFor(driver)),
    Effect.provideService(Tracer.Tracer, tracer),
  ));

const recordingTracer = () => {
  const spans: Array<Tracer.NativeSpan> = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      spans.push(span);
      return span;
    },
  });
  return { spans, tracer } as const;
};

const spansNamed = (
  spans: ReadonlyArray<Tracer.NativeSpan>,
  name: string,
): ReadonlyArray<Tracer.NativeSpan> =>
  spans.filter((span) => span.name === name);

const withDatabase = <A, E>(
  use: (database: FoundationDbShape) => Effect.Effect<A, E>,
): Effect.Effect<A, E, FoundationDb> =>
  Effect.gen(function* () {
    return yield* use(yield* FoundationDb);
  });

const readInTransaction = (
  key: Uint8Array,
): Effect.Effect<
  Uint8Array | undefined,
  FoundationDbError,
  FoundationDbTransaction
> =>
  Effect.flatMap(
    FoundationDbTransaction,
    (transaction) => transaction.get(key),
  );

Deno.test("layer scopes the native database and forwards the cluster file", async () => {
  const { driver, state } = makeFakeDriver();

  await runWith(driver, withDatabase(() => Effect.void));

  assertEquals(state.openDatabase, ["test.cluster"]);
  assertEquals(state.closeDatabase, [1n]);
});

Deno.test("transaction forwards options and all primitive operations", async () => {
  const { driver, state } = makeFakeDriver();
  const key = bytes(1);
  const value = bytes(2, 3);
  const operand = bytes(1, 0, 0, 0, 0, 0, 0, 0);
  const end = bytes(4);
  const selected = bytes(9);
  const selector = KeySelector.firstGreaterThan(key);
  state.getResults.push(value);
  state.getManyResults.push([value, undefined]);
  state.getKeyResults.push(selected);

  const actual = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const found = yield* transaction.get(key, { snapshot: true });
          const foundMany = yield* transaction.getMany([key, end], {
            snapshot: true,
          });
          const foundKey = yield* transaction.getKey(selector, {
            snapshot: true,
          });
          yield* transaction.set(key, value);
          yield* transaction.atomicAdd(key, operand);
          yield* transaction.setWithoutWriteConflict(key, value);
          yield* transaction.clear(key);
          yield* transaction.clearRange(key, end);
          yield* transaction.clearRangeWithoutWriteConflict(key, end);
          yield* transaction.addWriteConflictRange(key, end);
          return [found, foundMany, foundKey] as const;
        }),
        { timeoutMs: 500, retryLimit: 3, maxRetryDelayMs: 25 },
      )
    ),
  );

  assertEquals(actual, [value, [value, undefined], selected]);
  assertEquals(state.options, [
    [2n, "timeout", 500],
    [2n, "retryLimit", 3],
    [2n, "maxRetryDelay", 25],
  ]);
  assertEquals(state.gets, [[2n, key, true]]);
  assertEquals(state.getMany, [[2n, [key, end], true]]);
  assertEquals(state.getKeys, [[2n, selector, true]]);
  assertEquals(state.sets, [[2n, key, value]]);
  assertEquals(state.atomicAdds, [[2n, key, operand]]);
  assertEquals(state.setsWithoutWriteConflict, [[2n, key, value]]);
  assertEquals(state.clears, [[2n, key]]);
  assertEquals(state.clearRanges, [[2n, key, end]]);
  assertEquals(state.clearRangesWithoutWriteConflict, [[2n, key, end]]);
  assertEquals(state.writeConflictRanges, [[2n, key, end]]);
  assertEquals(state.commits, [2n]);
  assertEquals(state.closeTransaction, [2n]);
});

Deno.test("transaction tracing aggregates local mutations and traces remote operations", async () => {
  const { driver, state } = makeFakeDriver();
  const { spans, tracer } = recordingTracer();
  const key = bytes(1, 2);
  const value = bytes(3, 4, 5);
  const operand = bytes(6, 7, 8, 9);
  const end = bytes(10, 11, 12);
  const row = new KeyValue({ key, value });
  state.getResults.push(value);
  state.getManyResults.push([value, undefined]);
  state.rangeBatches.push({ values: [row], more: false });

  await runWithTracer(
    driver,
    withDatabase((database) =>
      database.withTransaction(Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        yield* transaction.set(key, value);
        yield* transaction.atomicAdd(key, operand);
        yield* transaction.setWithoutWriteConflict(key, value);
        yield* transaction.clear(key);
        yield* transaction.clearRange(key, end);
        yield* transaction.clearRangeWithoutWriteConflict(key, end);
        yield* transaction.addWriteConflictRange(key, end);
        yield* transaction.get(key);
        yield* transaction.getMany([key, end], { snapshot: true });
        yield* Stream.runCollect(
          transaction.getRange(keyRange(bytes(0), bytes(9))),
        );
      }))
    ),
    tracer,
  );

  assertEquals(
    spans.filter((span) =>
      [
        "FoundationDb.Transaction.set",
        "FoundationDb.Transaction.atomicAdd",
        "FoundationDb.Transaction.setWithoutWriteConflict",
        "FoundationDb.Transaction.clear",
        "FoundationDb.Transaction.clearRange",
        "FoundationDb.Transaction.clearRangeWithoutWriteConflict",
        "FoundationDb.Transaction.addWriteConflictRange",
      ].includes(span.name)
    ),
    [],
  );
  assertEquals(spans.map((span) => span.name), [
    "FoundationDb.withTransaction",
    "FoundationDb.Transaction.attempt",
    "FoundationDb.Transaction.get",
    "FoundationDb.Transaction.getMany",
    "FoundationDb.Transaction.getRangePage",
    "FoundationDb.Transaction.commit",
  ]);

  const transaction = spansNamed(spans, "FoundationDb.withTransaction")[0];
  const attempt = spansNamed(spans, "FoundationDb.Transaction.attempt")[0];
  const get = spansNamed(spans, "FoundationDb.Transaction.get")[0];
  const getMany = spansNamed(spans, "FoundationDb.Transaction.getMany")[0];
  const page = spansNamed(
    spans,
    "FoundationDb.Transaction.getRangePage",
  )[0];
  const commit = spansNamed(spans, "FoundationDb.Transaction.commit")[0];
  assert(transaction !== undefined);
  assert(attempt !== undefined);
  assert(get !== undefined);
  assert(getMany !== undefined);
  assert(page !== undefined);
  assert(commit !== undefined);

  assertEquals(transaction.kind, "internal");
  assertEquals(transaction.attributes.get("db.system.name"), "foundationdb");
  assertEquals(
    transaction.attributes.get("db.foundationdb.transaction.attempts"),
    1,
  );
  assertEquals(
    transaction.attributes.get("db.foundationdb.transaction.retries"),
    0,
  );
  assertEquals(
    transaction.attributes.get("db.foundationdb.mutation.count"),
    6,
  );
  assertEquals(
    transaction.attributes.get("db.foundationdb.mutation.bytes"),
    key.byteLength + value.byteLength +
      key.byteLength + operand.byteLength +
      key.byteLength + value.byteLength +
      key.byteLength +
      key.byteLength + end.byteLength +
      key.byteLength + end.byteLength,
  );
  assertEquals(
    attempt.attributes.get("db.foundationdb.mutation.count"),
    6,
  );
  assertEquals(
    attempt.attributes.get("db.foundationdb.mutation.bytes"),
    key.byteLength + value.byteLength +
      key.byteLength + operand.byteLength +
      key.byteLength + value.byteLength +
      key.byteLength +
      key.byteLength + end.byteLength +
      key.byteLength + end.byteLength,
  );
  assertEquals(get.kind, "client");
  assertEquals(getMany.kind, "client");
  assertEquals(page.kind, "client");
  assertEquals(commit.kind, "client");
  assertEquals(
    getMany.attributes.get("db.foundationdb.read.key_count"),
    2,
  );
  assertEquals(
    getMany.attributes.get("db.foundationdb.read.snapshot"),
    true,
  );
  assertEquals(page.attributes.get("db.foundationdb.range.page"), 1);
  assert(Option.isSome(attempt.parent));
  assertEquals(attempt.parent.value, transaction);
  for (const span of [get, getMany, page, commit]) {
    assert(Option.isSome(span.parent));
    assertEquals(span.parent.value, attempt);
  }
});

Deno.test("transaction tracing exposes retries as sibling attempts", async () => {
  const { driver, state } = makeFakeDriver();
  const { spans, tracer } = recordingTracer();
  const firstFailure = fdbError("Transaction.get", { code: 1020 });
  state.getResults.push(firstFailure, bytes(42));

  await runWithTracer(
    driver,
    withDatabase((database) =>
      database.withTransaction(Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        yield* transaction.set(bytes(2), bytes(3));
        return yield* transaction.get(bytes(1));
      }))
    ),
    tracer,
  );

  const transaction = spansNamed(spans, "FoundationDb.withTransaction")[0];
  const attempts = spansNamed(spans, "FoundationDb.Transaction.attempt");
  const gets = spansNamed(spans, "FoundationDb.Transaction.get");
  const onError = spansNamed(spans, "FoundationDb.Transaction.onError")[0];
  const commit = spansNamed(spans, "FoundationDb.Transaction.commit")[0];
  assert(transaction !== undefined);
  assertEquals(attempts.length, 2);
  assertEquals(gets.length, 2);
  assert(onError !== undefined);
  assert(commit !== undefined);

  assertEquals(
    transaction.attributes.get("db.foundationdb.transaction.attempts"),
    2,
  );
  assertEquals(
    transaction.attributes.get("db.foundationdb.transaction.retries"),
    1,
  );
  assertEquals(
    transaction.attributes.get("db.foundationdb.mutation.count"),
    2,
  );
  assertEquals(
    attempts.map((span) =>
      span.attributes.get("db.foundationdb.mutation.count")
    ),
    [1, 1],
  );
  assertEquals(attempts[0]?.status._tag, "Ended");
  assertEquals(attempts[1]?.status._tag, "Ended");
  if (attempts[0]?.status._tag === "Ended") {
    assert(Exit.isFailure(attempts[0].status.exit));
  }
  if (attempts[1]?.status._tag === "Ended") {
    assert(Exit.isSuccess(attempts[1].status.exit));
  }
  assertEquals(attempts[0]?.attributes.get("error.type"), "1020");
  assertEquals(
    onError.attributes.get("db.foundationdb.retry.error_code"),
    1020,
  );
  for (const attempt of attempts) {
    assert(Option.isSome(attempt.parent));
    assertEquals(attempt.parent.value, transaction);
  }
  assert(Option.isSome(onError.parent));
  assertEquals(onError.parent.value, transaction);
  assert(Option.isSome(commit.parent));
  assertEquals(commit.parent.value, attempts[1]);
});

Deno.test("transaction options override database defaults field by field", async () => {
  const { driver, state } = makeFakeDriver();
  const program = withDatabase((database) =>
    database.withTransaction(Effect.void, { retryLimit: 9 })
  );

  await Effect.runPromise(program.pipe(Effect.provide(layerFor(
    driver,
    "test.cluster",
    { timeoutMs: 500, retryLimit: 3, maxRetryDelayMs: 25 },
  ))));

  assertEquals(state.options, [
    [2n, "timeout", 500],
    [2n, "retryLimit", 9],
    [2n, "maxRetryDelay", 25],
  ]);
});

Deno.test("transaction-dependent effects compose without passing handles", async () => {
  const { driver, state } = makeFakeDriver();
  const key = bytes(7);
  const value = bytes(8, 9);
  state.getResults.push(value);

  const actual = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(readInTransaction(key))
    ),
  );

  assertEquals(actual, value);
  assertEquals(state.gets, [[2n, key, false]]);
  assertEquals(state.commits, [2n]);
});

Deno.test("transaction retries a body FoundationDB failure through onError", async () => {
  const { driver, state } = makeFakeDriver();
  const firstFailure = fdbError("Transaction.get");
  const value = bytes(42);
  const attempts: Array<TransactionAttempt> = [];
  state.getResults.push(firstFailure, value);

  const actual = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          attempts.push({
            attempt: transaction.attempt,
            maybeCommitted: transaction.maybeCommitted,
          });
          return yield* transaction.get(bytes(1));
        }),
      )
    ),
  );

  assertEquals(actual, value);
  assertEquals(attempts, [
    { attempt: 1, maybeCommitted: false },
    { attempt: 2, maybeCommitted: false },
  ]);
  assertEquals(state.onErrors, [[2n, firstFailure]]);
  assertEquals(state.commits, [2n]);
});

Deno.test("transaction retries commit failure and propagates maybeCommitted", async () => {
  const { driver, state } = makeFakeDriver();
  const commitFailure = fdbError("Transaction.commit", {
    code: 1021,
    message: "commit_unknown_result",
    maybeCommitted: true,
    retryableNotCommitted: false,
  });
  const attempts: Array<TransactionAttempt> = [];
  state.commitFailures.push(commitFailure);

  const actual = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          attempts.push({
            attempt: transaction.attempt,
            maybeCommitted: transaction.maybeCommitted,
          });
          return "done";
        }),
      )
    ),
  );

  assertEquals(actual, "done");
  assertEquals(attempts, [
    { attempt: 1, maybeCommitted: false },
    { attempt: 2, maybeCommitted: true },
  ]);
  assertEquals(state.commits, [2n, 2n]);
  assertEquals(state.onErrors, [[2n, commitFailure]]);
});

Deno.test("maybeCommitted remains true across later retryable failures", async () => {
  const { driver, state } = makeFakeDriver();
  const commitFailure = fdbError("Transaction.commit", {
    code: 1021,
    message: "commit_unknown_result",
    maybeCommitted: true,
  });
  const readFailure = fdbError("Transaction.get", { maybeCommitted: false });
  const attempts: Array<TransactionAttempt> = [];
  state.commitFailures.push(commitFailure);
  state.getResults.push(readFailure, bytes(1));

  await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          attempts.push({
            attempt: transaction.attempt,
            maybeCommitted: transaction.maybeCommitted,
          });
          return transaction.attempt === 1
            ? bytes(1)
            : yield* transaction.get(bytes(9));
        }),
      )
    ),
  );

  assertEquals(attempts, [
    { attempt: 1, maybeCommitted: false },
    { attempt: 2, maybeCommitted: true },
    { attempt: 3, maybeCommitted: true },
  ]);
  assertEquals(state.onErrors, [
    [2n, commitFailure],
    [2n, readFailure],
  ]);
});

Deno.test("domain failures are not retried or committed", async () => {
  const { driver, state } = makeFakeDriver();
  const domainFailure = { _tag: "DomainFailure" as const, message: "stop" };
  let attempts = 0;

  const failure = await runWith(
    driver,
    withDatabase((database) =>
      Effect.flip(
        database.withTransaction(
          Effect.gen(function* () {
            attempts += 1;
            return yield* Effect.fail(domainFailure);
          }),
        ),
      )
    ),
  );

  assertEquals(failure, domainFailure);
  assertEquals(attempts, 1);
  assertEquals(state.onErrors, []);
  assertEquals(state.commits, []);
  assertEquals(state.closeTransaction, [2n]);
});

Deno.test("composite transaction causes are preserved without retrying", async () => {
  const operationFailure = fdbError("Transaction.get");
  const domainFailure = { _tag: "DomainFailure" as const, message: "stop" };
  const defect = new Error("unexpected defect");
  const composites = [
    Cause.combine(Cause.fail(operationFailure), Cause.fail(domainFailure)),
    Cause.combine(Cause.fail(operationFailure), Cause.die(defect)),
  ];

  for (const composite of composites) {
    const { driver, state } = makeFakeDriver();
    const exit = await Effect.runPromiseExit(
      withDatabase((database) =>
        database.withTransaction(Effect.failCause(composite))
      ).pipe(Effect.provide(layerFor(driver))),
    );

    assert(Exit.isFailure(exit));
    assertEquals(exit.cause.reasons.length, 2);
    assertEquals(state.onErrors, []);
    assertEquals(state.commits, []);
    assertEquals(state.closeTransaction, [2n]);
  }
});

Deno.test("each transaction retry has an attempt-local scope", async () => {
  const { driver, state } = makeFakeDriver();
  const operationFailure = fdbError("Transaction.get");
  const finalized: Array<number> = [];
  state.getResults.push(operationFailure, bytes(42));

  const checkedDriver: NativeDriverShape = {
    ...driver,
    commit: (handle) =>
      Effect.sync(() => assertEquals(finalized, [1, 2])).pipe(
        Effect.andThen(driver.commit(handle)),
      ),
  };

  const result = await runWith(
    checkedDriver,
    withDatabase((database) =>
      database.withTransaction(Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        if (transaction.attempt === 2) {
          assertEquals(finalized, [1]);
        }
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => finalized.push(transaction.attempt))
        );
        return yield* transaction.get(bytes(1));
      }))
    ),
  );

  assertEquals(result, bytes(42));
  assertEquals(finalized, [1, 2]);
  assertEquals(state.onErrors, [[2n, operationFailure]]);
});

Deno.test("onError failure terminates the retry loop", async () => {
  const { driver, state } = makeFakeDriver();
  const operationFailure = fdbError("Transaction.get");
  const retryFailure = fdbError("Transaction.onError", {
    code: 2002,
    message: "retry limit exceeded",
    retryable: false,
    retryableNotCommitted: false,
  });
  state.getResults.push(operationFailure);
  state.onErrorFailures.push(retryFailure);

  const failure = await runWith(
    driver,
    withDatabase((database) =>
      Effect.flip(database.withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          transaction.get(bytes(1))),
      ))
    ),
  );

  assertEquals(failure, retryFailure);
  assertEquals(state.gets.length, 1);
  assertEquals(state.onErrors, [[2n, operationFailure]]);
  assertEquals(state.closeTransaction, [2n]);
});

Deno.test("onError failure preserves an earlier unknown commit result", async () => {
  const { driver, state } = makeFakeDriver();
  const commitFailure = fdbError("Transaction.commit", {
    code: 1021,
    maybeCommitted: true,
    retryableNotCommitted: false,
  });
  const retryFailure = fdbError("Transaction.onError", {
    code: 2002,
    retryable: false,
    maybeCommitted: false,
    retryableNotCommitted: false,
  });
  state.commitFailures.push(commitFailure);
  state.onErrorFailures.push(retryFailure);

  const failure = await runWith(
    driver,
    withDatabase((database) =>
      Effect.flip(database.withTransaction(Effect.succeed("value")))
    ),
  );

  assertEquals(failure.operation, "Transaction.onError");
  assertEquals(failure.code, 2002);
  assertEquals(failure.maybeCommitted, true);
  assertEquals(failure.retryableNotCommitted, false);
  assertEquals(state.commits, [2n]);
  assertEquals(state.onErrors, [[2n, commitFailure]]);
});

Deno.test("transaction finalizer runs after a defect", async () => {
  const { driver, state } = makeFakeDriver();

  await Effect.runPromiseExit(
    withDatabase((database) =>
      database.withTransaction(Effect.die("unexpected defect"))
    ).pipe(Effect.provide(layerFor(driver))),
  );

  assertEquals(state.closeTransaction, [2n]);
  assertEquals(state.closeDatabase, [1n]);
});

Deno.test("transaction close failures become defects", async () => {
  const { driver, state } = makeFakeDriver();
  const closeFailure = fdbError("Transaction.close", {
    retryable: false,
    retryableNotCommitted: false,
  });
  const failingDriver: NativeDriverShape = {
    ...driver,
    closeTransaction: (handle) =>
      driver.closeTransaction(handle).pipe(
        Effect.andThen(Effect.fail(closeFailure)),
      ),
  };

  const exit = await Effect.runPromiseExit(
    withDatabase((database) => database.withTransaction(Effect.void)).pipe(
      Effect.provide(layerFor(failingDriver)),
    ),
  );

  assert(Exit.isFailure(exit));
  assert(
    exit.cause.reasons.some((reason) =>
      Cause.isDieReason(reason) && reason.defect === closeFailure
    ),
  );
  assertEquals(state.commits, [2n]);
  assertEquals(state.closeTransaction, [2n]);
});

Deno.test("transaction range paginates and closes its cursor", async () => {
  const { driver, state } = makeFakeDriver();
  const first = new KeyValue({ key: bytes(1), value: bytes(11) });
  const second = new KeyValue({ key: bytes(2), value: bytes(22) });
  const options = keyRange(bytes(1), bytes(3), {
    targetBytes: 128,
    mode: StreamingMode.Iterator,
  });
  state.rangeBatches.push(
    { values: [first], more: true },
    { values: [second], more: false },
  );

  const values = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          return yield* Stream.runCollect(transaction.getRange(options)).pipe(
            Effect.map(Array.from),
          );
        }),
      )
    ),
  );

  assertEquals(values, [first, second]);
  assertEquals(state.openRanges, [[2n, options]]);
  assertEquals(state.nextRanges, [3n, 3n]);
  assertEquals(state.closeRanges, [3n]);
});

Deno.test("taking part of a range closes the cursor without fetching another page", async () => {
  const { driver, state } = makeFakeDriver();
  const first = new KeyValue({ key: bytes(1), value: bytes(11) });
  const ignored = new KeyValue({ key: bytes(2), value: bytes(22) });
  state.rangeBatches.push({ values: [first, ignored], more: true });

  const values = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          return yield* transaction.getRange(keyRange(bytes(1), bytes(9))).pipe(
            Stream.take(1),
            Stream.runCollect,
            Effect.map(Array.from),
          );
        }),
      )
    ),
  );

  assertEquals(values, [first]);
  assertEquals(state.nextRanges, [3n]);
  assertEquals(state.closeRanges, [3n]);
});

Deno.test("an empty intermediate range page does not terminate pagination", async () => {
  const { driver, state } = makeFakeDriver();
  const value = new KeyValue({ key: bytes(5), value: bytes(6) });
  state.rangeBatches.push(
    { values: [], more: true },
    { values: [value], more: false },
  );

  const values = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          return yield* Stream.runCollect(
            transaction.getRange(keyRange(bytes(0), bytes(9))),
          ).pipe(Effect.map(Array.from));
        }),
      )
    ),
  );

  assertEquals(values, [value]);
  assertEquals(state.nextRanges, [3n, 3n]);
});

Deno.test("a failed range page closes its cursor before retrying", async () => {
  const { driver, state } = makeFakeDriver();
  const pageFailure = fdbError("Transaction.getRange.next");
  const value = new KeyValue({ key: bytes(5), value: bytes(6) });
  state.rangeBatches.push(pageFailure, { values: [value], more: false });

  const values = await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          return yield* Stream.runCollect(
            transaction.getRange(keyRange(bytes(0), bytes(9))),
          ).pipe(Effect.map(Array.from));
        }),
      )
    ),
  );

  assertEquals(values, [value]);
  assertEquals(state.openRanges.length, 2);
  assertEquals(state.closeRanges, [3n, 3n]);
  assertEquals(state.onErrors, [[2n, pageFailure]]);
  assertEquals(state.commits, [2n]);
});

Deno.test("database convenience methods use scoped transactions", async () => {
  const { driver, state } = makeFakeDriver();
  const key = bytes(7);
  const value = bytes(8);
  const end = bytes(9);
  state.getResults.push(value);

  const found = await runWith(
    driver,
    withDatabase((database) =>
      Effect.gen(function* () {
        const result = yield* database.get(key, {
          snapshot: true,
          transaction: { timeoutMs: 12 },
        });
        yield* database.set(key, value);
        yield* database.clear(key);
        yield* database.clearRange(key, end);
        return result;
      })
    ),
  );

  assertEquals(found, value);
  assertEquals(state.gets, [[2n, key, true]]);
  assertEquals(state.sets, [[2n, key, value]]);
  assertEquals(state.clears, [[2n, key]]);
  assertEquals(state.clearRanges, [[2n, key, end]]);
  assertEquals(state.options, [[2n, "timeout", 12]]);
  assertEquals(state.openTransaction.length, 4);
  assertEquals(state.closeTransaction.length, 4);
  assertEquals(state.commits.length, 4);
});

Deno.test("database range convenience method keeps the transaction scoped", async () => {
  const { driver, state } = makeFakeDriver();
  const value = new KeyValue({ key: bytes(1), value: bytes(2) });
  state.rangeBatches.push({ values: [value], more: false });

  const values = await runWith(
    driver,
    withDatabase((database) =>
      database.getRange(
        keyRange(bytes(0), bytes(3)),
        { retryLimit: 2 },
      ).pipe(Effect.map(Array.from))
    ),
  );

  assertEquals(values, [value]);
  assertEquals(state.options, [[2n, "retryLimit", 2]]);
  assertEquals(state.closeRanges, [3n]);
  assertEquals(state.closeTransaction, [2n]);
});

Deno.test("snapshot defaults to false for get, getMany, and getKey", async () => {
  const { driver, state } = makeFakeDriver();
  const selector = KeySelector.lastLessThan(bytes(9));
  state.getResults.push(undefined);
  state.getManyResults.push([]);
  state.getKeyResults.push(bytes(8));

  await runWith(
    driver,
    withDatabase((database) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const missing = yield* transaction.get(bytes(1));
          assertEquals(missing, undefined);
          yield* transaction.getMany([]);
          yield* transaction.getKey(selector);
        }),
      )
    ),
  );

  assertEquals(state.gets[0]?.[2], false);
  assertEquals(state.getMany[0]?.[2], false);
  assertEquals(state.getKeys[0]?.[2], false);
  assert(state.closeTransaction.length === 1);
});
