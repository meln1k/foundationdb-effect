import { assert, assertEquals } from "@std/assert";
import {
  Cause,
  Duration,
  Effect,
  Exit,
  Fiber,
  Latch,
  Layer,
  Schedule,
  Schema,
  Scope,
} from "effect";
import { TestClock } from "effect/testing";
import { PersistedQueue } from "effect/unstable/persistence";
import {
  DirectoryLayer,
  DirectorySubspace,
  FoundationDb,
  keyRange,
  layerPersistedQueueStore,
  makePersistedQueueStore,
  Subspace,
} from "../../../mod.ts";
import type {
  FoundationDbShape,
  PersistedQueueStoreOptions,
  TransactionOptions,
} from "../../../mod.ts";
import { makeQueueRepository } from "../../../src/persisted-queue/repository.ts";
import { makeMemoryFoundationDb } from "../../support/memory-database.ts";

const Item = Schema.Struct({ value: Schema.String });

const waitUntil = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() =>
    predicate()
      ? Effect.void
      : Effect.andThen(Effect.yieldNow, waitUntil(predicate))
  );

const layerFor = (
  database: FoundationDbShape,
  options: PersistedQueueStoreOptions = {},
) => {
  const storeLayer = layerPersistedQueueStore(options).pipe(
    Layer.provide(Layer.succeed(FoundationDb, database)),
  );
  return PersistedQueue.layer.pipe(Layer.provideMerge(storeLayer));
};

const run = <A, E>(
  database: FoundationDbShape,
  effect: Effect.Effect<
    A,
    E,
    | PersistedQueue.PersistedQueueFactory
    | PersistedQueue.PersistedQueueStore
    | Scope.Scope
  >,
  options?: PersistedQueueStoreOptions,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.scoped,
      Effect.provide(layerFor(database, options)),
    ),
  );

Deno.test("PersistedQueue backend preserves FIFO order and custom-id deduplication", async () => {
  const { database } = makeMemoryFoundationDb();
  const values = await run(
    database,
    Effect.gen(function* () {
      const queue = yield* PersistedQueue.make({ name: "fifo", schema: Item });
      yield* queue.offer({ value: "first" }, { id: "same-id" });
      yield* queue.offer({ value: "duplicate" }, { id: "same-id" });
      yield* queue.offer({ value: "second" }, { id: "second-id" });
      return [
        yield* queue.take((value, metadata) =>
          Effect.succeed([value, metadata] as const)
        ),
        yield* queue.take((value, metadata) =>
          Effect.succeed([value, metadata] as const)
        ),
      ] as const;
    }),
    { pollInterval: "1 millis" },
  );

  assertEquals(values, [
    [{ value: "first" }, { id: "same-id", attempts: 1 }],
    [{ value: "second" }, { id: "second-id", attempts: 1 }],
  ]);
});

Deno.test("PersistedQueue stores named queues in directory-managed tuple subspaces", async () => {
  const { database } = makeMemoryFoundationDb();
  const directory = await Effect.runPromise(DirectoryLayer.make({
    nodeSubspace: new Subspace(Uint8Array.of(0xa0)),
    contentSubspace: new Subspace(Uint8Array.of(0xb0)),
  }));
  const directoryPath = ["queues"];

  await run(
    database,
    Effect.gen(function* () {
      const queue = yield* PersistedQueue.make({ name: "tuple", schema: Item });
      yield* queue.offer({ value: "value" }, { id: "custom-id" });
    }),
    { directory, directoryPath },
  );

  const inspection = await Effect.runPromise(Effect.gen(function* () {
    const { queue, queueNames } = yield* database.withTransaction(
      Effect.gen(function* () {
        const rootOutput = yield* directory.open(directoryPath);
        assert(rootOutput instanceof DirectorySubspace);
        const queueNames = yield* rootOutput.list();
        const queueOutput = yield* rootOutput.open(["tuple"]);
        assert(queueOutput instanceof DirectorySubspace);
        return { queue: queueOutput, queueNames };
      }),
    );
    const [begin, end] = yield* queue.range();
    const values = yield* database.getRange(keyRange(begin, end));
    const tuples = yield* Effect.forEach(
      values,
      ({ key }) => queue.unpack(key),
    );
    return { queueNames, tuples };
  }));

  assertEquals(inspection.queueNames, ["tuple"]);
  assertEquals(
    [...new Set(inspection.tuples.map((tuple) => tuple[0]))].sort(),
    ["counter", "entries", "ids", "pending", "signal"],
  );
  assert(
    inspection.tuples.some((tuple) =>
      tuple.length === 3 && tuple[0] === "entries" && tuple[1] === 0n &&
      tuple[2] === "metadata"
    ),
  );
  assert(
    inspection.tuples.some((tuple) =>
      tuple.length === 4 && tuple[0] === "entries" && tuple[1] === 0n &&
      tuple[2] === "element" && tuple[3] === 0n
    ),
  );
  assert(
    inspection.tuples.some((tuple) =>
      tuple.length === 2 && tuple[0] === "ids" && tuple[1] === "custom-id"
    ),
  );
});

Deno.test("PersistedQueue backend retries failures and does not count interruption", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await run(
    database,
    Effect.gen(function* () {
      const queue = yield* PersistedQueue.make({
        name: "retry",
        schema: Item,
        retrySchedule: Schedule.spaced(0),
      });
      yield* queue.offer({ value: "retry-me" });

      const firstAttempt = yield* queue.take((_, { attempts }) =>
        Effect.fail(attempts)
      ).pipe(Effect.flip);
      const secondAttempt = yield* queue.take((_, { attempts }) =>
        Effect.succeed(attempts)
      );

      const interrupted = yield* PersistedQueue.make({
        name: "interrupt",
        schema: Item,
      });
      yield* interrupted.offer({ value: "keep-attempt" });
      const started = Latch.makeUnsafe();
      const fiber = yield* interrupted.take(() =>
        Effect.andThen(started.open, Effect.never)
      ).pipe(Effect.forkScoped);
      yield* started.await;
      yield* Fiber.interrupt(fiber);
      const afterInterrupt = yield* interrupted.take((_, { attempts }) =>
        Effect.succeed(attempts)
      );

      return { firstAttempt, secondAttempt, afterInterrupt };
    }),
    { pollInterval: "1 millis" },
  );

  assertEquals(result, {
    firstAttempt: 1,
    secondAttempt: 2,
    afterInterrupt: 1,
  });
});

Deno.test("PersistedQueue retains fractional-delay retries until due", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const repository = yield* makeQueueRepository();
      yield* repository.offer({
        name: "fractional-delay",
        id: "item",
        element: { value: "payload" },
      });
      const first = yield* repository.claim("fractional-delay", "first", 3);
      assert(first !== true && first._tag !== "ClaimWait");
      yield* repository.finalizeClaim(
        "fractional-delay",
        first,
        3,
        () => Effect.succeed(Duration.millis(0.5)),
        Exit.fail("retry"),
      );

      const early = yield* repository.claim("fractional-delay", "early", 3);
      if (early !== true && early._tag === "ClaimWait") {
        yield* early.wake.cancel;
      }
      yield* TestClock.adjust("1 millis");
      const due = yield* repository.claim("fractional-delay", "due", 3);
      if (due !== true && due._tag === "ClaimWait") {
        yield* due.wake.cancel;
      }
      const earlyTag = early !== true && "_tag" in early ? early._tag : "Other";
      const dueEntry = due !== true && !("_tag" in due) ? due.entry : undefined;
      return { earlyTag, dueEntry };
    }).pipe(
      Effect.provideService(FoundationDb, database),
      Effect.provide(TestClock.layer()),
    ),
  );

  assertEquals(result.earlyTag, "ClaimWait");
  assertEquals(result.dueEntry?.element, { value: "payload" });
});

Deno.test("PersistedQueue backend dead-letters exhausted and undecodable elements", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await run(
    database,
    Effect.gen(function* () {
      const store = yield* PersistedQueue.PersistedQueueStore;
      const exhausted = yield* PersistedQueue.make({
        name: "dead-letter",
        schema: Item,
        maxAttempts: 1,
        retrySchedule: Schedule.spaced(0),
      });
      yield* exhausted.offer({ value: "fail" });
      yield* exhausted.take(() => Effect.fail("expected")).pipe(Effect.flip);
      const exhaustedResult = yield* Effect.race(
        exhausted.take(() => Effect.succeed("unexpected")),
        Effect.sleep("20 millis").pipe(Effect.as("waiting")),
      );

      yield* store.offer({
        name: "decode",
        id: "invalid",
        element: { value: null },
        isCustomId: true,
      });
      const decodeQueue = yield* PersistedQueue.make({
        name: "decode",
        schema: Item,
      });
      yield* decodeQueue.offer({ value: "valid" }, { id: "valid" });
      const decoded = yield* decodeQueue.take(Effect.succeed);
      return { exhaustedResult, decoded };
    }),
    { pollInterval: "1 millis" },
  );

  assertEquals(result, {
    exhaustedResult: "waiting",
    decoded: { value: "valid" },
  });
});

Deno.test("PersistedQueue cleanup expires completion deduplication records", async () => {
  const { database } = makeMemoryFoundationDb();
  const value = await run(
    database,
    Effect.gen(function* () {
      const store = yield* PersistedQueue.PersistedQueueStore;
      const queue = yield* PersistedQueue.make({
        name: "cleanup",
        schema: Item,
      });
      yield* queue.offer({ value: "old" }, { id: "reusable" });
      yield* queue.take(Effect.succeed);
      yield* store.cleanup({
        timeToLive: Duration.zero,
        failedTimeToLive: undefined,
      });
      yield* queue.offer({ value: "new" }, { id: "reusable" });
      return yield* queue.take(Effect.succeed);
    }),
    { pollInterval: "1 millis" },
  );

  assertEquals(value, { value: "new" });
});

Deno.test("PersistedQueue stores large elements in chunks", async () => {
  const { database, entries } = makeMemoryFoundationDb();
  const large = { value: "queue-value-".repeat(12_000) };

  const value = await run(
    database,
    Effect.gen(function* () {
      const queue = yield* PersistedQueue.make({
        name: "large",
        schema: Item,
        retrySchedule: Schedule.spaced(0),
      });
      yield* queue.offer(large);
      yield* queue.take(() => Effect.fail("retry large element")).pipe(
        Effect.flip,
      );
      return yield* queue.take(Effect.succeed);
    }),
    { pollInterval: "1 millis" },
  );

  assertEquals(value, large);
  assert(
    entries().filter(({ value }) => value.byteLength === 8 * 1_024).length > 1,
  );
  assert(entries().every(({ value }) => value.byteLength <= 8 * 1_024));
});

Deno.test("PersistedQueue backend recovers expired claims and ignores stale finalizers", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* PersistedQueue.PersistedQueueStore;
      const takeOptions = {
        name: "recovery",
        maxAttempts: 3,
        retryDelay: () => Effect.succeed(Duration.zero),
      };
      yield* store.offer({
        name: "recovery",
        id: "item",
        element: { value: "payload" },
        isCustomId: true,
      });

      const firstScope = yield* Scope.make();
      const first = yield* store.take(takeOptions).pipe(
        Scope.provide(firstScope),
      );
      yield* Effect.sleep("15 millis");

      const secondScope = yield* Scope.make();
      const second = yield* store.take(takeOptions).pipe(
        Scope.provide(secondScope),
      );
      yield* Scope.close(firstScope, Exit.void);
      yield* Scope.close(secondScope, Exit.void);
      return { first, second };
    }).pipe(
      Effect.provide(layerFor(database, {
        pollInterval: "1 millis",
        lockExpiration: "5 millis",
        lockRefreshInterval: "1 hour",
      })),
    ),
  );

  assertEquals(result.first.attempts, 1);
  assertEquals(result.second.attempts, 2);
  assertEquals(result.second.element, { value: "payload" });
});

Deno.test("PersistedQueue backend gives concurrent workers distinct elements", async () => {
  const { database } = makeMemoryFoundationDb();
  const values = await run(
    database,
    Effect.gen(function* () {
      const queue = yield* PersistedQueue.make({
        name: "workers",
        schema: Item,
      });
      yield* Effect.forEach(
        ["a", "b", "c", "d"],
        (value) => queue.offer({ value }),
        { discard: true },
      );
      return yield* Effect.forEach(
        [0, 1, 2, 3],
        () => queue.take((item) => Effect.succeed(item.value)),
        { concurrency: "unbounded" },
      );
    }),
    { pollInterval: "1 millis" },
  );

  assertEquals([...values].sort(), ["a", "b", "c", "d"]);
  assertEquals(new Set(values).size, 4);
  assert(values.length === 4);
});

Deno.test("PersistedQueue watch wakes a consumer for an offer from another store", async () => {
  const { activeWatches, database } = makeMemoryFoundationDb();
  const claimed = await Effect.runPromise(
    Effect.gen(function* () {
      const consumer = yield* makePersistedQueueStore({
        pollInterval: "1 hour",
      });
      const producer = yield* makePersistedQueueStore({
        pollInterval: "1 hour",
      });
      const fiber = yield* consumer.take({
        name: "remote-offer",
        maxAttempts: 3,
        retryDelay: () => Effect.succeed(Duration.zero),
      }).pipe(Effect.forkScoped);
      yield* waitUntil(() => activeWatches() === 1).pipe(
        Effect.timeout("1 second"),
      );
      yield* producer.offer({
        name: "remote-offer",
        id: "item",
        element: { value: "payload" },
        isCustomId: true,
      });
      return yield* Fiber.join(fiber).pipe(Effect.timeout("1 second"));
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(claimed, {
    id: "item",
    attempts: 1,
    element: { value: "payload" },
  });
  assertEquals(activeWatches(), 0);
});

Deno.test("PersistedQueue watch wakes another store when a claim is retried", async () => {
  const { activeWatches, database } = makeMemoryFoundationDb();
  const claimed = await Effect.runPromise(
    Effect.gen(function* () {
      const source = yield* makePersistedQueueStore({
        pollInterval: "1 hour",
      });
      const consumer = yield* makePersistedQueueStore({
        pollInterval: "1 hour",
      });
      const takeOptions = {
        name: "remote-retry",
        maxAttempts: 3,
        retryDelay: () => Effect.succeed(Duration.zero),
      };
      yield* source.offer({
        name: "remote-retry",
        id: "item",
        element: { value: "payload" },
        isCustomId: true,
      });
      const firstScope = yield* Scope.make();
      yield* source.take(takeOptions).pipe(Scope.provide(firstScope));

      const fiber = yield* consumer.take(takeOptions).pipe(Effect.forkScoped);
      yield* waitUntil(() => activeWatches() === 1).pipe(
        Effect.timeout("1 second"),
      );
      yield* Scope.close(firstScope, Exit.fail("retry"));
      return yield* Fiber.join(fiber).pipe(Effect.timeout("1 second"));
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(claimed, {
    id: "item",
    attempts: 2,
    element: { value: "payload" },
  });
  assertEquals(activeWatches(), 0);
});

Deno.test("PersistedQueue cancels its watch when a waiting take is interrupted", async () => {
  const { activeWatches, database } = makeMemoryFoundationDb();
  await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* makePersistedQueueStore({
        pollInterval: "1 hour",
      });
      const fiber = yield* store.take({
        name: "interrupted-watch",
        maxAttempts: 3,
        retryDelay: () => Effect.succeed(Duration.zero),
      }).pipe(Effect.forkScoped);
      yield* waitUntil(() => activeWatches() === 1).pipe(
        Effect.timeout("1 second"),
      );
      yield* Fiber.interrupt(fiber);
      yield* waitUntil(() => activeWatches() === 0).pipe(
        Effect.timeout("1 second"),
      );
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(activeWatches(), 0);
});

Deno.test("PersistedQueue claim acquisition is interruptible and bounded", async () => {
  const { database } = makeMemoryFoundationDb();
  const entered = Latch.makeUnsafe();
  const transactionOptions: Array<TransactionOptions | undefined> = [];
  const withTransaction: FoundationDbShape["withTransaction"] = (
    _effect,
    options,
  ) => {
    transactionOptions.push(options);
    return Effect.andThen(entered.open, Effect.never);
  };
  const hangingDatabase: FoundationDbShape = {
    ...database,
    withTransaction,
  };

  const exit = await run(
    hangingDatabase,
    Effect.gen(function* () {
      const store = yield* PersistedQueue.PersistedQueueStore;
      const fiber = yield* store.take({
        name: "interruptible-claim",
        maxAttempts: 3,
        retryDelay: () => Effect.succeed(Duration.zero),
      }).pipe(Effect.forkScoped);
      yield* entered.await;
      yield* Fiber.interrupt(fiber);
      return yield* Fiber.await(fiber);
    }),
  );

  assert(Exit.isFailure(exit));
  assert(Cause.hasInterruptsOnly(exit.cause));
  assertEquals(transactionOptions, [{
    timeoutMs: 5_000,
    retryLimit: 10,
    maxRetryDelayMs: 1_000,
  }]);
});
