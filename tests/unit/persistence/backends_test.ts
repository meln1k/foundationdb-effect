import { assert, assertEquals } from "@std/assert";
import { Duration, Effect, Encoding, Fiber, Latch, Layer } from "effect";
import { TestClock } from "effect/testing";
import {
  KeyValueStore,
  Persistence,
  RateLimiter,
} from "effect/unstable/persistence";
import {
  FoundationDb,
  FoundationDbTransaction,
} from "../../../src/FoundationDb.ts";
import { splitChunkedValue } from "../../../src/internal/chunked-value.ts";
import {
  layerBackingPersistence,
  layerFoundationDB,
  layerRateLimiterStore,
  makeBackingPersistence,
  makeKeyValueStore,
  makeRateLimiterStore,
} from "../../../src/persistence/mod.ts";
import { makeMemoryFoundationDb } from "../../support/memory-database.ts";

Deno.test("persistence chunk splitting reuses the encoded value buffer", () => {
  const value = new Uint8Array(2 * 8 * 1_024 + 1);
  const chunks = splitChunkedValue(value);

  assertEquals(chunks.map((chunk) => chunk.byteLength), [8_192, 8_192, 1]);
  assert(chunks.every((chunk) => chunk.buffer === value.buffer));
});

Deno.test("FoundationDB persistence layers compose with Effect services", async () => {
  const { database } = makeMemoryFoundationDb();
  const foundationDbLayer = Layer.succeed(FoundationDb, database);
  const applicationLayer = Layer.mergeAll(
    layerFoundationDB(),
    Persistence.layer.pipe(Layer.provide(layerBackingPersistence())),
    RateLimiter.layer.pipe(Layer.provide(layerRateLimiterStore())),
  ).pipe(Layer.provide(foundationDbLayer));

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const keyValueStore = yield* KeyValueStore.KeyValueStore;
      const persistence = yield* Persistence.Persistence;
      const rateLimiter = yield* RateLimiter.RateLimiter;
      yield* keyValueStore.set("layer", "available");
      yield* persistence.make({ storeId: "layer" });
      const consumed = yield* rateLimiter.consume({
        key: "layer",
        limit: 2,
        window: "1 second",
      });
      return {
        value: yield* keyValueStore.get("layer"),
        remaining: consumed.remaining,
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(applicationLayer),
    ),
  );

  assertEquals(result, { value: "available", remaining: 1 });
});

Deno.test("FoundationDB KeyValueStore supports typed values and atomic modification", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* makeKeyValueStore();
      assertEquals(yield* store.isEmpty, true);

      yield* store.set("text", "hello");
      yield* store.set("binary", new Uint8Array([0, 1, 255]));
      assertEquals(yield* store.get("text"), "hello");
      assertEquals(
        yield* store.getUint8Array("text"),
        new TextEncoder().encode("hello"),
      );
      assertEquals(
        yield* store.get("binary"),
        Encoding.encodeBase64(new Uint8Array([0, 1, 255])),
      );
      assertEquals(
        yield* store.getUint8Array("binary"),
        new Uint8Array([0, 1, 255]),
      );
      assertEquals(yield* store.size, 2);
      assertEquals(yield* store.has("missing"), false);
      assertEquals(yield* store.modify("missing", (value) => value), undefined);

      yield* store.set("counter", "0");
      yield* Effect.forEach(
        Array.from({ length: 20 }),
        () => store.modify("counter", (value) => String(Number(value) + 1)),
        { concurrency: "unbounded", discard: true },
      );
      const binary = yield* store.modifyUint8Array(
        "binary",
        (value) => new Uint8Array([...value, 2]),
      );

      const values = {
        counter: yield* store.get("counter"),
        binary,
        size: yield* store.size,
      };
      yield* store.remove("text");
      assertEquals(yield* store.has("text"), false);
      yield* store.clear;
      return { ...values, empty: yield* store.isEmpty };
    }).pipe(
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(result, {
    counter: "20",
    binary: new Uint8Array([0, 1, 255, 2]),
    size: 3,
    empty: true,
  });
});

Deno.test("FoundationDB KeyValueStore chunks values and tracks logical size", async () => {
  const { database, entries } = makeMemoryFoundationDb();
  const large = Uint8Array.from(
    { length: 20_000 },
    (_, index) => (index * 31) % 251,
  );

  await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* makeKeyValueStore();
      yield* store.set("large", large);
      assertEquals(yield* store.size, 1);
      assertEquals(yield* store.getUint8Array("large"), large);
      assert(
        entries().filter(({ value }) => value.byteLength === 8 * 1_024)
          .length >= 2,
      );
      assert(
        entries().every(({ value }) => value.byteLength <= 8 * 1_024),
      );

      yield* store.set("large", new Uint8Array([7]));
      assertEquals(yield* store.size, 1);
      assertEquals(yield* store.getUint8Array("large"), new Uint8Array([7]));
      yield* store.remove("large");
      yield* store.remove("large");
      assertEquals(yield* store.size, 0);

      yield* store.set("first", "one");
      yield* store.set("second", "two");
      assertEquals(yield* store.size, 2);
      yield* store.clear;
      assertEquals(yield* store.size, 0);
      assertEquals(yield* store.isEmpty, true);
    }).pipe(Effect.provideService(FoundationDb, database)),
  );
});

Deno.test("stateful persistence transitions reject ambiguous commit replay", async () => {
  const memory = makeMemoryFoundationDb();
  let retryOnMaybeCommitted: boolean | undefined;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) => {
      retryOnMaybeCommitted = options?.retryOnMaybeCommitted;
      return memory.database.withTransaction(effect, options);
    },
  };

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const keyValueStore = yield* makeKeyValueStore({
        transactionOptions: { retryOnMaybeCommitted: true },
      });
      yield* keyValueStore.set("counter", "0");
      yield* keyValueStore.modify(
        "counter",
        (value) => String(Number(value) + 1),
      );
      const keyValuePolicy = retryOnMaybeCommitted;

      const rateLimiterStore = yield* makeRateLimiterStore({
        transactionOptions: { retryOnMaybeCommitted: true },
      });
      yield* rateLimiterStore.fixedWindow({
        key: "fixed",
        tokens: 1,
        limit: 2,
        refillRate: Duration.seconds(1),
      });
      return {
        keyValuePolicy,
        rateLimiterPolicy: retryOnMaybeCommitted,
      };
    }).pipe(Effect.provideService(FoundationDb, database)),
  );

  assertEquals(result, {
    keyValuePolicy: false,
    rateLimiterPolicy: false,
  });
});

Deno.test("FoundationDB BackingPersistence preserves batches, TTLs, and store isolation", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const first = yield* backing.make("first");
      const second = yield* backing.make("second");

      yield* first.setMany([
        ["expiring", { value: 1 }, Duration.seconds(1)],
        ["stable", { value: 2 }, undefined],
      ]);
      yield* first.set("renewed", { value: 1 }, Duration.seconds(1));
      yield* first.set("renewed", { value: 2 }, Duration.seconds(2));
      yield* second.set("stable", { value: 3 }, undefined);
      assertEquals(
        yield* first.getMany(["stable", "missing", "expiring"]),
        [{ value: 2 }, undefined, { value: 1 }],
      );

      yield* TestClock.adjust("1 second");
      const expired = yield* first.get("expiring");
      const renewed = yield* first.get("renewed");
      yield* first.clear;
      return {
        expired,
        renewed,
        first: yield* first.get("stable"),
        second: yield* second.get("stable"),
      };
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
      Effect.provide(TestClock.layer()),
    ),
  );

  assertEquals(result, {
    expired: undefined,
    renewed: { value: 2 },
    first: undefined,
    second: { value: 3 },
  });
});

Deno.test("FoundationDB BackingPersistence chunks large values", async () => {
  const { database, entries } = makeMemoryFoundationDb();
  const large = { value: "persistence-value-".repeat(8_000) };

  await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("large");
      yield* store.set("entry", large, undefined);
      assertEquals(
        yield* store.getMany(["entry", "missing"]),
        [large, undefined],
      );
      assert(
        entries().filter(({ value }) => value.byteLength === 8 * 1_024)
          .length > 1,
      );
      assert(
        entries().every(({ value }) => value.byteLength <= 8 * 1_024),
      );

      yield* store.set("entry", { value: "small" }, undefined);
      assertEquals(yield* store.get("entry"), { value: "small" });
      yield* store.remove("entry");
      assertEquals(yield* store.get("entry"), undefined);
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );
});

Deno.test("FoundationDB BackingPersistence writes fitting values in one transaction", async () => {
  const memory = makeMemoryFoundationDb();
  let transactions = 0;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) => {
      transactions += 1;
      return memory.database.withTransaction(effect, options);
    },
  };

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("direct-write");
      while (transactions < 2) {
        yield* Effect.yieldNow;
      }
      transactions = 0;
      yield* store.set("one", { value: 1 }, undefined);
      const setTransactions = transactions;
      transactions = 0;
      yield* store.setMany([
        ["two", { value: 2 }, undefined],
        ["three", { value: 3 }, undefined],
      ]);
      return { setTransactions, setManyTransactions: transactions };
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(result, { setTransactions: 1, setManyTransactions: 1 });
});

Deno.test("FoundationDB BackingPersistence batches getMany manifests and deduplicates ranges", async () => {
  const memory = makeMemoryFoundationDb();
  let pointReads = 0;
  let batchReads = 0;
  let rangeReads = 0;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) =>
      memory.database.withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          Effect.provideService(effect, FoundationDbTransaction, {
            ...transaction,
            get: (key, readOptions) => {
              pointReads += 1;
              return transaction.get(key, readOptions);
            },
            getMany: (keys, readOptions) => {
              batchReads += 1;
              return transaction.getMany(keys, readOptions);
            },
            getRange: (range) => {
              rangeReads += 1;
              return transaction.getRange(range);
            },
          })),
        options,
      ),
  };

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("batched-read");
      yield* store.setMany([
        ["first", { value: 1 }, undefined],
        ["second", { value: 2 }, undefined],
      ]);
      pointReads = 0;
      batchReads = 0;
      rangeReads = 0;
      const values = yield* store.getMany([
        "first",
        "second",
        "first",
        "missing",
      ]);
      return { values, pointReads, batchReads, rangeReads };
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(result, {
    values: [{ value: 1 }, { value: 2 }, { value: 1 }, undefined],
    pointReads: 0,
    batchReads: 1,
    rangeReads: 2,
  });
});

Deno.test("FoundationDB BackingPersistence splits oversized setMany before publishing", async () => {
  const memory = makeMemoryFoundationDb();
  let transactions = 0;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) => {
      transactions += 1;
      return memory.database.withTransaction(effect, options);
    },
  };

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("staged");
      while (transactions < 2) {
        yield* Effect.yieldNow;
      }
      transactions = 0;
      yield* store.setMany([
        ["first", { value: "a".repeat(4_100_000) }, undefined],
        ["second", { value: "b".repeat(4_100_000) }, undefined],
      ]);
      return transactions;
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(result, 3);
});

Deno.test("FoundationDB BackingPersistence recognizes an ambiguously committed publication", async () => {
  const memory = makeMemoryFoundationDb();
  let transactions = 0;
  let replayTransaction = Number.POSITIVE_INFINITY;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) => {
      const transaction = ++transactions;
      const run = memory.database.withTransaction(effect, options);
      return transaction === replayTransaction
        ? run.pipe(
          Effect.andThen(memory.database.withTransaction(effect, options)),
        )
        : run;
    },
  };

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("ambiguous-publication");
      while (transactions < 2) {
        yield* Effect.yieldNow;
      }
      transactions = 0;
      replayTransaction = 3;
      yield* store.set(
        "entry",
        { version: "published", payload: "x".repeat(8_100_000) },
        undefined,
      );
      const stored = yield* store.get("entry");
      return {
        writeTransactions: transactions - 1,
        version: stored !== undefined && "version" in stored
          ? stored.version
          : undefined,
      };
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(result, {
    writeTransactions: 3,
    version: "published",
  });
});

Deno.test("FoundationDB BackingPersistence publishes a staged batch atomically", async () => {
  const memory = makeMemoryFoundationDb();
  const publishReached = Latch.makeUnsafe();
  const publishRelease = Latch.makeUnsafe();
  let transactions = 0;
  let blockTransaction = Number.POSITIVE_INFINITY;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) => {
      const transaction = ++transactions;
      const run = memory.database.withTransaction(effect, options);
      return transaction === blockTransaction
        ? publishReached.open.pipe(
          Effect.andThen(publishRelease.await),
          Effect.andThen(run),
        )
        : run;
    },
  };

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("atomic-publish");
      yield* store.setMany([
        ["first", { version: "old-first" }, undefined],
        ["second", { version: "old-second" }, undefined],
      ]);
      transactions = 0;
      blockTransaction = 3;
      const update = yield* store.setMany([
        [
          "first",
          { version: "new-first", payload: "a".repeat(4_100_000) },
          undefined,
        ],
        [
          "second",
          { version: "new-second", payload: "b".repeat(4_100_000) },
          undefined,
        ],
      ]).pipe(Effect.forkScoped);
      yield* publishReached.await;
      const duringPublish = yield* store.getMany(["first", "second"]);
      yield* publishRelease.open;
      yield* Fiber.join(update);
      const afterPublish = yield* store.getMany(["first", "second"]);
      return {
        duringPublish,
        afterPublish: afterPublish.map((value) =>
          value !== undefined && "version" in value ? value.version : undefined
        ),
      };
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(result, {
    duringPublish: [
      { version: "old-first" },
      { version: "old-second" },
    ],
    afterPublish: ["new-first", "new-second"],
  });
});

Deno.test("FoundationDB BackingPersistence restages after a concurrent clear", async () => {
  const memory = makeMemoryFoundationDb();
  const publishReached = Latch.makeUnsafe();
  const publishRelease = Latch.makeUnsafe();
  let transactions = 0;
  let blockTransaction = Number.POSITIVE_INFINITY;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) => {
      const transaction = ++transactions;
      const run = memory.database.withTransaction(effect, options);
      return transaction === blockTransaction
        ? publishReached.open.pipe(
          Effect.andThen(publishRelease.await),
          Effect.andThen(run),
        )
        : run;
    },
  };

  const value = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("clear-during-stage");
      while (transactions < 2) {
        yield* Effect.yieldNow;
      }
      transactions = 0;
      blockTransaction = 3;
      const write = yield* store.set(
        "key",
        { version: "after-clear", payload: "x".repeat(8_100_000) },
        undefined,
      ).pipe(Effect.forkScoped);
      yield* publishReached.await;
      yield* store.clear;
      yield* publishRelease.open;
      yield* Fiber.join(write);
      const stored = yield* store.get("key");
      return stored !== undefined && "version" in stored
        ? stored.version
        : undefined;
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(value, "after-clear");
});

Deno.test("FoundationDB BackingPersistence splits one large staged value", async () => {
  const memory = makeMemoryFoundationDb();
  let transactions = 0;
  const database: FoundationDb["Service"] = {
    ...memory.database,
    withTransaction: (effect, options) => {
      transactions += 1;
      return memory.database.withTransaction(effect, options);
    },
  };
  const large = { value: "x".repeat(3_300_000) };

  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const backing = yield* makeBackingPersistence();
      const store = yield* backing.make("split-stage");
      while (transactions < 2) {
        yield* Effect.yieldNow;
      }
      transactions = 0;
      yield* store.set("large", large, undefined);
      const writeTransactions = transactions;
      return {
        writeTransactions,
        value: yield* store.get("large"),
      };
    }).pipe(
      Effect.scoped,
      Effect.provideService(FoundationDb, database),
    ),
  );

  assertEquals(result.writeTransactions, 1);
  assertEquals(result.value, large);
});

Deno.test("FoundationDB RateLimiterStore implements fixed and token bucket transitions atomically", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* makeRateLimiterStore();
      const refillRate = Duration.millis(100);

      const fixedFirst = yield* store.fixedWindow({
        key: "fixed",
        tokens: 2,
        limit: 3,
        refillRate,
      });
      const fixedRejected = yield* store.fixedWindow({
        key: "fixed",
        tokens: 2,
        limit: 3,
        refillRate,
      });
      yield* TestClock.adjust("200 millis");
      const fixedReset = yield* store.fixedWindow({
        key: "fixed",
        tokens: 1,
        limit: 3,
        refillRate,
      });

      const bucketFirst = yield* store.tokenBucket({
        key: "bucket",
        tokens: 2,
        limit: 3,
        refillRate,
        allowOverflow: false,
      });
      yield* TestClock.adjust("50 millis");
      const bucketRejected = yield* store.tokenBucket({
        key: "bucket",
        tokens: 2,
        limit: 3,
        refillRate,
        allowOverflow: false,
      });
      yield* TestClock.adjust("50 millis");
      const bucketRefilled = yield* store.tokenBucket({
        key: "bucket",
        tokens: 2,
        limit: 3,
        refillRate,
        allowOverflow: false,
      });

      const counts = yield* Effect.forEach(
        Array.from({ length: 20 }),
        () =>
          store.fixedWindow({
            key: "concurrent",
            tokens: 1,
            limit: undefined,
            refillRate,
          }),
        { concurrency: "unbounded" },
      );

      return {
        fixedFirst,
        fixedRejected,
        fixedReset,
        bucketFirst,
        bucketRejected,
        bucketRefilled,
        counts: counts.map(([count]) => count).sort((a, b) => a - b),
      };
    }).pipe(
      Effect.provideService(FoundationDb, database),
      Effect.provide(TestClock.layer()),
    ),
  );

  assertEquals(result.fixedFirst, [2, 200]);
  assertEquals(result.fixedRejected, [4, 200]);
  assertEquals(result.fixedReset, [1, 100]);
  assertEquals(result.bucketFirst, [1, 0]);
  assertEquals(result.bucketRejected, [-1, 50]);
  assertEquals(result.bucketRefilled, [0, 0]);
  assertEquals(
    result.counts,
    Array.from({ length: 20 }, (_, index) => index + 1),
  );
});

Deno.test("FoundationDB RateLimiterStore persists adaptive feedback phases", async () => {
  const { database } = makeMemoryFoundationDb();
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* makeRateLimiterStore();
      const consumeOptions = {
        key: "adaptive",
        tokens: 2,
        fallbackLimit: 10,
        fallbackWindow: Duration.seconds(1),
      };
      const inactive = yield* store.adaptiveConsume(consumeOptions);
      yield* store.adaptiveFeedback({
        key: "adaptive",
        epoch: 0,
        tokens: 2,
        status: 429,
        retryAfter: Duration.millis(100),
      });
      const cooldown = yield* store.adaptiveConsume(consumeOptions);
      yield* TestClock.adjust("100 millis");
      const learning = yield* store.adaptiveConsume(consumeOptions);
      yield* store.adaptiveConsume({ ...consumeOptions, tokens: 3 });
      yield* store.adaptiveFeedback({
        key: "adaptive",
        epoch: learning.epoch,
        tokens: 2,
        status: 429,
        retryAfter: Duration.millis(50),
      });
      const learned = yield* store.adaptiveConsume({
        ...consumeOptions,
        tokens: 1,
      });
      return { inactive, cooldown, learning, learned };
    }).pipe(
      Effect.provideService(FoundationDb, database),
      Effect.provide(TestClock.layer()),
    ),
  );

  assertEquals(result.inactive.phase, "inactive");
  assertEquals(result.inactive.epoch, 0);
  assertEquals(Duration.toMillis(result.inactive.delay), 0);
  assertEquals(result.cooldown.phase, "cooldown");
  assertEquals(Duration.toMillis(result.cooldown.delay), 100);
  assertEquals(result.learning, {
    delay: Duration.zero,
    epoch: 1,
    phase: "learning",
  });
  assertEquals(result.learned.phase, "learned");
  assertEquals(result.learned.epoch, 2);
  assert(Duration.toMillis(result.learned.delay) > 0);
});
