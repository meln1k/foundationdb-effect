import { assert, assertEquals } from "@std/assert";
import { Effect, Layer, Result, Schema } from "effect";
import {
  Runner,
  RunnerAddress,
  RunnerStorage,
  ShardId,
  ShardingConfig,
} from "effect/cluster";
import { TestClock } from "effect/testing";
import { RunnerMachineIdsExhaustedError } from "../../../src/cluster/errors.ts";
import {
  layerRunnerStorage,
  makeRunnerStorage,
} from "../../../src/cluster/runner-storage.ts";
import { DirectorySubspace } from "../../../src/directory/mod.ts";
import {
  FoundationDb,
  FoundationDbTransaction,
} from "../../../src/FoundationDb.ts";
import type { TransactionOptions } from "../../../src/model.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const runner = (port: number, weight = 1) =>
  Runner.make({
    address: RunnerAddress.make("localhost", port),
    groups: ["default"],
    weight,
  });
const firstRunner = runner(3001);
const secondRunner = runner(3002);
const shard = ShardId.make("default", 1);
const otherShard = ShardId.make("other", 1);
const missingShard = ShardId.make("default", 2);

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    FoundationDb | ShardingConfig.ShardingConfig | TestClock.TestClock
  >,
  database = testDatabase().database,
  config: Partial<ShardingConfig.ShardingConfig["Service"]> = {
    shardLockExpiration: "1 second",
  },
) =>
  Effect.runPromise(effect.pipe(
    Effect.provideService(FoundationDb, database),
    Effect.provide(ShardingConfig.layer(config)),
    Effect.provide(TestClock.layer()),
  ));

storageTest(
  "runner registration is durable, stable, updated, and namespace isolated",
  async () => {
    await run(Effect.gen(function* () {
      const first = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      assertEquals(yield* first.getRunners, []);
      const id = yield* first.register(firstRunner, true);
      assertEquals(id, 0);
      const updated = runner(3001, 3);
      assertEquals(yield* second.register(updated, false), id);
      assertEquals(yield* first.getRunners, [[updated, false]]);
      const otherId = yield* second.register(secondRunner, true);
      assert(otherId !== id);
      const reopened = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      assertEquals(yield* reopened.register(updated, false), id);
      yield* reopened.setRunnerHealth(updated.address, true);
      assertEquals(yield* second.getRunners, [
        [updated, true],
        [secondRunner, true],
      ]);
      const isolated = yield* makeRunnerStorage({
        directory: testDatabase().directory,
        directoryPath: ["isolated"],
      });
      assertEquals(yield* isolated.getRunners, []);
      yield* first.acquire(firstRunner.address, [shard]);
      assertEquals(yield* isolated.acquire(secondRunner.address, [shard]), [
        shard,
      ]);
      yield* first.unregister(firstRunner.address);
      yield* first.unregister(firstRunner.address);
      yield* first.setRunnerHealth(firstRunner.address, true);
      yield* first.refresh(firstRunner.address, []);
      assertEquals(yield* reopened.getRunners, [[secondRunner, true]]);
      // unregister only changes registration, not independently held leases.
      assertEquals(yield* second.acquire(secondRunner.address, [shard]), []);
      yield* first.releaseAll(firstRunner.address);
      assertEquals(yield* second.acquire(secondRunner.address, [shard]), [
        shard,
      ]);
    }));
  },
);

storageTest(
  "independent runner stores atomically register and acquire concurrently",
  async () => {
    await run(Effect.gen(function* () {
      const stores = yield* Effect.forEach(
        Array.from({ length: 16 }),
        () => makeRunnerStorage({ directory: testDatabase().directory }),
      );
      const ids = yield* Effect.forEach(
        stores,
        (store, index) => store.register(runner(4000 + index), true),
        { concurrency: "unbounded" },
      );
      assertEquals(new Set(ids).size, stores.length);
      assert(ids.every((id) => Number.isInteger(id) && id >= 0 && id < 1024));
      const sameIds = yield* Effect.forEach(
        stores,
        (store) => store.register(firstRunner, true),
        { concurrency: "unbounded" },
      );
      assertEquals(new Set(sameIds).size, 1);
      const acquisitions = yield* Effect.forEach(
        stores,
        (store, index) => store.acquire(runner(4000 + index).address, [shard]),
        { concurrency: "unbounded" },
      );
      assertEquals(acquisitions.filter((ids) => ids.length === 1).length, 1);
      assertEquals((yield* stores[0].getRunners).length, 17);
    }));
  },
);

storageTest(
  "leases respect expiration boundary, stale ownership, and owned-only release",
  async () => {
    await run(Effect.gen(function* () {
      const first = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      const a = firstRunner.address;
      const b = secondRunner.address;
      assertEquals(yield* first.acquire(a, []), []);
      assertEquals(yield* first.refresh(a, [missingShard]), []);
      assertEquals(yield* first.acquire(a, [shard, shard, otherShard]), [
        shard,
        otherShard,
      ]);
      yield* TestClock.adjust("999 millis");
      assertEquals(yield* second.acquire(b, [shard]), []);
      yield* TestClock.adjust("1 millis");
      assertEquals(yield* second.acquire(b, [shard]), []);
      yield* TestClock.adjust("1 millis");
      assertEquals(yield* second.acquire(b, [shard]), [shard]);
      assertEquals(yield* first.refresh(a, [shard, otherShard, missingShard]), [
        otherShard,
      ]);
      yield* first.release(a, shard);
      yield* first.release(a, missingShard);
      yield* first.releaseAll(a);
      assertEquals(yield* second.refresh(b, [shard, otherShard]), [shard]);
      assertEquals(yield* first.acquire(a, [shard, otherShard]), [otherShard]);
      yield* second.release(b, shard);
      assertEquals(yield* first.acquire(a, [shard]), [shard]);
    }));
  },
);

storageTest(
  "owned acquire and refresh renew leases, including expired untouched leases",
  async () => {
    await run(Effect.gen(function* () {
      const first = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      const acquire = first.acquire(firstRunner.address, [shard]);
      yield* acquire;
      yield* TestClock.adjust("800 millis");
      yield* acquire;
      yield* TestClock.adjust("800 millis");
      assertEquals(yield* second.acquire(secondRunner.address, [shard]), []);
      assertEquals(yield* first.refresh(firstRunner.address, [shard]), [shard]);
      yield* TestClock.adjust("1001 millis");
      assertEquals(yield* first.refresh(firstRunner.address, [shard]), [shard]);
      assertEquals(yield* second.acquire(secondRunner.address, [shard]), []);
      yield* TestClock.adjust("1001 millis");
      assertEquals(yield* second.acquire(secondRunner.address, [shard]), [
        shard,
      ]);
      assertEquals(yield* first.refresh(firstRunner.address, [shard]), []);
    }));
  },
);

storageTest(
  "health does not extend heartbeat; empty refresh does; expired IDs stay reserved",
  async () => {
    await run(Effect.gen(function* () {
      const first = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      const id = yield* first.register(firstRunner, true);
      yield* TestClock.adjust("999 millis");
      yield* second.setRunnerHealth(firstRunner.address, false);
      assertEquals(yield* first.getRunners, [[firstRunner, false]]);
      yield* TestClock.adjust("1 millis");
      assertEquals(yield* first.getRunners, []);
      assertEquals(yield* second.refresh(firstRunner.address, []), []);
      assertEquals(yield* second.getRunners, [[firstRunner, false]]);
      yield* TestClock.adjust("1001 millis");
      const otherId = yield* second.register(secondRunner, true);
      assert(otherId !== id);
      assertEquals(yield* first.register(firstRunner, true), id);
      assertEquals(yield* first.getRunners, [
        [firstRunner, true],
        [secondRunner, true],
      ]);
    }));
  },
);

storageTest(
  "machine ID pool uses all 1024 slots, fails safely, and reuses freed holes",
  async () => {
    await run(Effect.gen(function* () {
      const store = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      // Fill through the real API, including ID zero and the final slot.
      const ids = yield* Effect.forEach(
        Array.from({ length: 1024 }, (_, index) => runner(10000 + index)),
        (value) => store.register(value, true),
      );
      assertEquals(ids, Array.from({ length: 1024 }, (_, index) => index));
      const full = yield* Effect.result(store.register(firstRunner, true));
      assert(Result.isFailure(full));
      assertEquals(full.failure._tag, "PersistenceError");
      assert(Schema.is(RunnerMachineIdsExhaustedError)(full.failure.cause));
      assertEquals(full.failure.cause.address, "localhost:3001");
      assertEquals(full.failure.cause.capacity, 1024);
      assertEquals((yield* store.getRunners).length, 1024);
      assertEquals(yield* store.register(runner(10000), false), 0);
      yield* store.unregister(runner(10512).address);
      const reopened = yield* makeRunnerStorage({
        directory: testDatabase().directory,
      });
      assertEquals(yield* reopened.register(firstRunner, true), 512);
      const stillFull = yield* Effect.result(
        store.register(secondRunner, true),
      );
      assert(Result.isFailure(stillFull));
      yield* store.unregister(runner(11023).address);
      assertEquals(yield* reopened.register(secondRunner, true), 1023);
    }));
  },
);

storageTest(
  "runner storage layer follows ShardingConfig's default lock duration",
  async () => {
    const { database, directory } = testDatabase();
    await Effect.runPromise(
      Effect.gen(function* () {
        const store = yield* RunnerStorage.RunnerStorage;
        yield* store.acquire(firstRunner.address, [shard]);
        yield* TestClock.adjust("35 seconds");
        assertEquals(yield* store.acquire(secondRunner.address, [shard]), []);
        yield* TestClock.adjust("1 millis");
        assertEquals(yield* store.acquire(secondRunner.address, [shard]), [
          shard,
        ]);
      }).pipe(
        Effect.provide(
          layerRunnerStorage({ directory }).pipe(
            Layer.provide(Layer.succeed(FoundationDb, database)),
            Layer.provide(ShardingConfig.layerDefaults),
          ),
        ),
        Effect.provide(TestClock.layer()),
      ),
    );
  },
);

storageTest(
  "runner storage validates lock duration and persistence records",
  async () => {
    for (const shardLockExpiration of [0, Infinity]) {
      await run(
        Effect.gen(function* () {
          const result = yield* Effect.result(
            makeRunnerStorage({ directory: testDatabase().directory }),
          );
          assert(Result.isFailure(result));
          assertEquals(result.failure._tag, "PersistenceError");
        }),
        undefined,
        { shardLockExpiration },
      );
    }
    await run(Effect.gen(function* () {
      const database = yield* FoundationDb;
      const { directory } = testDatabase();
      const store = yield* makeRunnerStorage({ directory });
      yield* store.register(firstRunner, true);
      yield* database.withTransaction(Effect.gen(function* () {
        const root = yield* directory.open([
          "effect-foundationdb",
          "runner-storage",
        ]);
        assert(root instanceof DirectorySubspace);
        const transaction = yield* FoundationDbTransaction;
        yield* transaction.set(
          yield* root.pack(["runners", "localhost:3001"]),
          new TextEncoder().encode('{"machineId":1024}'),
        );
      }));
      const corruptRegistration = yield* Effect.result(store.getRunners);
      assert(Result.isFailure(corruptRegistration));
      assertEquals(corruptRegistration.failure._tag, "PersistenceError");
      yield* database.withTransaction(Effect.gen(function* () {
        const root = yield* directory.open([
          "effect-foundationdb",
          "runner-storage",
        ]);
        assert(root instanceof DirectorySubspace);
        const transaction = yield* FoundationDbTransaction;
        yield* transaction.set(
          yield* root.pack(["leases", shard.toString()]),
          new TextEncoder().encode("not JSON"),
        );
      }));
      const corruptLease = yield* Effect.result(
        store.acquire(secondRunner.address, [shard]),
      );
      assert(Result.isFailure(corruptLease));
      assertEquals(corruptLease.failure._tag, "PersistenceError");
    }));
  },
);

storageTest(
  "runner operations bound transactions and reject ambiguous commit replay",
  async () => {
    const fixture = testDatabase();
    const observed: Array<TransactionOptions | undefined> = [];
    const database: FoundationDb["Service"] = {
      ...fixture.database,
      withTransaction: (effect, options) => {
        observed.push(options);
        return fixture.database.withTransaction(effect, options);
      },
    };
    await run(
      Effect.gen(function* () {
        const store = yield* makeRunnerStorage({
          directory: fixture.directory,
          transactionOptions: { retryOnMaybeCommitted: true },
        });
        yield* store.register(firstRunner, true);
        yield* store.acquire(firstRunner.address, [shard]);
        yield* store.refresh(firstRunner.address, []);
        yield* store.release(firstRunner.address, shard);
        yield* store.releaseAll(firstRunner.address);
        yield* store.setRunnerHealth(firstRunner.address, false);
        yield* store.getRunners;
        yield* store.unregister(firstRunner.address);
      }),
      database,
    );
    assertEquals(observed.length, 8);
    assert(
      observed.every((options) =>
        options?.timeoutMs === 5000 && options.retryLimit === 10 &&
        options.maxRetryDelayMs === 1000 &&
        options.retryOnMaybeCommitted === false
      ),
    );
  },
);
