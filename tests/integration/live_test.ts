import { assertEquals } from "@std/assert";
import { Context, Effect, Exit, Layer, Schedule, Schema, Scope } from "effect";
import { PersistedQueue } from "effect/unstable/persistence";
import {
  DirectoryLayer,
  DirectoryPartition,
  DirectorySubspace,
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  KeySelector,
  layerPersistedQueueStore,
  Subspace,
} from "../../mod.ts";
import type { DirectoryOutput } from "../../mod.ts";

const encoder = new TextEncoder();
const bytes = (value: string): Uint8Array => encoder.encode(value);

const directoryPrefixIds = (
  outputs: ReadonlyArray<DirectoryOutput>,
): ReadonlyArray<string> =>
  outputs.map((output) => {
    if (!(output instanceof DirectorySubspace)) {
      throw new Error("expected a regular directory");
    }
    return Array.from(output.prefix).join(",");
  });

Deno.test({
  name:
    "live FFI bridge performs primitives and concurrent persistent queue operations",
  permissions: { ffi: true, read: true },
  timeout: 120_000,
  fn: async () => {
    const prefix = bytes(
      `\x02effect-foundationdb/${Date.now()}-${Math.random()}/`,
    );
    const end = new Uint8Array([...prefix, 0xff]);
    const firstKey = new Uint8Array([...prefix, 1]);
    const secondKey = new Uint8Array([...prefix, 2]);
    const thirdKey = new Uint8Array([...prefix, 3]);
    const empty = new Uint8Array();
    const binary = new Uint8Array([0, 255, 1, 128]);
    const thirdValue = bytes("third");

    const program = Effect.gen(function* () {
      const database = yield* FoundationDb;
      yield* database.clearRange(prefix, end);

      const readYourWrites = yield* database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          yield* transaction.set(firstKey, empty);
          yield* transaction.set(secondKey, binary);
          yield* transaction.set(thirdKey, thirdValue);
          return yield* transaction.getMany([
            firstKey,
            secondKey,
            new Uint8Array([...prefix, 99]),
          ]);
        }),
      );
      assertEquals(readYourWrites, [empty, binary, undefined]);

      assertEquals(yield* database.get(firstKey), empty);
      assertEquals(yield* database.get(secondKey), binary);
      assertEquals(
        yield* database.get(new Uint8Array([...prefix, 99])),
        undefined,
      );
      assertEquals(
        yield* database.getMany([
          thirdKey,
          new Uint8Array([...prefix, 99]),
          firstKey,
        ], { snapshot: true }),
        [thirdValue, undefined, empty],
      );

      const firstTwo = yield* database.getRange(
        keyRange(prefix, end, { limit: 2 }),
      );
      assertEquals(
        Array.from(firstTwo, ({ key }) => key),
        [firstKey, secondKey],
      );

      const reversed = yield* database.getRange(
        keyRange(prefix, end, { reverse: true }),
      );
      assertEquals(
        Array.from(reversed, ({ key }) => key),
        [thirdKey, secondKey, firstKey],
      );

      const selected = yield* database.withTransaction(
        Effect.flatMap(FoundationDbTransaction, (transaction) =>
          Effect.all([
            transaction.getKey(KeySelector.firstGreaterOrEqual(secondKey)),
            transaction.getKey(KeySelector.firstGreaterThan(secondKey)),
          ])),
      );
      assertEquals(selected, [secondKey, thirdKey]);

      yield* database.clear(firstKey);
      assertEquals(yield* database.get(firstKey), undefined);
      yield* database.clearRange(secondKey, end);
      assertEquals(yield* database.get(secondKey), undefined);
      assertEquals(yield* database.get(thirdKey), undefined);

      const directory = yield* DirectoryLayer.make({
        nodeSubspace: new Subspace(new Uint8Array([...prefix, 0x10])),
        contentSubspace: new Subspace(new Uint8Array([...prefix, 0x20])),
      });
      const allocated = yield* Effect.forEach(
        ["alpha", "beta", "gamma", "delta"],
        (name) => database.withTransaction(directory.create([name])),
        { concurrency: "unbounded" },
      );
      const allocatedPrefixes = directoryPrefixIds(allocated);
      assertEquals(new Set(allocatedPrefixes).size, allocated.length);
      assertEquals(
        yield* database.withTransaction(directory.list()),
        ["alpha", "beta", "delta", "gamma"],
      );

      const partition = yield* database.withTransaction(
        directory.create(["partition"], { layer: bytes("partition") }),
      );
      if (!(partition instanceof DirectoryPartition)) {
        return yield* Effect.die("expected a directory partition");
      }
      const nested = yield* database.withTransaction(
        partition.create(["nested"]),
      );
      if (!(nested instanceof DirectorySubspace)) {
        return yield* Effect.die("expected a nested directory");
      }
      assertEquals(nested.path, ["partition", "nested"]);
      assertEquals(
        yield* database.withTransaction(
          directory.exists(["partition", "nested"]),
        ),
        true,
      );

      // Ported from foundationdb-rs 0.11.0 tests/hca.rs.
      const allocationCount = 1_000;
      const allocationNames = Array.from(
        { length: allocationCount },
        (_, index) => `allocation-${index}`,
      );
      const sequentialDirectory = yield* DirectoryLayer.make({
        nodeSubspace: new Subspace(new Uint8Array([...prefix, 0x30])),
        contentSubspace: new Subspace(new Uint8Array([...prefix, 0x31])),
      });
      const sequentialAllocations = yield* Effect.forEach(
        allocationNames,
        (name) => database.withTransaction(sequentialDirectory.create([name])),
        { concurrency: 1 },
      );
      assertEquals(
        new Set(directoryPrefixIds(sequentialAllocations)).size,
        allocationCount,
      );

      const concurrentDirectory = yield* DirectoryLayer.make({
        nodeSubspace: new Subspace(new Uint8Array([...prefix, 0x40])),
        contentSubspace: new Subspace(new Uint8Array([...prefix, 0x41])),
      });
      const concurrentAllocations = yield* Effect.forEach(
        allocationNames,
        (name) => database.withTransaction(concurrentDirectory.create([name])),
        { concurrency: "unbounded" },
      );
      assertEquals(
        new Set(directoryPrefixIds(concurrentAllocations)).size,
        allocationCount,
      );

      const persisted = yield* PersistedQueue.make({
        name: "live",
        schema: Schema.Struct({ value: Schema.String }),
        retrySchedule: Schedule.spaced(0),
      });
      yield* persisted.offer({ value: "first" }, { id: "deduplicated" });
      yield* persisted.offer({ value: "duplicate" }, { id: "deduplicated" });
      yield* persisted.offer({ value: "second" });
      assertEquals(
        yield* persisted.take((value, metadata) =>
          Effect.succeed([value.value, metadata.attempts] as const)
        ),
        ["first", 1],
      );

      const failedAttempt = yield* persisted.take((_, { attempts }) =>
        Effect.fail(attempts)
      ).pipe(Effect.flip);
      assertEquals(failedAttempt, 1);
      assertEquals(
        yield* persisted.take((value, { attempts }) =>
          Effect.succeed([value.value, attempts] as const)
        ),
        ["second", 2],
      );

      const workerValues = Array.from(
        { length: 8 },
        (_, index) => `worker-${index}`,
      );
      yield* Effect.forEach(
        workerValues,
        (value) => persisted.offer({ value }),
        { concurrency: "unbounded", discard: true },
      );
      const processed = yield* Effect.forEach(
        workerValues,
        () => persisted.take((item) => Effect.succeed(item.value)),
        { concurrency: "unbounded" },
      );
      assertEquals([...processed].sort(), workerValues.sort());
    }).pipe(
      Effect.ensuring(
        Effect.gen(function* () {
          const database = yield* FoundationDb;
          yield* database.clearRange(prefix, end);
        }).pipe(Effect.ignore),
      ),
    );

    const foundationDbOptions = {
      libraryPath: "./target/debug/libeffect_foundationdb_native.so",
      clusterFile: "/etc/foundationdb/fdb.cluster",
    } as const;

    await Effect.runPromise(
      Effect.scoped(Effect.gen(function* () {
        const firstScope = yield* Effect.acquireRelease(
          Scope.make(),
          (scope, exit) => Scope.close(scope, exit),
        );
        const secondScope = yield* Effect.acquireRelease(
          Scope.make(),
          (scope, exit) => Scope.close(scope, exit),
        );
        const firstContext = yield* Layer.buildWithScope(
          FoundationDb.layer(foundationDbOptions),
          firstScope,
        );
        const secondContext = yield* Layer.buildWithScope(
          FoundationDb.layer(foundationDbOptions),
          secondScope,
        );
        const firstDatabase = Context.get(firstContext, FoundationDb);
        const secondDatabase = Context.get(secondContext, FoundationDb);

        assertEquals(yield* firstDatabase.get(firstKey), undefined);
        yield* Scope.close(firstScope, Exit.succeed(undefined));

        const foundationDbLayer = Layer.succeed(
          FoundationDb,
          secondDatabase,
        );
        const queueDirectory = yield* DirectoryLayer.make({
          nodeSubspace: new Subspace(new Uint8Array([...prefix, 0xf0])),
          contentSubspace: new Subspace(new Uint8Array([...prefix, 0xf1])),
        });
        const storeLayer = layerPersistedQueueStore({
          directory: queueDirectory,
          directoryPath: ["persisted-queue"],
          pollInterval: "5 millis",
        }).pipe(Layer.provideMerge(foundationDbLayer));
        const applicationLayer = PersistedQueue.layer.pipe(
          Layer.provideMerge(storeLayer),
        );

        yield* program.pipe(Effect.provide(applicationLayer));
      })),
    );
  },
});
