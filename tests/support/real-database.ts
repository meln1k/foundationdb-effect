import { assert } from "@std/assert";
import { Context, Effect, Layer, Scope } from "effect";
import { FoundationDb } from "../../src/FoundationDb.ts";
import {
  DirectoryLayer,
  DirectorySubspace,
  strinc,
} from "../../src/directory/mod.ts";
import { keyRange } from "../../src/model.ts";
import { Subspace } from "../../src/tuple/mod.ts";

const cases: Array<{ name: string; run: () => unknown }> = [];
let current: Effect.Success<ReturnType<typeof makeTestFixture>> | undefined;

/** Registers a sequential case within the suite's single native network lifetime. */
export const storageTest = (name: string, run: () => unknown): void => {
  cases.push({ name, run });
};

/** Override for another development cluster; tests never clear its keyspace. */
export const testClusterFile = (): string =>
  Deno.env.get("FDB_TEST_CLUSTER_FILE") ?? "/etc/foundationdb/fdb.cluster";

/** Allocate a real directory; release removes only its metadata and prefix. */
export const makeTestFixture = (database: FoundationDb["Service"]) =>
  Effect.gen(function* () {
    const parent = yield* DirectoryLayer.make();
    const root = yield* Effect.acquireRelease(
      database.withTransaction(parent.create([
        `effect-foundationdb-test-${crypto.randomUUID()}`,
      ])),
      (root) =>
        database.withTransaction(parent.remove(root.path)).pipe(Effect.orDie),
    );
    assert(root instanceof DirectorySubspace);
    const prefix = (...suffix: number[]) =>
      Uint8Array.of(...root.prefix, ...suffix);
    // Keep even malformed directory metadata and manually allocated content
    // within the owned root. The client and transactions are not wrapped.
    const directory = yield* DirectoryLayer.make({
      nodeSubspace: new Subspace(prefix(0xfe)),
      contentSubspace: root,
    });
    return {
      database,
      root,
      prefix,
      directory,
      entries: () =>
        database.getRange(keyRange(root.prefix, strinc(root.prefix))),
    };
  });

/** Real native client and directory, shared within one sequential case. */
export const testDatabase = () => {
  if (current === undefined) {
    throw new Error("testDatabase must be used inside storageTest");
  }
  return current;
};

export const makeTestDirectory = (
  options: Parameters<typeof DirectoryLayer.make>[0] = {},
): DirectoryLayer => {
  const { prefix, root } = testDatabase();
  return Effect.runSync(DirectoryLayer.make({
    nodeSubspace: new Subspace(prefix(0xfe)),
    contentSubspace: root,
    ...options,
  }));
};

export const runStorageTests = (test: Deno.TestContext): Promise<void> =>
  Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const scope = yield* Scope.Scope;
    const context = yield* Layer.buildWithScope(
      FoundationDb.layer({
        libraryPath: "./target/debug/libeffect_foundationdb_native.so",
        clusterFile: testClusterFile(),
        transactionDefaults: { timeoutMs: 5_000, retryLimit: 10 },
      }),
      scope,
    );
    const database = Context.get(context, FoundationDb);
    // Real transactions, native futures and resource sanitizers stay enabled.
    // No mutex, key translation, fake clock, or emulated FDB semantics here.
    for (const { name, run } of cases) {
      yield* Effect.promise(() =>
        test.step(
          name,
          () =>
            Effect.runPromise(Effect.scoped(Effect.gen(function* () {
              current = yield* makeTestFixture(database);
              yield* Effect.promise(async () => {
                try {
                  await run();
                } finally {
                  current = undefined;
                }
              });
            }))),
        )
      );
    }
  })));
