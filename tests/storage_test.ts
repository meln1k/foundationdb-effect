import "./storage/directory/directory.ts";
import "./storage/persistence/backends.ts";
import "./storage/persisted-queue/store.ts";
import "./storage/cluster/message-storage.ts";
import "./storage/cluster/message-order.ts";
import "./storage/cluster/message-batching.ts";
import "./storage/cluster/message-readiness.ts";
import "./storage/cluster/runner-storage.ts";
import "./storage/eventlog/backends.ts";
import "./storage/eventlog/performance.ts";
import "./storage/eventlog/journal-order.ts";
import { assert, assertEquals } from "@std/assert";
import { Effect, Exit } from "effect";
import { DirectoryLayer, FoundationDb, makeKeyValueStore } from "../mod.ts";
import {
  makeTestFixture,
  runStorageTests,
  storageTest,
  testDatabase,
} from "./support/real-database.ts";

storageTest(
  "test directories isolate data and clean up after success and failure",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const survivor = testDatabase();
        const { database } = survivor;
        const parent = yield* DirectoryLayer.make();
        const outside = yield* makeKeyValueStore({
          directory: survivor.directory,
        });
        yield* outside.set("same-key", "survives");
        for (const fail of [false, true]) {
          let owned:
            | Effect.Success<ReturnType<typeof makeTestFixture>>
            | undefined;
          const exit = yield* Effect.exit(
            Effect.scoped(Effect.gen(function* () {
              const fixture = yield* makeTestFixture(database);
              owned = fixture;
              const inside = yield* makeKeyValueStore({
                directory: fixture.directory,
              });
              assertEquals(yield* inside.get("same-key"), undefined);
              yield* inside.set("same-key", "temporary");
              assertEquals(yield* outside.get("same-key"), "survives");
              // Cleanup must include raw keys and the entire prefix, even 0xff suffixes.
              yield* database.set(
                fixture.prefix(0xff, 0xff),
                Uint8Array.of(42),
              );
              assert((yield* fixture.entries()).length > 0);
              if (fail) {
                yield* Effect.promise(() =>
                  Promise.reject(new Error("test failure"))
                );
              }
            })),
          );
          assertEquals(Exit.isFailure(exit), fail);
          assert(owned !== undefined);
          assertEquals(yield* owned.entries(), []);
          assertEquals(
            yield* database.withTransaction(parent.exists(owned.root.path)),
            false,
          );
          assertEquals(yield* outside.get("same-key"), "survives");
          assertEquals(
            yield* database.withTransaction(parent.exists(survivor.root.path)),
            true,
          );
        }
      }).pipe(Effect.provideService(FoundationDb, testDatabase().database)),
    ),
);

Deno.test({
  name: "storage behavior against real FoundationDB",
  timeout: 300_000,
  fn: runStorageTests,
});
