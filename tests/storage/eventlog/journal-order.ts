import { assert, assertEquals } from "@std/assert";
import { Effect, Fiber, Latch, Option, PubSub, Schema } from "effect";
import * as Journal from "effect/eventlog/EventJournal";
import {
  FoundationDb,
  FoundationDbTransaction,
} from "../../../src/FoundationDb.ts";
import type { FoundationDbShape } from "../../../src/FoundationDb.ts";
import { DirectorySubspace } from "../../../src/directory/mod.ts";
import { makeEventJournal } from "../../../src/eventlog/mod.ts";
import { keyRange } from "../../../src/model.ts";
import { Versionstamp } from "../../../src/tuple/mod.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const options = () => ({ directory: testDatabase().directory, pageSize: 2 });
const run = <A, E>(effect: Effect.Effect<A, E, FoundationDb>) =>
  Effect.runPromise(effect.pipe(
    Effect.provideService(FoundationDb, testDatabase().database),
    Effect.timeout("15 seconds"),
  ));
const write = (journal: Journal.EventJournal["Service"], event: string) =>
  journal.write({
    event,
    primaryKey: event,
    payload: Uint8Array.of(42),
    effect: Effect.succeed,
  });
const ids = (entries: ReadonlyArray<Journal.Entry>) =>
  entries.map((e) => e.idString);
const collectIds = (entries: ReadonlyArray<Journal.Entry>) =>
  Effect.succeed(ids(entries));
const root = Effect.gen(function* () {
  const fixture = testDatabase();
  const root = yield* fixture.database.withTransaction(fixture.directory.open([
    "effect-foundationdb",
    "eventlog",
    "journal",
  ]));
  assert(root instanceof DirectorySubspace);
  return root;
});
const ordered = Effect.fnUntraced(function* (name: "inserted" | "local") {
  const space = yield* (yield* root).subspace(["index", name]);
  const rows = yield* testDatabase().database.getRange(
    keyRange(...yield* space.range()),
  );
  assert(rows.every((row) => row.value.length === 0));
  return yield* Effect.forEach(rows, (row) =>
    space.unpack(
      row.key,
      Schema.Tuple([Versionstamp.schema, Journal.EntryId]),
    ));
});

storageTest(
  "journal versionstamps allow same-snapshot writers with zero retries and preserve commit and within-transaction order",
  () =>
    run(Effect.scoped(Effect.gen(function* () {
      const { database } = testDatabase();
      const a = yield* makeEventJournal(options());
      const b = yield* makeEventJournal(options());
      const subscription = yield* a.changes;
      const staged = Latch.makeUnsafe();
      const release = Latch.makeUnsafe();
      let readVersion = 0n;
      let attempts = 0;
      const first = yield* database.withTransaction(
        Effect.gen(function* () {
          attempts++;
          readVersion = yield* (yield* FoundationDbTransaction)
            .getReadVersion();
          const one = yield* write(a, "a1");
          const two = yield* write(b, "a2");
          yield* staged.open;
          yield* release.await;
          return [one, two];
        }),
        { retryLimit: 0 },
      ).pipe(Effect.forkScoped);
      yield* staged.await;
      const remotes = [20, 10].map((msecs) =>
        new Journal.Entry({
          id: Journal.makeEntryIdUnsafe({ msecs }),
          event: "remote",
          primaryKey: String(msecs),
          payload: Uint8Array.of(msecs),
        })
      );
      const second = yield* database.withTransaction(
        Effect.gen(function* () {
          yield* (yield* FoundationDbTransaction).setReadVersion(readVersion);
          const local = yield* write(b, "b");
          yield* a.writeFromRemote({
            remoteId: Journal.makeRemoteIdUnsafe(),
            entries: remotes.map((entry, remoteSequence) =>
              new Journal.RemoteEntry({ entry, remoteSequence })
            ),
            effect: () => Effect.void,
          });
          return local;
        }),
        { retryLimit: 0 },
      );
      yield* release.open;
      const lateCommit = yield* Fiber.join(first);
      assertEquals(attempts, 1);
      const insertion = yield* ordered("inserted");
      assertEquals(
        insertion.map(([, id]) => id),
        [second, ...remotes, ...lateCommit].map((e) => e.id),
      );
      assertEquals(insertion.map(([stamp]) => stamp.userVersion), [
        0,
        1,
        2,
        0,
        1,
      ]);
      assert(insertion.every(([stamp]) => stamp.isComplete));
      assertEquals(
        (yield* Effect.forEach([0, 1, 2], () => PubSub.take(subscription))).map(
          (e) => e.id,
        ),
        [second, ...lateCommit].map((e) => e.id),
      );
      assertEquals(
        (yield* ordered("local")).map(([, id]) => id),
        [second, ...lateCommit].map((e) => e.id),
      );
      for (const counter of ["entryNext", "localNext"]) {
        assertEquals(
          yield* database.getRange(
            keyRange(...yield* (yield* root).range(["values", counter])),
          ),
          [],
        );
      }
    }))),
);

storageTest(
  "journal ACKs pending entries without skipping a foreign commit between its snapshot and commit",
  () =>
    run(Effect.gen(function* () {
      const { database } = testDatabase();
      const a = yield* makeEventJournal(options());
      const b = yield* makeEventJournal(options());
      const remote = Journal.makeRemoteIdUnsafe();
      const seed = yield* write(a, "seed");
      let foreign: Journal.Entry | undefined;
      yield* database.withTransaction(
        Effect.gen(function* () {
          const pending = yield* write(a, "pending");
          assertEquals(
            Option.getOrThrow(
              yield* b.withRemoteUncommited(remote, (entries) =>
                Effect.gen(function* () {
                  assertEquals(ids(entries), ids([seed, pending]));
                  // A different FDB transaction commits after our read version, before
                  // our pending stamps are assigned. Its entry must remain discoverable.
                  foreign = yield* Effect.promise(() =>
                    Effect.runPromise(write(b, "foreign"))
                  );
                  return ids(entries);
                })),
            ),
            ids([seed, pending]),
          );
          assert(
            Option.isNone(yield* a.withRemoteUncommited(remote, collectIds)),
          );
        }),
        { retryLimit: 0 },
      );
      assert(foreign !== undefined);
      const reopened = yield* makeEventJournal(options());
      assertEquals(
        Option.getOrThrow(
          yield* reopened.withRemoteUncommited(remote, collectIds),
        ),
        [foreign.idString],
      );
      assert(
        Option.isNone(yield* reopened.withRemoteUncommited(remote, collectIds)),
      );
    })),
);

storageTest(
  "journal pending order is directory-isolated and destroy removes staged indexes without resetting active subscriptions",
  () =>
    run(Effect.scoped(Effect.gen(function* () {
      const { database } = testDatabase();
      const a = yield* makeEventJournal(options());
      const b = yield* makeEventJournal(options());
      const isolated = yield* makeEventJournal({
        ...options(),
        directoryPath: ["isolated"],
      });
      const subscription = yield* a.changes;
      const remote = Journal.makeRemoteIdUnsafe();
      const survivor = yield* database.withTransaction(Effect.gen(function* () {
        const removed = yield* write(a, "removed");
        const outside = yield* write(isolated, "outside");
        assertEquals(
          Option.getOrThrow(yield* b.withRemoteUncommited(remote, collectIds)),
          [removed.idString],
        );
        assertEquals(
          Option.getOrThrow(
            yield* isolated.withRemoteUncommited(remote, collectIds),
          ),
          [outside.idString],
        );
        yield* b.destroy;
        assert(
          Option.isNone(yield* a.withRemoteUncommited(remote, collectIds)),
        );
        const kept = yield* write(a, "kept");
        assertEquals(
          Option.getOrThrow(yield* b.withRemoteUncommited(remote, collectIds)),
          [kept.idString],
        );
        assertEquals(ids(yield* isolated.entries), [outside.idString]);
        return kept;
      }));
      assertEquals((yield* PubSub.take(subscription)).id, survivor.id);
      assertEquals(ids(yield* b.entries), [survivor.idString]);
      assertEquals((yield* ordered("inserted")).map(([, id]) => id), [
        survivor.id,
      ]);
      assertEquals((yield* ordered("local")).map(([, id]) => id), [
        survivor.id,
      ]);
      assert(Option.isNone(yield* b.withRemoteUncommited(remote, collectIds)));
      assertEquals(
        yield* database.withTransaction(Effect.gen(function* () {
          yield* a.destroy;
          yield* write(b, "rollback");
          return yield* Effect.fail("rollback");
        })).pipe(Effect.flip),
        "rollback",
      );
      assertEquals(ids(yield* b.entries), [survivor.idString]);
    }))),
);

storageTest(
  "journal pending ordinals and ACKs restart on a real transaction conflict",
  () =>
    run(Effect.gen(function* () {
      const { database, prefix } = testDatabase();
      const journal = yield* makeEventJournal(options());
      const other = yield* makeEventJournal(options());
      const remote = Journal.makeRemoteIdUnsafe();
      const conflictKey = prefix(0x41);
      let attempts = 0;
      const committed = yield* database.withTransaction(
        Effect.gen(function* () {
          attempts++;
          yield* (yield* FoundationDbTransaction).get(conflictKey);
          const entry = yield* write(journal, `attempt-${attempts}`);
          assertEquals(
            Option.getOrThrow(
              yield* other.withRemoteUncommited(remote, collectIds),
            ),
            [entry.idString],
          );
          if (attempts === 1) {
            yield* Effect.promise(() =>
              Effect.runPromise(database.set(conflictKey, Uint8Array.of(1)))
            );
          }
          return entry;
        }),
        { retryLimit: 1 },
      );
      assertEquals(attempts, 2);
      assertEquals(ids(yield* journal.entries), [committed.idString]);
      const rows = yield* ordered("inserted");
      assertEquals(rows.map(([, id]) => id), [committed.id]);
      assertEquals(rows.map(([stamp]) => stamp.userVersion), [0]);
      assert(
        Option.isNone(yield* journal.withRemoteUncommited(remote, collectIds)),
      );
    })),
);

storageTest(
  "journal subscriptions exclude their starting snapshot but include appends before subscription setup finishes",
  () =>
    run(Effect.gen(function* () {
      const { database } = testDatabase();
      const writer = yield* makeEventJournal(options());
      for (const populated of [false, true]) {
        yield* writer.destroy;
        if (populated) yield* write(writer, "historical");
        let armed = false;
        let injected: Journal.Entry | undefined;
        const observed: FoundationDbShape = {
          ...database,
          withTransaction: (effect, opts) =>
            database.withTransaction(
              Effect.gen(function* () {
                const tx = yield* FoundationDbTransaction;
                return yield* effect.pipe(
                  Effect.provideService(FoundationDbTransaction, {
                    ...tx,
                    getReadVersion: () =>
                      tx.getReadVersion().pipe(Effect.tap(() => {
                        if (!armed) return Effect.void;
                        armed = false;
                        return Effect.promise(async () => {
                          injected = await Effect.runPromise(
                            write(writer, "during-setup"),
                          );
                        });
                      })),
                  }),
                );
              }),
              opts,
            ),
        };
        const reader = yield* makeEventJournal(options()).pipe(
          Effect.provideService(FoundationDb, observed),
        );
        yield* Effect.scoped(Effect.gen(function* () {
          armed = true;
          const subscription = yield* reader.changes;
          assert(injected !== undefined);
          const later = yield* write(writer, "after-setup");
          assertEquals(
            (yield* Effect.forEach([0, 1], () => PubSub.take(subscription)))
              .map((e) => e.id),
            [injected.id, later.id],
          );
        }));
      }
    })),
);
