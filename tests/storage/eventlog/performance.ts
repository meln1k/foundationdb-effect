import { assert, assertEquals } from "@std/assert";
import {
  Effect,
  Exit,
  Fiber,
  Latch,
  Option,
  PubSub,
  Schema,
  Stream,
} from "effect";
import * as Journal from "effect/eventlog/EventJournal";
import * as Message from "effect/eventlog/EventLogMessage";
import * as Encrypted from "effect/eventlog/EventLogServerEncrypted";
import {
  FoundationDb,
  FoundationDbTransaction,
} from "../../../src/FoundationDb.ts";
import type { FoundationDbShape } from "../../../src/FoundationDb.ts";
import { DirectorySubspace } from "../../../src/directory/mod.ts";
import {
  makeEventJournal,
  makeEventLogServerEncryptedStorage,
  makeEventLogServerUnencryptedStorage,
} from "../../../src/eventlog/mod.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const store = Schema.decodeSync(Message.StoreId)("store");
const options = () => ({ directory: testDatabase().directory, pageSize: 2 });
const entry = (msecs: number) =>
  new Journal.Entry({
    id: Journal.makeEntryIdUnsafe({ msecs }),
    event: "update",
    primaryKey: String(msecs),
    payload: Uint8Array.of(msecs % 256),
  });
const encrypted = (msecs: number, size = 1) =>
  new Encrypted.PersistedEntry({
    entryId: entry(msecs).id,
    iv: new Uint8Array(12),
    encryptedEntry: new Uint8Array(size).fill(msecs % 256),
  });
const run = <A, E>(effect: Effect.Effect<A, E, FoundationDb>) =>
  Effect.runPromise(effect.pipe(
    Effect.provideService(FoundationDb, testDatabase().database),
    Effect.timeout("15 seconds"),
  ));
const waitUntil = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() =>
    predicate() ? Effect.void : Effect.sleep(5).pipe(
      Effect.andThen(waitUntil(predicate)),
    )
  );
const rootFor = (kind: string) =>
  Effect.gen(function* () {
    const root = yield* testDatabase().database.withTransaction(
      testDatabase().directory.open(["effect-foundationdb", "eventlog", kind]),
    );
    assert(root instanceof DirectorySubspace);
    return root;
  });

// Observe real reads after adapter initialization, without replacing FDB results.
const observeDuplicateChecks = (database: FoundationDbShape) => {
  let pointReads = 0;
  const batches: Array<number> = [];
  const observed: FoundationDbShape = {
    ...database,
    withTransaction: (effect, opts) =>
      database.withTransaction(
        Effect.gen(function* () {
          const tx = yield* FoundationDbTransaction;
          return yield* effect.pipe(Effect.provideService(
            FoundationDbTransaction,
            {
              ...tx,
              get: (key, options) => {
                pointReads++;
                return tx.get(key, options);
              },
              getMany: (keys, options) => {
                assert(options?.snapshot !== true);
                batches.push(keys.length);
                return tx.getMany(keys, options);
              },
            },
          ));
        }),
        opts,
      ),
  };
  return {
    database: observed,
    reset: () => {
      pointReads = 0;
      batches.length = 0;
    },
    check: (expected: Array<number>) => {
      assertEquals(pointReads, 0);
      assertEquals(batches, expected);
    },
  };
};

storageTest(
  "eventlog journal batches duplicate checks and preserves ACK progress",
  () =>
    run(Effect.gen(function* () {
      const observed = observeDuplicateChecks(testDatabase().database);
      const journal = yield* makeEventJournal(options()).pipe(
        Effect.provideService(FoundationDb, observed.database),
      );
      const a = entry(1), b = entry(2), c = entry(3);
      const remoteId = Journal.makeRemoteIdUnsafe();
      const receive = (values: ReadonlyArray<Journal.Entry>, start: number) =>
        journal.writeFromRemote({
          remoteId,
          entries: values.map((entry, index) =>
            new Journal.RemoteEntry({ entry, remoteSequence: start + index })
          ),
          effect: ({ entry }) =>
            Effect.sync(() => callbacks.push(entry.idString)),
        });
      const callbacks: Array<string> = [];
      yield* receive([a], 0);
      observed.reset();
      callbacks.length = 0;
      const copy = new Journal.Entry({
        ...b,
        id: Schema.decodeSync(Journal.EntryId)(b.id.slice()),
        payload: Uint8Array.of(99),
      });
      const result = yield* receive([b, a, copy, c, a], 10);
      observed.check([2, 2, 1]);
      assertEquals(callbacks, [b.idString, c.idString]);
      assertEquals(result.duplicateEntries, [a, copy, a]);
      assertEquals(yield* journal.nextRemoteSequence(remoteId), 15);
      assertEquals(yield* journal.entries, [a, b, c]);
      assert(Option.isNone(
        yield* journal.withRemoteUncommited(
          remoteId,
          () => Effect.die("received entries must remain acknowledged"),
        ),
      ));
      observed.reset();
      callbacks.length = 0;
      yield* receive([c, a, b], 20);
      observed.check([2, 1]);
      assertEquals(callbacks, []);
      assertEquals(yield* journal.nextRemoteSequence(remoteId), 23);
    })),
);

storageTest(
  "encrypted eventlog batches duplicate checks without sequence gaps",
  () =>
    run(Effect.gen(function* () {
      const observed = observeDuplicateChecks(testDatabase().database);
      const server = yield* makeEventLogServerEncryptedStorage(options()).pipe(
        Effect.provideService(FoundationDb, observed.database),
      );
      const a = encrypted(1), b = encrypted(2), c = encrypted(3);
      yield* server.write("user", store, [a]);
      observed.reset();
      const copy = new Encrypted.PersistedEntry({
        ...b,
        entryId: Schema.decodeSync(Journal.EntryId)(b.entryId.slice()),
        encryptedEntry: Uint8Array.of(99),
      });
      const written = yield* server.write("user", store, [b, a, copy, c, a]);
      observed.check([2, 2, 1]);
      assertEquals(written.map((e) => e.sequence), [1, 2]);
      assertEquals(written.map((e) => e.entryId), [b.entryId, c.entryId]);
      assertEquals(written[0].encryptedEntry, b.encryptedEntry);
      observed.reset();
      assertEquals(yield* server.write("user", store, [c, a, b]), []);
      observed.check([2, 1]);
      const d = encrypted(4);
      assertEquals((yield* server.write("user", store, [d]))[0].sequence, 3);
      const stored = yield* server.changes("user", store, 0).pipe(
        Stream.take(4),
        Stream.runCollect,
      );
      assertEquals(
        stored.map((e) => e.encryptedEntry),
        [a, b, c, d].map((e) => e.encryptedEntry),
      );
    })),
);

storageTest(
  "plain eventlog batched duplicate checks reject and roll back the entire write",
  () =>
    run(Effect.gen(function* () {
      const observed = observeDuplicateChecks(testDatabase().database);
      const server = yield* makeEventLogServerUnencryptedStorage(options())
        .pipe(
          Effect.provideService(FoundationDb, observed.database),
        );
      const a = entry(1), b = entry(2), c = entry(3), d = entry(4);
      yield* server.write(store, [a]);
      const copy = new Journal.Entry({
        ...b,
        id: Schema.decodeSync(Journal.EntryId)(b.id.slice()),
      });
      // A persisted duplicate and a same-batch duplicate both occur on page two.
      for (const values of [[b, c, a], [b, c, copy]]) {
        observed.reset();
        assert(
          Exit.isFailure(yield* server.write(store, values).pipe(Effect.exit)),
        );
        observed.check([2, 1]);
        assertEquals(yield* server.entriesAfter(store, a), [a]);
      }
      observed.reset();
      const written = yield* server.write(store, [d, b, c]);
      observed.check([2, 1]);
      assertEquals(written.map((e) => e.remoteSequence), [2, 3, 4]);
      assertEquals(
        written.map((e) => e.entry.idString),
        [d, b, c].map((e) => e.idString),
      );
      assertEquals(yield* server.entriesAfter(store, a), [a, b, c, d]);
      observed.reset();
      assertEquals(yield* server.write(store, []), []);
      observed.check([]);
    })),
);

// Instrument the real client, not a simulated database. Mark watches ready only
// after commit; failed transaction attempts never count as active subscriptions.
const observeWatches = (database: FoundationDbShape) => {
  const watches: Array<{
    committed: boolean;
    resolved: boolean;
    cancelled: boolean;
  }> = [];
  let transactions = 0;
  const withTransaction: FoundationDbShape["withTransaction"] = (
    effect,
    opts,
  ) =>
    database.withTransaction(
      Effect.gen(function* () {
        const tx = yield* FoundationDbTransaction;
        const attempt: typeof watches = [];
        const value = yield* effect.pipe(Effect.provideService(
          FoundationDbTransaction,
          {
            ...tx,
            watch: (key) =>
              tx.watch(key).pipe(Effect.map((future) => {
                const watch = {
                  committed: false,
                  resolved: false,
                  cancelled: false,
                };
                watches.push(watch);
                attempt.push(watch);
                return {
                  await: future.await.pipe(Effect.tap(Effect.sync(() => {
                    watch.resolved = true;
                  }))),
                  cancel: future.cancel.pipe(Effect.tap(Effect.sync(() => {
                    watch.cancelled = true;
                  }))),
                };
              })),
          },
        ));
        return { value, attempt };
      }),
      opts,
    ).pipe(Effect.map(({ value, attempt }) => {
      transactions++;
      for (const watch of attempt) watch.committed = true;
      return value;
    }));
  return {
    database: { ...database, withTransaction },
    watches,
    transactions: () => transactions,
    ready: () =>
      watches.some((w) => w.committed && !w.resolved && !w.cancelled),
  };
};

storageTest(
  "eventlog replication skips acknowledged history, retains late IDs and rolls back progress on callback failure",
  () =>
    run(Effect.gen(function* () {
      const { database } = testDatabase();
      const writer = yield* makeEventJournal(options());
      const root = yield* rootFor("journal");
      const payloads = yield* root.subspace(["values", "entries"]);
      const inserted = yield* root.subspace(["index", "inserted"]);
      let payloadReads = 0;
      let indexRows = 0;
      const ackBatches: Array<number> = [];
      const observed: FoundationDbShape = {
        ...database,
        withTransaction: (effect, opts) =>
          database.withTransaction(
            Effect.gen(function* () {
              const tx = yield* FoundationDbTransaction;
              return yield* effect.pipe(Effect.provideService(
                FoundationDbTransaction,
                {
                  ...tx,
                  getMany: (keys, options) => {
                    ackBatches.push(keys.length);
                    return tx.getMany(keys, options);
                  },
                  getRange: (range) => {
                    if (payloads.contains(range.begin.key)) payloadReads++;
                    return tx.getRange(range).pipe(Stream.tap((row) =>
                      Effect.sync(() => {
                        if (inserted.contains(row.key)) indexRows++;
                      })
                    ));
                  },
                },
              ));
            }),
            opts,
          ),
      };
      const reader = yield* makeEventJournal(options()).pipe(
        Effect.provideService(FoundationDb, observed),
      );
      const source = Journal.makeRemoteIdUnsafe();
      const target = Journal.makeRemoteIdUnsafe();
      const history = Array.from({ length: 11 }, (_, i) => entry(100 + i));
      const receive = (entries: ReadonlyArray<Journal.Entry>) =>
        writer.writeFromRemote({
          remoteId: source,
          entries: entries.map((entry, remoteSequence) =>
            new Journal.RemoteEntry({ entry, remoteSequence })
          ),
          effect: () => Effect.void,
        });
      yield* receive(history.toReversed());
      assert(Option.isNone(
        yield* reader.withRemoteUncommited(
          source,
          () => Effect.die("received events must not be echoed"),
        ),
      ));
      assertEquals(payloadReads, 0);
      assertEquals(ackBatches, [2, 2, 2, 2, 2, 1]);
      const ids = (entries: ReadonlyArray<Journal.Entry>) =>
        Effect.succeed(entries.map((e) => e.idString));
      assertEquals(
        Option.getOrThrow(yield* reader.withRemoteUncommited(target, ids)),
        history.map((e) => e.idString),
      );
      assertEquals(payloadReads, 11);
      payloadReads = indexRows = 0;
      ackBatches.length = 0;
      assert(Option.isNone(yield* reader.withRemoteUncommited(target, ids)));
      assertEquals({ payloadReads, indexRows, ackBatches }, {
        payloadReads: 0,
        indexRows: 0,
        ackBatches: [],
      });

      // UUID time is older than every event already sent: a UUID cursor loses it.
      const late = entry(1);
      yield* receive([late]);
      assertEquals(
        yield* reader.withRemoteUncommited(
          target,
          () => Effect.fail("rollback"),
        )
          .pipe(Effect.flip),
        "rollback",
      );
      const created = Option.getOrThrow(
        yield* reader.withRemoteUncommited(
          target,
          (pending) => {
            assertEquals(pending.map((e) => e.idString), [late.idString]);
            // This insert must remain beyond the captured prefix, even though it
            // shares the transaction that advances the outgoing cursor.
            return writer.write({
              event: "nested",
              primaryKey: "key",
              payload: Uint8Array.of(42),
              effect: Effect.succeed,
            });
          },
        ),
      );
      const reopened = yield* makeEventJournal(options());
      assertEquals(
        Option.getOrThrow(yield* reopened.withRemoteUncommited(target, ids)),
        [created.idString],
      );
      yield* receive(history);
      assert(Option.isNone(yield* reopened.withRemoteUncommited(target, ids)));
      assertEquals(
        Option.getOrThrow(
          yield* reopened.withRemoteUncommited(
            Journal.makeRemoteIdUnsafe(),
            ids,
          ),
        ),
        [late, ...history, created].map((e) => e.idString),
      );
    })),
);

storageTest(
  "eventlog payload reads run eight at a time while preserving sequence and chunk order",
  () =>
    run(Effect.gen(function* () {
      const { database } = testDatabase();
      const config = { ...options(), pageSize: 12 };
      const writer = yield* makeEventLogServerEncryptedStorage(config);
      const values = Array.from(
        { length: 12 },
        (_, i) => encrypted(100 - i, i === 0 ? 130_000 : i + 1),
      );
      yield* writer.write("user", store, values);
      const root = yield* rootFor("encrypted");
      const payloads = yield* root.subspace(["values", "entries"]);
      const eightStarted = Latch.makeUnsafe();
      const anotherFinished = Latch.makeUnsafe();
      let active = 0, peak = 0, started = 0;
      const finished: Array<number> = [];
      const observed: FoundationDbShape = {
        ...database,
        withTransaction: (effect, opts) =>
          database.withTransaction(
            Effect.gen(function* () {
              const tx = yield* FoundationDbTransaction;
              return yield* effect.pipe(Effect.provideService(
                FoundationDbTransaction,
                {
                  ...tx,
                  getRange: (range) => {
                    if (!payloads.contains(range.begin.key)) {
                      return tx.getRange(range);
                    }
                    return Stream.unwrap(Effect.gen(function* () {
                      const index = started++;
                      peak = Math.max(peak, ++active);
                      if (active === 8) yield* eightStarted.open;
                      yield* eightStarted.await;
                      if (index === 0) yield* anotherFinished.await;
                      return tx.getRange(range).pipe(Stream.ensuring(
                        Effect.gen(function* () {
                          active--;
                          finished.push(index);
                          yield* anotherFinished.open;
                        }),
                      ));
                    }));
                  },
                },
              ));
            }),
            opts,
          ),
      };
      const reader = yield* makeEventLogServerEncryptedStorage(config).pipe(
        Effect.provideService(FoundationDb, observed),
      );
      const result = yield* reader.changes("user", store, 0).pipe(
        Stream.take(12),
        Stream.runCollect,
      );
      assertEquals(peak, 8);
      assertEquals(active, 0);
      assert(finished[0] !== 0);
      assertEquals(
        result.map((e) => e.sequence),
        Array.from({ length: 12 }, (_, i) => i),
      );
      assertEquals(
        result.map((e) => e.encryptedEntry),
        values.map((e) => e.encryptedEntry),
      );
    })),
);

storageTest(
  "eventlog idle feeds use committed tenant-specific watches, ignore rollback and cancel on scope exit",
  () =>
    run(Effect.gen(function* () {
      const { database } = testDatabase();
      const observed = observeWatches(database);
      const reader = yield* makeEventLogServerEncryptedStorage(options()).pipe(
        Effect.provideService(FoundationDb, observed.database),
      );
      const writer = yield* makeEventLogServerEncryptedStorage(options());
      yield* Effect.scoped(Effect.gen(function* () {
        const result = yield* reader.changes("user", store, 0).pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* waitUntil(observed.ready);
        const idleTransactions = observed.transactions();
        yield* writer.write("other", store, [encrypted(1)]);
        yield* writer.write(
          "user",
          Schema.decodeSync(Message.StoreId)("other"),
          [encrypted(2)],
        );
        assertEquals(
          yield* database.withTransaction(Effect.gen(function* () {
            yield* writer.write("user", store, [encrypted(3)]);
            return yield* Effect.fail("rollback");
          })).pipe(Effect.flip),
          "rollback",
        );
        yield* Effect.sleep(250);
        assertEquals(observed.transactions(), idleTransactions);
        assert(observed.watches.every((w) => !w.resolved));
        const value = encrypted(4);
        yield* writer.write("user", store, [value]);
        const entries = yield* Fiber.join(result);
        assertEquals(entries.map((e) => e.sequence), [0]);
        assertEquals(entries[0].entryId, value.entryId);
        yield* reader.changes("user", store, 1).pipe(
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* waitUntil(observed.ready);
      }));
      assert(
        observed.watches.filter((w) => w.committed).every((w) => w.cancelled),
      );
    })),
);

storageTest(
  "eventlog watches cannot miss an append between the empty read and watch commit",
  () =>
    run(Effect.gen(function* () {
      const { database } = testDatabase();
      const writer = yield* makeEventLogServerEncryptedStorage(options());
      const value = encrypted(42);
      let injected = false;
      const observed: FoundationDbShape = {
        ...database,
        withTransaction: (effect, opts) =>
          database.withTransaction(
            Effect.gen(function* () {
              const tx = yield* FoundationDbTransaction;
              return yield* effect.pipe(Effect.provideService(
                FoundationDbTransaction,
                {
                  ...tx,
                  watch: (key) =>
                    tx.watch(key).pipe(Effect.tap(() => {
                      if (injected) return Effect.void;
                      injected = true;
                      // Run outside this transaction's context, before it commits.
                      return Effect.promise(() =>
                        Effect.runPromise(writer.write("user", store, [value]))
                      );
                    })),
                },
              ));
            }),
            opts,
          ),
      };
      const reader = yield* makeEventLogServerEncryptedStorage(options()).pipe(
        Effect.provideService(FoundationDb, observed),
      );
      const entries = yield* reader.changes("user", store, 0).pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      assert(injected);
      assertEquals(entries.map((e) => e.entryId), [value.entryId]);
    })),
);

storageTest(
  "eventlog journal subscriptions bound unread payloads without blocking other readers or losing events",
  () =>
    run(Effect.scoped(Effect.gen(function* () {
      const { database } = testDatabase();
      const writer = yield* makeEventJournal(options());
      const root = yield* rootFor("journal");
      const payloads = yield* root.subspace(["values", "local"]);
      let payloadReads = 0;
      const observed: FoundationDbShape = {
        ...database,
        withTransaction: (effect, opts) =>
          database.withTransaction(
            Effect.gen(function* () {
              const tx = yield* FoundationDbTransaction;
              return yield* effect.pipe(Effect.provideService(
                FoundationDbTransaction,
                {
                  ...tx,
                  getRange: (range) => {
                    if (payloads.contains(range.begin.key)) payloadReads++;
                    return tx.getRange(range);
                  },
                },
              ));
            }),
            opts,
          ),
      };
      const reader = yield* makeEventJournal(options()).pipe(
        Effect.provideService(FoundationDb, observed),
      );
      const slow = yield* reader.changes;
      const fast = yield* writer.changes;
      const count = Array.from({ length: 12 }, (_, i) => i);
      const consumed = yield* Effect.forEach(count, () => PubSub.take(fast))
        .pipe(Effect.forkScoped);
      const append = (i: number) =>
        writer.write({
          event: "update",
          primaryKey: String(i),
          payload: Uint8Array.of(i),
          effect: Effect.succeed,
        });
      const written = yield* database.withTransaction(
        Effect.forEach(count, append),
      );
      assertEquals(
        (yield* Fiber.join(consumed)).map((e) => e.idString),
        written.map((e) => e.idString),
      );
      yield* waitUntil(() => payloadReads >= 4);
      yield* Effect.sleep(100);
      // Two buffered entries plus one fetched page waiting to publish.
      assertEquals(payloadReads, 4);
      assertEquals(
        (yield* Effect.forEach(count, () => PubSub.take(slow))).map((e) =>
          e.idString
        ),
        written.map((e) => e.idString),
      );
      // Leave another producer blocked at scope exit; cleanup must not hang.
      yield* reader.changes;
      yield* database.withTransaction(Effect.forEach(count, append));
      yield* Effect.sleep(100);
    }))),
);

storageTest(
  "eventlog destroy and append in one transaction wakes an already idle subscriber",
  () =>
    run(Effect.scoped(Effect.gen(function* () {
      const { database } = testDatabase();
      const observed = observeWatches(database);
      const reader = yield* makeEventJournal(options()).pipe(
        Effect.provideService(FoundationDb, observed.database),
      );
      const writer = yield* makeEventJournal(options());
      const append = (event: string) =>
        writer.write({
          event,
          primaryKey: "key",
          payload: new Uint8Array(),
          effect: Effect.succeed,
        });
      yield* append("before");
      const subscription = yield* reader.changes;
      yield* waitUntil(observed.ready);
      yield* database.withTransaction(Effect.gen(function* () {
        yield* writer.destroy;
        yield* append("after");
      }));
      assertEquals((yield* PubSub.take(subscription)).event, "after");
    }))),
);
