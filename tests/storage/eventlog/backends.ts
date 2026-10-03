import { assert, assertEquals } from "@std/assert";
import { Effect, Exit, Fiber, Option, PubSub, Schema, Stream } from "effect";
import * as Journal from "effect/eventlog/EventJournal";
import * as Message from "effect/eventlog/EventLogMessage";
import * as Encrypted from "effect/eventlog/EventLogServerEncrypted";
import * as Plain from "effect/eventlog/EventLogServerUnencrypted";
import {
  FoundationDb,
  FoundationDbTransaction,
} from "../../../src/FoundationDb.ts";
import {
  makeEventJournal,
  makeEventLogServerEncryptedStorage,
  makeEventLogServerUnencryptedStorage,
} from "../../../src/eventlog/mod.ts";
import type { TransactionOptions } from "../../../src/model.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const store = Schema.decodeSync(Message.StoreId)("store");
const otherStore = Schema.decodeSync(Message.StoreId)("other");
const options = () => ({
  pageSize: 2,
  watchRetryDelayMs: 1,
  directory: testDatabase().directory,
});
const entry = (msecs: number, primaryKey = "key", size = 1) =>
  new Journal.Entry({
    id: Journal.makeEntryIdUnsafe({ msecs }),
    event: "update",
    primaryKey,
    payload: new Uint8Array(size).fill(msecs % 256),
  });
const remote = (value: Journal.Entry, sequence: number) =>
  new Journal.RemoteEntry({ entry: value, remoteSequence: sequence });
const encrypted = (value: Journal.Entry) =>
  new Encrypted.PersistedEntry({
    entryId: value.id,
    iv: new Uint8Array(12),
    encryptedEntry: new Uint8Array(value.payload),
  });
const run = <A, E>(
  effect: Effect.Effect<A, E, FoundationDb>,
  fixture = testDatabase(),
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provideService(FoundationDb, fixture.database),
      Effect.timeout("30 seconds"),
    ),
  );

storageTest(
  "eventlog journal rollback, transaction reuse, chronological replay and durable remote acknowledgements",
  async () => {
    await run(Effect.gen(function* () {
      const journal = yield* makeEventJournal(options());
      const second = yield* makeEventJournal(options());
      const projectionKey = testDatabase().prefix(0x42, 1);
      const failure = yield* journal.write({
        event: "update",
        primaryKey: "key",
        payload: new Uint8Array([9]),
        effect: () =>
          Effect.gen(function* () {
            const tx = yield* Effect.serviceOption(FoundationDbTransaction);
            assert(Option.isSome(tx));
            yield* tx.value.set(projectionKey, new Uint8Array([1]));
            return yield* Effect.fail("rollback");
          }),
      }).pipe(Effect.exit);
      assert(Exit.isFailure(failure));
      assertEquals(yield* second.entries, []);
      assertEquals(yield* (yield* FoundationDb).get(projectionKey), undefined);
      const local = yield* journal.write({
        event: "update",
        primaryKey: "key",
        payload: new Uint8Array([8]),
        effect: Effect.succeed,
      });
      const older = entry(1);
      const remoteId = Journal.makeRemoteIdUnsafe();
      yield* second.writeFromRemote({
        remoteId,
        entries: [remote(older, 3)],
        effect: () => Effect.void,
      });
      assertEquals((yield* journal.entries).map((e) => e.idString), [
        older.idString,
        local.idString,
      ]);
      assertEquals(yield* journal.nextRemoteSequence(remoteId), 4);
      let sent = 0;
      yield* journal.withRemoteUncommited(
        remoteId,
        (entries) =>
          Effect.sync(() => {
            assertEquals(entries.map((e) => e.idString), [local.idString]);
            sent++;
          }),
      );
      assert(
        Option.isNone(
          yield* second.withRemoteUncommited(
            remoteId,
            () => Effect.die("must not run"),
          ),
        ),
      );
      assertEquals(sent, 1);
      assertEquals(
        (yield* second.writeFromRemote({
          remoteId,
          entries: [remote(local, 8)],
          effect: () => Effect.die("duplicate callback"),
        })).duplicateEntries.length,
        1,
      );
      assertEquals(yield* journal.nextRemoteSequence(remoteId), 9);
    }));
  },
);

storageTest(
  "eventlog journal compaction preserves originals, finds later conflicts and deduplicates within a batch",
  async () => {
    await run(Effect.gen(function* () {
      const journal = yield* makeEventJournal(options());
      const older = entry(10),
        middle = entry(20),
        later = entry(30),
        unrelated = entry(40, "other");
      const remoteId = Journal.makeRemoteIdUnsafe();
      yield* journal.writeFromRemote({
        remoteId,
        entries: [remote(later, 1), remote(unrelated, 2)],
        effect: () => Effect.void,
      });
      const callbacks: Array<string> = [];
      const result = yield* journal.writeFromRemote({
        remoteId,
        entries: [remote(older, 3), remote(middle, 4), remote(older, 5)],
        compact: (incoming) =>
          Effect.sync(() => {
            assertEquals(incoming.length, 2);
            return [middle, older];
          }),
        effect: ({ entry, conflicts }) =>
          Effect.sync(() => {
            callbacks.push(entry.idString);
            assertEquals(conflicts.map((e) => e.idString), [later.idString]);
          }),
      });
      assertEquals(result.duplicateEntries.map((e) => e.idString), [
        older.idString,
      ]);
      assertEquals(callbacks, [middle.idString, older.idString]);
      assertEquals(
        (yield* journal.entries).map((e) => e.idString),
        [older, middle, later, unrelated].map((e) => e.idString),
      );
      assertEquals(yield* journal.nextRemoteSequence(remoteId), 6);
      const fresh = entry(50);
      const failed = yield* journal.writeFromRemote({
        remoteId,
        entries: [remote(fresh, 9)],
        effect: () =>
          Effect.fail(
            new Journal.EventJournalError({
              method: "callback",
              cause: "failure",
            }),
          ),
      }).pipe(Effect.exit);
      assert(Exit.isFailure(failed));
      assertEquals((yield* journal.entries).length, 4);
      assertEquals(yield* journal.nextRemoteSequence(remoteId), 6);
      const newRemote = Journal.makeRemoteIdUnsafe();
      yield* journal.withRemoteUncommited(
        newRemote,
        () => Effect.fail("no ack"),
      )
        .pipe(Effect.exit);
      assertEquals(
        Option.getOrThrow(
          yield* journal.withRemoteUncommited(
            newRemote,
            (entries) => Effect.succeed(entries.length),
          ),
        ),
        4,
      );
    }));
  },
);

storageTest(
  "eventlog journal cross-instance subscriptions, large payloads, destroy and scoped cleanup",
  async () => {
    const fixture = testDatabase();
    await run(
      Effect.scoped(Effect.gen(function* () {
        const reader = yield* makeEventJournal(options());
        const writer = yield* makeEventJournal(options());
        const subscription = yield* reader.changes;
        const payload = new Uint8Array(150_000).fill(42);
        yield* writer.write({
          event: "update",
          primaryKey: "key",
          payload,
          effect: () => Effect.void,
        });
        assertEquals((yield* PubSub.take(subscription)).payload, payload);
        assertEquals((yield* reader.entries)[0].payload, payload);
        yield* writer.destroy;
        assertEquals(yield* reader.entries, []);
        yield* writer.write({
          event: "after",
          primaryKey: "key",
          payload: new Uint8Array(),
          effect: () => Effect.void,
        });
        assertEquals((yield* PubSub.take(subscription)).event, "after");
        assert(
          (yield* fixture.entries()).every((row) =>
            row.value.byteLength <= 8_192
          ),
        );
      })),
      fixture,
    );
  },
);

storageTest(
  "encrypted eventlog durable ID/auth, collision-free tenant namespaces, sequence ordering and dedup",
  async () => {
    await run(Effect.gen(function* () {
      const first = yield* makeEventLogServerEncryptedStorage(options());
      const second = yield* makeEventLogServerEncryptedStorage(options());
      assertEquals(yield* first.getId, yield* second.getId);
      const keys = yield* Effect.all([
        first.getOrCreateSessionAuthBinding("user", new Uint8Array([1])),
        second.getOrCreateSessionAuthBinding("user", new Uint8Array([2])),
      ], { concurrency: "unbounded" });
      assertEquals(keys[0], keys[1]);
      assertEquals(
        yield* second.getOrCreateSessionAuthBinding(
          "other",
          new Uint8Array([3]),
        ),
        new Uint8Array([3]),
      );
      const a = encrypted(entry(1)),
        b = encrypted(entry(2)),
        c = encrypted(entry(3));
      assertEquals(
        (yield* first.write("user", store, [a, a, b])).map((e) => e.sequence),
        [0, 1],
      );
      assertEquals(yield* second.write("user", store, [a, b]), []);
      assertEquals(
        (yield* second.write("user", store, [c])).map((e) => e.sequence),
        [2],
      );
      const replay = yield* first.changes("user", store, 1).pipe(
        Stream.take(2),
        Stream.runCollect,
      );
      assertEquals(replay.map((e) => e.sequence), [1, 2]);
      assertEquals((yield* first.write("other", store, [a]))[0].sequence, 0);
      assertEquals(
        (yield* first.write("user", otherStore, [a]))[0].sequence,
        0,
      );
      const suffix = Schema.decodeSync(Message.StoreId)("b/store");
      yield* first.write("a/b", store, [a]);
      assertEquals((yield* first.write("a", suffix, [b]))[0].sequence, 0);
    }));
  },
);

storageTest(
  "encrypted eventlog cross-instance tail includes concurrent commits and chunked ciphertext",
  async () => {
    const fixture = testDatabase();
    await run(
      Effect.scoped(Effect.gen(function* () {
        const reader = yield* makeEventLogServerEncryptedStorage(options());
        const writer = yield* makeEventLogServerEncryptedStorage(options());
        const a = encrypted(entry(1, "key", 120_000));
        const b = encrypted(entry(2)), c = encrypted(entry(3));
        yield* writer.write("user", store, [a]);
        const result = yield* reader.changes("user", store, 0).pipe(
          Stream.take(3),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* writer.write("user", store, [b, c]);
        const values = yield* Fiber.join(result);
        assertEquals(values.map((e) => e.sequence), [0, 1, 2]);
        assertEquals(values[0].encryptedEntry, a.encryptedEntry);
        assert(
          (yield* fixture.entries()).every((row) =>
            row.value.byteLength <= 8_192
          ),
        );
      })),
      fixture,
    );
  },
);

storageTest(
  "plain eventlog actual rollback, nested transactions, tenant isolation and inclusive entriesAfter",
  async () => {
    await run(Effect.gen(function* () {
      const first = yield* makeEventLogServerUnencryptedStorage(options());
      const second = yield* makeEventLogServerUnencryptedStorage(options());
      assertEquals(yield* first.getId, yield* second.getId);
      yield* first.getOrCreateSessionAuthBinding("user", new Uint8Array([1]));
      assertEquals(
        yield* second.getOrCreateSessionAuthBinding(
          "user",
          new Uint8Array([2]),
        ),
        new Uint8Array([1]),
      );
      const a = entry(1), b = entry(2), c = entry(3);
      const failed = yield* first.withTransaction(Effect.gen(function* () {
        yield* first.write(store, [a]);
        yield* second.withTransaction(second.write(store, [b]));
        assertEquals((yield* first.entriesAfter(store, a)).length, 2);
        return yield* Effect.fail("rollback");
      })).pipe(Effect.exit);
      assert(Exit.isFailure(failed));
      assertEquals(yield* second.entriesAfter(store, a), []);
      assertEquals(
        (yield* first.write(store, [b, a])).map((e) => e.remoteSequence),
        [1, 2],
      );
      assertEquals(
        (yield* second.entriesAfter(store, a)).map((e) => e.idString),
        [a.idString, b.idString],
      );
      assertEquals(yield* second.entriesAfter(otherStore, a), []);
      assert(
        Exit.isFailure(yield* first.write(store, [c, a]).pipe(Effect.exit)),
      );
      assertEquals((yield* second.write(store, [c]))[0].remoteSequence, 3);
    }));
  },
);

storageTest(
  "plain eventlog backlog compaction spans pages, retains sequence watermark, then tails other instances",
  async () => {
    await run(Effect.scoped(Effect.gen(function* () {
      const reader = yield* makeEventLogServerUnencryptedStorage(options());
      const writer = yield* makeEventLogServerUnencryptedStorage(options());
      const entries = [entry(1), entry(2), entry(3)];
      yield* writer.write(store, entries);
      let compactedCount = 0;
      type ChangesOptions = Parameters<Plain.Storage["Service"]["changes"]>[0];
      const compactors: ChangesOptions["compactors"] = new Map([["update", {
        events: new Set(["update"]),
        effect: ({ entries, write }) => {
          compactedCount = entries.length;
          return write(entries[entries.length - 1]);
        },
      }]]);
      const stream = reader.changes({
        storeId: store,
        startSequence: 1,
        compactors,
      });
      const fiber = yield* stream.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped,
      );
      while (compactedCount === 0) yield* Effect.sleep(1);
      yield* writer.write(store, [entry(4)]);
      const result = yield* Fiber.join(fiber);
      assertEquals(compactedCount, 3);
      assertEquals(result.map((e) => e.remoteSequence), [3, 4]);
      assertEquals(result[0].entry.idString, entries[2].idString);
    })));
  },
);

storageTest(
  "eventlog journal locks reuse one transaction across independently constructed journals",
  async () => {
    await run(Effect.gen(function* () {
      const a = yield* makeEventJournal(options());
      const b = yield* makeEventJournal(options());
      const failed = yield* a.withLock(store)(Effect.gen(function* () {
        yield* b.write({
          event: "nested",
          primaryKey: "key",
          payload: new Uint8Array(),
          effect: () => Effect.void,
        });
        assertEquals((yield* a.entries).length, 1);
        return yield* Effect.fail("rollback lock");
      })).pipe(Effect.exit);
      assert(Exit.isFailure(failed));
      assertEquals(yield* b.entries, []);
    }));
  },
);

storageTest(
  "eventlog callback retry policy is explicit and ambient transaction boundaries are reused",
  async () => {
    const fixture = testDatabase();
    const original = fixture.database.withTransaction;
    const calls: Array<TransactionOptions | undefined> = [];
    const database = {
      ...fixture.database,
      withTransaction: ((effect, options) => {
        calls.push(options);
        return original(effect, options);
      }) satisfies typeof original,
    };
    await run(
      Effect.gen(function* () {
        const journal = yield* makeEventJournal({
          directory: fixture.directory,
          transactionOptions: { retryLimit: 9 },
        });
        calls.length = 0;
        yield* journal.write({
          event: "one",
          primaryKey: "key",
          payload: new Uint8Array(),
          effect: () => Effect.void,
        });
        assertEquals(calls.length, 1);
        assertEquals(calls[0]?.retryLimit, 0);
        assertEquals(calls[0]?.retryOnMaybeCommitted, false);
        const storage = yield* makeEventLogServerUnencryptedStorage({
          directory: fixture.directory,
          callbackRetryLimit: 3,
          transactionOptions: { retryOnMaybeCommitted: true },
        });
        calls.length = 0;
        yield* storage.withTransaction(
          storage.withTransaction(storage.write(store, [entry(1)])),
        );
        assertEquals(calls.length, 1);
        assertEquals(calls[0]?.retryLimit, 3);
        assertEquals(calls[0]?.retryOnMaybeCommitted, false);
        calls.length = 0;
        yield* journal.entries;
        assertEquals(calls[0]?.retryLimit, 9);
        calls.length = 0;
        yield* storage.write(store, [entry(2)]);
        assertEquals(calls.length, 1);
        assertEquals(calls[0]?.retryOnMaybeCommitted, false);
        const encrypted = yield* makeEventLogServerEncryptedStorage({
          directory: fixture.directory,
          transactionOptions: { retryOnMaybeCommitted: true },
        });
        calls.length = 0;
        yield* encrypted.write("client", store, []);
        assertEquals(calls.length, 1);
        assertEquals(calls[0]?.retryOnMaybeCommitted, false);
      }),
      { ...fixture, database },
    );
  },
);

storageTest(
  "plain eventlog chunked payloads survive reopen and rolled-back writes never reach changes",
  async () => {
    const fixture = testDatabase();
    await run(
      Effect.scoped(Effect.gen(function* () {
        const writer = yield* makeEventLogServerUnencryptedStorage(options());
        const large = entry(1, "key", 180_000);
        yield* writer.write(store, [large]);
        const reader = yield* makeEventLogServerUnencryptedStorage(options());
        assertEquals(
          (yield* reader.entriesAfter(store, large))[0].payload,
          large.payload,
        );
        const fiber = yield* reader.changes({
          storeId: store,
          startSequence: 2,
          compactors: new Map(),
        }).pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped);
        yield* writer.withTransaction(Effect.gen(function* () {
          yield* writer.write(store, [entry(2)]);
          return yield* Effect.fail("rollback");
        })).pipe(Effect.exit);
        const committed = entry(3);
        yield* writer.write(store, [committed]);
        const observed = yield* Fiber.join(fiber);
        assertEquals(observed.map((r) => r.entry.idString), [
          committed.idString,
        ]);
        assertEquals(observed[0].remoteSequence, 2);
        assert(
          (yield* fixture.entries()).every((row) =>
            row.value.byteLength <= 8_192
          ),
        );
      })),
      fixture,
    );
  },
);

storageTest("eventlog directory isolation and invalid options", async () => {
  await run(Effect.gen(function* () {
    const a = yield* makeEventJournal({
      directory: testDatabase().directory,
      directoryPath: ["a"],
    });
    const b = yield* makeEventJournal({
      directory: testDatabase().directory,
      directoryPath: ["b"],
    });
    yield* a.write({
      event: "test",
      primaryKey: "key",
      payload: new Uint8Array(),
      effect: () => Effect.void,
    });
    assertEquals(yield* b.entries, []);
    assert(
      Exit.isFailure(
        yield* makeEventJournal({
          directory: testDatabase().directory,
          pageSize: 0,
        }).pipe(Effect.exit),
      ),
    );
    assert(
      Exit.isFailure(
        yield* makeEventLogServerEncryptedStorage({
          directory: testDatabase().directory,
          watchRetryDelayMs: 0,
        }).pipe(
          Effect.exit,
        ),
      ),
    );
  }));
});
