import {
  Array as Arr,
  Clock,
  Effect,
  Layer,
  Option,
  PubSub,
  Schema,
  Stream,
} from "effect";
import * as Journal from "effect/eventlog/EventJournal";
import { binary, EntryCodec, makeRepository } from "./internal.ts";
import type { EventLogStoreOptions } from "./internal.ts";
import { makeJournalOrder } from "./journal-order.ts";

export interface EventJournalOptions extends EventLogStoreOptions {}

const CursorCodec = binary(Schema.Uint8Array);

export const makeEventJournal = Effect.fnUntraced(function* (
  options: EventJournalOptions = {},
) {
  const repo = yield* makeRepository(options, "journal");
  const order = yield* makeJournalOrder(repo.root, repo.pageSize);
  const entries = Effect.gen(function* () {
    const ids = yield* repo.all(["entries"]);
    return yield* repo.readMany(
      ids.map((id) => ["entries", ...id]),
      EntryCodec,
    );
  });
  const insert = Effect.fnUntraced(
    function* (entries: ReadonlyArray<Journal.Entry>, local = false) {
      for (const entry of entries) {
        yield* repo.put(["entries", entry.id], EntryCodec, entry);
        yield* repo.mark(["entries", entry.id]);
        yield* repo.mark([
          "conflicts",
          entry.event,
          entry.primaryKey,
          entry.id,
        ]);
        yield* order.append(entry.id, local);
        if (local) yield* repo.put(["local", entry.id], EntryCodec, entry);
      }
    },
  );

  return Journal.EventJournal.of({
    entries: repo.transaction(entries),
    write: ({ event, primaryKey, payload, effect }) =>
      Effect.gen(function* () {
        // Keep the ID stable across explicitly enabled transaction retries.
        const id = Journal.makeEntryIdUnsafe({
          msecs: yield* Clock.currentTimeMillis,
        });
        const entry = new Journal.Entry({ id, event, primaryKey, payload });
        return yield* repo.transaction(
          Effect.gen(function* () {
            const result = yield* effect(entry);
            yield* insert([entry], true);
            yield* repo.notify(["local"]);
            return result;
          }),
          true,
        );
      }),
    writeFromRemote: (options) =>
      repo.transaction(
        Effect.gen(function* () {
          const duplicateEntries: Array<Journal.Entry> = [];
          const uncommitted: Array<Journal.RemoteEntry> = [];
          const seen = new Set<string>();
          let next = yield* repo.number(["remoteNext", options.remoteId]);
          const existing = yield* repo.hasMany(
            options.entries.map(({ entry }) => ["entries", entry.id]),
          );
          for (const [index, remote] of options.entries.entries()) {
            const entry = remote.entry;
            if (seen.has(entry.idString) || existing[index]) {
              duplicateEntries.push(entry);
            } else {
              seen.add(entry.idString);
              uncommitted.push(remote);
            }
            next = Math.max(next, remote.remoteSequence + 1);
            yield* repo.mark(["ack", options.remoteId, entry.id]);
          }
          const compacted = options.compact
            ? yield* options.compact(uncommitted)
            : uncommitted.map((r) => r.entry);
          // Like the memory/IndexedDB journal, replay against already committed
          // later entries, not against other incoming entries in this batch.
          for (const entry of compacted) {
            const ids = yield* repo.all([
              "conflicts",
              entry.event,
              entry.primaryKey,
            ], [entry.id]);
            const conflicts = yield* repo.readMany(
              ids.map((id) => ["entries", ...id]),
              EntryCodec,
            );
            yield* options.effect({
              entry,
              conflicts: conflicts.filter((other) =>
                Journal.Entry.Order(other, entry) > 0
              ),
            });
          }
          yield* insert(uncommitted.map((remote) => remote.entry));
          yield* repo.setNumber(["remoteNext", options.remoteId], next);
          return { duplicateEntries };
        }),
        true,
      ),
    withRemoteUncommited: (remoteId, f) =>
      repo.transaction(
        Effect.gen(function* () {
          const after = yield* repo.get(["sentAfter", remoteId], CursorCodec);
          const inserted = yield* order.unsent(after);
          const acknowledged = yield* repo.hasMany(
            inserted.ids.map((id) => ["ack", remoteId, id]),
          );
          const pending = yield* repo.readMany(
            inserted.ids.filter((_, i) => !acknowledged[i]).map((
              id,
            ) => ["entries", id]),
            EntryCodec,
          );
          pending.sort(Journal.Entry.Order);
          let result = Option.none<Effect.Success<ReturnType<typeof f>>>();
          if (Arr.isReadonlyArrayNonEmpty(pending)) {
            result = Option.some(yield* f(pending));
            for (const entry of pending) {
              yield* repo.mark(["ack", remoteId, entry.id]);
            }
          }
          // Only committed entries can advance the cursor. Pending entries are
          // ACKed above; callback-created events remain pending for the next call.
          if (inserted.cursor !== undefined) {
            yield* repo.put(
              ["sentAfter", remoteId],
              CursorCodec,
              inserted.cursor,
            );
          }
          return result;
        }),
        true,
      ),
    nextRemoteSequence: (remoteId) =>
      repo.transaction(repo.number(["remoteNext", remoteId])),
    changes: Effect.gen(function* () {
      const start = yield* repo.transaction(order.start).pipe(
        Effect.orDie,
      );
      const pubsub = yield* Effect.acquireRelease(
        PubSub.bounded<Journal.Entry>(repo.pageSize),
        PubSub.shutdown,
      );
      const subscription = yield* PubSub.subscribe(pubsub);
      yield* repo.tail(["local"], start, (next) =>
        Effect.gen(function* () {
          const batch = yield* order.localPage(next);
          const entries = yield* repo.readMany(
            batch.ids.map((id) => ["local", id]),
            EntryCodec,
          );
          return [entries, Option.some(batch.next)] as const;
        })).pipe(
          Stream.runForEach((entry) => PubSub.publish(pubsub, entry)),
          Effect.orDie,
          Effect.forkScoped,
        );
      return subscription;
    }),
    destroy: repo.transaction(Effect.gen(function* () {
      yield* repo.clear;
      yield* order.clearPending;
    })),
    withLock: (storeId) => (effect) =>
      repo.withTransaction(Effect.gen(function* () {
        // A read/write conflict key gives cross-process serializable isolation.
        // This is optimistic: callbacks may overlap; only one conflicting commit
        // succeeds. External side effects require their own idempotency protocol.
        const version = yield* repo.number(["lock", storeId]).pipe(
          Effect.orDie,
        );
        yield* repo.setNumber(["lock", storeId], version + 1).pipe(
          Effect.orDie,
        );
        return yield* effect;
      })),
  });
});

export const layerEventJournal = (options: EventJournalOptions = {}) =>
  Layer.effect(Journal.EventJournal, makeEventJournal(options));
