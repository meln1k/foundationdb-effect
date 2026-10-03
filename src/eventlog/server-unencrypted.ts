import { Effect, Layer, Stream } from "effect";
import * as Journal from "effect/eventlog/EventJournal";
import * as Server from "effect/eventlog/EventLogServerUnencrypted";
import { EntryCodec, makeRepository, RemoteEntryCodec } from "./internal.ts";
import type { EventLogStoreOptions } from "./internal.ts";

export interface EventLogServerUnencryptedStorageOptions
  extends EventLogStoreOptions {}

/** Plain sequences start at one. Duplicate IDs fail the entire write, as upstream. */
export const makeEventLogServerUnencryptedStorage = Effect.fnUntraced(
  function* (
    options: EventLogServerUnencryptedStorageOptions = {},
  ) {
    const repo = yield* makeRepository(options, "unencrypted");
    return Server.Storage.of({
      getId: repo.getId.pipe(Effect.orDie),
      getOrCreateSessionAuthBinding: (publicKey, key) =>
        repo.auth(publicKey, key).pipe(Effect.orDie),
      entriesAfter: (storeId, entry) =>
        repo.transaction(Effect.gen(function* () {
          const ids = yield* repo.all(["ordered", storeId], [entry.id]);
          return yield* repo.readMany(
            ids.map((id) => ["byId", storeId, ...id]),
            EntryCodec,
          );
        })).pipe(Effect.orDie),
      write: (storeId, entries) =>
        repo.transaction(Effect.gen(function* () {
          let next = yield* repo.number(["next", storeId], 1);
          const written: Array<Journal.RemoteEntry> = [];
          const existing = yield* repo.hasMany(
            entries.map((entry) => ["ordered", storeId, entry.id]),
          );
          const seen = new Set<string>();
          for (const [index, entry] of entries.entries()) {
            if (existing[index] || seen.has(entry.idString)) {
              return yield* Effect.die("Duplicate entries");
            }
            seen.add(entry.idString);
            const remote = new Journal.RemoteEntry({
              remoteSequence: next++,
              entry,
            });
            yield* repo.put(
              ["entries", storeId, BigInt(remote.remoteSequence)],
              RemoteEntryCodec,
              remote,
            );
            yield* repo.mark([
              "entries",
              storeId,
              BigInt(remote.remoteSequence),
            ]);
            yield* repo.put(["byId", storeId, entry.id], EntryCodec, entry);
            yield* repo.mark(["ordered", storeId, entry.id]);
            written.push(remote);
          }
          if (written.length > 0) {
            yield* repo.setNumber(["next", storeId], next);
            yield* repo.notify(["entries", storeId]);
          }
          return written;
        })).pipe(Effect.orDie),
      changes: ({ storeId, startSequence, compactors }) =>
        Stream.unwrap(Effect.gen(function* () {
          const start = Math.max(1, startSequence);
          const path = ["entries", storeId] as const;
          if (compactors.size === 0) {
            return repo.feed(path, RemoteEntryCodec, start).pipe(Stream.orDie);
          }
          // Capture a committed high-water mark. Compact the complete backlog so
          // compaction brackets can span storage pages, then tail from that mark.
          const end = yield* repo.transaction(repo.number(["next", storeId], 1))
            .pipe(Effect.orDie);
          const backlog = yield* Stream.runCollect(
            repo.feed(path, RemoteEntryCodec, start, end),
          ).pipe(Effect.orDie);
          const compacted = yield* Server.compactBacklog({
            remoteEntries: backlog,
            compactors,
          });
          return Stream.fromArray(compacted).pipe(Stream.concat(
            repo.feed(path, RemoteEntryCodec, Math.max(start, end)).pipe(
              Stream.orDie,
            ),
          ));
        })),
      withTransaction: repo.withTransaction,
    });
  },
);

export const layerEventLogServerUnencryptedStorage = (
  options: EventLogServerUnencryptedStorageOptions = {},
) =>
  Layer.effect(Server.Storage, makeEventLogServerUnencryptedStorage(options));
