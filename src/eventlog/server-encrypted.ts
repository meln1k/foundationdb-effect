import { Effect, Layer, Stream } from "effect";
import * as Encryption from "effect/eventlog/EventLogEncryption";
import * as Server from "effect/eventlog/EventLogServerEncrypted";
import { binary, makeRepository } from "./internal.ts";
import type { EventLogStoreOptions } from "./internal.ts";

export interface EventLogServerEncryptedStorageOptions
  extends EventLogStoreOptions {}

const EntryCodec = binary(Encryption.EncryptedRemoteEntry);

/** Encrypted sequences start at zero, matching Effect's memory storage. */
export const makeEventLogServerEncryptedStorage = Effect.fnUntraced(function* (
  options: EventLogServerEncryptedStorageOptions = {},
) {
  const repo = yield* makeRepository(options, "encrypted");
  return Server.Storage.of({
    getId: repo.getId.pipe(Effect.orDie),
    getOrCreateSessionAuthBinding: (publicKey, key) =>
      repo.auth(publicKey, key).pipe(Effect.orDie),
    write: (publicKey, storeId, entries) =>
      repo.transaction(Effect.gen(function* () {
        const path = ["entries", publicKey, storeId] as const;
        let sequence = yield* repo.number(["next", publicKey, storeId]);
        const written: Array<Encryption.EncryptedRemoteEntry> = [];
        const existing = yield* repo.hasMany(
          entries.map((entry) => ["ids", publicKey, storeId, entry.entryId]),
        );
        const seen = new Set<string>();
        for (const [index, entry] of entries.entries()) {
          const id = ["ids", publicKey, storeId, entry.entryId] as const;
          if (existing[index] || seen.has(entry.entryIdString)) continue;
          seen.add(entry.entryIdString);
          const remote = { ...entry, sequence: sequence++ };
          yield* repo.put(
            [...path, BigInt(remote.sequence)],
            EntryCodec,
            remote,
          );
          yield* repo.mark([...path, BigInt(remote.sequence)]);
          yield* repo.mark(id);
          written.push(remote);
        }
        if (written.length > 0) {
          yield* repo.setNumber(["next", publicKey, storeId], sequence);
          yield* repo.notify(path);
        }
        return written;
      })).pipe(Effect.orDie),
    changes: (publicKey, storeId, startSequence) =>
      repo.feed(
        ["entries", publicKey, storeId],
        EntryCodec,
        Math.max(0, startSequence),
      ).pipe(Stream.orDie),
  });
});

export const layerEventLogServerEncryptedStorage = (
  options: EventLogServerEncryptedStorageOptions = {},
) => Layer.effect(Server.Storage, makeEventLogServerEncryptedStorage(options));
