import { assert, assertEquals } from "@std/assert";
import {
  Cause,
  Clock,
  Effect,
  Exit,
  Fiber,
  Option,
  PubSub,
  Schema,
  Stream,
} from "effect";
import {
  EntityAddress,
  EntityId,
  EntityType,
  Runner,
  RunnerAddress,
  ShardId,
  ShardingConfig,
  Snowflake,
} from "effect/cluster";
import * as EventLogMessage from "effect/eventlog/EventLogMessage";
import * as Encrypted from "effect/eventlog/EventLogServerEncrypted";
import {
  DirectorySubspace,
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  makeEventJournal,
  makeEventLogServerEncryptedStorage,
  makeEventLogServerUnencryptedStorage,
  makeRunnerStorage,
} from "../../mod.ts";
import type { Directory } from "../../mod.ts";
import { makeEncodedMessageStorage } from "../../src/cluster/message-storage.ts";

// Called inside the existing live test's single native network lifetime. The
// supplied directory lives entirely under that test's disposable binary prefix.
export const testClusterAndEventlog = Effect.fnUntraced(
  function* (directory: Directory) {
    const options = { directory, pollIntervalMs: 5, pageSize: 2 };
    const db = yield* FoundationDb;
    const shard = ShardId.make("default", 1);
    const address = EntityAddress.make({
      shardId: shard,
      entityType: EntityType.make("Live"),
      entityId: EntityId.make("entity"),
    });
    const mailbox = yield* makeEncodedMessageStorage(options);
    const mailbox2 = yield* makeEncodedMessageStorage(options);
    const payload = "mailbox-".repeat(20_000);
    const saves = yield* Effect.forEach(
      [mailbox, mailbox2],
      (store, i) =>
        store.saveEnvelope({
          envelope: {
            _tag: "Request",
            requestId: String(i + 1),
            address,
            tag: "run",
            payload,
            headers: {},
          },
          primaryKey: "same-request",
          deliverAt: null,
        }),
      { concurrency: "unbounded" },
    );
    assertEquals(
      saves.filter((s) => s._tag === "Success").length,
      1,
    );
    const requestId = Option.getOrThrow(
      yield* mailbox.requestIdForPrimaryKey("same-request"),
    );
    const now = yield* Clock.currentTimeMillis;
    const claims = yield* Effect.all([
      mailbox.unprocessedMessages([shard.toString()], now, { limit: 1 }),
      mailbox2.unprocessedMessages([shard.toString()], now, { limit: 1 }),
    ], { concurrency: "unbounded" });
    assertEquals(claims.flat().length, 1);
    const claimed = claims.flat()[0].envelope;
    assert(claimed._tag === "Request");
    assertEquals(claimed.payload, payload);
    yield* mailbox.saveReply({
      _tag: "WithExit",
      id: "100",
      requestId: String(requestId),
      exit: { _tag: "Success", value: payload },
    });
    yield* mailbox2.clearReplies(requestId, {
      expectedReplyId: Snowflake.Snowflake("99"),
    });
    assertEquals((yield* mailbox.repliesFor([String(requestId)])).length, 1);
    yield* mailbox2.clearReplies(requestId, {
      expectedReplyId: Snowflake.Snowflake("100"),
    });
    assertEquals((yield* mailbox.repliesFor([String(requestId)])).length, 0);
    assertEquals(
      (yield* mailbox.unprocessedMessages([shard.toString()], now)).length,
      1,
    );
    yield* mailbox2.clearAddress(address);

    const laterShard = EntityAddress.make({
      ...address,
      shardId: ShardId.make("default", 2),
    });
    const cancelled = EntityAddress.make({
      ...address,
      entityId: EntityId.make("cancelled"),
    });
    const foreign = EntityAddress.make({
      ...address,
      entityId: EntityId.make("foreign"),
    });
    const offer = (store: typeof mailbox, id: string, target = address) =>
      store.saveEnvelope({
        envelope: {
          _tag: "Request",
          requestId: id,
          address: target,
          tag: "run",
          payload: id,
          headers: {},
        },
        primaryKey: null,
        deliverAt: null,
      });
    const selectedShards: [string, string] = ["default:1", "default:2"];
    yield* offer(mailbox, "900", laterShard);
    yield* offer(mailbox2, "100");
    assertEquals(
      (yield* mailbox.unprocessedMessages(selectedShards, now, { limit: 1 }))
        .map((m) => m.envelope.requestId),
      ["900"],
    );
    yield* mailbox.resetShards(selectedShards);
    assertEquals(
      (yield* mailbox.unprocessedMessages(selectedShards, now, {
        addresses: [address, laterShard],
      })).map((m) => m.envelope.requestId),
      ["900", "100"],
    );
    yield* mailbox.clearAddress(laterShard);
    yield* mailbox.clearAddress(address);
    yield* mailbox.withTransaction(Effect.gen(function* () {
      yield* offer(mailbox, "700", cancelled);
      yield* offer(mailbox2, "200");
      yield* offer(mailbox, "500", cancelled);
      assertEquals(
        (yield* mailbox2.unprocessedMessages(selectedShards, now)).map((m) =>
          m.envelope.requestId
        ),
        ["700", "200", "500"],
      );
      yield* mailbox.resetShards(selectedShards);
      yield* mailbox2.clearAddress(cancelled);
      assertEquals(
        (yield* mailbox.unprocessedMessages(selectedShards, now)).map((m) =>
          m.envelope.requestId
        ),
        ["200"],
      );
      yield* mailbox.resetShards(selectedShards);
    }));
    assertEquals(
      (yield* mailbox2.unprocessedMessages(selectedShards, now)).map((m) =>
        m.envelope.requestId
      ),
      ["200"],
    );
    yield* mailbox.clearAddress(address);

    // Cancelling a pending index rebuilds its future tail. A foreign insertion
    // committed after our read version must cause a retry, not be erased.
    let mailboxAttempts = 0;
    yield* mailbox.withTransaction(Effect.gen(function* () {
      mailboxAttempts++;
      yield* offer(mailbox, "600", cancelled);
      yield* offer(mailbox2, "400");
      if (mailboxAttempts === 1) {
        yield* db.withTransaction(offer(mailbox2, "300", foreign));
      }
      yield* mailbox.clearAddress(cancelled);
    }));
    assertEquals(mailboxAttempts, 2);
    assertEquals(
      (yield* mailbox.unprocessedMessages(selectedShards, now)).map((m) =>
        m.envelope.requestId
      ),
      ["300", "400"],
    );
    yield* mailbox.clearAddress(address);
    yield* mailbox.clearAddress(foreign);
    const mailboxRoot = yield* db.withTransaction(
      directory.open(["effect-foundationdb", "message-storage"]),
    );
    assert(mailboxRoot instanceof DirectorySubspace);
    const [mailboxBegin, mailboxEnd] = yield* mailboxRoot.range();
    assertEquals(
      yield* db.getRange(keyRange(mailboxBegin, mailboxEnd)),
      [],
    );

    const runners = yield* Effect.all([
      makeRunnerStorage(options),
      makeRunnerStorage(options),
    ]);
    const members = [3101, 3102].map((port) =>
      Runner.make({
        address: RunnerAddress.make("localhost", port),
        groups: ["default"],
        weight: 1,
      })
    );
    const machineIds = yield* Effect.forEach(
      runners,
      (store, i) => store.register(members[i], true),
      { concurrency: "unbounded" },
    );
    assertEquals(new Set(machineIds).size, 2);
    const locks = yield* Effect.forEach(
      runners,
      (store, i) => store.acquire(members[i].address, [shard]),
      { concurrency: "unbounded" },
    );
    assertEquals(locks.flat().length, 1);
    const owner = locks[0].length === 1 ? 0 : 1;
    const other = 1 - owner;
    yield* runners[other].release(members[other].address, shard);
    assertEquals(
      yield* runners[other].acquire(members[other].address, [shard]),
      [],
    );
    yield* runners[owner].releaseAll(members[owner].address);
    assertEquals(
      yield* runners[other].acquire(members[other].address, [shard]),
      [shard],
    );

    const journal = yield* makeEventJournal(options);
    const journal2 = yield* makeEventJournal(options);
    const storeId = Schema.decodeSync(EventLogMessage.StoreId)("live");
    const subscription = yield* journal2.changes;
    const large = new Uint8Array(150_000).fill(173);
    const entry = yield* journal.withLock(storeId)(journal.write({
      event: "changed",
      primaryKey: "key",
      payload: large,
      effect: Effect.succeed,
    }));
    assertEquals((yield* PubSub.take(subscription)).payload, large);
    const projected = yield* db.withTransaction(Effect.gen(function* () {
      const root = yield* directory.createOrOpen(["projection"]);
      assert(root instanceof DirectorySubspace);
      return yield* root.pack(["value"]);
    }));
    const failed = yield* journal.write({
      event: "rolled-back",
      primaryKey: "key",
      payload: new Uint8Array(),
      effect: () =>
        Effect.gen(function* () {
          const tx = Option.getOrThrow(
            yield* Effect.serviceOption(FoundationDbTransaction),
          );
          yield* tx.set(projected, new Uint8Array([9]));
          return yield* Effect.fail("rollback");
        }),
    }).pipe(Effect.exit);
    assert(Exit.isFailure(failed));
    assertEquals(Cause.squash(failed.cause), "rollback");
    assertEquals(yield* db.get(projected), undefined);
    assertEquals(
      (yield* journal2.entries).map((e) => e.idString),
      [entry.idString],
    );

    // Force a real FDB conflict. The explicit retry policy must replay the pure
    // callback, while the failed attempt must not publish a second journal entry.
    const retrying = yield* makeEventJournal({
      ...options,
      callbackRetryLimit: 2,
    });
    let attempts = 0;
    yield* retrying.write({
      event: "retried",
      primaryKey: "key",
      payload: new Uint8Array([7]),
      effect: () =>
        Effect.gen(function* () {
          const tx = Option.getOrThrow(
            yield* Effect.serviceOption(FoundationDbTransaction),
          );
          yield* tx.get(projected);
          attempts++;
          if (attempts === 1) {
            yield* db.set(projected, new Uint8Array([1]));
          }
          yield* tx.set(projected, new Uint8Array([2]));
        }),
    });
    assertEquals(attempts, 2);
    assertEquals(yield* db.get(projected), new Uint8Array([2]));
    assertEquals((yield* journal2.entries).length, 2);

    const encrypted = yield* makeEventLogServerEncryptedStorage(options);
    const encrypted2 = yield* makeEventLogServerEncryptedStorage(options);
    const ids = yield* Effect.all([encrypted.getId, encrypted2.getId], {
      concurrency: "unbounded",
    });
    assertEquals(ids[0], ids[1]);
    const bindings = yield* Effect.all([
      encrypted.getOrCreateSessionAuthBinding("client", new Uint8Array([1])),
      encrypted2.getOrCreateSessionAuthBinding("client", new Uint8Array([2])),
    ], { concurrency: "unbounded" });
    assertEquals(bindings[0], bindings[1]);
    const ciphertext = new Encrypted.PersistedEntry({
      entryId: entry.id,
      encryptedEntry: large,
      iv: new Uint8Array(12),
    });
    const writes = yield* Effect.all([
      encrypted.write("client", storeId, [ciphertext]),
      encrypted2.write("client", storeId, [ciphertext]),
    ], { concurrency: "unbounded" });
    assertEquals(
      writes.flat().map((e) => e.sequence),
      [0],
    );
    assertEquals(
      (yield* encrypted2.changes("client", storeId, 0).pipe(
        Stream.take(1),
        Stream.runCollect,
      ))[0].encryptedEntry,
      large,
    );

    const plain = yield* makeEventLogServerUnencryptedStorage(options);
    const plain2 = yield* makeEventLogServerUnencryptedStorage(options);
    assertEquals(yield* plain.getId, yield* plain2.getId);
    const rollback = yield* plain.withTransaction(Effect.gen(function* () {
      yield* plain.write(storeId, [entry]);
      return yield* Effect.fail("rollback");
    })).pipe(Effect.exit);
    assert(Exit.isFailure(rollback));
    assertEquals(yield* plain2.entriesAfter(storeId, entry), []);
    const tail = yield* plain2.changes({
      storeId,
      startSequence: 1,
      compactors: new Map(),
    }).pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped);
    assertEquals((yield* plain.write(storeId, [entry]))[0].remoteSequence, 1);
    assertEquals((yield* Fiber.join(tail))[0].entry.payload, large);
  },
  Effect.scoped,
  Effect.provide(ShardingConfig.layer()),
);
