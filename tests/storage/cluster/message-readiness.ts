import { assert, assertEquals } from "@std/assert";
import { Cause, Effect, Exit } from "effect";
import * as EntityAddress from "effect/cluster/EntityAddress";
import * as EntityId from "effect/cluster/EntityId";
import * as EntityType from "effect/cluster/EntityType";
import type * as Envelope from "effect/cluster/Envelope";
import type * as MessageStorage from "effect/cluster/MessageStorage";
import * as ShardId from "effect/cluster/ShardId";
import * as Snowflake from "effect/cluster/Snowflake";
import {
  FoundationDb,
  FoundationDbTransaction,
} from "../../../src/FoundationDb.ts";
import { makeEncodedMessageStorage } from "../../../src/cluster/message-storage.ts";
import { makeMessageSubspaces } from "../../../src/cluster/message-subspaces.ts";
import { DirectorySubspace } from "../../../src/directory/mod.ts";
import { keyRange } from "../../../src/model.ts";
import type { Subspace } from "../../../src/tuple/mod.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const address = (id: string, shard = 1) =>
  EntityAddress.make({
    entityType: EntityType.make("Ready"),
    entityId: EntityId.make(id),
    shardId: ShardId.make("default", shard),
  });
const a = address("a"), b = address("b"), c = address("c", 2);
const request = (id: string, target = a): Envelope.Encoded => ({
  _tag: "Request",
  requestId: id,
  address: target,
  tag: "run",
  payload: id,
  headers: {},
});
const save = (
  store: MessageStorage.Encoded,
  envelope: Envelope.Encoded,
  deliverAt: number | null = null,
) => store.saveEnvelope({ envelope, deliverAt, primaryKey: null });
const ids = (rows: ReadonlyArray<{ envelope: Envelope.Encoded }>) =>
  rows.map(({ envelope }) =>
    envelope._tag === "Request" ? envelope.requestId : envelope.id
  );
const run = <A, E>(effect: Effect.Effect<A, E, FoundationDb>) =>
  Effect.runPromise(
    effect.pipe(Effect.provideService(FoundationDb, testDatabase().database)),
  );
const layout = Effect.gen(function* () {
  const db = yield* FoundationDb;
  const root = yield* db.withTransaction(
    testDatabase().directory.open(["effect-foundationdb", "message-storage"]),
  );
  assert(root instanceof DirectorySubspace);
  return { root, ...yield* makeMessageSubspaces(root) };
});
const entries = Effect.fnUntraced(function* (space: Subspace) {
  return yield* (yield* FoundationDb).getRange(
    keyRange(...yield* space.range()),
  );
});
const indexedIds = Effect.fnUntraced(function* (space: Subspace) {
  return yield* Effect.forEach(
    yield* entries(space),
    (row) =>
      Effect.map(
        space.unpack(row.key),
        (tuple) => String(tuple[tuple.length - 1]),
      ),
  );
});

storageTest(
  "ready timers promote every due page in original order, at inclusive fractional boundaries and only for selected addresses/shards",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const backlog = Array.from({ length: 130 }, (_, i) => String(1000 - i));
      yield* store.withTransaction(Effect.forEach(backlog, (id, i) =>
        save(store, request(id), i === 0 ? 30.5 : 10), { discard: true }));
      yield* save(store, request("2", b), 10);
      yield* save(store, request("1", c), 10);
      yield* save(store, request("5"));
      const s = yield* layout;
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 0)), [
        "5",
      ]);
      assertEquals(
        ids(
          yield* store.unprocessedMessages(["default:1"], 30.499, {
            addresses: [a],
            limit: 1,
          }),
        ),
        ["999"],
      );
      // The oldest message is at the end of the timer range, not its first page.
      assertEquals(
        ids(
          yield* store.unprocessedMessages(["default:1"], 30.5, {
            addresses: [a, a, c],
            limit: 3,
          }),
        ),
        ["1000", "998", "997"],
      );
      assertEquals(
        yield* indexedIds(
          yield* s.readyByAddress.subspace(["Ready", "a", "default:1"]),
        ),
        backlog.slice(4),
      );
      assertEquals(
        yield* indexedIds(
          yield* s.scheduledByAddress.subspace(["Ready", "b", "default:1"]),
        ),
        ["2"],
      );
      assertEquals(
        yield* indexedIds(yield* s.scheduledByShard.subspace(["default:2"])),
        ["1"],
      );
      assertEquals(
        ids(
          yield* store.unprocessedMessages(["default:1"], 30.5, {
            addresses: [b],
          }),
        ),
        ["2"],
      );
      assertEquals(ids(yield* store.unprocessedMessages(["default:2"], 30.5)), [
        "1",
      ]);
      for (const target of [a, b, c]) {
        yield* store.clearAddress(target);
      }
      assertEquals(yield* entries(s.root), []);
    })),
);

storageTest(
  "ACK readiness excludes blocked and superseded controls, preserves claims and delays, and survives completion/reset/reopen",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(store, request("900"), 50);
      yield* save(store, request("100"));
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 10)), [
        "100",
      ]);
      for (const [id, sequence] of [["3000", 0], ["3001", 1]] as const) {
        yield* store.saveReply({
          _tag: "Chunk",
          requestId: "100",
          id,
          sequence,
          values: [id],
        });
      }
      yield* save(store, {
        _tag: "Interrupt",
        id: "101",
        requestId: "100",
        address: a,
      });
      const ack = (id: string, replyId: string): Envelope.Encoded => ({
        _tag: "AckChunk",
        id,
        replyId,
        requestId: "100",
        address: a,
      });
      yield* save(store, ack("102", "3000"));
      const s = yield* layout;
      assertEquals(yield* entries(s.readyByShard), []);
      assertEquals(yield* indexedIds(s.scheduledByShard), ["900"]);
      yield* save(store, ack("103", "3001"));
      assertEquals(yield* indexedIds(s.readyByShard), ["101", "103"]);
      yield* save(store, ack("104", "3001"), 70);
      yield* save(store, ack("102", "3000")); // Duplicate must not supersede 104.
      assertEquals(yield* indexedIds(s.readyByShard), ["101"]);
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 60)), [
        "900",
        "101",
      ]);
      assertEquals(
        ids(
          yield* store.unprocessedMessagesById(
            [Snowflake.Snowflake("100")],
            60,
          ),
        ),
        ["100"],
      );
      yield* store.saveReply({
        _tag: "WithExit",
        requestId: "100",
        id: "4000",
        exit: { _tag: "Success", value: "done" },
      });
      assertEquals(yield* indexedIds(s.scheduledByShard), ["900"]);
      yield* store.clearReplies(Snowflake.Snowflake("100"), {
        expectedReplyId: Snowflake.Snowflake("4000"),
      });
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 69)), [
        "100",
        "101",
      ]);
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 70)), [
        "104",
      ]);
      yield* store.resetAddresses([a]);
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 70)), [
        "900",
        "100",
        "101",
        "104",
      ]);
      yield* store.clearAddress(a);
      assertEquals(yield* entries(s.root), []);
    })),
);

storageTest(
  "timer promotion conflicts with a concurrent blocking reply and competing pollers claim distinct expired messages",
  () =>
    run(Effect.gen(function* () {
      const fixture = testDatabase();
      const writer = yield* makeEncodedMessageStorage({
        directory: fixture.directory,
      });
      yield* save(writer, request("900"), 25);
      const s = yield* layout;
      let raced = false, attempts = 0;
      const observed: FoundationDb["Service"] = {
        ...fixture.database,
        withTransaction: (effect, options) =>
          fixture.database.withTransaction(
            Effect.gen(function* () {
              attempts++;
              const tx = yield* FoundationDbTransaction;
              return yield* Effect.provideService(
                effect,
                FoundationDbTransaction,
                {
                  ...tx,
                  getMany: (keys, options) =>
                    tx.getMany(keys, options).pipe(Effect.tap(() => {
                      if (raced || !s.messages.contains(keys[0])) {
                        return Effect.void;
                      }
                      raced = true;
                      return Effect.promise(() =>
                        Effect.runPromise(writer.saveReply({
                          _tag: "Chunk",
                          requestId: "900",
                          id: "3000",
                          sequence: 0,
                          values: ["wait"],
                        }))
                      );
                    })),
                },
              );
            }),
            options,
          ),
      };
      const reader = yield* makeEncodedMessageStorage({
        directory: fixture.directory,
      }).pipe(Effect.provideService(FoundationDb, observed));
      assertEquals(yield* reader.unprocessedMessages(["default:1"], 25), []);
      assert(attempts >= 2);
      assertEquals(yield* entries(s.readyByShard), []);
      assertEquals(yield* entries(s.scheduledByShard), []);
      yield* save(writer, {
        _tag: "AckChunk",
        id: "901",
        replyId: "3000",
        requestId: "900",
        address: a,
      });
      assertEquals(ids(yield* writer.unprocessedMessages(["default:1"], 25)), [
        "900",
        "901",
      ]);
      assertEquals(
        yield* reader.unprocessedMessages(["default:1"], 600024.999),
        [],
      );
      const results = yield* Effect.all([
        writer.unprocessedMessages(["default:1"], 600025, { limit: 1 }),
        reader.unprocessedMessages(["default:1"], 600025, { limit: 1 }),
      ], { concurrency: 2 });
      assertEquals(results.map((rows) => rows.length), [1, 1]);
      assertEquals(ids(results.flat()).sort(), ["900", "901"]);
      assertEquals(yield* entries(s.readyByShard), []);
      assertEquals((yield* entries(s.scheduledByShard)).length, 2);
    })),
);

storageTest(
  "pending timers compose with claims, resets and completion and roll back with their ambient transaction",
  () =>
    run(Effect.gen(function* () {
      const options = { directory: testDatabase().directory };
      const first = yield* makeEncodedMessageStorage(options);
      const second = yield* makeEncodedMessageStorage(options);
      // Establish the directory outside the transaction whose records roll back.
      yield* first.unprocessedMessages(["default:1"], 0);
      const s = yield* layout;
      const failed = yield* Effect.exit(
        first.withTransaction(Effect.gen(function* () {
          yield* save(first, request("900"), 10);
          yield* save(second, request("100"), 20);
          assertEquals(
            ids(yield* second.unprocessedMessages(["default:1"], 10)),
            ["900"],
          );
          yield* first.resetRequests([Snowflake.Snowflake("900")]);
          assertEquals(yield* second.unprocessedMessages(["default:1"], 9), []);
          assertEquals(
            ids(yield* first.unprocessedMessages(["default:1"], 20)),
            ["900", "100"],
          );
          yield* second.saveReply({
            _tag: "WithExit",
            requestId: "900",
            id: "3000",
            exit: { _tag: "Success", value: "done" },
          });
          yield* second.resetRequests([Snowflake.Snowflake("100")]);
          assertEquals(
            ids(yield* first.unprocessedMessages(["default:1"], 20)),
            ["100"],
          );
          return yield* Effect.fail("rollback");
        })),
      );
      assert(Exit.isFailure(failed));
      assertEquals(Cause.squash(failed.cause), "rollback");
      assertEquals(yield* entries(s.root), []);
      yield* save(second, request("900"), 10);
      assertEquals(ids(yield* first.unprocessedMessages(["default:1"], 10)), [
        "900",
      ]);
    })),
);
