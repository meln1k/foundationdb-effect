import { assert, assertEquals } from "@std/assert";
import { Effect, Exit, Schema, Stream } from "effect";
import * as EntityAddress from "effect/cluster/EntityAddress";
import * as EntityId from "effect/cluster/EntityId";
import * as EntityType from "effect/cluster/EntityType";
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
import { Versionstamp } from "../../../src/tuple/mod.ts";
import type { Subspace } from "../../../src/tuple/mod.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const address = (name: string, shard: number) =>
  EntityAddress.make({
    entityType: EntityType.make("Ordered"),
    entityId: EntityId.make(name),
    shardId: ShardId.make("default", shard),
  });
const a = address("a", 2), b = address("b", 1);
const save = (
  store: MessageStorage.Encoded,
  id: string,
  target = a,
  deliverAt: number | null = null,
) =>
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
    deliverAt,
  });
const ids = (rows: ReadonlyArray<{ envelope: { requestId: string } }>) =>
  rows.map((row) => row.envelope.requestId);
const shards: [string, string] = ["default:1", "default:2"];
const run = <A, E>(effect: Effect.Effect<A, E, FoundationDb>) =>
  Effect.runPromise(effect.pipe(
    Effect.provideService(FoundationDb, testDatabase().database),
  ));

storageTest(
  "mailbox merges commit order across shards and addresses before claiming a bounded batch",
  () =>
    run(Effect.gen(function* () {
      const first = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(first, "900");
      yield* save(second, "100", b);
      yield* save(first, "700");
      yield* save(second, "800", b, 100);
      yield* save(first, "50");
      assertEquals(
        ids(
          yield* second.unprocessedMessagesById([
            Snowflake.Snowflake("50"),
            Snowflake.Snowflake("700"),
            Snowflake.Snowflake("900"),
            Snowflake.Snowflake("100"),
          ], 0),
        ),
        ["900", "100", "700", "50"],
      );
      assertEquals(
        ids(yield* second.unprocessedMessages(shards, 0, { limit: 2 })),
        ["900", "100"],
      );
      assertEquals(
        ids(yield* first.unprocessedMessages(shards, 0, { limit: 2 })),
        ["700", "50"],
      );
      yield* first.resetShards(shards);
      assertEquals(
        ids(
          yield* second.unprocessedMessages(shards, 0, {
            limit: 3,
            addresses: [b, a, b],
          }),
        ),
        ["900", "100", "700"],
      );
      assertEquals(ids(yield* first.unprocessedMessages(shards, 100)), [
        "800",
        "50",
      ]);
    })),
);

storageTest(
  "pending mailbox order survives nested adapters, cancellation, reinsertion and rollback without orphan indexes",
  () =>
    run(Effect.gen(function* () {
      const { directory } = testDatabase();
      const options = { directory, directoryPath: ["ordered"] };
      const first = yield* makeEncodedMessageStorage(options);
      const second = yield* makeEncodedMessageStorage(options);
      const db = yield* FoundationDb;
      yield* save(first, "950", b);
      yield* db.withTransaction(Effect.gen(function* () {
        yield* save(first, "900");
        yield* second.withTransaction(save(second, "100", b));
        yield* save(first, "700");
        assertEquals(
          ids(yield* second.unprocessedMessages(shards, 0, { limit: 3 })),
          ["950", "900", "100"],
        );
        yield* second.resetShards(shards);
        yield* first.clearAddress(a);
        yield* save(second, "900");
        assertEquals(ids(yield* first.unprocessedMessages(shards, 0)), [
          "950",
          "100",
          "900",
        ]);
        yield* first.resetShards(shards);
      }));
      assertEquals(ids(yield* second.unprocessedMessages(shards, 0)), [
        "950",
        "100",
        "900",
      ]);
      const failed = yield* first.withTransaction(Effect.gen(function* () {
        yield* save(second, "5");
        yield* first.clearAddress(b);
        return yield* Effect.fail("rollback");
      })).pipe(Effect.exit);
      assert(Exit.isFailure(failed));
      yield* second.resetShards(shards);
      assertEquals(ids(yield* first.unprocessedMessages(shards, 0)), [
        "950",
        "100",
        "900",
      ]);
      yield* db.withTransaction(Effect.gen(function* () {
        const root = yield* directory.open(["ordered"]);
        assert(root instanceof DirectorySubspace);
        const tx = yield* FoundationDbTransaction;
        const [begin, end] = yield* root.range(["unfinished", "shard"]);
        const rows = yield* Stream.runCollect(
          tx.getRange(keyRange(begin, end)),
        );
        const tuples = yield* Effect.forEach(
          rows,
          (row) =>
            root.unpack(
              row.key,
              Schema.Tuple([
                Schema.String,
                Schema.String,
                Schema.String,
                Versionstamp.schema,
                Schema.String,
              ]),
            ),
        );
        assertEquals(tuples.length, 3);
        assert(tuples.every((tuple) => tuple[3].isComplete));
        const byId = new Map(tuples.map((tuple) => [tuple[4], tuple[3]]));
        assertEquals(
          byId.get("100")!.transactionVersion,
          byId.get("900")!.transactionVersion,
        );
        assertEquals(byId.get("100")!.userVersion, 1);
        assertEquals(byId.get("900")!.userVersion, 3);
      }));
      yield* first.clearAddress(a);
      yield* first.clearAddress(b);
      yield* db.withTransaction(Effect.gen(function* () {
        const root = yield* directory.open(["ordered"]);
        assert(root instanceof DirectorySubspace);
        const tx = yield* FoundationDbTransaction;
        const [begin, end] = yield* root.range();
        assertEquals(
          yield* Stream.runCollect(tx.getRange(keyRange(begin, end))),
          [],
        );
      }));
    })),
);

storageTest(
  "mailbox ordering merges pages and skips ineligible entries without claiming later candidates",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const expected: Array<string> = [];
      yield* store.withTransaction(Effect.gen(function* () {
        for (let i = 0; i < 140; i++) {
          const id = String(1000 - i);
          yield* save(store, id, i % 2 === 0 ? a : b, i < 130 ? 100 : null);
          if (i >= 130) expected.push(id);
        }
      }));
      assertEquals(
        ids(yield* store.unprocessedMessages(shards, 0, { limit: 3 })),
        expected.slice(0, 3),
      );
      assertEquals(
        ids(yield* store.unprocessedMessages(shards, 0)),
        expected.slice(3),
      );
    })),
);

const complete = (
  store: MessageStorage.Encoded,
  requestId: string,
  id: string,
) =>
  store.saveReply({
    _tag: "WithExit",
    id,
    requestId,
    exit: { _tag: "Success", value: "done" },
  });
const mailboxRoot = Effect.gen(function* () {
  const db = yield* FoundationDb;
  const root = yield* db.withTransaction(
    testDatabase().directory.open([
      "effect-foundationdb",
      "message-storage",
    ]),
  );
  assert(root instanceof DirectorySubspace);
  return root;
});
const entries = Effect.fnUntraced(function* (space: Subspace) {
  const db = yield* FoundationDb;
  return yield* db.getRange(keyRange(...yield* space.range()));
});

storageTest(
  "unfinished polling does not read completed history in either index",
  () =>
    run(Effect.gen(function* () {
      const fixture = testDatabase();
      const store = yield* makeEncodedMessageStorage({
        directory: fixture.directory,
      });
      const history = Array.from({ length: 130 }, (_, i) => String(1000 + i));
      yield* store.withTransaction(
        Effect.forEach(
          history,
          (id, i) => save(store, id, i % 2 === 0 ? a : b),
          { discard: true },
        ),
      );
      yield* store.withTransaction(
        Effect.forEach(history, (id) =>
          complete(store, id, String(Number(id) + 10000)), { discard: true }),
      );
      const spaces = yield* makeMessageSubspaces(yield* mailboxRoot);
      assertEquals(yield* entries(spaces.unfinishedByShard), []);
      assertEquals(yield* entries(spaces.unfinishedByAddress), []);
      assertEquals((yield* entries(spaces.orders)).length, 130);
      assertEquals((yield* store.repliesFor(["1000"]))[0].id, "11000");
      assertEquals((yield* save(store, "1000"))._tag, "Duplicate");

      let metadataReads = 0;
      const observed: FoundationDb["Service"] = {
        ...fixture.database,
        withTransaction: (effect, options) =>
          fixture.database.withTransaction(
            Effect.flatMap(FoundationDbTransaction, (tx) =>
              Effect.provideService(
                effect,
                FoundationDbTransaction,
                {
                  ...tx,
                  get: (key, options) => {
                    if (spaces.messages.contains(key)) {
                      metadataReads++;
                    }
                    return tx.get(key, options);
                  },
                  getMany: (keys, options) => {
                    metadataReads += keys.filter((key) =>
                      spaces.messages.contains(key)
                    ).length;
                    return tx.getMany(keys, options);
                  },
                },
              )),
            options,
          ),
      };
      const reader = yield* makeEncodedMessageStorage({
        directory: fixture.directory,
      }).pipe(
        Effect.provideService(FoundationDb, observed),
      );
      assertEquals(
        yield* reader.unprocessedMessages(shards, 0, { limit: 1 }),
        [],
      );
      assertEquals(metadataReads, 0);
      yield* save(store, "2", b);
      yield* save(store, "1", a);
      assertEquals(
        ids(
          yield* reader.unprocessedMessages(shards, 0, {
            limit: 1,
            addresses: [a, b],
          }),
        ),
        ["2"],
      );
      // Both candidates are prefetched, but only the selected one is claimed.
      assertEquals(metadataReads, 2);
      metadataReads = 0;
      assertEquals(
        ids(yield* reader.unprocessedMessages(["default:2"], 0, { limit: 1 })),
        ["1"],
      );
      assertEquals(metadataReads, 1);
      assertEquals((yield* entries(spaces.unfinishedByShard)).length, 2);
      assertEquals((yield* entries(spaces.unfinishedByAddress)).length, 2);
    })),
);

storageTest(
  "completion, late envelopes, reopening and rollback maintain unfinished indexes and original stamps",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(store, "900");
      yield* save(store, "100", b);
      const root = yield* mailboxRoot;
      const spaces = yield* makeMessageSubspaces(root);
      const originalOrders = yield* entries(spaces.orders);
      yield* complete(store, "900", "9000");
      for (
        const envelope of [
          {
            _tag: "Interrupt" as const,
            id: "901",
            requestId: "900",
            address: a,
          },
          {
            _tag: "AckChunk" as const,
            id: "902",
            replyId: "8999",
            requestId: "900",
            address: a,
          },
        ]
      ) {
        yield* store.saveEnvelope({
          envelope,
          primaryKey: null,
          deliverAt: null,
        });
      }
      // A terminal reply can also precede the request envelope.
      yield* store.saveReply({
        _tag: "WithExit",
        id: "7000",
        requestId: "700",
        exit: { _tag: "Failure", cause: [{ _tag: "Fail", error: "rejected" }] },
      });
      yield* save(store, "700");
      assertEquals((yield* entries(spaces.unfinishedByShard)).length, 1);
      assertEquals((yield* entries(spaces.unfinishedByAddress)).length, 1);
      yield* store.resetShards(shards);
      yield* store.resetAddresses([a, b]);
      yield* store.resetRequests([Snowflake.Snowflake("900")]);
      yield* store.clearReplies(Snowflake.Snowflake("900"), {
        expectedReplyId: Snowflake.Snowflake("8999"),
      });
      assertEquals((yield* entries(spaces.unfinishedByShard)).length, 1);

      yield* store.clearReplies(Snowflake.Snowflake("900"), {
        expectedReplyId: Snowflake.Snowflake("9000"),
      });
      assertEquals((yield* entries(spaces.unfinishedByShard)).length, 4);
      assertEquals((yield* entries(spaces.unfinishedByAddress)).length, 4);
      const polled = yield* store.unprocessedMessages(shards, 0, {
        addresses: [b, a],
      });
      assertEquals(
        polled.map(({ envelope }) =>
          envelope._tag === "Request" ? envelope.requestId : envelope.id
        ),
        ["900", "100", "901", "902"],
      );
      const db = yield* FoundationDb;
      for (const { key, value } of originalOrders) {
        assertEquals(yield* db.get(key), value);
      }

      const beforeRollback = yield* entries(
        yield* root.subspace(["unfinished"]),
      );
      const failed = yield* Effect.exit(
        store.withTransaction(Effect.gen(function* () {
          yield* complete(store, "100", "1000");
          yield* store.clearReplies(Snowflake.Snowflake("700"));
          return yield* Effect.fail("rollback");
        })),
      );
      assert(Exit.isFailure(failed));
      assertEquals(
        yield* entries(yield* root.subspace(["unfinished"])),
        beforeRollback,
      );
      assertEquals((yield* store.repliesFor(["700"]))[0].id, "7000");
      yield* complete(store, "900", "9001");
      yield* store.clearReplies(Snowflake.Snowflake("900"));
      // Unconditional reopen removes interrupts but retains the request and ACK.
      assertEquals((yield* entries(spaces.unfinishedByShard)).length, 3);
      yield* store.clearAddress(a);
      yield* store.clearAddress(b);
      assertEquals(yield* entries(root), []);
    })),
);

storageTest(
  "pending completion and reopening preserve ordering and never reissue finished siblings",
  () =>
    run(Effect.gen(function* () {
      const options = { directory: testDatabase().directory };
      const first = yield* makeEncodedMessageStorage(options);
      const second = yield* makeEncodedMessageStorage(options);
      yield* first.withTransaction(Effect.gen(function* () {
        yield* save(first, "900");
        yield* save(second, "100", b);
        yield* save(first, "700");
        yield* complete(second, "900", "9000");
        yield* complete(first, "700", "7000");
        assertEquals(ids(yield* second.unprocessedMessages(shards, 0)), [
          "100",
        ]);
        yield* second.clearReplies(Snowflake.Snowflake("900"));
        yield* second.resetShards(shards);
        assertEquals(ids(yield* first.unprocessedMessages(shards, 0)), [
          "900",
          "100",
        ]);
        // Clear/reissue again while 700 remains finished in the pending map.
        yield* complete(first, "900", "9001");
        yield* second.clearReplies(Snowflake.Snowflake("900"));
        yield* second.resetShards(shards);
        // Initially unindexed pending messages can also reopen before commit.
        yield* complete(first, "500", "5000");
        yield* save(second, "500");
        yield* first.clearReplies(Snowflake.Snowflake("500"));
        assertEquals(ids(yield* second.unprocessedMessages(shards, 0)), [
          "900",
          "100",
          "500",
        ]);
        yield* complete(second, "500", "5001");
        yield* second.resetShards(shards);
      }));
      const root = yield* mailboxRoot;
      const spaces = yield* makeMessageSubspaces(root);
      assertEquals((yield* entries(spaces.unfinishedByShard)).length, 2);
      assertEquals((yield* entries(spaces.unfinishedByAddress)).length, 2);
      assertEquals((yield* entries(spaces.orders)).length, 4);
      assertEquals(ids(yield* second.unprocessedMessages(shards, 0)), [
        "900",
        "100",
      ]);
      // Reopen a message completed before its original enqueue committed.
      yield* second.clearReplies(Snowflake.Snowflake("700"));
      yield* first.resetShards(shards);
      assertEquals(ids(yield* second.unprocessedMessages(shards, 0)), [
        "900",
        "100",
        "700",
      ]);
      yield* first.clearAddress(a);
      yield* first.clearAddress(b);
      assertEquals(yield* entries(root), []);
    })),
);

storageTest(
  "an envelope racing a committed completion retries without leaving unfinished entries",
  () =>
    run(Effect.gen(function* () {
      const options = { directory: testDatabase().directory };
      const writer = yield* makeEncodedMessageStorage(options);
      const finisher = yield* makeEncodedMessageStorage(options);
      yield* save(writer, "900");
      let attempts = 0;
      yield* writer.withTransaction(Effect.gen(function* () {
        attempts++;
        yield* writer.saveEnvelope({
          envelope: {
            _tag: "Interrupt",
            id: "100",
            requestId: "900",
            address: a,
          },
          primaryKey: null,
          deliverAt: null,
        });
        if (attempts === 1) {
          // Separate Effect runtime intentionally avoids the ambient transaction.
          yield* Effect.promise(() =>
            Effect.runPromise(complete(finisher, "900", "9000"))
          );
        }
      }));
      assert(attempts >= 2);
      const root = yield* mailboxRoot;
      const spaces = yield* makeMessageSubspaces(root);
      assertEquals(yield* entries(spaces.unfinishedByShard), []);
      assertEquals(yield* entries(spaces.unfinishedByAddress), []);
      assertEquals((yield* entries(spaces.orders)).length, 2);
      yield* writer.clearReplies(Snowflake.Snowflake("900"), {
        expectedReplyId: Snowflake.Snowflake("9000"),
      });
      const reopened = yield* writer.unprocessedMessages(shards, 0);
      assertEquals(
        reopened.map(({ envelope }) =>
          envelope._tag === "Request" ? envelope.requestId : envelope.id
        ),
        ["900", "100"],
      );
      assertEquals((yield* entries(spaces.unfinishedByShard)).length, 2);
      assertEquals((yield* entries(spaces.unfinishedByAddress)).length, 2);
    })),
);
