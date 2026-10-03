import { assert, assertEquals } from "@std/assert";
import { Context, Effect, Exit, Layer, Option, Schema } from "effect";
import * as EntityAddress from "effect/cluster/EntityAddress";
import * as EntityId from "effect/cluster/EntityId";
import * as EntityType from "effect/cluster/EntityType";
import * as Envelope from "effect/cluster/Envelope";
import * as Message from "effect/cluster/Message";
import * as MessageStorage from "effect/cluster/MessageStorage";
import * as Reply from "effect/cluster/Reply";
import * as ShardId from "effect/cluster/ShardId";
import * as Snowflake from "effect/cluster/Snowflake";
import * as Headers from "effect/http/Headers";
import * as Rpc from "effect/rpc/Rpc";
import { FoundationDb } from "../../../src/FoundationDb.ts";
import {
  DuplicateReplyError,
  MissingMessageRecordError,
} from "../../../src/cluster/errors.ts";
import {
  layerMessageStorage,
  makeEncodedMessageStorage,
} from "../../../src/cluster/message-storage.ts";
import { makeMessageSubspaces } from "../../../src/cluster/message-subspaces.ts";
import { DirectorySubspace } from "../../../src/directory/mod.ts";
import { FoundationDbError } from "../../../src/errors.ts";
import { Subspace, Versionstamp } from "../../../src/tuple/mod.ts";
import type { Tuple } from "../../../src/tuple/mod.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const address = (id = "a", shard = 1) =>
  EntityAddress.make({
    entityType: EntityType.make("Test"),
    entityId: EntityId.make(id),
    shardId: ShardId.make("default", shard),
  });
const request = (
  id: string,
  target = address(),
  payload: unknown = { value: id },
): Envelope.Encoded => ({
  _tag: "Request",
  requestId: id,
  address: target,
  tag: "run",
  payload,
  headers: { test: "header" },
});
const save = (
  store: MessageStorage.Encoded,
  envelope: Envelope.Encoded,
  primaryKey: string | null = null,
  deliverAt: number | null = null,
) => store.saveEnvelope({ envelope, primaryKey, deliverAt });
const exit = (id: string, requestId = "1"): Reply.Encoded => ({
  _tag: "WithExit",
  id,
  requestId,
  exit: { _tag: "Success", value: "done" },
});
const chunk = (id: string, sequence = 0, requestId = "1"): Reply.Encoded => ({
  _tag: "Chunk",
  id,
  requestId,
  sequence,
  values: ["chunk"],
});
const ack = (id: string, replyId: string): Envelope.Encoded => ({
  _tag: "AckChunk",
  id,
  replyId,
  requestId: "1",
  address: address(),
});
const interrupt = (id: string): Envelope.Encoded => ({
  _tag: "Interrupt",
  id,
  requestId: "1",
  address: address(),
});
const ids = (
  messages: ReadonlyArray<{ readonly envelope: Envelope.Encoded }>,
) =>
  messages.map(({ envelope }) =>
    envelope._tag === "Request" ? envelope.requestId : envelope.id
  );
const snowflake = Snowflake.Snowflake;
const run = <A, E>(effect: Effect.Effect<A, E, FoundationDb>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provideService(FoundationDb, testDatabase().database),
    ),
  );

storageTest(
  "message subspaces preserve record, index and versionstamp key boundaries",
  () =>
    Effect.runPromise(Effect.gen(function* () {
      const root = Subspace.fromBytes(Uint8Array.of(0x15, 0x42, 0x00));
      const spaces = yield* makeMessageSubspaces(root);
      const layout: Record<keyof typeof spaces, Tuple> = {
        messages: ["message"],
        envelopes: ["envelope"],
        requestStates: ["state"],
        primaryKeys: ["primary"],
        replies: ["reply"],
        replyMetadata: ["reply-meta"],
        replyIds: ["replies"],
        uniqueReplies: ["reply-unique"],
        messagesByShard: ["shard"],
        messagesByAddress: ["address"],
        messagesByRequest: ["request"],
        readyByShard: ["ready", "shard"],
        readyByAddress: ["ready", "address"],
        scheduledByShard: ["scheduled", "shard"],
        scheduledByAddress: ["scheduled", "address"],
        orders: ["order"],
      };
      const suffix: Tuple = ["a\u0000b", 37n];
      for (const name of Object.keys(layout) as Array<keyof typeof spaces>) {
        assertEquals(
          yield* spaces[name].pack(suffix),
          yield* root.pack([...layout[name], ...suffix]),
        );
        assertEquals(
          yield* spaces[name].range(suffix),
          yield* root.range([...layout[name], ...suffix]),
        );
      }
      const shard = yield* spaces.readyByShard.subspace(["default:2"]);
      const stamp = yield* Versionstamp.incomplete(19);
      assertEquals(
        yield* shard.packWithVersionstamp([stamp, "900"]),
        yield* root.packWithVersionstamp([
          "ready",
          "shard",
          "default:2",
          stamp,
          "900",
        ]),
      );
    })),
);

storageTest(
  "message storage deduplicates atomically across independent instances",
  () =>
    run(Effect.gen(function* () {
      const first = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const primaryKey = "Test/a/run/" + "long-key".repeat(2_000);
      const results = yield* Effect.all([
        save(first, request("1"), primaryKey),
        save(second, request("2"), primaryKey),
      ], { concurrency: "unbounded" });
      assertEquals(
        results.filter((result) => result._tag === "Success").length,
        1,
      );
      const duplicate = results.find((result) => result._tag === "Duplicate");
      assert(duplicate?._tag === "Duplicate");
      const original = String(duplicate.originalId);
      assertEquals(
        yield* second.requestIdForPrimaryKey(primaryKey),
        Option.some(snowflake(original)),
      );
      yield* first.saveReply(exit("10", original));
      const third = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const repeated = yield* save(third, request("3"), primaryKey);
      assert(repeated._tag === "Duplicate");
      assertEquals(
        repeated.lastReceivedReply,
        Option.some(exit("10", original)),
      );
      assertEquals(yield* third.unprocessedMessages(["default:1"], 0), []);
      const isolated = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
        directoryPath: ["isolated-mailbox"],
      });
      assertEquals(
        yield* isolated.requestIdForPrimaryKey(primaryKey),
        Option.none(),
      );
    })),
);

storageTest(
  "message storage CAS clear rejects stale reply ids and releases current completion",
  () =>
    run(Effect.gen(function* () {
      const first = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(first, request("1"));
      yield* first.saveReply(exit("10"));
      yield* second.clearReplies(snowflake("1"), {
        expectedReplyId: snowflake("9"),
      });
      assertEquals(yield* first.repliesFor(["1"]), [exit("10")]);
      assertEquals(yield* first.unprocessedMessages(["default:1"], 0), []);
      yield* second.clearReplies(snowflake("1"), {
        expectedReplyId: snowflake("10"),
      });
      assertEquals(yield* first.repliesFor(["1"]), []);
      assertEquals(ids(yield* first.unprocessedMessages(["default:1"], 0)), [
        "1",
      ]);
      yield* first.saveReply(exit("11"));
      yield* second.clearReplies(snowflake("1"), {
        expectedReplyId: snowflake("10"),
      });
      assertEquals(yield* first.repliesFor(["1"]), [exit("11")]);
      yield* second.clearReplies(snowflake("1"));
      assertEquals(ids(yield* first.unprocessedMessages(["default:1"], 0)), [
        "1",
      ]);
    })),
);

storageTest(
  "message storage ACK, streaming history, interrupts and exit semantics",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(store, request("1"));
      yield* save(store, interrupt("2"));
      yield* store.saveReply(chunk("10"));
      yield* store.saveReply(chunk("10")); // idempotent, does not double-count
      assertEquals(yield* store.unprocessedMessages(["default:1"], 0), []);
      assertEquals(
        yield* store.unprocessedMessagesById([snowflake("1")], 0),
        [],
      );
      assertEquals(yield* store.repliesFor(["1"]), [chunk("10")]);
      yield* save(store, ack("3", "10"));
      yield* save(store, ack("4", "10"));
      assertEquals(yield* store.repliesFor(["1"]), []);
      assertEquals(yield* store.repliesForUnfiltered(["1"]), [chunk("10")]);
      const unprocessed = yield* store.unprocessedMessages(["default:1"], 0);
      assertEquals(ids(unprocessed), ["1", "2", "4"]);
      assertEquals(unprocessed[0].lastSentReply, Option.some(chunk("10")));
      assertEquals(unprocessed[2].lastSentReply, Option.none());
      yield* store.saveReply(chunk("11", 1));
      assertEquals(yield* store.repliesFor(["1"]), [chunk("11", 1)]);
      yield* save(store, ack("5", "11"));
      yield* store.saveReply(exit("12"));
      assertEquals(yield* store.repliesFor(["1"]), [exit("12")]);
      yield* store.resetShards(["default:1"]);
      assertEquals(yield* store.unprocessedMessages(["default:1"], 0), []);
      yield* store.clearReplies(snowflake("1"));
      assertEquals(yield* store.repliesForUnfiltered(["1"]), [
        chunk("10"),
        chunk("11", 1),
      ]);
      assertEquals(
        ids(yield* store.unprocessedMessagesById([snowflake("2")], 0)),
        [],
      );
      assertEquals(
        ids(yield* store.unprocessedMessagesById([snowflake("1")], 0)),
        ["1"],
      );
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 0)), [
        "1",
        "5",
      ]);
    })),
);

storageTest(
  "message storage claims only bounded, address-filtered, due messages",
  () =>
    run(Effect.gen(function* () {
      const first = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(first, request("1", address("a")));
      yield* save(first, request("2", address("b")));
      yield* save(first, request("3", address("b")));
      yield* save(first, request("4", address("b", 2)));
      yield* save(first, request("5", address("b")), null, 100);
      assertEquals(
        yield* first.unprocessedMessages(["default:1"], 0, { addresses: [] }),
        [],
      );
      assertEquals(
        yield* first.unprocessedMessages(["default:1"], 0, { limit: 0 }),
        [],
      );
      const claims = yield* Effect.all([
        first.unprocessedMessages(["default:1"], 0, {
          addresses: [address("b")],
          limit: 1,
        }),
        second.unprocessedMessages(["default:1"], 0, {
          addresses: [address("b")],
          limit: 1,
        }),
      ], { concurrency: "unbounded" });
      assertEquals(new Set(claims.flatMap(ids)), new Set(["2", "3"]));
      assertEquals(ids(yield* first.unprocessedMessages(["default:1"], 0)), [
        "1",
      ]);
      assertEquals(ids(yield* first.unprocessedMessages(["default:2"], 0)), [
        "4",
      ]);
      assertEquals(
        yield* first.unprocessedMessagesById([snowflake("5")], 99),
        [],
      );
      assertEquals(
        ids(yield* first.unprocessedMessagesById([snowflake("5")], 100)),
        ["5"],
      );
      assertEquals(ids(yield* second.unprocessedMessages(["default:1"], 100)), [
        "5",
      ]);
      assertEquals(
        ids(yield* first.unprocessedMessagesById([snowflake("1")], 100)),
        ["1"],
      ); // does not claim
      assertEquals(
        yield* first.unprocessedMessages(["default:1"], 599_999),
        [],
      );
      assertEquals(
        ids(yield* first.unprocessedMessages(["default:1"], 600_000)),
        ["1", "2", "3"],
      );
    })),
);

storageTest(
  "message storage resets release only selected claims without reviving exits",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(store, request("1"));
      yield* save(store, request("2", address("b")));
      yield* save(store, request("3", address("b", 2)));
      yield* store.unprocessedMessages(["default:1", "default:2"], 0);
      yield* store.resetRequests([snowflake("2")]);
      assertEquals(
        ids(yield* store.unprocessedMessages(["default:1", "default:2"], 0)),
        ["2"],
      );
      yield* store.resetAddresses([address("b")]);
      assertEquals(
        ids(yield* store.unprocessedMessages(["default:1", "default:2"], 0)),
        ["2"],
      );
      yield* store.resetShards(["default:2"]);
      assertEquals(
        ids(yield* store.unprocessedMessages(["default:1", "default:2"], 0)),
        ["3"],
      );
      yield* store.saveReply(exit("10"));
      yield* store.resetRequests([snowflake("1")]);
      yield* store.resetAddresses([address()]);
      yield* store.resetShards(["default:1"]);
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 0)), [
        "2",
      ]);
      assertEquals(yield* store.repliesFor(["1"]), [exit("10")]);
    })),
);

storageTest(
  "message storage clearAddress removes data and dedup across shard changes only for that entity",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(store, request("1"), "key-a");
      yield* save(store, request("2", address("a", 2)), "key-a-2");
      yield* save(store, request("3", address("b")), "key-b");
      yield* save(store, interrupt("4"));
      yield* store.saveReply(chunk("10"));
      yield* store.saveReply(exit("11", "2"));
      yield* store.saveReply(exit("12", "3"));
      yield* store.clearAddress(address());
      assertEquals(yield* store.requestIdForPrimaryKey("key-a"), Option.none());
      assertEquals(
        yield* store.requestIdForPrimaryKey("key-a-2"),
        Option.none(),
      );
      assertEquals(yield* store.repliesForUnfiltered(["1", "2", "3"]), [
        exit("12", "3"),
      ]);
      assertEquals(
        yield* store.unprocessedMessagesById([
          snowflake("1"),
          snowflake("2"),
          snowflake("4"),
        ], 0),
        [],
      );
      assertEquals((yield* save(store, request("5"), "key-a"))._tag, "Success");
      assertEquals(
        yield* store.requestIdForPrimaryKey("key-b"),
        Option.some(snowflake("3")),
      );
    })),
);

storageTest(
  "message storage reuses transaction context across instances and rolls back all mutations",
  () =>
    run(Effect.gen(function* () {
      const first = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const second = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      const failed = yield* Effect.exit(
        first.withTransaction(Effect.gen(function* () {
          yield* save(first, request("1"), "rollback");
          yield* second.withTransaction(second.saveReply(exit("10")));
          assertEquals(yield* first.repliesFor(["1"]), [exit("10")]);
          return yield* Effect.fail("rollback");
        })),
      );
      assert(Exit.isFailure(failed));
      assertEquals(
        yield* first.requestIdForPrimaryKey("rollback"),
        Option.none(),
      );
      assertEquals(yield* first.repliesForUnfiltered(["1"]), []);
      yield* first.withTransaction(save(second, request("1"), "committed"));
      const claimed = yield* Effect.exit(
        first.withTransaction(Effect.gen(function* () {
          assertEquals(
            ids(yield* second.unprocessedMessages(["default:1"], 0)),
            [
              "1",
            ],
          );
          return yield* Effect.fail("rollback-claim");
        })),
      );
      assert(Exit.isFailure(claimed));
      assertEquals(ids(yield* first.unprocessedMessages(["default:1"], 0)), [
        "1",
      ]);
      const database = yield* FoundationDb;
      yield* database.withTransaction(
        first.withTransaction(save(second, request("2"))),
      );
      assertEquals(ids(yield* first.unprocessedMessages(["default:1"], 0)), [
        "2",
      ]);
    })),
);

storageTest(
  "message storage chunks large envelopes and replies durably",
  async () => {
    const fixture = testDatabase();
    const payload = "large-🚀".repeat(30_000);
    await Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* makeEncodedMessageStorage({
          directory: fixture.directory,
        });
        yield* save(first, request("1", address(), payload));
        const second = yield* makeEncodedMessageStorage({
          directory: fixture.directory,
        });
        const loaded = yield* second.unprocessedMessagesById(
          [snowflake("1")],
          0,
        );
        assertEquals(loaded[0].envelope, {
          ...request("1", address(), payload),
          address: {
            shardId: { group: "default", id: 1 },
            entityType: "Test",
            entityId: "a",
          },
        });
        const largeReply: Reply.Encoded = {
          _tag: "Chunk",
          id: "10",
          requestId: "1",
          sequence: 0,
          values: [payload],
        };
        yield* second.saveReply(largeReply);
        const third = yield* makeEncodedMessageStorage({
          directory: fixture.directory,
        });
        assertEquals(yield* third.repliesFor(["1"]), [largeReply]);
        assert(
          (yield* fixture.entries()).filter((entry) =>
            entry.value.byteLength === 8_192
          )
            .length > 50,
        );
        assert(
          (yield* fixture.entries()).every((entry) =>
            entry.value.byteLength <= 8_192
          ),
        );
        yield* third.clearAddress(address());
        assertEquals(yield* first.repliesForUnfiltered(["1"]), []);
        assertEquals(
          yield* first.unprocessedMessagesById([snowflake("1")], 0),
          [],
        );
      }).pipe(Effect.provideService(FoundationDb, fixture.database)),
    );
  },
);

storageTest(
  "message storage traverses index pages without losing claims or clearing unrelated entities",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* store.withTransaction(
        Effect.forEach(
          Array.from({ length: 70 }, (_, index) => String(index + 1)),
          (id) => save(store, request(id)),
          { discard: true },
        ),
      );
      assertEquals(
        (yield* store.unprocessedMessages(["default:1"], 0, { limit: 65 }))
          .length,
        65,
      );
      assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 0)), [
        "66",
        "67",
        "68",
        "69",
        "70",
      ]);
      yield* store.resetShards(["default:1"]);
      assertEquals(
        (yield* store.unprocessedMessages(["default:1"], 0)).length,
        70,
      );
      yield* store.clearAddress(address());
      assertEquals(
        yield* store.unprocessedMessages(["default:1"], 600_000),
        [],
      );
    })),
);

storageTest(
  "message storage preserves tagged failure replies through persistence",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* save(store, request("91"));
      const reply: Reply.Encoded = {
        _tag: "WithExit",
        id: "502",
        requestId: "91",
        exit: {
          _tag: "Failure",
          cause: [
            { _tag: "Fail", error: { code: "rejected" } },
            { _tag: "Die", defect: "unexpected" },
            { _tag: "Interrupt", fiberId: 73 },
            { _tag: "Interrupt", fiberId: null },
            { _tag: "Interrupt", fiberId: undefined },
          ],
        },
      };
      yield* store.saveReply(reply);
      const reopened = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      assertEquals(yield* reopened.repliesForUnfiltered(["91"]), [reply]);
    })),
);

storageTest(
  "missing mailbox records retain their record type and ID in PersistenceError",
  () =>
    run(Effect.gen(function* () {
      const database = yield* FoundationDb;
      const { directory } = testDatabase();
      const store = yield* makeEncodedMessageStorage({ directory });
      for (
        const [record, id] of [["order", "91"], ["envelope", "92"], [
          "reply",
          "93",
        ]] as const
      ) {
        yield* save(store, request(id));
        if (record === "reply") yield* store.saveReply(chunk("501", 7, id));
        const root = yield* database.withTransaction(directory.open([
          "effect-foundationdb",
          "message-storage",
        ]));
        assert(root instanceof DirectorySubspace);
        if (record === "order") {
          yield* database.clear(yield* root.pack(["order", id]));
        } else {
          yield* database.clearRange(
            ...yield* root.range(
              record === "reply" ? ["reply", id, "501"] : ["envelope", id],
            ),
          );
        }
        const failure = record === "reply"
          ? yield* Effect.flip(store.repliesFor([id]))
          : yield* Effect.flip(
            store.unprocessedMessagesById([snowflake(id)], 0),
          );
        assertEquals(failure._tag, "PersistenceError");
        assert(Schema.is(MissingMessageRecordError)(failure.cause));
        assertEquals(failure.cause.record, record);
        assertEquals(failure.cause.id, record === "reply" ? "501" : id);
        assertEquals(failure.cause.message, `Missing mailbox ${record}`);
      }
    })),
);

storageTest(
  "message storage reports persistence failures and schema corruption in the typed channel",
  async () => {
    const fixture = testDatabase();
    await Effect.runPromise(
      Effect.gen(function* () {
        const store = yield* makeEncodedMessageStorage({
          directory: fixture.directory,
        });
        yield* save(store, request("1"));
        yield* store.saveReply(chunk("10"));
        const duplicate = yield* Effect.flip(store.saveReply(chunk("11")));
        assertEquals(duplicate._tag, "PersistenceError");
        assert(Schema.is(DuplicateReplyError)(duplicate.cause));
        assertEquals(duplicate.cause.requestId, "1");
        assertEquals(duplicate.cause.replyId, "11");
        assertEquals(duplicate.cause.sequence, 0);
        yield* store.saveReply(exit("12"));
        const duplicateExit = yield* Effect.flip(store.saveReply(exit("13")));
        assert(Schema.is(DuplicateReplyError)(duplicateExit.cause));
        assertEquals(duplicateExit.cause.requestId, "1");
        assertEquals(duplicateExit.cause.replyId, "13");
        assertEquals(duplicateExit.cause.sequence, null);
        const cyclic: { self?: unknown } = {};
        cyclic.self = cyclic;
        assertEquals(
          (yield* Effect.flip(save(store, request("2", address(), cyclic))))
            ._tag,
          "PersistenceError",
        );
        const meta = (yield* fixture.entries()).find((entry) =>
          new TextDecoder().decode(entry.value).includes('"claimedAt"')
        );
        assert(meta !== undefined);
        yield* fixture.database.set(
          meta.key,
          new TextEncoder().encode('{"bad":"metadata"}'),
        );
        assertEquals(
          (yield* Effect.flip(
            store.unprocessedMessagesById([snowflake("1")], 0),
          ))
            ._tag,
          "PersistenceError",
        );
      }).pipe(Effect.provideService(FoundationDb, fixture.database)),
    );
    const unavailable = new FoundationDbError({
      operation: "test",
      code: 1,
      message: "unavailable",
      retryable: false,
      maybeCommitted: false,
      retryableNotCommitted: false,
    });
    const store = await Effect.runPromise(
      makeEncodedMessageStorage({ directory: fixture.directory }).pipe(
        Effect.provideService(FoundationDb, {
          ...fixture.database,
          withTransaction: () => Effect.fail(unavailable),
        }),
      ),
    );
    assertEquals(
      (await Effect.runPromise(Effect.flip(save(store, request("1")))))._tag,
      "PersistenceError",
    );
    const transactionExit = await Effect.runPromise(
      Effect.exit(store.withTransaction(Effect.void)),
    );
    assert(Exit.isFailure(transactionExit));
  },
);

storageTest(
  "message claims never automatically replay an ambiguous commit",
  async () => {
    const fixture = testDatabase();
    const original = fixture.database.withTransaction;
    let operations = 0;
    const store = await Effect.runPromise(
      makeEncodedMessageStorage({
        directory: fixture.directory,
        transactionOptions: { retryOnMaybeCommitted: true },
      }).pipe(Effect.provideService(FoundationDb, {
        ...fixture.database,
        withTransaction: (effect, options) => {
          operations++;
          assertEquals(options?.retryOnMaybeCommitted, false);
          return original(effect, options);
        },
      })),
    );
    await Effect.runPromise(Effect.gen(function* () {
      yield* save(store, request("1"));
      assertEquals(
        (yield* store.unprocessedMessages(["default:1"], 0)).length,
        1,
      );
      yield* store.withTransaction(store.resetRequests([snowflake("1")]));
    }));
    assertEquals(operations, 3);
  },
);

storageTest(
  "message storage layer provides the decoded Effect adapter",
  async () => {
    const fixture = testDatabase();
    const layer = layerMessageStorage({ directory: fixture.directory }).pipe(
      Layer.provide(Layer.succeed(FoundationDb, fixture.database)),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const store = yield* MessageStorage.MessageStorage;
        assertEquals(
          yield* store.unprocessedMessages([ShardId.make("default", 1)]),
          [],
        );
        const rpc = Rpc.make("run", {
          payload: { value: Schema.String },
          success: Schema.Void,
        });
        const outgoing = new Message.OutgoingRequest({
          envelope: Envelope.makeRequest<typeof rpc>({
            requestId: snowflake("1"),
            address: address(),
            tag: "run",
            payload: { value: "request" },
            headers: Headers.empty,
          }),
          rpc,
          context: Context.empty(),
          annotations: Context.empty(),
          respond: () => Effect.void,
          lastReceivedReply: Option.none(),
        });
        assertEquals((yield* store.saveRequest(outgoing))._tag, "Success");
        const incoming = yield* store.unprocessedMessages([
          ShardId.make("default", 1),
        ]);
        assertEquals(incoming.length, 1);
        assertEquals(incoming[0].envelope.requestId, snowflake("1"));
        const reply = new Reply.WithExit<typeof rpc>({
          id: snowflake("10"),
          requestId: snowflake("1"),
          exit: Exit.void,
        });
        yield* store.saveReply(
          new Reply.ReplyWithContext({ rpc, context: Context.empty(), reply }),
        );
        assertEquals(yield* store.repliesFor([outgoing]), [reply]);
        assertEquals(
          yield* store.unprocessedMessages([ShardId.make("default", 1)]),
          [],
        );
        yield* store.resetAddress(address());
      }).pipe(Effect.provide(layer)),
    );
  },
);
