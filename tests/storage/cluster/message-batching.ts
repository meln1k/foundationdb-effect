import { assert, assertEquals } from "@std/assert";
import { Effect, Latch, Stream } from "effect";
import * as EntityAddress from "effect/cluster/EntityAddress";
import * as EntityId from "effect/cluster/EntityId";
import * as EntityType from "effect/cluster/EntityType";
import type * as Envelope from "effect/cluster/Envelope";
import * as ShardId from "effect/cluster/ShardId";
import * as Snowflake from "effect/cluster/Snowflake";
import {
  FoundationDb,
  FoundationDbTransaction,
} from "../../../src/FoundationDb.ts";
import { makeEncodedMessageStorage } from "../../../src/cluster/message-storage.ts";
import { makeMessageSubspaces } from "../../../src/cluster/message-subspaces.ts";
import { DirectorySubspace } from "../../../src/directory/mod.ts";
import { storageTest, testDatabase } from "../../support/real-database.ts";

const address = (shard: number) =>
  EntityAddress.make({
    entityType: EntityType.make("Batch"),
    entityId: EntityId.make("a"),
    shardId: ShardId.make("default", shard),
  });
const request = (
  id: string,
  shard = 1,
): Extract<Envelope.Encoded, { _tag: "Request" }> => ({
  _tag: "Request",
  requestId: id,
  address: address(shard),
  tag: "run",
  payload: id,
  headers: {},
});
const ids = (rows: ReadonlyArray<{ envelope: Envelope.Encoded }>) =>
  rows.map(({ envelope }) =>
    envelope._tag === "Request" ? envelope.requestId : envelope.id
  );
const run = <A, E>(effect: Effect.Effect<A, E, FoundationDb>) =>
  Effect.runPromise(
    effect.pipe(Effect.provideService(FoundationDb, testDatabase().database)),
  );
const spaces = Effect.gen(function* () {
  const db = yield* FoundationDb;
  const root = yield* db.withTransaction(
    testDatabase().directory.open([
      "effect-foundationdb",
      "message-storage",
    ]),
  );
  assert(root instanceof DirectorySubspace);
  return yield* makeMessageSubspaces(root);
});

storageTest(
  "mailbox batches candidates and distinct states, bounds parallel payload reads and claims only selected messages",
  () =>
    run(Effect.gen(function* () {
      const fixture = testDatabase();
      const store = yield* makeEncodedMessageStorage({
        directory: fixture.directory,
      });
      const allIds = Array.from({ length: 84 }, (_, i) => String(1000 - i));
      yield* store.withTransaction(Effect.forEach(allIds, (id, i) =>
        store.saveEnvelope({
          envelope: {
            ...request(id, i % 2 + 1),
            payload: i === 64 ? "large".repeat(24000) : id,
          },
          primaryKey: null,
          deliverAt: i < 64 ? 100 : null,
        }), { discard: true }));
      // One blocked request and a control sharing another request's state.
      yield* store.saveReply({
        _tag: "Chunk",
        requestId: "935",
        id: "2000",
        sequence: 0,
        values: ["wait"],
      });
      yield* store.saveEnvelope({
        envelope: {
          _tag: "AckChunk",
          id: "50",
          requestId: "936",
          replyId: "2001",
          address: address(1),
        },
        primaryKey: null,
        deliverAt: null,
      });
      const s = yield* spaces;
      const batches: Array<{ family: string; ids: unknown[] }> = [];
      const payloads: string[] = [];
      const completed: string[] = [];
      let active = 0, peak = 0, metadataPointReads = 0;
      const eightStarted = Latch.makeUnsafe();
      const anotherFinished = Latch.makeUnsafe();
      const observed: FoundationDb["Service"] = {
        ...fixture.database,
        withTransaction: (effect, options) =>
          fixture.database.withTransaction(
            Effect.flatMap(FoundationDbTransaction, (tx) =>
              Effect.provideService(effect, FoundationDbTransaction, {
                ...tx,
                get: (key, options) => {
                  assert(
                    !s.orders.contains(key),
                    "Claims must reuse the order read from the ready index",
                  );
                  if (
                    s.messages.contains(key) || s.requestStates.contains(key)
                  ) {
                    metadataPointReads++;
                  }
                  return tx.get(key, options);
                },
                getMany: (keys, options) => {
                  assertEquals(options?.snapshot ?? false, false);
                  const family = s.messages.contains(keys[0])
                    ? "messages"
                    : "states";
                  const subspace = family === "messages"
                    ? s.messages
                    : s.requestStates;
                  batches.push({
                    family,
                    ids: keys.map((key) =>
                      Effect.runSync(subspace.unpack(key))[0]
                    ),
                  });
                  return tx.getMany(keys, options);
                },
                getRange: (range) => {
                  assert(
                    !s.messages.contains(range.begin.key) &&
                      !s.requestStates.contains(range.begin.key),
                  );
                  if (!s.envelopes.contains(range.begin.key)) {
                    return tx.getRange(range);
                  }
                  const id = String(
                    Effect.runSync(s.envelopes.unpack(range.begin.key))[0],
                  );
                  return Stream.unwrap(Effect.gen(function* () {
                    payloads.push(id);
                    peak = Math.max(peak, ++active);
                    if (active === 8) {
                      yield* eightStarted.open;
                    }
                    yield* eightStarted.await.pipe(Effect.timeout("5 seconds"));
                    if (id === "936") {
                      yield* anotherFinished.await.pipe(
                        Effect.timeout("5 seconds"),
                      );
                    }
                    return tx.getRange(range);
                  })).pipe(Stream.ensuring(Effect.gen(function* () {
                    active--;
                    completed.push(id);
                    if (id !== "936") {
                      yield* anotherFinished.open;
                    }
                  })));
                },
              })),
            options,
          ),
      };
      const reader = yield* makeEncodedMessageStorage({
        directory: fixture.directory,
      }).pipe(
        Effect.provideService(FoundationDb, observed),
      );
      const selected = yield* reader.unprocessedMessages(
        ["default:2", "default:1"],
        0,
        { limit: 12 },
      );
      const eligible = allIds.slice(64).filter((id) =>
        id !== "935"
      );
      assertEquals(ids(selected), eligible.slice(0, 12));
      assert(selected[0].envelope._tag === "Request");
      assertEquals(selected[0].envelope.payload, "large".repeat(24000));
      assertEquals(batches, [
        // Future and acknowledgement-blocked messages are not even fetched.
        { family: "messages", ids: [...eligible, "50"] },
        { family: "states", ids: eligible },
      ]);
      assertEquals(metadataPointReads, 0);
      assertEquals(payloads, eligible.slice(0, 12));
      assertEquals(peak, 8);
      assert(completed.indexOf("936") > 0);
      assertEquals(
        ids(yield* store.unprocessedMessages(["default:1", "default:2"], 0)),
        [
          ...eligible.slice(12),
          "50",
        ],
      );
      // Everything is now future-delivery, claimed or blocked: none of its
      // metadata is read on another poll, through either index family.
      assertEquals(
        yield* reader.unprocessedMessages(["default:1", "default:2"], 0),
        [],
      );
      assertEquals(
        yield* reader.unprocessedMessages(["default:1", "default:2"], 0, {
          addresses: [address(1), address(2)],
        }),
        [],
      );
      assertEquals(batches.length, 2);
      assertEquals(metadataPointReads, 0);
    })),
);

storageTest(
  "batched by-ID reads deduplicate IDs, ignore claims and see same-transaction metadata",
  () =>
    run(Effect.gen(function* () {
      const store = yield* makeEncodedMessageStorage({
        directory: testDatabase().directory,
      });
      yield* store.withTransaction(Effect.gen(function* () {
        for (const id of ["900", "100", "700"]) {
          yield* store.saveEnvelope({
            envelope: request(id),
            primaryKey: null,
            deliverAt: null,
          });
        }
        assertEquals(
          ids(yield* store.unprocessedMessages(["default:1"], 0, { limit: 1 })),
          ["900"],
        );
        assertEquals(
          ids(
            yield* store.unprocessedMessagesById(
              [
                Snowflake.Snowflake("700"),
                Snowflake.Snowflake("404"),
                Snowflake.Snowflake("900"),
                Snowflake.Snowflake("100"),
                Snowflake.Snowflake("700"),
              ],
              0,
            ),
          ),
          ["900", "100", "700"],
        );
        assertEquals(ids(yield* store.unprocessedMessages(["default:1"], 0)), [
          "100",
          "700",
        ]);
      }));
    })),
);

storageTest(
  "batched claim conflicts retry the entire selection without leaking prefetched claims",
  () =>
    run(Effect.gen(function* () {
      const fixture = testDatabase();
      const writer = yield* makeEncodedMessageStorage({
        directory: fixture.directory,
      });
      for (const id of ["900", "100", "700"]) {
        yield* writer.saveEnvelope({
          envelope: request(id),
          primaryKey: null,
          deliverAt: null,
        });
      }
      const s = yield* spaces;
      let conflicted = false, attempts = 0;
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
                      if (!s.requestStates.contains(keys[0]) || conflicted) {
                        return Effect.void;
                      }
                      conflicted = true;
                      // A separate runtime commits outside the polling transaction.
                      return Effect.promise(() =>
                        Effect.runPromise(writer.saveReply({
                          _tag: "WithExit",
                          requestId: "900",
                          id: "9000",
                          exit: { _tag: "Success", value: "done" },
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
      }).pipe(
        Effect.provideService(FoundationDb, observed),
      );
      assertEquals(
        ids(yield* reader.unprocessedMessages(["default:1"], 0, { limit: 1 })),
        ["100"],
      );
      assert(attempts >= 2);
      assertEquals(ids(yield* writer.unprocessedMessages(["default:1"], 0)), [
        "700",
      ]);
    })),
);
