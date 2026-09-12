import { Console, Effect, Schema, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  pack,
  Subspace,
  unpack,
  Versionstamp,
} from "../mod.ts";
import { assert, runMain } from "./_shared.ts";

const VersionstampTuple = Schema.Tuple([
  Schema.String,
  Versionstamp.schema,
]);
const StringTuple = Schema.Tuple([Schema.String]);

// Tuple versionstamps are complete, but SetVersionstampedKey and
// SetVersionstampedValue mutations are not yet exposed by the native bridge.
// This runnable analogue uses complete versionstamps to demonstrate their
// canonical tuple ordering and a versionstamped-key reference.
const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const root = yield* Subspace.fromTuple(["versionstamp_example"]);
  const [begin, end] = yield* root.range();
  yield* database.clearRange(begin, end);

  const firstCommit = new Uint8Array(10);
  firstCommit[9] = 1;
  const secondCommit = new Uint8Array(10);
  secondCommit[9] = 2;
  const first1 = yield* Versionstamp.complete(firstCommit, 1);
  const first2 = yield* Versionstamp.complete(firstCommit, 2);
  const second1 = yield* Versionstamp.complete(secondCommit, 1);
  const second2 = yield* Versionstamp.complete(secondCommit, 2);

  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.set(
        yield* root.pack(["prefix", first1]),
        yield* pack(["value_2_1"]),
      );
      yield* transaction.set(
        yield* root.pack(["prefix", first2]),
        yield* pack(["value_2_2"]),
      );
    }),
  );
  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.set(
        yield* root.pack(["prefix", second1]),
        yield* pack(["value_1_2"]),
      );
      yield* transaction.set(
        yield* root.pack(["prefix", second2]),
        yield* pack(["value_1_1"]),
      );
    }),
  );

  const values = yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      const [rangeBegin, rangeEnd] = yield* root.range(["prefix"]);
      const rows = yield* Stream.runCollect(
        transaction.getRange(keyRange(rangeBegin, rangeEnd)),
      );
      return yield* Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          const [, stamp] = yield* root.unpack(
            row.key,
            VersionstampTuple,
          );
          const [value] = yield* unpack(row.value, StringTuple);
          yield* Console.log(
            `${
              Array.from(stamp.transactionVersion)
            } ${stamp.userVersion}: ${value}`,
          );
          return value;
        }));
    }),
  );
  assert(
    values.join(",") === "value_2_1,value_2_2,value_1_2,value_1_1",
    `unexpected versionstamp order: ${values}`,
  );

  const referencedStamp = yield* Versionstamp.complete(secondCommit, 3);
  const referencedKeyTuple = ["data", referencedStamp] as const;
  const referencedKey = yield* root.pack(referencedKeyTuple);
  const indexKey = yield* root.pack(["index"]);
  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.set(referencedKey, yield* pack(["some value"]));
      yield* transaction.set(indexKey, yield* pack(referencedKeyTuple));
    }),
  );
  const encodedReference = yield* database.get(indexKey);
  assert(encodedReference !== undefined, "versionstamp index was not found");
  const reference = yield* unpack(
    encodedReference,
    VersionstampTuple,
  );
  const stored = yield* database.get(yield* root.pack(reference));
  assert(stored !== undefined, "versionstamped value was not found");
  const [storedValue] = yield* unpack(stored, StringTuple);
  yield* Console.log(`got back value ${storedValue}`);
});

await runMain(program);
