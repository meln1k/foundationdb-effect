import { Console, Effect, Schema, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  MutationType,
  pack,
  packWithVersionstamp,
  Subspace,
  unpack,
  Versionstamp,
} from "../mod.ts";
import { assert, assertBytesEqual, runMain } from "./_shared.ts";

const VersionstampTuple = Schema.Tuple([
  Schema.String,
  Versionstamp.schema,
]);
const StringTuple = Schema.Tuple([Schema.String]);

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const root = yield* Subspace.fromTuple(["versionstamp_example"]);
  const [begin, end] = yield* root.range();
  yield* database.clearRange(begin, end);

  const first1 = yield* Versionstamp.incomplete(1);
  const first2 = yield* Versionstamp.incomplete(2);

  const firstVersionFuture = yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.atomicOp(
        yield* root.packWithVersionstamp(["prefix", first1]),
        yield* pack(["first_1"]),
        MutationType.SetVersionstampedKey,
      );
      yield* transaction.atomicOp(
        yield* root.packWithVersionstamp(["prefix", first2]),
        yield* pack(["first_2"]),
        MutationType.SetVersionstampedKey,
      );
      return yield* transaction.getVersionstamp();
    }),
  );
  const firstVersion = yield* firstVersionFuture.await.pipe(
    Effect.onInterrupt(() => firstVersionFuture.cancel),
  );

  const second1 = yield* Versionstamp.incomplete(1);
  const second2 = yield* Versionstamp.incomplete(2);
  const secondVersionFuture = yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.atomicOp(
        yield* root.packWithVersionstamp(["prefix", second1]),
        yield* pack(["second_1"]),
        MutationType.SetVersionstampedKey,
      );
      yield* transaction.atomicOp(
        yield* root.packWithVersionstamp(["prefix", second2]),
        yield* pack(["second_2"]),
        MutationType.SetVersionstampedKey,
      );
      return yield* transaction.getVersionstamp();
    }),
  );
  const secondVersion = yield* secondVersionFuture.await.pipe(
    Effect.onInterrupt(() => secondVersionFuture.cancel),
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
    values.join(",") === "first_1,first_2,second_1,second_2",
    `unexpected versionstamp order: ${values}`,
  );
  assert(
    firstVersion.some((byte, index) => byte !== secondVersion[index]),
    "separate commits must have separate transaction versions",
  );

  const referencedStamp = yield* Versionstamp.incomplete(3);
  const indexKey = yield* root.pack(["index"]);
  const referencedVersionFuture = yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.atomicOp(
        yield* root.packWithVersionstamp(["data", referencedStamp]),
        yield* pack(["some value"]),
        MutationType.SetVersionstampedKey,
      );
      yield* transaction.atomicOp(
        indexKey,
        yield* packWithVersionstamp(["data", referencedStamp]),
        MutationType.SetVersionstampedValue,
      );
      return yield* transaction.getVersionstamp();
    }),
  );
  const referencedVersion = yield* referencedVersionFuture.await.pipe(
    Effect.onInterrupt(() => referencedVersionFuture.cancel),
  );
  const encodedReference = yield* database.get(indexKey);
  assert(encodedReference !== undefined, "versionstamp index was not found");
  const reference = yield* unpack(
    encodedReference,
    VersionstampTuple,
  );
  assertBytesEqual(reference[1].transactionVersion, referencedVersion);
  const stored = yield* database.get(yield* root.pack(reference));
  assert(stored !== undefined, "versionstamped value was not found");
  const [storedValue] = yield* unpack(stored, StringTuple);
  yield* Console.log(`got back value ${storedValue}`);
});

await runMain(program);
