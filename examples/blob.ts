import { Console, Effect, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  Subspace,
} from "../mod.ts";
import { assertBytesEqual, concatBytes, runMain } from "./_shared.ts";

const chunkSize = 1_024;

const writeBlob = Effect.fn("example.writeBlob")(function* (
  subspace: Subspace,
  data: Uint8Array,
) {
  const transaction = yield* FoundationDbTransaction;
  for (let offset = 0; offset < data.byteLength; offset += chunkSize) {
    const chunk = data.slice(offset, offset + chunkSize);
    const key = yield* subspace.pack([BigInt(offset)]);
    yield* transaction.set(key, chunk);
  }
});

const readBlob = Effect.fn("example.readBlob")(function* (
  subspace: Subspace,
) {
  const transaction = yield* FoundationDbTransaction;
  const [begin, end] = yield* subspace.range();
  const rows = yield* Stream.runCollect(
    transaction.getRange(keyRange(begin, end)),
  );
  return concatBytes(...rows.map((row) => row.value));
});

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const root = yield* Subspace.fromTuple(["example-blob"]);
  const [begin, end] = yield* root.range();
  yield* database.clearRange(begin, end);

  yield* Effect.forEach(
    Array.from({ length: 100 }, (_, index) => index + 1),
    (iteration) =>
      Effect.gen(function* () {
        yield* Console.log(`Iteration #${iteration}`);
        const data = crypto.getRandomValues(new Uint8Array(10_000));
        const subspace = yield* root.subspace([BigInt(iteration)]);
        yield* database.withTransaction(writeBlob(subspace, data));
        const stored = yield* database.withTransaction(readBlob(subspace));
        assertBytesEqual(stored, data);
      }),
    { concurrency: 1, discard: true },
  );
});

await runMain(program);
