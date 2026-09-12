import { Console, Effect } from "effect";
import { FoundationDb, FoundationDbTransaction, Subspace } from "../mod.ts";
import {
  assert,
  littleEndianInt64,
  readLittleEndianInt64,
  runMain,
} from "./_shared.ts";

const readCounter = Effect.fn("example.readCounter")(function* (
  key: Uint8Array,
) {
  const transaction = yield* FoundationDbTransaction;
  const value = yield* transaction.get(key, { snapshot: true });
  assert(value !== undefined, "counter was not found");
  return readLittleEndianInt64(value);
});

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const counterKey = yield* Subspace.all().pack(["stats", "my_counter"]);

  yield* database.withTransaction(
    Effect.flatMap(
      FoundationDbTransaction,
      (transaction) => transaction.atomicAdd(counterKey, littleEndianInt64(1n)),
    ),
  );

  const first = yield* database.withTransaction(readCounter(counterKey));
  yield* Console.log(`counter after increment: ${first}`);
  assert(first > 0n, "counter must be positive after increment");

  yield* database.withTransaction(
    Effect.flatMap(
      FoundationDbTransaction,
      (transaction) =>
        transaction.atomicAdd(counterKey, littleEndianInt64(-1n)),
    ),
  );

  const second = yield* database.withTransaction(readCounter(counterKey));
  yield* Console.log(`counter after decrement: ${second}`);
  assert(second === first - 1n, "decrement did not change the counter by one");
});

await runMain(program);
