import { Console, Effect, Option, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  Subspace,
} from "../mod.ts";
import { bytes, runMain, text } from "./_shared.ts";

const names = [
  "Alice",
  "Bob",
  "Carol",
  "Dave",
  "Eve",
  "Frank",
  "George",
  "Harry",
  "Ian",
  "Jack",
  "Liz",
  "Mary",
  "Nathan",
] as const;

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const queue = Subspace.fromBytes(bytes("Q"));
  const [begin, end] = yield* queue.range();
  yield* database.clearRange(begin, end);

  const enqueue = Effect.fn("example.enqueue")(function* (value: string) {
    const count = (yield* database.getRange(
      keyRange(begin, end, { snapshot: true }),
    )).length;
    const random = crypto.getRandomValues(new Uint8Array(20));
    const key = yield* queue.pack([BigInt(count + 1), random]);
    yield* database.set(key, bytes(value));
  });

  const dequeue = Effect.fn("example.dequeue")(function* () {
    const item = yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        return yield* Stream.runHead(transaction.getRange(
          keyRange(begin, end, { limit: 1, reverse: true, snapshot: true }),
        ));
      }),
    );
    if (Option.isNone(item)) {
      return Option.none<string>();
    }
    yield* database.clear(item.value.key);
    return Option.some(text(item.value.value));
  });

  yield* Effect.forEach(names, enqueue, { concurrency: 1, discard: true });

  while (true) {
    const value = yield* dequeue();
    if (Option.isNone(value)) {
      break;
    }
    yield* Console.log(value.value);
  }
});

await runMain(program);
