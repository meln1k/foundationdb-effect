import { Console, Effect, Ref } from "effect";
import { FoundationDb, FoundationDbTransaction } from "../mod.ts";
import { bytes, runMain } from "./_shared.ts";

const hex = (value: Uint8Array): string =>
  Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("");

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const key = bytes("example_conflict_key");
  const observedAttempts = yield* Ref.make(0);

  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* Ref.set(observedAttempts, transaction.attempt);
      yield* Console.log(`attempt ${transaction.attempt} started`);
      for (const range of transaction.conflictingKeyRanges) {
        yield* Console.log(
          `previous commit conflicted in [${hex(range.begin)}, ${
            hex(range.end)
          })`,
        );
      }
      yield* transaction.get(key);
      if (transaction.attempt === 1) {
        yield* database.set(key, bytes("sneaky_write"));
        yield* Console.log("injected a conflicting write");
      }
      yield* transaction.set(key, bytes("my_value"));
    }),
    { reportConflictingKeys: true },
  );

  const attempts = yield* Ref.get(observedAttempts);
  yield* Console.log(`committed after ${attempts} attempt(s)`);
});

await runMain(program);
