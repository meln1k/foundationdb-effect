import { Console, Effect, Ref } from "effect";
import { FoundationDb, FoundationDbTransaction } from "../mod.ts";
import { bytes, runMain } from "./_shared.ts";

// The wrapper exposes retry-attempt context, but not the Rust binding's
// conflict-range introspection or lifecycle-hook report. This still creates a
// real 1020 conflict and makes the retry visible in ordinary Effect code.
const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const key = bytes("example_conflict_key");
  const observedAttempts = yield* Ref.make(0);

  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* Ref.set(observedAttempts, transaction.attempt);
      yield* Console.log(`attempt ${transaction.attempt} started`);
      yield* transaction.get(key);
      if (transaction.attempt === 1) {
        yield* database.set(key, bytes("sneaky_write"));
        yield* Console.log("injected a conflicting write");
      }
      yield* transaction.set(key, bytes("my_value"));
    }),
  );

  const attempts = yield* Ref.get(observedAttempts);
  yield* Console.log(`committed after ${attempts} attempt(s)`);
});

await runMain(program);
