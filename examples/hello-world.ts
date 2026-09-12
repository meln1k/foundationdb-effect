import { Console, Effect } from "effect";
import { FoundationDb } from "../mod.ts";
import { assertBytesEqual, bytes, runMain } from "./_shared.ts";

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const key = bytes("hello");
  const value = bytes("world");

  yield* database.set(key, value);
  yield* Console.log("transaction committed");

  const stored = yield* database.get(key);
  assertBytesEqual(stored ?? new Uint8Array(), value);
});

await runMain(program);
