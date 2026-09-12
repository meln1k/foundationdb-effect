import { Console, Effect } from "effect";
import { FoundationDb, FoundationDbTransaction, Subspace } from "../mod.ts";
import {
  assert,
  littleEndianInt64,
  readLittleEndianInt64,
  runMain,
} from "./_shared.ts";

// The bridge currently selects one client API version (7.4) when its native
// layer starts. External-client-directory and per-client multi-version network
// configuration are not exposed yet, so this ports the database operation from
// the Rust example without claiming multi-version-client coverage.
const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const key = yield* Subspace.all().pack(["examples", "multi_version_incr"]);

  yield* database.withTransaction(
    Effect.flatMap(
      FoundationDbTransaction,
      (transaction) => transaction.atomicAdd(key, littleEndianInt64(1n)),
    ),
  );

  const stored = yield* database.get(key, { snapshot: true });
  assert(stored !== undefined, "counter was not found");
  const counter = readLittleEndianInt64(stored);
  yield* Console.log(`counter = ${counter}`);
  assert(counter > 0n, "counter must be positive");
});

await runMain(program);
