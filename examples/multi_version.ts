import { Console, Effect } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  MutationType,
  Subspace,
} from "../mod.ts";
import {
  assert,
  littleEndianInt64,
  readLittleEndianInt64,
  runMain,
} from "./_shared.ts";

// The bridge selects one client API version (7.4) when its native layer starts.
// External-client-directory and per-client multi-version network configuration
// are not exposed, but transactions can exchange explicit read versions.
const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const key = yield* Subspace.all().pack(["examples", "multi_version_incr"]);

  const readVersion = yield* database.withTransaction(
    Effect.flatMap(
      FoundationDbTransaction,
      (transaction) => transaction.getReadVersion(),
    ),
  );

  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.setReadVersion(readVersion);
      yield* transaction.atomicOp(
        key,
        littleEndianInt64(1n),
        MutationType.Add,
      );
    }),
  );

  const stored = yield* database.get(key, { snapshot: true });
  assert(stored !== undefined, "counter was not found");
  const counter = readLittleEndianInt64(stored);
  yield* Console.log(`counter = ${counter}`);
  assert(counter > 0n, "counter must be positive");
});

await runMain(program);
