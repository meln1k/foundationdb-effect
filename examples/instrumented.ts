import { Clock, Console, Effect, Metric } from "effect";
import { FoundationDb, FoundationDbTransaction } from "../mod.ts";
import { bytes, runMain } from "./_shared.ts";

const calls = Metric.counter("foundationdb_example_operations", {
  incremental: true,
});
const bytesRead = Metric.counter("foundationdb_example_bytes_read", {
  incremental: true,
});
const bytesWritten = Metric.counter("foundationdb_example_bytes_written", {
  incremental: true,
});

// These are Effect spans and metrics around the canonical transaction service.
// Native FDB usage accounting and TransactionMetrics are not exposed by the
// bridge, so the values intentionally describe application-visible operations.
const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const key = bytes("instrumented_key");
  const value = bytes("instrumented_value");
  const started = yield* Clock.currentTimeNanos;

  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.set(key, value);
      yield* Metric.update(calls, 1);
      yield* Metric.update(bytesWritten, key.byteLength + value.byteLength);
      const stored = yield* transaction.get(key);
      yield* Metric.update(calls, 1);
      yield* Metric.update(
        bytesRead,
        key.byteLength + (stored?.byteLength ?? 0),
      );
    }).pipe(Effect.withSpan("foundationdb.transaction")),
  );

  const elapsed = yield* Clock.currentTimeNanos;
  const callState = yield* Metric.value(calls);
  const readState = yield* Metric.value(bytesRead);
  const writeState = yield* Metric.value(bytesWritten);
  yield* Console.log({
    operations: callState.count,
    bytesRead: readState.count,
    bytesWritten: writeState.count,
    elapsedMilliseconds: Number(elapsed - started) / 1_000_000,
  });
});

await runMain(program);
