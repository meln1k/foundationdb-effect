import { Clock, Console, Effect, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  KeySelector,
  StreamingMode,
} from "../mod.ts";
import { bytes, concatBytes, runMain } from "./_shared.ts";

const prefix = bytes("budgeted_scan/");
const end = bytes("budgeted_scan0");
const rows = 4_000;
const valueSize = 1_024;
const rowsPerSetupTransaction = 500;

type Budget =
  | { readonly _tag: "time"; readonly milliseconds: number }
  | { readonly _tag: "bytes"; readonly maximum: number };

interface Page {
  readonly rows: number;
  readonly lastKey: Uint8Array | undefined;
  readonly complete: boolean;
  readonly bytes: number;
}

const keyOf = (index: number): Uint8Array =>
  concatBytes(prefix, bytes(index.toString().padStart(8, "0")));

const scanPage = Effect.fn("example.scanPage")(function* (
  continuation: Uint8Array | undefined,
  budget: Budget,
) {
  const transaction = yield* FoundationDbTransaction;
  const started = yield* Clock.currentTimeMillis;
  let count = 0;
  let usedBytes = 0;
  let lastKey: Uint8Array | undefined;
  let stoppedByBudget = false;

  yield* Stream.runForEachWhile(
    transaction.getRange({
      begin: continuation === undefined
        ? KeySelector.firstGreaterOrEqual(prefix)
        : KeySelector.firstGreaterThan(continuation),
      end: KeySelector.firstGreaterOrEqual(end),
      mode: StreamingMode.Serial,
      targetBytes: 1_024 * 1_024,
    }),
    (row) =>
      Effect.gen(function* () {
        const nextBytes = usedBytes + row.key.byteLength + row.value.byteLength;
        const elapsed = (yield* Clock.currentTimeMillis) - started;
        if (
          (budget._tag === "bytes" && nextBytes > budget.maximum) ||
          (budget._tag === "time" && elapsed > budget.milliseconds)
        ) {
          stoppedByBudget = true;
          return false;
        }
        count++;
        usedBytes = nextBytes;
        lastKey = row.key;
        return true;
      }),
  );

  return {
    rows: count,
    lastKey,
    complete: !stoppedByBudget,
    bytes: usedBytes,
  } satisfies Page;
});

const scanAll = Effect.fn("example.scanAll")(function* (budget: Budget) {
  const database = yield* FoundationDb;
  let continuation: Uint8Array | undefined;
  let total = 0;
  let transactions = 0;
  while (true) {
    const page = yield* database.withTransaction(
      scanPage(continuation, budget),
    );
    transactions++;
    total += page.rows;
    yield* Console.log(
      `processed ${page.rows} rows (${page.bytes} bytes); total ${total}`,
    );
    if (page.complete) {
      return { rows: total, transactions } as const;
    }
    if (page.lastKey === undefined) {
      return { rows: total, transactions } as const;
    }
    continuation = page.lastKey;
  }
});

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const value = new Uint8Array(valueSize).fill("x".charCodeAt(0));
  yield* database.clearRange(prefix, end);
  yield* Effect.forEach(
    Array.from(
      { length: Math.ceil(rows / rowsPerSetupTransaction) },
      (_, index) => index * rowsPerSetupTransaction,
    ),
    (start) =>
      database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          for (
            let index = start;
            index < start + rowsPerSetupTransaction;
            index++
          ) {
            yield* transaction.set(keyOf(index), value);
          }
        }),
      ),
    { concurrency: 1, discard: true },
  );
  yield* Console.log(`${rows} rows written`);

  const timed = yield* scanAll({ _tag: "time", milliseconds: 2_500 });
  yield* Console.log(
    `time-budgeted scan: ${timed.rows} rows in ${timed.transactions} transaction(s)`,
  );
  const byteLimited = yield* scanAll({ _tag: "bytes", maximum: 512 * 1_024 });
  yield* Console.log(
    `byte-budgeted scan: ${byteLimited.rows} rows in ${byteLimited.transactions} transaction(s)`,
  );

  yield* database.clearRange(prefix, end);
});

await runMain(program);
