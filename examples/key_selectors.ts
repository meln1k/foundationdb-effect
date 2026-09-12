import { Console, Effect, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  KeySelector,
  type RangeOptions,
} from "../mod.ts";
import { assert, bytes, concatBytes, runMain, text } from "./_shared.ts";

const prefix = bytes("examples/key-selectors/");
const rangeEnd = concatBytes(prefix, Uint8Array.of(0xff));
const key = (value: string): Uint8Array => concatBytes(prefix, bytes(value));

const selectors = {
  llt: (value: string) => KeySelector.lastLessThan(key(value)),
  lloe: (value: string) => KeySelector.lastLessOrEqual(key(value)),
  fgt: (value: string) => KeySelector.firstGreaterThan(key(value)),
  fgoe: (value: string) => KeySelector.firstGreaterOrEqual(key(value)),
  custom: (value: string, orEqual: boolean, offset: number) =>
    new KeySelector({ key: key(value), orEqual, offset }),
};

interface Case {
  readonly description: string;
  readonly options: RangeOptions;
  readonly expected: ReadonlyArray<string>;
}

const cases: ReadonlyArray<Case> = [
  {
    description: 'FGOE("a") to FGT("h")',
    options: { begin: selectors.fgoe("a"), end: selectors.fgt("h") },
    expected: ["a", "b", "c", "d", "e", "f", "g", "h"],
  },
  {
    description: 'FGOE("b") to FGT("c")',
    options: { begin: selectors.fgoe("b"), end: selectors.fgt("c") },
    expected: ["b", "c"],
  },
  {
    description: 'FGOE("c") to FGOE("f")',
    options: { begin: selectors.fgoe("c"), end: selectors.fgoe("f") },
    expected: ["c", "d", "e"],
  },
  {
    description: 'FGT("b") to LLT("f")',
    options: { begin: selectors.fgt("b"), end: selectors.llt("f") },
    expected: ["c", "d"],
  },
  {
    description: "new(a,T,2) to new(g,F,0)",
    options: {
      begin: selectors.custom("a", true, 2),
      end: selectors.custom("g", false, 0),
    },
    // The Rust example lists f and g too, but (g, false, 0) resolves to
    // last_less_than(g) = f, and range ends are exclusive.
    expected: ["c", "d", "e"],
  },
  {
    description: "new(a,T,0) to new(g,F,-2)",
    options: {
      begin: selectors.custom("a", true, 0),
      end: selectors.custom("g", false, -2),
    },
    expected: ["a", "b", "c"],
  },
  {
    description: 'FGOE("d") to FGT("c")',
    options: { begin: selectors.fgoe("d"), end: selectors.fgt("c") },
    expected: [],
  },
  {
    description: 'LLOE("c") to FGT("f")',
    options: { begin: selectors.lloe("c"), end: selectors.fgt("f") },
    expected: ["c", "d", "e", "f"],
  },
  {
    description: 'LLT("c") to LLT("f")',
    options: { begin: selectors.llt("c"), end: selectors.llt("f") },
    expected: ["b", "c", "d"],
  },
  {
    description: 'LLT("c") to LLOE("f")',
    options: { begin: selectors.llt("c"), end: selectors.lloe("f") },
    expected: ["b", "c", "d", "e"],
  },
  {
    description: 'LLT("c") to FGT("f")',
    options: { begin: selectors.llt("c"), end: selectors.fgt("f") },
    expected: ["b", "c", "d", "e", "f"],
  },
  {
    description: 'LLT("c") to FGOE("f")',
    options: { begin: selectors.llt("c"), end: selectors.fgoe("f") },
    expected: ["b", "c", "d", "e"],
  },
  {
    description: 'LLOE("c") to LLT("f")',
    options: { begin: selectors.lloe("c"), end: selectors.llt("f") },
    expected: ["c", "d"],
  },
  {
    description: 'LLOE("c") to LLOE("f")',
    options: { begin: selectors.lloe("c"), end: selectors.lloe("f") },
    expected: ["c", "d", "e"],
  },
  {
    description: 'LLOE("c") to FGOE("f")',
    options: { begin: selectors.lloe("c"), end: selectors.fgoe("f") },
    expected: ["c", "d", "e"],
  },
  {
    description: 'FGT("c") to LLOE("f")',
    options: { begin: selectors.fgt("c"), end: selectors.lloe("f") },
    expected: ["d", "e"],
  },
  {
    description: 'FGT("c") to FGT("f")',
    options: { begin: selectors.fgt("c"), end: selectors.fgt("f") },
    expected: ["d", "e", "f"],
  },
  {
    description: 'FGT("c") to FGOE("f")',
    options: { begin: selectors.fgt("c"), end: selectors.fgoe("f") },
    expected: ["d", "e"],
  },
  {
    description: 'FGOE("c") to LLT("f")',
    options: { begin: selectors.fgoe("c"), end: selectors.llt("f") },
    expected: ["c", "d"],
  },
  {
    description: 'FGOE("c") to LLOE("f")',
    options: { begin: selectors.fgoe("c"), end: selectors.lloe("f") },
    expected: ["c", "d", "e"],
  },
];

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.clearRange(prefix, rangeEnd);
      for (let code = "a".charCodeAt(0); code <= "z".charCodeAt(0); code++) {
        const current = String.fromCharCode(code);
        yield* transaction.set(key(current), bytes(current));
      }
    }),
  );

  yield* Effect.forEach(cases, (testCase) =>
    Effect.gen(function* () {
      yield* Console.log(`Running: ${testCase.description}`);
      const actual = yield* database.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const rows = yield* Stream.runCollect(
            transaction.getRange(testCase.options),
          );
          return rows.map((row) => text(row.key.slice(prefix.length)));
        }),
      );
      assert(
        actual.length === testCase.expected.length &&
          actual.every((value, index) => value === testCase.expected[index]),
        `${testCase.description}: expected ${testCase.expected}, got ${actual}`,
      );
    }), { concurrency: 1, discard: true });

  yield* Console.log("All key selector examples ran successfully!");
});

await runMain(program);
