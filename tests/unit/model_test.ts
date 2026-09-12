import { assert, assertEquals, assertFalse } from "@std/assert";
import { Schema } from "effect";
import {
  FoundationDbError,
  isFoundationDbError,
  keyRange,
  KeySelector,
  StreamingMode,
} from "../../mod.ts";

const bytes = (...values: number[]): Uint8Array => new Uint8Array(values);

Deno.test("KeySelector constructors encode FoundationDB selector semantics", () => {
  const key = bytes(1, 2);

  const selectors = [
    KeySelector.lastLessThan(key),
    KeySelector.lastLessOrEqual(key),
    KeySelector.firstGreaterThan(key),
    KeySelector.firstGreaterOrEqual(key),
  ];

  assertEquals(
    selectors.map(({ key, orEqual, offset }) => ({ key, orEqual, offset })),
    [
      { key, orEqual: false, offset: 0 },
      { key, orEqual: true, offset: 0 },
      { key, orEqual: true, offset: 1 },
      { key, orEqual: false, offset: 1 },
    ],
  );
});

Deno.test("keyRange uses inclusive begin and exclusive end selectors", () => {
  const begin = bytes(0x10);
  const end = bytes(0x20);
  const range = keyRange(begin, end, {
    limit: 7,
    reverse: true,
    mode: StreamingMode.Small,
  });

  assertEquals(range, {
    begin: KeySelector.firstGreaterOrEqual(begin),
    end: KeySelector.firstGreaterOrEqual(end),
    limit: 7,
    reverse: true,
    mode: StreamingMode.Small,
  });
});

Deno.test("FoundationDbError is a schema-backed tagged error", () => {
  const error = new FoundationDbError({
    operation: "Transaction.get",
    code: 1007,
    message: "transaction_too_old",
    retryable: true,
    maybeCommitted: false,
    retryableNotCommitted: true,
  });

  assert(error instanceof Error);
  assert(isFoundationDbError(error));
  assert(Schema.is(FoundationDbError)(error));
  assertFalse(isFoundationDbError({ ...error, code: 1.5 }));
  assertFalse(isFoundationDbError(new Error("not an FDB error")));
});
