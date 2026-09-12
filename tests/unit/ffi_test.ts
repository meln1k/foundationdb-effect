import { assertEquals, assertThrows } from "@std/assert";
import { Effect, Fiber, Latch } from "effect";
import { ConflictRange, KeyValue } from "../../mod.ts";
import { internal } from "../../src/internal/ffi.ts";

const encode = (
  entries: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
): Uint8Array => {
  const length = entries.reduce(
    (total, [key, value]) => total + 8 + key.length + value.length,
    4,
  );
  const bytes = new Uint8Array(length);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, entries.length, true);
  let offset = 4;
  for (const [key, value] of entries) {
    view.setUint32(offset, key.length, true);
    view.setUint32(offset + 4, value.length, true);
    offset += 8;
    bytes.set(key, offset);
    offset += key.length;
    bytes.set(value, offset);
    offset += value.length;
  }
  return bytes;
};

const encodeI64 = (value: bigint): Uint8Array => {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigInt64(0, value, true);
  return bytes;
};

Deno.test("FFI shutdown keeps the callback alive through the native barrier", async () => {
  const events: Array<string> = [];
  const barrierEntered = Latch.makeUnsafe(false);
  const releaseBarrier = Latch.makeUnsafe(false);
  const fiber = Effect.runFork(Effect.scoped(internal.acquireDriverLifecycle(
    Effect.sync(() => events.push("initialize")),
    Effect.sync(() => events.push("barrier")).pipe(
      Effect.andThen(barrierEntered.open),
      Effect.andThen(releaseBarrier.await),
    ),
    Effect.succeed("driver"),
    () => Effect.sync(() => events.push("drain")),
    () => Effect.sync(() => events.push("close callback")),
  )));

  await Effect.runPromise(barrierEntered.await);
  assertEquals(events, ["initialize", "drain", "barrier"]);
  await Effect.runPromise(releaseBarrier.open);
  await Effect.runPromise(Fiber.join(fiber));
  assertEquals(events, [
    "initialize",
    "drain",
    "barrier",
    "close callback",
  ]);
});

Deno.test("native key batch encoder preserves binary and empty keys", () => {
  assertEquals(internal.encodeKeys([]), new Uint8Array([0, 0, 0, 0]));
  assertEquals(
    internal.encodeKeys([
      new Uint8Array([0, 255]),
      new Uint8Array(),
      new Uint8Array([3]),
    ]),
    new Uint8Array([
      3,
      0,
      0,
      0,
      2,
      0,
      0,
      0,
      0,
      255,
      0,
      0,
      0,
      0,
      1,
      0,
      0,
      0,
      3,
    ]),
  );
});

Deno.test("native value batch decoder distinguishes missing and empty values", () => {
  const encoded = new Uint8Array([
    3,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    255,
    0xff,
    0xff,
    0xff,
    0xff,
    0,
    0,
    0,
    0,
  ]);
  const decoded = internal.decodeOptionalValues(encoded);

  assertEquals(decoded, [
    new Uint8Array([0, 255]),
    undefined,
    new Uint8Array(),
  ]);
  encoded.fill(9);
  assertEquals(decoded, [
    new Uint8Array([0, 255]),
    undefined,
    new Uint8Array(),
  ]);
  assertEquals(
    internal.decodeOptionalValues(new Uint8Array([0, 0, 0, 0])),
    [],
  );
});

Deno.test("native value batch decoder rejects malformed payloads", () => {
  assertThrows(
    () => internal.decodeOptionalValues(new Uint8Array(3)),
    Error,
    "truncated",
  );
  assertThrows(
    () => internal.decodeOptionalValues(new Uint8Array([1, 0, 0, 0])),
    Error,
    "truncated",
  );
  assertThrows(
    () =>
      internal.decodeOptionalValues(
        new Uint8Array([1, 0, 0, 0, 2, 0, 0, 0, 1]),
      ),
    Error,
    "truncated",
  );
  assertThrows(
    () => internal.decodeOptionalValues(new Uint8Array([0, 0, 0, 0, 1])),
    Error,
    "trailing bytes",
  );
});

Deno.test("native signed 64-bit decoder preserves the full bigint range", () => {
  assertEquals(
    internal.decodeI64(encodeI64(9_007_199_254_740_993n)),
    9_007_199_254_740_993n,
  );
  assertEquals(internal.decodeI64(encodeI64(-1n)), -1n);
  assertThrows(
    () => internal.decodeI64(new Uint8Array(4)),
    Error,
    "invalid length",
  );
});

Deno.test("native conflict range decoder preserves binary and empty endpoints", () => {
  const encoded = encode([
    [new Uint8Array([0, 255]), new Uint8Array()],
    [new Uint8Array(), new Uint8Array([1, 2, 3])],
  ]);
  const decoded = internal.decodeConflictRanges(encoded);

  assertEquals(decoded, [
    new ConflictRange({
      begin: new Uint8Array([0, 255]),
      end: new Uint8Array(),
    }),
    new ConflictRange({
      begin: new Uint8Array(),
      end: new Uint8Array([1, 2, 3]),
    }),
  ]);
  encoded.fill(9);
  assertEquals(decoded[0]?.begin, new Uint8Array([0, 255]));
});

Deno.test("native conflict range decoder rejects malformed responses", () => {
  assertThrows(
    () => internal.decodeConflictRanges(new Uint8Array(3)),
    Error,
    "truncated",
  );
  assertThrows(
    () => internal.decodeConflictRanges(new Uint8Array([0, 0, 0, 0, 1])),
    Error,
    "trailing bytes",
  );
  assertThrows(
    () =>
      internal.decodeConflictRanges(
        new Uint8Array([1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0]),
      ),
    Error,
    "truncated",
  );
});

Deno.test("native range decoder copies binary keys and empty values", () => {
  const encoded = encode([
    [new Uint8Array([0, 255]), new Uint8Array()],
    [new Uint8Array([3]), new Uint8Array([4, 5])],
  ]);

  const decoded = internal.decodeKeyValues(encoded);
  assertEquals(decoded, [
    new KeyValue({ key: new Uint8Array([0, 255]), value: new Uint8Array() }),
    new KeyValue({
      key: new Uint8Array([3]),
      value: new Uint8Array([4, 5]),
    }),
  ]);

  encoded.fill(9);
  assertEquals(decoded[0]?.key, new Uint8Array([0, 255]));
  assertEquals(decoded[1]?.value, new Uint8Array([4, 5]));
  assertEquals(internal.decodeKeyValues(encode([])), []);
});

Deno.test("native range decoder rejects truncated and trailing payloads", () => {
  assertThrows(
    () => internal.decodeKeyValues(new Uint8Array([0, 0, 0])),
    Error,
    "truncated",
  );
  assertThrows(
    () => internal.decodeKeyValues(new Uint8Array([1, 0, 0, 0])),
    Error,
    "truncated",
  );
  assertThrows(
    () =>
      internal.decodeKeyValues(
        new Uint8Array([1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1]),
      ),
    Error,
    "truncated",
  );
  assertThrows(
    () => internal.decodeKeyValues(new Uint8Array([0, 0, 0, 0, 1])),
    Error,
    "trailing bytes",
  );
});
