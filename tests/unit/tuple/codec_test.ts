import {
  assert,
  assertEquals,
  assertFalse,
  assertNotEquals,
} from "@std/assert";
import { Effect, Equal, Schema } from "effect";
import {
  compare,
  Float32,
  pack,
  packWithVersionstamp,
  Subspace,
  TupleError,
  unpack,
  Uuid,
  Versionstamp,
} from "../../../src/tuple/mod.ts";
import type { Tuple, TupleValue } from "../../../src/tuple/mod.ts";

const bytes = (...values: number[]): Uint8Array => Uint8Array.from(values);
const run = <A>(effect: Effect.Effect<A, TupleError>): A =>
  Effect.runSync(effect);
const failure = <A>(effect: Effect.Effect<A, TupleError>): TupleError =>
  Effect.runSync(Effect.flip(effect));

Deno.test("pack matches canonical scalar and NUL escaping vectors", () => {
  assertEquals(run(pack([null, false, true])), bytes(0x00, 0x26, 0x27));
  assertEquals(
    run(pack([bytes(0x66, 0x00, 0x6f)])),
    bytes(0x01, 0x66, 0x00, 0xff, 0x6f, 0x00),
  );
  assertEquals(
    run(pack(["FÔO\0bar"])),
    bytes(
      0x02,
      0x46,
      0xc3,
      0x94,
      0x4f,
      0x00,
      0xff,
      0x62,
      0x61,
      0x72,
      0x00,
    ),
  );
});

Deno.test("text decoding preserves a leading Unicode BOM", () => {
  const value = "\uFEFFleading";
  assertEquals(run(unpack(run(pack([value])))), [value]);
});

Deno.test("pack matches canonical asymmetric integer boundaries", () => {
  const vectors: ReadonlyArray<readonly [bigint, Uint8Array]> = [
    [0n, bytes(0x14)],
    [1n, bytes(0x15, 0x01)],
    [255n, bytes(0x15, 0xff)],
    [256n, bytes(0x16, 0x01, 0x00)],
    [65_535n, bytes(0x16, 0xff, 0xff)],
    [65_536n, bytes(0x17, 0x01, 0x00, 0x00)],
    [16_777_216n, bytes(0x18, 0x01, 0x00, 0x00, 0x00)],
    [72_057_594_037_927_936n, bytes(0x1c, 0x01, 0, 0, 0, 0, 0, 0, 0)],
    [
      18_446_744_073_709_551_615n,
      bytes(0x1c, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
    ],
    [-1n, bytes(0x13, 0xfe)],
    [-255n, bytes(0x13, 0x00)],
    [-256n, bytes(0x12, 0xfe, 0xff)],
    [-65_535n, bytes(0x12, 0x00, 0x00)],
    [-65_536n, bytes(0x11, 0xfe, 0xff, 0xff)],
    [-16_777_216n, bytes(0x10, 0xfe, 0xff, 0xff, 0xff)],
    [
      -72_057_594_037_927_936n,
      bytes(0x0c, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
    ],
    [-18_446_744_073_709_551_615n, bytes(0x0c, 0, 0, 0, 0, 0, 0, 0, 0)],
  ];
  for (const [value, encoded] of vectors) {
    assertEquals(run(pack([value])), encoded);
    assertEquals(run(unpack(encoded)), [value]);
  }
});

Deno.test("arbitrary precision integers use canonical 9-to-255 byte forms", () => {
  const positive = 1n << 64n;
  const positiveBytes = bytes(0x1d, 0x09, 0x01, ...new Array(8).fill(0));
  const negativeBytes = bytes(0x0b, 0xf6, 0xfe, ...new Array(8).fill(0xff));
  assertEquals(run(pack([positive])), positiveBytes);
  assertEquals(run(pack([-positive])), negativeBytes);
  assertEquals(run(unpack(positiveBytes)), [positive]);
  assertEquals(run(unpack(negativeBytes)), [-positive]);

  const largest = 1n << (8n * 254n);
  assertEquals(run(pack([largest])).byteLength, 257);
  const tooLarge = 1n << (8n * 255n);
  assertEquals(failure(pack([tooLarge])).operation, "pack");
});

Deno.test("nested tuples escape only nested nulls and round trip recursively", () => {
  const tuple = [[bytes(0x66, 0x00, 0x6f), null, []], null] as const;
  const expected = bytes(
    0x05,
    0x01,
    0x66,
    0x00,
    0xff,
    0x6f,
    0x00,
    0x00,
    0xff,
    0x05,
    0x00,
    0x00,
    0x00,
  );
  assertEquals(run(pack(tuple)), expected);
  assertEquals(run(unpack(expected)), tuple);
  assert(Object.isFrozen(run(unpack(expected))));
  assert(Object.isFrozen(run(unpack(expected))[0]));
});

Deno.test("numbers are ordered binary64 values and preserve signed zero", () => {
  const vectors: ReadonlyArray<readonly [number, Uint8Array]> = [
    [-Infinity, bytes(0x21, 0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)],
    [-1, bytes(0x21, 0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)],
    [-0, bytes(0x21, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)],
    [0, bytes(0x21, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)],
    [1, bytes(0x21, 0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)],
    [Infinity, bytes(0x21, 0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)],
    [NaN, bytes(0x21, 0xff, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)],
  ];
  const encodings = vectors.map(([value, encoded]) => {
    assertEquals(run(pack([value])), encoded);
    const decoded = run(unpack(encoded))[0] as number;
    if (Number.isNaN(value)) {
      assert(Number.isNaN(decoded));
    } else {
      assert(Object.is(decoded, value));
    }
    return encoded;
  });
  for (let index = 1; index < encodings.length; index++) {
    assert(lexicographic(encodings[index - 1], encodings[index]) < 0);
  }

  // The tuple layer preserves arbitrary NaN payloads rather than canonicalizing.
  assert(Number.isNaN(
    run(unpack(
      bytes(0x21, 0xff, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
    ))[0] as number,
  ));
});

Deno.test("float32 has its own type band and preserves raw IEEE bits", () => {
  const vectors: ReadonlyArray<readonly [number, Uint8Array]> = [
    [-Infinity, bytes(0x20, 0x00, 0x7f, 0xff, 0xff)],
    [-42, bytes(0x20, 0x3d, 0xd7, 0xff, 0xff)],
    [-1, bytes(0x20, 0x40, 0x7f, 0xff, 0xff)],
    [-0, bytes(0x20, 0x7f, 0xff, 0xff, 0xff)],
    [0, bytes(0x20, 0x80, 0x00, 0x00, 0x00)],
    [1, bytes(0x20, 0xbf, 0x80, 0x00, 0x00)],
    [Infinity, bytes(0x20, 0xff, 0x80, 0x00, 0x00)],
  ];
  for (const [value, encoded] of vectors) {
    assertEquals(run(pack([Float32.fromNumber(value)])), encoded);
    const decoded = run(unpack(encoded))[0];
    assert(decoded instanceof Float32);
    assert(Object.is(decoded.value, value));
  }

  const nan = run(Float32.fromBits(0x7fc0_0001));
  const encoded = bytes(0x20, 0xff, 0xc0, 0x00, 0x01);
  assertEquals(run(pack([nan])), encoded);
  const decoded = run(unpack(encoded))[0];
  assert(decoded instanceof Float32);
  assertEquals(decoded.bits, 0x7fc0_0001);
  assert(Number.isNaN(decoded.value));
  assert(decoded.equals(nan));
  assert(Equal.equals(decoded, nan));
  const negativeNan = run(Float32.fromBits(0xffc0_0001));
  assertEquals(
    run(pack([negativeNan])),
    bytes(0x20, 0x00, 0x3f, 0xff, 0xfe),
  );
  assertEquals(failure(Float32.fromBits(-1)).operation, "pack");
  assertEquals(run(compare([Float32.fromNumber(Infinity)], [-Infinity])), -1);
});

Deno.test("binary64 decoding and repacking preserves NaN payload bits", () => {
  const encodings = [
    bytes(0x21, 0xff, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
    bytes(0x21, 0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe),
    bytes(0x21, 0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
  ];
  for (const encoded of encodings) {
    assertEquals(run(pack(run(unpack(encoded)))), encoded);
  }
});

Deno.test("UUID values match the foundationdb-rs wire representation", () => {
  const raw = bytes(
    0xba,
    0xff,
    0xff,
    0xff,
    0xff,
    0x5e,
    0xba,
    0x11,
    0x00,
    0x00,
    0x00,
    0x00,
    0x5c,
    0xa1,
    0xab,
    0x1e,
  );
  const expectedRaw = raw.slice();
  const uuid = run(Uuid.fromBytes(raw));
  raw.fill(0);
  const encoded = bytes(0x30, ...expectedRaw);
  assertEquals(run(pack([uuid])), encoded);
  const decoded = run(unpack(encoded))[0];
  assert(decoded instanceof Uuid);
  assertEquals(decoded.bytes, expectedRaw);
  assertEquals(decoded.toString(), "baffffff-ff5e-ba11-0000-00005ca1ab1e");
  assert(decoded.equals(run(Uuid.fromString(decoded.toString()))));
  assert(Equal.equals(decoded, run(Uuid.fromString(decoded.toString()))));
  assert(
    decoded.equals(run(Uuid.fromString("baffffffff5eba11000000005ca1ab1e"))),
  );
  assertEquals(failure(Uuid.fromBytes(bytes(1))).operation, "pack");
  assertEquals(
    failure(Uuid.fromString("baff-ffffff5eba11000000005ca1ab1e")).operation,
    "pack",
  );
});

Deno.test("versionstamps preserve bytes and append canonical offsets", () => {
  const transactionVersion = bytes(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  const complete = run(Versionstamp.complete(transactionVersion, 657));
  transactionVersion.fill(0xff);
  const expectedTransactionVersion = bytes(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  assert(complete.isComplete);
  assertEquals(complete.transactionVersion, expectedTransactionVersion);
  assertEquals(complete.userVersion, 657);
  assertEquals(
    run(pack([complete])),
    bytes(0x33, ...expectedTransactionVersion, 0x02, 0x91),
  );
  assert(complete.equals(run(Versionstamp.fromBytes(complete.bytes))));
  assert(Equal.equals(complete, run(Versionstamp.fromBytes(complete.bytes))));
  assertEquals(failure(Versionstamp.fromBytes(bytes(1))).operation, "pack");
  assertEquals(
    failure(Versionstamp.complete(expectedTransactionVersion, 0x1_0000))
      .operation,
    "pack",
  );
  assertEquals(failure(Versionstamp.incomplete(-1)).operation, "pack");
  assertFalse(
    run(Versionstamp.complete(new Uint8Array(10).fill(0xff), 0)).isComplete,
  );

  const incomplete = run(Versionstamp.incomplete(0));
  assertFalse(incomplete.isComplete);
  const raw = bytes(0x33, ...new Array(10).fill(0xff), 0, 0);
  assertEquals(run(pack([incomplete])), raw);
  assertEquals(
    run(packWithVersionstamp([incomplete])),
    bytes(...raw, 1, 0, 0, 0),
  );
  assertEquals(
    run(packWithVersionstamp(["foo", incomplete])),
    bytes(0x02, 0x66, 0x6f, 0x6f, 0, ...raw, 6, 0, 0, 0),
  );
  assertEquals(
    Array.from(run(packWithVersionstamp(["foo", [incomplete]])).slice(-4)),
    [7, 0, 0, 0],
  );
  assertEquals(
    Array.from(
      run(packWithVersionstamp(["foo", [null, incomplete]])).slice(-4),
    ),
    [9, 0, 0, 0],
  );
  assertEquals(run(packWithVersionstamp([complete])), run(pack([complete])));
  assertEquals(
    run(pack([incomplete, incomplete])).byteLength,
    26,
  );
  assertEquals(
    failure(packWithVersionstamp([incomplete, incomplete])).reason,
    "tuple contains multiple incomplete versionstamps",
  );
});

Deno.test("Subspace tracks incomplete versionstamps through child prefixes", () => {
  const incomplete = run(Versionstamp.incomplete(4));
  const root = run(Subspace.fromTuple(["root", incomplete]));
  const child = run(root.subspace(["child"]));
  const packed = run(child.packWithVersionstamp([1n]));
  assertEquals(Array.from(packed.slice(-4)), [7, 0, 0, 0]);
  assertEquals(
    failure(child.packWithVersionstamp([incomplete])).reason,
    "tuple contains multiple incomplete versionstamps",
  );

  const raw = Subspace.fromBytes(bytes(0xaa));
  assertEquals(
    run(raw.packWithVersionstamp([incomplete])).slice(-4),
    bytes(2, 0, 0, 0),
  );
  assert(root.equals(run(Subspace.fromTuple(["root", incomplete]))));
  assert(Equal.equals(root, run(Subspace.fromTuple(["root", incomplete]))));
  assertFalse(root.equals(Subspace.fromBytes(root.bytes)));
  assert(Subspace.all().equals(new Subspace()));
  assert(Subspace.all().isStartOf(bytes(1, 2, 3)));
  assertEquals(run(Subspace.all().range()), [bytes(0), bytes(0xff)]);
  assertEquals(
    new Subspace(bytes(0, 0x20, 0x5c, 0xff)).toString(),
    String.raw`\x00 \\\xff`,
  );
});

Deno.test("tuple comparison follows cross-type and prefix ordering", () => {
  const uuid = run(Uuid.fromBytes(new Uint8Array(16)));
  const versionstamp = run(Versionstamp.complete(new Uint8Array(10), 0));
  const ordered: ReadonlyArray<Tuple> = [
    [null],
    [bytes()],
    [""],
    [[]],
    [-1n],
    [0n],
    [1n],
    [Float32.fromNumber(0)],
    [0],
    [false],
    [true],
    [uuid],
    [versionstamp],
  ];
  for (let index = 1; index < ordered.length; index++) {
    assertEquals(run(compare(ordered[index - 1], ordered[index])), -1);
  }
  assertEquals(run(compare(["prefix"], ["prefix", 0n])), -1);
  assertEquals(run(compare(["same"], ["same"])), 0);
});

Deno.test("unpack rejects malformed and truncated encodings", () => {
  const invalid = [
    bytes(0x01, 0x61),
    bytes(0x02, 0xc3, 0x28, 0x00),
    bytes(0x05, 0x27),
    bytes(0x21, 0x80),
    bytes(0x20, 0x80, 0x00, 0x00),
    bytes(0x30, ...new Array(15).fill(0)),
    bytes(0x33, ...new Array(11).fill(0)),
    bytes(0xff),
  ];
  for (const encoded of invalid) {
    const error = failure(unpack(encoded));
    assert(error instanceof Error);
    assert(Schema.is(TupleError)(error));
    assertEquals(error.operation, "unpack");
  }

  const unsupported = failure(pack([undefined as unknown as TupleValue]));
  assert(Schema.is(TupleError)(unsupported));
  assertEquals(unsupported.operation, "pack");
  assertEquals(failure(pack(["\ud800"])).operation, "pack");
});

Deno.test("schema-directed unpacking validates tuple values with typed errors", () => {
  const User = Schema.Tuple([Schema.String, Schema.BigInt]);
  const encoded = run(pack(["alice", 42n]));

  const user = Effect.runSync(unpack(encoded, User));
  assertEquals(user, ["alice", 42n]);

  const mismatch = Effect.runSync(Effect.flip(
    unpack(run(pack(["alice", "42"])), User),
  ));
  assert(Schema.isSchemaError(mismatch));

  const malformed = Effect.runSync(Effect.flip(
    unpack(bytes(0x02, 0xc3, 0x28, 0x00), User),
  ));
  assert(malformed instanceof TupleError);
});

Deno.test("unpack accepts non-canonical integer widths like foundationdb-rs", () => {
  assertEquals(run(unpack(bytes(0x15, 0x00))), [0n]);
  assertEquals(run(unpack(bytes(0x16, 0x00, 0xff))), [255n]);
  assertEquals(run(unpack(bytes(0x13, 0xff))), [0n]);
  assertEquals(run(unpack(bytes(0x1d, 0x00))), [0n]);
  assertEquals(run(unpack(bytes(0x0b, 0xff))), [0n]);
  assertEquals(
    run(unpack(bytes(0x1d, 0x08, 0x01, 0, 0, 0, 0, 0, 0, 0))),
    [72_057_594_037_927_936n],
  );
  assertEquals(
    run(
      unpack(bytes(0x0b, 0xf7, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)),
    ),
    [-72_057_594_037_927_936n],
  );
});

Deno.test("pack and unpack own binary data defensively", () => {
  const source = bytes(1, 2, 3);
  const encoded = run(pack([source]));
  source.fill(9);
  assertEquals(encoded, bytes(0x01, 1, 2, 3, 0));

  const decoded = run(unpack(encoded));
  encoded.fill(8);
  assertEquals(decoded, [bytes(1, 2, 3)]);
});

Deno.test("Subspace copies prefixes and supports pack/unpack/contains/range/child", () => {
  const original = bytes(0xaa);
  const root = new Subspace(original);
  original[0] = 0xbb;
  const exposed = root.prefix;
  exposed[0] = 0xcc;

  const key = run(root.pack(["path", -1n]));
  assertEquals(key, bytes(0xaa, 0x02, 0x70, 0x61, 0x74, 0x68, 0, 0x13, 0xfe));
  assert(root.contains(key));
  assertFalse(root.contains(bytes(0xab, 0x14)));
  assertEquals(run(root.unpack(key)), ["path", -1n]);
  assertEquals(
    Effect.runSync(root.unpack(
      key,
      Schema.Tuple([Schema.String, Schema.BigInt]),
    )),
    ["path", -1n],
  );
  assertEquals(
    failure(root.unpack(bytes(0xab))).reason,
    "key is outside the subspace",
  );
  const outsideSchemaError = Effect.runSync(Effect.flip(
    root.unpack(bytes(0xab), Schema.Tuple([Schema.String])),
  ));
  assert(outsideSchemaError instanceof TupleError);

  const child = run(root.subspace([bytes(0x00), "layer"]));
  assertEquals(
    child.prefix,
    bytes(0xaa, 0x01, 0x00, 0xff, 0x00, 0x02, 0x6c, 0x61, 0x79, 0x65, 0x72, 0),
  );
  const [begin, end] = run(child.range([7n]));
  const packedPrefix = run(child.pack([7n]));
  assertEquals(begin, bytes(...packedPrefix, 0x00));
  assertEquals(end, bytes(...packedPrefix, 0xff));
  assertNotEquals(begin, end);
});

Deno.test("mixed tuples round trip with integer and float types preserved", () => {
  const tuple: Tuple = [
    null,
    true,
    bytes(0, 1, 255),
    "커피",
    -(1n << 100n),
    -123.5,
    ["nested", null, 42n],
  ];
  assertEquals(run(unpack(run(pack(tuple)))), tuple);
});

const lexicographic = (left: Uint8Array, right: Uint8Array): number => {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index++) {
    if (left[index] !== right[index]) {
      return left[index] - right[index];
    }
  }
  return left.length - right.length;
};
