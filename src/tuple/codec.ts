import { Effect, Schema } from "effect";
import { compareBytes, concatBytes } from "./bytes.ts";
import {
  Float32,
  tupleError,
  tupleErrorFromUnknown,
  Uuid,
  Versionstamp,
} from "./values.ts";
import type { Tuple, TupleError, TupleValue } from "./values.ts";

const NIL = 0x00;
const BYTES = 0x01;
const STRING = 0x02;
const NESTED = 0x05;
const NEGATIVE_BIG_INTEGER = 0x0b;
const INTEGER_ZERO = 0x14;
const POSITIVE_BIG_INTEGER = 0x1d;
const FLOAT = 0x20;
const DOUBLE = 0x21;
const FALSE = 0x26;
const TRUE = 0x27;
const UUID = 0x30;
const VERSIONSTAMP = 0x33;
const ESCAPE = 0xff;
const MAX_SMALL_INTEGER_BYTES = 8;
const MAX_BIG_INTEGER_BYTES = 255;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

const appendEscaped = (output: Array<number>, value: Uint8Array): void => {
  for (const byte of value) {
    output.push(byte);
    if (byte === NIL) {
      output.push(ESCAPE);
    }
  }
  output.push(NIL);
};

const assertWellFormedString = (value: string): void => {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (index + 1 >= value.length || next < 0xdc00 || next > 0xdfff) {
        throw tupleError(
          "pack",
          "string contains an unpaired UTF-16 surrogate",
        );
      }
      index++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw tupleError("pack", "string contains an unpaired UTF-16 surrogate");
    }
  }
};

const magnitudeBytes = (value: bigint): Array<number> => {
  const bytes: Array<number> = [];
  for (let remaining = value; remaining !== 0n; remaining >>= 8n) {
    bytes.push(Number(remaining & 0xffn));
  }
  bytes.reverse();
  return bytes;
};

const appendInteger = (output: Array<number>, value: bigint): void => {
  if (value === 0n) {
    output.push(INTEGER_ZERO);
    return;
  }

  const negative = value < 0n;
  const bytes = magnitudeBytes(negative ? -value : value);
  if (bytes.length > MAX_BIG_INTEGER_BYTES) {
    throw tupleError(
      "pack",
      "integer magnitude requires more than 255 bytes",
    );
  }

  if (bytes.length <= MAX_SMALL_INTEGER_BYTES) {
    output.push(INTEGER_ZERO + (negative ? -bytes.length : bytes.length));
  } else {
    output.push(
      negative ? NEGATIVE_BIG_INTEGER : POSITIVE_BIG_INTEGER,
      negative ? bytes.length ^ 0xff : bytes.length,
    );
  }
  output.push(...(negative ? bytes.map((byte) => byte ^ 0xff) : bytes));
};

const appendOrderedFloat = (
  output: Array<number>,
  code: number,
  encoded: Uint8Array,
): void => {
  if ((encoded[0] & 0x80) !== 0) {
    for (let index = 0; index < encoded.length; index++) {
      encoded[index] ^= 0xff;
    }
  } else {
    encoded[0] ^= 0x80;
  }
  output.push(code, ...encoded);
};

const appendFloat = (output: Array<number>, value: Float32): void => {
  const encoded = new Uint8Array(4);
  new DataView(encoded.buffer).setUint32(0, value.bits, false);
  appendOrderedFloat(output, FLOAT, encoded);
};

const appendDouble = (output: Array<number>, value: number): void => {
  const encoded = new Uint8Array(8);
  new DataView(encoded.buffer).setFloat64(0, value, false);
  appendOrderedFloat(output, DOUBLE, encoded);
};

const appendValue = (
  output: Array<number>,
  value: unknown,
  nested: boolean,
  incompleteVersionstamps: Array<number>,
): void => {
  if (value === null) {
    output.push(NIL);
    if (nested) {
      output.push(ESCAPE);
    }
  } else if (value instanceof Uint8Array) {
    output.push(BYTES);
    appendEscaped(output, value);
  } else if (typeof value === "string") {
    assertWellFormedString(value);
    output.push(STRING);
    appendEscaped(output, textEncoder.encode(value));
  } else if (typeof value === "bigint") {
    appendInteger(output, value);
  } else if (value instanceof Float32) {
    appendFloat(output, value);
  } else if (typeof value === "number") {
    appendDouble(output, value);
  } else if (typeof value === "boolean") {
    output.push(value ? TRUE : FALSE);
  } else if (value instanceof Uuid) {
    output.push(UUID, ...value.bytes);
  } else if (value instanceof Versionstamp) {
    output.push(VERSIONSTAMP);
    if (!value.isComplete) {
      incompleteVersionstamps.push(output.length);
    }
    output.push(...value.bytes);
  } else if (Array.isArray(value)) {
    output.push(NESTED);
    for (const child of value) {
      appendValue(output, child, true, incompleteVersionstamps);
    }
    output.push(NIL);
  } else {
    throw tupleError(
      "pack",
      `unsupported tuple value: ${Object.prototype.toString.call(value)}`,
    );
  }
};

export interface PackedTuple {
  readonly bytes: Uint8Array;
  readonly incompleteVersionstamps: ReadonlyArray<number>;
}

export const packTrackedSync = (tuple: Tuple): PackedTuple => {
  if (!Array.isArray(tuple)) {
    throw tupleError("pack", "tuple must be a readonly array");
  }
  const output: Array<number> = [];
  const incompleteVersionstamps: Array<number> = [];
  for (const value of tuple) {
    appendValue(output, value, false, incompleteVersionstamps);
  }
  return {
    bytes: Uint8Array.from(output),
    incompleteVersionstamps: Object.freeze(incompleteVersionstamps),
  };
};

const packSync = (tuple: Tuple): Uint8Array => packTrackedSync(tuple).bytes;

export const appendVersionstampOffset = (
  packed: PackedTuple,
): Uint8Array => {
  if (packed.incompleteVersionstamps.length === 0) {
    return packed.bytes;
  }
  if (packed.incompleteVersionstamps.length > 1) {
    throw tupleError(
      "pack",
      "tuple contains multiple incomplete versionstamps",
    );
  }
  const offset = packed.incompleteVersionstamps[0];
  if (offset > 0xffff_ffff) {
    throw tupleError("pack", "versionstamp offset exceeds 32 bits");
  }
  const trailer = new Uint8Array(4);
  new DataView(trailer.buffer).setUint32(0, offset, true);
  return concatBytes(packed.bytes, trailer);
};

/** Packs a tuple using the canonical FoundationDB tuple encoding. */
export const pack = (tuple: Tuple): Effect.Effect<Uint8Array, TupleError> =>
  Effect.try({
    try: () => packSync(tuple),
    catch: (cause) => tupleErrorFromUnknown("pack", cause),
  });

/** Packs a tuple and appends the offset of its one incomplete versionstamp. */
export const packWithVersionstamp = (
  tuple: Tuple,
): Effect.Effect<Uint8Array, TupleError> =>
  Effect.try({
    try: () => appendVersionstampOffset(packTrackedSync(tuple)),
    catch: (cause) => tupleErrorFromUnknown("pack", cause),
  });

/** Compares tuples by FoundationDB's unsigned lexicographic wire ordering. */
export const compare = (
  left: Tuple,
  right: Tuple,
): Effect.Effect<number, TupleError> =>
  Effect.all([pack(left), pack(right)]).pipe(
    Effect.map(([leftBytes, rightBytes]) =>
      compareBytes(leftBytes, rightBytes)
    ),
  );

class Parser {
  readonly #bytes: Uint8Array;
  #offset = 0;

  constructor(bytes: Uint8Array) {
    this.#bytes = bytes;
  }

  get done(): boolean {
    return this.#offset === this.#bytes.byteLength;
  }

  #fail(reason: string): never {
    throw tupleError("unpack", `${reason} at byte ${this.#offset}`);
  }

  #readByte(): number {
    if (this.done) {
      return this.#fail("truncated tuple");
    }
    return this.#bytes[this.#offset++];
  }

  #read(length: number): Uint8Array {
    if (this.#offset + length > this.#bytes.byteLength) {
      return this.#fail("truncated tuple");
    }
    const value = this.#bytes.slice(this.#offset, this.#offset + length);
    this.#offset += length;
    return value;
  }

  #readEscaped(): Uint8Array {
    const value: Array<number> = [];
    while (!this.done) {
      const byte = this.#readByte();
      if (byte !== NIL) {
        value.push(byte);
      } else if (!this.done && this.#bytes[this.#offset] === ESCAPE) {
        this.#offset++;
        value.push(NIL);
      } else {
        return Uint8Array.from(value);
      }
    }
    return this.#fail("unterminated byte or string value");
  }

  #readInteger(code: number): bigint {
    const negative = code < INTEGER_ZERO;
    let length: number;
    if (code === NEGATIVE_BIG_INTEGER) {
      length = this.#readByte() ^ 0xff;
    } else if (code === POSITIVE_BIG_INTEGER) {
      length = this.#readByte();
    } else {
      length = Math.abs(code - INTEGER_ZERO);
    }

    const encoded = this.#read(length);
    let magnitude = 0n;
    for (const byte of encoded) {
      magnitude = (magnitude << 8n) |
        BigInt(negative ? byte ^ 0xff : byte);
    }
    return negative ? -magnitude : magnitude;
  }

  #readOrderedFloat(length: number): Uint8Array {
    const encoded = this.#read(length);
    if ((encoded[0] & 0x80) !== 0) {
      encoded[0] ^= 0x80;
    } else {
      for (let index = 0; index < encoded.length; index++) {
        encoded[index] ^= 0xff;
      }
    }
    return encoded;
  }

  #readFloat(): Float32 {
    const encoded = this.#readOrderedFloat(4);
    return Float32.fromBitsUnsafe(new DataView(
      encoded.buffer,
      encoded.byteOffset,
      encoded.byteLength,
    ).getUint32(0, false));
  }

  #readDouble(): number {
    const encoded = this.#readOrderedFloat(8);
    return new DataView(
      encoded.buffer,
      encoded.byteOffset,
      encoded.byteLength,
    ).getFloat64(0, false);
  }

  #readValue(code: number): TupleValue {
    switch (code) {
      case NIL:
        return null;
      case BYTES:
        return this.#readEscaped();
      case STRING: {
        const encoded = this.#readEscaped();
        try {
          return textDecoder.decode(encoded);
        } catch {
          return this.#fail("string is not valid UTF-8");
        }
      }
      case NESTED:
        return this.#readTuple(true);
      case INTEGER_ZERO:
        return 0n;
      case FLOAT:
        return this.#readFloat();
      case DOUBLE:
        return this.#readDouble();
      case FALSE:
        return false;
      case TRUE:
        return true;
      case UUID:
        return Uuid.fromBytesUnsafe(this.#read(16));
      case VERSIONSTAMP:
        return Versionstamp.fromBytesUnsafe(this.#read(12));
      default:
        if (
          (code >= INTEGER_ZERO - MAX_SMALL_INTEGER_BYTES &&
            code <= INTEGER_ZERO + MAX_SMALL_INTEGER_BYTES) ||
          code === NEGATIVE_BIG_INTEGER || code === POSITIVE_BIG_INTEGER
        ) {
          return this.#readInteger(code);
        }
        return this.#fail(
          `unsupported or invalid tuple type code 0x${
            code.toString(16).padStart(2, "0")
          }`,
        );
    }
  }

  #readTuple(nested: boolean): Tuple {
    const values: Array<TupleValue> = [];
    while (!this.done) {
      const code = this.#readByte();
      if (nested && code === NIL) {
        if (!this.done && this.#bytes[this.#offset] === ESCAPE) {
          this.#offset++;
          values.push(null);
          continue;
        }
        return Object.freeze(values);
      }
      values.push(this.#readValue(code));
    }
    if (nested) {
      return this.#fail("unterminated nested tuple");
    }
    return Object.freeze(values);
  }

  parse(): Tuple {
    return this.#readTuple(false);
  }
}

/**
 * Unpacks a complete FoundationDB tuple. Integer values decode to `bigint`,
 * binary64 values to `number`, and byte strings to independently owned arrays.
 * When a Schema is provided, the unpacked tuple is validated before returning.
 */
export function unpack(
  bytes: Uint8Array<ArrayBufferLike>,
): Effect.Effect<Tuple, TupleError>;
export function unpack<S extends Schema.Constraint>(
  bytes: Uint8Array<ArrayBufferLike>,
  schema: S,
): Effect.Effect<
  S["Type"],
  TupleError | Schema.SchemaError,
  S["DecodingServices"]
>;
export function unpack(
  bytes: Uint8Array<ArrayBufferLike>,
  schema?: Schema.Constraint,
) {
  const decoded = Effect.try({
    try: () => {
      if (!(bytes instanceof Uint8Array)) {
        throw tupleError("unpack", "input must be a Uint8Array");
      }
      return new Parser(bytes.slice()).parse();
    },
    catch: (cause) => tupleErrorFromUnknown("unpack", cause),
  });
  return schema === undefined
    ? decoded
    : decoded.pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)));
}
