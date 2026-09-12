import { Effect, Equal, Hash, Schema } from "effect";
import { compareBytes } from "./bytes.ts";

/** A typed failure produced while packing or unpacking a tuple. */
export class TupleError extends Schema.TaggedError<TupleError>()("TupleError", {
  operation: Schema.Literals(["pack", "unpack"]),
  reason: Schema.String,
}) {}

export const tupleError = (
  operation: "pack" | "unpack",
  reason: string,
): TupleError => new TupleError({ operation, reason });

export const tupleErrorFromUnknown = (
  operation: "pack" | "unpack",
  cause: unknown,
): TupleError =>
  cause instanceof TupleError ? cause : tupleError(
    operation,
    cause instanceof Error ? cause.message : String(cause),
  );

const validatedBytes = (
  value: Uint8Array<ArrayBufferLike>,
  length: number,
  description: string,
): Effect.Effect<Uint8Array, TupleError> =>
  value instanceof Uint8Array && value.byteLength === length
    ? Effect.succeed(value.slice())
    : Effect.fail(tupleError(
      "pack",
      `${description} must contain exactly ${length} bytes`,
    ));

/** An IEEE-754 binary32 tuple value, distinct from JavaScript binary64 numbers. */
export class Float32 implements Equal.Equal {
  readonly #bits: number;

  private constructor(bits: number) {
    this.#bits = bits >>> 0;
  }

  static fromNumber(value: number): Float32 {
    const view = new DataView(new ArrayBuffer(4));
    view.setFloat32(0, value, false);
    return new Float32(view.getUint32(0, false));
  }

  static fromBits(bits: number): Effect.Effect<Float32, TupleError> {
    return Number.isInteger(bits) && bits >= 0 && bits <= 0xffff_ffff
      ? Effect.succeed(new Float32(bits))
      : Effect.fail(
        tupleError("pack", "float32 bits must be an unsigned integer"),
      );
  }

  /** @internal */
  static fromBitsUnsafe(bits: number): Float32 {
    return new Float32(bits);
  }

  get bits(): number {
    return this.#bits;
  }

  get value(): number {
    const view = new DataView(new ArrayBuffer(4));
    view.setUint32(0, this.#bits, false);
    return view.getFloat32(0, false);
  }

  equals(other: Float32): boolean {
    return other instanceof Float32 && this.#bits === other.#bits;
  }

  [Equal.symbol](other: Equal.Equal): boolean {
    return other instanceof Float32 && this.equals(other);
  }

  [Hash.symbol](): number {
    return Hash.number(this.#bits);
  }
}

/** A 16-byte RFC-4122 UUID tuple value. */
export class Uuid implements Equal.Equal {
  static readonly schema = Schema.declare<Uuid>(
    (value): value is Uuid => value instanceof Uuid,
    { expected: "FoundationDB tuple UUID" },
  );

  readonly #bytes: Uint8Array;

  private constructor(bytes: Uint8Array) {
    this.#bytes = bytes;
  }

  static fromBytes(
    bytes: Uint8Array<ArrayBufferLike>,
  ): Effect.Effect<Uuid, TupleError> {
    return validatedBytes(bytes, 16, "UUID").pipe(
      Effect.map((value) => new Uuid(value)),
    );
  }

  static fromString(value: string): Effect.Effect<Uuid, TupleError> {
    return Effect.try({
      try: () => {
        if (
          !/^(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/
            .test(value)
        ) {
          throw tupleError(
            "pack",
            "UUID must use 32 hexadecimal digits with optional canonical hyphens",
          );
        }
        const compact = value.replaceAll("-", "");
        return new Uuid(Uint8Array.from(
          compact.match(/../g)!,
          (byte) => Number.parseInt(byte, 16),
        ));
      },
      catch: (cause) => tupleErrorFromUnknown("pack", cause),
    });
  }

  /** @internal */
  static fromBytesUnsafe(bytes: Uint8Array): Uuid {
    return new Uuid(bytes.slice());
  }

  get bytes(): Uint8Array {
    return this.#bytes.slice();
  }

  equals(other: Uuid): boolean {
    return other instanceof Uuid &&
      compareBytes(this.#bytes, other.#bytes) === 0;
  }

  [Equal.symbol](other: Equal.Equal): boolean {
    return other instanceof Uuid && this.equals(other);
  }

  [Hash.symbol](): number {
    return Hash.array(this.#bytes);
  }

  toString(): string {
    const hex = Array.from(
      this.#bytes,
      (byte) => byte.toString(16).padStart(2, "0"),
    ).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${
      hex.slice(16, 20)
    }-${hex.slice(20)}`;
  }
}

/** A 12-byte FoundationDB transaction and user version tuple value. */
export class Versionstamp implements Equal.Equal {
  static readonly schema = Schema.declare<Versionstamp>(
    (value): value is Versionstamp => value instanceof Versionstamp,
    { expected: "FoundationDB tuple versionstamp" },
  );

  readonly #bytes: Uint8Array;

  private constructor(bytes: Uint8Array) {
    this.#bytes = bytes;
  }

  static fromBytes(
    bytes: Uint8Array<ArrayBufferLike>,
  ): Effect.Effect<Versionstamp, TupleError> {
    return validatedBytes(bytes, 12, "versionstamp").pipe(
      Effect.map((value) => new Versionstamp(value)),
    );
  }

  static complete(
    transactionVersion: Uint8Array<ArrayBufferLike>,
    userVersion: number,
  ): Effect.Effect<Versionstamp, TupleError> {
    return Effect.gen(function* () {
      const transaction = yield* validatedBytes(
        transactionVersion,
        10,
        "transaction version",
      );
      if (
        !Number.isInteger(userVersion) || userVersion < 0 ||
        userVersion > 0xffff
      ) {
        return yield* tupleError(
          "pack",
          "versionstamp user version must be an unsigned 16-bit integer",
        );
      }
      const bytes = new Uint8Array(12);
      bytes.set(transaction);
      new DataView(bytes.buffer).setUint16(10, userVersion, false);
      return new Versionstamp(bytes);
    });
  }

  static incomplete(
    userVersion: number,
  ): Effect.Effect<Versionstamp, TupleError> {
    return Versionstamp.complete(new Uint8Array(10).fill(0xff), userVersion);
  }

  /** @internal */
  static fromBytesUnsafe(bytes: Uint8Array): Versionstamp {
    return new Versionstamp(bytes.slice());
  }

  get transactionVersion(): Uint8Array {
    return this.#bytes.slice(0, 10);
  }

  get userVersion(): number {
    return new DataView(
      this.#bytes.buffer,
      this.#bytes.byteOffset,
      this.#bytes.byteLength,
    ).getUint16(10, false);
  }

  get isComplete(): boolean {
    return this.#bytes.slice(0, 10).some((byte) => byte !== 0xff);
  }

  get bytes(): Uint8Array {
    return this.#bytes.slice();
  }

  equals(other: Versionstamp): boolean {
    return other instanceof Versionstamp &&
      compareBytes(this.#bytes, other.#bytes) === 0;
  }

  [Equal.symbol](other: Equal.Equal): boolean {
    return other instanceof Versionstamp && this.equals(other);
  }

  [Hash.symbol](): number {
    return Hash.array(this.#bytes);
  }
}

/**
 * A value supported by the FoundationDB tuple layer.
 *
 * Integers are represented only by `bigint`; every JavaScript `number` is
 * encoded as an IEEE-754 binary64 value. Arrays represent nested tuples.
 */
export type TupleValue =
  | null
  | boolean
  | Uint8Array<ArrayBufferLike>
  | string
  | bigint
  | number
  | Float32
  | Uuid
  | Versionstamp
  | Tuple;

/** An ordered sequence of FoundationDB tuple values. */
export type Tuple = readonly TupleValue[];
