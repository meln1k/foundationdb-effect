import { Effect, Equal, Hash, Schema } from "effect";
import { compareBytes, concatBytes } from "./bytes.ts";
import {
  appendVersionstampOffset,
  pack,
  packTrackedSync,
  unpack,
} from "./codec.ts";
import { tupleError, tupleErrorFromUnknown } from "./values.ts";
import type { Tuple, TupleError } from "./values.ts";

/** An immutable, defensively-owned FoundationDB tuple key namespace. */
export class Subspace implements Equal.Equal {
  readonly #prefix: Uint8Array;
  #incompleteVersionstamps: ReadonlyArray<number> = Object.freeze([]);

  constructor(prefix: Uint8Array<ArrayBufferLike> = new Uint8Array()) {
    this.#prefix = prefix.slice();
  }

  static #tracked(
    prefix: Uint8Array<ArrayBufferLike>,
    incompleteVersionstamps: ReadonlyArray<number>,
  ): Subspace {
    const subspace = new Subspace(prefix);
    subspace.#incompleteVersionstamps = Object.freeze([
      ...incompleteVersionstamps,
    ]);
    return subspace;
  }

  static all(): Subspace {
    return new Subspace();
  }

  static fromBytes(prefix: Uint8Array<ArrayBufferLike>): Subspace {
    return new Subspace(prefix);
  }

  static fromTuple(tuple: Tuple): Effect.Effect<Subspace, TupleError> {
    return Effect.try({
      try: () => {
        const packed = packTrackedSync(tuple);
        return Subspace.#tracked(
          packed.bytes,
          packed.incompleteVersionstamps,
        );
      },
      catch: (cause) => tupleErrorFromUnknown("pack", cause),
    });
  }

  /** Returns an independent copy of this subspace's raw key prefix. */
  get prefix(): Uint8Array {
    return this.#prefix.slice();
  }

  /** Alias matching foundationdb-rs's raw subspace bytes accessor. */
  get bytes(): Uint8Array {
    return this.#prefix.slice();
  }

  /** Packs a tuple after this subspace's prefix. */
  pack(tuple: Tuple): Effect.Effect<Uint8Array, TupleError> {
    return pack(tuple).pipe(
      Effect.map((suffix) => concatBytes(this.#prefix, suffix)),
    );
  }

  /** Packs a key and appends its one incomplete-versionstamp offset. */
  packWithVersionstamp(tuple: Tuple): Effect.Effect<Uint8Array, TupleError> {
    return Effect.try({
      try: () => {
        const suffix = packTrackedSync(tuple);
        return appendVersionstampOffset({
          bytes: concatBytes(this.#prefix, suffix.bytes),
          incompleteVersionstamps: [
            ...this.#incompleteVersionstamps,
            ...suffix.incompleteVersionstamps.map((offset) =>
              this.#prefix.byteLength + offset
            ),
          ],
        });
      },
      catch: (cause) => tupleErrorFromUnknown("pack", cause),
    });
  }

  /**
   * Unpacks a key after verifying and removing this subspace's prefix. When a
   * Schema is provided, the tuple suffix is validated before returning.
   */
  unpack(
    key: Uint8Array<ArrayBufferLike>,
  ): Effect.Effect<Tuple, TupleError>;
  unpack<S extends Schema.Constraint>(
    key: Uint8Array<ArrayBufferLike>,
    schema: S,
  ): Effect.Effect<
    S["Type"],
    TupleError | Schema.SchemaError,
    S["DecodingServices"]
  >;
  unpack(
    key: Uint8Array<ArrayBufferLike>,
    schema?: Schema.Constraint,
  ) {
    if (!this.contains(key)) {
      return Effect.fail(tupleError("unpack", "key is outside the subspace"));
    }
    const suffix = key.slice(this.#prefix.byteLength);
    return schema === undefined ? unpack(suffix) : unpack(suffix, schema);
  }

  /** Returns whether a key starts with this subspace's prefix. */
  contains(key: Uint8Array<ArrayBufferLike>): boolean {
    if (!(key instanceof Uint8Array) || key.byteLength < this.#prefix.length) {
      return false;
    }
    for (let index = 0; index < this.#prefix.length; index++) {
      if (key[index] !== this.#prefix[index]) {
        return false;
      }
    }
    return true;
  }

  /** Alias matching foundationdb-rs's subspace prefix predicate. */
  isStartOf(key: Uint8Array<ArrayBufferLike>): boolean {
    return this.contains(key);
  }

  /**
   * Returns the inclusive begin and exclusive end keys covering tuples below
   * this subspace, optionally narrowed by an additional tuple prefix.
   */
  range(
    tuple: Tuple = [],
  ): Effect.Effect<readonly [Uint8Array, Uint8Array], TupleError> {
    return this.pack(tuple).pipe(
      Effect.map((prefix) =>
        [
          concatBytes(prefix, Uint8Array.of(0x00)),
          concatBytes(prefix, Uint8Array.of(0xff)),
        ] as const
      ),
    );
  }

  /** Returns a child subspace extended by the packed tuple. */
  subspace(tuple: Tuple): Effect.Effect<Subspace, TupleError> {
    return Effect.try({
      try: () => {
        const suffix = packTrackedSync(tuple);
        return Subspace.#tracked(
          concatBytes(this.#prefix, suffix.bytes),
          [
            ...this.#incompleteVersionstamps,
            ...suffix.incompleteVersionstamps.map((offset) =>
              this.#prefix.byteLength + offset
            ),
          ],
        );
      },
      catch: (cause) => tupleErrorFromUnknown("pack", cause),
    });
  }

  equals(other: Subspace): boolean {
    return other instanceof Subspace &&
      compareBytes(this.#prefix, other.#prefix) === 0 &&
      this.#incompleteVersionstamps.length ===
        other.#incompleteVersionstamps.length &&
      this.#incompleteVersionstamps.every(
        (offset, index) => offset === other.#incompleteVersionstamps[index],
      );
  }

  [Equal.symbol](other: Equal.Equal): boolean {
    return other instanceof Subspace && this.equals(other);
  }

  [Hash.symbol](): number {
    return Hash.array(this.#prefix);
  }

  toString(): string {
    let output = "";
    for (const byte of this.#prefix) {
      if (byte === 0x5c) {
        output += "\\\\";
      } else if (byte >= 0x20 && byte <= 0x7e) {
        output += String.fromCharCode(byte);
      } else {
        output += `\\x${byte.toString(16).padStart(2, "0")}`;
      }
    }
    return output;
  }
}
