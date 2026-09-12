import { Effect, Stream } from "effect";
import { FoundationDbTransaction } from "../FoundationDb.ts";
import type { FoundationDbError } from "../errors.ts";
import { keyRange } from "../model.ts";
import type { Bytes, KeyValue } from "../model.ts";
import { Subspace } from "../tuple/mod.ts";
import type { Tuple, TupleError, TupleValue } from "../tuple/mod.ts";
import {
  DirectoryError,
  type DirectoryErrorReason,
  type DirectoryLayerState,
  type DirectoryPath,
  type InternalDirectoryLayerOptions,
} from "./model.ts";

const encoder = new TextEncoder();
export const NODE_PREFIX = Uint8Array.of(0xfe);
export const VERSION = encoder.encode("version");
export const HCA = encoder.encode("hca");
export const LAYER = encoder.encode("layer");
export const PARTITION = encoder.encode("partition");
export const ONE = Uint8Array.of(1, 0, 0, 0, 0, 0, 0, 0);
export const DIRECTORY_VERSION = Uint8Array.of(
  1,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
);

export const error = (
  reason: DirectoryErrorReason,
  message: string,
): DirectoryError => new DirectoryError({ reason, message });

export const invalidMetadata = (message: string): DirectoryError =>
  error("InvalidMetadata", message);

const mapTupleError = <A>(
  effect: Effect.Effect<A, TupleError>,
  message: string,
): Effect.Effect<A, DirectoryError> =>
  effect.pipe(
    Effect.mapError((cause) => invalidMetadata(`${message}: ${cause.reason}`)),
  );

export const concat = (...parts: ReadonlyArray<Bytes>): Uint8Array => {
  const output = new Uint8Array(
    parts.reduce((length, part) => length + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
};

export const equalBytes = (left: Bytes, right: Bytes): boolean => {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let index = 0; index < left.byteLength; index++) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
};

export const startsWith = (value: Bytes, prefix: Bytes): boolean => {
  if (value.byteLength < prefix.byteLength) {
    return false;
  }
  for (let index = 0; index < prefix.byteLength; index++) {
    if (value[index] !== prefix[index]) {
      return false;
    }
  }
  return true;
};

export const pathStartsWith = (
  value: DirectoryPath,
  prefix: DirectoryPath,
): boolean =>
  value.length >= prefix.length &&
  prefix.every((component, index) => value[index] === component);

export const pathsEqual = (
  left: DirectoryPath,
  right: DirectoryPath,
): boolean =>
  left.length === right.length &&
  left.every((component, index) => component === right[index]);

/** @internal */
export const strinc = (key: Bytes): Uint8Array => {
  const output = key.slice();
  let index = output.byteLength - 1;
  while (index >= 0 && output[index] === 0xff) {
    index--;
  }
  if (index < 0) {
    return new Uint8Array();
  }
  output[index]++;
  return output.slice(0, index + 1);
};

export const incrementPrefix = (
  prefix: Bytes,
): Effect.Effect<Uint8Array, DirectoryError> =>
  Effect.suspend(() => {
    const output = strinc(prefix);
    if (output.byteLength === 0) {
      return Effect.fail(error(
        "InvalidPrefix",
        "directory prefix has no lexicographic successor",
      ));
    }
    return Effect.succeed(output);
  });

export const packed = (
  subspace: Subspace,
  tuple: Tuple,
  description: string,
): Effect.Effect<Uint8Array, DirectoryError> =>
  mapTupleError(subspace.pack(tuple), description);

export const childSubspace = (
  subspace: Subspace,
  tuple: Tuple,
  description: string,
): Effect.Effect<Subspace, DirectoryError> =>
  mapTupleError(subspace.subspace(tuple), description);

export const unpacked = (
  subspace: Subspace,
  key: Bytes,
  description: string,
): Effect.Effect<Tuple, DirectoryError> =>
  mapTupleError(subspace.unpack(key), description);

export const subspaceRange = (
  subspace: Subspace,
  tuple: Tuple = [],
): Effect.Effect<readonly [Uint8Array, Uint8Array], DirectoryError> =>
  mapTupleError(subspace.range(tuple), "invalid directory metadata range");

export const firstInRange = (
  transaction: FoundationDbTransaction["Service"],
  begin: Bytes,
  end: Bytes,
  options: { readonly reverse?: boolean; readonly snapshot?: boolean } = {},
): Effect.Effect<KeyValue | undefined, FoundationDbError> =>
  Stream.runCollect(transaction.getRange(keyRange(begin, end, {
    limit: 1,
    ...(options.reverse === undefined ? {} : { reverse: options.reverse }),
    ...(options.snapshot === undefined ? {} : { snapshot: options.snapshot }),
  }))).pipe(Effect.map((values) => values[0]));

export const allInRange = (
  transaction: FoundationDbTransaction["Service"],
  begin: Bytes,
  end: Bytes,
): Effect.Effect<ReadonlyArray<KeyValue>, FoundationDbError> =>
  Stream.runCollect(transaction.getRange(keyRange(begin, end)));

export const asBytes = (
  value: TupleValue | undefined,
  description: string,
): Effect.Effect<Uint8Array, DirectoryError> =>
  value instanceof Uint8Array
    ? Effect.succeed(value)
    : Effect.fail(invalidMetadata(`${description} is not a byte string`));

export const asInteger = (
  value: TupleValue | undefined,
  description: string,
): Effect.Effect<bigint, DirectoryError> =>
  typeof value === "bigint"
    ? Effect.succeed(value)
    : Effect.fail(invalidMetadata(`${description} is not an integer`));

export const asString = (
  value: TupleValue | undefined,
  description: string,
): Effect.Effect<string, DirectoryError> =>
  typeof value === "string"
    ? Effect.succeed(value)
    : Effect.fail(invalidMetadata(`${description} is not a string`));

export const directoryChildName = (
  tuple: Tuple,
): Effect.Effect<string, DirectoryError> =>
  tuple.length === 2 && tuple[0] === 0n
    ? asString(tuple[1], "directory child name")
    : Effect.fail(invalidMetadata(
      "directory child key must contain exactly an integer zero and a name",
    ));

export const allocatorWindow = (
  tuple: Tuple,
): Effect.Effect<bigint, DirectoryError> =>
  tuple.length === 1
    ? asInteger(tuple[0], "allocator counter window")
    : Effect.fail(invalidMetadata(
      "allocator counter key must contain exactly one integer",
    ));

export const makeDirectoryLayerState = (
  options: InternalDirectoryLayerOptions,
): Effect.Effect<DirectoryLayerState, DirectoryError> =>
  Effect.gen(function* () {
    const nodeSubspace = options.nodeSubspace ?? new Subspace(NODE_PREFIX);
    const contentSubspace = options.contentSubspace ?? new Subspace();
    const rootNode = yield* childSubspace(
      nodeSubspace,
      [nodeSubspace.prefix],
      "invalid directory root",
    );
    const allocator = yield* childSubspace(
      rootNode,
      [HCA],
      "invalid directory allocator",
    );
    return {
      rootNode,
      nodeSubspace,
      contentSubspace,
      allocator,
      allowManualPrefixes: options.allowManualPrefixes === true,
      path: Object.freeze([...(options.path ?? [])]),
    };
  });
