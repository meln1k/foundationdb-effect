import { Effect, Stream } from "effect";
import type { FoundationDbTransaction } from "../FoundationDb.ts";
import { keyRange } from "../model.ts";
import type { Subspace, Tuple } from "../tuple/mod.ts";

const chunkSize = 8 * 1_024;

const chunkedValueRange = Effect.fnUntraced(function* (
  subspace: Subspace,
  path: Tuple,
) {
  const [begin, end] = yield* subspace.range(path);
  return { begin, end };
});

const concatChunks = (chunks: ReadonlyArray<Uint8Array>): Uint8Array => {
  const output = new Uint8Array(
    chunks.reduce((size, chunk) => size + chunk.byteLength, 0),
  );
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
};

export const splitChunkedValue = (
  value: Uint8Array,
): ReadonlyArray<Uint8Array> => {
  const chunks = Math.max(1, Math.ceil(value.byteLength / chunkSize));
  return Array.from({ length: chunks }, (_, index) => {
    const offset = index * chunkSize;
    return value.subarray(offset, offset + chunkSize);
  });
};

export const readChunkedValue = Effect.fnUntraced(function* (
  transaction: FoundationDbTransaction["Service"],
  subspace: Subspace,
  path: Tuple,
) {
  const { begin, end } = yield* chunkedValueRange(subspace, path);
  const rows = yield* Stream.runCollect(
    transaction.getRange(keyRange(begin, end)),
  );
  return rows.length === 0
    ? undefined
    : concatChunks(Array.from(rows, (row) => row.value));
});

export const hasChunkedValue = Effect.fnUntraced(function* (
  transaction: FoundationDbTransaction["Service"],
  subspace: Subspace,
  path: Tuple,
) {
  const { begin, end } = yield* chunkedValueRange(subspace, path);
  const rows = yield* Stream.runCollect(
    transaction.getRange(keyRange(begin, end, { limit: 1 })),
  );
  return rows.length !== 0;
});

export const writeChunkedValue = Effect.fnUntraced(function* (
  transaction: FoundationDbTransaction["Service"],
  subspace: Subspace,
  path: Tuple,
  value: Uint8Array,
) {
  const { begin, end } = yield* chunkedValueRange(subspace, path);
  yield* transaction.clearRange(begin, end);
  const chunks = splitChunkedValue(value);
  for (let index = 0; index < chunks.length; index++) {
    yield* transaction.set(
      yield* subspace.pack([...path, BigInt(index)]),
      chunks[index],
    );
  }
});

export const clearChunkedValue = Effect.fnUntraced(function* (
  transaction: FoundationDbTransaction["Service"],
  subspace: Subspace,
  path: Tuple,
) {
  const { begin, end } = yield* chunkedValueRange(subspace, path);
  yield* transaction.clearRange(begin, end);
});
