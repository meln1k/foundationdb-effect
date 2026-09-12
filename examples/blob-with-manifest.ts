import { Console, Effect, Schema, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  pack,
  Subspace,
  unpack,
  Uuid,
} from "../mod.ts";
import { assert, concatBytes, runMain } from "./_shared.ts";

const defaultChunkSize = 1_024;
const chunkSizes = [500, 512, 1_000, 2_000, 4_000, 10_000, 20_000] as const;

interface FileManifest {
  readonly name: string;
  readonly chunkSize: number | undefined;
  readonly chunks: number;
  readonly digest: string;
  readonly size: number;
  readonly uuid: Uuid;
}

const ManifestTuple = Schema.Tuple([
  Schema.String,
  Schema.NullOr(Schema.BigInt),
  Schema.BigInt,
  Schema.String,
  Schema.BigInt,
  Uuid.schema,
]);

const sha256 = Effect.fn("example.sha256")((data: Uint8Array) =>
  Effect.promise(() =>
    crypto.subtle.digest("SHA-256", data.slice().buffer as ArrayBuffer)
  ).pipe(
    Effect.map((digest) =>
      Array.from(
        new Uint8Array(digest),
        (byte) => byte.toString(16).padStart(2, "0"),
      ).join("")
    ),
  )
);

const serializeManifest = (manifest: FileManifest) =>
  pack([
    manifest.name,
    manifest.chunkSize === undefined ? null : BigInt(manifest.chunkSize),
    BigInt(manifest.chunks),
    manifest.digest,
    BigInt(manifest.size),
    manifest.uuid,
  ]);

const deserializeManifest = Effect.fn("example.deserializeManifest")(function* (
  data: Uint8Array,
) {
  const [name, chunkSize, chunks, digest, size, uuid] = yield* unpack(
    data,
    ManifestTuple,
  );
  return {
    name,
    chunkSize: chunkSize === null ? undefined : Number(chunkSize),
    chunks: Number(chunks),
    digest,
    size: Number(size),
    uuid,
  } satisfies FileManifest;
});

const writeBlob = Effect.fn("example.writeBlob")(function* (
  dataSpace: Subspace,
  data: Uint8Array,
  chunkSize: number,
) {
  const transaction = yield* FoundationDbTransaction;
  let chunks = 0;
  for (let offset = 0; offset < data.byteLength; offset += chunkSize) {
    yield* transaction.set(
      yield* dataSpace.pack([BigInt(chunks)]),
      data.slice(offset, offset + chunkSize),
    );
    chunks++;
  }
  return chunks;
});

const readBlob = Effect.fn("example.readBlob")(function* (
  dataSpace: Subspace,
) {
  const transaction = yield* FoundationDbTransaction;
  const [begin, end] = yield* dataSpace.range();
  const rows = yield* Stream.runCollect(
    transaction.getRange(keyRange(begin, end)),
  );
  return concatBytes(...rows.map((row) => row.value));
});

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const images = yield* Subspace.fromTuple(["images"]);
  const [begin, end] = yield* images.range();
  yield* database.clearRange(begin, end);

  const data = new TextEncoder().encode(
    "Effect + FoundationDB blob example\n".repeat(2_048),
  );
  const digest = yield* sha256(data);
  const representations = yield* Effect.forEach(
    [...chunkSizes, undefined],
    (configuredSize) =>
      Effect.gen(function* () {
        const uuid = yield* Uuid.fromString(crypto.randomUUID());
        const fileSpace = yield* images.subspace([uuid]);
        const dataSpace = yield* fileSpace.subspace(["_data"]);
        const manifestKey = yield* fileSpace.pack(["_manifest"]);
        const size = configuredSize ?? defaultChunkSize;
        const chunks = yield* database.withTransaction(
          writeBlob(dataSpace, data, size),
        );
        const manifest: FileManifest = {
          name: "generated-effect-foundationdb.txt",
          chunkSize: configuredSize,
          chunks,
          digest,
          size: data.byteLength,
          uuid,
        };
        yield* database.set(
          manifestKey,
          yield* serializeManifest(manifest),
        );
        return { dataSpace, manifestKey } as const;
      }),
    { concurrency: 1 },
  );

  yield* Effect.forEach(
    representations,
    ({ dataSpace, manifestKey }) =>
      Effect.gen(function* () {
        const encoded = yield* database.get(manifestKey);
        assert(encoded !== undefined, "manifest was not found");
        const manifest = yield* deserializeManifest(encoded);
        const restored = yield* database.withTransaction(readBlob(dataSpace));
        const restoredDigest = yield* sha256(restored);
        assert(restoredDigest === manifest.digest, "blob digest mismatch");
        yield* Console.log(
          `${manifest.uuid}: ${manifest.size} bytes in ${manifest.chunks} chunks ` +
            `(chunk size ${
              manifest.chunkSize ?? defaultChunkSize
            }), ${manifest.digest}`,
        );
      }),
    { concurrency: 1, discard: true },
  );
});

await runMain(program);
