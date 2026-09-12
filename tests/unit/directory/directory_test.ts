import { assert, assertEquals, assertFalse } from "@std/assert";
import { Effect, Latch } from "effect";
import {
  DirectoryError,
  DirectoryLayer,
  DirectoryPartition,
  DirectorySubspace,
  FoundationDbTransaction,
  Subspace,
} from "../../../mod.ts";
import type {
  DirectoryOutput,
  FoundationDbShape,
  FoundationDbTransactionShape,
} from "../../../mod.ts";
import { strinc } from "../../../src/directory/mod.ts";
import { makeMemoryFoundationDb } from "../../support/memory-database.ts";

const encoder = new TextEncoder();
const bytes = (...values: number[]): Uint8Array => Uint8Array.from(values);
const text = (value: string): Uint8Array => encoder.encode(value);

const makeDirectory = (
  options: Parameters<typeof DirectoryLayer.make>[0] = {},
): DirectoryLayer => Effect.runSync(DirectoryLayer.make(options));

const runTransaction = <A, E>(
  database: FoundationDbShape,
  effect: Effect.Effect<A, E, FoundationDbTransaction>,
): Promise<A> => Effect.runPromise(database.withTransaction(effect));

const expectDirectoryError = async <A>(
  database: FoundationDbShape,
  effect: Effect.Effect<
    A,
    DirectoryError | unknown,
    FoundationDbTransaction
  >,
): Promise<DirectoryError> => {
  const failure = await Effect.runPromise(
    Effect.flip(database.withTransaction(effect)),
  );
  assert(failure instanceof DirectoryError);
  return failure;
};

const regular = (output: DirectoryOutput): DirectorySubspace => {
  assert(output instanceof DirectorySubspace);
  return output;
};

// Ported from foundationdb-rs 0.11.0 directory::tests::test_strinc.
Deno.test("foundationdb-rs parity: strinc vectors", () => {
  const vectors: ReadonlyArray<readonly [Uint8Array, Uint8Array]> = [
    [text("a"), text("b")],
    [text("y"), text("z")],
    [text("!"), text('"')],
    [text("*"), text("+")],
    [text("fdb"), text("fdc")],
    [text("foundation database 6"), text("foundation database 7")],
    [bytes(61, 62, 255), bytes(61, 63)],
    [bytes(253, 255), bytes(254)],
    [bytes(253, 255, 255), bytes(254)],
    [bytes(255, 255, 255), bytes()],
  ];

  for (const [input, expected] of vectors) {
    assertEquals(strinc(input), expected);
  }
});

// Ported from foundationdb-rs 0.11.0 tests/directory.rs::test_directory.
Deno.test("foundationdb-rs parity: create then open preserves prefixes", async () => {
  const { database } = makeMemoryFoundationDb();
  const directory = makeDirectory();

  for (const path of [["application"], ["1", "2"]] as const) {
    const created = regular(
      await runTransaction(
        database,
        directory.create(path),
      ),
    );
    const opened = regular(
      await runTransaction(
        database,
        directory.open(path),
      ),
    );
    assertEquals(opened.prefix, created.prefix);
  }
});

Deno.test("Directory creates, opens, lists, and composes as a transaction dependency", async () => {
  const { database } = makeMemoryFoundationDb();
  const directory = makeDirectory();
  const layer = text("application");

  const created = regular(
    await runTransaction(
      database,
      directory.createOrOpen(["app", "users"], { layer }),
    ),
  );
  const opened = regular(
    await runTransaction(
      database,
      directory.open(["app", "users"], { layer }),
    ),
  );

  assertEquals(opened.prefix, created.prefix);
  assertEquals(opened.path, ["app", "users"]);
  assertEquals(opened.layer, layer);
  assertEquals(await runTransaction(database, directory.exists(["app"])), true);
  assertEquals(
    await runTransaction(database, directory.list(["app"])),
    ["users"],
  );
  assertEquals(
    await runTransaction(database, opened.list()),
    [],
  );

  const key = Effect.runSync(opened.pack(["alice", 7n]));
  assertEquals(Effect.runSync(opened.unpack(key)), ["alice", 7n]);
  await database.set(key, text("Alice")).pipe(Effect.runPromise);
  assertEquals(await database.get(key).pipe(Effect.runPromise), text("Alice"));

  assertEquals(
    (await expectDirectoryError(
      database,
      directory.create(["app", "users"]),
    )).reason,
    "DirectoryAlreadyExists",
  );
  assertEquals(
    (await expectDirectoryError(
      database,
      directory.open(["app", "users"], { layer: text("other") }),
    )).reason,
    "IncompatibleLayer",
  );
});

Deno.test("Directory writes the canonical metadata representation", async () => {
  const { database, entries } = makeMemoryFoundationDb();
  const directory = makeDirectory({ allowManualPrefixes: true });
  const prefix = bytes(0x10, 0x00);
  const layer = text("records");

  await runTransaction(
    database,
    directory.create(["foo"], { prefix, layer }),
  );

  const metadata = entries();
  const root = bytes(0xfe, 0x01, 0xfe, 0x00);
  assert(
    metadata.some(({ key, value }) =>
      equals(key, bytes(...root, 0x01, ...text("version"), 0x00)) &&
      equals(value, bytes(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    ),
  );
  assert(
    metadata.some(({ key, value }) =>
      equals(key, bytes(...root, 0x14, 0x02, ...text("foo"), 0x00)) &&
      equals(value, prefix)
    ),
  );
  assert(metadata.some(({ key, value }) =>
    equals(
      key,
      bytes(
        0xfe,
        0x01,
        0x10,
        0x00,
        0xff,
        0x00,
        0x01,
        ...text("layer"),
        0x00,
      ),
    ) && equals(value, layer)
  ));
});

Deno.test("Directory rejects disabled, overlapping, and metadata prefixes", async () => {
  const { database } = makeMemoryFoundationDb();
  const standard = makeDirectory();
  assertEquals(
    (await expectDirectoryError(
      database,
      standard.create(["manual"], { prefix: bytes(0x20) }),
    )).reason,
    "PrefixNotAllowed",
  );

  const directory = makeDirectory({ allowManualPrefixes: true });
  await runTransaction(
    database,
    directory.create(["one"], { prefix: bytes(0x20, 0x01) }),
  );
  assertEquals(
    (await expectDirectoryError(
      database,
      directory.create(["parent-prefix"], { prefix: bytes(0x20) }),
    )).reason,
    "DirectoryPrefixInUse",
  );
  assertEquals(
    (await expectDirectoryError(
      database,
      directory.create(["child-prefix"], {
        prefix: bytes(0x20, 0x01, 0x02),
      }),
    )).reason,
    "DirectoryPrefixInUse",
  );
  assertEquals(
    (await expectDirectoryError(
      database,
      directory.create(["metadata"], { prefix: bytes(0xfe, 0x01) }),
    )).reason,
    "DirectoryPrefixInUse",
  );
});

Deno.test("Directory revalidates a manual leaf prefix after creating parents", async () => {
  const { database } = makeMemoryFoundationDb();
  const contentSubspace = new Subspace(bytes(0xb0));
  const directory = makeDirectory({
    contentSubspace,
    allowManualPrefixes: true,
  });

  assertEquals(
    (await expectDirectoryError(
      database,
      directory.create(["parent", "leaf"], {
        prefix: contentSubspace.prefix,
      }),
    )).reason,
    "DirectoryPrefixInUse",
  );
  assertFalse(await runTransaction(database, directory.exists(["parent"])));
});

Deno.test("Directory allocates unique prefixes concurrently in one transaction", async () => {
  const { database } = makeMemoryFoundationDb();
  const firstDirectory = makeDirectory();
  const secondDirectory = makeDirectory();
  const nodeSubspace = new Subspace(bytes(0xfe));
  const root = Effect.runSync(nodeSubspace.subspace([nodeSubspace.prefix]));
  const allocator = Effect.runSync(root.subspace([text("hca")]));
  const recent = Effect.runSync(allocator.subspace([1n]));
  const originalRandom = Object.getOwnPropertyDescriptor(
    crypto,
    "getRandomValues",
  );
  let randomCalls = 0;
  Object.defineProperty(crypto, "getRandomValues", {
    configurable: true,
    value: (values: Uint32Array): Uint32Array => {
      values[0] = randomCalls < 2 ? 0 : randomCalls - 1;
      randomCalls++;
      return values;
    },
  });

  try {
    const outputs = await Effect.runPromise(database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const candidateReadsReady = Latch.makeUnsafe(false);
        let candidateReads = 0;
        const delayedTransaction: FoundationDbTransactionShape = {
          ...transaction,
          get: (key, options) => {
            const read = transaction.get(key, options);
            if (!recent.contains(key)) {
              return read;
            }
            return Effect.gen(function* () {
              const value = yield* read;
              candidateReads++;
              if (candidateReads === 2) {
                candidateReadsReady.openUnsafe();
              }
              yield* Effect.race(
                candidateReadsReady.await,
                Effect.sleep("20 millis"),
              );
              return value;
            });
          },
        };
        return yield* Effect.all(
          [
            firstDirectory.create(["concurrent-0"]),
            secondDirectory.create(["concurrent-1"]),
          ],
          { concurrency: "unbounded" },
        ).pipe(
          Effect.provideService(
            FoundationDbTransaction,
            delayedTransaction,
          ),
        );
      }),
    ));
    const prefixIds = outputs.map((output) =>
      Array.from(regular(output).prefix).join(",")
    );

    assertEquals(new Set(prefixIds).size, 2);
  } finally {
    if (originalRandom === undefined) {
      delete (crypto as { getRandomValues?: unknown }).getRandomValues;
    } else {
      Object.defineProperty(crypto, "getRandomValues", originalRandom);
    }
  }
});

Deno.test("Directory move preserves prefixes and enforces hierarchy constraints", async () => {
  const { database } = makeMemoryFoundationDb();
  const directory = makeDirectory({ allowManualPrefixes: true });
  const original = regular(
    await runTransaction(
      database,
      directory.create(["source"], { prefix: bytes(0x31) }),
    ),
  );
  await runTransaction(database, directory.create(["target"]));

  const moved = regular(
    await runTransaction(
      database,
      directory.moveTo(["source"], ["target", "renamed"]),
    ),
  );
  assertEquals(moved.prefix, original.prefix);
  assertEquals(moved.path, ["target", "renamed"]);
  assertFalse(await runTransaction(database, directory.exists(["source"])));
  assert(
    await runTransaction(
      database,
      directory.exists(["target", "renamed"]),
    ),
  );

  assertEquals(
    (await expectDirectoryError(
      database,
      directory.moveTo(["target"], ["target", "renamed", "child"]),
    )).reason,
    "CannotMoveIntoSubdirectory",
  );
  assertEquals(
    (await expectDirectoryError(
      database,
      directory.moveTo(["target", "renamed"], ["missing", "renamed"]),
    )).reason,
    "ParentDirectoryDoesNotExist",
  );
});

Deno.test("Directory recursively removes metadata and directory contents", async () => {
  const { database } = makeMemoryFoundationDb();
  const directory = makeDirectory();
  const parent = regular(
    await runTransaction(
      database,
      directory.create(["tree"]),
    ),
  );
  const child = regular(
    await runTransaction(
      database,
      directory.create(["tree", "child"]),
    ),
  );
  const parentKey = Effect.runSync(parent.pack(["parent-value"]));
  const childKey = Effect.runSync(child.pack(["child-value"]));
  await Effect.runPromise(database.set(parentKey, bytes(1)));
  await Effect.runPromise(database.set(childKey, bytes(2)));

  assert(await runTransaction(database, directory.remove(["tree"])));
  assertFalse(await runTransaction(database, directory.exists(["tree"])));
  assertEquals(await Effect.runPromise(database.get(parentKey)), undefined);
  assertEquals(await Effect.runPromise(database.get(childKey)), undefined);
  assertFalse(
    await runTransaction(
      database,
      directory.removeIfExists(["tree"]),
    ),
  );
  assertEquals(
    (await expectDirectoryError(database, directory.remove([]))).reason,
    "CannotModifyRootDirectory",
  );
});

Deno.test("Directory partitions route descendants and reject cross-partition moves", async () => {
  const { database } = makeMemoryFoundationDb();
  const directory = makeDirectory();
  const partition = await runTransaction(
    database,
    directory.create(["tenant"], { layer: text("partition") }),
  );
  assert(partition instanceof DirectoryPartition);
  const child = regular(
    await runTransaction(
      database,
      partition.create(["records"]),
    ),
  );
  assert(child.prefix.length > partition.path.length);
  assertEquals(child.path, ["tenant", "records"]);

  const opened = regular(
    await runTransaction(
      database,
      directory.open(["tenant", "records"]),
    ),
  );
  assertEquals(opened.prefix, child.prefix);
  assertEquals(
    await runTransaction(database, directory.list(["tenant"])),
    ["records"],
  );
  assert(
    await runTransaction(
      database,
      directory.exists(["tenant", "records"]),
    ),
  );

  await runTransaction(database, directory.create(["outside"]));
  assertEquals(
    (await expectDirectoryError(
      database,
      directory.moveTo(["outside"], ["tenant", "inside"]),
    )).reason,
    "CannotMoveBetweenPartitions",
  );
  assert(await runTransaction(database, partition.remove([])));
  assertFalse(await runTransaction(database, directory.exists(["tenant"])));
});

Deno.test("Directory distinguishes partition paths containing NUL", async () => {
  const { database } = makeMemoryFoundationDb();
  const directory = makeDirectory();
  const firstPath = ["a\0b", "c"];
  const secondPath = ["a", "b\0c"];
  const first = await runTransaction(
    database,
    directory.create(firstPath, { layer: text("partition") }),
  );
  const second = await runTransaction(
    database,
    directory.create(secondPath, { layer: text("partition") }),
  );
  assert(first instanceof DirectoryPartition);
  assert(second instanceof DirectoryPartition);
  await runTransaction(database, first.create(["source"]));
  await runTransaction(database, second.create(["target"]));

  assertEquals(
    (await expectDirectoryError(
      database,
      directory.moveTo(
        [...firstPath, "source"],
        [...secondPath, "target"],
      ),
    )).reason,
    "CannotMoveBetweenPartitions",
  );
  assert(await runTransaction(database, first.exists(["source"])));
  assert(await runTransaction(database, second.exists(["target"])));
});

Deno.test("Directory version policy permits newer-minor reads but rejects writes", async () => {
  const { database } = makeMemoryFoundationDb();
  const nodeSubspace = new Subspace(bytes(0xa0));
  const directory = makeDirectory({ nodeSubspace, allowManualPrefixes: true });
  const root = Effect.runSync(nodeSubspace.subspace([nodeSubspace.prefix]));
  const versionKey = Effect.runSync(root.pack([text("version")]));
  const version = new Uint8Array(12);
  const view = new DataView(version.buffer);
  view.setUint32(0, 1, true);
  view.setUint32(4, 1, true);
  await Effect.runPromise(database.set(versionKey, version));

  assertEquals(await runTransaction(database, directory.list()), []);
  assertEquals(
    (await expectDirectoryError(
      database,
      directory.create(["blocked"], { prefix: bytes(0x40) }),
    )).reason,
    "VersionError",
  );

  await Effect.runPromise(database.set(versionKey, new Uint8Array(13)));
  assertEquals(
    (await expectDirectoryError(database, directory.list())).reason,
    "VersionError",
  );
});

Deno.test("Directory reports malformed child and allocator metadata", async () => {
  const childDatabase = makeMemoryFoundationDb();
  const childNodeSubspace = new Subspace(bytes(0xa1));
  const childDirectory = makeDirectory({ nodeSubspace: childNodeSubspace });
  const childRoot = Effect.runSync(
    childNodeSubspace.subspace([childNodeSubspace.prefix]),
  );
  const malformedChild = Effect.runSync(
    childRoot.pack([0n, "child", "unexpected"]),
  );
  await Effect.runPromise(
    childDatabase.database.set(malformedChild, bytes(0x40)),
  );

  assertEquals(
    (await expectDirectoryError(childDatabase.database, childDirectory.list()))
      .reason,
    "InvalidMetadata",
  );

  const allocatorDatabase = makeMemoryFoundationDb();
  const allocatorNodeSubspace = new Subspace(bytes(0xa2));
  const allocatorDirectory = makeDirectory({
    nodeSubspace: allocatorNodeSubspace,
  });
  const allocatorRoot = Effect.runSync(
    allocatorNodeSubspace.subspace([allocatorNodeSubspace.prefix]),
  );
  const allocator = Effect.runSync(allocatorRoot.subspace([text("hca")]));
  const counters = Effect.runSync(allocator.subspace([0n]));
  const malformedCounter = Effect.runSync(counters.pack([0n, 1n]));
  await Effect.runPromise(
    allocatorDatabase.database.set(malformedCounter, new Uint8Array(8)),
  );

  assertEquals(
    (await expectDirectoryError(
      allocatorDatabase.database,
      allocatorDirectory.create(["blocked"]),
    )).reason,
    "InvalidMetadata",
  );
});

const equals = (left: Uint8Array, right: Uint8Array): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((value, index) => value === right[index]);
};
