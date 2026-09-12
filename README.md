# effect-foundationdb

An Effect v4 service for [FoundationDB](https://www.foundationdb.org/) on Deno.
A small Rust `cdylib` exposes
[`foundationdb-rs`](https://github.com/foundationdb-rs/foundationdb-rs) through
Deno FFI; the TypeScript API handles scopes, typed errors, transaction retries,
and range streams.

The project pins `effect@4.0.0-rc.115`, `foundationdb@0.11.0`, and the
FoundationDB 7.4 API.

## How the interop works

```text
Effect.callback ← Deno microtask ← shared UnsafeCallback ← completed task
       │                                               ↑
       └── NativeDriver → Deno.dlopen → foundationdb-rs → async-executor ← libfdb_c
```

The Rust library owns opaque database, transaction, and range handles. Deno
never receives Rust references. `foundationdb-rs` registers
`fdb_future_set_callback` with FoundationDB and wakes a single-threaded,
explicitly owned `async-executor`. The executor polls Rust futures to completion
without intermediate Deno callbacks. A completed task signals one shared
`Deno.UnsafeCallback`; that callback queues a microtask, copies returned keys
and values into owned `Uint8Array`s, and resumes the suspended Effect fiber. The
callback stays referenced only while operations are pending. There is no
blocking wait and no Deno FFI worker per operation. Cancellation races the FDB
operation against a native channel, drops the `foundationdb-rs` future, and
notifies Deno only after native cleanup, so scoped transaction handles cannot be
closed underneath a future.

FoundationDB's network is process-global. `FoundationDb.layer` starts it once,
keeps its `NetworkAutoStop` guard alive for the layer's scope, then closes
range, transaction, and database handles before stopping the network and
unloading the library. Only shutdown uses a Deno nonblocking FFI call because it
waits for pending cancellations and joins both the async executor and
FoundationDB network threads. With `foundationdb-rs` 0.11, a stopped network
cannot be restarted in the same process. The native bridge reference-counts
overlapping `FoundationDb.layer` owners. Each owner release crosses an executor
barrier before its Deno callback closes, and the last owner stops the network.
Applications should still normally provide one shared layer for their lifetime;
creating a new layer after every owner has closed is unsupported.

## Prerequisites

- Deno 2.9.6 or newer
- Rust 1.85.1 or newer
- `libclang` and `pkg-config`
- FoundationDB 7.4 client library (`libfdb_c`)
- A FoundationDB cluster for integration tests and application use

On Debian, install the matching client package from the
[FoundationDB 7.4.6 release](https://github.com/apple/foundationdb/releases/tag/7.4.6):

```sh
curl -fLO https://github.com/apple/foundationdb/releases/download/7.4.6/foundationdb-clients_7.4.6-1_amd64.deb
sudo apt-get install -y ./foundationdb-clients_7.4.6-1_amd64.deb libclang-dev pkg-config
```

Install dependencies and build the native bridge:

```sh
deno ci
cargo build --release
```

Linux applications should then use
`target/release/libeffect_foundationdb_native.so` as `libraryPath`. The dynamic
library suffix is `.dylib` on macOS and `.dll` on Windows.

## Usage

```ts
import { Effect } from "effect";
import { FoundationDb, FoundationDbTransaction, keyRange } from "./mod.ts";

const encoder = new TextEncoder();
const key = encoder.encode("users/alice");
const value = encoder.encode('{"name":"Alice"}');

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;

  yield* database.set(key, value);
  const stored = yield* database.get(key);

  const users = yield* database.getRange(
    keyRange(encoder.encode("users/"), encoder.encode("users0")),
  );

  return { stored, users };
});

const FoundationDbLive = FoundationDb.layer({
  libraryPath: "./target/release/libeffect_foundationdb_native.so",
  clusterFile: "/etc/foundationdb/fdb.cluster",
  transactionDefaults: { timeoutMs: 5_000, retryLimit: 3 },
});

await Effect.runPromise(program.pipe(Effect.provide(FoundationDbLive)));
```

Deno needs `--allow-ffi` and read permission for the native library and cluster
file.

The [examples](./examples/README.md) directory contains Effect/Deno ports of all
13 programs shipped with `foundationdb-rs`.

### Transactions

`withTransaction` provides `FoundationDbTransaction` to an Effect and follows
FoundationDB's required retry protocol. A failed operation or commit is passed
to the native transaction's `on_error`; that API decides whether to delay and
retry or return a terminal error. Generic `Effect.retry` is not used because it
would lose FoundationDB's transaction state.

`transactionDefaults` on `FoundationDb.layer` establishes the policy for every
transaction. Options supplied to `withTransaction` or a convenience method
override individual default fields.

```ts
const updateValue = database.withTransaction(
  Effect.gen(function* () {
    // This Effect can run more than once.
    const transaction = yield* FoundationDbTransaction;
    if (transaction.maybeCommitted) {
      // A prior commit may have succeeded even though its result was unknown.
    }
    const current = yield* transaction.get(key);
    yield* transaction.set(key, update(current));
    return current;
  }),
  { timeoutMs: 5_000, retryLimit: 10, maxRetryDelayMs: 1_000 },
);
```

For many point reads, use `getMany` rather than creating one Effect and one FFI
call per key. It preserves input order, represents missing values as
`undefined`, and performs the reads concurrently within one transaction:

```ts
const values = yield * database.getMany(keys, { snapshot: true });
```

`FoundationDbError` is a schema-backed tagged error with the native error code
and FoundationDB classifications: `retryable`, `maybeCommitted`, and
`retryableNotCommitted`. A transaction retries only a cause consisting of one
`FoundationDbError`. Domain failures, defects, interruptions, and composite
causes are preserved without retrying.

Each transaction attempt has a fresh `Scope`, which closes attempt-local
resources before a retry or commit. Transaction and range resources use
`Effect.acquireUseRelease` and `Effect.acquireRelease`, so interruption, typed
failures, defects, and early stream termination all close their native handles.
Unexpected native close failures become defects instead of being discarded.

FoundationDB telemetry follows the client/server boundary. Asynchronous reads,
range pages, commits, and `on_error` calls produce client spans. `getMany`
produces one span with `db.foundationdb.read.key_count`, rather than one span
per key. Synchronous mutations only update FoundationDB's local transaction
cache, so they do not produce one span per key; transaction and attempt spans
instead report `db.foundationdb.mutation.count` and
`db.foundationdb.mutation.bytes`. The byte count measures mutation input keys,
values, and range boundaries, not the server's approximate transaction size.
Retries are sibling attempt spans under the logical transaction span, which
reports the total attempt and retry counts.

### Tuple and directory layers

The tuple and directory layers are implemented in TypeScript rather than
wrapping the Rust directory API. Their bytes and metadata follow FoundationDB's
canonical formats, so other FoundationDB clients can open the same directories.
Rust FFI is used only for primitive transaction operations, including the atomic
mutation and conflict-range controls required by the directory layer's
high-contention allocator.

Tuple bytes can be decoded directly into an Effect Schema. Malformed tuple bytes
fail with `TupleError`; a decoded tuple that does not match the requested shape
fails with `SchemaError`:

```ts
import { Schema } from "effect";
import { unpack } from "./mod.ts";

const UserKey = Schema.Tuple([Schema.String, Schema.BigInt]);
const [name, id] = yield* unpack(encodedKey, UserKey);

// Subspaces validate their prefix before decoding the tuple suffix.
const [childName] = yield* subspace.unpack(
  childKey,
  Schema.Tuple([Schema.String]),
);
```

Directory operations require `FoundationDbTransaction` in their Effect
environment. `FoundationDb.withTransaction` supplies that service, so directory
metadata and application writes can share one retried transaction:

```ts
import { Effect } from "effect";
import {
  DirectoryLayer,
  DirectorySubspace,
  FoundationDb,
  FoundationDbTransaction,
} from "./mod.ts";

const encoder = new TextEncoder();

const directoryProgram = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const directories = yield* DirectoryLayer.make();

  return yield* database.withTransaction(Effect.gen(function* () {
    const users = yield* directories.createOrOpen(
      ["application", "users"],
      { layer: encoder.encode("users") },
    );
    if (!(users instanceof DirectorySubspace)) {
      return yield* Effect.dieMessage("users is unexpectedly a partition");
    }

    const transaction = yield* FoundationDbTransaction;
    const key = yield* users.pack(["alice", 1n]);
    yield* transaction.set(key, encoder.encode("Alice"));
    return users;
  }));
});
```

`DirectoryLayer` supports create, open, list, move, recursive remove, custom
prefixes, nested partitions, version checks, and concurrent automatic prefix
allocation. Directory policy failures use the schema-backed `DirectoryError`;
native `FoundationDbError`s remain unchanged so `withTransaction` can apply
FoundationDB's retry protocol.

The tuple API covers every element implemented by `foundationdb-tuple` 0.11:
null, byte strings, strict UTF-8 strings, nested tuples, arbitrary signed
integers, binary32, binary64, booleans, UUIDs, and 12-byte versionstamps. Use
`bigint` for integers, `number` for binary64, `Float32` for binary32, and
`Uint8Array` for byte strings. The tuple value wrappers and `Subspace` are
immutable and support Effect's equality/hash protocols.

`packWithVersionstamp` and `Subspace.packWithVersionstamp` append the required
little-endian offset when exactly one incomplete versionstamp is present. A
tuple-created subspace preserves versionstamp metadata through child subspaces;
`Subspace.fromBytes` intentionally treats its prefix as opaque bytes. `compare`
uses FoundationDB's unsigned wire ordering. The tuple API does not implement
decimal, the unused 80-bit versionstamp code, or reserved/user-defined codes,
which `foundationdb-tuple` itself does not support.

## API

`FoundationDb` provides:

- `withTransaction`, which satisfies the `FoundationDbTransaction` dependency
- transactional and convenience `get`, `set`, `clear`, and `clearRange`
- incremental transactional `getRange` streams
- a convenience `database.getRange` effect that returns the complete retriable
  read as an array; process `transaction.getRange` inside `withTransaction` when
  incremental delivery matters
- key selectors through `KeySelector`
- transaction timeout, retry-limit, and maximum-retry-delay options
- range limit, target-byte, streaming-mode, reverse, and snapshot options

Keys and values are binary `Uint8Array`s. A missing value is `undefined`; an
empty value is an empty `Uint8Array`.

## Effect PersistedQueue backend

`layerPersistedQueueStore` implements Effect's unstable
`PersistedQueue.PersistedQueueStore` contract. Compose it with the FoundationDB
layer and Effect's queue factory layer:

```ts
import { Effect, Layer, Schema } from "effect";
import { PersistedQueue } from "effect/unstable/persistence";
import { FoundationDb, layerPersistedQueueStore } from "./mod.ts";

const foundationDbLayer = FoundationDb.layer({
  libraryPath: "./target/release/libeffect_foundationdb_native.so",
});
const storeLayer = layerPersistedQueueStore().pipe(
  Layer.provideMerge(foundationDbLayer),
);
const applicationLayer = PersistedQueue.layer.pipe(
  Layer.provideMerge(storeLayer),
);

const program = Effect.gen(function* () {
  const queue = yield* PersistedQueue.make({
    name: "email-delivery",
    schema: Schema.Struct({ recipient: Schema.String }),
    maxAttempts: 10,
  });

  yield* queue.offer(
    { recipient: "welcome@example.com" },
    { id: "welcome:user-123" },
  );
  yield* queue.take((message, { attempts, id }) =>
    Effect.log("deliver", message.recipient, { attempts, id })
  );
});

await Effect.runPromise(program.pipe(Effect.provide(applicationLayer)));
```

The backend creates a root at the Directory Layer path
`["effect-foundationdb", "persisted-queue"]` and represents every named queue as
a child directory. Canonical tuple subspaces encode counters, entries,
de-duplication IDs, claims, and ordered state indexes. Transactions provide
custom-ID de-duplication and exclusive claims across workers. Scoped claim
leases are refreshed during long handlers; expired leases are recovered after
worker crashes. Handler failures use `PersistedQueue`'s retry schedule,
interruptions release a claim without consuming an attempt, and exhausted or
undecodable entries are retained as failed records.

Completed IDs remain de-duplicated until `PersistedQueue.layerCleanup` calls the
store's TTL cleanup. Delivery is at least once, so handlers must be idempotent.
`directory`, `directoryPath`, `pollInterval`, `lockRefreshInterval`,
`lockExpiration`, and `transactionOptions` can be configured through
`layerPersistedQueueStore`. Supplying a custom `Directory` places the backend in
an application-owned directory layer or partition. Queue transactions default to
a 5-second timeout, 10 retries, and a 1-second maximum retry delay, so an
interrupted claim cannot remain stuck behind an unbounded FoundationDB retry.

## Development

The repository includes the Effect Solutions quick-start configuration and
language-service plugin. The `@types/deno` development dependency exists only
for the patched standalone TypeScript language service; Deno itself and
`deno check` use the runtime's built-in declarations. Run these checks from the
repository root:

```sh
deno task check:all       # formatting, lint, types, Deno unit tests, Rust tests and clippy
deno task test:integration # requires a local cluster and /etc/foundationdb/fdb.cluster
```

The live integration test builds the debug bridge and covers binary and empty
values, missing values, read-your-writes, key selectors, range order and limits,
clears, and Effect `PersistedQueue` de-duplication, retries, and concurrent
workers. It runs as one test because the FoundationDB network cannot restart
inside one process.
