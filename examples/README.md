# Examples

These are Effect/Deno ports of the 13 programs in
[`foundationdb-rs/foundationdb/examples`](https://github.com/foundationdb-rs/foundationdb-rs/tree/daa6640d2d48f7c041c8aa57660dbdce2b618d8b/foundationdb/examples).
They use `FoundationDb` as an Effect service and `FoundationDbTransaction` as
the transaction-scoped dependency.

Build the native bridge, start a local FoundationDB server, and run an example
from the repository root:

```sh
cargo build
deno run --allow-ffi --allow-read examples/hello-world.ts
```

`_shared.ts` expects the debug bridge built by `cargo build`. It selects the
platform extension automatically. The examples use the default FoundationDB
cluster file and apply a 5-second timeout with three retries.

Set `EFFECT_FOUNDATIONDB_OTLP_ENDPOINT` to export Effect traces and logs over
OTLP/HTTP. For example, with GoTel listening on its default port:

```sh
EFFECT_FOUNDATIONDB_OTLP_ENDPOINT=http://127.0.0.1:27686 \
  deno run --allow-ffi --allow-read --allow-env \
  --allow-net=127.0.0.1:27686 examples/hello-world.ts
```

Each example uses a distinct `effect-foundationdb-example-*` service name.
Metrics are not exported because GoTel does not expose an OTLP metrics endpoint.

| Rust example            | TypeScript port         | Notes                                                                                                                     |
| ----------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `atomic-op-counter.rs`  | `atomic-op-counter.ts`  | Native little-endian atomic add                                                                                           |
| `blob.rs`               | `blob.ts`               | 100 random 10 KB chunked round trips                                                                                      |
| `blob-with-manifest.rs` | `blob-with-manifest.ts` | Uses a generated payload instead of the upstream image asset; verifies all chunk sizes, including the default             |
| `budgeted_scan.rs`      | `budgeted_scan.ts`      | Uses application-observed row bytes and elapsed time because native client-budget accounting is not exposed               |
| `class-scheduling.rs`   | `class-scheduling.ts`   | Typed domain errors and 10 concurrent Effect fibers sharing one scoped native layer                                       |
| `conflict_reporting.rs` | `conflict_reporting.ts` | Produces a real retryable conflict and exposes `transaction.attempt`; native conflict-range hooks/reports are not exposed |
| `hello-world.rs`        | `hello-world.ts`        | Direct port                                                                                                               |
| `instrumented.rs`       | `instrumented.ts`       | Uses Effect spans and metrics; native `TransactionMetrics` is not exposed                                                 |
| `key_selectors.rs`      | `key_selectors.ts`      | All 20 selector scenarios with exact equality checks; corrects the upstream `(a,true,2)` to `(g,false,0)` expectation     |
| `micro-queue.rs`        | `micro-queue.ts`        | Preserves the upstream reverse-range behavior, so it drains last-in-first-out despite the upstream FIFO description       |
| `multi_version.rs`      | `multi_version.ts`      | Ports the counter operation; external-client multi-version network configuration and read-version warmup are not exposed  |
| `simple-index.rs`       | `simple-index.ts`       | Direct tuple/subspace index port                                                                                          |
| `versionstamp.rs`       | `versionstamp.ts`       | Demonstrates complete tuple versionstamp ordering and references; server-filled versionstamped mutations are not exposed  |

Run `deno task check:examples` to type-check every example without connecting to
FoundationDB.
