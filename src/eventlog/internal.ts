import { Effect, Option, Schema, Stream } from "effect";
import * as SchemaBinary from "effect/encoding/SchemaBinary";
import * as Journal from "effect/eventlog/EventJournal";
import { FoundationDb, FoundationDbTransaction } from "../FoundationDb.ts";
import { isFoundationDbError } from "../errors.ts";
import {
  readChunkedValue,
  writeChunkedValue,
} from "../internal/chunked-value.ts";
import { keyRange, MutationType } from "../model.ts";
import { makeDirectoryStore } from "../persistence/internal.ts";
import type { DirectoryStoreOptions } from "../persistence/internal.ts";
import type { Tuple } from "../tuple/mod.ts";

/**
 * Shared durable backend configuration. Each backend has a separate default
 * directory under `effect-foundationdb/eventlog` and a versioned directory layer.
 *
 * Writes and callback units are atomic, not automatically split into multiple
 * transactions: callers must stay within FDB's transaction size/time limits.
 * Index reads are paged; array-returning APIs and backlog compaction still
 * accumulate their complete result in memory. Consume change streams outside
 * callback transactions, so each page can see a fresh committed snapshot and
 * idle watches can commit before waiting for appends.
 */
export interface EventLogStoreOptions extends DirectoryStoreOptions {
  /** Maximum index rows fetched per page. Defaults to 100. */
  readonly pageSize?: number;
  /** Delay before rereading after a failed FDB watch. Defaults to 100 ms. */
  readonly watchRetryDelayMs?: number;
  /**
   * Callback transactions default to zero retries and never retry ambiguous
   * commits. Opt in only when callbacks are safe to re-execute. FDB mutations
   * roll back; external effects do not. Nested operations reuse the ambient
   * FoundationDbTransaction (which must belong to the same database). There are
   * no nested savepoints: propagate failures to the outer transaction boundary.
   */
  readonly callbackRetryLimit?: number;
}

export const binary = <A, I>(schema: Schema.Codec<A, I>) =>
  SchemaBinary.toCodec(schema);
export const EntryCodec = binary(Journal.Entry);
export const RemoteEntryCodec = binary(Journal.RemoteEntry);
const NumberCodec = binary(Schema.Natural);
const BytesCodec = binary(Schema.Uint8Array);
const RemoteIdCodec = binary(Journal.RemoteId);
const decodeOptions = Schema.decodeUnknownEffect(Schema.Struct({
  pageSize: Schema.Int.check(Schema.isGreaterThan(0)),
  watchRetryDelayMs: Schema.Finite.check(Schema.isGreaterThan(0)),
  callbackRetryLimit: Schema.Natural,
}));

export const protect = <A, E, R>(
  method: string,
  effect: Effect.Effect<A, E, R>,
) =>
  Effect.mapError(
    effect,
    (cause) => new Journal.EventJournalError({ method, cause }),
  );

export const makeRepository = Effect.fnUntraced(function* (
  options: EventLogStoreOptions,
  kind: string,
) {
  const { pageSize, watchRetryDelayMs, callbackRetryLimit } = yield* protect(
    "make",
    decodeOptions({
      pageSize: options.pageSize ?? 100,
      watchRetryDelayMs: options.watchRetryDelayMs ?? 100,
      callbackRetryLimit: options.callbackRetryLimit ?? 0,
    }),
  );
  const db = yield* FoundationDb;
  const directory = yield* makeDirectoryStore(options, [
    "effect-foundationdb",
    "eventlog",
    kind,
  ], `effect-foundationdb/eventlog/${kind}/v1`);
  const root = yield* protect(
    "directory",
    db.withTransaction(directory.root(), directory.transactionOptions),
  );

  // Unwrap storage-operation failures before passing them to FoundationDB, so
  // retryable read errors receive the same retry policy as commit conflicts.
  const retryable = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.mapError(
      effect,
      (error) =>
        error instanceof Journal.EventJournalError &&
          isFoundationDbError(error.cause)
          ? error.cause
          : error,
    );

  // User callback failures retain identity; only native FDB errors are translated.
  const transaction = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    callback = false,
  ) =>
    Effect.flatMap(
      Effect.serviceOption(FoundationDbTransaction),
      (
        current,
      ): Effect.Effect<
        A,
        E | Journal.EventJournalError,
        Exclude<R, FoundationDbTransaction>
      > =>
        Option.isSome(current)
          ? Effect.provideService(
            effect,
            FoundationDbTransaction,
            current.value,
          )
          : db.withTransaction(retryable(effect), {
            ...directory.transactionOptions,
            // A replay can lose the write's assigned sequence result or turn a
            // successful insert into a duplicate. Surface uncertain commits.
            retryOnMaybeCommitted: false,
            ...(callback
              ? {
                retryLimit: callbackRetryLimit,
              }
              : {}),
          }).pipe(Effect.catchIf(isFoundationDbError, (cause) =>
            Effect.fail(
              new Journal.EventJournalError({ method: "transaction", cause }),
            ))),
    );

  const withTransaction = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.flatMap(
      Effect.serviceOption(FoundationDbTransaction),
      (current): Effect.Effect<A, E, Exclude<R, FoundationDbTransaction>> =>
        Option.isSome(current)
          ? Effect.provideService(
            effect,
            FoundationDbTransaction,
            current.value,
          )
          : db.withTransaction(retryable(effect), {
            ...directory.transactionOptions,
            retryLimit: callbackRetryLimit,
            retryOnMaybeCommitted: false,
          }).pipe(Effect.catchIf(isFoundationDbError, Effect.die)),
    );

  const get = <A>(
    path: Tuple,
    codec: Schema.Codec<A, Uint8Array<ArrayBuffer>>,
  ) =>
    protect(
      "read",
      Effect.gen(function* () {
        const tx = yield* FoundationDbTransaction;
        const bytes = yield* readChunkedValue(tx, root, ["values", ...path]);
        return bytes === undefined
          ? undefined
          : yield* Schema.decodeUnknownEffect(codec)(bytes);
      }),
    );
  const put = <A>(
    path: Tuple,
    codec: Schema.Codec<A, Uint8Array<ArrayBuffer>>,
    value: A,
  ) =>
    protect(
      "write",
      Effect.gen(function* () {
        const tx = yield* FoundationDbTransaction;
        const bytes = yield* Schema.encodeEffect(codec)(value);
        yield* writeChunkedValue(tx, root, ["values", ...path], bytes);
      }),
    );
  const number = (path: Tuple, fallback = 0) =>
    Effect.map(get(path, NumberCodec), (n) => n ?? fallback);
  const setNumber = (path: Tuple, value: number) =>
    put(path, NumberCodec, value);
  const mark = Effect.fnUntraced(function* (path: Tuple) {
    const tx = yield* FoundationDbTransaction;
    yield* tx.set(yield* root.pack(["index", ...path]), new Uint8Array());
  }, (effect) => protect("index", effect));
  const has = Effect.fnUntraced(function* (path: Tuple) {
    const tx = yield* FoundationDbTransaction;
    return (yield* tx.get(yield* root.pack(["index", ...path]))) !== undefined;
  }, (effect) => protect("index", effect));
  const hasMany = Effect.fnUntraced(function* (paths: ReadonlyArray<Tuple>) {
    const tx = yield* FoundationDbTransaction;
    const result: Array<boolean> = [];
    for (let offset = 0; offset < paths.length; offset += pageSize) {
      const keys = yield* Effect.forEach(
        paths.slice(offset, offset + pageSize),
        (path) => root.pack(["index", ...path]),
      );
      result.push(
        ...(yield* tx.getMany(keys)).map((value) => value !== undefined),
      );
    }
    return result;
  }, (effect) => protect("index", effect));
  const page = Effect.fnUntraced(
    function* (path: Tuple, after?: Uint8Array, from?: Tuple) {
      const tx = yield* FoundationDbTransaction;
      const [begin, end] = yield* root.range(["index", ...path]);
      const rows = yield* Stream.runCollect(tx.getRange(keyRange(
        after === undefined
          ? (from === undefined
            ? begin
            : yield* root.pack(["index", ...path, ...from]))
          : new Uint8Array([...after, 0]),
        end,
        { limit: pageSize },
      )));
      const paths = yield* Effect.forEach(rows, (row) => root.unpack(row.key));
      return {
        paths: paths.map((p) => p.slice(path.length + 1)),
        cursor: rows.at(-1)?.key,
      };
    },
    (effect) => protect("page", effect),
  );
  const all = Effect.fnUntraced(function* (path: Tuple, from?: Tuple) {
    const paths: Array<Tuple> = [];
    let cursor: Uint8Array | undefined;
    while (true) {
      const batch = yield* page(path, cursor, from);
      paths.push(...batch.paths);
      if (batch.paths.length < pageSize) return paths;
      cursor = batch.cursor;
    }
  });
  const required = <A>(
    path: Tuple,
    codec: Schema.Codec<A, Uint8Array<ArrayBuffer>>,
  ) =>
    Effect.flatMap(
      get(path, codec),
      (value) =>
        value === undefined
          ? Effect.fail(
            new Journal.EventJournalError({
              method: "read",
              cause: "Missing indexed event-log record",
            }),
          )
          : Effect.succeed(value),
    );
  const readMany = <A>(
    paths: ReadonlyArray<Tuple>,
    codec: Schema.Codec<A, Uint8Array<ArrayBuffer>>,
  ) =>
    Effect.forEach(paths, (path) => required(path, codec), { concurrency: 8 });

  const getId = transaction(Effect.gen(function* () {
    const id = yield* get(["remoteId"], RemoteIdCodec);
    if (id !== undefined) return id;
    const created = Journal.makeRemoteIdUnsafe();
    yield* put(["remoteId"], RemoteIdCodec, created);
    return created;
  }));
  const bindAuth = Effect.fnUntraced(
    function* (publicKey: string, key: Uint8Array<ArrayBuffer>) {
      const existing = yield* get(["auth", publicKey], BytesCodec);
      if (existing !== undefined) return new Uint8Array(existing);
      yield* put(["auth", publicKey], BytesCodec, key);
      return key.slice();
    },
  );
  const auth = (publicKey: string, key: Uint8Array<ArrayBuffer>) =>
    transaction(bindAuth(publicKey, key));

  // Changed atomically with an append. A counter avoids lost wakeups from
  // identical payloads without introducing a read-modify-write conflict key.
  const notify = Effect.fnUntraced(function* (path: Tuple) {
    const tx = yield* FoundationDbTransaction;
    yield* tx.atomicOp(
      yield* root.pack(["signal", ...path]),
      Uint8Array.of(1, 0, 0, 0, 0, 0, 0, 0),
      MutationType.Add,
    );
  }, (effect) => protect("notify", effect));

  // Reading an empty page and registering its watch share a transaction. A
  // racing append either conflicts or wakes the committed watch; no polling
  // handshake can lose it. The watch belongs to this one stream pull's scope.
  const tail = <A, Cursor>(
    path: Tuple,
    start: Cursor,
    read: (cursor: Cursor) => Effect.Effect<
      readonly [ReadonlyArray<A>, Option.Option<Cursor>],
      Journal.EventJournalError,
      FoundationDbTransaction
    >,
  ) =>
    Stream.paginate(start, (next) =>
      Effect.scoped(Effect.gen(function* () {
        if (
          Option.isSome(yield* Effect.serviceOption(FoundationDbTransaction))
        ) {
          return yield* new Journal.EventJournalError({
            method: "changes",
            cause: "Consume change streams outside a transaction",
          });
        }
        const { output, next: cursor, wake } = yield* Effect.acquireRelease(
          transaction(Effect.gen(function* () {
            const [output, cursor] = yield* read(next);
            const tx = yield* FoundationDbTransaction;
            return {
              output,
              next: cursor,
              wake: output.length === 0 && Option.isSome(cursor)
                ? yield* tx.watch(yield* root.pack(["signal", ...path]))
                : undefined,
            };
          })),
          ({ wake }) => wake?.cancel ?? Effect.void,
        );
        if (wake !== undefined) {
          yield* wake.await.pipe(
            Effect.catch(() => Effect.sleep(watchRetryDelayMs)),
          );
        }
        return [output, cursor] as const;
      })));

  const feed = <A>(
    path: Tuple,
    codec: Schema.Codec<A, Uint8Array<ArrayBuffer>>,
    start: number,
    until?: number,
  ) =>
    tail(path, start, (next) =>
      Effect.gen(function* () {
        const batch = yield* page(path, undefined, [BigInt(next)]);
        const paths = batch.paths.filter((suffix) =>
          until === undefined || Number(suffix[0]) < until
        );
        const output = yield* readMany(
          paths.map((suffix) => [...path, ...suffix]),
          codec,
        );
        const last = paths.at(-1);
        const cursor = last === undefined ? next : Number(last[0]) + 1;
        return [
          output,
          until !== undefined && (output.length === 0 || cursor >= until)
            ? Option.none<number>()
            : Option.some(cursor),
        ] as const;
      }));

  const clear = protect(
    "destroy",
    Effect.gen(function* () {
      const tx = yield* FoundationDbTransaction;
      // Keep watch counters monotonic. Destroy + append in one transaction must
      // not reset a signal to its previous value and leave a subscriber asleep.
      yield* tx.clearRange(...yield* root.range(["index"]));
      yield* tx.clearRange(...yield* root.range(["values"]));
    }),
  );
  return {
    root,
    pageSize,
    transaction,
    withTransaction,
    get,
    put,
    required,
    readMany,
    number,
    setNumber,
    mark,
    has,
    hasMany,
    page,
    all,
    feed,
    tail,
    notify,
    getId,
    auth,
    clear,
  };
});
