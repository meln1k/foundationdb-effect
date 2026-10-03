import { Effect, Schema, Stream } from "effect";
import * as Journal from "effect/eventlog/EventJournal";
import { FoundationDbTransaction } from "../FoundationDb.ts";
import { keyRange, MutationType } from "../model.ts";
import { Versionstamp } from "../tuple/mod.ts";
import type { Subspace } from "../tuple/mod.ts";
import { protect } from "./internal.ts";

interface Pending {
  next: number;
  readonly ids: Array<Journal.EntryId>;
}

// Share pending order across adapters using the same transaction and directory.
// A fresh transaction service on each retry isolates failed attempts.
const attempts = new WeakMap<
  FoundationDbTransaction["Service"],
  Map<string, Pending>
>();
const OrderedId = Schema.Tuple([Versionstamp.schema, Journal.EntryId]);
const afterKey = (key: Uint8Array) => new Uint8Array([...key, 0]);

const committedEnd = Effect.gen(function* () {
  const tx = yield* FoundationDbTransaction;
  const version = yield* tx.getReadVersion();
  const bytes = new Uint8Array(10);
  new DataView(bytes.buffer).setBigUint64(0, version + 1n);
  return yield* Versionstamp.complete(bytes, 0);
});

export const makeJournalOrder = Effect.fnUntraced(function* (
  root: Subspace,
  pageSize: number,
) {
  const inserted = yield* root.subspace(["index", "inserted"]);
  const local = yield* root.subspace(["index", "local"]);
  const namespace = root.prefix.join(",");
  const pending = Effect.gen(function* () {
    const tx = yield* FoundationDbTransaction;
    let roots = attempts.get(tx);
    if (roots === undefined) attempts.set(tx, roots = new Map());
    let state = roots.get(namespace);
    if (state === undefined) roots.set(namespace, state = { next: 0, ids: [] });
    return state;
  });

  const append = Effect.fnUntraced(function* (
    id: Journal.EntryId,
    isLocal: boolean,
  ) {
    const tx = yield* FoundationDbTransaction;
    // Establish R before versionstamped mutations: their unreadable key range
    // then starts at R+1, above every committed entry visible in this attempt.
    yield* tx.getReadVersion();
    const state = yield* pending;
    const stamp = yield* Versionstamp.incomplete(state.next++);
    for (const space of isLocal ? [inserted, local] : [inserted]) {
      yield* tx.atomicOp(
        yield* space.packWithVersionstamp([stamp, id]),
        new Uint8Array(),
        MutationType.SetVersionstampedKey,
      );
    }
    state.ids.push(id);
  }, (effect) => protect("order.append", effect));

  const page = Effect.fnUntraced(function* (
    space: Subspace,
    begin: Uint8Array,
    end: Uint8Array,
  ) {
    const tx = yield* FoundationDbTransaction;
    const rows = yield* Stream.runCollect(
      tx.getRange(keyRange(begin, end, { limit: pageSize })),
    );
    const ids = yield* Effect.forEach(
      rows,
      (row) => Effect.map(space.unpack(row.key, OrderedId), ([, id]) => id),
    );
    return { ids, cursor: rows.at(-1)?.key };
  });

  const unsent = Effect.fnUntraced(function* (after: Uint8Array | undefined) {
    const [start] = yield* inserted.range();
    const end = yield* inserted.pack([yield* committedEnd]);
    let begin = after === undefined ? start : afterKey(after);
    let cursor: Uint8Array | undefined;
    const ids: Array<Journal.EntryId> = [];
    while (true) {
      const batch = yield* page(inserted, begin, end);
      ids.push(...batch.ids);
      if (batch.cursor !== undefined) cursor = batch.cursor;
      if (batch.ids.length < pageSize) break;
      begin = afterKey(batch.cursor!);
    }
    ids.push(...(yield* pending).ids);
    // Never advance to a pending stamp: another writer can commit after our
    // snapshot but before us. ACK pending IDs, then skip them on a later scan.
    return { ids, cursor };
  }, (effect) => protect("order.unsent", effect));

  const start = Effect.gen(function* () {
    return yield* local.pack([yield* committedEnd]);
  }).pipe((effect) => protect("order.start", effect));

  // Only used by the committed change feed, outside ambient transactions.
  const localPage = Effect.fnUntraced(function* (begin: Uint8Array) {
    const [, end] = yield* local.range();
    const batch = yield* page(local, begin, end);
    return {
      ids: batch.ids,
      next: batch.cursor === undefined ? begin : afterKey(batch.cursor),
    };
  }, (effect) => protect("order.local", effect));

  const clearPending = Effect.gen(function* () {
    (yield* pending).ids.length = 0;
  });
  return { append, unsent, start, localPage, clearPending };
}, (effect) => protect("order.make", effect));
