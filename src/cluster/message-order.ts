import { Effect, Schema, Stream } from "effect";
import type { FoundationDbTransaction } from "../FoundationDb.ts";
import { keyRange, MutationType } from "../model.ts";
import { compareBytes } from "../tuple/bytes.ts";
import { packWithVersionstamp, unpack, Versionstamp } from "../tuple/mod.ts";
import type { Subspace } from "../tuple/mod.ts";
import { MissingMessageRecordError } from "./errors.ts";
import type { MessageSubspaces } from "./message-subspaces.ts";

interface Store {
  readonly transaction: FoundationDbTransaction["Service"];
  readonly spaces: Pick<MessageSubspaces, "orders">;
}
interface Pending {
  readonly id: string;
  readonly stamp: Versionstamp;
  readonly indexes: ReadonlyArray<Subspace>;
  indexed: boolean;
}
interface Attempt {
  next: number;
  readonly roots: Map<string, Map<string, Pending>>;
}

// The service object is fresh for every FDB attempt. A WeakMap shares pending
// writes across independently constructed adapters, without leaking retries.
const attempts = new WeakMap<FoundationDbTransaction["Service"], Attempt>();
const pendingFor = (store: Store) => {
  let attempt = attempts.get(store.transaction);
  if (attempt === undefined) {
    attempt = { next: 0, roots: new Map() };
    attempts.set(store.transaction, attempt);
  }
  const namespace = store.spaces.orders.prefix.join(",");
  let pending = attempt.roots.get(namespace);
  if (pending === undefined) {
    pending = new Map();
    attempt.roots.set(namespace, pending);
  }
  return { attempt, pending };
};
const Stamp = Schema.Tuple([Versionstamp.schema]);
const OrderedId = Schema.Tuple([Versionstamp.schema, Schema.String]);
const empty = new Uint8Array();
const pageSize = 64;

const writeIndex = Effect.fnUntraced(
  function* (store: Store, space: Subspace, entry: Pending) {
    yield* store.transaction.atomicOp(
      yield* space.packWithVersionstamp([entry.stamp, entry.id]),
      empty,
      MutationType.SetVersionstampedKey,
    );
  },
);

// Acquire the read version before *any* versionstamped mutation. Its unreadable
// key interval then starts at R+1. Committed scans stop before that interval.
const committedEndStamp = Effect.fnUntraced(function* (store: Store) {
  const version = yield* store.transaction.getReadVersion();
  const bytes = new Uint8Array(10);
  new DataView(bytes.buffer).setBigUint64(0, version + 1n);
  return yield* Versionstamp.complete(bytes, 0);
});

export const insertMessageOrder = Effect.fnUntraced(function* (
  store: Store,
  id: string,
  indexes: ReadonlyArray<Subspace>,
  indexed: boolean,
) {
  yield* store.transaction.getReadVersion();
  const { attempt, pending } = pendingFor(store);
  const stamp = yield* Versionstamp.incomplete(attempt.next++);
  const entry = { id, indexes, stamp, indexed };
  pending.set(id, entry);
  if (indexed) {
    for (const index of indexes) yield* writeIndex(store, index, entry);
  }
  yield* store.transaction.atomicOp(
    yield* store.spaces.orders.pack([id]),
    yield* packWithVersionstamp([stamp]),
    MutationType.SetVersionstampedValue,
  );
});

export const messageOrder = Effect.fnUntraced(
  function* (store: Store, id: string) {
    const pending = pendingFor(store).pending.get(id);
    if (pending !== undefined) return pending.stamp;
    const value = yield* store.transaction.get(
      yield* store.spaces.orders.pack([id]),
    );
    if (value === undefined) {
      return yield* new MissingMessageRecordError({ record: "order", id });
    }
    return (yield* unpack(value, Stamp))[0];
  },
);

/** Change polling membership without changing the message's original order. */
export const setMessageOrderIndexed = Effect.fnUntraced(function* (
  store: Store,
  id: string,
  indexes: ReadonlyArray<Subspace>,
  indexed: boolean,
) {
  const { pending } = pendingFor(store);
  const entry = pending.get(id);
  if (entry !== undefined) {
    if (entry.indexed === indexed) return;
    entry.indexed = indexed;
    if (indexed) {
      for (const space of indexes) yield* writeIndex(store, space, entry);
      return;
    }
    const boundary = yield* committedEndStamp(store);
    // The pending key's final stamp is unknown. Clear the uncommitted tail and
    // reissue its survivors. A read conflict protects concurrent commits in
    // that tail from being silently erased; visible committed rows are below it.
    for (const space of indexes) {
      const begin = yield* space.pack([boundary]);
      const [, end] = yield* space.range();
      yield* store.transaction.addReadConflictRange(begin, end);
      yield* store.transaction.clearRange(begin, end);
      for (const entry of pending.values()) {
        if (
          entry.indexed &&
          entry.indexes.some((candidate) => candidate.equals(space))
        ) {
          yield* writeIndex(store, space, entry);
        }
      }
    }
  } else {
    const stamp = yield* messageOrder(store, id);
    for (const space of indexes) {
      const key = yield* space.pack([stamp, id]);
      if (indexed) yield* store.transaction.set(key, empty);
      else yield* store.transaction.clear(key);
    }
  }
});

export const removeMessageOrder = Effect.fnUntraced(function* (
  store: Store,
  id: string,
  indexes: ReadonlyArray<Subspace>,
) {
  yield* setMessageOrderIndexed(store, id, indexes, false);
  pendingFor(store).pending.delete(id);
  yield* store.transaction.clear(yield* store.spaces.orders.pack([id]));
});

// Merge paged index cursors into bounded candidate windows. The caller selects
// and claims eligible messages in order, never claiming the whole prefetch.
export const visitMessageOrder = Effect.fnUntraced(function* <E>(
  store: Store,
  indexes: ReadonlyArray<Subspace>,
  visit: (ids: ReadonlyArray<string>) => Effect.Effect<boolean, E>,
) {
  let batch: Array<string> = [];
  const append = Effect.fnUntraced(function* (id: string) {
    batch.push(id);
    if (batch.length < pageSize) return true;
    const more = yield* visit(batch);
    batch = [];
    return more;
  });
  const boundary = yield* committedEndStamp(store);
  const spaces = new Map<string, Subspace>();
  for (const space of indexes) {
    spaces.set(space.prefix.join(","), space);
  }
  const cursors = yield* Effect.forEach(
    spaces.values(),
    Effect.fnUntraced(function* (space) {
      const [start] = yield* space.range();
      const end = yield* space.pack([boundary]);
      let begin = start;
      let rows: Array<readonly [Versionstamp, string]> = [];
      let position = 0;
      let done = false;
      const next = Effect.gen(function* () {
        if (position === rows.length) {
          if (done) return undefined;
          const batch = yield* Stream.runCollect(
            store.transaction.getRange(
              keyRange(begin, end, { limit: pageSize }),
            ),
          );
          rows = yield* Effect.forEach(
            batch,
            (row) => space.unpack(row.key, OrderedId),
          );
          position = 0;
          done = batch.length < pageSize;
          if (batch.length > 0) {
            begin = new Uint8Array([...batch[batch.length - 1].key, 0]);
          }
        }
        return rows[position++];
      });
      return { next, head: yield* next };
    }),
  );
  while (true) {
    let selected: typeof cursors[number] | undefined;
    for (const cursor of cursors) {
      if (
        cursor.head !== undefined && (selected === undefined ||
          compareBytes(cursor.head[0].bytes, selected.head![0].bytes) < 0)
      ) {
        selected = cursor;
      }
    }
    if (selected === undefined) break;
    if (!(yield* append(selected.head![1]))) return;
    selected.head = yield* selected.next;
  }
  const pending = Array.from(pendingFor(store).pending.values())
    .filter((entry) =>
      entry.indexed &&
      entry.indexes.some((candidate) =>
        indexes.some((index) => candidate.equals(index))
      )
    )
    .sort((left, right) => left.stamp.userVersion - right.stamp.userVersion);
  for (const entry of pending) {
    if (!(yield* append(entry.id))) return;
  }
  if (batch.length > 0) yield* visit(batch);
});
