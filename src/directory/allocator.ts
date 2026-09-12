import { Effect, Semaphore } from "effect";
import { FoundationDbTransaction } from "../FoundationDb.ts";
import { MutationType } from "../model.ts";
import type { Bytes } from "../model.ts";
import {
  allocatorWindow,
  childSubspace,
  concat,
  firstInRange,
  invalidMetadata,
  ONE,
  subspaceRange,
  unpacked,
} from "./internal.ts";
import type {
  DirectoryEffect,
  DirectoryError,
  DirectoryFailure,
  DirectoryLayerState,
} from "./model.ts";

const allocationSemaphores = new WeakMap<
  FoundationDbTransaction["Service"],
  Semaphore.Semaphore
>();

const allocationSemaphore = (
  transaction: FoundationDbTransaction["Service"],
): Semaphore.Semaphore => {
  let semaphore = allocationSemaphores.get(transaction);
  if (semaphore === undefined) {
    semaphore = Semaphore.makeUnsafe(1);
    allocationSemaphores.set(transaction, semaphore);
  }
  return semaphore;
};

const decodeLittleEndianInt64 = (
  value: Bytes,
): Effect.Effect<bigint, DirectoryError> =>
  value.byteLength === 8
    ? Effect.succeed(new DataView(
      value.buffer,
      value.byteOffset,
      value.byteLength,
    ).getBigInt64(0, true))
    : Effect.fail(invalidMetadata(
      `allocator counter must contain 8 bytes, received ${value.byteLength}`,
    ));

const windowSize = (start: bigint): bigint =>
  start < 255n ? 64n : start < 65_535n ? 1_024n : 8_192n;

const randomCandidate = (start: bigint, window: bigint): bigint => {
  const random = crypto.getRandomValues(new Uint32Array(1))[0];
  return start + BigInt(random % Number(window));
};

const allocatePrefixLocked = (
  state: DirectoryLayerState,
  transaction: FoundationDbTransaction["Service"],
): Effect.Effect<bigint, DirectoryFailure> =>
  Effect.gen(function* () {
    const counters = yield* childSubspace(
      state.allocator,
      [0n],
      "invalid allocator counter subspace",
    );
    const recent = yield* childSubspace(
      state.allocator,
      [1n],
      "invalid allocator recent subspace",
    );
    const [counterBegin, counterEnd] = yield* subspaceRange(counters);

    while (true) {
      const latest = yield* firstInRange(
        transaction,
        counterBegin,
        counterEnd,
        { reverse: true, snapshot: true },
      );
      let start = latest === undefined
        ? 0n
        : yield* unpacked(counters, latest.key, "invalid allocator counter key")
          .pipe(Effect.flatMap(allocatorWindow));
      let advanced = false;
      let window: bigint;
      while (true) {
        const counter = yield* childSubspace(
          counters,
          [start],
          "invalid allocator counter key",
        );
        if (advanced) {
          yield* transaction.clearRangeWithoutWriteConflict(
            counters.prefix,
            counter.prefix,
          );
          const recentStart = yield* childSubspace(
            recent,
            [start],
            "invalid allocator recent key",
          );
          yield* transaction.clearRangeWithoutWriteConflict(
            recent.prefix,
            recentStart.prefix,
          );
        }
        yield* transaction.atomicOp(counter.prefix, ONE, MutationType.Add);
        const encodedCount = yield* transaction.get(counter.prefix, {
          snapshot: true,
        });
        const count = encodedCount === undefined
          ? 0n
          : yield* decodeLittleEndianInt64(encodedCount);
        window = windowSize(start);
        if (count * 2n < window) {
          break;
        }
        start += window;
        advanced = true;
      }

      while (true) {
        const candidate = randomCandidate(start, window);
        const recentCandidate = yield* childSubspace(
          recent,
          [candidate],
          "invalid allocator candidate key",
        );
        const currentLatest = yield* firstInRange(
          transaction,
          counterBegin,
          counterEnd,
          { reverse: true, snapshot: true },
        );
        const candidateValue = yield* transaction.get(recentCandidate.prefix);
        yield* transaction.setWithoutWriteConflict(
          recentCandidate.prefix,
          new Uint8Array(),
        );
        const currentStart = currentLatest === undefined ? 0n : yield* unpacked(
          counters,
          currentLatest.key,
          "invalid allocator counter key",
        ).pipe(Effect.flatMap(allocatorWindow));
        if (currentStart > start) {
          break;
        }
        if (candidateValue === undefined) {
          yield* transaction.addWriteConflictRange(
            recentCandidate.prefix,
            concat(recentCandidate.prefix, Uint8Array.of(0)),
          );
          return candidate;
        }
      }
    }
  });

export const allocatePrefix = (
  state: DirectoryLayerState,
): DirectoryEffect<bigint> =>
  Effect.gen(function* () {
    const transaction = yield* FoundationDbTransaction;
    return yield* allocationSemaphore(transaction).withPermits(1)(
      allocatePrefixLocked(state, transaction),
    );
  });
