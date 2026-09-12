/** Effect PersistedQueueStore backed by FoundationDB. */
import { Duration, Effect, Latch, Layer, Schedule } from "effect";
import { PersistedQueue } from "effect/unstable/persistence";
import { FoundationDb } from "../FoundationDb.ts";
import type { PersistedQueueStoreOptions } from "./model.ts";
import { makeQueueRepository } from "./repository.ts";

export type { PersistedQueueStoreOptions } from "./model.ts";

const storeError = (message: string, cause: unknown) =>
  new PersistedQueue.PersistedQueueError({ message, cause });

/**
 * Creates Effect's low-level `PersistedQueueStore` backed by FoundationDB.
 */
export const makePersistedQueueStore = Effect.fnUntraced(function* (
  options: PersistedQueueStoreOptions = {},
) {
  const repository = yield* makeQueueRepository(options);
  const pollInterval = Duration.max(
    Duration.fromInputUnsafe(options.pollInterval ?? "1 second"),
    Duration.millis(1),
  );
  const lockRefreshInterval = Duration.max(
    Duration.fromInputUnsafe(options.lockRefreshInterval ?? "30 seconds"),
    Duration.millis(1),
  );
  const latches = new Map<string, Latch.Latch>();
  const latchFor = (name: string): Latch.Latch => {
    let latch = latches.get(name);
    if (latch === undefined) {
      latch = Latch.makeUnsafe(false);
      latches.set(name, latch);
    }
    return latch;
  };

  const offer = Effect.fnUntraced(function* (offerOptions: {
    readonly name: string;
    readonly id: string;
    readonly element: unknown;
  }) {
    yield* repository.offer(offerOptions);
    latchFor(offerOptions.name).openUnsafe();
  });

  const take = Effect.fnUntraced(function* (takeOptions: {
    readonly name: string;
    readonly maxAttempts: number;
    readonly retryDelay: (
      attempts: number,
    ) => Effect.Effect<Duration.Duration>;
  }) {
    const latch = latchFor(takeOptions.name);
    const claimId = crypto.randomUUID();
    const result = yield* Effect.acquireRelease(
      Effect.gen(function* () {
        while (true) {
          latch.closeUnsafe();
          const result = yield* repository.claim(
            takeOptions.name,
            claimId,
            takeOptions.maxAttempts,
          );
          if (result === true) {
            continue;
          }
          if (result._tag === "ClaimWait") {
            const remoteWake = result.wake.await.pipe(
              Effect.matchEffect({
                onFailure: (error) =>
                  Effect.logWarning(error).pipe(
                    Effect.andThen(Effect.sleep(pollInterval)),
                  ),
                onSuccess: () => Effect.void,
              }),
            );
            yield* Effect.raceAll([
              latch.await,
              remoteWake,
              Effect.sleep(pollInterval),
            ]).pipe(Effect.ensuring(result.wake.cancel));
            continue;
          }
          return result;
        }
      }),
      (claimed, exit) =>
        repository.finalizeClaim(
          takeOptions.name,
          claimed,
          takeOptions.maxAttempts,
          takeOptions.retryDelay,
          exit,
        ).pipe(
          Effect.tap((state) =>
            Effect.sync(() => {
              if (state === "pending") {
                latch.openUnsafe();
              }
            })
          ),
          Effect.orDie,
        ),
      { interruptible: true },
    );
    yield* repository.refreshClaim(takeOptions.name, result).pipe(
      Effect.tapCause(Effect.logWarning),
      Effect.retry(Schedule.spaced(500)),
      Effect.schedule(Schedule.fixed(lockRefreshInterval)),
      Effect.forkScoped,
    );
    return {
      id: result.entry.id,
      attempts: result.entry.attempts,
      element: result.entry.element,
    };
  });

  return PersistedQueue.PersistedQueueStore.of({
    offer: (offerOptions) =>
      offer(offerOptions).pipe(
        Effect.mapError((cause) =>
          storeError("Failed to offer element to persisted queue", cause)
        ),
      ),
    take: (takeOptions) =>
      take(takeOptions).pipe(
        Effect.mapError((cause) =>
          storeError("Failed to take element from persisted queue", cause)
        ),
      ),
    cleanup: (cleanupOptions) =>
      repository.cleanup(cleanupOptions).pipe(
        Effect.mapError((cause) =>
          storeError("Failed to clean up persisted queue", cause)
        ),
      ),
  });
});

/**
 * Provides Effect's `PersistedQueueStore` using FoundationDB.
 */
export const layerPersistedQueueStore = (
  options?: PersistedQueueStoreOptions,
): Layer.Layer<PersistedQueue.PersistedQueueStore, never, FoundationDb> =>
  Layer.effect(
    PersistedQueue.PersistedQueueStore,
    makePersistedQueueStore(options),
  );
