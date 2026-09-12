/** Effect RateLimiterStore backed by FoundationDB. */
import { Clock, Duration, Effect, Layer } from "effect";
import { RateLimiter } from "effect/unstable/persistence";
import { FoundationDb, FoundationDbTransaction } from "../FoundationDb.ts";
import type { Subspace } from "../tuple/mod.ts";
import { makeDirectoryStore } from "./internal.ts";
import type { RateLimiterStoreOptions } from "./model.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const defaultDirectoryPath = ["effect-foundationdb", "rate-limiter"];
const adaptiveStateTtlGraceMillis = 60_000;
const adaptiveStateMaxWindowMillis = 60 * 60 * 1_000;

interface FixedCounter {
  readonly count: number;
  readonly expiresAt: number;
}

interface TokenBucket {
  readonly tokens: number;
  readonly lastRefill: number;
}

interface AdaptiveState {
  readonly phase: RateLimiter.AdaptivePhase;
  readonly epoch: number;
  readonly cooldownUntil: number;
  readonly learningStartedAt: number;
  readonly observedTokens: number;
  readonly learnedLimit: number;
  readonly learnedWindowMillis: number;
  readonly expiresAt: number;
}

interface RateLimiterKeys {
  readonly fixed: Subspace;
  readonly buckets: Subspace;
  readonly adaptive: Subspace;
}

const storeError = (method: string, key: string, cause: unknown) =>
  cause instanceof RateLimiter.RateLimiterError
    ? cause
    : new RateLimiter.RateLimiterError({
      reason: new RateLimiter.RateLimitStoreError({
        message: `FoundationDB rate limiter ${method} failed for key ${key}`,
        cause,
      }),
    });

const encodeState = <A>(
  method: string,
  key: string,
  value: A,
): Effect.Effect<Uint8Array, RateLimiter.RateLimiterError> =>
  Effect.try({
    try: () => encoder.encode(JSON.stringify(value)),
    catch: (cause) => storeError(method, key, cause),
  });

const decodeState = <A>(
  method: string,
  key: string,
  value: Uint8Array,
  validate: (value: unknown) => value is A,
): Effect.Effect<A, RateLimiter.RateLimiterError> =>
  Effect.try({
    try: () => {
      const parsed: unknown = JSON.parse(decoder.decode(value));
      if (!validate(parsed)) {
        throw new Error("invalid rate limiter state");
      }
      return parsed;
    },
    catch: (cause) => storeError(method, key, cause),
  });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const isNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isFixedCounter = (value: unknown): value is FixedCounter =>
  isRecord(value) && isNumber(value.count) && isNumber(value.expiresAt);

const isTokenBucket = (value: unknown): value is TokenBucket =>
  isRecord(value) && isNumber(value.tokens) && isNumber(value.lastRefill);

const isAdaptiveState = (value: unknown): value is AdaptiveState =>
  isRecord(value) &&
  (value.phase === "inactive" || value.phase === "cooldown" ||
    value.phase === "learning" || value.phase === "learned") &&
  isNumber(value.epoch) && isNumber(value.cooldownUntil) &&
  isNumber(value.learningStartedAt) && isNumber(value.observedTokens) &&
  isNumber(value.learnedLimit) && isNumber(value.learnedWindowMillis) &&
  isNumber(value.expiresAt);

const clampAdaptiveDurationMillis = (milliseconds: number): number => {
  if (Number.isNaN(milliseconds) || milliseconds <= 0) {
    return 1;
  }
  return Math.min(milliseconds, adaptiveStateMaxWindowMillis);
};

const cooldownExpiresAt = (cooldownUntil: number): number =>
  cooldownUntil + adaptiveStateTtlGraceMillis;

const learningExpiresAt = (
  now: number,
  fallbackWindow: Duration.Duration,
): number =>
  now + Duration.toMillis(fallbackWindow) + adaptiveStateTtlGraceMillis;

const learnedExpiresAt = (now: number, learnedWindowMillis: number): number =>
  now + learnedWindowMillis + adaptiveStateTtlGraceMillis;

/** Creates Effect's RateLimiterStore using FoundationDB transactions. */
export const makeRateLimiterStore = Effect.fnUntraced(function* (
  options: RateLimiterStoreOptions = {},
) {
  const database = yield* FoundationDb;
  const clock = yield* Clock.Clock;
  const { root, transactionOptions } = yield* makeDirectoryStore(
    options,
    defaultDirectoryPath,
    "effect-foundationdb/rate-limiter",
  );

  const keys = Effect.fnUntraced(function* () {
    const directory = yield* root();
    return {
      fixed: yield* directory.subspace(["fixed"]),
      buckets: yield* directory.subspace(["buckets"]),
      adaptive: yield* directory.subspace(["adaptive"]),
    } satisfies RateLimiterKeys;
  });

  const operation = <A, E, R>(
    method: string,
    key: string,
    effect: Effect.Effect<A, E, R>,
  ) =>
    database.withTransaction(effect, {
      ...transactionOptions,
      retryOnMaybeCommitted: false,
    }).pipe(
      Effect.mapError((cause) => storeError(method, key, cause)),
    );

  return RateLimiter.RateLimiterStore.of({
    fixedWindow: (rateOptions) =>
      operation(
        "fixedWindow",
        rateOptions.key,
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const key = yield* (yield* keys()).fixed.pack([rateOptions.key]);
          const encoded = yield* transaction.get(key);
          const now = clock.currentTimeMillisUnsafe();
          let counter = encoded === undefined ? undefined : yield* decodeState(
            "fixedWindow",
            rateOptions.key,
            encoded,
            isFixedCounter,
          );
          if (counter === undefined || counter.expiresAt <= now) {
            counter = { count: 0, expiresAt: now };
          }
          const nextCount = counter.count + rateOptions.tokens;
          if (
            rateOptions.limit !== undefined && nextCount > rateOptions.limit
          ) {
            return [nextCount, Math.max(0, counter.expiresAt - now)] as const;
          }
          const updated: FixedCounter = {
            count: nextCount,
            expiresAt: counter.expiresAt +
              Duration.toMillis(rateOptions.refillRate) * rateOptions.tokens,
          };
          yield* transaction.set(
            key,
            yield* encodeState("fixedWindow", rateOptions.key, updated),
          );
          return [updated.count, Math.max(0, updated.expiresAt - now)] as const;
        }),
      ),
    tokenBucket: (rateOptions) =>
      operation(
        "tokenBucket",
        rateOptions.key,
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const key = yield* (yield* keys()).buckets.pack([rateOptions.key]);
          const encoded = yield* transaction.get(key);
          const now = clock.currentTimeMillisUnsafe();
          let bucket: TokenBucket = encoded === undefined
            ? { tokens: rateOptions.limit, lastRefill: now }
            : yield* decodeState(
              "tokenBucket",
              rateOptions.key,
              encoded,
              isTokenBucket,
            );
          const refillRateMillis = Duration.toMillis(rateOptions.refillRate);
          const tokensToAdd = Math.floor(
            (now - bucket.lastRefill) / refillRateMillis,
          );
          if (tokensToAdd > 0) {
            bucket = {
              tokens: Math.min(rateOptions.limit, bucket.tokens + tokensToAdd),
              lastRefill: bucket.lastRefill +
                tokensToAdd * refillRateMillis,
            };
          }
          if (bucket.tokens >= rateOptions.limit) {
            bucket = { ...bucket, lastRefill: now };
          }

          const remaining = bucket.tokens - rateOptions.tokens;
          if (rateOptions.allowOverflow || remaining >= 0) {
            bucket = { ...bucket, tokens: remaining };
            yield* transaction.set(
              key,
              yield* encodeState("tokenBucket", rateOptions.key, bucket),
            );
          } else if (encoded === undefined) {
            yield* transaction.set(
              key,
              yield* encodeState("tokenBucket", rateOptions.key, bucket),
            );
          } else if (tokensToAdd > 0 || bucket.lastRefill === now) {
            yield* transaction.set(
              key,
              yield* encodeState("tokenBucket", rateOptions.key, bucket),
            );
          }
          return [remaining, Math.max(0, now - bucket.lastRefill)] as const;
        }),
      ),
    adaptiveConsume: (rateOptions) =>
      operation(
        "adaptiveConsume",
        rateOptions.key,
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const key = yield* (yield* keys()).adaptive.pack([rateOptions.key]);
          const encoded = yield* transaction.get(key);
          const now = clock.currentTimeMillisUnsafe();
          let state = encoded === undefined ? undefined : yield* decodeState(
            "adaptiveConsume",
            rateOptions.key,
            encoded,
            isAdaptiveState,
          );
          if (state !== undefined && state.expiresAt <= now) {
            yield* transaction.clear(key);
            state = undefined;
          }
          if (state === undefined) {
            return {
              delay: Duration.zero,
              epoch: 0,
              phase: "inactive",
            } satisfies RateLimiter.AdaptiveConsumeResult;
          }

          if (state.phase === "cooldown") {
            if (state.cooldownUntil > now) {
              return {
                delay: Duration.millis(state.cooldownUntil - now),
                epoch: state.epoch,
                phase: "cooldown",
              } satisfies RateLimiter.AdaptiveConsumeResult;
            }
            state = {
              ...state,
              phase: "learning",
              epoch: state.epoch + 1,
              learningStartedAt: now,
              observedTokens: rateOptions.tokens,
              expiresAt: learningExpiresAt(now, rateOptions.fallbackWindow),
            };
          } else if (state.phase === "learning") {
            state = {
              ...state,
              observedTokens: state.observedTokens + rateOptions.tokens,
            };
          } else if (state.phase === "learned") {
            const refillRateMillis = state.learnedWindowMillis /
              state.learnedLimit;
            const observedTokens = state.cooldownUntil <= now
              ? rateOptions.tokens
              : state.observedTokens + rateOptions.tokens;
            const cooldownUntil =
              (state.cooldownUntil <= now ? now : state.cooldownUntil) +
              refillRateMillis * rateOptions.tokens;
            const ttl = cooldownUntil - now;
            const ttlTotal = observedTokens * refillRateMillis;
            const elapsed = ttlTotal - ttl;
            const windowNumber = Math.floor(
              (observedTokens - 1) / state.learnedLimit,
            );
            const remaining = windowNumber * state.learnedWindowMillis -
              elapsed;
            state = { ...state, observedTokens, cooldownUntil };
            yield* transaction.set(
              key,
              yield* encodeState("adaptiveConsume", rateOptions.key, state),
            );
            return {
              delay: remaining <= 0
                ? Duration.zero
                : Duration.millis(remaining),
              epoch: state.epoch,
              phase: state.phase,
            } satisfies RateLimiter.AdaptiveConsumeResult;
          }

          yield* transaction.set(
            key,
            yield* encodeState("adaptiveConsume", rateOptions.key, state),
          );
          return {
            delay: Duration.zero,
            epoch: state.epoch,
            phase: state.phase,
          } satisfies RateLimiter.AdaptiveConsumeResult;
        }),
      ),
    adaptiveFeedback: (rateOptions) => {
      const retryAfter = rateOptions.retryAfter;
      if (rateOptions.status !== 429 || retryAfter === undefined) {
        return Effect.void;
      }
      return operation(
        "adaptiveFeedback",
        rateOptions.key,
        Effect.gen(function* () {
          const transaction = yield* FoundationDbTransaction;
          const key = yield* (yield* keys()).adaptive.pack([rateOptions.key]);
          const encoded = yield* transaction.get(key);
          const now = clock.currentTimeMillisUnsafe();
          let state = encoded === undefined ? undefined : yield* decodeState(
            "adaptiveFeedback",
            rateOptions.key,
            encoded,
            isAdaptiveState,
          );
          if (state !== undefined && state.expiresAt <= now) {
            yield* transaction.clear(key);
            state = undefined;
          }
          const retryAfterMillis = clampAdaptiveDurationMillis(
            Duration.toMillis(retryAfter),
          );
          const cooldownUntil = now + retryAfterMillis;

          if (state === undefined) {
            if (rateOptions.epoch !== 0) {
              return;
            }
            state = {
              phase: "cooldown",
              epoch: 0,
              cooldownUntil,
              learningStartedAt: 0,
              observedTokens: 0,
              learnedLimit: 0,
              learnedWindowMillis: 0,
              expiresAt: cooldownExpiresAt(cooldownUntil),
            };
          } else if (state.epoch !== rateOptions.epoch) {
            return;
          } else if (state.phase === "cooldown") {
            const nextCooldown = Math.max(state.cooldownUntil, cooldownUntil);
            state = {
              ...state,
              cooldownUntil: nextCooldown,
              expiresAt: cooldownExpiresAt(nextCooldown),
            };
          } else if (state.phase === "learning") {
            const acceptedTokens = state.observedTokens - rateOptions.tokens;
            if (acceptedTokens <= 0) {
              state = {
                phase: "cooldown",
                epoch: state.epoch,
                cooldownUntil,
                learningStartedAt: 0,
                observedTokens: 0,
                learnedLimit: 0,
                learnedWindowMillis: 0,
                expiresAt: cooldownExpiresAt(cooldownUntil),
              };
            } else {
              const learnedWindowMillis = clampAdaptiveDurationMillis(
                now - state.learningStartedAt + retryAfterMillis,
              );
              state = {
                phase: "learned",
                epoch: state.epoch + 1,
                cooldownUntil: state.learningStartedAt + learnedWindowMillis,
                learningStartedAt: state.learningStartedAt,
                observedTokens: acceptedTokens,
                learnedLimit: acceptedTokens,
                learnedWindowMillis,
                expiresAt: learnedExpiresAt(now, learnedWindowMillis),
              };
            }
          } else if (state.phase === "learned") {
            state = {
              phase: "cooldown",
              epoch: state.epoch,
              cooldownUntil,
              learningStartedAt: 0,
              observedTokens: 0,
              learnedLimit: 0,
              learnedWindowMillis: 0,
              expiresAt: cooldownExpiresAt(cooldownUntil),
            };
          }

          yield* transaction.set(
            key,
            yield* encodeState("adaptiveFeedback", rateOptions.key, state),
          );
        }),
      );
    },
  });
});

/** Provides Effect's RateLimiterStore using FoundationDB. */
export const layerRateLimiterStore = (
  options?: RateLimiterStoreOptions,
): Layer.Layer<RateLimiter.RateLimiterStore, never, FoundationDb> =>
  Layer.effect(
    RateLimiter.RateLimiterStore,
    makeRateLimiterStore(options),
  );
