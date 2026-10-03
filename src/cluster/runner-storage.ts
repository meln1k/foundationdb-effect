/** Effect Cluster runner registrations and shard leases in FoundationDB. */
import { Clock, Duration, Effect, Layer, Schema, Stream } from "effect";
import { PersistenceError } from "effect/cluster/ClusterError";
import * as RunnerStorage from "effect/cluster/RunnerStorage";
import type * as ShardingConfig from "effect/cluster/ShardingConfig";
import { FoundationDb, FoundationDbTransaction } from "../FoundationDb.ts";
import { keyRange } from "../model.ts";
import { makeDirectoryStore } from "../persistence/internal.ts";
import type { DirectoryStoreOptions } from "../persistence/internal.ts";
import type { Subspace } from "../tuple/mod.ts";
import { RunnerMachineIdsExhaustedError } from "./errors.ts";

/**
 * Directory and transaction settings for runner storage. All participants must
 * share the directory and ShardingConfig.shardLockExpiration. Lease timestamps
 * use Effect's Clock, so participating hosts must have synchronized clocks.
 */
export interface RunnerStorageOptions extends DirectoryStoreOptions {}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
// Snowflake encodes the machine component modulo 1024, even though MachineId's
// schema accepts any integer. Never hand out colliding live machine components.
const machineIdCount = 1024;
const Registration = Schema.Struct({
  runner: Schema.String,
  healthy: Schema.Boolean,
  machineId: Schema.Int.check(
    Schema.isBetween({ minimum: 0, maximum: machineIdCount - 1 }),
  ),
  heartbeat: Schema.Finite,
});
const Lease = Schema.Struct({
  address: Schema.String,
  expiresAt: Schema.Finite,
});
const registrationJson = Schema.fromJsonString(Registration);
const leaseJson = Schema.fromJsonString(Lease);
const decodeRegistration = Schema.decodeUnknownEffect(registrationJson);
const encodeRegistration = Schema.encodeEffect(registrationJson);
const decodeLease = Schema.decodeUnknownEffect(leaseJson);
const encodeLease = Schema.encodeEffect(leaseJson);
const decodeExpiration = Schema.decodeUnknownEffect(
  Schema.Finite.check(Schema.isGreaterThan(0)),
);

const readRegistration = Effect.fnUntraced(function* (key: Uint8Array) {
  const transaction = yield* FoundationDbTransaction;
  const bytes = yield* transaction.get(key);
  return bytes === undefined
    ? undefined
    : yield* decodeRegistration(decoder.decode(bytes));
});

const writeRegistration = Effect.fnUntraced(function* (
  key: Uint8Array,
  value: typeof Registration.Type,
) {
  const transaction = yield* FoundationDbTransaction;
  yield* transaction.set(key, encoder.encode(yield* encodeRegistration(value)));
});

const readLease = Effect.fnUntraced(function* (key: Uint8Array) {
  const transaction = yield* FoundationDbTransaction;
  const bytes = yield* transaction.get(key);
  return bytes === undefined
    ? undefined
    : yield* decodeLease(decoder.decode(bytes));
});

const readEntries = Effect.fnUntraced(function* (subspace: Subspace) {
  const transaction = yield* FoundationDbTransaction;
  const [begin, end] = yield* subspace.range();
  // Non-snapshot reads deliberately establish conflicts for allocation and
  // releaseAll, including concurrent inserts into the scanned range.
  return yield* Stream.runCollect(transaction.getRange(keyRange(begin, end)));
});

/**
 * Creates durable storage using transactional (not advisory) leases.
 * Registrations retain their machine ID until explicitly unregistered, even
 * when their heartbeat expires. This prevents a late heartbeat from reviving a
 * machine ID reassigned to another runner. At most 1024 addresses can remain
 * registered; unregister retired runners to return their IDs to the pool.
 */
export const makeRunnerStorage = Effect.fnUntraced(function* (
  options: RunnerStorageOptions = {},
) {
  const database = yield* FoundationDb;
  // Effect initializes its environment provider when this module is imported.
  // Load it only for runners, keeping basic package imports permission-free.
  const configModule = yield* Effect.promise(() =>
    import("effect/cluster/ShardingConfig")
  );
  const config = yield* configModule.ShardingConfig;
  const expiration = yield* Effect.try(() =>
    Duration.toMillis(Duration.fromInputUnsafe(config.shardLockExpiration))
  ).pipe(Effect.flatMap(decodeExpiration), PersistenceError.refail);
  const { root, transactionOptions } = yield* makeDirectoryStore(
    options,
    ["effect-foundationdb", "runner-storage"],
    "effect-foundationdb/runner-storage",
  );
  const keys = Effect.fnUntraced(function* () {
    const directory = yield* root();
    return {
      runners: yield* directory.subspace(["runners"]),
      leases: yield* directory.subspace(["leases"]),
    };
  });
  const operation = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    database.withTransaction(effect, {
      ...transactionOptions,
      // A replay after an ambiguous commit could observe a different owner or
      // newly re-registered address. Leave recovery to the cluster caller.
      retryOnMaybeCommitted: false,
    }).pipe(PersistenceError.refail);

  const updateRegistration = Effect.fnUntraced(function* (
    runners: Subspace,
    address: string,
    update: (value: typeof Registration.Type) => typeof Registration.Type,
  ) {
    const key = yield* runners.pack([address]);
    const registration = yield* readRegistration(key);
    if (registration !== undefined) {
      yield* writeRegistration(key, update(registration));
    }
  });

  const lock = Effect.fnUntraced(function* (
    address: string,
    shardIds: ReadonlyArray<string>,
    refresh: boolean,
  ) {
    const transaction = yield* FoundationDbTransaction;
    const store = yield* keys();
    // Read time within each transaction attempt, not when constructing the
    // effect: retries must not write an already expired lease.
    const now = yield* Clock.currentTimeMillis;
    if (refresh) {
      yield* updateRegistration(store.runners, address, (value) => ({
        ...value,
        heartbeat: now,
      }));
    }
    const acquired: Array<string> = [];
    for (const shardId of new Set(shardIds)) {
      const key = yield* store.leases.pack([shardId]);
      const lease = yield* readLease(key);
      // Like SQL RunnerStorage, refresh only renews an existing owner, even
      // after expiry; acquire can take over strictly after expiration.
      if (
        lease?.address === address ||
        (!refresh && (lease === undefined || lease.expiresAt < now))
      ) {
        yield* transaction.set(
          key,
          encoder.encode(
            yield* encodeLease({
              address,
              expiresAt: now + expiration,
            }),
          ),
        );
        acquired.push(shardId);
      }
    }
    return acquired;
  });

  return RunnerStorage.makeEncoded({
    getRunners: operation(Effect.gen(function* () {
      const entries = yield* readEntries((yield* keys()).runners);
      const now = yield* Clock.currentTimeMillis;
      const runners: Array<readonly [string, boolean]> = [];
      for (const entry of entries) {
        const value = yield* decodeRegistration(decoder.decode(entry.value));
        if (value.heartbeat + expiration > now) {
          runners.push([value.runner, value.healthy]);
        }
      }
      return runners;
    })),
    register: Effect.fnUntraced(function* (address, runner, healthy) {
      return yield* operation(Effect.gen(function* () {
        const store = yield* keys();
        const key = yield* store.runners.pack([address]);
        const existing = yield* readRegistration(key);
        let machineId = existing?.machineId;
        if (machineId === undefined) {
          const used = new Set<number>();
          for (const entry of yield* readEntries(store.runners)) {
            used.add(
              (yield* decodeRegistration(decoder.decode(entry.value)))
                .machineId,
            );
          }
          for (let candidate = 0; candidate < machineIdCount; candidate++) {
            if (!used.has(candidate)) {
              machineId = candidate;
              break;
            }
          }
          if (machineId === undefined) {
            return yield* new RunnerMachineIdsExhaustedError({
              address,
              capacity: machineIdCount,
            });
          }
        }
        yield* writeRegistration(key, {
          runner,
          healthy,
          machineId,
          heartbeat: yield* Clock.currentTimeMillis,
        });
        return machineId;
      }));
    }),
    unregister: Effect.fnUntraced(function* (address) {
      yield* operation(Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        yield* transaction.clear(
          yield* (yield* keys()).runners.pack([address]),
        );
      }));
    }),
    setRunnerHealth: Effect.fnUntraced(function* (address, healthy) {
      yield* operation(Effect.gen(function* () {
        yield* updateRegistration(
          (yield* keys()).runners,
          address,
          (value) => ({ ...value, healthy }),
        );
      }));
    }),
    acquire: (address, shardIds) => operation(lock(address, shardIds, false)),
    refresh: (address, shardIds) => operation(lock(address, shardIds, true)),
    release: Effect.fnUntraced(function* (address, shardId) {
      yield* operation(Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const key = yield* (yield* keys()).leases.pack([shardId]);
        if ((yield* readLease(key))?.address === address) {
          yield* transaction.clear(key);
        }
      }));
    }),
    releaseAll: Effect.fnUntraced(function* (address) {
      yield* operation(Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        for (const entry of yield* readEntries((yield* keys()).leases)) {
          const lease = yield* decodeLease(decoder.decode(entry.value));
          if (lease.address === address) {
            yield* transaction.clear(entry.key);
          }
        }
      }));
    }),
  });
});

/** Provides runner storage using FoundationDb and the cluster ShardingConfig. */
export const layerRunnerStorage = (
  options?: RunnerStorageOptions,
): Layer.Layer<
  RunnerStorage.RunnerStorage,
  PersistenceError,
  FoundationDb | ShardingConfig.ShardingConfig
> => Layer.effect(RunnerStorage.RunnerStorage, makeRunnerStorage(options));
