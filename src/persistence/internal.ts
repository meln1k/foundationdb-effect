import { Effect } from "effect";
import { DirectoryLayer, DirectorySubspace } from "../directory/mod.ts";
import type {
  Directory,
  DirectoryOutput,
  DirectoryPath,
} from "../directory/mod.ts";
import type { TransactionOptions } from "../model.ts";

const encoder = new TextEncoder();

export interface DirectoryStoreOptions {
  readonly directory?: Directory;
  readonly directoryPath?: DirectoryPath;
  readonly transactionOptions?: TransactionOptions;
}

export class PersistenceDirectoryError extends Error {
  override readonly name = "PersistenceDirectoryError";
}

const regularDirectory = (
  output: DirectoryOutput,
): Effect.Effect<DirectorySubspace, PersistenceDirectoryError> =>
  output instanceof DirectorySubspace ? Effect.succeed(output) : Effect.fail(
    new PersistenceDirectoryError(
      "persistence store directory cannot be a partition",
    ),
  );

export const makeDirectoryStore = Effect.fnUntraced(function* (
  options: DirectoryStoreOptions,
  defaultDirectoryPath: DirectoryPath,
  layerName: string,
) {
  const directory = options.directory ??
    (yield* DirectoryLayer.make().pipe(Effect.orDie));
  const directoryPath = (options.directoryPath ?? defaultDirectoryPath).slice();
  const transactionOptions: TransactionOptions = {
    ...options.transactionOptions,
    timeoutMs: options.transactionOptions?.timeoutMs ?? 5_000,
    retryLimit: options.transactionOptions?.retryLimit ?? 10,
    maxRetryDelayMs: options.transactionOptions?.maxRetryDelayMs ?? 1_000,
  };
  const root = Effect.fnUntraced(function* () {
    return yield* regularDirectory(
      yield* directory.createOrOpen(directoryPath, {
        layer: encoder.encode(layerName),
      }),
    );
  });
  return { root, transactionOptions } as const;
});
