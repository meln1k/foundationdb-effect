import type { Directory, DirectoryPath } from "../directory/mod.ts";
import type { TransactionOptions } from "../model.ts";

interface DirectoryStoreOptions {
  readonly directory?: Directory;
  readonly directoryPath?: DirectoryPath;
  readonly transactionOptions?: TransactionOptions;
}

export type KeyValueStoreOptions = DirectoryStoreOptions;

export type BackingPersistenceOptions = DirectoryStoreOptions;

export type RateLimiterStoreOptions = DirectoryStoreOptions;
