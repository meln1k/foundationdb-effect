import type { Duration } from "effect";
import type { Directory, DirectoryPath } from "../directory/mod.ts";
import type { TransactionOptions } from "../model.ts";

export interface PersistedQueueStoreOptions {
  readonly directory?: Directory;
  readonly directoryPath?: DirectoryPath;
  readonly pollInterval?: Duration.Input;
  readonly lockRefreshInterval?: Duration.Input;
  readonly lockExpiration?: Duration.Input;
  readonly transactionOptions?: TransactionOptions;
}
