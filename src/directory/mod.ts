/** FoundationDB directory layer, partitions, metadata, and allocation. */
export {
  DirectoryLayer,
  DirectoryPartition,
  DirectorySubspace,
} from "./engine.ts";
export type { Directory, DirectoryOutput } from "./engine.ts";
export { DirectoryError, DirectoryErrorReason } from "./model.ts";
export type {
  DirectoryCreateOptions,
  DirectoryLayerOptions,
  DirectoryOpenOptions,
  DirectoryPath,
} from "./model.ts";

/** @internal */
export { strinc } from "./internal.ts";
