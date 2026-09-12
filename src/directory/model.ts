import { Effect, Schema } from "effect";
import type { FoundationDbTransaction } from "../FoundationDb.ts";
import type { FoundationDbError } from "../errors.ts";
import type { Bytes } from "../model.ts";
import type { Subspace } from "../tuple/mod.ts";

export const DirectoryErrorReason = [
  "CannotModifyRootDirectory",
  "DirectoryPrefixInUse",
  "DirectoryDoesNotExist",
  "NoPathProvided",
  "DirectoryAlreadyExists",
  "PathDoesNotExist",
  "ParentDirectoryDoesNotExist",
  "IncompatibleLayer",
  "BadDestinationDirectory",
  "VersionError",
  "PrefixNotAllowed",
  "CannotPrefixInPartition",
  "CannotMoveRootDirectory",
  "CannotMoveBetweenPartitions",
  "CannotMoveIntoSubdirectory",
  "PrefixNotEmpty",
  "CannotCreateSubpath",
  "InvalidMetadata",
  "InvalidPrefix",
] as const;

export type DirectoryErrorReason = typeof DirectoryErrorReason[number];

export class DirectoryError extends Schema.TaggedError<DirectoryError>()(
  "DirectoryError",
  {
    reason: Schema.Literals(DirectoryErrorReason),
    message: Schema.String,
  },
) {}

export interface DirectoryCreateOptions {
  readonly prefix?: Bytes;
  readonly layer?: Bytes;
}

export interface DirectoryOpenOptions {
  readonly layer?: Bytes;
}

export interface DirectoryLayerOptions {
  readonly nodeSubspace?: Subspace;
  readonly contentSubspace?: Subspace;
  readonly allowManualPrefixes?: boolean;
}

export type DirectoryPath = ReadonlyArray<string>;

export type DirectoryFailure = DirectoryError | FoundationDbError;
export type DirectoryEffect<A> = Effect.Effect<
  A,
  DirectoryFailure,
  FoundationDbTransaction
>;

export interface DirectoryLayerState {
  readonly rootNode: Subspace;
  readonly nodeSubspace: Subspace;
  readonly contentSubspace: Subspace;
  readonly allocator: Subspace;
  readonly allowManualPrefixes: boolean;
  readonly path: ReadonlyArray<string>;
}

export type InternalDirectoryLayerOptions = DirectoryLayerOptions & {
  readonly path?: DirectoryPath;
};

export interface Node {
  readonly subspace: Subspace;
  readonly currentPath: ReadonlyArray<string>;
  readonly targetPath: ReadonlyArray<string>;
  readonly layer: Uint8Array;
}

export const directoryLayerState: unique symbol = Symbol(
  "DirectoryLayer.state",
);
export const makeInternalDirectoryLayer: unique symbol = Symbol(
  "DirectoryLayer.make",
);
