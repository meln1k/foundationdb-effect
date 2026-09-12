/** FoundationDB directory layer and partition engine. */
import { Effect } from "effect";
import { FoundationDbTransaction } from "../FoundationDb.ts";
import type { Bytes } from "../model.ts";
import { Subspace } from "../tuple/mod.ts";
import { allocatePrefix } from "./allocator.ts";
import {
  allInRange,
  asBytes,
  childSubspace,
  concat,
  DIRECTORY_VERSION,
  directoryChildName,
  equalBytes,
  error,
  firstInRange,
  incrementPrefix,
  invalidMetadata,
  LAYER,
  makeDirectoryLayerState,
  NODE_PREFIX,
  packed,
  PARTITION,
  pathsEqual,
  pathStartsWith,
  startsWith,
  subspaceRange,
  unpacked,
  VERSION,
} from "./internal.ts";
import {
  DirectoryError,
  directoryLayerState,
  makeInternalDirectoryLayer,
} from "./model.ts";
import type {
  DirectoryCreateOptions,
  DirectoryEffect,
  DirectoryFailure,
  DirectoryLayerOptions,
  DirectoryLayerState,
  DirectoryOpenOptions,
  DirectoryPath,
  InternalDirectoryLayerOptions,
  Node,
} from "./model.ts";

export interface Directory {
  readonly createOrOpen: (
    path: DirectoryPath,
    options?: DirectoryCreateOptions,
  ) => DirectoryEffect<DirectoryOutput>;
  readonly create: (
    path: DirectoryPath,
    options?: DirectoryCreateOptions,
  ) => DirectoryEffect<DirectoryOutput>;
  readonly open: (
    path: DirectoryPath,
    options?: DirectoryOpenOptions,
  ) => DirectoryEffect<DirectoryOutput>;
  readonly exists: (path: DirectoryPath) => DirectoryEffect<boolean>;
  readonly list: (
    path?: DirectoryPath,
  ) => DirectoryEffect<ReadonlyArray<string>>;
  readonly move: (
    newAbsolutePath: DirectoryPath,
  ) => DirectoryEffect<DirectoryOutput>;
  readonly moveTo: (
    oldPath: DirectoryPath,
    newPath: DirectoryPath,
  ) => DirectoryEffect<DirectoryOutput>;
  readonly remove: (path: DirectoryPath) => DirectoryEffect<boolean>;
  readonly removeIfExists: (path: DirectoryPath) => DirectoryEffect<boolean>;
}

export class DirectoryLayer implements Directory {
  readonly [directoryLayerState]: DirectoryLayerState;

  private constructor(state: DirectoryLayerState) {
    this[directoryLayerState] = state;
  }

  static [makeInternalDirectoryLayer](
    options: InternalDirectoryLayerOptions,
  ): Effect.Effect<DirectoryLayer, DirectoryError> {
    return makeDirectoryLayerState(options).pipe(
      Effect.map((state) => new DirectoryLayer(state)),
    );
  }

  static make(
    options: DirectoryLayerOptions = {},
  ): Effect.Effect<DirectoryLayer, DirectoryError> {
    return DirectoryLayer[makeInternalDirectoryLayer](options);
  }

  createOrOpen(
    path: DirectoryPath,
    options: DirectoryCreateOptions = {},
  ): DirectoryEffect<DirectoryOutput> {
    return createOrOpenInternal(
      this,
      path.slice(),
      options.prefix?.slice(),
      options.layer?.slice(),
      true,
      true,
    );
  }

  create(
    path: DirectoryPath,
    options: DirectoryCreateOptions = {},
  ): DirectoryEffect<DirectoryOutput> {
    return createOrOpenInternal(
      this,
      path.slice(),
      options.prefix?.slice(),
      options.layer?.slice(),
      true,
      false,
    );
  }

  open(
    path: DirectoryPath,
    options: DirectoryOpenOptions = {},
  ): DirectoryEffect<DirectoryOutput> {
    return createOrOpenInternal(
      this,
      path.slice(),
      undefined,
      options.layer?.slice(),
      false,
      true,
    );
  }

  exists(path: DirectoryPath): DirectoryEffect<boolean> {
    return existsInternal(this, path.slice());
  }

  list(path: DirectoryPath = []): DirectoryEffect<ReadonlyArray<string>> {
    return listInternal(this, path.slice());
  }

  move(_newAbsolutePath: DirectoryPath): DirectoryEffect<DirectoryOutput> {
    return Effect.fail(error(
      "CannotMoveRootDirectory",
      "the root directory cannot be moved",
    ));
  }

  moveTo(
    oldPath: DirectoryPath,
    newPath: DirectoryPath,
  ): DirectoryEffect<DirectoryOutput> {
    return moveInternal(this, oldPath.slice(), newPath.slice());
  }

  remove(path: DirectoryPath): DirectoryEffect<boolean> {
    return removeInternal(this, path.slice(), true);
  }

  removeIfExists(path: DirectoryPath): DirectoryEffect<boolean> {
    return removeInternal(this, path.slice(), false);
  }
}

export class DirectorySubspace extends Subspace implements Directory {
  readonly _tag = "DirectorySubspace";
  readonly #directoryLayer: DirectoryLayer;
  readonly #relativePath: ReadonlyArray<string>;
  readonly #path: ReadonlyArray<string>;
  readonly #layer: Uint8Array;

  constructor(
    directoryLayer: DirectoryLayer,
    relativePath: DirectoryPath,
    prefix: Bytes,
    layer: Bytes,
  ) {
    super(prefix);
    this.#directoryLayer = directoryLayer;
    this.#relativePath = Object.freeze([...relativePath]);
    this.#path = Object.freeze([
      ...directoryLayer[directoryLayerState].path,
      ...relativePath,
    ]);
    this.#layer = layer.slice();
  }

  get path(): ReadonlyArray<string> {
    return this.#path.slice();
  }

  get layer(): Uint8Array {
    return this.#layer.slice();
  }

  createOrOpen(
    path: DirectoryPath,
    options?: DirectoryCreateOptions,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.createOrOpen(
      [...this.#relativePath, ...path],
      options,
    );
  }

  create(
    path: DirectoryPath,
    options?: DirectoryCreateOptions,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.create(
      [...this.#relativePath, ...path],
      options,
    );
  }

  open(
    path: DirectoryPath,
    options?: DirectoryOpenOptions,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.open(
      [...this.#relativePath, ...path],
      options,
    );
  }

  exists(path: DirectoryPath): DirectoryEffect<boolean> {
    return this.#directoryLayer.exists([...this.#relativePath, ...path]);
  }

  list(path: DirectoryPath = []): DirectoryEffect<ReadonlyArray<string>> {
    return this.#directoryLayer.list([...this.#relativePath, ...path]);
  }

  move(newAbsolutePath: DirectoryPath): DirectoryEffect<DirectoryOutput> {
    return Effect.flatMap(
      relativePath(this.#directoryLayer, newAbsolutePath),
      (newPath) => this.#directoryLayer.moveTo(this.#relativePath, newPath),
    );
  }

  moveTo(
    oldPath: DirectoryPath,
    newPath: DirectoryPath,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.moveTo(
      [...this.#relativePath, ...oldPath],
      [...this.#relativePath, ...newPath],
    );
  }

  remove(path: DirectoryPath): DirectoryEffect<boolean> {
    return this.#directoryLayer.remove([...this.#relativePath, ...path]);
  }

  removeIfExists(path: DirectoryPath): DirectoryEffect<boolean> {
    return this.#directoryLayer.removeIfExists([
      ...this.#relativePath,
      ...path,
    ]);
  }
}

export class DirectoryPartition implements Directory {
  readonly _tag = "DirectoryPartition";
  readonly #parentLayer: DirectoryLayer;
  readonly #parentRelativePath: ReadonlyArray<string>;
  readonly #directoryLayer: DirectoryLayer;
  readonly #path: ReadonlyArray<string>;

  constructor(
    parentLayer: DirectoryLayer,
    parentRelativePath: DirectoryPath,
    directoryLayer: DirectoryLayer,
  ) {
    this.#parentLayer = parentLayer;
    this.#parentRelativePath = Object.freeze([...parentRelativePath]);
    this.#directoryLayer = directoryLayer;
    this.#path = Object.freeze([
      ...directoryLayer[directoryLayerState].path,
    ]);
  }

  get path(): ReadonlyArray<string> {
    return this.#path.slice();
  }

  get layer(): Uint8Array {
    return PARTITION.slice();
  }

  createOrOpen(
    path: DirectoryPath,
    options?: DirectoryCreateOptions,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.createOrOpen(path, options);
  }

  create(
    path: DirectoryPath,
    options?: DirectoryCreateOptions,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.create(path, options);
  }

  open(
    path: DirectoryPath,
    options?: DirectoryOpenOptions,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.open(path, options);
  }

  exists(path: DirectoryPath): DirectoryEffect<boolean> {
    return path.length === 0
      ? this.#parentLayer.exists(this.#parentRelativePath)
      : this.#directoryLayer.exists(path);
  }

  list(path: DirectoryPath = []): DirectoryEffect<ReadonlyArray<string>> {
    return this.#directoryLayer.list(path);
  }

  move(newAbsolutePath: DirectoryPath): DirectoryEffect<DirectoryOutput> {
    return Effect.flatMap(
      relativePath(this.#parentLayer, newAbsolutePath),
      (newPath) => this.#parentLayer.moveTo(this.#parentRelativePath, newPath),
    );
  }

  moveTo(
    oldPath: DirectoryPath,
    newPath: DirectoryPath,
  ): DirectoryEffect<DirectoryOutput> {
    return this.#directoryLayer.moveTo(oldPath, newPath);
  }

  remove(path: DirectoryPath): DirectoryEffect<boolean> {
    return path.length === 0
      ? this.#parentLayer.remove(this.#parentRelativePath)
      : this.#directoryLayer.remove(path);
  }

  removeIfExists(path: DirectoryPath): DirectoryEffect<boolean> {
    return path.length === 0
      ? this.#parentLayer.removeIfExists(this.#parentRelativePath)
      : this.#directoryLayer.removeIfExists(path);
  }
}

export type DirectoryOutput = DirectorySubspace | DirectoryPartition;

const relativePath = (
  layer: DirectoryLayer,
  absolutePath: DirectoryPath,
): Effect.Effect<ReadonlyArray<string>, DirectoryError> =>
  pathStartsWith(absolutePath, layer[directoryLayerState].path)
    ? Effect.succeed(
      absolutePath.slice(layer[directoryLayerState].path.length),
    )
    : Effect.fail(error(
      "CannotMoveBetweenPartitions",
      "destination is outside the current directory partition",
    ));

const nodeWithPrefix = (
  layer: DirectoryLayer,
  prefix: Bytes,
): Effect.Effect<Subspace, DirectoryError> =>
  childSubspace(
    layer[directoryLayerState].nodeSubspace,
    [prefix],
    "invalid directory node prefix",
  );

const loadLayer = (
  transaction: FoundationDbTransaction["Service"],
  node: Subspace,
): Effect.Effect<Uint8Array, DirectoryFailure> =>
  Effect.gen(function* () {
    const key = yield* packed(node, [LAYER], "invalid layer key");
    return (yield* transaction.get(key)) ?? new Uint8Array();
  });

const findNode = (
  layer: DirectoryLayer,
  path: DirectoryPath,
): DirectoryEffect<Node | undefined> =>
  Effect.gen(function* () {
    const transaction = yield* FoundationDbTransaction;
    const currentPath: Array<string> = [];
    let node = layer[directoryLayerState].rootNode;
    let nodeLayer: Uint8Array = new Uint8Array();
    let loaded = false;
    for (const component of path) {
      currentPath.push(component);
      const key = yield* packed(
        node,
        [0n, component],
        "invalid directory child key",
      );
      const prefix = yield* transaction.get(key);
      loaded = true;
      if (prefix === undefined) {
        return undefined;
      }
      node = yield* nodeWithPrefix(layer, prefix);
      nodeLayer = yield* loadLayer(transaction, node);
      if (equalBytes(nodeLayer, PARTITION)) {
        break;
      }
    }
    if (!loaded) {
      nodeLayer = yield* loadLayer(transaction, node);
    }
    return {
      subspace: node,
      currentPath: Object.freeze(currentPath.slice()),
      targetPath: Object.freeze(path.slice()),
      layer: nodeLayer,
    };
  });

const partitionSubpath = (node: Node): ReadonlyArray<string> =>
  node.targetPath.slice(node.currentPath.length);

const isInPartition = (node: Node, includeRoot: boolean): boolean =>
  equalBytes(node.layer, PARTITION) &&
  (includeRoot || node.targetPath.length > node.currentPath.length);

const contentsOfNode = (
  layer: DirectoryLayer,
  node: Subspace,
  path: DirectoryPath,
  nodeLayer: Bytes,
): Effect.Effect<DirectoryOutput, DirectoryError> =>
  Effect.gen(function* () {
    const tuple = yield* unpacked(
      layer[directoryLayerState].nodeSubspace,
      node.prefix,
      "invalid directory node",
    );
    if (tuple.length !== 1) {
      return yield* invalidMetadata(
        "directory node has an invalid prefix tuple",
      );
    }
    const prefix = yield* asBytes(tuple[0], "directory node prefix");
    if (equalBytes(nodeLayer, PARTITION)) {
      const absolutePath = [...layer[directoryLayerState].path, ...path];
      const nested = yield* DirectoryLayer[makeInternalDirectoryLayer]({
        nodeSubspace: new Subspace(concat(prefix, NODE_PREFIX)),
        contentSubspace: new Subspace(prefix),
        path: absolutePath,
      });
      return new DirectoryPartition(layer, path, nested);
    }
    return new DirectorySubspace(layer, path, prefix, nodeLayer);
  });

const getContents = (
  layer: DirectoryLayer,
  node: Node,
): Effect.Effect<DirectoryOutput, DirectoryError> =>
  contentsOfNode(layer, node.subspace, node.currentPath, node.layer);

const checkVersion = (
  layer: DirectoryLayer,
  write: boolean,
): DirectoryEffect<void> =>
  Effect.gen(function* () {
    const transaction = yield* FoundationDbTransaction;
    const key = yield* packed(
      layer[directoryLayerState].rootNode,
      [VERSION],
      "invalid directory version key",
    );
    const version = yield* transaction.get(key);
    if (version === undefined) {
      if (write) {
        yield* transaction.set(key, DIRECTORY_VERSION);
      }
      return;
    }
    if (version.byteLength !== 12) {
      return yield* error(
        "VersionError",
        `directory version must contain exactly 12 bytes, received ${version.byteLength}`,
      );
    }
    const view = new DataView(
      version.buffer,
      version.byteOffset,
      version.byteLength,
    );
    const major = view.getUint32(0, true);
    const minor = view.getUint32(4, true);
    const patch = view.getUint32(8, true);
    if (major > 1) {
      return yield* error(
        "VersionError",
        `cannot load directory version ${major}.${minor}.${patch} using 1.0.0`,
      );
    }
    if (write && minor > 0) {
      return yield* error(
        "VersionError",
        `directory version ${major}.${minor}.${patch} is read-only using 1.0.0`,
      );
    }
  });

const createOrOpenInternal = (
  layer: DirectoryLayer,
  path: DirectoryPath,
  prefix: Uint8Array | undefined,
  expectedLayer: Uint8Array | undefined,
  allowCreate: boolean,
  allowOpen: boolean,
): DirectoryEffect<DirectoryOutput> =>
  Effect.gen(function* () {
    yield* checkVersion(layer, false);
    if (
      prefix !== undefined &&
      !layer[directoryLayerState].allowManualPrefixes
    ) {
      return yield* error(
        layer[directoryLayerState].path.length === 0
          ? "PrefixNotAllowed"
          : "CannotPrefixInPartition",
        "manual directory prefixes are not enabled",
      );
    }
    if (path.length === 0) {
      return yield* error("NoPathProvided", "directory path must not be empty");
    }

    const found = yield* findNode(layer, path);
    if (found !== undefined) {
      if (isInPartition(found, false)) {
        const partition = yield* getContents(layer, found);
        if (partition._tag !== "DirectoryPartition") {
          return yield* invalidMetadata(
            "partition node has a non-partition value",
          );
        }
        const subpath = partitionSubpath(found);
        if (!allowCreate) {
          return yield* partition.open(
            subpath,
            expectedLayer === undefined ? {} : { layer: expectedLayer },
          );
        }
        const options: DirectoryCreateOptions = {
          ...(prefix === undefined ? {} : { prefix }),
          ...(expectedLayer === undefined ? {} : { layer: expectedLayer }),
        };
        return yield* (allowOpen
          ? partition.createOrOpen(subpath, options)
          : partition.create(subpath, options));
      }
      if (!allowOpen) {
        return yield* error(
          "DirectoryAlreadyExists",
          `directory /${path.join("/")} already exists`,
        );
      }
      if (
        expectedLayer !== undefined && expectedLayer.byteLength > 0 &&
        !equalBytes(expectedLayer, found.layer)
      ) {
        return yield* error(
          "IncompatibleLayer",
          `directory /${path.join("/")} has an incompatible layer`,
        );
      }
      return yield* getContents(layer, found);
    }
    if (!allowCreate) {
      return yield* error(
        "DirectoryDoesNotExist",
        `directory /${path.join("/")} does not exist`,
      );
    }
    return yield* createInternal(layer, path, prefix, expectedLayer);
  });

const nodeContainingKey = (
  layer: DirectoryLayer,
  key: Bytes,
  snapshot: boolean,
): DirectoryEffect<Subspace | undefined> =>
  Effect.gen(function* () {
    const transaction = yield* FoundationDbTransaction;
    if (startsWith(key, layer[directoryLayerState].nodeSubspace.prefix)) {
      return layer[directoryLayerState].rootNode;
    }
    const [metadataBegin] = yield* subspaceRange(
      layer[directoryLayerState].nodeSubspace,
    );
    const rangeEnd = yield* packed(
      layer[directoryLayerState].nodeSubspace,
      [concat(key, Uint8Array.of(0))],
      "invalid prefix lookup key",
    );
    const previous = yield* firstInRange(
      transaction,
      metadataBegin,
      rangeEnd,
      { reverse: true, snapshot },
    );
    if (previous === undefined) {
      return undefined;
    }
    const tuple = yield* unpacked(
      layer[directoryLayerState].nodeSubspace,
      previous.key,
      "invalid directory node key",
    );
    const prefix = yield* asBytes(tuple[0], "directory node prefix");
    return startsWith(key, prefix)
      ? yield* nodeWithPrefix(layer, prefix)
      : undefined;
  });

const isPrefixFree = (
  layer: DirectoryLayer,
  prefix: Bytes,
  snapshot: boolean,
): DirectoryEffect<boolean> =>
  Effect.gen(function* () {
    if (prefix.byteLength === 0) {
      return false;
    }
    if ((yield* nodeContainingKey(layer, prefix, snapshot)) !== undefined) {
      return false;
    }
    const endPrefix = yield* incrementPrefix(prefix);
    const begin = yield* packed(
      layer[directoryLayerState].nodeSubspace,
      [prefix],
      "invalid prefix lookup",
    );
    const end = yield* packed(
      layer[directoryLayerState].nodeSubspace,
      [endPrefix],
      "invalid prefix lookup",
    );
    const transaction = yield* FoundationDbTransaction;
    return (yield* firstInRange(transaction, begin, end, { snapshot })) ===
      undefined;
  });

const choosePrefix = (
  layer: DirectoryLayer,
  explicit: Uint8Array | undefined,
): DirectoryEffect<Uint8Array> =>
  Effect.gen(function* () {
    if (explicit !== undefined) {
      return explicit;
    }
    const allocated = yield* allocatePrefix(layer[directoryLayerState]);
    const prefix = yield* packed(
      layer[directoryLayerState].contentSubspace,
      [allocated],
      "invalid allocated directory prefix",
    );
    const end = yield* incrementPrefix(prefix);
    const transaction = yield* FoundationDbTransaction;
    if ((yield* firstInRange(transaction, prefix, end)) !== undefined) {
      return yield* error(
        "PrefixNotEmpty",
        "the allocated directory prefix is not empty",
      );
    }
    return prefix;
  });

const getParentNode = (
  layer: DirectoryLayer,
  path: DirectoryPath,
): DirectoryEffect<Subspace> =>
  Effect.gen(function* () {
    const parentPath = path.slice(0, -1);
    if (parentPath.length === 0) {
      return layer[directoryLayerState].rootNode;
    }
    const parent = yield* createOrOpenInternal(
      layer,
      parentPath,
      undefined,
      undefined,
      true,
      true,
    );
    if (parent._tag !== "DirectorySubspace") {
      return yield* invalidMetadata("directory parent is a partition root");
    }
    return yield* nodeWithPrefix(layer, parent.prefix);
  });

const createInternal = (
  layer: DirectoryLayer,
  path: DirectoryPath,
  explicitPrefix: Uint8Array | undefined,
  nodeLayer: Uint8Array | undefined,
): DirectoryEffect<DirectoryOutput> =>
  Effect.gen(function* () {
    const name = path[path.length - 1];
    if (name === undefined) {
      return yield* error("NoPathProvided", "directory path must not be empty");
    }
    yield* checkVersion(layer, true);
    const parent = yield* getParentNode(layer, path);
    const prefix = yield* choosePrefix(layer, explicitPrefix);
    if (!(yield* isPrefixFree(layer, prefix, explicitPrefix === undefined))) {
      return yield* error(
        "DirectoryPrefixInUse",
        "the requested directory prefix is already in use",
      );
    }
    const node = yield* nodeWithPrefix(layer, prefix);
    const childKey = yield* packed(
      parent,
      [0n, name],
      "invalid directory child key",
    );
    const layerKey = yield* packed(node, [LAYER], "invalid layer key");
    const transaction = yield* FoundationDbTransaction;
    yield* transaction.set(childKey, prefix);
    yield* transaction.set(layerKey, nodeLayer ?? new Uint8Array());
    return yield* contentsOfNode(
      layer,
      node,
      path,
      nodeLayer ?? new Uint8Array(),
    );
  });

const existsInternal = (
  layer: DirectoryLayer,
  path: DirectoryPath,
): DirectoryEffect<boolean> =>
  Effect.gen(function* () {
    yield* checkVersion(layer, false);
    const node = yield* findNode(layer, path);
    if (node === undefined) {
      return false;
    }
    if (isInPartition(node, false)) {
      const partition = yield* getContents(layer, node);
      if (partition._tag !== "DirectoryPartition") {
        return yield* invalidMetadata(
          "partition node has a non-partition value",
        );
      }
      return yield* partition.exists(partitionSubpath(node));
    }
    return true;
  });

const listInternal = (
  layer: DirectoryLayer,
  path: DirectoryPath,
): DirectoryEffect<ReadonlyArray<string>> =>
  Effect.gen(function* () {
    yield* checkVersion(layer, false);
    const node = yield* findNode(layer, path);
    if (node === undefined) {
      return yield* error(
        "PathDoesNotExist",
        `directory /${path.join("/")} does not exist`,
      );
    }
    if (isInPartition(node, true)) {
      const partition = yield* getContents(layer, node);
      if (partition._tag !== "DirectoryPartition") {
        return yield* invalidMetadata(
          "partition node has a non-partition value",
        );
      }
      return yield* partition.list(partitionSubpath(node));
    }
    const [begin, end] = yield* subspaceRange(node.subspace, [0n]);
    const transaction = yield* FoundationDbTransaction;
    const children = yield* allInRange(transaction, begin, end);
    const names: Array<string> = [];
    for (const child of children) {
      const tuple = yield* unpacked(
        node.subspace,
        child.key,
        "invalid directory child key",
      );
      names.push(yield* directoryChildName(tuple));
    }
    return Object.freeze(names);
  });

const removeFromParent = (
  layer: DirectoryLayer,
  path: DirectoryPath,
): DirectoryEffect<void> =>
  Effect.gen(function* () {
    const name = path[path.length - 1];
    if (name === undefined) {
      return yield* error(
        "BadDestinationDirectory",
        "directory path must not be empty",
      );
    }
    const parent = yield* findNode(layer, path.slice(0, -1));
    if (parent === undefined) {
      return;
    }
    const key = yield* packed(
      parent.subspace,
      [0n, name],
      "invalid directory child key",
    );
    const transaction = yield* FoundationDbTransaction;
    yield* transaction.clear(key);
  });

const removeRecursive = (
  layer: DirectoryLayer,
  node: Subspace,
): DirectoryEffect<void> =>
  Effect.gen(function* () {
    const [begin, end] = yield* subspaceRange(node, [0n]);
    const transaction = yield* FoundationDbTransaction;
    const children = yield* allInRange(transaction, begin, end);
    for (const child of children) {
      yield* unpacked(
        node,
        child.key,
        "invalid directory child key",
      ).pipe(Effect.flatMap(directoryChildName));
      yield* removeRecursive(layer, yield* nodeWithPrefix(layer, child.value));
    }
    const tuple = yield* unpacked(
      layer[directoryLayerState].nodeSubspace,
      node.prefix,
      "invalid directory node",
    );
    const prefix = yield* asBytes(tuple[0], "directory node prefix");
    yield* transaction.clearRange(prefix, yield* incrementPrefix(prefix));
    const [metadataBegin, metadataEnd] = yield* subspaceRange(node);
    yield* transaction.clearRange(metadataBegin, metadataEnd);
  });

const removeInternal = (
  layer: DirectoryLayer,
  path: DirectoryPath,
  failIfMissing: boolean,
): DirectoryEffect<boolean> =>
  Effect.gen(function* () {
    yield* checkVersion(layer, true);
    if (path.length === 0) {
      return yield* error(
        "CannotModifyRootDirectory",
        "the root directory cannot be removed",
      );
    }
    const node = yield* findNode(layer, path);
    if (node === undefined) {
      if (failIfMissing) {
        return yield* error(
          "DirectoryDoesNotExist",
          `directory /${path.join("/")} does not exist`,
        );
      }
      return false;
    }
    if (isInPartition(node, false)) {
      const partition = yield* getContents(layer, node);
      if (partition._tag !== "DirectoryPartition") {
        return yield* invalidMetadata(
          "partition node has a non-partition value",
        );
      }
      return yield* (failIfMissing
        ? partition.remove(partitionSubpath(node))
        : partition.removeIfExists(partitionSubpath(node)));
    }
    yield* removeRecursive(layer, node.subspace);
    yield* removeFromParent(layer, path);
    return true;
  });

const moveInternal = (
  layer: DirectoryLayer,
  oldPath: DirectoryPath,
  newPath: DirectoryPath,
): DirectoryEffect<DirectoryOutput> =>
  Effect.gen(function* () {
    yield* checkVersion(layer, true);
    if (oldPath.length === 0) {
      return yield* error(
        "CannotMoveRootDirectory",
        "the root directory cannot be moved",
      );
    }
    if (pathStartsWith(newPath, oldPath)) {
      return yield* error(
        "CannotMoveIntoSubdirectory",
        "a directory cannot be moved into itself or one of its descendants",
      );
    }
    const oldNode = yield* findNode(layer, oldPath);
    if (oldNode === undefined) {
      return yield* error(
        "PathDoesNotExist",
        `directory /${oldPath.join("/")} does not exist`,
      );
    }
    const newNode = yield* findNode(layer, newPath);
    const oldInPartition = isInPartition(oldNode, false);
    if (newNode !== undefined) {
      const newInPartition = isInPartition(newNode, false);
      if (
        oldInPartition && newInPartition &&
        pathsEqual(oldNode.currentPath, newNode.currentPath)
      ) {
        const partition = yield* getContents(layer, oldNode);
        if (partition._tag !== "DirectoryPartition") {
          return yield* invalidMetadata(
            "partition node has a non-partition value",
          );
        }
        return yield* partition.moveTo(
          partitionSubpath(oldNode),
          partitionSubpath(newNode),
        );
      }
      if (oldInPartition || newInPartition) {
        return yield* error(
          "CannotMoveBetweenPartitions",
          "directories cannot be moved across partition boundaries",
        );
      }
      return yield* error(
        "DirectoryAlreadyExists",
        `directory /${newPath.join("/")} already exists`,
      );
    }
    if (oldInPartition) {
      return yield* error(
        "CannotMoveBetweenPartitions",
        "directories cannot be moved across partition boundaries",
      );
    }
    const name = newPath[newPath.length - 1];
    if (name === undefined) {
      return yield* error(
        "BadDestinationDirectory",
        "destination path must not be empty",
      );
    }
    const parent = yield* findNode(layer, newPath.slice(0, -1));
    if (parent === undefined) {
      return yield* error(
        "ParentDirectoryDoesNotExist",
        "destination parent directory does not exist",
      );
    }
    const prefixTuple = yield* unpacked(
      layer[directoryLayerState].nodeSubspace,
      oldNode.subspace.prefix,
      "invalid directory node",
    );
    const prefix = yield* asBytes(prefixTuple[0], "directory node prefix");
    const newKey = yield* packed(
      parent.subspace,
      [0n, name],
      "invalid destination directory key",
    );
    const transaction = yield* FoundationDbTransaction;
    yield* transaction.set(newKey, prefix);
    yield* removeFromParent(layer, oldPath);
    return yield* contentsOfNode(
      layer,
      oldNode.subspace,
      newPath,
      oldNode.layer,
    );
  });
