import { createHash } from "node:crypto";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  readlink,
  rename,
  rm,
  symlink,
  utimes,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

import {
  chunkKeyFromDigest,
  excludedPathsForConfig,
  isBuiltInDeniedWorkspacePath,
  isUserExcludedWorkspacePath,
  largeChunkSizeBytes,
  manifestKeyFromDigest,
  ManifestDescriptor,
  planSmallFilePacks,
  protocolVersion,
  SafeManifestPath,
  SaveManifest,
  Sha256Digest,
  sha256HexFromDigest,
  ChunkedFileContent,
  SnapshotObjectInventory,
  SnapshotManifest,
  smallFileThresholdBytes,
  workspacePathsForConfig,
} from "@stateful-ci/core";
import type {
  ChunkFileContentEntry,
  ChunkObjectInventoryEntry,
  GitContext,
  GitHubContext,
  ManifestObjectInventoryEntry,
  PackFileContent,
  PackObjectInventoryEntry,
  SnapshotDirectoryEntry,
  SnapshotFileEntry,
  SnapshotManifestEntry,
  SnapshotObjectInventoryEntry,
  SnapshotSymlinkEntry,
  StatefulCiConfigType,
  WorkspaceRef,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import {
  decodePack,
  encodePack,
  readPackEntry,
  sha256Bytes,
} from "./snapshot-pack";

export interface SnapshotProvenanceInput {
  readonly git: GitContext;
  readonly github: GitHubContext;
  readonly runId: string;
}

export interface CreateWorkspaceSnapshotInput {
  readonly config: StatefulCiConfigType;
  readonly provenance: SnapshotProvenanceInput;
  readonly workspace: WorkspaceRef;
  readonly workspaceRoot: string;
}

export interface CreatedWorkspaceSnapshot {
  readonly manifest: SnapshotManifest;
  readonly manifestDescriptor: ManifestDescriptor;
  readonly objects: SnapshotObjectInventory;
  readonly saveManifest: SaveManifest;
}

export interface RestoreWorkspaceSnapshotInput {
  readonly manifest: ManifestDescriptor;
  readonly workspaceRoot: string;
}

type EngineErrorReason =
  | "corrupt_object"
  | "duplicate_path"
  | "immutable_object_conflict"
  | "invalid_manifest"
  | "invalid_path"
  | "invalid_root"
  | "invalid_symlink"
  | "io_failed"
  | "restore_failed";

export class SnapshotEngineError extends Schema.TaggedErrorClass<SnapshotEngineError>()(
  "SnapshotEngineError",
  {
    message: Schema.String,
    path: Schema.optional(Schema.String),
    reason: Schema.Literals([
      "corrupt_object",
      "duplicate_path",
      "immutable_object_conflict",
      "invalid_manifest",
      "invalid_path",
      "invalid_root",
      "invalid_symlink",
      "io_failed",
      "restore_failed",
    ]),
  }
) {}

interface ScanState {
  readonly directories: SnapshotDirectoryEntry[];
  readonly files: ScannedFile[];
  readonly paths: Set<string>;
  readonly safety: {
    skippedByBuiltInDenylist: number;
    skippedByUserExclude: number;
    skippedUnsupportedType: number;
  };
  readonly symlinks: SnapshotSymlinkEntry[];
}

interface SmallScannedFile {
  readonly bytes: Uint8Array;
  readonly digest: Sha256Digest;
  readonly kind: "small";
  readonly mode: number;
  readonly mtime: number;
  readonly path: SafeManifestPath;
  readonly size: number;
}

interface LargeScannedFile {
  readonly absolutePath: string;
  readonly digest: Sha256Digest;
  readonly kind: "large";
  readonly mode: number;
  readonly mtime: number;
  readonly path: SafeManifestPath;
  readonly size: number;
}

type ScannedFile = SmallScannedFile | LargeScannedFile;

const storeRoot = ".stateful-ci/store";
const tempRoot = ".stateful-ci/tmp";

const engineError = (
  reason: EngineErrorReason,
  message: string,
  filePath?: string
) => new SnapshotEngineError({ message, path: filePath, reason });

const nodeFsErrorCode = (error: unknown) => {
  if (typeof error !== "object" || error === null || !("code" in error)) {
    return null;
  }

  return typeof error.code === "string" ? error.code : null;
};

const isMissingPathError = (error: unknown) => {
  const code = nodeFsErrorCode(error);

  return code === "ENOENT" || code === "ENOTDIR";
};

const toBytes = (source: string) =>
  new Uint8Array(Buffer.from(source, "utf-8"));

const concatBytes = (parts: readonly Uint8Array[]) => {
  const size = parts.reduce((total, part) => total + part.byteLength, 0);
  const bytes = new Uint8Array(size);
  let offset = 0;

  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }

  return bytes;
};

const sha256Text = (source: string) => sha256Bytes(toBytes(source));

const sha256File = (absolutePath: string) =>
  Effect.scoped(
    Effect.gen(function* sha256FileEffect() {
      const handle = yield* Effect.acquireRelease(
        Effect.tryPromise({
          catch: () =>
            engineError(
              "io_failed",
              `Could not open file ${absolutePath} for hashing. Check permissions and retry.`,
              absolutePath
            ),
          try: () => open(absolutePath, "r"),
        }),
        (fileHandle) =>
          Effect.promise(() => fileHandle.close()).pipe(Effect.orDie)
      );
      const hash = createHash("sha256");
      const buffer = Buffer.allocUnsafe(largeChunkSizeBytes);

      for (;;) {
        const { bytesRead } = yield* Effect.tryPromise({
          catch: () =>
            engineError(
              "io_failed",
              `Could not hash file ${absolutePath}. Check permissions and retry.`,
              absolutePath
            ),
          try: () => handle.read(buffer, 0, buffer.byteLength, null),
        });

        if (bytesRead === 0) {
          return Schema.decodeSync(Sha256Digest)(
            `sha256:${hash.digest("hex")}`
          );
        }

        hash.update(buffer.subarray(0, bytesRead));
      }
    })
  );

const normalizeRelativePath = (candidate: string) => {
  if (
    candidate.length === 0 ||
    candidate === "." ||
    candidate === ".." ||
    candidate === "~" ||
    candidate.includes("\0") ||
    candidate.includes("\\") ||
    path.isAbsolute(candidate) ||
    /^[A-Za-z]:/u.test(candidate)
  ) {
    return null;
  }

  const normalized = path.posix.normalize(candidate);

  if (
    normalized.length === 0 ||
    normalized === "." ||
    normalized === ".." ||
    normalized.startsWith("../") ||
    normalized === ".stateful-ci" ||
    normalized.startsWith(".stateful-ci/")
  ) {
    return null;
  }

  return Schema.decodeSync(SafeManifestPath)(normalized);
};

const rootContainsPath = (root: string, candidate: string) =>
  candidate === root || candidate.startsWith(`${root}/`);

const resolveManagedRoots = (config: StatefulCiConfigType) => {
  const roots = workspacePathsForConfig(config)
    .map((candidate) => normalizeRelativePath(candidate))
    .filter(
      (candidate): candidate is SafeManifestPath =>
        candidate !== null && !isBuiltInDeniedWorkspacePath(candidate)
    )
    .toSorted((left, right) => left.localeCompare(right));

  if (roots.length !== workspacePathsForConfig(config).length) {
    return Effect.fail(
      engineError(
        "invalid_root",
        "Configured managed roots include a broad, absolute, credential, or internal Stateful CI path. Fix stateful-ci.json and retry."
      )
    );
  }

  if (roots.length === 0) {
    return Effect.fail(
      engineError(
        "invalid_root",
        "Configured managed roots are empty. Add at least one narrow cache path to stateful-ci.json."
      )
    );
  }

  const duplicates = new Set<string>();

  for (const root of roots) {
    if (duplicates.has(root)) {
      return Effect.fail(
        engineError(
          "invalid_root",
          `Configured managed root ${root} is duplicated after normalization.`
        )
      );
    }
    duplicates.add(root);
  }

  const rootSource: unknown = roots;

  return Effect.succeed(
    Schema.decodeUnknownSync(Schema.NonEmptyArray(SafeManifestPath))(rootSource)
  );
};

const localObjectPath = (workspaceRoot: string, key: string) =>
  path.join(workspaceRoot, storeRoot, key);

const writeImmutableObject = (
  workspaceRoot: string,
  key: string,
  bytes: Uint8Array
) =>
  Effect.tryPromise({
    catch: (cause) =>
      cause instanceof SnapshotEngineError
        ? cause
        : engineError(
            "io_failed",
            `Could not write local snapshot object ${key}. Check workspace permissions and retry.`,
            key
          ),
    try: async () => {
      const objectPath = localObjectPath(workspaceRoot, key);
      const existing = await readFile(objectPath).catch(() => null);

      if (existing !== null) {
        if (Buffer.compare(existing, Buffer.from(bytes)) !== 0) {
          throw engineError(
            "immutable_object_conflict",
            `Local snapshot object ${key} already exists with different bytes. The store may be corrupted; remove .stateful-ci/store only after preserving diagnostics.`,
            key
          );
        }
        return;
      }

      await mkdir(path.dirname(objectPath), { recursive: true });
      await writeFile(objectPath, bytes, { flag: "wx" });
    },
  });

const readLocalObject = (workspaceRoot: string, key: string) =>
  Effect.tryPromise({
    catch: () =>
      engineError(
        "io_failed",
        `Could not read local snapshot object ${key}. Restore cannot safely continue until all backend-authorized objects are present.`,
        key
      ),
    try: async () =>
      new Uint8Array(await readFile(localObjectPath(workspaceRoot, key))),
  });

const verifyObjectBytes = (
  key: string,
  expectedDigest: Sha256Digest,
  expectedSize: number,
  bytes: Uint8Array
) => {
  if (bytes.byteLength !== expectedSize) {
    return Effect.fail(
      engineError(
        "corrupt_object",
        `Snapshot object ${key} size did not match the manifest. Restore did not mutate the workspace.`,
        key
      )
    );
  }

  if (sha256Bytes(bytes) !== expectedDigest) {
    return Effect.fail(
      engineError(
        "corrupt_object",
        `Snapshot object ${key} digest did not match the manifest. Restore did not mutate the workspace.`,
        key
      )
    );
  }

  return Effect.void;
};

export const storeVerifiedSnapshotObject = Effect.fn(
  "storeVerifiedSnapshotObject"
)(function* storeVerifiedSnapshotObjectEffect(input: {
  readonly digest: Sha256Digest;
  readonly key: string;
  readonly size: number;
  readonly bytes: Uint8Array;
  readonly workspaceRoot: string;
}) {
  yield* verifyObjectBytes(input.key, input.digest, input.size, input.bytes);
  yield* writeImmutableObject(input.workspaceRoot, input.key, input.bytes);
});

const addPath = (state: ScanState, manifestPath: SafeManifestPath) => {
  if (state.paths.has(manifestPath)) {
    return Effect.fail(
      engineError(
        "duplicate_path",
        `Snapshot path ${manifestPath} appeared more than once after normalization. Save cannot produce an ambiguous manifest.`,
        manifestPath
      )
    );
  }
  state.paths.add(manifestPath);
  return Effect.void;
};

const scanPath = (
  workspaceRoot: string,
  relativePath: string,
  managedRoot: SafeManifestPath,
  excludes: readonly string[],
  state: ScanState
): Effect.Effect<void, SnapshotEngineError> =>
  Effect.gen(function* scanPathEffect() {
    const manifestPath = normalizeRelativePath(relativePath);

    if (manifestPath === null || !rootContainsPath(managedRoot, manifestPath)) {
      return yield* engineError(
        "invalid_path",
        `Workspace path ${relativePath} is outside the configured managed root ${managedRoot}.`,
        relativePath
      );
    }

    if (isBuiltInDeniedWorkspacePath(manifestPath)) {
      state.safety.skippedByBuiltInDenylist += 1;
      return;
    }

    if (isUserExcludedWorkspacePath(manifestPath, excludes)) {
      state.safety.skippedByUserExclude += 1;
      return;
    }

    const absolutePath = path.join(workspaceRoot, manifestPath);
    const info = yield* Effect.tryPromise({
      catch: (error) => error,
      try: () => lstat(absolutePath),
    }).pipe(
      Effect.catch((error) =>
        isMissingPathError(error)
          ? Effect.succeed(null)
          : Effect.fail(
              engineError(
                "io_failed",
                `Could not inspect workspace path ${manifestPath}. Check permissions and retry.`,
                manifestPath
              )
            )
      )
    );

    if (info === null) {
      return;
    }

    yield* addPath(state, manifestPath);

    if (info.isDirectory()) {
      state.directories.push({
        mode: info.mode % 0o1000,
        mtime: Math.floor(info.mtimeMs),
        path: manifestPath,
        type: "directory",
      });

      const entries = yield* Effect.tryPromise({
        catch: () =>
          engineError(
            "io_failed",
            `Could not read managed directory ${manifestPath}. Check permissions and retry.`,
            manifestPath
          ),
        try: () => readdir(absolutePath),
      });

      for (const entry of entries.toSorted()) {
        yield* scanPath(
          workspaceRoot,
          `${manifestPath}/${entry}`,
          managedRoot,
          excludes,
          state
        );
      }
      return;
    }

    if (info.isSymbolicLink()) {
      const target = yield* Effect.tryPromise({
        catch: () =>
          engineError(
            "io_failed",
            `Could not read symlink target for ${manifestPath}. Check permissions and retry.`,
            manifestPath
          ),
        try: () => readlink(absolutePath),
      });
      const parent = path.posix.dirname(manifestPath);
      const resolvedTarget = path.posix.normalize(
        path.posix.join(parent, target)
      );

      if (
        target.length === 0 ||
        target.includes("\\") ||
        path.isAbsolute(target) ||
        /^[A-Za-z]:/u.test(target) ||
        resolvedTarget === ".." ||
        resolvedTarget.startsWith("../") ||
        !rootContainsPath(managedRoot, resolvedTarget)
      ) {
        return yield* engineError(
          "invalid_symlink",
          `Symlink ${manifestPath} points outside its managed root. Save refused to serialize an unsafe workspace state.`,
          manifestPath
        );
      }

      state.symlinks.push({ path: manifestPath, target, type: "symlink" });
      return;
    }

    if (!info.isFile()) {
      state.safety.skippedUnsupportedType += 1;
      return;
    }

    if (info.size > smallFileThresholdBytes) {
      state.files.push({
        absolutePath,
        digest: yield* sha256File(absolutePath),
        kind: "large",
        mode: info.mode % 0o1000,
        mtime: Math.floor(info.mtimeMs),
        path: manifestPath,
        size: info.size,
      });
      return;
    }

    const bytes = yield* Effect.tryPromise({
      catch: () =>
        engineError(
          "io_failed",
          `Could not read file ${manifestPath}. Check permissions and retry.`,
          manifestPath
        ),
      try: async () => new Uint8Array(await readFile(absolutePath)),
    });

    state.files.push({
      bytes,
      digest: sha256Bytes(bytes),
      kind: "small",
      mode: info.mode % 0o1000,
      mtime: Math.floor(info.mtimeMs),
      path: manifestPath,
      size: info.size,
    });
  });

const createPackObjects = Effect.fn("createPackObjects")(
  function* createPackObjectsEffect(
    workspaceRoot: string,
    files: readonly SmallScannedFile[]
  ) {
    const smallFiles = files;
    const byDigest = new Map(smallFiles.map((file) => [file.digest, file]));
    const packObjects: PackObjectInventoryEntry[] = [];
    const packContentByDigest = new Map<Sha256Digest, PackFileContent>();

    for (const plan of planSmallFilePacks(smallFiles)) {
      const encoded = yield* encodePack(
        plan.entries.flatMap((entry) => {
          const file = byDigest.get(entry.digest);
          return file === undefined
            ? []
            : [{ bytes: file.bytes, digest: file.digest }];
        })
      );
      const object = {
        digest: encoded.digest,
        key: encoded.key,
        kind: "pack",
        size: encoded.bytes.byteLength,
      } satisfies PackObjectInventoryEntry;

      yield* writeImmutableObject(workspaceRoot, encoded.key, encoded.bytes);
      packObjects.push(object);

      for (const entry of encoded.index.entries) {
        packContentByDigest.set(entry.entryDigest, {
          compressedLength: entry.compressedLength,
          compressedOffset: entry.compressedOffset,
          compression: entry.compression,
          entryDigest: entry.entryDigest,
          kind: "pack",
          packDigest: encoded.digest,
          packKey: encoded.key,
          uncompressedSize: entry.uncompressedSize,
        });
      }
    }

    return { packContentByDigest, packObjects };
  }
);

const createChunkContent = Effect.fn("createChunkContent")(
  function* createChunkContentEffect(
    workspaceRoot: string,
    file: LargeScannedFile
  ) {
    const chunks: ChunkFileContentEntry[] = [];
    const objects: ChunkObjectInventoryEntry[] = [];

    yield* Effect.scoped(
      Effect.gen(function* readLargeFileChunksEffect() {
        const handle = yield* Effect.acquireRelease(
          Effect.tryPromise({
            catch: () =>
              engineError(
                "io_failed",
                `Could not open large file ${file.path} for chunking.`,
                file.path
              ),
            try: () => open(file.absolutePath, "r"),
          }),
          (fileHandle) =>
            Effect.promise(() => fileHandle.close()).pipe(Effect.orDie)
        );
        const fileHash = createHash("sha256");
        const buffer = Buffer.allocUnsafe(largeChunkSizeBytes);
        let ordinal = 0;
        let totalSize = 0;

        for (;;) {
          const { bytesRead } = yield* Effect.tryPromise({
            catch: () =>
              engineError(
                "io_failed",
                `Could not read large file ${file.path} while writing chunks.`,
                file.path
              ),
            try: () => handle.read(buffer, 0, buffer.byteLength, null),
          });

          if (bytesRead === 0) {
            const digest = Schema.decodeSync(Sha256Digest)(
              `sha256:${fileHash.digest("hex")}`
            );

            if (digest !== file.digest || totalSize !== file.size) {
              return yield* engineError(
                "io_failed",
                `Large file ${file.path} changed while the snapshot was being created. Save was aborted before publishing an inconsistent manifest. Retry after the workspace is stable.`,
                file.path
              );
            }

            return;
          }

          const bytes = new Uint8Array(buffer.subarray(0, bytesRead));
          const digest = sha256Bytes(bytes);
          const key = chunkKeyFromDigest(digest);
          const object = {
            digest,
            key,
            kind: "chunk",
            size: bytes.byteLength,
          } satisfies ChunkObjectInventoryEntry;

          fileHash.update(bytes);
          totalSize += bytes.byteLength;
          yield* writeImmutableObject(workspaceRoot, key, bytes);
          chunks.push({ digest, key, ordinal, size: bytes.byteLength });
          objects.push(object);
          ordinal += 1;
        }
      })
    );

    return { chunks, objects };
  }
);

export const createWorkspaceSnapshot = Effect.fn("createWorkspaceSnapshot")(
  function* createWorkspaceSnapshotEffect(input: CreateWorkspaceSnapshotInput) {
    const managedRoots = yield* resolveManagedRoots(input.config);
    const excludes = excludedPathsForConfig(input.config);
    const state: ScanState = {
      directories: [],
      files: [],
      paths: new Set(),
      safety: {
        skippedByBuiltInDenylist: 0,
        skippedByUserExclude: 0,
        skippedUnsupportedType: 0,
      },
      symlinks: [],
    };

    for (const root of managedRoots) {
      yield* scanPath(input.workspaceRoot, root, root, excludes, state);
    }

    const { packContentByDigest, packObjects } = yield* createPackObjects(
      input.workspaceRoot,
      state.files.filter((file) => file.kind === "small")
    );
    const chunkObjectsByKey = new Map<string, ChunkObjectInventoryEntry>();
    const fileEntries: SnapshotFileEntry[] = [];

    for (const file of state.files.toSorted((left, right) =>
      left.path.localeCompare(right.path)
    )) {
      if (file.kind === "small") {
        const content = packContentByDigest.get(file.digest);

        if (content === undefined) {
          return yield* engineError(
            "invalid_manifest",
            `No pack content reference was produced for small file ${file.path}.`,
            file.path
          );
        }

        fileEntries.push({
          content,
          mode: file.mode,
          mtime: file.mtime,
          path: file.path,
          sha256: file.digest,
          size: file.size,
          type: "file",
        });
        continue;
      }

      const chunked = yield* createChunkContent(input.workspaceRoot, file);

      for (const object of chunked.objects) {
        chunkObjectsByKey.set(object.key, object);
      }

      const contentSource: unknown = {
        chunks: chunked.chunks,
        kind: "chunks",
      };
      const content =
        Schema.decodeUnknownSync(ChunkedFileContent)(contentSource);

      fileEntries.push({
        content,
        mode: file.mode,
        mtime: file.mtime,
        path: file.path,
        sha256: file.digest,
        size: file.size,
        type: "file",
      });
    }

    const payloadObjects = [
      ...packObjects.toSorted((left, right) =>
        left.key.localeCompare(right.key)
      ),
      ...[...chunkObjectsByKey.values()].toSorted((left, right) =>
        left.key.localeCompare(right.key)
      ),
    ];
    const manifestWithoutId = {
      createdAt: "snapshot-v1",
      entries: [
        ...state.directories,
        ...fileEntries,
        ...state.symlinks,
      ].toSorted((left, right) => left.path.localeCompare(right.path)),
      formatVersion: 1 as const,
      managedRoots,
      objects: payloadObjects,
      provenance: input.provenance,
      safety: state.safety,
      stats: {
        chunkCount: chunkObjectsByKey.size,
        directoryCount: state.directories.length,
        fileCount: state.files.length,
        packCount: packObjects.length,
        symlinkCount: state.symlinks.length,
        totalBytes: state.files.reduce((total, file) => total + file.size, 0),
      },
      workspace: input.workspace,
    };
    const manifestSeedJson = Schema.encodeUnknownSync(
      Schema.fromJsonString(Schema.Unknown)
    )(manifestWithoutId);
    const snapshotId = `snap_${sha256HexFromDigest(
      sha256Text(manifestSeedJson)
    ).slice(0, 24)}`;
    const manifestSource: unknown = {
      ...manifestWithoutId,
      snapshotId,
    };
    const manifest = Schema.decodeUnknownSync(SnapshotManifest)(manifestSource);
    const manifestJson = Schema.encodeUnknownSync(
      Schema.fromJsonString(SnapshotManifest)
    )(manifest);
    const manifestBytes = toBytes(manifestJson);
    const manifestDigest = sha256Bytes(manifestBytes);
    const manifestKey = manifestKeyFromDigest(manifestDigest);
    const manifestObject = {
      digest: manifestDigest,
      key: manifestKey,
      kind: "manifest",
      size: manifestBytes.byteLength,
    } satisfies ManifestObjectInventoryEntry;
    const objects = Schema.decodeSync(SnapshotObjectInventory)([
      manifestObject,
      ...payloadObjects,
    ]);
    const manifestDescriptor = Schema.decodeSync(ManifestDescriptor)({
      digest: manifestDigest,
      key: manifestKey,
      size: manifestBytes.byteLength,
      snapshotId,
    });
    const saveManifest = Schema.decodeSync(SaveManifest)({
      chunkCount: manifest.stats.chunkCount,
      fileCount: manifest.stats.fileCount,
      hash: manifestDigest,
      id: snapshotId,
      key: manifestKey,
      objects,
      safety: manifest.safety,
      totalBytes: manifest.stats.totalBytes,
    });

    yield* writeImmutableObject(
      input.workspaceRoot,
      manifestKey,
      manifestBytes
    );

    return {
      manifest,
      manifestDescriptor,
      objects,
      saveManifest,
    } satisfies CreatedWorkspaceSnapshot;
  }
);

const validateManifestEntryPath = (
  manifest: SnapshotManifest,
  entry: SnapshotManifestEntry,
  paths: Set<string>,
  entryTypesByPath: Map<string, SnapshotManifestEntry["type"]>
) => {
  const safePath = normalizeRelativePath(entry.path);

  if (safePath === null) {
    return Effect.fail(
      engineError("invalid_path", `Manifest entry ${entry.path} is unsafe.`)
    );
  }

  if (!manifest.managedRoots.some((root) => rootContainsPath(root, safePath))) {
    return Effect.fail(
      engineError(
        "invalid_path",
        `Manifest entry ${entry.path} is outside all managed roots.`
      )
    );
  }

  if (paths.has(safePath)) {
    return Effect.fail(
      engineError(
        "duplicate_path",
        `Manifest path ${safePath} appears more than once.`
      )
    );
  }

  paths.add(safePath);
  entryTypesByPath.set(safePath, entry.type);
  return Effect.succeed(safePath);
};

const validateManifestSymlinkTarget = (
  manifest: SnapshotManifest,
  entry: SnapshotSymlinkEntry,
  safePath: string
) => {
  const root = manifest.managedRoots.find((candidate) =>
    rootContainsPath(candidate, safePath)
  );
  const resolvedTarget = path.posix.normalize(
    path.posix.join(path.posix.dirname(safePath), entry.target)
  );

  return root === undefined ||
    entry.target.length === 0 ||
    entry.target.includes("\\") ||
    path.isAbsolute(entry.target) ||
    /^[A-Za-z]:/u.test(entry.target) ||
    resolvedTarget === ".." ||
    resolvedTarget.startsWith("../") ||
    !rootContainsPath(root, resolvedTarget)
    ? Effect.fail(
        engineError(
          "invalid_symlink",
          `Manifest symlink ${safePath} points outside its managed root.`
        )
      )
    : Effect.void;
};

const validateNoSymlinkParents = (
  entry: SnapshotManifestEntry,
  safePath: string,
  entryTypesByPath: ReadonlyMap<string, SnapshotManifestEntry["type"]>
) => {
  let ancestor = "";

  for (const part of safePath.split("/").slice(0, -1)) {
    ancestor = ancestor.length === 0 ? part : `${ancestor}/${part}`;

    if (entryTypesByPath.get(ancestor) === "symlink") {
      return Effect.fail(
        engineError(
          "invalid_symlink",
          `Manifest path ${safePath} restores through symlink parent ${ancestor}.`,
          safePath
        )
      );
    }
  }

  if (
    entry.type === "symlink" &&
    [...entryTypesByPath.keys()].some((candidate) =>
      candidate.startsWith(`${safePath}/`)
    )
  ) {
    return Effect.fail(
      engineError(
        "invalid_symlink",
        `Manifest symlink ${safePath} has descendant entries.`,
        safePath
      )
    );
  }

  return Effect.void;
};

const validateFileObjectReferences = (
  entry: SnapshotFileEntry,
  safePath: string,
  objectsByKey: ReadonlyMap<string, SnapshotObjectInventoryEntry>
) => {
  if (entry.content.kind === "pack") {
    const object = objectsByKey.get(entry.content.packKey);

    return object === undefined ||
      object.kind !== "pack" ||
      object.digest !== entry.content.packDigest
      ? Effect.fail(
          engineError(
            "invalid_manifest",
            `Manifest file ${safePath} references pack ${entry.content.packKey} outside the object inventory.`,
            safePath
          )
        )
      : Effect.void;
  }

  for (const chunk of entry.content.chunks) {
    const object = objectsByKey.get(chunk.key);

    if (
      object === undefined ||
      object.kind !== "chunk" ||
      object.digest !== chunk.digest ||
      object.size !== chunk.size
    ) {
      return Effect.fail(
        engineError(
          "invalid_manifest",
          `Manifest file ${safePath} references chunk ${chunk.key} outside the object inventory.`,
          safePath
        )
      );
    }
  }

  return Effect.void;
};

const validateManifestForRestore = Effect.fn("validateManifestForRestore")(
  function* validateManifestForRestoreEffect(manifest: SnapshotManifest) {
    const paths = new Set<string>();
    const entryTypesByPath = new Map<string, SnapshotManifestEntry["type"]>();
    const safePathsByEntry = new Map<SnapshotManifestEntry, string>();
    const objectsByKey = new Map<string, SnapshotObjectInventoryEntry>();

    for (const object of manifest.objects) {
      objectsByKey.set(object.key, object);
    }

    for (const root of manifest.managedRoots) {
      if (normalizeRelativePath(root) === null) {
        return yield* engineError(
          "invalid_root",
          `Manifest managed root ${root} is unsafe.`
        );
      }
    }

    for (const entry of manifest.entries) {
      const safePath = yield* validateManifestEntryPath(
        manifest,
        entry,
        paths,
        entryTypesByPath
      );

      safePathsByEntry.set(entry, safePath);

      if (entry.type === "symlink") {
        yield* validateManifestSymlinkTarget(manifest, entry, safePath);
      }
    }

    for (const entry of manifest.entries) {
      const safePath = safePathsByEntry.get(entry);

      if (safePath === undefined) {
        return yield* engineError(
          "invalid_path",
          `Manifest entry ${entry.path} is unsafe.`
        );
      }

      yield* validateNoSymlinkParents(entry, safePath, entryTypesByPath);

      if (entry.type === "file") {
        yield* validateFileObjectReferences(entry, safePath, objectsByKey);
      }
    }

    return objectsByKey;
  }
);

const materializeEntry = Effect.fn("materializeEntry")(
  function* materializeEntryEffect(
    workspaceRoot: string,
    stagingRoot: string,
    manifest: SnapshotManifest,
    entry: SnapshotManifestEntry,
    objectsByKey: ReadonlyMap<string, SnapshotObjectInventoryEntry>,
    packBytesByKey: ReadonlyMap<string, Uint8Array>
  ) {
    const target = path.join(stagingRoot, entry.path);

    if (entry.type === "directory") {
      yield* Effect.tryPromise({
        catch: () =>
          engineError(
            "io_failed",
            `Could not create staged directory ${entry.path}.`,
            entry.path
          ),
        try: () => mkdir(target, { recursive: true }),
      });
      yield* Effect.tryPromise({
        catch: () =>
          engineError(
            "io_failed",
            `Could not apply metadata to staged directory ${entry.path}.`,
            entry.path
          ),
        try: async () => {
          await chmod(target, entry.mode);
          await utimes(target, entry.mtime / 1000, entry.mtime / 1000);
        },
      });
      return;
    }

    yield* Effect.tryPromise({
      catch: () =>
        engineError(
          "io_failed",
          `Could not create staged parent for ${entry.path}.`,
          entry.path
        ),
      try: () => mkdir(path.dirname(target), { recursive: true }),
    });

    if (entry.type === "symlink") {
      yield* Effect.tryPromise({
        catch: () =>
          engineError(
            "io_failed",
            `Could not create staged symlink ${entry.path}.`,
            entry.path
          ),
        try: () => symlink(entry.target, target),
      });
      return;
    }

    const bytes = yield* Effect.gen(function* materializeFileBytesEffect() {
      if (entry.content.kind === "pack") {
        const packBytes = packBytesByKey.get(entry.content.packKey);

        if (packBytes === undefined) {
          return yield* engineError(
            "corrupt_object",
            `Pack ${entry.content.packKey} is missing for ${entry.path}.`,
            entry.path
          );
        }

        const packEntry = yield* readPackEntry(
          packBytes,
          entry.content.entryDigest,
          entry.content.packDigest
        );
        return packEntry.bytes;
      }

      const chunks: Uint8Array[] = [];

      for (const chunk of entry.content.chunks.toSorted(
        (left, right) => left.ordinal - right.ordinal
      )) {
        const object = objectsByKey.get(chunk.key);

        if (object === undefined || object.kind !== "chunk") {
          return yield* engineError(
            "invalid_manifest",
            `Chunk ${chunk.key} is missing from the manifest object inventory.`,
            entry.path
          );
        }

        const chunkBytes = yield* readLocalObject(workspaceRoot, chunk.key);
        yield* verifyObjectBytes(
          chunk.key,
          object.digest,
          object.size,
          chunkBytes
        );
        chunks.push(chunkBytes);
      }

      return concatBytes(chunks);
    });

    if (
      sha256Bytes(bytes) !== entry.sha256 ||
      bytes.byteLength !== entry.size
    ) {
      return yield* engineError(
        "corrupt_object",
        `Restored bytes for ${entry.path} did not match the manifest digest and size. Restore did not mutate the workspace.`,
        entry.path
      );
    }

    yield* Effect.tryPromise({
      catch: () =>
        engineError(
          "io_failed",
          `Could not write staged file ${entry.path}.`,
          entry.path
        ),
      try: () => writeFile(target, bytes),
    });
    yield* Effect.tryPromise({
      catch: () =>
        engineError(
          "io_failed",
          `Could not apply metadata to staged file ${entry.path}.`,
          entry.path
        ),
      try: async () => {
        await chmod(target, entry.mode);
        await utimes(target, entry.mtime / 1000, entry.mtime / 1000);
      },
    });

    if (
      !manifest.managedRoots.some((root) => rootContainsPath(root, entry.path))
    ) {
      return yield* engineError(
        "invalid_path",
        `Manifest path ${entry.path} escaped managed roots.`,
        entry.path
      );
    }
  }
);

const exactReplaceManagedRoots = (
  workspaceRoot: string,
  stagingRoot: string,
  roots: readonly SafeManifestPath[]
) =>
  Effect.tryPromise({
    catch: () =>
      engineError(
        "restore_failed",
        "Could not replace managed roots atomically. Previous roots were preserved or restored; inspect .stateful-ci/tmp if diagnostics are needed."
      ),
    try: async () => {
      const backupRoot = await mkdtemp(
        path.join(workspaceRoot, tempRoot, "restore-backup-")
      );
      const movedRoots: { live: string; backup: string }[] = [];
      const assertRealDirectoryAncestors = async (absolutePath: string) => {
        const relative = path.relative(workspaceRoot, absolutePath);
        const parts = relative
          .split(path.sep)
          .filter((part) => part.length > 0);
        let current = workspaceRoot;

        for (const part of parts) {
          current = path.join(current, part);
          const info = await lstat(current).catch(() => null);

          if (info === null) {
            return;
          }

          if (!info.isDirectory() || info.isSymbolicLink()) {
            throw new Error(
              `Restore parent ${current} is not a real directory.`
            );
          }
        }
      };

      try {
        for (const root of roots) {
          const live = path.join(workspaceRoot, root);
          const staged = path.join(stagingRoot, root);
          const backup = path.join(backupRoot, root);
          const liveInfo = await lstat(live).catch(() => null);

          await assertRealDirectoryAncestors(path.dirname(live));
          await assertRealDirectoryAncestors(path.dirname(backup));
          if (liveInfo !== null && liveInfo.isSymbolicLink()) {
            throw new Error(`Managed root ${live} is a symlink.`);
          }
          await mkdir(path.dirname(live), { recursive: true });
          await mkdir(path.dirname(backup), { recursive: true });

          if (liveInfo !== null) {
            await rename(live, backup);
            movedRoots.push({ backup, live });
          }

          await rename(staged, live);
        }

        await rm(backupRoot, { force: true, recursive: true });
      } catch (error) {
        for (const moved of movedRoots.toReversed()) {
          const liveInfo = await lstat(moved.live).catch(() => null);
          if (liveInfo !== null) {
            await rm(moved.live, { force: true, recursive: true });
          }
          const backupInfo = await lstat(moved.backup).catch(() => null);
          if (backupInfo !== null) {
            await mkdir(path.dirname(moved.live), { recursive: true });
            await rename(moved.backup, moved.live);
          }
        }
        throw error;
      }
    },
  });

export const restoreWorkspaceSnapshot = Effect.fn("restoreWorkspaceSnapshot")(
  function* restoreWorkspaceSnapshotEffect(
    input: RestoreWorkspaceSnapshotInput
  ) {
    const manifestBytes = yield* readLocalObject(
      input.workspaceRoot,
      input.manifest.key
    );

    yield* verifyObjectBytes(
      input.manifest.key,
      input.manifest.digest,
      input.manifest.size,
      manifestBytes
    );

    const manifest = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(SnapshotManifest)
    )(Buffer.from(manifestBytes).toString("utf-8")).pipe(
      Effect.mapError(() =>
        engineError(
          "invalid_manifest",
          "Snapshot manifest JSON did not match the production schema. Restore did not mutate the workspace."
        )
      )
    );

    const objectsByKey = yield* validateManifestForRestore(manifest);

    const packBytesByKey = new Map<string, Uint8Array>();

    for (const object of manifest.objects) {
      const bytes = yield* readLocalObject(input.workspaceRoot, object.key);
      yield* verifyObjectBytes(object.key, object.digest, object.size, bytes);

      if (object.kind === "pack") {
        yield* decodePack(bytes, object.digest);
        packBytesByKey.set(object.key, bytes);
      }
    }

    const stagingRoot = yield* Effect.tryPromise({
      catch: () =>
        engineError(
          "io_failed",
          "Could not create a restore staging directory under .stateful-ci/tmp."
        ),
      try: async () => {
        await mkdir(path.join(input.workspaceRoot, tempRoot), {
          recursive: true,
        });
        return mkdtemp(path.join(input.workspaceRoot, tempRoot, "restore-"));
      },
    });

    yield* Effect.tryPromise({
      catch: () =>
        engineError(
          "io_failed",
          "Could not create staged managed root parents."
        ),
      try: async () => {
        for (const root of manifest.managedRoots) {
          await mkdir(path.join(stagingRoot, root), { recursive: true });
        }
      },
    });

    for (const entry of manifest.entries.toSorted((left, right) => {
      const order = { directory: 0, file: 1, symlink: 2 } as const;
      return (
        order[left.type] - order[right.type] ||
        left.path.localeCompare(right.path)
      );
    })) {
      yield* materializeEntry(
        input.workspaceRoot,
        stagingRoot,
        manifest,
        entry,
        objectsByKey,
        packBytesByKey
      );
    }

    yield* exactReplaceManagedRoots(
      input.workspaceRoot,
      stagingRoot,
      manifest.managedRoots
    ).pipe(
      Effect.flatMap(() =>
        Effect.tryPromise({
          catch: () =>
            engineError(
              "io_failed",
              "Could not remove restore staging directory after success."
            ),
          try: () => rm(stagingRoot, { force: true, recursive: true }),
        })
      ),
      Effect.catch((error: SnapshotEngineError) =>
        Effect.tryPromise({
          catch: () => error,
          try: () => rm(stagingRoot, { force: true, recursive: true }),
        }).pipe(Effect.flatMap(() => Effect.fail(error)))
      )
    );

    return manifest;
  }
);

export const saveManifestRequestBody = (
  snapshot: CreatedWorkspaceSnapshot
) => ({
  manifest: snapshot.saveManifest,
  protocolVersion,
});
