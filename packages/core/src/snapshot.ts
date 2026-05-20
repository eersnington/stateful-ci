import { Schema } from "effect";

import { GitContext, GitHubContext } from "./github";
import {
  ChunkKey,
  ManifestKey,
  PackKey,
  RunId,
  Sha256Digest,
  sha256DigestFromObjectKey,
  SnapshotId,
} from "./ids";
import { WorkspaceRef } from "./workspace";

const NonNegativeInteger = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0)
);

export const SafeManifestPath = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isPattern(/^(?!\/)(?![A-Za-z]:)(?!.*\\)(?!.*(?:^|\/)\.\.(?:\/|$)).+$/u)
)
  .check(
    Schema.makeFilter((path: string) =>
      path.includes("\0")
        ? { issue: "manifest paths must not contain null bytes", path: [] }
        : undefined
    )
  )
  .pipe(Schema.brand("@stateful-ci/SafeManifestPath"));
export type SafeManifestPath = Schema.Schema.Type<typeof SafeManifestPath>;

const SymlinkTarget = Schema.String.check(Schema.isMinLength(1)).check(
  Schema.makeFilter((target: string) =>
    target.includes("\0")
      ? { issue: "symlink targets must not contain null bytes", path: [] }
      : undefined
  )
);

export const CompressionAlgorithm = Schema.Literal("gzip");
export type CompressionAlgorithm = Schema.Schema.Type<
  typeof CompressionAlgorithm
>;

export const SnapshotObjectKind = Schema.Literals([
  "manifest",
  "pack",
  "chunk",
]);
export type SnapshotObjectKind = Schema.Schema.Type<typeof SnapshotObjectKind>;

const objectDigestMatchesKey = <
  A extends {
    readonly digest: Sha256Digest;
    readonly key: ManifestKey | PackKey | ChunkKey;
  },
>() =>
  Schema.makeFilter((object: A) =>
    object.digest === sha256DigestFromObjectKey(object.key)
      ? undefined
      : {
          issue:
            "object digest must match the canonical digest embedded in key",
          path: ["digest"],
        }
  );

export const ManifestObjectInventoryEntry = Schema.Struct({
  digest: Sha256Digest,
  key: ManifestKey,
  kind: Schema.Literal("manifest"),
  size: NonNegativeInteger,
}).check(objectDigestMatchesKey());
export type ManifestObjectInventoryEntry = Schema.Schema.Type<
  typeof ManifestObjectInventoryEntry
>;

export const PackObjectInventoryEntry = Schema.Struct({
  digest: Sha256Digest,
  key: PackKey,
  kind: Schema.Literal("pack"),
  size: NonNegativeInteger,
}).check(objectDigestMatchesKey());
export type PackObjectInventoryEntry = Schema.Schema.Type<
  typeof PackObjectInventoryEntry
>;

export const ChunkObjectInventoryEntry = Schema.Struct({
  digest: Sha256Digest,
  key: ChunkKey,
  kind: Schema.Literal("chunk"),
  size: NonNegativeInteger,
}).check(objectDigestMatchesKey());
export type ChunkObjectInventoryEntry = Schema.Schema.Type<
  typeof ChunkObjectInventoryEntry
>;

export const SnapshotObjectInventoryEntry = Schema.Union([
  ManifestObjectInventoryEntry,
  PackObjectInventoryEntry,
  ChunkObjectInventoryEntry,
]);
export type SnapshotObjectInventoryEntry = Schema.Schema.Type<
  typeof SnapshotObjectInventoryEntry
>;

export const SnapshotObjectInventory = Schema.NonEmptyArray(
  SnapshotObjectInventoryEntry
);
export type SnapshotObjectInventory = Schema.Schema.Type<
  typeof SnapshotObjectInventory
>;

export const SnapshotManifestObjectInventory = Schema.Array(
  Schema.Union([PackObjectInventoryEntry, ChunkObjectInventoryEntry])
);
export type SnapshotManifestObjectInventory = Schema.Schema.Type<
  typeof SnapshotManifestObjectInventory
>;

export const SafetySummary = Schema.Struct({
  skippedByBuiltInDenylist: NonNegativeInteger,
  skippedByUserExclude: NonNegativeInteger,
  skippedUnsupportedType: NonNegativeInteger,
});
export type SafetySummary = Schema.Schema.Type<typeof SafetySummary>;

export const PackIndexEntry = Schema.Struct({
  compressedLength: NonNegativeInteger,
  compressedOffset: NonNegativeInteger,
  compression: CompressionAlgorithm,
  entryDigest: Sha256Digest,
  uncompressedSize: NonNegativeInteger,
});
export type PackIndexEntry = Schema.Schema.Type<typeof PackIndexEntry>;

export const PackIndex = Schema.Struct({
  compression: CompressionAlgorithm,
  entries: Schema.Array(PackIndexEntry),
  formatVersion: Schema.Literal(1),
});
export type PackIndex = Schema.Schema.Type<typeof PackIndex>;

export const PackFileContent = Schema.Struct({
  compressedLength: NonNegativeInteger,
  compressedOffset: NonNegativeInteger,
  compression: CompressionAlgorithm,
  entryDigest: Sha256Digest,
  kind: Schema.Literal("pack"),
  packDigest: Sha256Digest,
  packKey: PackKey,
  uncompressedSize: NonNegativeInteger,
}).check(
  Schema.makeFilter((content) =>
    content.packDigest === sha256DigestFromObjectKey(content.packKey)
      ? undefined
      : {
          issue:
            "packDigest must match the canonical digest embedded in packKey",
          path: ["packDigest"],
        }
  )
);
export type PackFileContent = Schema.Schema.Type<typeof PackFileContent>;

export const ChunkFileContentEntry = Schema.Struct({
  digest: Sha256Digest,
  key: ChunkKey,
  ordinal: NonNegativeInteger,
  size: NonNegativeInteger,
}).check(objectDigestMatchesKey());
export type ChunkFileContentEntry = Schema.Schema.Type<
  typeof ChunkFileContentEntry
>;

export const ChunkedFileContent = Schema.Struct({
  chunks: Schema.NonEmptyArray(ChunkFileContentEntry),
  kind: Schema.Literal("chunks"),
});
export type ChunkedFileContent = Schema.Schema.Type<typeof ChunkedFileContent>;

export const SnapshotFileContent = Schema.Union([
  PackFileContent,
  ChunkedFileContent,
]);
export type SnapshotFileContent = Schema.Schema.Type<
  typeof SnapshotFileContent
>;

const EntryMetadata = {
  mode: NonNegativeInteger,
  mtime: NonNegativeInteger,
  path: SafeManifestPath,
} as const;

export const SnapshotDirectoryEntry = Schema.Struct({
  ...EntryMetadata,
  type: Schema.Literal("directory"),
});
export type SnapshotDirectoryEntry = Schema.Schema.Type<
  typeof SnapshotDirectoryEntry
>;

export const SnapshotFileEntry = Schema.Struct({
  ...EntryMetadata,
  content: SnapshotFileContent,
  sha256: Sha256Digest,
  size: NonNegativeInteger,
  type: Schema.Literal("file"),
});
export type SnapshotFileEntry = Schema.Schema.Type<typeof SnapshotFileEntry>;

export const SnapshotSymlinkEntry = Schema.Struct({
  path: SafeManifestPath,
  target: SymlinkTarget,
  type: Schema.Literal("symlink"),
});
export type SnapshotSymlinkEntry = Schema.Schema.Type<
  typeof SnapshotSymlinkEntry
>;

export const SnapshotManifestEntry = Schema.Union([
  SnapshotDirectoryEntry,
  SnapshotFileEntry,
  SnapshotSymlinkEntry,
]);
export type SnapshotManifestEntry = Schema.Schema.Type<
  typeof SnapshotManifestEntry
>;

export const SnapshotManifestStats = Schema.Struct({
  chunkCount: NonNegativeInteger,
  directoryCount: NonNegativeInteger,
  fileCount: NonNegativeInteger,
  packCount: NonNegativeInteger,
  symlinkCount: NonNegativeInteger,
  totalBytes: NonNegativeInteger,
});
export type SnapshotManifestStats = Schema.Schema.Type<
  typeof SnapshotManifestStats
>;

export const SnapshotManifestProvenance = Schema.Struct({
  git: GitContext,
  github: GitHubContext,
  runId: RunId,
});
export type SnapshotManifestProvenance = Schema.Schema.Type<
  typeof SnapshotManifestProvenance
>;

export const SnapshotManifest = Schema.Struct({
  createdAt: Schema.String.check(Schema.isMinLength(1)),
  entries: Schema.Array(SnapshotManifestEntry),
  formatVersion: Schema.Literal(1),
  managedRoots: Schema.NonEmptyArray(SafeManifestPath),
  objects: SnapshotManifestObjectInventory,
  provenance: SnapshotManifestProvenance,
  safety: SafetySummary,
  snapshotId: SnapshotId,
  stats: SnapshotManifestStats,
  workspace: WorkspaceRef,
});
export type SnapshotManifest = Schema.Schema.Type<typeof SnapshotManifest>;

export const ManifestDescriptor = Schema.Struct({
  digest: Sha256Digest,
  key: ManifestKey,
  size: NonNegativeInteger,
  snapshotId: SnapshotId,
}).check(objectDigestMatchesKey());
export type ManifestDescriptor = Schema.Schema.Type<typeof ManifestDescriptor>;

export const SnapshotRef = Schema.Struct({
  id: SnapshotId,
  manifestKey: ManifestKey,
  parent: Schema.NullOr(SnapshotId),
});
export type SnapshotRef = Schema.Schema.Type<typeof SnapshotRef>;

export const SaveManifest = Schema.Struct({
  chunkCount: NonNegativeInteger,
  fileCount: NonNegativeInteger,
  hash: Sha256Digest,
  id: SnapshotId,
  key: ManifestKey,
  objects: SnapshotObjectInventory,
  safety: SafetySummary,
  totalBytes: NonNegativeInteger,
});
export type SaveManifest = Schema.Schema.Type<typeof SaveManifest>;
