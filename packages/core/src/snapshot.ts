import { Schema } from "effect";

import {
  ChunkKey,
  ManifestKey,
  PackKey,
  Sha256Digest,
  sha256DigestFromObjectKey,
  SnapshotId,
} from "./ids";

const NonNegativeInteger = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0)
);

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

export const SafetySummary = Schema.Struct({
  skippedByBuiltInDenylist: NonNegativeInteger,
  skippedByUserExclude: NonNegativeInteger,
  skippedUnsupportedType: NonNegativeInteger,
});
export type SafetySummary = Schema.Schema.Type<typeof SafetySummary>;

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
