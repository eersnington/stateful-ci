import { Schema } from "effect";

import { ArchiveKey, ManifestKey, Sha256Digest, SnapshotId } from "./ids";

export const SnapshotRef = Schema.Struct({
  id: SnapshotId,
  manifestDigest: Sha256Digest,
  manifestKey: ManifestKey,
  parent: Schema.NullOr(SnapshotId),
});
export type SnapshotRef = Schema.Schema.Type<typeof SnapshotRef>;

export const SafetySummary = Schema.Struct({
  skippedByBuiltInDenylist: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
  skippedByUserExclude: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
  skippedUnsupportedType: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
});
export type SafetySummary = Schema.Schema.Type<typeof SafetySummary>;

export const SaveManifest = Schema.Struct({
  archiveDigest: Sha256Digest,
  archiveKey: ArchiveKey,
  chunkCount: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
  fileCount: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
  id: SnapshotId,
  key: ManifestKey,
  manifestDigest: Sha256Digest,
  safety: SafetySummary,
  totalBytes: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
});
export type SaveManifest = Schema.Schema.Type<typeof SaveManifest>;
