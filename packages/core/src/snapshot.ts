import { Schema } from "effect";

import { ManifestKey, Sha256Digest, SnapshotId } from "./ids";

export const SnapshotRef = Schema.Struct({
  id: SnapshotId,
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
  chunkCount: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
  fileCount: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
  hash: Sha256Digest,
  id: SnapshotId,
  key: ManifestKey,
  safety: SafetySummary,
  totalBytes: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0)
  ),
});
export type SaveManifest = Schema.Schema.Type<typeof SaveManifest>;
