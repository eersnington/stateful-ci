import { Schema } from "effect";

const SHA256_DIGEST_PATTERN = /^sha256:[a-f0-9]{64}$/u;
const MANIFEST_KEY_PATTERN = /^manifests\/sha256-[a-f0-9]{64}\.json$/u;
const ARCHIVE_KEY_PATTERN = /^archives\/sha256-[a-f0-9]{64}\.sciar$/u;

export const WorkspaceId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@stateful-ci/WorkspaceId")
);
export type WorkspaceId = Schema.Schema.Type<typeof WorkspaceId>;

export const SnapshotId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@stateful-ci/SnapshotId")
);
export type SnapshotId = Schema.Schema.Type<typeof SnapshotId>;

export const RunId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@stateful-ci/RunId")
);
export type RunId = Schema.Schema.Type<typeof RunId>;

export const ManifestKey = Schema.String.check(
  Schema.isPattern(MANIFEST_KEY_PATTERN)
).pipe(Schema.brand("@stateful-ci/ManifestKey"));
export type ManifestKey = Schema.Schema.Type<typeof ManifestKey>;

export const ArchiveKey = Schema.String.check(
  Schema.isPattern(ARCHIVE_KEY_PATTERN)
).pipe(Schema.brand("@stateful-ci/ArchiveKey"));
export type ArchiveKey = Schema.Schema.Type<typeof ArchiveKey>;

export const Sha256Digest = Schema.String.check(
  Schema.isPattern(SHA256_DIGEST_PATTERN)
).pipe(Schema.brand("@stateful-ci/Sha256Digest"));
export type Sha256Digest = Schema.Schema.Type<typeof Sha256Digest>;
