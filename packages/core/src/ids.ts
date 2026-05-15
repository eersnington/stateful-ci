import { Schema } from "effect";

const SHA256_DIGEST_PATTERN = /^sha256:[a-f0-9]{64}$/u;

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

export const ManifestKey = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@stateful-ci/ManifestKey")
);
export type ManifestKey = Schema.Schema.Type<typeof ManifestKey>;

export const Sha256Digest = Schema.String.check(
  Schema.isPattern(SHA256_DIGEST_PATTERN)
).pipe(Schema.brand("@stateful-ci/Sha256Digest"));
export type Sha256Digest = Schema.Schema.Type<typeof Sha256Digest>;
