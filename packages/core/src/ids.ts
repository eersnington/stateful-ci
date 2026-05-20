import { Schema } from "effect";

const SHA256_DIGEST_PATTERN = /^sha256:[a-f0-9]{64}$/u;
const SHA256_HEX_PATTERN = /[a-f0-9]{64}/u;
const MANIFEST_KEY_PATTERN = /^manifests\/sha256\/([a-f0-9]{64})\.json$/u;
const PACK_KEY_PATTERN = /^packs\/sha256\/([a-f0-9]{64})\.scipack$/u;
const CHUNK_KEY_PATTERN = /^chunks\/sha256\/([a-f0-9]{64})$/u;

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

export const HeadGeneration = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0)
).pipe(Schema.brand("@stateful-ci/HeadGeneration"));
export type HeadGeneration = Schema.Schema.Type<typeof HeadGeneration>;

export const IdempotencyKey = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@stateful-ci/IdempotencyKey")
);
export type IdempotencyKey = Schema.Schema.Type<typeof IdempotencyKey>;

export const ManifestKey = Schema.String.check(
  Schema.isPattern(MANIFEST_KEY_PATTERN)
).pipe(Schema.brand("@stateful-ci/ManifestKey"));
export type ManifestKey = Schema.Schema.Type<typeof ManifestKey>;

export const PackKey = Schema.String.check(
  Schema.isPattern(PACK_KEY_PATTERN)
).pipe(Schema.brand("@stateful-ci/PackKey"));
export type PackKey = Schema.Schema.Type<typeof PackKey>;

export const ChunkKey = Schema.String.check(
  Schema.isPattern(CHUNK_KEY_PATTERN)
).pipe(Schema.brand("@stateful-ci/ChunkKey"));
export type ChunkKey = Schema.Schema.Type<typeof ChunkKey>;

export const SnapshotObjectKey = Schema.Union([ManifestKey, PackKey, ChunkKey]);
export type SnapshotObjectKey = Schema.Schema.Type<typeof SnapshotObjectKey>;

export const Sha256Digest = Schema.String.check(
  Schema.isPattern(SHA256_DIGEST_PATTERN)
).pipe(Schema.brand("@stateful-ci/Sha256Digest"));
export type Sha256Digest = Schema.Schema.Type<typeof Sha256Digest>;

export const sha256DigestFromHex = (hex: string) =>
  Schema.decodeSync(Sha256Digest)(`sha256:${hex}`);

export const sha256HexFromDigest = (digest: Sha256Digest) =>
  digest.slice("sha256:".length);

export const manifestKeyFromDigest = (digest: Sha256Digest) =>
  Schema.decodeSync(ManifestKey)(
    `manifests/sha256/${sha256HexFromDigest(digest)}.json`
  );

export const packKeyFromDigest = (digest: Sha256Digest) =>
  Schema.decodeSync(PackKey)(
    `packs/sha256/${sha256HexFromDigest(digest)}.scipack`
  );

export const chunkKeyFromDigest = (digest: Sha256Digest) =>
  Schema.decodeSync(ChunkKey)(`chunks/sha256/${sha256HexFromDigest(digest)}`);

export const sha256DigestFromObjectKey = (key: SnapshotObjectKey) => {
  const digest = SHA256_HEX_PATTERN.exec(key)?.[0];

  if (digest === undefined) {
    throw new Error(
      `Snapshot object key did not contain a canonical SHA-256 digest: ${key}`
    );
  }

  return sha256DigestFromHex(digest);
};
