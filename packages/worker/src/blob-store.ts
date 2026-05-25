import type { Sha256Digest, SnapshotObjectKey } from "@stateful-ci/core";
import { Context, Effect } from "effect";

import { BlobStoreError } from "./blob-store-error";

export interface BlobObjectHead {
  readonly key: SnapshotObjectKey;
  readonly size: number;
}

export interface PutIfAbsentInput {
  readonly body: Uint8Array;
  readonly expectedDigest: Sha256Digest;
  readonly expectedSize: number;
  readonly key: SnapshotObjectKey;
}

export interface PutIfAbsentResult {
  readonly status: "already-present" | "inserted" | "stored";
}

export class BlobStore extends Context.Service<
  BlobStore,
  {
    readonly get: (
      key: SnapshotObjectKey
    ) => Effect.Effect<Uint8Array, BlobStoreError>;
    readonly getRange: (
      key: SnapshotObjectKey,
      offset: number,
      length: number
    ) => Effect.Effect<Uint8Array, BlobStoreError>;
    readonly head: (
      key: SnapshotObjectKey
    ) => Effect.Effect<BlobObjectHead | null, BlobStoreError>;
    readonly presignGet: (
      key: SnapshotObjectKey,
      ttlSeconds: number
    ) => Effect.Effect<null, BlobStoreError>;
    readonly presignPut: (
      key: SnapshotObjectKey,
      ttlSeconds: number,
      constraints: {
        readonly expectedDigest: Sha256Digest;
        readonly expectedSize: number;
      }
    ) => Effect.Effect<null, BlobStoreError>;
    readonly putIfAbsent: (
      input: PutIfAbsentInput
    ) => Effect.Effect<PutIfAbsentResult, BlobStoreError>;
  }
>()("stateful-ci/worker/BlobStore") {}

export const unsupportedPresign = (key: SnapshotObjectKey) =>
  Effect.fail(
    new BlobStoreError({
      key,
      message:
        "Signed object URLs are not configured for this backend. Use authenticated Worker object routes instead.",
      reason: "unsupported",
    })
  );
