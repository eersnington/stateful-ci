import type { Sha256Digest, SnapshotObjectKey } from "@stateful-ci/core";
import { Context, Effect } from "effect";

import { BlobStoreError } from "./blob-store-error";

export interface BlobObjectHead {
  readonly size: number;
}

export interface PutIfAbsentInput {
  readonly body: ReadableStream<Uint8Array>;
  readonly expectedDigest: Sha256Digest;
  readonly expectedSize: number;
  readonly key: SnapshotObjectKey;
}

export class BlobStore extends Context.Service<
  BlobStore,
  {
    readonly get: (
      key: SnapshotObjectKey
    ) => Effect.Effect<
      { readonly body: ReadableStream<Uint8Array>; readonly size: number },
      BlobStoreError
    >;
    readonly head: (
      key: SnapshotObjectKey
    ) => Effect.Effect<BlobObjectHead | null, BlobStoreError>;
    readonly putIfAbsent: (
      input: PutIfAbsentInput
    ) => Effect.Effect<void, BlobStoreError>;
  }
>()("stateful-ci/worker/BlobStore") {}

export const validateExistingObjectBytes = (
  input: {
    readonly body: Uint8Array;
    readonly key: SnapshotObjectKey;
  },
  existing: Uint8Array
) => {
  if (existing.byteLength !== input.body.byteLength) {
    return Effect.fail(
      new BlobStoreError({
        key: input.key,
        message: `Snapshot object ${input.key} already exists with different bytes. Immutable snapshot objects cannot be overwritten.`,
        reason: "conflict",
      })
    );
  }

  for (let index = 0; index < existing.byteLength; index += 1) {
    if (existing[index] !== input.body[index]) {
      return Effect.fail(
        new BlobStoreError({
          key: input.key,
          message: `Snapshot object ${input.key} already exists with different bytes. Immutable snapshot objects cannot be overwritten.`,
          reason: "conflict",
        })
      );
    }
  }

  return Effect.void;
};
