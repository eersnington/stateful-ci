import type { Sha256Digest, SnapshotObjectKey } from "@stateful-ci/core";
import { Sha256Digest as Sha256DigestSchema } from "@stateful-ci/core";
import { Context, Effect, Schema } from "effect";

import { BlobStoreError } from "./blob-store-error";

export interface BlobObjectHead {
  readonly size: number;
}

export interface PutIfAbsentInput {
  readonly body: Uint8Array;
  readonly expectedDigest: Sha256Digest;
  readonly expectedSize: number;
  readonly key: SnapshotObjectKey;
}

export class BlobStore extends Context.Service<
  BlobStore,
  {
    readonly get: (
      key: SnapshotObjectKey
    ) => Effect.Effect<Uint8Array, BlobStoreError>;
    readonly head: (
      key: SnapshotObjectKey
    ) => Effect.Effect<BlobObjectHead | null, BlobStoreError>;
    readonly putIfAbsent: (
      input: PutIfAbsentInput
    ) => Effect.Effect<void, BlobStoreError>;
  }
>()("stateful-ci/worker/BlobStore") {}

const digestForSnapshotObject = (bytes: Uint8Array) =>
  Effect.promise(() => crypto.subtle.digest("SHA-256", bytes)).pipe(
    Effect.map((digest) =>
      Schema.decodeSync(Sha256DigestSchema)(
        `sha256:${[...new Uint8Array(digest)]
          .map((byte) => byte.toString(16).padStart(2, "0"))
          .join("")}`
      )
    )
  );

export const validatePutIfAbsentInput = Effect.fn("validatePutIfAbsentInput")(
  function* validatePutIfAbsentInputEffect(input: PutIfAbsentInput) {
    if (input.body.byteLength !== input.expectedSize) {
      return yield* new BlobStoreError({
        key: input.key,
        message: `Snapshot object ${input.key} upload size was ${input.body.byteLength}, but the backend expected ${input.expectedSize}.`,
        reason: "size_mismatch",
      });
    }

    const digest = yield* digestForSnapshotObject(input.body);

    if (digest !== input.expectedDigest) {
      return yield* new BlobStoreError({
        key: input.key,
        message: `Snapshot object ${input.key} upload digest did not match the expected digest. The object was not stored.`,
        reason: "digest_mismatch",
      });
    }
  }
);

export const validateExistingObjectBytes = (
  input: PutIfAbsentInput,
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
