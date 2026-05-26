import type { SnapshotObjectKey } from "@stateful-ci/core";
import { Effect } from "effect";

import { BlobStore, unsupportedPresign } from "./blob-store";
import { BlobStoreError } from "./blob-store-error";
import { sha256BytesEffect } from "./object-hash";

const sameBytes = (left: Uint8Array, right: Uint8Array) => {
  if (left.byteLength !== right.byteLength) {
    return false;
  }

  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }

  return true;
};

export const createInMemoryBlobStore = (
  seed: ReadonlyMap<SnapshotObjectKey, Uint8Array> = new Map(),
  options: { readonly failHead?: boolean } = {}
): BlobStore["Service"] => {
  const objects = new Map(
    [...seed].map(([key, bytes]) => [key, new Uint8Array(bytes)] as const)
  );

  return BlobStore.of({
    get: (key) => {
      const bytes = objects.get(key);

      return bytes === undefined
        ? Effect.fail(
            new BlobStoreError({
              key,
              message: `Snapshot object ${key} is missing from the object store.`,
              reason: "missing",
            })
          )
        : Effect.succeed(new Uint8Array(bytes));
    },
    getRange: (key, offset, length) => {
      const bytes = objects.get(key);

      return bytes === undefined
        ? Effect.fail(
            new BlobStoreError({
              key,
              message: `Snapshot object ${key} is missing from the object store.`,
              reason: "missing",
            })
          )
        : Effect.succeed(new Uint8Array(bytes).slice(offset, offset + length));
    },
    head: (key) => {
      if (options.failHead === true) {
        return Effect.fail(
          new BlobStoreError({
            key,
            message: `Could not inspect snapshot object ${key} in in-memory storage. The configured test backend failed the HEAD operation.`,
            reason: "io_failed",
          })
        );
      }

      const bytes = objects.get(key);

      return Effect.succeed(
        bytes === undefined ? null : { key, size: bytes.byteLength }
      );
    },
    presignGet: (key) => unsupportedPresign(key),
    presignPut: (key) => unsupportedPresign(key),
    putIfAbsent: (input) =>
      Effect.gen(function* putIfAbsentEffect() {
        if (input.body.byteLength !== input.expectedSize) {
          return yield* new BlobStoreError({
            key: input.key,
            message: `Snapshot object ${input.key} upload size was ${input.body.byteLength}, but the backend expected ${input.expectedSize}.`,
            reason: "size_mismatch",
          });
        }

        const digest = yield* sha256BytesEffect(input.body);

        if (digest !== input.expectedDigest) {
          return yield* new BlobStoreError({
            key: input.key,
            message: `Snapshot object ${input.key} upload digest did not match the expected digest. The object was not stored.`,
            reason: "digest_mismatch",
          });
        }

        const existing = objects.get(input.key);

        if (existing !== undefined) {
          if (!sameBytes(existing, input.body)) {
            return yield* new BlobStoreError({
              key: input.key,
              message: `Snapshot object ${input.key} already exists with different bytes. Immutable snapshot objects cannot be overwritten.`,
              reason: "conflict",
            });
          }

          return { status: "already-present" as const };
        }

        objects.set(input.key, new Uint8Array(input.body));
        return { status: "inserted" as const };
      }),
  });
};
