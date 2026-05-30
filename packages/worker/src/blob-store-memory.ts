import type { SnapshotObjectKey } from "@stateful-ci/core";
import { Effect } from "effect";

import {
  BlobStore,
  validateExistingObjectBytes,
  validatePutIfAbsentInput,
} from "./blob-store";
import { BlobStoreError } from "./blob-store-error";

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
        bytes === undefined ? null : { size: bytes.byteLength }
      );
    },
    putIfAbsent: (input) =>
      Effect.gen(function* putIfAbsentEffect() {
        yield* validatePutIfAbsentInput(input);

        const existing = objects.get(input.key);

        if (existing !== undefined) {
          return yield* validateExistingObjectBytes(input, existing);
        }

        objects.set(input.key, new Uint8Array(input.body));
      }),
  });
};
