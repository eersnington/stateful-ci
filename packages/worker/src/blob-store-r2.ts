import type { SnapshotObjectKey } from "@stateful-ci/core";
import { Effect } from "effect";

import {
  BlobStore,
  validateExistingObjectBytes,
  validatePutIfAbsentInput,
} from "./blob-store";
import { BlobStoreError } from "./blob-store-error";

export interface R2BlobStoreBucket {
  readonly get: (
    key: string,
    options?: R2GetOptions
  ) => Promise<R2ObjectBody | null>;
  readonly head: (key: string) => Promise<R2Object | null>;
  readonly put: (
    key: string,
    value:
      | ReadableStream
      | ArrayBuffer
      | ArrayBufferView
      | string
      | null
      | Blob,
    options?: R2PutOptions
  ) => Promise<R2Object | null>;
}

const missingObject = (key: SnapshotObjectKey) =>
  new BlobStoreError({
    key,
    message: `Snapshot object ${key} is missing from R2.`,
    reason: "missing",
  });

export const createR2BlobStore = (
  bucket: R2BlobStoreBucket
): BlobStore["Service"] =>
  BlobStore.of({
    get: (key) =>
      Effect.gen(function* getEffect() {
        const object = yield* Effect.tryPromise({
          catch: () =>
            new BlobStoreError({
              key,
              message: `Could not read snapshot object ${key} from R2. Check the bucket binding and retry.`,
              reason: "io_failed",
            }),
          try: () => bucket.get(key),
        });

        if (object === null) {
          return yield* missingObject(key);
        }

        return yield* Effect.tryPromise({
          catch: () =>
            new BlobStoreError({
              key,
              message: `Could not read snapshot object ${key} bytes from R2. Check the bucket binding and retry.`,
              reason: "io_failed",
            }),
          try: async () => new Uint8Array(await object.arrayBuffer()),
        });
      }),
    head: (key) =>
      Effect.tryPromise({
        catch: () =>
          new BlobStoreError({
            key,
            message: `Could not inspect snapshot object ${key} in R2. Check the bucket binding and retry.`,
            reason: "io_failed",
          }),
        try: async () => {
          const object = await bucket.head(key);

          return object === null ? null : { size: object.size };
        },
      }),
    putIfAbsent: (input) =>
      Effect.gen(function* putIfAbsentEffect() {
        yield* validatePutIfAbsentInput(input);

        const inserted = yield* Effect.tryPromise({
          catch: () =>
            new BlobStoreError({
              key: input.key,
              message: `Could not conditionally store snapshot object ${input.key} in R2. Check the bucket binding and retry.`,
              reason: "io_failed",
            }),
          try: () =>
            bucket.put(input.key, input.body, {
              onlyIf: new Headers({ "If-None-Match": "*" }),
            }),
        });

        if (inserted !== null) {
          return;
        }

        const existing = yield* Effect.tryPromise({
          catch: () =>
            new BlobStoreError({
              key: input.key,
              message: `Could not inspect existing snapshot object ${input.key} in R2 after conditional upload was not inserted. Check the bucket binding and retry.`,
              reason: "io_failed",
            }),
          try: async () => {
            const object = await bucket.get(input.key);

            return object === null
              ? null
              : new Uint8Array(await object.arrayBuffer());
          },
        });

        if (existing === null) {
          return yield* new BlobStoreError({
            key: input.key,
            message: `Snapshot object ${input.key} was not inserted by R2, but no existing object could be read. Retry after checking the bucket binding and object key.`,
            reason: "io_failed",
          });
        }

        yield* validateExistingObjectBytes(input, existing);
      }),
  });
