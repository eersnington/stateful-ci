import type { SnapshotObjectKey } from "@stateful-ci/core";
import { Effect } from "effect";

import { BlobStore, validateExistingObjectBytes } from "./blob-store";
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
        : Effect.succeed({
            body: new ReadableStream<Uint8Array>({
              start: (controller) => {
                controller.enqueue(new Uint8Array(bytes));
                controller.close();
              },
            }),
            size: bytes.byteLength,
          });
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
        const uploadBody = yield* Effect.scoped(
          Effect.gen(function* readObjectBodyEffect() {
            const bodyReader = yield* Effect.acquireRelease(
              Effect.sync(() => input.body.getReader()),
              (activeReader) =>
                Effect.promise(() => activeReader.cancel()).pipe(Effect.orDie)
            );
            const chunks: Uint8Array[] = [];
            let byteLength = 0;

            for (;;) {
              const chunk = yield* Effect.tryPromise({
                catch: () =>
                  new BlobStoreError({
                    key: input.key,
                    message: `Could not read snapshot object ${input.key} stream. The object was not stored.`,
                    reason: "io_failed",
                  }),
                try: () => bodyReader.read(),
              });

              if (chunk.done) {
                break;
              }

              byteLength += chunk.value.byteLength;
              chunks.push(chunk.value);
            }

            const bytes = new Uint8Array(byteLength);
            let offset = 0;

            for (const chunk of chunks) {
              bytes.set(chunk, offset);
              offset += chunk.byteLength;
            }

            return bytes;
          })
        );

        if (uploadBody.byteLength !== input.expectedSize) {
          return yield* new BlobStoreError({
            key: input.key,
            message: `Snapshot object ${input.key} upload size was ${uploadBody.byteLength}, but the backend expected ${input.expectedSize}.`,
            reason: "size_mismatch",
          });
        }

        const digestBytes = yield* Effect.promise(() =>
          crypto.subtle.digest("SHA-256", uploadBody)
        );
        const digest = `sha256:${[...new Uint8Array(digestBytes)]
          .map((byte) => byte.toString(16).padStart(2, "0"))
          .join("")}`;

        if (digest !== input.expectedDigest) {
          return yield* new BlobStoreError({
            key: input.key,
            message: `Snapshot object ${input.key} upload digest did not match the expected digest. The object was not stored.`,
            reason: "digest_mismatch",
          });
        }

        const existing = objects.get(input.key);

        if (existing !== undefined) {
          return yield* validateExistingObjectBytes(
            { body: uploadBody, key: input.key },
            existing
          );
        }

        objects.set(input.key, new Uint8Array(uploadBody));
      }),
  });
};
