import { assert, describe, it } from "@effect/vitest";
import { ManifestKey } from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import { createR2BlobStore } from "../src/blob-store-r2";
import type { R2BlobStoreBucket } from "../src/blob-store-r2";

const objectKey = Schema.decodeSync(ManifestKey)(
  "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json"
);

const missingBucket = {
  get: () => Promise.resolve(null),
  head: () => Promise.resolve(null),
  put: () => Promise.resolve(null),
} satisfies R2BlobStoreBucket;

describe("R2 blob store", () => {
  it.effect("preserves missing object errors for GET", () =>
    Effect.gen(function* missingGetEffect() {
      const blobStore = createR2BlobStore(missingBucket);
      const error = yield* Effect.flip(blobStore.get(objectKey));

      assert.strictEqual(error.reason, "missing");
      assert.strictEqual(error.key, objectKey);
    })
  );
});
