import type {
  ObjectTransferMethod,
  ObjectTransferPlanEntry,
  SnapshotObjectInventoryEntry,
  SnapshotObjectKey,
} from "@stateful-ci/core";
import {
  routes,
  SnapshotObjectKey as SnapshotObjectKeySchema,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import { BlobStore } from "./blob-store";

export const objectRouteForKey = (key: SnapshotObjectKey) =>
  `${routes.objects.pathPrefix}${key}`;

export const parseObjectRouteKey = (path: string) => {
  if (!path.startsWith(routes.objects.pathPrefix)) {
    return null;
  }

  const rawKey = path.slice(routes.objects.pathPrefix.length);
  const decoded = Schema.decodeUnknownExit(SnapshotObjectKeySchema)(rawKey);

  return Exit.isFailure(decoded) ? null : decoded.value;
};

const transferPlanEntry = (
  method: ObjectTransferMethod,
  object: SnapshotObjectInventoryEntry
): ObjectTransferPlanEntry => ({
  headers:
    method === "PUT"
      ? {
          "x-stateful-ci-object-digest": object.digest,
          "x-stateful-ci-object-kind": object.kind,
          "x-stateful-ci-object-size": String(object.size),
        }
      : undefined,
  method,
  object,
  route: objectRouteForKey(object.key),
  transport: "worker-route",
});

export const uploadPlanEntry = (object: SnapshotObjectInventoryEntry) =>
  transferPlanEntry("PUT", object);

export const downloadPlanEntry = (object: SnapshotObjectInventoryEntry) =>
  transferPlanEntry("GET", object);

export const objectKindForKey = (key: SnapshotObjectKey) => {
  if (key.startsWith("manifests/")) {
    return "manifest" as const;
  }

  if (key.startsWith("packs/")) {
    return "pack" as const;
  }

  return "chunk" as const;
};

export const objectMatchesCanonicalKey = (
  object: SnapshotObjectInventoryEntry
) => object.kind === objectKindForKey(object.key);

export const missingObjectPlans = Effect.fn("missingObjectPlans")(
  function* missingObjectPlansEffect(
    objects: readonly SnapshotObjectInventoryEntry[]
  ) {
    const blobStore = yield* BlobStore;
    const missing: ObjectTransferPlanEntry[] = [];

    for (const object of objects) {
      if (!objectMatchesCanonicalKey(object)) {
        return yield* Effect.fail("snapshot_object_mismatch" as const);
      }

      const head = yield* blobStore.head(object.key);

      if (head === null) {
        missing.push(uploadPlanEntry(object));
        continue;
      }

      if (head.size !== object.size) {
        return yield* Effect.fail("snapshot_object_mismatch" as const);
      }
    }

    return missing;
  }
);

export const validateObjectsPresent = Effect.fn("validateObjectsPresent")(
  function* validateObjectsPresentEffect(
    objects: readonly SnapshotObjectInventoryEntry[]
  ) {
    const blobStore = yield* BlobStore;

    for (const object of objects) {
      if (!objectMatchesCanonicalKey(object)) {
        return yield* Effect.fail("snapshot_object_mismatch" as const);
      }

      const head = yield* blobStore.head(object.key);

      if (head === null) {
        return yield* Effect.fail("snapshot_object_missing" as const);
      }

      if (head.size !== object.size) {
        return yield* Effect.fail("snapshot_object_mismatch" as const);
      }
    }
  }
);

export const downloadPlanForObjects = (
  objects: readonly SnapshotObjectInventoryEntry[]
) => objects.map(downloadPlanEntry);
