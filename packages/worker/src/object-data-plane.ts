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

const uploadPlanEntry = (object: SnapshotObjectInventoryEntry) =>
  transferPlanEntry("PUT", object);

const downloadPlanEntry = (object: SnapshotObjectInventoryEntry) =>
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

const objectMatchesCanonicalKey = (object: SnapshotObjectInventoryEntry) =>
  object.kind === objectKindForKey(object.key);

const objectAvailability = Effect.fn("objectAvailability")(
  function* objectAvailabilityEffect(
    objects: readonly SnapshotObjectInventoryEntry[],
    missing: "collect" | "fail"
  ) {
    const blobStore = yield* BlobStore;
    const missingObjects: ObjectTransferPlanEntry[] = [];

    for (const object of objects) {
      if (!objectMatchesCanonicalKey(object)) {
        return yield* Effect.fail("snapshot_object_mismatch" as const);
      }

      const head = yield* blobStore.head(object.key);

      if (head === null) {
        if (missing === "fail") {
          return yield* Effect.fail("snapshot_object_missing" as const);
        }

        missingObjects.push(uploadPlanEntry(object));
        continue;
      }

      if (head.size !== object.size) {
        return yield* Effect.fail("snapshot_object_mismatch" as const);
      }
    }

    return missingObjects;
  }
);

export const missingObjectPlans = Effect.fn("missingObjectPlans")(
  function* missingObjectPlansEffect(
    objects: readonly SnapshotObjectInventoryEntry[]
  ) {
    return yield* objectAvailability(objects, "collect");
  }
);

export const validateObjectsPresent = Effect.fn("validateObjectsPresent")(
  function* validateObjectsPresentEffect(
    objects: readonly SnapshotObjectInventoryEntry[]
  ) {
    yield* objectAvailability(objects, "fail");
  }
);

export const downloadPlanForObjects = (
  objects: readonly SnapshotObjectInventoryEntry[]
) => objects.map(downloadPlanEntry);
