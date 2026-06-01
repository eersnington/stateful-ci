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

export class SnapshotObjectAvailabilityError extends Schema.TaggedErrorClass<SnapshotObjectAvailabilityError>()(
  "SnapshotObjectAvailabilityError",
  {
    reason: Schema.Literals([
      "snapshot_object_missing",
      "snapshot_object_mismatch",
    ]),
  }
) {}

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
  route: `${routes.objects.pathPrefix}${object.key}`,
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
        return yield* new SnapshotObjectAvailabilityError({
          reason: "snapshot_object_mismatch",
        });
      }

      const head = yield* blobStore.head(object.key);

      if (head === null) {
        if (missing === "fail") {
          return yield* new SnapshotObjectAvailabilityError({
            reason: "snapshot_object_missing",
          });
        }

        missingObjects.push(uploadPlanEntry(object));
        continue;
      }

      if (head.size !== object.size) {
        return yield* new SnapshotObjectAvailabilityError({
          reason: "snapshot_object_mismatch",
        });
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
