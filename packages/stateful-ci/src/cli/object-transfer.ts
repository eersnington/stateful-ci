import type { ObjectTransferPlanEntry } from "@stateful-ci/core";
import { Effect } from "effect";

import {
  readLocalSnapshotObject,
  storeVerifiedSnapshotObject,
} from "../snapshot-engine";
import { cliFailure } from "./failure";
import type { ApiConfig } from "./protocol-client";
import { relativeWorkerRouteUrl } from "./protocol-client";

export const downloadPlannedObject = Effect.fn("downloadPlannedObject")(
  function* downloadPlannedObjectEffect(
    api: ApiConfig,
    plan: ObjectTransferPlanEntry,
    workspaceRoot: string
  ) {
    if (plan.method !== "GET") {
      return yield* Effect.fail(
        cliFailure(
          `Restore download plan for ${plan.object.key} used ${plan.method}, but restore only supports GET object plans.`
        )
      );
    }

    const url =
      plan.transport === "worker-route"
        ? yield* relativeWorkerRouteUrl(api, plan.route, "restore")
        : plan.url;
    const headers = new Headers(plan.headers ?? {});

    if (plan.transport === "worker-route" && api.token !== null) {
      headers.set("authorization", `Bearer ${api.token}`);
    }

    const response = yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          `Could not download backend-authorized snapshot object ${plan.object.key}. Restore did not mutate the workspace.`
        ),
      try: (signal) => fetch(url, { headers, method: "GET", signal }),
    });

    if (!response.ok) {
      return yield* Effect.fail(
        cliFailure(
          `Backend object download for ${plan.object.key} returned HTTP ${response.status}. Restore did not mutate the workspace.`
        )
      );
    }

    const bytes = yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          `Could not read snapshot object ${plan.object.key} from the backend response. Restore did not mutate the workspace.`
        ),
      try: async () => new Uint8Array(await response.arrayBuffer()),
    });

    yield* storeVerifiedSnapshotObject({
      bytes,
      digest: plan.object.digest,
      key: plan.object.key,
      size: plan.object.size,
      workspaceRoot,
    }).pipe(
      Effect.mapError((error) =>
        cliFailure(`${error.message} Restore did not mutate the workspace.`)
      )
    );
  }
);

export const uploadPlannedObject = Effect.fn("uploadPlannedObject")(
  function* uploadPlannedObjectEffect(
    api: ApiConfig,
    plan: ObjectTransferPlanEntry,
    workspaceRoot: string
  ) {
    if (plan.method !== "PUT") {
      return yield* Effect.fail(
        cliFailure(
          `Save upload plan for ${plan.object.key} used ${plan.method}, but save only supports PUT object plans.`
        )
      );
    }

    const bytes = yield* readLocalSnapshotObject({
      key: plan.object.key,
      workspaceRoot,
    }).pipe(
      Effect.mapError((error) =>
        cliFailure(`${error.message} Save did not upload or commit objects.`)
      )
    );
    const url =
      plan.transport === "worker-route"
        ? yield* relativeWorkerRouteUrl(api, plan.route, "save")
        : plan.url;
    const headers = new Headers(plan.headers ?? {});

    if (plan.transport === "worker-route" && api.token !== null) {
      headers.set("authorization", `Bearer ${api.token}`);
    }

    const response = yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          `Could not upload backend-requested snapshot object ${plan.object.key}. Save did not commit metadata.`
        ),
      try: (signal) =>
        fetch(url, { body: bytes, headers, method: "PUT", signal }),
    });

    if (!response.ok) {
      return yield* Effect.fail(
        cliFailure(
          `Backend object upload for ${plan.object.key} returned HTTP ${response.status}. Save did not commit metadata.`
        )
      );
    }
  }
);
