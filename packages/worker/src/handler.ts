import type { ApiErrorType } from "@stateful-ci/core";
import {
  HealthResponse,
  InvalidProtocolPayload,
  MethodNotAllowed,
  RouteNotFound,
  routes,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import { BlobStore } from "./blob-store";
import { BlobStoreError } from "./blob-store-error";
import { createR2BlobStore } from "./blob-store-r2";
import {
  handleCommitSave,
  handlePrepareSave,
  handleRestore,
} from "./control-plane-routes";
import { createDurableObjectSnapshotCoordinator } from "./durable-object";
import {
  MetadataBackend,
  createD1MetadataBackend,
  createInMemoryMetadataBackend,
} from "./metadata";
import { MetadataBackendError } from "./metadata-backend-error";
import { parseObjectRouteKey } from "./object-data-plane";
import { handleObjectRoute } from "./object-transfer";
import {
  SnapshotCoordinator,
  createMetadataSnapshotCoordinator,
} from "./snapshot-coordinator";
import type { WorkerEnv } from "./worker-env";

export interface HandleFetchOptions {
  readonly blobStore?: BlobStore["Service"];
  readonly coordinator?: SnapshotCoordinator["Service"];
  readonly metadata?: MetadataBackend["Service"];
}

const defaultMetadata = createInMemoryMetadataBackend();

const unconfiguredBlobStoreError = (key?: string) =>
  new BlobStoreError({
    ...(key === undefined ? {} : { key }),
    message:
      "The Worker does not have STATEFUL_CI_OBJECTS configured, so snapshot object storage is disabled. Bind the R2 bucket before using the object data plane.",
    reason: "io_failed",
  });

const unconfiguredBlobStore = BlobStore.of({
  get: (key) => Effect.fail(unconfiguredBlobStoreError(key)),
  head: (key) => Effect.fail(unconfiguredBlobStoreError(key)),
  putIfAbsent: (input) => Effect.fail(unconfiguredBlobStoreError(input.key)),
});

const unconfiguredCoordinatorError = (operation: string) =>
  new MetadataBackendError({
    message:
      "The Worker has D1 metadata configured without a Durable Object coordinator binding, so production snapshot head decisions are disabled. Bind STATEFUL_CI_COORDINATORS before using restore/save flows.",
    operation,
  });

const unconfiguredCoordinator = SnapshotCoordinator.of({
  authorizeRestore: () =>
    Effect.fail(unconfiguredCoordinatorError("authorizeRestore")),
  commitSave: () => Effect.fail(unconfiguredCoordinatorError("commitSave")),
  prepareSave: () => Effect.fail(unconfiguredCoordinatorError("prepareSave")),
  recordRestoreAllowed: () =>
    Effect.fail(unconfiguredCoordinatorError("recordRestoreAllowed")),
  recordRestoreObjectDenial: () =>
    Effect.fail(unconfiguredCoordinatorError("recordRestoreObjectDenial")),
});

const coordinatorForEnv = (env: WorkerEnv | undefined) => {
  if (env?.STATEFUL_CI_COORDINATORS !== undefined) {
    return createDurableObjectSnapshotCoordinator(env.STATEFUL_CI_COORDINATORS);
  }

  return env?.STATEFUL_CI_METADATA === undefined
    ? createMetadataSnapshotCoordinator()
    : unconfiguredCoordinator;
};

const invalidObjectRouteResponse = (path: string) =>
  Response.json(
    new InvalidProtocolPayload({
      message: `Snapshot object route ${path} does not contain a canonical production object key. Expected manifests/sha256/<hex>.json, packs/sha256/<hex>.scipack, or chunks/sha256/<hex>.`,
    }),
    { status: 400 }
  );

const methodNotAllowed = (path: string, method: string, allowed: string) =>
  Response.json(
    new MethodNotAllowed({
      allowed: [allowed],
      message: `The ${path} route only accepts ${allowed} requests.`,
      method,
      path,
    }),
    { headers: { Allow: allowed }, status: 405 }
  );

const fixedRoutes: readonly {
  readonly handle: (
    request: Request,
    env: WorkerEnv | undefined
  ) => Effect.Effect<
    Response,
    ApiErrorType | BlobStoreError | MetadataBackendError,
    BlobStore | MetadataBackend | SnapshotCoordinator
  >;
  readonly method: string;
  readonly path: string;
}[] = [
  {
    handle: () =>
      Effect.succeed(
        Response.json(
          Schema.encodeUnknownSync(HealthResponse)({
            protocolVersion: 1,
            service: "stateful-ci-worker",
            status: "ok",
          })
        )
      ),
    method: routes.health.method,
    path: routes.health.path,
  },
  {
    handle: handleRestore,
    method: routes.restore.method,
    path: routes.restore.path,
  },
  {
    handle: handlePrepareSave,
    method: routes.prepareSave.method,
    path: routes.prepareSave.path,
  },
  {
    handle: handleCommitSave,
    method: routes.commitSave.method,
    path: routes.commitSave.path,
  },
];

const handleRequest = (
  request: Request,
  env?: WorkerEnv
): Effect.Effect<
  Response,
  ApiErrorType | BlobStoreError | MetadataBackendError,
  BlobStore | MetadataBackend | SnapshotCoordinator
> => {
  const path = new URL(request.url).pathname;

  if (path.startsWith(routes.objects.pathPrefix)) {
    const key = parseObjectRouteKey(path);

    return key === null
      ? Effect.succeed(invalidObjectRouteResponse(path))
      : handleObjectRoute(request, env, key, path);
  }

  for (const route of fixedRoutes) {
    if (path !== route.path) {
      continue;
    }

    return request.method === route.method
      ? route.handle(request, env)
      : Effect.succeed(methodNotAllowed(path, request.method, route.method));
  }

  return Effect.succeed(
    Response.json(
      new RouteNotFound({
        message: "No Stateful CI route matches this request.",
        method: request.method,
        path,
      }),
      { status: 404 }
    )
  );
};

const apiErrorStatuses = {
  Forbidden: 403,
  InvalidJsonBody: 400,
  InvalidProtocolPayload: 400,
  MethodNotAllowed: 405,
  RequestBodyTooLarge: 413,
  RouteNotFound: 404,
  Unauthorized: 401,
} satisfies Record<ApiErrorType["_tag"], number>;

const apiErrorStatus = (error: ApiErrorType) => apiErrorStatuses[error._tag];

const apiErrorResponse = (error: ApiErrorType) =>
  error._tag === "MethodNotAllowed"
    ? Response.json(error, {
        headers: { Allow: error.allowed.join(", ") },
        status: apiErrorStatus(error),
      })
    : Response.json(error, { status: apiErrorStatus(error) });

const objectDataPlaneResponse = (error: BlobStoreError) => {
  let status = 400;

  if (error.reason === "missing") {
    status = 404;
  } else if (error.reason === "conflict") {
    status = 409;
  } else if (error.reason === "io_failed") {
    status = 500;
  }

  return Response.json(error, { status });
};

const errorResponse = (
  error: ApiErrorType | BlobStoreError | MetadataBackendError
) => {
  if (error._tag === "BlobStoreError") {
    return objectDataPlaneResponse(error);
  }

  if (error._tag === "MetadataBackendError") {
    return Response.json(error, { status: 500 });
  }

  return apiErrorResponse(error);
};

export const handleFetch = (
  request: Request,
  env?: WorkerEnv,
  options: HandleFetchOptions = {}
): Promise<Response> =>
  Effect.runPromise(
    handleRequest(request, env).pipe(
      Effect.provideService(
        BlobStore,
        options.blobStore ??
          (env?.STATEFUL_CI_OBJECTS === undefined
            ? unconfiguredBlobStore
            : createR2BlobStore(env.STATEFUL_CI_OBJECTS))
      ),
      Effect.provideService(
        MetadataBackend,
        options.metadata ??
          (env?.STATEFUL_CI_METADATA === undefined
            ? defaultMetadata
            : createD1MetadataBackend(env.STATEFUL_CI_METADATA))
      ),
      Effect.provideService(
        SnapshotCoordinator,
        options.coordinator ?? coordinatorForEnv(env)
      ),
      Effect.match({
        onFailure: errorResponse,
        onSuccess: (response) => response,
      })
    ),
    { signal: request.signal }
  );
