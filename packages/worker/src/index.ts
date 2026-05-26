import type { ApiErrorType, DenialReason } from "@stateful-ci/core";
import {
  CommitSaveDeniedResponse,
  CommitSaveRequest,
  CommitSaveResponse,
  Forbidden,
  HealthResponse,
  InvalidJsonBody,
  InvalidProtocolPayload,
  MethodNotAllowed,
  PrepareSaveAllowedResponse,
  PrepareSaveDeniedResponse,
  PrepareSaveRequest,
  RequestBodyTooLarge,
  RestoreAllowedResponse,
  RestoreDeniedResponse,
  RestoreRequest,
  RouteNotFound,
  RunId,
  routes,
  SaveDeniedResponse,
  SnapshotObjectInventoryEntry,
  Unauthorized,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import { BlobStore } from "./blob-store";
import { BlobStoreError } from "./blob-store-error";
import { createR2BlobStore } from "./blob-store-r2";
import { createDurableObjectSnapshotCoordinator } from "./durable-object";
import {
  createD1MetadataBackend,
  createInMemoryMetadataBackend,
  currentIsoTimestamp,
  MetadataBackend,
} from "./metadata";
import type { RefTarget } from "./metadata";
import { MetadataBackendError } from "./metadata-backend-error";
import {
  downloadPlanForObjects,
  missingObjectPlans,
  objectKindForKey,
  parseObjectRouteKey,
  validateObjectsPresent,
} from "./object-data-plane";
import { classifyRunTrust, defaultTrustedRefs } from "./run-classification";
import {
  createMetadataSnapshotCoordinator,
  SnapshotCoordinator,
} from "./snapshot-coordinator";

export { WorkspaceSnapshotCoordinatorDurableObject } from "./durable-object";

/**
 * Maximum accepted size for JSON protocol request bodies.
 *
 * Restore/save requests are metadata-only control-plane messages. Snapshot bytes
 * must use the object-store data plane instead of these Worker routes, so a
 * small cap protects public endpoints from buffering oversized unauthenticated
 * bodies while leaving room for normal GitHub context and manifest metadata.
 */
export const maxProtocolBodyBytes = 64 * 1024;

interface WorkerEnv {
  readonly STATEFUL_CI_COORDINATORS?: DurableObjectNamespace;
  readonly STATEFUL_CI_METADATA?: D1Database;
  readonly STATEFUL_CI_OBJECTS?: R2Bucket;
  readonly STATEFUL_CI_API_TOKEN?: string;
  readonly STATEFUL_CI_TRUSTED_REFS?: string;
}

export interface HandleFetchOptions {
  readonly blobStore?: BlobStore["Service"];
  readonly coordinator?: SnapshotCoordinator["Service"];
  readonly metadata?: MetadataBackend["Service"];
}

const protocolBodyTooLarge = new RequestBodyTooLarge({
  limitBytes: maxProtocolBodyBytes,
  message:
    "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
});

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
  getRange: (key) => Effect.fail(unconfiguredBlobStoreError(key)),
  head: (key) => Effect.fail(unconfiguredBlobStoreError(key)),
  presignGet: (key) => Effect.fail(unconfiguredBlobStoreError(key)),
  presignPut: (key) => Effect.fail(unconfiguredBlobStoreError(key)),
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

const trustedRefsForEnv = (env: WorkerEnv | undefined) => {
  const configured = env?.STATEFUL_CI_TRUSTED_REFS?.split(",")
    .map((ref) => ref.trim())
    .filter((ref) => ref.length > 0);

  return configured === undefined || configured.length === 0
    ? defaultTrustedRefs
    : configured;
};

const isApiError = (error: unknown): error is ApiErrorType =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  (error._tag === "InvalidJsonBody" ||
    error._tag === "InvalidProtocolPayload" ||
    error._tag === "MethodNotAllowed" ||
    error._tag === "RequestBodyTooLarge" ||
    error._tag === "RouteNotFound" ||
    error._tag === "Unauthorized" ||
    error._tag === "Forbidden");

const isBlobStoreError = (error: unknown): error is BlobStoreError =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  error._tag === "BlobStoreError";

const isMetadataBackendError = (
  error: unknown
): error is MetadataBackendError =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  error._tag === "MetadataBackendError";

const producerContextFor = (
  request: Pick<PrepareSaveRequest, "git" | "github" | "workspace">
) => ({
  actor: request.github.actor,
  event: request.github.event,
  job: request.workspace.job,
  ref: request.git.ref,
  repository: request.workspace.repo,
  runId: Schema.decodeSync(RunId)(request.github.runId),
  sha: request.git.sha,
  workflow: request.workspace.workflow,
});

const authorizeRequest = (request: Request, env: WorkerEnv | undefined) => {
  const expectedToken = env?.STATEFUL_CI_API_TOKEN;
  const authorization = request.headers.get("authorization");

  if (authorization === null || !authorization.startsWith("Bearer ")) {
    return Effect.fail(
      new Unauthorized({
        message:
          "Restore and save requests require an Authorization bearer token. Set STATEFUL_CI_API_TOKEN in CI and send it as Authorization: Bearer <token>.",
      })
    );
  }

  if (expectedToken === undefined || expectedToken.length === 0) {
    return Effect.fail(
      new Forbidden({
        message:
          "The Worker does not have STATEFUL_CI_API_TOKEN configured, so restore/save requests are disabled. Configure the backend token before using Stateful CI.",
      })
    );
  }

  if (authorization.slice("Bearer ".length) !== expectedToken) {
    return Effect.fail(
      new Forbidden({
        message:
          "The Authorization bearer token did not match this Stateful CI backend. Check STATEFUL_CI_API_TOKEN and retry.",
      })
    );
  }

  return Effect.void;
};

const concatChunks = (chunks: readonly Uint8Array[]) => {
  const bytes = new Uint8Array(
    chunks.reduce((total, chunk) => total + chunk.byteLength, 0)
  );
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return bytes;
};

const invalidJsonBody = () =>
  new InvalidJsonBody({
    message:
      "The request body was not valid JSON. Send a JSON object that matches Stateful CI protocol v1.",
  });

const readProtocolBody = (request: Request) =>
  Effect.tryPromise({
    catch: (error) => (isApiError(error) ? error : invalidJsonBody()),
    try: async () => {
      const contentLength = request.headers.get("content-length");

      if (
        contentLength !== null &&
        Number(contentLength) > maxProtocolBodyBytes
      ) {
        throw protocolBodyTooLarge;
      }

      const chunks: Uint8Array[] = [];
      let byteLength = 0;

      for await (const chunk of request.body ?? []) {
        byteLength += chunk.byteLength;

        if (byteLength > maxProtocolBodyBytes) {
          throw protocolBodyTooLarge;
        }

        chunks.push(chunk);
      }

      return new TextDecoder().decode(concatChunks(chunks));
    },
  });

const parseProtocolJson = (source: string) =>
  Effect.gen(function* parseProtocolJsonEffect() {
    const decoded = Schema.decodeUnknownExit(Schema.UnknownFromJsonString)(
      source
    );

    if (Exit.isFailure(decoded)) {
      return yield* invalidJsonBody();
    }

    return decoded.value;
  });

const invalidProtocolPayload = () =>
  new InvalidProtocolPayload({
    message:
      "The request body was valid JSON but did not match Stateful CI protocol v1. Check the client version and request payload.",
  });

const decodeProtocolPayload =
  <A>(schema: Schema.Decoder<A>) =>
  (body: unknown) => {
    const decoded = Schema.decodeUnknownExit(schema)(body);

    return Exit.isFailure(decoded)
      ? Effect.fail(invalidProtocolPayload())
      : Effect.succeed(decoded.value);
  };

const refSegment = (ref: string) => {
  if (ref.startsWith("refs/heads/")) {
    return ref.slice("refs/heads/".length);
  }

  if (ref.startsWith("refs/tags/")) {
    return ref.slice("refs/tags/".length);
  }

  return ref.replaceAll("/", "-");
};

const metadataTargetForRestore = (
  request: Pick<RestoreRequest, "client" | "git" | "github" | "workspace">,
  trustedRefs: readonly string[]
): RefTarget => ({
  namespace: `repo=${request.workspace.repo}/workflow=${request.workspace.workflow}/job=${request.workspace.job}/config=${request.client.configHash}`,
  refName: `${classifyRunTrust(request, { trustedRefs })}/${refSegment(request.git.ref)}/latest`,
});

const trustedSeedTargetsFor = (
  target: RefTarget,
  trustedRefs: readonly string[]
): readonly RefTarget[] =>
  trustedRefs.map((trustedRef) => ({
    namespace: target.namespace,
    refName: `trusted/${refSegment(trustedRef)}/latest`,
  }));

const restoreCandidateTargets = (
  target: RefTarget,
  trustClass: ReturnType<typeof classifyRunTrust>,
  trustedRefs: readonly string[]
) =>
  trustClass === "trusted" || trustClass === "unknown"
    ? [target]
    : [target, ...trustedSeedTargetsFor(target, trustedRefs)];

const workspaceIdForTarget = (target: RefTarget) =>
  Schema.decodeSync(WorkspaceId)(`ws:${target.namespace}:${target.refName}`);

const runIdFromRestore = (request: RestoreRequest) =>
  Schema.decodeSync(RunId)(request.github.runId);

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

const invalidObjectRouteResponse = (path: string) =>
  Response.json(
    new InvalidProtocolPayload({
      message: `Snapshot object route ${path} does not contain a canonical production object key. Expected manifests/sha256/<hex>.json, packs/sha256/<hex>.scipack, or chunks/sha256/<hex>.`,
    }),
    { status: 400 }
  );

const invalidObjectUploadPlanResponse = (path: string) =>
  Response.json(
    new InvalidProtocolPayload({
      message: `Snapshot object upload ${path} is missing or has invalid prepare-plan headers. Send x-stateful-ci-object-digest, x-stateful-ci-object-kind, and x-stateful-ci-object-size from the upload plan.`,
    }),
    { status: 400 }
  );

const expectedObjectFromHeaders = (
  key: SnapshotObjectInventoryEntry["key"],
  headers: Headers
) => {
  const expectedDigest = headers.get("x-stateful-ci-object-digest");
  const expectedKind = headers.get("x-stateful-ci-object-kind");
  const expectedSize = headers.get("x-stateful-ci-object-size");

  if (
    expectedDigest === null ||
    expectedKind === null ||
    expectedSize === null
  ) {
    return null;
  }

  const decoded = Schema.decodeUnknownExit(SnapshotObjectInventoryEntry)({
    digest: expectedDigest,
    key,
    kind: expectedKind,
    size: Number(expectedSize),
  });

  return Exit.isFailure(decoded) ? null : decoded.value;
};

const readObjectBody = (request: Request) =>
  Effect.tryPromise({
    catch: () =>
      new BlobStoreError({
        message:
          "Could not read the snapshot object upload body. The object was not stored.",
        reason: "io_failed",
      }),
    try: async () => new Uint8Array(await request.arrayBuffer()),
  });

const handleObjectRoute = Effect.fn("handleObjectRoute")(
  function* handleObjectRouteEffect(
    request: Request,
    env: WorkerEnv | undefined,
    key: SnapshotObjectInventoryEntry["key"]
  ) {
    const blobStore = yield* BlobStore;
    yield* authorizeRequest(request, env);

    if (request.method === "HEAD") {
      const head = yield* blobStore.head(key);

      return head === null
        ? new Response(null, { status: 404 })
        : new Response(null, {
            headers: {
              "content-length": String(head.size),
              "x-stateful-ci-object-kind": objectKindForKey(key),
            },
            status: 200,
          });
    }

    if (request.method === "GET") {
      const bytes = yield* blobStore.get(key);

      return new Response(bytes, {
        headers: {
          "content-length": String(bytes.byteLength),
          "content-type": "application/octet-stream",
          "x-stateful-ci-object-kind": objectKindForKey(key),
        },
      });
    }

    if (request.method === "PUT") {
      const object = expectedObjectFromHeaders(key, request.headers);

      if (object === null) {
        return invalidObjectUploadPlanResponse(new URL(request.url).pathname);
      }

      const contentLength = request.headers.get("content-length");
      const declaredSize = Number(contentLength);

      if (
        contentLength === null ||
        !Number.isSafeInteger(declaredSize) ||
        declaredSize !== object.size
      ) {
        return yield* new BlobStoreError({
          key,
          message: `Snapshot object ${key} upload declared ${contentLength ?? "no"} Content-Length, but the backend expected ${object.size}. The object was not stored.`,
          reason: "size_mismatch",
        });
      }

      const body = yield* readObjectBody(request);

      yield* blobStore.putIfAbsent({
        body,
        expectedDigest: object.digest,
        expectedSize: object.size,
        key,
      });

      return new Response(null, { status: 204 });
    }

    const path = new URL(request.url).pathname;

    return Response.json(
      new MethodNotAllowed({
        allowed: ["HEAD", "GET", "PUT"],
        message: `The ${path} route only accepts HEAD, GET, or PUT requests.`,
        method: request.method,
        path,
      }),
      { headers: { Allow: "HEAD, GET, PUT" }, status: 405 }
    );
  }
);

const denyPrepareSave = (
  reason: DenialReason,
  trustClass: ReturnType<typeof classifyRunTrust>,
  workspaceId?: WorkspaceId
) =>
  Response.json(
    Schema.encodeUnknownSync(PrepareSaveDeniedResponse)({
      decision: "denied",
      reason,
      trustClass,
      ...(workspaceId === undefined ? {} : { workspaceId }),
    })
  );

const handleRestore = Effect.fn("handleRestore")(function* handleRestoreEffect(
  request: Request,
  env: WorkerEnv | undefined
) {
  const coordinator = yield* SnapshotCoordinator;
  yield* authorizeRequest(request, env);
  const restoreRequest = yield* readProtocolBody(request).pipe(
    Effect.flatMap(parseProtocolJson),
    Effect.flatMap(decodeProtocolPayload(RestoreRequest))
  );
  const trustedRefs = trustedRefsForEnv(env);
  const trustClass = classifyRunTrust(restoreRequest, { trustedRefs });
  const target = metadataTargetForRestore(restoreRequest, trustedRefs);
  const workspaceId = workspaceIdForTarget(target);
  const runId = runIdFromRestore(restoreRequest);
  const restore = yield* coordinator.authorizeRestore({
    candidates: restoreCandidateTargets(target, trustClass, trustedRefs),
    runId,
    target,
    trustClass,
    workspaceId,
  });

  if (restore.decision === "denied") {
    return Response.json(
      Schema.encodeUnknownSync(RestoreDeniedResponse)({
        decision: "denied",
        reason: restore.reason,
        save:
          restore.saveTarget === null
            ? { allowed: false }
            : { allowed: true, target: restore.saveTarget.refName },
        trustClass,
        ...(restore.saveTarget === null ? {} : { workspaceId }),
      })
    );
  }

  const objectValidation = yield* validateObjectsPresent(restore.objects).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        isBlobStoreError(error)
          ? Effect.fail(error)
          : Effect.succeed({ ok: false as const, reason: error }),
      onSuccess: () => Effect.succeed({ ok: true as const }),
    })
  );

  if (!objectValidation.ok) {
    yield* coordinator.recordRestoreObjectDenial({
      reason: objectValidation.reason,
      runId,
      snapshotId: restore.snapshot.snapshotId,
      target,
      trustClass,
      workspaceId,
    });

    return Response.json(
      Schema.encodeUnknownSync(RestoreDeniedResponse)({
        decision: "denied",
        reason: objectValidation.reason,
        save:
          restore.saveTarget === null
            ? { allowed: false }
            : { allowed: true, target: restore.saveTarget.refName },
        trustClass,
        ...(restore.saveTarget === null ? {} : { workspaceId }),
      })
    );
  }

  yield* coordinator.recordRestoreAllowed({
    runId,
    snapshotId: restore.snapshot.snapshotId,
    target,
    trustClass,
    workspaceId,
  });

  return Response.json(
    Schema.encodeUnknownSync(RestoreAllowedResponse)({
      decision: "allowed",
      downloadPlan: downloadPlanForObjects(restore.objects),
      manifest: restore.manifest,
      save:
        restore.saveTarget === null
          ? { allowed: false }
          : { allowed: true, target: restore.saveTarget.refName },
      snapshot: {
        id: restore.snapshot.snapshotId,
        manifestKey: restore.snapshot.manifestKey,
        parent: restore.snapshot.parentSnapshotId,
      },
      trustClass,
      workspaceId,
    })
  );
});

const handlePrepareSave = Effect.fn("handlePrepareSave")(
  function* handlePrepareSaveEffect(
    request: Request,
    env: WorkerEnv | undefined
  ) {
    const coordinator = yield* SnapshotCoordinator;
    yield* authorizeRequest(request, env);
    const prepareRequest = yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(PrepareSaveRequest))
    );
    const trustedRefs = trustedRefsForEnv(env);
    const trustClass = classifyRunTrust(prepareRequest, { trustedRefs });
    const target = metadataTargetForRestore(prepareRequest, trustedRefs);
    const workspaceId = workspaceIdForTarget(target);
    const runId = Schema.decodeSync(RunId)(prepareRequest.github.runId);
    const expiresAt = yield* currentIsoTimestamp;
    const saveAuthorization = yield* coordinator.prepareSave({
      expiresAt,
      producer: producerContextFor(prepareRequest),
      runId,
      target,
      trustClass,
      workspaceId,
    });

    if (saveAuthorization.decision === "denied") {
      return denyPrepareSave(saveAuthorization.reason, trustClass, workspaceId);
    }

    const missing = yield* missingObjectPlans(prepareRequest.objects).pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          isBlobStoreError(error)
            ? Effect.fail(error)
            : Effect.succeed({ ok: false as const, reason: error }),
        onSuccess: (plans) => Effect.succeed({ ok: true as const, plans }),
      })
    );

    if (!missing.ok) {
      return denyPrepareSave(missing.reason, trustClass, workspaceId);
    }

    return Response.json(
      Schema.encodeUnknownSync(PrepareSaveAllowedResponse)({
        baseSnapshotId: saveAuthorization.baseSnapshotId,
        commitTarget: target,
        decision: "allowed",
        expectedHeadGeneration: saveAuthorization.expectedHeadGeneration,
        missingObjects: missing.plans,
        trustClass,
        workspaceId,
      })
    );
  }
);

const handleCommitSave = Effect.fn("handleCommitSave")(
  function* handleCommitSaveEffect(
    request: Request,
    env: WorkerEnv | undefined
  ) {
    const coordinator = yield* SnapshotCoordinator;
    yield* authorizeRequest(request, env);
    const commitRequest = yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(CommitSaveRequest))
    );

    const objectValidation = yield* validateObjectsPresent(
      commitRequest.objects
    ).pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          isBlobStoreError(error)
            ? Effect.fail(error)
            : Effect.succeed({ ok: false as const, reason: error }),
        onSuccess: () => Effect.succeed({ ok: true as const }),
      })
    );

    if (!objectValidation.ok) {
      return Response.json(
        Schema.encodeUnknownSync(CommitSaveDeniedResponse)({
          decision: "denied",
          reason: objectValidation.reason,
        })
      );
    }

    const result = yield* coordinator.commitSave({
      baseSnapshotId: commitRequest.baseSnapshotId,
      expectedHeadGeneration: commitRequest.expectedHeadGeneration,
      idempotencyKey: commitRequest.idempotencyKey,
      manifest: commitRequest.manifest,
      objects: commitRequest.objects,
      producer: {
        actor: "unknown",
        event: "unknown",
        job: commitRequest.target.refName,
        ref: commitRequest.target.refName,
        repository: commitRequest.target.namespace,
        runId: commitRequest.runId,
        sha: "unknown",
        workflow: commitRequest.target.namespace,
      },
      target: commitRequest.target,
      workspaceId: commitRequest.workspaceId,
    });

    return Response.json(Schema.encodeUnknownSync(CommitSaveResponse)(result));
  }
);

const handleLegacySave = Effect.fn("handleLegacySave")(
  function* handleLegacySaveEffect(
    request: Request,
    env: WorkerEnv | undefined
  ) {
    yield* authorizeRequest(request, env);
    yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(Schema.Unknown))
    );

    return Response.json(
      Schema.encodeUnknownSync(SaveDeniedResponse)({
        decision: "denied",
        reason: "backend_policy_not_configured",
      }),
      { status: 410 }
    );
  }
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

const handleRequest = (
  request: Request,
  env?: WorkerEnv
): Effect.Effect<
  Response,
  ApiErrorType | BlobStoreError | MetadataBackendError,
  BlobStore | MetadataBackend | SnapshotCoordinator
> => {
  const path = new URL(request.url).pathname;

  if (path === routes.health.path && request.method !== routes.health.method) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.health.method)
    );
  }

  if (path === routes.health.path) {
    return Effect.succeed(
      Response.json(
        Schema.encodeUnknownSync(HealthResponse)({
          protocolVersion: 1,
          service: "stateful-ci-worker",
          status: "ok",
        })
      )
    );
  }

  if (path.startsWith(routes.objects.pathPrefix)) {
    const key = parseObjectRouteKey(path);

    return key === null
      ? Effect.succeed(invalidObjectRouteResponse(path))
      : handleObjectRoute(request, env, key);
  }

  if (
    path === routes.restore.path &&
    request.method !== routes.restore.method
  ) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.restore.method)
    );
  }

  if (path === routes.restore.path) {
    return handleRestore(request, env);
  }

  if (
    path === routes.prepareSave.path &&
    request.method !== routes.prepareSave.method
  ) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.prepareSave.method)
    );
  }

  if (path === routes.prepareSave.path) {
    return handlePrepareSave(request, env);
  }

  if (
    path === routes.commitSave.path &&
    request.method !== routes.commitSave.method
  ) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.commitSave.method)
    );
  }

  if (path === routes.commitSave.path) {
    return handleCommitSave(request, env);
  }

  if (path === routes.save.path && request.method !== routes.save.method) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.save.method)
    );
  }

  if (path === routes.save.path) {
    return handleLegacySave(request, env);
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

const apiErrorStatus = (error: ApiErrorType) => {
  switch (error._tag) {
    case "InvalidJsonBody":
    case "InvalidProtocolPayload": {
      return 400;
    }
    case "MethodNotAllowed": {
      return 405;
    }
    case "RequestBodyTooLarge": {
      return 413;
    }
    case "RouteNotFound": {
      return 404;
    }
    case "Unauthorized": {
      return 401;
    }
    case "Forbidden": {
      return 403;
    }
    default: {
      return 500;
    }
  }
};

const apiErrorResponse = (error: ApiErrorType) =>
  error._tag === "MethodNotAllowed"
    ? Response.json(error, {
        headers: { Allow: error.allowed.join(", ") },
        status: apiErrorStatus(error),
      })
    : Response.json(error, { status: apiErrorStatus(error) });

const errorResponse = (error: unknown) => {
  if (isBlobStoreError(error)) {
    return objectDataPlaneResponse(error);
  }

  if (isApiError(error)) {
    return apiErrorResponse(error);
  }

  if (isMetadataBackendError(error)) {
    return Response.json(error, { status: 500 });
  }

  return Response.json(
    new InvalidProtocolPayload({
      message:
        "The Worker could not process this Stateful CI request. Check backend logs for the unexpected failure.",
    }),
    { status: 500 }
  );
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
    )
  );

export default {
  fetch(request: Request, env: WorkerEnv) {
    return handleFetch(request, env);
  },
} satisfies ExportedHandler<WorkerEnv>;
