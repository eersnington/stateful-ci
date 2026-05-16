import type { ApiErrorType, DenialReason } from "@stateful-ci/core";
import {
  Forbidden,
  HealthResponse,
  InvalidJsonBody,
  InvalidProtocolPayload,
  MethodNotAllowed,
  RequestBodyTooLarge,
  RestoreAllowedResponse,
  RestoreDeniedResponse,
  RestoreRequest,
  RouteNotFound,
  RunId,
  SaveCommittedResponse,
  routes,
  SaveDeniedResponse,
  SaveRequest,
  Unauthorized,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import {
  createInMemoryMetadataBackend,
  MetadataBackend,
  snapshotHeaderFromManifest,
} from "./metadata";
import type { RefRow, RefTarget, SnapshotHeader } from "./metadata";
import { evaluateRestorePolicy, evaluateSavePolicy } from "./policy";
import { classifyRunTrust, defaultTrustedRefs } from "./run-classification";

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
  readonly STATEFUL_CI_API_TOKEN?: string;
  readonly STATEFUL_CI_TRUSTED_REFS?: string;
}

export interface HandleFetchOptions {
  readonly metadata?: MetadataBackend["Service"];
}

const protocolBodyTooLarge = new RequestBodyTooLarge({
  limitBytes: maxProtocolBodyBytes,
  message:
    "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
});

const defaultMetadata = createInMemoryMetadataBackend();

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
  request: RestoreRequest,
  trustedRefs: readonly string[]
): RefTarget => ({
  namespace: `repo=${request.workspace.repo}/workflow=${request.workspace.workflow}/job=${request.workspace.job}/config=${request.client.configHash}`,
  refName: `${classifyRunTrust(request, { trustedRefs })}/${refSegment(request.git.ref)}/latest`,
});

const trustedSeedTargetFor = (
  target: RefTarget,
  trustedRefs: readonly string[]
): RefTarget => ({
  namespace: target.namespace,
  refName: `trusted/${refSegment(trustedRefs[0] ?? defaultTrustedRefs[0])}/latest`,
});

const scopeKeyForTarget = (target: RefTarget) =>
  `${target.namespace}\n${target.refName}`;

const restoreCandidateTargets = (
  target: RefTarget,
  trustClass: ReturnType<typeof classifyRunTrust>,
  trustedRefs: readonly string[]
) =>
  trustClass === "trusted" || trustClass === "unknown"
    ? [target]
    : [target, trustedSeedTargetFor(target, trustedRefs)];

const candidateProducerScope = (
  target: RefTarget,
  ref: RefRow,
  snapshot: SnapshotHeader
) => ({
  scopeKey: scopeKeyForTarget(target),
  trustClass: snapshot.trustClass ?? ref.trustClass,
});

const workspaceIdForTarget = (target: RefTarget) =>
  Schema.decodeSync(WorkspaceId)(`ws:${target.namespace}:${target.refName}`);

const runIdFromRestore = (request: RestoreRequest) =>
  Schema.decodeSync(RunId)(request.github.runId);

const nowIso = () => new Date().toISOString();

const handleRestore = (request: Request, env: WorkerEnv | undefined) =>
  Effect.gen(function* handleRestoreEffect() {
    const metadata = yield* MetadataBackend;
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

    if (trustClass === "unknown") {
      yield* metadata.appendAuditEvent({
        ...target,
        createdAt: nowIso(),
        decision: "denied",
        eventType: "restore",
        reason: "unable_to_classify_run_context",
        runId,
        snapshotId: null,
        trustClass,
        workspaceId,
      });

      return Response.json(
        Schema.encodeUnknownSync(RestoreDeniedResponse)({
          decision: "denied",
          reason: "unable_to_classify_run_context",
          save: { allowed: false },
          trustClass,
        })
      );
    }

    const candidates = restoreCandidateTargets(target, trustClass, trustedRefs);
    let deniedReason: DenialReason | null = null;
    let deniedSnapshotId: SnapshotHeader["snapshotId"] | null = null;
    let restored: {
      readonly snapshot: SnapshotHeader;
    } | null = null;

    for (const candidateTarget of candidates) {
      const ref = yield* metadata.getRef(
        candidateTarget.namespace,
        candidateTarget.refName
      );
      const snapshot =
        ref === null ? null : yield* metadata.getSnapshotHeader(ref.snapshotId);

      if (ref !== null && snapshot !== null) {
        const decision =
          snapshot.workspaceId === workspaceIdForTarget(candidateTarget)
            ? evaluateRestorePolicy({
                consumer: { scopeKey: scopeKeyForTarget(target), trustClass },
                producer: candidateProducerScope(
                  candidateTarget,
                  ref,
                  snapshot
                ),
              })
            : ({ allowed: false, reason: "restore_policy_denied" } as const);

        if (decision.allowed) {
          restored = { snapshot };
          break;
        }

        deniedReason = decision.reason;
        deniedSnapshotId = snapshot.snapshotId;
      }
    }

    if (restored === null) {
      const savePolicy = evaluateSavePolicy({
        scopeKey: scopeKeyForTarget(target),
        trustClass,
      });

      if (savePolicy.allowed) {
        yield* metadata.rememberWorkspaceTarget({
          ...target,
          runId,
          trustClass,
          workspaceId,
        });
      }

      yield* metadata.appendAuditEvent({
        ...target,
        createdAt: nowIso(),
        decision: "denied",
        eventType: "restore",
        reason: deniedReason ?? "no_compatible_snapshot",
        runId,
        snapshotId: deniedSnapshotId,
        trustClass,
        workspaceId,
      });

      return Response.json(
        Schema.encodeUnknownSync(RestoreDeniedResponse)({
          decision: "denied",
          reason: deniedReason ?? "no_compatible_snapshot",
          save: savePolicy.allowed
            ? { allowed: true, target: target.refName }
            : { allowed: false },
          trustClass,
          ...(savePolicy.allowed ? { workspaceId } : {}),
        })
      );
    }

    const savePolicy = evaluateSavePolicy({
      scopeKey: scopeKeyForTarget(target),
      trustClass,
    });

    if (savePolicy.allowed) {
      yield* metadata.rememberWorkspaceTarget({
        ...target,
        runId,
        trustClass,
        workspaceId,
      });
    }

    yield* metadata.appendAuditEvent({
      ...target,
      createdAt: nowIso(),
      decision: "allowed",
      eventType: "restore",
      reason: null,
      runId,
      snapshotId: restored.snapshot.snapshotId,
      trustClass,
      workspaceId,
    });

    return Response.json(
      Schema.encodeUnknownSync(RestoreAllowedResponse)({
        decision: "allowed",
        save: savePolicy.allowed
          ? { allowed: true, target: target.refName }
          : { allowed: false },
        snapshot: {
          id: restored.snapshot.snapshotId,
          manifestKey: restored.snapshot.manifestKey,
          parent: restored.snapshot.parentSnapshotId,
        },
        trustClass,
        workspaceId,
      })
    );
  });

const handleSave = (request: Request, env: WorkerEnv | undefined) =>
  Effect.gen(function* handleSaveEffect() {
    const metadata = yield* MetadataBackend;
    yield* authorizeRequest(request, env);
    const saveRequest = yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(SaveRequest))
    );
    const target = yield* metadata.getWorkspaceTarget(saveRequest.workspaceId);

    if (target === null) {
      yield* metadata.appendAuditEvent({
        createdAt: nowIso(),
        decision: "denied",
        eventType: "save",
        namespace: "",
        reason: "restore_required_before_save",
        refName: "",
        runId: saveRequest.runId,
        snapshotId: saveRequest.manifest.id,
        trustClass: null,
        workspaceId: saveRequest.workspaceId,
      });

      return Response.json(
        Schema.encodeUnknownSync(SaveDeniedResponse)({
          decision: "denied",
          reason: "restore_required_before_save",
        })
      );
    }

    if (target.runId !== saveRequest.runId) {
      yield* metadata.appendAuditEvent({
        createdAt: nowIso(),
        decision: "denied",
        eventType: "save",
        namespace: target.namespace,
        reason: "save_run_context_mismatch",
        refName: target.refName,
        runId: saveRequest.runId,
        snapshotId: saveRequest.manifest.id,
        trustClass: target.trustClass,
        workspaceId: saveRequest.workspaceId,
      });

      return Response.json(
        Schema.encodeUnknownSync(SaveDeniedResponse)({
          decision: "denied",
          reason: "save_run_context_mismatch",
        })
      );
    }

    const savePolicy = evaluateSavePolicy({
      scopeKey: scopeKeyForTarget(target),
      trustClass: target.trustClass,
    });

    if (!savePolicy.allowed) {
      yield* metadata.appendAuditEvent({
        createdAt: nowIso(),
        decision: "denied",
        eventType: "save",
        namespace: target.namespace,
        reason: savePolicy.reason,
        refName: target.refName,
        runId: saveRequest.runId,
        snapshotId: saveRequest.manifest.id,
        trustClass: target.trustClass,
        workspaceId: saveRequest.workspaceId,
      });

      return Response.json(
        Schema.encodeUnknownSync(SaveDeniedResponse)({
          decision: "denied",
          reason: savePolicy.reason,
        })
      );
    }

    yield* metadata.putSnapshotHeader(
      snapshotHeaderFromManifest(saveRequest.manifest, {
        createdAt: nowIso(),
        parentSnapshotId: saveRequest.baseSnapshotId,
        runId: saveRequest.runId,
        trustClass: target.trustClass,
        workspaceId: saveRequest.workspaceId,
      })
    );
    yield* metadata.setRef(target, saveRequest.manifest.id, target.trustClass);
    yield* metadata.appendAuditEvent({
      ...target,
      createdAt: nowIso(),
      decision: "committed",
      eventType: "save",
      reason: null,
      runId: saveRequest.runId,
      snapshotId: saveRequest.manifest.id,
      trustClass: target.trustClass,
      workspaceId: saveRequest.workspaceId,
    });

    return Response.json(
      Schema.encodeUnknownSync(SaveCommittedResponse)({
        decision: "committed",
        latest: true,
        snapshotId: saveRequest.manifest.id,
        workspaceId: saveRequest.workspaceId,
      })
    );
  });

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

const handleRequest = (request: Request, env?: WorkerEnv) => {
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

  if (path === routes.save.path && request.method !== routes.save.method) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.save.method)
    );
  }

  if (path === routes.save.path) {
    return handleSave(request, env);
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

export const handleFetch = (
  request: Request,
  env?: WorkerEnv,
  options: HandleFetchOptions = {}
): Promise<Response> =>
  Effect.runPromise(
    handleRequest(request, env).pipe(
      Effect.provideService(
        MetadataBackend,
        // Until D1/DO bindings exist, default Worker fetches are intentionally
        // request-local. Tests and local dev can inject a shared backend.
        options.metadata ?? defaultMetadata
      ),
      Effect.match({
        onFailure: apiErrorResponse,
        onSuccess: (response) => response,
      })
    )
  );

export default {
  fetch(request: Request, env: WorkerEnv) {
    return handleFetch(request, env);
  },
} satisfies ExportedHandler<WorkerEnv>;
