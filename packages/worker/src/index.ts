import type {
  ApiErrorType,
  DenialReason,
  ObjectTransferPlanEntry,
  VerifiedGitHubActionsIdentity,
} from "@stateful-ci/core";
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
  defaultGitHubOidcAudience,
  GitHubOidcVerificationError,
  GitHubJwks,
  identityAuditPayload,
  verifyGitHubOidcToken,
} from "./github-oidc";
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
import {
  classifyVerifiedGitHubTrust,
  defaultTrustedRefs,
} from "./run-classification";
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
  readonly ALLOWED_REPOSITORIES?: string;
  readonly DEV_AUTH_ENABLED?: string;
  readonly OIDC_AUDIENCE?: string;
  readonly STATEFUL_CI_ALLOWED_REPOSITORIES?: string;
  readonly STATEFUL_CI_COORDINATORS?: DurableObjectNamespace;
  readonly STATEFUL_CI_METADATA?: D1Database;
  readonly STATEFUL_CI_OBJECTS?: R2Bucket;
  readonly STATEFUL_CI_API_TOKEN?: string;
  readonly STATEFUL_CI_DEV_AUTH_ENABLED?: string;
  readonly STATEFUL_CI_GITHUB_JWKS_JSON?: string;
  readonly STATEFUL_CI_OIDC_AUDIENCE?: string;
  readonly STATEFUL_CI_TRUSTED_REFS?: string;
  readonly STATEFUL_CI_TRANSFER_SECRET?: string;
  readonly TRUSTED_REFS?: string;
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
  const configured = (env?.TRUSTED_REFS ?? env?.STATEFUL_CI_TRUSTED_REFS)
    ?.split(",")
    .map((ref) => ref.trim())
    .filter((ref) => ref.length > 0);

  return configured === undefined || configured.length === 0
    ? defaultTrustedRefs
    : configured;
};

const configuredList = (source: string | undefined) =>
  source
    ?.split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0) ?? [];

const allowedRepositoriesForEnv = (env: WorkerEnv | undefined) =>
  configuredList(
    env?.ALLOWED_REPOSITORIES ?? env?.STATEFUL_CI_ALLOWED_REPOSITORIES
  );

const oidcAudienceForEnv = (env: WorkerEnv | undefined) => {
  const configured = (env?.OIDC_AUDIENCE ?? env?.STATEFUL_CI_OIDC_AUDIENCE)
    ?.trim()
    .replaceAll("\n", "");

  return configured === undefined || configured.length === 0
    ? defaultGitHubOidcAudience
    : configured;
};

const devAuthEnabled = (env: WorkerEnv | undefined) =>
  (env?.DEV_AUTH_ENABLED ?? env?.STATEFUL_CI_DEV_AUTH_ENABLED) === "1" ||
  (env?.DEV_AUTH_ENABLED ?? env?.STATEFUL_CI_DEV_AUTH_ENABLED) === "true";

const oidcJwksForEnv = (env: WorkerEnv | undefined) => {
  if (env?.STATEFUL_CI_GITHUB_JWKS_JSON === undefined) {
    return { status: "unset" as const };
  }

  const parsed = Schema.decodeUnknownExit(Schema.fromJsonString(GitHubJwks))(
    env.STATEFUL_CI_GITHUB_JWKS_JSON
  );

  return Exit.isFailure(parsed)
    ? { status: "invalid" as const }
    : { jwks: parsed.value.keys, status: "valid" as const };
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

const producerContextForIdentity = (
  identity: VerifiedGitHubActionsIdentity
) => ({
  actor: identity.actor,
  event: identity.event,
  job: identity.checkRunId ?? identity.workflow,
  ref: identity.ref,
  repository: identity.repository,
  runId: Schema.decodeSync(RunId)(identity.runId),
  sha: identity.sha,
  workflow: identity.workflow,
});

const authorizeDevToken = (request: Request, env: WorkerEnv | undefined) => {
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

const transferSecretForEnv = (env: WorkerEnv | undefined) => {
  const secret = env?.STATEFUL_CI_TRANSFER_SECRET;

  return secret === undefined || secret.length === 0 ? null : secret;
};

const transferTokenHeader = "x-stateful-ci-transfer-token";
const transferExpiresAtHeader = "x-stateful-ci-transfer-expires-at";
const transferTokenTtlMillis = 15 * 60 * 1000;

const bytesToHex = (bytes: Uint8Array) =>
  [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");

const hexToBytes = (source: string) => {
  if (source.length % 2 !== 0 || !/^[0-9a-f]+$/iu.test(source)) {
    return null;
  }

  const bytes = new Uint8Array(source.length / 2);

  for (let index = 0; index < source.length; index += 2) {
    bytes[index / 2] = Number.parseInt(source.slice(index, index + 2), 16);
  }

  return bytes;
};

const constantTimeEqual = (actual: string, expected: string) => {
  const actualBytes = hexToBytes(actual);
  const expectedBytes = hexToBytes(expected);

  if (actualBytes === null || expectedBytes === null) {
    return false;
  }

  if (actualBytes.byteLength !== expectedBytes.byteLength) {
    return false;
  }

  let difference = 0;

  for (let index = 0; index < expectedBytes.byteLength; index += 1) {
    difference += Math.abs(
      (actualBytes[index] ?? 0) - (expectedBytes[index] ?? 0)
    );
  }

  return difference === 0;
};

const transferTokenPayload = (input: {
  readonly digest: string;
  readonly expiresAt: string;
  readonly key: string;
  readonly method: string;
  readonly size: number;
}) =>
  [
    "v1",
    input.method,
    input.key,
    input.digest,
    String(input.size),
    input.expiresAt,
  ].join("\n");

const signTransferPayload = Effect.fn("signTransferPayload")(
  function* signTransferPayloadEffect(secret: string, payload: string) {
    const key = yield* Effect.tryPromise({
      catch: () =>
        new Forbidden({
          message:
            "The Worker could not prepare object transfer authorization. Check STATEFUL_CI_TRANSFER_SECRET and retry.",
        }),
      try: () =>
        crypto.subtle.importKey(
          "raw",
          new TextEncoder().encode(secret),
          { hash: "SHA-256", name: "HMAC" },
          false,
          ["sign"]
        ),
    });
    const signature = yield* Effect.tryPromise({
      catch: () =>
        new Forbidden({
          message:
            "The Worker could not sign object transfer authorization. Check backend crypto support and retry.",
        }),
      try: () =>
        crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload)),
    });

    return bytesToHex(new Uint8Array(signature));
  }
);

const planWithObjectHeaders = (plan: ObjectTransferPlanEntry) => ({
  ...plan,
  headers: {
    ...plan.headers,
    "x-stateful-ci-object-digest": plan.object.digest,
    "x-stateful-ci-object-kind": plan.object.kind,
    "x-stateful-ci-object-size": String(plan.object.size),
  },
});

const transferHeadersForPlan = Effect.fn("transferHeadersForPlan")(
  function* transferHeadersForPlanEffect(
    env: WorkerEnv | undefined,
    plan: ObjectTransferPlanEntry
  ) {
    if (devAuthEnabled(env)) {
      return planWithObjectHeaders(plan) satisfies ObjectTransferPlanEntry;
    }

    const secret = transferSecretForEnv(env);

    if (secret === null) {
      return yield* new Forbidden({
        message:
          "The Worker does not have STATEFUL_CI_TRANSFER_SECRET configured, so backend-authorized object transfer plans cannot be issued. Configure the transfer secret before using restore/save object downloads or uploads.",
      });
    }

    const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const expiresAt = String(now + transferTokenTtlMillis);
    const token = yield* signTransferPayload(
      secret,
      transferTokenPayload({
        digest: plan.object.digest,
        expiresAt,
        key: plan.object.key,
        method: plan.method,
        size: plan.object.size,
      })
    );

    return {
      ...planWithObjectHeaders(plan),
      headers: {
        ...planWithObjectHeaders(plan).headers,
        [transferExpiresAtHeader]: expiresAt,
        [transferTokenHeader]: token,
      },
    } satisfies ObjectTransferPlanEntry;
  }
);

const withTransferHeaders = (
  env: WorkerEnv | undefined,
  plans: readonly ObjectTransferPlanEntry[]
) =>
  Effect.gen(function* withTransferHeadersEffect() {
    const authorizedPlans: ObjectTransferPlanEntry[] = [];

    for (const plan of plans) {
      authorizedPlans.push(yield* transferHeadersForPlan(env, plan));
    }

    return authorizedPlans;
  });

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

const authorizeObjectTransfer = Effect.fn("authorizeObjectTransfer")(
  function* authorizeObjectTransferEffect(
    request: Request,
    env: WorkerEnv | undefined,
    key: SnapshotObjectInventoryEntry["key"]
  ) {
    if (devAuthEnabled(env)) {
      return yield* authorizeDevToken(request, env);
    }

    const expectedToken = transferSecretForEnv(env);
    const actualToken = request.headers.get(transferTokenHeader);
    const expiresAt = request.headers.get(transferExpiresAtHeader);
    const object = expectedObjectFromHeaders(key, request.headers);

    if (expectedToken === null) {
      return yield* new Forbidden({
        message:
          "The Worker does not have STATEFUL_CI_TRANSFER_SECRET configured, so object downloads and uploads are disabled. Configure the transfer secret and retry.",
      });
    }

    if (actualToken === null || expiresAt === null || object === null) {
      return yield* new Forbidden({
        message:
          "The object transfer request did not include complete backend-issued transfer authorization. Restore/save object bytes were not served or stored.",
      });
    }

    const expiresAtMillis = Number(expiresAt);

    if (!Number.isSafeInteger(expiresAtMillis)) {
      return yield* new Forbidden({
        message:
          "The object transfer request included an invalid transfer expiration. Restore/save object bytes were not served or stored.",
      });
    }

    const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

    if (expiresAtMillis <= now) {
      return yield* new Forbidden({
        message:
          "The object transfer authorization expired. Retry restore/save so the backend can issue a fresh object plan.",
      });
    }

    const expected = yield* signTransferPayload(
      expectedToken,
      transferTokenPayload({
        digest: object.digest,
        expiresAt,
        key,
        method: request.method,
        size: object.size,
      })
    );

    if (!constantTimeEqual(actualToken, expected)) {
      return yield* new Forbidden({
        message:
          "The object transfer authorization did not match this method, object, digest, size, and expiry. Restore/save object bytes were not served or stored.",
      });
    }

    return yield* Effect.void;
  }
);

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

const metadataTargetForIdentity = (
  request: Pick<RestoreRequest, "client">,
  identity: VerifiedGitHubActionsIdentity,
  trustClass: ReturnType<typeof classifyVerifiedGitHubTrust>
): RefTarget => ({
  namespace: `repo=${identity.repository}/workflow=${identity.workflow}/config=${request.client.configHash}`,
  refName: `${trustClass}/${refSegment(identity.ref)}/latest`,
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
  trustClass: ReturnType<typeof classifyVerifiedGitHubTrust>,
  trustedRefs: readonly string[]
) =>
  trustClass === "trusted" || trustClass === "unknown"
    ? [target]
    : [target, ...trustedSeedTargetsFor(target, trustedRefs)];

const workspaceIdForTarget = (target: RefTarget) =>
  Schema.decodeSync(WorkspaceId)(`ws:${target.namespace}:${target.refName}`);

const verifyRequestIdentity = (
  token: string | null | undefined,
  env: WorkerEnv | undefined
) =>
  Effect.gen(function* verifyRequestIdentityEffect() {
    const audience = oidcAudienceForEnv(env);

    const jwks = oidcJwksForEnv(env);
    if (jwks.status === "invalid") {
      return yield* new GitHubOidcVerificationError({
        message:
          "STATEFUL_CI_GITHUB_JWKS_JSON was configured but did not match the expected JWKS schema. Restore/save was denied because identity signatures could not be verified.",
        reason: "oidc_invalid",
      });
    }

    const identity = yield* verifyGitHubOidcToken(token ?? "", {
      audience,
      ...(jwks.status === "valid" ? { jwks: jwks.jwks } : {}),
    });
    const allowedRepositories = allowedRepositoriesForEnv(env);

    if (allowedRepositories.length === 0) {
      return yield* new GitHubOidcVerificationError({
        message:
          "The Worker does not have ALLOWED_REPOSITORIES configured, so restore/save control-plane requests are disabled. Configure a comma-separated repository allowlist such as owner/repo before using Stateful CI.",
        reason: "unknown_context_denied",
      });
    }

    if (!allowedRepositories.includes(identity.repository)) {
      return yield* new GitHubOidcVerificationError({
        message: `GitHub repository ${identity.repository} is not allowed to use this Stateful CI backend. Add it to ALLOWED_REPOSITORIES or use the correct backend URL.`,
        reason: "unknown_context_denied",
      });
    }

    return { identity } as const;
  });

const restoreOidcDenied = (error: GitHubOidcVerificationError) =>
  Response.json(
    Schema.encodeUnknownSync(RestoreDeniedResponse)({
      decision: "denied",
      reason: error.reason,
      save: { allowed: false },
      trustClass: "unknown",
    })
  );

const prepareOidcDenied = (error: GitHubOidcVerificationError) =>
  Response.json(
    Schema.encodeUnknownSync(PrepareSaveDeniedResponse)({
      decision: "denied",
      reason: error.reason,
      trustClass: "unknown",
    })
  );

const unverifiedAuditTargetFor = (
  request: Pick<RestoreRequest, "client" | "git" | "workspace">
): RefTarget => ({
  namespace: `repo=${request.workspace.repo}/workflow=${request.workspace.workflow}/job=${request.workspace.job}/config=${request.client.configHash}`,
  refName: `unknown/${refSegment(request.git.ref)}/latest`,
});

const recordOidcDeniedAudit = Effect.fn("recordOidcDeniedAudit")(
  function* recordOidcDeniedAuditEffect(input: {
    readonly eventType: "prepare-save" | "restore";
    readonly reason: DenialReason;
    readonly request: Pick<
      RestoreRequest,
      "client" | "git" | "github" | "workspace"
    >;
  }) {
    const metadata = yield* MetadataBackend;
    const target = unverifiedAuditTargetFor(input.request);
    const createdAt = yield* currentIsoTimestamp;

    yield* metadata.appendAuditEvent({
      ...target,
      createdAt,
      decision: "denied",
      eventType: input.eventType,
      payloadJson: identityAuditPayload(null, input.reason),
      reason: input.reason,
      runId: Schema.decodeSync(RunId)(input.request.github.runId),
      snapshotId: null,
      trustClass: "unknown",
      workspaceId: workspaceIdForTarget(target),
    });
  }
);

const recordCommitDeniedAudit = Effect.fn("recordCommitDeniedAudit")(
  function* recordCommitDeniedAuditEffect(input: {
    readonly payloadJson: string | null;
    readonly reason: DenialReason;
    readonly request: CommitSaveRequest;
    readonly trustClass: ReturnType<typeof classifyVerifiedGitHubTrust>;
  }) {
    const metadata = yield* MetadataBackend;
    const createdAt = yield* currentIsoTimestamp;

    yield* metadata.appendAuditEvent({
      ...input.request.target,
      createdAt,
      decision: "denied",
      eventType: "commit",
      payloadJson: input.payloadJson,
      reason: input.reason,
      runId: input.request.runId,
      snapshotId: input.request.manifest.snapshotId,
      trustClass: input.trustClass,
      workspaceId: input.request.workspaceId,
    });
  }
);

const recordPrepareDeniedAudit = Effect.fn("recordPrepareDeniedAudit")(
  function* recordPrepareDeniedAuditEffect(input: {
    readonly auditPayloadJson: string | null;
    readonly reason: DenialReason;
    readonly runId: RunId;
    readonly target: RefTarget;
    readonly trustClass: ReturnType<typeof classifyVerifiedGitHubTrust>;
    readonly workspaceId: WorkspaceId;
  }) {
    const metadata = yield* MetadataBackend;
    const createdAt = yield* currentIsoTimestamp;

    yield* metadata.appendAuditEvent({
      ...input.target,
      createdAt,
      decision: "denied",
      eventType: "prepare-save",
      payloadJson: input.auditPayloadJson,
      reason: input.reason,
      runId: input.runId,
      snapshotId: null,
      trustClass: input.trustClass,
      workspaceId: input.workspaceId,
    });
  }
);

const commitDeniedResponse = (reason: DenialReason) =>
  Response.json(
    Schema.encodeUnknownSync(CommitSaveDeniedResponse)({
      decision: "denied",
      reason,
    })
  );

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
    yield* authorizeObjectTransfer(request, env, key);

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
      if (
        !devAuthEnabled(env) &&
        expectedObjectFromHeaders(key, request.headers) === null
      ) {
        return invalidObjectUploadPlanResponse(new URL(request.url).pathname);
      }

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
  trustClass: ReturnType<typeof classifyVerifiedGitHubTrust>,
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
  const restoreRequest = yield* readProtocolBody(request).pipe(
    Effect.flatMap(parseProtocolJson),
    Effect.flatMap(decodeProtocolPayload(RestoreRequest))
  );
  const verified = yield* verifyRequestIdentity(
    restoreRequest.identity?.token,
    env
  ).pipe(
    Effect.match({
      onFailure: (error) => ({ error, status: "denied" as const }),
      onSuccess: (result) => ({
        identity: result.identity,
        status: "verified" as const,
      }),
    })
  );

  if (verified.status === "denied") {
    yield* recordOidcDeniedAudit({
      eventType: "restore",
      reason: verified.error.reason,
      request: restoreRequest,
    });
    return restoreOidcDenied(verified.error);
  }

  const trustedRefs = trustedRefsForEnv(env);
  const trustClass = classifyVerifiedGitHubTrust(verified.identity, {
    trustedRefs,
  });
  const target = metadataTargetForIdentity(
    restoreRequest,
    verified.identity,
    trustClass
  );
  const workspaceId = workspaceIdForTarget(target);
  const runId = Schema.decodeSync(RunId)(verified.identity.runId);
  const restore = yield* coordinator.authorizeRestore({
    auditPayloadJson: identityAuditPayload(verified.identity, null),
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
      auditPayloadJson: identityAuditPayload(verified.identity, null),
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
    auditPayloadJson: identityAuditPayload(verified.identity, null),
    runId,
    snapshotId: restore.snapshot.snapshotId,
    target,
    trustClass,
    workspaceId,
  });

  return Response.json(
    Schema.encodeUnknownSync(RestoreAllowedResponse)({
      decision: "allowed",
      downloadPlan: yield* withTransferHeaders(
        env,
        downloadPlanForObjects(restore.objects)
      ),
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
    const prepareRequest = yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(PrepareSaveRequest))
    );
    const verified = yield* verifyRequestIdentity(
      prepareRequest.identity?.token,
      env
    ).pipe(
      Effect.match({
        onFailure: (error) => ({ error, status: "denied" as const }),
        onSuccess: (result) => ({
          identity: result.identity,
          status: "verified" as const,
        }),
      })
    );

    if (verified.status === "denied") {
      yield* recordOidcDeniedAudit({
        eventType: "prepare-save",
        reason: verified.error.reason,
        request: prepareRequest,
      });
      return prepareOidcDenied(verified.error);
    }

    const trustedRefs = trustedRefsForEnv(env);
    const trustClass = classifyVerifiedGitHubTrust(verified.identity, {
      trustedRefs,
    });
    const target = metadataTargetForIdentity(
      prepareRequest,
      verified.identity,
      trustClass
    );
    const workspaceId = workspaceIdForTarget(target);
    const runId = Schema.decodeSync(RunId)(verified.identity.runId);
    const expiresAt = yield* currentIsoTimestamp;
    const auditPayloadJson = identityAuditPayload(verified.identity, null);
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
      yield* recordPrepareDeniedAudit({
        auditPayloadJson,
        reason: missing.reason,
        runId,
        target,
        trustClass,
        workspaceId,
      });
      return denyPrepareSave(missing.reason, trustClass, workspaceId);
    }

    const saveAuthorization = yield* coordinator.prepareSave({
      auditPayloadJson,
      expiresAt,
      producer: producerContextForIdentity(verified.identity),
      runId,
      target,
      trustClass,
      workspaceId,
    });

    if (saveAuthorization.decision === "denied") {
      return denyPrepareSave(saveAuthorization.reason, trustClass, workspaceId);
    }

    return Response.json(
      Schema.encodeUnknownSync(PrepareSaveAllowedResponse)({
        baseSnapshotId: saveAuthorization.baseSnapshotId,
        commitTarget: target,
        decision: "allowed",
        expectedHeadGeneration: saveAuthorization.expectedHeadGeneration,
        missingObjects: yield* withTransferHeaders(env, missing.plans),
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
    const commitRequest = yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(CommitSaveRequest))
    );
    const verified = yield* verifyRequestIdentity(
      commitRequest.identity?.token,
      env
    ).pipe(
      Effect.match({
        onFailure: (error) => ({ error, status: "denied" as const }),
        onSuccess: (result) => ({
          identity: result.identity,
          status: "verified" as const,
        }),
      })
    );

    if (verified.status === "denied") {
      yield* recordCommitDeniedAudit({
        payloadJson: identityAuditPayload(null, verified.error.reason),
        reason: verified.error.reason,
        request: commitRequest,
        trustClass: "unknown",
      });
      return commitDeniedResponse(verified.error.reason);
    }

    if (verified.identity.runId !== commitRequest.runId) {
      yield* recordCommitDeniedAudit({
        payloadJson: identityAuditPayload(
          verified.identity,
          "save_run_context_mismatch"
        ),
        reason: "save_run_context_mismatch",
        request: commitRequest,
        trustClass: classifyVerifiedGitHubTrust(verified.identity, {
          trustedRefs: trustedRefsForEnv(env),
        }),
      });
      return commitDeniedResponse("save_run_context_mismatch");
    }

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
      yield* recordCommitDeniedAudit({
        payloadJson: identityAuditPayload(verified.identity, null),
        reason: objectValidation.reason,
        request: commitRequest,
        trustClass: classifyVerifiedGitHubTrust(verified.identity, {
          trustedRefs: trustedRefsForEnv(env),
        }),
      });
      return commitDeniedResponse(objectValidation.reason);
    }

    const producer = producerContextForIdentity(verified.identity);
    const result = yield* coordinator.commitSave({
      auditPayloadJson: identityAuditPayload(verified.identity, null),
      baseSnapshotId: commitRequest.baseSnapshotId,
      expectedHeadGeneration: commitRequest.expectedHeadGeneration,
      idempotencyKey: commitRequest.idempotencyKey,
      manifest: commitRequest.manifest,
      objects: commitRequest.objects,
      producer,
      target: commitRequest.target,
      workspaceId: commitRequest.workspaceId,
    });

    return Response.json(Schema.encodeUnknownSync(CommitSaveResponse)(result));
  }
);

const handleLegacySave = Effect.fn("handleLegacySave")(
  function* handleLegacySaveEffect(
    request: Request,
    _env: WorkerEnv | undefined
  ) {
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
