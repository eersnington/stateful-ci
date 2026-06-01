import type {
  ApiErrorType,
  DenialReason,
  TrustClass,
  VerifiedGitHubActionsIdentity,
  WorkspaceId,
} from "@stateful-ci/core";
import {
  CommitSaveDeniedResponse,
  CommitSaveRequest,
  CommitSaveResponse,
  PrepareSaveAllowedResponse,
  PrepareSaveDeniedResponse,
  PrepareSaveRequest,
  RestoreAllowedResponse,
  RestoreDeniedResponse,
  RestoreRequest,
  RunId,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import type { BlobStore } from "./blob-store";
import type { BlobStoreError } from "./blob-store-error";
import {
  GitHubOidcVerificationError,
  identityAuditPayload,
  verifyGitHubOidcToken,
} from "./github-oidc";
import {
  MetadataBackend,
  currentIsoTimestamp,
  workspaceIdForRefTarget,
} from "./metadata";
import type { RefTarget } from "./metadata";
import type { MetadataBackendError } from "./metadata-backend-error";
import {
  downloadPlanForObjects,
  missingObjectPlans,
  validateObjectsPresent,
} from "./object-data-plane";
import { withTransferHeaders } from "./object-transfer";
import { decodeProtocolRequest } from "./protocol-request";
import { classifyVerifiedGitHubTrust } from "./run-classification";
import { SnapshotCoordinator } from "./snapshot-coordinator";
import type { ProducerContext } from "./snapshot-coordinator";
import { authorizeDevToken } from "./worker-auth";
import {
  allowedRepositoriesForEnv,
  devAuthEnabled,
  oidcAudienceForEnv,
  oidcJwksForEnv,
  trustedRefsForEnv,
} from "./worker-env";
import type { WorkerEnv } from "./worker-env";

type ControlPlaneAuth =
  | {
      readonly _tag: "VerifiedGitHubOidc";
      readonly identity: VerifiedGitHubActionsIdentity;
    }
  | { readonly _tag: "DevBearer" };

interface RestorePrepareContext {
  readonly auditPayloadJson: string | null;
  readonly producer: ProducerContext;
  readonly runId: RunId;
  readonly target: RefTarget;
  readonly trustClass: TrustClass;
  readonly workspaceId: WorkspaceId;
}

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

const producerContextForRequest = (
  request: Pick<RestoreRequest, "git" | "github" | "workspace">
): ProducerContext => ({
  actor: request.github.actor,
  event: request.github.event,
  job: request.workspace.job,
  ref: request.git.ref,
  repository: request.workspace.repo,
  runId: Schema.decodeSync(RunId)(request.github.runId),
  sha: request.git.sha,
  workflow: request.workspace.workflow,
});

const producerContextForCommitRequest = (
  request: Pick<CommitSaveRequest, "runId">
): ProducerContext => ({
  actor: "unknown",
  event: "unknown",
  job: "unknown",
  ref: "unknown",
  repository: "unknown",
  runId: request.runId,
  sha: "unknown",
  workflow: "unknown",
});

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
  // GitHub OIDC does not expose a stable signed job name, so production
  // namespace authority must not come from request.workspace.job.
  namespace: `repo=${identity.repository}/workflow=${identity.workflow}/config=${request.client.configHash}`,
  refName: `${trustClass}/${refSegment(identity.ref)}/latest`,
});

const metadataTargetForRequest = (
  request: Pick<RestoreRequest, "client" | "git" | "workspace">,
  trustClass: TrustClass
): RefTarget => ({
  namespace: `repo=${request.workspace.repo}/workflow=${request.workspace.workflow}/job=${request.workspace.job}/config=${request.client.configHash}`,
  refName: `${trustClass}/${refSegment(request.git.ref)}/latest`,
});

const restorePrepareContextForAuth = (
  request: Pick<RestoreRequest, "client" | "git" | "github" | "workspace">,
  auth: ControlPlaneAuth,
  trustedRefs: readonly string[]
): RestorePrepareContext => {
  if (auth._tag === "VerifiedGitHubOidc") {
    const trustClass = classifyVerifiedGitHubTrust(auth.identity, {
      trustedRefs,
    });
    const target = metadataTargetForIdentity(
      request,
      auth.identity,
      trustClass
    );

    return {
      auditPayloadJson: identityAuditPayload(auth.identity, null),
      producer: producerContextForIdentity(auth.identity),
      runId: Schema.decodeSync(RunId)(auth.identity.runId),
      target,
      trustClass,
      workspaceId: workspaceIdForRefTarget(target),
    };
  }

  const trustClass = "internal" as const;
  const target = metadataTargetForRequest(request, trustClass);

  return {
    auditPayloadJson: null,
    producer: producerContextForRequest(request),
    runId: Schema.decodeSync(RunId)(request.github.runId),
    target,
    trustClass,
    workspaceId: workspaceIdForRefTarget(target),
  };
};

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

    if (!allowedRepositories.includes(identity.repository.toLowerCase())) {
      return yield* new GitHubOidcVerificationError({
        message: `GitHub repository ${identity.repository} is not allowed to use this Stateful CI backend. Add it to ALLOWED_REPOSITORIES or use the correct backend URL.`,
        reason: "unknown_context_denied",
      });
    }

    return identity;
  });

const authorizeControlPlane = (
  request: Request,
  token: string | null | undefined,
  env: WorkerEnv | undefined
) =>
  Effect.gen(function* authorizeControlPlaneEffect() {
    if (token !== null && token !== undefined) {
      const identity = yield* verifyRequestIdentity(token, env);
      return { _tag: "VerifiedGitHubOidc", identity } as const;
    }

    if (devAuthEnabled(env)) {
      yield* authorizeDevToken(request, env);
      return { _tag: "DevBearer" } as const;
    }

    const identity = yield* verifyRequestIdentity(token, env);
    return { _tag: "VerifiedGitHubOidc", identity } as const;
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
    const createdAt = yield* currentIsoTimestamp;

    yield* metadata.appendAuditEvent({
      createdAt,
      decision: "denied",
      eventType: input.eventType,
      namespace: "unbound/control-plane-auth",
      payloadJson: identityAuditPayload(null, input.reason),
      reason: input.reason,
      refName: "unknown",
      runId: Schema.decodeSync(RunId)(input.request.github.runId),
      snapshotId: null,
      trustClass: "unknown",
      workspaceId: null,
    });
  }
);

const unboundCommitDeniedTarget = {
  namespace: "unbound/commit-save",
  refName: "unknown",
} satisfies RefTarget;

const recordUnboundCommitDeniedAudit = Effect.fn(
  "recordUnboundCommitDeniedAudit"
)(function* recordUnboundCommitDeniedAuditEffect(input: {
  readonly payloadJson: string | null;
  readonly reason: DenialReason;
  readonly runId: RunId | null;
}) {
  const metadata = yield* MetadataBackend;
  const createdAt = yield* currentIsoTimestamp;

  yield* metadata.appendAuditEvent({
    ...unboundCommitDeniedTarget,
    createdAt,
    decision: "denied",
    eventType: "commit",
    payloadJson: input.payloadJson,
    reason: input.reason,
    runId: input.runId,
    snapshotId: null,
    trustClass: "unknown",
    workspaceId: null,
  });
});

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

type ControlPlaneRouteError =
  | ApiErrorType
  | BlobStoreError
  | MetadataBackendError;

export const handleRestore: (
  request: Request,
  env: WorkerEnv | undefined
) => Effect.Effect<
  Response,
  ControlPlaneRouteError,
  BlobStore | MetadataBackend | SnapshotCoordinator
> = Effect.fn("handleRestore")(function* handleRestoreEffect(request, env) {
  const coordinator = yield* SnapshotCoordinator;
  const restoreRequest = yield* decodeProtocolRequest(request, RestoreRequest);

  return yield* authorizeControlPlane(
    request,
    restoreRequest.identity?.token,
    env
  ).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        Effect.gen(function* restoreAuthFailureEffect() {
          if (error._tag === "GitHubOidcVerificationError") {
            yield* recordOidcDeniedAudit({
              eventType: "restore",
              reason: error.reason,
              request: restoreRequest,
            });
            return restoreOidcDenied(error);
          }

          return yield* error;
        }),
      onSuccess: (auth) =>
        Effect.gen(function* restoreAuthorizedEffect() {
          const trustedRefs = trustedRefsForEnv(env);
          const context = restorePrepareContextForAuth(
            restoreRequest,
            auth,
            trustedRefs
          );
          const restore = yield* coordinator.authorizeRestore({
            auditPayloadJson: context.auditPayloadJson,
            candidates: restoreCandidateTargets(
              context.target,
              context.trustClass,
              trustedRefs
            ),
            runId: context.runId,
            target: context.target,
            trustClass: context.trustClass,
            workspaceId: context.workspaceId,
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
                trustClass: context.trustClass,
                ...(restore.saveTarget === null
                  ? {}
                  : { workspaceId: context.workspaceId }),
              })
            );
          }

          return yield* validateObjectsPresent(restore.objects).pipe(
            Effect.matchEffect({
              onFailure: (error) =>
                Effect.gen(function* restoreObjectFailureEffect() {
                  if (error._tag === "SnapshotObjectAvailabilityError") {
                    yield* coordinator.recordRestoreObjectDenial({
                      auditPayloadJson: context.auditPayloadJson,
                      reason: error.reason,
                      runId: context.runId,
                      snapshotId: restore.snapshot.snapshotId,
                      target: context.target,
                      trustClass: context.trustClass,
                      workspaceId: context.workspaceId,
                    });

                    return Response.json(
                      Schema.encodeUnknownSync(RestoreDeniedResponse)({
                        decision: "denied",
                        reason: error.reason,
                        save:
                          restore.saveTarget === null
                            ? { allowed: false }
                            : {
                                allowed: true,
                                target: restore.saveTarget.refName,
                              },
                        trustClass: context.trustClass,
                        ...(restore.saveTarget === null
                          ? {}
                          : { workspaceId: context.workspaceId }),
                      })
                    );
                  }

                  return yield* error;
                }),
              onSuccess: () =>
                Effect.gen(function* restoreAllowedEffect() {
                  yield* coordinator.recordRestoreAllowed({
                    auditPayloadJson: context.auditPayloadJson,
                    runId: context.runId,
                    snapshotId: restore.snapshot.snapshotId,
                    target: context.target,
                    trustClass: context.trustClass,
                    workspaceId: context.workspaceId,
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
                          : {
                              allowed: true,
                              target: restore.saveTarget.refName,
                            },
                      snapshot: {
                        id: restore.snapshot.snapshotId,
                        manifestKey: restore.snapshot.manifestKey,
                        parent: restore.snapshot.parentSnapshotId,
                      },
                      trustClass: context.trustClass,
                      workspaceId: context.workspaceId,
                    })
                  );
                }),
            })
          );
        }),
    })
  );
});

export const handlePrepareSave: (
  request: Request,
  env: WorkerEnv | undefined
) => Effect.Effect<
  Response,
  ControlPlaneRouteError,
  BlobStore | MetadataBackend | SnapshotCoordinator
> = Effect.fn("handlePrepareSave")(
  function* handlePrepareSaveEffect(request, env) {
    const coordinator = yield* SnapshotCoordinator;
    const prepareRequest = yield* decodeProtocolRequest(
      request,
      PrepareSaveRequest
    );

    return yield* authorizeControlPlane(
      request,
      prepareRequest.identity?.token,
      env
    ).pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          Effect.gen(function* prepareAuthFailureEffect() {
            if (error._tag === "GitHubOidcVerificationError") {
              yield* recordOidcDeniedAudit({
                eventType: "prepare-save",
                reason: error.reason,
                request: prepareRequest,
              });
              return prepareOidcDenied(error);
            }

            return yield* error;
          }),
        onSuccess: (auth) =>
          Effect.gen(function* prepareAuthorizedEffect() {
            const trustedRefs = trustedRefsForEnv(env);
            const context = restorePrepareContextForAuth(
              prepareRequest,
              auth,
              trustedRefs
            );
            const preparedAt = yield* currentIsoTimestamp;

            if (
              auth._tag === "VerifiedGitHubOidc" &&
              auth.identity.event === "pull_request_target"
            ) {
              yield* recordPrepareDeniedAudit({
                auditPayloadJson: context.auditPayloadJson,
                reason: "pull_request_target_denied",
                runId: context.runId,
                target: context.target,
                trustClass: context.trustClass,
                workspaceId: context.workspaceId,
              });
              return denyPrepareSave(
                "pull_request_target_denied",
                context.trustClass,
                context.workspaceId
              );
            }

            return yield* missingObjectPlans(prepareRequest.objects).pipe(
              Effect.matchEffect({
                onFailure: (error) =>
                  Effect.gen(function* prepareObjectFailureEffect() {
                    if (error._tag === "SnapshotObjectAvailabilityError") {
                      yield* recordPrepareDeniedAudit({
                        auditPayloadJson: context.auditPayloadJson,
                        reason: error.reason,
                        runId: context.runId,
                        target: context.target,
                        trustClass: context.trustClass,
                        workspaceId: context.workspaceId,
                      });
                      return denyPrepareSave(
                        error.reason,
                        context.trustClass,
                        context.workspaceId
                      );
                    }

                    return yield* error;
                  }),
                onSuccess: (missing) =>
                  Effect.gen(function* prepareAllowedEffect() {
                    const saveAuthorization = yield* coordinator.prepareSave({
                      auditPayloadJson: context.auditPayloadJson,
                      preparedAt,
                      producer: context.producer,
                      runId: context.runId,
                      target: context.target,
                      trustClass: context.trustClass,
                      workspaceId: context.workspaceId,
                    });

                    if (saveAuthorization.decision === "denied") {
                      return denyPrepareSave(
                        saveAuthorization.reason,
                        context.trustClass,
                        context.workspaceId
                      );
                    }

                    return Response.json(
                      Schema.encodeUnknownSync(PrepareSaveAllowedResponse)({
                        baseSnapshotId: saveAuthorization.baseSnapshotId,
                        commitTarget: context.target,
                        decision: "allowed",
                        expectedHeadGeneration:
                          saveAuthorization.expectedHeadGeneration,
                        missingObjects: yield* withTransferHeaders(
                          env,
                          missing
                        ),
                        trustClass: context.trustClass,
                        workspaceId: context.workspaceId,
                      })
                    );
                  }),
              })
            );
          }),
      })
    );
  }
);

export const handleCommitSave: (
  request: Request,
  env: WorkerEnv | undefined
) => Effect.Effect<
  Response,
  ControlPlaneRouteError,
  BlobStore | MetadataBackend | SnapshotCoordinator
> = Effect.fn("handleCommitSave")(
  function* handleCommitSaveEffect(request, env) {
    const coordinator = yield* SnapshotCoordinator;
    const commitRequest = yield* decodeProtocolRequest(
      request,
      CommitSaveRequest
    );

    return yield* authorizeControlPlane(
      request,
      commitRequest.identity?.token,
      env
    ).pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          Effect.gen(function* commitAuthFailureEffect() {
            if (error._tag === "GitHubOidcVerificationError") {
              yield* recordUnboundCommitDeniedAudit({
                payloadJson: identityAuditPayload(null, error.reason),
                reason: error.reason,
                runId: commitRequest.runId,
              });
              return commitDeniedResponse(error.reason);
            }

            return yield* error;
          }),
        onSuccess: (auth) =>
          Effect.gen(function* commitAuthorizedEffect() {
            const producer =
              auth._tag === "VerifiedGitHubOidc"
                ? producerContextForIdentity(auth.identity)
                : producerContextForCommitRequest(commitRequest);
            const auditPayloadJson =
              auth._tag === "VerifiedGitHubOidc"
                ? identityAuditPayload(auth.identity, null)
                : null;

            if (producer.runId !== commitRequest.runId) {
              yield* recordUnboundCommitDeniedAudit({
                payloadJson: identityAuditPayload(
                  auth._tag === "VerifiedGitHubOidc" ? auth.identity : null,
                  "save_run_context_mismatch"
                ),
                reason: "save_run_context_mismatch",
                runId: commitRequest.runId,
              });
              return commitDeniedResponse("save_run_context_mismatch");
            }

            if (
              auth._tag === "VerifiedGitHubOidc" &&
              auth.identity.event === "pull_request_target"
            ) {
              yield* recordUnboundCommitDeniedAudit({
                payloadJson: identityAuditPayload(
                  auth.identity,
                  "pull_request_target_denied"
                ),
                reason: "pull_request_target_denied",
                runId: commitRequest.runId,
              });
              return commitDeniedResponse("pull_request_target_denied");
            }

            return yield* validateObjectsPresent(commitRequest.objects).pipe(
              Effect.matchEffect({
                onFailure: (error) =>
                  Effect.gen(function* commitObjectFailureEffect() {
                    if (error._tag === "SnapshotObjectAvailabilityError") {
                      yield* recordUnboundCommitDeniedAudit({
                        payloadJson: auditPayloadJson,
                        reason: error.reason,
                        runId: commitRequest.runId,
                      });
                      return commitDeniedResponse(error.reason);
                    }

                    return yield* error;
                  }),
                onSuccess: () =>
                  Effect.gen(function* commitAllowedEffect() {
                    const result = yield* coordinator.commitSave({
                      auditPayloadJson,
                      baseSnapshotId: commitRequest.baseSnapshotId,
                      expectedHeadGeneration:
                        commitRequest.expectedHeadGeneration,
                      idempotencyKey: commitRequest.idempotencyKey,
                      manifest: commitRequest.manifest,
                      objects: commitRequest.objects,
                      producer,
                      target: commitRequest.target,
                      workspaceId: commitRequest.workspaceId,
                    });

                    return Response.json(
                      Schema.encodeUnknownSync(CommitSaveResponse)(result)
                    );
                  }),
              })
            );
          }),
      })
    );
  }
);
