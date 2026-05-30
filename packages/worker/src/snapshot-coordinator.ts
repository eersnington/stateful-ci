import type {
  CommitSaveResponse,
  DenialReason,
  HeadGeneration,
  IdempotencyKey,
  ManifestDescriptor,
  RunId,
  SnapshotId,
  SnapshotObjectInventory,
  SnapshotObjectInventoryEntry,
  TrustClass,
  WorkspaceId,
} from "@stateful-ci/core";
import {
  CommitSaveCommittedResponse,
  CommitSaveConflictResponse,
  CommitSaveDeniedResponse,
  CommitSaveIdempotentResponse,
  HeadGeneration as HeadGenerationSchema,
} from "@stateful-ci/core";
import { Context, Effect, Schema } from "effect";

import {
  currentIsoTimestamp,
  inventoryFromSnapshotRows,
  MetadataBackend,
  scopeKeyForRefTarget,
  workspaceIdForRefTarget,
} from "./metadata";
import type {
  RefRow,
  RefTarget,
  SnapshotHeader,
  WorkspaceTarget,
} from "./metadata";
import type { MetadataBackendError } from "./metadata-backend-error";
import { evaluateRestorePolicy, evaluateSavePolicy } from "./policy";

export interface ProducerContext {
  readonly actor: string;
  readonly event: string;
  readonly job: string;
  readonly ref: string;
  readonly repository: string;
  readonly runId: RunId;
  readonly sha: string;
  readonly workflow: string;
}

export type RestoreCandidate = RefTarget;

export type RestoreCoordinatorResult =
  | {
      readonly decision: "allowed";
      readonly manifest: ManifestDescriptor;
      readonly objects: readonly SnapshotObjectInventoryEntry[];
      readonly saveTarget: RefTarget | null;
      readonly snapshot: SnapshotHeader;
    }
  | {
      readonly decision: "denied";
      readonly reason: DenialReason;
      readonly saveTarget: RefTarget | null;
      readonly snapshotId: SnapshotId | null;
    };

export interface AuthorizeRestoreInput {
  readonly auditPayloadJson?: string | null | undefined;
  readonly candidates: readonly RestoreCandidate[];
  readonly runId: RunId;
  readonly target: RefTarget;
  readonly trustClass: TrustClass;
  readonly workspaceId: WorkspaceId;
}

export interface PrepareSaveInput {
  readonly auditPayloadJson?: string | null | undefined;
  readonly expiresAt: string;
  readonly producer: ProducerContext;
  readonly runId: RunId;
  readonly target: RefTarget;
  readonly trustClass: TrustClass;
  readonly workspaceId: WorkspaceId;
}

export type PrepareSaveCoordinatorResult =
  | {
      readonly decision: "allowed";
      readonly baseSnapshotId: SnapshotId | null;
      readonly expectedHeadGeneration: HeadGeneration;
    }
  | { readonly decision: "denied"; readonly reason: DenialReason };

export interface CommitSaveInput {
  readonly auditPayloadJson?: string | null | undefined;
  readonly baseSnapshotId: SnapshotId | null;
  readonly expectedHeadGeneration: HeadGeneration;
  readonly idempotencyKey: IdempotencyKey;
  readonly manifest: ManifestDescriptor;
  readonly objects: SnapshotObjectInventory;
  readonly producer: ProducerContext;
  readonly target: RefTarget;
  readonly workspaceId: WorkspaceId;
}

export class SnapshotCoordinator extends Context.Service<
  SnapshotCoordinator,
  {
    readonly authorizeRestore: (
      input: AuthorizeRestoreInput
    ) => Effect.Effect<
      RestoreCoordinatorResult,
      MetadataBackendError,
      MetadataBackend
    >;
    readonly commitSave: (
      input: CommitSaveInput
    ) => Effect.Effect<
      CommitSaveResponse,
      MetadataBackendError,
      MetadataBackend
    >;
    readonly prepareSave: (
      input: PrepareSaveInput
    ) => Effect.Effect<
      PrepareSaveCoordinatorResult,
      MetadataBackendError,
      MetadataBackend
    >;
    readonly recordRestoreAllowed: (input: {
      readonly auditPayloadJson?: string | null | undefined;
      readonly runId: RunId;
      readonly snapshotId: SnapshotId;
      readonly target: RefTarget;
      readonly trustClass: TrustClass;
      readonly workspaceId: WorkspaceId;
    }) => Effect.Effect<void, MetadataBackendError, MetadataBackend>;
    readonly recordRestoreObjectDenial: (input: {
      readonly auditPayloadJson?: string | null | undefined;
      readonly reason: DenialReason;
      readonly runId: RunId;
      readonly snapshotId: SnapshotId;
      readonly target: RefTarget;
      readonly trustClass: TrustClass;
      readonly workspaceId: WorkspaceId;
    }) => Effect.Effect<void, MetadataBackendError, MetadataBackend>;
  }
>()("stateful-ci/worker/SnapshotCoordinator") {}

const refGeneration = (ref: RefRow | null) =>
  ref?.generation ?? Schema.decodeSync(HeadGenerationSchema)(0);

const manifestFromSnapshotHeader = (
  snapshot: SnapshotHeader
): ManifestDescriptor => ({
  digest: snapshot.manifestDigest,
  key: snapshot.manifestKey,
  size: snapshot.manifestSize,
  snapshotId: snapshot.snapshotId,
});

const saveTargetForPolicy = (target: RefTarget, trustClass: TrustClass) => {
  const savePolicy = evaluateSavePolicy({
    scopeKey: scopeKeyForRefTarget(target),
    trustClass,
  });

  return savePolicy.allowed ? target : null;
};

const snapshotHeaderForCommit = (
  input: CommitSaveInput,
  trustClass: TrustClass,
  createdAt: string
): SnapshotHeader => ({
  createdAt,
  manifestDigest: input.manifest.digest,
  manifestKey: input.manifest.key,
  manifestSize: input.manifest.size,
  namespace: input.target.namespace,
  parentSnapshotId: input.baseSnapshotId,
  producerActor: input.producer.actor,
  producerEvent: input.producer.event,
  producerJob: input.producer.job,
  producerRef: input.producer.ref,
  producerRepository: input.producer.repository,
  producerRunId: input.producer.runId,
  producerSha: input.producer.sha,
  producerWorkflow: input.producer.workflow,
  safetyJson: "{}",
  snapshotId: input.manifest.snapshotId,
  statsJson: JSON.stringify({
    chunkCount: input.objects.filter((object) => object.kind === "chunk")
      .length,
    objectCount: input.objects.length,
    totalObjectBytes: input.objects.reduce(
      (total, object) => total + object.size,
      0
    ),
  }),
  trustClass,
  workspaceId: input.workspaceId,
});

const idempotencyMatches = (
  commit: {
    readonly manifestDigest: CommitSaveInput["manifest"]["digest"];
    readonly runId: RunId;
    readonly snapshotId: SnapshotId;
    readonly workspaceId: WorkspaceId;
  },
  input: CommitSaveInput
) =>
  commit.workspaceId === input.workspaceId &&
  commit.runId === input.producer.runId &&
  commit.snapshotId === input.manifest.snapshotId &&
  commit.manifestDigest === input.manifest.digest;

const producerFromTarget = (
  target: WorkspaceTarget | null,
  fallback: ProducerContext
): ProducerContext => ({
  actor: target?.producerActor ?? fallback.actor,
  event: target?.producerEvent ?? fallback.event,
  job: target?.producerJob ?? fallback.job,
  ref: target?.producerRef ?? fallback.ref,
  repository: target?.producerRepository ?? fallback.repository,
  runId: fallback.runId,
  sha: target?.producerSha ?? fallback.sha,
  workflow: target?.producerWorkflow ?? fallback.workflow,
});

const producerMatchesTarget = (
  target: WorkspaceTarget,
  producer: ProducerContext
) =>
  (target.producerActor === undefined ||
    target.producerActor === producer.actor) &&
  (target.producerEvent === undefined ||
    target.producerEvent === producer.event) &&
  (target.producerJob === undefined || target.producerJob === producer.job) &&
  (target.producerRef === undefined || target.producerRef === producer.ref) &&
  (target.producerRepository === undefined ||
    target.producerRepository === producer.repository) &&
  (target.producerSha === undefined || target.producerSha === producer.sha) &&
  (target.producerWorkflow === undefined ||
    target.producerWorkflow === producer.workflow);

/**
 * Validates that commit-save targets the same prepared run, namespace, and ref.
 *
 * When an OIDC audit payload is present, the commit carries verified producer
 * context derived from signed identity claims. In that case, producer fields are
 * also matched against the prepared target to prevent committing a manifest for
 * a different actor/event/ref/repository/sha/workflow than the one prepared.
 */
const validateCommitTarget = (
  target: WorkspaceTarget,
  input: CommitSaveInput
): DenialReason | null => {
  if (
    target.runId !== input.producer.runId ||
    target.namespace !== input.target.namespace ||
    target.refName !== input.target.refName
  ) {
    return "save_run_context_mismatch";
  }

  if (
    input.auditPayloadJson !== null &&
    input.auditPayloadJson !== undefined &&
    !producerMatchesTarget(target, input.producer)
  ) {
    return "save_run_context_mismatch";
  }

  return null;
};

const recoverIdempotentCommit = Effect.fn("recoverIdempotentCommit")(
  function* recoverIdempotentCommit(
    metadata: MetadataBackend["Service"],
    input: CommitSaveInput,
    actualGeneration: HeadGeneration,
    ref: RefRow | null
  ): Effect.fn.Return<CommitSaveResponse | null, MetadataBackendError> {
    const matchingSnapshot =
      ref?.snapshotId === input.manifest.snapshotId
        ? yield* metadata.getSnapshotHeader(input.manifest.snapshotId)
        : null;

    if (
      matchingSnapshot?.workspaceId !== input.workspaceId ||
      matchingSnapshot.manifestDigest !== input.manifest.digest ||
      matchingSnapshot.producerRunId !== input.producer.runId
    ) {
      return null;
    }

    const idempotent = Schema.decodeSync(CommitSaveIdempotentResponse)({
      decision: "idempotent",
      headGeneration: actualGeneration,
      snapshotId: input.manifest.snapshotId,
      workspaceId: input.workspaceId,
    });
    const createdAt = yield* currentIsoTimestamp;

    yield* metadata.rememberIdempotentCommit({
      createdAt,
      headGeneration: actualGeneration,
      idempotencyKey: input.idempotencyKey,
      latest: true,
      manifestDigest: input.manifest.digest,
      result: idempotent,
      runId: input.producer.runId,
      snapshotId: input.manifest.snapshotId,
      workspaceId: input.workspaceId,
    });

    return idempotent;
  }
);

const replayIdempotentCommit = Effect.fn("replayIdempotentCommit")(
  function* replayIdempotentCommit(
    metadata: MetadataBackend["Service"],
    input: CommitSaveInput,
    commit: {
      readonly headGeneration: HeadGeneration;
      readonly snapshotId: SnapshotId;
      readonly workspaceId: WorkspaceId;
    }
  ): Effect.fn.Return<CommitSaveResponse, MetadataBackendError> {
    const ref = yield* metadata.getRef(
      input.target.namespace,
      input.target.refName
    );
    const actualHeadGeneration = refGeneration(ref);

    if (
      ref?.snapshotId !== commit.snapshotId ||
      actualHeadGeneration !== commit.headGeneration
    ) {
      return Schema.decodeSync(CommitSaveConflictResponse)({
        actualHeadGeneration,
        decision: "conflict",
        reason: "head_generation_mismatch",
      });
    }

    return Schema.decodeSync(CommitSaveIdempotentResponse)({
      decision: "idempotent",
      headGeneration: commit.headGeneration,
      snapshotId: commit.snapshotId,
      workspaceId: commit.workspaceId,
    });
  }
);

const appendAudit = (
  event: Parameters<MetadataBackend["Service"]["appendAuditEvent"]>[0]
) =>
  Effect.gen(function* appendAuditEffect() {
    const metadata = yield* MetadataBackend;
    yield* metadata.appendAuditEvent(event);
  });

export const createMetadataSnapshotCoordinator =
  (): SnapshotCoordinator["Service"] =>
    SnapshotCoordinator.of({
      authorizeRestore: (input) =>
        Effect.gen(function* authorizeRestoreEffect() {
          const metadata = yield* MetadataBackend;
          const saveTarget = saveTargetForPolicy(
            input.target,
            input.trustClass
          );

          if (saveTarget !== null) {
            const expiresAt = yield* currentIsoTimestamp;
            yield* metadata.rememberWorkspaceTarget({
              ...saveTarget,
              expiresAt,
              runId: input.runId,
              trustClass: input.trustClass,
              workspaceId: input.workspaceId,
            });
          }

          if (input.trustClass === "unknown") {
            const createdAt = yield* currentIsoTimestamp;
            yield* metadata.appendAuditEvent({
              ...input.target,
              createdAt,
              decision: "denied",
              eventType: "restore",
              payloadJson: input.auditPayloadJson ?? null,
              reason: "unable_to_classify_run_context",
              runId: input.runId,
              snapshotId: null,
              trustClass: input.trustClass,
              workspaceId: input.workspaceId,
            });
            return {
              decision: "denied" as const,
              reason: "unable_to_classify_run_context" as const,
              saveTarget,
              snapshotId: null,
            };
          }

          let deniedReason: DenialReason | null = null;
          let deniedSnapshotId: SnapshotId | null = null;

          for (const candidate of input.candidates) {
            const ref = yield* metadata.getRef(
              candidate.namespace,
              candidate.refName
            );
            const snapshot =
              ref === null
                ? null
                : yield* metadata.getSnapshotHeader(ref.snapshotId);

            if (ref === null || snapshot === null) {
              continue;
            }

            const decision =
              snapshot.workspaceId === workspaceIdForRefTarget(candidate)
                ? evaluateRestorePolicy({
                    consumer: {
                      scopeKey: scopeKeyForRefTarget(input.target),
                      trustClass: input.trustClass,
                    },
                    producer: {
                      scopeKey: scopeKeyForRefTarget(candidate),
                      trustClass: snapshot.trustClass ?? ref.trustClass,
                    },
                  })
                : ({
                    allowed: false,
                    reason: "restore_policy_denied",
                  } as const);

            if (!decision.allowed) {
              deniedReason = decision.reason;
              deniedSnapshotId = snapshot.snapshotId;
              continue;
            }

            const objects = inventoryFromSnapshotRows(
              yield* metadata.getSnapshotObjects(snapshot.snapshotId)
            );
            return {
              decision: "allowed" as const,
              manifest: manifestFromSnapshotHeader(snapshot),
              objects,
              saveTarget,
              snapshot,
            };
          }

          const createdAt = yield* currentIsoTimestamp;
          yield* metadata.appendAuditEvent({
            ...input.target,
            createdAt,
            decision: "denied",
            eventType: "restore",
            payloadJson: input.auditPayloadJson ?? null,
            reason: deniedReason ?? "no_compatible_snapshot",
            runId: input.runId,
            snapshotId: deniedSnapshotId,
            trustClass: input.trustClass,
            workspaceId: input.workspaceId,
          });

          return {
            decision: "denied" as const,
            reason: deniedReason ?? "no_compatible_snapshot",
            saveTarget,
            snapshotId: deniedSnapshotId,
          };
        }),
      commitSave: (input) =>
        Effect.gen(function* commitSaveEffect() {
          const metadata = yield* MetadataBackend;
          const existing = yield* metadata.getIdempotentCommit(
            input.idempotencyKey
          );

          if (existing !== null) {
            if (idempotencyMatches(existing, input)) {
              return yield* replayIdempotentCommit(metadata, input, existing);
            }

            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: "idempotency_conflict",
            });
          }

          const target = yield* metadata.getWorkspaceTarget(input.workspaceId);

          if (target === null) {
            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: "restore_required_before_save",
            });
          }

          const targetValidation = validateCommitTarget(target, input);

          if (targetValidation !== null) {
            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: targetValidation,
            });
          }

          const savePolicy = evaluateSavePolicy({
            scopeKey: scopeKeyForRefTarget(target),
            trustClass: target.trustClass,
          });

          if (!savePolicy.allowed) {
            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: savePolicy.reason,
            });
          }

          const ref = yield* metadata.getRef(target.namespace, target.refName);
          const actualGeneration = refGeneration(ref);

          if (actualGeneration !== input.expectedHeadGeneration) {
            const recovered = yield* recoverIdempotentCommit(
              metadata,
              input,
              actualGeneration,
              ref
            );

            if (recovered !== null) {
              return recovered;
            }

            return Schema.decodeSync(CommitSaveConflictResponse)({
              actualHeadGeneration: actualGeneration,
              decision: "conflict",
              reason: "head_generation_mismatch",
            });
          }

          if ((ref?.snapshotId ?? null) !== input.baseSnapshotId) {
            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: "save_run_context_mismatch",
            });
          }

          if (input.baseSnapshotId !== null) {
            const baseSnapshot = yield* metadata.getSnapshotHeader(
              input.baseSnapshotId
            );
            if (baseSnapshot === null) {
              return Schema.decodeSync(CommitSaveDeniedResponse)({
                decision: "denied",
                reason: "save_run_context_mismatch",
              });
            }
          }

          const existingSnapshot = yield* metadata.getSnapshotHeader(
            input.manifest.snapshotId
          );
          if (existingSnapshot !== null) {
            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: "invalid_protocol_payload",
            });
          }

          const createdAt = yield* currentIsoTimestamp;
          const producer = producerFromTarget(target, input.producer);

          if (
            [producer.actor, producer.event, producer.sha].includes("unknown")
          ) {
            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: "invalid_protocol_payload",
            });
          }

          const header = snapshotHeaderForCommit(
            { ...input, producer },
            target.trustClass,
            createdAt
          );

          const committed = Schema.decodeSync(CommitSaveCommittedResponse)({
            decision: "committed",
            headGeneration: Schema.decodeSync(HeadGenerationSchema)(
              input.expectedHeadGeneration + 1
            ),
            snapshotId: input.manifest.snapshotId,
            workspaceId: input.workspaceId,
          });
          const idempotentCommit = {
            createdAt,
            headGeneration: committed.headGeneration,
            idempotencyKey: input.idempotencyKey,
            latest: true,
            manifestDigest: input.manifest.digest,
            result: committed,
            runId: input.producer.runId,
            snapshotId: input.manifest.snapshotId,
            workspaceId: input.workspaceId,
          };

          const idempotencyClaimed =
            yield* metadata.rememberIdempotentCommit(idempotentCommit);

          if (!idempotencyClaimed) {
            const reserved = yield* metadata.getIdempotentCommit(
              input.idempotencyKey
            );
            if (reserved !== null && idempotencyMatches(reserved, input)) {
              return yield* replayIdempotentCommit(metadata, input, reserved);
            }

            return Schema.decodeSync(CommitSaveDeniedResponse)({
              decision: "denied",
              reason: "idempotency_conflict",
            });
          }

          yield* metadata.putSnapshotHeader(header);
          yield* metadata.putSnapshotObjects(
            input.manifest.snapshotId,
            input.objects
          );

          const advanced = yield* metadata.compareAndAdvanceRef(
            target.namespace,
            target.refName,
            input.expectedHeadGeneration,
            {
              snapshotId: input.manifest.snapshotId,
              trustClass: target.trustClass,
              updatedByActor: producer.actor,
              updatedByRunId: input.producer.runId,
            }
          );

          if (advanced === null) {
            yield* metadata.releaseIdempotentCommit(idempotentCommit);
            const latestRef = yield* metadata.getRef(
              target.namespace,
              target.refName
            );
            return Schema.decodeSync(CommitSaveConflictResponse)({
              actualHeadGeneration: refGeneration(latestRef),
              decision: "conflict",
              reason: "head_generation_mismatch",
            });
          }

          yield* metadata.appendAuditEvent({
            ...target,
            createdAt,
            decision: "committed",
            eventType: "commit",
            payloadJson: input.auditPayloadJson ?? null,
            reason: null,
            runId: input.producer.runId,
            snapshotId: input.manifest.snapshotId,
            trustClass: target.trustClass,
            workspaceId: input.workspaceId,
          });

          return committed;
        }),
      prepareSave: (input) =>
        Effect.gen(function* prepareSaveEffect() {
          const metadata = yield* MetadataBackend;
          const savePolicy = evaluateSavePolicy({
            scopeKey: scopeKeyForRefTarget(input.target),
            trustClass: input.trustClass,
          });

          if (!savePolicy.allowed) {
            const createdAt = yield* currentIsoTimestamp;
            yield* metadata.appendAuditEvent({
              ...input.target,
              createdAt,
              decision: "denied",
              eventType: "prepare-save",
              payloadJson: input.auditPayloadJson ?? null,
              reason: savePolicy.reason,
              runId: input.runId,
              snapshotId: null,
              trustClass: input.trustClass,
              workspaceId: input.workspaceId,
            });
            return { decision: "denied" as const, reason: savePolicy.reason };
          }

          const ref = yield* metadata.getRef(
            input.target.namespace,
            input.target.refName
          );
          yield* metadata.rememberWorkspaceTarget({
            ...input.target,
            expiresAt: input.expiresAt,
            producerActor: input.producer.actor,
            producerEvent: input.producer.event,
            producerJob: input.producer.job,
            producerRef: input.producer.ref,
            producerRepository: input.producer.repository,
            producerSha: input.producer.sha,
            producerWorkflow: input.producer.workflow,
            runId: input.runId,
            trustClass: input.trustClass,
            workspaceId: input.workspaceId,
          });

          const createdAt = yield* currentIsoTimestamp;
          yield* metadata.appendAuditEvent({
            ...input.target,
            createdAt,
            decision: "allowed",
            eventType: "prepare-save",
            payloadJson: input.auditPayloadJson ?? null,
            reason: null,
            runId: input.runId,
            snapshotId: ref?.snapshotId ?? null,
            trustClass: input.trustClass,
            workspaceId: input.workspaceId,
          });

          return {
            baseSnapshotId: ref?.snapshotId ?? null,
            decision: "allowed" as const,
            expectedHeadGeneration: refGeneration(ref),
          };
        }),
      recordRestoreAllowed: (input) =>
        Effect.gen(function* recordRestoreAllowedEffect() {
          const createdAt = yield* currentIsoTimestamp;
          yield* appendAudit({
            ...input.target,
            createdAt,
            decision: "allowed",
            eventType: "restore",
            payloadJson: input.auditPayloadJson ?? null,
            reason: null,
            runId: input.runId,
            snapshotId: input.snapshotId,
            trustClass: input.trustClass,
            workspaceId: input.workspaceId,
          });
        }),
      recordRestoreObjectDenial: (input) =>
        Effect.gen(function* recordRestoreObjectDenialEffect() {
          const createdAt = yield* currentIsoTimestamp;
          yield* appendAudit({
            ...input.target,
            createdAt,
            decision: "denied",
            eventType: "restore",
            payloadJson: input.auditPayloadJson ?? null,
            reason: input.reason,
            runId: input.runId,
            snapshotId: input.snapshotId,
            trustClass: input.trustClass,
            workspaceId: input.workspaceId,
          });
        }),
    });
