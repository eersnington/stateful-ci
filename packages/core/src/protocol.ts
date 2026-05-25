import { Schema } from "effect";

import { WorkspacePath } from "./config";
import {
  ClientContext,
  GitContext,
  GitHubContext,
  GitHubOidcIdentity,
} from "./github";
import {
  HeadGeneration,
  IdempotencyKey,
  RunId,
  SnapshotId,
  WorkspaceId,
} from "./ids";
import {
  ManifestDescriptor,
  SaveManifest,
  SnapshotObjectInventory,
  SnapshotObjectInventoryEntry,
  SnapshotRef,
} from "./snapshot";
import { DenialReason, TrustClass } from "./trust";
import { WorkspaceRef } from "./workspace";

export const protocolVersion = 1 as const;
export const objectRoutePrefix = "/v1/objects/" as const;

export const HealthResponse = Schema.Struct({
  protocolVersion: Schema.Literal(protocolVersion),
  service: Schema.Literal("stateful-ci-worker"),
  status: Schema.Literal("ok"),
});
export type HealthResponse = Schema.Schema.Type<typeof HealthResponse>;

export const RestoreRequest = Schema.Struct({
  client: ClientContext,
  git: GitContext,
  github: GitHubContext,
  identity: GitHubOidcIdentity,
  managedRoots: Schema.Array(WorkspacePath),
  protocolVersion: Schema.Literal(protocolVersion),
  workspace: WorkspaceRef,
});
export type RestoreRequest = Schema.Schema.Type<typeof RestoreRequest>;

export const CommitTarget = Schema.Struct({
  namespace: Schema.String.check(Schema.isMinLength(1)),
  refName: Schema.String.check(Schema.isMinLength(1)),
});
export type CommitTarget = Schema.Schema.Type<typeof CommitTarget>;

export const ObjectTransferMethod = Schema.Literals(["GET", "PUT"]);
export type ObjectTransferMethod = Schema.Schema.Type<
  typeof ObjectTransferMethod
>;

const TransferHeaders = Schema.Record(Schema.String, Schema.String);

const TransferPlanBase = {
  headers: Schema.optional(TransferHeaders),
  method: ObjectTransferMethod,
  object: SnapshotObjectInventoryEntry,
} as const;

export const WorkerRouteTransferPlanEntry = Schema.Struct({
  ...TransferPlanBase,
  route: Schema.String.check(Schema.isMinLength(1)),
  transport: Schema.Literal("worker-route"),
}).check(
  Schema.makeFilter((entry) =>
    entry.route === `${objectRoutePrefix}${entry.object.key}`
      ? undefined
      : {
          issue:
            "worker-route transfer plans must target the canonical object route for their object",
          path: ["route"],
        }
  )
);
export type WorkerRouteTransferPlanEntry = Schema.Schema.Type<
  typeof WorkerRouteTransferPlanEntry
>;

export const SignedUrlTransferPlanEntry = Schema.Struct({
  ...TransferPlanBase,
  expiresAt: Schema.String.check(Schema.isMinLength(1)),
  transport: Schema.Literal("signed-url"),
  url: Schema.String.check(Schema.isMinLength(1)),
});
export type SignedUrlTransferPlanEntry = Schema.Schema.Type<
  typeof SignedUrlTransferPlanEntry
>;

export const ObjectTransferPlanEntry = Schema.Union([
  WorkerRouteTransferPlanEntry,
  SignedUrlTransferPlanEntry,
]);
export type ObjectTransferPlanEntry = Schema.Schema.Type<
  typeof ObjectTransferPlanEntry
>;

export const ObjectTransferPlan = Schema.Array(ObjectTransferPlanEntry);
export type ObjectTransferPlan = Schema.Schema.Type<typeof ObjectTransferPlan>;

export const ObjectTransferPlanNonEmpty = Schema.NonEmptyArray(
  ObjectTransferPlanEntry
);
export type ObjectTransferPlanNonEmpty = Schema.Schema.Type<
  typeof ObjectTransferPlanNonEmpty
>;

export const SaveRequest = Schema.Struct({
  baseSnapshotId: Schema.NullOr(SnapshotId),
  manifest: SaveManifest,
  protocolVersion: Schema.Literal(protocolVersion),
  runId: RunId,
  workspaceId: WorkspaceId,
});
export type SaveRequest = Schema.Schema.Type<typeof SaveRequest>;

const inventoryContainsManifest = <
  A extends {
    readonly manifest: ManifestDescriptor;
    readonly objects: readonly SnapshotObjectInventoryEntry[];
  },
>() =>
  Schema.makeFilter((request: A) => {
    const matchingManifestObjects = request.objects.filter(
      (object) =>
        object.kind === "manifest" &&
        object.digest === request.manifest.digest &&
        object.key === request.manifest.key &&
        object.size === request.manifest.size
    );

    return matchingManifestObjects.length === 1
      ? undefined
      : {
          issue:
            "manifest descriptor must match exactly one manifest object in the object inventory",
          path: ["manifest"],
        };
  });

export const RestoreSavePlan = Schema.Union([
  Schema.Struct({ allowed: Schema.Literal(false) }),
  Schema.Struct({
    allowed: Schema.Literal(true),
    target: Schema.String.check(Schema.isMinLength(1)),
  }),
]);
export type RestoreSavePlan = Schema.Schema.Type<typeof RestoreSavePlan>;

export const RestoreAllowedResponse = Schema.Struct({
  decision: Schema.Literal("allowed"),
  downloadPlan: ObjectTransferPlanNonEmpty,
  manifest: ManifestDescriptor,
  save: RestoreSavePlan,
  snapshot: SnapshotRef,
  trustClass: TrustClass,
  workspaceId: WorkspaceId,
});
export type RestoreAllowedResponse = Schema.Schema.Type<
  typeof RestoreAllowedResponse
>;

export const RestoreDeniedResponse = Schema.Struct({
  decision: Schema.Literal("denied"),
  reason: DenialReason,
  save: RestoreSavePlan,
  trustClass: TrustClass,
  workspaceId: Schema.optional(WorkspaceId),
});
export type RestoreDeniedResponse = Schema.Schema.Type<
  typeof RestoreDeniedResponse
>;

export const RestoreResponse = Schema.Union([
  RestoreAllowedResponse,
  RestoreDeniedResponse,
]);
export type RestoreResponse = Schema.Schema.Type<typeof RestoreResponse>;

export const SaveCommittedResponse = Schema.Struct({
  decision: Schema.Literal("committed"),
  latest: Schema.Boolean,
  snapshotId: SnapshotId,
  workspaceId: WorkspaceId,
});
export type SaveCommittedResponse = Schema.Schema.Type<
  typeof SaveCommittedResponse
>;

export const SaveDeniedResponse = Schema.Struct({
  decision: Schema.Literal("denied"),
  reason: DenialReason,
});
export type SaveDeniedResponse = Schema.Schema.Type<typeof SaveDeniedResponse>;

export const SaveResponse = Schema.Union([
  SaveCommittedResponse,
  SaveDeniedResponse,
]);
export type SaveResponse = Schema.Schema.Type<typeof SaveResponse>;

export const PrepareSaveRequest = Schema.Struct({
  client: ClientContext,
  git: GitContext,
  github: GitHubContext,
  idempotencyKey: IdempotencyKey,
  identity: GitHubOidcIdentity,
  manifest: ManifestDescriptor,
  objects: SnapshotObjectInventory,
  protocolVersion: Schema.Literal(protocolVersion),
  workspace: WorkspaceRef,
}).check(inventoryContainsManifest());
export type PrepareSaveRequest = Schema.Schema.Type<typeof PrepareSaveRequest>;

export const PrepareSaveDeniedResponse = Schema.Struct({
  decision: Schema.Literal("denied"),
  reason: DenialReason,
  trustClass: TrustClass,
  workspaceId: Schema.optional(WorkspaceId),
});
export type PrepareSaveDeniedResponse = Schema.Schema.Type<
  typeof PrepareSaveDeniedResponse
>;

export const PrepareSaveAllowedResponse = Schema.Struct({
  baseSnapshotId: Schema.NullOr(SnapshotId),
  commitTarget: CommitTarget,
  decision: Schema.Literal("allowed"),
  expectedHeadGeneration: HeadGeneration,
  missingObjects: ObjectTransferPlan,
  trustClass: TrustClass,
  workspaceId: WorkspaceId,
});
export type PrepareSaveAllowedResponse = Schema.Schema.Type<
  typeof PrepareSaveAllowedResponse
>;

export const PrepareSaveResponse = Schema.Union([
  PrepareSaveAllowedResponse,
  PrepareSaveDeniedResponse,
]);
export type PrepareSaveResponse = Schema.Schema.Type<
  typeof PrepareSaveResponse
>;

export const CommitSaveRequest = Schema.Struct({
  baseSnapshotId: Schema.NullOr(SnapshotId),
  expectedHeadGeneration: HeadGeneration,
  idempotencyKey: IdempotencyKey,
  identity: GitHubOidcIdentity,
  manifest: ManifestDescriptor,
  objects: SnapshotObjectInventory,
  protocolVersion: Schema.Literal(protocolVersion),
  runId: RunId,
  target: CommitTarget,
  workspaceId: WorkspaceId,
}).check(inventoryContainsManifest());
export type CommitSaveRequest = Schema.Schema.Type<typeof CommitSaveRequest>;

export const CommitSaveCommittedResponse = Schema.Struct({
  decision: Schema.Literal("committed"),
  headGeneration: HeadGeneration,
  snapshotId: SnapshotId,
  workspaceId: WorkspaceId,
});
export type CommitSaveCommittedResponse = Schema.Schema.Type<
  typeof CommitSaveCommittedResponse
>;

export const CommitSaveIdempotentResponse = Schema.Struct({
  decision: Schema.Literal("idempotent"),
  headGeneration: HeadGeneration,
  snapshotId: SnapshotId,
  workspaceId: WorkspaceId,
});
export type CommitSaveIdempotentResponse = Schema.Schema.Type<
  typeof CommitSaveIdempotentResponse
>;

export const CommitSaveConflictResponse = Schema.Struct({
  actualHeadGeneration: HeadGeneration,
  decision: Schema.Literal("conflict"),
  reason: Schema.Literal("head_generation_mismatch"),
});
export type CommitSaveConflictResponse = Schema.Schema.Type<
  typeof CommitSaveConflictResponse
>;

export const CommitSaveDeniedResponse = Schema.Struct({
  decision: Schema.Literal("denied"),
  reason: DenialReason,
});
export type CommitSaveDeniedResponse = Schema.Schema.Type<
  typeof CommitSaveDeniedResponse
>;

export const CommitSaveResponse = Schema.Union([
  CommitSaveCommittedResponse,
  CommitSaveIdempotentResponse,
  CommitSaveConflictResponse,
  CommitSaveDeniedResponse,
]);
export type CommitSaveResponse = Schema.Schema.Type<typeof CommitSaveResponse>;

export const routes = {
  commitSave: { method: "POST", path: "/v1/save/commit" },
  health: { method: "GET", path: "/health" },
  objects: { pathPrefix: objectRoutePrefix },
  prepareSave: { method: "POST", path: "/v1/save/prepare" },
  restore: { method: "POST", path: "/v1/restore" },
  save: { method: "POST", path: "/v1/save" },
} as const;
