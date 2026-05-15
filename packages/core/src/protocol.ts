import { Schema } from "effect";

import { ClientContext, GitContext, GitHubContext } from "./github";
import { RunId, SnapshotId, WorkspaceId } from "./ids";
import { SaveManifest, SnapshotRef } from "./snapshot";
import { DenialReason, TrustClass } from "./trust";
import { WorkspaceRef } from "./workspace";

export const protocolVersion = 1 as const;

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
  workspace: WorkspaceRef,
});
export type RestoreRequest = Schema.Schema.Type<typeof RestoreRequest>;

export const SaveRequest = Schema.Struct({
  baseSnapshotId: Schema.NullOr(SnapshotId),
  manifest: SaveManifest,
  runId: RunId,
  workspaceId: WorkspaceId,
});
export type SaveRequest = Schema.Schema.Type<typeof SaveRequest>;

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

export const routes = {
  health: { method: "GET", path: "/health" },
  restore: { method: "POST", path: "/v1/restore" },
  save: { method: "POST", path: "/v1/save" },
} as const;
