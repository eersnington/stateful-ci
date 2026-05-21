import type {
  DenialReason,
  ManifestDescriptor,
  ManifestKey,
  RunId,
  SaveManifest,
  Sha256Digest,
  SnapshotId,
  SnapshotObjectInventory,
  TrustClass,
  WorkspaceId,
} from "@stateful-ci/core";
import { Clock, Context, Effect } from "effect";

import { SnapshotHeaderFromManifestFailed } from "./metadata-errors";

export interface RefTarget {
  readonly namespace: string;
  readonly refName: string;
}

export interface RefRow extends RefTarget {
  readonly snapshotId: SnapshotId;
  readonly trustClass: TrustClass;
  readonly updatedAt: string;
  readonly version: number;
}

export interface SnapshotHeader {
  readonly chunkCount: number;
  readonly createdAt: string;
  readonly manifestDigest: Sha256Digest;
  readonly manifestKey: ManifestKey;
  readonly manifestSize: number;
  readonly objects: SnapshotObjectInventory;
  readonly parentSnapshotId: SnapshotId | null;
  readonly runId: RunId;
  readonly snapshotId: SnapshotId;
  readonly totalBytes: number;
  readonly trustClass: TrustClass;
  readonly workspaceId: WorkspaceId;
}

export type AuditDecision = "allowed" | "denied" | "committed";
export type AuditEventType = "restore" | "save";

export interface AuditEvent extends RefTarget {
  readonly createdAt: string;
  readonly decision: AuditDecision;
  readonly eventType: AuditEventType;
  readonly reason: DenialReason | null;
  readonly runId: RunId | null;
  readonly snapshotId: SnapshotId | null;
  readonly trustClass: TrustClass | null;
  readonly workspaceId: WorkspaceId | null;
}

export interface WorkspaceTarget extends RefTarget {
  readonly runId: RunId;
  readonly trustClass: TrustClass;
  readonly workspaceId: WorkspaceId;
}

export interface InMemoryMetadataSeed {
  readonly auditEvents?: readonly AuditEvent[];
  readonly refs?: readonly RefRow[];
  readonly snapshots?: readonly SnapshotHeader[];
  readonly workspaceTargets?: readonly WorkspaceTarget[];
}

export class MetadataBackend extends Context.Service<
  MetadataBackend,
  {
    readonly appendAuditEvent: (event: AuditEvent) => Effect.Effect<void>;
    readonly getRef: (
      namespace: string,
      refName: string
    ) => Effect.Effect<RefRow | null>;
    readonly getSnapshotHeader: (
      snapshotId: SnapshotId
    ) => Effect.Effect<SnapshotHeader | null>;
    readonly getWorkspaceTarget: (
      workspaceId: WorkspaceId
    ) => Effect.Effect<WorkspaceTarget | null>;
    readonly listAuditEvents: Effect.Effect<readonly AuditEvent[]>;
    readonly putSnapshotHeader: (header: SnapshotHeader) => Effect.Effect<void>;
    readonly rememberWorkspaceTarget: (
      target: WorkspaceTarget
    ) => Effect.Effect<void>;
    readonly setRef: (
      target: RefTarget,
      snapshotId: SnapshotId,
      trustClass: TrustClass
    ) => Effect.Effect<RefRow>;
  }
>()("stateful-ci/worker/MetadataBackend") {}

const refKey = (target: RefTarget) => `${target.namespace}\n${target.refName}`;

export const snapshotHeaderFromManifest = (
  manifest: SaveManifest,
  options: {
    readonly createdAt: string;
    readonly parentSnapshotId: SnapshotId | null;
    readonly runId: RunId;
    readonly trustClass: TrustClass;
    readonly workspaceId: WorkspaceId;
  }
): Effect.Effect<SnapshotHeader, SnapshotHeaderFromManifestFailed> => {
  const manifestObject = manifest.objects.find(
    (object) =>
      object.kind === "manifest" &&
      object.digest === manifest.hash &&
      object.key === manifest.key
  );

  if (manifestObject === undefined) {
    return Effect.fail(
      new SnapshotHeaderFromManifestFailed({
        message:
          "Save manifest inventory did not include its manifest object. The snapshot header was not persisted.",
      })
    );
  }

  return Effect.succeed({
    chunkCount: manifest.chunkCount,
    createdAt: options.createdAt,
    manifestDigest: manifest.hash,
    manifestKey: manifest.key,
    manifestSize: manifestObject.size,
    objects: manifest.objects,
    parentSnapshotId: options.parentSnapshotId,
    runId: options.runId,
    snapshotId: manifest.id,
    totalBytes: manifest.totalBytes,
    trustClass: options.trustClass,
    workspaceId: options.workspaceId,
  });
};

const currentIsoTimestamp = Clock.currentTimeMillis.pipe(
  Effect.map((millis) => new Date(millis).toISOString())
);

export const manifestDescriptorFromSnapshotHeader = (
  snapshot: SnapshotHeader
): ManifestDescriptor => ({
  digest: snapshot.manifestDigest,
  key: snapshot.manifestKey,
  size: snapshot.manifestSize,
  snapshotId: snapshot.snapshotId,
});

export const createInMemoryMetadataBackend = (
  seed: InMemoryMetadataSeed = {}
): MetadataBackend["Service"] => {
  const refs = new Map(
    (seed.refs ?? []).map((ref) => [refKey(ref), ref] as const)
  );
  const snapshots = new Map(
    (seed.snapshots ?? []).map(
      (snapshot) => [snapshot.snapshotId, snapshot] as const
    )
  );
  const workspaceTargets = new Map(
    (seed.workspaceTargets ?? []).map(
      (target) => [target.workspaceId, target] as const
    )
  );
  const auditEvents = [...(seed.auditEvents ?? [])];

  return MetadataBackend.of({
    appendAuditEvent: (event) =>
      Effect.sync(() => {
        auditEvents.push(event);
      }),
    getRef: (namespace, refName) =>
      Effect.sync(() => refs.get(refKey({ namespace, refName })) ?? null),
    getSnapshotHeader: (snapshotId) =>
      Effect.sync(() => snapshots.get(snapshotId) ?? null),
    getWorkspaceTarget: (workspaceId) =>
      Effect.sync(() => workspaceTargets.get(workspaceId) ?? null),
    listAuditEvents: Effect.sync(() => [...auditEvents]),
    putSnapshotHeader: (header) =>
      Effect.sync(() => {
        snapshots.set(header.snapshotId, header);
      }),
    rememberWorkspaceTarget: (target) =>
      Effect.sync(() => {
        workspaceTargets.set(target.workspaceId, target);
      }),
    setRef: (target, snapshotId, trustClass) =>
      Effect.gen(function* setRefEffect() {
        const updatedAt = yield* currentIsoTimestamp;
        const key = refKey(target);
        const previous = refs.get(key);
        const next = {
          ...target,
          snapshotId,
          trustClass,
          updatedAt,
          version: previous === undefined ? 1 : previous.version + 1,
        } satisfies RefRow;

        refs.set(key, next);
        return next;
      }),
  });
};
