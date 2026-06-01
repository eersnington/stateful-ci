import type {
  CommitSaveResponse,
  DenialReason,
  IdempotencyKey,
  RunId,
  SnapshotId,
  SnapshotObjectInventoryEntry,
  TrustClass,
  WorkspaceId,
} from "@stateful-ci/core";
import {
  CommitSaveResponse as CommitSaveResponseSchema,
  DenialReason as DenialReasonSchema,
  HeadGeneration,
  IdempotencyKey as IdempotencyKeySchema,
  ManifestKey,
  RunId as RunIdSchema,
  Sha256Digest,
  SnapshotId as SnapshotIdSchema,
  SnapshotObjectInventory,
  SnapshotObjectInventoryEntry as SnapshotObjectInventoryEntrySchema,
  TrustClass as TrustClassSchema,
  WorkspaceId as WorkspaceIdSchema,
} from "@stateful-ci/core";
import { Clock, Context, Effect, Schema } from "effect";

import { MetadataBackendError } from "./metadata-backend-error";

export interface RefTarget {
  readonly namespace: string;
  readonly refName: string;
}

export interface RefRow extends RefTarget {
  readonly generation: HeadGeneration;
  readonly snapshotId: SnapshotId;
  readonly trustClass: TrustClass;
  readonly updatedAt: string;
  readonly updatedByActor: string | null;
  readonly updatedByRunId: RunId | null;
}

export interface SnapshotHeader {
  readonly chunkCount?: number;
  readonly createdAt: string;
  readonly manifestDigest: Sha256Digest;
  readonly manifestKey: ManifestKey;
  readonly manifestSize: number;
  readonly namespace: string;
  readonly objects?: SnapshotObjectInventory;
  readonly parentSnapshotId: SnapshotId | null;
  readonly producerActor: string;
  readonly producerEvent: string;
  readonly producerJob: string;
  readonly producerRef: string;
  readonly producerRepository: string;
  readonly producerRunId: RunId;
  readonly producerSha: string;
  readonly producerWorkflow: string;
  readonly runId?: RunId;
  readonly safetyJson: string;
  readonly snapshotId: SnapshotId;
  readonly statsJson: string;
  readonly trustClass: TrustClass;
  readonly totalBytes?: number;
  readonly workspaceId: WorkspaceId;
}

export interface SnapshotObjectRow {
  readonly digest: Sha256Digest;
  readonly key: SnapshotObjectInventoryEntry["key"];
  readonly kind: SnapshotObjectInventoryEntry["kind"];
  readonly size: number;
  readonly snapshotId: SnapshotId;
}

export type AuditDecision = "allowed" | "committed" | "denied" | "idempotent";
export type AuditEventType = "commit" | "prepare-save" | "restore";

const AuditDecisionSchema = Schema.Literals([
  "allowed",
  "committed",
  "denied",
  "idempotent",
]);
const AuditEventTypeSchema = Schema.Literals([
  "commit",
  "prepare-save",
  "restore",
]);

export interface AuditEvent extends RefTarget {
  readonly createdAt: string;
  readonly decision: AuditDecision;
  readonly eventType: AuditEventType;
  readonly id?: string;
  readonly payloadJson: string | null;
  readonly reason: DenialReason | null;
  readonly runId: RunId | null;
  readonly snapshotId: SnapshotId | null;
  readonly trustClass: TrustClass | null;
  readonly workspaceId: WorkspaceId | null;
}

export interface WorkspaceTarget extends RefTarget {
  readonly preparedAt?: string;
  readonly producerActor?: string;
  readonly producerEvent?: string;
  readonly producerJob?: string;
  readonly producerRef?: string;
  readonly producerRepository?: string;
  readonly producerSha?: string;
  readonly producerWorkflow?: string;
  readonly runId: RunId;
  readonly trustClass: TrustClass;
  readonly workspaceId: WorkspaceId;
}

export interface IdempotentCommit {
  readonly createdAt: string;
  readonly headGeneration: HeadGeneration;
  readonly idempotencyKey: IdempotencyKey;
  readonly latest: boolean;
  readonly manifestDigest: Sha256Digest;
  readonly result: CommitSaveResponse;
  readonly runId: RunId;
  readonly snapshotId: SnapshotId;
  readonly workspaceId: WorkspaceId;
}

export interface AuditEventQuery {
  readonly namespace?: string;
  readonly refName?: string;
  readonly workspaceId?: WorkspaceId;
  readonly runId?: RunId;
}

interface RefD1Row {
  readonly generation: number;
  readonly namespace: string;
  readonly ref_name: string;
  readonly snapshot_id: string;
  readonly trust_class: string;
  readonly updated_at: string;
  readonly updated_by_actor: string | null;
  readonly updated_by_run_id: string | null;
}

interface SnapshotD1Row {
  readonly created_at: string;
  readonly manifest_digest: string;
  readonly manifest_key: string;
  readonly manifest_size: number;
  readonly namespace: string;
  readonly parent_snapshot_id: string | null;
  readonly producer_actor: string;
  readonly producer_event: string;
  readonly producer_job: string;
  readonly producer_ref: string;
  readonly producer_repository: string;
  readonly producer_run_id: string;
  readonly producer_sha: string;
  readonly producer_workflow: string;
  readonly safety_json: string;
  readonly snapshot_id: string;
  readonly stats_json: string;
  readonly trust_class: string;
  readonly workspace_id: string;
}

interface SnapshotObjectD1Row {
  readonly object_digest: string;
  readonly object_key: string;
  readonly object_kind: string;
  readonly size: number;
  readonly snapshot_id: string;
}

interface WorkspaceTargetD1Row {
  readonly namespace: string;
  readonly prepared_at: string | null;
  readonly producer_actor: string | null;
  readonly producer_event: string | null;
  readonly producer_job: string | null;
  readonly producer_ref: string | null;
  readonly producer_repository: string | null;
  readonly producer_sha: string | null;
  readonly producer_workflow: string | null;
  readonly ref_name: string;
  readonly run_id: string;
  readonly trust_class: string;
  readonly workspace_id: string;
}

interface IdempotentCommitD1Row {
  readonly created_at: string;
  readonly head_generation: number;
  readonly idempotency_key: string;
  readonly latest: number;
  readonly manifest_digest: string;
  readonly result_json: string;
  readonly run_id: string;
  readonly snapshot_id: string;
  readonly workspace_id: string;
}

interface AuditEventD1Row {
  readonly created_at: string;
  readonly decision: string;
  readonly event_type: string;
  readonly id: string;
  readonly namespace: string;
  readonly payload_json: string | null;
  readonly reason: string | null;
  readonly ref_name: string;
  readonly run_id: string | null;
  readonly snapshot_id: string | null;
  readonly trust_class: string | null;
  readonly workspace_id: string | null;
}

export interface InMemoryMetadataSeed {
  readonly auditEvents?: readonly AuditEvent[];
  readonly idempotentCommits?: readonly IdempotentCommit[];
  readonly refs?: readonly RefRow[];
  readonly snapshotObjects?: readonly SnapshotObjectRow[];
  readonly snapshots?: readonly SnapshotHeader[];
  readonly workspaceTargets?: readonly WorkspaceTarget[];
}

export class MetadataBackend extends Context.Service<
  MetadataBackend,
  {
    readonly appendAuditEvent: (
      event: AuditEvent
    ) => Effect.Effect<void, MetadataBackendError>;
    readonly compareAndAdvanceRef: (
      namespace: string,
      refName: string,
      expectedGeneration: HeadGeneration,
      next: {
        readonly snapshotId: SnapshotId;
        readonly trustClass: TrustClass;
        readonly updatedByActor: string | null;
        readonly updatedByRunId: RunId | null;
      }
    ) => Effect.Effect<RefRow | null, MetadataBackendError>;
    readonly getIdempotentCommit: (
      idempotencyKey: IdempotencyKey
    ) => Effect.Effect<IdempotentCommit | null, MetadataBackendError>;
    readonly getRef: (
      namespace: string,
      refName: string
    ) => Effect.Effect<RefRow | null, MetadataBackendError>;
    readonly getSnapshotHeader: (
      snapshotId: SnapshotId
    ) => Effect.Effect<SnapshotHeader | null, MetadataBackendError>;
    readonly getSnapshotObjects: (
      snapshotId: SnapshotId
    ) => Effect.Effect<readonly SnapshotObjectRow[], MetadataBackendError>;
    readonly getWorkspaceTarget: (
      workspaceId: WorkspaceId
    ) => Effect.Effect<WorkspaceTarget | null, MetadataBackendError>;
    readonly listAuditEvents: (
      query?: AuditEventQuery
    ) => Effect.Effect<readonly AuditEvent[], MetadataBackendError>;
    readonly putSnapshotHeader: (
      header: SnapshotHeader
    ) => Effect.Effect<void, MetadataBackendError>;
    readonly putSnapshotObjects: (
      snapshotId: SnapshotId,
      objects: readonly SnapshotObjectInventoryEntry[]
    ) => Effect.Effect<void, MetadataBackendError>;
    readonly releaseIdempotentCommit: (
      commit: IdempotentCommit
    ) => Effect.Effect<void, MetadataBackendError>;
    readonly rememberIdempotentCommit: (
      commit: IdempotentCommit
    ) => Effect.Effect<boolean, MetadataBackendError>;
    readonly rememberWorkspaceTarget: (
      target: WorkspaceTarget
    ) => Effect.Effect<void, MetadataBackendError>;
  }
>()("stateful-ci/worker/MetadataBackend") {}

export const scopeKeyForRefTarget = (target: RefTarget) =>
  `${target.namespace}\n${target.refName}`;

export const workspaceIdForRefTarget = (target: RefTarget) =>
  Schema.decodeSync(WorkspaceIdSchema)(
    `ws:${target.namespace}:${target.refName}`
  );

export const currentIsoTimestamp = Clock.currentTimeMillis.pipe(
  Effect.map((millis) => new Date(millis).toISOString())
);

export const inventoryFromSnapshotRows = (
  objects: readonly SnapshotObjectRow[]
): SnapshotObjectInventory =>
  Schema.decodeUnknownSync(SnapshotObjectInventory)(
    objects.map((object) => ({
      digest: object.digest,
      key: object.key,
      kind: object.kind,
      size: object.size,
    }))
  );

export const snapshotRowsFromInventory = (
  snapshotId: SnapshotId,
  objects: readonly SnapshotObjectInventoryEntry[]
): readonly SnapshotObjectRow[] =>
  objects.map((object) => ({
    digest: object.digest,
    key: object.key,
    kind: object.kind,
    size: object.size,
    snapshotId,
  }));

const byObjectKey = (left: SnapshotObjectRow, right: SnapshotObjectRow) =>
  left.key.localeCompare(right.key);

const snapshotHeaderMatches = (left: SnapshotHeader, right: SnapshotHeader) =>
  left.manifestDigest === right.manifestDigest &&
  left.manifestKey === right.manifestKey &&
  left.manifestSize === right.manifestSize &&
  left.namespace === right.namespace &&
  left.parentSnapshotId === right.parentSnapshotId &&
  left.producerActor === right.producerActor &&
  left.producerEvent === right.producerEvent &&
  left.producerJob === right.producerJob &&
  left.producerRef === right.producerRef &&
  left.producerRepository === right.producerRepository &&
  left.producerRunId === right.producerRunId &&
  left.producerSha === right.producerSha &&
  left.producerWorkflow === right.producerWorkflow &&
  left.safetyJson === right.safetyJson &&
  left.snapshotId === right.snapshotId &&
  left.statsJson === right.statsJson &&
  left.trustClass === right.trustClass &&
  left.workspaceId === right.workspaceId;

const snapshotObjectMatches = (
  left: SnapshotObjectRow,
  right: SnapshotObjectRow
) =>
  left.digest === right.digest &&
  left.key === right.key &&
  left.kind === right.kind &&
  left.size === right.size &&
  left.snapshotId === right.snapshotId;

export const snapshotObjectsMatch = (
  left: readonly SnapshotObjectRow[],
  right: readonly SnapshotObjectRow[]
) => {
  if (left.length !== right.length) {
    return false;
  }

  const leftSorted = left.toSorted(byObjectKey);
  const rightSorted = right.toSorted(byObjectKey);

  return leftSorted.every((object, index) => {
    const other = rightSorted[index];
    return other !== undefined && snapshotObjectMatches(object, other);
  });
};

export const snapshotObjectConflict = (
  existing: readonly SnapshotObjectRow[],
  desired: readonly SnapshotObjectRow[]
) => {
  const desiredByKey = new Map(desired.map((object) => [object.key, object]));

  return existing.some((object) => {
    const match = desiredByKey.get(object.key);
    return match === undefined || !snapshotObjectMatches(object, match);
  });
};

const auditMatchesQuery = (event: AuditEvent, query: AuditEventQuery = {}) =>
  (query.namespace === undefined || event.namespace === query.namespace) &&
  (query.refName === undefined || event.refName === query.refName) &&
  (query.workspaceId === undefined ||
    event.workspaceId === query.workspaceId) &&
  (query.runId === undefined || event.runId === query.runId);

export const createInMemoryMetadataBackend = (
  seed: InMemoryMetadataSeed = {}
): MetadataBackend["Service"] => {
  const refs = new Map(
    (seed.refs ?? []).map((ref) => [scopeKeyForRefTarget(ref), ref] as const)
  );
  const snapshots = new Map(
    (seed.snapshots ?? []).map(
      (snapshot) => [snapshot.snapshotId, snapshot] as const
    )
  );
  const snapshotObjects = new Map<SnapshotId, readonly SnapshotObjectRow[]>();
  for (const object of seed.snapshotObjects ?? []) {
    snapshotObjects.set(object.snapshotId, [
      ...(snapshotObjects.get(object.snapshotId) ?? []),
      object,
    ]);
  }
  for (const snapshot of seed.snapshots ?? []) {
    if (snapshot.objects !== undefined) {
      snapshotObjects.set(
        snapshot.snapshotId,
        snapshotRowsFromInventory(snapshot.snapshotId, snapshot.objects)
      );
    } else if (!snapshotObjects.has(snapshot.snapshotId)) {
      snapshotObjects.set(snapshot.snapshotId, []);
    }
  }
  const workspaceTargets = new Map(
    (seed.workspaceTargets ?? []).map(
      (target) => [target.workspaceId, target] as const
    )
  );
  const idempotentCommits = new Map(
    (seed.idempotentCommits ?? []).map(
      (commit) => [commit.idempotencyKey, commit] as const
    )
  );
  const auditEvents = [...(seed.auditEvents ?? [])];

  return MetadataBackend.of({
    appendAuditEvent: (event) =>
      Effect.sync(() => {
        auditEvents.push(event);
      }),
    compareAndAdvanceRef: (namespace, refName, expectedGeneration, next) =>
      Effect.gen(function* compareAndAdvanceRefEffect() {
        const key = scopeKeyForRefTarget({ namespace, refName });
        const previous = refs.get(key);
        const actualGeneration =
          previous?.generation ?? Schema.decodeSync(HeadGeneration)(0);

        if (actualGeneration !== expectedGeneration) {
          return null;
        }

        const updatedAt = yield* currentIsoTimestamp;
        const advanced = {
          generation: Schema.decodeSync(HeadGeneration)(expectedGeneration + 1),
          namespace,
          refName,
          snapshotId: next.snapshotId,
          trustClass: next.trustClass,
          updatedAt,
          updatedByActor: next.updatedByActor,
          updatedByRunId: next.updatedByRunId,
        } satisfies RefRow;

        refs.set(key, advanced);
        return advanced;
      }),
    getIdempotentCommit: (idempotencyKey) =>
      Effect.sync(() => idempotentCommits.get(idempotencyKey) ?? null),
    getRef: (namespace, refName) =>
      Effect.sync(
        () => refs.get(scopeKeyForRefTarget({ namespace, refName })) ?? null
      ),
    getSnapshotHeader: (snapshotId) =>
      Effect.sync(() => snapshots.get(snapshotId) ?? null),
    getSnapshotObjects: (snapshotId) =>
      Effect.sync(() => [...(snapshotObjects.get(snapshotId) ?? [])]),
    getWorkspaceTarget: (workspaceId) =>
      Effect.sync(() => workspaceTargets.get(workspaceId) ?? null),
    listAuditEvents: (query) =>
      Effect.sync(() =>
        auditEvents.filter((event) => auditMatchesQuery(event, query))
      ),
    putSnapshotHeader: (header) =>
      Effect.gen(function* putSnapshotHeaderInMemoryEffect() {
        const existing = snapshots.get(header.snapshotId);

        if (existing !== undefined) {
          if (snapshotHeaderMatches(existing, header)) {
            return;
          }

          return yield* new MetadataBackendError({
            message:
              "In-memory metadata write failed during putSnapshotHeader. Snapshot IDs are immutable; choose a new snapshot ID before retrying.",
            operation: "putSnapshotHeader",
          });
        }

        snapshots.set(header.snapshotId, header);
      }),
    putSnapshotObjects: (snapshotId, objects) =>
      Effect.gen(function* putSnapshotObjectsInMemoryEffect() {
        const desired = snapshotRowsFromInventory(snapshotId, objects);
        const existing = snapshotObjects.get(snapshotId) ?? [];

        if (snapshotObjectConflict(existing, desired)) {
          return yield* new MetadataBackendError({
            message:
              "In-memory metadata write failed during putSnapshotObjects. Snapshot object rows are immutable; choose a new snapshot ID before retrying.",
            operation: "putSnapshotObjects",
          });
        }

        snapshotObjects.set(snapshotId, desired);
      }),
    releaseIdempotentCommit: (commit) =>
      Effect.sync(() => {
        const existing = idempotentCommits.get(commit.idempotencyKey);

        if (
          existing?.workspaceId === commit.workspaceId &&
          existing.runId === commit.runId &&
          existing.snapshotId === commit.snapshotId &&
          existing.manifestDigest === commit.manifestDigest &&
          existing.headGeneration === commit.headGeneration
        ) {
          idempotentCommits.delete(commit.idempotencyKey);
        }
      }),
    rememberIdempotentCommit: (commit) =>
      Effect.sync(() => {
        if (idempotentCommits.has(commit.idempotencyKey)) {
          return false;
        }

        idempotentCommits.set(commit.idempotencyKey, commit);
        return true;
      }),
    rememberWorkspaceTarget: (target) =>
      Effect.sync(() => {
        workspaceTargets.set(target.workspaceId, target);
      }),
  });
};

const queryOne = <Row, A>(
  statement: D1PreparedStatement,
  operation: string,
  map: (row: Row) => A
) =>
  Effect.tryPromise({
    catch: (cause) =>
      new MetadataBackendError({
        cause,
        message: `D1 metadata query failed during ${operation}. Check the D1 binding and migration state before retrying.`,
        operation,
      }),
    try: async () => {
      const row = await statement.first<Row>();
      return row === null ? null : map(row);
    },
  });

const runStatement = (statement: D1PreparedStatement, operation: string) =>
  Effect.tryPromise({
    catch: (cause) =>
      new MetadataBackendError({
        cause,
        message: `D1 metadata write failed during ${operation}. Metadata was not fully persisted; retry after checking the D1 binding and migration state.`,
        operation,
      }),
    try: () => statement.run(),
  });

const changedRows = (result: D1Result<unknown>) => {
  const { meta } = result;
  const changes = "changes" in meta ? meta.changes : undefined;
  const rowsWritten = "rows_written" in meta ? meta.rows_written : undefined;

  return typeof changes === "number" ? changes : (rowsWritten ?? 0);
};

const refFromRow = (row: RefD1Row): RefRow => ({
  generation: Schema.decodeSync(HeadGeneration)(row.generation),
  namespace: row.namespace,
  refName: row.ref_name,
  snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(row.snapshot_id),
  trustClass: Schema.decodeUnknownSync(TrustClassSchema)(row.trust_class),
  updatedAt: row.updated_at,
  updatedByActor: row.updated_by_actor,
  updatedByRunId:
    row.updated_by_run_id === null
      ? null
      : Schema.decodeUnknownSync(RunIdSchema)(row.updated_by_run_id),
});

const snapshotFromRow = (row: SnapshotD1Row): SnapshotHeader => ({
  createdAt: row.created_at,
  manifestDigest: Schema.decodeUnknownSync(Sha256Digest)(row.manifest_digest),
  manifestKey: Schema.decodeUnknownSync(ManifestKey)(row.manifest_key),
  manifestSize: row.manifest_size,
  namespace: row.namespace,
  parentSnapshotId:
    row.parent_snapshot_id === null
      ? null
      : Schema.decodeUnknownSync(SnapshotIdSchema)(row.parent_snapshot_id),
  producerActor: row.producer_actor,
  producerEvent: row.producer_event,
  producerJob: row.producer_job,
  producerRef: row.producer_ref,
  producerRepository: row.producer_repository,
  producerRunId: Schema.decodeUnknownSync(RunIdSchema)(row.producer_run_id),
  producerSha: row.producer_sha,
  producerWorkflow: row.producer_workflow,
  safetyJson: row.safety_json,
  snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(row.snapshot_id),
  statsJson: row.stats_json,
  trustClass: Schema.decodeUnknownSync(TrustClassSchema)(row.trust_class),
  workspaceId: Schema.decodeUnknownSync(WorkspaceIdSchema)(row.workspace_id),
});

const objectFromRow = (row: SnapshotObjectD1Row): SnapshotObjectRow =>
  ({
    ...Schema.decodeUnknownSync(SnapshotObjectInventoryEntrySchema)({
      digest: row.object_digest,
      key: row.object_key,
      kind: row.object_kind,
      size: row.size,
    }),
    snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(row.snapshot_id),
  }) satisfies SnapshotObjectRow;

const workspaceTargetFromRow = (row: WorkspaceTargetD1Row): WorkspaceTarget =>
  ({
    namespace: row.namespace,
    ...(row.prepared_at === null ? {} : { preparedAt: row.prepared_at }),
    ...(row.producer_actor === null
      ? {}
      : { producerActor: row.producer_actor }),
    ...(row.producer_event === null
      ? {}
      : { producerEvent: row.producer_event }),
    ...(row.producer_job === null ? {} : { producerJob: row.producer_job }),
    ...(row.producer_ref === null ? {} : { producerRef: row.producer_ref }),
    ...(row.producer_repository === null
      ? {}
      : { producerRepository: row.producer_repository }),
    ...(row.producer_sha === null ? {} : { producerSha: row.producer_sha }),
    ...(row.producer_workflow === null
      ? {}
      : { producerWorkflow: row.producer_workflow }),
    refName: row.ref_name,
    runId: Schema.decodeUnknownSync(RunIdSchema)(row.run_id),
    trustClass: Schema.decodeUnknownSync(TrustClassSchema)(row.trust_class),
    workspaceId: Schema.decodeUnknownSync(WorkspaceIdSchema)(row.workspace_id),
  }) satisfies WorkspaceTarget;

export const createD1MetadataBackend = (
  db: D1Database
): MetadataBackend["Service"] =>
  MetadataBackend.of({
    appendAuditEvent: (event) =>
      runStatement(
        db
          .prepare(
            "insert into audit_events (id, namespace, ref_name, workspace_id, run_id, snapshot_id, trust_class, event_type, decision, reason, payload_json, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
          )
          .bind(
            event.id ?? crypto.randomUUID(),
            event.namespace,
            event.refName,
            event.workspaceId,
            event.runId,
            event.snapshotId,
            event.trustClass,
            event.eventType,
            event.decision,
            event.reason,
            event.payloadJson,
            event.createdAt
          ),
        "appendAuditEvent"
      ),
    compareAndAdvanceRef: (namespace, refName, expectedGeneration, next) =>
      Effect.gen(function* compareAndAdvanceRefD1Effect() {
        const updatedAt = yield* currentIsoTimestamp;
        const current = yield* queryOne(
          db
            .prepare("select * from refs where namespace = ? and ref_name = ?")
            .bind(namespace, refName),
          "getRefForAdvance",
          refFromRow
        );
        const actualGeneration =
          current?.generation ?? Schema.decodeSync(HeadGeneration)(0);

        if (actualGeneration !== expectedGeneration) {
          return null;
        }

        const advanced = {
          generation: Schema.decodeSync(HeadGeneration)(expectedGeneration + 1),
          namespace,
          refName,
          snapshotId: next.snapshotId,
          trustClass: next.trustClass,
          updatedAt,
          updatedByActor: next.updatedByActor,
          updatedByRunId: next.updatedByRunId,
        } satisfies RefRow;

        const result = yield* runStatement(
          db
            .prepare(
              "insert into refs (namespace, ref_name, snapshot_id, generation, trust_class, updated_at, updated_by_run_id, updated_by_actor) values (?, ?, ?, ?, ?, ?, ?, ?) on conflict(namespace, ref_name) do update set snapshot_id = excluded.snapshot_id, generation = excluded.generation, trust_class = excluded.trust_class, updated_at = excluded.updated_at, updated_by_run_id = excluded.updated_by_run_id, updated_by_actor = excluded.updated_by_actor where refs.generation = ?"
            )
            .bind(
              namespace,
              refName,
              next.snapshotId,
              advanced.generation,
              next.trustClass,
              updatedAt,
              next.updatedByRunId,
              next.updatedByActor,
              expectedGeneration
            ),
          "compareAndAdvanceRef"
        );

        if (changedRows(result) === 0) {
          return null;
        }

        return advanced;
      }),
    getIdempotentCommit: (idempotencyKey) =>
      queryOne(
        db
          .prepare("select * from idempotent_commits where idempotency_key = ?")
          .bind(idempotencyKey),
        "getIdempotentCommit",
        (row: IdempotentCommitD1Row): IdempotentCommit => ({
          createdAt: row.created_at,
          headGeneration: Schema.decodeSync(HeadGeneration)(
            row.head_generation
          ),
          idempotencyKey: Schema.decodeUnknownSync(IdempotencyKeySchema)(
            row.idempotency_key
          ),
          latest: row.latest === 1,
          manifestDigest: Schema.decodeSync(Sha256Digest)(row.manifest_digest),
          result: Schema.decodeUnknownSync(
            Schema.fromJsonString(CommitSaveResponseSchema)
          )(row.result_json),
          runId: Schema.decodeUnknownSync(RunIdSchema)(row.run_id),
          snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(
            row.snapshot_id
          ),
          workspaceId: Schema.decodeUnknownSync(WorkspaceIdSchema)(
            row.workspace_id
          ),
        })
      ),
    getRef: (namespace, refName) =>
      queryOne(
        db
          .prepare("select * from refs where namespace = ? and ref_name = ?")
          .bind(namespace, refName),
        "getRef",
        refFromRow
      ),
    getSnapshotHeader: (snapshotId) =>
      queryOne(
        db
          .prepare("select * from snapshots where snapshot_id = ?")
          .bind(snapshotId),
        "getSnapshotHeader",
        snapshotFromRow
      ),
    getSnapshotObjects: (snapshotId) =>
      Effect.tryPromise({
        catch: (cause) =>
          new MetadataBackendError({
            cause,
            message:
              "D1 metadata query failed during getSnapshotObjects. Restore/commit metadata could not be loaded; check the D1 binding and migration state before retrying.",
            operation: "getSnapshotObjects",
          }),
        try: async () => {
          const result = await db
            .prepare(
              "select * from snapshot_objects where snapshot_id = ? order by object_key"
            )
            .bind(snapshotId)
            .all<SnapshotObjectD1Row>();
          return result.results.map(objectFromRow);
        },
      }),
    getWorkspaceTarget: (workspaceId) =>
      queryOne(
        db
          .prepare("select * from workspace_targets where workspace_id = ?")
          .bind(workspaceId),
        "getWorkspaceTarget",
        workspaceTargetFromRow
      ),
    listAuditEvents: (query = {}) =>
      Effect.tryPromise({
        catch: (cause) =>
          new MetadataBackendError({
            cause,
            message:
              "D1 metadata query failed during listAuditEvents. Audit data could not be loaded; check the D1 binding and migration state before retrying.",
            operation: "listAuditEvents",
          }),
        try: async () => {
          const predicates = [
            query.namespace === undefined
              ? null
              : { sql: "namespace = ?", value: query.namespace },
            query.refName === undefined
              ? null
              : { sql: "ref_name = ?", value: query.refName },
            query.workspaceId === undefined
              ? null
              : { sql: "workspace_id = ?", value: query.workspaceId },
            query.runId === undefined
              ? null
              : { sql: "run_id = ?", value: query.runId },
          ].filter(
            (
              predicate
            ): predicate is { readonly sql: string; readonly value: string } =>
              predicate !== null
          );
          const where =
            predicates.length === 0
              ? ""
              : ` where ${predicates.map((predicate) => predicate.sql).join(" and ")}`;
          const result = await db
            .prepare(
              `select * from audit_events${where} order by created_at, id`
            )
            .bind(...predicates.map((predicate) => predicate.value))
            .all<AuditEventD1Row>();
          return result.results.map(
            (row): AuditEvent => ({
              createdAt: row.created_at,
              decision: Schema.decodeUnknownSync(AuditDecisionSchema)(
                row.decision
              ),
              eventType: Schema.decodeUnknownSync(AuditEventTypeSchema)(
                row.event_type
              ),
              id: row.id,
              namespace: row.namespace,
              payloadJson: row.payload_json,
              reason:
                row.reason === null
                  ? null
                  : Schema.decodeUnknownSync(DenialReasonSchema)(row.reason),
              refName: row.ref_name,
              runId:
                row.run_id === null
                  ? null
                  : Schema.decodeUnknownSync(RunIdSchema)(row.run_id),
              snapshotId:
                row.snapshot_id === null
                  ? null
                  : Schema.decodeUnknownSync(SnapshotIdSchema)(row.snapshot_id),
              trustClass:
                row.trust_class === null
                  ? null
                  : Schema.decodeUnknownSync(TrustClassSchema)(row.trust_class),
              workspaceId:
                row.workspace_id === null
                  ? null
                  : Schema.decodeUnknownSync(WorkspaceIdSchema)(
                      row.workspace_id
                    ),
            })
          );
        },
      }),
    putSnapshotHeader: (header) =>
      Effect.gen(function* putSnapshotHeaderD1Effect() {
        yield* runStatement(
          db
            .prepare(
              "insert or ignore into snapshots (snapshot_id, workspace_id, namespace, parent_snapshot_id, manifest_key, manifest_digest, manifest_size, trust_class, producer_repository, producer_workflow, producer_job, producer_ref, producer_event, producer_sha, producer_actor, producer_run_id, stats_json, safety_json, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(
              header.snapshotId,
              header.workspaceId,
              header.namespace,
              header.parentSnapshotId,
              header.manifestKey,
              header.manifestDigest,
              header.manifestSize,
              header.trustClass,
              header.producerRepository,
              header.producerWorkflow,
              header.producerJob,
              header.producerRef,
              header.producerEvent,
              header.producerSha,
              header.producerActor,
              header.producerRunId,
              header.statsJson,
              header.safetyJson,
              header.createdAt
            ),
          "putSnapshotHeader"
        );

        const stored = yield* queryOne(
          db
            .prepare("select * from snapshots where snapshot_id = ?")
            .bind(header.snapshotId),
          "verifySnapshotHeaderWrite",
          snapshotFromRow
        );

        if (stored === null || !snapshotHeaderMatches(stored, header)) {
          return yield* new MetadataBackendError({
            message:
              "D1 metadata write failed during putSnapshotHeader. Snapshot IDs are immutable; choose a new snapshot ID before retrying.",
            operation: "putSnapshotHeader",
          });
        }
      }),
    putSnapshotObjects: (snapshotId, objects) =>
      Effect.gen(function* putSnapshotObjectsD1Effect() {
        const desired = snapshotRowsFromInventory(snapshotId, objects);

        yield* Effect.forEach(
          desired,
          (object) =>
            runStatement(
              db
                .prepare(
                  "insert or ignore into snapshot_objects (snapshot_id, object_key, object_digest, object_kind, size) values (?, ?, ?, ?, ?)"
                )
                .bind(
                  object.snapshotId,
                  object.key,
                  object.digest,
                  object.kind,
                  object.size
                ),
              "putSnapshotObjects"
            ),
          { discard: true }
        );

        const finalResult = yield* Effect.tryPromise({
          catch: (cause) =>
            new MetadataBackendError({
              cause,
              message:
                "D1 metadata query failed during verifySnapshotObjectsWrite. Snapshot object rows could not be verified; retry after checking the D1 binding and migration state.",
              operation: "verifySnapshotObjectsWrite",
            }),
          try: () =>
            db
              .prepare(
                "select * from snapshot_objects where snapshot_id = ? order by object_key"
              )
              .bind(snapshotId)
              .all<SnapshotObjectD1Row>(),
        });

        if (
          !snapshotObjectsMatch(finalResult.results.map(objectFromRow), desired)
        ) {
          return yield* new MetadataBackendError({
            message:
              "D1 metadata write failed during putSnapshotObjects. Snapshot object rows were not fully persisted; retry after checking the D1 binding and migration state.",
            operation: "putSnapshotObjects",
          });
        }
      }),
    releaseIdempotentCommit: (commit) =>
      runStatement(
        db
          .prepare(
            "delete from idempotent_commits where idempotency_key = ? and workspace_id = ? and run_id = ? and snapshot_id = ? and manifest_digest = ? and head_generation = ?"
          )
          .bind(
            commit.idempotencyKey,
            commit.workspaceId,
            commit.runId,
            commit.snapshotId,
            commit.manifestDigest,
            commit.headGeneration
          ),
        "releaseIdempotentCommit"
      ).pipe(Effect.asVoid),
    rememberIdempotentCommit: (commit) =>
      Effect.map(
        runStatement(
          db
            .prepare(
              "insert or ignore into idempotent_commits (idempotency_key, workspace_id, run_id, snapshot_id, manifest_digest, head_generation, latest, result_json, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(
              commit.idempotencyKey,
              commit.workspaceId,
              commit.runId,
              commit.snapshotId,
              commit.manifestDigest,
              commit.headGeneration,
              commit.latest ? 1 : 0,
              Schema.encodeUnknownSync(
                Schema.fromJsonString(CommitSaveResponseSchema)
              )(commit.result),
              commit.createdAt
            ),
          "rememberIdempotentCommit"
        ),
        (result) => changedRows(result) > 0
      ),
    rememberWorkspaceTarget: (target) =>
      runStatement(
        db
          .prepare(
            "insert into workspace_targets (workspace_id, namespace, ref_name, run_id, trust_class, prepared_at, producer_repository, producer_workflow, producer_job, producer_ref, producer_event, producer_sha, producer_actor) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(workspace_id) do update set namespace = excluded.namespace, ref_name = excluded.ref_name, run_id = excluded.run_id, trust_class = excluded.trust_class, prepared_at = excluded.prepared_at, producer_repository = excluded.producer_repository, producer_workflow = excluded.producer_workflow, producer_job = excluded.producer_job, producer_ref = excluded.producer_ref, producer_event = excluded.producer_event, producer_sha = excluded.producer_sha, producer_actor = excluded.producer_actor"
          )
          .bind(
            target.workspaceId,
            target.namespace,
            target.refName,
            target.runId,
            target.trustClass,
            target.preparedAt ?? null,
            target.producerRepository ?? null,
            target.producerWorkflow ?? null,
            target.producerJob ?? null,
            target.producerRef ?? null,
            target.producerEvent ?? null,
            target.producerSha ?? null,
            target.producerActor ?? null
          ),
        "rememberWorkspaceTarget"
      ),
  });
