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
  readonly expiresAt?: string;
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

const snapshotRowsFromInventory = (
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
        if (snapshots.has(header.snapshotId)) {
          return yield* Effect.fail(
            new MetadataBackendError({
              message:
                "In-memory metadata write failed during putSnapshotHeader. Snapshot IDs are immutable; choose a new snapshot ID before retrying.",
              operation: "putSnapshotHeader",
            })
          );
        }

        snapshots.set(header.snapshotId, header);
      }),
    putSnapshotObjects: (snapshotId, objects) =>
      Effect.gen(function* putSnapshotObjectsInMemoryEffect() {
        if ((snapshotObjects.get(snapshotId)?.length ?? 0) > 0) {
          return yield* Effect.fail(
            new MetadataBackendError({
              message:
                "In-memory metadata write failed during putSnapshotObjects. Snapshot object rows are immutable; choose a new snapshot ID before retrying.",
              operation: "putSnapshotObjects",
            })
          );
        }

        snapshotObjects.set(
          snapshotId,
          snapshotRowsFromInventory(snapshotId, objects)
        );
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

const queryOne = <A>(
  statement: D1PreparedStatement,
  operation: string,
  map: (row: Record<string, unknown>) => A
) =>
  Effect.tryPromise({
    catch: (cause) =>
      new MetadataBackendError({
        cause,
        message: `D1 metadata query failed during ${operation}. Check the D1 binding and migration state before retrying.`,
        operation,
      }),
    try: async () => {
      const row = await statement.first<Record<string, unknown>>();
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

const stringColumn = (row: Record<string, unknown>, column: string) => {
  const value = row[column];

  if (typeof value !== "string") {
    throw new TypeError(
      `Invalid D1 metadata row: expected string column "${column}", got ${typeof value}. Check the D1 migration and stored metadata before retrying.`
    );
  }

  return value;
};

const numberColumn = (row: Record<string, unknown>, column: string) => {
  const value = row[column];

  if (typeof value !== "number" || Number.isNaN(value)) {
    throw new TypeError(
      `Invalid D1 metadata row: expected number column "${column}", got ${typeof value}. Check the D1 migration and stored metadata before retrying.`
    );
  }

  return value;
};

const nullableStringColumn = (row: Record<string, unknown>, column: string) => {
  const value = row[column];
  return typeof value === "string" ? value : null;
};

const nullableDecodedColumn = <S extends Schema.Decoder<unknown>>(
  schema: S,
  row: Record<string, unknown>,
  column: string
) => {
  const value = nullableStringColumn(row, column);

  return value === null ? null : Schema.decodeUnknownSync(schema)(value);
};

const refFromRow = (row: Record<string, unknown>): RefRow => ({
  generation: Schema.decodeSync(HeadGeneration)(
    numberColumn(row, "generation")
  ),
  namespace: stringColumn(row, "namespace"),
  refName: stringColumn(row, "ref_name"),
  snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(
    stringColumn(row, "snapshot_id")
  ),
  trustClass: Schema.decodeUnknownSync(TrustClassSchema)(
    stringColumn(row, "trust_class")
  ),
  updatedAt: stringColumn(row, "updated_at"),
  updatedByActor: nullableStringColumn(row, "updated_by_actor"),
  updatedByRunId: nullableDecodedColumn(RunIdSchema, row, "updated_by_run_id"),
});

const snapshotFromRow = (row: Record<string, unknown>): SnapshotHeader => ({
  createdAt: stringColumn(row, "created_at"),
  manifestDigest: Schema.decodeUnknownSync(Sha256Digest)(
    stringColumn(row, "manifest_digest")
  ),
  manifestKey: Schema.decodeUnknownSync(ManifestKey)(
    stringColumn(row, "manifest_key")
  ),
  manifestSize: numberColumn(row, "manifest_size"),
  namespace: stringColumn(row, "namespace"),
  parentSnapshotId: nullableDecodedColumn(
    SnapshotIdSchema,
    row,
    "parent_snapshot_id"
  ),
  producerActor: stringColumn(row, "producer_actor"),
  producerEvent: stringColumn(row, "producer_event"),
  producerJob: stringColumn(row, "producer_job"),
  producerRef: stringColumn(row, "producer_ref"),
  producerRepository: stringColumn(row, "producer_repository"),
  producerRunId: Schema.decodeUnknownSync(RunIdSchema)(
    stringColumn(row, "producer_run_id")
  ),
  producerSha: stringColumn(row, "producer_sha"),
  producerWorkflow: stringColumn(row, "producer_workflow"),
  safetyJson: stringColumn(row, "safety_json"),
  snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(
    stringColumn(row, "snapshot_id")
  ),
  statsJson: stringColumn(row, "stats_json"),
  trustClass: Schema.decodeUnknownSync(TrustClassSchema)(
    stringColumn(row, "trust_class")
  ),
  workspaceId: Schema.decodeUnknownSync(WorkspaceIdSchema)(
    stringColumn(row, "workspace_id")
  ),
});

const objectFromRow = (row: Record<string, unknown>): SnapshotObjectRow =>
  ({
    ...Schema.decodeUnknownSync(SnapshotObjectInventoryEntrySchema)({
      digest: stringColumn(row, "object_digest"),
      key: stringColumn(row, "object_key"),
      kind: stringColumn(row, "object_kind"),
      size: numberColumn(row, "size"),
    }),
    snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(
      stringColumn(row, "snapshot_id")
    ),
  }) satisfies SnapshotObjectRow;

const optionalStringField = <Key extends string>(
  key: Key,
  value: string | null
) => (value === null ? {} : { [key]: value });

const workspaceTargetFromRow = (
  row: Record<string, unknown>
): WorkspaceTarget =>
  ({
    namespace: stringColumn(row, "namespace"),
    ...optionalStringField(
      "expiresAt",
      nullableStringColumn(row, "expires_at")
    ),
    ...optionalStringField(
      "producerActor",
      nullableStringColumn(row, "producer_actor")
    ),
    ...optionalStringField(
      "producerEvent",
      nullableStringColumn(row, "producer_event")
    ),
    ...optionalStringField(
      "producerJob",
      nullableStringColumn(row, "producer_job")
    ),
    ...optionalStringField(
      "producerRef",
      nullableStringColumn(row, "producer_ref")
    ),
    ...optionalStringField(
      "producerRepository",
      nullableStringColumn(row, "producer_repository")
    ),
    ...optionalStringField(
      "producerSha",
      nullableStringColumn(row, "producer_sha")
    ),
    ...optionalStringField(
      "producerWorkflow",
      nullableStringColumn(row, "producer_workflow")
    ),
    refName: stringColumn(row, "ref_name"),
    runId: Schema.decodeUnknownSync(RunIdSchema)(stringColumn(row, "run_id")),
    trustClass: Schema.decodeUnknownSync(TrustClassSchema)(
      stringColumn(row, "trust_class")
    ),
    workspaceId: Schema.decodeUnknownSync(WorkspaceIdSchema)(
      stringColumn(row, "workspace_id")
    ),
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
        (row): IdempotentCommit => ({
          createdAt: stringColumn(row, "created_at"),
          headGeneration: Schema.decodeSync(HeadGeneration)(
            numberColumn(row, "head_generation")
          ),
          idempotencyKey: Schema.decodeUnknownSync(IdempotencyKeySchema)(
            stringColumn(row, "idempotency_key")
          ),
          latest: numberColumn(row, "latest") === 1,
          manifestDigest: Schema.decodeSync(Sha256Digest)(
            stringColumn(row, "manifest_digest")
          ),
          result: Schema.decodeUnknownSync(
            Schema.fromJsonString(CommitSaveResponseSchema)
          )(stringColumn(row, "result_json")),
          runId: Schema.decodeUnknownSync(RunIdSchema)(
            stringColumn(row, "run_id")
          ),
          snapshotId: Schema.decodeUnknownSync(SnapshotIdSchema)(
            stringColumn(row, "snapshot_id")
          ),
          workspaceId: Schema.decodeUnknownSync(WorkspaceIdSchema)(
            stringColumn(row, "workspace_id")
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
            .all<Record<string, unknown>>();
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
            .all<Record<string, unknown>>();
          return result.results.map(
            (row): AuditEvent => ({
              createdAt: stringColumn(row, "created_at"),
              decision: Schema.decodeUnknownSync(AuditDecisionSchema)(
                stringColumn(row, "decision")
              ),
              eventType: Schema.decodeUnknownSync(AuditEventTypeSchema)(
                stringColumn(row, "event_type")
              ),
              id: stringColumn(row, "id"),
              namespace: stringColumn(row, "namespace"),
              payloadJson: nullableStringColumn(row, "payload_json"),
              reason: nullableDecodedColumn(DenialReasonSchema, row, "reason"),
              refName: stringColumn(row, "ref_name"),
              runId: nullableDecodedColumn(RunIdSchema, row, "run_id"),
              snapshotId: nullableDecodedColumn(
                SnapshotIdSchema,
                row,
                "snapshot_id"
              ),
              trustClass: nullableDecodedColumn(
                TrustClassSchema,
                row,
                "trust_class"
              ),
              workspaceId: nullableDecodedColumn(
                WorkspaceIdSchema,
                row,
                "workspace_id"
              ),
            })
          );
        },
      }),
    putSnapshotHeader: (header) =>
      runStatement(
        db
          .prepare(
            "insert into snapshots (snapshot_id, workspace_id, namespace, parent_snapshot_id, manifest_key, manifest_digest, manifest_size, trust_class, producer_repository, producer_workflow, producer_job, producer_ref, producer_event, producer_sha, producer_actor, producer_run_id, stats_json, safety_json, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
      ),
    putSnapshotObjects: (snapshotId, objects) =>
      Effect.gen(function* putSnapshotObjectsD1Effect() {
        const existing = yield* queryOne(
          db
            .prepare(
              "select object_key from snapshot_objects where snapshot_id = ? limit 1"
            )
            .bind(snapshotId),
          "getSnapshotObjectsForWrite",
          (row) => stringColumn(row, "object_key")
        );

        if (existing !== null) {
          return yield* Effect.fail(
            new MetadataBackendError({
              message:
                "D1 metadata write failed during putSnapshotObjects. Snapshot object rows are immutable; choose a new snapshot ID before retrying.",
              operation: "putSnapshotObjects",
            })
          );
        }

        yield* Effect.forEach(
          snapshotRowsFromInventory(snapshotId, objects),
          (object) =>
            runStatement(
              db
                .prepare(
                  "insert into snapshot_objects (snapshot_id, object_key, object_digest, object_kind, size) values (?, ?, ?, ?, ?)"
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
              JSON.stringify(commit.result),
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
            "insert into workspace_targets (workspace_id, namespace, ref_name, run_id, trust_class, expires_at, producer_repository, producer_workflow, producer_job, producer_ref, producer_event, producer_sha, producer_actor) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(workspace_id) do update set namespace = excluded.namespace, ref_name = excluded.ref_name, run_id = excluded.run_id, trust_class = excluded.trust_class, expires_at = excluded.expires_at, producer_repository = excluded.producer_repository, producer_workflow = excluded.producer_workflow, producer_job = excluded.producer_job, producer_ref = excluded.producer_ref, producer_event = excluded.producer_event, producer_sha = excluded.producer_sha, producer_actor = excluded.producer_actor"
          )
          .bind(
            target.workspaceId,
            target.namespace,
            target.refName,
            target.runId,
            target.trustClass,
            target.expiresAt ?? null,
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
