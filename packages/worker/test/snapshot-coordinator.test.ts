import { assert, describe, it } from "@effect/vitest";
import {
  HeadGeneration,
  IdempotencyKey,
  ManifestKey,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import {
  createInMemoryMetadataBackend,
  MetadataBackend,
} from "../src/metadata";
import type { RefRow, SnapshotHeader } from "../src/metadata";
import { createMetadataSnapshotCoordinator } from "../src/snapshot-coordinator";

const namespace =
  "repo=eersnington/stateful-ci/workflow=ci/job=test/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const refName = "trusted/main/latest";
const workspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${refName}`
);
const runId = Schema.decodeSync(RunId)("123456789");
const expiresAt = "2026-05-22T01:00:00.000Z";
const manifestDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
);
const manifestKey = Schema.decodeSync(ManifestKey)(
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
);

const commitInput = {
  baseSnapshotId: null,
  expectedHeadGeneration: Schema.decodeSync(HeadGeneration)(0),
  idempotencyKey: Schema.decodeSync(IdempotencyKey)("run-123456789-save"),
  manifest: {
    digest: manifestDigest,
    key: manifestKey,
    size: 8,
    snapshotId: Schema.decodeSync(SnapshotId)("snap_300"),
  },
  objects: [
    { digest: manifestDigest, key: manifestKey, kind: "manifest", size: 8 },
  ],
  producer: {
    actor: "eersnington",
    event: "push",
    job: "test",
    ref: "refs/heads/main",
    repository: "eersnington/stateful-ci",
    runId,
    sha: "abc123",
    workflow: "ci.yml",
  },
  target: { namespace, refName },
  workspaceId,
} as const;

const committedHeader = {
  createdAt: "2026-05-22T00:00:00.000Z",
  manifestDigest,
  manifestKey,
  manifestSize: 8,
  namespace,
  parentSnapshotId: null,
  producerActor: "eersnington",
  producerEvent: "push",
  producerJob: "test",
  producerRef: "refs/heads/main",
  producerRepository: "eersnington/stateful-ci",
  producerRunId: runId,
  producerSha: "abc123",
  producerWorkflow: "ci.yml",
  safetyJson: "{}",
  snapshotId: commitInput.manifest.snapshotId,
  statsJson: "{}",
  trustClass: "trusted",
  workspaceId,
} satisfies SnapshotHeader;

const refFor = (snapshotId: SnapshotId) =>
  ({
    generation: Schema.decodeSync(HeadGeneration)(1),
    namespace,
    refName,
    snapshotId,
    trustClass: "trusted",
    updatedAt: "2026-05-22T00:00:00.000Z",
    updatedByActor: "eersnington",
    updatedByRunId: runId,
  }) satisfies RefRow;

const testWorkspaceTarget = {
  expiresAt,
  namespace,
  refName,
  runId,
  trustClass: "trusted",
  workspaceId,
} as const;

describe("snapshot coordinator", () => {
  it.effect("commits once and replays identical idempotency keys", () =>
    Effect.gen(function* coordinatorReplaysIdempotencyEffect() {
      const metadata = createInMemoryMetadataBackend({
        workspaceTargets: [testWorkspaceTarget],
      });
      const coordinator = createMetadataSnapshotCoordinator();
      const first = yield* coordinator
        .commitSave(commitInput)
        .pipe(Effect.provideService(MetadataBackend, metadata));
      const replay = yield* coordinator
        .commitSave(commitInput)
        .pipe(Effect.provideService(MetadataBackend, metadata));

      assert.strictEqual(first.decision, "committed");
      assert.deepStrictEqual(replay, {
        decision: "idempotent",
        headGeneration: Schema.decodeSync(HeadGeneration)(1),
        snapshotId: commitInput.manifest.snapshotId,
        workspaceId,
      });
    })
  );

  it.effect("rejects conflicting idempotency key reuse", () =>
    Effect.gen(function* coordinatorRejectsIdempotencyConflictEffect() {
      const metadata = createInMemoryMetadataBackend({
        workspaceTargets: [testWorkspaceTarget],
      });
      const coordinator = createMetadataSnapshotCoordinator();
      yield* coordinator
        .commitSave(commitInput)
        .pipe(Effect.provideService(MetadataBackend, metadata));
      const conflict = yield* coordinator
        .commitSave({
          ...commitInput,
          manifest: {
            ...commitInput.manifest,
            snapshotId: Schema.decodeSync(SnapshotId)("snap_301"),
          },
        })
        .pipe(Effect.provideService(MetadataBackend, metadata));

      assert.deepStrictEqual(conflict, {
        decision: "denied",
        reason: "idempotency_conflict",
      });
    })
  );

  it.effect("rejects unresolved producer placeholders", () =>
    Effect.gen(function* coordinatorRejectsProducerPlaceholdersEffect() {
      const metadata = createInMemoryMetadataBackend({
        workspaceTargets: [testWorkspaceTarget],
      });
      const coordinator = createMetadataSnapshotCoordinator();
      const denied = yield* coordinator
        .commitSave({
          ...commitInput,
          producer: {
            ...commitInput.producer,
            actor: "unknown",
            event: "unknown",
            sha: "unknown",
          },
        })
        .pipe(Effect.provideService(MetadataBackend, metadata));

      assert.deepStrictEqual(denied, {
        decision: "denied",
        reason: "invalid_protocol_payload",
      });
    })
  );

  it.effect(
    "returns conflict for stale generation against a different head",
    () =>
      Effect.gen(function* coordinatorRejectsStaleGenerationEffect() {
        const otherSnapshotId = Schema.decodeSync(SnapshotId)("snap_302");
        const metadata = createInMemoryMetadataBackend({
          refs: [refFor(otherSnapshotId)],
          snapshots: [{ ...committedHeader, snapshotId: otherSnapshotId }],
          workspaceTargets: [testWorkspaceTarget],
        });
        const coordinator = createMetadataSnapshotCoordinator();
        const conflict = yield* coordinator
          .commitSave(commitInput)
          .pipe(Effect.provideService(MetadataBackend, metadata));

        assert.deepStrictEqual(conflict, {
          actualHeadGeneration: Schema.decodeSync(HeadGeneration)(1),
          decision: "conflict",
          reason: "head_generation_mismatch",
        });
      })
  );

  it.effect(
    "recovers idempotent retry when head advanced before idempotency persisted",
    () =>
      Effect.gen(function* coordinatorRecoversMissingIdempotencyEffect() {
        const metadata = createInMemoryMetadataBackend({
          refs: [refFor(commitInput.manifest.snapshotId)],
          snapshots: [committedHeader],
          workspaceTargets: [testWorkspaceTarget],
        });
        const coordinator = createMetadataSnapshotCoordinator();
        const recovered = yield* coordinator
          .commitSave(commitInput)
          .pipe(Effect.provideService(MetadataBackend, metadata));
        const replay = yield* coordinator
          .commitSave(commitInput)
          .pipe(Effect.provideService(MetadataBackend, metadata));

        assert.deepStrictEqual(recovered, {
          decision: "idempotent",
          headGeneration: Schema.decodeSync(HeadGeneration)(1),
          snapshotId: commitInput.manifest.snapshotId,
          workspaceId,
        });
        assert.deepStrictEqual(replay, recovered);
      })
  );

  it.effect(
    "does not replay idempotent when ref CAS fails after idempotency claim",
    () =>
      Effect.gen(function* coordinatorRejectsStaleIdempotencyEffect() {
        const backing = createInMemoryMetadataBackend({
          workspaceTargets: [testWorkspaceTarget],
        });
        const metadata = MetadataBackend.of({
          ...backing,
          compareAndAdvanceRef: () => Effect.succeed(null),
        });
        const coordinator = createMetadataSnapshotCoordinator();
        const first = yield* coordinator
          .commitSave(commitInput)
          .pipe(Effect.provideService(MetadataBackend, metadata));
        const claimed = yield* metadata.getIdempotentCommit(
          commitInput.idempotencyKey
        );
        const replay = yield* coordinator
          .commitSave(commitInput)
          .pipe(Effect.provideService(MetadataBackend, metadata));

        assert.deepStrictEqual(first, {
          actualHeadGeneration: Schema.decodeSync(HeadGeneration)(0),
          decision: "conflict",
          reason: "head_generation_mismatch",
        });
        assert.strictEqual(claimed, null);
        assert.notStrictEqual(replay.decision, "idempotent");
      })
  );
});
