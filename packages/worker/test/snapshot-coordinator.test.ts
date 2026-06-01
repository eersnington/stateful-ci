import { assert, describe, it } from "@effect/vitest";
import {
  ChunkKey,
  HeadGeneration,
  IdempotencyKey,
  ManifestKey,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import {
  createInMemoryMetadataBackend,
  MetadataBackend,
} from "../src/metadata";
import type { RefRow, SnapshotHeader } from "../src/metadata";
import { MetadataBackendError } from "../src/metadata-backend-error";
import { createMetadataSnapshotCoordinator } from "../src/snapshot-coordinator";

const namespace =
  "repo=eersnington/stateful-ci/workflow=ci/job=test/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const refName = "trusted/main/latest";
const workspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${refName}`
);
const runId = Schema.decodeSync(RunId)("123456789");
const preparedAt = "2026-05-22T01:00:00.000Z";
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

const retryHeader = {
  ...committedHeader,
  statsJson: JSON.stringify({
    chunkCount: 0,
    objectCount: 1,
    totalObjectBytes: 8,
  }),
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
  namespace,
  preparedAt,
  refName,
  runId,
  trustClass: "trusted",
  workspaceId,
} as const;

const internalRefName = "internal/feature/latest";
const internalWorkspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${internalRefName}`
);

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

  it.effect("advances internal heads only inside their isolated target", () =>
    Effect.gen(function* coordinatorAdvancesInternalHeadEffect() {
      const metadata = createInMemoryMetadataBackend({
        workspaceTargets: [
          {
            ...testWorkspaceTarget,
            refName: internalRefName,
            trustClass: "internal",
            workspaceId: internalWorkspaceId,
          },
        ],
      });
      const coordinator = createMetadataSnapshotCoordinator();
      const result = yield* coordinator
        .commitSave({
          ...commitInput,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)(
            "run-123456789-save-internal"
          ),
          producer: {
            ...commitInput.producer,
            ref: "refs/heads/feature",
          },
          target: { namespace, refName: internalRefName },
          workspaceId: internalWorkspaceId,
        })
        .pipe(Effect.provideService(MetadataBackend, metadata));
      const trustedRef = yield* metadata.getRef(namespace, refName);
      const internalRef = yield* metadata.getRef(namespace, internalRefName);

      assert.strictEqual(result.decision, "committed");
      assert.isNull(trustedRef);
      assert.strictEqual(
        internalRef?.snapshotId,
        commitInput.manifest.snapshotId
      );
      assert.strictEqual(internalRef?.trustClass, "internal");
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
          snapshotObjects: [
            {
              digest: manifestDigest,
              key: manifestKey,
              kind: "manifest",
              size: 8,
              snapshotId: commitInput.manifest.snapshotId,
            },
          ],
          snapshots: [retryHeader],
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
    "rejects idempotent replay when persisted objects do not match the request",
    () =>
      Effect.gen(
        function* coordinatorRejectsMismatchedIdempotentReplayEffect() {
          const chunkDigest = Schema.decodeSync(Sha256Digest)(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
          );
          const chunkKey = Schema.decodeSync(ChunkKey)(
            "chunks/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
          );
          const metadata = createInMemoryMetadataBackend({
            workspaceTargets: [testWorkspaceTarget],
          });
          const coordinator = createMetadataSnapshotCoordinator();

          yield* coordinator
            .commitSave(commitInput)
            .pipe(Effect.provideService(MetadataBackend, metadata));

          const replay = yield* coordinator
            .commitSave({
              ...commitInput,
              objects: [
                ...commitInput.objects,
                { digest: chunkDigest, key: chunkKey, kind: "chunk", size: 4 },
              ],
            })
            .pipe(Effect.provideService(MetadataBackend, metadata));

          assert.deepStrictEqual(replay, {
            decision: "denied",
            reason: "idempotency_conflict",
          });
        }
      )
  );

  it.effect(
    "does not persist idempotency before snapshot metadata succeeds",
    () =>
      Effect.gen(function* coordinatorDoesNotPoisonIdempotencyEffect() {
        const backing = createInMemoryMetadataBackend({
          workspaceTargets: [testWorkspaceTarget],
        });
        let failHeader = true;
        const metadata = MetadataBackend.of({
          ...backing,
          putSnapshotHeader: (header) => {
            if (failHeader) {
              failHeader = false;
              return Effect.fail(
                new MetadataBackendError({
                  message:
                    "Injected metadata failure during putSnapshotHeader.",
                  operation: "putSnapshotHeader",
                })
              );
            }

            return backing.putSnapshotHeader(header);
          },
        });
        const coordinator = createMetadataSnapshotCoordinator();
        const first = yield* Effect.exit(
          coordinator
            .commitSave(commitInput)
            .pipe(Effect.provideService(MetadataBackend, metadata))
        );
        const claimed = yield* metadata.getIdempotentCommit(
          commitInput.idempotencyKey
        );
        const retry = yield* coordinator
          .commitSave(commitInput)
          .pipe(Effect.provideService(MetadataBackend, metadata));

        assert.isTrue(Exit.isFailure(first));
        assert.strictEqual(claimed, null);
        assert.strictEqual(retry.decision, "committed");
      })
  );

  it.effect("completes a retry after snapshot header was already written", () =>
    Effect.gen(function* coordinatorRetriesAfterHeaderWriteEffect() {
      const metadata = createInMemoryMetadataBackend({
        snapshots: [retryHeader],
        workspaceTargets: [testWorkspaceTarget],
      });
      const coordinator = createMetadataSnapshotCoordinator();
      const result = yield* coordinator
        .commitSave(commitInput)
        .pipe(Effect.provideService(MetadataBackend, metadata));
      const ref = yield* metadata.getRef(namespace, refName);

      assert.strictEqual(result.decision, "committed");
      assert.strictEqual(ref?.snapshotId, commitInput.manifest.snapshotId);
    })
  );

  it.effect(
    "completes a retry after partial snapshot object rows were written",
    () =>
      Effect.gen(function* coordinatorRetriesAfterPartialObjectsEffect() {
        const chunkDigest = Schema.decodeSync(Sha256Digest)(
          "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        const chunkKey = Schema.decodeSync(ManifestKey)(
          "manifests/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
        );
        const input = {
          ...commitInput,
          objects: [
            ...commitInput.objects,
            { digest: chunkDigest, key: chunkKey, kind: "manifest", size: 4 },
          ],
        } as const;
        const metadata = createInMemoryMetadataBackend({
          snapshotObjects: [
            {
              digest: manifestDigest,
              key: manifestKey,
              kind: "manifest",
              size: 8,
              snapshotId: commitInput.manifest.snapshotId,
            },
          ],
          snapshots: [
            {
              ...retryHeader,
              statsJson: JSON.stringify({
                chunkCount: 0,
                objectCount: 2,
                totalObjectBytes: 12,
              }),
            },
          ],
          workspaceTargets: [testWorkspaceTarget],
        });
        const coordinator = createMetadataSnapshotCoordinator();
        const result = yield* coordinator
          .commitSave(input)
          .pipe(Effect.provideService(MetadataBackend, metadata));
        const objects = yield* metadata.getSnapshotObjects(
          commitInput.manifest.snapshotId
        );

        assert.strictEqual(result.decision, "committed");
        assert.strictEqual(objects.length, 2);
      })
  );

  it.effect(
    "does not replay idempotent when ref CAS fails after metadata writes",
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
