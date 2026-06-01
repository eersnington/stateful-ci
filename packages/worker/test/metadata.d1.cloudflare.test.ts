import {
  HeadGeneration,
  IdempotencyKey,
  ChunkKey,
  ManifestKey,
  PackKey,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import { env } from "cloudflare:workers";
import { Effect, Schema } from "effect";
import { beforeEach, describe, expect, it } from "vitest";

import { createD1MetadataBackend } from "../src/metadata";
import type {
  AuditEvent,
  IdempotentCommit,
  SnapshotHeader,
} from "../src/metadata";

const namespace =
  "repo=eersnington/stateful-ci/workflow=ci/job=test/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const refName = "trusted/main/latest";
const runId = Schema.decodeSync(RunId)("123456789");
const workspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${refName}`
);
const snapshotId = Schema.decodeSync(SnapshotId)("snap_200");
const nextSnapshotId = Schema.decodeSync(SnapshotId)("snap_201");
const manifestDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
);
const packDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
);
const chunkDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
);
const manifestKey = Schema.decodeSync(ManifestKey)(
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
);
const packKey = Schema.decodeSync(PackKey)(
  "packs/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.scipack"
);
const chunkKey = Schema.decodeSync(ChunkKey)(
  "chunks/sha256/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
);

const snapshotHeader = {
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
  snapshotId,
  statsJson: "{}",
  trustClass: "trusted",
  workspaceId,
} satisfies SnapshotHeader;

const cleanD1 = async () => {
  await env.STATEFUL_CI_METADATA.prepare("delete from audit_events").run();
  await env.STATEFUL_CI_METADATA.prepare(
    "delete from idempotent_commits"
  ).run();
  await env.STATEFUL_CI_METADATA.prepare("delete from workspace_targets").run();
  await env.STATEFUL_CI_METADATA.prepare("delete from snapshot_objects").run();
  await env.STATEFUL_CI_METADATA.prepare("delete from refs").run();
  await env.STATEFUL_CI_METADATA.prepare("delete from snapshots").run();
};

describe("D1 metadata backend contract", () => {
  beforeEach(cleanD1);

  it("covers refs read/write/CAS", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);

    await expect(
      Effect.runPromise(metadata.getRef(namespace, refName))
    ).resolves.toBeNull();

    const first = await Effect.runPromise(
      metadata.compareAndAdvanceRef(
        namespace,
        refName,
        Schema.decodeSync(HeadGeneration)(0),
        {
          snapshotId,
          trustClass: "trusted",
          updatedByActor: "eersnington",
          updatedByRunId: runId,
        }
      )
    );
    const stale = await Effect.runPromise(
      metadata.compareAndAdvanceRef(
        namespace,
        refName,
        Schema.decodeSync(HeadGeneration)(0),
        {
          snapshotId: nextSnapshotId,
          trustClass: "trusted",
          updatedByActor: "eersnington",
          updatedByRunId: runId,
        }
      )
    );
    const second = await Effect.runPromise(
      metadata.compareAndAdvanceRef(
        namespace,
        refName,
        Schema.decodeSync(HeadGeneration)(1),
        {
          snapshotId: nextSnapshotId,
          trustClass: "trusted",
          updatedByActor: "eersnington",
          updatedByRunId: runId,
        }
      )
    );
    const persisted = await Effect.runPromise(
      metadata.getRef(namespace, refName)
    );

    expect(first?.generation).toBe(1);
    expect(stale).toBeNull();
    expect(second?.generation).toBe(2);
    expect(persisted?.snapshotId).toBe(nextSnapshotId);
  });

  it("covers snapshot headers and restart persistence", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);
    const restarted = createD1MetadataBackend(env.STATEFUL_CI_METADATA);

    await Effect.runPromise(metadata.putSnapshotHeader(snapshotHeader));
    await Effect.runPromise(metadata.putSnapshotHeader(snapshotHeader));

    await expect(
      Effect.runPromise(restarted.getSnapshotHeader(snapshotId))
    ).resolves.toStrictEqual(snapshotHeader);
  });

  it("rejects conflicting snapshot header rewrites", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);

    await Effect.runPromise(metadata.putSnapshotHeader(snapshotHeader));

    await expect(
      Effect.runPromise(
        metadata.putSnapshotHeader({
          ...snapshotHeader,
          manifestDigest: packDigest,
          manifestKey: Schema.decodeSync(ManifestKey)(
            "manifests/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
          ),
          manifestSize: 16,
        })
      )
    ).rejects.toMatchObject({ _tag: "MetadataBackendError" });
    await expect(
      Effect.runPromise(metadata.getSnapshotHeader(snapshotId))
    ).resolves.toStrictEqual(snapshotHeader);
  });

  it("covers snapshot object reachability without per-file rows", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);
    const objects = [
      { digest: manifestDigest, key: manifestKey, kind: "manifest", size: 8 },
      {
        digest: packDigest,
        key: packKey,
        kind: "pack",
        size: 16,
      },
      {
        digest: chunkDigest,
        key: chunkKey,
        kind: "chunk",
        size: 32,
      },
    ] as const;

    await Effect.runPromise(metadata.putSnapshotObjects(snapshotId, objects));
    await Effect.runPromise(metadata.putSnapshotObjects(snapshotId, objects));

    const rows = await Effect.runPromise(
      metadata.getSnapshotObjects(snapshotId)
    );
    const raw = await env.STATEFUL_CI_METADATA.prepare(
      "select * from snapshot_objects where snapshot_id = ?"
    )
      .bind(snapshotId)
      .all<Record<string, unknown>>();

    expect(rows).toHaveLength(3);
    expect(raw.results).toHaveLength(3);
    expect(Object.keys(raw.results[0] ?? {}).toSorted()).toStrictEqual([
      "object_digest",
      "object_key",
      "object_kind",
      "size",
      "snapshot_id",
    ]);
  });

  it("completes snapshot object writes after partial retry state", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);
    const manifestOnly = [
      { digest: manifestDigest, key: manifestKey, kind: "manifest", size: 8 },
    ] as const;
    const full = [
      ...manifestOnly,
      { digest: packDigest, key: packKey, kind: "pack", size: 16 },
    ] as const;

    await Effect.runPromise(
      metadata.putSnapshotObjects(snapshotId, manifestOnly)
    );
    await Effect.runPromise(metadata.putSnapshotObjects(snapshotId, full));

    await expect(
      Effect.runPromise(metadata.getSnapshotObjects(snapshotId))
    ).resolves.toStrictEqual([
      {
        digest: manifestDigest,
        key: manifestKey,
        kind: "manifest",
        size: 8,
        snapshotId,
      },
      {
        digest: packDigest,
        key: packKey,
        kind: "pack",
        size: 16,
        snapshotId,
      },
    ]);
  });

  it("rejects conflicting snapshot object rewrites", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);
    const original = [
      { digest: manifestDigest, key: manifestKey, kind: "manifest", size: 8 },
    ] as const;

    await Effect.runPromise(metadata.putSnapshotObjects(snapshotId, original));

    await expect(
      Effect.runPromise(
        metadata.putSnapshotObjects(snapshotId, [
          { digest: packDigest, key: manifestKey, kind: "manifest", size: 16 },
        ])
      )
    ).rejects.toMatchObject({ _tag: "MetadataBackendError" });
    await expect(
      Effect.runPromise(metadata.getSnapshotObjects(snapshotId))
    ).resolves.toStrictEqual([
      {
        digest: manifestDigest,
        key: manifestKey,
        kind: "manifest",
        size: 8,
        snapshotId,
      },
    ]);
  });

  it("covers workspace target upsert/read", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);

    await Effect.runPromise(
      metadata.rememberWorkspaceTarget({
        namespace,
        preparedAt: "2026-05-22T01:00:00.000Z",
        producerActor: "eersnington",
        producerEvent: "push",
        producerJob: "test",
        producerRef: "refs/heads/main",
        producerRepository: "eersnington/stateful-ci",
        producerSha: "abc123",
        producerWorkflow: "ci.yml",
        refName,
        runId,
        trustClass: "trusted",
        workspaceId,
      })
    );
    await Effect.runPromise(
      metadata.rememberWorkspaceTarget({
        namespace,
        preparedAt: "2026-05-22T02:00:00.000Z",
        refName,
        runId,
        trustClass: "trusted",
        workspaceId,
      })
    );

    await expect(
      Effect.runPromise(metadata.getWorkspaceTarget(workspaceId))
    ).resolves.toMatchObject({
      namespace,
      preparedAt: "2026-05-22T02:00:00.000Z",
      refName,
      runId,
      trustClass: "trusted",
      workspaceId,
    });
  });

  it("covers idempotent commit write/read without overwrite", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);
    const commit = {
      createdAt: "2026-05-22T00:00:00.000Z",
      headGeneration: Schema.decodeSync(HeadGeneration)(1),
      idempotencyKey: Schema.decodeSync(IdempotencyKey)("run-123456789-save"),
      latest: true,
      manifestDigest,
      result: {
        decision: "committed",
        headGeneration: Schema.decodeSync(HeadGeneration)(1),
        snapshotId,
        workspaceId,
      },
      runId,
      snapshotId,
      workspaceId,
    } satisfies IdempotentCommit;

    await expect(
      Effect.runPromise(metadata.rememberIdempotentCommit(commit))
    ).resolves.toBeTruthy();
    await expect(
      Effect.runPromise(
        metadata.rememberIdempotentCommit({
          ...commit,
          result: {
            decision: "committed",
            headGeneration: Schema.decodeSync(HeadGeneration)(2),
            snapshotId: nextSnapshotId,
            workspaceId,
          },
          snapshotId: nextSnapshotId,
        })
      )
    ).resolves.toBeFalsy();

    await expect(
      Effect.runPromise(metadata.getIdempotentCommit(commit.idempotencyKey))
    ).resolves.toStrictEqual(commit);
  });

  it("covers audit append/list/filter", async () => {
    const metadata = createD1MetadataBackend(env.STATEFUL_CI_METADATA);
    const event = {
      createdAt: "2026-05-22T00:00:00.000Z",
      decision: "committed",
      eventType: "commit",
      id: "audit-1",
      namespace,
      payloadJson: null,
      reason: null,
      refName,
      runId,
      snapshotId,
      trustClass: "trusted",
      workspaceId,
    } satisfies AuditEvent;
    const other = {
      ...event,
      id: "audit-2",
      namespace: `${namespace}/other`,
      workspaceId: Schema.decodeSync(WorkspaceId)(
        `ws:${namespace}/other:${refName}`
      ),
    } satisfies AuditEvent;

    await Effect.runPromise(metadata.appendAuditEvent(event));
    await Effect.runPromise(metadata.appendAuditEvent(other));

    await expect(
      Effect.runPromise(metadata.listAuditEvents())
    ).resolves.toHaveLength(2);
    await expect(
      Effect.runPromise(metadata.listAuditEvents({ namespace }))
    ).resolves.toStrictEqual([event]);
    await expect(
      Effect.runPromise(metadata.listAuditEvents({ refName }))
    ).resolves.toStrictEqual([event, other]);
    await expect(
      Effect.runPromise(metadata.listAuditEvents({ workspaceId }))
    ).resolves.toStrictEqual([event]);
    const byRun = await Effect.runPromise(metadata.listAuditEvents({ runId }));
    expect({ byRun, byRunLength: byRun.length }).toStrictEqual({
      byRun: expect.arrayContaining([event, other]),
      byRunLength: 2,
    });
  });
});
