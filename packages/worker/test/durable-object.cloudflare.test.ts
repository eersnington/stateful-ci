import {
  HeadGeneration,
  IdempotencyKey,
  ManifestKey,
  RunId,
  Sha256Digest,
  SnapshotId,
  SnapshotObjectInventory,
  WorkspaceId,
} from "@stateful-ci/core";
import { env } from "cloudflare:workers";
import { Effect, Schema } from "effect";
import { describe, expect, it, beforeEach } from "vitest";

import { createDurableObjectSnapshotCoordinator } from "../src/durable-object";
import { createD1MetadataBackend, MetadataBackend } from "../src/metadata";
import type { SnapshotHeader } from "../src/metadata";

const namespace =
  "repo=eersnington/stateful-ci/workflow=ci/job=test/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const refName = "trusted/main/latest";
const target = { namespace, refName } as const;
const otherRefName = "trusted/main/other";
const otherTarget = { namespace, refName: otherRefName } as const;
const runId = Schema.decodeSync(RunId)("123456789");
const workspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${refName}`
);
const otherWorkspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${otherRefName}`
);
const manifestDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
);
const manifestKey = Schema.decodeSync(ManifestKey)(
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
);
const otherDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
);
const otherKey = Schema.decodeSync(ManifestKey)(
  "manifests/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
);

const producer = {
  actor: "eersnington",
  event: "push",
  job: "test",
  ref: "refs/heads/main",
  repository: "eersnington/stateful-ci",
  runId,
  sha: "abc123",
  workflow: "ci.yml",
} as const;

const snapshotHeader = {
  createdAt: "2026-05-22T00:00:00.000Z",
  manifestDigest,
  manifestKey,
  manifestSize: 8,
  namespace,
  parentSnapshotId: null,
  producerActor: producer.actor,
  producerEvent: producer.event,
  producerJob: producer.job,
  producerRef: producer.ref,
  producerRepository: producer.repository,
  producerRunId: runId,
  producerSha: producer.sha,
  producerWorkflow: producer.workflow,
  safetyJson: "{}",
  snapshotId: Schema.decodeSync(SnapshotId)("snap_100"),
  statsJson: "{}",
  trustClass: "trusted",
  workspaceId,
} satisfies SnapshotHeader;

const metadata = () => createD1MetadataBackend(env.STATEFUL_CI_METADATA);
const coordinator = () =>
  createDurableObjectSnapshotCoordinator(env.STATEFUL_CI_COORDINATORS);
const futureExpiresAt = () => new Date(Date.now() + 60_000).toISOString();
const runCoordinator = <A, E>(effect: Effect.Effect<A, E, MetadataBackend>) =>
  Effect.runPromise(
    effect.pipe(Effect.provideService(MetadataBackend, metadata()))
  );

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

const seedRestoreSnapshot = async () => {
  const backend = metadata();
  await Effect.runPromise(
    Effect.gen(function* seedRestoreSnapshotEffect() {
      yield* backend.putSnapshotHeader(snapshotHeader);
      yield* backend.putSnapshotObjects(snapshotHeader.snapshotId, [
        { digest: manifestDigest, key: manifestKey, kind: "manifest", size: 8 },
      ]);
      yield* backend.compareAndAdvanceRef(
        namespace,
        refName,
        Schema.decodeSync(HeadGeneration)(0),
        {
          snapshotId: snapshotHeader.snapshotId,
          trustClass: "trusted",
          updatedByActor: producer.actor,
          updatedByRunId: runId,
        }
      );
    })
  );
};

const prepareSave = () =>
  runCoordinator(
    coordinator().prepareSave({
      expiresAt: futureExpiresAt(),
      producer,
      runId,
      target,
      trustClass: "trusted",
      workspaceId,
    })
  );

const commitInput = (input: {
  readonly digest?: typeof manifestDigest;
  readonly expectedHeadGeneration: number;
  readonly idempotencyKey?: string;
  readonly key?: typeof manifestKey;
  readonly snapshotId: string;
  readonly target?: {
    readonly namespace: typeof namespace;
    readonly refName: string;
  };
  readonly workspaceId?: typeof workspaceId;
}) => ({
  baseSnapshotId: null,
  expectedHeadGeneration: Schema.decodeSync(HeadGeneration)(
    input.expectedHeadGeneration
  ),
  idempotencyKey: Schema.decodeSync(IdempotencyKey)(
    input.idempotencyKey ?? `${input.snapshotId}-save`
  ),
  manifest: {
    digest: input.digest ?? manifestDigest,
    key: input.key ?? manifestKey,
    size: 8,
    snapshotId: Schema.decodeSync(SnapshotId)(input.snapshotId),
  },
  objects: Schema.decodeSync(SnapshotObjectInventory)([
    {
      digest: input.digest ?? manifestDigest,
      key: input.key ?? manifestKey,
      kind: "manifest",
      size: 8,
    },
  ]),
  producer,
  target: input.target ?? target,
  workspaceId: input.workspaceId ?? workspaceId,
});

describe("Durable Object snapshot coordinator", () => {
  beforeEach(cleanD1);

  it("is bound to the exported coordinator class", async () => {
    const id = env.STATEFUL_CI_COORDINATORS.idFromName(
      `${namespace}\n${refName}`
    );
    const response = await env.STATEFUL_CI_COORDINATORS.get(id).fetch(
      "https://stateful-ci.internal/"
    );

    await expect(response.json()).resolves.toStrictEqual({
      message: "Workspace snapshot coordinator accepts POST requests only.",
    });
    expect(response.status).toBe(405);
  });

  it("rejects malformed internal RPC payloads", async () => {
    const id = env.STATEFUL_CI_COORDINATORS.idFromName(
      `${namespace}\n${refName}`
    );
    const invalidJson = await env.STATEFUL_CI_COORDINATORS.get(id).fetch(
      "https://stateful-ci.internal/",
      { body: "not json", method: "POST" }
    );
    const invalidAction = await env.STATEFUL_CI_COORDINATORS.get(id).fetch(
      "https://stateful-ci.internal/",
      {
        body: JSON.stringify({ action: "unknown", input: {} }),
        method: "POST",
      }
    );

    await expect(invalidJson.json()).resolves.toMatchObject({
      message: expect.stringContaining("Invalid coordinator request"),
    });
    await expect(invalidAction.json()).resolves.toMatchObject({
      message: expect.stringContaining("Invalid coordinator request"),
    });
    expect(invalidJson.status).toBe(400);
    expect(invalidAction.status).toBe(400);
  });

  it("selects an allowed restore snapshot and records audit", async () => {
    await seedRestoreSnapshot();

    const restore = await runCoordinator(
      coordinator().authorizeRestore({
        candidates: [target],
        runId,
        target,
        trustClass: "trusted",
        workspaceId,
      })
    );

    expect(restore.decision).toBe("allowed");
    if (restore.decision !== "allowed") {
      throw new Error("expected restore to be allowed");
    }

    await runCoordinator(
      coordinator().recordRestoreAllowed({
        runId,
        snapshotId: restore.snapshot.snapshotId,
        target,
        trustClass: "trusted",
        workspaceId,
      })
    );

    const audit = await Effect.runPromise(
      metadata().listAuditEvents({ workspaceId })
    );
    expect(audit).toMatchObject([
      {
        decision: "allowed",
        eventType: "restore",
        snapshotId: snapshotHeader.snapshotId,
      },
    ]);
  });

  it("serializes concurrent stale commits through one Durable Object", async () => {
    const prepared = await prepareSave();
    expect(prepared.decision).toBe("allowed");

    const first = commitInput({
      expectedHeadGeneration: 0,
      snapshotId: "snap_300",
    });
    const second = commitInput({
      digest: otherDigest,
      expectedHeadGeneration: 0,
      key: otherKey,
      snapshotId: "snap_301",
    });

    const results = await Promise.all([
      runCoordinator(coordinator().commitSave(first)),
      runCoordinator(coordinator().commitSave(second)),
    ]);
    const committed = results.find((result) => result.decision === "committed");
    const conflict = results.find((result) => result.decision === "conflict");
    const ref = await Effect.runPromise(metadata().getRef(namespace, refName));

    expect(committed?.decision).toBe("committed");
    expect(conflict).toMatchObject({
      decision: "conflict",
      reason: "head_generation_mismatch",
    });
    expect(ref?.generation).toBe(1);
    expect(ref?.snapshotId).toBe(
      committed?.decision === "committed" ? committed.snapshotId : undefined
    );
  });

  it("rejects conflicting snapshot id reuse before advancing the ref", async () => {
    const duplicateSnapshotId = Schema.decodeSync(SnapshotId)("snap_350");

    await Effect.runPromise(
      metadata().putSnapshotHeader({
        ...snapshotHeader,
        snapshotId: duplicateSnapshotId,
      })
    );
    await prepareSave();

    const denied = await runCoordinator(
      coordinator().commitSave(
        commitInput({
          expectedHeadGeneration: 0,
          snapshotId: duplicateSnapshotId,
        })
      )
    );
    const ref = await Effect.runPromise(metadata().getRef(namespace, refName));

    expect(denied).toStrictEqual({
      decision: "denied",
      reason: "invalid_protocol_payload",
    });
    expect(ref).toBeNull();
  });

  it("prevents one idempotency key from advancing multiple Durable Objects", async () => {
    const sharedIdempotencyKey = "run-123456789-shared-save";

    await prepareSave();
    await runCoordinator(
      coordinator().prepareSave({
        expiresAt: futureExpiresAt(),
        producer,
        runId,
        target: otherTarget,
        trustClass: "trusted",
        workspaceId: otherWorkspaceId,
      })
    );

    const first = commitInput({
      expectedHeadGeneration: 0,
      idempotencyKey: sharedIdempotencyKey,
      snapshotId: "snap_360",
    });
    const second = commitInput({
      digest: otherDigest,
      expectedHeadGeneration: 0,
      idempotencyKey: sharedIdempotencyKey,
      key: otherKey,
      snapshotId: "snap_361",
      target: otherTarget,
      workspaceId: otherWorkspaceId,
    });

    const results = await Promise.all([
      runCoordinator(coordinator().commitSave(first)),
      runCoordinator(coordinator().commitSave(second)),
    ]);
    const committed = results.filter(
      (result) => result.decision === "committed"
    );
    const denied = results.filter((result) => result.decision === "denied");
    const ref = await Effect.runPromise(metadata().getRef(namespace, refName));
    const otherRef = await Effect.runPromise(
      metadata().getRef(namespace, otherRefName)
    );
    const idempotentCommit = await Effect.runPromise(
      metadata().getIdempotentCommit(
        Schema.decodeSync(IdempotencyKey)(sharedIdempotencyKey)
      )
    );
    const snapshotRows = await env.STATEFUL_CI_METADATA.prepare(
      "select snapshot_id from snapshots where snapshot_id in (?, ?)"
    )
      .bind("snap_360", "snap_361")
      .all<Record<string, unknown>>();

    expect(committed).toHaveLength(1);
    expect(denied).toStrictEqual([
      { decision: "denied", reason: "idempotency_conflict" },
    ]);
    expect([ref, otherRef].filter((row) => row !== null)).toHaveLength(1);
    expect(idempotentCommit?.snapshotId).toBe(committed[0]?.snapshotId);
    expect(snapshotRows.results).toHaveLength(1);
  });

  it("replays identical idempotency keys and rejects conflicting reuse", async () => {
    await prepareSave();
    const input = commitInput({
      expectedHeadGeneration: 0,
      idempotencyKey: "run-123456789-save",
      snapshotId: "snap_400",
    });

    const first = await runCoordinator(coordinator().commitSave(input));
    const replay = await runCoordinator(coordinator().commitSave(input));
    const conflict = await runCoordinator(
      coordinator().commitSave({
        ...input,
        manifest: {
          digest: otherDigest,
          key: otherKey,
          size: 8,
          snapshotId: Schema.decodeSync(SnapshotId)("snap_401"),
        },
        objects: Schema.decodeSync(SnapshotObjectInventory)([
          { digest: otherDigest, key: otherKey, kind: "manifest", size: 8 },
        ]),
      })
    );
    const ref = await Effect.runPromise(metadata().getRef(namespace, refName));

    expect(first.decision).toBe("committed");
    expect(replay.decision).toBe("idempotent");
    expect(conflict).toStrictEqual({
      decision: "denied",
      reason: "idempotency_conflict",
    });
    expect(ref?.generation).toBe(1);
  });

  it("records denied restore audit for unknown trust", async () => {
    await seedRestoreSnapshot();

    const restore = await runCoordinator(
      coordinator().authorizeRestore({
        candidates: [target],
        runId,
        target,
        trustClass: "unknown",
        workspaceId,
      })
    );
    const audit = await Effect.runPromise(
      metadata().listAuditEvents({ workspaceId })
    );

    expect(restore).toStrictEqual({
      decision: "denied",
      reason: "unable_to_classify_run_context",
      saveTarget: null,
      snapshotId: null,
    });
    expect(audit).toMatchObject([
      {
        decision: "denied",
        eventType: "restore",
        reason: "unable_to_classify_run_context",
        snapshotId: null,
      },
    ]);
  });

  it("persists prepare target and committed metadata rows", async () => {
    await prepareSave();

    const committed = await runCoordinator(
      coordinator().commitSave(
        commitInput({ expectedHeadGeneration: 0, snapshotId: "snap_450" })
      )
    );
    const targetRow = await Effect.runPromise(
      metadata().getWorkspaceTarget(workspaceId)
    );
    const header = await Effect.runPromise(
      metadata().getSnapshotHeader(Schema.decodeSync(SnapshotId)("snap_450"))
    );
    const objects = await Effect.runPromise(
      metadata().getSnapshotObjects(Schema.decodeSync(SnapshotId)("snap_450"))
    );
    const idempotentCommit = await Effect.runPromise(
      metadata().getIdempotentCommit(
        Schema.decodeSync(IdempotencyKey)("snap_450-save")
      )
    );
    const audit = await Effect.runPromise(
      metadata().listAuditEvents({ workspaceId })
    );

    expect({
      audit,
      decision: committed.decision,
      idempotentSnapshotId: idempotentCommit?.snapshotId,
      objectCount: objects.length,
      snapshotId: header?.snapshotId,
      targetRow,
    }).toMatchObject({
      audit: [
        {
          decision: "committed",
          eventType: "commit",
          snapshotId: "snap_450",
        },
      ],
      decision: "committed",
      idempotentSnapshotId: "snap_450",
      objectCount: 1,
      snapshotId: "snap_450",
      targetRow: { namespace, refName, runId, workspaceId },
    });
  });

  it("denies base snapshot mismatch without advancing the ref", async () => {
    await seedRestoreSnapshot();
    await prepareSave();

    const denied = await runCoordinator(
      coordinator().commitSave({
        ...commitInput({ expectedHeadGeneration: 1, snapshotId: "snap_500" }),
        baseSnapshotId: Schema.decodeSync(SnapshotId)("snap_missing"),
      })
    );
    const ref = await Effect.runPromise(metadata().getRef(namespace, refName));

    expect(denied).toStrictEqual({
      decision: "denied",
      reason: "save_run_context_mismatch",
    });
    expect(ref?.snapshotId).toBe(snapshotHeader.snapshotId);
    expect(ref?.generation).toBe(1);
  });
});
