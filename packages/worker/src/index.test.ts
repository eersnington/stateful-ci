import {
  ManifestKey,
  RestoreAllowedResponse,
  RestoreDeniedResponse,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";
import { describe, expect, test } from "vitest";

import worker, { handleFetch, maxProtocolBodyBytes } from "./index";
import { createInMemoryMetadataBackend } from "./metadata";
import type { RefRow, SnapshotHeader } from "./metadata";

const env = {
  STATEFUL_CI_API_TOKEN: "test-token",
};

const restoreRequest = {
  client: {
    configHash: "sha256:config",
    version: "0.1.0",
  },
  git: {
    baseRef: null,
    headRef: null,
    headRepo: null,
    ref: "refs/heads/main",
    sha: "abc123",
  },
  github: {
    actor: "eersnington",
    event: "push",
    runId: "123456789",
  },
  workspace: {
    job: "test",
    repo: "eersnington/stateful-ci",
    workflow: "ci.yml",
  },
};

const saveRequest = {
  baseSnapshotId: "snap_123",
  manifest: {
    chunkCount: 1,
    fileCount: 21_903,
    hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    id: "snap_124",
    key: "manifests/snap_124.json",
    safety: {
      skippedByBuiltInDenylist: 3,
      skippedByUserExclude: 12,
      skippedUnsupportedType: 1,
    },
    totalBytes: 481_203_912,
  },
  runId: "123456789",
  workspaceId: "ws_123",
};

const seededNamespace =
  "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=sha256:config";
const seededRefName = "trusted/main/latest";
const seededWorkspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${seededNamespace}:${seededRefName}`
);
const seededSnapshotId = Schema.decodeSync(SnapshotId)("snap_123");

const seededSnapshot = {
  chunkCount: 1,
  createdAt: "2026-05-16T00:00:00.000Z",
  manifestDigest: Schema.decodeSync(Sha256Digest)(
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  ),
  manifestKey: Schema.decodeSync(ManifestKey)("manifests/snap_123.json"),
  parentSnapshotId: null,
  runId: Schema.decodeSync(RunId)("123456788"),
  snapshotId: seededSnapshotId,
  totalBytes: 128,
  trustClass: "trusted",
  workspaceId: seededWorkspaceId,
} satisfies SnapshotHeader;

const seededRef = {
  namespace: seededNamespace,
  refName: seededRefName,
  snapshotId: seededSnapshotId,
  trustClass: "trusted",
  updatedAt: "2026-05-16T00:00:00.000Z",
  version: 1,
} satisfies RefRow;

const workspaceIdFor = (namespace: string, refName: string) =>
  Schema.decodeSync(WorkspaceId)(`ws:${namespace}:${refName}`);

const snapshotFor = (
  snapshotId: string,
  trustClass: SnapshotHeader["trustClass"],
  workspaceId: WorkspaceId
) =>
  ({
    ...seededSnapshot,
    snapshotId: Schema.decodeSync(SnapshotId)(snapshotId),
    trustClass,
    workspaceId,
  }) satisfies SnapshotHeader;

const refFor = (
  refName: string,
  snapshotId: string,
  trustClass: RefRow["trustClass"]
) =>
  ({
    ...seededRef,
    refName,
    snapshotId: Schema.decodeSync(SnapshotId)(snapshotId),
    trustClass,
  }) satisfies RefRow;

const internalPullRequest = {
  ...restoreRequest,
  git: {
    ...restoreRequest.git,
    baseRef: "main",
    headRef: "feature",
    headRepo: "eersnington/stateful-ci",
    ref: "refs/pull/12/merge",
  },
  github: { ...restoreRequest.github, event: "pull_request" },
};

const externalPullRequest = {
  ...internalPullRequest,
  git: { ...internalPullRequest.git, headRepo: "contributor/stateful-ci" },
};

const privilegedRelease = {
  ...restoreRequest,
  git: { ...restoreRequest.git, ref: "refs/tags/v1.0.0" },
  github: { ...restoreRequest.github, event: "release" },
};

const jsonRequest = (path: string, body: unknown) =>
  new Request(`https://stateful-ci.test${path}`, {
    body: JSON.stringify(body),
    headers: {
      authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
      "content-type": "application/json",
    },
    method: "POST",
  });

describe("worker API", () => {
  test("GET /health returns protocol health", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/health"),
      env
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      protocolVersion: 1,
      service: "stateful-ci-worker",
      status: "ok",
    });
  });

  test("POST /v1/restore denies when no compatible ref exists", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/restore", restoreRequest),
      env
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "no_compatible_snapshot",
      save: { allowed: true, target: seededRefName },
      trustClass: "trusted",
      workspaceId: seededWorkspaceId,
    });
  });

  test("POST /v1/restore allows compatible seeded latest snapshot", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      {
        metadata,
      }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "allowed",
      save: { allowed: true, target: seededRefName },
      snapshot: {
        id: "snap_123",
        manifestKey: "manifests/snap_123.json",
        parent: null,
      },
      trustClass: "trusted",
      workspaceId: seededWorkspaceId,
    });
  });

  test("POST /v1/restore lets trusted main seed same-repo pull requests", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", internalPullRequest),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "allowed",
      save: { allowed: true, target: "internal/refs-pull-12-merge/latest" },
      snapshot: { id: "snap_123" },
      trustClass: "internal",
    });
  });

  test("POST /v1/restore lets trusted main seed fork pull requests", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", externalPullRequest),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "allowed",
      save: { allowed: false },
      snapshot: { id: "snap_123" },
      trustClass: "external",
    });
  });

  test("POST /v1/restore lets trusted main seed privileged release runs", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", privilegedRelease),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "allowed",
      save: { allowed: false },
      snapshot: { id: "snap_123" },
      trustClass: "privileged",
    });
  });

  test("POST /v1/restore denies external snapshots renamed into trusted latest", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [refFor(seededRefName, "snap_125", "external")],
      snapshots: [snapshotFor("snap_125", "external", seededWorkspaceId)],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "restore_policy_denied",
      save: { allowed: true, target: seededRefName },
      trustClass: "trusted",
      workspaceId: seededWorkspaceId,
    });
    await expect(
      Effect.runPromise(metadata.listAuditEvents)
    ).resolves.toStrictEqual([
      expect.objectContaining({
        decision: "denied",
        reason: "restore_policy_denied",
        snapshotId: "snap_125",
      }),
    ]);
  });

  test("POST /v1/restore denies internal snapshots retargeted across internal scopes", async () => {
    const featureRefName = "internal/feature/latest";
    const otherRefName = "internal/other/latest";
    const metadata = createInMemoryMetadataBackend({
      refs: [refFor(featureRefName, "snap_128", "internal")],
      snapshots: [
        snapshotFor(
          "snap_128",
          "internal",
          workspaceIdFor(seededNamespace, otherRefName)
        ),
      ],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", {
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/heads/feature" },
      }),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "restore_policy_denied",
      save: { allowed: true, target: featureRefName },
      trustClass: "internal",
      workspaceId: workspaceIdFor(seededNamespace, featureRefName),
    });
  });

  test("POST /v1/restore denies external snapshots retargeted across external scopes", async () => {
    const pr12RefName = "external/refs-pull-12-merge/latest";
    const pr13RefName = "external/refs-pull-13-merge/latest";
    const metadata = createInMemoryMetadataBackend({
      refs: [refFor(pr12RefName, "snap_129", "external")],
      snapshots: [
        snapshotFor(
          "snap_129",
          "external",
          workspaceIdFor(seededNamespace, pr13RefName)
        ),
      ],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", externalPullRequest),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "restore_policy_denied",
      save: { allowed: false },
      trustClass: "external",
    });
  });

  test("POST /v1/restore denies external snapshots in privileged runs", async () => {
    const privilegedRefName = "privileged/v1.0.0/latest";
    const metadata = createInMemoryMetadataBackend({
      refs: [refFor(privilegedRefName, "snap_126", "external")],
      snapshots: [
        snapshotFor(
          "snap_126",
          "external",
          workspaceIdFor(seededNamespace, privilegedRefName)
        ),
      ],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", privilegedRelease),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "restore_policy_denied",
      trustClass: "privileged",
    });
  });

  test("POST /v1/restore denies internal branch snapshots in privileged runs", async () => {
    const privilegedRefName = "privileged/v1.0.0/latest";
    const metadata = createInMemoryMetadataBackend({
      refs: [refFor(privilegedRefName, "snap_127", "internal")],
      snapshots: [
        snapshotFor(
          "snap_127",
          "internal",
          workspaceIdFor(seededNamespace, privilegedRefName)
        ),
      ],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", privilegedRelease),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "restore_policy_denied",
      trustClass: "privileged",
    });
  });

  test("POST /v1/restore denies latest snapshots from a different config hash", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", {
        ...restoreRequest,
        client: {
          ...restoreRequest.client,
          configHash: "sha256:different-config",
        },
      }),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "no_compatible_snapshot",
      save: { allowed: true, target: seededRefName },
      trustClass: "trusted",
      workspaceId: workspaceIdFor(
        "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=sha256:different-config",
        seededRefName
      ),
    });
  });

  test("POST /v1/restore includes derived fork pull request trust class", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/restore", {
        ...restoreRequest,
        git: {
          ...restoreRequest.git,
          baseRef: "main",
          headRef: "feature",
          headRepo: "contributor/stateful-ci",
          ref: "refs/pull/12/merge",
        },
        github: { ...restoreRequest.github, event: "pull_request" },
      }),
      env
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "no_compatible_snapshot",
      save: { allowed: false },
      trustClass: "external",
    });
  });

  test("POST /v1/save validates requests and denies without an allowed workspace target", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/save", saveRequest),
      env
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "restore_required_before_save",
    });
  });

  test("POST /v1/save commits snapshot metadata and advances latest after allowed restore", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      { metadata }
    );
    const restoreBody = Schema.decodeUnknownSync(RestoreAllowedResponse)(
      await restoreResponse.json()
    );

    const response = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        workspaceId: restoreBody.workspaceId,
      }),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "committed",
      latest: true,
      snapshotId: "snap_124",
      workspaceId: seededWorkspaceId,
    });

    await expect(
      Effect.runPromise(
        metadata.getSnapshotHeader(Schema.decodeSync(SnapshotId)("snap_124"))
      )
    ).resolves.toMatchObject({
      manifestKey: "manifests/snap_124.json",
      parentSnapshotId: "snap_123",
      snapshotId: "snap_124",
      workspaceId: seededWorkspaceId,
    });
    await expect(
      Effect.runPromise(metadata.getRef(seededNamespace, seededRefName))
    ).resolves.toMatchObject({
      snapshotId: "snap_124",
      version: 2,
    });
  });

  test("POST /v1/save rejects saves whose run id does not match the restored run", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      { metadata }
    );
    const restoreBody = Schema.decodeUnknownSync(RestoreAllowedResponse)(
      await restoreResponse.json()
    );
    const response = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        runId: "987654321",
        workspaceId: restoreBody.workspaceId,
      }),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "save_run_context_mismatch",
    });
    await expect(
      Effect.runPromise(
        metadata.getSnapshotHeader(Schema.decodeSync(SnapshotId)("snap_124"))
      )
    ).resolves.toBeNull();
  });

  test("POST /v1/restore lets a trusted first run save when no compatible snapshot exists", async () => {
    const metadata = createInMemoryMetadataBackend();
    const response = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    const restoreBody = Schema.decodeUnknownSync(RestoreDeniedResponse)(
      await response.json()
    );

    expect(restoreBody).toStrictEqual({
      decision: "denied",
      reason: "no_compatible_snapshot",
      save: { allowed: true, target: seededRefName },
      trustClass: "trusted",
      workspaceId: seededWorkspaceId,
    });

    const saveResponse = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        baseSnapshotId: null,
        workspaceId: restoreBody.workspaceId,
      }),
      env,
      { metadata }
    );

    await expect(saveResponse.json()).resolves.toStrictEqual({
      decision: "committed",
      latest: true,
      snapshotId: "snap_124",
      workspaceId: seededWorkspaceId,
    });
    await expect(
      Effect.runPromise(
        metadata.getSnapshotHeader(Schema.decodeSync(SnapshotId)("snap_124"))
      )
    ).resolves.toMatchObject({
      parentSnapshotId: null,
      snapshotId: "snap_124",
      workspaceId: seededWorkspaceId,
    });
    await expect(
      Effect.runPromise(metadata.getRef(seededNamespace, seededRefName))
    ).resolves.toMatchObject({
      snapshotId: "snap_124",
      trustClass: "trusted",
    });
  });

  test("POST /v1/restore uses configured trusted refs for seed snapshots", async () => {
    const masterRefName = "trusted/master/latest";
    const masterSnapshotId = "snap_130";
    const metadata = createInMemoryMetadataBackend({
      refs: [refFor(masterRefName, masterSnapshotId, "trusted")],
      snapshots: [
        snapshotFor(
          masterSnapshotId,
          "trusted",
          workspaceIdFor(seededNamespace, masterRefName)
        ),
      ],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", internalPullRequest),
      {
        ...env,
        STATEFUL_CI_TRUSTED_REFS: "refs/heads/master",
      },
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "allowed",
      snapshot: { id: masterSnapshotId },
      trustClass: "internal",
    });
  });

  test("default worker fetches preserve restore targets for the following save request", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      { metadata }
    );
    const restoreBody = Schema.decodeUnknownSync(RestoreAllowedResponse)(
      await restoreResponse.json()
    );
    const response = await worker.fetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        workspaceId: restoreBody.workspaceId,
      }),
      env
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "committed",
      snapshotId: "snap_124",
      workspaceId: restoreBody.workspaceId,
    });
  });

  test("POST /v1/save commits same-repo pull request snapshots to internal refs", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", internalPullRequest),
      env,
      { metadata }
    );
    const restoreBody = Schema.decodeUnknownSync(RestoreAllowedResponse)(
      await restoreResponse.json()
    );
    const response = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        workspaceId: restoreBody.workspaceId,
      }),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "committed",
      snapshotId: "snap_124",
    });
    await expect(
      Effect.runPromise(
        metadata.getRef(seededNamespace, "internal/refs-pull-12-merge/latest")
      )
    ).resolves.toMatchObject({
      snapshotId: "snap_124",
      trustClass: "internal",
    });
  });

  test("POST /v1/save has no remembered target for fork pull requests", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", externalPullRequest),
      env,
      { metadata }
    );
    const restoreBody = Schema.decodeUnknownSync(RestoreAllowedResponse)(
      await restoreResponse.json()
    );
    const response = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        workspaceId: restoreBody.workspaceId,
      }),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "restore_required_before_save",
    });
  });

  test("POST /v1/save has no remembered target for privileged snapshots", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", privilegedRelease),
      env,
      { metadata }
    );
    const restoreBody = Schema.decodeUnknownSync(RestoreAllowedResponse)(
      await restoreResponse.json()
    );
    const response = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        workspaceId: restoreBody.workspaceId,
      }),
      env,
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "restore_required_before_save",
    });
  });

  test("restore and save record audit metadata for allow and deny decisions", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });

    await handleFetch(jsonRequest("/v1/restore", restoreRequest), env, {
      metadata,
    });
    await handleFetch(jsonRequest("/v1/save", saveRequest), env, { metadata });

    const auditEvents = await Effect.runPromise(metadata.listAuditEvents);

    expect(auditEvents).toHaveLength(2);
    expect(
      auditEvents.map(({ decision, eventType, reason }) => ({
        decision,
        eventType,
        reason,
      }))
    ).toStrictEqual([
      { decision: "allowed", eventType: "restore", reason: null },
      {
        decision: "denied",
        eventType: "save",
        reason: "restore_required_before_save",
      },
    ]);
  });

  test("invalid JSON returns structured 400", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore", {
        body: "{not-json",
        headers: {
          authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
          "content-type": "application/json",
        },
        method: "POST",
      }),
      env
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "InvalidJsonBody",
    });
  });

  test("schema-invalid JSON returns structured 400", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/restore", { ...restoreRequest, github: {} }),
      env
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "InvalidProtocolPayload",
    });
  });

  test("oversized JSON returns structured 413 before schema validation", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore", {
        body: JSON.stringify({ payload: "a".repeat(maxProtocolBodyBytes) }),
        headers: {
          authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
          "content-type": "application/json",
        },
        method: "POST",
      }),
      env
    );

    expect(response.status).toBe(413);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "RequestBodyTooLarge",
      limitBytes: maxProtocolBodyBytes,
    });
  });

  test("unknown routes return structured 404", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/missing"),
      env
    );

    expect(response.status).toBe(404);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "RouteNotFound",
    });
  });

  test("wrong methods return structured 405", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore"),
      env
    );

    expect(response.status).toBe(405);
    expect(response.headers.get("Allow")).toBe("POST");
    await expect(response.json()).resolves.toMatchObject({
      _tag: "MethodNotAllowed",
      allowed: ["POST"],
    });
  });

  test("restore without an authorization token returns structured 401", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore", {
        body: JSON.stringify(restoreRequest),
        headers: { "content-type": "application/json" },
        method: "POST",
      }),
      env
    );

    expect(response.status).toBe(401);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "Unauthorized",
    });
  });

  test("restore with the wrong authorization token returns structured 403", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore", {
        body: JSON.stringify(restoreRequest),
        headers: {
          authorization: "Bearer wrong-token",
          "content-type": "application/json",
        },
        method: "POST",
      }),
      env
    );

    expect(response.status).toBe(403);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "Forbidden",
    });
  });
});
