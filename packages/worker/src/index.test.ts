import {
  ManifestKey,
  RestoreAllowedResponse,
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
      save: { allowed: false },
      trustClass: "trusted",
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
      save: { allowed: false },
      trustClass: "trusted",
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
