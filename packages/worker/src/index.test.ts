import { describe, expect, test } from "vitest";

import worker, { maxProtocolBodyBytes } from "./index";

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
    },
    totalBytes: 481_203_912,
  },
  runId: "123456789",
  workspaceId: "ws_123",
};

const jsonRequest = (path: string, body: unknown) =>
  new Request(`https://stateful-ci.test${path}`, {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
    method: "POST",
  });

describe("worker API", () => {
  test("GET /health returns protocol health", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/health")
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      protocolVersion: 1,
      service: "stateful-ci-worker",
      status: "ok",
    });
  });

  test("POST /v1/restore validates requests and denies until policy exists", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/restore", restoreRequest)
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "backend_policy_not_configured",
      save: { allowed: false },
      trustClass: "unknown",
    });
  });

  test("POST /v1/save validates requests and denies until policy exists", async () => {
    const response = await worker.fetch(jsonRequest("/v1/save", saveRequest));

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "backend_policy_not_configured",
    });
  });

  test("invalid JSON returns structured 400", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore", {
        body: "{not-json",
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "InvalidJsonBody",
    });
  });

  test("schema-invalid JSON returns structured 400", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/restore", { ...restoreRequest, github: {} })
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
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(response.status).toBe(413);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "RequestBodyTooLarge",
      limitBytes: maxProtocolBodyBytes,
    });
  });

  test("unknown routes return structured 404", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/missing")
    );

    expect(response.status).toBe(404);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "RouteNotFound",
    });
  });

  test("wrong methods return structured 405", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore")
    );

    expect(response.status).toBe(405);
    expect(response.headers.get("Allow")).toBe("POST");
    await expect(response.json()).resolves.toMatchObject({
      _tag: "MethodNotAllowed",
      allowed: ["POST"],
    });
  });
});
