import {
  HeadGeneration,
  IdempotencyKey,
  ManifestKey,
  PackKey,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
  routes,
} from "@stateful-ci/core";
import { env } from "cloudflare:workers";
import { Effect, Schema } from "effect";
import { beforeEach, describe, expect, it } from "vitest";

import worker, { handleFetch } from "../src/index";
import { createD1MetadataBackend } from "../src/metadata";

const namespace =
  "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const refName = "trusted/main/latest";
const runId = Schema.decodeSync(RunId)("123456789");
const workspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${refName}`
);
const manifestDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
);
const manifestKey = Schema.decodeSync(ManifestKey)(
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
);
const packDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
);
const packKey = Schema.decodeSync(PackKey)(
  "packs/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.scipack"
);
const snapshotId = Schema.decodeSync(SnapshotId)("snap_900");
const manifestBytes = new TextEncoder().encode("manifest");
const packBytes = new TextEncoder().encode("pack");

const metadata = () => createD1MetadataBackend(env.STATEFUL_CI_METADATA);

const authHeaders = {
  authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
  "content-type": "application/json",
};

const jsonRequest = (path: string, body: unknown) =>
  new Request(`https://stateful-ci.test${path}`, {
    body: JSON.stringify(body),
    headers: authHeaders,
    method: "POST",
  });

const prepareRequest = {
  client: {
    configHash:
      "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
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
    runId,
  },
  idempotencyKey: Schema.decodeSync(IdempotencyKey)("run-123456789-save"),
  identity: {
    provider: "github-actions",
    token: "oidc.jwt.token",
  },
  manifest: {
    digest: manifestDigest,
    key: manifestKey,
    size: manifestBytes.byteLength,
    snapshotId,
  },
  objects: [
    {
      digest: manifestDigest,
      key: manifestKey,
      kind: "manifest",
      size: manifestBytes.byteLength,
    },
    {
      digest: packDigest,
      key: packKey,
      kind: "pack",
      size: packBytes.byteLength,
    },
  ],
  protocolVersion: 1,
  workspace: {
    job: "test",
    repo: "eersnington/stateful-ci",
    workflow: "ci.yml",
  },
} as const;

const cleanD1 = async () => {
  await env.STATEFUL_CI_METADATA.prepare("delete from audit_events").run();
  await env.STATEFUL_CI_METADATA.prepare(
    "delete from idempotent_commits"
  ).run();
  await env.STATEFUL_CI_METADATA.prepare("delete from workspace_targets").run();
  await env.STATEFUL_CI_METADATA.prepare("delete from snapshot_objects").run();
  await env.STATEFUL_CI_METADATA.prepare("delete from refs").run();
  await env.STATEFUL_CI_METADATA.prepare("delete from snapshots").run();
  await env.STATEFUL_CI_OBJECTS.delete(manifestKey);
  await env.STATEFUL_CI_OBJECTS.delete(packKey);
};

describe("Worker Cloudflare runtime bindings", () => {
  beforeEach(cleanD1);

  it("uses the Durable Object coordinator path for prepare and commit", async () => {
    await env.STATEFUL_CI_OBJECTS.put(manifestKey, manifestBytes);
    await env.STATEFUL_CI_OBJECTS.put(packKey, packBytes);

    const prepare = await worker.fetch(
      jsonRequest(routes.prepareSave.path, prepareRequest),
      env
    );
    const prepareBody = await prepare.json<{
      expectedHeadGeneration: number;
      workspaceId: string;
    }>();

    expect(prepare.status).toBe(200);
    expect(prepareBody.expectedHeadGeneration).toBe(0);
    expect(prepareBody.workspaceId).toBe(workspaceId);

    const commit = await worker.fetch(
      jsonRequest(routes.commitSave.path, {
        baseSnapshotId: null,
        expectedHeadGeneration: Schema.decodeSync(HeadGeneration)(0),
        idempotencyKey: prepareRequest.idempotencyKey,
        manifest: prepareRequest.manifest,
        objects: prepareRequest.objects,
        protocolVersion: 1,
        runId,
        target: { namespace, refName },
        workspaceId,
      }),
      env
    );
    const commitBody = await commit.json<{ decision: string }>();
    const ref = await Effect.runPromise(metadata().getRef(namespace, refName));
    const header = await Effect.runPromise(
      metadata().getSnapshotHeader(snapshotId)
    );
    const objects = await Effect.runPromise(
      metadata().getSnapshotObjects(snapshotId)
    );

    expect(commit.status).toBe(200);
    expect({
      decision: commitBody.decision,
      objectCount: objects.length,
      refSnapshotId: ref?.snapshotId,
      snapshotId: header?.snapshotId,
    }).toStrictEqual({
      decision: "committed",
      objectCount: 2,
      refSnapshotId: snapshotId,
      snapshotId,
    });
  });

  it("rejects commit before direct head mutation when R2 object is missing", async () => {
    await env.STATEFUL_CI_OBJECTS.put(manifestKey, manifestBytes);

    const prepare = await worker.fetch(
      jsonRequest(routes.prepareSave.path, prepareRequest),
      env
    );
    expect(prepare.status).toBe(200);

    const commit = await worker.fetch(
      jsonRequest(routes.commitSave.path, {
        baseSnapshotId: null,
        expectedHeadGeneration: Schema.decodeSync(HeadGeneration)(0),
        idempotencyKey: prepareRequest.idempotencyKey,
        manifest: prepareRequest.manifest,
        objects: prepareRequest.objects,
        protocolVersion: 1,
        runId,
        target: { namespace, refName },
        workspaceId,
      }),
      env
    );
    const commitBody = await commit.json<{
      decision: string;
      reason: string;
    }>();
    const ref = await Effect.runPromise(metadata().getRef(namespace, refName));

    expect(commit.status).toBe(200);
    expect(commitBody).toStrictEqual({
      decision: "denied",
      reason: "snapshot_object_missing",
    });
    expect(ref).toBeNull();
  });

  it("fails closed when D1 metadata is configured without a Durable Object binding", async () => {
    const response = await handleFetch(
      jsonRequest(routes.prepareSave.path, prepareRequest),
      {
        STATEFUL_CI_API_TOKEN: env.STATEFUL_CI_API_TOKEN,
        STATEFUL_CI_METADATA: env.STATEFUL_CI_METADATA,
        STATEFUL_CI_OBJECTS: env.STATEFUL_CI_OBJECTS,
      }
    );
    const body = await response.json<{ message: string }>();

    expect(response.status).toBe(500);
    expect(body.message).toContain(
      "D1 metadata configured without a Durable Object coordinator binding"
    );
  });
});
