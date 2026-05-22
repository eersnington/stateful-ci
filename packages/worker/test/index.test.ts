import { assert, describe, expect, it } from "@effect/vitest";
import {
  ChunkKey,
  ManifestKey,
  PackKey,
  RestoreAllowedResponse,
  PrepareSaveAllowedResponse,
  CommitSaveResponse,
  HeadGeneration,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import type { SnapshotObjectKey } from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import { BlobStore } from "../src/blob-store";
import { BlobStoreError } from "../src/blob-store-error";
import { createInMemoryBlobStore } from "../src/blob-store-memory";
import worker, { handleFetch, maxProtocolBodyBytes } from "../src/index";
import { createInMemoryMetadataBackend } from "../src/metadata";
import type { RefRow, SnapshotHeader } from "../src/metadata";

const env = {
  STATEFUL_CI_API_TOKEN: "test-token",
};

const restoreRequest = {
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
    runId: "123456789",
  },
  identity: {
    provider: "github-actions",
    token: "oidc.jwt.token",
  },
  managedRoots: [".turbo"],
  protocolVersion: 1,
  workspace: {
    job: "test",
    repo: "eersnington/stateful-ci",
    workflow: "ci.yml",
  },
};

const manifestDigest =
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const manifestKey =
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json";
const seededManifestDigest =
  "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f";
const seededManifestKey =
  "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json";
const seededPackDigest =
  "sha256:4862f447f2c7f272fa2f4aaf89dadb3b1ac09105bd5864f8d1a0c9452bb0a226";
const seededPackKey =
  "packs/sha256/4862f447f2c7f272fa2f4aaf89dadb3b1ac09105bd5864f8d1a0c9452bb0a226.scipack";
const seededChunkDigest =
  "sha256:6c87f68371b28954707ebb92afee7ccffb74c6f71ec8fea8a98cf6104289585b";
const seededChunkKey =
  "chunks/sha256/6c87f68371b28954707ebb92afee7ccffb74c6f71ec8fea8a98cf6104289585b";
const seededManifestBytes = new TextEncoder().encode("manifest");
const seededPackBytes = new TextEncoder().encode("pack");
const seededChunkBytes = new TextEncoder().encode("chunk");
const seededWorkspaceTotalBytes = 481_203_912;
const saveObjects = [
  {
    digest: manifestDigest,
    key: manifestKey,
    kind: "manifest",
    size: 512,
  },
] as const;

const saveRequest = {
  baseSnapshotId: "snap_123",
  manifest: {
    chunkCount: 1,
    fileCount: 21_903,
    hash: manifestDigest,
    id: "snap_124",
    key: manifestKey,
    objects: saveObjects,
    safety: {
      skippedByBuiltInDenylist: 3,
      skippedByUserExclude: 12,
      skippedUnsupportedType: 1,
    },
    totalBytes: seededWorkspaceTotalBytes,
  },
  protocolVersion: 1,
  runId: "123456789",
  workspaceId: "ws_123",
};

const seededNamespace =
  "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const seededRefName = "trusted/main/latest";
const seededWorkspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${seededNamespace}:${seededRefName}`
);
const seededSnapshotId = Schema.decodeSync(SnapshotId)("snap_123");

const seededSnapshot = {
  chunkCount: 1,
  createdAt: "2026-05-16T00:00:00.000Z",
  manifestDigest: Schema.decodeSync(Sha256Digest)(seededManifestDigest),
  manifestKey: Schema.decodeSync(ManifestKey)(seededManifestKey),
  manifestSize: seededManifestBytes.byteLength,
  namespace: seededNamespace,
  objects: [
    {
      digest: Schema.decodeSync(Sha256Digest)(seededManifestDigest),
      key: Schema.decodeSync(ManifestKey)(seededManifestKey),
      kind: "manifest",
      size: seededManifestBytes.byteLength,
    },
    {
      digest: Schema.decodeSync(Sha256Digest)(seededPackDigest),
      key: Schema.decodeSync(PackKey)(seededPackKey),
      kind: "pack",
      size: seededPackBytes.byteLength,
    },
    {
      digest: Schema.decodeSync(Sha256Digest)(seededChunkDigest),
      key: Schema.decodeSync(ChunkKey)(seededChunkKey),
      kind: "chunk",
      size: seededChunkBytes.byteLength,
    },
  ],
  parentSnapshotId: null,
  producerActor: "eersnington",
  producerEvent: "push",
  producerJob: "test",
  producerRef: "refs/heads/main",
  producerRepository: "eersnington/stateful-ci",
  producerRunId: Schema.decodeSync(RunId)("123456788"),
  producerSha: "abc123",
  producerWorkflow: "ci.yml",
  runId: Schema.decodeSync(RunId)("123456788"),
  safetyJson: "{}",
  snapshotId: seededSnapshotId,
  statsJson: "{}",
  totalBytes: seededWorkspaceTotalBytes,
  trustClass: "trusted",
  workspaceId: seededWorkspaceId,
} satisfies SnapshotHeader;

const seededRef = {
  generation: Schema.decodeSync(HeadGeneration)(1),
  namespace: seededNamespace,
  refName: seededRefName,
  snapshotId: seededSnapshotId,
  trustClass: "trusted",
  updatedAt: "2026-05-16T00:00:00.000Z",
  updatedByActor: "eersnington",
  updatedByRunId: Schema.decodeSync(RunId)("123456788"),
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

const seededBlobStore = () =>
  createInMemoryBlobStore(
    new Map<SnapshotObjectKey, Uint8Array>([
      [Schema.decodeSync(ManifestKey)(seededManifestKey), seededManifestBytes],
      [Schema.decodeSync(PackKey)(seededPackKey), seededPackBytes],
      [Schema.decodeSync(ChunkKey)(seededChunkKey), seededChunkBytes],
    ])
  );

const failingHeadBlobStore = () => {
  const backing = createInMemoryBlobStore();

  return BlobStore.of({
    get: (key) => backing.get(key),
    getRange: (key, offset, length) => backing.getRange(key, offset, length),
    head: (key) =>
      Effect.fail(
        new BlobStoreError({
          key,
          message: `Could not inspect snapshot object ${key} in test storage.`,
          reason: "io_failed",
        })
      ),
    presignGet: (key, ttlSeconds) => backing.presignGet(key, ttlSeconds),
    presignPut: (key, ttlSeconds, constraints) =>
      backing.presignPut(key, ttlSeconds, constraints),
    putIfAbsent: (input) => backing.putIfAbsent(input),
  });
};

describe("worker API", () => {
  it("GET /health returns protocol health", async () => {
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

  it.effect("PUT /v1/objects stores and serves immutable objects", () =>
    Effect.gen(function* objectRouteStoresAndServesEffect() {
      const blobStore = createInMemoryBlobStore();
      const bytes = new TextEncoder().encode("manifest");
      const route = `/v1/objects/${seededManifestKey}`;
      const put = () =>
        handleFetch(
          new Request(`https://stateful-ci.test${route}`, {
            body: bytes,
            headers: {
              authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
              "content-length": String(bytes.byteLength),
              "x-stateful-ci-object-digest": seededManifestDigest,
              "x-stateful-ci-object-kind": "manifest",
              "x-stateful-ci-object-size": String(bytes.byteLength),
            },
            method: "PUT",
          }),
          env,
          { blobStore }
        );

      const firstPut = yield* Effect.promise(put);
      const secondPut = yield* Effect.promise(put);
      const head = yield* Effect.promise(() =>
        handleFetch(
          new Request(`https://stateful-ci.test${route}`, {
            headers: { authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}` },
            method: "HEAD",
          }),
          env,
          { blobStore }
        )
      );
      const get = yield* Effect.promise(() =>
        handleFetch(
          new Request(`https://stateful-ci.test${route}`, {
            headers: { authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}` },
            method: "GET",
          }),
          env,
          { blobStore }
        )
      );

      assert.strictEqual(firstPut.status, 204);
      assert.strictEqual(secondPut.status, 204);
      assert.strictEqual(head.status, 200);
      assert.strictEqual(
        head.headers.get("content-length"),
        String(bytes.byteLength)
      );
      assert.strictEqual(get.status, 200);
      assert.deepStrictEqual(
        new Uint8Array(yield* Effect.promise(() => get.arrayBuffer())),
        bytes
      );
    })
  );

  it("PUT /v1/objects rejects non-canonical keys and digest mismatches", async () => {
    const malformed = await handleFetch(
      new Request("https://stateful-ci.test/v1/objects/chunks/sha256/ABC", {
        body: "bad",
        headers: { authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}` },
        method: "PUT",
      }),
      env,
      { blobStore: createInMemoryBlobStore() }
    );
    const mismatch = await handleFetch(
      new Request(`https://stateful-ci.test/v1/objects/${seededManifestKey}`, {
        body: "wrong",
        headers: {
          authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
          "content-length": "5",
          "x-stateful-ci-object-digest": seededManifestDigest,
          "x-stateful-ci-object-kind": "manifest",
          "x-stateful-ci-object-size": "5",
        },
        method: "PUT",
      }),
      env,
      { blobStore: createInMemoryBlobStore() }
    );

    expect(malformed.status).toBe(400);
    expect(mismatch.status).toBe(400);
    await expect(mismatch.json()).resolves.toMatchObject({
      reason: "digest_mismatch",
    });
  });

  it("object routes fail closed without an R2 binding or injected blob store", async () => {
    const response = await worker.fetch(
      new Request(`https://stateful-ci.test/v1/objects/${seededManifestKey}`, {
        headers: { authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}` },
        method: "HEAD",
      }),
      env
    );

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "BlobStoreError",
      reason: "io_failed",
    });
  });

  it("PUT /v1/objects requires prepare-plan headers", async () => {
    const response = await handleFetch(
      new Request(`https://stateful-ci.test/v1/objects/${seededManifestKey}`, {
        body: "manifest",
        headers: { authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}` },
        method: "PUT",
      }),
      env,
      { blobStore: createInMemoryBlobStore() }
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "InvalidProtocolPayload",
    });
  });

  it("PUT /v1/objects rejects missing prepare-plan headers without reading the body", async () => {
    const request = new Request(
      `https://stateful-ci.test/v1/objects/${seededManifestKey}`,
      {
        headers: { authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}` },
        method: "PUT",
      }
    );

    Object.defineProperty(request, "arrayBuffer", {
      value: () => {
        throw new Error("object body should not be read without upload plan");
      },
    });

    const response = await handleFetch(request, env, {
      blobStore: createInMemoryBlobStore(),
    });

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "InvalidProtocolPayload",
    });
  });

  it("PUT /v1/objects rejects invalid prepare-plan headers", async () => {
    const response = await handleFetch(
      new Request(`https://stateful-ci.test/v1/objects/${seededManifestKey}`, {
        body: "manifest",
        headers: {
          authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
          "x-stateful-ci-object-digest": seededManifestDigest,
          "x-stateful-ci-object-kind": "chunk",
          "x-stateful-ci-object-size": "not-a-number",
        },
        method: "PUT",
      }),
      env,
      { blobStore: createInMemoryBlobStore() }
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "InvalidProtocolPayload",
    });
  });

  it("PUT /v1/objects rejects declared size mismatches before storage", async () => {
    const response = await handleFetch(
      new Request(`https://stateful-ci.test/v1/objects/${seededManifestKey}`, {
        body: "manifest",
        headers: {
          authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
          "content-length": "999",
          "x-stateful-ci-object-digest": seededManifestDigest,
          "x-stateful-ci-object-kind": "manifest",
          "x-stateful-ci-object-size": "8",
        },
        method: "PUT",
      }),
      env,
      { blobStore: createInMemoryBlobStore() }
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "BlobStoreError",
      reason: "size_mismatch",
    });
  });

  it("POST /v1/restore denies when no compatible ref exists", async () => {
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

  it("POST /v1/restore denies compatible snapshots with missing objects", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      {
        blobStore: createInMemoryBlobStore(),
        metadata,
      }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "snapshot_object_missing",
      save: { allowed: true, target: seededRefName },
      trustClass: "trusted",
      workspaceId: seededWorkspaceId,
    });
  });

  it.effect("POST /v1/restore returns object download plans", () =>
    Effect.gen(function* restoreReturnsDownloadPlansEffect() {
      const metadata = createInMemoryMetadataBackend({
        refs: [seededRef],
        snapshots: [seededSnapshot],
      });
      const response = yield* Effect.promise(() =>
        handleFetch(jsonRequest("/v1/restore", restoreRequest), env, {
          blobStore: seededBlobStore(),
          metadata,
        })
      );
      const body = Schema.decodeUnknownSync(RestoreAllowedResponse)(
        yield* Effect.promise(() => response.json())
      );

      assert.strictEqual(response.status, 200);
      assert.strictEqual(body.decision, "allowed");
      assert.deepStrictEqual(
        body.downloadPlan.map((entry) => ({
          key: entry.object.key,
          method: entry.method,
          route: entry.transport === "worker-route" ? entry.route : null,
        })),
        seededSnapshot.objects.map((object) => ({
          key: object.key,
          method: "GET" as const,
          route: `/v1/objects/${object.key}`,
        }))
      );
    })
  );

  it("POST /v1/restore lets trusted main seed same-repo pull requests", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", internalPullRequest),
      env,
      { blobStore: createInMemoryBlobStore(), metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "snapshot_object_missing",
      save: { allowed: true, target: "internal/refs-pull-12-merge/latest" },
      trustClass: "internal",
    });
  });

  it("POST /v1/restore lets trusted main seed fork pull requests", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", externalPullRequest),
      env,
      { blobStore: createInMemoryBlobStore(), metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "snapshot_object_missing",
      save: { allowed: false },
      trustClass: "external",
    });
  });

  it("POST /v1/restore lets trusted main seed privileged release runs", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", privilegedRelease),
      env,
      { blobStore: createInMemoryBlobStore(), metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "snapshot_object_missing",
      save: { allowed: false },
      trustClass: "privileged",
    });
  });

  it.effect(
    "POST /v1/restore denies external snapshots renamed into trusted latest",
    () =>
      Effect.gen(function* restoreDeniesExternalSnapshotRetargetEffect() {
        const metadata = createInMemoryMetadataBackend({
          refs: [refFor(seededRefName, "snap_125", "external")],
          snapshots: [snapshotFor("snap_125", "external", seededWorkspaceId)],
        });
        const response = yield* Effect.promise(() =>
          handleFetch(jsonRequest("/v1/restore", restoreRequest), env, {
            metadata,
          })
        );
        const body = yield* Effect.promise(() => response.json());
        const auditEvents = yield* metadata.listAuditEvents();

        assert.strictEqual(response.status, 200);
        assert.deepStrictEqual(body, {
          decision: "denied",
          reason: "restore_policy_denied",
          save: { allowed: true, target: seededRefName },
          trustClass: "trusted",
          workspaceId: seededWorkspaceId,
        });
        assert.strictEqual(auditEvents.length, 1);
        assert.strictEqual(auditEvents[0]?.decision, "denied");
        assert.strictEqual(auditEvents[0]?.reason, "restore_policy_denied");
        assert.strictEqual(auditEvents[0]?.snapshotId, "snap_125");
      })
  );

  it("POST /v1/restore denies internal snapshots retargeted across internal scopes", async () => {
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

  it("POST /v1/restore denies external snapshots retargeted across external scopes", async () => {
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

  it("POST /v1/restore denies external snapshots in privileged runs", async () => {
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

  it("POST /v1/restore denies internal branch snapshots in privileged runs", async () => {
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

  it("POST /v1/restore denies latest snapshots from a different config hash", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", {
        ...restoreRequest,
        client: {
          ...restoreRequest.client,
          configHash:
            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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
        "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        seededRefName
      ),
    });
  });

  it("POST /v1/restore includes derived fork pull request trust class", async () => {
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

  it("POST /v1/save fails closed", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/save", saveRequest),
      env
    );

    expect(response.status).toBe(410);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "backend_policy_not_configured",
    });
  });

  it.effect("POST /v1/save/prepare returns missing upload plans", () =>
    Effect.gen(function* prepareReturnsMissingObjectsEffect() {
      const blobStore = createInMemoryBlobStore(
        new Map([
          [
            Schema.decodeSync(ManifestKey)(seededManifestKey),
            new TextEncoder().encode("manifest"),
          ],
        ])
      );
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/save/prepare", {
            client: restoreRequest.client,
            git: restoreRequest.git,
            github: restoreRequest.github,
            idempotencyKey: "run-123456789-save",
            identity: restoreRequest.identity,
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: 8,
              snapshotId: "snap_132",
            },
            objects: seededSnapshot.objects,
            protocolVersion: 1,
            workspace: restoreRequest.workspace,
          }),
          env,
          { blobStore }
        )
      );
      const body = Schema.decodeUnknownSync(PrepareSaveAllowedResponse)(
        yield* Effect.promise(() => response.json())
      );

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(
        body.missingObjects.map((entry) => entry.object.key),
        [seededPackKey, seededChunkKey]
      );
      assert.strictEqual(body.expectedHeadGeneration, 0);
    })
  );

  it("POST /v1/save/prepare returns backend failure when object HEAD fails", async () => {
    const response = await handleFetch(
      jsonRequest("/v1/save/prepare", {
        client: restoreRequest.client,
        git: restoreRequest.git,
        github: restoreRequest.github,
        idempotencyKey: "run-123456789-save",
        identity: restoreRequest.identity,
        manifest: {
          digest: seededManifestDigest,
          key: seededManifestKey,
          size: 8,
          snapshotId: "snap_132",
        },
        objects: seededSnapshot.objects,
        protocolVersion: 1,
        workspace: restoreRequest.workspace,
      }),
      env,
      { blobStore: failingHeadBlobStore() }
    );

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "BlobStoreError",
      reason: "io_failed",
    });
  });

  it("POST /v1/save/prepare fails closed without object storage", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/save/prepare", {
        client: restoreRequest.client,
        git: restoreRequest.git,
        github: restoreRequest.github,
        idempotencyKey: "run-123456789-save",
        identity: restoreRequest.identity,
        manifest: {
          digest: seededManifestDigest,
          key: seededManifestKey,
          size: 8,
          snapshotId: "snap_132",
        },
        objects: seededSnapshot.objects,
        protocolVersion: 1,
        workspace: restoreRequest.workspace,
      }),
      env
    );

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "BlobStoreError",
      reason: "io_failed",
    });
  });

  it.effect("POST /v1/save/commit validates objects before commit", () =>
    Effect.gen(function* commitValidatesObjectsBeforeCommitEffect() {
      const metadata = createInMemoryMetadataBackend();
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/save/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
            idempotencyKey: "run-123456789-save",
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: 8,
              snapshotId: "snap_132",
            },
            objects: seededSnapshot.objects,
            protocolVersion: 1,
            runId: "123456789",
            target: { namespace: seededNamespace, refName: seededRefName },
            workspaceId: seededWorkspaceId,
          }),
          env,
          { blobStore: failingHeadBlobStore(), metadata }
        )
      );
      const body = yield* Effect.promise(() => response.json());

      assert.strictEqual(response.status, 500);
      assert.deepInclude(body, { _tag: "BlobStoreError", reason: "io_failed" });
    })
  );

  it.effect("POST /v1/save/commit validates object presence", () =>
    Effect.gen(function* commitValidatesObjectsEffect() {
      const metadata = createInMemoryMetadataBackend({
        workspaceTargets: [
          {
            namespace: seededNamespace,
            refName: seededRefName,
            runId: Schema.decodeSync(RunId)("123456789"),
            trustClass: "trusted",
            workspaceId: seededWorkspaceId,
          },
        ],
      });
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/save/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
            idempotencyKey: "run-123456789-save",
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: 8,
              snapshotId: "snap_132",
            },
            objects: seededSnapshot.objects,
            protocolVersion: 1,
            runId: "123456789",
            target: { namespace: seededNamespace, refName: seededRefName },
            workspaceId: seededWorkspaceId,
          }),
          env,
          { blobStore: createInMemoryBlobStore(), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(CommitSaveResponse)(
        yield* Effect.promise(() => response.json())
      );
      const header = yield* metadata.getSnapshotHeader(
        Schema.decodeSync(SnapshotId)("snap_132")
      );

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "denied",
        reason: "snapshot_object_missing",
      });
      assert.isNull(header);
    })
  );

  it.effect("POST /v1/save/commit advances refs through coordinator", () =>
    Effect.gen(function* commitDoesNotMutateWithoutCoordinatorEffect() {
      const metadata = createInMemoryMetadataBackend({
        workspaceTargets: [
          {
            namespace: seededNamespace,
            refName: seededRefName,
            runId: Schema.decodeSync(RunId)("123456789"),
            trustClass: "trusted",
            workspaceId: seededWorkspaceId,
          },
        ],
      });
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/save/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
            idempotencyKey: "run-123456789-save",
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: 8,
              snapshotId: "snap_132",
            },
            objects: seededSnapshot.objects,
            protocolVersion: 1,
            runId: "123456789",
            target: { namespace: seededNamespace, refName: seededRefName },
            workspaceId: seededWorkspaceId,
          }),
          env,
          { blobStore: seededBlobStore(), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(CommitSaveResponse)(
        yield* Effect.promise(() => response.json())
      );
      const header = yield* metadata.getSnapshotHeader(
        Schema.decodeSync(SnapshotId)("snap_132")
      );
      const ref = yield* metadata.getRef(seededNamespace, seededRefName);

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "committed",
        headGeneration: Schema.decodeSync(HeadGeneration)(1),
        snapshotId: Schema.decodeSync(SnapshotId)("snap_132"),
        workspaceId: seededWorkspaceId,
      });
      assert.isNotNull(header);
      assert.strictEqual(ref?.snapshotId, "snap_132");
    })
  );

  it("POST /v1/restore uses configured trusted refs for seed snapshots", async () => {
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
      { blobStore: createInMemoryBlobStore(), metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "snapshot_object_missing",
      trustClass: "internal",
    });
  });

  it("POST /v1/restore tries each configured trusted ref for seed snapshots", async () => {
    const releaseRefName = "trusted/release/latest";
    const releaseSnapshotId = "snap_131";
    const metadata = createInMemoryMetadataBackend({
      refs: [refFor(releaseRefName, releaseSnapshotId, "trusted")],
      snapshots: [
        snapshotFor(
          releaseSnapshotId,
          "trusted",
          workspaceIdFor(seededNamespace, releaseRefName)
        ),
      ],
    });
    const response = await handleFetch(
      jsonRequest("/v1/restore", internalPullRequest),
      {
        ...env,
        STATEFUL_CI_TRUSTED_REFS: "refs/heads/main,refs/heads/release",
      },
      { blobStore: createInMemoryBlobStore(), metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "snapshot_object_missing",
      trustClass: "internal",
    });
  });

  it.effect("POST /v1/save is fail-closed and does not mutate metadata", () =>
    Effect.gen(function* legacySaveIsFailClosedEffect() {
      const metadata = createInMemoryMetadataBackend({
        workspaceTargets: [
          {
            namespace: seededNamespace,
            refName: seededRefName,
            runId: Schema.decodeSync(RunId)("123456789"),
            trustClass: "trusted",
            workspaceId: seededWorkspaceId,
          },
        ],
      });
      const response = yield* Effect.promise(() =>
        handleFetch(jsonRequest("/v1/save", saveRequest), env, { metadata })
      );
      const body = yield* Effect.promise(() => response.json());
      const header = yield* metadata.getSnapshotHeader(
        Schema.decodeSync(SnapshotId)("snap_124")
      );
      const ref = yield* metadata.getRef(seededNamespace, seededRefName);

      assert.strictEqual(response.status, 410);
      assert.deepStrictEqual(body, {
        decision: "denied",
        reason: "backend_policy_not_configured",
      });
      assert.isNull(header);
      assert.isNull(ref);
    })
  );

  it("invalid JSON returns structured 400", async () => {
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

  it("schema-invalid JSON returns structured 400", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/restore", { ...restoreRequest, github: {} }),
      env
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "InvalidProtocolPayload",
    });
  });

  it("oversized JSON returns structured 413 before schema validation", async () => {
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

  it("unknown routes return structured 404", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/missing"),
      env
    );

    expect(response.status).toBe(404);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "RouteNotFound",
    });
  });

  it("wrong methods return structured 405", async () => {
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

  it("restore without an authorization token returns structured 401", async () => {
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

  it("restore with the wrong authorization token returns structured 403", async () => {
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
