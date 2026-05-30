import { assert, describe, expect, it } from "@effect/vitest";
import {
  ChunkKey,
  ManifestKey,
  PackKey,
  RestoreAllowedResponse,
  PrepareSaveAllowedResponse,
  PrepareSaveDeniedResponse,
  CommitSaveResponse,
  HeadGeneration,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import type { SnapshotObjectKey } from "@stateful-ci/core";
import { Effect, Schema } from "effect";
import { beforeAll } from "vitest";

import { createInMemoryBlobStore } from "../src/blob-store-memory";
import { handleFetch, maxProtocolBodyBytes } from "../src/handler";
import worker from "../src/index";
import { createInMemoryMetadataBackend } from "../src/metadata";
import type { RefRow, SnapshotHeader } from "../src/metadata";
import {
  createSignedGitHubOidcToken,
  githubOidcClaims,
} from "./oidc-test-token";

let signedOidcToken = "";
let signedOidcJwksJson = "";
let featureOidcToken = "";
let pullRequestOidcToken = "";
let pullRequestTargetOidcToken = "";
let releaseOidcToken = "";

const setupOidcTokens = Effect.gen(function* setupOidcTokensEffect() {
  const nowSeconds = Math.floor(
    (yield* Effect.clockWith((clock) => clock.currentTimeMillis)) / 1000
  );
  const signed = yield* createSignedGitHubOidcToken(
    githubOidcClaims(nowSeconds),
    "main-key"
  );
  signedOidcToken = signed.token;
  const featureSigned = yield* createSignedGitHubOidcToken(
    githubOidcClaims(nowSeconds, {
      ref: "refs/heads/feature",
      sub: "repo:eersnington/stateful-ci:ref:refs/heads/feature",
    }),
    "feature-key"
  );
  featureOidcToken = featureSigned.token;
  const pullRequestSigned = yield* createSignedGitHubOidcToken(
    githubOidcClaims(nowSeconds, {
      base_ref: "main",
      event_name: "pull_request",
      head_ref: "feature",
      ref: "refs/pull/12/merge",
      sub: "repo:eersnington/stateful-ci:pull_request",
    }),
    "pull-request-key"
  );
  pullRequestOidcToken = pullRequestSigned.token;
  const pullRequestTargetSigned = yield* createSignedGitHubOidcToken(
    githubOidcClaims(nowSeconds, {
      base_ref: "main",
      event_name: "pull_request_target",
      head_ref: "feature",
      ref: "refs/heads/main",
      sub: "repo:eersnington/stateful-ci:pull_request",
    }),
    "pull-request-target-key"
  );
  pullRequestTargetOidcToken = pullRequestTargetSigned.token;
  const releaseSigned = yield* createSignedGitHubOidcToken(
    githubOidcClaims(nowSeconds, {
      event_name: "release",
      ref: "refs/tags/v1.0.0",
      ref_type: "tag",
      sub: "repo:eersnington/stateful-ci:ref:refs/tags/v1.0.0",
    }),
    "release-key"
  );
  releaseOidcToken = releaseSigned.token;
  signedOidcJwksJson = JSON.stringify({
    keys: [
      ...signed.jwks.keys,
      ...featureSigned.jwks.keys,
      ...pullRequestSigned.jwks.keys,
      ...pullRequestTargetSigned.jwks.keys,
      ...releaseSigned.jwks.keys,
    ],
  });
});

const oidcIdentityFor = (token: () => string) => ({
  provider: "github-actions" as const,
  get token() {
    return token();
  },
});

const env = {
  ALLOWED_REPOSITORIES: "eersnington/stateful-ci",
  STATEFUL_CI_API_TOKEN: "test-token",
  STATEFUL_CI_DEV_AUTH_ENABLED: "true",
  get STATEFUL_CI_GITHUB_JWKS_JSON() {
    return signedOidcJwksJson;
  },
  STATEFUL_CI_TRANSFER_SECRET: "test-transfer-token",
};

const productionEnv = {
  ALLOWED_REPOSITORIES: "eersnington/stateful-ci",
  get STATEFUL_CI_GITHUB_JWKS_JSON() {
    return signedOidcJwksJson;
  },
  STATEFUL_CI_TRANSFER_SECRET: "test-transfer-token",
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
  identity: oidcIdentityFor(() => signedOidcToken),
  managedRoots: [".turbo"],
  protocolVersion: 1,
  workspace: {
    job: "test",
    repo: "eersnington/stateful-ci",
    workflow: "ci.yml",
  },
};

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
const seededNamespace =
  "repo=eersnington/stateful-ci/workflow=ci.yml/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
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
  identity: oidcIdentityFor(() => pullRequestOidcToken),
};

const externalPullRequest = {
  ...internalPullRequest,
  git: { ...internalPullRequest.git, headRepo: "contributor/stateful-ci" },
};

const privilegedRelease = {
  ...restoreRequest,
  git: { ...restoreRequest.git, ref: "refs/tags/v1.0.0" },
  github: { ...restoreRequest.github, event: "release" },
  identity: oidcIdentityFor(() => releaseOidcToken),
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

const seededObjectBytes = () =>
  new Map<SnapshotObjectKey, Uint8Array>([
    [Schema.decodeSync(ManifestKey)(seededManifestKey), seededManifestBytes],
    [Schema.decodeSync(PackKey)(seededPackKey), seededPackBytes],
    [Schema.decodeSync(ChunkKey)(seededChunkKey), seededChunkBytes],
  ]);

const seededBlobStore = () => createInMemoryBlobStore(seededObjectBytes());

const failingHeadBlobStore = () =>
  createInMemoryBlobStore(new Map(), { failHead: true });

describe("worker API", () => {
  beforeAll(() => Effect.runPromise(setupOidcTokens));

  it("GET /health returns protocol health", async () => {
    const response = await handleFetch(
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

  it.effect(
    "PUT /v1/objects rejects overwriting existing different bytes",
    () =>
      Effect.gen(function* objectRouteRejectsImmutableConflictsEffect() {
        const blobStore = createInMemoryBlobStore(
          new Map([
            [
              Schema.decodeSync(ManifestKey)(seededManifestKey),
              new TextEncoder().encode("corrupt!"),
            ],
          ])
        );
        const response = yield* Effect.promise(() =>
          handleFetch(
            new Request(
              `https://stateful-ci.test/v1/objects/${seededManifestKey}`,
              {
                body: seededManifestBytes,
                headers: {
                  authorization: `Bearer ${env.STATEFUL_CI_API_TOKEN}`,
                  "content-length": String(seededManifestBytes.byteLength),
                  "x-stateful-ci-object-digest": seededManifestDigest,
                  "x-stateful-ci-object-kind": "manifest",
                  "x-stateful-ci-object-size": String(
                    seededManifestBytes.byteLength
                  ),
                },
                method: "PUT",
              }
            ),
            env,
            { blobStore }
          )
        );

        assert.strictEqual(response.status, 409);
        assert.deepInclude(yield* Effect.promise(() => response.json()), {
          _tag: "BlobStoreError",
          reason: "conflict",
        });
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
        handleFetch(jsonRequest("/v1/restore", restoreRequest), productionEnv, {
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
      for (const [index, entry] of body.downloadPlan.entries()) {
        const object = seededSnapshot.objects[index];

        if (object === undefined) {
          return yield* Effect.die("Expected matching seeded object.");
        }

        assert.strictEqual(
          entry.headers?.["x-stateful-ci-object-digest"],
          object.digest
        );
        assert.strictEqual(
          entry.headers?.["x-stateful-ci-object-kind"],
          object.kind
        );
        assert.strictEqual(
          entry.headers?.["x-stateful-ci-object-size"],
          String(object.size)
        );
        assert.isString(entry.headers?.["x-stateful-ci-transfer-expires-at"]);
        assert.isString(entry.headers?.["x-stateful-ci-transfer-token"]);
        assert.notStrictEqual(
          entry.headers?.["x-stateful-ci-transfer-token"],
          productionEnv.STATEFUL_CI_TRANSFER_SECRET
        );
      }
    })
  );

  it.effect("POST /v1/restore accepts dev bearer auth without OIDC", () =>
    Effect.gen(function* restoreAcceptsDevBearerAuthWithoutOidcEffect() {
      const internalRefName = "internal/main/latest";
      const internalSnapshotId = "snap_dev_restore";
      const metadata = createInMemoryMetadataBackend({
        refs: [refFor(internalRefName, internalSnapshotId, "internal")],
        snapshots: [
          snapshotFor(
            internalSnapshotId,
            "internal",
            workspaceIdFor(seededNamespace, internalRefName)
          ),
        ],
      });
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/restore", {
            ...restoreRequest,
            identity: undefined,
          }),
          env,
          { blobStore: seededBlobStore(), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(RestoreAllowedResponse)(
        yield* Effect.promise(() => response.json())
      );

      assert.strictEqual(response.status, 200);
      assert.strictEqual(body.decision, "allowed");
      assert.strictEqual(body.trustClass, "internal");
      assert.strictEqual(
        body.workspaceId,
        workspaceIdFor(seededNamespace, internalRefName)
      );
    })
  );

  it("POST /v1/restore treats same-repo pull requests as external without verified PR metadata", async () => {
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
      save: { allowed: false },
      trustClass: "external",
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
        identity: oidcIdentityFor(() => featureOidcToken),
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
        "repo=eersnington/stateful-ci/workflow=ci.yml/config=sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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
        identity: oidcIdentityFor(() => pullRequestOidcToken),
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

  it("POST /v1/restore denies unverified production identity", async () => {
    const metadata = createInMemoryMetadataBackend();
    const response = await handleFetch(
      jsonRequest("/v1/restore", {
        ...restoreRequest,
        identity: { provider: "github-actions", token: "" },
      }),
      { STATEFUL_CI_API_TOKEN: env.STATEFUL_CI_API_TOKEN },
      { metadata }
    );
    const audit = await Effect.runPromise(metadata.listAuditEvents());

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "oidc_missing",
      save: { allowed: false },
      trustClass: "unknown",
    });
    expect(audit).toHaveLength(1);
    expect(audit[0]).toMatchObject({
      decision: "denied",
      eventType: "restore",
      reason: "oidc_missing",
      trustClass: "unknown",
    });
  });

  it("POST /v1/restore denies when repository allowlist is missing", async () => {
    const response = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      {
        STATEFUL_CI_GITHUB_JWKS_JSON: env.STATEFUL_CI_GITHUB_JWKS_JSON,
        STATEFUL_CI_TRANSFER_SECRET: env.STATEFUL_CI_TRANSFER_SECRET,
      },
      { metadata: createInMemoryMetadataBackend() }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "unknown_context_denied",
      save: { allowed: false },
      trustClass: "unknown",
    });
  });

  it("POST /v1/restore denies repositories outside the allowlist", async () => {
    const response = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      {
        ALLOWED_REPOSITORIES: "other/repo",
        STATEFUL_CI_GITHUB_JWKS_JSON: env.STATEFUL_CI_GITHUB_JWKS_JSON,
        STATEFUL_CI_TRANSFER_SECRET: env.STATEFUL_CI_TRANSFER_SECRET,
      },
      { metadata: createInMemoryMetadataBackend() }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "unknown_context_denied",
      save: { allowed: false },
      trustClass: "unknown",
    });
  });

  it("POST /v1/restore accepts mixed-case repository allowlist entries", async () => {
    const response = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      {
        ALLOWED_REPOSITORIES: "Eersnington/Stateful-CI",
        STATEFUL_CI_GITHUB_JWKS_JSON: env.STATEFUL_CI_GITHUB_JWKS_JSON,
        STATEFUL_CI_TRANSFER_SECRET: env.STATEFUL_CI_TRANSFER_SECRET,
      },
      { metadata: createInMemoryMetadataBackend() }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "no_compatible_snapshot",
      save: { allowed: true, target: seededRefName },
      trustClass: "trusted",
    });
  });

  it("POST /v1/prepare fails closed with malformed configured JWKS", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/prepare", {
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
      {
        STATEFUL_CI_API_TOKEN: env.STATEFUL_CI_API_TOKEN,
        STATEFUL_CI_GITHUB_JWKS_JSON: "not json",
      }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toStrictEqual({
      decision: "denied",
      reason: "oidc_invalid",
      trustClass: "unknown",
    });
  });

  it.effect("POST /v1/prepare returns missing upload plans", () =>
    Effect.gen(function* prepareReturnsMissingObjectsEffect() {
      const blobStore = createInMemoryBlobStore(
        new Map([
          [
            Schema.decodeSync(ManifestKey)(seededManifestKey),
            new TextEncoder().encode("manifest"),
          ],
        ])
      );
      const metadata = createInMemoryMetadataBackend();
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/prepare", {
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
          { blobStore, metadata }
        )
      );
      const body = Schema.decodeUnknownSync(PrepareSaveAllowedResponse)(
        yield* Effect.promise(() => response.json())
      );
      const audit = yield* metadata.listAuditEvents();

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(
        body.missingObjects.map((entry) => entry.object.key),
        [seededPackKey, seededChunkKey]
      );
      assert.strictEqual(body.expectedHeadGeneration, 0);
      assert.deepInclude(audit[0], {
        decision: "allowed",
        eventType: "prepare-save",
        reason: null,
      });
    })
  );

  it.effect("POST /v1/prepare and /v1/commit accept dev bearer auth", () =>
    Effect.gen(function* saveAcceptsDevBearerAuthEffect() {
      const metadata = createInMemoryMetadataBackend();
      const prepareRequest = {
        client: restoreRequest.client,
        git: restoreRequest.git,
        github: restoreRequest.github,
        idempotencyKey: "run-123456789-dev-save",
        manifest: {
          digest: seededManifestDigest,
          key: seededManifestKey,
          size: 8,
          snapshotId: "snap_dev_save",
        },
        objects: seededSnapshot.objects,
        protocolVersion: 1,
        workspace: restoreRequest.workspace,
      };
      const prepareResponse = yield* Effect.promise(() =>
        handleFetch(jsonRequest("/v1/prepare", prepareRequest), env, {
          blobStore: seededBlobStore(),
          metadata,
        })
      );
      const prepareBody = Schema.decodeUnknownSync(PrepareSaveAllowedResponse)(
        yield* Effect.promise(() => prepareResponse.json())
      );
      const commitResponse = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/commit", {
            baseSnapshotId: prepareBody.baseSnapshotId,
            expectedHeadGeneration: prepareBody.expectedHeadGeneration,
            idempotencyKey: prepareRequest.idempotencyKey,
            manifest: prepareRequest.manifest,
            objects: prepareRequest.objects,
            protocolVersion: 1,
            runId: restoreRequest.github.runId,
            target: prepareBody.commitTarget,
            workspaceId: prepareBody.workspaceId,
          }),
          env,
          { blobStore: seededBlobStore(), metadata }
        )
      );
      const commitBody = Schema.decodeUnknownSync(CommitSaveResponse)(
        yield* Effect.promise(() => commitResponse.json())
      );
      const header = yield* metadata.getSnapshotHeader(
        Schema.decodeSync(SnapshotId)("snap_dev_save")
      );

      assert.strictEqual(prepareResponse.status, 200);
      assert.strictEqual(prepareBody.trustClass, "internal");
      assert.strictEqual(
        prepareBody.commitTarget.refName,
        "internal/main/latest"
      );
      assert.strictEqual(commitResponse.status, 200);
      assert.strictEqual(commitBody.decision, "committed");
      if (commitBody.decision !== "committed") {
        return yield* Effect.die("Expected committed dev save.");
      }
      assert.strictEqual(commitBody.snapshotId, "snap_dev_save");
      assert.strictEqual(header?.trustClass, "internal");
    })
  );

  it.effect("POST /v1/prepare denies fork pull request writes", () =>
    Effect.gen(function* prepareDeniesForkPullRequestWritesEffect() {
      const metadata = createInMemoryMetadataBackend();
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/prepare", {
            client: externalPullRequest.client,
            git: externalPullRequest.git,
            github: externalPullRequest.github,
            idempotencyKey: "run-123456789-save",
            identity: externalPullRequest.identity,
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: 8,
              snapshotId: "snap_external_prepare",
            },
            objects: seededSnapshot.objects,
            protocolVersion: 1,
            workspace: externalPullRequest.workspace,
          }),
          env,
          { blobStore: seededBlobStore(), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(PrepareSaveDeniedResponse)(
        yield* Effect.promise(() => response.json())
      );
      const audit = yield* metadata.listAuditEvents();

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "denied",
        reason: "external_save_disabled",
        trustClass: "external",
        workspaceId: workspaceIdFor(
          seededNamespace,
          "external/refs-pull-12-merge/latest"
        ),
      });
      assert.deepInclude(audit.at(-1), {
        decision: "denied",
        eventType: "prepare-save",
        reason: "external_save_disabled",
        trustClass: "external",
      });
    })
  );

  it.effect("POST /v1/prepare denies pull_request_target writes", () =>
    Effect.gen(function* prepareDeniesPullRequestTargetWritesEffect() {
      const metadata = createInMemoryMetadataBackend();
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/prepare", {
            client: restoreRequest.client,
            git: {
              ...restoreRequest.git,
              baseRef: "main",
              headRef: "feature",
              headRepo: "eersnington/stateful-ci",
            },
            github: { ...restoreRequest.github, event: "pull_request_target" },
            idempotencyKey: "run-123456789-save",
            identity: oidcIdentityFor(() => pullRequestTargetOidcToken),
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: 8,
              snapshotId: "snap_pull_request_target_prepare",
            },
            objects: seededSnapshot.objects,
            protocolVersion: 1,
            workspace: restoreRequest.workspace,
          }),
          env,
          { blobStore: createInMemoryBlobStore(), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(PrepareSaveDeniedResponse)(
        yield* Effect.promise(() => response.json())
      );
      const audit = yield* metadata.listAuditEvents();

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "denied",
        reason: "pull_request_target_denied",
        trustClass: "unknown",
        workspaceId: workspaceIdFor(seededNamespace, "unknown/main/latest"),
      });
      assert.strictEqual(audit.length, 1);
      assert.deepInclude(audit[0], {
        decision: "denied",
        eventType: "prepare-save",
        reason: "pull_request_target_denied",
        trustClass: "unknown",
      });
    })
  );

  it("POST /v1/prepare returns backend failure when object HEAD fails", async () => {
    const response = await handleFetch(
      jsonRequest("/v1/prepare", {
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

  it("POST /v1/prepare fails closed without object storage", async () => {
    const response = await worker.fetch(
      jsonRequest("/v1/prepare", {
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

  it.effect("POST /v1/commit validates objects before commit", () =>
    Effect.gen(function* commitValidatesObjectsBeforeCommitEffect() {
      const metadata = createInMemoryMetadataBackend();
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
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

  it.effect("POST /v1/commit denies unverified production identity", () =>
    Effect.gen(function* commitDeniesUnverifiedProductionIdentityEffect() {
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
          jsonRequest("/v1/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
            idempotencyKey: "run-123456789-save",
            identity: { provider: "github-actions", token: "" },
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
          { STATEFUL_CI_API_TOKEN: env.STATEFUL_CI_API_TOKEN },
          { blobStore: seededBlobStore(), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(CommitSaveResponse)(
        yield* Effect.promise(() => response.json())
      );
      const ref = yield* metadata.getRef(seededNamespace, seededRefName);
      const audit = yield* metadata.listAuditEvents();

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "denied",
        reason: "oidc_missing",
      });
      assert.isNull(ref);
      assert.deepInclude(audit[0], {
        eventType: "commit",
        namespace: "unverified/commit-save",
        refName: "unknown",
        snapshotId: null,
        workspaceId: null,
      });
    })
  );

  it.effect("POST /v1/commit denies pull_request_target writes", () =>
    Effect.gen(function* commitDeniesPullRequestTargetWritesEffect() {
      const metadata = createInMemoryMetadataBackend();
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
            idempotencyKey: "run-123456789-save-pull-request-target",
            identity: oidcIdentityFor(() => pullRequestTargetOidcToken),
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: seededManifestBytes.byteLength,
              snapshotId: "snap_pull_request_target_commit",
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
      const body = Schema.decodeUnknownSync(CommitSaveResponse)(
        yield* Effect.promise(() => response.json())
      );
      const header = yield* metadata.getSnapshotHeader(
        Schema.decodeSync(SnapshotId)("snap_pull_request_target_commit")
      );
      const ref = yield* metadata.getRef(seededNamespace, seededRefName);
      const audit = yield* metadata.listAuditEvents();

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "denied",
        reason: "pull_request_target_denied",
      });
      assert.isNull(header);
      assert.isNull(ref);
      assert.deepInclude(audit.at(-1), {
        decision: "denied",
        eventType: "commit",
        reason: "pull_request_target_denied",
        trustClass: "unknown",
      });
    })
  );

  it.effect("POST /v1/commit validates object presence", () =>
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
          jsonRequest("/v1/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
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

  it.effect("POST /v1/commit rejects each missing object kind", () =>
    Effect.gen(function* commitRejectsEachMissingObjectKindEffect() {
      for (const missingKind of ["manifest", "pack", "chunk"] as const) {
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
        const bytes = seededObjectBytes();

        for (const object of seededSnapshot.objects) {
          if (object.kind === missingKind) {
            bytes.delete(object.key);
          }
        }

        const response = yield* Effect.promise(() =>
          handleFetch(
            jsonRequest("/v1/commit", {
              baseSnapshotId: null,
              expectedHeadGeneration: 0,
              idempotencyKey: `run-123456789-save-${missingKind}`,
              identity: restoreRequest.identity,
              manifest: {
                digest: seededManifestDigest,
                key: seededManifestKey,
                size: 8,
                snapshotId: `snap_missing_${missingKind}`,
              },
              objects: seededSnapshot.objects,
              protocolVersion: 1,
              runId: "123456789",
              target: { namespace: seededNamespace, refName: seededRefName },
              workspaceId: seededWorkspaceId,
            }),
            env,
            { blobStore: createInMemoryBlobStore(bytes), metadata }
          )
        );
        const body = Schema.decodeUnknownSync(CommitSaveResponse)(
          yield* Effect.promise(() => response.json())
        );
        const header = yield* metadata.getSnapshotHeader(
          Schema.decodeSync(SnapshotId)(`snap_missing_${missingKind}`)
        );

        assert.strictEqual(response.status, 200);
        assert.deepStrictEqual(body, {
          decision: "denied",
          reason: "snapshot_object_missing",
        });
        assert.isNull(header);
      }
    })
  );

  it.effect("POST /v1/commit rejects stored object size mismatches", () =>
    Effect.gen(function* commitRejectsStoredObjectSizeMismatchEffect() {
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
      const bytes = seededObjectBytes();

      bytes.set(
        Schema.decodeSync(PackKey)(seededPackKey),
        new TextEncoder().encode("pack-too-large")
      );

      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
            idempotencyKey: "run-123456789-save-size-mismatch",
            identity: restoreRequest.identity,
            manifest: {
              digest: seededManifestDigest,
              key: seededManifestKey,
              size: 8,
              snapshotId: "snap_size_mismatch",
            },
            objects: seededSnapshot.objects,
            protocolVersion: 1,
            runId: "123456789",
            target: { namespace: seededNamespace, refName: seededRefName },
            workspaceId: seededWorkspaceId,
          }),
          env,
          { blobStore: createInMemoryBlobStore(bytes), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(CommitSaveResponse)(
        yield* Effect.promise(() => response.json())
      );
      const header = yield* metadata.getSnapshotHeader(
        Schema.decodeSync(SnapshotId)("snap_size_mismatch")
      );

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "denied",
        reason: "snapshot_object_mismatch",
      });
      assert.isNull(header);
    })
  );

  it.effect("POST /v1/commit advances refs through coordinator", () =>
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
          jsonRequest("/v1/commit", {
            baseSnapshotId: null,
            expectedHeadGeneration: 0,
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
      const audit = yield* metadata.listAuditEvents();

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(body, {
        decision: "committed",
        headGeneration: Schema.decodeSync(HeadGeneration)(1),
        snapshotId: Schema.decodeSync(SnapshotId)("snap_132"),
        workspaceId: seededWorkspaceId,
      });
      assert.isNotNull(header);
      assert.strictEqual(ref?.snapshotId, "snap_132");
      assert.deepInclude(audit.at(-1), {
        decision: "committed",
        eventType: "commit",
        reason: null,
      });
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
      trustClass: "external",
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
      trustClass: "external",
    });
  });

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

  it("restore authenticates production requests with OIDC instead of a static token", async () => {
    const response = await worker.fetch(
      new Request("https://stateful-ci.test/v1/restore", {
        body: JSON.stringify(restoreRequest),
        headers: { "content-type": "application/json" },
        method: "POST",
      }),
      env
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      trustClass: "trusted",
    });
  });

  it("object routes reject missing transfer authorization in production", async () => {
    const response = await handleFetch(
      new Request(`https://stateful-ci.test/v1/objects/${seededManifestKey}`, {
        method: "GET",
      }),
      { STATEFUL_CI_TRANSFER_SECRET: env.STATEFUL_CI_TRANSFER_SECRET },
      { blobStore: seededBlobStore() }
    );

    expect(response.status).toBe(403);
    await expect(response.json()).resolves.toMatchObject({
      _tag: "Forbidden",
    });
  });

  it.effect("object routes accept scoped backend transfer authorization", () =>
    Effect.gen(function* objectRoutesAcceptScopedTransferAuthorizationEffect() {
      const metadata = createInMemoryMetadataBackend({
        refs: [seededRef],
        snapshots: [seededSnapshot],
      });
      const restoreResponse = yield* Effect.promise(() =>
        handleFetch(jsonRequest("/v1/restore", restoreRequest), productionEnv, {
          blobStore: seededBlobStore(),
          metadata,
        })
      );
      const body = Schema.decodeUnknownSync(RestoreAllowedResponse)(
        yield* Effect.promise(() => restoreResponse.json())
      );
      const [plan] = body.downloadPlan;

      if (plan === undefined || plan.transport !== "worker-route") {
        return yield* Effect.die("Expected worker-route download plan.");
      }

      const objectResponse = yield* Effect.promise(() =>
        handleFetch(
          new Request(`https://stateful-ci.test${plan.route}`, {
            headers: plan.headers ?? {},
            method: "GET",
          }),
          {
            STATEFUL_CI_TRANSFER_SECRET:
              productionEnv.STATEFUL_CI_TRANSFER_SECRET,
          },
          { blobStore: seededBlobStore() }
        )
      );

      assert.strictEqual(objectResponse.status, 200);
      assert.deepStrictEqual(
        new Uint8Array(
          yield* Effect.promise(() => objectResponse.arrayBuffer())
        ),
        seededManifestBytes
      );
    })
  );

  it.effect("dev auth object plans do not require a transfer secret", () =>
    Effect.gen(function* devAuthObjectPlansDoNotRequireTransferSecretEffect() {
      const metadata = createInMemoryMetadataBackend({
        refs: [seededRef],
        snapshots: [seededSnapshot],
      });
      const response = yield* Effect.promise(() =>
        handleFetch(
          jsonRequest("/v1/restore", restoreRequest),
          {
            ALLOWED_REPOSITORIES: env.ALLOWED_REPOSITORIES,
            STATEFUL_CI_API_TOKEN: env.STATEFUL_CI_API_TOKEN,
            STATEFUL_CI_DEV_AUTH_ENABLED: "true",
            STATEFUL_CI_GITHUB_JWKS_JSON: env.STATEFUL_CI_GITHUB_JWKS_JSON,
          },
          { blobStore: seededBlobStore(), metadata }
        )
      );
      const body = Schema.decodeUnknownSync(RestoreAllowedResponse)(
        yield* Effect.promise(() => response.json())
      );

      assert.strictEqual(response.status, 200);
      assert.strictEqual(body.decision, "allowed");
      for (const entry of body.downloadPlan) {
        assert.isUndefined(entry.headers?.["x-stateful-ci-transfer-token"]);
        assert.isUndefined(
          entry.headers?.["x-stateful-ci-transfer-expires-at"]
        );
        assert.isString(entry.headers?.["x-stateful-ci-object-digest"]);
      }
    })
  );

  it.effect(
    "object routes reject transfer authorization for another method",
    () =>
      Effect.gen(
        function* objectRoutesRejectWrongMethodTransferAuthorizationEffect() {
          const metadata = createInMemoryMetadataBackend({
            refs: [seededRef],
            snapshots: [seededSnapshot],
          });
          const restoreResponse = yield* Effect.promise(() =>
            handleFetch(
              jsonRequest("/v1/restore", restoreRequest),
              productionEnv,
              {
                blobStore: seededBlobStore(),
                metadata,
              }
            )
          );
          const body = Schema.decodeUnknownSync(RestoreAllowedResponse)(
            yield* Effect.promise(() => restoreResponse.json())
          );
          const [plan] = body.downloadPlan;

          if (plan === undefined || plan.transport !== "worker-route") {
            return yield* Effect.die("Expected worker-route download plan.");
          }

          const response = yield* Effect.promise(() =>
            handleFetch(
              new Request(`https://stateful-ci.test${plan.route}`, {
                headers: {
                  ...plan.headers,
                  "content-length": String(plan.object.size),
                },
                method: "PUT",
              }),
              {
                STATEFUL_CI_TRANSFER_SECRET:
                  productionEnv.STATEFUL_CI_TRANSFER_SECRET,
              },
              { blobStore: seededBlobStore() }
            )
          );

          assert.strictEqual(response.status, 403);
          assert.deepStrictEqual(yield* Effect.promise(() => response.json()), {
            _tag: "Forbidden",
            message:
              "The object transfer authorization did not match this method, object, digest, size, and expiry. Restore/save object bytes were not served or stored.",
          });
        }
      )
  );
});
