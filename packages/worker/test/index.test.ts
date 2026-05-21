import { assert, describe, expect, it } from "@effect/vitest";
import {
  ChunkKey,
  ManifestKey,
  PackKey,
  RestoreDeniedResponse,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

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
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const seededManifestKey =
  "manifests/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json";
const seededPackDigest =
  "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const seededPackKey =
  "packs/sha256/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd.scipack";
const seededChunkDigest =
  "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const seededChunkKey =
  "chunks/sha256/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
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
    totalBytes: 481_203_912,
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
  manifestSize: 128,
  objects: [
    {
      digest: Schema.decodeSync(Sha256Digest)(seededManifestDigest),
      key: Schema.decodeSync(ManifestKey)(seededManifestKey),
      kind: "manifest",
      size: 128,
    },
    {
      digest: Schema.decodeSync(Sha256Digest)(seededPackDigest),
      key: Schema.decodeSync(PackKey)(seededPackKey),
      kind: "pack",
      size: 2048,
    },
    {
      digest: Schema.decodeSync(Sha256Digest)(seededChunkDigest),
      key: Schema.decodeSync(ChunkKey)(seededChunkKey),
      kind: "chunk",
      size: 4096,
    },
  ],
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

  it("POST /v1/restore denies compatible snapshots until object data plane is configured", async () => {
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
      decision: "denied",
      reason: "backend_policy_not_configured",
      save: { allowed: true, target: seededRefName },
      trustClass: "trusted",
      workspaceId: seededWorkspaceId,
    });
  });

  it("POST /v1/restore lets trusted main seed same-repo pull requests", async () => {
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
      decision: "denied",
      reason: "backend_policy_not_configured",
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
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "backend_policy_not_configured",
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
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "backend_policy_not_configured",
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
        const auditEvents = yield* metadata.listAuditEvents;

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

  it("POST /v1/save validates requests and denies without an allowed workspace target", async () => {
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

  it.effect(
    "POST /v1/save commits snapshot metadata and advances latest after allowed restore",
    () =>
      Effect.gen(function* saveCommitsSnapshotMetadataEffect() {
        const metadata = createInMemoryMetadataBackend({
          refs: [seededRef],
          snapshots: [seededSnapshot],
        });
        const restoreResponse = yield* Effect.promise(() =>
          handleFetch(jsonRequest("/v1/restore", restoreRequest), env, {
            metadata,
          })
        );
        const restoreBody = Schema.decodeUnknownSync(RestoreDeniedResponse)(
          yield* Effect.promise(() => restoreResponse.json())
        );

        if (restoreBody.workspaceId === undefined) {
          return yield* Effect.die(
            "Restore response did not include a save workspace."
          );
        }

        const response = yield* Effect.promise(() =>
          handleFetch(
            jsonRequest("/v1/save", {
              ...saveRequest,
              workspaceId: restoreBody.workspaceId,
            }),
            env,
            { metadata }
          )
        );
        const body = yield* Effect.promise(() => response.json());
        const header = yield* metadata.getSnapshotHeader(
          Schema.decodeSync(SnapshotId)("snap_124")
        );
        const ref = yield* metadata.getRef(seededNamespace, seededRefName);

        assert.strictEqual(response.status, 200);
        assert.deepStrictEqual(body, {
          decision: "committed",
          latest: true,
          snapshotId: "snap_124",
          workspaceId: seededWorkspaceId,
        });
        assert.isNotNull(header);
        assert.strictEqual(header.manifestKey, manifestKey);
        assert.strictEqual(header.parentSnapshotId, "snap_123");
        assert.strictEqual(header.snapshotId, "snap_124");
        assert.strictEqual(header.workspaceId, seededWorkspaceId);
        assert.isNotNull(ref);
        assert.strictEqual(ref.snapshotId, "snap_124");
        assert.strictEqual(ref.version, 2);
      })
  );

  it.effect(
    "POST /v1/save rejects saves whose run id does not match the restored run",
    () =>
      Effect.gen(function* saveRejectsRunMismatchEffect() {
        const metadata = createInMemoryMetadataBackend({
          refs: [seededRef],
          snapshots: [seededSnapshot],
        });
        const restoreResponse = yield* Effect.promise(() =>
          handleFetch(jsonRequest("/v1/restore", restoreRequest), env, {
            metadata,
          })
        );
        const restoreBody = Schema.decodeUnknownSync(RestoreDeniedResponse)(
          yield* Effect.promise(() => restoreResponse.json())
        );

        if (restoreBody.workspaceId === undefined) {
          return yield* Effect.die(
            "Restore response did not include a save workspace."
          );
        }

        const response = yield* Effect.promise(() =>
          handleFetch(
            jsonRequest("/v1/save", {
              ...saveRequest,
              runId: "987654321",
              workspaceId: restoreBody.workspaceId,
            }),
            env,
            { metadata }
          )
        );
        const body = yield* Effect.promise(() => response.json());
        const header = yield* metadata.getSnapshotHeader(
          Schema.decodeSync(SnapshotId)("snap_124")
        );

        assert.strictEqual(response.status, 200);
        assert.deepStrictEqual(body, {
          decision: "denied",
          reason: "save_run_context_mismatch",
        });
        assert.isNull(header);
      })
  );

  it.effect(
    "POST /v1/restore lets a trusted first run save when no compatible snapshot exists",
    () =>
      Effect.gen(function* restoreAllowsTrustedFirstRunSaveEffect() {
        const metadata = createInMemoryMetadataBackend();
        const response = yield* Effect.promise(() =>
          handleFetch(jsonRequest("/v1/restore", restoreRequest), env, {
            metadata,
          })
        );

        assert.strictEqual(response.status, 200);
        const restoreBody = Schema.decodeUnknownSync(RestoreDeniedResponse)(
          yield* Effect.promise(() => response.json())
        );

        assert.deepStrictEqual(restoreBody, {
          decision: "denied",
          reason: "no_compatible_snapshot",
          save: { allowed: true, target: seededRefName },
          trustClass: "trusted",
          workspaceId: seededWorkspaceId,
        });

        const saveResponse = yield* Effect.promise(() =>
          handleFetch(
            jsonRequest("/v1/save", {
              ...saveRequest,
              baseSnapshotId: null,
              workspaceId: restoreBody.workspaceId,
            }),
            env,
            { metadata }
          )
        );
        const saveBody = yield* Effect.promise(() => saveResponse.json());
        const header = yield* metadata.getSnapshotHeader(
          Schema.decodeSync(SnapshotId)("snap_124")
        );
        const ref = yield* metadata.getRef(seededNamespace, seededRefName);

        assert.deepStrictEqual(saveBody, {
          decision: "committed",
          latest: true,
          snapshotId: "snap_124",
          workspaceId: seededWorkspaceId,
        });
        assert.isNotNull(header);
        assert.strictEqual(header.parentSnapshotId, null);
        assert.strictEqual(header.snapshotId, "snap_124");
        assert.strictEqual(header.workspaceId, seededWorkspaceId);
        assert.isNotNull(ref);
        assert.strictEqual(ref.snapshotId, "snap_124");
        assert.strictEqual(ref.trustClass, "trusted");
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
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "backend_policy_not_configured",
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
      { metadata }
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "backend_policy_not_configured",
      trustClass: "internal",
    });
  });

  it("default worker fetches preserve restore targets for the following save request", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", restoreRequest),
      env,
      { metadata }
    );
    const restoreBody = Schema.decodeUnknownSync(RestoreDeniedResponse)(
      await restoreResponse.json()
    );

    if (restoreBody.workspaceId === undefined) {
      throw new Error("Restore response did not include a save workspace.");
    }

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

  it.effect(
    "POST /v1/save commits same-repo pull request snapshots to internal refs",
    () =>
      Effect.gen(function* saveCommitsInternalPullRequestSnapshotEffect() {
        const metadata = createInMemoryMetadataBackend({
          refs: [seededRef],
          snapshots: [seededSnapshot],
        });
        const restoreResponse = yield* Effect.promise(() =>
          handleFetch(jsonRequest("/v1/restore", internalPullRequest), env, {
            metadata,
          })
        );
        const restoreBody = Schema.decodeUnknownSync(RestoreDeniedResponse)(
          yield* Effect.promise(() => restoreResponse.json())
        );

        if (restoreBody.workspaceId === undefined) {
          return yield* Effect.die(
            "Restore response did not include a save workspace."
          );
        }

        const response = yield* Effect.promise(() =>
          handleFetch(
            jsonRequest("/v1/save", {
              ...saveRequest,
              workspaceId: restoreBody.workspaceId,
            }),
            env,
            { metadata }
          )
        );
        const body = yield* Effect.promise(() => response.json());
        const ref = yield* metadata.getRef(
          seededNamespace,
          "internal/refs-pull-12-merge/latest"
        );

        assert.strictEqual(response.status, 200);
        assert.deepStrictEqual(body, {
          decision: "committed",
          latest: true,
          snapshotId: "snap_124",
          workspaceId: restoreBody.workspaceId,
        });
        assert.isNotNull(ref);
        assert.strictEqual(ref.snapshotId, "snap_124");
        assert.strictEqual(ref.trustClass, "internal");
      })
  );

  it("POST /v1/save has no remembered target for fork pull requests", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", externalPullRequest),
      env,
      { metadata }
    );
    await expect(restoreResponse.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "backend_policy_not_configured",
      save: { allowed: false },
      trustClass: "external",
    });
    const response = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        workspaceId: seededWorkspaceId,
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

  it("POST /v1/save has no remembered target for privileged snapshots", async () => {
    const metadata = createInMemoryMetadataBackend({
      refs: [seededRef],
      snapshots: [seededSnapshot],
    });
    const restoreResponse = await handleFetch(
      jsonRequest("/v1/restore", privilegedRelease),
      env,
      { metadata }
    );
    await expect(restoreResponse.json()).resolves.toMatchObject({
      decision: "denied",
      reason: "backend_policy_not_configured",
      save: { allowed: false },
      trustClass: "privileged",
    });
    const response = await handleFetch(
      jsonRequest("/v1/save", {
        ...saveRequest,
        workspaceId: seededWorkspaceId,
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

  it.effect(
    "restore and save record audit metadata for allow and deny decisions",
    () =>
      Effect.gen(function* restoreAndSaveRecordAuditMetadataEffect() {
        const metadata = createInMemoryMetadataBackend({
          refs: [seededRef],
          snapshots: [seededSnapshot],
        });

        yield* Effect.promise(() =>
          handleFetch(jsonRequest("/v1/restore", restoreRequest), env, {
            metadata,
          })
        );
        yield* Effect.promise(() =>
          handleFetch(jsonRequest("/v1/save", saveRequest), env, { metadata })
        );

        const auditEvents = yield* metadata.listAuditEvents;

        assert.deepStrictEqual(
          auditEvents.map(({ decision, eventType, reason }) => ({
            decision,
            eventType,
            reason,
          })),
          [
            {
              decision: "denied",
              eventType: "restore",
              reason: "backend_policy_not_configured",
            },
            {
              decision: "denied",
              eventType: "save",
              reason: "restore_required_before_save",
            },
          ]
        );
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
