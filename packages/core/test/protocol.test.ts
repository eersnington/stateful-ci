import { describe, expect, it } from "@effect/vitest";
import { Result, Schema } from "effect";

import {
  CommitSaveRequest,
  CommitSaveResponse,
  DenialReason,
  ManifestKey,
  ObjectTransferPlanEntry,
  PackKey,
  PrepareSaveRequest,
  PrepareSaveResponse,
  RestoreAllowedResponse,
  RestoreRequest,
  SaveRequest,
  Sha256Digest,
  SnapshotObjectInventoryEntry,
  TrustClass,
} from "../src/index";

const configHash =
  "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const manifestDigest =
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const packDigest =
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const chunkDigest =
  "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const manifestKey =
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json";
const packKey =
  "packs/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.scipack";
const chunkKey =
  "chunks/sha256/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

const client = {
  configHash,
  version: "0.1.0",
};

const git = {
  baseRef: null,
  headRef: null,
  headRepo: null,
  ref: "refs/heads/main",
  sha: "abc123",
};

const github = {
  actor: "eersnington",
  event: "push",
  runId: "123456789",
};

const identity = {
  provider: "github-actions",
  token: "oidc.jwt.token",
};

const workspace = {
  job: "test",
  repo: "eersnington/stateful-ci",
  workflow: "ci.yml",
};

const objects = [
  {
    digest: manifestDigest,
    key: manifestKey,
    kind: "manifest",
    size: 512,
  },
  {
    digest: packDigest,
    key: packKey,
    kind: "pack",
    size: 2048,
  },
  {
    digest: chunkDigest,
    key: chunkKey,
    kind: "chunk",
    size: 4096,
  },
] as const;

const manifest = {
  digest: manifestDigest,
  key: manifestKey,
  size: 512,
  snapshotId: "snap_124",
};

const restoreRequest = {
  client,
  git,
  github,
  identity,
  managedRoots: [".turbo", ".next/cache"],
  protocolVersion: 1,
  workspace,
};

const prepareSaveRequest = {
  client,
  git,
  github,
  idempotencyKey: "run-123456789-save",
  identity,
  manifest,
  objects,
  protocolVersion: 1,
  workspace,
};

const commitSaveRequest = {
  baseSnapshotId: "snap_123",
  expectedHeadGeneration: 7,
  idempotencyKey: "run-123456789-save",
  identity,
  manifest,
  objects,
  protocolVersion: 1,
  runId: "123456789",
  target: {
    namespace:
      "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=trusted",
    refName: "trusted/main/latest",
  },
  workspaceId: "ws_123",
};

const saveRequest = {
  baseSnapshotId: "snap_123",
  manifest: {
    chunkCount: 1,
    fileCount: 21_903,
    hash: manifestDigest,
    id: "snap_124",
    key: manifestKey,
    objects,
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

const decodeRestoreRequest = Schema.decodeUnknownResult(RestoreRequest);
const decodeRestoreAllowedResponse = Schema.decodeUnknownResult(
  RestoreAllowedResponse
);
const decodeSaveRequest = Schema.decodeUnknownResult(SaveRequest);
const decodePrepareSaveRequest = Schema.decodeUnknownResult(PrepareSaveRequest);
const decodeCommitSaveRequest = Schema.decodeUnknownResult(CommitSaveRequest);
const decodeSha256Digest = Schema.decodeUnknownResult(Sha256Digest);
const decodeTrustClass = Schema.decodeUnknownResult(TrustClass);
const decodeDenialReason = Schema.decodeUnknownResult(DenialReason);
const decodeManifestKey = Schema.decodeUnknownResult(ManifestKey);
const decodePackKey = Schema.decodeUnknownResult(PackKey);
const decodeInventoryEntry = Schema.decodeUnknownResult(
  SnapshotObjectInventoryEntry
);

describe("protocol schemas", () => {
  it("RestoreRequest decodes the PRD restore payload shape", () => {
    expect(
      Schema.decodeUnknownSync(RestoreRequest)(restoreRequest)
    ).toStrictEqual(restoreRequest);
  });

  it("RestoreRequest rejects missing protocol version", () => {
    expect(
      Result.isFailure(
        decodeRestoreRequest({
          ...restoreRequest,
          protocolVersion: undefined,
        })
      )
    ).toBeTruthy();
  });

  it("RestoreRequest rejects malformed config hashes", () => {
    expect(
      Result.isFailure(
        decodeRestoreRequest({
          ...restoreRequest,
          client: { ...client, configHash: "sha256:config" },
        })
      )
    ).toBeTruthy();
  });

  it("RestoreRequest accepts missing OIDC identity for audited Worker denial", () => {
    expect(
      Schema.decodeUnknownSync(RestoreRequest)({
        ...restoreRequest,
        identity: undefined,
      }).identity
    ).toBeUndefined();
  });

  it("RestoreRequest does not decode client-provided trust class as authority", () => {
    expect(
      Schema.decodeUnknownSync(RestoreRequest)({
        ...restoreRequest,
        trustClass: "trusted",
      })
    ).toStrictEqual(restoreRequest);
  });

  it("SaveRequest decodes manifest metadata and complete object inventory", () => {
    expect(Schema.decodeUnknownSync(SaveRequest)(saveRequest)).toStrictEqual(
      saveRequest
    );
  });

  it("PrepareSaveRequest decodes explicit prepare-save payloads", () => {
    expect(
      Schema.decodeUnknownSync(PrepareSaveRequest)(prepareSaveRequest)
    ).toStrictEqual(prepareSaveRequest);
  });

  it("CommitSaveRequest requires backend-issued workspace and head generation", () => {
    expect(
      Schema.decodeUnknownSync(CommitSaveRequest)(commitSaveRequest)
    ).toStrictEqual(commitSaveRequest);
    expect(
      Result.isFailure(
        decodeCommitSaveRequest({
          ...commitSaveRequest,
          expectedHeadGeneration: undefined,
        })
      )
    ).toBeTruthy();
  });

  it("PrepareSaveResponse carries missing-object upload plans", () => {
    expect(
      Schema.decodeUnknownSync(PrepareSaveResponse)({
        baseSnapshotId: "snap_123",
        commitTarget: commitSaveRequest.target,
        decision: "allowed",
        expectedHeadGeneration: 7,
        missingObjects: [
          {
            method: "PUT",
            object: objects[1],
            route: `/v1/objects/${packKey}`,
            transport: "worker-route",
          },
        ],
        trustClass: "trusted",
        workspaceId: "ws_123",
      })
    ).toMatchObject({
      decision: "allowed",
      missingObjects: [{ object: objects[1] }],
    });
  });

  it("PrepareSaveResponse accepts empty missing-object upload plans", () => {
    expect(
      Schema.decodeUnknownSync(PrepareSaveResponse)({
        baseSnapshotId: "snap_123",
        commitTarget: commitSaveRequest.target,
        decision: "allowed",
        expectedHeadGeneration: 7,
        missingObjects: [],
        trustClass: "trusted",
        workspaceId: "ws_123",
      })
    ).toMatchObject({
      decision: "allowed",
      missingObjects: [],
    });
  });

  it("RestoreAllowedResponse rejects empty object download plans", () => {
    expect(
      Result.isFailure(
        decodeRestoreAllowedResponse({
          decision: "allowed",
          downloadPlan: [],
          manifest,
          save: { allowed: true, target: commitSaveRequest.target.refName },
          snapshot: {
            id: "snap_123",
            manifestKey,
            parent: null,
          },
          trustClass: "trusted",
          workspaceId: "ws_123",
        })
      )
    ).toBeTruthy();
  });

  it("CommitSaveResponse covers committed, idempotent, conflict, and denied", () => {
    expect(
      Schema.decodeUnknownSync(CommitSaveResponse)({
        decision: "committed",
        headGeneration: 8,
        snapshotId: "snap_124",
        workspaceId: "ws_123",
      }).decision
    ).toBe("committed");
    expect(
      Schema.decodeUnknownSync(CommitSaveResponse)({
        decision: "idempotent",
        headGeneration: 8,
        snapshotId: "snap_124",
        workspaceId: "ws_123",
      }).decision
    ).toBe("idempotent");
    expect(
      Schema.decodeUnknownSync(CommitSaveResponse)({
        actualHeadGeneration: 9,
        decision: "conflict",
        reason: "head_generation_mismatch",
      }).decision
    ).toBe("conflict");
    expect(
      Schema.decodeUnknownSync(CommitSaveResponse)({
        decision: "denied",
        reason: "save_policy_denied",
      }).decision
    ).toBe("denied");
  });

  it("object inventory accepts canonical manifest, pack, and chunk entries", () => {
    for (const object of objects) {
      expect(
        Schema.decodeUnknownSync(SnapshotObjectInventoryEntry)(object)
      ).toStrictEqual(object);
    }
  });

  it("object inventory rejects wrong kind/key pairs", () => {
    expect(
      Result.isFailure(
        decodeInventoryEntry({
          digest: packDigest,
          key: packKey,
          kind: "manifest",
          size: 128,
        })
      )
    ).toBeTruthy();
  });

  it("object inventory rejects key/digest mismatches", () => {
    expect(
      Result.isFailure(
        decodeInventoryEntry({
          ...objects[0],
          digest: packDigest,
        })
      )
    ).toBeTruthy();
  });

  it("canonical keys reject unsafe or malformed grammar", () => {
    for (const key of [
      "manifests/sha256/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.json",
      "manifests/snap_124.json",
      "packs/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "chunks/sha256/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd.json",
      "../chunks/sha256/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      "C:/chunks/sha256/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      "chunks\\sha256\\dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    ]) {
      expect(Result.isFailure(decodeManifestKey(key))).toBeTruthy();
    }
  });

  it("transfer plans support worker routes and signed URLs", () => {
    expect(
      Schema.decodeUnknownSync(ObjectTransferPlanEntry)({
        headers: { "x-stateful-ci-object": "pack" },
        method: "GET",
        object: objects[1],
        route: `/v1/objects/${packKey}`,
        transport: "worker-route",
      }).transport
    ).toBe("worker-route");
    expect(
      Schema.decodeUnknownSync(ObjectTransferPlanEntry)({
        expiresAt: "2026-05-20T00:00:00.000Z",
        method: "GET",
        object: objects[2],
        transport: "signed-url",
        url: `https://r2.example/${chunkKey}`,
      }).transport
    ).toBe("signed-url");
  });

  it("worker-route transfer plans must target the canonical object route", () => {
    expect(
      Result.isFailure(
        Schema.decodeUnknownResult(ObjectTransferPlanEntry)({
          method: "GET",
          object: objects[1],
          route: "https://example.test/leak-token",
          transport: "worker-route",
        })
      )
    ).toBeTruthy();
    expect(
      Result.isFailure(
        Schema.decodeUnknownResult(ObjectTransferPlanEntry)({
          method: "GET",
          object: objects[1],
          route: `/v1/objects/${chunkKey}`,
          transport: "worker-route",
        })
      )
    ).toBeTruthy();
  });

  it("SaveRequest rejects malformed manifest digests", () => {
    expect(
      Result.isFailure(
        decodeSaveRequest({
          ...saveRequest,
          manifest: {
            ...saveRequest.manifest,
            hash: "sha256:not-a-real-digest",
          },
        })
      )
    ).toBeTruthy();
  });

  it("Sha256Digest rejects non-canonical uppercase hex", () => {
    expect(
      Result.isFailure(
        decodeSha256Digest(
          "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        )
      )
    ).toBeTruthy();
  });

  it("TrustClass accepts known trust classes and rejects unknown classes", () => {
    for (const trustClass of [
      "external",
      "internal",
      "trusted",
      "privileged",
      "unknown",
    ]) {
      expect(Schema.decodeUnknownSync(TrustClass)(trustClass)).toBe(trustClass);
    }

    expect(
      Result.isFailure(decodeTrustClass("release-candidate"))
    ).toBeTruthy();
  });

  it("DenialReason accepts known reasons and rejects unknown reasons", () => {
    for (const reason of [
      "backend_policy_not_configured",
      "external_save_disabled",
      "head_generation_mismatch",
      "idempotency_conflict",
      "invalid_protocol_payload",
      "manifest_digest_mismatch",
      "manifest_schema_invalid",
      "no_compatible_snapshot",
      "oidc_audience_mismatch",
      "oidc_invalid",
      "oidc_issuer_mismatch",
      "oidc_missing",
      "privileged_save_disabled",
      "pull_request_target_denied",
      "restore_policy_denied",
      "restore_required_before_save",
      "save_policy_denied",
      "save_run_context_mismatch",
      "snapshot_object_mismatch",
      "snapshot_object_missing",
      "unable_to_classify_identity",
      "unable_to_classify_run_context",
      "unknown_context_denied",
      "unsafe_manifest_path",
      "external_snapshot_cannot_update_trusted_workspace",
    ]) {
      expect(Schema.decodeUnknownSync(DenialReason)(reason)).toBe(reason);
    }

    expect(
      Result.isFailure(decodeDenialReason("temporary_backend_error"))
    ).toBeTruthy();
  });

  it("PackKey accepts only canonical pack keys", () => {
    expect(Schema.decodeUnknownSync(PackKey)(packKey)).toBe(packKey);
    expect(
      Result.isFailure(
        decodePackKey(
          "packs/sha256/BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB.scipack"
        )
      )
    ).toBeTruthy();
  });

  it("PrepareSaveRequest rejects empty object inventory", () => {
    expect(
      Result.isFailure(
        decodePrepareSaveRequest({
          ...prepareSaveRequest,
          objects: [],
        })
      )
    ).toBeTruthy();
  });

  it("PrepareSaveRequest binds manifest descriptor to object inventory", () => {
    expect(
      Result.isFailure(
        decodePrepareSaveRequest({
          ...prepareSaveRequest,
          manifest: { ...manifest, size: 999 },
        })
      )
    ).toBeTruthy();
  });

  it("CommitSaveRequest binds manifest descriptor to object inventory", () => {
    expect(
      Result.isFailure(
        decodeCommitSaveRequest({
          ...commitSaveRequest,
          objects: objects.filter((object) => object.kind !== "manifest"),
        })
      )
    ).toBeTruthy();
  });
});
