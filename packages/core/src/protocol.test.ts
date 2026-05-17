import { Result, Schema } from "effect";
import { describe, expect, test } from "vitest";

import {
  ArchiveKey,
  ManifestKey,
  RestoreRequest,
  RestoreSavePlan,
  SaveRequest,
  Sha256Digest,
  TrustClass,
} from "./index";

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
    archiveDigest:
      "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    archiveKey:
      "archives/sha256-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.sciar",
    chunkCount: 1,
    fileCount: 21_903,
    id: "snap_124",
    key: "manifests/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
    manifestDigest:
      "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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

const decodeRestoreRequest = Schema.decodeUnknownResult(RestoreRequest);
const decodeRestoreSavePlan = Schema.decodeUnknownResult(RestoreSavePlan);
const decodeSaveRequest = Schema.decodeUnknownResult(SaveRequest);
const decodeArchiveKey = Schema.decodeUnknownResult(ArchiveKey);
const decodeManifestKey = Schema.decodeUnknownResult(ManifestKey);
const decodeSha256Digest = Schema.decodeUnknownResult(Sha256Digest);
const decodeTrustClass = Schema.decodeUnknownResult(TrustClass);

describe("protocol schemas", () => {
  test("RestoreRequest decodes the PRD restore payload shape", () => {
    expect(
      Schema.decodeUnknownSync(RestoreRequest)(restoreRequest)
    ).toStrictEqual(restoreRequest);
  });

  test("RestoreRequest rejects missing GitHub run context", () => {
    expect(
      Result.isFailure(
        decodeRestoreRequest({
          ...restoreRequest,
          github: { actor: "eersnington", event: "push" },
        })
      )
    ).toBeTruthy();
  });

  test("SaveRequest decodes manifest metadata and safety summary", () => {
    expect(Schema.decodeUnknownSync(SaveRequest)(saveRequest)).toStrictEqual(
      saveRequest
    );
  });

  test("SaveRequest rejects malformed manifest digests", () => {
    expect(
      Result.isFailure(
        decodeSaveRequest({
          ...saveRequest,
          manifest: {
            ...saveRequest.manifest,
            manifestDigest: "sha256:not-a-real-digest",
          },
        })
      )
    ).toBeTruthy();
  });

  test("object keys reject traversal and unexpected prefixes", () => {
    expect(Result.isFailure(decodeManifestKey("../snap.json"))).toBeTruthy();
    expect(
      Result.isFailure(
        decodeManifestKey(
          "archives/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.sciar"
        )
      )
    ).toBeTruthy();
    expect(
      Result.isFailure(
        decodeArchiveKey(
          "manifests/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
        )
      )
    ).toBeTruthy();
    expect(
      Result.isFailure(
        decodeArchiveKey(
          "archives/../sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.sciar"
        )
      )
    ).toBeTruthy();
  });

  test("Sha256Digest rejects non-canonical uppercase hex", () => {
    expect(
      Result.isFailure(
        decodeSha256Digest(
          "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        )
      )
    ).toBeTruthy();
  });

  test("RestoreSavePlan accepts allowed plans with targets", () => {
    expect(
      Schema.decodeUnknownSync(RestoreSavePlan)({
        allowed: true,
        target: "trusted/main/ci/test",
      })
    ).toStrictEqual({ allowed: true, target: "trusted/main/ci/test" });
  });

  test("RestoreSavePlan rejects allowed plans without targets", () => {
    expect(
      Result.isFailure(decodeRestoreSavePlan({ allowed: true }))
    ).toBeTruthy();
  });

  test("TrustClass rejects unknown trust classes", () => {
    expect(
      Result.isFailure(decodeTrustClass("release-candidate"))
    ).toBeTruthy();
  });
});
