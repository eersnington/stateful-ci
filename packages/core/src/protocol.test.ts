import { Schema } from "effect";
import { describe, expect, test } from "vitest";

import {
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

describe("protocol schemas", () => {
  test("RestoreRequest decodes the PRD restore payload shape", () => {
    expect(
      Schema.decodeUnknownSync(RestoreRequest)(restoreRequest)
    ).toStrictEqual(restoreRequest);
  });

  test("RestoreRequest rejects missing GitHub run context", () => {
    expect(() =>
      Schema.decodeUnknownSync(RestoreRequest)({
        ...restoreRequest,
        github: { actor: "eersnington", event: "push" },
      })
    ).toThrow(/Missing key/u);
  });

  test("SaveRequest decodes manifest metadata and safety summary", () => {
    expect(Schema.decodeUnknownSync(SaveRequest)(saveRequest)).toStrictEqual(
      saveRequest
    );
  });

  test("SaveRequest rejects malformed manifest digests", () => {
    expect(() =>
      Schema.decodeUnknownSync(SaveRequest)({
        ...saveRequest,
        manifest: { ...saveRequest.manifest, hash: "sha256:not-a-real-digest" },
      })
    ).toThrow(/RegExp/u);
  });

  test("Sha256Digest rejects non-canonical uppercase hex", () => {
    expect(() =>
      Schema.decodeUnknownSync(Sha256Digest)(
        "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      )
    ).toThrow(/RegExp/u);
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
    expect(() =>
      Schema.decodeUnknownSync(RestoreSavePlan)({ allowed: true })
    ).toThrow(/Missing key/u);
  });

  test("TrustClass rejects unknown trust classes", () => {
    expect(() =>
      Schema.decodeUnknownSync(TrustClass)("release-candidate")
    ).toThrow(/Expected/u);
  });
});
