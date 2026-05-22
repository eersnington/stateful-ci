import { assert, describe, it } from "@effect/vitest";
import {
  HeadGeneration,
  ManifestKey,
  RunId,
  Sha256Digest,
  SnapshotId,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import { createInMemoryMetadataBackend } from "../src/metadata";
import type { SnapshotHeader } from "../src/metadata";

const namespace =
  "repo=eersnington/stateful-ci/workflow=ci/job=test/config=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const refName = "trusted/main/latest";
const workspaceId = Schema.decodeSync(WorkspaceId)(
  `ws:${namespace}:${refName}`
);
const runId = Schema.decodeSync(RunId)("123456789");
const snapshotId = Schema.decodeSync(SnapshotId)("snap_200");
const manifestDigest = Schema.decodeSync(Sha256Digest)(
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
);
const manifestKey = Schema.decodeSync(ManifestKey)(
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
);

const snapshotHeader = {
  createdAt: "2026-05-22T00:00:00.000Z",
  manifestDigest,
  manifestKey,
  manifestSize: 8,
  namespace,
  parentSnapshotId: null,
  producerActor: "eersnington",
  producerEvent: "push",
  producerJob: "test",
  producerRef: "refs/heads/main",
  producerRepository: "eersnington/stateful-ci",
  producerRunId: runId,
  producerSha: "abc123",
  producerWorkflow: "ci.yml",
  safetyJson: "{}",
  snapshotId,
  statsJson: "{}",
  trustClass: "trusted",
  workspaceId,
} satisfies SnapshotHeader;

describe("metadata backend contract", () => {
  it.effect("stores compact snapshot metadata and object reachability", () =>
    Effect.gen(function* metadataStoresSnapshotObjectsEffect() {
      const metadata = createInMemoryMetadataBackend();

      yield* metadata.putSnapshotHeader(snapshotHeader);
      yield* metadata.putSnapshotObjects(snapshotId, [
        { digest: manifestDigest, key: manifestKey, kind: "manifest", size: 8 },
      ]);

      const header = yield* metadata.getSnapshotHeader(snapshotId);
      const objects = yield* metadata.getSnapshotObjects(snapshotId);

      assert.deepStrictEqual(header, snapshotHeader);
      assert.deepStrictEqual(objects, [
        {
          digest: manifestDigest,
          key: manifestKey,
          kind: "manifest",
          size: 8,
          snapshotId,
        },
      ]);
    })
  );

  it.effect("compares and advances refs by generation", () =>
    Effect.gen(function* metadataAdvancesRefByGenerationEffect() {
      const metadata = createInMemoryMetadataBackend();
      const first = yield* metadata.compareAndAdvanceRef(
        namespace,
        refName,
        Schema.decodeSync(HeadGeneration)(0),
        {
          snapshotId,
          trustClass: "trusted",
          updatedByActor: "eersnington",
          updatedByRunId: runId,
        }
      );
      const stale = yield* metadata.compareAndAdvanceRef(
        namespace,
        refName,
        Schema.decodeSync(HeadGeneration)(0),
        {
          snapshotId,
          trustClass: "trusted",
          updatedByActor: "eersnington",
          updatedByRunId: runId,
        }
      );

      assert.strictEqual(first?.generation, 1);
      assert.isNull(stale);
    })
  );
});
