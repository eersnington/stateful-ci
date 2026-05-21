import { createHash } from "node:crypto";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  utimes,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import {
  largeChunkSizeBytes,
  manifestKeyFromDigest,
  Sha256Digest,
  SnapshotManifest,
  StatefulCiConfig,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

import {
  createWorkspaceSnapshot,
  restoreWorkspaceSnapshot,
} from "./snapshot-engine";
import type { SnapshotEngineError } from "./snapshot-engine";

const workspace = {
  job: "test",
  repo: "eersnington/stateful-ci",
  workflow: "ci.yml",
};

const provenance = {
  git: {
    baseRef: null,
    headRef: null,
    headRepo: null,
    ref: "refs/heads/main",
    sha: "abc123",
  },
  github: { actor: "eersnington", event: "push", runId: "123456789" },
  runId: "123456789",
};

const createSnapshotEffect = (workspaceRoot: string) =>
  createWorkspaceSnapshot({
    config: Schema.decodeUnknownSync(StatefulCiConfig)({
      exclude: [".turbo/cache/skip.txt"],
      paths: [".turbo", "target"],
    }),
    provenance,
    workspace,
    workspaceRoot,
  });

const createSnapshot = (workspaceRoot: string) =>
  Effect.runPromise(createSnapshotEffect(workspaceRoot));

const objectPath = (workspaceRoot: string, key: string) =>
  path.join(workspaceRoot, ".stateful-ci/store", key);

const writeManifestObject = async (
  workspaceRoot: string,
  manifest: SnapshotManifest
) => {
  const source = Schema.encodeUnknownSync(
    Schema.fromJsonString(SnapshotManifest)
  )(manifest);
  const manifestDigest = `sha256:${createHash("sha256").update(source).digest("hex")}`;
  const digest = Schema.decodeSync(Sha256Digest)(manifestDigest);
  const key = manifestKeyFromDigest(digest);

  await mkdir(path.dirname(objectPath(workspaceRoot, key)), {
    recursive: true,
  });
  await writeFile(objectPath(workspaceRoot, key), source);

  return {
    digest,
    key,
    size: Buffer.byteLength(source),
    snapshotId: manifest.snapshotId,
  };
};

const expectEngineError = (
  error: { readonly reason: string },
  reason: SnapshotEngineError["reason"]
) => {
  expect(error.reason).toBe(reason);
};

describe("local snapshot engine", () => {
  let tempDir = "";

  beforeEach(async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), "stateful-ci-snapshot-"));
    await mkdir(path.join(tempDir, ".turbo/cache"), { recursive: true });
    await mkdir(path.join(tempDir, "target"), { recursive: true });
    await writeFile(
      path.join(tempDir, ".turbo/cache/result.txt"),
      "cached output"
    );
    await writeFile(
      path.join(tempDir, ".turbo/cache/skip.txt"),
      "ignored output"
    );
    await writeFile(path.join(tempDir, ".turbo/cache/.env"), "SECRET=value");
    await writeFile(
      path.join(tempDir, "target/large.bin"),
      Buffer.alloc(largeChunkSizeBytes + 7, 7)
    );
    await symlink("cache/result.txt", path.join(tempDir, ".turbo/result-link"));
    await chmod(path.join(tempDir, ".turbo/cache/result.txt"), 0o755);
    await utimes(path.join(tempDir, ".turbo/cache/result.txt"), 1000, 1000);
  });

  afterEach(async () => {
    await rm(tempDir, { force: true, recursive: true });
  });

  test("save writes canonical manifest, pack, and chunk objects with complete inventory", async () => {
    const snapshot = await createSnapshot(tempDir);

    expect(
      snapshot.objects.some((object) => object.kind === "manifest")
    ).toBeTruthy();
    expect(
      snapshot.objects.some((object) => object.kind === "pack")
    ).toBeTruthy();
    expect(
      snapshot.objects.some((object) => object.kind === "chunk")
    ).toBeTruthy();
    expect(snapshot.saveManifest.safety).toStrictEqual({
      skippedByBuiltInDenylist: 1,
      skippedByUserExclude: 1,
      skippedUnsupportedType: 0,
    });

    for (const object of snapshot.objects) {
      await expect(
        readFile(objectPath(tempDir, object.key))
      ).resolves.toHaveLength(object.size);
    }
  });

  test("restore exactly replaces managed roots and preserves paths outside them", async () => {
    const snapshot = await createSnapshot(tempDir);

    await writeFile(path.join(tempDir, "outside.txt"), "keep");
    await writeFile(path.join(tempDir, ".turbo/cache/stale.txt"), "stale");
    await rm(path.join(tempDir, ".turbo/cache/result.txt"));

    await Effect.runPromise(
      restoreWorkspaceSnapshot({
        manifest: snapshot.manifestDescriptor,
        workspaceRoot: tempDir,
      })
    );

    await expect(
      readFile(path.join(tempDir, ".turbo/cache/result.txt"), "utf-8")
    ).resolves.toBe("cached output");
    await expect(
      readFile(path.join(tempDir, "outside.txt"), "utf-8")
    ).resolves.toBe("keep");
    await expect(
      lstat(path.join(tempDir, ".turbo/cache/stale.txt"))
    ).rejects.toBeDefined();
  });

  test("modes, mtimes, packs, chunks, and symlinks round-trip", async () => {
    const snapshot = await createSnapshot(tempDir);

    await rm(path.join(tempDir, ".turbo"), { recursive: true });
    await rm(path.join(tempDir, "target"), { recursive: true });

    await Effect.runPromise(
      restoreWorkspaceSnapshot({
        manifest: snapshot.manifestDescriptor,
        workspaceRoot: tempDir,
      })
    );

    const restored = await lstat(path.join(tempDir, ".turbo/cache/result.txt"));
    const link = await lstat(path.join(tempDir, ".turbo/result-link"));

    expect(restored.mode % 0o1000).toBe(0o755);
    expect(Math.round(restored.mtimeMs / 1000)).toBe(1000);
    expect(link.isSymbolicLink()).toBeTruthy();
    await expect(
      readFile(path.join(tempDir, "target/large.bin"))
    ).resolves.toHaveLength(largeChunkSizeBytes + 7);
  });

  test("corrupted objects reject before mutating previous managed roots", async () => {
    const snapshot = await createSnapshot(tempDir);
    const pack = snapshot.objects.find((object) => object.kind === "pack");

    if (pack === undefined) {
      throw new Error("test setup expected a pack object");
    }

    await writeFile(objectPath(tempDir, pack.key), "corrupt");
    await writeFile(
      path.join(tempDir, ".turbo/cache/current.txt"),
      "still here"
    );

    expectEngineError(
      await Effect.runPromise(
        Effect.flip(
          restoreWorkspaceSnapshot({
            manifest: snapshot.manifestDescriptor,
            workspaceRoot: tempDir,
          })
        )
      ),
      "corrupt_object"
    );
    await expect(
      readFile(path.join(tempDir, ".turbo/cache/current.txt"), "utf-8")
    ).resolves.toBe("still here");
  });

  test("restore rejects entries below symlink paths", async () => {
    const snapshot = await createSnapshot(tempDir);
    const unsafeManifest = {
      ...snapshot.manifest,
      entries: [
        ...snapshot.manifest.entries,
        {
          content: snapshot.manifest.entries.find(
            (entry) => entry.type === "file"
          )?.content,
          mode: 420,
          mtime: 1,
          path: ".turbo/result-link/escaped.txt",
          sha256: snapshot.manifest.entries.find(
            (entry) => entry.type === "file"
          )?.sha256,
          size: 13,
          type: "file" as const,
        },
      ],
    };
    const manifest = Schema.decodeUnknownSync(SnapshotManifest)(unsafeManifest);
    const descriptor = await writeManifestObject(tempDir, manifest);

    await expect(
      Effect.runPromise(
        Effect.flip(
          restoreWorkspaceSnapshot({
            manifest: descriptor,
            workspaceRoot: tempDir,
          })
        )
      )
    ).resolves.toMatchObject({ reason: "invalid_symlink" });
  });

  test("restore rejects chunk references missing from object inventory", async () => {
    const snapshot = await createSnapshot(tempDir);
    const manifest = Schema.decodeUnknownSync(SnapshotManifest)({
      ...snapshot.manifest,
      objects: snapshot.manifest.objects.filter(
        (object) => object.kind !== "chunk"
      ),
    });
    const descriptor = await writeManifestObject(tempDir, manifest);

    await expect(
      Effect.runPromise(
        Effect.flip(
          restoreWorkspaceSnapshot({
            manifest: descriptor,
            workspaceRoot: tempDir,
          })
        )
      )
    ).resolves.toMatchObject({ reason: "invalid_manifest" });
  });

  test("restore creates empty configured roots that were absent at save time", async () => {
    await rm(path.join(tempDir, "target"), { force: true, recursive: true });
    const snapshot = await createSnapshot(tempDir);

    await rm(path.join(tempDir, ".turbo"), { recursive: true });

    await Effect.runPromise(
      restoreWorkspaceSnapshot({
        manifest: snapshot.manifestDescriptor,
        workspaceRoot: tempDir,
      })
    );

    const target = await lstat(path.join(tempDir, "target"));
    expect(target.isDirectory()).toBeTruthy();
  });

  test("unsafe roots and symlink escapes are rejected", async () => {
    await expect(
      Effect.runPromise(
        Effect.flip(
          createWorkspaceSnapshot({
            config: Schema.decodeUnknownSync(StatefulCiConfig)({
              paths: [".stateful-ci"],
            }),
            provenance,
            workspace,
            workspaceRoot: tempDir,
          })
        )
      )
    ).resolves.toMatchObject({ reason: "invalid_root" });

    await rm(path.join(tempDir, ".turbo/result-link"));
    await symlink("../outside", path.join(tempDir, ".turbo/result-link"));

    await expect(
      Effect.runPromise(Effect.flip(createSnapshotEffect(tempDir)))
    ).resolves.toMatchObject({ reason: "invalid_symlink" });
  });
});
