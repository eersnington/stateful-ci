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

import { afterEach, assert, beforeEach, describe, it } from "@effect/vitest";
import {
  largeChunkSizeBytes,
  manifestKeyFromDigest,
  Sha256Digest,
  SnapshotManifest,
  StatefulCiConfig,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import {
  createWorkspaceSnapshot,
  restoreWorkspaceSnapshot,
} from "../src/snapshot-engine";

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

  it.effect(
    "save writes canonical manifest, pack, and chunk objects with complete inventory",
    () =>
      Effect.gen(function* saveWritesCanonicalManifestEffect() {
        const snapshot = yield* createSnapshotEffect(tempDir);

        assert.isTrue(
          snapshot.objects.some((object) => object.kind === "manifest")
        );
        assert.isTrue(
          snapshot.objects.some((object) => object.kind === "pack")
        );
        assert.isTrue(
          snapshot.objects.some((object) => object.kind === "chunk")
        );
        assert.deepStrictEqual(snapshot.saveManifest.safety, {
          skippedByBuiltInDenylist: 1,
          skippedByUserExclude: 1,
          skippedUnsupportedType: 0,
        });

        for (const object of snapshot.objects) {
          const { key, size } = object;
          const storedObjectPath = objectPath(tempDir, key);
          const bytes = yield* Effect.promise(() => readFile(storedObjectPath));
          assert.strictEqual(bytes.byteLength, size);
        }
      })
  );

  it.effect(
    "restore exactly replaces managed roots and preserves paths outside them",
    () =>
      Effect.gen(function* restoreReplacesManagedRootsEffect() {
        const snapshot = yield* createSnapshotEffect(tempDir);

        yield* Effect.promise(() =>
          writeFile(path.join(tempDir, "outside.txt"), "keep")
        );
        yield* Effect.promise(() =>
          writeFile(path.join(tempDir, ".turbo/cache/stale.txt"), "stale")
        );
        yield* Effect.promise(() =>
          rm(path.join(tempDir, ".turbo/cache/result.txt"))
        );

        yield* restoreWorkspaceSnapshot({
          manifest: snapshot.manifestDescriptor,
          workspaceRoot: tempDir,
        });

        const restored = yield* Effect.promise(() =>
          readFile(path.join(tempDir, ".turbo/cache/result.txt"), "utf-8")
        );
        const outside = yield* Effect.promise(() =>
          readFile(path.join(tempDir, "outside.txt"), "utf-8")
        );
        const staleExit = yield* Effect.exit(
          Effect.promise(() =>
            lstat(path.join(tempDir, ".turbo/cache/stale.txt"))
          )
        );

        assert.strictEqual(restored, "cached output");
        assert.strictEqual(outside, "keep");
        assert.isTrue(Exit.isFailure(staleExit));
      })
  );

  it.effect("modes, mtimes, packs, chunks, and symlinks round-trip", () =>
    Effect.gen(function* snapshotMetadataRoundTripsEffect() {
      const snapshot = yield* createSnapshotEffect(tempDir);

      yield* Effect.promise(() =>
        rm(path.join(tempDir, ".turbo"), { recursive: true })
      );
      yield* Effect.promise(() =>
        rm(path.join(tempDir, "target"), { recursive: true })
      );

      yield* restoreWorkspaceSnapshot({
        manifest: snapshot.manifestDescriptor,
        workspaceRoot: tempDir,
      });

      const restored = yield* Effect.promise(() =>
        lstat(path.join(tempDir, ".turbo/cache/result.txt"))
      );
      const link = yield* Effect.promise(() =>
        lstat(path.join(tempDir, ".turbo/result-link"))
      );
      const largeBytes = yield* Effect.promise(() =>
        readFile(path.join(tempDir, "target/large.bin"))
      );

      assert.strictEqual(restored.mode % 0o1000, 0o755);
      assert.strictEqual(Math.round(restored.mtimeMs / 1000), 1000);
      assert.isTrue(link.isSymbolicLink());
      assert.strictEqual(largeBytes.byteLength, largeChunkSizeBytes + 7);
    })
  );

  it.effect(
    "corrupted objects reject before mutating previous managed roots",
    () =>
      Effect.gen(function* corruptedObjectsRejectBeforeMutationEffect() {
        const snapshot = yield* createSnapshotEffect(tempDir);
        const pack = snapshot.objects.find((object) => object.kind === "pack");

        if (pack === undefined) {
          return yield* Effect.die("test setup expected a pack object");
        }

        yield* Effect.promise(() =>
          writeFile(objectPath(tempDir, pack.key), "corrupt")
        );
        yield* Effect.promise(() =>
          writeFile(
            path.join(tempDir, ".turbo/cache/current.txt"),
            "still here"
          )
        );

        const error = yield* Effect.flip(
          restoreWorkspaceSnapshot({
            manifest: snapshot.manifestDescriptor,
            workspaceRoot: tempDir,
          })
        );
        const current = yield* Effect.promise(() =>
          readFile(path.join(tempDir, ".turbo/cache/current.txt"), "utf-8")
        );

        assert.strictEqual(error.reason, "corrupt_object");
        assert.strictEqual(current, "still here");
      })
  );

  it.effect("restore rejects entries below symlink paths", () =>
    Effect.gen(function* restoreRejectsEntriesBelowSymlinksEffect() {
      const snapshot = yield* createSnapshotEffect(tempDir);
      const fileEntry = snapshot.manifest.entries.find(
        (entry) => entry.type === "file"
      );

      if (fileEntry === undefined) {
        return yield* Effect.die("test setup expected a file entry");
      }

      if (fileEntry.content.kind !== "pack") {
        return yield* Effect.die("test setup expected a packed file entry");
      }

      const unsafeManifest = {
        ...snapshot.manifest,
        entries: [
          ...snapshot.manifest.entries,
          {
            content: fileEntry.content,
            mode: 420,
            mtime: 1,
            path: ".turbo/result-link/escaped.txt",
            sha256: fileEntry.sha256,
            size: 13,
            type: "file" as const,
          },
        ],
      };
      const manifest =
        Schema.decodeUnknownSync(SnapshotManifest)(unsafeManifest);
      const descriptor = yield* Effect.promise(() =>
        writeManifestObject(tempDir, manifest)
      );
      const error = yield* Effect.flip(
        restoreWorkspaceSnapshot({
          manifest: descriptor,
          workspaceRoot: tempDir,
        })
      );

      assert.strictEqual(error.reason, "invalid_symlink");
    })
  );

  it.effect(
    "restore rejects chunk references missing from object inventory",
    () =>
      Effect.gen(function* restoreRejectsMissingChunkInventoryEffect() {
        const snapshot = yield* createSnapshotEffect(tempDir);
        const manifest = Schema.decodeUnknownSync(SnapshotManifest)({
          ...snapshot.manifest,
          objects: snapshot.manifest.objects.filter(
            (object) => object.kind !== "chunk"
          ),
        });
        const descriptor = yield* Effect.promise(() =>
          writeManifestObject(tempDir, manifest)
        );
        const error = yield* Effect.flip(
          restoreWorkspaceSnapshot({
            manifest: descriptor,
            workspaceRoot: tempDir,
          })
        );

        assert.strictEqual(error.reason, "invalid_manifest");
      })
  );

  it.effect(
    "restore creates empty configured roots that were absent at save time",
    () =>
      Effect.gen(function* restoreCreatesAbsentConfiguredRootsEffect() {
        yield* Effect.promise(() =>
          rm(path.join(tempDir, "target"), { force: true, recursive: true })
        );
        const snapshot = yield* createSnapshotEffect(tempDir);

        yield* Effect.promise(() =>
          rm(path.join(tempDir, ".turbo"), { recursive: true })
        );

        yield* restoreWorkspaceSnapshot({
          manifest: snapshot.manifestDescriptor,
          workspaceRoot: tempDir,
        });

        const target = yield* Effect.promise(() =>
          lstat(path.join(tempDir, "target"))
        );
        assert.isTrue(target.isDirectory());
      })
  );

  it.effect("unsafe roots and symlink escapes are rejected", () =>
    Effect.gen(function* unsafeRootsAndSymlinksRejectEffect() {
      const invalidRootError = yield* Effect.flip(
        createWorkspaceSnapshot({
          config: Schema.decodeUnknownSync(StatefulCiConfig)({
            paths: [".stateful-ci"],
          }),
          provenance,
          workspace,
          workspaceRoot: tempDir,
        })
      );

      yield* Effect.promise(() => rm(path.join(tempDir, ".turbo/result-link")));
      yield* Effect.promise(() =>
        symlink("../outside", path.join(tempDir, ".turbo/result-link"))
      );
      const invalidSymlinkError = yield* Effect.flip(
        createSnapshotEffect(tempDir)
      );

      assert.strictEqual(invalidRootError.reason, "invalid_root");
      assert.strictEqual(invalidSymlinkError.reason, "invalid_symlink");
    })
  );
});
