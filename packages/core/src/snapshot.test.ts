import { Result, Schema } from "effect";
import { describe, expect, test } from "vitest";

import {
  ChunkKey,
  ManifestKey,
  PackKey,
  Sha256Digest,
  SnapshotManifest,
  SnapshotManifestEntry,
  SnapshotObjectInventoryEntry,
} from "./index";

const manifestDigest =
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const packDigest =
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const chunkDigest =
  "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const entryDigest =
  "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const manifestKey =
  "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json";
const packKey =
  "packs/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.scipack";
const chunkKey =
  "chunks/sha256/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

const objects = [
  { digest: manifestDigest, key: manifestKey, kind: "manifest", size: 512 },
  { digest: packDigest, key: packKey, kind: "pack", size: 1024 },
  { digest: chunkDigest, key: chunkKey, kind: "chunk", size: 4096 },
] as const;

const decodeManifestKey = Schema.decodeUnknownResult(ManifestKey);
const decodePackKey = Schema.decodeUnknownResult(PackKey);
const decodeChunkKey = Schema.decodeUnknownResult(ChunkKey);
const decodeSnapshotManifest = Schema.decodeUnknownResult(SnapshotManifest);
const decodeSnapshotManifestEntry = Schema.decodeUnknownResult(
  SnapshotManifestEntry
);
const decodeInventoryEntry = Schema.decodeUnknownResult(
  SnapshotObjectInventoryEntry
);

describe("snapshot object graph schemas", () => {
  test("object keys accept only canonical grammar", () => {
    expect(Schema.decodeUnknownSync(ManifestKey)(manifestKey)).toBe(
      manifestKey
    );
    expect(Schema.decodeUnknownSync(PackKey)(packKey)).toBe(packKey);
    expect(Schema.decodeUnknownSync(ChunkKey)(chunkKey)).toBe(chunkKey);

    for (const key of [
      "/manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
      "manifests/sha256/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.json",
      "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "packs/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json",
      "chunks/sha256/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc.scipack",
      "../chunks/sha256/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "chunks\\sha256\\cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "C:/chunks/sha256/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "chunks/sha256/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\0",
    ]) {
      expect(Result.isFailure(decodeManifestKey(key))).toBeTruthy();
      expect(Result.isFailure(decodePackKey(key))).toBeTruthy();
      expect(Result.isFailure(decodeChunkKey(key))).toBeTruthy();
    }
  });

  test("object inventory validates kind, key, size, and digest identity", () => {
    for (const object of objects) {
      expect(
        Schema.decodeUnknownSync(SnapshotObjectInventoryEntry)(object)
      ).toStrictEqual(object);
    }

    expect(
      Result.isFailure(
        decodeInventoryEntry({ ...objects[1], digest: manifestDigest })
      )
    ).toBeTruthy();
  });

  test("manifest entries support directories, packed files, chunked files, and symlinks", () => {
    const entries = [
      { mode: 493, mtime: 1_779_230_000, path: ".turbo", type: "directory" },
      {
        content: {
          compressedLength: 41,
          compressedOffset: 0,
          compression: "gzip",
          entryDigest,
          kind: "pack",
          packDigest,
          packKey,
          uncompressedSize: 12,
        },
        mode: 420,
        mtime: 1_779_230_001,
        path: ".turbo/cache/index.db",
        sha256: entryDigest,
        size: 12,
        type: "file",
      },
      {
        content: {
          chunks: [
            { digest: chunkDigest, key: chunkKey, ordinal: 0, size: 4096 },
          ],
          kind: "chunks",
        },
        mode: 420,
        mtime: 1_779_230_002,
        path: ".turbo/cache/large.bin",
        sha256: chunkDigest,
        size: 4096,
        type: "file",
      },
      {
        path: "node_modules/.bin/vite",
        target: "../vite/bin/vite.js",
        type: "symlink",
      },
    ] as const;

    for (const entry of entries) {
      expect(
        Schema.decodeUnknownSync(SnapshotManifestEntry)(entry)
      ).toStrictEqual(entry);
    }
  });

  test("manifest schema stores file tree and complete object inventory", () => {
    const manifest = {
      createdAt: "2026-05-20T00:00:00.000Z",
      entries: [
        { mode: 493, mtime: 1_779_230_000, path: ".turbo", type: "directory" },
      ],
      formatVersion: 1,
      managedRoots: [".turbo"],
      objects,
      provenance: {
        git: {
          baseRef: null,
          headRef: null,
          headRepo: null,
          ref: "refs/heads/main",
          sha: "abc123",
        },
        github: { actor: "eersnington", event: "push", runId: "123456789" },
        runId: "123456789",
      },
      safety: {
        skippedByBuiltInDenylist: 0,
        skippedByUserExclude: 0,
        skippedUnsupportedType: 0,
      },
      snapshotId: "snap_124",
      stats: {
        chunkCount: 1,
        directoryCount: 1,
        fileCount: 0,
        packCount: 1,
        symlinkCount: 0,
        totalBytes: 0,
      },
      workspace: {
        job: "test",
        repo: "eersnington/stateful-ci",
        workflow: "ci.yml",
      },
    };

    expect(Schema.decodeUnknownSync(SnapshotManifest)(manifest)).toStrictEqual(
      manifest
    );
  });

  test("manifest rejects unsafe paths and mismatched content keys", () => {
    for (const unsafePath of [
      ".",
      "./cache",
      "cache/.",
      "cache//entry",
      "../outside",
    ]) {
      expect(
        Result.isFailure(
          decodeSnapshotManifestEntry({
            mode: 493,
            mtime: 1,
            path: unsafePath,
            type: "directory",
          })
        )
      ).toBeTruthy();
    }

    for (const target of [
      "/tmp/cache",
      "C:/cache",
      "..\\cache",
      "bad\0target",
    ]) {
      expect(
        Result.isFailure(
          decodeSnapshotManifestEntry({
            path: "node_modules/.bin/tool",
            target,
            type: "symlink",
          })
        )
      ).toBeTruthy();
    }

    expect(
      Result.isFailure(
        decodeSnapshotManifest({
          createdAt: "2026-05-20T00:00:00.000Z",
          entries: [],
          formatVersion: 1,
          managedRoots: [".turbo"],
          objects: [objects[1]],
          provenance: {
            git: {
              baseRef: null,
              headRef: null,
              headRepo: null,
              ref: "refs/heads/main",
              sha: "abc123",
            },
            github: { actor: "eersnington", event: "push", runId: "123456789" },
            runId: "123456789",
          },
          safety: {
            skippedByBuiltInDenylist: 0,
            skippedByUserExclude: 0,
            skippedUnsupportedType: 0,
          },
          snapshotId: "snap_124",
          stats: {
            chunkCount: 0,
            directoryCount: 0,
            fileCount: 0,
            packCount: 1,
            symlinkCount: 0,
            totalBytes: 0,
          },
          workspace: {
            job: "test",
            repo: "eersnington/stateful-ci",
            workflow: "ci.yml",
          },
        })
      )
    ).toBeTruthy();
  });

  test("Sha256Digest rejects non-canonical digest text", () => {
    expect(Schema.decodeUnknownSync(Sha256Digest)(manifestDigest)).toBe(
      manifestDigest
    );
    expect(
      Result.isFailure(
        Schema.decodeUnknownResult(Sha256Digest)(
          "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        )
      )
    ).toBeTruthy();
  });
});
