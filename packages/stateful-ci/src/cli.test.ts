import { createHash } from "node:crypto";
import { once } from "node:events";
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { NodeServices } from "@effect/platform-node";
import {
  configFileName,
  RestoreAllowedResponse,
  RestoreDeniedResponse,
  SaveDeniedResponse,
  StatefulCiConfig,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { restoreProgram, runCli, saveProgram } from "./cli";
import {
  createSnapshotArchive,
  defaultLocalStorePath,
  restoreSnapshotArchive,
} from "./snapshot-archive";

const runInit = () =>
  Effect.runPromise(runCli(["init"]).pipe(Effect.provide(NodeServices.layer)));

const githubEnv = {
  GITHUB_ACTOR: "eersnington",
  GITHUB_EVENT_NAME: "push",
  GITHUB_JOB: "test",
  GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "eersnington/stateful-ci",
  GITHUB_RUN_ID: "123456789",
  GITHUB_SHA: "abc123",
  GITHUB_WORKFLOW: "ci.yml",
};

const sha256 = (bytes: Buffer | string) =>
  `sha256:${createHash("sha256").update(bytes).digest("hex")}`;

const archiveKeyForDigest = (digest: string) =>
  `archives/sha256-${digest.slice("sha256:".length)}.sciar`;

const manifestKeyForDigest = (digest: string) =>
  `manifests/sha256-${digest.slice("sha256:".length)}.json`;

const withProtocolServer = async <A>(
  handler: (
    request: IncomingMessage,
    response: ServerResponse
  ) => Promise<void> | void,
  run: (url: string) => Promise<A>
) => {
  const server = createServer(async (request, response) => {
    try {
      await handler(request, response);
    } catch (error) {
      response.writeHead(500, { "content-type": "text/plain" });
      response.end(String(error));
    }
  });

  server.listen(0, "127.0.0.1");
  await once(server, "listening");

  const address = server.address();

  if (address === null || typeof address === "string") {
    throw new Error("Test server did not bind to a TCP address.");
  }

  try {
    return await run(`http://127.0.0.1:${address.port}`);
  } finally {
    server.close();
    await once(server, "close");
  }
};

describe("stateful-ci init", () => {
  let previousCwd = process.cwd();
  let tempDir = "";

  beforeEach(async () => {
    previousCwd = process.cwd();
    tempDir = await mkdtemp(join(tmpdir(), "stateful-ci-"));
  });

  afterEach(async () => {
    process.chdir(previousCwd);
    await rm(tempDir, { force: true, recursive: true });
  });

  test("init writes the default node config", async () => {
    await mkdir(join(tempDir, "workspace"));
    process.chdir(join(tempDir, "workspace"));

    await runInit();

    await expect(readFile(configFileName, "utf-8")).resolves.toBe(
      '{"preset":"node"}\n'
    );
  });

  test("init does not overwrite an existing config", async () => {
    const path = join(tempDir, configFileName);

    process.chdir(tempDir);
    await writeFile(path, '{"paths":[".turbo"]}\n');

    await expect(runInit()).rejects.toBeDefined();
    await expect(readFile(path, "utf-8")).resolves.toBe(
      '{"paths":[".turbo"]}\n'
    );
  });
});

describe("stateful-ci restore", () => {
  let previousCwd = process.cwd();
  let tempDir = "";

  beforeEach(async () => {
    previousCwd = process.cwd();
    tempDir = await mkdtemp(join(tmpdir(), "stateful-ci-"));
    process.chdir(tempDir);
    await writeFile(configFileName, '{"preset":"node"}\n');
  });

  afterEach(async () => {
    process.chdir(previousCwd);
    await rm(tempDir, { force: true, recursive: true });
  });

  test("restore sends a valid protocol request and handles denial", async () => {
    let body: unknown;

    await withProtocolServer(
      async (request, response) => {
        expect(request.url).toBe("/v1/restore");
        expect(request.headers.authorization).toBe("Bearer test-token");
        body = await new Response(request).json();
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          Schema.encodeUnknownSync(
            Schema.fromJsonString(RestoreDeniedResponse)
          )({
            decision: "denied",
            reason: "backend_policy_not_configured",
            save: { allowed: false },
            trustClass: "unknown",
          })
        );
      },
      (url) =>
        Effect.runPromise(
          restoreProgram({
            ...githubEnv,
            STATEFUL_CI_API_TOKEN: "test-token",
            STATEFUL_CI_API_URL: url,
          }).pipe(Effect.provide(NodeServices.layer))
        )
    );

    expect(body).toMatchObject({
      git: { ref: "refs/heads/main", sha: "abc123" },
      github: { actor: "eersnington", event: "push", runId: "123456789" },
      workspace: {
        job: "test",
        repo: "eersnington/stateful-ci",
        workflow: "ci.yml",
      },
    });
  });

  test("restore fails before network calls when API URL is missing", async () => {
    await expect(
      Effect.runPromise(
        restoreProgram({
          ...githubEnv,
          STATEFUL_CI_API_TOKEN: "test-token",
        }).pipe(Effect.provide(NodeServices.layer))
      )
    ).rejects.toMatchObject({ _tag: "CliFailure" });
  });

  test("restore reports reachable backend HTTP errors separately from network failures", async () => {
    await withProtocolServer(
      (_request, response) => {
        response.writeHead(403, { "content-type": "text/plain" });
        response.end("forbidden");
      },
      async (url) => {
        await expect(
          Effect.runPromise(
            restoreProgram({
              ...githubEnv,
              STATEFUL_CI_API_TOKEN: "test-token",
              STATEFUL_CI_API_URL: url,
            }).pipe(Effect.provide(NodeServices.layer))
          )
        ).rejects.toMatchObject({
          _tag: "CliFailure",
          message: expect.stringContaining("HTTP 403"),
        });
      }
    );
  });

  test("restore reports unreachable backends as network failures", async () => {
    const server = createServer();

    server.listen(0, "127.0.0.1");
    await once(server, "listening");

    const address = server.address();

    if (address === null || typeof address === "string") {
      throw new Error("Test server did not bind to a TCP address.");
    }

    const url = `http://127.0.0.1:${address.port}`;

    server.close();
    await once(server, "close");

    await expect(
      Effect.runPromise(
        restoreProgram({
          ...githubEnv,
          STATEFUL_CI_API_TOKEN: "test-token",
          STATEFUL_CI_API_URL: url,
        }).pipe(Effect.provide(NodeServices.layer))
      )
    ).rejects.toMatchObject({
      _tag: "CliFailure",
      message: expect.stringContaining("Could not reach Stateful CI backend"),
    });
  });
});

describe("stateful-ci save", () => {
  let previousCwd = process.cwd();
  let tempDir = "";

  beforeEach(async () => {
    previousCwd = process.cwd();
    tempDir = await mkdtemp(join(tmpdir(), "stateful-ci-"));
    process.chdir(tempDir);
    await mkdir(join(tempDir, ".turbo/cache"), { recursive: true });
    await mkdir(join(tempDir, ".aws"), { recursive: true });
    await writeFile(
      configFileName,
      '{"paths":[".turbo",".env",".aws","linked-cache"],"exclude":[".turbo/cache/skip.txt"]}\n'
    );
    await writeFile(join(tempDir, ".turbo/cache/result.txt"), "cached output");
    await writeFile(join(tempDir, ".turbo/cache/skip.txt"), "ignored output");
    await writeFile(join(tempDir, ".env"), "SECRET=value");
    await writeFile(join(tempDir, ".aws/credentials"), "SECRET=value");
    await symlink(join(tempDir, ".turbo/cache/result.txt"), "linked-cache");
  });

  afterEach(async () => {
    process.chdir(previousCwd);
    await rm(tempDir, { force: true, recursive: true });
  });

  test("save scans configured paths and sends manifest metadata", async () => {
    let body: unknown;

    await withProtocolServer(
      async (request, response) => {
        expect(request.url).toBe("/v1/save");
        body = await new Response(request).json();
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          Schema.encodeUnknownSync(Schema.fromJsonString(SaveDeniedResponse))({
            decision: "denied",
            reason: "backend_policy_not_configured",
          })
        );
      },
      (url) =>
        Effect.runPromise(
          saveProgram({
            ...githubEnv,
            STATEFUL_CI_API_TOKEN: "test-token",
            STATEFUL_CI_API_URL: url,
          }).pipe(Effect.provide(NodeServices.layer))
        )
    );

    expect(body).toMatchObject({
      baseSnapshotId: null,
      manifest: {
        archiveKey: expect.stringMatching(
          /^archives\/sha256-[a-f0-9]{64}\.sciar$/u
        ),
        chunkCount: 1,
        fileCount: 1,
        manifestDigest: expect.stringMatching(/^sha256:[a-f0-9]{64}$/u),
        safety: {
          skippedByBuiltInDenylist: 2,
          skippedByUserExclude: 1,
          skippedUnsupportedType: 1,
        },
        totalBytes: 13,
      },
      runId: "123456789",
    });
  });

  test("save creates a local archive that restore can round trip", async () => {
    const savedManifest: {
      current: {
        readonly id: string;
        readonly key: string;
        readonly manifestDigest: string;
      } | null;
    } = { current: null };

    await withProtocolServer(
      async (request, response) => {
        expect(request.url).toBe("/v1/save");
        const body = await new Response(request).json();

        if (
          typeof body === "object" &&
          body !== null &&
          "manifest" in body &&
          typeof body.manifest === "object" &&
          body.manifest !== null &&
          "id" in body.manifest &&
          "key" in body.manifest &&
          "manifestDigest" in body.manifest &&
          typeof body.manifest.id === "string" &&
          typeof body.manifest.key === "string" &&
          typeof body.manifest.manifestDigest === "string"
        ) {
          savedManifest.current = {
            id: body.manifest.id,
            key: body.manifest.key,
            manifestDigest: body.manifest.manifestDigest,
          };
        }

        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          Schema.encodeUnknownSync(Schema.fromJsonString(SaveDeniedResponse))({
            decision: "denied",
            reason: "backend_policy_not_configured",
          })
        );
      },
      (url) =>
        Effect.runPromise(
          saveProgram({
            ...githubEnv,
            STATEFUL_CI_API_TOKEN: "test-token",
            STATEFUL_CI_API_URL: url,
          }).pipe(Effect.provide(NodeServices.layer))
        )
    );

    const capturedManifest = savedManifest.current;

    if (capturedManifest === null) {
      throw new Error("Save request did not include manifest metadata.");
    }

    await rm(join(tempDir, ".turbo"), { force: true, recursive: true });

    await withProtocolServer(
      (_request, response) => {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          Schema.encodeUnknownSync(
            Schema.fromJsonString(RestoreAllowedResponse)
          )({
            decision: "allowed",
            save: { allowed: true, target: "trusted/main/ci/test" },
            snapshot: {
              id: capturedManifest.id,
              manifestDigest: capturedManifest.manifestDigest,
              manifestKey: capturedManifest.key,
              parent: null,
            },
            trustClass: "trusted",
            workspaceId: "ws_test",
          })
        );
      },
      (url) =>
        Effect.runPromise(
          restoreProgram({
            ...githubEnv,
            STATEFUL_CI_API_TOKEN: "test-token",
            STATEFUL_CI_API_URL: url,
          }).pipe(Effect.provide(NodeServices.layer))
        )
    );

    await expect(
      readFile(join(tempDir, ".turbo/cache/result.txt"), "utf-8")
    ).resolves.toBe("cached output");
    await expect(
      readFile(join(tempDir, ".turbo/cache/skip.txt"), "utf-8")
    ).rejects.toBeDefined();
    await expect(readFile(join(tempDir, ".env"), "utf-8")).resolves.toBe(
      "SECRET=value"
    );
  });

  test("restore rejects malicious archive paths", async () => {
    const storeRoot = defaultLocalStorePath(tempDir);
    const payload = Buffer.from("evil");
    const payloadDigest = sha256(payload);
    const entries = JSON.stringify([
      { path: "../outside.txt", sha256: payloadDigest, size: payload.length },
    ]);
    const archive = Buffer.concat([
      Buffer.from(
        `stateful-ci-archive-v1\n${Buffer.byteLength(entries)}\n${entries}`
      ),
      payload,
    ]);
    const archiveDigest = sha256(archive);
    const archiveKey = archiveKeyForDigest(archiveDigest);
    const manifest = `${JSON.stringify({
      archiveDigest,
      archiveKey,
      entries: [{ path: "../outside.txt", sha256: payloadDigest, size: 4 }],
      formatVersion: 1,
      snapshotId: "snap_bad",
      stats: { fileCount: 1, totalBytes: 4 },
    })}\n`;
    const manifestDigest = sha256(manifest);
    const manifestKey = manifestKeyForDigest(manifestDigest);

    await mkdir(join(storeRoot, "archives"), { recursive: true });
    await mkdir(join(storeRoot, "manifests"), { recursive: true });
    await writeFile(join(storeRoot, archiveKey), archive);
    await writeFile(join(storeRoot, manifestKey), manifest);

    await expect(
      restoreSnapshotArchive({
        manifestDigest,
        manifestKey,
        root: tempDir,
        snapshotId: "snap_bad",
        storeRoot,
      })
    ).rejects.toThrow(/unsafe/u);
    await expect(
      readFile(join(tempDir, "../outside.txt"), "utf-8")
    ).rejects.toBeDefined();
  });

  test("restore rejects invalid store keys and protected paths", async () => {
    const storeRoot = defaultLocalStorePath(tempDir);
    const payload = Buffer.from("SECRET=value");
    const payloadDigest = sha256(payload);
    const entries = JSON.stringify([
      { path: ".env", sha256: payloadDigest, size: payload.length },
    ]);
    const archive = Buffer.concat([
      Buffer.from(
        `stateful-ci-archive-v1\n${Buffer.byteLength(entries)}\n${entries}`
      ),
      payload,
    ]);
    const archiveDigest = sha256(archive);
    const archiveKey = archiveKeyForDigest(archiveDigest);
    const manifest = `${JSON.stringify({
      archiveDigest,
      archiveKey,
      entries: [{ path: ".env", sha256: payloadDigest, size: payload.length }],
      formatVersion: 1,
      snapshotId: "snap_env",
      stats: { fileCount: 1, totalBytes: payload.length },
    })}\n`;
    const manifestDigest = sha256(manifest);
    const manifestKey = manifestKeyForDigest(manifestDigest);

    await mkdir(join(storeRoot, "archives"), { recursive: true });
    await mkdir(join(storeRoot, "manifests"), { recursive: true });
    await writeFile(join(storeRoot, archiveKey), archive);
    await writeFile(join(storeRoot, manifestKey), manifest);

    await expect(
      restoreSnapshotArchive({
        manifestDigest,
        manifestKey: "../manifests/snap_env.json",
        root: tempDir,
        snapshotId: "snap_env",
        storeRoot,
      })
    ).rejects.toThrow(/manifestKey/u);
    await expect(
      restoreSnapshotArchive({
        manifestDigest,
        manifestKey,
        root: tempDir,
        snapshotId: "snap_env",
        storeRoot,
      })
    ).rejects.toThrow(/protected/u);
  });

  test("restore rejects symlink parent escapes", async () => {
    const outsideDir = await mkdtemp(join(tmpdir(), "stateful-ci-outside-"));

    try {
      const storeRoot = defaultLocalStorePath(tempDir);
      const payload = Buffer.from("evil");
      const payloadDigest = sha256(payload);
      const entries = JSON.stringify([
        {
          path: "linked-dir/outside.txt",
          sha256: payloadDigest,
          size: payload.length,
        },
      ]);
      const archive = Buffer.concat([
        Buffer.from(
          `stateful-ci-archive-v1\n${Buffer.byteLength(entries)}\n${entries}`
        ),
        payload,
      ]);
      const archiveDigest = sha256(archive);
      const archiveKey = archiveKeyForDigest(archiveDigest);
      const manifest = `${JSON.stringify({
        archiveDigest,
        archiveKey,
        entries: [
          {
            path: "linked-dir/outside.txt",
            sha256: payloadDigest,
            size: 4,
          },
        ],
        formatVersion: 1,
        snapshotId: "snap_link",
        stats: { fileCount: 1, totalBytes: 4 },
      })}\n`;
      const manifestDigest = sha256(manifest);
      const manifestKey = manifestKeyForDigest(manifestDigest);

      await symlink(outsideDir, join(tempDir, "linked-dir"));
      await mkdir(join(storeRoot, "archives"), { recursive: true });
      await mkdir(join(storeRoot, "manifests"), { recursive: true });
      await writeFile(join(storeRoot, archiveKey), archive);
      await writeFile(join(storeRoot, manifestKey), manifest);

      await expect(
        restoreSnapshotArchive({
          manifestDigest,
          manifestKey,
          root: tempDir,
          snapshotId: "snap_link",
          storeRoot,
        })
      ).rejects.toThrow(/safe directory|outside the workspace/u);
      await expect(
        readFile(join(outsideDir, "outside.txt"), "utf-8")
      ).rejects.toBeDefined();
    } finally {
      await rm(outsideDir, { force: true, recursive: true });
    }
  });

  test("restore rejects a tampered local manifest", async () => {
    const savedManifest: {
      current: {
        readonly id: string;
        readonly key: string;
        readonly manifestDigest: string;
      } | null;
    } = { current: null };

    await withProtocolServer(
      async (request, response) => {
        const body = await new Response(request).json();

        if (
          typeof body === "object" &&
          body !== null &&
          "manifest" in body &&
          typeof body.manifest === "object" &&
          body.manifest !== null &&
          "id" in body.manifest &&
          "key" in body.manifest &&
          "manifestDigest" in body.manifest &&
          typeof body.manifest.id === "string" &&
          typeof body.manifest.key === "string" &&
          typeof body.manifest.manifestDigest === "string"
        ) {
          savedManifest.current = {
            id: body.manifest.id,
            key: body.manifest.key,
            manifestDigest: body.manifest.manifestDigest,
          };
        }

        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          Schema.encodeUnknownSync(Schema.fromJsonString(SaveDeniedResponse))({
            decision: "denied",
            reason: "backend_policy_not_configured",
          })
        );
      },
      (url) =>
        Effect.runPromise(
          saveProgram({
            ...githubEnv,
            STATEFUL_CI_API_TOKEN: "test-token",
            STATEFUL_CI_API_URL: url,
          }).pipe(Effect.provide(NodeServices.layer))
        )
    );

    const capturedManifest = savedManifest.current;

    if (capturedManifest === null) {
      throw new Error("Save request did not include manifest metadata.");
    }

    await writeFile(
      join(defaultLocalStorePath(tempDir), capturedManifest.key),
      "{}\n"
    );

    await withProtocolServer(
      (_request, response) => {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          Schema.encodeUnknownSync(
            Schema.fromJsonString(RestoreAllowedResponse)
          )({
            decision: "allowed",
            save: { allowed: true, target: "trusted/main/ci/test" },
            snapshot: {
              id: capturedManifest.id,
              manifestDigest: capturedManifest.manifestDigest,
              manifestKey: capturedManifest.key,
              parent: null,
            },
            trustClass: "trusted",
            workspaceId: "ws_test",
          })
        );
      },
      (url) =>
        expect(
          Effect.runPromise(
            restoreProgram({
              ...githubEnv,
              STATEFUL_CI_API_TOKEN: "test-token",
              STATEFUL_CI_API_URL: url,
            }).pipe(Effect.provide(NodeServices.layer))
          )
        ).rejects.toMatchObject({ _tag: "CliFailure" })
    );
  });

  test("save rejects symlink parent paths that escape the workspace", async () => {
    const outsideDir = await mkdtemp(join(tmpdir(), "stateful-ci-outside-"));

    try {
      await mkdir(join(outsideDir, "cache"), { recursive: true });
      await writeFile(join(outsideDir, "cache/result.txt"), "outside");
      await symlink(outsideDir, join(tempDir, "linked-dir"));

      await expect(
        createSnapshotArchive({
          config: Schema.decodeSync(StatefulCiConfig)({
            paths: ["linked-dir/cache"],
          }),
          identity: {
            configHash: "sha256:config",
            gitSha: "abc123",
            job: "test",
            repo: "eersnington/stateful-ci",
            runId: "123456789",
            workflow: "ci.yml",
          },
          root: tempDir,
          storeRoot: defaultLocalStorePath(tempDir),
        })
      ).rejects.toThrow(/safe directory|outside the workspace/u);
    } finally {
      await rm(outsideDir, { force: true, recursive: true });
    }
  });

  test("save refuses archives over the local restore cap", async () => {
    await expect(
      createSnapshotArchive({
        config: Schema.decodeSync(StatefulCiConfig)({ paths: [".turbo"] }),
        identity: {
          configHash: "sha256:config",
          gitSha: "abc123",
          job: "test",
          repo: "eersnington/stateful-ci",
          runId: "123456789",
          workflow: "ci.yml",
        },
        maxArchiveBytes: 16,
        root: tempDir,
        storeRoot: defaultLocalStorePath(tempDir),
      })
    ).rejects.toThrow(/too large/u);
  });
});
