import { once } from "node:events";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { NodeServices } from "@effect/platform-node";
import { configFileName } from "@stateful-ci/core";
import { Effect } from "effect";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { restoreProgram, runCli, saveProgram } from "./cli";

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

const withProtocolServer = async <A>(
  handler: (
    request: IncomingMessage,
    response: ServerResponse
  ) => Promise<void>,
  run: (url: string) => Promise<A>
) => {
  const server = createServer(async (request, response) => {
    try {
      await handler(request, response);
    } catch (error) {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(await Response.json({ error: String(error) }).text());
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
          await Response.json({
            decision: "denied",
            reason: "backend_policy_not_configured",
            save: { allowed: false },
            trustClass: "unknown",
          }).text()
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
});

describe("stateful-ci save", () => {
  let previousCwd = process.cwd();
  let tempDir = "";

  beforeEach(async () => {
    previousCwd = process.cwd();
    tempDir = await mkdtemp(join(tmpdir(), "stateful-ci-"));
    process.chdir(tempDir);
    await mkdir(join(tempDir, ".turbo/cache"), { recursive: true });
    await writeFile(
      configFileName,
      '{"paths":[".turbo",".env"],"exclude":[".turbo/cache/skip.txt"]}\n'
    );
    await writeFile(join(tempDir, ".turbo/cache/result.txt"), "cached output");
    await writeFile(join(tempDir, ".turbo/cache/skip.txt"), "ignored output");
    await writeFile(join(tempDir, ".env"), "SECRET=value");
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
          await Response.json({
            decision: "denied",
            reason: "backend_policy_not_configured",
          }).text()
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
        chunkCount: 0,
        fileCount: 1,
        safety: {
          skippedByBuiltInDenylist: 1,
          skippedByUserExclude: 1,
        },
        totalBytes: 13,
      },
      runId: "123456789",
    });
  });
});
