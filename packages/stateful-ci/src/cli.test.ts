import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { runCli } from "./cli";

const configFileName = "stateful-ci.json";

const runInit = () =>
  Effect.runPromise(runCli(["init"]).pipe(Effect.provide(NodeServices.layer)));

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
      '{\n  "preset": "node"\n}\n'
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
