import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { NodeServices } from "@effect/platform-node";
import { StatefulCiConfig } from "@stateful-ci/core";
import { Effect, Schema } from "effect";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { configFileName, runCli, writeDefaultConfig } from "./cli";

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

  test("writes the default node config", async () => {
    const path = await Effect.runPromise(writeDefaultConfig(tempDir));

    expect(path).toBe(join(tempDir, configFileName));
    expect(JSON.parse(await readFile(path, "utf-8"))).toStrictEqual({
      preset: "node",
    });
    expect(
      Schema.decodeUnknownSync(StatefulCiConfig)(
        JSON.parse(await readFile(path, "utf-8"))
      )
    ).toStrictEqual({ preset: "node" });
  });

  test("does not overwrite an existing config", async () => {
    const path = join(tempDir, configFileName);

    await writeFile(path, '{"paths":[".turbo"]}\n');

    await expect(
      Effect.runPromise(writeDefaultConfig(tempDir))
    ).rejects.toMatchObject({
      _tag: "ConfigAlreadyExists",
      path,
    });
    await expect(readFile(path, "utf-8")).resolves.toBe(
      '{"paths":[".turbo"]}\n'
    );
  });

  test("registers init through the Effect CLI command", async () => {
    await mkdir(join(tempDir, "workspace"));
    process.chdir(join(tempDir, "workspace"));

    await Effect.runPromise(
      runCli(["init"]).pipe(Effect.provide(NodeServices.layer))
    );

    expect(
      JSON.parse(await readFile(join(process.cwd(), configFileName), "utf-8"))
    ).toStrictEqual({ preset: "node" });
  });
});
