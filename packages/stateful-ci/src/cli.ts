#!/usr/bin/env node
import { pathToFileURL } from "node:url";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { defaultConfig, StatefulCiConfig } from "@stateful-ci/core";
import type { PlatformError } from "effect";
import { Console, Effect, FileSystem, Path, Schema } from "effect";
import { Command } from "effect/unstable/cli";

export const configFileName = "stateful-ci.json";

interface ConfigAlreadyExists {
  readonly _tag: "ConfigAlreadyExists";
  readonly path: string;
}

interface ConfigWriteFailed {
  readonly _tag: "ConfigWriteFailed";
  readonly cause: PlatformError.PlatformError;
  readonly path: string;
}

const configText = `${JSON.stringify(
  Schema.encodeUnknownSync(StatefulCiConfig)(defaultConfig),
  null,
  2
)}\n`;

const configWriteError = (
  path: string,
  cause: PlatformError.PlatformError
): ConfigAlreadyExists | ConfigWriteFailed =>
  cause.reason._tag === "AlreadyExists"
    ? { _tag: "ConfigAlreadyExists", path }
    : { _tag: "ConfigWriteFailed", cause, path };

const writeDefaultConfig = (directory: string) =>
  Effect.gen(function* writeDefaultConfigEffect() {
    const fs = yield* FileSystem.FileSystem;
    const path = (yield* Path.Path).join(directory, configFileName);

    yield* fs
      .writeFileString(path, configText, { flag: "wx" })
      .pipe(Effect.mapError((cause) => configWriteError(path, cause)));

    return path;
  });

const failExistingConfig = (error: ConfigAlreadyExists) =>
  Console.error(
    `${configFileName} already exists at ${error.path}; leaving it unchanged.`
  ).pipe(Effect.flatMap(() => Effect.fail(error)));

export const initCommand = Command.make("init", {}, () =>
  writeDefaultConfig(process.cwd()).pipe(
    Effect.tap((path) => Console.log(`Created ${path}`)),
    Effect.catchTag("ConfigAlreadyExists", failExistingConfig)
  )
).pipe(Command.withDescription("Create a Stateful CI workspace config"));

export const command = Command.make("stateful-ci").pipe(
  Command.withDescription("Persistent, trust-aware CI workspace snapshots"),
  Command.withSubcommands([initCommand])
);

export const runCli = Command.runWith(command, {
  version: "0.0.1",
});

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runCli(process.argv.slice(2)).pipe(
    Effect.provide(NodeServices.layer),
    NodeRuntime.runMain
  );
}
