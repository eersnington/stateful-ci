#!/usr/bin/env node
import { constants } from "node:fs";
import { access, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { defaultConfig, StatefulCiConfig } from "@stateful-ci/core";
import { Console, Effect, Schema } from "effect";
import { Command } from "effect/unstable/cli";

export const configFileName = "stateful-ci.json";

interface ConfigAlreadyExists {
  readonly _tag: "ConfigAlreadyExists";
  readonly path: string;
}

const pathExists = (path: string) =>
  Effect.promise(async () => {
    try {
      await access(path, constants.F_OK);
      return true;
    } catch {
      return false;
    }
  });

export const writeDefaultConfig = (directory: string) =>
  Effect.gen(function* writeDefaultConfigEffect() {
    const path = join(directory, configFileName);

    if (yield* pathExists(path)) {
      return yield* Effect.fail({ _tag: "ConfigAlreadyExists", path } as const);
    }

    yield* Effect.tryPromise({
      catch: (cause) => ({ _tag: "ConfigWriteFailed", cause, path }) as const,
      try: () =>
        writeFile(
          path,
          `${JSON.stringify(
            Schema.encodeUnknownSync(StatefulCiConfig)(defaultConfig),
            null,
            2
          )}\n`,
          { flag: "wx" }
        ),
    });

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
