import { createHash } from "node:crypto";

import {
  configFileName,
  defaultConfig,
  StatefulCiConfig,
} from "@stateful-ci/core";
import type { StatefulCiConfigType } from "@stateful-ci/core";
import type { PlatformError } from "effect";
import { Console, Effect, Exit, FileSystem, Path, Schema } from "effect";

import { cliFailure } from "./failure";

interface ConfigAlreadyExists {
  readonly _tag: "ConfigAlreadyExists";
  readonly path: string;
}

interface ConfigWriteFailed {
  readonly _tag: "ConfigWriteFailed";
  readonly cause: PlatformError.PlatformError;
  readonly path: string;
}

export interface LoadedConfig {
  readonly config: StatefulCiConfigType;
  readonly hash: string;
}

const configText = `${Schema.encodeUnknownSync(
  Schema.fromJsonString(StatefulCiConfig)
)(defaultConfig)}\n`;

const configWriteError = (
  path: string,
  cause: PlatformError.PlatformError
): ConfigAlreadyExists | ConfigWriteFailed =>
  cause.reason._tag === "AlreadyExists"
    ? { _tag: "ConfigAlreadyExists", path }
    : { _tag: "ConfigWriteFailed", cause, path };

export const writeDefaultConfig = Effect.fn("writeDefaultConfig")(
  function* writeDefaultConfigEffect(directory: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = (yield* Path.Path).join(directory, configFileName);

    yield* fs
      .writeFileString(path, configText, { flag: "wx" })
      .pipe(Effect.mapError((cause) => configWriteError(path, cause)));

    return path;
  }
);

export const failExistingConfig = (error: ConfigAlreadyExists) =>
  Console.error(
    `${configFileName} already exists at ${error.path}; leaving it unchanged.`
  ).pipe(Effect.flatMap(() => Effect.fail(error)));

export const loadConfig = Effect.fn("loadConfig")(function* loadConfigEffect(
  directory: string
) {
  const path = (yield* Path.Path).join(directory, configFileName);
  const source = yield* (yield* FileSystem.FileSystem)
    .readFileString(path)
    .pipe(
      Effect.mapError(() =>
        cliFailure(
          `Could not read ${configFileName} at ${path}. Run stateful-ci init first.`
        )
      )
    );
  const decoded = Schema.decodeUnknownExit(
    Schema.fromJsonString(StatefulCiConfig)
  )(source);

  if (Exit.isFailure(decoded)) {
    return yield* Effect.fail(
      cliFailure(
        `${configFileName} does not match the Stateful CI config schema. Run stateful-ci init or fix the configured paths.`
      )
    );
  }

  return {
    config: decoded.value,
    hash: `sha256:${createHash("sha256").update(source).digest("hex")}`,
  } satisfies LoadedConfig;
});
