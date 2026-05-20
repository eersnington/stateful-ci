#!/usr/bin/env node
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import type { StatefulCiConfigType } from "@stateful-ci/core";
import {
  clientVersion,
  configFileName,
  defaultConfig,
  protocolVersion,
  RestoreRequest,
  RestoreResponse,
  routes,
  SaveRequest,
  SaveResponse,
  StatefulCiConfig,
  workspacePathsForConfig,
} from "@stateful-ci/core";
import type { PlatformError } from "effect";
import { Console, Effect, Exit, FileSystem, Path, Schema } from "effect";
import { Command } from "effect/unstable/cli";

import {
  createWorkspaceSnapshot,
  restoreWorkspaceSnapshot,
} from "./snapshot-engine";

interface ConfigAlreadyExists {
  readonly _tag: "ConfigAlreadyExists";
  readonly path: string;
}

interface ConfigWriteFailed {
  readonly _tag: "ConfigWriteFailed";
  readonly cause: PlatformError.PlatformError;
  readonly path: string;
}

interface CliFailure {
  readonly _tag: "CliFailure";
  readonly message: string;
}

type RuntimeEnv = Readonly<Record<string, string | undefined>>;

interface LoadedConfig {
  readonly config: StatefulCiConfigType;
  readonly hash: string;
}

interface ApiConfig {
  readonly token: string;
  readonly url: string;
}

interface PreparedSaveRequest {
  readonly request: SaveRequest;
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

const cliFailure = (message: string): CliFailure => ({
  _tag: "CliFailure",
  message,
});

const failCliFailure = (error: CliFailure) =>
  Console.error(error.message).pipe(Effect.flatMap(() => Effect.fail(error)));

const sha256 = (source: string) =>
  `sha256:${createHash("sha256").update(source).digest("hex")}`;

const optionalEnv = (env: RuntimeEnv, key: string) => {
  const value = env[key];
  return value === undefined || value.length === 0 ? null : value;
};

const requiredEnv = (env: RuntimeEnv, key: string) => {
  const value = env[key];
  return value === undefined || value.length === 0
    ? Effect.fail(
        cliFailure(
          `Missing ${key}. Set ${key} before running stateful-ci in CI.`
        )
      )
    : Effect.succeed(value);
};

const loadConfig = (directory: string) =>
  Effect.gen(function* loadConfigEffect() {
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
      hash: sha256(source),
    } satisfies LoadedConfig;
  });

const apiConfigFromEnv = (env: RuntimeEnv) =>
  Effect.gen(function* apiConfigFromEnvEffect() {
    return {
      token: yield* requiredEnv(env, "STATEFUL_CI_API_TOKEN"),
      url: yield* requiredEnv(env, "STATEFUL_CI_API_URL"),
    } satisfies ApiConfig;
  });

const restoreRequestFromEnv = (env: RuntimeEnv, loaded: LoadedConfig) =>
  Effect.gen(function* restoreRequestFromEnvEffect() {
    const request = {
      client: { configHash: loaded.hash, version: clientVersion },
      git: {
        baseRef: optionalEnv(env, "GITHUB_BASE_REF"),
        headRef: optionalEnv(env, "GITHUB_HEAD_REF"),
        headRepo: optionalEnv(env, "GITHUB_HEAD_REPOSITORY"),
        ref: yield* requiredEnv(env, "GITHUB_REF"),
        sha: yield* requiredEnv(env, "GITHUB_SHA"),
      },
      github: {
        actor: yield* requiredEnv(env, "GITHUB_ACTOR"),
        event: yield* requiredEnv(env, "GITHUB_EVENT_NAME"),
        runId: yield* requiredEnv(env, "GITHUB_RUN_ID"),
      },
      identity: {
        provider: "github-actions",
        token: yield* requiredEnv(env, "STATEFUL_CI_OIDC_TOKEN"),
      },
      managedRoots: workspacePathsForConfig(loaded.config),
      protocolVersion,
      workspace: {
        job: yield* requiredEnv(env, "GITHUB_JOB"),
        repo: yield* requiredEnv(env, "GITHUB_REPOSITORY"),
        workflow: yield* requiredEnv(env, "GITHUB_WORKFLOW"),
      },
    };
    const decoded = Schema.decodeUnknownExit(RestoreRequest)(request);

    return Exit.isFailure(decoded)
      ? yield* Effect.fail(
          cliFailure(
            "GitHub environment variables did not produce a valid restore request. Check the Actions runtime context."
          )
        )
      : decoded.value;
  });

const protocolUrl = (api: ApiConfig, route: string) =>
  new URL(route, api.url.endsWith("/") ? api.url : `${api.url}/`).href;

const postProtocol = (api: ApiConfig, route: string, body: string) =>
  Effect.gen(function* postProtocolEffect() {
    const response = yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          `Could not reach Stateful CI backend at ${api.url}. Check STATEFUL_CI_API_URL and network access.`
        ),
      try: () =>
        fetch(protocolUrl(api, route), {
          body,
          headers: {
            authorization: `Bearer ${api.token}`,
            "content-type": "application/json",
          },
          method: "POST",
        }),
    });

    if (!response.ok) {
      return yield* Effect.fail(
        cliFailure(`Stateful CI backend returned HTTP ${response.status}.`)
      );
    }

    return yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          `Could not read Stateful CI backend response body after HTTP ${response.status}. Retry or check backend logs.`
        ),
      try: () => response.text(),
    });
  });

const decodeProtocolResponse = <A>(
  schema: Schema.Decoder<A>,
  source: string
) => {
  const decoded = Schema.decodeUnknownExit(Schema.fromJsonString(schema))(
    source
  );

  return Exit.isFailure(decoded)
    ? Effect.fail(
        cliFailure(
          "The Stateful CI backend returned a response that does not match protocol v1. Check client and backend versions."
        )
      )
    : Effect.succeed(decoded.value);
};

const saveRequestFromRestore = (
  restoreRequest: RestoreRequest,
  loaded: LoadedConfig
) =>
  Effect.gen(function* saveRequestFromRestoreEffect() {
    const snapshot = yield* createWorkspaceSnapshot({
      config: loaded.config,
      provenance: {
        git: restoreRequest.git,
        github: restoreRequest.github,
        runId: restoreRequest.github.runId,
      },
      workspace: restoreRequest.workspace,
      workspaceRoot: process.cwd(),
    }).pipe(
      Effect.mapError((error) =>
        cliFailure(
          `${error.message} Stateful CI did not send a save request because the local snapshot is incomplete.`
        )
      )
    );
    const request = {
      baseSnapshotId: null,
      manifest: snapshot.saveManifest,
      protocolVersion,
      runId: restoreRequest.github.runId,
      workspaceId: `ws_${sha256([restoreRequest.workspace.repo, restoreRequest.workspace.workflow, restoreRequest.workspace.job].join("\n")).slice("sha256:".length, "sha256:".length + 24)}`,
    };
    const decoded = Schema.decodeUnknownExit(SaveRequest)(request);

    return Exit.isFailure(decoded)
      ? yield* Effect.fail(
          cliFailure(
            "The scanned workspace did not produce a valid save request."
          )
        )
      : ({ request: decoded.value } satisfies PreparedSaveRequest);
  });

const printRestoreResponse = (response: RestoreResponse) =>
  response.decision === "denied"
    ? Console.log(
        `Restore denied: ${response.reason} (trust class: ${response.trustClass}). Save allowed: ${response.save.allowed ? "yes" : "no"}.`
      )
    : Console.log(
        `Restore allowed: snapshot ${response.snapshot.id} (${response.trustClass}).`
      );

const printSaveResponse = (response: SaveResponse) =>
  response.decision === "denied"
    ? Console.log(`Save denied: ${response.reason}.`)
    : Console.log(
        `Save committed: snapshot ${response.snapshotId} for workspace ${response.workspaceId}. Latest: ${response.latest ? "yes" : "no"}.`
      );

export const restoreProgram = (env: RuntimeEnv) =>
  Effect.gen(function* restoreProgramEffect() {
    const loaded = yield* loadConfig(process.cwd());
    const api = yield* apiConfigFromEnv(env);
    const request = yield* restoreRequestFromEnv(env, loaded);
    const responseText = yield* postProtocol(
      api,
      routes.restore.path,
      Schema.encodeUnknownSync(Schema.fromJsonString(RestoreRequest))(request)
    );
    const response = yield* decodeProtocolResponse(
      RestoreResponse,
      responseText
    );

    if (response.decision === "allowed") {
      yield* restoreWorkspaceSnapshot({
        manifest: response.manifest,
        workspaceRoot: process.cwd(),
      }).pipe(
        Effect.mapError((error) =>
          cliFailure(`${error.message} Restore did not mutate the workspace.`)
        )
      );
    }

    yield* printRestoreResponse(response);
  }).pipe(Effect.catchTag("CliFailure", failCliFailure));

export const saveProgram = (env: RuntimeEnv) =>
  Effect.gen(function* saveProgramEffect() {
    const loaded = yield* loadConfig(process.cwd());
    const api = yield* apiConfigFromEnv(env);
    const restoreRequest = yield* restoreRequestFromEnv(env, loaded);
    const prepared = yield* saveRequestFromRestore(restoreRequest, loaded);
    const responseText = yield* postProtocol(
      api,
      routes.save.path,
      Schema.encodeUnknownSync(Schema.fromJsonString(SaveRequest))(
        prepared.request
      )
    );
    const response = yield* decodeProtocolResponse(SaveResponse, responseText);

    yield* printSaveResponse(response);
  }).pipe(Effect.catchTag("CliFailure", failCliFailure));

export const initCommand = Command.make("init", {}, () =>
  writeDefaultConfig(process.cwd()).pipe(
    Effect.tap((path) => Console.log(`Created ${path}`)),
    Effect.catchTag("ConfigAlreadyExists", failExistingConfig)
  )
).pipe(Command.withDescription("Create a Stateful CI workspace config"));

export const restoreCommand = Command.make("restore", {}, () =>
  restoreProgram(process.env)
).pipe(
  Command.withDescription(
    "Ask the backend what workspace snapshot is safe to restore"
  )
);

export const saveCommand = Command.make("save", {}, () =>
  saveProgram(process.env)
).pipe(
  Command.withDescription(
    "Scan configured workspace paths and ask the backend to commit metadata"
  )
);

export const command = Command.make("stateful-ci").pipe(
  Command.withDescription("Persistent, trust-aware CI workspace snapshots"),
  Command.withSubcommands([initCommand, restoreCommand, saveCommand])
);

export const runCli = Command.runWith(command, {
  version: clientVersion,
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
