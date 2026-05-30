import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";

import { Console, Effect, Exit, FileSystem, Path, Schema } from "effect";

import { cliFailure, failCliFailure } from "./failure";
import type { CliFailure } from "./failure";
import { optionalEnv, requiredDeployEnv } from "./github-actions";
import type { RuntimeEnv } from "./github-actions";

export type DeployStepRunner = (input: {
  readonly args: readonly string[];
  readonly stdin?: string;
}) => Effect.Effect<DeployStepOutput, CliFailure>;

export interface DeployStepOutput {
  readonly stderr: string;
  readonly stdout: string;
}

type DeployProcessResult =
  | {
      readonly _tag: "close";
      readonly code: number | null;
      readonly stderr: string;
      readonly stdout: string;
    }
  | {
      readonly _tag: "error";
      readonly error: Error;
      readonly stderr: string;
      readonly stdout: string;
    };

const deployWranglerConfigFile = ".stateful-ci/deploy/wrangler.toml";
const deployResourceNamePattern = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u;
const defaultOidcAudience = "stateful-ci";
const repositoryRootDirectory = fileURLToPath(
  new URL("../../../..", import.meta.url)
);

const D1DatabaseListEntry = Schema.Struct({
  database_id: Schema.optional(Schema.String),
  id: Schema.optional(Schema.String),
  name: Schema.String,
  uuid: Schema.optional(Schema.String),
});

const D1DatabaseList = Schema.Array(D1DatabaseListEntry);
type D1DatabaseListEntry = Schema.Schema.Type<typeof D1DatabaseListEntry>;

const R2BucketListEntry = Schema.Struct({
  name: Schema.String,
});

const R2BucketList = Schema.Array(R2BucketListEntry);

const deployResourceNameFromEnv = (
  env: RuntimeEnv,
  key: string,
  fallback: string
) => {
  const value = optionalEnv(env, key) ?? fallback;

  return deployResourceNamePattern.test(value)
    ? Effect.succeed(value)
    : Effect.fail(
        cliFailure(
          `${key} must use 3-63 lowercase letters, numbers, dots, or hyphens, and must start and end with a letter or number. Refusing to pass an invalid resource name to Wrangler.`
        )
      );
};

const parseD1DatabaseId = (source: string, databaseName: string) => {
  const decoded = Schema.decodeUnknownExit(
    Schema.fromJsonString(D1DatabaseList)
  )(source);

  if (Exit.isFailure(decoded)) {
    return null;
  }

  const matching = decoded.value.find((entry) => entry.name === databaseName);

  if (matching === undefined) {
    return null;
  }

  for (const databaseId of [matching.uuid, matching.database_id, matching.id]) {
    if (databaseId !== undefined && databaseId.length > 0) {
      return databaseId;
    }
  }

  return null;
};

const parseR2BucketExists = (source: string, bucketName: string) => {
  const decoded = Schema.decodeUnknownExit(Schema.fromJsonString(R2BucketList))(
    source
  );

  return Exit.isSuccess(decoded)
    ? decoded.value.some((entry) => entry.name === bucketName)
    : false;
};

const deployConfigText = (input: {
  readonly allowedRepositories: string;
  readonly bucket: string;
  readonly database: string;
  readonly databaseId: string;
  readonly devAuthEnabled: string | null;
  readonly oidcAudience: string;
  readonly trustedRefs: string | null;
}) => {
  const vars = [
    `ALLOWED_REPOSITORIES = ${JSON.stringify(input.allowedRepositories)}`,
    `OIDC_AUDIENCE = ${JSON.stringify(input.oidcAudience)}`,
    ...(input.trustedRefs === null
      ? []
      : [`TRUSTED_REFS = ${JSON.stringify(input.trustedRefs)}`]),
    ...(input.devAuthEnabled === null
      ? []
      : [`DEV_AUTH_ENABLED = ${JSON.stringify(input.devAuthEnabled)}`]),
  ].join("\n");

  return `name = "stateful-ci-worker"\nmain = "../../packages/worker/src/index.ts"\ncompatibility_date = "2026-05-22"\n\n[vars]\n${vars}\n\n[[d1_databases]]\nbinding = "STATEFUL_CI_METADATA"\ndatabase_name = ${JSON.stringify(input.database)}\ndatabase_id = ${JSON.stringify(input.databaseId)}\nmigrations_dir = "../../packages/worker/migrations"\n\n[[r2_buckets]]\nbinding = "STATEFUL_CI_OBJECTS"\nbucket_name = ${JSON.stringify(input.bucket)}\n\n[[durable_objects.bindings]]\nname = "STATEFUL_CI_COORDINATORS"\nclass_name = "WorkspaceSnapshotCoordinatorDurableObject"\n\n[[migrations]]\ntag = "v1"\nnew_sqlite_classes = ["WorkspaceSnapshotCoordinatorDurableObject"]\n`;
};

const runDeployStep: DeployStepRunner = Effect.fn("runDeployStep")(
  function* runDeployStepEffect(input: {
    readonly args: readonly string[];
    readonly stdin?: string;
  }) {
    const processResult = yield* Effect.tryPromise({
      catch: (error) =>
        cliFailure(
          `Deploy step failed while running bunx ${input.args.join(" ")}. Stateful CI backend resources may be partially provisioned; fix the reported Cloudflare/Wrangler issue and rerun stateful-ci deploy.${error instanceof Error ? `\nProcess error: ${error.message}` : ""}`
        ),
      try: async (): Promise<DeployProcessResult> => {
        const stdoutChunks: Uint8Array[] = [];
        const stderrChunks: Uint8Array[] = [];
        const child = spawn("bunx", [...input.args], {
          cwd: repositoryRootDirectory,
          stdio: "pipe",
        });

        child.stdout.on("data", (chunk: Uint8Array) => {
          stdoutChunks.push(chunk);
        });
        child.stderr.on("data", (chunk: Uint8Array) => {
          stderrChunks.push(chunk);
        });
        child.stdin.end(input.stdin ?? "");

        const exit = await Promise.race([
          once(child, "close").then(([code]) => ({
            _tag: "close" as const,
            code: typeof code === "number" ? code : null,
          })),
          once(child, "error").then(([error]) => ({
            _tag: "error" as const,
            error: error instanceof Error ? error : new Error(String(error)),
          })),
        ]);
        const stderr = Buffer.concat(stderrChunks).toString("utf-8");
        const stdout = Buffer.concat(stdoutChunks).toString("utf-8");

        return { ...exit, stderr, stdout };
      },
    });

    if (processResult._tag === "close" && processResult.code === 0) {
      return { stderr: processResult.stderr, stdout: processResult.stdout };
    }

    const output = [processResult.stdout, processResult.stderr]
      .map((text) => text.trim())
      .filter((text) => text.length > 0)
      .join("\n");
    const outputMessage =
      output.length === 0 ? "" : `\nWrangler output:\n${output}`;
    const processMessage =
      processResult._tag === "error"
        ? `\nProcess error: ${processResult.error.message}`
        : `\nProcess exited with code ${processResult.code}.`;

    return yield* Effect.fail(
      cliFailure(
        `Deploy step failed while running bunx ${input.args.join(" ")}. Stateful CI backend resources may be partially provisioned; fix the reported Cloudflare/Wrangler issue and rerun stateful-ci deploy.${processMessage}${outputMessage}`
      )
    );
  }
);

const ensureR2Bucket = Effect.fn("ensureR2Bucket")(
  function* ensureR2BucketEffect(bucket: string, runStep: DeployStepRunner) {
    yield* runStep({
      args: ["wrangler", "r2", "bucket", "create", bucket],
    }).pipe(
      Effect.matchEffect({
        onFailure: () =>
          Effect.gen(function* confirmExistingR2BucketEffect() {
            const list = yield* runStep({
              args: ["wrangler", "r2", "bucket", "list", "--json"],
            });

            if (parseR2BucketExists(list.stdout, bucket)) {
              yield* Console.log(
                `R2 bucket ${bucket} already exists; reusing it.`
              );
              return;
            }

            return yield* Effect.fail(
              cliFailure(
                `Could not create R2 bucket ${bucket}, and Wrangler JSON output did not confirm that the bucket already exists. Stateful CI backend resources may be partially provisioned; run bunx wrangler r2 bucket list --json and retry stateful-ci deploy after confirming the bucket exists.`
              )
            );
          }),
        onSuccess: () => Effect.void,
      })
    );
  }
);

const findD1DatabaseId = Effect.fn("findD1DatabaseId")(
  function* findD1DatabaseIdEffect(
    database: string,
    runStep: DeployStepRunner
  ) {
    const list = yield* runStep({
      args: ["wrangler", "d1", "list", "--json"],
    });
    const databaseId = parseD1DatabaseId(list.stdout, database);

    return databaseId === null
      ? yield* Effect.fail(
          cliFailure(
            `Cloudflare D1 database ${database} already exists or was created, but Wrangler did not return its database id. Run bunx wrangler d1 list --json and retry stateful-ci deploy after confirming the database exists.`
          )
        )
      : databaseId;
  }
);

const ensureD1Database = Effect.fn("ensureD1Database")(
  function* ensureD1DatabaseEffect(
    database: string,
    runStep: DeployStepRunner
  ) {
    const created = yield* runStep({
      args: ["wrangler", "d1", "create", database],
    }).pipe(
      Effect.match({
        onFailure: () => false,
        onSuccess: () => true,
      })
    );
    const databaseId = yield* findD1DatabaseId(database, runStep);

    if (!created) {
      yield* Console.log(`D1 database ${database} already exists; reusing it.`);
    }

    return databaseId;
  }
);

const writeDeployWranglerConfig = Effect.fn("writeDeployWranglerConfig")(
  function* writeDeployWranglerConfigEffect(input: {
    readonly allowedRepositories: string;
    readonly bucket: string;
    readonly database: string;
    readonly databaseId: string;
    readonly devAuthEnabled: string | null;
    readonly oidcAudience: string;
    readonly trustedRefs: string | null;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const configPath = path.join(
      repositoryRootDirectory,
      deployWranglerConfigFile
    );

    yield* fs
      .makeDirectory(path.dirname(configPath), { recursive: true })
      .pipe(
        Effect.mapError(() =>
          cliFailure(
            `Could not create ${path.dirname(configPath)} for generated Wrangler deploy config. Check filesystem permissions and retry stateful-ci deploy.`
          )
        )
      );
    yield* fs
      .writeFileString(configPath, deployConfigText(input))
      .pipe(
        Effect.mapError(() =>
          cliFailure(
            `Could not write generated Wrangler deploy config at ${configPath}. Check filesystem permissions and retry stateful-ci deploy.`
          )
        )
      );

    return configPath;
  }
);

export const deployProgramWithRunner = (
  env: RuntimeEnv,
  runStep: DeployStepRunner
) =>
  Effect.gen(function* deployProgramEffect() {
    const bucket = yield* deployResourceNameFromEnv(
      env,
      "STATEFUL_CI_R2_BUCKET",
      "stateful-ci-objects"
    );
    const database = yield* deployResourceNameFromEnv(
      env,
      "STATEFUL_CI_D1_DATABASE",
      "stateful-ci-metadata"
    );
    const allowedRepositories = yield* requiredDeployEnv(
      env,
      "STATEFUL_CI_ALLOWED_REPOSITORIES"
    );
    const transferSecret = yield* requiredDeployEnv(
      env,
      "STATEFUL_CI_TRANSFER_SECRET"
    );
    const databaseId = yield* ensureD1Database(database, runStep);
    const config = yield* writeDeployWranglerConfig({
      allowedRepositories,
      bucket,
      database,
      databaseId,
      devAuthEnabled:
        optionalEnv(env, "DEV_AUTH_ENABLED") ??
        optionalEnv(env, "STATEFUL_CI_DEV_AUTH_ENABLED"),
      oidcAudience:
        optionalEnv(env, "OIDC_AUDIENCE") ??
        optionalEnv(env, "STATEFUL_CI_OIDC_AUDIENCE") ??
        defaultOidcAudience,
      trustedRefs:
        optionalEnv(env, "TRUSTED_REFS") ??
        optionalEnv(env, "STATEFUL_CI_TRUSTED_REFS"),
    });

    yield* Console.log(
      `Deploying Stateful CI backend with R2 bucket ${bucket} and D1 database ${database}.`
    );
    yield* ensureR2Bucket(bucket, runStep);
    yield* runStep({
      args: [
        "wrangler",
        "d1",
        "migrations",
        "apply",
        database,
        "--remote",
        "--config",
        config,
      ],
    });
    yield* runStep({
      args: [
        "wrangler",
        "secret",
        "put",
        "STATEFUL_CI_TRANSFER_SECRET",
        "--config",
        config,
      ],
      stdin: `${transferSecret}\n`,
    });
    yield* runStep({ args: ["wrangler", "deploy", "--config", config] });
    yield* Console.log(
      "Stateful CI backend deploy finished. Set STATEFUL_CI_API_URL in GitHub Actions to the deployed Worker URL."
    );
  }).pipe(Effect.catchTag("CliFailure", failCliFailure));

export const deployProgram = (env: RuntimeEnv) =>
  deployProgramWithRunner(env, runDeployStep);
