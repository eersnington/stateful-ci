import {
  clientVersion,
  protocolVersion,
  RestoreRequest,
  workspacePathsForConfig,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import { cliFailure } from "./failure";
import type { ApiConfig } from "./protocol-client";
import type { LoadedConfig } from "./workspace-config";

export type RuntimeEnv = Readonly<Record<string, string | undefined>>;

const defaultOidcAudience = "stateful-ci";

export const optionalEnv = (env: RuntimeEnv, key: string) => {
  const value = env[key];
  return value === undefined || value.length === 0 ? null : value;
};

export const requiredDeployEnv = (env: RuntimeEnv, key: string) => {
  const value = optionalEnv(env, key);

  return value === null
    ? Effect.fail(
        cliFailure(
          `Missing ${key}. Set ${key} before running stateful-ci deploy so the generated Worker is secure and usable.`
        )
      )
    : Effect.succeed(value);
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

export const apiConfigFromEnv = Effect.fn("apiConfigFromEnv")(
  function* apiConfigFromEnvEffect(env: RuntimeEnv) {
    const source = yield* requiredEnv(env, "STATEFUL_CI_API_URL");
    const url = yield* Effect.try({
      catch: () =>
        cliFailure(
          "STATEFUL_CI_API_URL was invalid. Set it to the deployed Worker http(s) URL before running stateful-ci restore or save."
        ),
      try: () => new URL(source),
    });

    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return yield* Effect.fail(
        cliFailure(
          "STATEFUL_CI_API_URL must be an http(s) URL. Set it to the deployed Worker URL before running stateful-ci restore or save."
        )
      );
    }

    if (url.pathname !== "/" || url.search !== "" || url.hash !== "") {
      return yield* Effect.fail(
        cliFailure(
          "STATEFUL_CI_API_URL must be the deployed Worker root URL without a path, query, or fragment. Set it to the Worker origin, for example https://stateful-ci.example.workers.dev."
        )
      );
    }

    return {
      token: optionalEnv(env, "STATEFUL_CI_API_TOKEN"),
      url,
    } satisfies ApiConfig;
  }
);

export const oidcTokenFromEnv = Effect.fn("oidcTokenFromEnv")(
  function* oidcTokenFromEnvEffect(env: RuntimeEnv) {
    const explicitToken = optionalEnv(env, "STATEFUL_CI_OIDC_TOKEN");

    if (explicitToken !== null) {
      return explicitToken;
    }

    const requestUrl = optionalEnv(env, "ACTIONS_ID_TOKEN_REQUEST_URL");
    const requestToken = optionalEnv(env, "ACTIONS_ID_TOKEN_REQUEST_TOKEN");

    if (requestUrl === null || requestToken === null) {
      return yield* Effect.fail(
        cliFailure(
          "Missing GitHub Actions OIDC acquisition environment. Set ACTIONS_ID_TOKEN_REQUEST_URL and ACTIONS_ID_TOKEN_REQUEST_TOKEN by granting id-token: write, or provide STATEFUL_CI_OIDC_TOKEN only for local/bootstrap flows."
        )
      );
    }

    const url = yield* Effect.try({
      catch: () =>
        cliFailure(
          "GitHub Actions OIDC token endpoint URL was invalid. Restore/save did not contact the backend; check ACTIONS_ID_TOKEN_REQUEST_URL."
        ),
      try: () => new URL(requestUrl),
    });
    url.searchParams.set(
      "audience",
      optionalEnv(env, "STATEFUL_CI_OIDC_AUDIENCE") ?? defaultOidcAudience
    );

    const response = yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          "Could not acquire a GitHub Actions OIDC token. Restore/save did not contact the backend; check id-token: write permissions and retry."
        ),
      try: (signal) =>
        fetch(url, {
          headers: { authorization: `Bearer ${requestToken}` },
          signal,
        }),
    });

    if (!response.ok) {
      return yield* Effect.fail(
        cliFailure(
          `GitHub Actions OIDC token endpoint returned HTTP ${response.status}. Restore/save did not contact the backend; check id-token: write permissions.`
        )
      );
    }

    const body = yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          "GitHub Actions OIDC token response was not readable JSON. Restore/save did not contact the backend."
        ),
      try: () => response.json(),
    });
    const decoded = Schema.decodeUnknownExit(
      Schema.Struct({ value: Schema.String.check(Schema.isMinLength(1)) })
    )(body);

    return Exit.isFailure(decoded)
      ? yield* Effect.fail(
          cliFailure(
            "GitHub Actions OIDC token response did not include a token value. Restore/save did not contact the backend."
          )
        )
      : decoded.value.value;
  }
);

export const githubOidcIdentityFromEnv = Effect.fn("githubOidcIdentityFromEnv")(
  function* githubOidcIdentityFromEnvEffect(env: RuntimeEnv) {
    return {
      provider: "github-actions" as const,
      token: yield* oidcTokenFromEnv(env),
    };
  }
);

const devAuthEnabledFromEnv = (env: RuntimeEnv) => {
  const value =
    optionalEnv(env, "DEV_AUTH_ENABLED") ??
    optionalEnv(env, "STATEFUL_CI_DEV_AUTH_ENABLED");

  return value === "1" || value === "true";
};

export const githubOidcIdentityFromEnvOptional = Effect.fn(
  "githubOidcIdentityFromEnvOptional"
)(function* githubOidcIdentityFromEnvOptionalEffect(env: RuntimeEnv) {
  const hasOidcInput =
    optionalEnv(env, "STATEFUL_CI_OIDC_TOKEN") !== null ||
    optionalEnv(env, "ACTIONS_ID_TOKEN_REQUEST_URL") !== null ||
    optionalEnv(env, "ACTIONS_ID_TOKEN_REQUEST_TOKEN") !== null;

  if (hasOidcInput || !devAuthEnabledFromEnv(env)) {
    return yield* githubOidcIdentityFromEnv(env);
  }

  return null;
});

export const restoreRequestFromEnv = Effect.fn("restoreRequestFromEnv")(
  function* restoreRequestFromEnvEffect(env: RuntimeEnv, loaded: LoadedConfig) {
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
  }
);
