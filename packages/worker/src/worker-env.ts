import { Exit, Schema } from "effect";

import { GitHubJwks, defaultGitHubOidcAudience } from "./github-oidc";
import { defaultTrustedRefs } from "./run-classification";

export interface WorkerEnv {
  readonly ALLOWED_REPOSITORIES?: string;
  readonly DEV_AUTH_ENABLED?: string;
  readonly OIDC_AUDIENCE?: string;
  readonly STATEFUL_CI_ALLOWED_REPOSITORIES?: string;
  readonly STATEFUL_CI_COORDINATORS?: DurableObjectNamespace;
  readonly STATEFUL_CI_METADATA?: D1Database;
  readonly STATEFUL_CI_OBJECTS?: R2Bucket;
  readonly STATEFUL_CI_API_TOKEN?: string;
  readonly STATEFUL_CI_DEV_AUTH_ENABLED?: string;
  readonly STATEFUL_CI_GITHUB_JWKS_JSON?: string;
  readonly STATEFUL_CI_OIDC_AUDIENCE?: string;
  readonly STATEFUL_CI_TRUSTED_REFS?: string;
  readonly STATEFUL_CI_TRANSFER_SECRET?: string;
  readonly TRUSTED_REFS?: string;
}

const configuredList = (source: string | undefined) =>
  source
    ?.split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0) ?? [];

export const trustedRefsForEnv = (env: WorkerEnv | undefined) => {
  const configured = configuredList(
    env?.TRUSTED_REFS ?? env?.STATEFUL_CI_TRUSTED_REFS
  );

  return configured.length === 0 ? defaultTrustedRefs : configured;
};

export const allowedRepositoriesForEnv = (env: WorkerEnv | undefined) =>
  configuredList(
    env?.ALLOWED_REPOSITORIES ?? env?.STATEFUL_CI_ALLOWED_REPOSITORIES
  ).map((repository) => repository.toLowerCase());

export const oidcAudienceForEnv = (env: WorkerEnv | undefined) => {
  const configured = (env?.OIDC_AUDIENCE ?? env?.STATEFUL_CI_OIDC_AUDIENCE)
    ?.trim()
    .replaceAll("\n", "");

  return configured === undefined || configured.length === 0
    ? defaultGitHubOidcAudience
    : configured;
};

export const devAuthEnabled = (env: WorkerEnv | undefined) =>
  (env?.DEV_AUTH_ENABLED ?? env?.STATEFUL_CI_DEV_AUTH_ENABLED) === "1" ||
  (env?.DEV_AUTH_ENABLED ?? env?.STATEFUL_CI_DEV_AUTH_ENABLED) === "true";

export const oidcJwksForEnv = (env: WorkerEnv | undefined) => {
  if (env?.STATEFUL_CI_GITHUB_JWKS_JSON === undefined) {
    return { status: "unset" as const };
  }

  const parsed = Schema.decodeUnknownExit(Schema.fromJsonString(GitHubJwks))(
    env.STATEFUL_CI_GITHUB_JWKS_JSON
  );

  return Exit.isFailure(parsed)
    ? { status: "invalid" as const }
    : { jwks: parsed.value.keys, status: "valid" as const };
};

export const transferSecretForEnv = (env: WorkerEnv | undefined) => {
  const secret = env?.STATEFUL_CI_TRANSFER_SECRET;

  return secret === undefined || secret.length === 0 ? null : secret;
};
