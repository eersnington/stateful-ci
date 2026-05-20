import { Schema } from "effect";

import { Sha256Digest } from "./ids";

export const GitHubOidcToken = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@stateful-ci/GitHubOidcToken")
);
export type GitHubOidcToken = Schema.Schema.Type<typeof GitHubOidcToken>;

export const GitHubOidcIdentity = Schema.Struct({
  provider: Schema.Literal("github-actions"),
  token: GitHubOidcToken,
});
export type GitHubOidcIdentity = Schema.Schema.Type<typeof GitHubOidcIdentity>;

export const GitHubContext = Schema.Struct({
  actor: Schema.String.check(Schema.isMinLength(1)),
  event: Schema.String.check(Schema.isMinLength(1)),
  runId: Schema.String.check(Schema.isMinLength(1)),
});
export type GitHubContext = Schema.Schema.Type<typeof GitHubContext>;

export const GitContext = Schema.Struct({
  baseRef: Schema.NullOr(Schema.String),
  headRef: Schema.NullOr(Schema.String),
  headRepo: Schema.NullOr(Schema.String),
  ref: Schema.String.check(Schema.isMinLength(1)),
  sha: Schema.String.check(Schema.isMinLength(1)),
});
export type GitContext = Schema.Schema.Type<typeof GitContext>;

export const ClientContext = Schema.Struct({
  configHash: Sha256Digest,
  version: Schema.String.check(Schema.isMinLength(1)),
});
export type ClientContext = Schema.Schema.Type<typeof ClientContext>;
