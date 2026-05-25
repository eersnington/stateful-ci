import type {
  DenialReason,
  VerifiedGitHubActionsIdentity,
} from "@stateful-ci/core";
import {
  DenialReason as DenialReasonSchema,
  VerifiedGitHubActionsIdentity as VerifiedGitHubActionsIdentitySchema,
} from "@stateful-ci/core";
import { Clock, Effect, Exit, Schema } from "effect";

export const githubActionsIssuer =
  "https://token.actions.githubusercontent.com" as const;
export const defaultGitHubOidcAudience = "stateful-ci" as const;

const githubJwksUrl = `${githubActionsIssuer}/.well-known/jwks`;

const JwtHeader = Schema.Struct({
  alg: Schema.Literal("RS256"),
  kid: Schema.String.check(Schema.isMinLength(1)),
});

const GitHubOidcClaims = Schema.Struct({
  actor: Schema.String.check(Schema.isMinLength(1)),
  aud: Schema.Union([
    Schema.String.check(Schema.isMinLength(1)),
    Schema.Array(Schema.String.check(Schema.isMinLength(1))),
  ]),
  base_ref: Schema.optional(Schema.String),
  event_name: Schema.String.check(Schema.isMinLength(1)),
  exp: Schema.Number,
  head_ref: Schema.optional(Schema.String),
  head_repository: Schema.optional(Schema.String),
  iat: Schema.Number,
  iss: Schema.String.check(Schema.isMinLength(1)),
  job: Schema.optional(Schema.String),
  job_workflow_ref: Schema.optional(Schema.String),
  nbf: Schema.optional(Schema.Number),
  ref: Schema.String.check(Schema.isMinLength(1)),
  repository: Schema.String.check(Schema.isMinLength(1)),
  repository_owner: Schema.String.check(Schema.isMinLength(1)),
  run_id: Schema.String.check(Schema.isMinLength(1)),
  sha: Schema.String.check(Schema.isMinLength(1)),
  sub: Schema.String.check(Schema.isMinLength(1)),
  workflow: Schema.String.check(Schema.isMinLength(1)),
});

const GitHubJwk = Schema.Struct({
  alg: Schema.optional(Schema.String),
  e: Schema.String.check(Schema.isMinLength(1)),
  kid: Schema.String.check(Schema.isMinLength(1)),
  kty: Schema.Literal("RSA"),
  n: Schema.String.check(Schema.isMinLength(1)),
  use: Schema.optional(Schema.String),
});

export const GitHubJwks = Schema.Struct({
  keys: Schema.Array(GitHubJwk),
});

type JwtHeader = Schema.Schema.Type<typeof JwtHeader>;
type GitHubOidcClaims = Schema.Schema.Type<typeof GitHubOidcClaims>;
export type GitHubJwk = Schema.Schema.Type<typeof GitHubJwk>;

export class GitHubOidcVerificationError extends Schema.TaggedErrorClass<GitHubOidcVerificationError>()(
  "GitHubOidcVerificationError",
  {
    message: Schema.String,
    reason: DenialReasonSchema,
  }
) {}

export interface GitHubOidcVerificationOptions {
  readonly audience: string;
  readonly issuedAtToleranceSeconds?: number;
  readonly jwks?: readonly GitHubJwk[];
  readonly jwksUrl?: string;
}

const oidcError = (reason: DenialReason, message: string) =>
  new GitHubOidcVerificationError({ message, reason });

const base64UrlToBytes = (source: string, segment: string) =>
  Effect.try({
    catch: () =>
      oidcError(
        "oidc_invalid",
        `GitHub OIDC token ${segment} is not valid base64url data. Restore/save was denied because identity could not be verified.`
      ),
    try: () => {
      const padded = source
        .replaceAll("-", "+")
        .replaceAll("_", "/")
        .padEnd(Math.ceil(source.length / 4) * 4, "=");
      const binary = atob(padded);
      const bytes = new Uint8Array(binary.length);

      for (let index = 0; index < binary.length; index += 1) {
        bytes[index] = binary.codePointAt(index) ?? 0;
      }

      return bytes;
    },
  });

const decodeJsonSegment = <A>(
  schema: Schema.Decoder<A>,
  source: string,
  segment: string
) =>
  Effect.gen(function* decodeJsonSegmentEffect() {
    const bytes = yield* base64UrlToBytes(source, segment);
    const json = yield* Effect.try({
      catch: () =>
        oidcError(
          "oidc_invalid",
          `GitHub OIDC token ${segment} is not valid JSON. Restore/save was denied because identity could not be verified.`
        ),
      try: () => JSON.parse(new TextDecoder().decode(bytes)),
    });
    const decoded = Schema.decodeUnknownExit(schema)(json);

    return Exit.isFailure(decoded)
      ? yield* oidcError(
          "oidc_invalid",
          `GitHub OIDC token ${segment} did not contain the required claims. Restore/save was denied because identity could not be verified.`
        )
      : decoded.value;
  });

const audienceMatches = (actual: GitHubOidcClaims["aud"], expected: string) =>
  Array.isArray(actual) ? actual.includes(expected) : actual === expected;

const verifyTimeClaims = (
  claims: GitHubOidcClaims,
  nowSeconds: number,
  issuedAtToleranceSeconds: number
) => {
  if (claims.exp <= nowSeconds) {
    return oidcError(
      "oidc_invalid",
      "GitHub OIDC token is expired. Restore/save was denied; retry the job so GitHub issues a fresh identity token."
    );
  }

  if (claims.nbf !== undefined && claims.nbf > nowSeconds) {
    return oidcError(
      "oidc_invalid",
      "GitHub OIDC token is not valid yet. Restore/save was denied; check runner clock skew and retry."
    );
  }

  if (claims.iat > nowSeconds + issuedAtToleranceSeconds) {
    return oidcError(
      "oidc_invalid",
      "GitHub OIDC token was issued too far in the future. Restore/save was denied; check runner clock skew and retry."
    );
  }

  return null;
};

const verifySubjectClaim = (claims: GitHubOidcClaims) => {
  const repositoryPrefix = `repo:${claims.repository}:`;

  if (!claims.sub.startsWith(repositoryPrefix)) {
    return oidcError(
      "oidc_invalid",
      "GitHub OIDC token subject did not match the verified repository claim. Restore/save was denied because identity could not be trusted."
    );
  }

  if (claims.event_name === "push") {
    const expectedSubject = `${repositoryPrefix}ref:${claims.ref}`;

    return claims.sub === expectedSubject
      ? null
      : oidcError(
          "oidc_invalid",
          "GitHub OIDC token subject did not match the verified push ref. Restore/save was denied because identity could not be trusted."
        );
  }

  if (claims.event_name === "release" || claims.event_name === "deployment") {
    const expectedSubject = `${repositoryPrefix}ref:${claims.ref}`;

    return claims.sub === expectedSubject
      ? null
      : oidcError(
          "oidc_invalid",
          "GitHub OIDC token subject did not match the verified privileged ref. Restore/save was denied because identity could not be trusted."
        );
  }

  if (
    claims.event_name === "pull_request" ||
    claims.event_name === "pull_request_target"
  ) {
    return claims.sub === `${repositoryPrefix}pull_request`
      ? null
      : oidcError(
          "oidc_invalid",
          "GitHub OIDC token subject did not match the verified pull request event. Restore/save was denied because identity could not be trusted."
        );
  }

  return null;
};

const fetchJwks = (jwksUrl: string) =>
  Effect.gen(function* fetchJwksEffect() {
    const body = yield* Effect.tryPromise({
      catch: () =>
        oidcError(
          "oidc_invalid",
          "Could not fetch GitHub OIDC signing keys. Restore/save was denied because identity signatures could not be verified."
        ),
      try: async () => {
        const response = await fetch(jwksUrl);

        if (!response.ok) {
          throw new Error(`GitHub JWKS returned HTTP ${response.status}.`);
        }

        const json: unknown = await response.json();
        return json;
      },
    });
    const decoded = Schema.decodeUnknownExit(GitHubJwks)(body);

    return Exit.isFailure(decoded)
      ? yield* oidcError(
          "oidc_invalid",
          "GitHub OIDC signing key response did not match the expected JWKS schema. Restore/save was denied because identity signatures could not be verified."
        )
      : decoded.value.keys;
  });

const importVerificationKey = (jwk: GitHubJwk) =>
  Effect.tryPromise({
    catch: () =>
      oidcError(
        "oidc_invalid",
        "GitHub OIDC signing key could not be imported. Restore/save was denied because identity signatures could not be verified."
      ),
    try: () =>
      crypto.subtle.importKey(
        "jwk",
        {
          alg: jwk.alg ?? "RS256",
          e: jwk.e,
          kty: jwk.kty,
          n: jwk.n,
          ...(jwk.use === undefined ? {} : { use: jwk.use }),
        },
        { hash: "SHA-256", name: "RSASSA-PKCS1-v1_5" },
        false,
        ["verify"]
      ),
  });

const normalizeIdentity = (claims: GitHubOidcClaims) => {
  const decoded = Schema.decodeUnknownExit(VerifiedGitHubActionsIdentitySchema)(
    {
      actor: claims.actor,
      audience: claims.aud,
      baseRef: claims.base_ref ?? null,
      event: claims.event_name,
      headRef: claims.head_ref ?? null,
      headRepository: claims.head_repository ?? null,
      issuer: claims.iss,
      job: claims.job ?? null,
      jobWorkflowRef: claims.job_workflow_ref ?? null,
      provider: "github-actions",
      ref: claims.ref,
      repository: claims.repository,
      repositoryOwner: claims.repository_owner,
      runId: claims.run_id,
      sha: claims.sha,
      subject: claims.sub,
      workflow: claims.workflow,
    }
  );

  return Exit.isFailure(decoded) ? null : decoded.value;
};

export const verifyGitHubOidcToken = Effect.fn("verifyGitHubOidcToken")(
  function* verifyGitHubOidcTokenEffect(
    token: string,
    options: GitHubOidcVerificationOptions
  ) {
    if (token.trim().length === 0) {
      return yield* oidcError(
        "oidc_missing",
        "GitHub OIDC token was missing. Restore/save was denied because production trust identity is required."
      );
    }

    const [encodedHeader, encodedClaims, encodedSignature, extra] =
      token.split(".");

    if (
      encodedHeader === undefined ||
      encodedClaims === undefined ||
      encodedSignature === undefined ||
      extra !== undefined
    ) {
      return yield* oidcError(
        "oidc_invalid",
        "GitHub OIDC token did not have the expected JWT structure. Restore/save was denied because identity could not be verified."
      );
    }

    const header = yield* decodeJsonSegment(JwtHeader, encodedHeader, "header");
    const claims = yield* decodeJsonSegment(
      GitHubOidcClaims,
      encodedClaims,
      "claims"
    );

    if (claims.iss !== githubActionsIssuer) {
      return yield* oidcError(
        "oidc_issuer_mismatch",
        `GitHub OIDC token issuer ${claims.iss} did not match ${githubActionsIssuer}. Restore/save was denied because identity could not be trusted.`
      );
    }

    if (!audienceMatches(claims.aud, options.audience)) {
      return yield* oidcError(
        "oidc_audience_mismatch",
        "GitHub OIDC token audience did not match this Stateful CI backend. Restore/save was denied; check STATEFUL_CI_OIDC_AUDIENCE on the CLI and Worker."
      );
    }

    const nowMillis = yield* Clock.currentTimeMillis;
    const timeError = verifyTimeClaims(
      claims,
      Math.floor(nowMillis / 1000),
      options.issuedAtToleranceSeconds ?? 300
    );

    if (timeError !== null) {
      return yield* timeError;
    }

    const subjectError = verifySubjectClaim(claims);

    if (subjectError !== null) {
      return yield* subjectError;
    }

    const keys =
      options.jwks ?? (yield* fetchJwks(options.jwksUrl ?? githubJwksUrl));
    const jwk = keys.find((key) => key.kid === header.kid);

    if (jwk === undefined) {
      return yield* oidcError(
        "oidc_invalid",
        "GitHub OIDC token signing key was not found in GitHub JWKS. Restore/save was denied because identity signatures could not be verified."
      );
    }

    const key = yield* importVerificationKey(jwk);
    const signature = yield* base64UrlToBytes(encodedSignature, "signature");
    const signedData = new TextEncoder().encode(
      `${encodedHeader}.${encodedClaims}`
    );
    const verified = yield* Effect.tryPromise({
      catch: () =>
        oidcError(
          "oidc_invalid",
          "GitHub OIDC signature verification failed unexpectedly. Restore/save was denied because identity could not be verified."
        ),
      try: () =>
        crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, signature, signedData),
    });

    if (!verified) {
      return yield* oidcError(
        "oidc_invalid",
        "GitHub OIDC token signature was invalid. Restore/save was denied because identity could not be verified."
      );
    }

    const identity = normalizeIdentity(claims);

    return identity === null
      ? yield* oidcError(
          "oidc_invalid",
          "GitHub OIDC token claims could not be normalized. Restore/save was denied because identity could not be verified."
        )
      : identity;
  }
);

export const identityAuditPayload = (
  identity: VerifiedGitHubActionsIdentity | null,
  reason: DenialReason | null
) =>
  JSON.stringify({
    identity,
    reason,
  });
