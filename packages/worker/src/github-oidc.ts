import type {
  DenialReason,
  VerifiedGitHubActionsIdentity,
} from "@stateful-ci/core";
import {
  DenialReason as DenialReasonSchema,
  VerifiedGitHubActionsIdentity as VerifiedGitHubActionsIdentitySchema,
} from "@stateful-ci/core";
import { Clock, Effect, Exit, Schema } from "effect";

const githubActionsIssuer =
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
  check_run_id: Schema.optional(Schema.String),
  environment: Schema.optional(Schema.String),
  event_name: Schema.String.check(Schema.isMinLength(1)),
  exp: Schema.Number,
  head_ref: Schema.optional(Schema.String),
  iat: Schema.Number,
  iss: Schema.String.check(Schema.isMinLength(1)),
  job_workflow_ref: Schema.optional(Schema.String),
  nbf: Schema.optional(Schema.Number),
  ref: Schema.String.check(Schema.isMinLength(1)),
  ref_type: Schema.optional(Schema.String),
  repository: Schema.String.check(Schema.isMinLength(1)),
  repository_owner: Schema.String.check(Schema.isMinLength(1)),
  run_id: Schema.String.check(Schema.isMinLength(1)),
  sha: Schema.String.check(Schema.isMinLength(1)),
  sub: Schema.String.check(Schema.isMinLength(1)),
  workflow: Schema.String.check(Schema.isMinLength(1)),
  workflow_ref: Schema.optional(Schema.String),
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

interface GitHubOidcVerificationOptions {
  readonly audience: string;
  readonly issuedAtToleranceSeconds?: number;
  readonly jwks?: readonly GitHubJwk[];
  readonly jwksUrl?: string;
}

interface CachedJwks {
  readonly expiresAtMillis: number;
  readonly keys: readonly GitHubJwk[];
  readonly url: string;
}

const jwksCacheTtlMillis = 5 * 60 * 1000;
let cachedJwks: CachedJwks | null = null;

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

  if (
    claims.sub.startsWith("repo:") &&
    !claims.sub.startsWith(repositoryPrefix)
  ) {
    return oidcError(
      "oidc_invalid",
      "GitHub OIDC token subject did not match the verified repository claim. Restore/save was denied because identity could not be trusted."
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

/**
 * Returns GitHub OIDC signing keys using a bounded per-isolate cache.
 *
 * Cloudflare may reuse Worker isolates across requests, so the module-level
 * JWKS cache can be reused by later requests in the same isolate. The cache
 * stores only GitHub public signing keys, expires after a short TTL, and is
 * refreshed when a token references an unknown `kid`.
 */
const jwksForVerification = Effect.fn("jwksForVerification")(
  function* jwksForVerificationEffect(jwksUrl: string, forceRefresh: boolean) {
    const now = yield* Clock.currentTimeMillis;

    if (
      !forceRefresh &&
      cachedJwks !== null &&
      cachedJwks.url === jwksUrl &&
      cachedJwks.expiresAtMillis > now
    ) {
      return cachedJwks.keys;
    }

    const keys = yield* fetchJwks(jwksUrl);
    cachedJwks = {
      expiresAtMillis: now + jwksCacheTtlMillis,
      keys,
      url: jwksUrl,
    };

    return keys;
  }
);

const verificationKeysFor = Effect.fn("verificationKeysFor")(
  function* verificationKeysForEffect(
    header: JwtHeader,
    options: GitHubOidcVerificationOptions
  ) {
    if (options.jwks !== undefined) {
      return options.jwks.find((key) => key.kid === header.kid);
    }

    const jwksUrl = options.jwksUrl ?? githubJwksUrl;
    const cachedKeys = yield* jwksForVerification(jwksUrl, false);
    const cachedKey = cachedKeys.find((key) => key.kid === header.kid);

    if (cachedKey !== undefined) {
      return cachedKey;
    }

    const refreshedKeys = yield* jwksForVerification(jwksUrl, true);

    return refreshedKeys.find((key) => key.kid === header.kid);
  }
);

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

const normalizeIdentity = Effect.fn("normalizeIdentity")(
  function* normalizeIdentityEffect(claims: GitHubOidcClaims) {
    return yield* Schema.decodeUnknownEffect(
      VerifiedGitHubActionsIdentitySchema
    )({
      actor: claims.actor,
      audience: claims.aud,
      baseRef: claims.base_ref ?? null,
      checkRunId: claims.check_run_id ?? null,
      environment: claims.environment ?? null,
      event: claims.event_name,
      headRef: claims.head_ref ?? null,
      issuer: claims.iss,
      jobWorkflowRef: claims.job_workflow_ref ?? null,
      provider: "github-actions",
      ref: claims.ref,
      refType: claims.ref_type ?? null,
      repository: claims.repository,
      repositoryOwner: claims.repository_owner,
      runId: claims.run_id,
      sha: claims.sha,
      subject: claims.sub,
      workflow: claims.workflow,
      workflowRef: claims.workflow_ref ?? null,
    }).pipe(
      Effect.mapError(() =>
        oidcError(
          "oidc_invalid",
          "GitHub OIDC token claims could not be normalized. Restore/save was denied because identity could not be verified."
        )
      )
    );
  }
);

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

    const jwk = yield* verificationKeysFor(header, options);

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

    return yield* normalizeIdentity(claims);
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
