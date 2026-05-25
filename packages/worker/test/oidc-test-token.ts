import { Effect, Schema } from "effect";

import { GitHubJwks } from "../src/github-oidc";

const base64Url = (bytes: Uint8Array) =>
  btoa(String.fromCodePoint(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");

const jsonSegment = (value: unknown) =>
  base64Url(new TextEncoder().encode(JSON.stringify(value)));

export const githubOidcClaims = (
  nowSeconds: number,
  overrides: Record<string, unknown> = {}
) =>
  ({
    actor: "eersnington",
    aud: "stateful-ci",
    check_run_id: "987654321",
    event_name: "push",
    exp: nowSeconds + 600,
    iat: nowSeconds,
    iss: "https://token.actions.githubusercontent.com",
    ref: "refs/heads/main",
    ref_type: "branch",
    repository: "eersnington/stateful-ci",
    repository_owner: "eersnington",
    run_id: "123456789",
    sha: "abc123",
    sub: "repo:eersnington/stateful-ci:ref:refs/heads/main",
    workflow: "ci.yml",
    workflow_ref:
      "eersnington/stateful-ci/.github/workflows/ci.yml@refs/heads/main",
    ...overrides,
  }) satisfies Record<string, unknown>;

export const createSignedGitHubOidcToken = Effect.fn(
  "createSignedGitHubOidcToken"
)(function* createSignedGitHubOidcTokenEffect(
  claims: Record<string, unknown>,
  kid = "test-key"
) {
  const keyPair = yield* Effect.promise(() =>
    crypto.subtle.generateKey(
      {
        hash: "SHA-256",
        modulusLength: 2048,
        name: "RSASSA-PKCS1-v1_5",
        publicExponent: new Uint8Array([1, 0, 1]),
      },
      true,
      ["sign", "verify"]
    )
  );
  if (!("publicKey" in keyPair) || !("privateKey" in keyPair)) {
    return yield* Effect.die("Expected RSA key pair for OIDC test token.");
  }
  const publicJwk = yield* Effect.promise(() =>
    crypto.subtle.exportKey("jwk", keyPair.publicKey)
  );
  const header = jsonSegment({ alg: "RS256", kid, typ: "JWT" });
  const payload = jsonSegment(claims);
  const signedData = new TextEncoder().encode(`${header}.${payload}`);
  const signature = new Uint8Array(
    yield* Effect.promise(() =>
      crypto.subtle.sign("RSASSA-PKCS1-v1_5", keyPair.privateKey, signedData)
    )
  );
  const jwks = Schema.decodeUnknownSync(GitHubJwks)({
    keys: [{ ...publicJwk, alg: "RS256", kid }],
  });

  return {
    jwks,
    jwksJson: Schema.encodeUnknownSync(Schema.fromJsonString(GitHubJwks))(jwks),
    token: `${header}.${payload}.${base64Url(signature)}`,
  };
});
