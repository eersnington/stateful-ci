import { assert, describe, it } from "@effect/vitest";
import { Schema, Effect, Clock } from "effect";

import { GitHubJwks, verifyGitHubOidcToken } from "../src/github-oidc";
import { classifyVerifiedGitHubTrust } from "../src/run-classification";

const base64Url = (bytes: Uint8Array) =>
  btoa(String.fromCodePoint(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");

const jsonSegment = (value: unknown) =>
  base64Url(new TextEncoder().encode(JSON.stringify(value)));

const createSignedToken = Effect.fn("createSignedToken")(
  function* createSignedTokenEffect(
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
    }).keys;

    return { jwks, token: `${header}.${payload}.${base64Url(signature)}` };
  }
);

const claims = (nowSeconds: number, overrides: Record<string, unknown> = {}) =>
  ({
    actor: "eersnington",
    aud: "stateful-ci",
    event_name: "push",
    exp: nowSeconds + 600,
    iat: nowSeconds,
    iss: "https://token.actions.githubusercontent.com",
    job: "test",
    ref: "refs/heads/main",
    repository: "eersnington/stateful-ci",
    repository_owner: "eersnington",
    run_id: "123456789",
    sha: "abc123",
    sub: "repo:eersnington/stateful-ci:ref:refs/heads/main",
    workflow: "ci.yml",
    ...overrides,
  }) satisfies Record<string, unknown>;

describe("GitHub OIDC verification", () => {
  it.effect(
    "verifies a signed token and classifies trusted branch pushes",
    () =>
      Effect.gen(function* verifiesSignedTokenEffect() {
        const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const { jwks, token } = yield* createSignedToken(claims(nowSeconds));
        const identity = yield* verifyGitHubOidcToken(token, {
          audience: "stateful-ci",
          jwks,
        });

        assert.strictEqual(identity.repository, "eersnington/stateful-ci");
        assert.strictEqual(classifyVerifiedGitHubTrust(identity), "trusted");
      })
  );

  it.effect("denies invalid issuer", () =>
    Effect.gen(function* deniesInvalidIssuerEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedToken(
        claims(nowSeconds, { iss: "https://example.com" })
      );
      const error = yield* Effect.flip(
        verifyGitHubOidcToken(token, { audience: "stateful-ci", jwks })
      );

      assert.strictEqual(error.reason, "oidc_issuer_mismatch");
    })
  );

  it.effect("denies invalid audience", () =>
    Effect.gen(function* deniesInvalidAudienceEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedToken(claims(nowSeconds));
      const error = yield* Effect.flip(
        verifyGitHubOidcToken(token, { audience: "other-backend", jwks })
      );

      assert.strictEqual(error.reason, "oidc_audience_mismatch");
    })
  );

  it.effect("denies expired tokens", () =>
    Effect.gen(function* deniesExpiredTokenEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedToken(
        claims(nowSeconds, { exp: nowSeconds - 1 })
      );
      const error = yield* Effect.flip(
        verifyGitHubOidcToken(token, { audience: "stateful-ci", jwks })
      );

      assert.strictEqual(error.reason, "oidc_invalid");
    })
  );

  it.effect(
    "classifies fork and same-repo pull requests from verified claims",
    () =>
      Effect.gen(function* classifiesPullRequestsEffect() {
        const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const external = yield* createSignedToken(
          claims(nowSeconds, {
            base_ref: "main",
            event_name: "pull_request",
            head_ref: "feature",
            head_repository: "contributor/stateful-ci",
            ref: "refs/pull/12/merge",
            sub: "repo:eersnington/stateful-ci:pull_request",
          })
        );
        const internal = yield* createSignedToken(
          claims(nowSeconds, {
            base_ref: "main",
            event_name: "pull_request",
            head_ref: "feature",
            head_repository: "eersnington/stateful-ci",
            ref: "refs/pull/12/merge",
            sub: "repo:eersnington/stateful-ci:pull_request",
          })
        );

        assert.strictEqual(
          classifyVerifiedGitHubTrust(
            yield* verifyGitHubOidcToken(external.token, {
              audience: "stateful-ci",
              jwks: external.jwks,
            })
          ),
          "external"
        );
        assert.strictEqual(
          classifyVerifiedGitHubTrust(
            yield* verifyGitHubOidcToken(internal.token, {
              audience: "stateful-ci",
              jwks: internal.jwks,
            })
          ),
          "internal"
        );
      })
  );

  it.effect(
    "classifies pull requests without verified head repository as external",
    () =>
      Effect.gen(function* classifiesMissingHeadRepositoryAsExternalEffect() {
        const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const { jwks, token } = yield* createSignedToken(
          claims(nowSeconds, {
            base_ref: "main",
            event_name: "pull_request",
            head_ref: "feature",
            ref: "refs/pull/12/merge",
            sub: "repo:eersnington/stateful-ci:pull_request",
          })
        );
        const identity = yield* verifyGitHubOidcToken(token, {
          audience: "stateful-ci",
          jwks,
        });

        assert.strictEqual(classifyVerifiedGitHubTrust(identity), "external");
      })
  );

  it.effect("classifies pull_request_target as unknown", () =>
    Effect.gen(function* classifiesPullRequestTargetEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedToken(
        claims(nowSeconds, {
          event_name: "pull_request_target",
          sub: "repo:eersnington/stateful-ci:pull_request",
        })
      );
      const identity = yield* verifyGitHubOidcToken(token, {
        audience: "stateful-ci",
        jwks,
      });

      assert.strictEqual(classifyVerifiedGitHubTrust(identity), "unknown");
    })
  );
});
