import { assert, describe, it } from "@effect/vitest";
import { Effect, Clock } from "effect";

import { verifyGitHubOidcToken } from "../src/github-oidc";
import { classifyVerifiedGitHubTrust } from "../src/run-classification";
import {
  createSignedGitHubOidcToken,
  githubOidcClaims,
} from "./oidc-test-token";

describe("GitHub OIDC verification", () => {
  it.effect(
    "verifies a signed token and classifies trusted branch pushes",
    () =>
      Effect.gen(function* verifiesSignedTokenEffect() {
        const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const { jwks, token } = yield* createSignedGitHubOidcToken(
          githubOidcClaims(nowSeconds)
        );
        const identity = yield* verifyGitHubOidcToken(token, {
          audience: "stateful-ci",
          jwks: jwks.keys,
        });

        assert.strictEqual(identity.repository, "eersnington/stateful-ci");
        assert.strictEqual(classifyVerifiedGitHubTrust(identity), "trusted");
      })
  );

  it.effect("denies invalid issuer", () =>
    Effect.gen(function* deniesInvalidIssuerEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedGitHubOidcToken(
        githubOidcClaims(nowSeconds, { iss: "https://example.com" })
      );
      const error = yield* Effect.flip(
        verifyGitHubOidcToken(token, {
          audience: "stateful-ci",
          jwks: jwks.keys,
        })
      );

      assert.strictEqual(error.reason, "oidc_issuer_mismatch");
    })
  );

  it.effect("denies invalid audience", () =>
    Effect.gen(function* deniesInvalidAudienceEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedGitHubOidcToken(
        githubOidcClaims(nowSeconds)
      );
      const error = yield* Effect.flip(
        verifyGitHubOidcToken(token, {
          audience: "other-backend",
          jwks: jwks.keys,
        })
      );

      assert.strictEqual(error.reason, "oidc_audience_mismatch");
    })
  );

  it.effect("denies expired tokens", () =>
    Effect.gen(function* deniesExpiredTokenEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedGitHubOidcToken(
        githubOidcClaims(nowSeconds, { exp: nowSeconds - 1 })
      );
      const error = yield* Effect.flip(
        verifyGitHubOidcToken(token, {
          audience: "stateful-ci",
          jwks: jwks.keys,
        })
      );

      assert.strictEqual(error.reason, "oidc_invalid");
    })
  );

  it.effect(
    "classifies pull requests as external without independently verified PR metadata",
    () =>
      Effect.gen(function* classifiesPullRequestsEffect() {
        const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const pullRequest = yield* createSignedGitHubOidcToken(
          githubOidcClaims(nowSeconds, {
            base_ref: "main",
            event_name: "pull_request",
            head_ref: "feature",
            ref: "refs/pull/12/merge",
            sub: "repo:eersnington/stateful-ci:pull_request",
          })
        );

        assert.strictEqual(
          classifyVerifiedGitHubTrust(
            yield* verifyGitHubOidcToken(pullRequest.token, {
              audience: "stateful-ci",
              jwks: pullRequest.jwks.keys,
            })
          ),
          "external"
        );
      })
  );

  it.effect("classifies unknown signed events as unknown", () =>
    Effect.gen(function* classifiesUnknownSignedEventsEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedGitHubOidcToken(
        githubOidcClaims(nowSeconds, {
          event_name: "workflow_dispatch",
          sub: "repo:eersnington/stateful-ci:ref:refs/heads/main",
        })
      );
      const identity = yield* verifyGitHubOidcToken(token, {
        audience: "stateful-ci",
        jwks: jwks.keys,
      });

      assert.strictEqual(classifyVerifiedGitHubTrust(identity), "unknown");
    })
  );

  it.effect("accepts environment subject form", () =>
    Effect.gen(function* acceptsEnvironmentSubjectEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedGitHubOidcToken(
        githubOidcClaims(nowSeconds, {
          environment: "Production:V1",
          sub: "repo:eersnington/stateful-ci:environment:Production%3AV1",
        })
      );
      const identity = yield* verifyGitHubOidcToken(token, {
        audience: "stateful-ci",
        jwks: jwks.keys,
      });

      assert.strictEqual(identity.environment, "Production:V1");
    })
  );

  it.effect("accepts customized subject templates", () =>
    Effect.gen(function* acceptsCustomSubjectEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedGitHubOidcToken(
        githubOidcClaims(nowSeconds, {
          sub: "repository_owner:eersnington:repository_visibility:private",
        })
      );
      const identity = yield* verifyGitHubOidcToken(token, {
        audience: "stateful-ci",
        jwks: jwks.keys,
      });

      assert.strictEqual(identity.repositoryOwner, "eersnington");
    })
  );

  it.effect("classifies pull_request_target as unknown", () =>
    Effect.gen(function* classifiesPullRequestTargetEffect() {
      const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const { jwks, token } = yield* createSignedGitHubOidcToken(
        githubOidcClaims(nowSeconds, {
          event_name: "pull_request_target",
          sub: "repo:eersnington/stateful-ci:pull_request",
        })
      );
      const identity = yield* verifyGitHubOidcToken(token, {
        audience: "stateful-ci",
        jwks: jwks.keys,
      });

      assert.strictEqual(classifyVerifiedGitHubTrust(identity), "unknown");
    })
  );
});
