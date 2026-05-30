import { describe, expect, it } from "@effect/vitest";
import type { VerifiedGitHubActionsIdentity } from "@stateful-ci/core";

import { classifyVerifiedGitHubTrust } from "../src/run-classification";

const identity = {
  actor: "eersnington",
  audience: "stateful-ci",
  baseRef: null,
  checkRunId: "987654321",
  environment: null,
  event: "push",
  headRef: null,
  issuer: "https://token.actions.githubusercontent.com",
  jobWorkflowRef: null,
  provider: "github-actions",
  ref: "refs/heads/main",
  refType: "branch",
  repository: "eersnington/stateful-ci",
  repositoryOwner: "eersnington",
  runId: "123456789",
  sha: "abc123",
  subject: "repo:eersnington/stateful-ci:ref:refs/heads/main",
  workflow: "ci.yml",
  workflowRef:
    "eersnington/stateful-ci/.github/workflows/ci.yml@refs/heads/main",
} satisfies VerifiedGitHubActionsIdentity;

describe("verified run classification", () => {
  it("classifies trusted branch pushes", () => {
    expect(classifyVerifiedGitHubTrust(identity)).toBe("trusted");
  });

  it("classifies same-repository branch pushes as internal", () => {
    expect(
      classifyVerifiedGitHubTrust({ ...identity, ref: "refs/heads/feature" })
    ).toBe("internal");
  });

  it("uses configured trusted refs", () => {
    expect(
      classifyVerifiedGitHubTrust(
        { ...identity, ref: "refs/heads/release" },
        { trustedRefs: ["refs/heads/release"] }
      )
    ).toBe("trusted");
  });

  it("treats verified pull requests as external without repository metadata", () => {
    expect(
      classifyVerifiedGitHubTrust({
        ...identity,
        baseRef: "main",
        event: "pull_request",
        headRef: "feature",
        ref: "refs/pull/12/merge",
        subject: "repo:eersnington/stateful-ci:pull_request",
      })
    ).toBe("external");
  });

  it("classifies pull_request_target and tag pushes as unknown", () => {
    expect(
      classifyVerifiedGitHubTrust({
        ...identity,
        event: "pull_request_target",
      })
    ).toBe("unknown");
    expect(
      classifyVerifiedGitHubTrust({ ...identity, ref: "refs/tags/v1.0.0" })
    ).toBe("unknown");
  });

  it("classifies releases and deployments as privileged", () => {
    expect(
      classifyVerifiedGitHubTrust({
        ...identity,
        event: "release",
        ref: "refs/tags/v1.0.0",
        refType: "tag",
      })
    ).toBe("privileged");
    expect(
      classifyVerifiedGitHubTrust({ ...identity, event: "deployment" })
    ).toBe("privileged");
  });

  it("classifies incomplete identities as unknown", () => {
    expect(classifyVerifiedGitHubTrust({ ...identity, event: "" })).toBe(
      "unknown"
    );
    expect(classifyVerifiedGitHubTrust({ ...identity, repository: "" })).toBe(
      "unknown"
    );
  });
});
