import { describe, expect, it } from "@effect/vitest";
import { RestoreRequest } from "@stateful-ci/core";
import { Schema } from "effect";

import { classifyRunTrust } from "../src/run-classification";

const restoreRequest = Schema.decodeSync(RestoreRequest)({
  client: {
    configHash:
      "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    version: "0.1.0",
  },
  git: {
    baseRef: null,
    headRef: null,
    headRepo: null,
    ref: "refs/heads/main",
    sha: "abc123",
  },
  github: {
    actor: "eersnington",
    event: "push",
    runId: "123456789",
  },
  identity: {
    provider: "github-actions",
    token: "oidc.jwt.token",
  },
  managedRoots: [".turbo"],
  protocolVersion: 1,
  workspace: {
    job: "test",
    repo: "eersnington/stateful-ci",
    workflow: "ci.yml",
  },
});

describe("run classification", () => {
  it("classifies push to refs/heads/main as trusted", () => {
    expect(classifyRunTrust(restoreRequest)).toBe("trusted");
  });

  it("classifies same-repo branch pushes as internal", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/heads/feature" },
      })
    ).toBe("internal");
  });

  it("does not assume every repository uses main as the only trusted branch", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/heads/master" },
      })
    ).toBe("trusted");
  });

  it("classifies same-repo pull requests as internal", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: {
          ...restoreRequest.git,
          baseRef: "main",
          headRef: "feature",
          headRepo: "eersnington/stateful-ci",
          ref: "refs/pull/12/merge",
        },
        github: { ...restoreRequest.github, event: "pull_request" },
      })
    ).toBe("internal");
  });

  it("classifies fork pull requests as external", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: {
          ...restoreRequest.git,
          baseRef: "main",
          headRef: "feature",
          headRepo: "contributor/stateful-ci",
          ref: "refs/pull/12/merge",
        },
        github: { ...restoreRequest.github, event: "pull_request" },
      })
    ).toBe("external");
  });

  it("classifies pull_request_target as unknown because it is unsafe for state production", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: {
          ...restoreRequest.git,
          baseRef: "main",
          headRef: "feature",
          headRepo: "eersnington/stateful-ci",
          ref: "refs/heads/main",
        },
        github: { ...restoreRequest.github, event: "pull_request_target" },
      })
    ).toBe("unknown");
  });

  it("classifies release events on tag refs as privileged", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/tags/v1.0.0" },
        github: { ...restoreRequest.github, event: "release" },
      })
    ).toBe("privileged");
  });

  it("classifies tag refs without release events as unknown", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/tags/v1.0.0" },
      })
    ).toBe("unknown");
  });

  it("classifies release events without tag refs as unknown", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        github: { ...restoreRequest.github, event: "release" },
      })
    ).toBe("unknown");
  });

  it("classifies pull requests with tag refs as unknown", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: {
          ...restoreRequest.git,
          baseRef: "main",
          headRef: "feature",
          headRepo: "contributor/stateful-ci",
          ref: "refs/tags/v1.0.0",
        },
        github: { ...restoreRequest.github, event: "pull_request" },
      })
    ).toBe("unknown");
  });

  it("classifies incomplete pull request metadata as unknown", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: {
          ...restoreRequest.git,
          baseRef: "main",
          headRef: "feature",
          headRepo: null,
          ref: "refs/pull/12/merge",
        },
        github: { ...restoreRequest.github, event: "pull_request" },
      })
    ).toBe("unknown");
  });
});
