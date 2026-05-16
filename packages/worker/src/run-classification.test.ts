import type { RestoreRequest } from "@stateful-ci/core";
import { describe, expect, test } from "vitest";

import { classifyRunTrust } from "./run-classification";

const restoreRequest = {
  client: {
    configHash: "sha256:config",
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
  workspace: {
    job: "test",
    repo: "eersnington/stateful-ci",
    workflow: "ci.yml",
  },
} satisfies RestoreRequest;

describe("run classification", () => {
  test("classifies push to refs/heads/main as trusted", () => {
    expect(classifyRunTrust(restoreRequest)).toBe("trusted");
  });

  test("classifies same-repo branch pushes as internal", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/heads/feature" },
      })
    ).toBe("internal");
  });

  test("does not assume every repository uses main as the only trusted branch", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/heads/master" },
      })
    ).toBe("trusted");
  });

  test("classifies same-repo pull requests as internal", () => {
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

  test("classifies fork pull requests as external", () => {
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

  test("classifies pull_request_target as unknown because it is unsafe for state production", () => {
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

  test("classifies release events on tag refs as privileged", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/tags/v1.0.0" },
        github: { ...restoreRequest.github, event: "release" },
      })
    ).toBe("privileged");
  });

  test("classifies tag refs without release events as unknown", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        git: { ...restoreRequest.git, ref: "refs/tags/v1.0.0" },
      })
    ).toBe("unknown");
  });

  test("classifies release events without tag refs as unknown", () => {
    expect(
      classifyRunTrust({
        ...restoreRequest,
        github: { ...restoreRequest.github, event: "release" },
      })
    ).toBe("unknown");
  });

  test("classifies pull requests with tag refs as unknown", () => {
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

  test("classifies incomplete pull request metadata as unknown", () => {
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
