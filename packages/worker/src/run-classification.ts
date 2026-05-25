import type { RestoreRequest, TrustClass } from "@stateful-ci/core";

type RunClassificationRequest = Pick<
  RestoreRequest,
  "git" | "github" | "workspace"
>;

export const defaultTrustedRefs = Object.freeze([
  "refs/heads/main",
  "refs/heads/master",
] as const);

export interface RunClassificationOptions {
  readonly trustedRefs?: readonly string[] | undefined;
}

const isPresent = (value: string | null) =>
  value !== null && value.trim().length > 0;

const isBranchRef = (ref: string) => ref.startsWith("refs/heads/");

const isTagRef = (ref: string) => ref.startsWith("refs/tags/");

export const classifyRunTrust = (
  request: RunClassificationRequest,
  options: RunClassificationOptions = {}
): TrustClass => {
  const { git, github, workspace } = request;
  const trustedRefs =
    options.trustedRefs === undefined || options.trustedRefs.length === 0
      ? defaultTrustedRefs
      : options.trustedRefs;

  if (!isPresent(github.event)) {
    return "unknown";
  }
  if (!isPresent(git.ref)) {
    return "unknown";
  }
  if (!isPresent(workspace.repo)) {
    return "unknown";
  }

  if (github.event === "pull_request_target") {
    return "unknown";
  }
  if (github.event === "release") {
    return isTagRef(git.ref) ? "privileged" : "unknown";
  }
  if (isTagRef(git.ref)) {
    return "unknown";
  }

  if (github.event === "pull_request") {
    if (!isPresent(git.baseRef)) {
      return "unknown";
    }
    if (!isPresent(git.headRef)) {
      return "unknown";
    }
    if (!isPresent(git.headRepo)) {
      return "unknown";
    }

    return git.headRepo === workspace.repo ? "internal" : "external";
  }

  if (github.event !== "push") {
    return "unknown";
  }
  if (!isBranchRef(git.ref)) {
    return "unknown";
  }
  if (trustedRefs.includes(git.ref)) {
    return "trusted";
  }

  return "internal";
};
