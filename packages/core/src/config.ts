import { Schema } from "effect";

const WORKSPACE_PATH_PATTERN =
  /^(?!\.\.?$)(?!\/)(?![A-Za-z]:)(?!.*\\)(?!.*(?:^|\/)\.\.(?:\/|$)).+$/u;

export const configFileName = "stateful-ci.json";

export const clientVersion = "0.0.1";

export const nodePresetPaths = Object.freeze([
  "node_modules",
  ".pnpm-store",
  ".turbo",
  ".next/cache",
] as const);

export const builtInDeniedPathParts = Object.freeze([
  ".aws",
  ".azure",
  ".config/gcloud",
  ".docker/config.json",
  ".env",
  ".env.local",
  ".env.production",
  ".netrc",
  ".npmrc",
  ".pypirc",
  ".ssh",
] as const);

export const WorkspacePath = Schema.String.check(
  Schema.isPattern(WORKSPACE_PATH_PATTERN)
).pipe(Schema.brand("@stateful-ci/WorkspacePath"));
export type WorkspacePath = Schema.Schema.Type<typeof WorkspacePath>;

export const NodePresetConfig = Schema.Struct({
  preset: Schema.Literal("node"),
});
export type NodePresetConfig = Schema.Schema.Type<typeof NodePresetConfig>;

export const ExplicitWorkspaceConfig = Schema.Struct({
  exclude: Schema.optional(Schema.Array(WorkspacePath)),
  paths: Schema.NonEmptyArray(WorkspacePath),
});
export type ExplicitWorkspaceConfig = Schema.Schema.Type<
  typeof ExplicitWorkspaceConfig
>;

export const StatefulCiConfig = Schema.Union([
  NodePresetConfig,
  ExplicitWorkspaceConfig,
]);
export type StatefulCiConfig = Schema.Schema.Type<typeof StatefulCiConfig>;

export const defaultConfig: StatefulCiConfig = { preset: "node" };

const normalizeWorkspacePath = (path: string) => {
  const segments: string[] = [];

  for (const segment of path.replaceAll("\\", "/").split("/")) {
    if (segment === "" || segment === ".") {
      continue;
    }

    if (segment === "..") {
      segments.pop();
      continue;
    }

    segments.push(segment);
  }

  return segments.join("/");
};

const matchesPathOrDescendant = (path: string, candidate: string) => {
  const normalizedPath = normalizeWorkspacePath(path);
  const normalizedCandidate = normalizeWorkspacePath(candidate);

  return (
    normalizedPath === normalizedCandidate ||
    normalizedPath.startsWith(`${normalizedCandidate}/`)
  );
};

const pathSuffixes = (path: string) => {
  const segments = normalizeWorkspacePath(path).split("/").filter(Boolean);

  return segments.map((_, index) => segments.slice(index).join("/"));
};

const hasDotenvSegment = (path: string) =>
  normalizeWorkspacePath(path)
    .split("/")
    .some((segment) => segment === ".env" || segment.startsWith(".env."));

export const workspacePathsForConfig = (
  config: StatefulCiConfig
): readonly string[] => ("preset" in config ? nodePresetPaths : config.paths);

export const excludedPathsForConfig = (
  config: StatefulCiConfig
): readonly string[] => ("preset" in config ? [] : (config.exclude ?? []));

export const isBuiltInDeniedWorkspacePath = (path: string) =>
  hasDotenvSegment(path) ||
  pathSuffixes(path).some((suffix) =>
    builtInDeniedPathParts.some((denied) =>
      matchesPathOrDescendant(suffix, denied)
    )
  );

export const isUserExcludedWorkspacePath = (
  path: string,
  excludes: readonly string[]
) => excludes.some((excluded) => matchesPathOrDescendant(path, excluded));
