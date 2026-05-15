import { Schema } from "effect";

const WORKSPACE_PATH_PATTERN =
  /^(?!\.\.?$)(?!\/)(?![A-Za-z]:)(?!.*\\)(?!.*(?:^|\/)\.\.(?:\/|$)).+$/u;

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
