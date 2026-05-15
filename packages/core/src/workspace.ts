import { Schema } from "effect";

export const WorkspaceRef = Schema.Struct({
  job: Schema.String.check(Schema.isMinLength(1)),
  repo: Schema.String.check(Schema.isMinLength(1)),
  workflow: Schema.String.check(Schema.isMinLength(1)),
});
export type WorkspaceRef = Schema.Schema.Type<typeof WorkspaceRef>;
