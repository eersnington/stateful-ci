import { defineConfig } from "oxlint";
import core from "ultracite/oxlint/core";
import react from "ultracite/oxlint/react";
import solid from "ultracite/oxlint/solid";
import vitest from "ultracite/oxlint/vitest";

export default defineConfig({
  extends: [core, react, solid, vitest],
});
