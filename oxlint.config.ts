import { defineConfig } from "oxlint";
import core from "ultracite/oxlint/core";
import react from "ultracite/oxlint/react";
import solid from "ultracite/oxlint/solid";
import vitest from "ultracite/oxlint/vitest";

/*
Patch the Ultracite Vitest override directly; a later local override does not
suppress this rule once it is enabled inside the preset override.
*/
const patchedVitest = defineConfig({
  ...vitest,
  overrides: vitest.overrides?.map((override) => ({
    ...override,
    rules: {
      ...override.rules,
      "vitest/prefer-importing-vitest-globals": "off",
    },
  })),
});

export default defineConfig({
  extends: [core, react, solid, patchedVitest],
});
