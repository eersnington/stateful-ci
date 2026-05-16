import { Result, Schema } from "effect";
import { describe, expect, test } from "vitest";

import {
  builtInDeniedPathParts,
  excludedPathsForConfig,
  isBuiltInDeniedWorkspacePath,
  isUserExcludedWorkspacePath,
  nodePresetPaths,
  StatefulCiConfig,
  workspacePathsForConfig,
} from "./index";

const decodeResult = Schema.decodeUnknownResult(StatefulCiConfig);

describe("config schemas", () => {
  test("decodes the default node preset", () => {
    expect(
      Schema.decodeUnknownSync(StatefulCiConfig)({ preset: "node" })
    ).toStrictEqual({ preset: "node" });
  });

  test("decodes explicit workspace paths and excludes", () => {
    const config = {
      exclude: ["coverage"],
      paths: ["node_modules", ".pnpm-store", ".turbo", ".next/cache"],
    };

    expect(Schema.decodeUnknownSync(StatefulCiConfig)(config)).toStrictEqual(
      config
    );
  });

  test("rejects unknown presets", () => {
    expect(Result.isFailure(decodeResult({ preset: "rust" }))).toBeTruthy();
  });

  test("rejects empty explicit paths", () => {
    expect(Result.isFailure(decodeResult({ paths: [] }))).toBeTruthy();
  });

  test("rejects absolute paths", () => {
    expect(
      Result.isFailure(decodeResult({ paths: ["/tmp/cache"] }))
    ).toBeTruthy();
  });

  test("rejects paths that escape the workspace", () => {
    expect(
      Result.isFailure(decodeResult({ paths: ["../node_modules"] }))
    ).toBeTruthy();
  });

  test("expands preset and explicit workspace path policy", () => {
    const explicitConfig = Schema.decodeUnknownSync(StatefulCiConfig)({
      exclude: ["coverage"],
      paths: [".turbo"],
    });

    expect(workspacePathsForConfig({ preset: "node" })).toBe(nodePresetPaths);
    expect(workspacePathsForConfig(explicitConfig)).toStrictEqual([".turbo"]);
    expect(excludedPathsForConfig(explicitConfig)).toStrictEqual(["coverage"]);
  });

  test("freezes exported workspace path defaults", () => {
    expect(Object.isFrozen(nodePresetPaths)).toBeTruthy();
    expect(Object.isFrozen(builtInDeniedPathParts)).toBeTruthy();
  });

  test("matches built-in dotenv workspace path safety rules", () => {
    expect(isBuiltInDeniedWorkspacePath(".env")).toBeTruthy();
    expect(isBuiltInDeniedWorkspacePath("./.env")).toBeTruthy();
    expect(isBuiltInDeniedWorkspacePath(".env.test")).toBeTruthy();
    expect(isBuiltInDeniedWorkspacePath("apps/.env.preview")).toBeTruthy();
    expect(isBuiltInDeniedWorkspacePath("apps/.environment")).toBeFalsy();
  });

  test("matches built-in nested credential path safety rules", () => {
    expect(isBuiltInDeniedWorkspacePath(".ssh/id_rsa")).toBeTruthy();
    expect(isBuiltInDeniedWorkspacePath("apps/.ssh/id_rsa")).toBeTruthy();
    expect(
      isBuiltInDeniedWorkspacePath("apps/.config/gcloud/config.json")
    ).toBeTruthy();
    expect(isBuiltInDeniedWorkspacePath("apps/.docker/trust")).toBeTruthy();
    expect(isBuiltInDeniedWorkspacePath("apps/not.ssh/id_rsa")).toBeFalsy();
  });

  test("matches workspace excludes with normalized paths", () => {
    expect(isBuiltInDeniedWorkspacePath(".turbo/cache")).toBeFalsy();
    expect(
      isUserExcludedWorkspacePath("./.turbo/cache/index.db", [".turbo/cache"])
    ).toBeTruthy();
  });
});
