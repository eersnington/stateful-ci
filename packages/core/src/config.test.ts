import { Result, Schema } from "effect";
import { describe, expect, test } from "vitest";

import { StatefulCiConfig } from "./index";

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
});
