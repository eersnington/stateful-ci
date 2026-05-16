import { Schema } from "effect";
import { describe, expect, test } from "vitest";

import { StatefulCiConfig } from "./index";

const decodeFails = (value: unknown) => {
  try {
    Schema.decodeUnknownSync(StatefulCiConfig)(value);
    return false;
  } catch {
    return true;
  }
};

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
    expect(decodeFails({ preset: "rust" })).toBeTruthy();
  });

  test("rejects empty explicit paths", () => {
    expect(decodeFails({ paths: [] })).toBeTruthy();
  });

  test("rejects absolute paths", () => {
    expect(decodeFails({ paths: ["/tmp/cache"] })).toBeTruthy();
  });

  test("rejects paths that escape the workspace", () => {
    expect(decodeFails({ paths: ["../node_modules"] })).toBeTruthy();
  });
});
