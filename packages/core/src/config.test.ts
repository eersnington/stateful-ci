import { Schema } from "effect";
import { describe, expect, test } from "vitest";

import { StatefulCiConfig } from "./index";

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
    expect(() =>
      Schema.decodeUnknownSync(StatefulCiConfig)({ preset: "rust" })
    ).toThrow(/Missing key|Expected/u);
  });

  test("rejects empty explicit paths", () => {
    expect(() =>
      Schema.decodeUnknownSync(StatefulCiConfig)({ paths: [] })
    ).toThrow(/Missing key/u);
  });

  test("rejects absolute paths", () => {
    expect(() =>
      Schema.decodeUnknownSync(StatefulCiConfig)({ paths: ["/tmp/cache"] })
    ).toThrow(/RegExp/u);
  });

  test("rejects paths that escape the workspace", () => {
    expect(() =>
      Schema.decodeUnknownSync(StatefulCiConfig)({ paths: ["../node_modules"] })
    ).toThrow(/RegExp/u);
  });
});
