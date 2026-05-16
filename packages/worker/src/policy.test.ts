import type { TrustClass } from "@stateful-ci/core";
import { describe, expect, test } from "vitest";

import { evaluateRestorePolicy, evaluateSavePolicy } from "./policy";

const scope = (trustClass: TrustClass, scopeKey: string = trustClass) => ({
  scopeKey,
  trustClass,
});

describe("trust policy", () => {
  test("allows trusted snapshots to seed every classified run", () => {
    expect(
      ["trusted", "internal", "external", "privileged"].map((trustClass) =>
        evaluateRestorePolicy({
          consumer: scope(trustClass as TrustClass, `consumer:${trustClass}`),
          producer: scope("trusted", "trusted:main"),
        })
      )
    ).toStrictEqual([
      { allowed: true },
      { allowed: true },
      { allowed: true },
      { allowed: true },
    ]);
  });

  test("allows external and internal snapshots only in the same scope", () => {
    expect(
      evaluateRestorePolicy({
        consumer: scope("external", "pr:12"),
        producer: scope("external", "pr:12"),
      })
    ).toStrictEqual({ allowed: true });
    expect(
      evaluateRestorePolicy({
        consumer: scope("internal", "branch:feature"),
        producer: scope("internal", "branch:feature"),
      })
    ).toStrictEqual({ allowed: true });
    expect(
      evaluateRestorePolicy({
        consumer: scope("privileged", "release"),
        producer: scope("privileged", "release"),
      })
    ).toStrictEqual({ allowed: true });
  });

  test("denies untrusted state flowing upward", () => {
    expect(
      evaluateRestorePolicy({
        consumer: scope("trusted", "trusted:main"),
        producer: scope("external", "pr:12"),
      })
    ).toStrictEqual({ allowed: false, reason: "restore_policy_denied" });
    expect(
      evaluateRestorePolicy({
        consumer: scope("privileged", "release"),
        producer: scope("internal", "branch:feature"),
      })
    ).toStrictEqual({ allowed: false, reason: "restore_policy_denied" });
  });

  test("denies unknown restore contexts", () => {
    expect(
      evaluateRestorePolicy({
        consumer: scope("trusted"),
        producer: scope("unknown"),
      })
    ).toStrictEqual({ allowed: false, reason: "unknown_context_denied" });
  });

  test("allows trusted and internal saves but disables external and privileged saves", () => {
    expect(evaluateSavePolicy(scope("trusted"))).toStrictEqual({
      allowed: true,
      target: scope("trusted"),
    });
    expect(evaluateSavePolicy(scope("internal"))).toStrictEqual({
      allowed: true,
      target: scope("internal"),
    });
    expect(evaluateSavePolicy(scope("external"))).toStrictEqual({
      allowed: false,
      reason: "external_save_disabled",
    });
    expect(evaluateSavePolicy(scope("privileged"))).toStrictEqual({
      allowed: false,
      reason: "privileged_save_disabled",
    });
  });
});
