import type { DenialReason, TrustClass } from "@stateful-ci/core";

export interface PolicyScope {
  readonly scopeKey: string;
  readonly trustClass: TrustClass;
}

export type RestorePolicyDecision =
  | { readonly allowed: true }
  | { readonly allowed: false; readonly reason: DenialReason };

export type SavePolicyDecision =
  | { readonly allowed: true; readonly target: PolicyScope }
  | { readonly allowed: false; readonly reason: DenialReason };

const sameScope = (producer: PolicyScope, consumer: PolicyScope) =>
  producer.scopeKey === consumer.scopeKey;

export const evaluateRestorePolicy = (input: {
  readonly consumer: PolicyScope;
  readonly producer: PolicyScope;
}): RestorePolicyDecision => {
  const { consumer, producer } = input;

  if (consumer.trustClass === "unknown" || producer.trustClass === "unknown") {
    return { allowed: false, reason: "unknown_context_denied" };
  }

  if (producer.trustClass === "trusted") {
    return { allowed: true };
  }

  if (producer.trustClass === "external") {
    return consumer.trustClass === "external" && sameScope(producer, consumer)
      ? { allowed: true }
      : { allowed: false, reason: "restore_policy_denied" };
  }

  if (producer.trustClass === "internal") {
    return consumer.trustClass === "internal" && sameScope(producer, consumer)
      ? { allowed: true }
      : { allowed: false, reason: "restore_policy_denied" };
  }

  if (producer.trustClass === "privileged") {
    return consumer.trustClass === "privileged" && sameScope(producer, consumer)
      ? { allowed: true }
      : { allowed: false, reason: "restore_policy_denied" };
  }

  return { allowed: false, reason: "restore_policy_denied" };
};

export const evaluateSavePolicy = (target: PolicyScope): SavePolicyDecision => {
  if (target.trustClass === "trusted" || target.trustClass === "internal") {
    return { allowed: true, target };
  }

  if (target.trustClass === "external") {
    return { allowed: false, reason: "external_save_disabled" };
  }

  if (target.trustClass === "privileged") {
    return { allowed: false, reason: "privileged_save_disabled" };
  }

  return { allowed: false, reason: "unknown_context_denied" };
};
