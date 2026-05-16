import { Schema } from "effect";

export const TrustClass = Schema.Literals([
  "external",
  "internal",
  "trusted",
  "privileged",
  "unknown",
]);
export type TrustClass = Schema.Schema.Type<typeof TrustClass>;

export const DenialReason = Schema.Literals([
  "backend_policy_not_configured",
  "unable_to_classify_run_context",
  "no_compatible_snapshot",
  "restore_required_before_save",
  "external_snapshot_cannot_update_trusted_workspace",
  "invalid_protocol_payload",
]);
export type DenialReason = Schema.Schema.Type<typeof DenialReason>;
