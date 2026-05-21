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
  "external_save_disabled",
  "head_generation_mismatch",
  "idempotency_conflict",
  "invalid_protocol_payload",
  "manifest_digest_mismatch",
  "manifest_schema_invalid",
  "no_compatible_snapshot",
  "oidc_audience_mismatch",
  "oidc_invalid",
  "oidc_issuer_mismatch",
  "oidc_missing",
  "privileged_save_disabled",
  "pull_request_target_denied",
  "restore_policy_denied",
  "restore_required_before_save",
  "save_policy_denied",
  "save_run_context_mismatch",
  "snapshot_object_mismatch",
  "snapshot_object_missing",
  "unable_to_classify_identity",
  "unable_to_classify_run_context",
  "unknown_context_denied",
  "unsafe_manifest_path",
  "external_snapshot_cannot_update_trusted_workspace",
]);
export type DenialReason = Schema.Schema.Type<typeof DenialReason>;
