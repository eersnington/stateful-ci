create table if not exists refs (
  namespace text not null,
  ref_name text not null,
  snapshot_id text not null,
  generation integer not null,
  trust_class text not null,
  updated_at text not null,
  updated_by_run_id text,
  updated_by_actor text,
  primary key (namespace, ref_name)
);

create table if not exists snapshots (
  snapshot_id text primary key,
  workspace_id text not null,
  namespace text not null,
  parent_snapshot_id text,
  manifest_key text not null,
  manifest_digest text not null,
  manifest_size integer not null,
  trust_class text not null,
  producer_repository text not null,
  producer_workflow text not null,
  producer_job text not null,
  producer_ref text not null,
  producer_event text not null,
  producer_sha text not null,
  producer_actor text not null,
  producer_run_id text not null,
  stats_json text not null,
  safety_json text not null,
  created_at text not null
);

create table if not exists snapshot_objects (
  snapshot_id text not null,
  object_key text not null,
  object_digest text not null,
  object_kind text not null,
  size integer not null,
  primary key (snapshot_id, object_key)
);

create table if not exists workspace_targets (
  workspace_id text primary key,
  namespace text not null,
  ref_name text not null,
  run_id text not null,
  trust_class text not null,
  expires_at text,
  producer_repository text,
  producer_workflow text,
  producer_job text,
  producer_ref text,
  producer_event text,
  producer_sha text,
  producer_actor text
);

create table if not exists idempotent_commits (
  idempotency_key text primary key,
  workspace_id text not null,
  run_id text not null,
  snapshot_id text not null,
  manifest_digest text not null,
  head_generation integer not null,
  latest integer not null,
  result_json text not null,
  created_at text not null
);

create table if not exists audit_events (
  id text primary key,
  namespace text not null,
  ref_name text not null,
  workspace_id text,
  run_id text,
  snapshot_id text,
  trust_class text,
  event_type text not null,
  decision text not null,
  reason text,
  payload_json text,
  created_at text not null
);

create index if not exists snapshots_workspace_id_idx on snapshots (workspace_id);
create index if not exists snapshot_objects_snapshot_id_idx on snapshot_objects (snapshot_id);
create index if not exists audit_events_workspace_id_idx on audit_events (workspace_id);
create index if not exists audit_events_run_id_idx on audit_events (run_id);
