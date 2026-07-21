export type Id = string;

export type SourceTopology = 'standalone' | 'physical_ha' | 'citus';
export type RuntimeState =
  | 'starting'
  | 'running'
  | 'resource_wait'
  | 'stopped'
  | 'failed'
  | 'degraded';
export type PipelinePhase =
  | 'draft'
  | 'validating'
  | 'snapshotting'
  | 'catching_up'
  | 'running'
  | 'paused'
  | 'degraded'
  | 'failed'
  | 'stopped';
export type OperationState =
  | 'requested'
  | 'pending'
  | 'running'
  | 'completed'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export interface Session {
  username: string;
  csrf_token: string;
  expires_in_seconds: number;
}

export interface ConnectionSummary {
  host?: string;
  port?: number;
  username?: string;
  tls_mode?: string;
}

export interface Settings {
  connection?: ConnectionSummary;
  [key: string]: unknown;
}

export interface Source {
  id: Id;
  name: string;
  prefix: string;
  database_name: string;
  topology: SourceTopology;
  settings: Settings;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface Target {
  id: Id;
  name: string;
  database_name: string;
  settings: Settings;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface Pipeline {
  id: Id;
  name: string;
  source_id: Id;
  target_id: Id;
  desired_running: boolean;
  config_revision: number;
  snapshot_generation: number;
  settings: Settings;
  runtime_state: RuntimeState;
  runtime?: PipelineRuntime | null;
  created_at: string;
  updated_at: string;
}

export interface PipelineRuntime {
  pipeline_id: Id;
  phase: PipelinePhase;
  state: RuntimeState;
  source_received_lsn?: string | null;
  source_current_lsn?: string | null;
  target_checkpoint_lsn?: string | null;
  estimated_byte_lag?: number | null;
  spool_bytes?: number | null;
  resource_wait_reason?: string | null;
  slot_retained_wal_bytes?: number | null;
  slot_safe_wal_bytes?: number | null;
  wal_retention_warning: boolean;
  last_transaction_at?: string | null;
  last_apply_at?: string | null;
  last_ack_at?: string | null;
  started_at?: string | null;
  stopped_at?: string | null;
  restart_count: number;
  last_error?: string | null;
}

export interface Operation {
  id: Id;
  pipeline_id: Id | null;
  operation_type: string;
  state: OperationState;
  detail: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
  runtime?: PipelineRuntime | null;
}

export interface Overview {
  sources: number;
  targets: number;
  pipelines: number;
  running_pipelines: number;
}

export interface SourceForm {
  name: string;
  prefix: string;
  database_name: string;
  topology: SourceTopology;
  host: string;
  port: number;
  username: string;
  password: string;
  tls_mode: string;
}

export interface TargetForm {
  name: string;
  database_name: string;
  host: string;
  port: number;
  username: string;
  password: string;
  tls_mode: string;
}

export interface CreateSourceRequest {
  name: string;
  prefix: string;
  database_name: string;
  topology: SourceTopology;
  dsn: string;
  settings: Settings;
}

export interface CreateTargetRequest {
  name: string;
  database_name: string;
  dsn: string;
  settings: Settings;
}

export interface CreatePipelineRequest {
  name: string;
  source_id: Id;
  target_id: Id;
  settings: Settings;
}

export interface ConnectionReport {
  server_version: string;
  topology: string;
  warnings: string[];
}
