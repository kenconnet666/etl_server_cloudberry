export type Id = string

export type SourceTopology = 'standalone' | 'physical_ha' | 'citus'
export type RuntimeState =
  | 'starting'
  | 'running'
  | 'resource_wait'
  | 'stopped'
  | 'failed'
  | 'degraded'
export type PipelinePhase =
  | 'draft'
  | 'validating'
  | 'snapshotting'
  | 'catching_up'
  | 'running'
  | 'paused'
  | 'degraded'
  | 'failed'
  | 'stopped'
export type OperationState =
  | 'requested'
  | 'pending'
  | 'running'
  | 'completed'
  | 'succeeded'
  | 'failed'
  | 'cancelled'

export interface Session {
  username: string
  csrf_token: string
  expires_in_seconds: number
}

export interface ConnectionSummary {
  host?: string
  port?: number
  username?: string
  tls_mode?: string
}

export interface Settings {
  connection?: ConnectionSummary
  [key: string]: unknown
}

export interface Source {
  id: Id
  name: string
  prefix: string
  database_name: string
  topology: SourceTopology
  settings: Settings
  enabled: boolean
  created_at: string
  updated_at: string
}

export interface Target {
  id: Id
  name: string
  database_name: string
  settings: Settings
  enabled: boolean
  created_at: string
  updated_at: string
}

export interface Pipeline {
  id: Id
  name: string
  source_id: Id
  target_id: Id
  desired_running: boolean
  config_revision: number
  snapshot_generation: number
  settings: Settings
  runtime_state: RuntimeState
  runtime?: PipelineRuntime | null
  created_at: string
  updated_at: string
}

export interface PipelineRuntime {
  phase: PipelinePhase
  message?: string | null
  telemetry?: PipelineTelemetry | null
}

export interface PipelineTelemetry {
  applied_lsn?: string | null
  wal_lag_bytes?: number | null
  replication_lag_seconds?: number | null
}

export interface Operation {
  id: Id
  pipeline_id: Id
  state: OperationState
  operation_type: string
  requested_at: string
  started_at?: string | null
  completed_at?: string | null
  error_message?: string | null
}
