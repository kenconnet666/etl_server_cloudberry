export type Id = string;

export type SourceTopology = 'standalone' | 'physical_ha' | 'citus';
export type RuntimeState = 'running' | 'stopped';
export type OperationState = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';

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
  settings: Settings;
  runtime_state: RuntimeState;
  created_at: string;
  updated_at: string;
}

export interface Operation {
  id: Id;
  operation_type: string;
  state: OperationState;
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
