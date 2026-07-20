import type {
  CreateSourceRequest,
  CreateTargetRequest,
  SourceForm,
  TargetForm
} from './types';

interface DsnFields {
  host: string;
  port: number;
  database_name: string;
  username: string;
  password: string;
  tls_mode: string;
}

export function postgresDsn(fields: DsnFields): string {
  const host = fields.host.trim();
  const formattedHost = host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
  return (
    `postgresql://${encodeURIComponent(fields.username)}:${encodeURIComponent(fields.password)}` +
    `@${formattedHost}:${fields.port}/${encodeURIComponent(fields.database_name)}` +
    `?sslmode=${encodeURIComponent(fields.tls_mode)}`
  );
}

export function sourceRequest(form: SourceForm): CreateSourceRequest {
  return {
    name: form.name.trim(),
    prefix: form.prefix.trim(),
    database_name: form.database_name.trim(),
    topology: form.topology,
    dsn: postgresDsn(form),
    settings: {
      connection: {
        host: form.host.trim(),
        port: form.port,
        username: form.username,
        tls_mode: form.tls_mode
      }
    }
  };
}

export function targetRequest(form: TargetForm): CreateTargetRequest {
  return {
    name: form.name.trim(),
    database_name: form.database_name.trim(),
    dsn: postgresDsn(form),
    settings: {
      connection: {
        host: form.host.trim(),
        port: form.port,
        username: form.username,
        tls_mode: form.tls_mode
      }
    }
  };
}
