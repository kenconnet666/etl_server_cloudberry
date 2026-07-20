import type {
  ConnectionReport,
  CreatePipelineRequest,
  CreateSourceRequest,
  CreateTargetRequest,
  Operation,
  Overview,
  Pipeline,
  Session,
  Source,
  Target
} from './types';

const API_ROOT = (import.meta.env.VITE_API_ROOT || '/api/v1').replace(/\/$/, '');
let csrfToken = '';

interface ApiEnvelope<T> {
  data?: T;
  items?: T;
  message?: string;
  error?: string | { code?: string; message?: string };
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export function setCsrfToken(value?: string): void {
  csrfToken = value || '';
}

function errorDetails(payload: unknown, fallback: string): { message: string; code?: string } {
  if (!payload || typeof payload !== 'object') return { message: fallback };
  const envelope = payload as ApiEnvelope<unknown>;
  if (typeof envelope.error === 'string') return { message: envelope.error };
  if (envelope.error && typeof envelope.error === 'object') {
    return {
      message: envelope.error.message || envelope.message || fallback,
      code: envelope.error.code
    };
  }
  return { message: envelope.message || fallback };
}

async function decodeResponse(response: Response): Promise<unknown> {
  if (response.status === 204) return undefined;
  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) return response.json();
  const body = await response.text();
  return body ? { message: body } : undefined;
}

export async function apiRequest<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers = new Headers(options.headers);
  headers.set('Accept', 'application/json');
  if (options.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  if (csrfToken && options.method && !['GET', 'HEAD'].includes(options.method.toUpperCase())) {
    headers.set('X-CSRF-Token', csrfToken);
  }

  let response: Response;
  try {
    response = await fetch(`${API_ROOT}${path}`, {
      ...options,
      headers,
      credentials: 'include'
    });
  } catch (error) {
    const detail = error instanceof Error ? error.message : 'Network request failed';
    throw new ApiError(`Management API unavailable: ${detail}`, 0, 'network_error');
  }

  const payload = await decodeResponse(response);
  if (!response.ok) {
    const details = errorDetails(payload, `${response.status} ${response.statusText}`);
    throw new ApiError(details.message, response.status, details.code);
  }
  if (payload && typeof payload === 'object' && 'data' in payload) {
    return (payload as ApiEnvelope<T>).data as T;
  }
  return payload as T;
}

export function collection<T>(payload: T[] | { items?: T[]; data?: T[] } | undefined): T[] {
  if (Array.isArray(payload)) return payload;
  if (payload && Array.isArray(payload.items)) return payload.items;
  if (payload && Array.isArray(payload.data)) return payload.data;
  return [];
}

export const api = {
  session: () => apiRequest<Session>('/auth/session'),
  login: (username: string, password: string) =>
    apiRequest<Session>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password })
    }),
  logout: () => apiRequest<void>('/auth/logout', { method: 'POST' }),

  overview: () => apiRequest<Overview>('/overview'),

  sources: async () => collection(await apiRequest<Source[] | { items: Source[] }>('/sources')),
  createSource: (input: CreateSourceRequest) =>
    apiRequest<Source>('/sources', { method: 'POST', body: JSON.stringify(input) }),
  testSource: (dsn: string) =>
    apiRequest<ConnectionReport>('/sources/test', {
      method: 'POST',
      body: JSON.stringify({ dsn })
    }),

  targets: async () => collection(await apiRequest<Target[] | { items: Target[] }>('/targets')),
  createTarget: (input: CreateTargetRequest) =>
    apiRequest<Target>('/targets', { method: 'POST', body: JSON.stringify(input) }),
  testTarget: (dsn: string) =>
    apiRequest<ConnectionReport>('/targets/test', {
      method: 'POST',
      body: JSON.stringify({ dsn })
    }),

  pipelines: async () => collection(await apiRequest<Pipeline[] | { items: Pipeline[] }>('/pipelines')),
  pipeline: (id: string) => apiRequest<Pipeline>(`/pipelines/${encodeURIComponent(id)}`),
  createPipeline: (input: CreatePipelineRequest) =>
    apiRequest<Pipeline>('/pipelines', { method: 'POST', body: JSON.stringify(input) }),
  pipelineAction: (id: string, action: 'start' | 'pause' | 'rebuild') =>
    apiRequest<Pipeline>(`/pipelines/${encodeURIComponent(id)}/${action}`, { method: 'POST' }),

  operations: async () =>
    collection(await apiRequest<Operation[] | { items: Operation[] }>('/operations'))
};

export function apiErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unexpected request failure';
}
