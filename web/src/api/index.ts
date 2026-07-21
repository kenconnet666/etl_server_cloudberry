import type { Session, Source, Target, Pipeline, Operation } from '../types'

let csrfToken: string | undefined

export function setCsrfToken(token?: string) {
  csrfToken = token
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export function apiErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return `API error (${error.status}): ${error.message}`
  }
  if (error instanceof Error) {
    return error.message
  }
  return String(error)
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json'
  }
  if (csrfToken) {
    headers['X-CSRF-Token'] = csrfToken
  }

  const response = await fetch(`/api${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    credentials: 'same-origin'
  })

  if (!response.ok) {
    const text = await response.text()
    throw new ApiError(response.status, text || response.statusText)
  }

  if (response.status === 204) {
    return undefined as T
  }

  return response.json()
}

export const api = {
  // Auth
  session: () => request<Session>('GET', '/session'),
  login: (username: string, password: string) =>
    request<Session>('POST', '/session', { username, password }),
  logout: () => request<void>('DELETE', '/session'),

  // Sources
  listSources: () => request<Source[]>('GET', '/sources'),
  createSource: (source: Omit<Source, 'id' | 'created_at' | 'updated_at'>) =>
    request<Source>('POST', '/sources', source),
  updateSource: (id: string, source: Partial<Source>) =>
    request<Source>('PUT', `/sources/${id}`, source),
  deleteSource: (id: string) => request<void>('DELETE', `/sources/${id}`),

  // Targets
  listTargets: () => request<Target[]>('GET', '/targets'),
  createTarget: (target: Omit<Target, 'id' | 'created_at' | 'updated_at'>) =>
    request<Target>('POST', '/targets', target),
  updateTarget: (id: string, target: Partial<Target>) =>
    request<Target>('PUT', `/targets/${id}`, target),
  deleteTarget: (id: string) => request<void>('DELETE', `/targets/${id}`),

  // Pipelines
  listPipelines: () => request<Pipeline[]>('GET', '/pipelines'),
  getPipeline: (id: string) => request<Pipeline>('GET', `/pipelines/${id}`),
  createPipeline: (pipeline: Omit<Pipeline, 'id' | 'created_at' | 'updated_at' | 'runtime'>) =>
    request<Pipeline>('POST', '/pipelines', pipeline),
  updatePipeline: (id: string, pipeline: Partial<Pipeline>) =>
    request<Pipeline>('PUT', `/pipelines/${id}`, pipeline),
  deletePipeline: (id: string) => request<void>('DELETE', `/pipelines/${id}`),

  // Operations
  listOperations: (pipelineId?: string) => {
    const path = pipelineId ? `/operations?pipeline_id=${pipelineId}` : '/operations'
    return request<Operation[]>('GET', path)
  },
  requestOperation: (pipelineId: string, operationType: string) =>
    request<Operation>('POST', '/operations', { pipeline_id: pipelineId, operation_type: operationType })
}
