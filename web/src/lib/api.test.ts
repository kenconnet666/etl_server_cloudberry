import { afterEach, describe, expect, it, vi } from 'vitest';

import { api, apiRequest, setCsrfToken } from './api';

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' }
  });
}

afterEach(() => {
  setCsrfToken();
  vi.unstubAllGlobals();
});

describe('api client', () => {
  it('normalizes list envelopes and includes cookie credentials', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ items: [] }));
    vi.stubGlobal('fetch', fetchMock);

    await expect(api.sources()).resolves.toEqual([]);
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/sources',
      expect.objectContaining({ credentials: 'include' })
    );
  });

  it('sends the CSRF token and exact pipeline body on mutation', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ id: 'pipeline-1' }, 201));
    vi.stubGlobal('fetch', fetchMock);
    setCsrfToken('csrf-value');

    await api.createPipeline({
      name: 'Orders',
      source_id: 'source-1',
      target_id: 'target-1',
      settings: {}
    });

    const [, options] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(new Headers(options.headers).get('X-CSRF-Token')).toBe('csrf-value');
    expect(JSON.parse(options.body as string)).toEqual({
      name: 'Orders',
      source_id: 'source-1',
      target_id: 'target-1',
      settings: {}
    });
  });

  it('parses the backend error envelope', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        jsonResponse({ error: { code: 'conflict', message: 'prefix already exists' } }, 409)
      )
    );

    const request = apiRequest('/sources');
    await expect(request).rejects.toMatchObject({
      status: 409,
      code: 'conflict',
      message: 'prefix already exists'
    });
  });

  it('wraps network failures with an offline status', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('connection refused')));

    await expect(api.overview()).rejects.toMatchObject({
      status: 0,
      code: 'network_error'
    });
  });
});
