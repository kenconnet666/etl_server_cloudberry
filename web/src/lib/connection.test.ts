import { describe, expect, it } from 'vitest';

import { postgresDsn, sourceRequest, targetRequest } from './connection';

describe('postgresDsn', () => {
  it('escapes credentials and database names', () => {
    expect(
      postgresDsn({
        host: 'pg.internal',
        port: 5432,
        database_name: 'sales data',
        username: 'etl@reader',
        password: 'p/a:ss?#',
        tls_mode: 'verify-full'
      })
    ).toBe(
      'postgresql://etl%40reader:p%2Fa%3Ass%3F%23@pg.internal:5432/sales%20data?sslmode=verify-full'
    );
  });

  it('wraps a bare IPv6 host', () => {
    expect(
      postgresDsn({
        host: '2001:db8::1',
        port: 5433,
        database_name: 'postgres',
        username: 'etl',
        password: 'secret',
        tls_mode: 'require'
      })
    ).toContain('@[2001:db8::1]:5433/postgres');
  });
});

describe('connection requests', () => {
  it('keeps source passwords out of settings', () => {
    const request = sourceRequest({
      name: ' Orders ',
      prefix: ' orders_prod ',
      database_name: ' orders ',
      topology: 'citus',
      host: ' coordinator.internal ',
      port: 5432,
      username: 'reader',
      password: 'do-not-store',
      tls_mode: 'verify-full'
    });

    expect(request).toMatchObject({
      name: 'Orders',
      prefix: 'orders_prod',
      database_name: 'orders',
      topology: 'citus',
      settings: {
        connection: {
          host: 'coordinator.internal',
          port: 5432,
          username: 'reader',
          tls_mode: 'verify-full'
        }
      }
    });
    expect(JSON.stringify(request.settings)).not.toContain('do-not-store');
    expect(request.dsn).toContain(':do-not-store@');
  });

  it('builds the target API contract', () => {
    const request = targetRequest({
      name: 'Warehouse',
      database_name: 'analytics',
      host: 'cb.internal',
      port: 5432,
      username: 'writer',
      password: 'secret',
      tls_mode: 'require'
    });

    expect(Object.keys(request).sort()).toEqual(['database_name', 'dsn', 'name', 'settings']);
  });
});
