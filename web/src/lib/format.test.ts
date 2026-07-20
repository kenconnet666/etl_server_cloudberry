import { describe, expect, it } from 'vitest';

import { formatBytes, formatDateTime, formatDuration, truncateMiddle } from './format';

describe('format helpers', () => {
  it('formats byte boundaries', () => {
    expect(formatBytes(0)).toBe('0 B');
    expect(formatBytes(0.5)).toBe('1 B');
    expect(formatBytes(1024)).toBe('1.0 KiB');
    expect(formatBytes(10 * 1024 * 1024)).toBe('10 MiB');
  });

  it('formats durations without unstable precision', () => {
    expect(formatDuration(0.2)).toBe('<1s');
    expect(formatDuration(90)).toBe('1m 30s');
    expect(formatDuration(3660)).toBe('1h 1m');
  });

  it('preserves invalid timestamps for diagnosis', () => {
    expect(formatDateTime('not-a-timestamp')).toBe('not-a-timestamp');
  });

  it('truncates identifiers from the middle', () => {
    expect(truncateMiddle('abcdefghijklmnop', 9)).toBe('abcd…mnop');
  });
});
