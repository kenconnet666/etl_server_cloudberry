const NUMBER = new Intl.NumberFormat('en-US');
const DATE_TIME = new Intl.DateTimeFormat('en-US', {
  dateStyle: 'medium',
  timeStyle: 'short'
});

export function formatNumber(value?: number): string {
  return value === undefined || !Number.isFinite(value) ? '—' : NUMBER.format(value);
}

export function formatBytes(value?: number): string {
  if (value === undefined || !Number.isFinite(value)) return '—';
  if (value === 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  const exponent = Math.min(Math.floor(Math.log(Math.abs(value)) / Math.log(1024)), units.length - 1);
  const amount = value / 1024 ** exponent;
  return `${amount >= 10 || exponent === 0 ? amount.toFixed(0) : amount.toFixed(1)} ${units[exponent]}`;
}

export function formatDuration(seconds?: number): string {
  if (seconds === undefined || !Number.isFinite(seconds)) return '—';
  if (seconds < 1) return '<1s';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
}

export function formatDateTime(value?: string): string {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : DATE_TIME.format(date);
}

export function label(value?: string): string {
  if (!value) return 'Unknown';
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export function truncateMiddle(value: string, maximum = 24): string {
  if (value.length <= maximum) return value;
  const side = Math.floor((maximum - 1) / 2);
  return `${value.slice(0, side)}…${value.slice(-side)}`;
}
