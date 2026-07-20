<script lang="ts">
  import { label } from '../format';

  let { value, compact = false }: { value?: string; compact?: boolean } = $props();

  const positive = new Set(['healthy', 'running', 'succeeded', 'enabled']);
  const warning = new Set([
    'validating',
    'snapshotting',
    'catching_up',
    'degraded',
    'pending',
    'rebuild_required',
    'rebuilding'
  ]);
  const negative = new Set(['failed', 'unreachable', 'blocked', 'cancelled']);
  const quiet = new Set(['paused', 'stopped', 'quarantined', 'unknown', 'draft', 'disabled']);

  let tone = $derived(
    positive.has(value || '')
      ? 'positive'
      : warning.has(value || '')
        ? 'warning'
        : negative.has(value || '')
          ? 'negative'
          : quiet.has(value || '')
            ? 'quiet'
            : 'neutral'
  );
</script>

<span class:compact class="status-badge {tone}">
  <span class="status-dot" aria-hidden="true"></span>
  {label(value)}
</span>
