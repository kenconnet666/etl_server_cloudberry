<script lang="ts">
  import { AlertCircle, CheckCircle2, Info, TriangleAlert } from '@lucide/svelte';

  let {
    tone = 'info',
    title,
    detail,
    actionLabel,
    onAction
  }: {
    tone?: 'info' | 'success' | 'warning' | 'error';
    title: string;
    detail?: string;
    actionLabel?: string;
    onAction?: () => void;
  } = $props();
</script>

<div class="inline-notice {tone}" role={tone === 'error' ? 'alert' : 'status'}>
  <div class="notice-icon" aria-hidden="true">
    {#if tone === 'success'}
      <CheckCircle2 size={18} />
    {:else if tone === 'warning'}
      <TriangleAlert size={18} />
    {:else if tone === 'error'}
      <AlertCircle size={18} />
    {:else}
      <Info size={18} />
    {/if}
  </div>
  <div class="notice-copy">
    <strong>{title}</strong>
    {#if detail}<span>{detail}</span>{/if}
  </div>
  {#if actionLabel && onAction}
    <button class="button ghost small" type="button" onclick={onAction}>{actionLabel}</button>
  {/if}
</div>
