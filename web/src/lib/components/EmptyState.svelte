<script lang="ts">
  import type { Component } from 'svelte';

  let {
    icon: Icon,
    title,
    detail,
    actionLabel,
    onAction
  }: {
    icon: Component<{ size?: number; strokeWidth?: number }>;
    title: string;
    detail: string;
    actionLabel?: string;
    onAction?: () => void;
  } = $props();
</script>

<div class="empty-state">
  <div class="empty-icon" aria-hidden="true"><Icon size={22} strokeWidth={1.8} /></div>
  <div>
    <h3>{title}</h3>
    <p>{detail}</p>
  </div>
  {#if actionLabel && onAction}
    <button class="button secondary" type="button" onclick={onAction}>{actionLabel}</button>
  {/if}
</div>
