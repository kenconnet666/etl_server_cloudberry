<script lang="ts">
  import { X } from '@lucide/svelte';
  import type { Snippet } from 'svelte';

  let {
    open,
    title,
    description,
    size = 'medium',
    onClose,
    children
  }: {
    open: boolean;
    title: string;
    description?: string;
    size?: 'small' | 'medium' | 'large';
    onClose: () => void;
    children: Snippet;
  } = $props();

  function handleKeydown(event: KeyboardEvent): void {
    if (open && event.key === 'Escape') onClose();
  }
</script>

<svelte:window onkeydown={handleKeydown} />

{#if open}
  <div class="modal-layer">
    <button class="modal-backdrop" aria-label="Close dialog" type="button" onclick={onClose}></button>
    <div class="modal-panel {size}" role="dialog" aria-modal="true" aria-labelledby="modal-title">
      <header class="modal-header">
        <div>
          <h2 id="modal-title">{title}</h2>
          {#if description}<p>{description}</p>{/if}
        </div>
        <button class="icon-button" type="button" onclick={onClose} title="Close dialog" aria-label="Close dialog">
          <X size={18} />
        </button>
      </header>
      <div class="modal-content">{@render children()}</div>
    </div>
  </div>
{/if}
