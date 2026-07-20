<script lang="ts">
  import { LoaderCircle, RefreshCw } from '@lucide/svelte';

  import Modal from './Modal.svelte';

  let {
    open,
    title,
    detail,
    confirmLabel,
    busy,
    onClose,
    onConfirm
  }: {
    open: boolean;
    title: string;
    detail: string;
    confirmLabel: string;
    busy: boolean;
    onClose: () => void;
    onConfirm: () => void;
  } = $props();
</script>

<Modal {open} {title} size="small" onClose={onClose}>
  <div class="confirm-body">
    <p>{detail}</p>
    <footer class="dialog-actions">
      <div class="action-spacer"></div>
      <button class="button ghost" type="button" disabled={busy} onclick={onClose}>Cancel</button>
      <button class="button danger" type="button" disabled={busy} onclick={onConfirm}>
        {#if busy}<LoaderCircle class="spin" size={16} />{:else}<RefreshCw size={16} />{/if}
        {busy ? 'Starting rebuild' : confirmLabel}
      </button>
    </footer>
  </div>
</Modal>
