<script lang="ts">
  import { History, RefreshCw } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import { formatDateTime, label } from '../format';
  import type { Operation } from '../types';

  let { refreshVersion, onApiState }: { refreshVersion: number; onApiState: (online: boolean) => void } = $props();
  let operations = $state<Operation[]>([]);
  let loading = $state(true);
  let error = $state('');

  async function load(): Promise<void> {
    loading = true;
    error = '';
    try {
      operations = await api.operations();
      onApiState(true);
    } catch (requestError) {
      error = apiErrorMessage(requestError);
      onApiState(false);
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    refreshVersion;
    void load();
  });
</script>

<div class="page-content">
  <div class="page-heading">
    <div><p class="eyebrow">Lifecycle history</p><h2>Operations</h2></div>
    <button class="icon-button" type="button" disabled={loading} onclick={load} title="Refresh operations" aria-label="Refresh operations"><RefreshCw class:spin={loading} size={18} /></button>
  </div>

  {#if error}<InlineNotice tone="error" title="Operations could not be loaded" detail={error} actionLabel="Retry" onAction={load} />{/if}

  <section class="page-section flush-top">
    {#if operations.length > 0}
      <div class="table-scroll">
        <table>
          <thead><tr><th>Operation</th><th>Pipeline</th><th>Status</th><th>Progress</th><th>Started</th><th>Finished</th><th>Detail</th></tr></thead>
          <tbody>
            {#each operations as operation}
              <tr>
                <td data-label="Operation"><strong>{label(operation.kind)}</strong><small class="mono">{operation.id}</small></td>
                <td data-label="Pipeline">{operation.pipeline_name || operation.pipeline_id || 'System'}</td>
                <td data-label="Status"><StatusBadge value={operation.state} compact /></td>
                <td data-label="Progress">
                  {#if operation.progress !== undefined}
                    <div class="progress-cell"><progress max="100" value={operation.progress}></progress><span>{Math.round(operation.progress)}%</span></div>
                  {:else}—{/if}
                </td>
                <td data-label="Started">{formatDateTime(operation.started_at || operation.created_at)}</td>
                <td data-label="Finished">{formatDateTime(operation.finished_at)}</td>
                <td data-label="Detail" class="detail-cell">{operation.detail || '—'}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if !loading && !error}
      <EmptyState icon={History} title="No operations" detail="Pipeline lifecycle and reconciliation work will appear here." />
    {/if}
  </section>
</div>
