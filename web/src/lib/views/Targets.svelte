<script lang="ts">
  import { Plus, RefreshCw, Server } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import TargetDialog from '../components/TargetDialog.svelte';
  import { formatDateTime } from '../format';
  import type { Target } from '../types';

  let {
    refreshVersion,
    onApiState,
    onDataChanged
  }: { refreshVersion: number; onApiState: (online: boolean) => void; onDataChanged: () => void } = $props();

  let targets = $state<Target[]>([]);
  let loading = $state(true);
  let error = $state('');
  let dialogOpen = $state(false);

  async function load(): Promise<void> {
    loading = true;
    error = '';
    try {
      targets = await api.targets();
      onApiState(true);
    } catch (requestError) {
      error = apiErrorMessage(requestError);
      onApiState(false);
    } finally {
      loading = false;
    }
  }

  async function saved(): Promise<void> {
    await load();
    onDataChanged();
  }

  $effect(() => {
    refreshVersion;
    void load();
  });
</script>

<div class="page-content">
  <div class="page-heading">
    <div><p class="eyebrow">Apache Cloudberry 2.1</p><h2>Targets</h2></div>
    <div class="page-actions">
      <button class="icon-button" type="button" disabled={loading} onclick={load} title="Refresh targets" aria-label="Refresh targets"><RefreshCw class:spin={loading} size={18} /></button>
      <button class="button primary" type="button" onclick={() => (dialogOpen = true)}><Plus size={16} /> Add target</button>
    </div>
  </div>

  {#if error}<InlineNotice tone="error" title="Targets could not be loaded" detail={error} actionLabel="Retry" onAction={load} />{/if}

  <section class="page-section flush-top">
    {#if targets.length > 0}
      <div class="table-scroll">
        <table>
          <thead><tr><th>Name</th><th>Coordinator</th><th>Database</th><th>Version</th><th>Segments</th><th>Health</th><th>Updated</th></tr></thead>
          <tbody>
            {#each targets as target}
              <tr>
                <td data-label="Name"><strong>{target.name}</strong><small class="mono">{target.id}</small></td>
                <td data-label="Coordinator" class="mono">{target.host}:{target.port}</td>
                <td data-label="Database">{target.database}</td>
                <td data-label="Version">{target.cloudberry_version ? `Cloudberry ${target.cloudberry_version}` : '—'}</td>
                <td data-label="Segments">{target.segment_count ?? '—'}</td>
                <td data-label="Health"><StatusBadge value={target.health} compact />{#if target.detail}<small>{target.detail}</small>{/if}</td>
                <td data-label="Updated">{formatDateTime(target.updated_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if !loading && !error}
      <EmptyState icon={Server} title="No Cloudberry targets" detail="Add the coordinator connection for an Apache Cloudberry 2.1 cluster." actionLabel="Add target" onAction={() => (dialogOpen = true)} />
    {/if}
  </section>
</div>

<TargetDialog open={dialogOpen} onClose={() => (dialogOpen = false)} onSaved={saved} />
