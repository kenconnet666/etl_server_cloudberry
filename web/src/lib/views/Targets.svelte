<script lang="ts">
  import { Plus, RefreshCw, Server } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import TargetDialog from '../components/TargetDialog.svelte';
  import { formatDateTime, truncateMiddle } from '../format';
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

  function endpoint(target: Target): string {
    const connection = target.settings.connection;
    return connection?.host ? `${connection.host}:${connection.port ?? 5432}` : 'Not recorded';
  }

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
    <div><p class="eyebrow">Apache Cloudberry</p><h2>Targets</h2></div>
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
          <thead><tr><th>Name</th><th>Coordinator</th><th>Database</th><th>TLS</th><th>State</th><th>Updated</th></tr></thead>
          <tbody>
            {#each targets as target}
              <tr>
                <td data-label="Name"><strong>{target.name}</strong><small class="mono">{truncateMiddle(target.id, 20)}</small></td>
                <td data-label="Coordinator" class="mono">{endpoint(target)}</td>
                <td data-label="Database">{target.database_name}</td>
                <td data-label="TLS">{target.settings.connection?.tls_mode || 'Not recorded'}</td>
                <td data-label="State"><StatusBadge value={target.enabled ? 'enabled' : 'disabled'} compact /></td>
                <td data-label="Updated">{formatDateTime(target.updated_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if !loading && !error}
      <EmptyState icon={Server} title="No Cloudberry targets" detail="Add an Apache Cloudberry coordinator connection." actionLabel="Add target" onAction={() => (dialogOpen = true)} />
    {/if}
  </section>
</div>

<TargetDialog open={dialogOpen} onClose={() => (dialogOpen = false)} onSaved={saved} />
