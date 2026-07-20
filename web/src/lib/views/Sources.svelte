<script lang="ts">
  import { Database, Plus, RefreshCw } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import SourceDialog from '../components/SourceDialog.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import { formatDateTime, label } from '../format';
  import type { Source } from '../types';

  let {
    refreshVersion,
    onApiState,
    onDataChanged
  }: { refreshVersion: number; onApiState: (online: boolean) => void; onDataChanged: () => void } = $props();

  let sources = $state<Source[]>([]);
  let loading = $state(true);
  let error = $state('');
  let dialogOpen = $state(false);

  async function load(): Promise<void> {
    loading = true;
    error = '';
    try {
      sources = await api.sources();
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
    <div><p class="eyebrow">PostgreSQL 18</p><h2>Sources</h2></div>
    <div class="page-actions">
      <button class="icon-button" type="button" disabled={loading} onclick={load} title="Refresh sources" aria-label="Refresh sources"><RefreshCw class:spin={loading} size={18} /></button>
      <button class="button primary" type="button" onclick={() => (dialogOpen = true)}><Plus size={16} /> Add source</button>
    </div>
  </div>

  {#if error}<InlineNotice tone="error" title="Sources could not be loaded" detail={error} actionLabel="Retry" onAction={load} />{/if}

  <section class="page-section flush-top">
    {#if sources.length > 0}
      <div class="table-scroll">
        <table>
          <thead><tr><th>Name</th><th>Topology</th><th>Endpoint</th><th>Database</th><th>Version</th><th>Nodes</th><th>Health</th><th>Updated</th></tr></thead>
          <tbody>
            {#each sources as source}
              <tr>
                <td data-label="Name"><strong>{source.name}</strong><small class="mono">{source.id}</small></td>
                <td data-label="Topology">{label(source.topology)}</td>
                <td data-label="Endpoint" class="mono">{source.host}:{source.port}</td>
                <td data-label="Database">{source.database}</td>
                <td data-label="Version">{source.citus_version ? `Citus ${source.citus_version}` : source.postgres_version ? `PostgreSQL ${source.postgres_version}` : '—'}</td>
                <td data-label="Nodes">{source.node_count ?? (source.topology === 'citus' ? '—' : 1)}</td>
                <td data-label="Health"><StatusBadge value={source.health} compact />{#if source.detail}<small>{source.detail}</small>{/if}</td>
                <td data-label="Updated">{formatDateTime(source.updated_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if !loading && !error}
      <EmptyState icon={Database} title="No PostgreSQL sources" detail="Add a PostgreSQL 18 or Citus coordinator connection." actionLabel="Add source" onAction={() => (dialogOpen = true)} />
    {/if}
  </section>
</div>

<SourceDialog open={dialogOpen} onClose={() => (dialogOpen = false)} onSaved={saved} />
