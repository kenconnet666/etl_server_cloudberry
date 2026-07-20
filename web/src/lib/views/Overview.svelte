<script lang="ts">
  import { Activity, ArrowRight, Database, GitBranch, History, RefreshCw, Server } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import { formatDateTime } from '../format';
  import type { ViewName } from '../navigation';
  import type { Operation, Overview, Pipeline, Source, Target } from '../types';

  let {
    refreshVersion,
    onNavigate,
    onApiState
  }: {
    refreshVersion: number;
    onNavigate: (view: ViewName) => void;
    onApiState: (online: boolean) => void;
  } = $props();

  const emptyOverview = (): Overview => ({ sources: 0, targets: 0, pipelines: 0, running_pipelines: 0 });

  let overview = $state<Overview>(emptyOverview());
  let sources = $state<Source[]>([]);
  let targets = $state<Target[]>([]);
  let pipelines = $state<Pipeline[]>([]);
  let operations = $state<Operation[]>([]);
  let loading = $state(true);
  let error = $state('');

  function sourceName(id: string): string {
    return sources.find((source) => source.id === id)?.name || id;
  }

  function targetName(id: string): string {
    return targets.find((target) => target.id === id)?.name || id;
  }

  async function load(): Promise<void> {
    loading = true;
    error = '';
    const results = await Promise.allSettled([
      api.overview(),
      api.sources(),
      api.targets(),
      api.pipelines(),
      api.operations()
    ]);
    const failures = results.filter((result) => result.status === 'rejected');

    if (results[0].status === 'fulfilled') overview = results[0].value;
    if (results[1].status === 'fulfilled') sources = results[1].value;
    if (results[2].status === 'fulfilled') targets = results[2].value;
    if (results[3].status === 'fulfilled') pipelines = results[3].value;
    if (results[4].status === 'fulfilled') operations = results[4].value;

    onApiState(failures.length < results.length);
    if (failures.length > 0) error = apiErrorMessage(failures[0].reason);
    loading = false;
  }

  $effect(() => {
    refreshVersion;
    void load();
  });
</script>

<div class="page-content">
  <div class="page-heading">
    <div><p class="eyebrow">System state</p><h2>Replication overview</h2></div>
    <button class="icon-button" type="button" disabled={loading} onclick={load} title="Refresh overview" aria-label="Refresh overview">
      <RefreshCw class={loading ? 'spin' : undefined} size={18} />
    </button>
  </div>

  {#if error}
    <InlineNotice tone="error" title="Some management data could not be loaded" detail={error} actionLabel="Retry" onAction={load} />
  {/if}

  <section class="metric-strip" aria-label="Replication summary">
    <button type="button" onclick={() => onNavigate('sources')}>
      <span class="metric-icon green"><Database size={18} /></span>
      <span><strong>{overview.sources}</strong><small>Sources</small></span>
      <em>Configured</em>
    </button>
    <button type="button" onclick={() => onNavigate('targets')}>
      <span class="metric-icon rust"><Server size={18} /></span>
      <span><strong>{overview.targets}</strong><small>Targets</small></span>
      <em>Configured</em>
    </button>
    <button type="button" onclick={() => onNavigate('pipelines')}>
      <span class="metric-icon blue"><GitBranch size={18} /></span>
      <span><strong>{overview.pipelines}</strong><small>Pipelines</small></span>
      <em>{overview.running_pipelines} running</em>
    </button>
    <button type="button" onclick={() => onNavigate('operations')}>
      <span class="metric-icon amber"><Activity size={18} /></span>
      <span><strong>{operations.length}</strong><small>Operations</small></span>
      <em>Active now</em>
    </button>
  </section>

  <section class="page-section">
    <div class="section-heading">
      <div><h3>Pipelines</h3><span>Desired state and current runtime state</span></div>
      <button class="text-action" type="button" onclick={() => onNavigate('pipelines')}>View all <ArrowRight size={15} /></button>
    </div>
    {#if pipelines.length > 0}
      <div class="table-scroll">
        <table>
          <thead><tr><th>Pipeline</th><th>Route</th><th>Runtime</th><th>Desired</th><th>Revision</th><th>Updated</th></tr></thead>
          <tbody>
            {#each pipelines.slice(0, 6) as pipeline}
              <tr>
                <td data-label="Pipeline"><button class="table-link" type="button" onclick={() => onNavigate('pipelines')}>{pipeline.name}</button></td>
                <td data-label="Route">{sourceName(pipeline.source_id)}<span class="route-arrow">→</span>{targetName(pipeline.target_id)}</td>
                <td data-label="Runtime"><StatusBadge value={pipeline.runtime_state} compact /></td>
                <td data-label="Desired">{pipeline.desired_running ? 'Running' : 'Paused'}</td>
                <td data-label="Revision">{pipeline.config_revision}</td>
                <td data-label="Updated">{formatDateTime(pipeline.updated_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if !loading}
      <EmptyState icon={GitBranch} title="No pipelines" detail="Create a source and target before configuring replication." actionLabel="Open pipelines" onAction={() => onNavigate('pipelines')} />
    {/if}
  </section>

  <div class="split-sections">
    <section class="page-section">
      <div class="section-heading"><div><h3>Sources</h3><span>PostgreSQL 18 and Citus registrations</span></div></div>
      {#if sources.length > 0}
        <ul class="health-list">
          {#each sources.slice(0, 5) as source}
            <li><span class="source-symbol"><Database size={16} /></span><div><strong>{source.name}</strong><small>{source.database_name} · {source.prefix}</small></div><StatusBadge value={source.enabled ? 'enabled' : 'disabled'} compact /></li>
          {/each}
        </ul>
      {:else}<div class="section-empty">No source connections</div>{/if}
    </section>

    <section class="page-section">
      <div class="section-heading">
        <div><h3>Active operations</h3><span>Replication processes reported by the supervisor</span></div>
        <button class="icon-button" type="button" onclick={() => onNavigate('operations')} title="Open operations" aria-label="Open operations"><History size={17} /></button>
      </div>
      {#if operations.length > 0}
        <ul class="operation-list">
          {#each operations.slice(0, 5) as operation}
            <li><div><strong>{operation.operation_type}</strong><small>{operation.id}</small></div><StatusBadge value={operation.state} compact /></li>
          {/each}
        </ul>
      {:else}<div class="section-empty">No active operations</div>{/if}
    </section>
  </div>
</div>
