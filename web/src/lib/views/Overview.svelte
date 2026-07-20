<script lang="ts">
  import { Activity, ArrowRight, Database, GitBranch, History, RefreshCw, Server } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import { formatBytes, formatDateTime, formatDuration } from '../format';
  import type { ViewName } from '../navigation';
  import type { Operation, Pipeline, Source, Target } from '../types';

  let {
    refreshVersion,
    onNavigate,
    onApiState
  }: {
    refreshVersion: number;
    onNavigate: (view: ViewName) => void;
    onApiState: (online: boolean) => void;
  } = $props();

  let sources = $state<Source[]>([]);
  let targets = $state<Target[]>([]);
  let pipelines = $state<Pipeline[]>([]);
  let operations = $state<Operation[]>([]);
  let loading = $state(true);
  let error = $state('');

  let healthySources = $derived(sources.filter((source) => source.health === 'healthy').length);
  let healthyTargets = $derived(targets.filter((target) => target.health === 'healthy').length);
  let activePipelines = $derived(pipelines.filter((pipeline) => pipeline.phase === 'running').length);
  let maximumLag = $derived(
    pipelines.reduce((maximum, pipeline) => Math.max(maximum, pipeline.lag_bytes || 0), 0)
  );

  async function load(): Promise<void> {
    loading = true;
    error = '';
    const results = await Promise.allSettled([api.sources(), api.targets(), api.pipelines(), api.operations()]);
    const failures = results.filter((result) => result.status === 'rejected');

    if (results[0].status === 'fulfilled') sources = results[0].value;
    if (results[1].status === 'fulfilled') targets = results[1].value;
    if (results[2].status === 'fulfilled') pipelines = results[2].value;
    if (results[3].status === 'fulfilled') operations = results[3].value;

    onApiState(failures.length < results.length);
    if (failures.length > 0) {
      error = apiErrorMessage(failures[0].reason);
    }
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
      <RefreshCw class:spin={loading} size={18} />
    </button>
  </div>

  {#if error}
    <InlineNotice tone="error" title="Some management data could not be loaded" detail={error} actionLabel="Retry" onAction={load} />
  {/if}

  <section class="metric-strip" aria-label="Replication summary">
    <button type="button" onclick={() => onNavigate('sources')}>
      <span class="metric-icon green"><Database size={18} /></span>
      <span><strong>{sources.length}</strong><small>Sources</small></span>
      <em>{healthySources} healthy</em>
    </button>
    <button type="button" onclick={() => onNavigate('targets')}>
      <span class="metric-icon rust"><Server size={18} /></span>
      <span><strong>{targets.length}</strong><small>Targets</small></span>
      <em>{healthyTargets} healthy</em>
    </button>
    <button type="button" onclick={() => onNavigate('pipelines')}>
      <span class="metric-icon blue"><GitBranch size={18} /></span>
      <span><strong>{pipelines.length}</strong><small>Pipelines</small></span>
      <em>{activePipelines} running</em>
    </button>
    <button type="button" onclick={() => onNavigate('pipelines')}>
      <span class="metric-icon amber"><Activity size={18} /></span>
      <span><strong>{formatBytes(maximumLag)}</strong><small>Maximum lag</small></span>
      <em>Across pipelines</em>
    </button>
  </section>

  <section class="page-section">
    <div class="section-heading">
      <div><h3>Pipelines</h3><span>Current apply state and end-to-end lag</span></div>
      <button class="text-action" type="button" onclick={() => onNavigate('pipelines')}>View all <ArrowRight size={15} /></button>
    </div>
    {#if pipelines.length > 0}
      <div class="table-scroll">
        <table>
          <thead><tr><th>Pipeline</th><th>Route</th><th>Status</th><th>Lag</th><th>Updated</th></tr></thead>
          <tbody>
            {#each pipelines.slice(0, 6) as pipeline}
              <tr>
                <td data-label="Pipeline"><button class="table-link" type="button" onclick={() => onNavigate('pipelines')}>{pipeline.name}</button><small class="mono">{pipeline.source_prefix}</small></td>
                <td data-label="Route">{pipeline.source_name || pipeline.source_id}<span class="route-arrow">→</span>{pipeline.target_name || pipeline.target_id}</td>
                <td data-label="Status"><StatusBadge value={pipeline.phase} compact /></td>
                <td data-label="Lag"><strong>{formatBytes(pipeline.lag_bytes)}</strong><small>{formatDuration(pipeline.lag_seconds)}</small></td>
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
      <div class="section-heading"><div><h3>Source health</h3><span>PostgreSQL and Citus connections</span></div></div>
      {#if sources.length > 0}
        <ul class="health-list">
          {#each sources.slice(0, 5) as source}
            <li><span class="source-symbol"><Database size={16} /></span><div><strong>{source.name}</strong><small>{source.host}:{source.port}/{source.database}</small></div><StatusBadge value={source.health} compact /></li>
          {/each}
        </ul>
      {:else}<div class="section-empty">No source connections</div>{/if}
    </section>

    <section class="page-section">
      <div class="section-heading">
        <div><h3>Recent operations</h3><span>Lifecycle and reconciliation work</span></div>
        <button class="icon-button" type="button" onclick={() => onNavigate('operations')} title="Open operations" aria-label="Open operations"><History size={17} /></button>
      </div>
      {#if operations.length > 0}
        <ul class="operation-list">
          {#each operations.slice(0, 5) as operation}
            <li><div><strong>{operation.kind}</strong><small>{operation.pipeline_name || operation.pipeline_id || 'System'} · {formatDateTime(operation.started_at || operation.created_at)}</small></div><StatusBadge value={operation.state} compact /></li>
          {/each}
        </ul>
      {:else}<div class="section-empty">No recorded operations</div>{/if}
    </section>
  </div>
</div>
