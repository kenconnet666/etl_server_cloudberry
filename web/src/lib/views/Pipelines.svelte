<script lang="ts">
  import { CirclePause, GitBranch, LoaderCircle, Play, Plus, RefreshCw, RotateCcw } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import ConfirmAction from '../components/ConfirmAction.svelte';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import PipelineDialog from '../components/PipelineDialog.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import { formatBytes, formatDateTime, label, truncateMiddle } from '../format';
  import type { Pipeline, Source, Target } from '../types';

  let {
    refreshVersion,
    onApiState,
    onDataChanged
  }: { refreshVersion: number; onApiState: (online: boolean) => void; onDataChanged: () => void } = $props();

  let pipelines = $state<Pipeline[]>([]);
  let sources = $state<Source[]>([]);
  let targets = $state<Target[]>([]);
  let selectedId = $state('');
  let detail = $state<Pipeline>();
  let loading = $state(true);
  let detailLoading = $state(false);
  let error = $state('');
  let actionError = $state('');
  let busyAction = $state('');
  let dialogOpen = $state(false);
  let confirmRebuild = $state(false);

  let selected = $derived(detail?.id === selectedId ? detail : pipelines.find((pipeline) => pipeline.id === selectedId));
  let selectedSource = $derived(sources.find((source) => source.id === selected?.source_id));
  let selectedTarget = $derived(targets.find((target) => target.id === selected?.target_id));

  function sourceName(id: string): string {
    return sources.find((source) => source.id === id)?.name || truncateMiddle(id, 16);
  }

  function targetName(id: string): string {
    return targets.find((target) => target.id === id)?.name || truncateMiddle(id, 16);
  }

  async function loadDetail(id: string): Promise<void> {
    detailLoading = true;
    actionError = '';
    try {
      detail = await api.pipeline(id);
      onApiState(true);
    } catch (requestError) {
      actionError = apiErrorMessage(requestError);
      detail = pipelines.find((pipeline) => pipeline.id === id);
      onApiState(false);
    } finally {
      detailLoading = false;
    }
  }

  async function selectPipeline(id: string): Promise<void> {
    selectedId = id;
    detail = pipelines.find((pipeline) => pipeline.id === id);
    await loadDetail(id);
  }

  async function load(): Promise<void> {
    loading = true;
    error = '';
    try {
      [pipelines, sources, targets] = await Promise.all([api.pipelines(), api.sources(), api.targets()]);
      onApiState(true);
      const nextId = pipelines.some((pipeline) => pipeline.id === selectedId) ? selectedId : pipelines[0]?.id || '';
      selectedId = nextId;
      if (nextId) {
        detail = pipelines.find((pipeline) => pipeline.id === nextId);
        await loadDetail(nextId);
      } else {
        detail = undefined;
      }
    } catch (requestError) {
      error = apiErrorMessage(requestError);
      onApiState(false);
    } finally {
      loading = false;
    }
  }

  async function runAction(action: 'start' | 'pause' | 'rebuild'): Promise<void> {
    if (!selected) return;
    busyAction = action;
    actionError = '';
    try {
      detail = await api.pipelineAction(selected.id, action);
      confirmRebuild = false;
      await load();
      onDataChanged();
    } catch (requestError) {
      actionError = apiErrorMessage(requestError);
    } finally {
      busyAction = '';
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
    <div><p class="eyebrow">Final-state replication</p><h2>Pipelines</h2></div>
    <div class="page-actions">
      <button class="icon-button" type="button" disabled={loading} onclick={load} title="Refresh pipelines" aria-label="Refresh pipelines"><RefreshCw class={loading ? 'spin' : undefined} size={18} /></button>
      <button class="button primary" type="button" onclick={() => (dialogOpen = true)}><Plus size={16} /> New pipeline</button>
    </div>
  </div>

  {#if error}<InlineNotice tone="error" title="Pipelines could not be loaded" detail={error} actionLabel="Retry" onAction={load} />{/if}

  <section class="page-section flush-top">
    {#if pipelines.length > 0}
      <div class="table-scroll pipeline-list">
        <table>
          <thead><tr><th>Pipeline</th><th>Route</th><th>Runtime</th><th>Desired</th><th>Revision</th><th>Updated</th></tr></thead>
          <tbody>
            {#each pipelines as pipeline}
              <tr class:selected-row={pipeline.id === selectedId}>
                <td data-label="Pipeline"><button class="table-link" type="button" onclick={() => selectPipeline(pipeline.id)}>{pipeline.name}</button><small class="mono">{truncateMiddle(pipeline.id, 20)}</small></td>
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
    {:else if !loading && !error}
      <EmptyState icon={GitBranch} title="No pipelines" detail="Connect a PostgreSQL source and Cloudberry target, then create a route." actionLabel="Create pipeline" onAction={() => (dialogOpen = true)} />
    {/if}
  </section>

  {#if selected}
    <section class="pipeline-detail" aria-labelledby="pipeline-detail-title">
      <div class="detail-heading">
        <div>
          <div class="detail-title-row"><h3 id="pipeline-detail-title">{selected.name}</h3><StatusBadge value={selected.runtime_state} /></div>
          <p>{sourceName(selected.source_id)} <span>→</span> {targetName(selected.target_id)}</p>
        </div>
        <div class="pipeline-actions">
          {#if selected.desired_running}
            <button class="button secondary" type="button" disabled={Boolean(busyAction)} onclick={() => runAction('pause')}><CirclePause size={16} /> {busyAction === 'pause' ? 'Pausing' : 'Pause'}</button>
          {:else}
            <button class="button primary" type="button" disabled={Boolean(busyAction)} onclick={() => runAction('start')}><Play size={16} /> {busyAction === 'start' ? 'Starting' : 'Start'}</button>
          {/if}
          <button class="button danger-outline" type="button" disabled={Boolean(busyAction)} onclick={() => (confirmRebuild = true)}><RotateCcw size={16} /> Rebuild</button>
          <button class="icon-button" type="button" disabled={detailLoading} onclick={() => loadDetail(selected.id)} title="Refresh pipeline detail" aria-label="Refresh pipeline detail">
            {#if detailLoading}<LoaderCircle class="spin" size={18} />{:else}<RefreshCw size={18} />{/if}
          </button>
        </div>
      </div>

      {#if actionError}<InlineNotice tone="error" title="Pipeline operation failed" detail={actionError} />{/if}

      <div class="detail-metrics">
        <div><span>Runtime state</span><strong>{label(selected.runtime_state)}</strong><small>Supervisor observation</small></div>
        <div><span>Desired state</span><strong>{selected.desired_running ? 'Running' : 'Paused'}</strong><small>Control-plane intent</small></div>
        <div><span>Config revision</span><strong>{selected.config_revision}</strong><small>Monotonic revision</small></div>
        <div><span>Snapshot generation</span><strong>{selected.snapshot_generation}</strong><small>Rebuild epoch</small></div>
        <div><span>Last updated</span><strong class="metric-date">{formatDateTime(selected.updated_at)}</strong><small>Control database</small></div>
      </div>

      <section class="detail-section route-details">
        <div class="section-heading compact"><div><h4>Replication route</h4><span>Namespace and database ownership for this pipeline</span></div></div>
        <dl class="definition-grid">
          <div><dt>Source</dt><dd>{selectedSource?.name || selected.source_id}</dd></div>
          <div><dt>Source topology</dt><dd>{selectedSource ? label(selectedSource.topology) : 'Unavailable'}</dd></div>
          <div><dt>Source database</dt><dd class="mono">{selectedSource?.database_name || 'Unavailable'}</dd></div>
          <div><dt>Source prefix</dt><dd class="mono">{selectedSource?.prefix || 'Unavailable'}</dd></div>
          <div><dt>Target</dt><dd>{selectedTarget?.name || selected.target_id}</dd></div>
          <div><dt>Target database</dt><dd class="mono">{selectedTarget?.database_name || 'Unavailable'}</dd></div>
        </dl>
      </section>

      {#if selected.runtime}
        <section class="detail-section runtime-details">
          <div class="section-heading compact"><div><h4>Runtime telemetry</h4><span>Durable progress observed by the supervisor</span></div></div>
          <dl class="definition-grid">
            <div><dt>Replication phase</dt><dd><StatusBadge value={selected.runtime.phase} compact /></dd></div>
            <div><dt>Source received LSN</dt><dd class="mono">{selected.runtime.source_received_lsn || '—'}</dd></div>
            <div><dt>Source current LSN</dt><dd class="mono">{selected.runtime.source_current_lsn || '—'}</dd></div>
            <div><dt>Target checkpoint LSN</dt><dd class="mono">{selected.runtime.target_checkpoint_lsn || '—'}</dd></div>
            <div><dt>Estimated WAL lag</dt><dd>{formatBytes(selected.runtime.estimated_byte_lag ?? undefined)}</dd></div>
            <div><dt>Restarts</dt><dd>{selected.runtime.restart_count}</dd></div>
            <div><dt>Last transaction</dt><dd>{formatDateTime(selected.runtime.last_transaction_at ?? undefined)}</dd></div>
            <div><dt>Last apply</dt><dd>{formatDateTime(selected.runtime.last_apply_at ?? undefined)}</dd></div>
            <div><dt>Last acknowledgement</dt><dd>{formatDateTime(selected.runtime.last_ack_at ?? undefined)}</dd></div>
          </dl>
          {#if selected.runtime.last_error}<InlineNotice tone="error" title="Last runtime error" detail={selected.runtime.last_error} />{/if}
        </section>
      {/if}
    </section>
  {/if}
</div>

<PipelineDialog open={dialogOpen} {sources} {targets} onClose={() => (dialogOpen = false)} onSaved={saved} />
<ConfirmAction
  open={confirmRebuild}
  title="Rebuild pipeline"
  detail={selected ? `Request a rebuild for ${selected.name}? This creates a new snapshot generation and the supervisor will reconcile the request.` : ''}
  confirmLabel="Request rebuild"
  busy={busyAction === 'rebuild'}
  onClose={() => (confirmRebuild = false)}
  onConfirm={() => runAction('rebuild')}
/>
