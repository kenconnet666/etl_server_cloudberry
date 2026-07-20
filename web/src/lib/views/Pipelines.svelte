<script lang="ts">
  import {
    CirclePause,
    GitBranch,
    LoaderCircle,
    Play,
    Plus,
    RefreshCw,
    RotateCcw,
    Rows3,
    ServerCog
  } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import ConfirmAction from '../components/ConfirmAction.svelte';
  import EmptyState from '../components/EmptyState.svelte';
  import InlineNotice from '../components/InlineNotice.svelte';
  import PipelineDialog from '../components/PipelineDialog.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import { formatBytes, formatDateTime, formatDuration, formatNumber, truncateMiddle } from '../format';
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

  let selected = $derived(
    detail?.id === selectedId ? detail : pipelines.find((pipeline) => pipeline.id === selectedId)
  );
  let canPause = $derived(
    selected ? ['validating', 'snapshotting', 'catching_up', 'running', 'degraded'].includes(selected.phase) : false
  );
  let tableCounts = $derived({
    total: selected?.tables?.length || 0,
    running: selected?.tables?.filter((table) => table.phase === 'running').length || 0,
    attention:
      selected?.tables?.filter((table) => ['rebuild_required', 'blocked', 'quarantined'].includes(table.phase)).length || 0
  });

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
      const nextId = pipelines.some((pipeline) => pipeline.id === selectedId)
        ? selectedId
        : pipelines[0]?.id || '';
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
      await api.pipelineAction(selected.id, action);
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
      <button class="icon-button" type="button" disabled={loading} onclick={load} title="Refresh pipelines" aria-label="Refresh pipelines"><RefreshCw class:spin={loading} size={18} /></button>
      <button class="button primary" type="button" onclick={() => (dialogOpen = true)}><Plus size={16} /> New pipeline</button>
    </div>
  </div>

  {#if error}<InlineNotice tone="error" title="Pipelines could not be loaded" detail={error} actionLabel="Retry" onAction={load} />{/if}

  <section class="page-section flush-top">
    {#if pipelines.length > 0}
      <div class="table-scroll pipeline-list">
        <table>
          <thead><tr><th>Pipeline</th><th>Route</th><th>Prefix</th><th>Status</th><th>Lag</th><th>Revision</th><th>Updated</th></tr></thead>
          <tbody>
            {#each pipelines as pipeline}
              <tr class:selected-row={pipeline.id === selectedId}>
                <td data-label="Pipeline"><button class="table-link" type="button" onclick={() => selectPipeline(pipeline.id)}>{pipeline.name}</button><small class="mono">{truncateMiddle(pipeline.id, 20)}</small></td>
                <td data-label="Route">{pipeline.source_name || truncateMiddle(pipeline.source_id, 12)}<span class="route-arrow">→</span>{pipeline.target_name || truncateMiddle(pipeline.target_id, 12)}</td>
                <td data-label="Prefix" class="mono">{pipeline.source_prefix}</td>
                <td data-label="Status"><StatusBadge value={pipeline.phase} compact /></td>
                <td data-label="Lag"><strong>{formatBytes(pipeline.lag_bytes)}</strong><small>{formatDuration(pipeline.lag_seconds)}</small></td>
                <td data-label="Revision">{pipeline.config_revision ?? '—'}</td>
                <td data-label="Updated">{formatDateTime(pipeline.updated_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if !loading && !error}
      <EmptyState icon={GitBranch} title="No pipelines" detail="Connect a PostgreSQL source and Cloudberry target, then define a source prefix." actionLabel="Create pipeline" onAction={() => (dialogOpen = true)} />
    {/if}
  </section>

  {#if selected}
    <section class="pipeline-detail" aria-labelledby="pipeline-detail-title">
      <div class="detail-heading">
        <div>
          <div class="detail-title-row"><h3 id="pipeline-detail-title">{selected.name}</h3><StatusBadge value={selected.phase} /></div>
          <p>{selected.source_name || selected.source_id} <span>→</span> {selected.target_name || selected.target_id}</p>
          {#if selected.detail}<small>{selected.detail}</small>{/if}
        </div>
        <div class="pipeline-actions">
          {#if canPause}
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
        <div><span>Apply lag</span><strong>{formatBytes(selected.lag_bytes)}</strong><small>{formatDuration(selected.lag_seconds)}</small></div>
        <div><span>Node checkpoints</span><strong>{selected.checkpoints?.length || 0}</strong><small>Independent LSN streams</small></div>
        <div><span>Tables running</span><strong>{tableCounts.running}/{tableCounts.total}</strong><small>{tableCounts.attention} need attention</small></div>
        <div><span>Namespace prefix</span><strong class="mono">{selected.source_prefix}</strong><small>{selected.target_database || 'Target database not reported'}</small></div>
      </div>

      <section class="detail-section">
        <div class="section-heading compact"><div><h4><ServerCog size={17} /> Node checkpoints</h4><span>Received, applied, and flushed positions per physical source node</span></div></div>
        {#if selected.checkpoints && selected.checkpoints.length > 0}
          <div class="table-scroll">
            <table>
              <thead><tr><th>Node</th><th>Slot</th><th>Timeline</th><th>Received LSN</th><th>Applied LSN</th><th>Flushed LSN</th><th>Lag</th></tr></thead>
              <tbody>
                {#each selected.checkpoints as checkpoint}
                  <tr>
                    <td data-label="Node"><strong>{checkpoint.node_name || `Node ${checkpoint.node_id}`}</strong><small class="mono">{checkpoint.system_identifier}</small></td>
                    <td data-label="Slot" class="mono">{checkpoint.slot_name}</td>
                    <td data-label="Timeline">{checkpoint.timeline}</td>
                    <td data-label="Received LSN" class="mono">{checkpoint.received_lsn}</td>
                    <td data-label="Applied LSN" class="mono">{checkpoint.applied_lsn}</td>
                    <td data-label="Flushed LSN" class="mono">{checkpoint.flushed_lsn}</td>
                    <td data-label="Lag"><strong>{formatBytes(checkpoint.lag_bytes)}</strong><small>{formatDuration(checkpoint.lag_seconds)}</small></td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else}<div class="section-empty">No node checkpoints reported</div>{/if}
      </section>

      <section class="detail-section">
        <div class="section-heading compact"><div><h4><Rows3 size={17} /> Table state</h4><span>Snapshot, catch-up, rebuild, and schema generations</span></div></div>
        {#if selected.tables && selected.tables.length > 0}
          <div class="table-scroll">
            <table>
              <thead><tr><th>Source table</th><th>Target table</th><th>Status</th><th>Rows copied</th><th>Lag</th><th>Generation</th><th>Schema</th><th>Detail</th></tr></thead>
              <tbody>
                {#each selected.tables as table}
                  <tr>
                    <td data-label="Source table" class="mono">{table.source_name}</td>
                    <td data-label="Target table" class="mono">{table.target_name}</td>
                    <td data-label="Status"><StatusBadge value={table.phase} compact /></td>
                    <td data-label="Rows copied">{formatNumber(table.rows_copied)}</td>
                    <td data-label="Lag">{formatBytes(table.lag_bytes)}</td>
                    <td data-label="Generation">{table.generation ?? '—'}</td>
                    <td data-label="Schema">{table.schema_version ?? '—'}</td>
                    <td data-label="Detail" class="detail-cell">{table.detail || '—'}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else}<div class="section-empty">No table state reported</div>{/if}
      </section>
    </section>
  {/if}
</div>

<PipelineDialog open={dialogOpen} {sources} {targets} onClose={() => (dialogOpen = false)} onSaved={saved} />
<ConfirmAction
  open={confirmRebuild}
  title="Rebuild pipeline"
  detail={selected ? `Create a new generation for ${selected.name} and resnapshot its tables? Existing target data remains active until the new generation is ready.` : ''}
  confirmLabel="Start rebuild"
  busy={busyAction === 'rebuild'}
  onClose={() => (confirmRebuild = false)}
  onConfirm={() => runAction('rebuild')}
/>
