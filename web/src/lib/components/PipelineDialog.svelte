<script lang="ts">
  import { ArrowRight, Database, LoaderCircle, Save, Server } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import type { CreatePipelineRequest, Source, Target } from '../types';
  import InlineNotice from './InlineNotice.svelte';
  import Modal from './Modal.svelte';

  let {
    open,
    sources,
    targets,
    onClose,
    onSaved
  }: {
    open: boolean;
    sources: Source[];
    targets: Target[];
    onClose: () => void;
    onSaved: () => Promise<void> | void;
  } = $props();

  const blank = (): CreatePipelineRequest => ({
    name: '',
    source_id: '',
    target_id: '',
    settings: {}
  });

  let form = $state<CreatePipelineRequest>(blank());
  let saving = $state(false);
  let error = $state('');

  let selectedSource = $derived(sources.find((source) => source.id === form.source_id));
  let selectedTarget = $derived(targets.find((target) => target.id === form.target_id));
  let canSubmit = $derived(Boolean(form.name.trim() && selectedSource && selectedTarget));

  $effect(() => {
    if (open) {
      if (!form.source_id && sources[0]) form.source_id = sources[0].id;
      if (!form.target_id && targets[0]) form.target_id = targets[0].id;
    }
  });

  function close(): void {
    if (saving) return;
    form = blank();
    error = '';
    onClose();
  }

  async function save(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!canSubmit) return;
    saving = true;
    error = '';
    let succeeded = false;
    try {
      await api.createPipeline({ ...form, name: form.name.trim() });
      await onSaved();
      succeeded = true;
    } catch (requestError) {
      error = apiErrorMessage(requestError);
    } finally {
      saving = false;
    }
    if (succeeded) close();
  }
</script>

<Modal {open} title="Create pipeline" description="PostgreSQL 18 to Apache Cloudberry" onClose={close}>
  <form class="dialog-form" onsubmit={save}>
    {#if error}<InlineNotice tone="error" title="Pipeline request failed" detail={error} />{/if}
    {#if sources.length === 0 || targets.length === 0}
      <InlineNotice tone="warning" title="Connection required" detail="Add at least one source and one target before creating a pipeline." />
    {/if}

    <div class="form-grid two-column">
      <label class="field full-span">
        <span>Pipeline name</span>
        <input required maxlength="128" bind:value={form.name} placeholder="Orders replication" />
      </label>
      <label class="field">
        <span>Source</span>
        <select required bind:value={form.source_id}>
          <option value="" disabled>Select source</option>
          {#each sources as source}<option value={source.id}>{source.name}</option>{/each}
        </select>
      </label>
      <label class="field">
        <span>Target</span>
        <select required bind:value={form.target_id}>
          <option value="" disabled>Select target</option>
          {#each targets as target}<option value={target.id}>{target.name}</option>{/each}
        </select>
      </label>
    </div>

    <section class="route-preview" aria-label="Pipeline route">
      <div class="route-endpoint">
        <Database size={18} />
        <div><span>Source</span><strong>{selectedSource?.name || 'Not selected'}</strong><small>{selectedSource ? `${selectedSource.database_name} · ${selectedSource.prefix}` : '—'}</small></div>
      </div>
      <ArrowRight size={18} aria-hidden="true" />
      <div class="route-endpoint">
        <Server size={18} />
        <div><span>Target</span><strong>{selectedTarget?.name || 'Not selected'}</strong><small>{selectedTarget?.database_name || '—'}</small></div>
      </div>
    </section>
    {#if selectedSource && selectedTarget}
      <p class="mapping-rule">Schemas are created as <code>{selectedSource.prefix}__{selectedSource.database_name}__&lt;source_schema&gt;</code> in <code>{selectedTarget.database_name}</code>.</p>
    {/if}

    <footer class="dialog-actions">
      <div class="action-spacer"></div>
      <button class="button ghost" type="button" disabled={saving} onclick={close}>Cancel</button>
      <button class="button primary" type="submit" disabled={!canSubmit || saving}>
        {#if saving}<LoaderCircle class="spin" size={16} />{:else}<Save size={16} />{/if}
        {saving ? 'Creating' : 'Create pipeline'}
      </button>
    </footer>
  </form>
</Modal>
