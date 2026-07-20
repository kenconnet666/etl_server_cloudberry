<script lang="ts">
  import { CheckCircle2, LoaderCircle, PlugZap, Save } from '@lucide/svelte';

  import { api, apiErrorMessage } from '../api';
  import { targetRequest } from '../connection';
  import type { ConnectionReport, TargetForm } from '../types';
  import InlineNotice from './InlineNotice.svelte';
  import Modal from './Modal.svelte';

  let {
    open,
    onClose,
    onSaved
  }: { open: boolean; onClose: () => void; onSaved: () => Promise<void> | void } = $props();

  const blank = (): TargetForm => ({
    name: '',
    host: '',
    port: 5432,
    database_name: '',
    username: '',
    password: '',
    tls_mode: 'verify-full'
  });

  let form = $state<TargetForm>(blank());
  let testing = $state(false);
  let saving = $state(false);
  let error = $state('');
  let testResult = $state<ConnectionReport>();

  function close(): void {
    if (testing || saving) return;
    form = blank();
    error = '';
    testResult = undefined;
    onClose();
  }

  function invalidateTest(): void {
    testResult = undefined;
  }

  async function testConnection(): Promise<void> {
    testing = true;
    error = '';
    testResult = undefined;
    try {
      testResult = await api.testTarget(targetRequest(form).dsn);
    } catch (requestError) {
      error = apiErrorMessage(requestError);
    } finally {
      testing = false;
    }
  }

  async function save(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    saving = true;
    error = '';
    let succeeded = false;
    try {
      await api.createTarget(targetRequest(form));
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

<Modal {open} title="Add Cloudberry target" description="Apache Cloudberry coordinator connection" onClose={close}>
  <form class="dialog-form" onsubmit={save} oninput={invalidateTest}>
    {#if error}<InlineNotice tone="error" title="Target connection failed" detail={error} />{/if}
    {#if testResult}
      <InlineNotice tone={testResult.warnings.length > 0 ? 'warning' : 'success'} title="Connection verified" detail={[testResult.server_version, ...testResult.warnings].join(' · ')} />
    {/if}

    <div class="form-grid two-column">
      <label class="field full-span">
        <span>Display name</span>
        <input required maxlength="80" bind:value={form.name} placeholder="Analytics warehouse" />
      </label>
      <label class="field host-field">
        <span>Coordinator host</span>
        <input required bind:value={form.host} placeholder="cloudberry.example.internal" />
      </label>
      <label class="field port-field">
        <span>Port</span>
        <input required type="number" min="1" max="65535" bind:value={form.port} />
      </label>
      <label class="field">
        <span>Database</span>
        <input required bind:value={form.database_name} placeholder="analytics" />
      </label>
      <label class="field">
        <span>TLS mode</span>
        <select bind:value={form.tls_mode}>
          <option value="verify-full">Verify full</option>
          <option value="verify-ca">Verify CA</option>
          <option value="require">Require</option>
          <option value="disable">Disable</option>
        </select>
      </label>
      <label class="field">
        <span>Username</span>
        <input required autocomplete="off" bind:value={form.username} />
      </label>
      <label class="field">
        <span>Password</span>
        <input required type="password" autocomplete="new-password" bind:value={form.password} />
      </label>
    </div>

    <footer class="dialog-actions">
      <button class="button secondary" type="button" disabled={testing || saving} onclick={testConnection}>
        {#if testing}<LoaderCircle class="spin" size={16} />{:else}<PlugZap size={16} />{/if}
        {testing ? 'Testing' : 'Test connection'}
      </button>
      <div class="action-spacer"></div>
      <button class="button ghost" type="button" disabled={testing || saving} onclick={close}>Cancel</button>
      <button class="button primary" type="submit" disabled={testing || saving}>
        {#if saving}<LoaderCircle class="spin" size={16} />{:else if testResult}<CheckCircle2 size={16} />{:else}<Save size={16} />{/if}
        {saving ? 'Saving' : 'Save target'}
      </button>
    </footer>
  </form>
</Modal>
