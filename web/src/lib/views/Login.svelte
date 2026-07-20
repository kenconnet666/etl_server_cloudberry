<script lang="ts">
  import { Activity, Eye, EyeOff, LoaderCircle, LockKeyhole, RefreshCw, UserRound } from '@lucide/svelte';

  let {
    checking,
    submitting,
    error,
    onLogin,
    onRetry
  }: {
    checking: boolean;
    submitting: boolean;
    error?: string;
    onLogin: (username: string, password: string) => Promise<void>;
    onRetry: () => void;
  } = $props();

  let username = $state('');
  let password = $state('');
  let showPassword = $state(false);

  async function submit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    await onLogin(username, password);
  }
</script>

<main class="login-page">
  <section class="login-panel" aria-labelledby="login-title">
    <header class="login-brand">
      <div class="login-mark" aria-hidden="true"><Activity size={24} strokeWidth={2.2} /></div>
      <div>
        <h1 id="login-title">ETL Server Cloudberry</h1>
        <p>Operations console</p>
      </div>
    </header>

    {#if checking}
      <div class="session-check" role="status">
        <LoaderCircle class="spin" size={19} />
        <span>Checking session</span>
      </div>
    {:else}
      {#if error}
        <div class="login-error" role="alert">
          <span>{error}</span>
          <button class="icon-button" type="button" onclick={onRetry} title="Retry API connection" aria-label="Retry API connection">
            <RefreshCw size={17} />
          </button>
        </div>
      {/if}

      <form onsubmit={submit}>
        <label for="username">Username</label>
        <div class="input-with-icon">
          <UserRound size={17} aria-hidden="true" />
          <input id="username" name="username" autocomplete="username" required bind:value={username} />
        </div>

        <label for="password">Password</label>
        <div class="input-with-icon trailing-control">
          <LockKeyhole size={17} aria-hidden="true" />
          <input id="password" name="password" type={showPassword ? 'text' : 'password'} autocomplete="current-password" required bind:value={password} />
          <button class="icon-button" type="button" onclick={() => (showPassword = !showPassword)} title={showPassword ? 'Hide password' : 'Show password'} aria-label={showPassword ? 'Hide password' : 'Show password'}>
            {#if showPassword}<EyeOff size={17} />{:else}<Eye size={17} />{/if}
          </button>
        </div>

        <button class="button primary login-submit" type="submit" disabled={submitting}>
          {#if submitting}<LoaderCircle class="spin" size={17} />{/if}
          {submitting ? 'Signing in' : 'Sign in'}
        </button>
      </form>
    {/if}
  </section>
</main>
