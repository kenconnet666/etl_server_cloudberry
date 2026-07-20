<script lang="ts">
  import { onMount } from 'svelte';

  import { ApiError, api, apiErrorMessage, setCsrfToken } from './lib/api';
  import AppShell from './lib/components/AppShell.svelte';
  import type { ViewName } from './lib/navigation';
  import type { Session } from './lib/types';
  import Login from './lib/views/Login.svelte';
  import Operations from './lib/views/Operations.svelte';
  import Overview from './lib/views/Overview.svelte';
  import Pipelines from './lib/views/Pipelines.svelte';
  import Sources from './lib/views/Sources.svelte';
  import Targets from './lib/views/Targets.svelte';

  let authenticated = $state(false);
  let checking = $state(true);
  let submitting = $state(false);
  let apiOnline = $state(true);
  let username = $state('');
  let loginError = $state('');
  let current = $state<ViewName>('overview');
  let refreshVersion = $state(0);

  function acceptSession(session: Session): void {
    setCsrfToken(session.csrf_token);
    username = session.username;
    authenticated = true;
    apiOnline = true;
    loginError = '';
  }

  function clearSession(): void {
    setCsrfToken();
    authenticated = false;
    username = '';
    current = 'overview';
  }

  async function checkSession(): Promise<void> {
    checking = true;
    loginError = '';
    try {
      acceptSession(await api.session());
    } catch (error) {
      clearSession();
      if (error instanceof ApiError && error.status === 401) {
        apiOnline = true;
      } else {
        apiOnline = false;
        loginError =
          error instanceof ApiError && error.status >= 500
            ? `Management API unavailable (${error.message}).`
            : apiErrorMessage(error);
      }
    } finally {
      checking = false;
    }
  }

  async function login(loginUsername: string, password: string): Promise<void> {
    submitting = true;
    loginError = '';
    try {
      acceptSession(await api.login(loginUsername, password));
      refreshVersion += 1;
    } catch (error) {
      apiOnline = !(error instanceof ApiError && error.status === 0);
      loginError =
        error instanceof ApiError && error.status >= 500
          ? `Management API unavailable (${error.message}).`
          : apiErrorMessage(error);
    } finally {
      submitting = false;
    }
  }

  async function logout(): Promise<void> {
    let failure = '';
    try {
      await api.logout();
    } catch (error) {
      failure = apiErrorMessage(error);
    }
    clearSession();
    loginError = failure;
  }

  function updateApiState(online: boolean): void {
    apiOnline = online;
  }

  function dataChanged(): void {
    refreshVersion += 1;
  }

  onMount(() => {
    const unauthorized = (): void => {
      if (!authenticated) return;
      clearSession();
      apiOnline = true;
      loginError = 'Your session expired. Sign in again.';
    };
    window.addEventListener('etl:unauthorized', unauthorized);
    void checkSession();
    const refreshTimer = window.setInterval(() => {
      if (authenticated) refreshVersion += 1;
    }, 15_000);

    return () => {
      window.removeEventListener('etl:unauthorized', unauthorized);
      window.clearInterval(refreshTimer);
    };
  });
</script>

{#if !authenticated}
  <Login {checking} {submitting} error={loginError} onLogin={login} onRetry={checkSession} />
{:else}
  <AppShell {current} {username} {apiOnline} onNavigate={(view) => (current = view)} onLogout={logout}>
    {#if current === 'overview'}
      <Overview {refreshVersion} onNavigate={(view) => (current = view)} onApiState={updateApiState} />
    {:else if current === 'sources'}
      <Sources {refreshVersion} onApiState={updateApiState} onDataChanged={dataChanged} />
    {:else if current === 'targets'}
      <Targets {refreshVersion} onApiState={updateApiState} onDataChanged={dataChanged} />
    {:else if current === 'pipelines'}
      <Pipelines {refreshVersion} onApiState={updateApiState} onDataChanged={dataChanged} />
    {:else}
      <Operations {refreshVersion} onApiState={updateApiState} />
    {/if}
  </AppShell>
{/if}
