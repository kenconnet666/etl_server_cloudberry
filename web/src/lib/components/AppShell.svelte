<script lang="ts">
  import {
    Activity,
    Database,
    GitBranch,
    History,
    LogOut,
    Menu,
    PanelLeftClose,
    Server,
    Wifi,
    WifiOff,
    X
  } from '@lucide/svelte';
  import type { Snippet } from 'svelte';

  import type { ViewName } from '../navigation';
  import { VIEW_TITLES } from '../navigation';

  let {
    current,
    username,
    apiOnline,
    onNavigate,
    onLogout,
    children
  }: {
    current: ViewName;
    username: string;
    apiOnline: boolean;
    onNavigate: (view: ViewName) => void;
    onLogout: () => void;
    children: Snippet;
  } = $props();

  let mobileOpen = $state(false);
  let collapsed = $state(false);

  function navigate(view: ViewName): void {
    onNavigate(view);
    mobileOpen = false;
  }
</script>

<div class:collapsed class="app-shell">
  {#if mobileOpen}
    <button class="mobile-scrim" type="button" aria-label="Close navigation" onclick={() => (mobileOpen = false)}></button>
  {/if}

  <aside class:mobile-open={mobileOpen} class="sidebar">
    <div class="brand-row">
      <div class="brand-mark" aria-hidden="true"><Activity size={20} strokeWidth={2.2} /></div>
      <div class="brand-copy">
        <strong>ETL Server</strong>
        <span>Cloudberry</span>
      </div>
      <button class="icon-button mobile-only" type="button" onclick={() => (mobileOpen = false)} title="Close navigation" aria-label="Close navigation">
        <X size={19} />
      </button>
    </div>

    <nav aria-label="Primary navigation">
      <button class:active={current === 'overview'} type="button" onclick={() => navigate('overview')} title="Overview">
        <Activity size={18} /><span>Overview</span>
      </button>
      <button class:active={current === 'sources'} type="button" onclick={() => navigate('sources')} title="Sources">
        <Database size={18} /><span>Sources</span>
      </button>
      <button class:active={current === 'targets'} type="button" onclick={() => navigate('targets')} title="Targets">
        <Server size={18} /><span>Targets</span>
      </button>
      <button class:active={current === 'pipelines'} type="button" onclick={() => navigate('pipelines')} title="Pipelines">
        <GitBranch size={18} /><span>Pipelines</span>
      </button>
      <button class:active={current === 'operations'} type="button" onclick={() => navigate('operations')} title="Operations">
        <History size={18} /><span>Operations</span>
      </button>
    </nav>

    <div class="sidebar-footer">
      <div class="api-state" class:offline={!apiOnline} title={apiOnline ? 'Management API connected' : 'Management API unavailable'}>
        {#if apiOnline}<Wifi size={16} />{:else}<WifiOff size={16} />{/if}
        <span>{apiOnline ? 'API connected' : 'API unavailable'}</span>
      </div>
      <div class="account-row">
        <div class="account-avatar" aria-hidden="true">{username.slice(0, 1).toUpperCase()}</div>
        <div class="account-copy"><strong>{username}</strong><span>Administrator</span></div>
        <button class="icon-button" type="button" onclick={onLogout} title="Sign out" aria-label="Sign out">
          <LogOut size={17} />
        </button>
      </div>
    </div>
  </aside>

  <div class="workspace">
    <header class="topbar">
      <div class="topbar-leading">
        <button class="icon-button mobile-menu" type="button" onclick={() => (mobileOpen = true)} title="Open navigation" aria-label="Open navigation">
          <Menu size={20} />
        </button>
        <button class="icon-button collapse-button" type="button" onclick={() => (collapsed = !collapsed)} title={collapsed ? 'Expand navigation' : 'Collapse navigation'} aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}>
          <PanelLeftClose class:flipped={collapsed} size={19} />
        </button>
        <h1>{VIEW_TITLES[current]}</h1>
      </div>
      <div class:offline={!apiOnline} class="topbar-connection">
        <span class="connection-dot"></span>
        {apiOnline ? 'Live' : 'Disconnected'}
      </div>
    </header>
    <main>{@render children()}</main>
  </div>
</div>
