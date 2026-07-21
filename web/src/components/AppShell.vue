<script setup lang="ts">
import { computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { NLayout, NLayoutHeader, NLayoutContent, NMenu, NButton } from 'naive-ui'
import type { MenuOption } from 'naive-ui'
import { useAuthStore } from '../stores/auth'

const router = useRouter()
const route = useRoute()
const authStore = useAuthStore()

const activeKey = computed(() => route.name as string)

const menuOptions: MenuOption[] = [
  { label: 'Overview', key: 'overview' },
  { label: 'Pipelines', key: 'pipelines' },
  { label: 'Sources', key: 'sources' },
  { label: 'Targets', key: 'targets' },
  { label: 'Operations', key: 'operations' }
]

function handleMenuSelect(key: string) {
  router.push({ name: key })
}

async function handleLogout() {
  await authStore.logout()
}
</script>

<template>
  <NLayout style="min-height: 100vh">
    <NLayoutHeader bordered style="padding: 0 24px; display: flex; align-items: center; height: 64px">
      <div style="flex: 1; display: flex; align-items: center; gap: 32px">
        <h2 style="margin: 0; font-size: 18px; font-weight: 600">ETL Server Cloudberry</h2>
        <NMenu
          mode="horizontal"
          :value="activeKey"
          :options="menuOptions"
          @update:value="handleMenuSelect"
        />
      </div>
      <div style="display: flex; align-items: center; gap: 16px">
        <span style="font-size: 14px; color: #666">{{ authStore.username }}</span>
        <NButton text @click="handleLogout">Sign Out</NButton>
      </div>
    </NLayoutHeader>

    <NLayoutContent style="padding: 24px">
      <router-view />
    </NLayoutContent>
  </NLayout>
</template>
