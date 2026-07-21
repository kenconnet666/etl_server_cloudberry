<script setup lang="ts">
import { onMounted, computed } from 'vue'
import { NConfigProvider, NMessageProvider, NDialogProvider } from 'naive-ui'
import { useAuthStore } from './stores/auth'
import Login from './views/Login.vue'
import AppShell from './components/AppShell.vue'

const authStore = useAuthStore()

const isAuthenticated = computed(() => authStore.authenticated)

onMounted(async () => {
  try {
    await authStore.checkSession()
  } catch {
    // Session check failed, show login
  }
})
</script>

<template>
  <NConfigProvider :theme="null">
    <NMessageProvider>
      <NDialogProvider>
        <Login v-if="!isAuthenticated" />
        <AppShell v-else />
      </NDialogProvider>
    </NMessageProvider>
  </NConfigProvider>
</template>
