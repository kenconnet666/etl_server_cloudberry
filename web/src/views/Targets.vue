<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { NCard, NButton, NDataTable, NSpin, NAlert } from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import { api, apiErrorMessage } from '../api'
import type { Target } from '../types'

const loading = ref(true)
const error = ref('')
const targets = ref<Target[]>([])

const columns: DataTableColumns<Target> = [
  { title: 'Name', key: 'name' },
  { title: 'Database', key: 'database_name' },
  { title: 'Enabled', key: 'enabled', render: (row) => (row.enabled ? 'Yes' : 'No') }
]

async function loadTargets() {
  loading.value = true
  error.value = ''
  try {
    targets.value = await api.listTargets()
  } catch (err) {
    error.value = apiErrorMessage(err)
  } finally {
    loading.value = false
  }
}

onMounted(loadTargets)
</script>

<template>
  <div>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px">
      <h1 style="margin: 0">Targets</h1>
      <NButton type="primary">Create Target</NButton>
    </div>

    <NAlert v-if="error" type="error" style="margin-bottom: 16px" closable @close="error = ''">
      {{ error }}
    </NAlert>

    <NCard>
      <NSpin :show="loading">
        <NDataTable :columns="columns" :data="targets" :bordered="false" />
      </NSpin>
    </NCard>
  </div>
</template>
