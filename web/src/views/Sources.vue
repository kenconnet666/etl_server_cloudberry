<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { NCard, NButton, NDataTable, NSpin, NAlert } from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import { api, apiErrorMessage } from '../api'
import type { Source } from '../types'

const loading = ref(true)
const error = ref('')
const sources = ref<Source[]>([])

const columns: DataTableColumns<Source> = [
  { title: 'Name', key: 'name' },
  { title: 'Database', key: 'database_name' },
  { title: 'Topology', key: 'topology' },
  { title: 'Enabled', key: 'enabled', render: (row) => (row.enabled ? 'Yes' : 'No') }
]

async function loadSources() {
  loading.value = true
  error.value = ''
  try {
    sources.value = await api.listSources()
  } catch (err) {
    error.value = apiErrorMessage(err)
  } finally {
    loading.value = false
  }
}

onMounted(loadSources)
</script>

<template>
  <div>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px">
      <h1 style="margin: 0">Sources</h1>
      <NButton type="primary">Create Source</NButton>
    </div>

    <NAlert v-if="error" type="error" style="margin-bottom: 16px" closable @close="error = ''">
      {{ error }}
    </NAlert>

    <NCard>
      <NSpin :show="loading">
        <NDataTable :columns="columns" :data="sources" :bordered="false" />
      </NSpin>
    </NCard>
  </div>
</template>
