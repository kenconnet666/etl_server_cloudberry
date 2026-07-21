<script setup lang="ts">
import { ref, onMounted, h } from 'vue'
import { NCard, NButton, NDataTable, NSpin, NAlert, NTag, NSpace } from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import { api, apiErrorMessage } from '../api'
import type { Pipeline } from '../types'

const loading = ref(true)
const error = ref('')
const pipelines = ref<Pipeline[]>([])

const columns: DataTableColumns<Pipeline> = [
  { title: 'Name', key: 'name' },
  { title: 'Source', key: 'source_id', width: 100 },
  { title: 'Target', key: 'target_id', width: 100 },
  {
    title: 'State',
    key: 'runtime_state',
    width: 120,
    render: (row) => {
      const stateColorMap: Record<string, 'default' | 'error' | 'warning' | 'success'> = {
        running: 'success',
        stopped: 'default',
        failed: 'error',
        degraded: 'warning'
      }
      return h(NTag, { type: stateColorMap[row.runtime_state] || 'default' }, {
        default: () => row.runtime_state
      })
    }
  },
  {
    title: 'Phase',
    key: 'phase',
    width: 140,
    render: (row) => row.runtime?.phase || '-'
  },
  {
    title: 'Desired',
    key: 'desired_running',
    width: 100,
    render: (row) => (row.desired_running ? 'Running' : 'Stopped')
  },
  {
    title: 'Actions',
    key: 'actions',
    width: 200,
    render: () => {
      return h(NSpace, {}, {
        default: () => [
          h(NButton, { size: 'small', secondary: true }, { default: () => 'Edit' }),
          h(NButton, { size: 'small', type: 'error', secondary: true }, { default: () => 'Delete' })
        ]
      })
    }
  }
]

async function loadPipelines() {
  loading.value = true
  error.value = ''
  try {
    pipelines.value = await api.listPipelines()
  } catch (err) {
    error.value = apiErrorMessage(err)
  } finally {
    loading.value = false
  }
}

onMounted(loadPipelines)
</script>

<template>
  <div>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px">
      <h1 style="margin: 0">Pipelines</h1>
      <NButton type="primary">Create Pipeline</NButton>
    </div>

    <NAlert v-if="error" type="error" style="margin-bottom: 16px" closable @close="error = ''">
      {{ error }}
    </NAlert>

    <NCard>
      <NSpin :show="loading">
        <NDataTable :columns="columns" :data="pipelines" :bordered="false" />
      </NSpin>
    </NCard>
  </div>
</template>
