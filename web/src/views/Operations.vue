<script setup lang="ts">
import { ref, onMounted, h } from 'vue'
import { NCard, NDataTable, NSpin, NAlert, NTag } from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import { api, apiErrorMessage } from '../api'
import type { Operation } from '../types'

const loading = ref(true)
const error = ref('')
const operations = ref<Operation[]>([])

const columns: DataTableColumns<Operation> = [
  { title: 'Pipeline ID', key: 'pipeline_id', ellipsis: { tooltip: true } },
  { title: 'Type', key: 'operation_type' },
  {
    title: 'State',
    key: 'state',
    width: 120,
    render: (row) => {
      const stateColorMap: Record<string, 'default' | 'error' | 'warning' | 'success' | 'info'> = {
        succeeded: 'success',
        running: 'info',
        failed: 'error',
        cancelled: 'warning'
      }
      return h(NTag, { type: stateColorMap[row.state] || 'default' }, {
        default: () => row.state
      })
    }
  },
  { title: 'Requested', key: 'requested_at', width: 180 },
  { title: 'Error', key: 'error_message', ellipsis: { tooltip: true } }
]

async function loadOperations() {
  loading.value = true
  error.value = ''
  try {
    operations.value = await api.listOperations()
  } catch (err) {
    error.value = apiErrorMessage(err)
  } finally {
    loading.value = false
  }
}

onMounted(loadOperations)
</script>

<template>
  <div>
    <h1 style="margin-bottom: 24px">Operations</h1>

    <NAlert v-if="error" type="error" style="margin-bottom: 16px" closable @close="error = ''">
      {{ error }}
    </NAlert>

    <NCard>
      <NSpin :show="loading">
        <NDataTable :columns="columns" :data="operations" :bordered="false" />
      </NSpin>
    </NCard>
  </div>
</template>
