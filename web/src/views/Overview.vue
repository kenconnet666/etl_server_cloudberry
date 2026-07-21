<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { NCard, NStatistic, NSpin, NAlert, NGrid, NGridItem } from 'naive-ui'
import { api, apiErrorMessage } from '../api'
import type { Pipeline, Source, Target } from '../types'

const loading = ref(true)
const error = ref('')
const pipelines = ref<Pipeline[]>([])
const sources = ref<Source[]>([])
const targets = ref<Target[]>([])

const runningPipelines = computed(() =>
  pipelines.value.filter(p => p.runtime_state === 'running').length
)

async function loadData() {
  loading.value = true
  error.value = ''
  try {
    const [pipelinesData, sourcesData, targetsData] = await Promise.all([
      api.listPipelines(),
      api.listSources(),
      api.listTargets()
    ])
    pipelines.value = pipelinesData
    sources.value = sourcesData
    targets.value = targetsData
  } catch (err) {
    error.value = apiErrorMessage(err)
  } finally {
    loading.value = false
  }
}

onMounted(loadData)
</script>

<template>
  <div>
    <h1 style="margin-bottom: 24px">Overview</h1>

    <NAlert v-if="error" type="error" style="margin-bottom: 16px" closable @close="error = ''">
      {{ error }}
    </NAlert>

    <NSpin :show="loading">
      <NGrid cols="1 s:2 m:4" responsive="screen" :x-gap="16" :y-gap="16">
        <NGridItem>
          <NCard>
            <NStatistic label="Total Pipelines" :value="pipelines.length" />
          </NCard>
        </NGridItem>
        <NGridItem>
          <NCard>
            <NStatistic label="Running" :value="runningPipelines" />
          </NCard>
        </NGridItem>
        <NGridItem>
          <NCard>
            <NStatistic label="Sources" :value="sources.length" />
          </NCard>
        </NGridItem>
        <NGridItem>
          <NCard>
            <NStatistic label="Targets" :value="targets.length" />
          </NCard>
        </NGridItem>
      </NGrid>
    </NSpin>
  </div>
</template>
