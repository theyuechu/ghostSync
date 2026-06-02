<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { fetchTaskLogs } from '../api'
import type { RunRecord } from '../api'

const props = defineProps<{ name: string }>()

const runs = ref<RunRecord[]>([])
const loading = ref(true)
const error = ref<string | null>(null)

async function load() {
  loading.value = true
  try {
    runs.value = await fetchTaskLogs(props.name, 50)
    error.value = null
  } catch (e: any) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>

<template>
  <div class="mb-2">
    <router-link :to="`/tasks/${encodeURIComponent(name)}`" class="text-sm">← Back to Task</router-link>
    <span class="text-dim text-sm" style="margin-left: 0.5rem">/ {{ name }} / Logs</span>
  </div>

  <div class="flex-between mb-2">
    <h2>Run History</h2>
    <button class="btn btn-sm" @click="load">Refresh</button>
  </div>

  <div v-if="loading" class="card">
    <div class="skeleton" v-for="i in 5" :key="i"></div>
  </div>

  <div v-else-if="error" class="card">
    <p class="text-dim">Failed to load logs: {{ error }}</p>
  </div>

  <div v-else-if="runs.length === 0" class="card">
    <p class="text-dim">No run history for this task.</p>
  </div>

  <div v-else class="card" style="padding: 0; overflow: hidden">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Status</th>
          <th>Processed</th>
          <th>Total</th>
          <th>RPS</th>
          <th>Started</th>
          <th>Duration</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="run in runs" :key="run.id">
          <td class="text-dim">{{ run.id }}</td>
          <td>
            <span class="badge" :class="run.status === 'success' ? 'badge-success' : run.status === 'running' ? 'badge-pending' : 'badge-error'">
              {{ run.status }}
            </span>
          </td>
          <td>{{ run.processed_rows.toLocaleString() }}</td>
          <td>{{ run.total_rows.toLocaleString() }}</td>
          <td>{{ run.rps > 0 ? Math.round(run.rps).toLocaleString() : '-' }}</td>
          <td class="text-sm">{{ new Date(run.started_at).toLocaleString() }}</td>
          <td class="text-sm">
            <template v-if="run.finished_at">
              {{ ((new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000).toFixed(1) }}s
            </template>
            <span v-else class="text-dim">-</span>
          </td>
          <td class="text-sm text-dim">{{ run.error_message || '-' }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
