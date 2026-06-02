<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { fetchTaskLogs, triggerTask, fetchTasks } from '../api'
import type { RunRecord, TaskSummary } from '../api'

const props = defineProps<{ name: string }>()
const router = useRouter()

const task = ref<TaskSummary | null>(null)
const runs = ref<RunRecord[]>([])
const loading = ref(true)
const error = ref<string | null>(null)
const running = ref(false)

async function load() {
  loading.value = true
  try {
    const tasks = await fetchTasks()
    task.value = tasks.find((t) => t.name === props.name) || null
    if (task.value) {
      runs.value = await fetchTaskLogs(props.name)
    }
    error.value = null
  } catch (e: any) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

async function runNow() {
  running.value = true
  try {
    await triggerTask(props.name)
    setTimeout(load, 1000)
  } catch (e: any) {
    alert(`Failed: ${e.message}`)
  } finally {
    running.value = false
  }
}

onMounted(load)
</script>

<template>
  <div class="mb-2">
    <router-link to="/" class="text-sm">← Back</router-link>
  </div>

  <div v-if="loading" class="card">
    <div class="skeleton" style="width: 50%"></div>
    <div class="skeleton" style="height: 4rem"></div>
  </div>

  <div v-else-if="!task" class="card">
    <p class="text-dim">Task "{{ name }}" not found.</p>
  </div>

  <template v-else>
    <div class="card">
      <div class="card-header">
        <h2>{{ task.name }}</h2>
        <span v-if="task.schedule" class="badge badge-info">{{ task.schedule }}</span>
      </div>

      <table>
        <tr><td class="text-dim text-sm">Source</td><td>{{ task.source }}</td></tr>
        <tr><td class="text-dim text-sm">Target</td><td>{{ task.target }}</td></tr>
        <tr><td class="text-dim text-sm">Tables</td><td>{{ task.tables.length }}</td></tr>
      </table>

      <div class="flex gap-1 mt-2">
        <button class="btn btn-primary btn-sm" :disabled="running" @click="runNow">
          {{ running ? 'Running...' : '▶ Run Now' }}
        </button>
        <button class="btn btn-sm" @click="router.push(`/tasks/${encodeURIComponent(name)}/logs`)">
          View All Logs
        </button>
      </div>
    </div>

    <!-- Recent runs -->
    <h3 class="mb-1">Recent Runs</h3>
    <div class="card" style="padding: 0; overflow: hidden">
      <table v-if="runs.length > 0">
        <thead>
          <tr>
            <th>Status</th>
            <th>Rows</th>
            <th>RPS</th>
            <th>Started</th>
            <th>Error</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="run in runs" :key="run.id">
            <td>
              <span class="badge" :class="run.status === 'success' ? 'badge-success' : 'badge-error'">
                {{ run.status }}
              </span>
            </td>
            <td>{{ run.processed_rows.toLocaleString() }}</td>
            <td>{{ run.rps > 0 ? Math.round(run.rps).toLocaleString() : '-' }}</td>
            <td class="text-sm text-dim">{{ new Date(run.started_at).toLocaleString() }}</td>
            <td class="text-sm text-dim">{{ run.error_message || '-' }}</td>
          </tr>
        </tbody>
      </table>
      <div v-else style="padding: 1rem">
        <p class="text-dim text-sm">No runs recorded yet.</p>
      </div>
    </div>
  </template>
</template>
