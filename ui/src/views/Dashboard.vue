<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { fetchTasks, triggerTask } from '../api'
import type { TaskSummary } from '../api'

const router = useRouter()
const tasks = ref<TaskSummary[]>([])
const loading = ref(true)
const error = ref<string | null>(null)
const runningTasks = ref<Set<string>>(new Set())

async function load() {
  loading.value = true
  try {
    tasks.value = await fetchTasks()
    error.value = null
  } catch (e: any) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

async function runTask(name: string) {
  runningTasks.value.add(name)
  try {
    await triggerTask(name)
  } catch (e: any) {
    alert(`Failed to trigger task: ${e.message}`)
  } finally {
    setTimeout(() => runningTasks.value.delete(name), 3000)
  }
}

function viewLogs(name: string) {
  router.push(`/tasks/${encodeURIComponent(name)}/logs`)
}

onMounted(load)
</script>

<template>
  <div class="flex-between mb-2">
    <h2>Tasks</h2>
    <button class="btn btn-sm" @click="load">Refresh</button>
  </div>

  <div v-if="loading">
    <div class="card" v-for="i in 3" :key="i">
      <div class="skeleton" style="width: 40%"></div>
      <div class="skeleton" style="width: 60%"></div>
    </div>
  </div>

  <div v-else-if="error" class="card">
    <p class="text-dim">Failed to load tasks: {{ error }}</p>
    <button class="btn btn-sm mt-1" @click="load">Retry</button>
  </div>

  <div v-else-if="tasks.length === 0" class="card">
    <p class="text-dim">No tasks found. Make sure the daemon is running with a valid config.</p>
  </div>

  <div v-else class="grid-2">
    <div v-for="task in tasks" :key="task.name" class="card">
      <div class="card-header">
        <h3>{{ task.name }}</h3>
        <span v-if="task.schedule" class="badge badge-info">Scheduled</span>
        <span v-else class="badge badge-pending">Manual</span>
      </div>

      <table>
        <tr>
          <td class="text-dim text-sm">Source</td>
          <td>{{ task.source }}</td>
        </tr>
        <tr>
          <td class="text-dim text-sm">Target</td>
          <td>{{ task.target }}</td>
        </tr>
        <tr v-if="task.schedule">
          <td class="text-dim text-sm">Schedule</td>
          <td><code>{{ task.schedule }}</code></td>
        </tr>
        <tr>
          <td class="text-dim text-sm">Tables</td>
          <td>{{ task.tables.length }} table(s)</td>
        </tr>
        <tr v-if="task.last_run">
          <td class="text-dim text-sm">Last Run</td>
          <td>
            <span class="badge" :class="task.status === 'success' ? 'badge-success' : 'badge-error'">
              {{ task.status }}
            </span>
            <span class="text-dim text-sm" style="margin-left: 0.5rem">{{ new Date(task.last_run).toLocaleString() }}</span>
          </td>
        </tr>
      </table>

      <div class="flex gap-1 mt-2">
        <button
          class="btn btn-primary btn-sm"
          :disabled="runningTasks.has(task.name)"
          @click="runTask(task.name)"
        >
          {{ runningTasks.has(task.name) ? 'Running...' : '▶ Run Now' }}
        </button>
        <button class="btn btn-sm" @click="viewLogs(task.name)">Logs</button>
      </div>
    </div>
  </div>
</template>
