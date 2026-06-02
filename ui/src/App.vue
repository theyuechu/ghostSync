<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { fetchHealth } from './api'
import type { HealthStatus } from './api'

const health = ref<HealthStatus | null>(null)
const error = ref<string | null>(null)

async function loadHealth() {
  try {
    health.value = await fetchHealth()
    error.value = null
  } catch (e: any) {
    error.value = e.message
  }
}

onMounted(loadHealth)
</script>

<template>
  <header class="app-header">
    <h1>GhostSync</h1>
    <nav>
      <router-link to="/">Dashboard</router-link>
    </nav>
    <div class="status-badge">
      <span v-if="error" class="badge badge-error">Offline</span>
      <span v-else-if="health" class="badge badge-success">Online</span>
      <span v-else class="badge badge-pending">...</span>
    </div>
  </header>

  <main class="app-main">
    <router-view />
  </main>
</template>
