const API_BASE = import.meta.env.VITE_API_URL || '/api'

export interface TaskSummary {
  name: string
  source: string
  target: string
  schedule: string | null
  tables: { name: string; mode: string }[]
  last_run: string | null
  status: string
}

export interface RunRecord {
  id: number
  task_name: string
  status: string
  started_at: string
  finished_at: string | null
  processed_rows: number
  total_rows: number
  error_message: string | null
  rps: number
}

export interface HealthStatus {
  status: string
  tasks: number
  scheduled: number
}

export async function fetchHealth(): Promise<HealthStatus> {
  const res = await fetch(`${API_BASE}/health`)
  if (!res.ok) throw new Error(`Health check failed: ${res.status}`)
  return res.json()
}

export async function fetchTasks(): Promise<TaskSummary[]> {
  const res = await fetch(`${API_BASE}/tasks`)
  if (!res.ok) throw new Error(`Failed to fetch tasks: ${res.status}`)
  return res.json()
}

export async function triggerTask(name: string): Promise<{ message: string; run_id: number | null }> {
  const res = await fetch(`${API_BASE}/tasks/${encodeURIComponent(name)}/run`, {
    method: 'POST',
  })
  if (!res.ok) throw new Error(`Failed to trigger task: ${res.status}`)
  return res.json()
}

export async function fetchTaskLogs(name: string, limit = 20): Promise<RunRecord[]> {
  const res = await fetch(`${API_BASE}/tasks/${encodeURIComponent(name)}/logs?limit=${limit}`)
  if (!res.ok) throw new Error(`Failed to fetch logs: ${res.status}`)
  return res.json()
}
