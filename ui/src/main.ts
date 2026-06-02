import { createApp } from 'vue'
import { createRouter, createWebHistory } from 'vue-router'
import App from './App.vue'
import Dashboard from './views/Dashboard.vue'
import TaskDetail from './views/TaskDetail.vue'
import TaskLogs from './views/TaskLogs.vue'
import './style.css'

const routes = [
  { path: '/', name: 'dashboard', component: Dashboard },
  { path: '/tasks/:name', name: 'task-detail', component: TaskDetail, props: true },
  { path: '/tasks/:name/logs', name: 'task-logs', component: TaskLogs, props: true },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

createApp(App).use(router).mount('#app')
