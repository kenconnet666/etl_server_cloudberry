import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '../stores/auth'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'overview',
      component: () => import('../views/Overview.vue')
    },
    {
      path: '/pipelines',
      name: 'pipelines',
      component: () => import('../views/Pipelines.vue')
    },
    {
      path: '/sources',
      name: 'sources',
      component: () => import('../views/Sources.vue')
    },
    {
      path: '/targets',
      name: 'targets',
      component: () => import('../views/Targets.vue')
    },
    {
      path: '/operations',
      name: 'operations',
      component: () => import('../views/Operations.vue')
    }
  ]
})

router.beforeEach((_to, _from, next) => {
  const authStore = useAuthStore()

  // Allow navigation if authenticated or still checking
  if (authStore.authenticated || authStore.checking) {
    next()
  } else {
    // Redirect to root which will show login
    next()
  }
})

export default router
