import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { api, setCsrfToken, ApiError } from '../api'
import type { Session } from '../types'

export const useAuthStore = defineStore('auth', () => {
  const username = ref('')
  const authenticated = ref(false)
  const checking = ref(false)
  const apiOnline = ref(true)

  const isAuthenticated = computed(() => authenticated.value)

  async function checkSession() {
    checking.value = true
    apiOnline.value = true
    try {
      const session = await api.session()
      acceptSession(session)
    } catch (error) {
      clearSession()
      if (error instanceof ApiError) {
        apiOnline.value = error.status !== 401
      } else {
        apiOnline.value = false
      }
      throw error
    } finally {
      checking.value = false
    }
  }

  async function login(user: string, password: string) {
    const session = await api.login(user, password)
    acceptSession(session)
  }

  async function logout() {
    await api.logout()
    clearSession()
  }

  function acceptSession(session: Session) {
    setCsrfToken(session.csrf_token)
    username.value = session.username
    authenticated.value = true
    apiOnline.value = true
  }

  function clearSession() {
    setCsrfToken()
    username.value = ''
    authenticated.value = false
  }

  return {
    username,
    authenticated,
    checking,
    apiOnline,
    isAuthenticated,
    checkSession,
    login,
    logout
  }
})
