<script setup lang="ts">
import { ref } from 'vue'
import { NCard, NForm, NFormItem, NInput, NButton, NAlert } from 'naive-ui'
import { useAuthStore } from '../stores/auth'
import { apiErrorMessage } from '../api'

const authStore = useAuthStore()

const username = ref('')
const password = ref('')
const loading = ref(false)
const error = ref('')

async function handleLogin() {
  if (!username.value || !password.value) {
    error.value = 'Username and password are required'
    return
  }

  loading.value = true
  error.value = ''

  try {
    await authStore.login(username.value, password.value)
  } catch (err) {
    error.value = apiErrorMessage(err)
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="login-container">
    <NCard class="login-card" title="ETL Server Cloudberry">
      <template #header-extra>
        <span style="font-size: 14px; color: #888">Sign In</span>
      </template>

      <NAlert v-if="!authStore.apiOnline" type="error" style="margin-bottom: 16px">
        Management API is unavailable
      </NAlert>

      <NAlert v-if="error" type="error" style="margin-bottom: 16px" closable @close="error = ''">
        {{ error }}
      </NAlert>

      <NForm @submit.prevent="handleLogin">
        <NFormItem label="Username">
          <NInput
            v-model:value="username"
            placeholder="Enter username"
            :disabled="loading"
            @keyup.enter="handleLogin"
          />
        </NFormItem>

        <NFormItem label="Password">
          <NInput
            v-model:value="password"
            type="password"
            placeholder="Enter password"
            :disabled="loading"
            show-password-on="click"
            @keyup.enter="handleLogin"
          />
        </NFormItem>

        <NButton
          type="primary"
          block
          :loading="loading"
          :disabled="!username || !password"
          @click="handleLogin"
        >
          Sign In
        </NButton>
      </NForm>
    </NCard>
  </div>
</template>

<style scoped>
.login-container {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.login-card {
  width: 100%;
  max-width: 400px;
  margin: 20px;
}
</style>
