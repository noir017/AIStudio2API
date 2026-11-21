<script setup lang="ts">
import { ref, watch } from 'vue'
import { formatCDPHeaders, parseCDPHeaders } from '@/cdpHeaders'
import { useI18n } from '@/i18n'
import type { AccountLoginInput } from '@/types'
import UiIcon from './UiIcon.vue'

// environment 是账户的代理、语言与时区，语言与时区留空时按出口位置填写
const environment = defineModel<AccountLoginInput>({ required: true })
const { t } = useI18n()

const cdpHeadersText = ref(formatCDPHeaders(environment.value.cdp_headers))
const cdpHeadersError = ref('')
const revealCDPHeaders = ref(false)

watch(
  () => environment.value.cdp_headers,
  (next) => {
    const formatted = formatCDPHeaders(next)
    const currentParsed = parseCDPHeaders(cdpHeadersText.value)
    if (!currentParsed.error && formatCDPHeaders(currentParsed.headers) === formatted) return
    cdpHeadersText.value = formatted
    cdpHeadersError.value = ''
    revealCDPHeaders.value = false
  },
)

function updateCDPHeaders(value: string): void {
  cdpHeadersText.value = value
  const parsed = parseCDPHeaders(value)
  cdpHeadersError.value = parsed.error ?? ''
  if (!parsed.error) {
    environment.value.cdp_headers = parsed.headers
  }
}

function clearCDPHeaders(): void {
  cdpHeadersText.value = ''
  cdpHeadersError.value = ''
  environment.value.cdp_headers = undefined
  revealCDPHeaders.value = false
}
</script>

<template>
  <label class="block">
    <span class="mb-1 block text-sm font-medium text-gray-400">{{ t('accounts.proxy') }}</span>
    <input
      v-model.trim="environment.proxy"
      class="w-full rounded border border-line bg-canvas px-3 py-2 text-white transition focus:border-blue-500 focus:outline-none"
      placeholder="socks5://127.0.0.1:1080"
    />
  </label>
  <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
    <label class="block">
      <span class="mb-1 block text-sm font-medium text-gray-400">{{ t('accounts.locale') }}</span>
      <input
        v-model.trim="environment.locale"
        class="w-full rounded border border-line bg-canvas px-3 py-2 text-white transition focus:border-blue-500 focus:outline-none"
        :placeholder="t('accounts.byExit')"
      />
    </label>
    <label class="block">
      <span class="mb-1 block text-sm font-medium text-gray-400">{{ t('accounts.timezone') }}</span>
      <input
        v-model.trim="environment.timezone"
        class="w-full rounded border border-line bg-canvas px-3 py-2 text-white transition focus:border-blue-500 focus:outline-none"
        :placeholder="t('accounts.byExit')"
      />
    </label>
  </div>
  <label class="block">
    <span class="mb-1 block text-sm font-medium text-gray-400">{{
      t('accounts.cdpEndpoint')
    }}</span>
    <input
      v-model.trim="environment.cdp_endpoint"
      class="w-full rounded border border-line bg-canvas px-3 py-2 font-mono text-sm text-white transition focus:border-blue-500 focus:outline-none"
      :placeholder="t('accounts.cdpEndpointPlaceholder')"
    />
  </label>
  <div class="block">
    <span class="mb-1 block text-sm font-medium text-gray-400">{{ t('accounts.cdpHeaders') }}</span>
    <div v-if="cdpHeadersText && !revealCDPHeaders" class="flex items-center gap-2">
      <input
        class="w-full rounded border border-line bg-canvas px-3 py-2 font-mono text-sm text-gray-400"
        type="password"
        value="••••••••••••"
        readonly
      />
      <button
        type="button"
        class="shrink-0 rounded border border-line px-2.5 py-2 text-xs text-gray-300 transition hover:bg-line hover:text-white"
        :aria-label="t('settings.cdpHeadersShow')"
        @click="revealCDPHeaders = true"
      >
        <UiIcon name="eye" :size="14" />
      </button>
      <button
        type="button"
        class="shrink-0 rounded border border-line px-2.5 py-2 text-xs text-gray-400 transition hover:bg-line hover:text-white"
        @click="clearCDPHeaders"
      >
        {{ t('common.clear') }}
      </button>
    </div>
    <div v-else class="space-y-1">
      <textarea
        :value="cdpHeadersText"
        class="min-h-16 w-full resize-y rounded border border-line bg-canvas px-3 py-2 font-mono text-xs text-white transition focus:border-blue-500 focus:outline-none"
        rows="2"
        :placeholder="t('accounts.cdpHeadersPlaceholder')"
        @input="updateCDPHeaders(($event.target as HTMLTextAreaElement).value)"
      />
      <div class="flex items-center justify-between gap-2">
        <p v-if="cdpHeadersError" class="text-xs text-red-400">{{ cdpHeadersError }}</p>
        <span v-else class="text-xs text-gray-500">{{ t('accounts.cdpFallbackHint') }}</span>
        <button
          v-if="cdpHeadersText"
          type="button"
          class="inline-flex shrink-0 items-center gap-1 text-xs text-gray-400 transition hover:text-white"
          @click="revealCDPHeaders = false"
        >
          <UiIcon name="eyeOff" :size="13" />
          {{ t('settings.cdpHeadersHide') }}
        </button>
      </div>
    </div>
  </div>
</template>
