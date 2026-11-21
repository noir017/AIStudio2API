// formatCDPHeaders 把账户级 CDP 自定义请求头对象格式化为按行排列的 Key: Value 文本
export function formatCDPHeaders(headers?: Record<string, string>): string {
  if (!headers) return ''
  return Object.keys(headers)
    .sort((a, b) => a.localeCompare(b))
    .map((key) => `${key}: ${headers[key] ?? ''}`)
    .join('\n')
}

// parseCDPHeaders 解析多行 Key: Value 或 JSON 对象格式的 CDP 自定义请求头，留空返回 undefined
export function parseCDPHeaders(raw: string): {
  headers?: Record<string, string> | undefined
  error?: string | undefined
} {
  const trimmed = raw.trim()
  if (trimmed === '') return { headers: undefined }
  if (trimmed.startsWith('{')) {
    try {
      const parsed: unknown = JSON.parse(trimmed)
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
        return { error: 'JSON object required' }
      }
      const headers: Record<string, string> = {}
      for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
        const cleanKey = key.trim()
        if (!cleanKey || /[\s:]/.test(cleanKey)) return { error: `Invalid header name: ${key}` }
        if (typeof value !== 'string') return { error: `Header ${key} value must be a string` }
        headers[cleanKey] = value.trim()
      }
      return { headers: Object.keys(headers).length > 0 ? headers : undefined }
    } catch (err) {
      return { error: err instanceof Error ? err.message : 'Invalid JSON' }
    }
  }
  const headers: Record<string, string> = {}
  for (const rawLine of trimmed.replace(/\r\n/g, '\n').split('\n')) {
    const line = rawLine.trim()
    if (!line) continue
    const sep = line.indexOf(':')
    if (sep <= 0) return { error: 'Each line must be Key: Value' }
    const key = line.slice(0, sep).trim()
    const value = line.slice(sep + 1).trim()
    if (!key || /[\s:]/.test(key)) return { error: `Invalid header name: ${key}` }
    headers[key] = value
  }
  return { headers: Object.keys(headers).length > 0 ? headers : undefined }
}
