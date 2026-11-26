import { createPublicKey, verify } from 'node:crypto'

export const issuer = 'https://auth.openai.com'
export const resource = 'https://api.openai.com/v1'
export const usagePage = 'https://chatgpt.com/settings/usage'
export const shareScope = 'chatgpt.tokens.use.direct'
export const dynamicClient = 'dynamic_agent_client'
export const expired = '接続の有効期限が切れました。もう一度サインインしてください。'
export const needsPlan = '質問するには、このアカウントでChatGPTプランの利用を許可してください。'
export const invalidLogin = 'サインインの応答を検証できませんでした。もう一度サインインしてください。'
const malformed = 'OpenAIの応答を読み取れませんでした。'
const incomplete = '回答の受信が途中で終了しました。もう一度質問してください。'

export class ChatError extends Error {
  constructor(message, code = '') { super(message); this.code = code }
}

export function messageFor(error) {
  if (error instanceof ChatError) return error.message
  if (error?.name === 'TimeoutError') return '通信がタイムアウトしました。もう一度お試しください。'
  if (error?.name === 'AbortError') return '操作を中止しました。'
  return 'ChatGPTに接続できませんでした。ネットワークの接続を確認してください。'
}

export function remoteError(status, body) {
  const code = body?.error?.code ?? (typeof body?.error === 'string' ? body.error : body?.code)
  if (status === 429 || code === 'subscription_sharing_usage_limit_exceeded')
    return new ChatError('利用上限に達しました。ChatGPTの利用状況を確認してください。', 'usage_limit')
  if (code === 'access_denied') return new ChatError('ChatGPTとの接続が許可されませんでした。', code)
  if (code === 'subscription_sharing_unsupported_capability')
    return new ChatError('選んだモデルまたはエフォートを利用できません。モデル一覧を更新してください。', code)
  if (status === 401) return new ChatError('認証が認められませんでした。アカウントの接続を確認してください。', code)
  if (status === 403 || code === 'subscription_sharing_user_not_eligible')
    return new ChatError('このアカウントまたはワークスペースでは、この操作が許可されていません。', code)
  if (status === 503 || code === 'subscription_sharing_usage_unavailable')
    return new ChatError('ChatGPTが一時的に利用できません。時間をおいてお試しください。', code)
  return new ChatError('OpenAIへのリクエストを完了できませんでした（HTTP ' + status + '）。', code)
}

export async function requestJSON(fetchImpl, url, options = {}) {
  const response = await fetchImpl(url, { ...options, redirect: 'error' })
  let body
  try { body = await response.json() } catch { /* An HTTP error may have no JSON body. */ }
  if (!response.ok) throw remoteError(response.status, body)
  if (!body || typeof body !== 'object') throw new ChatError(malformed)
  return body
}

export function tokenData(body, previous) {
  const scopes = typeof body.scope === 'string' ? body.scope.split(/\s+/).filter(Boolean) : previous?.scopes ?? []
  const idToken = body.id_token ?? previous?.idToken
  const access = body.access_token
  if (scopes.includes(shareScope)) {
    if (typeof access !== 'string' || !access || body.token_type?.toLowerCase() !== 'bearer' ||
        !Number.isFinite(body.expires_in) || body.expires_in <= 0) throw new ChatError(malformed)
  } else if (!idToken) throw new ChatError(invalidLogin)
  return { access, idToken, scopes, refresh: body.refresh_token ?? previous?.refresh,
    expires: Date.now() + (body.expires_in || 0) * 1000 }
}

export function verifyIdentity(token, clientId, nonce, keys, now = Date.now() / 1000) {
  try {
    const pieces = token.split('.')
    if (pieces.length !== 3 || pieces.some(part => !/^[A-Za-z0-9_-]+$/.test(part))) throw new Error()
    const header = JSON.parse(Buffer.from(pieces[0], 'base64url'))
    if (header.alg !== 'RS256' || typeof header.kid !== 'string' || !header.kid) throw new Error()
    const jwk = keys.find(k => k.kid === header.kid && k.kty === 'RSA' &&
      (!k.use || k.use === 'sig') && (!k.alg || k.alg === 'RS256'))
    const key = createPublicKey({key: jwk, format:'jwk'})
    if (key.asymmetricKeyDetails.modulusLength < 2048 ||
        !verify('RSA-SHA256', Buffer.from(pieces.slice(0, 2).join('.')), key, Buffer.from(pieces[2], 'base64url'))) throw new Error()
    const c = JSON.parse(Buffer.from(pieces[1], 'base64url'))
    const audiences = Array.isArray(c.aud) ? c.aud : [c.aud]
    if (c.iss !== issuer || c.nonce !== nonce || typeof c.sub !== 'string' || !c.sub ||
        !audiences.includes(clientId) || (c.azp !== undefined && c.azp !== clientId) ||
        (audiences.length > 1 && c.azp !== clientId) || !Number.isFinite(c.iat) || c.iat > now + 5 ||
        !Number.isFinite(c.exp) || c.exp <= now - 5 ||
        (c.nbf !== undefined && (!Number.isFinite(c.nbf) || c.nbf > now + 5))) throw new Error()
    return {subject:c.sub, email:c.email_verified === true && typeof c.email === 'string' ? c.email : null}
  } catch { throw new ChatError(invalidLogin) }
}

const supportedEfforts = new Set(['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'])
// Model-specific fallbacks for catalogs that advertise only the slug and display name.
// See https://developers.openai.com/api/docs/models and the matching Mac catalog.
function fallbackEfforts(id) {
  const levels = ['low', 'medium', 'high', 'xhigh', 'max']
  if (['gpt-6.2-astra', 'gpt-6.2-sol', 'gpt-6.1-sol', 'gpt-6-astra'].includes(id)) return levels
  if (['gpt-6.1-terra', 'gpt-6-sol', 'gpt-6-luna', 'gpt-5.6', 'gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna'].includes(id))
    return ['none', ...levels]
  if (['gpt-5.5', 'gpt-5.4', 'gpt-5.4-mini', 'gpt-5.4-nano', 'gpt-5.2'].includes(id))
    return ['none', 'low', 'medium', 'high', 'xhigh']
  return []
}

export function modelCatalog(body) {
  if (!Array.isArray(body?.models)) throw new ChatError(malformed)
  const models = new Map()
  for (const row of body.models) {
    if (row?.visibility !== 'list' || typeof row.slug !== 'string') continue
    const id = row.slug.trim()
    if (!id || models.has(id)) continue
    let efforts = []
    if (!Object.hasOwn(row, 'supported_reasoning_levels')) efforts = fallbackEfforts(id)
    else if (Array.isArray(row.supported_reasoning_levels)) {
      efforts = [...new Set(row.supported_reasoning_levels.map(item => item?.effort ?? item).filter(item => supportedEfforts.has(item)))]
    }
    const name = typeof row.display_name === 'string' ? row.display_name.trim() || id : id
    models.set(id, {id, name, efforts})
  }
  if (!models.size) throw new ChatError('このアカウントで利用できるモデルが見つかりません。', 'no_models')
  return [...models.values()]
}

export async function streamAnswer(response, onPartial) {
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw remoteError(response.status, body)
  }
  if (!response.body) throw new ChatError(incomplete)
  let buffer = '', data = [], answer = '', lastDisplay = 0
  const decoder = new TextDecoder()
  function acceptLine(line) {
    if (line.startsWith('data:')) { data.push(line.slice(5).replace(/^ /, '')); return }
    if (line || !data.length) return
    const value = data.join('\n')
    data = []
    if (value === '[DONE]') throw new ChatError(incomplete)
    let event
    try { event = JSON.parse(value) } catch { throw new ChatError(malformed) }
    if (event.type === 'response.output_text.delta' || event.type === 'response.refusal.delta') {
      if (typeof event.delta !== 'string') throw new ChatError(malformed)
      answer += event.delta
      if (Date.now() - lastDisplay >= 100) { onPartial(answer); lastDisplay = Date.now() }
    }
    if (event.type === 'error') throw remoteError(200, event)
    if (event.type === 'response.failed') throw remoteError(200, event.response)
    if (event.type === 'response.incomplete') throw new ChatError(incomplete)
    if (event.type === 'response.completed') {
      if (!answer.trim()) throw new ChatError(incomplete)
      return answer.trim()
    }
  }
  for await (const chunk of response.body) {
    buffer += decoder.decode(chunk, {stream:true})
    let end
    while ((end = buffer.indexOf('\n')) >= 0) {
      const result = acceptLine(buffer.slice(0, end).replace(/\r$/, ''))
      if (result !== undefined) return result
      buffer = buffer.slice(end + 1)
    }
  }
  buffer += decoder.decode()
  const final = acceptLine(buffer.replace(/\r$/, '')) ?? acceptLine('')
  if (final !== undefined) return final
  throw new ChatError(incomplete)
}
