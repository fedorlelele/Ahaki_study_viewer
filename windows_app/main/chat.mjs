import { randomUUID } from 'node:crypto'
import { createVaultStore } from './chat-vault.mjs'
import { browserLogin, callbackClient } from './chat-oauth.mjs'
import {
  ChatError, issuer, resource, usagePage, expired, needsPlan, invalidLogin, shareScope,
  messageFor, requestJSON, verifyIdentity, tokenData, modelCatalog, streamAnswer
} from './chat-protocol.mjs'

const terminalRefresh = new Set(['invalid_grant', 'invalid_refresh_token', 'token_expired',
  'refresh_token_expired', 'refresh_token_invalidated', 'refresh_token_reused'])
const instruction = 'あん摩マッサージ指圧師、はり師、きゅう師の国家試験の学習を助ける講師として、日本語で質問に答えてください。問題文・選択肢と解答の根拠を確認し、読み上げで理解しやすい簡潔な文章で説明してください。'
const busyMessage = '実行中の処理が終わるか、中止してから操作してください。'
function requiredString(value) {
  if (typeof value !== 'string' || !value.trim()) throw new ChatError('質問の入力内容を確認してください。')
  return value.trim()
}

// A canceled question stops waiting immediately. The shared refresh still needs to save its rotated token.
function waitFor(promise, signal) {
  signal.throwIfAborted()
  return new Promise((resolve, reject) => {
    const cancel = () => reject(signal.reason)
    signal.addEventListener('abort', cancel, {once:true})
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', cancel))
  })
}

export function createChatService({ userData, safeStorage, openExternal, emit = () => {}, fetchImpl = fetch,
  store = createVaultStore(userData, safeStorage) }) {
  let vault, status = '', models = [], catalogWork = null, modelError = '', operation = null
  let usageLimitReached = false, planWelcome = false, closed = false
  const refreshes = new Map(), conversations = new Map()
  try { vault = store.load() } catch (error) { status = messageFor(error) }

  function active() { return vault?.profiles.find(profile => profile.id === vault.active) }
  function model() { return models.find(item => item.id === active()?.modelId) ?? models[0] }
  function effortFor(selected) {
    const value = active()?.efforts?.[selected?.id]
    return selected?.efforts.includes(value) ? value : null
  }

  function state() {
    const current = active(), selected = model()
    return {
      accounts:(vault?.profiles ?? []).map((p, index) => ({id:p.id, label:'接続 ' + (index + 1) + (p.email ? '（' + p.email + '）' : '')})),
      activeAccountId:current?.id ?? null, signedIn:!!current?.tokens, canAsk:!!current?.tokens?.scopes.includes(shareScope),
      busy:!!operation, answeringProblemId:operation?.problemId ?? null, models, modelId:selected?.id ?? null,
      effort:effortFor(selected), status, loadingModels:!!catalogWork, modelError, usageLimitReached, planWelcome
    }
  }
  function notify() { if (!closed) emit({type:'state', state:state()}) }
  function alter(edit) {
    const next = structuredClone(vault)
    edit(next)
    store.save(next)
    vault = next
  }
  function profile(v, id) { return v.profiles.find(p => p.id === id) }

  function clearCatalog(reason) {
    const work = catalogWork
    catalogWork = null
    models = []
    modelError = ''
    work?.controller.abort(reason)
  }
  function clearSession(id, tokens) {
    if (profile(vault, id)?.tokens?.access !== tokens.access || profile(vault, id)?.tokens?.refresh !== tokens.refresh) return
    alter(v => { profile(v, id).tokens = null })
    if (vault.active === id) {
      clearCatalog(new ChatError(expired))
      status = expired
      planWelcome = false
    }
    notify()
  }

  function tokenRequest(fields, signal) {
    return requestJSON(fetchImpl, issuer + '/api/accounts/oauth/token', {
      method:'POST', body:new URLSearchParams({...fields, resource}), signal
    })
  }
  async function access(id, signal) {
    signal.throwIfAborted()
    const tokens = profile(vault, id)?.tokens
    if (!tokens) throw new ChatError(expired)
    if (!tokens.scopes.includes(shareScope)) throw new ChatError(needsPlan)
    if (tokens.expires > Date.now() + 60000) return tokens.access
    if (!tokens.refresh) { clearSession(id, tokens); throw new ChatError(expired) }
    if (!refreshes.has(id)) {
      const renewal = (async () => {
        try {
          const body = await tokenRequest({grant_type:'refresh_token', client_id:id, refresh_token:tokens.refresh}, AbortSignal.timeout(25000))
          const updated = tokenData(body, tokens), current = profile(vault, id)?.tokens
          if (current?.access !== tokens.access || current?.refresh !== tokens.refresh) throw new ChatError(expired)
          alter(v => { profile(v, id).tokens = updated })
          if (!updated.scopes.includes(shareScope)) {
            if (id === vault.active) { clearCatalog(new ChatError(needsPlan)); status = needsPlan }
            throw new ChatError(needsPlan)
          }
          return updated.access
        } catch (error) {
          if (terminalRefresh.has(error.code)) { clearSession(id, tokens); throw new ChatError(expired) }
          throw error
        } finally { refreshes.delete(id); notify() }
      })()
      refreshes.set(id, renewal)
    }
    return await waitFor(refreshes.get(id), signal)
  }

  async function loadModels(force = false) {
    const current = active()
    if (!current?.tokens?.scopes.includes(shareScope)) throw new ChatError(needsPlan)
    if (catalogWork) return catalogWork.promise
    if (!force && models.length) return models
    const work = {controller:new AbortController()}
    catalogWork = work
    modelError = ''
    work.promise = (async () => {
      try {
        const signal = AbortSignal.any([work.controller.signal, AbortSignal.timeout(30000)])
        const bearer = await access(current.id, signal)
        const body = await requestJSON(fetchImpl, resource + '/models', {headers:{Authorization:'Bearer ' + bearer}, signal})
        signal.throwIfAborted()
        const result = modelCatalog(body)
        if (catalogWork !== work || vault.active !== current.id) throw new DOMException('Stopped', 'AbortError')
        models = result
        return result
      } catch (error) {
        if (catalogWork === work) {
          modelError = messageFor(error)
          if (error.code === 'no_models') models = []
          if (error.code === 'usage_limit') usageLimitReached = true
        }
        throw error
      } finally {
        if (catalogWork === work) { catalogWork = null; notify() }
      }
    })()
    notify()
    return work.promise
  }

  async function signIn(newAccount, signal) {
    // Retry reading DPAPI data before sign-in. Never overwrite an inaccessible saved account.
    vault = store.load()
    const previous = newAccount ? null : active()
    if (previous && refreshes.has(previous.id)) await waitFor(refreshes.get(previous.id).catch(() => {}), signal)
    const known = newAccount ? null : active()
    const expected = known?.id ?? vault.pending
    alter(() => {}) // Persist the stable host ID before opening the browser, even on first sign-in.
    const callback = await browserLogin({
      clientId:expected, host:vault.host, hint:known?.tokens?.idToken,
      consent:!!known?.tokens && !known.tokens.scopes.includes(shareScope)
    }, openExternal, signal)
    const id = callbackClient(callback.params, expected)
    if (!known) alter(v => { v.pending = id })
    const requestSignal = AbortSignal.any([signal, AbortSignal.timeout(30000)])
    const body = await tokenRequest({grant_type:'authorization_code', code:callback.params.get('code'), client_id:id,
      code_verifier:callback.verifier, redirect_uri:callback.redirect}, requestSignal)
    if (!body.id_token) throw new ChatError(invalidLogin)
    const jwks = await requestJSON(fetchImpl, issuer + '/.well-known/jwks.json', {signal:requestSignal})
    const identity = verifyIdentity(body.id_token, id, callback.nonce, jwks.keys)
    const saved = profile(vault, id)
    if (saved && saved.subject !== identity.subject) throw new ChatError(invalidLogin)
    const tokens = tokenData(body)
    signal.throwIfAborted()
    const welcome = tokens.scopes.includes(shareScope) && !saved?.welcomed
    alter(v => {
      let account = profile(v, id)
      if (!account) { account = {id, efforts:{}}; v.profiles.push(account) }
      Object.assign(account, identity, {tokens, welcomed:!!account.welcomed || welcome})
      v.active = id
      if (v.pending === id) v.pending = null
    })
    clearCatalog()
    usageLimitReached = false
    planWelcome = welcome
    status = tokens.scopes.includes(shareScope) ? 'ChatGPTに接続しました。' : needsPlan
  }

  async function signOut() {
    const id = active()?.id
    if (!id) return
    clearCatalog()
    // Revoke the newest refresh token if a background model lookup rotated it.
    if (refreshes.has(id)) await refreshes.get(id).catch(() => {})
    const token = active()?.tokens?.refresh
    let revoked = !token
    if (token) {
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const response = await fetchImpl(issuer + '/api/accounts/oauth/revoke', {
            method:'POST', body:new URLSearchParams({client_id:id, token, token_type_hint:'refresh_token'}),
            redirect:'error', signal:AbortSignal.timeout(10000)
          })
          await response.body?.cancel()
          revoked = response.status === 200
          if (revoked || response.status < 500) break
        } catch { /* A local sign-out also works offline. */ }
        if (attempt === 0) await new Promise(resolve => setTimeout(resolve, 700))
      }
    }
    alter(v => { profile(v, id).tokens = null })
    planWelcome = false
    usageLimitReached = false
    status = revoked ? 'サインアウトしました。' : 'このPCからサインアウトしました。サーバー側の解除はChatGPTの設定で確認してください。'
  }

  function threadFor(id, problemId) {
    if (!conversations.has(id)) conversations.set(id, new Map())
    const threads = conversations.get(id)
    if (!threads.has(problemId)) threads.set(problemId, [])
    return threads.get(problemId)
  }
  function startSend({problemId, context, prompt}) {
    if (operation) throw new ChatError(busyMessage)
    const account = active()
    if (!account?.tokens?.scopes.includes(shareScope)) throw new ChatError(needsPlan)
    problemId = requiredString(problemId)
    prompt = requiredString(prompt)
    context = requiredString(context)
    const thread = threadFor(account.id, problemId)
    const input = [{role:'user',content:'【国家試験問題と解説】\n' + context}]
    for (const turn of thread) if (turn.state === 'complete') {
      input.push({role:'user',content:turn.question}, {role:'assistant',content:turn.answer})
    }
    input.push({role:'user',content:prompt})
    const turn = {id:randomUUID(), question:prompt, answer:'', state:'receiving'}
    const update = () => { if (!closed) emit({type:'turn', accountId:account.id, problemId, turn:{...turn}}) }
    const controller = new AbortController()
    operation = {controller, problemId}
    const selected = model()
    status = ''
    usageLimitReached = false
    thread.push(turn)
    update()
    notify()
    void (async () => {
      try {
        const signal = controller.signal
        let chosen = selected
        if (!chosen) {
          const list = await waitFor(loadModels(), signal)
          chosen = list.find(item => item.id === account.modelId) ?? list[0]
        }
        const bearer = await access(account.id, signal)
        signal.throwIfAborted()
        const body = {model:chosen.id, instructions:instruction, input, store:false, stream:true}
        const effort = account.efforts?.[chosen.id]
        if (chosen.efforts.includes(effort)) body.reasoning = {effort}
        const response = await fetchImpl(resource + '/responses', {method:'POST', redirect:'error',
          signal:AbortSignal.any([signal, AbortSignal.timeout(600000)]),
          headers:{Authorization:'Bearer ' + bearer, 'Content-Type':'application/json', Accept:'text/event-stream'},
          body:JSON.stringify(body)})
        turn.answer = await streamAnswer(response, text => { turn.answer = text; update() })
        signal.throwIfAborted()
        turn.state = 'complete'
      } catch (error) {
        turn.state = 'interrupted'
        turn.error = controller.signal.aborted ? '回答を中止しました。' : messageFor(error)
        usageLimitReached = error.code === 'usage_limit'
      } finally { operation = null; update(); notify() }
    })()
  }

  async function accountAction(action, payload) {
    if (operation) throw new ChatError(busyMessage)
    const controller = new AbortController()
    operation = {controller}
    status = action === 'signIn' ? 'ブラウザでChatGPTへのサインインを完了してください。' : 'サインアウトしています。'
    notify()
    try {
      if (action === 'signIn') await signIn(payload.newAccount === true, controller.signal)
      else await signOut()
    } catch (error) { status = messageFor(error) }
    finally { operation = null; notify() }
  }

  return {
    async handle(action, payload = {}) {
      if (closed) throw new ChatError('ASVを終了しています。')
      switch (action) {
        case 'state': break
        case 'history': return active()?.tokens ? threadFor(vault.active, requiredString(payload.problemId)).map(t => ({...t})) : []
        case 'usage':
          try { await openExternal(usagePage) } catch { throw new ChatError('利用状況のページを開けませんでした。') }
          break
        case 'ackWelcome': planWelcome = false; notify(); break
        case 'cancel': operation?.controller.abort(); break
        case 'signIn': case 'signOut': await accountAction(action, payload); break
        case 'models':
          if (active()?.tokens?.scopes.includes(shareScope)) await loadModels(payload.force === true).catch(() => {})
          break
        case 'selectAccount': case 'selectModel': case 'selectEffort': {
          if (operation) throw new ChatError(busyMessage)
          if (action === 'selectAccount') {
            if (!vault?.profiles.some(p => p.id === payload.id)) throw new ChatError('アカウントを選んでください。')
            if (vault.active === payload.id) break
            alter(v => { v.active = payload.id })
            clearCatalog()
            planWelcome = false
            usageLimitReached = false
            status = active().tokens ? 'アカウントを切り替えました。' : '選んだアカウントでサインインしてください。'
          } else if (action === 'selectModel') {
            if (!models.some(m => m.id === payload.id)) throw new ChatError('モデルを選んでください。')
            alter(v => { profile(v, v.active).modelId = payload.id })
          } else {
            const selected = model()
            if (!selected || (payload.effort !== null && !selected.efforts.includes(payload.effort))) throw new ChatError('エフォートを選んでください。')
            alter(v => {
              const p = profile(v, v.active)
              p.modelId = selected.id
              p.efforts = {...p.efforts, [selected.id]:payload.effort}
            })
          }
          notify()
          break
        }
        case 'send': startSend(payload); break
        default: throw new ChatError('この操作は利用できません。')
      }
      return state()
    },
    close() { closed = true; operation?.controller.abort(); clearCatalog() }
  }
}
