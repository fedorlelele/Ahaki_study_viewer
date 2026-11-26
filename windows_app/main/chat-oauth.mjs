import { createHash, randomBytes } from 'node:crypto'
import { createServer } from 'node:http'
import { ChatError, dynamicClient, invalidLogin, issuer, resource, shareScope } from './chat-protocol.mjs'

export function authorizationRequest({clientId, host, redirect, state, nonce, verifier, hint, consent}) {
  const url = new URL('/api/accounts/authorize', issuer)
  url.search = new URLSearchParams({
    client_id:clientId || dynamicClient, ext_agent_host_id:host, redirect_uri:redirect,
    response_type:'code', scope:'openid profile email offline_access resource.invoke ' + shareScope,
    resource, state, nonce, code_challenge_method:'S256',
    code_challenge:createHash('sha256').update(verifier).digest('base64url')
  })
  if (!clientId) url.searchParams.set('agent_name_hint', 'Ahaki Study Viewer')
  if (hint) url.searchParams.set('id_token_hint', hint)
  if (consent) url.searchParams.set('prompt', 'consent')
  // Electron's shell.openExternal accepts at most 2081 characters on Windows.
  // A long optional hint can be omitted; the issued client ID still binds the workspace.
  if (url.href.length > 2081) url.searchParams.delete('id_token_hint')
  return url.href
}

export function browserLogin(options, openExternal, signal) {
  const random = () => randomBytes(32).toString('base64url')
  const state = random(), nonce = random(), verifier = random()
  return new Promise((resolve, reject) => {
    signal.throwIfAborted()
    let redirect, done = false
    const server = createServer({ maxHeaderSize:16384, headersTimeout:10000, requestTimeout:10000 }, (req, res) => {
      res.setHeader('Content-Type', 'text/plain; charset=utf-8')
      res.setHeader('Cache-Control', 'no-store')
      res.setHeader('Referrer-Policy', 'no-referrer')
      res.setHeader('Connection', 'close')
      let url
      if (req.method === 'GET' && req.url?.startsWith('/auth/callback?') && req.headers.host === redirect?.host) {
        try { url = new URL(req.url, redirect) } catch { /* Ignore invalid local requests. */ }
      }
      const params = url?.searchParams
      if (!params || params.get('state') !== state || [...params.keys()].some(k => params.getAll(k).length !== 1) ||
          (!params.get('code') && !params.get('error')) || (params.has('code') && params.has('error'))) {
        res.writeHead(400); res.end('このリクエストは受け付けられません。'); return
      }
      res.end('ASVに戻ってサインイン結果を確認してください。このタブを閉じて構いません。')
      finish(null, {params, redirect:redirect.href, nonce, verifier})
    })
    server.maxConnections = 12
    server.setTimeout(12000, socket => socket.destroy())
    const cancel = () => finish(signal.reason)
    const timeout = setTimeout(() => finish(new ChatError('サインインの有効時間が過ぎました。もう一度お試しください。')), 300000)
    function finish(error, result) {
      if (done) return
      done = true
      clearTimeout(timeout)
      signal.removeEventListener('abort', cancel)
      server.close()
      if (error) { server.closeAllConnections(); reject(error) } else resolve(result)
    }
    signal.addEventListener('abort', cancel, {once:true})
    server.on('error', () => finish(new ChatError('サインイン用の接続を開けませんでした。')))
    server.listen({host:'127.0.0.1',port:0,exclusive:true}, async () => {
      if (done) { server.close(); return }
      redirect = new URL('http://127.0.0.1:' + server.address().port + '/auth/callback')
      try { await openExternal(authorizationRequest({...options, redirect:redirect.href, state, nonce, verifier})) }
      catch { finish(new ChatError('サインイン用のブラウザを開けませんでした。')) }
    })
  })
}

export function callbackClient(query, expected) {
  if (query.get('error') === 'access_denied') throw new ChatError('サインインをキャンセルしました。')
  const id = query.get('client_id') ?? expected
  if (query.has('error') || !query.get('code') || typeof id !== 'string' || !id ||
      id === dynamicClient || (expected && expected !== id)) throw new ChatError(invalidLogin)
  return id
}
