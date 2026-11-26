import test from 'node:test'
import assert from 'node:assert/strict'
import { createHash, generateKeyPairSync, sign } from 'node:crypto'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { get } from 'node:http'
import { tmpdir } from 'node:os'
import { authorizationRequest, browserLogin, callbackClient } from '../main/chat-oauth.mjs'
import { modelCatalog, streamAnswer, verifyIdentity, issuer, resource, tokenData, shareScope } from '../main/chat-protocol.mjs'
import { createVaultStore } from '../main/chat-vault.mjs'

test('モデル一覧は表示順と対応エフォートを保持する', () => {
  const rows = [
    {slug:'hidden', visibility:'none'},
    {slug:'gpt-6.2-sol', visibility:'list'},
    {slug:'test', display_name:'テスト', visibility:'list',supported_reasoning_levels:[{effort:'low'},'ultra','high','high']},
    {slug:'gpt-6.2-astra',visibility:'list',supported_reasoning_levels:[]},
    {slug:'test',display_name:'duplicate',visibility:'list'},
  ]
  const models = modelCatalog({models:rows})
  assert.deepEqual(models.map(x => x.id), ['gpt-6.2-sol','test','gpt-6.2-astra'])
  assert.deepEqual(models[0].efforts,['low','medium','high','xhigh','max'])
  assert.deepEqual(models[1], {id:'test',name:'テスト',efforts:['low','high']})
  assert.deepEqual(models[2].efforts,[])
  assert.throws(() => modelCatalog({models:[]}), /モデルが見つかりません/)
})

test('トークンの認可範囲と有効期限、更新後のトークンを一緒に確定する', () => {
  assert.throws(() => tokenData({scope:shareScope, access_token:'access', token_type:'other', expires_in:30}))
  const before = tokenData({scope:shareScope, access_token:'access', refresh_token:'refresh', token_type:'Bearer', expires_in:30})
  const renewed = tokenData({access_token:'new-access', refresh_token:'rotated',token_type:'Bearer', expires_in:600}, before)
  assert.deepEqual(renewed.scopes,[shareScope])
  assert.equal(renewed.refresh,'rotated')
  assert.deepEqual(tokenData({id_token:'id',scope:'openid'}).scopes,['openid'])
})

test('検証済みの発行元、受領者、署名、nonce の一致を必要とする', () => {
  const pair = generateKeyPairSync('rsa', {modulusLength:2048})
  const key = {...pair.publicKey.export({format:'jwk'}), kid:'key'}
  const base = value => Buffer.from(JSON.stringify(value)).toString('base64url')
  const claims = {iss:issuer,aud:'client',nonce:'nonce',sub:'user',iat:990,exp:1060, email:'verified@example.invalid',email_verified:true}
  function jwt(overrides={}) {
    const payload = base({kid:'key',alg:'RS256'}) + '.' + base({...claims,...overrides})
    return payload + '.' + sign('RSA-SHA256',Buffer.from(payload),pair.privateKey).toString('base64url')
  }
  function verifyToken(token) {return verifyIdentity(token,'client','nonce',[key],1000)}
  assert.deepEqual(verifyToken(jwt()),{subject:'user',email:'verified@example.invalid'})
  for (const change of [{iss:'elsewhere'},{aud:'other'},{aud:['client','other']},{nonce:'other'}, {exp:990},{iat:1020},{nbf:1020}]) {
    assert.throws(() => verifyToken(jwt(change)), /検証できません/)
  }
  assert.equal(verifyToken(jwt({email_verified:false})).email,null)
  const signed = jwt()
  assert.throws(() => verifyToken(signed.slice(0,signed.lastIndexOf('.')+1) + 'wrong'))
  assert.throws(() => callbackClient(new URLSearchParams({code:'code',client_id:'different'}),'client'))
})

test('SSEはUTF-8文字の途中で分割でき、完了前の結果や利用上限を成功としない', async () => {
  function dataResponse(events) {
    const bytes = new TextEncoder().encode(events.map(e=>'data: '+JSON.stringify(e)+'\r\n\r\n').join(''))
    return new Response(new ReadableStream({start(c){ for (const b of bytes) c.enqueue(Uint8Array.of(b)); c.close() }}))
  }
  const delta = {type:'response.output_text.delta',delta:'筋肉\n'}
  const partial = []
  assert.equal(await streamAnswer(dataResponse([delta,{type:'response.completed'}]), t=>partial.push(t)), '筋肉')
  assert.deepEqual(partial,['筋肉\n'])
  await assert.rejects(streamAnswer(dataResponse([delta]),()=>{}),/途中で終了/)
  await assert.rejects(streamAnswer(dataResponse([delta,{type:'response.failed',response:{error:{code:'subscription_sharing_usage_limit_exceeded'}}}]),()=>{}), {code:'usage_limit'})
})

test('ループバック認証は state と host を照合し、PKCEと発行済みIDを保持する', async () => {
  let sent, exercise
  const controller = new AbortController()
  const result = await browserLogin({host:'urn:uuid:test'}, address => {
    exercise = (async () => {
      const url = new URL(address)
      assert.equal(url.origin,issuer)
      sent = url.searchParams
      assert.equal(sent.get('client_id'),'dynamic_agent_client')
      assert.equal(sent.get('resource'),resource)
      const target = new URL(sent.get('redirect_uri'))
      assert.equal(target.hostname, '127.0.0.1')
      assert.equal(target.pathname, '/auth/callback')
      target.search = new URLSearchParams({state:'wrong',code:'fixture'})
      assert.equal((await fetch(target)).status,400)
      target.search = new URLSearchParams({state:sent.get('state'),client_id:'issued',code:'fixture'})
      const badHost = await new Promise((resolve, reject) => {
        get(target, {headers:{Host:'wrong.invalid'}}, res => { res.resume(); resolve(res.statusCode) }).on('error',reject)
      })
      assert.equal(badHost,400)
      assert.equal((await fetch(target)).status,200)
    })()
    return exercise
  }, controller.signal)
  await exercise
  assert.equal(result.nonce, sent.get('nonce'))
  assert.equal(result.redirect,sent.get('redirect_uri'))
  assert.equal(createHash('sha256').update(result.verifier).digest('base64url'),sent.get('code_challenge'))
  assert.equal(callbackClient(result.params,null),'issued')
})

test('Windowsのブラウザ引き渡し上限を超える場合も同じ登録で再認証できる', () => {
  const url = new URL(authorizationRequest({clientId:'issued',host:'urn:uuid:test',
    redirect:'http://127.0.0.1:10000/auth/callback',state:'state',nonce:'nonce',verifier:'verifier',
    hint:'a'.repeat(2200),consent:true}))
  assert.ok(url.href.length <= 2081)
  assert.equal(url.searchParams.has('id_token_hint'),false)
  assert.equal(url.searchParams.get('client_id'),'issued')
  assert.equal(url.searchParams.get('prompt'),'consent')
})

test('接続データは暗号化されたバイト列で保存し、復号できない記録は上書きしない', async t => {
  const dir = mkdtempSync(join(tmpdir(),'asv-account-test-'))
  t.after(()=>rmSync(dir,{recursive:true,force:true}))
  // Exercise the safeStorage boundary without touching machine/user credentials.
  const reverse = value => Buffer.from(value).reverse()
  const storage = {isEncryptionAvailable:()=>true, encryptString:reverse, decryptString:b=>reverse(b).toString()}
  const store = createVaultStore(dir,storage), original = store.load()
  original.profiles.push({id:'registration',tokens:{access:'fixture-secret'}})
  store.save(original)
  const blob = join(dir,'chat-accounts.bin')
  assert.equal(readFileSync(blob).includes('fixture-secret'),false)
  assert.deepEqual(store.load(),original)
  storage.isEncryptionAvailable = () => false
  assert.throws(()=>store.save({...original,profiles:[]}))
  storage.isEncryptionAvailable = () => true
  assert.deepEqual(store.load(),original)
  writeFileSync(blob, 'unreadable fixture')
  assert.throws(()=>store.load(),/読み書きできません/)
  assert.equal(readFileSync(blob,'utf8'),'unreadable fixture')
})
