import { test } from 'node:test'
import assert from 'node:assert/strict'
import { generateKeyPairSync, sign } from 'node:crypto'
import { setTimeout as pause } from 'node:timers/promises'
import { createChatService } from '../main/chat.mjs'
import { issuer, resource, shareScope } from '../main/chat-protocol.mjs'

function memoryStore(profiles = []) {
  let data = {version:1, host:'urn:uuid:test',active:profiles[0]?.id ?? null,pending:null,profiles}
  return {load:()=>structuredClone(data), save:v=>{data = structuredClone(v)}}
}
function account(id, expires = Date.now() + 3600000) {
  return {id,subject:id,efforts:{},tokens:{access:'fixture-'+id, refresh:'refresh-'+id,
    idToken:'id-'+id,scopes:[shareScope],expires}}
}
function catalog() {
  return Response.json({models:[{slug:'standard',visibility:'list'},
    {slug:'reasoner',visibility:'list',display_name:'推論モデル',supported_reasoning_levels:['low','high']}]})
}
function success(text) {
  return new Response('data: '+JSON.stringify({type:'response.output_text.delta',delta:text})+'\n\ndata: {"type":"response.completed"}\n\n')
}
async function until(condition) {
  for (let i=0; i<200; i++) { if (await condition()) return; await pause(5) }
  assert.fail('timed out waiting for operation')
}

test('同じ問題でモデルとエフォートを切り替えても会話を継続し、アカウントを分離する', async () => {
  const store = memoryStore([account('A'),account('B')]), events = [], posted = []
  const fetchImpl = async (url,opts) => {
    assert.equal(opts.redirect,'error')
    if (url === resource+'/models') return catalog()
    assert.equal(url,resource+'/responses')
    assert.match(opts.headers.Authorization,/^Bearer fixture-/)
    posted.push(JSON.parse(opts.body))
    return success('回答'+posted.length)
  }
  const service = createChatService({store,fetchImpl,emit:e=>events.push(e)})
  assert.equal((await service.handle('models')).modelId,'standard')
  await service.handle('selectModel',{id:'reasoner'})
  await service.handle('selectEffort',{effort:'high'})
  async function send(prompt) {
    await service.handle('send',{prompt,context:'問題文・選択肢・解説',problemId:'Q'})
    await until(async()=>!(await service.handle('state')).busy)
  }
  await send('質問1')
  await service.handle('selectModel',{id:'standard'})
  await send('質問2')
  assert.deepEqual(posted.map(p=>[p.model,p.reasoning,p.input.length]),[['reasoner',{effort:'high'},2],['standard',undefined,4]])
  assert.equal(posted[1].input[2].content,'回答1')
  assert.equal(posted[0].stream,true)
  assert.equal(posted[0].store,false)
  assert.ok(events.some(e=>e.type==='turn' && e.accountId==='A' && e.turn.state==='complete'))
  assert.deepEqual((await service.handle('history',{problemId:'Q'})).map(t=>t.answer),['回答1','回答2'])
  await service.handle('selectModel',{id:'reasoner'})
  await service.handle('selectAccount',{id:'B'})
  assert.deepEqual(await service.handle('history',{problemId:'Q'}),[])
  assert.equal((await service.handle('models')).effort,null)
  assert.equal((await service.handle('selectAccount',{id:'A'})).models.length,0)
  assert.equal((await service.handle('models')).effort,'high')
  const restarted = createChatService({store,fetchImpl})
  assert.equal((await restarted.handle('models')).effort,'high')
  assert.deepEqual(await restarted.handle('history',{problemId:'Q'}),[])
  assert.doesNotMatch(JSON.stringify(await restarted.handle('state')),/fixture-A|refresh-A|id-A/)
})

test('回答を中止すると進行中のモデル一覧を待たずに戻り、不完全な回答は履歴入力から外す', async () => {
  const store = memoryStore([account('A')])
  const gate = Promise.withResolvers()
  let postCount=0
  const service = createChatService({store,fetchImpl:async url=>{
    if (url === resource+'/models') { await gate.promise; return catalog() }
    postCount++
    return success('回答')
  }})
  const loading = service.handle('models')
  await service.handle('send',{problemId:'A01',context:'症例',prompt:'質問'})
  await service.handle('cancel')
  await until(async()=>!(await service.handle('state')).busy)
  assert.equal((await service.handle('history',{problemId:'A01'}))[0].state,'interrupted')
  assert.equal((await service.handle('state')).loadingModels,true)
  gate.resolve()
  await loading
  assert.equal(postCount,0)
})

test('更新トークンを一度だけ交換し、サインアウト時は最新のトークンを取り消す', async () => {
  const store = memoryStore([account('A',0)]), gate = Promise.withResolvers()
  let revoked, renewals = 0
  const service = createChatService({store,fetchImpl:async (url,options)=>{
    if (url.endsWith('/oauth/token')) {
      assert.equal(options.body.get('resource'),resource)
      renewals++
      await gate.promise
      return Response.json({access_token:'renewed',refresh_token:'rotated',expires_in:3600,token_type:'Bearer'})
    }
    if (url.endsWith('/oauth/revoke')) { revoked=options.body.get('token'); return new Response(null,{status:200}) }
    return catalog()
  }})
  const catalogOne = service.handle('models'), catalogTwo = service.handle('models')
  const leaving = service.handle('signOut')
  gate.resolve()
  await Promise.all([catalogOne,catalogTwo,leaving])
  assert.equal(renewals,1)
  assert.equal(revoked,'rotated')
  assert.equal((await service.handle('state')).signedIn,false)
  assert.equal(store.load().profiles[0].tokens,null)
})

test('認証の失効はアカウントの保存設定を残し、日本語で再サインインを案内する', async () => {
  const store = memoryStore([account('A',0)]), turns = []
  const service = createChatService({store,emit:e=>{if(e.type==='turn')turns.push(e.turn)},fetchImpl:async()=>
    Response.json({error:'invalid_grant'},{status:400})})
  await service.handle('send',{problemId:'Q',context:'問',prompt:'質問'})
  await until(async()=>!(await service.handle('state')).busy)
  const state = await service.handle('state')
  assert.equal(state.canAsk,false)
  assert.match(state.status,/もう一度サインイン/)
  assert.match(turns.at(-1).error,/もう一度サインイン/)
  assert.equal(store.load().profiles.length,1)
})

test('初回登録で検証済みIDを保存し、再同意によって利用を許可する', async () => {
  const pair = generateKeyPairSync('rsa',{modulusLength:2048})
  const key = {...pair.publicKey.export({format:'jwk'}),kid:'fixture'}
  const store=memoryStore()
  let authorizations=0, exchanges=0, issuedNonce
  const service=createChatService({
    store,
    openExternal:async addr=>{
      const url=new URL(addr), p=url.searchParams
      authorizations++
      const host=store.load().host
      assert.equal(p.get('ext_agent_host_id'),host)
      assert.equal(p.get('client_id'),authorizations===1?'dynamic_agent_client':'registered')
      assert.equal(p.get('agent_name_hint'),authorizations===1?'Ahaki Study Viewer':null)
      assert.equal(p.get('prompt'),authorizations===1?null:'consent')
      issuedNonce=p.get('nonce')
      const back=new URL(p.get('redirect_uri'))
      back.search=new URLSearchParams({state:p.get('state'),client_id:'registered',code:'fixture'})
      await fetch(back)
    },
    fetchImpl:async(url,opts)=>{
      if(url.endsWith('/jwks.json')) return Response.json({keys:[key]})
      assert.equal(url,issuer+'/api/accounts/oauth/token')
      assert.equal(opts.body.get('client_id'),'registered')
      assert.equal(opts.body.get('code'),'fixture')
      assert.equal(opts.body.get('grant_type'),'authorization_code')
      const now=Math.floor(Date.now()/1000)
      const claims={iss:issuer,aud:'registered',nonce:issuedNonce,sub:'subject',iat:now,exp:now+300}
      const parts=[{alg:'RS256',kid:'fixture'},claims].map(v=>Buffer.from(JSON.stringify(v)).toString('base64url')).join('.')
      const id=parts+'.'+sign('RSA-SHA256',Buffer.from(parts),pair.privateKey).toString('base64url')
      exchanges++
      return Response.json({id_token:id,access_token:'fixture',refresh_token:'refresh',token_type:'Bearer',expires_in:1800,
        scope:exchanges===1?'openid':'openid '+shareScope})
    }
  })
  const first=await service.handle('signIn')
  assert.equal(first.signedIn,true)
  assert.equal(first.canAsk,false)
  await assert.rejects(service.handle('send',{problemId:'Q',prompt:'p',context:'C'}),/利用を許可/)
  const allowed =await service.handle('signIn')
  assert.equal(allowed.canAsk,true)
  assert.equal(allowed.planWelcome,true)
  assert.equal(allowed.accounts.length,1)
  assert.equal((await service.handle('ackWelcome')).planWelcome,false)
})
