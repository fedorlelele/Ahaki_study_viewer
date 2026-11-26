import assert from 'node:assert/strict'
import test from 'node:test'
import { JSDOM } from 'jsdom'
import { mountChat, shouldSendOnEnter } from '../renderer/chat.mjs'
import { renderMarkdown, spokenMarkdown } from '../renderer/chat-markdown.mjs'

const tick = () => new Promise(resolve => setImmediate(resolve))
const models = [
  { id: 'fast', name: 'すばやい回答', efforts: [] },
  { id: 'reason', name: '推論モデル', efforts: ['low', 'high'] }
]
const signed = {
  accounts: [{ id: 'A', label: '接続 1' }, { id: 'B', label: '接続 2' }], activeAccountId: 'A', signedIn: true,
  canAsk: true, busy: false, models, modelId: 'reason', effort: 'low', status: '', loadingModels: false,
  modelError: '', usageLimitReached: false, answeringProblemId: null
}
function setup(state = signed, fixtureHistory = () => []) {
  const { window } = new JSDOM('<aside id="chat-panel" hidden></aside>', { url: 'https://asv.test' })
  let snapshot = structuredClone(state), listener
  const calls = []
  const api = { chat: {
    subscribe: fn => { listener = fn },
    invoke: async (name, payload) => {
      calls.push({ name, payload })
      if (name === 'history') return fixtureHistory(payload, snapshot.activeAccountId)
      if (name === 'selectEffort') snapshot.effort = payload.effort
      if (name === 'selectModel') { snapshot.modelId = payload.id; snapshot.effort = null }
      if (name === 'selectAccount') snapshot.activeAccountId = payload.id
      return structuredClone(snapshot)
    }
  }, openExternal: url => calls.push({ name: 'link', url }) }
  const panel = window.document.querySelector('aside')
  mountChat(window, panel, api)
  const get = id => panel.querySelector('#chat-' + id)
  const emit = (name, detail) => window.dispatchEvent(new window.CustomEvent(name, { detail }))
  const problem = id => ({ question: { serial: id, subject: '解剖学' }, context: '解説 ' + id })
  const change = (id, value) => { get(id).value = value; get(id).dispatchEvent(new window.Event('change')) }
  const type = value => { get('prompt').value = value; get('prompt').dispatchEvent(new window.Event('input')) }
  const incoming = (accountId, problemId, turn) => listener({ type: 'turn', accountId, problemId, turn })
  return { window, panel, get, calls, change, type, emit, problem, incoming,
    state: next => { snapshot = next; listener({ type: 'state', state: structuredClone(next) }) } }
}

test('Markdown builds semantic DOM and cannot load remote images, frames or scripts', () => {
  const { document } = new JSDOM('').window
  const source = '## 見出し\n\n> **太字**と\\*記号\n\n1. 先\n   - 後\n\n|部位|読み|\n|---|---|\n|腹|ふく|\n\n```js\nconst n = 2 < 3;\n```\n\n[資料](https://example.org/page) [危険](javascript:alert) ![図](https://example.org/pixel) <svg onload=alert(1)>\n\nA &amp; B'
  const body = document.createElement('section')
  body.append(renderMarkdown(document, source))
  assert.equal(body.querySelector('h5').textContent, '見出し')
  assert.equal(body.querySelector('strong').textContent, '太字')
  assert.equal(body.querySelector('ol li ul li').textContent, '後')
  assert.equal(body.querySelector('th[scope="col"]').textContent, '部位')
  assert.deepEqual(Array.from(body.querySelectorAll('td'), cell => cell.textContent), ['腹', 'ふく'])
  assert.equal(body.querySelector('pre code').textContent, 'const n = 2 < 3;')
  assert.equal(body.querySelectorAll('a').length, 1)
  assert.equal(body.querySelector('a').href, 'https://example.org/page')
  assert.equal(body.querySelector('img, svg, iframe, script'), null)
  assert.match(body.textContent, /A & B/)
  const speech = spokenMarkdown(document, source)
  assert.match(speech, /腹、ふく/)
  assert.match(speech, /画像：図/)
  assert.doesNotMatch(speech, /\*\*太字|https:\/\/example.org\/pixel/)
})

test('Enter sends once, while Shift+Enter and Japanese conversion keep editor behavior', async () => {
  const { window, get, type, emit, calls, problem } = setup()
  emit('asv:chat', problem('A01-001'))
  await tick()
  const field = get('prompt')
  const keydown = options => field.dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true, ...options }))
  type('なぜですか？')
  assert.equal(keydown({ shiftKey: true }), true)
  field.dispatchEvent(new window.CompositionEvent('compositionstart'))
  assert.equal(keydown({}), true)
  field.dispatchEvent(new window.CompositionEvent('compositionend'))
  assert.equal(keydown({ keyCode: 229 }), true)
  assert.equal(keydown({}), false)
  await tick()
  assert.deepEqual(calls.filter(call => call.name === 'send'), [
    { name: 'send', payload: { problemId: 'A01-001', context: '解説 A01-001', prompt: 'なぜですか？' } }
  ])
  assert.equal(field.value, '')
  assert.equal(shouldSendOnEnter({ key: 'Enter', repeat: true }, false), false)
})

test('model controls, keyboard focus, account and question isolation and accessible Markdown', async () => {
  const { window, panel, get, type, emit, change, calls, problem, incoming } = setup()
  emit('asv:chat', problem('A01-001'))
  await tick()
  assert.equal(window.document.activeElement, get('prompt'))
  assert.equal(get('effort-select').options.length, 3)
  change('effort-select', 'high')
  await tick()
  assert.match(get('model-summary').textContent, /推論モデル · 高/)
  change('model-select', 'fast')
  await tick()
  assert.equal(get('effort-row').hidden, true)
  const turn = { id: 't', question: '質問', answer: '**理由**', state: 'complete' }
  incoming('B', 'A01-001', turn)
  incoming('A', 'A01-002', turn)
  assert.equal(get('turns').children.length, 0)
  incoming('A', 'A01-001', { ...turn, answer: '部', state: 'receiving' })
  incoming('A', 'A01-001', { ...turn, answer: '部分', state: 'receiving' })
  assert.equal(get('turns').querySelector('.chat-markdown').textContent, '部分')
  incoming('A', 'A01-001', turn)
  assert.equal(get('turns').querySelector('strong').textContent, '理由')
  let spoken
  window.addEventListener('asv:read', event => { spoken = event.detail.text })
  get('turns').querySelector('button').click()
  assert.equal(spoken, '理由')
  type('アカウントAの下書き')
  emit('asv:problem', problem('A01-002'))
  assert.equal(get('prompt').value, '')
  type('別の問題の下書き')
  emit('asv:problem', problem('A01-001'))
  assert.equal(get('prompt').value, 'アカウントAの下書き')
  change('account-select', 'B')
  await tick()
  assert.equal(get('prompt').value, '')
  assert.equal(get('turns').children.length, 0)
  change('account-select', 'A')
  await tick()
  assert.equal(get('prompt').value, 'アカウントAの下書き')
  assert.ok(calls.some(call => call.name === 'selectEffort'))
  assert.ok(!panel.textContent.includes('アカウントAの下書き'))
})

test('受信中の回答をMarkdownで表示し、続きが届いてもリンクのフォーカスと選択範囲を保つ', async () => {
  const { window, get, emit, problem, incoming } = setup()
  emit('asv:chat', problem('Q'))
  await tick()
  const receive = (answer, state = 'receiving') =>
    incoming('A', 'Q', { id: 'turn', question: '質問', answer, state })
  const initial = '## 解説\n\n[資料](https://example.org)は**重要**です'
  receive(initial)
  const body = get('turns').querySelector('.chat-markdown')
  assert.equal(body.querySelector('h5').textContent, '解説')
  const link = body.querySelector('a')
  link.focus()
  const strong = body.querySelector('strong')
  const selection = window.getSelection()
  selection.selectAllChildren(strong)
  receive(initial + '。\n\n|分類|')
  assert.equal(window.document.activeElement, link)
  assert.equal(selection.toString(), '重要')
  const complete = initial + '。\n\n|分類|\n|---|\n|答え|'
  receive(complete)
  assert.equal(body.querySelector('th').textContent, '分類')
  assert.equal(body.querySelector('td').textContent, '答え')
  receive(complete, 'complete')
  assert.equal(window.document.activeElement, link)
  assert.equal(selection.toString(), '重要')
  assert.equal(body.querySelectorAll('table').length, 1)
})

test('a delayed history fetch cannot overwrite new stream events or another problem', async () => {
  let resume
  const pending = new Promise(resolve => { resume = resolve })
  const { get, emit, problem, incoming } = setup(signed, ({ problemId }) => problemId === 'P' ? pending : [])
  emit('asv:chat', problem('P'))
  await tick()
  const initial = { id: 'r', question: '前問', answer: '', state: 'receiving' }
  incoming('A', 'P', { ...initial, answer: '完了', state: 'complete' })
  resume([initial])
  await tick()
  assert.equal(get('turns').querySelector('.chat-markdown').textContent, '完了')
  emit('asv:problem', problem('Q'))
  incoming('A', 'P', initial)
  await tick()
  assert.equal(get('turns').children.length, 0)
})
