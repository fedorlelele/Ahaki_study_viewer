import { renderMarkdown, spokenMarkdown } from './chat-markdown.mjs'

const efforts = { none: 'なし', minimal: '最小', low: '低', medium: '中', high: '高', xhigh: '非常に高い', max: '最大' }
const blank = { accounts: [], activeAccountId: null, signedIn: false, canAsk: false, busy: false,
  models: [], modelId: null, effort: null, loadingModels: false, modelError: '', status: '', answeringProblemId: null }

export function shouldSendOnEnter(event, composing) {
  return event.key === 'Enter' && !event.shiftKey && !event.ctrlKey && !event.altKey && !event.metaKey &&
    !composing && !event.isComposing && event.keyCode !== 229 && !event.repeat
}

function errorMessage(error) {
  return String(error?.message || error || '操作を完了できませんでした。').replace(/^Error invoking remote method '[^']+': (?:Error: )?/, '')
}

// Preserve the reader's selection and focused links as new Markdown arrives.
// A later delimiter can also change the type of an earlier node (e.g. a table header).
function updateAnswerNodes(parent, rendered) {
  const old = Array.from(parent.childNodes)
  const next = Array.from(rendered.childNodes)
  for (const [index, node] of next.entries()) {
    const previous = old[index]
    if (!previous) parent.append(node)
    else if (!previous.isEqualNode(node)) {
      if (previous.nodeType === 3 && node.nodeType === 3 && node.data.startsWith(previous.data)) {
        previous.appendData(node.data.slice(previous.data.length))
      } else if (previous.nodeType === 1 && previous.cloneNode(false).isEqualNode(node.cloneNode(false))) {
        updateAnswerNodes(previous, node)
      } else previous.replaceWith(node)
    }
  }
  for (const removed of old.slice(next.length)) removed.remove()
}

// The main process owns tokens, conversations and in-flight requests. This view keeps
// unsent text in memory and displays only the current question and registration.
export function mountChat(window, panel, api) {
  const document = window.document
  panel.innerHTML = `
    <header class="chat-head">
      <div><h2>ChatGPTに質問</h2><p id="chat-question-id"></p></div>
      <button id="chat-close" type="button" aria-label="チャットを閉じる" title="チャットを閉じる">閉じる</button>
    </header>
    <div class="chat-session">
      <details id="chat-account"><summary>アカウント</summary>
        <label id="chat-account-row">接続先<select id="chat-account-select"></select></label>
        <div class="chat-actions"><button id="chat-add" type="button">別のアカウントを追加</button>
          <button id="chat-signout" type="button">サインアウト</button></div>
      </details>
      <div id="chat-login"><p>質問には、対応するChatGPTプランでのサインインと利用の許可が必要です。</p>
        <button id="chat-signin" type="button">Continue with ChatGPT</button></div>
      <div id="chat-welcome" hidden><p>このアプリの質問はChatGPTプランの利用量に含まれます。</p>
        <button id="chat-welcome-ok" type="button">確認</button></div>
      <button id="chat-usage" type="button" class="chat-text-button">ChatGPTの利用状況を確認</button>
      <button id="chat-cancel-login" type="button" hidden>接続処理を中止</button>
      <p id="chat-state-message" role="status"></p>
    </div>
    <section id="chat-transcript" tabindex="0" aria-label="この問題の会話">
      <p id="chat-privacy">送信すると、問題文・選択肢・正答・解説と、この問題の会話をOpenAIに送ります。</p>
      <div id="chat-turns"></div>
    </section>
    <div class="chat-composer">
      <details id="chat-models" hidden><summary id="chat-model-summary">モデルとエフォート</summary>
        <label>回答モデル<select id="chat-model-select"></select></label>
        <label id="chat-effort-row">エフォート（推論の強さ）<select id="chat-effort-select"></select></label>
        <p id="chat-effort-fixed" hidden>このモデルではエフォートを選択できません。</p>
        <p id="chat-model-error" role="status"></p>
        <button id="chat-model-refresh" type="button">モデル一覧を更新</button>
      </details>
      <label for="chat-prompt">質問を入力</label><textarea id="chat-prompt" rows="2" aria-describedby="chat-prompt-help"></textarea>
      <div class="chat-actions chat-submit-row"><small id="chat-prompt-help">Enterで送信・Shift＋Enterで改行</small>
        <button id="chat-send" type="button">送信</button><button id="chat-interrupt" type="button" hidden>回答を中止</button></div>
      <button id="chat-stop-speech" type="button" hidden>読み上げを停止</button>
      <p id="chat-notice" role="status"></p>
    </div>`
  const get = id => panel.querySelector('#chat-' + id)
  const prompt = get('prompt')
  const announce = (message, screenReaderOnly = false) => {
    const notice = get('notice')
    notice.classList.toggle('visually-hidden', screenReaderOnly)
    notice.textContent = message
  }
  const emit = (type, detail) => window.dispatchEvent(new window.CustomEvent(type, { detail }))
  const invoke = (name, payload = {}) => api.chat.invoke(name, payload)
  let snapshot = blank, question = null, context = '', initialized = false, expanded = false
  let composing = false, currentKey, historyGeneration = 0, stateVersion = 0, catalogAccount = null
  let activeHistory = null, sendingKey = null, pendingSignIn = null
  const drafts = new Map()
  const key = id => JSON.stringify([snapshot.signedIn, snapshot.activeAccountId, id])
  const account = () => snapshot.signedIn ? snapshot.activeAccountId : null
  const followBottom = () => {
    const region = get('transcript')
    return region.scrollHeight - region.scrollTop - region.clientHeight <= 32
  }
  const scrollLatest = () => { const region = get('transcript'); region.scrollTop = region.scrollHeight }
  const stopReading = () => {
    if (!get('stop-speech').hidden) {
      emit('asv:stop')
      get('stop-speech').hidden = true
    }
  }

  function options(select, choices, selected) {
    const old = Array.from(select.options)
    if (old.length !== choices.length || old.some((opt, i) => opt.value !== choices[i][0] || opt.textContent !== choices[i][1])) {
      select.replaceChildren(...choices.map(([value, title]) => new window.Option(title, value)))
    }
    select.value = selected ?? ''
  }

  function updateButtons() {
    get('send').hidden = !!snapshot.answeringProblemId
    get('send').disabled = !snapshot.canAsk || !question || !prompt.value.trim() || snapshot.busy || sendingKey !== null
    get('interrupt').hidden = !snapshot.answeringProblemId
    if (snapshot.answeringProblemId) {
      get('interrupt').textContent = snapshot.answeringProblemId === question?.serial ? '回答を中止' : `${snapshot.answeringProblemId} の回答を中止`
    }
  }

  function showState(next) {
    if (!next || !Array.isArray(next.accounts)) return
    const oldAccount = account()
    snapshot = next
    initialized = true
    if (oldAccount !== account()) catalogAccount = null
    get('account').hidden = !next.accounts.length
    get('account-row').hidden = !next.accounts.length
    options(get('account-select'), next.accounts.map(profile => [profile.id, profile.label]), next.activeAccountId)
    get('account-select').disabled = next.busy
    get('add').disabled = next.busy
    get('signout').hidden = !next.signedIn
    get('signout').disabled = next.busy
    get('login').hidden = next.canAsk
    get('signin').disabled = next.busy
    get('welcome').hidden = !next.planWelcome
    get('usage').hidden = !next.canAsk && !next.usageLimitReached
    get('usage').textContent = next.canAsk ? 'プラン使用中' : '利用状況'
    get('usage').setAttribute('aria-label', 'ChatGPTのプラン利用状況を確認')
    get('usage').title = 'ChatGPTの利用状況を確認'
    get('cancel-login').hidden = !next.busy || !!next.answeringProblemId
    get('state-message').textContent = next.status || ''
    get('models').hidden = !next.canAsk
    const model = next.models.find(item => item.id === next.modelId)
    const effort = efforts[next.effort] || next.effort || '自動'
    const summary = model ? `${model.name}${model.efforts.length ? ' · ' + effort : ''}` :
      next.loadingModels ? 'モデルを読み込み中…' : next.modelError ? 'モデル一覧を確認' : 'モデルとエフォート'
    get('model-summary').textContent = summary
    get('model-summary').setAttribute('aria-label', 'モデルとエフォート　' + summary)
    options(get('model-select'), next.models.map(item => [item.id, item.name]), next.modelId)
    get('model-select').title = model?.name || '回答モデル'
    get('model-select').disabled = next.busy || next.loadingModels || !next.models.length
    get('effort-row').hidden = !model?.efforts.length
    get('effort-fixed').hidden = !model || !!model.efforts.length
    options(get('effort-select'), [['', '自動'], ...(model?.efforts || []).map(id => [id, efforts[id] || id])], next.effort)
    get('effort-select').disabled = next.busy || next.loadingModels
    get('model-error').textContent = next.modelError || ''
    get('model-refresh').disabled = next.busy || next.loadingModels
    get('model-refresh').textContent = next.modelError ? 'モデル一覧を再読み込み' : 'モデル一覧を更新'
    if (expanded) {
      switchThread()
      if (next.canAsk && catalogAccount !== account()) {
        catalogAccount = account()
        void act('models', { force: false })
      }
    }
    updateButtons()
  }

  async function act(name, payload = {}) {
    const version = stateVersion
    try {
      const next = await invoke(name, payload)
      if (version === stateVersion) showState(next)
      return true
    } catch (error) {
      announce(errorMessage(error))
      return false
    }
  }

  const views = new Map()
  function showTurn(turn) {
    let view = views.get(turn.id)
    if (!view) {
      const article = document.createElement('article')
      article.className = 'chat-turn'
      article.innerHTML = `<div class="chat-user"><h3>あなた</h3><p></p></div>
        <div class="chat-response"><h3>ChatGPT</h3><div class="chat-markdown"></div>
          <p class="chat-turn-status"></p><div class="chat-actions"></div></div>`
      article.querySelector('.chat-user p').textContent = turn.question
      view = { article, body: article.querySelector('.chat-markdown'), status: article.querySelector('.chat-turn-status'),
        buttons: article.querySelector('.chat-response .chat-actions'), turn: null }
      views.set(turn.id, view)
      get('turns').append(article)
    }
    const last = view.turn
    view.turn = turn
    if (last?.answer !== turn.answer) updateAnswerNodes(view.body, renderMarkdown(document, turn.answer))
    if (last?.state !== turn.state) {
      view.status.textContent = turn.state === 'interrupted' ? turn.error || '回答を中止しました。' : turn.state === 'receiving' && !turn.answer ? '回答を作成中…' : ''
      view.buttons.replaceChildren()
      if (turn.state === 'complete' || turn.state === 'interrupted') {
        const button = document.createElement('button')
        button.type = 'button'
        if (turn.state === 'complete') {
          button.textContent = '回答を読み上げる'
          button.addEventListener('click', () => {
            emit('asv:read', { text: spokenMarkdown(document, view.turn.answer) })
            get('stop-speech').hidden = false
          })
        } else {
          button.textContent = '質問を入力欄に戻す'
          button.addEventListener('click', () => {
            if (prompt.value) { announce('入力欄に書きかけの質問があります。'); prompt.focus(); return }
            prompt.value = turn.question
            saveDraft()
            prompt.focus()
          })
        }
        view.buttons.append(button)
      }
    } else if (turn.state === 'receiving' && turn.answer) view.status.textContent = ''
  }

  async function switchThread(force = false) {
    const id = question?.serial
    const nextKey = key(id)
    if (nextKey === currentKey && !force) return
    if (currentKey !== nextKey) {
      stopReading()
      prompt.value = drafts.get(nextKey) || ''
      announce('')
    }
    currentKey = nextKey
    views.clear()
    get('turns').replaceChildren()
    get('privacy').hidden = false
    get('question-id').textContent = question ? [id, question.subject].filter(Boolean).join('　') : '問題を選んでください。'
    const generation = ++historyGeneration
    const received = new Map()
    activeHistory = received
    updateButtons()
    if (!id || !initialized || !snapshot.signedIn) { activeHistory = null; return }
    try {
      const history = await invoke('history', { problemId: id })
      if (generation !== historyGeneration) return
      const ordered = new Map(history.map(turn => [turn.id, turn]))
      for (const [id, turn] of received) ordered.set(id, turn)
      views.clear()
      get('turns').replaceChildren()
      for (const turn of ordered.values()) showTurn(turn)
      get('privacy').hidden = ordered.size > 0
      scrollLatest()
    } catch (error) {
      if (generation === historyGeneration) announce(errorMessage(error))
    } finally {
      if (generation === historyGeneration) activeHistory = null
    }
  }

  function saveDraft() {
    if (currentKey) {
      if (prompt.value) drafts.set(currentKey, prompt.value)
      else drafts.delete(currentKey)
    }
    updateButtons()
  }

  async function send() {
    if (!snapshot.canAsk) { announce('質問するにはChatGPTプランの利用を許可してください。'); return }
    if (!question || snapshot.busy || sendingKey !== null || !prompt.value.trim()) return
    const original = prompt.value
    const originalKey = currentKey
    sendingKey = originalKey
    updateButtons()
    stopReading()
    // Clear immediately so another problem/account can be opened while IPC is pending.
    prompt.value = ''
    saveDraft()
    const accepted = await act('send', { problemId: question.serial, context, prompt: original.trim() })
    if (!accepted && !drafts.has(originalKey)) {
      drafts.set(originalKey, original)
      if (currentKey === originalKey) prompt.value = original
    }
    sendingKey = null
    updateButtons()
  }

  function setProblem(detail) {
    if (!detail) return
    question = detail.question
    context = detail.context || ''
    if (expanded) void switchThread()
  }
  async function open(detail) {
    if (detail) setProblem(detail)
    panel.hidden = false
    expanded = true
    void switchThread()
    prompt.focus()
    if (!initialized) await act('state')
    else showState(snapshot)
  }
  function endView() {
    expanded = false
    activeHistory = null
    ++historyGeneration
    currentKey = undefined
    stopReading()
  }
  function close() {
    endView()
    emit('asv:close-chat')
  }
  async function signIn(newAccount) {
    if (pendingSignIn) return
    const from = !snapshot.signedIn && question ? { key: currentKey, id: question.serial } : null
    pendingSignIn = { from }
    try {
      if (await act('signIn', { newAccount }) && from && account()) {
        const originalDraft = drafts.get(from.key)
        const target = key(from.id)
        if (originalDraft && !drafts.has(target)) {
          drafts.set(target, originalDraft)
          drafts.delete(from.key)
          if (currentKey === target) prompt.value = originalDraft
          updateButtons()
        }
      }
    } finally { pendingSignIn = null }
  }

  api.chat.subscribe(event => {
    if (event.type === 'state') { ++stateVersion; showState(event.state); return }
    if (event.type !== 'turn' || !expanded || event.accountId !== account() || event.problemId !== question?.serial) return
    const turn = event.turn
    const follow = followBottom()
    if (activeHistory) activeHistory.set(turn.id, turn)
    showTurn(turn)
    get('privacy').hidden = true
    if (follow) {
      if (turn.state === 'complete') {
        const region = get('transcript')
        const answer = views.get(turn.id).article.querySelector('.chat-response')
        region.scrollTop += answer.getBoundingClientRect().top - region.getBoundingClientRect().top - 8
      } else scrollLatest()
    }
    if (turn.state === 'complete') announce('ChatGPTの回答を受信しました。', true)
    if (turn.state === 'interrupted') announce(turn.error || '回答を中止しました。')
  })
  window.addEventListener('asv:problem', event => setProblem(event.detail))
  window.addEventListener('asv:chat', event => void open(event.detail))
  window.addEventListener('asv:chat-closed', () => { if (expanded) endView() })
  get('close').addEventListener('click', close)
  get('signin').addEventListener('click', () => void signIn(false))
  get('add').addEventListener('click', () => void signIn(true))
  get('account-select').addEventListener('change', event => void act('selectAccount', { id: event.target.value }))
  get('signout').addEventListener('click', () => void act('signOut'))
  get('usage').addEventListener('click', () => void act('usage'))
  get('welcome-ok').addEventListener('click', () => void act('ackWelcome'))
  get('cancel-login').addEventListener('click', () => void act('cancel'))
  get('model-select').addEventListener('change', event => void act('selectModel', { id: event.target.value }))
  get('effort-select').addEventListener('change', event => void act('selectEffort', { effort: event.target.value || null }))
  get('model-refresh').addEventListener('click', () => void act('models', { force: true }))
  get('send').addEventListener('click', () => void send())
  get('interrupt').addEventListener('click', () => void act('cancel'))
  get('stop-speech').addEventListener('click', stopReading)
  prompt.addEventListener('input', saveDraft)
  prompt.addEventListener('compositionstart', () => { composing = true })
  prompt.addEventListener('compositionend', () => { composing = false })
  prompt.addEventListener('keydown', event => {
    if (event.defaultPrevented || !shouldSendOnEnter(event, composing)) return
    event.preventDefault()
    void send()
  })
  panel.addEventListener('keydown', event => {
    if (event.key === 'Escape' && !event.isComposing && !composing && event.target.tagName !== 'SELECT') {
      event.stopPropagation()
      event.preventDefault()
      close()
    }
  })
  get('turns').addEventListener('click', event => {
    const link = event.target.closest?.('a')
    if (!link) return
    event.preventDefault()
    void Promise.resolve(api.openExternal(link.href)).catch(error => announce(errorMessage(error)))
  })
  emit('asv:chat-ready')
}

if (typeof window !== 'undefined' && window.asv?.chat && document.getElementById('chat-panel')) {
  mountChat(window, document.getElementById('chat-panel'), window.asv)
}
