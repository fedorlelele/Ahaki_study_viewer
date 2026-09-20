const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync('web_app/index.html', 'utf8');
const qaStart = html.indexOf('          const qaSection = document.createElement("div");');
const qaEnd = html.indexOf('          const actions = document.createElement("div");', qaStart);
const qaSource = html.slice(qaStart, qaEnd);
const modelSource = html.slice(html.indexOf('      function formatQaModelName('), html.indexOf('      function getQaDraftActorId('));

function setup({ draft = '', fetch, session } = {}) {
  const elements = [];
  const document = { activeElement: null };
  document.createElement = (tag) => {
    const element = {
      tagName: tag, children: [], attrs: {}, listeners: {}, hidden: false, disabled: false, value: '',
      appendChild(child) { this.children.push(child); return child; },
      setAttribute(name, value) { this.attrs[name] = String(value); },
      addEventListener(name, fn) { this.listeners[name] = fn; },
      focus() { document.activeElement = this; },
      querySelectorAll(selector) { return this.children.flatMap(child => [...(child.tagName === selector ? [child] : []), ...child.querySelectorAll(selector)]); },
      set textContent(value) { this.text = value; this.children = []; },
      get textContent() { return this.text || ''; }
    };
    elements.push(element);
    return element;
  };
  const drafts = [];
  const context = vm.createContext({
    document, q: { serial: 'A01-001', stem: '問題', choices: ['1', '2'] }, menuPanels: [],
    state: { aiPublicGeneration: true, qaLiked: new Set(), qaViewed: new Set() },
    window: { AI_API_BASE: 'https://worker.example.test' },
    canUseAiGeneration: () => true, canUseTts: () => false,
    getQaDraft: () => draft, setQaDraft: (_serial, value) => drafts.push(value), clearQaDraft: () => drafts.push(''),
    getQaLikeKey: (_serial, item) => item.id || item.question,
    getQaViewKey: (_serial, item) => item.id || item.question,
    renderMarkdownToElement: (element, text) => { element.textContent = text; },
    formatAnswerLabel: () => '1', expText: '解説',
    supabaseClient: session ? { auth: { getSession: session } } : null,
    isRoleAtLeast: () => false, announceStatusNow() {}, trackAnalyticsEvent() {},
    fetch: fetch || (async () => ({ ok: true, json: async () => ({ items: [] }) }))
  });
  vm.runInContext(`${modelSource}\n${qaSource}\nthis.qa = { qaSection, loadQaItems, renderQaItems };`, context);
  return {
    context, elements, document, drafts,
    input: elements.find(element => element.tagName === 'textarea'),
    send: elements.find(element => element.textContent === 'AIに質問する'),
    status: elements.find(element => element.attrs.role === 'status')
  };
}

test('Q&A distinguishes actual model IDs from the Gemini 3 Flash legacy default', () => {
  const { context, elements } = setup();
  for (const [model, expected] of [[undefined, 'Gemini 3 Flash'], ['Gemini3Flash', 'Gemini 3 Flash'], ['gemini-3-flash-preview', 'Gemini 3 Flash'], ['models/gemini-3.8-flash', 'Gemini 3.8 Flash'], ['gemini-3.7-flash', 'Gemini 3.7 Flash']]) {
    assert.equal(context.formatQaModelName(model), expected);
  }
  context.qa.renderQaItems([{ question: '旧質問', answer: '旧回答' }, { question: '新質問', answer: '新回答', model: 'gemini-3.7-flash' }]);
  assert.ok(elements.some(element => element.textContent === '回答モデル: Gemini 3 Flash'));
  assert.ok(elements.some(element => element.textContent === '回答モデル: Gemini 3.7 Flash'));
});

test('a suggestion fills a draft without spending a generation request', () => {
  let requests = 0;
  const ui = setup({ fetch: async () => { requests++; } });
  assert.equal(ui.input.hidden, false);
  assert.equal(ui.send.disabled, true);
  ui.elements.find(element => element.textContent === '選択肢の違い').listeners.click();
  assert.match(ui.input.value, /各選択肢の違い/);
  assert.equal(ui.send.disabled, false);
  assert.equal(ui.document.activeElement, ui.input);
  assert.equal(ui.drafts.at(-1), ui.input.value);
  assert.equal(requests, 0);
});

test('one pending question sends once and shows its returned answer/model without a second fetch', async () => {
  let releaseSession;
  const requests = [];
  const ui = setup({
    draft: '違いは？',
    session: () => new Promise(resolve => { releaseSession = resolve; }),
    fetch: async (url, options) => {
      requests.push({ url, body: JSON.parse(options.body) });
      return { ok: true, json: async () => ({ status: 'ok', model: 'gemini-3.7-flash', fallback_used: true, item: { id: 'new-id', question: '違いは？', answer: 'ここが違います。', model: 'gemini-3.7-flash' } }) };
    }
  });
  const pending = ui.send.listeners.click();
  await ui.send.listeners.click();
  assert.equal(ui.input.disabled, true);
  releaseSession({ data: { session: null } });
  await pending;
  assert.equal(requests.length, 1);
  assert.equal('model' in requests[0].body, false, 'the server selects the free model');
  assert.match(ui.status.textContent, /Gemini 3\.7 Flash.*自動切り替え/);
  const answer = ui.elements.find(element => element.className === 'note qa-answer');
  assert.equal(answer.textContent, 'ここが違います。');
  assert.equal(answer.hidden, false);
  assert.equal(ui.input.value, '');
  assert.equal(ui.input.disabled, false);
  assert.equal(ui.send.disabled, true);
});

test('quota exhaustion keeps the question editable for retry', async () => {
  const ui = setup({ draft: '覚え方は？', fetch: async () => ({ ok: false, status: 429, json: async () => ({ message: 'Gemini API rate limit' }) }) });
  await ui.send.listeners.click();
  assert.equal(ui.input.value, '覚え方は？');
  assert.equal(ui.input.disabled, false);
  assert.equal(ui.send.disabled, false);
  assert.match(ui.status.textContent, /無料枠の利用上限/);
  assert.equal(ui.drafts.length, 0, 'the saved draft is not cleared');
});

test('a delayed history response cannot erase a question generated after the history request began', async () => {
  for (const historyOk of [true, false]) {
    let releaseHistory;
    const oldItem = { id: 'old-id', question: '以前の質問', answer: '以前の回答' };
    const newItem = { id: 'new-id', question: '新しい質問', answer: '生成した回答', model: 'gemini-3.8-flash' };
    const ui = setup({ draft: newItem.question, fetch: async (_url, options) => {
      if (!options) return new Promise(resolve => { releaseHistory = resolve; });
      return { ok: true, json: async () => ({ status: 'ok', model: newItem.model, item: newItem }) };
    } });
    ui.context.q.qa_list = [oldItem];
    const pendingHistory = ui.context.qa.loadQaItems();
    await ui.send.listeners.click();
    releaseHistory({ ok: historyOk, json: async () => ({ items: [oldItem] }) });
    await pendingHistory;
    const answers = ui.context.qa.qaSection.querySelectorAll('div').filter(element => element.className === 'note qa-answer');
    assert.deepEqual(answers.map(element => element.textContent).sort(), ['以前の回答', '生成した回答'].sort());
    assert.equal(answers.find(element => element.textContent === newItem.answer).hidden, false);
    assert.match(ui.status.textContent, /Gemini 3\.8 Flashで回答/);
  }
});
