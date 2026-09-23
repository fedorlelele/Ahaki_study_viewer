const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const AhakiQuestions = require('../web_app/shared/questions.js');
const html = fs.readFileSync('web_app/index.html', 'utf8');
function setup(extra = {}) {
  const elements = [];
  const document = { createElement(tagName) {
    const node = { tagName, children: [], listeners: {}, disabled: false,
      appendChild(child) { this.children.push(child); },
      setAttribute() {}, addEventListener(event, fn) { this.listeners[event] = fn; },
      set textContent(value) { this.text = value; this.children = []; },
      get textContent() { return this.text || ''; }
    };
    elements.push(node); return node;
  } };
  const ctx = vm.createContext({ document, AhakiQuestions, ...extra });
  const ranges = [
    ['      function formatDeepDiveLabel(', '      function buildExplanationAiReviewButton('],
    ['      function normalizeExplanationStatus(', '      function getAnswerIndices('],
    ['      function formatQaModelName(', '      function getQaDraftActorId(']
  ];
  for (const [start, end] of ranges) vm.runInContext(html.slice(html.indexOf(start), html.indexOf(end)), ctx);
  return { ctx, elements, document };
}

test('deep-dive labels use exactly the normal explanation wording for every review status', () => {
  const { ctx } = setup();
  for (const status of ['ai', 'ai_fact_checked', 'teacher_approved', 'teacher_edited']) {
    assert.equal(ctx.formatDeepDiveLabel({ model_name: 'Gemini 3 Flash', review_status: status }), ctx.formatExplanationLabel('', 'Gemini 3 Flash', status));
  }
  assert.equal(ctx.formatDeepDiveLabel(), '（Gemini 3 Flash）');
  assert.equal(ctx.formatDeepDiveLabel({ model_name: 'gemini-3.7-flash', review_status: 'ai_fact_checked' }), '（Gemini 3.7 Flash・AI検証済み）');
  assert.equal(ctx.formatDeepDiveLabel({ model_name: 'GPT5.5', review_status: 'teacher_edited' }), '（GPT5.5・教師編集済み）');
});

test('review controls toggle AI and teacher labels without changing the model or body', async () => {
  for (const [status, label, expected] of [['ai', 'AI検証済みにする', 'ai_fact_checked'], ['ai_fact_checked', 'AI検証取消', 'ai'], ['ai', '解説承認', 'teacher_approved'], ['teacher_approved', '承認取消', 'ai']]) {
    const { ctx, document, elements } = setup();
    let saved;
    ctx.renderDeepDiveReviewControls(document.createElement('div'), { review_status: status }, patch => { saved = patch; }, '既存本文');
    await elements.find(el => el.textContent === label).listeners.click();
    assert.deepEqual(JSON.parse(JSON.stringify(saved)), { review_status: expected });
  }
});

test('teacher edits mark the edited body and pending updates cannot be submitted twice', async () => {
  const { ctx, document, elements } = setup();
  const saved = [];
  let release;
  ctx.renderDeepDiveReviewControls(document.createElement('div'), { review_status: 'ai_fact_checked' }, patch => { saved.push(patch); return new Promise(r => { release = r; }); }, '既存本文');
  elements.find(el => el.tagName === 'textarea').value = '編集後';
  const save = elements.find(el => el.textContent === '解説を反映');
  const pending = save.listeners.click();
  await save.listeners.click();
  assert.equal(saved.length, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(saved[0])), { explanation: '編集後', review_status: 'teacher_edited' });
  assert.equal(save.disabled, true);
  release(); await pending;
  assert.equal(save.disabled, false);
});

test('failed or stale saves do not replace the displayed record', async () => {
  const { ctx } = setup({ window: { AI_API_BASE: 'https://worker.invalid' }, supabaseClient: { auth: { getSession: async () => ({ data: { session: { access_token: 'mock-token' } } }) } }, fetch: async () => ({ ok: false, json: async () => ({ message: '再読み込みしてください。' }) }) });
  await assert.rejects(ctx.saveDeepDiveReview('A01-001', '2026-09-23T00:00:00Z', { review_status: 'ai_fact_checked' }), /再読み込み/);
});
