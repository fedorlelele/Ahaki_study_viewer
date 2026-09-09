const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const Q = require('../web_app/shared/questions.js');
const Study = require('../web_app/shared/study-tools.js');
const html = fs.readFileSync(require.resolve('../web_app/simple.html'), 'utf8');

function functionSource(name) {
  const match = new RegExp('^([\\t ]+)(?:async )?function ' + name + '\\(', 'm').exec(html);
  assert.ok(match, `missing simple function ${name}`);
  const closing = /^[\t ]+}/gm;
  closing.lastIndex = match.index + match[0].length;
  let end;
  while ((end = closing.exec(html))) {
    const source = html.slice(match.index, end.index + end[0].length);
    try { new vm.Script(source); return source; } catch (_) {}
  }
  throw Error(`missing function end ${name}`);
}
function addFunctions(ctx, names) {
  for (const name of names) vm.runInContext(functionSource(name), ctx);
}
function element() {
  const classes = new Set();
  return { textContent: '', hidden: false, disabled: false, checked: false, children: [], dataset: {},
    classList: { add: name => classes.add(name), remove: name => classes.delete(name), contains: name => classes.has(name) },
    focus() {}, setAttribute() {}, querySelectorAll() { return this.children; }, querySelector() { return this.children[0]; }
  };
}
const a = { serial: 'A01-001', stem: '問題A', choices: ['A1', 'A2'], answer_indices: [1], explanation_latest: 'Aの解説', case_text: '症例A' };
const b = { serial: 'A01-002', stem: '問題B', choices: ['B1', 'B2'], answer_indices: [2], explanation_latest: 'Bの解説' };
const orderQuestions = Study.orderQuestions;
function sessionContext(incoming, saved = null) {
  const data = new Map(saved ? [['session', JSON.stringify(saved)]] : []);
  const ctx = vm.createContext({ AhakiQuestions: Q, AhakiStudy: { readHandoff: () => incoming, orderQuestions },
    SIMPLE_SESSION_KEY: 'session', localStorage: { getItem: key => data.get(key), setItem: (key, value) => data.set(key, value), removeItem: key => data.delete(key) },
    state: { questions: [a, b], filtered: [], currentIndex: 0, activeSessionSerials: [], sessionResults: [] },
    studySetInfo: element(), questionSection: element(), confirmBtn: element(), questionRenderGuard: Q.createRenderGuard(),
    getCurrentFilters: () => ({}), applyFilters() {}, updateResumeControls() {}, updateJumpOptions() {},
    filterQuestions() { throw Error('must not substitute search results'); },
    renderQuestion() { ctx.renders++; ctx.questionSection.hidden = false; },
    showStep(step) { ctx.step = step; }, setStatus(message) { ctx.message = message; }, renders: 0,
  });
  addFunctions(ctx, ['loadSavedSession', 'clearSavedSession', 'saveSimpleSession', 'updateStudySetInfo', 'showStudySetError', 'loadIncomingStudySet', 'resumeSavedSession']);
  ctx.data = data;
  return ctx;
}

test('simple handoff keeps selected order and persists position beyond token lifetime', () => {
  const bundle = { title: '生理学の検索結果', serials: [b.serial, a.serial], createdAt: 12345 };
  const ctx = sessionContext({ status: 'ready', bundle });
  assert.equal(ctx.loadIncomingStudySet('?studySet=test'), true);
  assert.deepEqual(Array.from(ctx.state.filtered, q => q.serial), [b.serial, a.serial]);
  assert.equal(ctx.renders, 1);
  assert.match(ctx.studySetInfo.textContent, /2問・元の並び順/);
  ctx.state.currentIndex = 1;
  ctx.saveSimpleSession();
  const saved = JSON.parse(ctx.data.get('session'));
  assert.equal(saved.currentSerial, a.serial);
  const resumed = sessionContext({ status: 'error', message: '期限切れ' }, saved);
  resumed.resumeSavedSession();
  assert.deepEqual(Array.from(resumed.state.filtered, q => q.serial), [b.serial, a.serial]);
  assert.equal(resumed.state.currentIndex, 1);
  assert.equal(resumed.state.studySet.title, bundle.title);
  const reloaded = sessionContext({ status: 'ready', bundle }, saved);
  reloaded.loadIncomingStudySet('?studySet=test');
  assert.equal(reloaded.state.currentIndex, 1, 'reloading the same transfer resumes rather than overwriting progress');
});

test('invalid and missing handoffs never open all or a partial set of questions', () => {
  for (const incoming of [
    { status: 'error', message: '引き継ぎの有効期限が切れています。' },
    { status: 'error', message: '引き継いだ問題が見つかりません。' },
    { status: 'ready', bundle: { serials: [b.serial, 'B99-999'], title: '検索結果', createdAt: 1 } },
  ]) {
    const ctx = sessionContext(incoming);
    ctx.loadIncomingStudySet('?studySet=bad');
    assert.equal(ctx.renders, 0);
    assert.equal(ctx.state.filtered.length, 0);
    assert.equal(ctx.confirmBtn.disabled, true);
    assert.equal(ctx.questionSection.hidden, true);
    assert.ok(ctx.message);
    assert.equal(ctx.step, 'guide');
  }
  const ctx = sessionContext({ status: 'none' });
  assert.equal(ctx.loadIncomingStudySet(''), false);
  assert.equal(ctx.renders, 0);
});

test('resume rejects incomplete transferred sets without silently broadening or dropping questions', () => {
  for (const serials of [[a.serial, 'B99-999'], [42], []]) {
    const ctx = sessionContext({ status: 'none' }, { filters: {}, currentIndex: 0, activeSessionSerials: serials, studySet: { title: '元の検索結果' } });
    ctx.resumeSavedSession();
    assert.equal(ctx.renders, 0);
    assert.equal(ctx.state.filtered.length, 0);
    assert.match(ctx.message, /復元できません/);
  }
});

test('simple copy includes only explicitly chosen content and snapshots choice and order while loading', async () => {
  let mode = { showAnswer: false, showExplanation: false };
  let release, copied;
  const ctx = vm.createContext({ AhakiQuestions: Q, AhakiStudy: { getCopyMode: () => mode }, state: { filtered: [b, a] },
    applyOverridesToQuestion: q => q, buildFilterHeader: () => '検索結果', ensureOverridesLoaded: () => new Promise(resolve => { release = resolve; }),
    navigator: { clipboard: { writeText: async text => { copied = text; } } }, setStatus() {}
  });
  addFunctions(ctx, ['formatAnswerLabel', 'formatQuestionForCopy', 'copySearchResults']);
  assert.doesNotMatch(ctx.formatQuestionForCopy(a), /解答|解説/);
  assert.match(ctx.formatQuestionForCopy(a), /症例A/);
  mode = { showAnswer: true, showExplanation: false };
  assert.match(ctx.formatQuestionForCopy(a), /解答　1/);
  assert.doesNotMatch(ctx.formatQuestionForCopy(a), /解説/);
  mode = { showAnswer: true, showExplanation: true };
  assert.match(ctx.formatQuestionForCopy(a), /Aの解説/);
  mode = { showAnswer: false, showExplanation: false };
  const pending = ctx.copySearchResults();
  ctx.state.filtered = [a];
  mode = { showAnswer: true, showExplanation: true };
  release();
  await pending;
  assert.ok(copied.indexOf(b.serial) < copied.indexOf(a.serial));
  assert.doesNotMatch(copied, /解答|解説/);
});

test('optional confirmation keeps selection validation, visible-question grading and duplicate lock', () => {
  const records = [];
  const ctx = vm.createContext({ AhakiQuestions: Q, state: { displayedQuestion: b, filtered: [a], selectedIndex: null, answered: false },
    skipAnswerConfirmation: element(), document: { activeElement: null }, confirmBtn: element(), confirmChoice: element(), confirmOverlay: element(), confirmYes: element(),
    choices: element(), resultOverlay: element(), resultBack: element(), resultStatus: element(), resultText: element(), status: element(),
    setStatus(message) { ctx.message = message; }, setResultButtonsEnabled() {}, announce() {},
    markAnswered: (q, selected, correct) => records.push({ serial: q.serial, selected, correct }), getAnonId: () => '', supabaseClient: null, setTimeout() {},
  });
  addFunctions(ctx, ['openConfirm', 'showResult', 'getAnswerIndices', 'isAnswerCorrect', 'formatAnswerLabel']);
  ctx.openConfirm();
  assert.match(ctx.message, /選択肢を選んで/);
  ctx.state.selectedIndex = 2;
  ctx.openConfirm();
  assert.equal(ctx.confirmOverlay.classList.contains('show'), true, 'default still asks for confirmation');
  assert.equal(records.length, 0);
  ctx.confirmOverlay.classList.remove('show');
  ctx.skipAnswerConfirmation.checked = true;
  ctx.confirmBtn.disabled = true;
  ctx.openConfirm();
  assert.equal(records.length, 0, 'pending display cannot be answered');
  ctx.confirmBtn.disabled = false;
  ctx.openConfirm(); ctx.openConfirm(); ctx.showResult();
  assert.equal(ctx.confirmOverlay.classList.contains('show'), false);
  assert.deepEqual(records, [{ serial: b.serial, selected: 2, correct: true }]);
});

test('question Enter and confirm button share the same optional-confirmation path; controls retain native keys', () => {
  let listener;
  const active = { tagName: 'LI', dataset: { index: '1' } };
  const ctx = vm.createContext({ state: { focusItems: [active], answered: false, focusIndex: 0 }, questionSection: { hidden: false },
    document: { activeElement: active, addEventListener: (_, handler) => { listener = handler; } },
    openConfirm: () => { ctx.confirmed++; }, setFocusIndex() {}, confirmed: 0
  });
  const start = html.indexOf('      document.addEventListener("keydown", (e) => {');
  const end = html.indexOf('      confirmBtn.addEventListener("click", openConfirm);', start);
  vm.runInContext(html.slice(start, end), ctx);
  listener({ key: 'Enter', preventDefault() {} });
  assert.equal(ctx.confirmed, 1);
  ctx.document.activeElement = { tagName: 'SELECT' };
  listener({ key: 'ArrowDown', preventDefault() { throw Error('select arrow was intercepted'); } });
  ctx.document.activeElement = { tagName: 'INPUT' };
  listener({ key: 'Enter', preventDefault() { throw Error('setting keyboard event was intercepted'); } });
  assert.equal(ctx.confirmed, 1);
  assert.match(html, /confirmBtn\.addEventListener\("click", openConfirm\)/);
});
