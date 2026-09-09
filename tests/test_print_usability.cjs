const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const Q = require('../web_app/shared/questions.js');
const html = fs.readFileSync(require('node:path').join(__dirname, '../web_app/print_export.html'), 'utf8');

function source(name) {
  const start = new RegExp('^([\\t ]+)(?:async )?function ' + name + '\\(', 'm').exec(html);
  assert.ok(start, name);
  const close = /^[\t ]+}/gm;
  close.lastIndex = start.index + start[0].length;
  let end;
  while ((end = close.exec(html))) {
    const text = html.slice(start.index, end.index + end[0].length);
    try { new vm.Script(text); return text; } catch (_) { /* nested block */ }
  }
  throw new Error('Missing function end: ' + name);
}
function functions(ctx, names) { names.forEach(name => vm.runInContext(source(name), ctx)); }
function element() {
  return { value: '', checked: false, hidden: false, textContent: '', open: false, attrs: {},
    setAttribute(name, value) { this.attrs[name] = value; } };
}
function context(bundle = { status: 'none' }) {
  const els = Object.fromEntries([
    'keywordInput', 'subjectSelect', 'subtopicSelect', 'examTypeSelect', 'sessionFrom', 'sessionTo',
    'serialFrom', 'serialTo', 'sortSelect', 'numberingSelect', 'includeAnswer', 'includeExplanation',
    'includeDeepDive', 'handoffNotice', 'handoffMessage', 'handoffSortOption', 'textPreviewDetails',
    'textPreviewStatus', 'textPreviewContent', 'retryTextPreviewBtn',
  ].map(name => [name, element()]));
  els.sortSelect.value = 'serial_asc';
  els.numberingSelect.value = 'serial';
  const window = { location: { href: 'https://example.test/print_export.html?studySet=abc&keep=1', search: '?studySet=abc&keep=1' }, history: { replaceState(_state, _title, value) { this.url = value; } } };
  const ctx = vm.createContext({
    AhakiQuestions: Q, URL, window, els, console,
    collator: new Intl.Collator('ja'),
    state: { questions: [], filtered: [], handoff: { status: 'none' }, handoffQuestions: [], ready: true, textPreviewGeneration: 0 },
    AhakiStudy: {
      readHandoff: () => bundle,
      orderQuestions: (questions, serials) => {
        const bySerial = new Map(questions.map(q => [q.serial, q]));
        return { questions: serials.filter(id => bySerial.has(id)).map(id => bySerial.get(id)), missingSerials: serials.filter(id => !bySerial.has(id)) };
      },
    },
    updateSubtopicOptions() { els.subtopicSelect.value = ''; },
    applyFilters() { ctx.state.filtered = ctx.filterQuestions(); },
    supabaseClient: {}, ensureDeepDiveContentLoaded: async () => {},
    getDeepDiveText: q => q.deep_dive?.explanation || '',
  });
  functions(ctx, ['normalizeText', 'normalizeSerialInput', 'parseSerialParts', 'isValidSerialParts',
    'compareSerialValue', 'compareBySession', 'compareBySubjectSerial', 'buildSearchText', 'preprocessQuestions',
    'getKeywordTerms', 'matchesKeyword', 'getNumberOrNull', 'initializeStudyHandoff', 'renderStudyHandoff',
    'clearStudyHandoff', 'filterQuestions', 'resetControls', 'buildFilterSummary', 'getSubtopicGroups',
    'getSortLabel', 'getExportMode', 'formatAnswerLabel', 'buildQuestionBlock', 'updateTextPreview']);
  return ctx;
}
function row(i, extra = {}) {
  return { serial: `A01-${String(i).padStart(3, '0')}`, subject: '生理学', exam_type: 'あん摩マッサージ指圧師', exam_session: 1,
    case_text: '症例の全文', stem: `問題文${i}`, choices: ['選択肢甲', '選択肢乙'], answer_indices: [2],
    subtopics: ['筋'], explanation_latest: '通常解説の全文', ...extra };
}

test('print transfer preserves every result and original order beyond the 40-row preview, including ZIP groups', () => {
  const rows = Array.from({ length: 90 }, (_, i) => row(i + 1));
  const serials = rows.slice(5, 88).reverse().map(q => q.serial);
  const ctx = context({ status: 'ready', bundle: { serials, title: '生理学・検索結果', createdAt: Date.now() } });
  ctx.state.questions = ctx.preprocessQuestions(rows);
  ctx.initializeStudyHandoff();
  ctx.state.filtered = ctx.filterQuestions();
  assert.equal(ctx.els.sortSelect.value, 'handoff');
  assert.deepEqual(Array.from(ctx.state.filtered, q => q.serial), serials);
  assert.match(ctx.els.handoffMessage.textContent, /83 問/);
  assert.match(ctx.buildFilterSummary(), /生理学・検索結果.*並び順: 検索結果の順/);
  ctx.els.subjectSelect.value = '生理学';
  assert.deepEqual(Array.from(ctx.getSubtopicGroups()[0].questions, q => q.serial), serials);
  ctx.els.keywordInput.value = '問題文8';
  assert.deepEqual(Array.from(ctx.filterQuestions(), q => q.serial), serials.filter(id => /008$|08[0-9]$/.test(id)));
  ctx.els.sortSelect.value = 'serial_asc';
  ctx.resetControls();
  assert.deepEqual(Array.from(ctx.state.filtered, q => q.serial), serials);
});

test('invalid, expired, or partially missing transfers remain blocked until explicitly cleared', () => {
  for (const bundle of [{ status: 'error', message: '引き継ぎの期限が切れました。' },
    { status: 'ready', bundle: { serials: ['A01-001', 'A01-999'], title: '検索結果' } }]) {
    const ctx = context(bundle);
    ctx.state.questions = ctx.preprocessQuestions([row(1), row(2)]);
    ctx.initializeStudyHandoff();
    assert.equal(ctx.state.handoff.status, 'error');
    assert.equal(ctx.filterQuestions().length, 0);
    ctx.resetControls();
    assert.equal(ctx.state.filtered.length, 0, 'ordinary condition reset must not escape the transferred set');
    assert.equal(ctx.els.handoffNotice.hidden, false);
    ctx.clearStudyHandoff();
    assert.equal(ctx.state.handoff.status, 'none');
    assert.equal(ctx.state.filtered.length, 2);
    assert.equal(ctx.els.handoffNotice.hidden, true);
    assert.equal(ctx.els.handoffSortOption.disabled, true);
    assert.equal(ctx.els.sortSelect.value, 'serial_asc');
    assert.equal(ctx.window.history.url, 'https://example.test/print_export.html?keep=1');
  }
});

test('normal print visits use all questions with normal sort controls', () => {
  const ctx = context();
  ctx.state.questions = ctx.preprocessQuestions([row(3), row(1), row(2)]);
  ctx.initializeStudyHandoff();
  assert.equal(ctx.els.handoffNotice.hidden, true);
  assert.equal(ctx.els.handoffSortOption.hidden, true);
  assert.deepEqual(Array.from(ctx.filterQuestions(), q => q.serial), ['A01-001', 'A01-002', 'A01-003']);
});

test('expanded text preview uses the exported complete block and follows answer, explanation, and numbering options', async () => {
  const ctx = context();
  const q = row(1, { deep_dive: { explanation: '深掘り解説の全文' } });
  ctx.state.filtered = [q, row(2)];
  ctx.els.textPreviewDetails.open = true;
  await ctx.updateTextPreview();
  assert.equal(ctx.els.textPreviewContent.textContent, ctx.buildQuestionBlock(q, 0, ctx.getExportMode()));
  assert.match(ctx.els.textPreviewContent.textContent, /症例の全文\nA01-001　問題文1\n１．選択肢甲\n２．選択肢乙/);
  assert.doesNotMatch(ctx.els.textPreviewContent.textContent, /解答|解説/);
  ctx.els.numberingSelect.value = 'continuous';
  ctx.els.includeExplanation.checked = true;
  ctx.els.includeDeepDive.checked = true;
  await ctx.updateTextPreview();
  assert.equal(ctx.els.textPreviewContent.textContent, ctx.buildQuestionBlock(q, 0, ctx.getExportMode()));
  assert.match(ctx.els.textPreviewContent.textContent, /問題1　問題文1/);
  assert.match(ctx.els.textPreviewContent.textContent, /解答　２\n解説\n通常解説の全文\n深掘り解説\n深掘り解説の全文/);
  assert.match(ctx.els.textPreviewStatus.textContent, /2 問中の先頭1問/);
});

test('late deep-dive loads cannot restore another question, changed options, or a collapsed preview', async () => {
  const ctx = context();
  const waits = [];
  ctx.ensureDeepDiveContentLoaded = questions => new Promise(resolve => waits.push(() => { questions[0].deep_dive = { explanation: '取得済み ' + questions[0].serial }; resolve(); }));
  ctx.els.textPreviewDetails.open = true;
  ctx.els.includeDeepDive.checked = true;
  ctx.state.filtered = [row(1)];
  const first = ctx.updateTextPreview();
  assert.equal(ctx.els.textPreviewContent.textContent, '');
  assert.equal(ctx.els.textPreviewContent.attrs['aria-busy'], 'true');
  ctx.state.filtered = [row(2)];
  const second = ctx.updateTextPreview();
  waits[1](); await second; waits[0](); await first;
  assert.match(ctx.els.textPreviewContent.textContent, /A01-002/);
  assert.doesNotMatch(ctx.els.textPreviewContent.textContent, /A01-001/);
  const third = ctx.updateTextPreview();
  ctx.els.includeDeepDive.checked = false;
  await ctx.updateTextPreview();
  waits[2](); await third;
  assert.doesNotMatch(ctx.els.textPreviewContent.textContent, /深掘り解説/);
  ctx.els.includeDeepDive.checked = true;
  const fourth = ctx.updateTextPreview();
  ctx.els.textPreviewDetails.open = false;
  await ctx.updateTextPreview();
  waits[3](); await fourth;
  assert.equal(ctx.els.textPreviewContent.textContent, '');
});

test('failed deep-dive previews clear stale content and can retry; absent content is explicitly identified', async () => {
  const ctx = context();
  ctx.state.filtered = [row(1)];
  ctx.els.textPreviewDetails.open = true;
  ctx.els.includeDeepDive.checked = true;
  ctx.els.textPreviewContent.textContent = '前回の古い出力';
  ctx.ensureDeepDiveContentLoaded = async () => { throw new Error('offline'); };
  await ctx.updateTextPreview();
  assert.equal(ctx.els.textPreviewContent.textContent, '');
  assert.match(ctx.els.textPreviewStatus.textContent, /取得できない.*再試行/);
  assert.equal(ctx.els.retryTextPreviewBtn.hidden, false);
  ctx.ensureDeepDiveContentLoaded = async () => {};
  await ctx.updateTextPreview();
  assert.match(ctx.els.textPreviewStatus.textContent, /作成済みの深掘り解説がありません.*保存時も/);
  assert.match(ctx.els.textPreviewContent.textContent, /A01-001/);
  assert.equal(ctx.els.retryTextPreviewBtn.hidden, true);
});
