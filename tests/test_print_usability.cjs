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
    focus() { this.focused = true; },
    setAttribute(name, value) { this.attrs[name] = value; } };
}
function context(bundle = { status: 'none' }) {
  const els = Object.fromEntries([
    'keywordInput', 'subjectSelect', 'subtopicSelect', 'examTypeSelect', 'sessionFrom', 'sessionTo',
    'serialFrom', 'serialTo', 'sortSelect', 'numberingSelect', 'contentQuestions', 'contentAnswers',
    'contentExplanations', 'filterDetails', 'handoffNotice', 'handoffMessage', 'handoffSortOption', 'textPreviewDetails',
    'textPreviewStatus', 'textPreviewContent',
  ].map(name => [name, element()]));
  els.sortSelect.value = 'serial_asc';
  els.numberingSelect.value = 'serial';
  els.contentQuestions.checked = true;
  const window = { location: { href: 'https://example.test/print_export.html?studySet=abc&keep=1', search: '?studySet=abc&keep=1' }, history: { replaceState(_state, _title, value) { this.url = value; } } };
  let nextTimer = 1;
  const timers = new Map();
  const setTimeout = (callback, delay) => { const id = nextTimer++; timers.set(id, { callback, delay }); return id; };
  const clearTimeout = id => timers.delete(id);
  Object.assign(window, { setTimeout, clearTimeout });
  const ctx = vm.createContext({
    AhakiQuestions: Q, URL, Blob, window, els, console, setTimeout, clearTimeout, timers,
    encoder: new TextEncoder(),
    collator: new Intl.Collator('ja'),
    state: { questions: [], filtered: [], handoff: { status: 'none' }, handoffQuestions: [], ready: true, filterTimer: null },
    AhakiStudy: {
      readHandoff: () => bundle,
      orderQuestions: (questions, serials) => {
        const bySerial = new Map(questions.map(q => [q.serial, q]));
        return { questions: serials.filter(id => bySerial.has(id)).map(id => bySerial.get(id)), missingSerials: serials.filter(id => !bySerial.has(id)) };
      },
    },
    updateSubtopicOptions() { els.subtopicSelect.value = ''; },
    renderPreview() { ctx.renderCount = (ctx.renderCount || 0) + 1; },
    syncActionButtons() {},
    announce() {},
    setStatus(message) { ctx.status = message; },
    downloadText(filename, text) { ctx.savedTxt = { filename, text }; },
    downloadBlob(filename, blob) { ctx.savedZip = { filename, blob }; },
  });
  functions(ctx, ['normalizeText', 'normalizeSerialInput', 'parseSerialParts', 'isValidSerialParts',
    'compareSerialValue', 'compareBySession', 'compareBySubjectSerial', 'buildSearchText', 'preprocessQuestions',
    'getKeywordTerms', 'matchesKeyword', 'getNumberOrNull', 'initializeStudyHandoff', 'renderStudyHandoff',
    'clearStudyHandoff', 'filterQuestions', 'resetControls', 'buildFilterSummary', 'getSubtopicGroups',
    'getSortLabel', 'getExportMode', 'formatAnswerLabel', 'buildQuestionBlock', 'updateTextPreview',
    'applyFilters', 'scheduleFilters', 'formatLocalTimestamp', 'formatDisplayDate', 'buildExportHeader',
    'buildExportText', 'sanitizeFilename', 'buildCurrentExportTitle', 'downloadCurrentTxt', 'downloadSubtopicZip',
    'crc32', 'createBuffer', 'writeDosDateTime', 'createZipBlob']);
  const crcTable = /const CRC32_TABLE = \(\(\) => \{[\s\S]*?\}\)\(\);/.exec(html);
  assert.ok(crcTable, 'ZIP CRC table initializer');
  vm.runInContext(crcTable[0], ctx);
  return ctx;
}
function row(i, extra = {}) {
  return { serial: `A01-${String(i).padStart(3, '0')}`, subject: '生理学', exam_type: 'あん摩マッサージ指圧師', exam_session: 1,
    case_text: '症例の全文', stem: `問題文${i}`, choices: ['選択肢甲', '選択肢乙'], answer_indices: [2],
    subtopics: ['筋'], explanation_latest: '通常解説の全文', ...extra };
}

function chooseContent(ctx, mode) {
  for (const name of ['contentQuestions', 'contentAnswers', 'contentExplanations']) {
    ctx.els[name].checked = name === mode;
  }
}

async function unzipTextEntries(blob) {
  const bytes = Buffer.from(await blob.arrayBuffer());
  const entries = [];
  let offset = 0;
  while (bytes.readUInt32LE(offset) === 0x04034b50) {
    assert.equal(bytes.readUInt16LE(offset + 8), 0, 'archive uses uncompressed UTF-8 text');
    const size = bytes.readUInt32LE(offset + 18);
    const nameLength = bytes.readUInt16LE(offset + 26);
    const extraLength = bytes.readUInt16LE(offset + 28);
    const start = offset + 30 + nameLength + extraLength;
    entries.push({
      name: bytes.subarray(offset + 30, offset + 30 + nameLength).toString('utf8'),
      text: bytes.subarray(start, start + size).toString('utf8'),
    });
    offset = start + size;
  }
  assert.equal(bytes.readUInt32LE(offset), 0x02014b50, 'archive includes its central directory');
  return entries;
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
    assert.equal(ctx.els.filterDetails.open, true);
    assert.equal(ctx.els.keywordInput.focused, true);
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

test('three content choices give the same complete question blocks in preview, TXT, and every ZIP group', async () => {
  const ctx = context();
  const q = row(1, { deep_dive: { explanation: '深掘り解説の全文' } });
  ctx.state.questions = ctx.preprocessQuestions([q, row(2, { subtopics: ['神経'], deep_dive: { explanation: '出力禁止の深掘り本文' } })]);
  ctx.els.subjectSelect.value = '生理学';
  ctx.els.textPreviewDetails.open = true;
  for (const [content, answers, explanations] of [
    ['contentQuestions', false, false],
    ['contentAnswers', true, false],
    ['contentExplanations', true, true],
  ]) {
    chooseContent(ctx, content);
    ctx.applyFilters();
    const mode = ctx.getExportMode();
    assert.equal(mode.includeAnswer, answers, content);
    assert.equal(mode.includeExplanation, explanations, content);
    const block = ctx.buildQuestionBlock(q, 0, mode);
    assert.equal(ctx.els.textPreviewContent.textContent, block);
    assert.match(block, /症例の全文\nA01-001　問題文1\n１．選択肢甲\n２．選択肢乙/);
    assert.equal(block.includes('解答　２'), answers, content);
    assert.equal(block.includes('解説\n通常解説の全文'), explanations, content);
    assert.match(ctx.els.textPreviewStatus.textContent, /2 問中の先頭1問/);
    await ctx.downloadCurrentTxt();
    assert.match(ctx.savedTxt.filename, /\.txt$/);
    assert.ok(ctx.savedTxt.text.includes(block), content);
    await ctx.downloadSubtopicZip();
    assert.match(ctx.savedZip.filename, /\.zip$/);
    const entries = await unzipTextEntries(ctx.savedZip.blob);
    assert.equal(entries.length, 2);
    assert.ok(entries.some(entry => entry.text.includes(block)), content);
    for (const text of [ctx.savedTxt.text, ...entries.map(entry => entry.text)]) {
      assert.equal(text.includes('解答　２'), answers, content);
      assert.equal(text.includes('解説\n通常解説の全文'), explanations, content);
      assert.doesNotMatch(text, /深掘り|出力禁止/);
    }
  }
});

test('search includes ordinary explanations but never a cached deep-dive-only term', () => {
  const ctx = context();
  ctx.state.questions = ctx.preprocessQuestions([
    row(1, { explanation_latest: '通常で見つかる用語', deep_dive: { explanation: '深掘りだけの用語' } }), row(2),
  ]);
  ctx.els.keywordInput.value = '通常で見つかる用語';
  assert.deepEqual(Array.from(ctx.filterQuestions(), q => q.serial), ['A01-001']);
  ctx.els.keywordInput.value = '深掘りだけの用語';
  assert.equal(ctx.filterQuestions().length, 0);
});

test('preview clears old question text when closed, loading, empty, or blocked by an invalid handoff', () => {
  const ctx = context();
  ctx.state.filtered = [row(1)];
  ctx.els.textPreviewDetails.open = true;
  ctx.updateTextPreview();
  assert.match(ctx.els.textPreviewContent.textContent, /A01-001/);
  ctx.els.textPreviewDetails.open = false;
  ctx.updateTextPreview();
  assert.equal(ctx.els.textPreviewContent.textContent, '');
  ctx.els.textPreviewDetails.open = true;
  for (const [ready, filtered, handoff, message] of [
    [false, [row(1)], { status: 'none' }, /読み込み中/],
    [true, [], { status: 'none' }, /条件に合う問題がありません/],
    [true, [row(1)], { status: 'error', message: '引き継ぎの期限が切れました。' }, /引き継ぎの期限が切れました/],
  ]) {
    Object.assign(ctx.state, { ready, filtered, handoff });
    ctx.els.textPreviewContent.textContent = '前回の古い出力';
    ctx.updateTextPreview();
    assert.equal(ctx.els.textPreviewContent.textContent, '');
    assert.match(ctx.els.textPreviewStatus.textContent, message);
  }
});

test('preview follows numbering and ordinary-booklet answer corrections, and identifies a missing explanation', () => {
  const ctx = context();
  const q = row(1, {
    stem: '訂正後の問題文', choices: ['訂正後の甲', '訂正後の乙'],
    answer_indices: [2], answer_variants: { default: [1], braille: [2] },
    answer_notes: ['採点上の注記', '点字問題は２'], explanation_latest: '',
    deep_dive: { explanation: '通常解説の代わりに出してはいけない' },
  });
  ctx.state.filtered = [q];
  ctx.els.numberingSelect.value = 'continuous';
  chooseContent(ctx, 'contentExplanations');
  ctx.els.textPreviewDetails.open = true;
  ctx.updateTextPreview();
  const text = ctx.els.textPreviewContent.textContent;
  assert.match(text, /問題1　訂正後の問題文\n１．訂正後の甲\n２．訂正後の乙/);
  assert.match(text, /解答　１\n注記: 採点上の注記/);
  assert.doesNotMatch(text, /点字|代わりに|解答　２/);
  assert.match(ctx.els.textPreviewStatus.textContent, /解説がありません/);
});

test('reset clears only search conditions and restores the transferred scope while preserving output choices', () => {
  const ctx = context({ status: 'ready', bundle: { serials: ['A01-003', 'A01-001'], title: '検索結果' } });
  ctx.state.questions = ctx.preprocessQuestions([row(1), row(2), row(3)]);
  ctx.initializeStudyHandoff();
  const filterNames = ['keywordInput', 'subjectSelect', 'subtopicSelect', 'examTypeSelect',
    'sessionFrom', 'sessionTo', 'serialFrom', 'serialTo'];
  for (const name of filterNames) ctx.els[name].value = '古い条件';
  ctx.els.sortSelect.value = 'serial_asc';
  ctx.els.numberingSelect.value = 'continuous';
  chooseContent(ctx, 'contentExplanations');
  ctx.resetControls();
  for (const name of filterNames) assert.equal(ctx.els[name].value, '', name);
  assert.equal(ctx.els.numberingSelect.value, 'continuous');
  assert.equal(ctx.els.contentExplanations.checked, true);
  assert.equal(ctx.getExportMode().includeAnswer, true);
  assert.deepEqual(Array.from(ctx.state.filtered, q => q.serial), ['A01-003', 'A01-001']);
});

test('typing batches updates and both downloads flush pending input before selecting the exported questions', async () => {
  const ctx = context();
  ctx.state.questions = ctx.preprocessQuestions([row(1), row(2)]);
  ctx.els.subjectSelect.value = '生理学';
  ctx.applyFilters();
  const initialRenders = ctx.renderCount;
  ctx.els.keywordInput.value = '問題';
  ctx.scheduleFilters();
  ctx.els.keywordInput.value = '問題文2';
  ctx.scheduleFilters();
  assert.equal(ctx.timers.size, 1, 'only the latest input update stays scheduled');
  assert.equal([...ctx.timers.values()][0].delay, 300);
  assert.equal(ctx.renderCount, initialRenders, 'typing does not render on every keystroke');
  await ctx.downloadCurrentTxt();
  assert.equal(ctx.timers.size, 0, 'download cancels the pending update');
  assert.match(ctx.savedTxt.text, /A01-002　問題文2/);
  assert.doesNotMatch(ctx.savedTxt.text, /A01-001　問題文1/);
  ctx.els.keywordInput.value = '問題文1';
  ctx.scheduleFilters();
  await ctx.downloadSubtopicZip();
  assert.equal(ctx.timers.size, 0);
  const entries = await unzipTextEntries(ctx.savedZip.blob);
  assert.equal(entries.length, 1);
  assert.match(entries[0].text, /A01-001　問題文1/);
  assert.doesNotMatch(entries[0].text, /A01-002　問題文2/);
});
