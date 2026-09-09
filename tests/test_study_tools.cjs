const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../web_app/shared/study-tools.js'), 'utf8');
const SET_PREFIX = 'ahaki_study_set_v1:';
const PRESETS_KEY = 'ahaki_search_presets_v1';
const plain = value => JSON.parse(JSON.stringify(value));
function memoryStorage() {
  const rows = new Map();
  return { rows, get length() { return rows.size; }, key: index => [...rows.keys()][index] || null,
    getItem: key => rows.has(key) ? rows.get(key) : null,
    setItem: (key, value) => rows.set(key, String(value)), removeItem: key => rows.delete(key) };
}
function load(store = memoryStorage(), options = {}) {
  const events = {};
  const context = { URLSearchParams, localStorage: store, addEventListener: (name, callback) => { events[name] = callback; }, ...options };
  vm.runInNewContext(source, context, { filename: 'study-tools.js' });
  return { api: context.AhakiStudy, store, context, events };
}
function changeBundle(store, token, update) {
  const key = SET_PREFIX + token;
  const record = JSON.parse(store.getItem(key));
  update(record);
  store.setItem(key, JSON.stringify(record));
}

test('guest handoff preserves exact requested order and reports missing questions without expanding the set', () => {
  const { api, store } = load();
  const token = api.saveHandoff(['B20-095', 'A01-003', 'B20-095', 'A01-002'], '生理学の3問');
  assert.match(token, /^[a-zA-Z0-9_-]{16,80}$/);
  const nextPage = load(store).api;
  const result = nextPage.readHandoff('?other=1&studySet=' + token);
  assert.equal(result.status, 'ready');
  assert.deepEqual(plain(result.bundle.serials), ['B20-095', 'A01-003', 'A01-002']);
  assert.equal(result.bundle.title, '生理学の3問');
  const known = [{ serial: 'A01-001' }, { serial: 'A01-002' }, { serial: 'B20-095' }];
  const ordered = nextPage.orderQuestions(known, result.bundle.serials);
  assert.deepEqual(Array.from(ordered.questions, q => q.serial), ['B20-095', 'A01-002']);
  assert.deepEqual(plain(ordered.missingSerials), ['A01-003']);
  assert.equal(ordered.questions[0], known[2]);
  const absent = nextPage.orderQuestions(known, ['B01-001']);
  assert.equal(absent.questions.length, 0);
  assert.deepEqual(plain(absent.missingSerials), ['B01-001']);
});

test('all 10,720 questions transfer without truncation and an empty or oversized handoff is rejected', () => {
  const { api } = load();
  const serials = Array.from({ length: 10720 }, (_, i) => {
    const index = i % 5360;
    return (i < 5360 ? 'A' : 'B') + String(Math.floor(index / 160) + 1).padStart(2, '0') + '-' + String(index % 160 + 1).padStart(3, '0');
  }).reverse();
  const token = api.saveHandoff(serials, '全問');
  const result = api.readHandoff('?studySet=' + token);
  assert.equal(result.status, 'ready');
  assert.deepEqual(plain(result.bundle.serials), serials);
  assert.throws(() => api.saveHandoff([], '空'), /1〜10,720/);
  assert.throws(() => api.saveHandoff([...serials, 'B34-160'], '多すぎる'), /1〜10,720/);
});

test('missing, malformed, duplicated and expired handoff tokens return an error, never the unfiltered question list', () => {
  const { api, store } = load();
  assert.equal(api.readHandoff('?subject=生理学').status, 'none');
  for (const query of ['?studySet=', '?studySet=__proto__', '?studySet=' + 'a'.repeat(32), '?studySet=' + 'a'.repeat(32) + '&studySet=' + 'b'.repeat(32)]) {
    const result = api.readHandoff(query);
    assert.equal(result.status, 'error');
    assert.equal(result.bundle, null);
  }
  const token = api.saveHandoff(['A01-001'], '期限');
  changeBundle(store, token, item => { item.createdAt = new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString(); });
  assert.match(api.readHandoff('?studySet=' + token).message, /有効期限/);
  for (const mutate of [item => { item.version = 2; }, item => { item.serials = ['__proto__']; }, item => { item.serials = []; }, item => { item.serials = ['A01-001', 'A01-001']; }, item => { item.createdAt = 'invalid'; }]) {
    const id = api.saveHandoff(['A01-001'], '不正データ');
    changeBundle(store, id, mutate);
    assert.equal(api.readHandoff('?studySet=' + id).status, 'error');
  }
  const broken = api.saveHandoff(['A01-001'], '破損');
  store.setItem(SET_PREFIX + broken, '{broken');
  assert.equal(api.readHandoff('?studySet=' + broken).status, 'error');
});

test('handoff retention is bounded and storage failure stops transfer explicitly', () => {
  const { api, store } = load();
  let last;
  for (let i = 0; i < 13; i += 1) last = api.saveHandoff(['A01-001'], 'set ' + i);
  assert.equal([...store.rows.keys()].filter(key => key.startsWith(SET_PREFIX)).length, 10);
  assert.equal(api.readHandoff('?studySet=' + last).status, 'ready');
  store.setItem = () => { throw new Error('quota'); };
  assert.throws(() => api.saveHandoff(['A01-001'], '保存失敗'), /保存できません/);
  const blocked = load({ getItem() { throw new Error('disabled'); }, setItem() { throw new Error('disabled'); } }).api;
  assert.equal(blocked.readHandoff('?studySet=' + last).status, 'error');
  assert.throws(() => blocked.saveHandoff(['A01-001'], '保存失敗'), /保存できません/);
});

test('copy format defaults to questions, persists between guest pages and yields explicit copy content', () => {
  const { api, store } = load();
  assert.equal(api.getCopyFormat(), 'questions');
  assert.deepEqual(plain(api.getCopyMode()), { showAnswer: false, showExplanation: false });
  assert.equal(api.setCopyFormat('answers'), true);
  assert.deepEqual(plain(load(store).api.getCopyMode()), { showAnswer: true, showExplanation: false });
  assert.equal(api.setCopyFormat('explanations'), true);
  assert.deepEqual(plain(load(store).api.getCopyMode()), { showAnswer: true, showExplanation: true });
  assert.equal(api.setCopyFormat('__proto__'), true);
  assert.equal(api.getCopyFormat(), 'questions');
});

test('copy bindings retain the current choice in memory on quota failure and synchronize another tab', () => {
  const { api, store, events } = load();
  const listeners = {};
  const select = { value: '', addEventListener: (name, callback) => { listeners[name] = callback; } };
  const notifications = [];
  api.bindCopyFormat(select, persisted => notifications.push(persisted));
  assert.equal(select.value, 'questions');
  const realWrite = store.setItem;
  store.setItem = () => { throw new Error('quota'); };
  select.value = 'explanations';
  listeners.change();
  assert.equal(select.value, 'explanations');
  assert.equal(api.getCopyFormat(), 'explanations');
  assert.deepEqual(notifications, [false]);
  store.setItem = realWrite;
  load(store).api.setCopyFormat('answers');
  events.storage({ key: 'ahaki_copy_format_v1' });
  assert.equal(select.value, 'answers');
  assert.deepEqual(notifications, [false, true]);
});

test('skip confirmation defaults off, persists explicitly and remains usable when storage is disabled', () => {
  const { api, store } = load();
  assert.equal(api.getSkipAnswerConfirmation(), false);
  assert.equal(api.setSkipAnswerConfirmation(true), true);
  assert.equal(load(store).api.getSkipAnswerConfirmation(), true);
  store.setItem = () => { throw new Error('quota'); };
  assert.equal(api.setSkipAnswerConfirmation(false), false);
  assert.equal(api.getSkipAnswerConfirmation(), false);
  const blocked = load(null).api;
  assert.equal(blocked.getSkipAnswerConfirmation(), false);
  assert.equal(blocked.setSkipAnswerConfirmation(true), false);
  assert.equal(blocked.getSkipAnswerConfirmation(), true);
  assert.equal(blocked.setCopyFormat('answers'), false);
  assert.equal(blocked.getCopyFormat(), 'answers');
});

test('named presets persist across pages, update in place, reject duplicate names and delete precisely', () => {
  const { api, store } = load();
  assert.deepEqual(plain(api.listSearchPresets()), []);
  const first = api.saveSearchPreset(' 生理学 ', { keyword: '筋 収縮', subject: '生理学', sessionFrom: 20, randomSeed: '1234' });
  const second = api.saveSearchPreset('解剖学', { subject: '解剖学' });
  const other = load(store).api;
  assert.deepEqual(plain(other.listSearchPresets()[0]), { ...plain(first), name: '生理学', filters: { keyword: '筋 収縮', subject: '生理学', sessionFrom: '20', randomSeed: '1234' } });
  assert.throws(() => other.saveSearchPreset('生理学', {}), /同じ名前/);
  assert.throws(() => other.saveSearchPreset('解剖学', {}, first.id), /同じ名前/);
  const updated = other.saveSearchPreset('筋の復習', { keyword: '筋' }, first.id);
  assert.equal(updated.id, first.id);
  assert.equal(other.listSearchPresets().length, 2);
  other.deleteSearchPreset(first.id);
  assert.deepEqual(Array.from(api.listSearchPresets(), item => item.id), [second.id]);
  assert.throws(() => api.saveSearchPreset('消えた条件', {}, first.id), /見つかりません/);
});

test('preset names and count are bounded and filter objects cannot carry prototype or unknown keys', () => {
  const { api } = load();
  assert.throws(() => api.saveSearchPreset(' ', {}), /1〜60/);
  assert.throws(() => api.saveSearchPreset('長'.repeat(61), {}), /1〜60/);
  const hostile = JSON.parse('{"__proto__":{"polluted":true},"constructor":"bad","keyword":"筋","account":"private"}');
  const saved = api.saveSearchPreset('__proto__', hostile);
  assert.deepEqual(plain(saved.filters), { keyword: '筋' });
  assert.equal({}.polluted, undefined);
  const inherited = Object.create({ keyword: 'inherited' });
  inherited.subject = '生理学';
  assert.deepEqual(plain(api.saveSearchPreset('own only', inherited).filters), { subject: '生理学' });
  assert.throws(() => api.saveSearchPreset('nested', { keyword: {} }), /形式/);
  for (let i = 2; i < 30; i += 1) api.saveSearchPreset('条件 ' + i, {});
  assert.equal(api.listSearchPresets().length, 30);
  assert.throws(() => api.saveSearchPreset('31個目', {}), /30件/);
});

test('malformed or unreadable presets never overwrite prior bytes and quota failure does not pretend to save', () => {
  for (const broken of ['{broken', JSON.stringify({ version: 2, presets: [] }), JSON.stringify({ version: 1, presets: [{ id: '__proto__', name: 'bad', filters: {}, updatedAt: 'now' }] })]) {
    const { api, store } = load();
    store.setItem(PRESETS_KEY, broken);
    assert.deepEqual(plain(api.listSearchPresets()), []);
    assert.throws(() => api.saveSearchPreset('new', {}), /読み込めません/);
    assert.equal(store.getItem(PRESETS_KEY), broken);
  }
  const { api, store } = load();
  const saved = api.saveSearchPreset('既存', { keyword: 'old' });
  const before = store.getItem(PRESETS_KEY);
  store.setItem = () => { throw new Error('quota'); };
  assert.throws(() => api.saveSearchPreset('新規', {}), /保存できません/);
  assert.throws(() => api.deleteSearchPreset(saved.id), /保存できません/);
  assert.equal(store.getItem(PRESETS_KEY), before);
  assert.deepEqual(plain(load(null).api.listSearchPresets()), []);
});

test('ID generation also works with crypto random bytes without requiring randomUUID', () => {
  let counter = 0;
  const crypto = { getRandomValues(bytes) { bytes.fill(++counter); return bytes; } };
  const { api } = load(memoryStorage(), { crypto });
  const first = api.saveHandoff(['A01-001'], 'one');
  const second = api.saveHandoff(['A01-001'], 'two');
  assert.notEqual(first, second);
  assert.equal(api.readHandoff('?studySet=' + first).bundle.title, 'one');
});
