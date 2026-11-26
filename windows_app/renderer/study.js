import {
  actions, defaultShortcuts, keyChord, shortcutError, validateSnapshot, searchIndex, search,
  validChoice, isCorrect, answerLabel, answerNote, questionText, solutionText
} from './study-core.mjs';

const $ = id => document.getElementById(id);
const api = window.asv;
const pageSize = 50;
const defaults = {
  theme: 'system', font: 'system', size: 20, spacing: 5, rate: 0, voice: '',
  autoRead: false, dictionary: true, keysEnabled: true
};
const fonts = {
  system: '"Segoe UI", "Yu Gothic UI", Meiryo, sans-serif',
  gothic: '"Yu Gothic", "Yu Gothic UI", Meiryo, sans-serif',
  mincho: '"Yu Mincho", "MS PMincho", serif',
  ud: '"BIZ UDGothic", "BIZ UD Gothic", Meiryo, sans-serif'
};
const prefKey = 'asv.windows.preferences.v1';
const answerKey = 'asv.windows.answers.v1';
let errorTimer;
let assignment = null;
let hasSpoken = false;
let index = [], results = [], position = -1;
let queryTimer;
let currentSearchPage = -1;
let resultsHiddenForChat = false;

function readObject(key) {
  try {
    const value = JSON.parse(localStorage.getItem(key));
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  } catch { return {}; }
}
function reportError(e) {
  const message = e?.message ?? String(e);
  clearTimeout(errorTimer);
  $('error').hidden = false;
  $('error').textContent = message;
  if ($('settings').open) $('speech-state').textContent = message;
  errorTimer = setTimeout(() => { $('error').hidden = true; }, 12000);
}
function saveObject(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); }
  catch { reportError('この端末に保存できません。今回の設定と回答は、アプリを開いている間だけ使えます。'); }
}
function bounded(value, min, max, fallback) {
  return Number.isFinite(value) ? Math.max(min, Math.min(max, Math.round(value))) : fallback;
}
const saved = readObject(prefKey);
const prefs = {
  theme: ['light', 'dark', 'system'].includes(saved.theme) ? saved.theme : 'system',
  font: Object.hasOwn(fonts, saved.font) ? saved.font : 'system',
  size: bounded(saved.size, 14, 40, defaults.size), spacing: bounded(saved.spacing, 0, 18, defaults.spacing),
  rate: bounded(saved.rate, -10, 10, defaults.rate), voice: typeof saved.voice === 'string' ? saved.voice : '',
  autoRead: saved.autoRead === true, dictionary: saved.dictionary !== false, keysEnabled: saved.keysEnabled !== false,
  shortcuts: {...defaultShortcuts}
};
// Restore a complete key map so swapping keys across several assignments survives a restart.
const storedKeys = saved.shortcuts;
if (storedKeys && typeof storedKeys === 'object') {
  const restored = {...defaultShortcuts};
  for (const id of Object.keys(actions)) if (typeof storedKeys[id] === 'string') restored[id] = storedKeys[id];
  if (Object.keys(actions).every(id => !shortcutError(id, restored[id], restored))) prefs.shortcuts = restored;
}
const answers = readObject(answerKey);
function savePreferences() { saveObject(prefKey, prefs); }
function announce(text) { $('notice').textContent = text; }
function selected() { return results[position]; }
function node(tag, value = '', className = '') {
  const el = document.createElement(tag);
  if (value) el.textContent = value;
  if (className) el.className = className;
  return el;
}
function showText(id, text) {
  const el = $(id);
  el.textContent = text || '';
  el.hidden = !text;
}
function questionEvent(type) {
  const q = selected() ?? null;
  window.dispatchEvent(new CustomEvent(type, {detail: {
    question: q, context: q ? questionText(q) + '\n' + solutionText(q) : ''
  }}));
}
function setLookup(show) {
  resultsHiddenForChat = false;
  $('lookup').hidden = !show;
  document.body.classList.toggle('lookup-closed', !show);
  $('toggle-results').setAttribute('aria-expanded', String(show));
}
function fitChat() {
  if ($('chat-panel').hidden) return;
  const compact = window.innerWidth > 780 && window.innerWidth < Math.max(1080, prefs.size * 34);
  if (compact && !$('lookup').hidden) {
    setLookup(false);
    resultsHiddenForChat = true;
  } else if (!compact && resultsHiddenForChat) {
    setLookup(true);
  }
}
window.addEventListener('resize', fitChat);

function renderPage(force = false) {
  const page = Math.max(0, Math.floor(position / pageSize));
  const list = $('results');
  if (force || page !== currentSearchPage) {
    currentSearchPage = page;
    const fragment = document.createDocumentFragment();
    const start = page * pageSize;
    for (const [offset, q] of results.slice(start, start + pageSize).entries()) {
      const li = node('li');
      li.id = 'option-' + q.serial;
      li.dataset.index = start + offset;
      li.setAttribute('role', 'option');
      li.setAttribute('aria-posinset', String(start + offset + 1));
      li.setAttribute('aria-setsize', String(results.length));
      li.append(node('strong', q.serial), node('span', q.subject, 'result-subject'), node('span', q.stem, 'result-stem'));
      fragment.append(li);
    }
    list.replaceChildren(fragment);
  }
  for (const item of list.children) {
    const active = Number(item.dataset.index) === position;
    item.tabIndex = active ? 0 : -1;
    item.setAttribute('aria-selected', String(active));
  }
  $('pages').hidden = results.length <= pageSize;
  $('page-number').textContent = (page + 1) + ' / ' + Math.ceil(results.length / pageSize);
  $('page-prev').disabled = page === 0;
  $('page-next').disabled = (page + 1) * pageSize >= results.length;
}
function activeOption() { return $('results').querySelector('[aria-selected="true"]'); }
function revealOption(focus) {
  const item = activeOption();
  if (!item || $('lookup').hidden) return;
  if (focus) item.focus({preventScroll:true});
  // Scroll only the results list. Searching on narrow screens must leave the search field visible.
  const area = $('results').getBoundingClientRect();
  const bounds = item.getBoundingClientRect();
  if (bounds.top < area.top) $('results').scrollTop += bounds.top - area.top;
  else if (bounds.bottom > area.bottom) $('results').scrollTop += bounds.bottom - area.bottom;
}

function renderReader() {
  const q = selected();
  $('problem').hidden = !q;
  $('loading').hidden = Boolean(q);
  if (!q) {
    $('loading').textContent = '一致する問題はありません。検索条件を変更してください。';
    return;
  }
  const choice = validChoice(q, answers[q.serial]) ? answers[q.serial] : null;
  $('serial').textContent = q.serial;
  $('subject-name').textContent = q.subject;
  $('exam-info').textContent = '第' + q.exam_session + '回　' + q.exam_type;
  showText('case', q.case_text);
  $('stem').textContent = q.stem;
  const fragment = document.createDocumentFragment();
  q.choices.forEach((value, i) => {
    const number = i + 1;
    const row = node(choice ? 'div' : 'button', '', 'choice');
    if (!choice) {
      row.type = 'button';
      row.dataset.choice = number;
      row.setAttribute('aria-label', '選択肢' + number + '。' + value);
      row.title = prefs.keysEnabled ? prefs.shortcuts['choice' + number] ?? '' : '';
    }
    const numberEl = node('span', String(number), 'choice-num');
    const label = node('span', value);
    if (number === choice) {
      row.classList.add('selected');
      label.append(node('span', 'あなたの回答', 'response-note'));
    }
    row.append(numberEl, label);
    fragment.append(row);
  });
  $('choices').replaceChildren(fragment);
  $('solution').hidden = !choice;
  if (choice) {
    $('feedback').textContent = isCorrect(q, choice) ? '正解' : '不正解';
    $('key-answer').textContent = '正答　' + answerLabel(q);
    showText('answer-note', answerNote(q));
    $('explanation-wrap').hidden = !q.explanation_latest;
    $('explanation').textContent = q.explanation_latest ?? '';
  }
  $('position').textContent = (position + 1).toLocaleString('ja-JP') + ' / ' + results.length.toLocaleString('ja-JP');
  $('prev').disabled = position <= 0;
  $('next').disabled = position >= results.length - 1;
}

function readText(text) {
  if (!text) return;
  hasSpoken = true;
  api.speech.speak({text, rate: prefs.rate, voice: prefs.voice, dictionary: prefs.dictionary}).catch(reportError);
}
function stopReading(force = false) {
  if (hasSpoken || force) api.speech.stop().catch(reportError);
  hasSpoken = false;
}
function readProblem() {
  const q = selected();
  if (q) readText(questionText(q) + (validChoice(q, answers[q.serial]) ? '\n' + solutionText(q) : ''));
}
function changedProblem() {
  $('reader').scrollTop = 0;
  questionEvent('asv:problem');
  if (prefs.autoRead && selected()) readProblem(); else stopReading();
}
function selectPosition(next, focus = false) {
  if (!Number.isInteger(next) || next < 0 || next >= results.length) return;
  const changed = next !== position;
  position = next;
  renderPage();
  revealOption(focus);
  if (changed) {
    renderReader();
    changedProblem();
    if (!focus && !prefs.autoRead) announce(selected().serial + '。' + selected().stem);
  }
}
function runSearch() {
  clearTimeout(queryTimer);
  const old = selected()?.serial;
  const filters = {query: $('search').value, exam: $('exam').value, subject: $('subject').value, session: $('session').value};
  results = search(index, filters);
  const found = old ? results.findIndex(q => q.serial === old) : -1;
  position = found >= 0 ? found : results.length ? 0 : -1;
  const n = [filters.exam, filters.subject, filters.session].filter(Boolean).length;
  $('filter-count').textContent = n ? '（' + n + '）' : '';
  $('result-count').textContent = results.length.toLocaleString('ja-JP') + ' 件の問題';
  renderPage(true);
  revealOption(false);
  renderReader();
  if (old !== selected()?.serial) changedProblem();
  if (!results.length && !$('chat-panel').hidden) closeChat(false);
}
function answer(number) {
  const q = selected();
  if (!q || !validChoice(q, number) || validChoice(q, answers[q.serial])) return;
  answers[q.serial] = number;
  saveObject(answerKey, answers);
  renderReader();
  $('feedback').focus();
  const result = $('feedback').textContent + '。正答は' + answerLabel(q);
  announce(result);
  if (prefs.autoRead) readText($('feedback').textContent + '。' + solutionText(q));
}
function retry() {
  const q = selected();
  if (!q || !validChoice(q, answers[q.serial])) return;
  delete answers[q.serial];
  saveObject(answerKey, answers);
  stopReading();
  renderReader();
  $('choices').querySelector('button')?.focus();
  announce('もう一度、選択肢を選んでください。');
}
function openChat() {
  if (!selected()) return;
  $('chat-panel').hidden = false;
  document.body.classList.add('chat-open');
  fitChat();
  questionEvent('asv:chat');
}
function closeChat(focus = true) {
  $('chat-panel').hidden = true;
  document.body.classList.remove('chat-open');
  if (resultsHiddenForChat) setLookup(true);
  if (focus) $('ask').focus();
  window.dispatchEvent(new CustomEvent('asv:chat-closed'));
}
$('toggle-results').onclick = () => {
  const show = $('lookup').hidden;
  setLookup(show);
  if (show) $('search').focus();
};
$('results').onclick = e => {
  const item = e.target.closest('[role=option]');
  if (item) selectPosition(Number(item.dataset.index), true);
};
$('results').onkeydown = e => {
  if (e.ctrlKey || e.altKey || e.metaKey || e.shiftKey || e.isComposing) return;
  const target = {ArrowDown: position + 1, ArrowUp: position - 1, PageDown: position + pageSize,
    PageUp: position - pageSize, Home: 0, End: results.length - 1};
  if (Object.hasOwn(target, e.key)) {
    e.preventDefault();
    selectPosition(Math.min(results.length - 1, Math.max(0, target[e.key])), true);
  } else if (e.key === 'Enter' || e.key === ' ') {
    e.preventDefault();
    $('stem').focus();
  }
};
$('page-prev').onclick = () => selectPosition((currentSearchPage - 1) * pageSize, true);
$('page-next').onclick = () => selectPosition((currentSearchPage + 1) * pageSize, true);
$('choices').onclick = e => { const el = e.target.closest('button[data-choice]'); if (el) answer(Number(el.dataset.choice)); };
$('search').addEventListener('input', e => {
  clearTimeout(queryTimer);
  if (!e.isComposing) queryTimer = setTimeout(runSearch, 100);
});
$('search').addEventListener('compositionend', () => { clearTimeout(queryTimer); queryTimer = setTimeout(runSearch, 100); });
for (const id of ['exam', 'subject', 'session']) $(id).onchange = runSearch;
$('reset-filters').onclick = () => {
  for (const id of ['search', 'exam', 'subject', 'session']) $(id).value = '';
  runSearch();
  $('search').focus();
};
$('read').onclick = readProblem;
$('stop').onclick = () => stopReading(true);
$('prev').onclick = () => selectPosition(position - 1);
$('next').onclick = () => selectPosition(position + 1);
$('retry').onclick = retry;
$('ask').onclick = openChat;
window.addEventListener('asv:close-chat', () => closeChat());
window.addEventListener('asv:read', e => readText(e.detail?.text));
window.addEventListener('asv:stop', () => stopReading(true));
window.addEventListener('asv:chat-ready', () => questionEvent('asv:problem'));

function applyAppearance() {
  const root = document.documentElement;
  root.dataset.theme = prefs.theme;
  root.style.setProperty('--size', prefs.size + 'pt');
  root.style.setProperty('--gap-line', prefs.spacing + 'pt');
  root.style.setProperty('--family', fonts[prefs.font]);
  $('smaller').disabled = prefs.size <= 14;
  $('larger').disabled = prefs.size >= 40;
  $('size').value = prefs.size;
  fitChat();
}
function resize(by) {
  prefs.size = bounded(prefs.size + by, 14, 40, defaults.size);
  applyAppearance();
  savePreferences();
  announce('文字サイズ ' + prefs.size + 'ポイント');
}
$('smaller').onclick = () => resize(-2);
$('larger').onclick = () => resize(2);
function refreshShortcutLabels() {
  const elements = {search: 'search', results: 'results', prev: 'prev', next: 'next', retry: 'retry',
    read: 'read', stop: 'stop', ask: 'ask', larger: 'larger', smaller: 'smaller'};
  for (const [id, el] of Object.entries(elements)) {
    $(el).title = actions[id][0] + (prefs.keysEnabled || id === 'larger' || id === 'smaller' ? '（' + prefs.shortcuts[id] + '）' : '');
  }
  for (const el of $('shortcut-list').querySelectorAll('button[data-action]')) {
    const id = el.dataset.action;
    el.textContent = assignment === id ? 'キーを入力…' : prefs.shortcuts[id];
    el.setAttribute('aria-label', actions[id][0] + 'のキー割り当て。' + el.textContent);
  }
}
for (const [id, [label]] of Object.entries(actions)) {
  const row = node('div', '', 'shortcut-line');
  const button = node('button');
  button.dataset.action = id;
  row.append(node('span', label), button);
  $('shortcut-list').append(row);
}
$('shortcut-list').onclick = e => {
  const button = e.target.closest('button[data-action]');
  if (button) {
    assignment = button.dataset.action;
    $('key-status').textContent = actions[assignment][0] + 'に使うキーを押してください。';
    refreshShortcutLabels();
  }
};
$('shortcut-list').addEventListener('focusout', e => {
  if (assignment && e.target.dataset.action === assignment) { assignment = null; refreshShortcutLabels(); }
});
$('reset-keys').onclick = () => {
  prefs.shortcuts = {...defaultShortcuts}; assignment = null; savePreferences(); refreshShortcutLabels();
  $('key-status').textContent = 'ショートカットを初期設定に戻しました。';
};

function focusResults() {
  setLookup(true);
  const item = activeOption();
  if (item) revealOption(true); else $('search').focus();
}
const handlers = {
  search: () => { setLookup(true); $('search').focus(); }, results: focusResults,
  prev: () => selectPosition(position - 1), next: () => selectPosition(position + 1),
  retry, read: readProblem, stop: () => stopReading(true), ask: openChat,
  larger: () => resize(2), smaller: () => resize(-2)
};
window.addEventListener('keydown', e => {
  if (e.defaultPrevented || e.isComposing || e.keyCode === 229) return;
  if (assignment && $('settings').open) {
    if (e.key === 'Tab') { assignment = null; refreshShortcutLabels(); return; }
    e.preventDefault(); e.stopPropagation();
    if (e.key === 'Escape') {
      assignment = null;
      $('key-status').textContent = '入力を取り消しました。';
    } else if (!['Shift','Control','Alt','Meta'].includes(e.key)) {
      const chord = keyChord(e);
      const error = shortcutError(assignment, chord, prefs.shortcuts);
      if (error) $('key-status').textContent = error;
      else {
        $('key-status').textContent = actions[assignment][0] + 'を ' + chord + ' に設定しました。';
        prefs.shortcuts[assignment] = chord;
        assignment = null; savePreferences();
      }
    }
    refreshShortcutLabels();
    return;
  }
  const chord = keyChord(e);
  const action = Object.keys(actions).find(id => prefs.shortcuts[id] === chord);
  if (!action) return;
  const zoom = action === 'larger' || action === 'smaller';
  if (!zoom && (!prefs.keysEnabled || $('settings').open)) return;
  e.preventDefault();
  if (e.repeat && !zoom) return;
  if (action.startsWith('choice')) answer(Number(action.slice(-1)));
  else handlers[action]();
}, true);

function paintUpdate(update) {
  if (!update || typeof update.status !== 'string') return;
  $('update-state').textContent = update.message ?? '';
  const busy = ['checking','downloading','installing'].includes(update.status);
  $('check-update').disabled = busy;
  $('apply-update').hidden = update.status !== 'available';
}
function updateError(err) {
  paintUpdate({status:'error', message: err?.message ?? 'アップデートを確認できません。'});
}
$('check-update').onclick = async () => {
  paintUpdate({status:'checking',message:'アップデートを確認しています…'});
  try { paintUpdate(await api.updates.check()); } catch (e) { updateError(e); }
};
$('apply-update').onclick = async () => {
  paintUpdate({status:'downloading',message:'更新を準備しています…'});
  try { paintUpdate(await api.updates.apply()); } catch (e) { updateError(e); }
};
function showDictionary(state) {
  if (!state || state.cancelled) return;
  $('dictionary-state').textContent = state.count > 0
    ? '理療科用読み辞書　登録語数 ' + state.count.toLocaleString('ja-JP') + ' 語'
    : '読み辞書は未登録です。Jukugo.usr を選んでください。';
}
$('import-dictionary').onclick = async () => {
  $('import-dictionary').disabled = true;
  try {
    const result = await api.speech.importDictionary();
    if (!result?.cancelled && result?.count > 0) {
      prefs.dictionary = true; $('dictionary-enabled').checked = true; savePreferences();
    }
    showDictionary(result);
  } catch (e) { $('dictionary-state').textContent = e.message; }
  finally { $('import-dictionary').disabled = false; }
};

async function loadVoiceOptions() {
  try {
    const voices = await api.speech.voices();
    const sel = $('voice');
    sel.replaceChildren(new Option('日本語の音声を自動選択', ''));
    for (const voice of voices) sel.add(new Option(voice.name + (voice.language ? '（' + voice.language + '）' : ''), voice.id));
    sel.value = voices.some(v => v.id === prefs.voice) ? prefs.voice : '';
    if (!voices.length) $('speech-state').textContent = '利用できる音声を確認できません。Windows の音声設定を確認してください。';
  } catch (e) { $('speech-state').textContent = e.message; }
}
function openSettings() {
  $('theme').value = prefs.theme; $('font').value = prefs.font;
  $('size').value = prefs.size; $('spacing').value = prefs.spacing; $('rate').value = prefs.rate;
  $('auto-read').checked = prefs.autoRead; $('dictionary-enabled').checked = prefs.dictionary;
  $('keys-enabled').checked = prefs.keysEnabled;
  $('key-status').textContent = ''; $('speech-state').textContent = '';
  $('settings').showModal();
  loadVoiceOptions();
  api.speech.dictionary().then(showDictionary).catch(e => { $('dictionary-state').textContent = e.message; });
  api.updates.status().then(paintUpdate).catch(updateError);
}
$('settings-open').onclick = openSettings;
$('settings-close').onclick = () => $('settings').close();
$('settings').addEventListener('close', () => {
  assignment = null; refreshShortcutLabels();
  $('settings-open').focus();
});
for (const key of ['theme','font','voice']) $(key).onchange = e => {
  prefs[key] = e.target.value; applyAppearance(); savePreferences();
};
for (const [key, min, max] of [['size',14,40],['spacing',0,18],['rate',-10,10]]) {
  $(key).onchange = e => {
    prefs[key] = bounded(e.target.valueAsNumber, min, max, prefs[key]); e.target.value = prefs[key];
    applyAppearance(); savePreferences();
  };
}
for (const [id, key] of [['auto-read','autoRead'],['dictionary-enabled','dictionary'],['keys-enabled','keysEnabled']]) {
  $(id).onchange = e => { prefs[key] = e.target.checked; savePreferences(); refreshShortcutLabels(); };
}

async function initialize() {
  applyAppearance();
  refreshShortcutLabels();
  try {
    const rows = validateSnapshot(await api.home());
    index = searchIndex(rows);
    const examTypes = [...new Set(rows.map(q => q.exam_type))].sort((a,b) => a.localeCompare(b, 'ja'));
    const subjects = [...new Set(rows.map(q => q.subject))];
    const sessions = [...new Set(rows.map(q => q.exam_session))].sort((a,b) => b-a);
    for (const exam of examTypes) $('exam').add(new Option(exam, exam));
    for (const subject of subjects) $('subject').add(new Option(subject, subject));
    for (const session of sessions) $('session').add(new Option('第' + session + '回', String(session)));
    runSearch();
    api.updates.subscribe(paintUpdate);
  } catch (e) {
    $('loading').textContent = '問題データを開けません。' + (e?.message ?? '');
    $('result-count').textContent = '読み込みに失敗しました。';
  }
}
initialize();
