/** Use the normal exam booklet's one-based choice numbers, as in ASV for Mac. */
export function correctChoices(q) {
  return q.answer_variants?.default ?? (q.answer_indices?.length ? q.answer_indices :
    (Number.isInteger(q.answer_index) ? [q.answer_index] : []));
}

export function validChoice(q, number) {
  return Number.isInteger(number) && number >= 1 && number <= q.choices.length;
}

export function isCorrect(q, number) {
  return validChoice(q, number) && (q.answer_none === true || correctChoices(q).includes(number));
}

export function validateSnapshot(snapshot) {
  const rows = snapshot?.questions;
  if (!Array.isArray(rows) || !rows.length || rows.length !== snapshot.manifest?.total ||
      !snapshot.manifest.generated_at) throw new Error('問題データの件数または版情報が正しくありません。');
  const seen = new Set();
  for (const q of rows) {
    if (!q || typeof q.serial !== 'string' || !q.serial || seen.has(q.serial) ||
        typeof q.stem !== 'string' || !Array.isArray(q.choices) || !q.choices.length) {
      throw new Error('問題データに重複、または読み取れない問題があります。');
    }
    seen.add(q.serial);
    const indices = correctChoices(q);
    if (!Array.isArray(indices) || (indices.length === 0) !== Boolean(q.answer_none) ||
        indices.some(n => !validChoice(q, n)) || new Set(indices).size !== indices.length) {
      throw new Error(q.serial + ' の正答を読み取れません。');
    }
  }
  return rows;
}

export function answerLabel(q) {
  return q.answer_none ? 'なし（全員正解）' : correctChoices(q).join('・');
}

export function answerNote(q) {
  const notes = Array.isArray(q.answer_notes) ? q.answer_notes : [q.answer_notes];
  return notes.filter(s => typeof s === 'string' && s && !s.includes('点字')).join(' / ');
}

export function questionText(q) {
  return [q.serial, '第' + q.exam_session + '回 ' + q.exam_type, q.subject, q.case_text, q.stem,
    ...q.choices.map((s, i) => '選択肢' + (i + 1) + '。' + s)].filter(Boolean).join('\n');
}

export function solutionText(q) {
  return ['正答 ' + answerLabel(q), answerNote(q), q.explanation_latest].filter(Boolean).join('\n');
}

function normalize(text) { return String(text ?? '').normalize('NFKC').toLowerCase(); }

export function searchIndex(rows) {
  return rows.map(q => ({
    q,
    serial: normalize(q.serial),
    tags: (q.tags ?? []).map(normalize),
    text: normalize([q.serial, q.subject, q.case_text, q.stem, ...q.choices,
      ...(q.subtopics ?? []), ...(q.tags ?? [])].filter(Boolean).join(' '))
  }));
}

function searchTerm(word) {
  if (word.startsWith('#')) return entry => entry.tags.includes(word.slice(1));
  const serial = /^([ab])(\d{1,2})-(\d{1,3})$/.exec(word) ?? /^([ab])(\d{1,2})(\d{3})$/.exec(word);
  if (serial) {
    const id = serial[1] + serial[2].padStart(2, '0') + '-' + serial[3].padStart(3, '0');
    return entry => entry.serial === id;
  }
  return entry => entry.text.includes(word);
}

export function search(index, {query = '', exam = '', subject = '', session = ''} = {}) {
  const groups = normalize(query).split(/[,、]/).map(group => group.trim()).filter(Boolean)
    .map(group => group.split(/\s+/).map(searchTerm));
  return index.filter(entry => (!exam || entry.q.exam_type === exam) &&
    (!subject || entry.q.subject === subject) && (!session || String(entry.q.exam_session) === String(session)) &&
    (!groups.length || groups.some(group => group.every(matches => matches(entry))))).map(entry => entry.q);
}

export const actions = {
  search: ['検索欄へ移動', 'Ctrl+F'], results: ['検索結果へ移動', 'Ctrl+J'],
  prev: ['前の問題', 'Ctrl+['], next: ['次の問題', 'Ctrl+]'],
  choice1: ['選択肢1で回答', 'Ctrl+1'], choice2: ['選択肢2で回答', 'Ctrl+2'],
  choice3: ['選択肢3で回答', 'Ctrl+3'], choice4: ['選択肢4で回答', 'Ctrl+4'],
  retry: ['もう一度解く', 'Ctrl+Shift+X'], read: ['読み上げる', 'Ctrl+R'],
  stop: ['読み上げを停止', 'Ctrl+Shift+R'], ask: ['ChatGPTに質問', 'Ctrl+Shift+A'],
  larger: ['文字を拡大', 'Ctrl++'], smaller: ['文字を縮小', 'Ctrl+-']
};

export const defaultShortcuts = Object.fromEntries(Object.entries(actions).map(([id, [, chord]]) => [id, chord]));

export function keyChord(e) {
  if (!e.ctrlKey || e.metaKey || e.isComposing || e.keyCode === 229 || e.getModifierState?.('AltGraph')) return '';
  let key = normalize(e.key).toUpperCase();
  if (key === '=' || e.code === 'NumpadAdd' || key === 'ADD') key = '+';
  if (key === '−' || e.code === 'NumpadSubtract' || key === 'SUBTRACT') key = '-';
  if (!/^[A-Z0-9+[\]\-]$/.test(key)) return '';
  // Plus normally requires Shift on JIS and US keyboards. Treat it as one key.
  return 'Ctrl+' + (e.altKey ? 'Alt+' : '') + (e.shiftKey && key !== '+' ? 'Shift+' : '') + key;
}

const reserved = new Set(['Ctrl+A', 'Ctrl+C', 'Ctrl+V', 'Ctrl+X', 'Ctrl+Y', 'Ctrl+Z', 'Ctrl+W',
  'Ctrl+Shift+C', 'Ctrl+Shift+I', 'Ctrl+Shift+J', 'Ctrl+Shift+V', 'Ctrl+Shift+Z']);

export function shortcutError(id, chord, shortcuts) {
  if (!Object.hasOwn(actions, id) || !/^Ctrl\+(?:Alt\+)?(?:Shift\+)?[A-Z0-9+[\]\-]$/.test(chord) || reserved.has(chord)) {
    return 'Ctrlと英数字・角括弧・＋・−を組み合わせてください。コピーなどの標準操作は使用できません。';
  }
  const other = Object.keys(actions).find(k => k !== id && shortcuts[k] === chord);
  return other ? 'このキーは「' + actions[other][0] + '」で使っています。' : '';
}
