/* Browser-local study tools. A handoff always identifies an explicit ordered set. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.AhakiStudy = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function (root) {
  "use strict";
  const SET_PREFIX = "ahaki_study_set_v1:";
  const COPY_KEY = "ahaki_copy_format_v1";
  const SKIP_KEY = "ahaki_skip_answer_confirmation_v1";
  const PRESETS_KEY = "ahaki_search_presets_v1";
  const SET_TTL = 24 * 60 * 60 * 1000;
  const MAX_QUESTIONS = 10720;
  const MAX_SETS = 10;
  const MAX_PRESETS = 30;
  const TOKEN_PATTERN = /^[a-zA-Z0-9_-]{16,80}$/;
  const SERIAL_PATTERN = /^[AB]\d{2,3}-\d{3}$/;
  const FILTER_LIMITS = { keyword: 4000, subject: 200, subtopic: 200, examType: 40, sessionFrom: 8, sessionTo: 8, progress: 40, answered: 40, sort: 40, randomSeed: 80 };
  const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
  const isRecord = value => value !== null && typeof value === "object" && !Array.isArray(value);
  let copyFormat = "questions";
  let copyMemoryOnly = false;
  let skipConfirmation = false;
  let skipMemoryOnly = false;

  function storage() {
    const value = root && root.localStorage;
    if (!value) throw new Error("保存機能が利用できません。");
    return value;
  }
  function newId() {
    if (root && root.crypto && typeof root.crypto.randomUUID === "function") return root.crypto.randomUUID();
    const bytes = new Uint8Array(16);
    if (root && root.crypto && typeof root.crypto.getRandomValues === "function") root.crypto.getRandomValues(bytes);
    else for (let i = 0; i < bytes.length; i += 1) bytes[i] = Math.floor(Math.random() * 256);
    return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
  }
  function orderedSerials(serials, requireUnique = false) {
    if (!Array.isArray(serials) || !serials.length || serials.length > MAX_QUESTIONS) throw new Error("引き継ぐ問題を1〜10,720問の範囲で選んでください。");
    const result = [];
    const seen = new Set();
    for (const serial of serials) {
      if (typeof serial !== "string" || !SERIAL_PATTERN.test(serial)) throw new Error("問題番号の形式が正しくありません。");
      if (seen.has(serial)) {
        if (requireUnique) throw new Error("問題番号が重複しています。");
        continue;
      }
      seen.add(serial);
      result.push(serial);
    }
    return result;
  }
  function readBundle(raw, now = Date.now()) {
    if (typeof raw !== "string" || raw.length > 250000) throw new Error("引き継ぎデータが正しくありません。");
    const value = JSON.parse(raw);
    if (!isRecord(value) || value.version !== 1 || typeof value.title !== "string" || value.title.length > 200 || typeof value.createdAt !== "string") throw new Error("引き継ぎデータが正しくありません。");
    const created = Date.parse(value.createdAt);
    if (!Number.isFinite(created) || created > now + 60000) throw new Error("引き継ぎデータの日時が正しくありません。");
    if (now - created >= SET_TTL) throw new Error("引き継ぎの有効期限（24時間）が切れました。元の画面からもう一度開いてください。");
    return { serials: orderedSerials(value.serials, true), title: value.title, createdAt: value.createdAt };
  }
  function pruneSets(store, currentKey) {
    try {
      const keys = [];
      for (let i = 0; i < store.length; i += 1) {
        const key = store.key(i);
        if (typeof key === "string" && key.startsWith(SET_PREFIX)) keys.push(key);
      }
      const sets = [];
      for (const key of keys) {
        try { sets.push({ key, created: Date.parse(readBundle(store.getItem(key)).createdAt) }); }
        catch (_) { if (key !== currentKey) store.removeItem(key); }
      }
      sets.sort((a, b) => (a.key === currentKey ? -1 : b.key === currentKey ? 1 : b.created - a.created));
      for (const item of sets.slice(MAX_SETS)) store.removeItem(item.key);
    } catch (_) { /* Retention cleanup must not invalidate a successfully saved set. */ }
  }
  function saveHandoff(serials, title = "選択した問題") {
    const list = orderedSerials(serials);
    if (typeof title !== "string") throw new Error("問題一覧の名前が正しくありません。");
    const label = title.trim().slice(0, 200) || "選択した問題";
    const token = newId();
    const key = SET_PREFIX + token;
    const raw = JSON.stringify({ version: 1, serials: list, title: label, createdAt: new Date().toISOString() });
    try {
      const store = storage();
      store.setItem(key, raw);
      if (store.getItem(key) !== raw) throw new Error("保存内容を確認できません。");
      pruneSets(store, key);
    } catch (_) { throw new Error("問題一覧をこのブラウザに保存できませんでした。ブラウザの保存設定や空き容量を確認してください。"); }
    return token;
  }
  function readHandoff(searchString = "") {
    let tokens;
    try { tokens = new URLSearchParams(searchString).getAll("studySet"); }
    catch (_) { return { status: "error", bundle: null, message: "引き継ぎURLが正しくありません。元の画面からもう一度開いてください。" }; }
    if (!tokens.length) return { status: "none", bundle: null, message: "" };
    if (tokens.length !== 1 || !TOKEN_PATTERN.test(tokens[0])) return { status: "error", bundle: null, message: "引き継ぎURLが正しくありません。元の画面からもう一度開いてください。" };
    let raw;
    try { raw = storage().getItem(SET_PREFIX + tokens[0]); }
    catch (_) { return { status: "error", bundle: null, message: "このブラウザの保存データを読み込めません。元の画面からもう一度開いてください。" }; }
    if (raw === null) return { status: "error", bundle: null, message: "引き継ぐ問題一覧が見つかりません。同じブラウザで元の画面からもう一度開いてください。" };
    try { return { status: "ready", bundle: readBundle(raw), message: "" }; }
    catch (error) { return { status: "error", bundle: null, message: error instanceof SyntaxError ? "引き継ぎデータが壊れています。元の画面からもう一度開いてください。" : error.message }; }
  }
  function orderQuestions(allQuestions, serials) {
    const requested = orderedSerials(serials);
    const bySerial = new Map();
    for (const question of Array.isArray(allQuestions) ? allQuestions : []) {
      if (question && typeof question.serial === "string" && !bySerial.has(question.serial)) bySerial.set(question.serial, question);
    }
    const questions = [];
    const missingSerials = [];
    for (const serial of requested) {
      if (bySerial.has(serial)) questions.push(bySerial.get(serial));
      else missingSerials.push(serial);
    }
    return { questions, missingSerials };
  }

  function normalizeCopyFormat(value) {
    return ["questions", "answers", "explanations"].includes(value) ? value : "questions";
  }
  function getCopyFormat() {
    if (!copyMemoryOnly) {
      try { copyFormat = normalizeCopyFormat(storage().getItem(COPY_KEY)); }
      catch (_) { /* Keep this page's choice when browser storage is disabled. */ }
    }
    return copyFormat;
  }
  function setCopyFormat(value) {
    copyFormat = normalizeCopyFormat(value);
    try { storage().setItem(COPY_KEY, copyFormat); copyMemoryOnly = false; return true; }
    catch (_) { copyMemoryOnly = true; return false; }
  }
  function getCopyMode() {
    const value = getCopyFormat();
    return { showAnswer: value !== "questions", showExplanation: value === "explanations" };
  }
  function bindCopyFormat(select, onChange) {
    if (!select) return;
    select.value = getCopyFormat();
    select.addEventListener("change", () => {
      const persisted = setCopyFormat(select.value);
      select.value = getCopyFormat();
      if (typeof onChange === "function") onChange(persisted);
    });
    if (root && typeof root.addEventListener === "function") root.addEventListener("storage", event => {
      if (event.key !== COPY_KEY && event.key !== null) return;
      copyMemoryOnly = false;
      select.value = getCopyFormat();
      if (typeof onChange === "function") onChange(true);
    });
  }
  function getSkipAnswerConfirmation() {
    if (!skipMemoryOnly) {
      try { skipConfirmation = storage().getItem(SKIP_KEY) === "true"; }
      catch (_) { /* Keep this page's choice. */ }
    }
    return skipConfirmation;
  }
  function setSkipAnswerConfirmation(value) {
    skipConfirmation = value === true;
    try { storage().setItem(SKIP_KEY, String(skipConfirmation)); skipMemoryOnly = false; return true; }
    catch (_) { skipMemoryOnly = true; return false; }
  }

  function cleanFilters(value) {
    if (!isRecord(value)) throw new Error("検索条件の形式が正しくありません。");
    const result = {};
    for (const key of Object.keys(FILTER_LIMITS)) {
      if (!own(value, key) || value[key] === null || value[key] === undefined) continue;
      let text = value[key];
      if ((key === "sessionFrom" || key === "sessionTo") && typeof text === "number" && Number.isInteger(text)) text = String(text);
      if (typeof text !== "string" || text.length > FILTER_LIMITS[key]) throw new Error("検索条件の形式または長さが正しくありません。");
      result[key] = text;
    }
    return result;
  }
  function presetName(value) {
    if (typeof value !== "string" || !value.trim() || value.trim().length > 60) throw new Error("名前は1〜60文字で入力してください。");
    return value.trim();
  }
  function readPresets(strict = false) {
    try {
      const raw = storage().getItem(PRESETS_KEY);
      if (raw === null) return [];
      if (typeof raw !== "string" || raw.length > 200000) throw new Error();
      const value = JSON.parse(raw);
      if (!isRecord(value) || value.version !== 1 || !Array.isArray(value.presets) || value.presets.length > MAX_PRESETS) throw new Error();
      const ids = new Set();
      const names = new Set();
      return value.presets.map(item => {
        if (!isRecord(item) || typeof item.id !== "string" || !TOKEN_PATTERN.test(item.id) || typeof item.updatedAt !== "string" || !Number.isFinite(Date.parse(item.updatedAt))) throw new Error();
        const name = presetName(item.name);
        if (ids.has(item.id) || names.has(name)) throw new Error();
        ids.add(item.id);
        names.add(name);
        return { id: item.id, name, filters: cleanFilters(item.filters), updatedAt: item.updatedAt };
      });
    } catch (_) {
      if (strict) throw new Error("保存済みの検索条件を読み込めません。ブラウザの保存設定や保存データを確認してください。");
      return [];
    }
  }
  function writePresets(presets) {
    try {
      const raw = JSON.stringify({ version: 1, presets });
      const store = storage();
      store.setItem(PRESETS_KEY, raw);
      if (store.getItem(PRESETS_KEY) !== raw) throw new Error();
    } catch (_) { throw new Error("検索条件をこのブラウザに保存できませんでした。ブラウザの保存設定や空き容量を確認してください。"); }
  }
  function listSearchPresets() { return readPresets(); }
  function saveSearchPreset(name, filters, id = "") {
    const label = presetName(name);
    const clean = cleanFilters(filters);
    const presets = readPresets(true);
    if (typeof id !== "string" || (id && !TOKEN_PATTERN.test(id))) throw new Error("保存する検索条件のIDが正しくありません。");
    const index = id ? presets.findIndex(item => item.id === id) : -1;
    if (id && index < 0) throw new Error("更新する検索条件が見つかりません。もう一度選び直してください。");
    if (presets.some(item => item.name === label && item.id !== id)) throw new Error("同じ名前の検索条件が保存されています。別の名前を付けてください。");
    if (index < 0 && presets.length >= MAX_PRESETS) throw new Error("検索条件は30件まで保存できます。不要な条件を削除してください。");
    const record = { id: id || newId(), name: label, filters: clean, updatedAt: new Date().toISOString() };
    if (index < 0) presets.push(record);
    else presets[index] = record;
    writePresets(presets);
    return record;
  }
  function deleteSearchPreset(id) {
    if (typeof id !== "string" || !TOKEN_PATTERN.test(id)) throw new Error("削除する検索条件のIDが正しくありません。");
    const presets = readPresets(true);
    writePresets(presets.filter(item => item.id !== id));
  }
  return { readHandoff, orderQuestions, saveHandoff, bindCopyFormat, getCopyMode, getCopyFormat, setCopyFormat, getSkipAnswerConfirmation, setSkipAnswerConfirmation, listSearchPresets, saveSearchPreset, deleteSearchPreset };
});
