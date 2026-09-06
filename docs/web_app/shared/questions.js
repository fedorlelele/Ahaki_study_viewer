/* Shared question contract for the viewer, simple study and print export.
 * Cloud rows remain relevant after local sync until the exported question has
 * incorporated the same override_updated_at. No trust is placed in synced_at.
 */
(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.AhakiQuestions = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";
  const OVERRIDE_FIELDS = "serial, explanation, explanation_source, tags, subtopics, case_text, stem, choices, answer_indices, answer_index, answer_none, updated_at, synced_at";
  const MEDIUM_KEY = "ahaki_answer_medium_v1";
  let currentMedium = "default";
  function normalizeMedium(value) { return value === "braille" ? "braille" : "default"; }
  function getAnswerMedium() {
    try { currentMedium = normalizeMedium(localStorage.getItem(MEDIUM_KEY)); } catch (_) { /* use this page's selection */ }
    return currentMedium;
  }
  function setAnswerMedium(value) {
    const medium = normalizeMedium(value);
    currentMedium = medium;
    try { localStorage.setItem(MEDIUM_KEY, medium); } catch (_) { /* storage is optional */ }
    return medium;
  }
  function bindAnswerMedium(select, onChange) {
    if (!select) return;
    select.value = getAnswerMedium();
    select.addEventListener("change", () => {
      setAnswerMedium(select.value);
      if (onChange) onChange();
    });
    if (typeof window !== "undefined") window.addEventListener("storage", (event) => {
      if (event.key !== MEDIUM_KEY) return;
      select.value = getAnswerMedium();
      if (onChange) onChange();
    });
  }
  function getAnswerIndices(question, medium = getAnswerMedium()) {
    if (!question) return [];
    const variants = question.answer_variants || {};
    const variant = normalizeMedium(medium);
    let values = Array.isArray(variants[variant]) ? variants[variant]
      : Array.isArray(variants.default) ? variants.default : question.answer_indices;
    const hasVariant = Array.isArray(variants[variant]) || Array.isArray(variants.default);
    if (!Array.isArray(values) || (!hasVariant && !values.length)) values = question.answer_index ? [question.answer_index] : [];
    const limit = Array.isArray(question.choices) && question.choices.length ? question.choices.length : 9;
    return [...new Set(values.map(Number).filter(n => Number.isInteger(n) && n >= 1 && n <= limit))];
  }
  function isAnswerCorrect(question, selectedIndex, medium = getAnswerMedium()) {
    if (!question) return false;
    const selected = Number(selectedIndex);
    const limit = Array.isArray(question.choices) ? question.choices.length : 9;
    if (!Number.isInteger(selected) || selected < 1 || selected > limit) return false;
    if (question.answer_none) return true;
    return getAnswerIndices(question, medium).includes(selected);
  }
  function formatAnswerLabel(question, medium = getAnswerMedium(), fullwidth = false) {
    if (!question) return "";
    if (question.answer_none) return "なし";
    let label = getAnswerIndices(question, medium).join("・");
    if (fullwidth) label = label.replace(/[0-9]/g, n => String.fromCharCode(n.charCodeAt(0) + 0xfee0));
    return label;
  }
  function getAnswerNote(question) {
    if (!question) return "";
    const notes = question.answer_notes;
    return Array.isArray(notes) ? notes.filter(Boolean).join(" / ") : String(notes || "");
  }
  function normalizeExplanationStatus(status) {
    const value = String(status || "").trim();
    if (!value) return "";
    const aliases = {
      ai: "ai",
      llm: "ai",
      raw: "ai",
      checked: "teacher_approved",
      approved: "teacher_approved",
      teacher_checked: "teacher_approved",
      teacher_approved: "teacher_approved",
      teacher: "teacher_edited",
      edited: "teacher_edited",
      human: "teacher_edited",
      teacher_edited: "teacher_edited"
    };
    return aliases[value] || "";
  }
  function parseModelExplanationSource(source) {
    const text = String(source || "").trim();
    if (!text.startsWith("model:")) return null;
    let payload = text.slice("model:".length);
    const suffixes = [
      [":teacher_approved", "teacher_approved"],
      [":teacher_edited", "teacher_edited"],
      [":checked", "teacher_approved"],
      [":approved", "teacher_approved"],
      [":teacher", "teacher_edited"],
      [":edited", "teacher_edited"]
    ];
    for (const [suffix, status] of suffixes) {
      if (payload.endsWith(suffix)) {
        return {
          model_name: payload.slice(0, -suffix.length).trim(),
          review_status: status
        };
      }
    }
    return { model_name: payload.trim(), review_status: "ai" };
  }
  function getExplanationMetadata(source, modelName = "", reviewStatus = "") {
    const src = String(source || "").trim();
    let model = String(modelName || "").trim();
    let status = normalizeExplanationStatus(reviewStatus);
    const legacy = {
      llm: ["Gemini3Flash", "ai"],
      ai: ["Gemini3Flash", "ai"],
      llm_checked: ["Gemini3Flash", "teacher_approved"],
      teacher: ["Gemini3Flash", "teacher_edited"],
      human: ["Gemini3Flash", "teacher_edited"],
      llm_teacher: ["Gemini3Flash", "teacher_edited"],
      ai_teacher: ["Gemini3Flash", "teacher_edited"],
      codex_case_text_rewrite_20260616: ["GPT5.5", "ai"],
      codex_case_text_rewrite_20260616_checked: ["GPT5.5", "teacher_approved"],
      codex_case_text_rewrite_20260616_teacher: ["GPT5.5", "teacher_edited"]
    };
    if (legacy[src]) {
      model = model || legacy[src][0];
      status = status || legacy[src][1];
    }
    const parsed = parseModelExplanationSource(src);
    if (parsed) {
      model = model || parsed.model_name;
      status = status || parsed.review_status;
    }
    if (!model && src) {
      const suffixes = [
        ["_checked", "teacher_approved"],
        ["_approved", "teacher_approved"],
        ["_teacher", "teacher_edited"],
        ["_edited", "teacher_edited"]
      ];
      for (const [suffix, nextStatus] of suffixes) {
        if (src.endsWith(suffix)) {
          model = src.slice(0, -suffix.length);
          status = status || nextStatus;
          break;
        }
      }
    }
    if (!model && src) model = src;
    if (!status) status = "ai";
    return { source: src, model_name: model, review_status: status };
  }
  function buildExplanationSource(modelName, reviewStatus = "ai", fallbackSource = "") {
    const model = String(modelName || "").trim();
    const status = normalizeExplanationStatus(reviewStatus) || "ai";
    const fallback = String(fallbackSource || "").trim();
    if (fallback) {
      const meta = getExplanationMetadata(fallback);
      if (meta.model_name === model && meta.review_status === status) return fallback;
    }
    if (model === "GPT5.5") {
      if (status === "teacher_approved") return "codex_case_text_rewrite_20260616_checked";
      if (status === "teacher_edited") return "codex_case_text_rewrite_20260616_teacher";
      return "codex_case_text_rewrite_20260616";
    }
    if (!model || model === "Gemini3Flash") {
      if (status === "teacher_approved") return "llm_checked";
      if (status === "teacher_edited") return "teacher";
      return "llm";
    }
    if (status === "teacher_approved") return `model:${model}:checked`;
    if (status === "teacher_edited") return `model:${model}:teacher`;
    return `model:${model}`;
  }
  function applyExplanationOverride(base, row, merged) {
    const hasBody = row.explanation !== null && row.explanation !== undefined;
    const source = String(row.explanation_source || "").trim();
    if (!hasBody && !source) return;
    const previous = getExplanationMetadata(base.explanation_latest_source, base.explanation_latest_model_name, base.explanation_latest_review_status);
    let meta;
    if (source) {
      // The incoming source describes this revision; old explicit fields belong
      // to the previous revision and must not override a newly named model.
      meta = getExplanationMetadata(source);
      const generic = new Set(["llm", "ai", "llm_checked", "teacher", "human", "llm_teacher", "ai_teacher"]);
      if (generic.has(source) && ["teacher_approved", "teacher_edited"].includes(meta.review_status) && previous.model_name) meta.model_name = previous.model_name;
    } else {
      meta = { ...previous };
      if (hasBody && row.explanation !== base.explanation_latest) meta.review_status = "teacher_edited";
    }
    if (hasBody) merged.explanation_latest = row.explanation;
    merged.explanation_latest_source = buildExplanationSource(meta.model_name, meta.review_status, source || base.explanation_latest_source);
    const resolved = getExplanationMetadata(merged.explanation_latest_source, meta.model_name, meta.review_status);
    merged.explanation_latest_model_name = resolved.model_name;
    merged.explanation_latest_review_status = resolved.review_status;
  }
  function applyQuestionOverride(base, row) {
    if (!base || !row) return base;
    const incomingTime = Date.parse(row.updated_at || "");
    const appliedTime = Date.parse(base.override_updated_at || "");
    if (Number.isFinite(incomingTime) && Number.isFinite(appliedTime) && incomingTime <= appliedTime) return base;
    const merged = { ...base };
    for (const key of ["tags", "subtopics", "case_text", "stem", "choices"]) {
      if (row[key] !== null && row[key] !== undefined) merged[key] = row[key];
    }
    applyExplanationOverride(base, row, merged);
    if (["answer_indices", "answer_index"].some(key => row[key] !== null && row[key] !== undefined) || row.answer_none === true) {
      const indices = Array.isArray(row.answer_indices) ? row.answer_indices.map(Number) : row.answer_index ? [Number(row.answer_index)] : [];
      merged.answer_indices = row.answer_none ? [] : indices;
      merged.answer_index = merged.answer_indices[0] || null;
      merged.answer_none = Boolean(row.answer_none);
      merged.answer_variants = { default: merged.answer_indices.slice() };
      merged.answer_text = merged.answer_none ? "なし" : merged.answer_indices.join("・");
      merged.answer_notes = [];
    }
    if (row.updated_at) merged.override_updated_at = row.updated_at;
    return merged;
  }
  function applyQuestionOverrides(questions, rows) {
    const map = new Map((rows || []).map(row => [row.serial, row]));
    return (questions || []).map(question => applyQuestionOverride(question, map.get(question.serial)));
  }
  function buildQuestionIndexes(questions) {
    const result = { bySubject: {}, byTag: {}, bySubtopic: {} };
    for (const q of questions || []) {
      if (!q || !q.serial) continue;
      for (const [key, values] of [["bySubject", [q.subject]], ["byTag", q.tags || []], ["bySubtopic", q.subtopics || []]]) {
        for (const label of new Set(values.filter(Boolean))) {
          if (!result[key][label]) result[key][label] = [];
          result[key][label].push(q.serial);
        }
      }
    }
    return result;
  }
  function applyIndexOverrides(indexes, rows, versions = {}) {
    for (const row of rows || []) {
      const incoming = Date.parse(row.updated_at || "");
      const applied = Date.parse(versions[row.serial] || "");
      if (Number.isFinite(incoming) && Number.isFinite(applied) && incoming <= applied) continue;
      for (const [key, field] of [["byTag", "tags"], ["bySubtopic", "subtopics"]]) {
        if (!Array.isArray(row[field]) || !indexes[key]) continue;
        const index = indexes[key];
        for (const label of Object.keys(index)) index[label] = index[label].filter(serial => serial !== row.serial);
        for (const label of new Set(row[field].filter(Boolean))) {
          if (!index[label]) index[label] = [];
          index[label].push(row.serial);
        }
      }
    }
    return indexes;
  }
  function withTimeout(promise, milliseconds = 8000, onTimeout) {
    let timer;
    return Promise.race([
      Promise.resolve(promise),
      new Promise((_, reject) => { timer = setTimeout(() => {
        if (onTimeout) onTimeout();
        reject(new Error("通信がタイムアウトしました"));
      }, milliseconds); })
    ]).finally(() => clearTimeout(timer));
  }
  async function loadQuestionOverrides(client, options = {}) {
    if (!client) return [];
    const pageSize = options.pageSize || 500;
    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    const work = (async () => {
      const rows = [];
      for (let offset = 0; ; offset += pageSize) {
        if (controller && controller.signal.aborted) throw new Error("訂正データの取得を中断しました");
        let query = client.from("question_overrides").select(OVERRIDE_FIELDS).order("serial", { ascending: true }).range(offset, offset + pageSize - 1);
        if (controller && typeof query.abortSignal === "function") query = query.abortSignal(controller.signal);
        const { data, error } = await query;
        if (error) throw error;
        if (!Array.isArray(data)) throw new Error("訂正データの形式が不正です");
        rows.push(...data);
        if (data.length < pageSize) return rows;
      }
    })();
    return withTimeout(work, options.timeoutMs || 8000, () => controller && controller.abort());
  }
  async function loadAnswerStats(base, serials, options = {}) {
    if (!base) throw new Error("回答集計の取得先が未設定です");
    const fetcher = options.fetch || fetch;
    const unique = [...new Set((serials || []).filter(Boolean))];
    const all = [];
    for (let offset = 0; offset < unique.length; offset += 200) {
      const batch = unique.slice(offset, offset + 200);
      const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      const data = await withTimeout((async () => {
        const response = await fetcher(`${base.replace(/\/$/, "")}/stats/answers?serials=${encodeURIComponent(batch.join(","))}`, controller ? { signal: controller.signal } : {});
        if (!response.ok) throw new Error(`回答集計の取得に失敗しました (${response.status})`);
        return response.json();
      })(), options.timeoutMs || 8000, () => controller && controller.abort());
      if (!data || data.ok !== true || !Array.isArray(data.items)) throw new Error("回答集計の形式が不正です");
      const bySerial = new Map(data.items.map(item => [item.serial, item]));
      for (const serial of batch) {
        const item = bySerial.get(serial);
        if (!item || !Number.isFinite(Number(item.total)) || !Number.isFinite(Number(item.correct)) || Number(item.total) < Number(item.correct) || Number(item.correct) < 0) throw new Error("回答集計が不足しています");
        all.push(item);
      }
    }
    return all;
  }
  function createRenderGuard() {
    let generation = 0;
    return { begin() { const current = ++generation; return () => generation === current; }, cancel() { generation += 1; } };
  }
  function createAnswerEventId() {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
    return `answer_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  }
  function manifestVersionText(manifest) {
    return manifest && manifest.generated_at ? `問題データ版: ${manifest.generated_at}` : "問題データ版: 不明";
  }
  return { OVERRIDE_FIELDS, MEDIUM_KEY, normalizeMedium, getAnswerMedium, setAnswerMedium, bindAnswerMedium, getAnswerIndices, isAnswerCorrect, formatAnswerLabel, getAnswerNote, normalizeExplanationStatus, parseModelExplanationSource, getExplanationMetadata, buildExplanationSource, applyQuestionOverride, applyQuestionOverrides, buildQuestionIndexes, applyIndexOverrides, withTimeout, loadQuestionOverrides, loadAnswerStats, createRenderGuard, createAnswerEventId, manifestVersionText };
});
