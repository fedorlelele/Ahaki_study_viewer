const JSON_HEADERS = { "Content-Type": "application/json; charset=utf-8" };
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Authorization,Content-Type"
};

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const path = url.pathname;

      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: CORS_HEADERS });
      }
      if (path.startsWith("/admin/")) {
        return handleAdmin(request, env);
      }
      if (path.startsWith("/ai/")) {
        return handleAi(request, env);
      }
      if (path.startsWith("/progress/")) {
        return handleProgress(request, env);
      }
      return jsonResponse({ message: "Not found" }, 404);
    } catch (_err) {
      return jsonResponse({ message: "Internal error" }, 500);
    }
  }
};

async function handleAdmin(request, env) {
  const user = await authenticate(request, env);
  if (!user) return jsonResponse({ message: "Unauthorized" }, 401);
  const role = getRole(user);

  const url = new URL(request.url);
  const path = url.pathname;

  if (path === "/admin/users") {
    if (!isRoleAtLeast(role, "teacher")) {
      return jsonResponse({ message: "Forbidden" }, 403);
    }
    const page = Number(url.searchParams.get("page") || "1");
    const limit = Number(url.searchParams.get("limit") || "200");
    return listUsers(env, page, limit);
  }

  if (path === "/admin/role") {
    if (!isRoleAtLeast(role, "admin")) {
      return jsonResponse({ message: "Forbidden" }, 403);
    }
    if (request.method !== "POST") {
      return jsonResponse({ message: "Method not allowed" }, 405);
    }
    const body = await readJson(request);
    return updateRole(env, body, user);
  }

  if (path === "/admin/disable") {
    if (!isRoleAtLeast(role, "admin")) {
      return jsonResponse({ message: "Forbidden" }, 403);
    }
    if (request.method !== "POST") {
      return jsonResponse({ message: "Method not allowed" }, 405);
    }
    const body = await readJson(request);
    return updateDisable(env, body, user);
  }

  if (path === "/admin/role_changes") {
    if (!isRoleAtLeast(role, "admin")) {
      return jsonResponse({ message: "Forbidden" }, 403);
    }
    const limit = Number(url.searchParams.get("limit") || "200");
    return listRoleChanges(env, limit);
  }

  if (path === "/admin/ai_generation") {
    if (!isRoleAtLeast(role, "admin")) {
      return jsonResponse({ message: "Forbidden" }, 403);
    }
    if (request.method === "GET") {
      const enabled = await getAiGenerationEnabled(env);
      return jsonResponse({ ok: true, public_generation: enabled });
    }
    if (request.method === "POST") {
      const body = await readJson(request);
      const enabled = Boolean(body.public_generation);
      return setAiGenerationEnabled(env, enabled, user);
    }
    return jsonResponse({ message: "Method not allowed" }, 405);
  }

  return jsonResponse({ message: "Not found" }, 404);
}

async function handleAi(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  if (request.method === "GET" && path === "/ai/config") {
    const enabled = await getAiGenerationEnabled(env);
    return jsonResponse({ public_generation: enabled });
  }
  if (request.method === "GET" && path === "/ai/deep_dive_index") {
    return fetchDeepDiveIndex(env);
  }
  if (path === "/ai/question_qa") {
    if (request.method === "GET") {
      const serial = (url.searchParams.get("serial") || "").trim();
      if (!serial) {
        return jsonResponse({ message: "serial is required" }, 400);
      }
      return fetchQuestionQa(env, serial);
    }
    if (request.method === "POST") {
      return handleQuestionQa(request, env);
    }
  }
  if (path === "/ai/question_qa_index" && request.method === "GET") {
    return fetchQuestionQaIndex(env);
  }
  if (path === "/ai/question_qa/view" && request.method === "POST") {
    const body = await readJson(request);
    return incrementQaCounter(env, body, "view_count");
  }
  if (path === "/ai/question_qa/like" && request.method === "POST") {
    const body = await readJson(request);
    return incrementQaCounter(env, body, "like_count");
  }
  if (request.method === "GET" && path === "/ai/deep_dive") {
    const serial = (url.searchParams.get("serial") || "").trim();
    if (!serial) {
      return jsonResponse({ message: "serial is required" }, 400);
    }
    return fetchDeepDive(env, serial);
  }

  if (request.method !== "POST") {
    return jsonResponse({ message: "Method not allowed" }, 405);
  }
  const user = await authenticate(request, env);
  const role = user ? getRole(user) : "";
  const allowPublic = await getAiGenerationEnabled(env);
  if (!allowPublic && !isRoleAtLeast(role, "admin")) {
    return jsonResponse({ message: "Forbidden" }, 403);
  }
  const body = await readJson(request);
  const prompt = (body.prompt || "").trim();
  const serial = (body.serial || "").trim();
  if (!prompt) {
    return jsonResponse({ message: "Prompt is required" }, 400);
  }
  if (!serial) {
    return jsonResponse({ message: "serial is required" }, 400);
  }
  const exists = await hasDeepDive(env, serial);
  if (exists) {
    return jsonResponse({ message: "深掘り解説は既に保存されています。" }, 409);
  }
  const model = body.model || env.GEMINI_MODEL || "gemini-3-flash-preview";
  const limit = await checkRateLimit(env, getRateLimitActor(request, user));
  if (!limit.ok) {
    return jsonResponse({ message: limit.message }, 429);
  }
  const payload = {
    contents: [
      {
        role: "user",
        parts: [{ text: prompt }]
      }
    ]
  };
  const resp = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${env.GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }
  );
  if (!resp.ok) {
    const errorText = await resp.text();
    return jsonResponse({ message: "Gemini API error", detail: errorText }, 500);
  }
  const data = await resp.json();
  const text =
    data?.candidates?.[0]?.content?.parts?.map(part => part.text).join("") || "";
  const parsed = parseDeepDive(text);
  await upsertDeepDive(env, {
    serial,
    explanation: parsed.explanation || "",
    tags: parsed.tags || [],
    created_by: user && user.id ? user.id : null
  });
  return jsonResponse({ text, explanation: parsed.explanation, tags: parsed.tags });
}

async function handleProgress(request, env) {
  try {
    const user = await authenticate(request, env);
    if (!user) return jsonResponse({ message: "Unauthorized" }, 401);
    const role = getRole(user);
    const url = new URL(request.url);
    const path = url.pathname;

  if (path === "/progress/bulk" && request.method === "POST") {
    const body = await readJson(request);
    const targetUserId = resolveProgressTargetUserId(body.user_id, user, role);
    if (!targetUserId) return jsonResponse({ message: "Forbidden" }, 403);
    const serials = Array.isArray(body.serials)
      ? body.serials.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const items = await fetchUserProgressBySerials(env, targetUserId, serials);
    return jsonResponse({ ok: true, items });
  }

  if (path === "/progress/list" && request.method === "GET") {
    const targetUserId = resolveProgressTargetUserId(url.searchParams.get("user_id"), user, role);
    if (!targetUserId) return jsonResponse({ message: "Forbidden" }, 403);
    const items = await fetchAllUserProgress(env, targetUserId);
    return jsonResponse({ ok: true, items });
  }

  if (path === "/progress/summary" && request.method === "GET") {
    const targetUserId = resolveProgressTargetUserId(url.searchParams.get("user_id"), user, role);
    if (!targetUserId) return jsonResponse({ message: "Forbidden" }, 403);
    const summary = await buildProgressSummary(env, targetUserId);
    return jsonResponse({ ok: true, summary });
  }

  if (path === "/progress/users_summary" && request.method === "GET") {
    if (!isRoleAtLeast(role, "teacher")) return jsonResponse({ message: "Forbidden" }, 403);
    const items = await buildUsersProgressSummary(env);
    return jsonResponse({ ok: true, items });
  }

  if (path === "/progress/review_queue" && request.method === "GET") {
    const targetUserId = resolveProgressTargetUserId(url.searchParams.get("user_id"), user, role);
    if (!targetUserId) return jsonResponse({ message: "Forbidden" }, 403);
    const limit = Math.max(1, Math.min(100, Number(url.searchParams.get("limit") || "20")));
    const items = await buildReviewQueue(env, targetUserId, limit);
    return jsonResponse({ ok: true, items });
  }

  if (path === "/progress/goals" && request.method === "GET") {
    const targetUserId = resolveProgressTargetUserId(url.searchParams.get("user_id"), user, role);
    if (!targetUserId) return jsonResponse({ message: "Forbidden" }, 403);
    const goals = await fetchUserGoals(env, targetUserId);
    return jsonResponse({ ok: true, goals });
  }

  if (path === "/progress/goals" && request.method === "POST") {
    const body = await readJson(request);
    const targetUserId = resolveProgressTargetUserId(body.user_id, user, role);
    if (!targetUserId) return jsonResponse({ message: "Forbidden" }, 403);
    const goals = await upsertUserGoals(env, targetUserId, body);
    return jsonResponse({ ok: true, goals });
  }

  if (path === "/progress/status" && request.method === "POST") {
    const body = await readJson(request);
    const targetUserId = resolveProgressTargetUserId(body.user_id, user, role);
    if (!targetUserId) return jsonResponse({ message: "Forbidden" }, 403);
    const serial = String(body.serial || "").trim();
    const status = normalizeProgressStatus(body.status);
    if (!serial || !status) {
      return jsonResponse({ message: "serial と status は必須です。" }, 400);
    }
    const current = await fetchUserProgressBySerials(env, targetUserId, [serial]);
    const prev = current && current[0] ? current[0] : null;
    const now = new Date().toISOString();
    const next = {
      user_id: targetUserId,
      serial,
      status,
      attempt_count: Number(prev?.attempt_count || 0),
      correct_count: Number(prev?.correct_count || 0),
      last_answered_at: prev?.last_answered_at || null,
      last_is_correct: prev?.last_is_correct ?? null,
      next_review_at:
        status === "needs_review"
          ? now
          : status === "mastered"
            ? plusDaysIso(now, 7)
            : null,
      updated_at: now
    };
    const saved = await upsertUserProgress(env, next);
    return jsonResponse({ ok: true, item: saved });
  }

  if (path === "/progress/answer" && request.method === "POST") {
    const body = await readJson(request);
    const serial = String(body.serial || "").trim();
    const isCorrect = Boolean(body.is_correct);
    if (!serial) return jsonResponse({ message: "serial は必須です。" }, 400);
    const current = await fetchUserProgressBySerials(env, user.id, [serial]);
    const prev = current && current[0] ? current[0] : null;
    const attemptCount = Number(prev?.attempt_count || 0) + 1;
    const correctCount = Number(prev?.correct_count || 0) + (isCorrect ? 1 : 0);
    const status = isCorrect ? "mastered" : "needs_review";
    const now = new Date().toISOString();
    const nextReviewAt =
      status === "needs_review"
        ? now
        : status === "mastered"
          ? plusDaysIso(now, 7)
          : plusDaysIso(now, 2);
    const next = {
      user_id: user.id,
      serial,
      status,
      attempt_count: attemptCount,
      correct_count: correctCount,
      last_answered_at: now,
      last_is_correct: isCorrect,
      next_review_at: nextReviewAt,
      updated_at: now
    };
    const saved = await upsertUserProgress(env, next);
    return jsonResponse({ ok: true, item: saved });
  }

  if (path === "/progress/import_local" && request.method === "POST") {
    const body = await readJson(request);
    const items = Array.isArray(body.items) ? body.items : [];
    const dedup = {};
    items.forEach((item) => {
      const serial = String(item && item.serial ? item.serial : "").trim();
      if (!serial) return;
      dedup[serial] = Boolean(item && item.is_correct);
    });
    const serials = Object.keys(dedup);
    if (!serials.length) return jsonResponse({ ok: true, inserted: 0, items: [] });
    const existing = await fetchUserProgressBySerials(env, user.id, serials);
    const existingSet = new Set((existing || []).map((row) => row.serial));
    const now = new Date().toISOString();
    const pending = [];
    serials.forEach((serial) => {
      if (existingSet.has(serial)) return;
      const isCorrect = dedup[serial];
      pending.push({
        user_id: user.id,
        serial,
        status: isCorrect ? "mastered" : "needs_review",
        attempt_count: 1,
        correct_count: isCorrect ? 1 : 0,
        last_answered_at: now,
        last_is_correct: isCorrect,
        next_review_at: isCorrect ? plusDaysIso(now, 7) : now,
        updated_at: now
      });
    });
    if (!pending.length) return jsonResponse({ ok: true, inserted: 0, items: [] });
    const inserted = await upsertUserProgressMany(env, pending);
    return jsonResponse({ ok: true, inserted: inserted.length, items: inserted });
  }

    return jsonResponse({ message: "Not found" }, 404);
  } catch (err) {
    return jsonResponse(
      {
        message: "Progress API error",
        detail: err && err.message ? err.message : String(err || "")
      },
      500
    );
  }
}

async function hasDeepDive(env, serial) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/deep_dive_explanations?select=serial&serial=eq.${encodeURIComponent(serial)}&limit=1`,
    {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) return false;
  const rows = await resp.json();
  return Array.isArray(rows) && rows.length > 0;
}

async function handleQuestionQa(request, env) {
  const user = await authenticate(request, env);
  const role = user ? getRole(user) : "";
  const allowPublic = await getAiGenerationEnabled(env);
  if (!allowPublic && !isRoleAtLeast(role, "admin")) {
    return jsonResponse({ message: "Forbidden" }, 403);
  }
  const body = await readJson(request);
  const serial = (body.serial || "").trim();
  const question = (body.question || "").trim();
  if (!serial || !question) {
    return jsonResponse({ message: "serial and question are required" }, 400);
  }
  const prompt = buildQuestionQaPrompt(body);
  const model = body.model || env.GEMINI_MODEL || "gemini-3-flash-preview";
  const limit = await checkRateLimit(env, getRateLimitActor(request, user));
  if (!limit.ok) {
    return jsonResponse({ message: limit.message }, 429);
  }
  const payload = {
    contents: [
      {
        role: "user",
        parts: [{ text: prompt }]
      }
    ]
  };
  const resp = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${env.GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }
  );
  if (!resp.ok) {
    const errorText = await resp.text();
    return jsonResponse({ message: "Gemini API error", detail: errorText }, 500);
  }
  const data = await resp.json();
  const text =
    data?.candidates?.[0]?.content?.parts?.map(part => part.text).join("") || "";
  const parsed = parseQuestionQa(text);
  const status = parsed.status || "irrelevant";
  await saveQuestionQa(env, {
    serial,
    status,
    question: parsed.question || question,
    answer: parsed.answer || "",
    created_by: user && user.id ? user.id : null
  });
  return jsonResponse({
    status,
    question: parsed.question || question,
    answer: parsed.answer || ""
  });
}

function buildQuestionQaPrompt(body) {
  const caseText = body.case_text || "（なし）";
  const stem = body.stem || "";
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const answer = body.answer || "";
  const explanation = body.explanation || "";
  const question = body.question || "";
  return [
    "あなたは医療系国家試験問題に対するQ&A作成AIです。",
    "以下の質問が、この問題に関係するか判定し、関係があれば学習者向けの深掘りQ&Aを作成してください。",
    "",
    "【問題】",
    "症例文:",
    caseText,
    "",
    "問題文:",
    stem,
    "",
    "選択肢:",
    ...choices.map((c, i) => `${i + 1}. ${c}`),
    "",
    "解答:",
    answer,
    "",
    "解説:",
    explanation,
    "",
    "【ユーザーの質問】",
    question,
    "",
    "【出力形式（JSONのみ）】",
    "{\"status\":\"ok|irrelevant\",\"question\":\"整形した質問\",\"answer\":\"回答\"}",
    "",
    "ルール:",
    "- 無関係なら status=irrelevant とし、answerは空でよい",
    "- 関係がある場合は status=ok",
    "- questionは自然な日本語に整形する",
    "- answerは短くしすぎず、学習が深まる説明にする",
    "- answerは次の流れで書く: 1)結論 2)理由・機序 3)各選択肢の判断ポイント 4)臨床での見方や注意点",
    "- 必要に応じて症例文の情報(年齢・症状・所見)を引用して根拠を示す",
    "- 回答は日本語で、目安として300〜700文字程度",
    "- 冗長な前置きや免責だけの文章は避ける",
    "- 断定できない内容は推測と明記する"
  ].join("\n");
}

function parseQuestionQa(text) {
  if (!text) return { status: "irrelevant", question: "", answer: "" };
  try {
    const jsonStart = text.indexOf("{");
    const jsonEnd = text.lastIndexOf("}");
    if (jsonStart >= 0 && jsonEnd > jsonStart) {
      const json = JSON.parse(text.slice(jsonStart, jsonEnd + 1));
      return {
        status: json.status || "irrelevant",
        question: json.question || "",
        answer: json.answer || ""
      };
    }
  } catch (_err) {
    // fall through
  }
  return { status: "irrelevant", question: "", answer: "" };
}

async function saveQuestionQa(env, record) {
  const payload = {
    serial: record.serial,
    status: record.status,
    question: record.question || "",
    answer: record.answer || "",
    created_by: record.created_by || null,
    view_count: 0,
    like_count: 0
  };
  await fetch(`${env.SUPABASE_URL}/rest/v1/question_qa`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });
}

async function fetchQuestionQa(env, serial) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/question_qa?select=id,serial,question,answer,view_count,like_count,created_at&serial=eq.${encodeURIComponent(serial)}&status=eq.ok&order=like_count.desc&order=view_count.desc&order=created_at.desc`,
    {
      headers: {
        apikey: env.SUPABASE_ANON_KEY,
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase fetch failed", detail }, 500);
  }
  const rows = await resp.json();
  return jsonResponse({ items: rows || [] });
}

async function fetchQuestionQaIndex(env) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/question_qa?select=serial,created_at&status=eq.ok&order=created_at.desc&limit=10000`,
    {
      headers: {
        apikey: env.SUPABASE_ANON_KEY,
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase fetch failed", detail }, 500);
  }
  const rows = await resp.json();
  const serials = Array.isArray(rows) ? rows.map(row => row.serial).filter(Boolean) : [];
  const unique = Array.from(new Set(serials));
  const latestBySerial = {};
  if (Array.isArray(rows)) {
    rows.forEach((row) => {
      if (!row || !row.serial || !row.created_at) return;
      if (!latestBySerial[row.serial] || row.created_at > latestBySerial[row.serial]) {
        latestBySerial[row.serial] = row.created_at;
      }
    });
  }
  return jsonResponse({ serials: unique, latest_by_serial: latestBySerial });
}

async function incrementQaCounter(env, body, field) {
  const id = body.id;
  if (!id) return jsonResponse({ message: "id required" }, 400);
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/question_qa?select=id,${field}&id=eq.${encodeURIComponent(id)}`,
    {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) return jsonResponse({ message: "Supabase fetch failed" }, 500);
  const rows = await resp.json();
  const current = rows && rows[0] ? rows[0][field] || 0 : 0;
  const next = current + 1;
  const update = await fetch(
    `${env.SUPABASE_URL}/rest/v1/question_qa?id=eq.${encodeURIComponent(id)}`,
    {
      method: "PATCH",
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ [field]: next })
    }
  );
  if (!update.ok) return jsonResponse({ message: "Supabase update failed" }, 500);
  return jsonResponse({ ok: true, [field]: next });
}

const PROGRESS_STATUSES = new Set(["unstarted", "in_progress", "mastered", "needs_review"]);

function normalizeProgressStatus(status) {
  const value = String(status || "").trim().toLowerCase();
  if (value === "in_progress") return "mastered";
  return PROGRESS_STATUSES.has(value) ? value : "";
}

function resolveProgressTargetUserId(requestedUserId, user, role) {
  const requested = String(requestedUserId || "").trim();
  if (!requested || requested === user.id) return user.id;
  if (!isRoleAtLeast(role, "teacher")) return "";
  return requested;
}

function plusDaysIso(baseIso, days) {
  const date = new Date(baseIso || Date.now());
  date.setDate(date.getDate() + Number(days || 0));
  return date.toISOString();
}

function buildInClause(values) {
  const quoted = values.map((value) => `"${String(value).replace(/"/g, '\\"')}"`).join(",");
  return encodeURIComponent(`(${quoted})`);
}

async function fetchUserProgressBySerials(env, userId, serials) {
  const cleanSerials = Array.isArray(serials)
    ? serials.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  if (!cleanSerials.length) return [];
  const url =
    `${env.SUPABASE_URL}/rest/v1/user_progress` +
    `?select=user_id,serial,status,attempt_count,correct_count,last_answered_at,last_is_correct,next_review_at,updated_at,created_at` +
    `&user_id=eq.${encodeURIComponent(userId)}` +
    `&serial=in.${buildInClause(cleanSerials)}`;
  const resp = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!resp.ok) return [];
  const rows = (await resp.json()) || [];
  rows.forEach((row) => {
    if (!row) return;
    row.status = normalizeProgressStatus(row.status) || "unstarted";
  });
  return rows;
}

async function fetchAllUserProgress(env, userId) {
  const url =
    `${env.SUPABASE_URL}/rest/v1/user_progress` +
    `?select=user_id,serial,status,attempt_count,correct_count,last_answered_at,last_is_correct,next_review_at,updated_at,created_at` +
    `&user_id=eq.${encodeURIComponent(userId)}` +
    `&order=updated_at.desc&limit=20000`;
  const resp = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!resp.ok) return [];
  const rows = (await resp.json()) || [];
  rows.forEach((row) => {
    if (!row) return;
    row.status = normalizeProgressStatus(row.status) || "unstarted";
  });
  return rows;
}

async function upsertUserProgress(env, payload) {
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/user_progress`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=representation"
    },
    body: JSON.stringify(payload)
  });
  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`progress upsert failed: ${detail}`);
  }
  const rows = await resp.json();
  return rows && rows[0] ? rows[0] : payload;
}

async function upsertUserProgressMany(env, payloads) {
  if (!Array.isArray(payloads) || !payloads.length) return [];
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/user_progress`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=representation"
    },
    body: JSON.stringify(payloads)
  });
  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`progress batch upsert failed: ${detail}`);
  }
  return (await resp.json()) || [];
}

async function fetchUserGoals(env, userId) {
  const url =
    `${env.SUPABASE_URL}/rest/v1/user_goals` +
    `?select=user_id,weekly_answer_target,weekly_review_target,target_mastery_rate,updated_at,created_at` +
    `&user_id=eq.${encodeURIComponent(userId)}&limit=1`;
  const resp = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!resp.ok) return null;
  const rows = await resp.json();
  return rows && rows[0] ? rows[0] : null;
}

async function upsertUserGoals(env, userId, body) {
  const payload = {
    user_id: userId,
    weekly_answer_target: Math.max(0, Number(body.weekly_answer_target || 0)),
    weekly_review_target: Math.max(0, Number(body.weekly_review_target || 0)),
    target_mastery_rate: Math.max(0, Math.min(100, Number(body.target_mastery_rate || 0))),
    updated_at: new Date().toISOString()
  };
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/user_goals`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=representation"
    },
    body: JSON.stringify(payload)
  });
  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`goals upsert failed: ${detail}`);
  }
  const rows = await resp.json();
  return rows && rows[0] ? rows[0] : payload;
}

async function buildProgressSummary(env, userId) {
  const items = await fetchAllUserProgress(env, userId);
  const now = Date.now();
  const weekMs = 7 * 24 * 60 * 60 * 1000;
  const byStatus = {
    unstarted: 0,
    in_progress: 0,
    mastered: 0,
    needs_review: 0
  };
  let totalAttempts = 0;
  let totalCorrect = 0;
  let weekAnswered = 0;
  let reviewDue = 0;
  items.forEach((row) => {
    const status = normalizeProgressStatus(row.status) || "unstarted";
    byStatus[status] = (byStatus[status] || 0) + 1;
    totalAttempts += Number(row.attempt_count || 0);
    totalCorrect += Number(row.correct_count || 0);
    const answeredAt = row.last_answered_at ? Date.parse(row.last_answered_at) : 0;
    if (answeredAt && now - answeredAt <= weekMs) weekAnswered += 1;
    const nextReviewAt = row.next_review_at ? Date.parse(row.next_review_at) : 0;
    if (status === "needs_review" || (nextReviewAt && nextReviewAt <= now)) {
      reviewDue += 1;
    }
  });
  const goals = await fetchUserGoals(env, userId);
  return {
    user_id: userId,
    total_items: items.length,
    by_status: byStatus,
    total_attempts: totalAttempts,
    total_correct: totalCorrect,
    accuracy: totalAttempts > 0 ? totalCorrect / totalAttempts : 0,
    weekly_answered: weekAnswered,
    review_due: reviewDue,
    goals: goals || null
  };
}

async function buildUsersProgressSummary(env) {
  const url =
    `${env.SUPABASE_URL}/rest/v1/user_progress` +
    `?select=user_id,status,attempt_count,correct_count,last_answered_at,next_review_at&limit=50000`;
  const resp = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!resp.ok) return [];
  const rows = (await resp.json()) || [];
  const now = Date.now();
  const map = {};
  rows.forEach((row) => {
    const userId = row.user_id || "";
    if (!userId) return;
    if (!map[userId]) {
      map[userId] = {
        user_id: userId,
        total_items: 0,
        review_due: 0,
        total_attempts: 0,
        total_correct: 0
      };
    }
    const target = map[userId];
    target.total_items += 1;
    target.total_attempts += Number(row.attempt_count || 0);
    target.total_correct += Number(row.correct_count || 0);
    const status = normalizeProgressStatus(row.status) || "unstarted";
    const nextReviewAt = row.next_review_at ? Date.parse(row.next_review_at) : 0;
    if (status === "needs_review" || (nextReviewAt && nextReviewAt <= now)) {
      target.review_due += 1;
    }
  });
  return Object.values(map)
    .map((item) => ({
      ...item,
      accuracy: item.total_attempts > 0 ? item.total_correct / item.total_attempts : 0
    }))
    .sort((a, b) => (b.review_due - a.review_due) || (b.total_attempts - a.total_attempts));
}

async function buildReviewQueue(env, userId, limit) {
  const items = await fetchAllUserProgress(env, userId);
  const now = Date.now();
  const queued = items
    .filter((row) => {
      const status = normalizeProgressStatus(row.status) || "unstarted";
      if (status === "needs_review") return true;
      const nextReviewAt = row.next_review_at ? Date.parse(row.next_review_at) : 0;
      return nextReviewAt > 0 && nextReviewAt <= now;
    })
    .map((row) => {
      const attempts = Number(row.attempt_count || 0);
      const correct = Number(row.correct_count || 0);
      const missRate = attempts > 0 ? (attempts - correct) / attempts : 0;
      return { ...row, miss_rate: missRate };
    })
    .sort((a, b) => {
      const statusA = normalizeProgressStatus(a.status);
      const statusB = normalizeProgressStatus(b.status);
      if (statusA === "needs_review" && statusB !== "needs_review") return -1;
      if (statusB === "needs_review" && statusA !== "needs_review") return 1;
      const missDiff = Number(b.miss_rate || 0) - Number(a.miss_rate || 0);
      if (missDiff !== 0) return missDiff;
      return String(b.updated_at || "").localeCompare(String(a.updated_at || ""));
    })
    .slice(0, limit);
  return queued;
}

async function authenticate(request, env) {
  const auth = request.headers.get("Authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (!token) return null;
  const resp = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${token}`
    }
  });
  if (!resp.ok) return null;
  return await resp.json();
}

function getRole(user) {
  const role =
    user?.app_metadata?.role ||
    user?.user_metadata?.role ||
    "";
  return role === "student" || role === "teacher" || role === "admin" ? role : "";
}

function isRoleAtLeast(role, target) {
  const rank = roleRank(role);
  return rank >= roleRank(target);
}

function roleRank(role) {
  if (role === "admin") return 3;
  if (role === "teacher") return 2;
  if (role === "student") return 1;
  return 0;
}

function isPublicGenerationEnabled(env) {
  const value = String(env.AI_PUBLIC_GENERATION || "").toLowerCase().trim();
  return value === "1" || value === "true" || value === "on" || value === "yes";
}

async function getAiGenerationEnabled(env) {
  const fallback = isPublicGenerationEnabled(env);
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/app_settings?select=setting_key,setting_value&setting_key=eq.ai_public_generation&limit=1`,
    {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) return fallback;
  const rows = await resp.json();
  const row = rows && rows[0] ? rows[0] : null;
  if (!row || row.setting_value === undefined || row.setting_value === null) return fallback;
  if (typeof row.setting_value === "boolean") return row.setting_value;
  const text = String(row.setting_value).toLowerCase().trim();
  return text === "1" || text === "true" || text === "on" || text === "yes";
}

async function setAiGenerationEnabled(env, enabled, actor) {
  const key = "ai_public_generation";
  await fetch(`${env.SUPABASE_URL}/rest/v1/app_settings?setting_key=eq.${encodeURIComponent(key)}`, {
    method: "DELETE",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  const payload = {
    setting_key: key,
    setting_value: enabled,
    updated_at: new Date().toISOString(),
    updated_by: actor?.id || null
  };
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/app_settings`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase update failed", detail }, 500);
  }
  return jsonResponse({ ok: true, public_generation: enabled });
}

function getRateLimitActor(request, user) {
  if (user && user.id) return `user:${user.id}`;
  const ip =
    request.headers.get("CF-Connecting-IP") ||
    (request.headers.get("X-Forwarded-For") || "").split(",")[0].trim() ||
    "";
  return ip ? `ip:${ip}` : "anon";
}

function parseDeepDive(text) {
  if (!text) return { explanation: "", tags: [] };
  try {
    const jsonStart = text.indexOf("{");
    const jsonEnd = text.lastIndexOf("}");
    if (jsonStart >= 0 && jsonEnd > jsonStart) {
      const json = JSON.parse(text.slice(jsonStart, jsonEnd + 1));
      const tags = Array.isArray(json.tags)
        ? json.tags.map(tag => String(tag).trim()).filter(Boolean)
        : [];
      return { explanation: json.explanation || "", tags };
    }
  } catch (_err) {
    // fall through
  }
  return { explanation: text, tags: [] };
}

async function fetchDeepDive(env, serial) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/deep_dive_explanations?select=serial,explanation,tags,updated_at&serial=eq.${encodeURIComponent(serial)}&limit=1`,
    {
      headers: {
        apikey: env.SUPABASE_ANON_KEY,
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase fetch failed", detail }, 500);
  }
  const rows = await resp.json();
  if (!rows || !rows.length) {
    return jsonResponse({ ok: true, found: false });
  }
  const row = rows[0];
  return jsonResponse({ ok: true, found: true, data: row });
}

async function fetchDeepDiveIndex(env) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/deep_dive_explanations?select=serial,updated_at&order=updated_at.desc`,
    {
      headers: {
        apikey: env.SUPABASE_ANON_KEY,
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase fetch failed", detail }, 500);
  }
  const rows = await resp.json();
  const serials = Array.isArray(rows) ? rows.map(row => row.serial).filter(Boolean) : [];
  const latestBySerial = {};
  if (Array.isArray(rows)) {
    rows.forEach((row) => {
      if (!row || !row.serial || !row.updated_at) return;
      if (!latestBySerial[row.serial] || row.updated_at > latestBySerial[row.serial]) {
        latestBySerial[row.serial] = row.updated_at;
      }
    });
  }
  return jsonResponse({ serials, latest_by_serial: latestBySerial });
}

async function upsertDeepDive(env, record) {
  const now = new Date().toISOString();
  const payload = {
    serial: record.serial,
    explanation: record.explanation || "",
    tags: record.tags || [],
    updated_at: now,
    created_by: record.created_by || null
  };
  await fetch(`${env.SUPABASE_URL}/rest/v1/deep_dive_explanations`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates"
    },
    body: JSON.stringify(payload)
  });
}

async function listUsers(env, page, limit) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/auth/v1/admin/users?page=${page}&per_page=${limit}`,
    {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  const data = await resp.json();
  if (!resp.ok) {
    return jsonResponse({ message: "Supabase admin error", detail: data }, 500);
  }
  const flags = await fetchUserFlags(env);
  const users = (data.users || []).map(user => {
    const role = getRole(user);
    const flag = flags[user.id] || {};
    return {
      id: user.id,
      email: user.email || "",
      role,
      created_at: user.created_at || "",
      last_sign_in_at: user.last_sign_in_at || "",
      disabled: Boolean(flag.disabled)
    };
  });
  return jsonResponse({ ok: true, users, count: users.length });
}

async function fetchUserFlags(env) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/user_flags?select=user_id,disabled`,
    {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  if (!resp.ok) return {};
  const data = await resp.json();
  const map = {};
  (data || []).forEach(row => {
    if (row.user_id) map[row.user_id] = row;
  });
  return map;
}

async function updateRole(env, body, actor) {
  const userId = body.user_id || "";
  const nextRole = body.role || "";
  if (!userId) return jsonResponse({ message: "user_id required" }, 400);
  const beforeRole = await fetchUserRole(env, userId);
  const resp = await fetch(`${env.SUPABASE_URL}/auth/v1/admin/users/${userId}`, {
    method: "PUT",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ app_metadata: { role: nextRole || null } })
  });
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase update failed", detail }, 500);
  }
  await logRoleChange(env, {
    target_user_id: userId,
    before_role: beforeRole || null,
    after_role: nextRole || null,
    changed_by: actor?.id || null
  });
  return jsonResponse({ ok: true });
}

async function fetchUserRole(env, userId) {
  const resp = await fetch(`${env.SUPABASE_URL}/auth/v1/admin/users/${userId}`, {
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!resp.ok) return "";
  const data = await resp.json();
  return getRole(data);
}

async function updateDisable(env, body, actor) {
  const userId = body.user_id || "";
  const disabled = Boolean(body.disabled);
  if (!userId) return jsonResponse({ message: "user_id required" }, 400);
  await fetch(`${env.SUPABASE_URL}/rest/v1/user_flags?user_id=eq.${userId}`, {
    method: "DELETE",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!disabled) {
    return jsonResponse({ ok: true });
  }
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/user_flags`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      user_id: userId,
      disabled: true,
      note: body.note || "",
      updated_at: new Date().toISOString(),
      updated_by: actor?.id || null
    })
  });
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase update failed", detail }, 500);
  }
  return jsonResponse({ ok: true });
}

async function listRoleChanges(env, limit) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/role_changes?select=target_user_id,before_role,after_role,changed_by,created_at&order=created_at.desc&limit=${limit}`,
    {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );
  const data = await resp.json();
  if (!resp.ok) {
    return jsonResponse({ message: "Supabase query failed", detail: data }, 500);
  }
  return jsonResponse({ ok: true, items: data || [] });
}

async function logRoleChange(env, payload) {
  await fetch(`${env.SUPABASE_URL}/rest/v1/role_changes`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });
}

async function readJson(request) {
  try {
    return await request.json();
  } catch (_err) {
    return {};
  }
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { ...JSON_HEADERS, ...CORS_HEADERS }
  });
}

async function checkRateLimit(env, userId) {
  if (!env.RATE_LIMIT_PER_DAY && !env.RATE_LIMIT_PER_MIN) {
    return { ok: true };
  }
  const now = new Date();
  const dayKey = `${userId}:${now.toISOString().slice(0, 10)}`;
  const minKey = `${userId}:${now.toISOString().slice(0, 16)}`;
  const dayLimit = Number(env.RATE_LIMIT_PER_DAY || 0);
  const minLimit = Number(env.RATE_LIMIT_PER_MIN || 0);

  if (dayLimit > 0) {
    const allowed = await bumpRate(dayKey, dayLimit, 60 * 60 * 24);
    if (!allowed) {
      return { ok: false, message: "1日の利用上限に達しました。" };
    }
  }
  if (minLimit > 0) {
    const allowed = await bumpRate(minKey, minLimit, 60 * 2);
    if (!allowed) {
      return { ok: false, message: "短時間の利用上限に達しました。" };
    }
  }
  return { ok: true };

  async function bumpRate(key, limit, ttlSeconds) {
    const cacheKey = new Request(`https://rate/${key}`);
    const cached = await caches.default.match(cacheKey);
    const current = cached ? Number(await cached.text()) : 0;
    if (current >= limit) return false;
    const next = current + 1;
    await caches.default.put(
      cacheKey,
      new Response(String(next), {
        headers: { "Cache-Control": `max-age=${ttlSeconds}` }
      })
    );
    return true;
  }
}
