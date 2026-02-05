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

  return jsonResponse({ message: "Not found" }, 404);
}

async function handleAi(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
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
  if (!user) return jsonResponse({ message: "Unauthorized" }, 401);
  const role = getRole(user);
  if (!isRoleAtLeast(role, "admin")) {
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
  const model = body.model || env.GEMINI_MODEL || "gemini-3-flash-preview";
  const limit = await checkRateLimit(env, user.id);
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
    created_by: user.id || null
  });
  return jsonResponse({ text, explanation: parsed.explanation, tags: parsed.tags });
}

async function handleQuestionQa(request, env) {
  const user = await authenticate(request, env);
  if (!user) return jsonResponse({ message: "Unauthorized" }, 401);
  const role = getRole(user);
  if (!isRoleAtLeast(role, "admin")) {
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
  const limit = await checkRateLimit(env, user.id);
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
    created_by: user.id || null
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
    "以下の質問が、この問題に関係するか判定し、関係があればQ&Aを作成してください。",
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
    "- questionは簡潔に整形する",
    "- answerは簡潔に要点を説明する"
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
    `${env.SUPABASE_URL}/rest/v1/question_qa?select=serial&status=eq.ok&limit=10000`,
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
  return jsonResponse({ serials: unique });
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
    `${env.SUPABASE_URL}/rest/v1/deep_dive_explanations?select=serial`,
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
  return jsonResponse({ serials });
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
