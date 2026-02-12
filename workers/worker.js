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
      const adminPaid = await getAiAdminPaidEnabled(env);
      return jsonResponse({ ok: true, public_generation: enabled, admin_paid_generation: adminPaid });
    }
    if (request.method === "POST") {
      const body = await readJson(request);
      const updates = {};
      if (Object.prototype.hasOwnProperty.call(body, "public_generation")) {
        updates.public_generation = Boolean(body.public_generation);
      }
      if (Object.prototype.hasOwnProperty.call(body, "admin_paid_generation")) {
        updates.admin_paid_generation = Boolean(body.admin_paid_generation);
      }
      return setAiGenerationSettings(env, updates, user);
    }
    return jsonResponse({ message: "Method not allowed" }, 405);
  }

  if (path === "/admin/ai_usage") {
    if (!isRoleAtLeast(role, "admin")) {
      return jsonResponse({ message: "Forbidden" }, 403);
    }
    if (request.method !== "GET") {
      return jsonResponse({ message: "Method not allowed" }, 405);
    }
    const days = Number(url.searchParams.get("days") || "30");
    return listAiUsage(env, days);
  }

  return jsonResponse({ message: "Not found" }, 404);
}

async function handleAi(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  if (request.method === "GET" && path === "/ai/config") {
    const enabled = await getAiGenerationEnabled(env);
    const adminPaid = await getAiAdminPaidEnabled(env);
    return jsonResponse({ public_generation: enabled, admin_paid_generation: adminPaid });
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
  if (path === "/ai/tag_deep_dive") {
    if (request.method === "GET") {
      const tag = (url.searchParams.get("tag") || "").trim();
      if (!tag) {
        return jsonResponse({ message: "tag is required" }, 400);
      }
      return fetchTagDeepDive(env, tag);
    }
    if (request.method === "POST") {
      return handleTagDeepDive(request, env);
    }
  }
  if (path === "/ai/tag_qa") {
    if (request.method === "GET") {
      const tag = (url.searchParams.get("tag") || "").trim();
      if (!tag) {
        return jsonResponse({ message: "tag is required" }, 400);
      }
      return fetchTagQa(env, tag);
    }
    if (request.method === "POST") {
      return handleTagQa(request, env);
    }
  }
  if (path === "/ai/tag_qa/view" && request.method === "POST") {
    const body = await readJson(request);
    return incrementTagQaCounter(env, body, "view_count");
  }
  if (path === "/ai/tag_qa/like" && request.method === "POST") {
    const body = await readJson(request);
    return incrementTagQaCounter(env, body, "like_count");
  }
  if (path === "/ai/tag_views" && request.method === "GET") {
    const tags = (url.searchParams.get("tags") || "")
      .split(",")
      .map((item) => String(item || "").trim())
      .filter(Boolean);
    return fetchTagViewCounts(env, tags);
  }
  if (path === "/ai/tag_view" && request.method === "POST") {
    const body = await readJson(request);
    return incrementTagView(env, body);
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
  const gemini = await resolveGeminiRoute(env, role);
  if (!gemini.apiKey) {
    return jsonResponse({ message: "Gemini API key is not configured." }, 500);
  }
  const requestedModel = body.model || gemini.defaultModel || "gemini-3-flash-preview";
  const fallbackModel = "gemini-3-flash-preview";
  const limit = await checkRateLimit(env, getRateLimitActor(request, user), gemini.mode);
  if (!limit.ok) {
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "deep_dive",
      outcome: "rate_limited",
      serial,
      user_id: user && user.id ? user.id : null,
      model: requestedModel
    });
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
  const callGemini = (model) =>
    fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${gemini.apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }
    );
  let usedModel = requestedModel;
  let resp = await callGemini(usedModel);
  if (!resp.ok && resp.status === 404 && usedModel !== fallbackModel) {
    usedModel = fallbackModel;
    resp = await callGemini(usedModel);
  }
  if (!resp.ok) {
    const errorText = await resp.text();
    const parsedError = parseGeminiError(errorText);
    if (isGeminiRateLimit(resp.status, parsedError)) {
      await safeLogAiUsage(env, {
        mode: gemini.mode,
        endpoint: "deep_dive",
        outcome: "rate_limited",
        serial,
        user_id: user && user.id ? user.id : null,
        model: usedModel
      });
      return jsonResponse(
        {
          message: "Gemini API rate limit",
          model: usedModel,
          mode: gemini.mode,
          detail: parsedError.detail
        },
        429
      );
    }
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "deep_dive",
      outcome: "error",
      serial,
      user_id: user && user.id ? user.id : null,
      model: usedModel
    });
    return jsonResponse(
      { message: "Gemini API error", model: usedModel, mode: gemini.mode, detail: parsedError.detail },
      500
    );
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
  await safeLogAiUsage(env, {
    mode: gemini.mode,
    endpoint: "deep_dive",
    outcome: "success",
    serial,
    user_id: user && user.id ? user.id : null,
    model: usedModel
  });
  return jsonResponse({ text, explanation: parsed.explanation, tags: parsed.tags, mode: gemini.mode });
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

async function hasTagDeepDive(env, tag) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/tag_deep_dive_explanations?select=tag&tag=eq.${encodeURIComponent(tag)}&limit=1`,
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

function parseGeminiError(errorText) {
  const raw = String(errorText || "");
  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch (_err) {
    parsed = null;
  }
  const detail =
    (parsed &&
      parsed.error &&
      (parsed.error.message || parsed.error.status || parsed.error.code)) ||
    raw;
  const status =
    (parsed && parsed.error && (parsed.error.status || "")) ||
    "";
  return {
    raw,
    detail: String(detail || ""),
    status: String(status || "")
  };
}

function isGeminiRateLimit(respStatus, parsedError) {
  if (respStatus === 429) return true;
  const detail = String(parsedError?.detail || "").toLowerCase();
  const status = String(parsedError?.status || "").toUpperCase();
  return (
    status === "RESOURCE_EXHAUSTED" ||
    detail.includes("resource_exhausted") ||
    detail.includes("quota") ||
    detail.includes("rate limit") ||
    detail.includes("too many requests") ||
    detail.includes("429")
  );
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
  const gemini = await resolveGeminiRoute(env, role);
  if (!gemini.apiKey) {
    return jsonResponse({ message: "Gemini API key is not configured." }, 500);
  }
  const requestedModel = body.model || gemini.defaultModel || "gemini-3-flash-preview";
  const fallbackModel = "gemini-3-flash-preview";
  const limit = await checkRateLimit(env, getRateLimitActor(request, user), gemini.mode);
  if (!limit.ok) {
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "question_qa",
      outcome: "rate_limited",
      serial,
      user_id: user && user.id ? user.id : null,
      model: requestedModel
    });
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
  const callGemini = (model) =>
    fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${gemini.apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }
    );
  let usedModel = requestedModel;
  let resp = await callGemini(usedModel);
  if (!resp.ok && resp.status === 404 && usedModel !== fallbackModel) {
    usedModel = fallbackModel;
    resp = await callGemini(usedModel);
  }
  if (!resp.ok) {
    const errorText = await resp.text();
    const parsedError = parseGeminiError(errorText);
    if (isGeminiRateLimit(resp.status, parsedError)) {
      await safeLogAiUsage(env, {
        mode: gemini.mode,
        endpoint: "question_qa",
        outcome: "rate_limited",
        serial,
        user_id: user && user.id ? user.id : null,
        model: usedModel
      });
      return jsonResponse(
        {
          message: "Gemini API rate limit",
          model: usedModel,
          mode: gemini.mode,
          detail: parsedError.detail
        },
        429
      );
    }
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "question_qa",
      outcome: "error",
      serial,
      user_id: user && user.id ? user.id : null,
      model: usedModel
    });
    return jsonResponse(
      {
        message: "Gemini API error",
        model: usedModel,
        mode: gemini.mode,
        detail: parsedError.detail
      },
      500
    );
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
  await safeLogAiUsage(env, {
    mode: gemini.mode,
    endpoint: "question_qa",
    outcome: status === "ok" ? "success" : "irrelevant",
    serial,
    user_id: user && user.id ? user.id : null,
    model: usedModel
  });
  return jsonResponse({
    status,
    question: parsed.question || question,
    answer: parsed.answer || "",
    mode: gemini.mode
  });
}

async function handleTagDeepDive(request, env) {
  const user = await authenticate(request, env);
  const role = user ? getRole(user) : "";
  const allowPublic = await getAiGenerationEnabled(env);
  if (!allowPublic && !isRoleAtLeast(role, "admin")) {
    return jsonResponse({ message: "Forbidden" }, 403);
  }
  const body = await readJson(request);
  const tag = String(body.tag || "").trim();
  const force = Boolean(body.force);
  if (!tag) {
    return jsonResponse({ message: "tag is required" }, 400);
  }
  const exists = await hasTagDeepDive(env, tag);
  if (exists && !force) {
    return jsonResponse({ message: "タグ深掘り解説は既に保存されています。" }, 409);
  }
  const prompt = buildTagDeepDivePrompt(body);
  const gemini = await resolveGeminiRoute(env, role);
  if (!gemini.apiKey) {
    return jsonResponse({ message: "Gemini API key is not configured." }, 500);
  }
  const requestedModel = body.model || gemini.defaultModel || "gemini-3-flash-preview";
  const fallbackModel = "gemini-3-flash-preview";
  const limit = await checkRateLimit(env, getRateLimitActor(request, user), gemini.mode);
  if (!limit.ok) {
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "tag_deep_dive",
      outcome: "rate_limited",
      serial: `tag:${tag}`,
      user_id: user && user.id ? user.id : null,
      model: requestedModel
    });
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
  const callGemini = (model) =>
    fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${gemini.apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }
    );
  let usedModel = requestedModel;
  let resp = await callGemini(usedModel);
  if (!resp.ok && resp.status === 404 && usedModel !== fallbackModel) {
    usedModel = fallbackModel;
    resp = await callGemini(usedModel);
  }
  if (!resp.ok) {
    const errorText = await resp.text();
    const parsedError = parseGeminiError(errorText);
    if (isGeminiRateLimit(resp.status, parsedError)) {
      await safeLogAiUsage(env, {
        mode: gemini.mode,
        endpoint: "tag_deep_dive",
        outcome: "rate_limited",
        serial: `tag:${tag}`,
        user_id: user && user.id ? user.id : null,
        model: usedModel
      });
      return jsonResponse(
        {
          message: "Gemini API rate limit",
          model: usedModel,
          mode: gemini.mode,
          detail: parsedError.detail
        },
        429
      );
    }
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "tag_deep_dive",
      outcome: "error",
      serial: `tag:${tag}`,
      user_id: user && user.id ? user.id : null,
      model: usedModel
    });
    return jsonResponse(
      {
        message: "Gemini API error",
        model: usedModel,
        mode: gemini.mode,
        detail: parsedError.detail
      },
      500
    );
  }
  const data = await resp.json();
  const text =
    data?.candidates?.[0]?.content?.parts?.map(part => part.text).join("") || "";
  const parsed = parseTagDeepDive(text);
  const updatedAt = await upsertTagDeepDive(env, {
    tag,
    explanation: parsed.explanation || text || "",
    created_by: user && user.id ? user.id : null,
    model: usedModel
  });
  await safeLogAiUsage(env, {
    mode: gemini.mode,
    endpoint: "tag_deep_dive",
    outcome: "success",
    serial: `tag:${tag}`,
    user_id: user && user.id ? user.id : null,
    model: usedModel
  });
  return jsonResponse({
    tag,
    explanation: parsed.explanation || text || "",
    mode: gemini.mode,
    model: usedModel,
    updated_at: updatedAt
  });
}

function buildTagDeepDivePrompt(body) {
  const tag = String(body.tag || "").trim();
  const canonical = String(body.canonical_tag || tag || "").trim();
  const description = String(body.description || "").trim();
  const aliases = Array.isArray(body.aliases)
    ? body.aliases.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const relatedTags = Array.isArray(body.related_tags)
    ? body.related_tags.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const subjects = Array.isArray(body.subjects)
    ? body.subjects.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const subtopics = Array.isArray(body.subtopics)
    ? body.subtopics.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const sampleSerials = Array.isArray(body.sample_serials)
    ? body.sample_serials.map((v) => String(v || "").trim()).filter(Boolean)
    : [];

  return [
    "あなたは医療系国家試験のタグ学習ガイド作成AIです。",
    "以下のタグ情報をもとに、学習者が用語を深く理解できるような解説を作成してください。",
    "",
    "【タグ情報】",
    `タグ: ${tag || "（不明）"}`,
    `正規タグ: ${canonical || "（不明）"}`,
    `説明: ${description || "（未登録）"}`,
    `別名: ${aliases.length ? aliases.join(" / ") : "（なし）"}`,
    `関連タグ: ${relatedTags.length ? relatedTags.join(" / ") : "（なし）"}`,
    `科目: ${subjects.length ? subjects.join(" / ") : "（なし）"}`,
    `小項目: ${subtopics.length ? subtopics.join(" / ") : "（なし）"}`,
    `関連問題シリアル例: ${sampleSerials.length ? sampleSerials.join(", ") : "（なし）"}`,
    "",
    "【出力要件】",
    "- explanation はMarkdownで作成する",
    "- 構成例: 1)定義 2)病態・機序 3)関連疾患/鑑別 4)試験での問われ方 5)覚え方",
    "- 医学的に不確かな内容は断定しすぎない",
    "- 日本語で、目安700〜1800文字",
    "",
    "【出力形式（JSONのみ）】",
    "{\"explanation\":\"...\"}"
  ].join("\n");
}

function parseTagDeepDive(text) {
  if (!text) return { explanation: "" };
  try {
    const jsonStart = text.indexOf("{");
    const jsonEnd = text.lastIndexOf("}");
    if (jsonStart >= 0 && jsonEnd > jsonStart) {
      const json = JSON.parse(text.slice(jsonStart, jsonEnd + 1));
      return { explanation: String(json.explanation || "").trim() };
    }
  } catch (_err) {
    // fall through
  }
  return { explanation: String(text || "").trim() };
}

async function handleTagQa(request, env) {
  const user = await authenticate(request, env);
  const role = user ? getRole(user) : "";
  const allowPublic = await getAiGenerationEnabled(env);
  if (!allowPublic && !isRoleAtLeast(role, "admin")) {
    return jsonResponse({ message: "Forbidden" }, 403);
  }
  const body = await readJson(request);
  const tag = String(body.tag || "").trim();
  const question = String(body.question || "").trim();
  if (!tag || !question) {
    return jsonResponse({ message: "tag and question are required" }, 400);
  }
  const prompt = buildTagQaPrompt(body);
  const gemini = await resolveGeminiRoute(env, role);
  if (!gemini.apiKey) {
    return jsonResponse({ message: "Gemini API key is not configured." }, 500);
  }
  const requestedModel = body.model || gemini.defaultModel || "gemini-3-flash-preview";
  const fallbackModel = "gemini-3-flash-preview";
  const limit = await checkRateLimit(env, getRateLimitActor(request, user), gemini.mode);
  if (!limit.ok) {
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "tag_qa",
      outcome: "rate_limited",
      serial: `tag:${tag}`,
      user_id: user && user.id ? user.id : null,
      model: requestedModel
    });
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
  const callGemini = (model) =>
    fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${gemini.apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }
    );
  let usedModel = requestedModel;
  let resp = await callGemini(usedModel);
  if (!resp.ok && resp.status === 404 && usedModel !== fallbackModel) {
    usedModel = fallbackModel;
    resp = await callGemini(usedModel);
  }
  if (!resp.ok) {
    const errorText = await resp.text();
    const parsedError = parseGeminiError(errorText);
    if (isGeminiRateLimit(resp.status, parsedError)) {
      await safeLogAiUsage(env, {
        mode: gemini.mode,
        endpoint: "tag_qa",
        outcome: "rate_limited",
        serial: `tag:${tag}`,
        user_id: user && user.id ? user.id : null,
        model: usedModel
      });
      return jsonResponse(
        {
          message: "Gemini API rate limit",
          model: usedModel,
          mode: gemini.mode,
          detail: parsedError.detail
        },
        429
      );
    }
    await safeLogAiUsage(env, {
      mode: gemini.mode,
      endpoint: "tag_qa",
      outcome: "error",
      serial: `tag:${tag}`,
      user_id: user && user.id ? user.id : null,
      model: usedModel
    });
    return jsonResponse(
      {
        message: "Gemini API error",
        model: usedModel,
        mode: gemini.mode,
        detail: parsedError.detail
      },
      500
    );
  }
  const data = await resp.json();
  const text =
    data?.candidates?.[0]?.content?.parts?.map(part => part.text).join("") || "";
  const parsed = parseQuestionQa(text);
  const status = parsed.status || "irrelevant";
  await saveTagQa(env, {
    tag,
    status,
    question: parsed.question || question,
    answer: parsed.answer || "",
    created_by: user && user.id ? user.id : null
  });
  await safeLogAiUsage(env, {
    mode: gemini.mode,
    endpoint: "tag_qa",
    outcome: status === "ok" ? "success" : "irrelevant",
    serial: `tag:${tag}`,
    user_id: user && user.id ? user.id : null,
    model: usedModel
  });
  return jsonResponse({
    status,
    question: parsed.question || question,
    answer: parsed.answer || "",
    mode: gemini.mode
  });
}

function buildTagQaPrompt(body) {
  const tag = String(body.tag || "").trim();
  const question = String(body.question || "").trim();
  const description = String(body.description || "").trim();
  const aliases = Array.isArray(body.aliases)
    ? body.aliases.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const relatedTags = Array.isArray(body.related_tags)
    ? body.related_tags.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const subjects = Array.isArray(body.subjects)
    ? body.subjects.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const subtopics = Array.isArray(body.subtopics)
    ? body.subtopics.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const sampleSerials = Array.isArray(body.sample_serials)
    ? body.sample_serials.map((v) => String(v || "").trim()).filter(Boolean)
    : [];

  return [
    "あなたは医療系国家試験のタグ学習Q&A作成AIです。",
    "質問がタグ学習に関係するか判定し、関係がある場合は学習が深まる回答を作成してください。",
    "",
    "【タグ情報】",
    `タグ: ${tag || "（不明）"}`,
    `説明: ${description || "（未登録）"}`,
    `別名: ${aliases.length ? aliases.join(" / ") : "（なし）"}`,
    `関連タグ: ${relatedTags.length ? relatedTags.join(" / ") : "（なし）"}`,
    `科目: ${subjects.length ? subjects.join(" / ") : "（なし）"}`,
    `小項目: ${subtopics.length ? subtopics.join(" / ") : "（なし）"}`,
    `関連問題シリアル例: ${sampleSerials.length ? sampleSerials.join(", ") : "（なし）"}`,
    "",
    "【ユーザーの質問】",
    question || "（なし）",
    "",
    "【出力形式（JSONのみ）】",
    "{\"status\":\"ok|irrelevant\",\"question\":\"整形した質問\",\"answer\":\"回答\"}",
    "",
    "ルール:",
    "- 無関係なら status=irrelevant, answerは空にする",
    "- 関係がある場合は status=ok",
    "- question は自然な日本語へ整形する",
    "- answer はMarkdownで、結論→理由→覚え方の順で簡潔にまとめる",
    "- 回答は日本語、目安400〜1000文字"
  ].join("\n");
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
    "- answerはMarkdownで作成する（見出し・箇条書き・太字を適切に使う）",
    "- answerは短くしすぎず、学習が深まる説明にする",
    "- answerは次の流れで書く: 1)結論 2)理由・機序 3)各選択肢の判断ポイント 4)臨床での見方や注意点 5)覚え方や確認問題",
    "- 見出し例: 「## 結論」「## 理由と機序」「## 選択肢の判断」「## つまずきやすい点」",
    "- 必要に応じて症例文の情報(年齢・症状・所見)を引用して根拠を示す",
    "- 回答は日本語で、目安として500〜1200文字程度",
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

async function saveTagQa(env, record) {
  const payload = {
    tag: record.tag,
    status: record.status,
    question: record.question || "",
    answer: record.answer || "",
    created_by: record.created_by || null,
    view_count: 0,
    like_count: 0
  };
  await fetch(`${env.SUPABASE_URL}/rest/v1/tag_qa`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });
}

async function fetchTagQa(env, tag) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/tag_qa?select=id,tag,question,answer,view_count,like_count,created_at&tag=eq.${encodeURIComponent(tag)}&status=eq.ok&order=like_count.desc&order=view_count.desc&order=created_at.desc`,
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

async function incrementTagQaCounter(env, body, field) {
  const id = body.id;
  if (!id) return jsonResponse({ message: "id required" }, 400);
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/tag_qa?select=id,${field}&id=eq.${encodeURIComponent(id)}`,
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
    `${env.SUPABASE_URL}/rest/v1/tag_qa?id=eq.${encodeURIComponent(id)}`,
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

function buildInClause(values) {
  const quoted = values.map((value) => `"${String(value).replace(/"/g, '\\"')}"`).join(",");
  return encodeURIComponent(`(${quoted})`);
}

async function fetchTagViewCounts(env, tags) {
  const clean = Array.isArray(tags)
    ? tags.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  if (!clean.length) return jsonResponse({ counts: {} });
  const query =
    `${env.SUPABASE_URL}/rest/v1/tag_view_stats` +
    `?select=tag,view_count&tag=in.${buildInClause(clean)}`;
  const resp = await fetch(query, {
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!resp.ok) {
    const detail = await resp.text();
    return jsonResponse({ message: "Supabase fetch failed", detail }, 500);
  }
  const rows = (await resp.json()) || [];
  const counts = {};
  clean.forEach((tag) => {
    counts[tag] = 0;
  });
  rows.forEach((row) => {
    const tag = String(row.tag || "").trim();
    if (!tag) return;
    counts[tag] = Number(row.view_count || 0);
  });
  return jsonResponse({ counts });
}

async function incrementTagView(env, body) {
  const tag = String(body && body.tag ? body.tag : "").trim();
  if (!tag) return jsonResponse({ message: "tag required" }, 400);
  const selectUrl =
    `${env.SUPABASE_URL}/rest/v1/tag_view_stats` +
    `?select=tag,view_count&tag=eq.${encodeURIComponent(tag)}&limit=1`;
  const resp = await fetch(selectUrl, {
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  if (!resp.ok) return jsonResponse({ message: "Supabase fetch failed" }, 500);
  const rows = (await resp.json()) || [];
  const current = rows[0] ? Number(rows[0].view_count || 0) : 0;
  const next = current + 1;
  const now = new Date().toISOString();
  if (!rows.length) {
    const insert = await fetch(`${env.SUPABASE_URL}/rest/v1/tag_view_stats`, {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "resolution=merge-duplicates"
      },
      body: JSON.stringify({ tag, view_count: next, updated_at: now })
    });
    if (!insert.ok) return jsonResponse({ message: "Supabase insert failed" }, 500);
    return jsonResponse({ ok: true, tag, view_count: next });
  }
  const update = await fetch(
    `${env.SUPABASE_URL}/rest/v1/tag_view_stats?tag=eq.${encodeURIComponent(tag)}`,
    {
      method: "PATCH",
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ view_count: next, updated_at: now })
    }
  );
  if (!update.ok) return jsonResponse({ message: "Supabase update failed" }, 500);
  return jsonResponse({ ok: true, tag, view_count: next });
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

function isAdminPaidGenerationEnabled(env) {
  const value = String(env.AI_ADMIN_PAID_GENERATION || "").toLowerCase().trim();
  return value === "1" || value === "true" || value === "on" || value === "yes";
}

async function getAppSettingBool(env, key, fallback) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/app_settings?select=setting_key,setting_value&setting_key=eq.${encodeURIComponent(key)}&limit=1`,
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

async function setAppSettingBool(env, key, enabled, actor) {
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
    throw new Error(detail || "Supabase update failed");
  }
}

async function getAiGenerationEnabled(env) {
  const fallback = isPublicGenerationEnabled(env);
  return getAppSettingBool(env, "ai_public_generation", fallback);
}

async function getAiAdminPaidEnabled(env) {
  const fallback = isAdminPaidGenerationEnabled(env);
  return getAppSettingBool(env, "ai_admin_paid_generation", fallback);
}

async function setAiGenerationSettings(env, updates, actor) {
  try {
    let publicGeneration = await getAiGenerationEnabled(env);
    let adminPaidGeneration = await getAiAdminPaidEnabled(env);
    if (Object.prototype.hasOwnProperty.call(updates, "public_generation")) {
      publicGeneration = Boolean(updates.public_generation);
      await setAppSettingBool(env, "ai_public_generation", publicGeneration, actor);
    }
    if (Object.prototype.hasOwnProperty.call(updates, "admin_paid_generation")) {
      adminPaidGeneration = Boolean(updates.admin_paid_generation);
      await setAppSettingBool(env, "ai_admin_paid_generation", adminPaidGeneration, actor);
    }
    return jsonResponse({
      ok: true,
      public_generation: publicGeneration,
      admin_paid_generation: adminPaidGeneration
    });
  } catch (err) {
    return jsonResponse({ message: "Supabase update failed", detail: String(err.message || err) }, 500);
  }
}

async function resolveGeminiRoute(env, role) {
  const adminPaidEnabled = await getAiAdminPaidEnabled(env);
  const hasPaidKey = Boolean(env.GEMINI_API_KEY_PAID);
  const usePaid = role === "admin" && adminPaidEnabled && hasPaidKey;
  const apiKey = usePaid ? env.GEMINI_API_KEY_PAID : env.GEMINI_API_KEY_FREE || env.GEMINI_API_KEY;
  const defaultModel = usePaid
    ? env.GEMINI_MODEL_PAID || env.GEMINI_MODEL || "gemini-3-flash-preview"
    : env.GEMINI_MODEL_FREE || env.GEMINI_MODEL || "gemini-3-flash-preview";
  return {
    mode: usePaid ? "paid" : "free",
    apiKey,
    defaultModel
  };
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

async function fetchTagDeepDive(env, tag) {
  const resp = await fetch(
    `${env.SUPABASE_URL}/rest/v1/tag_deep_dive_explanations?select=tag,explanation,updated_at,model&tag=eq.${encodeURIComponent(tag)}&limit=1`,
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
  return jsonResponse({ ok: true, found: true, data: rows[0] });
}

async function upsertTagDeepDive(env, record) {
  const now = new Date().toISOString();
  const payload = {
    tag: record.tag,
    explanation: record.explanation || "",
    updated_at: now,
    created_by: record.created_by || null,
    model: record.model || ""
  };
  await fetch(`${env.SUPABASE_URL}/rest/v1/tag_deep_dive_explanations`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates"
    },
    body: JSON.stringify(payload)
  });
  return now;
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

function createAiUsageBucket() {
  return {
    all: 0,
    success: 0,
    irrelevant: 0,
    rate_limited: 0,
    error: 0
  };
}

function normalizeAiUsageMode(mode) {
  return mode === "paid" ? "paid" : "free";
}

function normalizeAiUsageOutcome(outcome) {
  const value = String(outcome || "").trim().toLowerCase();
  if (value === "success") return "success";
  if (value === "irrelevant") return "irrelevant";
  if (value === "rate_limited" || value === "rate-limit" || value === "ratelimited") return "rate_limited";
  return "error";
}

async function listAiUsage(env, days) {
  const safeDays = Math.max(1, Math.min(90, Number(days || 30)));
  const since = new Date(Date.now() - safeDays * 24 * 60 * 60 * 1000).toISOString();
  const url =
    `${env.SUPABASE_URL}/rest/v1/ai_usage_logs` +
    `?select=created_at,mode,endpoint,outcome` +
    `&created_at=gte.${encodeURIComponent(since)}` +
    `&order=created_at.desc&limit=20000`;
  const resp = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    }
  });
  const data = await resp.json();
  if (!resp.ok) {
    return jsonResponse({ message: "Supabase query failed", detail: data }, 500);
  }

  const rows = Array.isArray(data) ? data : [];
  const totals = {
    free: createAiUsageBucket(),
    paid: createAiUsageBucket()
  };
  const byDayMap = {};
  const byEndpointMap = {};

  rows.forEach((row) => {
    const mode = normalizeAiUsageMode(row && row.mode);
    const outcome = normalizeAiUsageOutcome(row && row.outcome);
    const endpoint = String((row && row.endpoint) || "").trim() || "unknown";
    const date = String((row && row.created_at) || "").slice(0, 10) || "";

    totals[mode].all += 1;
    totals[mode][outcome] += 1;

    if (date) {
      if (!byDayMap[date]) {
        byDayMap[date] = {
          date,
          free: createAiUsageBucket(),
          paid: createAiUsageBucket()
        };
      }
      byDayMap[date][mode].all += 1;
      byDayMap[date][mode][outcome] += 1;
    }

    if (!byEndpointMap[endpoint]) {
      byEndpointMap[endpoint] = {
        endpoint,
        free: createAiUsageBucket(),
        paid: createAiUsageBucket()
      };
    }
    byEndpointMap[endpoint][mode].all += 1;
    byEndpointMap[endpoint][mode][outcome] += 1;
  });

  const byDay = Object.values(byDayMap).sort((a, b) => String(b.date).localeCompare(String(a.date)));
  const byEndpoint = Object.values(byEndpointMap).sort(
    (a, b) => (b.free.all + b.paid.all) - (a.free.all + a.paid.all)
  );

  return jsonResponse({
    ok: true,
    days: safeDays,
    since,
    count: rows.length,
    totals,
    by_day: byDay,
    by_endpoint: byEndpoint
  });
}

async function safeLogAiUsage(env, payload) {
  try {
    await logAiUsage(env, payload);
  } catch (_err) {
    // no-op
  }
}

async function logAiUsage(env, payload) {
  const body = {
    created_at: new Date().toISOString(),
    mode: normalizeAiUsageMode(payload && payload.mode),
    endpoint: String((payload && payload.endpoint) || "").trim() || "unknown",
    outcome: normalizeAiUsageOutcome(payload && payload.outcome),
    serial: payload && payload.serial ? String(payload.serial) : null,
    user_id: payload && payload.user_id ? String(payload.user_id) : null,
    model: payload && payload.model ? String(payload.model) : null
  };
  await fetch(`${env.SUPABASE_URL}/rest/v1/ai_usage_logs`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
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

async function checkRateLimit(env, userId, mode = "free") {
  const isPaid = mode === "paid";
  const daySetting = isPaid
    ? env.PAID_RATE_LIMIT_PER_DAY || env.RATE_LIMIT_PER_DAY
    : env.RATE_LIMIT_PER_DAY;
  const minSetting = isPaid
    ? env.PAID_RATE_LIMIT_PER_MIN || env.RATE_LIMIT_PER_MIN
    : env.RATE_LIMIT_PER_MIN;
  if (!daySetting && !minSetting) {
    return { ok: true };
  }
  const now = new Date();
  const dayKey = `${userId}:${now.toISOString().slice(0, 10)}`;
  const minKey = `${userId}:${now.toISOString().slice(0, 16)}`;
  const dayLimit = Number(daySetting || 0);
  const minLimit = Number(minSetting || 0);

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
