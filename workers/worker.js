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
  if (request.method !== "POST") {
    return jsonResponse({ message: "Method not allowed" }, 405);
  }
  const user = await authenticate(request, env);
  if (!user) return jsonResponse({ message: "Unauthorized" }, 401);
  const role = getRole(user);
  if (!isRoleAtLeast(role, "student")) {
    return jsonResponse({ message: "Forbidden" }, 403);
  }
  const body = await readJson(request);
  const prompt = (body.prompt || "").trim();
  if (!prompt) {
    return jsonResponse({ message: "Prompt is required" }, 400);
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
  return jsonResponse({ text });
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
