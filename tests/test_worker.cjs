// Run: node --test tests/test_worker.cjs (all external services are mocked).
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../workers/worker.js'), 'utf8').replace('export default {', 'globalThis.worker = {');
const env = { SUPABASE_URL: 'https://db.invalid', SUPABASE_SERVICE_KEY: 'mock-service', SUPABASE_ANON_KEY: 'mock-anon', GEMINI_API_KEY_FREE: 'mock-google', GOOGLE_TTS_API_KEY: 'mock-tts', TTS_MONTHLY_CHAR_LIMIT: '100' };
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
function harness(overrides = {}) {
  const calls = [];
  const user = overrides.user || { id: '00000000-0000-0000-0000-000000000001', app_metadata: { role: 'student' } };
  const ctx = vm.createContext({ Request, Response, URL, Intl, Date, atob, crypto: require('node:crypto').webcrypto,
    fetch: async (url, options = {}) => {
      const item = { url: String(url), ...options }; calls.push(item);
      const custom = overrides.fetch && await overrides.fetch(item);
      if (custom) return custom;
      if (item.url.endsWith('/auth/v1/user')) return json(user);
      if (item.url.includes('/rest/v1/user_flags?')) return json([]);
      if (item.url.includes('/rest/v1/app_settings?')) {
        const key = new URL(item.url).searchParams.get('setting_key').slice(3);
        return json([{ setting_key: key, setting_value: ({ ai_public_generation: true, tts_enabled: true })[key] ?? false }]);
      }
      if (item.url.includes('/rest/v1/ai_usage_logs') && item.method === 'POST') return json({});
      if (item.url.includes('/rest/v1/deep_dive_explanations?') || item.url.includes('/rest/v1/tag_deep_dive_explanations?')) return json([]);
      throw new Error(`Unexpected service call: ${item.url}`);
    }
  });
  vm.runInContext(source, ctx);
  const request = (route, body, authenticated = true, headers = {}) => new Request(`https://worker.invalid${route}`, {
    method: body === undefined ? 'GET' : 'POST', headers: { ...(authenticated ? { Authorization: 'Bearer mock-token' } : {}), 'Content-Type': 'application/json', ...headers },
    ...(body === undefined ? {} : { body: JSON.stringify(body) })
  });
  return { ctx, calls, request, run: (route, body, authenticated = true, headers = {}) => ctx.worker.fetch(request(route, body, authenticated, headers), env) };
}

test('user-editable metadata cannot authorize admin or teacher operations', async () => {
  const h = harness({ user: { id: 'mock-user', app_metadata: {}, user_metadata: { role: 'admin' } } });
  assert.equal((await h.run('/admin/role', { user_id: 'target', role: 'admin' })).status, 403);
  assert.equal((await h.run('/admin/users')).status, 403);
  assert.ok(!h.calls.some(x => x.url.includes('/auth/v1/admin/users')));
});

test('trusted app_metadata still authorizes an admin; settings config is valid', async () => {
  const h = harness({ user: { id: 'mock-admin', app_metadata: { role: 'admin' } } });
  const result = await h.run('/admin/ai_generation');
  assert.equal(result.status, 200);
  assert.equal((await result.json()).tts_enabled, true);
});

test('disabled accounts cannot downgrade to anonymous TTS generation', async () => {
  const h = harness({ fetch: x => x.url.includes('/user_flags?') ? json([{ disabled: true }]) : undefined });
  assert.equal((await h.run('/ai/tts', { text: '学習' })).status, 403);
  assert.ok(!h.calls.some(x => x.url.includes('googleapis.com')));
});

test('unavailable account flags and unavailable settings fail closed', async () => {
  for (const fragment of ['/user_flags?', '/app_settings?']) {
    const h = harness({ fetch: x => x.url.includes(fragment) ? json({}, 503) : undefined });
    assert.equal((await h.run('/ai/tts', { text: '学習' })).status, 503);
    assert.ok(!h.calls.some(x => x.url.includes('googleapis.com')));
  }
});

for (const [route, body] of [
  ['/ai/deep_dive', { serial: 'A01-001', prompt: 'Explain the question' }],
  ['/ai/question_qa', { serial: 'A01-001', question: 'Explain the question' }],
  ['/ai/practice_questions', { serial: 'A01-001' }],
  ['/ai/question_senryu', { serial: 'A01-001' }],
  ['/ai/tag_deep_dive', { tag: '解剖学' }],
  ['/ai/tag_qa', { tag: '解剖学', question: 'Explain the tag' }]
]) {
  test(`${route}: missing reservation RPC stops generation without unsafe fallback`, async () => {
    const h = harness({ fetch: x => x.url.endsWith('/rpc/worker_reserve_usage') ? json({ code: 'PGRST202' }, 404) : undefined });
    const result = await h.run(route, body, false);
    assert.equal(result.status, 503);
    assert.equal(h.calls.filter(x => x.url.endsWith('/rpc/worker_reserve_usage')).length, 1);
    assert.ok(!h.calls.some(x => x.url.includes('googleapis.com')));
  });
}

test('a denied shared paid budget returns 429 and makes no Google request', async () => {
  const h = harness({ fetch: x => {
    if (x.url.includes('/app_settings?')) {
      const key = new URL(x.url).searchParams.get('setting_key').slice(3);
      return json([{ setting_key: key, setting_value: ({ ai_public_generation: true, ai_public_paid_generation: true, ai_public_paid_limit: 1, ai_public_paid_started_at: '2026-09-06T00:00:00Z' })[key] ?? false }]);
    }
    if (x.url.endsWith('/rpc/worker_usage_total')) return json(0);
    if (x.url.endsWith('/rpc/worker_reserve_usage')) return json({ ok: false, reason: 'public_paid' });
  } });
  const result = await h.ctx.worker.fetch(h.request('/ai/question_qa', { serial: 'A01-001', question: 'Explain' }, false), { ...env, GEMINI_API_KEY_PAID: 'mock-paid' });
  assert.equal(result.status, 429);
  const reservation = JSON.parse(h.calls.find(x => x.url.endsWith('/rpc/worker_reserve_usage')).body);
  assert.equal(reservation.p_kind, 'gemini_paid');
  assert.equal(reservation.p_public_limit, 1);
  assert.equal(reservation.p_public_started_at, '2026-09-06T00:00:00Z');
  assert.ok(!h.calls.some(x => x.url.includes('googleapis.com')));
});

test('fallback model requires a separate reservation before a second upstream call', async () => {
  let count = 0;
  const h = harness({ fetch: x => {
    if (x.url.endsWith('/rpc/worker_reserve_usage')) return json({ ok: ++count === 1 });
    if (x.url.includes('generativelanguage.googleapis.com')) return json({ error: { message: 'model not found' } }, 404);
  } });
  const result = await h.run('/ai/question_qa', { serial: 'A01-001', question: 'Explain', model: 'mock-model' }, false);
  assert.equal(result.status, 429);
  assert.equal(count, 2);
  assert.equal(h.calls.filter(x => x.url.includes('generativelanguage.googleapis.com')).length, 1);
});

test('TTS always requires a successful reservation; a zero cap remains unlimited', async () => {
  for (const limit of ['0', '100']) {
    const h = harness({ fetch: x => x.url.endsWith('/rpc/worker_reserve_usage') ? json({}, 503) : undefined });
    const result = await h.ctx.worker.fetch(h.request('/ai/tts', { text: '<speak>学習</speak>', ssml: true }, false), { ...env, TTS_MONTHLY_CHAR_LIMIT: limit });
    assert.equal(result.status, 503);
    assert.ok(!h.calls.some(x => x.url.includes('googleapis.com')));
    const call = h.calls.find(x => x.url.endsWith('/rpc/worker_reserve_usage'));
    assert.equal(JSON.parse(call.body).p_amount, '<speak>学習</speak>'.length);
    assert.equal(JSON.parse(call.body).p_month_limit, Number(limit));
  }
});

test('progress forwards the stable event ID and uses only the atomic RPC', async () => {
  let payload;
  const h = harness({ fetch: x => {
    if (x.url.endsWith('/rpc/worker_record_answer')) { payload = JSON.parse(x.body); return json({ attempt_count: 11, event_id: payload.p_event_id }); }
  } });
  const result = await h.run('/progress/answer', { serial: 'A01-001', is_correct: true, event_id: 'same-operation' });
  assert.equal(result.status, 200);
  assert.equal(payload.p_event_id, 'same-operation');
  assert.equal(payload.p_user_id, '00000000-0000-0000-0000-000000000001');
  assert.ok(!h.calls.some(x => x.url.includes('/rest/v1/user_progress')));
});

test('progress supports Idempotency-Key and refuses key reuse conflicts', async () => {
  let payload;
  const h = harness({ fetch: x => {
    if (x.url.endsWith('/rpc/worker_record_answer')) { payload = JSON.parse(x.body); return json({ code: '22023' }, 400); }
  } });
  assert.equal((await h.run('/progress/answer', { serial: 'A01-001', is_correct: false }, true, { 'Idempotency-Key': 'same-operation' })).status, 409);
  assert.equal(payload.p_event_id, 'same-operation');
});

test('progress RPC outage preserves stored counts, without REST upsert fallback', async () => {
  const h = harness({ fetch: x => x.url.includes('/rpc/worker_') ? json({}, 503) : undefined });
  for (const [route, body] of [
    ['/progress/answer', { serial: 'A01-001', is_correct: true }],
    ['/progress/status', { serial: 'A01-001', status: 'mastered' }],
    ['/progress/import_local', { items: [{ serial: 'A01-001', is_correct: true }] }]
  ]) assert.equal((await h.run(route, body)).status, 503);
  assert.ok(!h.calls.some(x => x.url.includes('/rest/v1/user_progress')));
});

test('progress read outage is an error, never a successful empty history', async () => {
  const h = harness({ fetch: x => x.url.includes('/rest/v1/user_progress?') ? json({}, 503) : undefined });
  assert.equal((await h.run('/progress/bulk', { serials: ['A01-001'] })).status, 503);
  assert.equal((await h.run('/progress/list')).status, 503);
});

test('public answer stats expose only the aggregate RPC with a bounded serial batch', async () => {
  let payload;
  const h = harness({ fetch: x => {
    if (x.url.endsWith('/rpc/worker_answer_stats')) { payload = JSON.parse(x.body); return json([{ serial: 'A01-001', total: 2200, correct: 1100 }]); }
  } });
  const result = await h.run('/stats/answers?serials=A01-001', undefined, false);
  assert.equal(result.status, 200);
  assert.deepEqual((await result.json()).items, [{ serial: 'A01-001', total: 2200, correct: 1100 }]);
  assert.deepEqual(payload.p_serials, ['A01-001']);
  assert.ok(!h.calls.some(x => x.url.includes('/rest/v1/answers')));
  assert.equal((await h.run('/stats/answers?serials=' + Array.from({length:201},(_,i) => `A01-${i}`).join(','), undefined, false)).status, 400);
});

test('unavailable public paid usage never falls back to another generation route', async () => {
  const h = harness({ fetch: x => {
    if (x.url.includes('/app_settings?')) {
      const key = new URL(x.url).searchParams.get('setting_key').slice(3);
      return json([{ setting_key: key, setting_value: ({ ai_public_generation: true, ai_public_paid_generation: true, ai_public_paid_limit: 1, ai_public_paid_started_at: '2026-09-06T00:00:00Z' })[key] ?? false }]);
    }
    if (x.url.endsWith('/rpc/worker_usage_total')) return json({}, 503);
  } });
  const response = await h.ctx.worker.fetch(h.request('/ai/question_qa', { serial: 'A01-001', question: 'Explain' }, false), { ...env, GEMINI_API_KEY_PAID: 'mock-paid' });
  assert.equal(response.status, 503);
  assert.ok(!h.calls.some(x => x.url.includes('googleapis.com')));
});
