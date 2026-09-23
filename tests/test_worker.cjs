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
  const ctx = vm.createContext({ Request, Response, URL, URLSearchParams, Intl, Date, atob, crypto: require('node:crypto').webcrypto,
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
  const result = await h.ctx.worker.fetch(h.request('/ai/tag_qa', { tag: '解剖学', question: 'Explain' }, false), { ...env, GEMINI_API_KEY_PAID: 'mock-paid' });
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
    if (x.url.includes('generativelanguage.googleapis.com')) return json({ error: { status: 'RESOURCE_EXHAUSTED' } }, 429);
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
  const response = await h.ctx.worker.fetch(h.request('/ai/tag_qa', { tag: '解剖学', question: 'Explain' }, false), { ...env, GEMINI_API_KEY_PAID: 'mock-paid' });
  assert.equal(response.status, 503);
  assert.ok(!h.calls.some(x => x.url.includes('googleapis.com')));
});

const qaBody = { serial: 'A01-001', question: '選択肢の違いを説明して' };
const qaGoogleSuccess = () => json({ candidates: [{ content: { parts: [{ text: JSON.stringify({ status: 'ok', question: '選択肢の違いは？', answer: '学習用の回答です。' }) }] } }] });
const qaFetchSuccess = x => {
  if (x.url.endsWith('/rpc/worker_reserve_usage')) return json({ ok: true });
  if (x.url.includes('generativelanguage.googleapis.com')) return qaGoogleSuccess();
  if (x.url.endsWith('/rest/v1/question_qa') && x.method === 'POST') return json([{ ...JSON.parse(x.body), id: 'qa-id', created_at: '2026-09-20T00:00:00Z' }]);
};
const googleCalls = h => h.calls.filter(x => x.url.includes('generativelanguage.googleapis.com'));

test('question Q&A ignores old client/env model overrides and saves the preferred free model', async () => {
  const h = harness({ fetch: qaFetchSuccess });
  const response = await h.ctx.worker.fetch(h.request('/ai/question_qa', { ...qaBody, model: 'gemini-3-flash-preview' }, false), {
    ...env, GEMINI_MODEL_FREE: 'gemini-paid-only', GEMINI_MODEL: 'gemini-paid-only'
  });
  assert.equal(response.status, 200);
  const result = await response.json();
  assert.equal(result.model, 'gemini-3.8-flash');
  assert.equal(result.preferred_model, 'gemini-3.8-flash');
  assert.equal(result.fallback_used, false);
  assert.equal(result.mode, 'free');
  assert.equal(result.item.id, 'qa-id');
  assert.equal(result.item.model, result.model);
  assert.equal(result.item.created_by, undefined);
  assert.match(googleCalls(h)[0].url, /models\/gemini-3\.8-flash:generateContent\?key=mock-google$/);
  const saved = h.calls.find(x => x.url.endsWith('/rest/v1/question_qa') && x.method === 'POST');
  assert.equal(saved.headers.Prefer, 'return=representation');
  assert.equal(JSON.parse(saved.body).model, result.model);
});

test('question Q&A provider quota falls back one grade with distinct free reservations and actual model metadata', async () => {
  for (const [httpStatus, errorStatus] of [[429, 'RESOURCE_EXHAUSTED'], [400, 'RESOURCE_EXHAUSTED']]) {
    let upstream = 0;
    const h = harness({ fetch: x => {
      if (x.url.includes('generativelanguage.googleapis.com') && ++upstream === 1) return json({ error: { status: errorStatus } }, httpStatus);
      return qaFetchSuccess(x);
    } });
    const response = await h.run('/ai/question_qa', qaBody, false);
    assert.equal(response.status, 200);
    const result = await response.json();
    assert.equal(result.model, 'gemini-3.7-flash');
    assert.equal(result.item.model, result.model);
    assert.equal(result.preferred_model, 'gemini-3.8-flash');
    assert.equal(result.fallback_used, true);
    assert.deepEqual(googleCalls(h).map(x => new URL(x.url).pathname), [
      '/v1beta/models/gemini-3.8-flash:generateContent', '/v1beta/models/gemini-3.7-flash:generateContent'
    ]);
    const reservations = h.calls.filter(x => x.url.endsWith('/rpc/worker_reserve_usage')).map(x => JSON.parse(x.body));
    assert.equal(reservations.length, 2);
    assert.notEqual(reservations[0].p_request_id, reservations[1].p_request_id);
    assert.ok(reservations.every(x => x.p_kind === 'gemini_free' && x.p_public_limit === 0));
    const log = JSON.parse(h.calls.find(x => x.url.includes('/ai_usage_logs') && x.method === 'POST').body);
    assert.equal(log.model, result.model);
    assert.equal(log.fallback_model, result.model);
  }
});

test('question Q&A does not retry provider input, authentication, missing model, or server errors', async () => {
  for (const status of [400, 401, 403, 404, 500, 503]) {
    const h = harness({ fetch: x => x.url.includes('generativelanguage.googleapis.com') ? json({ error: { status: 'OTHER_ERROR' } }, status) : qaFetchSuccess(x) });
    assert.equal((await h.run('/ai/question_qa', qaBody, false)).status, 500);
    assert.equal(googleCalls(h).length, 1);
    assert.ok(!h.calls.some(x => x.url.endsWith('/rest/v1/question_qa') && x.method === 'POST'));
  }
});

test('question Q&A stops when both free model quotas are exhausted', async () => {
  const h = harness({ fetch: x => x.url.includes('generativelanguage.googleapis.com') ? json({ error: { status: 'RESOURCE_EXHAUSTED' } }, 429) : qaFetchSuccess(x) });
  const response = await h.run('/ai/question_qa', qaBody, false);
  assert.equal(response.status, 429);
  assert.equal((await response.json()).model, 'gemini-3.7-flash');
  assert.equal(googleCalls(h).length, 2);
});

test('question Q&A site budget or reservation failure stops before any additional model call', async () => {
  for (const failOn of [1, 2]) {
    for (const unavailable of [false, true]) {
      let reservations = 0;
      const h = harness({ fetch: x => {
        if (x.url.endsWith('/rpc/worker_reserve_usage') && ++reservations === failOn) return unavailable ? json({}, 503) : json({ ok: false, reason: 'day' });
        if (x.url.includes('generativelanguage.googleapis.com')) return json({ error: { status: 'RESOURCE_EXHAUSTED' } }, 429);
        return qaFetchSuccess(x);
      } });
      assert.equal((await h.run('/ai/question_qa', qaBody, false)).status, unavailable ? 503 : 429);
      assert.equal(reservations, failOn);
      assert.equal(googleCalls(h).length, failOn - 1);
    }
  }
});

test('question Q&A never selects the paid key even for an admin with paid generation enabled', async () => {
  const h = harness({ user: { id: 'admin', app_metadata: { role: 'admin' } }, fetch: x => {
    if (x.url.includes('/app_settings?')) return json([{ setting_value: true }]);
    return qaFetchSuccess(x);
  } });
  const response = await h.ctx.worker.fetch(h.request('/ai/question_qa', qaBody), { ...env, GEMINI_API_KEY_PAID: 'never-use-paid' });
  assert.equal(response.status, 200);
  assert.ok(googleCalls(h).every(x => new URL(x.url).searchParams.get('key') === env.GEMINI_API_KEY_FREE));
  assert.ok(!h.calls.some(x => x.url.endsWith('/rpc/worker_usage_total')));
});

test('question Q&A with only a paid key stops before reservation and generation', async () => {
  const h = harness({ fetch: qaFetchSuccess });
  const response = await h.ctx.worker.fetch(h.request('/ai/question_qa', qaBody, false), { ...env, GEMINI_API_KEY_FREE: '', GEMINI_API_KEY_PAID: 'never-use-paid' });
  assert.equal(response.status, 500);
  assert.equal(googleCalls(h).length, 0);
  assert.ok(!h.calls.some(x => x.url.endsWith('/rpc/worker_reserve_usage')));
});

test('question Q&A history returns model metadata and labels only missing legacy models as Gemini 3 Flash', async () => {
  const h = harness({ fetch: x => x.url.includes('/rest/v1/question_qa?') ? json([
    { id: 'old', model: null, answer: '旧回答' }, { id: 'new', model: 'gemini-3.7-flash', answer: '新回答' }
  ]) : undefined });
  const response = await h.run('/ai/question_qa?serial=A01-001', undefined, false);
  assert.equal(response.status, 200);
  assert.deepEqual((await response.json()).items.map(x => x.model), ['gemini-3-flash-preview', 'gemini-3.7-flash']);
  assert.ok(new URL(h.calls[0].url).searchParams.get('select').split(',').includes('model'));
});

test('question Q&A does not claim success when saving the actual model and answer fails', async () => {
  const h = harness({ fetch: x => x.url.endsWith('/rest/v1/question_qa') && x.method === 'POST' ? json({}, 500) : qaFetchSuccess(x) });
  assert.equal((await h.run('/ai/question_qa', qaBody, false)).status, 503);
  assert.equal(googleCalls(h).length, 1);
});

test('deep dives persist and return the actual fallback model without automatic verification', async () => {
  let saved;
  const h = harness({ fetch: x => {
    if (x.url.endsWith('/rpc/worker_reserve_usage')) return json({ ok: true });
    if (x.url.includes('models/missing-model:')) return json({}, 404);
    if (x.url.includes('generativelanguage.googleapis.com')) return json({ candidates: [{ content: { parts: [{ text: JSON.stringify({ explanation: '本文', tags: ['タグ'] }) }] } }] });
    if (x.url.endsWith('/rest/v1/deep_dive_explanations') && x.method === 'POST') { saved = JSON.parse(x.body); return json({}); }
  } });
  const result = await h.run('/ai/deep_dive', { serial: 'A01-001', prompt: '説明', model: 'missing-model' }, false);
  assert.equal(result.status, 200);
  const payload = await result.json();
  assert.equal(saved.model_name, 'gemini-3-flash-preview');
  assert.equal(saved.review_status, 'ai');
  assert.equal(payload.model_name, saved.model_name);
  assert.equal(payload.updated_at, saved.updated_at);
  assert.equal(payload.review_status, 'ai');
});

test('deep-dive review is restricted to trusted teachers and admins', async () => {
  const body = { serial: 'A01-001', review_status: 'ai_fact_checked', expected_updated_at: '2026-09-01T00:00:00Z' };
  const guest = harness();
  assert.equal((await guest.run('/admin/deep_dive_review', body, false)).status, 401);
  for (const role of ['student', 'guest']) {
    const h = harness({ user: { id: 'user', app_metadata: { role }, user_metadata: { role: 'admin' } } });
    assert.equal((await h.run('/admin/deep_dive_review', body)).status, 403);
    assert.ok(!h.calls.some(x => x.method === 'PATCH'));
  }
});

test('deep-dive metadata survives a review/save/read cycle and stale updates are refused', async () => {
  let stored = { serial: 'A01-001', explanation: '既存本文', tags: ['タグ'], updated_at: '2026-09-01T00:00:00Z', model_name: 'Gemini 3 Flash', review_status: 'ai' };
  const h = harness({ user: { id: 'teacher', app_metadata: { role: 'teacher' } }, fetch: x => {
    if (!x.url.includes('/rest/v1/deep_dive_explanations?')) return;
    const params = new URL(x.url).searchParams;
    assert.match(params.get('select'), /model_name,review_status/);
    if (x.method === 'PATCH') {
      if (params.get('updated_at') !== `eq.${stored.updated_at}`) return json([]);
      const patch = JSON.parse(x.body);
      assert.equal('model_name' in patch, false);
      stored = { ...stored, ...patch };
    }
    return json([stored]);
  } });
  const body = { serial: stored.serial, review_status: 'ai_fact_checked', expected_updated_at: stored.updated_at, model_name: 'untrusted-model' };
  assert.equal((await h.run('/admin/deep_dive_review', body)).status, 200);
  const reloaded = await (await h.run('/ai/deep_dive?serial=A01-001', undefined, false)).json();
  assert.equal(reloaded.data.model_name, 'Gemini 3 Flash');
  assert.equal(reloaded.data.review_status, 'ai_fact_checked');
  assert.equal(reloaded.data.explanation, '既存本文');
  assert.equal((await h.run('/admin/deep_dive_review', body)).status, 409);
  assert.equal((await h.run('/admin/deep_dive_review', { ...body, explanation: '編集', expected_updated_at: stored.updated_at })).status, 400);
  assert.equal((await h.run('/admin/deep_dive_review', { ...body, review_status: 'invalid' })).status, 400);
});

test('deep-dive storage failures are returned as failures', async () => {
  const h = harness({ user: { id: 'teacher', app_metadata: { role: 'teacher' } }, fetch: x => {
    if (x.method === 'PATCH' || x.url.endsWith('/rest/v1/deep_dive_explanations')) return json({}, 503);
  } });
  assert.equal((await h.run('/admin/deep_dive_review', { serial: 'A01-001', review_status: 'ai', expected_updated_at: '2026-09-01T00:00:00Z' })).status, 503);
  await assert.rejects(h.ctx.upsertDeepDive(env, { serial: 'A01-001', explanation: '本文', model_name: 'new-model' }), /保存できません/);
});
