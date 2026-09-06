// PostgreSQL integration suite; no network/database credentials used.
// Run after npm ci: node --test tests/test_worker_sql.cjs
// Install @electric-sql/pglite in a temporary folder, then run with its module path:
// AHAKI_PGLITE_MODULE=/tmp/ahaki-worker-sql-test/node_modules/@electric-sql/pglite node --test tests/test_worker_sql.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const modulePath = process.env.AHAKI_PGLITE_MODULE || '@electric-sql/pglite';
const files = ['progress_tables.sql', 'ai_usage_logs.sql', 'analytics_events.sql', 'ai_feedback_events.sql'];
const sql = name => fs.readFileSync(path.join(__dirname, '../workers/sql', name), 'utf8');
const migration = sql('migration_20260906_worker_safety.sql');
const uid = '00000000-0000-0000-0000-000000000001';
const secondUid = '00000000-0000-0000-0000-000000000002';

test('PostgreSQL: migration, authorization, progress transactions, and usage budgets', async t => {
  const { PGlite } = require(modulePath);
  const db = new PGlite();
  try {
    await db.exec(`create role anon; create role authenticated; create role service_role bypassrls;
      create schema auth;
      create function auth.uid() returns uuid language sql stable as 'select nullif(current_setting(''request.jwt.claim.sub'',true),'''')::uuid';`);
    for (const file of files) await db.exec(sql(file));
    // Simulate an existing installation with sensitive public telemetry policies.
    await db.exec(`grant select on public.analytics_events, public.ai_feedback_events to anon, authenticated;
      create policy analytics_events_read on public.analytics_events for select using (true);
      create policy ai_feedback_events_read on public.ai_feedback_events for select using (true);
      insert into public.user_progress(user_id,serial,status,attempt_count,correct_count) values ('${uid}','A01-001','mastered',7,5);
      insert into public.ai_usage_logs(mode,endpoint,outcome,char_count,request_id) values
        ('paid','question_qa','success',0,'legacy-paid'), ('free','tts_standard','success',10,'legacy-tts');`);
    await db.exec(migration);
    await db.exec(migration);
    await db.exec(sql('answer_stats.sql'));
    await db.exec(sql('answer_stats.sql'));

    await t.test('repeatable migration preserves history and seeds legacy usage exactly once', async () => {
      assert.equal((await db.query('select count(*)::int as n from public.worker_usage_reservations')).rows[0].n, 2);
      assert.equal((await db.query('select attempt_count from public.user_progress')).rows[0].attempt_count, 7);
    });

    await t.test('anon and authenticated cannot read telemetry or invoke privileged RPCs', async () => {
      for (const role of ['anon', 'authenticated']) {
        const result = await db.query(`select
          has_table_privilege($1,'public.analytics_events','select') as analytics,
          has_table_privilege($1,'public.ai_feedback_events','select') as feedback,
          has_function_privilege($1,'public.worker_record_answer(uuid,text,boolean,text)','execute') as answer,
          has_function_privilege($1,'public.worker_reserve_usage(text,text,text,bigint,bigint,bigint,bigint,timestamptz,bigint)','execute') as reserve`, [role]);
        assert.deepEqual(result.rows[0], { analytics: false, feedback: false, answer: false, reserve: false });
      }
      assert.equal((await db.query("select has_function_privilege('service_role','public.worker_record_answer(uuid,text,boolean,text)','execute') as allowed")).rows[0].allowed, true);
    });

    await t.test('answer replay is idempotent; distinct answers increment existing history', async () => {
      const answer = () => db.query('select public.worker_record_answer($1,$2,$3,$4) as item', [uid, 'A01-001', true, 'event-1']);
      const first = (await answer()).rows[0].item;
      const replay = (await answer()).rows[0].item;
      assert.deepEqual(replay, first);
      assert.equal(first.attempt_count, 8);
      assert.equal(first.correct_count, 6);
      await assert.rejects(db.query('select public.worker_record_answer($1,$2,$3,$4)', [uid, 'A01-002', true, 'event-1']), /Idempotency key/);
      await Promise.all(Array.from({ length: 20 }, (_, i) => db.query('select public.worker_record_answer($1,$2,$3,$4)', [uid, 'A01-001', false, `distinct-${i}`])));
      const row = (await db.query('select attempt_count,correct_count from public.user_progress where user_id=$1 and serial=$2', [uid,'A01-001'])).rows[0];
      assert.deepEqual(row, { attempt_count: 28, correct_count: 6 });
    });

    await t.test('status and repeated import never replace answer counts', async () => {
      await db.query('select public.worker_set_progress_status($1,$2,$3)', [uid,'A01-001','needs_review']);
      const data = JSON.stringify([{ serial: 'A01-001', is_correct: false }, { serial: 'A01-002', is_correct: true }]);
      const first = (await db.query('select public.worker_import_progress($1,$2) as items', [uid,data])).rows[0].items;
      assert.equal(first.length, 1);
      assert.equal(first[0].serial, 'A01-002');
      assert.deepEqual((await db.query('select public.worker_import_progress($1,$2) as items', [uid,data])).rows[0].items, []);
      assert.equal((await db.query('select attempt_count from public.user_progress where user_id=$1 and serial=$2',[uid,'A01-001'])).rows[0].attempt_count, 28);
    });

    await t.test('invalid import rolls back all rows in the operation', async () => {
      await assert.rejects(db.query('select public.worker_import_progress($1,$2)', [secondUid, JSON.stringify([{serial:'A01-001',is_correct:true},{serial:'A01-002',is_correct:'bad'}])]), /Invalid imported/);
      assert.equal((await db.query('select count(*)::int as n from public.user_progress where user_id=$1',[secondUid])).rows[0].n, 0);
    });

    const reserve = (id, actor, kind, amount, day = 0, minute = 0, month = 0, started = null, publicLimit = 0) =>
      db.query('select public.worker_reserve_usage($1,$2,$3,$4,$5,$6,$7,$8,$9) as result', [id,actor,kind,amount,day,minute,month,started,publicLimit]).then(x => x.rows[0].result);

    await t.test('daily budget admits one of twenty requested reservations', async () => {
      const responses = await Promise.all(Array.from({length:20},(_,i) => reserve(`limited-${i}`,'actor-one','gemini_free',1,1)));
      assert.equal(responses.filter(x => x.ok).length, 1);
      assert.equal((await reserve('limited-0','actor-one','gemini_free',1,1)).ok, false);
    });

    await t.test('minute and paid public limits stop further reservations', async () => {
      assert.equal((await reserve('minute-1','actor-minute','gemini_free',1,0,1)).ok, true);
      assert.equal((await reserve('minute-2','actor-minute','gemini_free',1,0,1)).reason, 'minute');
      // One legacy paid call is already counted.
      assert.equal((await reserve('paid-1','actor-paid','gemini_paid',1,0,0,0,'2000-01-01T00:00:00Z',2)).ok, true);
      assert.equal((await reserve('paid-2','actor-other','gemini_paid',1,0,0,0,'2000-01-01T00:00:00Z',2)).reason, 'public_paid');
      assert.equal(Number((await db.query("select public.worker_usage_total('gemini_paid','2000-01-01') as n")).rows[0].n), 2);
    });

    await t.test('TTS quota includes imported usage and is retained after failed upstream calls', async () => {
      assert.equal((await reserve('tts-1','actor-tts','tts_standard',5,0,0,15)).ok, true);
      assert.equal((await reserve('tts-2','another-actor','tts_standard',1,0,0,15)).reason, 'month');
      assert.equal((await reserve('tts-unlimited','actor-tts','tts_high',1)).ok, true);
      // No usage-log insertion is required for the reserved amount to count.
      assert.equal(Number((await db.query("select public.worker_usage_total('tts_standard','2000-01-01') as n")).rows[0].n), 15);
    });

    await t.test('answer stats preserve per-actor/day latest semantics beyond a 1000-row REST cap', async () => {
      await db.exec(`insert into public.answers(serial,is_correct,anon_id,created_at)
        select 'A99-001',false,'actor-'||n,'2026-09-01T00:00:00Z'::timestamptz from generate_series(1,1200) n;
        insert into public.answers(serial,is_correct,anon_id,created_at) values
          ('A99-001',true,'actor-1','2026-09-01T01:00:00Z'),
          ('A99-001',true,'actor-1','2026-09-02T01:00:00Z');`);
      const rows = (await db.query('select public.worker_answer_stats($1) as items', [['A99-001','A99-002']])).rows[0].items;
      assert.deepEqual(rows, [{serial:'A99-001',total:1201,correct:2},{serial:'A99-002',total:0,correct:0}]);
      assert.equal((await db.query("select has_function_privilege('anon','public.worker_answer_stats(text[])','execute') as allowed")).rows[0].allowed,false);
    });

    await t.test('migration rerun does not count current reservation-backed logs twice', async () => {
      await db.exec("insert into public.ai_usage_logs(mode,endpoint,outcome,char_count,request_id) values ('paid','question_qa','success',0,'paid-1')");
      await db.exec(migration);
      assert.equal(Number((await db.query("select public.worker_usage_total('gemini_paid','2000-01-01') as n")).rows[0].n), 2);
    });
  } finally { await db.close(); }
});
