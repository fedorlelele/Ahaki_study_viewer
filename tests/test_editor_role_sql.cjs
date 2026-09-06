// Real PostgreSQL RLS behavior in PGlite; no network or production data.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { PGlite } = require(process.env.AHAKI_PGLITE_MODULE || '@electric-sql/pglite');
const migration = fs.readFileSync(path.join(__dirname, '../workers/sql/migration_20260906_editor_roles.sql'), 'utf8');
const oldPredicate = `coalesce(auth.jwt()->'app_metadata'->>'role', auth.jwt()->'user_metadata'->>'role', auth.jwt()->>'role', '') = any (array['teacher','admin'])`;

async function fixture() {
  const db = new PGlite();
  await db.exec(`
    create role anon;
    create role authenticated;
    create schema auth;
    create function auth.jwt() returns jsonb language sql stable as
      'select coalesce(nullif(current_setting(''request.jwt.claims'', true), '''')::jsonb, ''{}''::jsonb)';
    grant usage on schema auth to anon, authenticated;
    create table public.question_overrides(serial text primary key, explanation text);
    create table public.override_history(id bigint generated always as identity primary key, serial text, kind text);
    alter table public.question_overrides enable row level security;
    alter table public.override_history enable row level security;
    grant select, insert, update on public.question_overrides to anon, authenticated;
    grant select, insert on public.override_history to anon, authenticated;
    grant usage on sequence public.override_history_id_seq to anon, authenticated;
    create policy qo_read on public.question_overrides for select to public using (true);
    create policy qo_insert on public.question_overrides for insert to public with check (${oldPredicate});
    create policy qo_update on public.question_overrides for update to public using (${oldPredicate}) with check (${oldPredicate});
    create policy "override_history read" on public.override_history for select to public using (${oldPredicate});
    create policy "override_history write" on public.override_history for insert to public with check (${oldPredicate});
    create table public.unrelated_policy_fixture(id integer);
    alter table public.unrelated_policy_fixture enable row level security;
    create policy unrelated_read on public.unrelated_policy_fixture for select using (true);
    insert into public.question_overrides values ('existing', 'original');
    insert into public.override_history(serial, kind) values ('existing', 'explanation');
  `);
  return db;
}

async function asRole(db, role, claims, callback) {
  assert.ok(['anon', 'authenticated'].includes(role));
  await db.exec(`set role ${role}`);
  try {
    await db.query("select set_config('request.jwt.claims', $1, false)", [JSON.stringify(claims)]);
    return await callback();
  } finally {
    await db.exec('reset role');
  }
}

const policies = db => db.query(`select tablename, policyname, permissive, roles, cmd, qual, with_check
  from pg_policies where schemaname='public' order by tablename,policyname`).then(x => x.rows);
const isRlsDenied = error => error.code === '42501' && /row-level security/.test(error.message);

test('editor role migration preserves public reads and trusted editor writes', async t => {
  const db = await fixture();
  try {
    await t.test('fixture reproduces the observed fallback vulnerability', async () => {
      await asRole(db, 'authenticated', { user_metadata: { role: 'admin' } }, async () => {
        await db.exec("insert into public.question_overrides values ('legacy-spoof', 'before migration')");
        assert.equal((await db.query('select count(*)::int as n from public.override_history')).rows[0].n, 1);
      });
    });

    const before = await policies(db);
    const rowsBefore = (await db.query('select * from public.question_overrides order by serial')).rows;
    const historyBefore = (await db.query('select * from public.override_history order by id')).rows;
    await db.exec(migration);
    const after = await policies(db);

    await t.test('only four predicates change, preserving scopes, grants and stored rows', async () => {
      assert.equal(after.length, before.length);
      for (let i = 0; i < before.length; i++) {
        const stripPredicate = ({ qual, with_check, ...scope }) => scope;
        assert.deepEqual(stripPredicate(after[i]), stripPredicate(before[i]));
        if (['qo_read', 'unrelated_read'].includes(before[i].policyname)) {
          assert.deepEqual(after[i], before[i]);
        } else {
          assert.doesNotMatch(`${after[i].qual} ${after[i].with_check}`, /user_metadata/);
          assert.match(`${after[i].qual} ${after[i].with_check}`, /app_metadata/);
        }
      }
      assert.deepEqual((await db.query('select * from public.question_overrides order by serial')).rows, rowsBefore);
      assert.deepEqual((await db.query('select * from public.override_history order by id')).rows, historyBefore);
      assert.equal((await db.query("select has_table_privilege('authenticated','public.question_overrides','INSERT,UPDATE,SELECT') as allowed")).rows[0].allowed, true);
    });

    await t.test('same migration can be applied twice without further changes', async () => {
      await db.exec(migration);
      assert.deepEqual(await policies(db), after);
    });

    await t.test('anonymous visitors retain question reads but cannot write or read history', async () => {
      await asRole(db, 'anon', {}, async () => {
        assert.equal((await db.query('select count(*)::int as n from public.question_overrides')).rows[0].n, rowsBefore.length);
        assert.equal((await db.query('select count(*)::int as n from public.override_history')).rows[0].n, 0);
        await assert.rejects(db.exec("insert into public.question_overrides values ('anon-spoof','denied')"), isRlsDenied);
        await assert.rejects(db.exec("insert into public.override_history(serial,kind) values ('existing','denied')"), isRlsDenied);
        assert.equal((await db.query("update public.question_overrides set explanation='denied' where serial='existing' returning serial")).rows.length, 0);
      });
    });

    await t.test('untrusted user or top-level role claims cannot grant editor access', async () => {
      const claims = [
        { user_metadata: { role: 'admin' } },
        { user_metadata: { role: 'teacher' } },
        { role: 'admin' },
        { role: 'teacher' },
        { app_metadata: { role: '' }, user_metadata: { role: 'teacher' } },
        { app_metadata: { role: 'student' }, user_metadata: { role: 'admin' }, role: 'admin' },
      ];
      for (const item of claims) {
        await asRole(db, 'authenticated', item, async () => {
          assert.equal((await db.query('select count(*)::int as n from public.override_history')).rows[0].n, 0);
          await assert.rejects(db.exec("insert into public.question_overrides values ('spoof','denied') on conflict(serial) do update set explanation=excluded.explanation"), isRlsDenied);
          await assert.rejects(db.exec("insert into public.question_overrides values ('existing','denied') on conflict(serial) do update set explanation=excluded.explanation"), isRlsDenied);
          await assert.rejects(db.exec("insert into public.override_history(serial,kind) values ('existing','denied')"), isRlsDenied);
          assert.equal((await db.query("update public.question_overrides set explanation='denied' where serial='existing' returning serial")).rows.length, 0);
        });
      }
      assert.equal((await db.query("select explanation from public.question_overrides where serial='existing'")).rows[0].explanation, 'original');
    });

    await t.test('trusted teachers and admins can upsert new and existing questions and save/read history', async () => {
      for (const role of ['teacher', 'admin']) {
        await asRole(db, 'authenticated', { app_metadata: { role }, user_metadata: { role: 'student' }, role: 'authenticated' }, async () => {
          for (const serial of [`new-${role}`, 'existing']) {
            const result = await db.query('insert into public.question_overrides values ($1,$2) on conflict(serial) do update set explanation=excluded.explanation returning explanation', [serial, role]);
            assert.equal(result.rows[0].explanation, role);
          }
          await db.query('insert into public.override_history(serial,kind) values ($1,$2)', ['existing', role]);
          assert.ok((await db.query('select count(*)::int as n from public.override_history')).rows[0].n > 1);
        });
      }
    });
  } finally { await db.close(); }
});

test('editor migration rejects definition drift atomically', async () => {
  const db = await fixture();
  try {
    for (const mutation of [
      'alter policy qo_update on public.question_overrides using (true)',
      'alter policy qo_insert on public.question_overrides to authenticated',
      'drop policy "override_history write" on public.override_history',
      'alter table public.question_overrides disable row level security',
    ]) {
      await db.exec('begin');
      await db.exec(mutation);
      const drifted = await policies(db);
      // Run the migration body within this test transaction; a failure must
      // leave all four changes unapplied, and rollback restores the fixture.
      const body = migration.replace(/^begin;$/m, '').replace(/^commit;$/m, '');
      await assert.rejects(db.exec(body), /Expected editor policy is missing|Unexpected editor/);
      await db.exec('rollback');
      const restored = await policies(db);
      assert.ok(restored.filter(x => ['qo_insert','qo_update','override_history read','override_history write'].includes(x.policyname))
        .every(x => /user_metadata/.test(`${x.qual} ${x.with_check}`)));
      if (!mutation.includes('disable row level')) assert.notDeepEqual(restored, drifted);
    }
  } finally { await db.close(); }
});
