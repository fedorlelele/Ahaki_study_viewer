-- Existing production editor policies, inspected on 2026-09-06.
-- Only the four role predicates below change. Public question reads, grants,
-- policy commands/roles and all stored rows remain as they are.
-- Requires the inspected tables/policies; unknown definitions fail atomically.
begin;
set local lock_timeout = '5s';
set local statement_timeout = '30s';
set local search_path = pg_catalog;

-- Prevent concurrent policy changes between the assertion and ALTER POLICY.
lock table public.override_history, public.question_overrides in access exclusive mode;

do $migration$
declare
  expected record;
  actual record;
  old_predicate constant text := $old$(COALESCE(((auth.jwt() -> 'app_metadata'::text) ->> 'role'::text), ((auth.jwt() -> 'user_metadata'::text) ->> 'role'::text), (auth.jwt() ->> 'role'::text), ''::text) = ANY (ARRAY['teacher'::text, 'admin'::text]))$old$;
  new_predicate constant text := $new$(COALESCE(((auth.jwt() -> 'app_metadata'::text) ->> 'role'::text), ''::text) = ANY (ARRAY['teacher'::text, 'admin'::text]))$new$;
begin
  for expected in
    select * from (values
      ('override_history', 'override_history read', 'r'),
      ('override_history', 'override_history write', 'a'),
      ('question_overrides', 'qo_insert', 'a'),
      ('question_overrides', 'qo_update', 'w')
    ) as policies(table_name, policy_name, command)
  loop
    select c.relrowsecurity, p.polcmd::text as command, p.polpermissive,
      p.polroles, pg_get_expr(p.polqual, p.polrelid) as qual,
      pg_get_expr(p.polwithcheck, p.polrelid) as with_check
    into actual
    from pg_policy p join pg_class c on c.oid = p.polrelid
      join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public' and c.relname = expected.table_name
      and p.polname = expected.policy_name;
    if not found then
      raise exception 'Expected editor policy is missing: %.%', expected.table_name, expected.policy_name;
    end if;
    if not actual.relrowsecurity or actual.command <> expected.command
      or not actual.polpermissive or actual.polroles <> array[0::oid] then
      raise exception 'Unexpected editor policy scope: %.%', expected.table_name, expected.policy_name;
    end if;
    if expected.command = 'a' then
      if actual.qual is not null then
        raise exception 'Unexpected editor INSERT predicate: %.%', expected.table_name, expected.policy_name;
      end if;
    elsif actual.qual is distinct from old_predicate and actual.qual is distinct from new_predicate then
      raise exception 'Unexpected editor USING predicate: %.%', expected.table_name, expected.policy_name;
    end if;
    if expected.command = 'r' then
      if actual.with_check is not null then
        raise exception 'Unexpected editor SELECT predicate: %.%', expected.table_name, expected.policy_name;
      end if;
    elsif actual.with_check is distinct from old_predicate and actual.with_check is distinct from new_predicate then
      raise exception 'Unexpected editor WITH CHECK predicate: %.%', expected.table_name, expected.policy_name;
    end if;
  end loop;
end $migration$;

alter policy "override_history read" on public.override_history
  using (coalesce(auth.jwt() -> 'app_metadata' ->> 'role', '') = any (array['teacher', 'admin']));
alter policy "override_history write" on public.override_history
  with check (coalesce(auth.jwt() -> 'app_metadata' ->> 'role', '') = any (array['teacher', 'admin']));
alter policy qo_insert on public.question_overrides
  with check (coalesce(auth.jwt() -> 'app_metadata' ->> 'role', '') = any (array['teacher', 'admin']));
alter policy qo_update on public.question_overrides
  using (coalesce(auth.jwt() -> 'app_metadata' ->> 'role', '') = any (array['teacher', 'admin']))
  with check (coalesce(auth.jwt() -> 'app_metadata' ->> 'role', '') = any (array['teacher', 'admin']));

commit;
