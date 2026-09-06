-- Apply after progress_tables.sql, ai_usage_logs.sql, analytics_events.sql,
-- and ai_feedback_events.sql. This file is shared by new and existing installs.
-- Pause AI/TTS generation during migration/deployment so legacy calls cannot
-- complete between the usage backfill and the new reservation-based Worker.
begin;

-- Worker-only private telemetry: also repair existing installations.
drop policy if exists analytics_events_read on public.analytics_events;
drop policy if exists ai_feedback_events_read on public.ai_feedback_events;
revoke all on table public.analytics_events, public.ai_feedback_events from anon, authenticated;

create table if not exists public.user_flags (
  user_id uuid primary key,
  disabled boolean not null default false,
  note text not null default '',
  updated_at timestamptz not null default now(),
  updated_by uuid
);
create unique index if not exists user_flags_user_id_unique on public.user_flags(user_id);
alter table public.user_flags enable row level security;
revoke all on public.user_flags from anon, authenticated;

create table if not exists public.app_settings (
  setting_key text primary key,
  setting_value jsonb,
  updated_at timestamptz not null default now(),
  updated_by uuid
);
create unique index if not exists app_settings_key_unique on public.app_settings(setting_key);
alter table public.app_settings enable row level security;
revoke all on public.app_settings from anon, authenticated;

create table if not exists public.worker_answer_events (
  user_id uuid not null,
  event_id text not null check (length(event_id) between 1 and 160),
  serial text not null,
  is_correct boolean not null,
  result jsonb not null,
  created_at timestamptz not null default now(),
  primary key (user_id, event_id)
);
alter table public.worker_answer_events enable row level security;
revoke all on public.worker_answer_events from anon, authenticated;

create or replace function public.worker_record_answer(
  p_user_id uuid, p_serial text, p_is_correct boolean, p_event_id text
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  prior public.worker_answer_events%rowtype;
  saved public.user_progress%rowtype;
  result jsonb;
  stamp timestamptz := now();
begin
  if p_user_id is null or p_is_correct is null or p_serial is null or length(p_serial) not between 1 and 100
     or p_event_id is null or length(p_event_id) not between 1 and 160 then
    raise exception 'Invalid answer' using errcode = '22023';
  end if;
  -- Serialize duplicate operations before they can both increment a row.
  perform pg_advisory_xact_lock(hashtextextended('answer:' || p_user_id::text || ':' || p_event_id, 0));
  select * into prior from public.worker_answer_events where user_id = p_user_id and event_id = p_event_id;
  if found then
    if prior.serial <> p_serial or prior.is_correct <> p_is_correct then
      raise exception 'Idempotency key was reused with different content' using errcode = '22023';
    end if;
    return prior.result;
  end if;
  insert into public.user_progress as current (
    user_id, serial, status, attempt_count, correct_count, last_answered_at,
    last_is_correct, next_review_at, updated_at
  ) values (
    p_user_id, p_serial, case when p_is_correct then 'mastered' else 'needs_review' end,
    1, case when p_is_correct then 1 else 0 end, stamp, p_is_correct,
    case when p_is_correct then stamp + interval '7 days' else stamp end, stamp
  ) on conflict (user_id, serial) do update set
    status = excluded.status,
    attempt_count = current.attempt_count + 1,
    correct_count = current.correct_count + excluded.correct_count,
    last_answered_at = excluded.last_answered_at,
    last_is_correct = excluded.last_is_correct,
    next_review_at = excluded.next_review_at,
    updated_at = excluded.updated_at
  returning * into saved;
  result := to_jsonb(saved) || jsonb_build_object('event_id', p_event_id);
  insert into public.worker_answer_events(user_id, event_id, serial, is_correct, result)
    values (p_user_id, p_event_id, p_serial, p_is_correct, result);
  return result;
end $$;

create or replace function public.worker_set_progress_status(
  p_user_id uuid, p_serial text, p_status text
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare saved public.user_progress%rowtype;
begin
  if p_user_id is null or p_serial is null or length(p_serial) not between 1 and 100
     or p_status is null or p_status not in ('unstarted','in_progress','mastered','needs_review') then
    raise exception 'Invalid progress status' using errcode = '22023';
  end if;
  insert into public.user_progress(user_id, serial, status, next_review_at, updated_at)
  values (p_user_id, p_serial, p_status,
    case when p_status = 'mastered' then now() + interval '7 days'
         when p_status = 'needs_review' then now() else null end, now())
  on conflict (user_id, serial) do update set
    status = excluded.status, next_review_at = excluded.next_review_at, updated_at = excluded.updated_at
  returning * into saved;
  return to_jsonb(saved);
end $$;

create or replace function public.worker_import_progress(p_user_id uuid, p_items jsonb)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare item jsonb; saved public.user_progress%rowtype; result jsonb := '[]'::jsonb;
begin
  if p_user_id is null or jsonb_typeof(p_items) is distinct from 'array' then
    raise exception 'Invalid progress import' using errcode = '22023';
  end if;
  if jsonb_array_length(p_items) > 20000 then
    raise exception 'Too many imported items' using errcode = '22023';
  end if;
  -- Stable order also avoids deadlocks when two imports overlap.
  for item in select value from jsonb_array_elements(p_items) order by value->>'serial' loop
    if item->>'serial' is null or length(item->>'serial') not between 1 and 100
       or jsonb_typeof(item->'is_correct') is distinct from 'boolean' then
      raise exception 'Invalid imported item' using errcode = '22023';
    end if;
    insert into public.user_progress(user_id, serial, status, attempt_count, correct_count,
      last_answered_at, last_is_correct, next_review_at, updated_at)
    values (p_user_id, item->>'serial', case when (item->>'is_correct')::boolean then 'mastered' else 'needs_review' end,
      1, case when (item->>'is_correct')::boolean then 1 else 0 end,
      now(), (item->>'is_correct')::boolean,
      case when (item->>'is_correct')::boolean then now() + interval '7 days' else now() end, now())
    on conflict (user_id, serial) do nothing returning * into saved;
    if found then result := result || jsonb_build_array(to_jsonb(saved)); end if;
  end loop;
  return result;
end $$;

-- Reservations are consumed before an upstream call; failures retain the charge.
-- This conservative ledger is independent of optional best-effort analytics logs.
create table if not exists public.worker_usage_reservations (
  request_id text primary key,
  actor text not null,
  kind text not null check (kind in ('gemini_free','gemini_paid','tts_standard','tts_high')),
  amount bigint not null check (amount > 0),
  created_at timestamptz not null default now()
);
create index if not exists worker_usage_kind_time on public.worker_usage_reservations(kind, created_at);
create index if not exists worker_usage_actor_time on public.worker_usage_reservations(actor, created_at);
alter table public.worker_usage_reservations enable row level security;
revoke all on public.worker_usage_reservations from anon, authenticated;

-- Existing paid and TTS usage must continue to count after deployment. Re-running
-- does not double-count logs emitted for already-reserved request IDs.
insert into public.worker_usage_reservations(request_id, actor, kind, amount, created_at)
select 'legacy-ai-usage:' || logs.id::text, 'legacy',
  case when logs.endpoint in ('tts_standard','tts_high') then logs.endpoint
       when logs.mode = 'paid' then 'gemini_paid' else 'gemini_free' end,
  case when logs.endpoint in ('tts_standard','tts_high') then logs.char_count else 1 end,
  logs.created_at
from public.ai_usage_logs logs
where ((logs.endpoint in ('tts_standard','tts_high') and logs.outcome = 'success' and logs.char_count > 0)
   or (logs.endpoint not like 'tts%' and logs.outcome <> 'rate_limited'))
  and not exists (select 1 from public.worker_usage_reservations reservations where reservations.request_id = logs.request_id)
on conflict (request_id) do nothing;

create or replace function public.worker_usage_total(p_kind text, p_since timestamptz)
returns bigint language sql security definer set search_path = '' as $$
  select coalesce(sum(amount), 0)::bigint from public.worker_usage_reservations
  where kind = p_kind and created_at >= p_since;
$$;

create or replace function public.worker_reserve_usage(
  p_request_id text, p_actor text, p_kind text, p_amount bigint,
  p_day_limit bigint, p_minute_limit bigint, p_month_limit bigint,
  p_public_started_at timestamptz, p_public_limit bigint
) returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  stamp timestamptz := clock_timestamp();
  used bigint;
  group_prefix text;
begin
  if p_request_id is null or length(p_request_id) not between 1 and 160 or p_actor is null or length(p_actor) not between 1 and 200
     or p_kind is null or p_kind not in ('gemini_free','gemini_paid','tts_standard','tts_high')
     or p_amount is null or p_amount <= 0
     or p_day_limit is null or p_day_limit < 0 or p_minute_limit is null or p_minute_limit < 0
     or p_month_limit is null or p_month_limit < 0 or p_public_limit is null or p_public_limit < 0
     or (p_kind like 'gemini%' and p_amount <> 1)
     or (p_public_limit > 0 and p_public_started_at is null) then
    raise exception 'Invalid usage reservation' using errcode = '22023';
  end if;
  -- One short transaction lock protects both per-actor and shared budgets.
  -- Appropriate for a small school deployment; no remote/API call holds it.
  perform pg_advisory_xact_lock(710224, 60906);
  if exists(select 1 from public.worker_usage_reservations where request_id = p_request_id) then
    return jsonb_build_object('ok', false, 'reason', 'duplicate_request');
  end if;
  group_prefix := case when p_kind like 'tts%' then 'tts%' else 'gemini%' end;
  if p_day_limit > 0 then
    select count(*) into used from public.worker_usage_reservations where actor = p_actor and kind like group_prefix
      and created_at >= date_trunc('day', stamp at time zone 'UTC') at time zone 'UTC';
    if used >= p_day_limit then return jsonb_build_object('ok',false,'reason','day','used',used,'limit',p_day_limit); end if;
  end if;
  if p_minute_limit > 0 then
    select count(*) into used from public.worker_usage_reservations where actor = p_actor and kind like group_prefix
      and created_at >= date_trunc('minute', stamp);
    if used >= p_minute_limit then return jsonb_build_object('ok',false,'reason','minute','used',used,'limit',p_minute_limit); end if;
  end if;
  if p_month_limit > 0 then
    select coalesce(sum(amount),0) into used from public.worker_usage_reservations where kind = p_kind
      and created_at >= date_trunc('month', stamp at time zone 'UTC') at time zone 'UTC';
    if used + p_amount > p_month_limit then return jsonb_build_object('ok',false,'reason','month','used',used,'limit',p_month_limit); end if;
  end if;
  if p_public_limit > 0 then
    select coalesce(sum(amount),0) into used from public.worker_usage_reservations where kind = 'gemini_paid' and created_at >= p_public_started_at;
    if used + p_amount > p_public_limit then return jsonb_build_object('ok',false,'reason','public_paid','used',used,'limit',p_public_limit); end if;
  end if;
  insert into public.worker_usage_reservations(request_id,actor,kind,amount,created_at)
    values (p_request_id,p_actor,p_kind,p_amount,stamp);
  return jsonb_build_object('ok',true,'request_id',p_request_id);
end $$;

-- Supabase grants functions to PUBLIC by default. These RPCs intentionally
-- accept explicit user IDs and budgets, so only the trusted Worker may call them.
revoke all on function public.worker_record_answer(uuid,text,boolean,text) from public, anon, authenticated;
revoke all on function public.worker_set_progress_status(uuid,text,text) from public, anon, authenticated;
revoke all on function public.worker_import_progress(uuid,jsonb) from public, anon, authenticated;
revoke all on function public.worker_usage_total(text,timestamptz) from public, anon, authenticated;
revoke all on function public.worker_reserve_usage(text,text,text,bigint,bigint,bigint,bigint,timestamptz,bigint) from public, anon, authenticated;
grant execute on function public.worker_record_answer(uuid,text,boolean,text) to service_role;
grant execute on function public.worker_set_progress_status(uuid,text,text) to service_role;
grant execute on function public.worker_import_progress(uuid,jsonb) to service_role;
grant execute on function public.worker_usage_total(text,timestamptz) to service_role;
grant execute on function public.worker_reserve_usage(text,text,text,bigint,bigint,bigint,bigint,timestamptz,bigint) to service_role;
grant all on public.user_flags, public.app_settings, public.worker_answer_events, public.worker_usage_reservations to service_role;

commit;
