-- 学習進捗管理テーブル
-- Supabase SQL Editor で実行してください。

create table if not exists public.user_progress (
  user_id uuid not null,
  serial text not null,
  status text not null default 'unstarted',
  attempt_count integer not null default 0,
  correct_count integer not null default 0,
  last_answered_at timestamptz,
  last_is_correct boolean,
  next_review_at timestamptz,
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  primary key (user_id, serial),
  constraint user_progress_status_check check (
    status in ('unstarted', 'in_progress', 'mastered', 'needs_review')
  )
);

create index if not exists user_progress_user_idx
  on public.user_progress (user_id);

create index if not exists user_progress_review_idx
  on public.user_progress (user_id, status, next_review_at);

create table if not exists public.user_goals (
  user_id uuid primary key,
  weekly_answer_target integer not null default 0,
  weekly_review_target integer not null default 0,
  target_mastery_rate numeric(5,2) not null default 0,
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

-- Worker(service key) 経由運用のため、最低限のRLSにしておく
alter table public.user_progress enable row level security;
alter table public.user_goals enable row level security;

-- 既存ポリシーがなければ読み取りは本人のみ（参考）
do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'user_progress'
      and policyname = 'user_progress_self_select'
  ) then
    create policy user_progress_self_select
      on public.user_progress
      for select
      using (auth.uid() = user_id);
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'user_goals'
      and policyname = 'user_goals_self_select'
  ) then
    create policy user_goals_self_select
      on public.user_goals
      for select
      using (auth.uid() = user_id);
  end if;
end $$;
