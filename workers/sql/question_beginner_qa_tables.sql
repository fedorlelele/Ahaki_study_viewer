-- 初学者向けの事前生成Q&A保存テーブル
-- Supabase SQL Editor で実行してください。

begin;

create table if not exists public.question_beginner_qa (
  serial text primary key,
  items jsonb not null default '[]'::jsonb,
  model text not null default '',
  prompt_version text not null default 'v1',
  is_active boolean not null default true,
  created_by uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.question_beginner_qa
  add column if not exists items jsonb not null default '[]'::jsonb;

alter table public.question_beginner_qa
  add column if not exists model text not null default '';

alter table public.question_beginner_qa
  add column if not exists prompt_version text not null default 'v1';

alter table public.question_beginner_qa
  add column if not exists is_active boolean not null default true;

alter table public.question_beginner_qa
  add column if not exists created_by uuid;

alter table public.question_beginner_qa
  add column if not exists created_at timestamptz not null default now();

alter table public.question_beginner_qa
  add column if not exists updated_at timestamptz not null default now();

update public.question_beginner_qa
set items = '[]'::jsonb
where items is null;

update public.question_beginner_qa
set model = ''
where model is null;

update public.question_beginner_qa
set prompt_version = 'v1'
where prompt_version is null or prompt_version = '';

update public.question_beginner_qa
set is_active = true
where is_active is null;

update public.question_beginner_qa
set created_at = now()
where created_at is null;

update public.question_beginner_qa
set updated_at = now()
where updated_at is null;

alter table public.question_beginner_qa
  alter column items set default '[]'::jsonb;

alter table public.question_beginner_qa
  alter column items set not null;

alter table public.question_beginner_qa
  alter column model set default '';

alter table public.question_beginner_qa
  alter column model set not null;

alter table public.question_beginner_qa
  alter column prompt_version set default 'v1';

alter table public.question_beginner_qa
  alter column prompt_version set not null;

alter table public.question_beginner_qa
  alter column is_active set default true;

alter table public.question_beginner_qa
  alter column is_active set not null;

alter table public.question_beginner_qa
  alter column created_at set default now();

alter table public.question_beginner_qa
  alter column created_at set not null;

alter table public.question_beginner_qa
  alter column updated_at set default now();

alter table public.question_beginner_qa
  alter column updated_at set not null;

create index if not exists question_beginner_qa_active_updated_at_idx
  on public.question_beginner_qa (is_active, updated_at desc);

alter table public.question_beginner_qa enable row level security;

drop policy if exists question_beginner_qa_read on public.question_beginner_qa;

create policy question_beginner_qa_read
  on public.question_beginner_qa
  for select
  using (is_active = true);

commit;
