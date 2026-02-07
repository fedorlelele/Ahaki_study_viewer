begin;

create table if not exists public.tag_view_stats (
  tag text primary key,
  view_count bigint not null default 0,
  updated_at timestamptz not null default now()
);

alter table public.tag_view_stats enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'tag_view_stats'
      and policyname = 'tag_view_stats_read'
  ) then
    create policy tag_view_stats_read
      on public.tag_view_stats
      for select
      using (true);
  end if;
end $$;

commit;
