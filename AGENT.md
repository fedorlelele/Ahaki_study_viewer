# Project Direction (Draft)

## Goals
- Teachers:
  - Use the WebUI to browse questions and quickly copy them into Word, etc.
  - Edit questions, tags, and subtopics via an intuitive UI.
  - If AI regeneration is needed, generate prompts, run them in ChatGPT/Gemini, then import JSONL.
- Students (and teachers):
  - Browse many questions to deepen understanding.
  - Report errors in explanations/tags/subtopics easily.
- Collect per-question correctness data (answer statistics).
- Allow both login and guest usage.

## Constraints and Scope
- Start free if possible; acceptable monthly cost: a few hundred yen.
- Begin with a small group in-school (a few to ~30 users), then expand to public use.

## Recommended Architecture (Phased)

### Phase 1 (Minimal, safe, low cost)
- WebUI stays on GitHub Pages (static hosting).
- Use Supabase for:
  - Feedback reports (explanation/tag/subtopic)
  - Answer correctness data
- No login required (guest usage supported via anonymous ID).

### Phase 2 (School use)
- Add teacher login (Supabase Auth).
- Admin-only access to view/manage reports and stats.
- Local admin app can remain for data generation/import.

### Phase 3 (Public expansion)
- Consider web-based admin UI (teacher editing in browser).
- Continue using prompt generation + JSONL import flow for AI re-generation.

## Data Model (Initial)

### feedback
- serial (question ID)
- kind (explanation | tag | subtopic)
- note (optional)
- created_at
- anon_id

### answers
- serial (question ID)
- is_correct (bool)
- selected_index
- created_at
- anon_id

## WebUI Behavior (Initial)
- On load, generate/store anon_id in localStorage.
- "Report" buttons insert into feedback table.
- Answer selection inserts into answers table.

## Security Notes
- Enable Supabase RLS.
- Allow anonymous inserts; restrict reads to admin context.

## Next Steps
- Create Supabase project.
- Provide SUPABASE_URL and SUPABASE_ANON_KEY for WebUI integration.
