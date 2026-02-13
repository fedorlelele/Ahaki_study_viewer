# Cloudflare Workers 管理API / Gemini中継

## 役割
- `/admin/*` : 管理API（権限管理・監査ログ）
- `/ai/*` : Gemini API中継

## 必要な環境変数
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_KEY`
- `GEMINI_API_KEY_FREE` (通常ユーザー向け)
- `GEMINI_API_KEY_PAID` (管理者向け・任意)
- `GEMINI_API_KEY` (`GEMINI_API_KEY_FREE` 未設定時の互換用)
- `GEMINI_MODEL_FREE` (任意)
- `GEMINI_MODEL_PAID` (任意)
- `GEMINI_MODEL` (任意・互換用)
- `RATE_LIMIT_PER_DAY` (任意)
- `RATE_LIMIT_PER_MIN` (任意)
- `PAID_RATE_LIMIT_PER_DAY` (任意。未設定時は `RATE_LIMIT_PER_DAY` を使用)
- `PAID_RATE_LIMIT_PER_MIN` (任意。未設定時は `RATE_LIMIT_PER_MIN` を使用)
- `AI_ADMIN_PAID_GENERATION` (任意。初期値のfallback)

## 想定URL
- 管理API: `https://<worker>.workers.dev/admin/*`
- Gemini中継: `https://<worker>.workers.dev/ai/*`
- API使用状況: `https://<worker>.workers.dev/admin/ai_usage?days=30`（adminのみ）

主なタグ学習API:

- `GET /ai/tag_deep_dive?tag=...`
- `POST /ai/tag_deep_dive`
- `GET /ai/tag_qa?tag=...`
- `POST /ai/tag_qa`
- `POST /ai/tag_qa/view`
- `POST /ai/tag_qa/like`

主な問題別AI API:

- `GET /ai/question_qa?serial=...`
- `POST /ai/question_qa`
- `GET /ai/practice_questions?serial=...`
- `POST /ai/practice_questions`（1回で5問生成して保存）
- `POST /ai/practice_questions/good`
- `POST /ai/practice_questions/bad`
- `GET /admin/practice_questions?days=30&serial=...`（teacher/admin向け評価一覧）

## WebUI側の設定
`web_app/config.js` に以下を追加:

```js
window.ADMIN_API_BASE = "https://<worker>.workers.dev";
window.AI_API_BASE = "https://<worker>.workers.dev";
```

## 注意
- admin APIはSupabase JWTが必須です（Bearer token）。
- 管理APIはteacher以上、権限変更はadminのみ許可されます。
- `workers/sql/ai_usage_logs.sql` を Supabase SQL Editor で実行すると、free/paid別のAI利用集計が利用できます。
- タグ学習機能を使う場合は `workers/sql/tag_study_tables.sql` を Supabase SQL Editor で実行してください。
- 問題別の練習問題機能を使う場合は `workers/sql/practice_question_tables.sql` を Supabase SQL Editor で実行してください。
