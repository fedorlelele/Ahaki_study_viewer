# Cloudflare Workers 管理API / Gemini中継

## 役割
- `/admin/*` : 管理API（権限管理・監査ログ）
- `/ai/*` : Gemini API中継
- `/analytics/*` : 学習/AI品質分析イベント収集・集計API

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
- `GOOGLE_TTS_API_KEY` (任意。TTS利用時に必須)
- `GOOGLE_TTS_LANGUAGE_CODE` (任意。既定 `ja-JP`)
- `GOOGLE_TTS_VOICE_STANDARD` (任意。既定 `ja-JP-Neural2-B`)
- `GOOGLE_TTS_VOICE_HIGH` (任意。高音質ボイス)
- `TTS_MONTHLY_CHAR_LIMIT_STANDARD` (任意。通常音質の月間文字数上限)
- `TTS_MONTHLY_CHAR_LIMIT_HIGH` (任意。高音質の月間文字数上限)
- `TTS_MONTHLY_CHAR_LIMIT` (任意。上記未設定時の共通上限)

## 想定URL
- 管理API: `https://<worker>.workers.dev/admin/*`
- Gemini中継: `https://<worker>.workers.dev/ai/*`
- 分析API: `https://<worker>.workers.dev/analytics/*`
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
  - `question_type`: `mcq` (4択) / `tf` (○×) / `short` (一問一答)。未指定時は `mcq`。
- `POST /ai/practice_questions/helpful`
- `POST /ai/practice_questions/not_helpful`
- `GET /admin/practice_questions?days=30&serial=...`（teacher/admin向け評価一覧）
- `POST /admin/practice_questions/publish`（teacher/adminが個別/一括で公開・非公開）

主な分析API:

- `POST /analytics/collect`（匿名/ログイン問わずイベント送信）
- `POST /analytics/ai_feedback`（helpful / not_helpful フィードバック送信）
- `GET /analytics/overview?days=30`（teacher/admin）
- `GET /analytics/learning?days=30`（teacher/admin）
- `GET /analytics/ai_quality?days=30`（teacher/admin）
- `GET /analytics/content?days=30`（teacher/admin）

## WebUI側の設定
`web_app/config.js` に以下を追加:

```js
window.ADMIN_API_BASE = "https://<worker>.workers.dev";
window.AI_API_BASE = "https://<worker>.workers.dev";
```

## 注意
- admin APIはSupabase JWTが必須です（Bearer token）。
- 管理APIはteacher以上、権限変更はadminのみ許可されます。
- 分析機能を使う場合は以下SQLを Supabase SQL Editor で実行してください。
  - `workers/sql/ai_usage_logs.sql`
  - `workers/sql/analytics_events.sql`
  - `workers/sql/ai_feedback_events.sql`
- タグ学習機能を使う場合は `workers/sql/tag_study_tables.sql` を Supabase SQL Editor で実行してください。
- 問題別の練習問題機能を使う場合は `workers/sql/practice_question_tables.sql` を Supabase SQL Editor で実行してください。
- teacher/admin が生成した練習問題は初期状態で非公開です（生成者本人のみ閲覧可）。
- 非公開/公開の切り替えは `POST /admin/practice_questions/publish` で個別・一括実行できます。
