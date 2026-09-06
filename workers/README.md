# Cloudflare Workers 管理API / Gemini中継

## 役割
- `/admin/*` : 管理API（権限管理・監査ログ）
- `/ai/*` : Gemini API中継
- `/analytics/*` : 学習/AI品質分析イベント収集・集計API
- `/progress/*` : ログインユーザーの学習進捗
- `/stats/answers` : 公開用の問題別回答統計

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
- `TTS_RATE_LIMIT_PER_DAY` / `TTS_RATE_LIMIT_PER_MIN` (任意。未設定時は対応する `RATE_LIMIT_*` を使用)

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

- `GET /ai/question_beginner_qa?serial=...`
- `GET /ai/question_beginner_qa_batch?serials=A01-001,A01-002,...`
- `GET /ai/question_qa?serial=...`
- `POST /ai/question_qa`
- `GET /ai/practice_questions?serial=...`
- `POST /ai/practice_questions`（1回で5問生成して保存）
  - `question_type`: `mcq` (4択) / `tf` (○×) / `short` (一問一答)。未指定時は `mcq`。
- `POST /ai/practice_questions/helpful`
- `POST /ai/practice_questions/not_helpful`
- `GET /ai/question_senryu?serial=...`
- `POST /ai/question_senryu`（1回で3つ生成・各項目に解説付き）
- `POST /ai/question_senryu/helpful`
- `POST /ai/question_senryu/not_helpful`
- `GET /ai/question_senryu_ranking?days=30&limit=50`
- `GET /admin/practice_questions?days=30&serial=...`（teacher/admin向け評価一覧）
- `POST /admin/practice_questions/publish`（teacher/adminが個別/一括で公開・非公開）

主な分析API:

- `POST /analytics/collect`（匿名/ログイン問わずイベント送信）
- `POST /analytics/ai_feedback`（helpful / not_helpful フィードバック送信）
- `GET /analytics/overview?days=30`（admin）
- `GET /analytics/learning?days=30`（admin）
- `GET /analytics/ai_quality?days=30`（admin）
- `GET /analytics/content?days=30`（admin）

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
- 初学者向けの事前生成Q&Aを使う場合は `workers/sql/question_beginner_qa_tables.sql` を Supabase SQL Editor で実行してください。
- 問題別の練習問題機能を使う場合は `workers/sql/practice_question_tables.sql` を Supabase SQL Editor で実行してください。
- 問題別の川柳機能を使う場合は `workers/sql/question_senryu_tables.sql` を Supabase SQL Editor で実行してください。
- teacher/admin が生成した練習問題は初期状態で非公開です（生成者本人のみ閲覧可）。
- 非公開/公開の切り替えは `POST /admin/practice_questions/publish` で個別・一括実行できます。


## 2026-09-06 の安全性変更

認可は Supabase の `app_metadata.role` のみを参照します。利用者が編集できる `user_metadata.role` は権限として扱いません。`user_flags.disabled = true` のアカウントはログインを伴う操作を拒否します。認証・停止状態・機能設定を取得できない場合も処理を止めます。

`analytics_events` と `ai_feedback_events` の生ログはブラウザから読めなくなります。管理者向けの分析は Worker 経由で取得してください。進捗更新・利用枠・回答統計の `worker_*` RPC は `service_role` 専用です。ブラウザへ service key を渡したり、クライアントロールへ RPC 実行権限を追加したりする必要はありません。

### 利用枠と既存設定

既存の API キー・モデル・上限環境変数は引き続き使えます。TTS の月間上限は音質別設定を優先し、未設定時は共通設定を使います。有効な設定値が未指定または `0` なら、従来どおり月間上限は無制限です。上限を使う場合は、`TTS_MONTHLY_CHAR_LIMIT` または音質別の変数に正の整数を設定してください。

Gemini と TTS は外部 API を呼ぶ前に、Supabase のトランザクション内で利用枠を予約します。上限超過は `429`、予約処理の障害・必要な migration の未適用は `503` とし、生成を止めます。上限を無効にした場合も予約処理は必要です。

- 日・分の利用回数、有料公開枠、TTS の音質別月間文字数を確認します。日・月の境界は UTC です。
- 生成に失敗した場合も予約は戻しません。応答が届かなかった場合の二重呼出を安全側に扱うため、成功ログの件数と残枠が一致しないことがあります。
- fallback model の呼出にも別の予約が必要です。
- TTS は SSML を含む送信文字数で予約します。
- 有料公開枠を有効にした時点以降の既存の有料利用も引き続き数えます。migration は旧利用ログを予約台帳へ移行し、再実行で二重計上しません。

### 進捗更新と回答統計

`POST /progress/answer` は既存の `serial`、`is_correct` に加え、`event_id` を受け付けます。1 回の回答につき 1 個の ID を作り、通信失敗後の再送では同じ ID を使ってください。`Idempotency-Key` ヘッダでも指定でき、両方ある場合は本文の `event_id` を優先します。

```json
{"serial":"A01-001","is_correct":true,"event_id":"同じ回答の再送で保持するID"}
```

ID は 160 文字以内です。同一ユーザー・同一 ID・同一内容の再送は回数を増やさず、保存済みの結果を返します。同じ ID に別の問題や正誤を指定すると `409` になります。ID を送らない旧クライアントも使えますが、再送の重複排除には同じ ID の送信が必要です。状態変更・ローカル履歴の取り込みも DB 内で更新し、既存の回答回数を上書きしません。

`GET /stats/answers?serials=A01-001,A01-002` は最大 200 問の統計を返します。

```json
{"ok":true,"items":[{"serial":"A01-001","total":12,"correct":9},{"serial":"A01-002","total":0,"correct":0}]}
```

統計は「同一問題・匿名利用者 ID・UTC 日ごとの最新回答」を 1 件として集計します。回答のない指定問題も 0 件で返します。DB 内で集計するため、生ログ取得の行数上限による欠落を避けられます。取得失敗時は `503` を返し、クライアントは未取得を 0 件として確定しないでください。

## 本番への適用順

2026-09-06 に本番 Supabase の安全性・回答統計・編集権限の移行と Worker 更新を実施しました。既存10表の非公開バックアップを取り、全既存行の保持、6つのRPCと非公開表の権限、旧利用履歴352件の引継ぎを確認しています。Worker は `b2ae4c31-c94d-4ad1-ab81-335d85a0da19` です。以下は再構築時の適用手順です。

1. AI/TTS の公開利用を一時停止し、管理者も新しい生成を止めます。進行中の生成がすべて完了してから移行します。旧 Worker の呼出がログ移行後に完了すると、予約台帳との間に計上漏れが生じるためです。
2. 対象 Supabase のバックアップを確保し、SQL Editor または管理用 DB 接続で次を順に適用します。各ファイルは再実行可能です。既存データの重複などで失敗した場合は原因を解消してから続けます。

   ```text
   workers/sql/progress_tables.sql
   workers/sql/ai_usage_logs.sql
   workers/sql/analytics_events.sql
   workers/sql/ai_feedback_events.sql
   workers/sql/migration_20260906_worker_safety.sql
   workers/sql/answer_stats.sql
   ```

   既存本番にある編集用 RLS の旧定義は、`workers/sql/migration_20260906_editor_roles.sql` も適用します。対象は `question_overrides` と `override_history` の確認済み 4 ポリシーです。変更前の定義が想定と異なる場合は停止し、公開読取や既存データは変更しません。新規環境には対象ポリシーがないため、この追加移行は適用しません。

3. 更新した `workers/worker.js` をデプロイします。先に Worker を更新すると、未作成の RPC に依存する進捗・生成・統計がエラーになります。
4. 更新した Pages を公開します。進捗の再送 ID と回答統計 API を使うため、Worker の更新後に公開します。
5. 認証・停止アカウントの拒否・進捗の同一 ID 再送・統計を確認してから、AI/TTS の公開設定を戻します。

本番反映前のローカル検証は、リポジトリのルートで `npm ci`、`npm test` を実行してください。Worker の外部 API はモックし、SQL は PGlite で検証します。本番の資格情報や課金 API はテストに使用しません。
