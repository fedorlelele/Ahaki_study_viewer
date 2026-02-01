# Cloudflare Workers 管理API / Gemini中継

## 役割
- `/admin/*` : 管理API（権限管理・監査ログ）
- `/ai/*` : Gemini API中継

## 必要な環境変数
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_KEY`
- `GEMINI_API_KEY`
- `GEMINI_MODEL` (任意)
- `RATE_LIMIT_PER_DAY` (任意)
- `RATE_LIMIT_PER_MIN` (任意)

## 想定URL
- 管理API: `https://<worker>.workers.dev/admin/*`
- Gemini中継: `https://<worker>.workers.dev/ai/*`

## WebUI側の設定
`web_app/config.js` に以下を追加:

```js
window.ADMIN_API_BASE = "https://<worker>.workers.dev";
window.AI_API_BASE = "https://<worker>.workers.dev";
```

## 注意
- admin APIはSupabase JWTが必須です（Bearer token）。
- 管理APIはteacher以上、権限変更はadminのみ許可されます。
