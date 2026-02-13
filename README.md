# Ahaki Study Viewer

あん摩マッサージ指圧師・はり師きゅう師国家試験の問題データを管理し、学習用WebUIとして公開するプロジェクトです。

## 1. システム概要

- 基本データ: `output/ahaki.sqlite`
- 公開用データ: `output/web/questions.json` と `output/web/index/*.json`
- WebUI:
  - 開発用: `web_app/index.html`
  - GitHub Pages用: `docs/web_app/index.html`
- クラウド差分:
  - `Supabase` (回答ログ、修正提案、進捗、Q&A、深掘り解説、権限)
  - `Cloudflare Worker` (Supabase/Gemini API中継と権限制御)

`SQLite` を基盤にしつつ、ユーザー操作で増える情報は Supabase 側に保持する構成です。

## 2. 主な機能

- 問題検索
  - キーワード、`#タグ`、シリアル、科目、小項目、試験種別、回数範囲
  - 並び替え: 新旧、回答統計、頻出、Q&A追加順、深掘り解説追加順、復習優先順
- 学習機能
  - 回答・正誤表示
  - 検索結果コピー、問題単位コピー、深掘り用プロンプトコピー
  - かんたんビュー (`web_app/simple.html`)
- 進捗管理 (ログインユーザー)
  - 未着手 / 理解済み / 要復習
  - 復習キュー、弱点小項目、週目標
  - 端末ローカル回答履歴の取り込み
- 編集・レビュー
  - 修正提案の投稿、教師/管理者による確認・反映
  - 編集履歴、タグ無効化
- AI連携
  - 深掘り解説生成
  - 問題ごとのQ&A生成
  - 問題ごとの関連練習問題（5問）生成 + good/bad 評価（教師/管理者生成分は初期非公開、後で公開可能）
  - 管理者による AI機能の一時公開切り替え
- タグ辞書
  - タグ説明生成
  - 同義語結合
  - 全タグ文字列を使った自動タグ補完

## 3. 権限モデル

- `guest`: 閲覧中心
- `student`: `guest` + 学習進捗
- `teacher`: `student` + 修正提案管理・履歴確認・タグ無効化・回答推移確認
- `admin`: `teacher` + 権限管理 + AI公開切替 + 管理機能全体

## 4. ディレクトリ構成

- `build_ahaki_sqlite.py`: TXTからSQLiteを再構築
- `local_admin_app.py`: ローカル管理画面
- `scripts/`: 変換・テンプレ生成・インポート・JSON生成・タグ辞書関連
- `workers/worker.js`: Cloudflare Worker
- `workers/sql/`: Supabaseテーブル/ポリシー作成SQL
- `web_app/`: 開発用WebUI
- `docs/`: Pages公開物 (`scripts/prepare_pages.sh` で再生成)
- `config/subtopics_catalog.json`: 小項目カタログ
- `config/update_notes.json`: 更新情報ソース
- `output/`: SQLiteとWeb用生成物

## 5. セットアップ

### 5.1 前提

- Python 3.9+
- `pandas` (必要スクリプトで使用)

### 5.2 初期生成

```bash
python build_ahaki_sqlite.py
python scripts/generate_web_json.py --db output/ahaki.sqlite --out output/web/questions.json --index-dir output/web/index
bash scripts/prepare_pages.sh
```

### 5.3 WebUI設定

`web_app/config.example.js` を `web_app/config.js` として配置し、以下を設定します。

- `window.SUPABASE_URL`
- `window.SUPABASE_KEY` (Publishable key)
- `window.AI_API_BASE` (Cloudflare Worker URL)
- `window.ADMIN_API_BASE` (必要時。未設定なら `AI_API_BASE` を利用)

`web_app/config.js` は機密情報を含むため Git 管理しません。

## 6. ローカル起動

### 6.1 WebUI

```bash
python -m http.server 8000
```

- `http://127.0.0.1:8000/web_app/`

### 6.2 ローカル管理画面

`local_admin_app.py` は起動時に `.env` を自動読み込みします。  
`SUPABASE_URL` / `SUPABASE_SERVICE_KEY` を毎回コマンド前置きする必要はありません。

```bash
python local_admin_app.py --port 8001
```

- `http://127.0.0.1:8001/`

任意で以下の環境変数を使って起動デフォルトを変更できます。

- `AHAKI_ADMIN_DB`
- `AHAKI_ADMIN_HOST`
- `AHAKI_ADMIN_PORT`
- `AHAKI_ADMIN_SUBTOPICS`
- `AHAKI_ADMIN_PROMPT_SAMPLE`
- `AHAKI_ADMIN_DOWNLOADS`

## 7. 日常運用フロー

1. `SQLite` を更新 (インポート、手修正、スクリプト処理)
2. Web用JSONを再生成
3. Pages公開物を再生成
4. `docs/` をコミットして `main` に push

```bash
python scripts/generate_web_json.py --db output/ahaki.sqlite --out output/web/questions.json --index-dir output/web/index
bash scripts/prepare_pages.sh
```

## 8. JSONL運用 (解説・タグ・小項目)

### 8.1 テンプレート生成

```bash
python scripts/generate_explanation_template.py --limit 20 --out output/explanations_batch.jsonl --prompt-out output/explanations_batch_prompt.txt
python scripts/generate_tag_template.py --limit 20 --out output/tags_batch.jsonl --prompt-out output/tags_batch_prompt.txt
python scripts/generate_subtopic_assignment_template.py --limit 20 --catalog config/subtopics_catalog.json --out output/subtopics_batch.jsonl --prompt-out output/subtopics_batch_prompt.txt
```

### 8.2 インポート

```bash
python scripts/import_explanations.py --infile output/explanations_batch_filled.jsonl
python scripts/import_tags.py --infile output/tags_batch_filled.jsonl
python scripts/import_subtopics.py --infile output/subtopics_batch_filled.jsonl
```

## 9. タグ辞書と自動タグ補完

### 9.1 タグ説明生成 + 同義語結合

```bash
python scripts/build_tag_dictionary_with_gemini.py --db output/ahaki.sqlite --batch-size 8 --apply-merge
```

### 9.2 全タグから機械的にタグ補完

```bash
python scripts/auto_assign_tags_from_full_tags.py --db output/ahaki.sqlite
```

## 10. Gemini API一括生成 (CLI)

Geminiで解説・タグ・小項目を20問単位で生成し、自動インポートします。

`.env` 例:

```env
GEMINI_API_KEY=YOUR_KEY
```

実行例:

```bash
python scripts/run_gemini_combined.py --limit 20 --batches 3 --sleep-seconds 20
```

主なオプション:

- `--batches`: 実行回数 (`0` で上限到達まで)
- `--sleep-seconds`: バッチ間待機秒数
- `--max-per-day`: 1日上限
- `--model`: 使用モデル
- `--thinking-budget`: 思考予算
- `--rebuild-web` / `--no-rebuild-web`: JSON再生成の有無

### 10.1 深掘り解説の一括生成 (CLI)

深掘り解説（`deep_dive_explanations`）を Gemini で生成し、SQLite に直接登録します。  
対象は **未生成（`deep_dive_explanations` に未登録）** の問題のみです。

実行例:

```bash
# freeキーで生成
python scripts/run_gemini_deep_dive.py --limit 20 --api-key-source free

# paidキーで生成
python scripts/run_gemini_deep_dive.py --limit 20 --api-key-source paid
```

主なオプション:

- `--api-key-source`: `free` / `paid` / `auto` / `legacy`
- `--serials`: 対象シリアル指定（例: `A01-001,A01-002` or `A01-001..A01-020`）
- `--exam-type` / `--exam-session` / `--subject`: 生成対象フィルタ
- `--max-retries`: API/出力形式エラー時の再試行回数
- `--dry-run`: 1件分のプロンプトを表示して終了

## 11. バックアップ

最重要は `output/ahaki.sqlite` です。  
この1ファイルがあれば主要データの復旧が可能です。

## 12. トラブルシュート

- `fatal: ... .git/index.lock`
  - `rm -f .git/index.lock`
- Pagesが古い表示
  1. `generate_web_json.py` を再実行
  2. `prepare_pages.sh` を再実行
  3. ブラウザを強制リロード
- Supabase差分が反映されない
  - `web_app/config.js` のURL/キーを確認
  - Worker URLとRLS設定を確認
