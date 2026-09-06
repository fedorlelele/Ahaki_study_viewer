# 問題データの共有契約

`questions.js` は通常ビュー・かんたんビュー・プリント作成・タグ研究室から読み込む共通処理です。Node では `require()` で同じ実装を検証できます。

## 訂正の優先順位

- `question_overrides` は `serial` 順に500件ずつ取得し、検索索引を作る前に適用します。`synced_at` はローカル同期の状態であり、公開済みである証拠として使いません。
- 公開問題の `override_updated_at` は、SQLiteへ取り込んだクラウド行の `updated_at` です。クラウド行がこの時刻以下なら適用済みとして無視します。時刻が欠ける場合はクラウド差分を優先します。
- タグ研究室は軽量な `index/question_override_versions.json`（`{serial: override_updated_at}`）で同じ判定を行います。
- 解説の訂正は本文・source・モデル名・確認状態を一緒に更新します。新sourceが `model:NewModel:checked` のようにモデルを明示した場合はそのモデルを採用し、`teacher`・`llm_checked` 等の一般的な承認／編集マーカーだけ元モデルを引き継ぎます。本文だけが変わった場合は元モデルを保ち、`teacher_edited` として扱います。sourceの解析・生成も通常ビューと共通です。
- NULL・未指定は変更なし、空文字・空配列は明示的な削除です。正答の更新は `answer_indices` / `answer_index` が非NULL、または `answer_none === true` の場合です。`answer_none: false` だけの既存行は、正答を変更しません。
- 正答を訂正した場合、古い `answer_variants`・`answer_notes` を破棄します。媒体別の訂正を保存する契約が追加されるまでは、訂正後の正答を両媒体で使います。
- 訂正取得は8秒で打ち切り、部分取得は採用せず、公開済みデータで学習を継続します。画面とコピー・出力ヘッダに取得失敗を残します。

## 採点と表示

- `answer_variants: {default: [1], braille: [1,2]}` は通常用と点字用の違いです。`answer_indices` は通常用の後方互換フィールドです。
- 媒体設定は `ahaki_answer_medium_v1` で3画面に共有します。通常ビューの共有URLには `medium=braille` を付与します。
- `answer_text` は原記録、`answer_notes` は注記です。採点には選択媒体の番号を使い、注記は表示・コピー・印刷に残します。
- 遅延した表示要求は `createRenderGuard()` で破棄します。かんたんビューは表示が確定した問題を保持し、採点・記録に使います。
- `questionManifest.generated_at` を画面・出力のデータ版として表示します。

## 集計・進捗

回答集計は `GET /stats/answers?serials=...` を200問単位で取得します。レスポンスは `{ok:true,items:[{serial,total,correct}]}`。取得失敗を0件と扱わず、生の回答履歴へもフォールバックしません。問題表示は集計を待ちません。統計ソートだけは必要な集計を待ちます。

通常ビューの進捗送信は回答ごとの `event_id` を保持します。同じ回答の再送は同じIDを使い、明示的な再回答には新しいIDを割り当てます。

## 検証

```sh
node --test tests/test_frontend*.cjs
```

共有契約の単体試験に加えて、実HTML内の関数を使い、逆順通信、訂正後検索、媒体別印刷、印刷処理中の条件変更、キーボードによるタグ移動、権限判定、進捗IDの再利用を検証します。本番サービスへの通信は不要です。
