import argparse
import json
import os
import re
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse, quote
from urllib.request import Request, urlopen
from urllib.error import URLError


HTML_PAGE = """<!doctype html>
<html lang="ja">
  <head>
    <meta charset="utf-8" />
    <title>Ahaki Admin</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 24px; }
      label { display: block; margin: 12px 0 6px; }
      input[type="text"] { width: 100%; padding: 8px; }
      input[type="number"] { width: 120px; padding: 6px; }
      button { padding: 8px 16px; margin-top: 12px; }
      .row { margin-top: 16px; }
      .result { margin-top: 20px; }
      .tabs { display: flex; flex-wrap: wrap; gap: 8px; margin: 16px 0 20px; }
      .preview-card { margin-bottom: 12px; }
      .tab { padding: 8px 12px; border: 1px solid #bbb; background: #fff; cursor: pointer; border-radius: 8px; }
      .tab.active { background: #1a73e8; color: #fff; border-color: #1a73e8; }
      .import-tabs { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0 16px; }
      .import-tab { padding: 6px 10px; border: 1px solid #bbb; background: #fff; cursor: pointer; border-radius: 6px; }
      .import-tab.active { background: #1a73e8; color: #fff; border-color: #1a73e8; }
      .section[hidden] { display: none; }
      .section { margin-top: 28px; padding-top: 8px; border-top: 1px solid #ddd; }
      .report-panel { margin-top: 16px; padding-top: 8px; border-top: 1px dashed #ccc; }
      .download { margin-right: 12px; }
      .note { color: #555; font-size: 0.9em; }
      pre { background: #f6f6f6; padding: 8px; white-space: pre-wrap; }
    </style>
  </head>
  <body>
    <h1>ローカル管理画面</h1>
    <p class="note">解説・タグ・小項目のプロンプト生成/インポート/進捗確認をまとめて行います。</p>
    <div class="row">
      <button id="importClipboard">クリップボードからインポート</button>
      <button id="runBackup">SQLiteをバックアップ</button>
      <span id="clipboardImportResult" class="note"></span>
      <span id="backupResult" class="note"></span>
    </div>

    <div class="tabs" role="tablist" aria-label="管理タブ">
      <button class="tab active" data-target="prompts" role="tab" aria-selected="true">プロンプト</button>
      <button class="tab" data-target="imports" role="tab">インポート</button>
      <button class="tab" data-target="edits" role="tab">編集提案</button>
      <button class="tab" data-target="reports" role="tab">報告一覧</button>
      <button class="tab" data-target="cloud" role="tab">クラウド集計</button>
      <button class="tab" data-target="preview" role="tab">検索・プレビュー</button>
      <button class="tab" data-target="missing" role="tab">未設定一覧</button>
      <button class="tab" data-target="build" role="tab">ファイル生成</button>
      <button class="tab" data-target="progress" role="tab">進捗</button>
      <button class="tab" data-target="history" role="tab">履歴</button>
    </div>

    <div class="section" data-section="prompts">
    <label>対象シリアル（カンマ区切り / 範囲: B33-131..B33-180）</label>
    <input id="serials" type="text" placeholder="A09-001,A09-002 / B33-131..B33-180" />
    <div class="row">
      <label>件数（未指定時の上限）</label>
      <input id="limit" type="number" value="50" min="1" />
      <label><input id="unannotated" type="checkbox" checked /> 未設定のみ（解説・タグ・小項目が空）</label>
    </div>

    <div class="row">
      <label>試験種別</label>
      <select id="examType">
        <option value="">指定なし</option>
        <option value="A">A（あん摩マッサージ指圧師）</option>
        <option value="B">B（はり師・きゆう師）</option>
      </select>
      <label>回数</label>
      <input id="examSession" type="number" min="1" placeholder="例: 33" />
      <label>科目</label>
      <select id="subjectFilter"><option value="">指定なし</option></select>
    </div>

    <div class="row">
      <label><input type="radio" name="orderMode" value="new" checked /> 新しい順（A33/B33 → A01/B01）</label>
      <label><input type="radio" name="orderMode" value="old" /> 古い順（A01/B01 → A33/B33）</label>
    </div>

    <div class="row">
      <label><input type="radio" name="outputMode" value="download" /> ダウンロード</label>
      <label><input type="radio" name="outputMode" value="clipboard" checked /> クリップボード</label>
    </div>

    <div class="row">
      <label><input type="checkbox" id="promptExplain" checked /> 解説</label>
      <label><input type="checkbox" id="promptTag" checked /> タグ</label>
      <label><input type="checkbox" id="promptSubtopic" checked /> 小項目</label>
    </div>

    <button id="generate">プロンプト生成</button>

    <div class="result" id="result"></div>
    </div>

    <div class="section" data-section="imports" hidden>
      <h2>インポート</h2>
      <p class="note">解説・タグ・小項目のJSONLをアップロードしてSQLiteに取り込みます。</p>

      <div class="import-tabs" role="tablist" aria-label="インポートタブ">
        <button class="import-tab active" data-import-target="import-files" role="tab">ファイル</button>
        <button class="import-tab" data-import-target="import-bulk" role="tab">フォルダ</button>
        <button class="import-tab" data-import-target="import-downloads" role="tab">ダウンロード</button>
        <button class="import-tab" data-import-target="import-paste" role="tab">貼り付け</button>
      </div>

      <div class="section" data-import-section="import-files">
        <label>解説JSONL</label>
        <input id="explanationsFile" type="file" accept=".jsonl" />
        <div>
          <label>解説バージョン（空欄で自動）</label>
          <input id="explanationVersion" type="number" min="1" placeholder="auto" />
          <label>解説インポート方式</label>
          <select id="explanationMode">
            <option value="append">追記</option>
            <option value="skip">既存があればスキップ</option>
            <option value="replace">既存を置き換え</option>
          </select>
        </div>
        <button id="importExplanations">解説をインポート</button>

        <label>タグJSONL</label>
        <input id="tagsFile" type="file" accept=".jsonl" />
        <div>
          <label>タグインポート方式</label>
          <select id="tagMode">
            <option value="append">追記</option>
            <option value="skip">既存があればスキップ</option>
            <option value="replace">既存を置き換え</option>
          </select>
        </div>
        <button id="importTags">タグをインポート</button>

        <label>小項目JSONL</label>
        <input id="subtopicsFile" type="file" accept=".jsonl" />
        <div>
          <label>小項目インポート方式</label>
          <select id="subtopicMode">
            <option value="append">追記</option>
            <option value="skip">既存があればスキップ</option>
            <option value="replace">既存を置き換え</option>
          </select>
        </div>
        <button id="importSubtopics">小項目をインポート</button>

        <label>解説・タグ・小項目 同時JSONL</label>
        <input id="combinedFile" type="file" accept=".jsonl" />
        <p class="note">上記のインポート方式が適用されます。</p>
        <button id="importCombined">同時JSONLをインポート</button>
      </div>

      <div class="section" data-import-section="import-bulk" hidden>
        <label>一括インポート（フォルダ指定）</label>
        <input id="bulkFolder" type="file" webkitdirectory directory multiple />
        <button id="bulkImport">フォルダ内を一括インポート</button>
      </div>

      <div class="section" data-import-section="import-downloads" hidden>
        <button id="importDownloads">ダウンロードフォルダから一括インポート</button>
      </div>

      <div class="section" data-import-section="import-paste" hidden>
        <p class="note">出力を貼り付けてインポートできます（内容を自動判別）。</p>
        <label>JSONLを貼り付け</label>
        <textarea id="jsonlPaste" rows="8" style="width: 100%;"></textarea>
        <div class="row">
          <button id="importPaste">貼り付けをインポート</button>
        </div>
      </div>

      <div class="result" id="importResult"></div>
    </div>

    <div class="section" data-section="progress" hidden>
      <h2>進捗レポート</h2>
      <button id="loadProgress">進捗を表示</button>
      <div id="progressResult"></div>
    </div>

    <div class="section" data-section="build" hidden>
      <h2>ファイル生成</h2>
      <p class="note">WebUI表示用のJSONや学習用セットをまとめて生成します。</p>
      <button id="buildWeb">Web表示用ファイルを生成</button>
      <button id="buildAll">一括生成（Web/学習/進捗）</button>
      <pre id="buildResult"></pre>
    </div>

    <div class="section" data-section="history" hidden>
      <h2>履歴（最新20件）</h2>
      <button id="loadHistory">履歴を表示</button>
      <div id="historyResult"></div>
    </div>

    <div class="section" data-section="preview" hidden>
      <h2>検索・プレビュー</h2>
      <label>シリアルまたはキーワード</label>
      <input id="previewQuery" type="text" placeholder="A09-001 / キーワード" />
      <button id="runPreview">検索</button>
      <div id="previewResult"></div>
    </div>

    <div class="section" data-section="missing" hidden>
      <h2>未設定一覧</h2>
      <label><input type="checkbox" id="missingExplanations" checked /> 解説なし</label>
      <label><input type="checkbox" id="missingTags" checked /> タグなし</label>
      <label><input type="checkbox" id="missingSubtopics" checked /> 小項目なし</label>
      <button id="loadMissing">一覧表示</button>
      <button id="downloadMissingCsv">CSVダウンロード</button>
      <div id="missingResult"></div>
    </div>

    <div class="section" data-section="reports" hidden>
      <h2>報告一覧</h2>
      <p class="note">Supabaseのfeedbackを参照します（SUPABASE_URL / SUPABASE_SERVICE_KEY が必要）。</p>
      <button id="loadReports">報告を表示</button>
      <div class="report-panel">
        <strong>プロンプト対象にセットする条件</strong>
        <div class="note">ここで選んだ種別の報告だけが「プロンプト対象にセット」に反映されます。</div>
        <div class="row">
          <label><input type="checkbox" id="reportExplain" checked /> 解説</label>
          <label><input type="checkbox" id="reportTag" checked /> タグ</label>
          <label><input type="checkbox" id="reportSubtopic" checked /> 小項目</label>
        </div>
        <button id="useReports">報告をプロンプト対象にセット</button>
      </div>
      <div class="report-panel">
        <strong>消去する報告フラグ</strong>
        <div class="note">表のチェックボックスを選択してから消去します。</div>
      <div class="row">
        <button id="checkAllReports">全てチェック</button>
        <button id="uncheckAllReports">全て解除</button>
      </div>
      <button id="clearReports">チェックした報告フラグを消去</button>
      </div>
      <div id="reportResult"></div>
    </div>

    <div class="section" data-section="edits" hidden>
      <h2>編集提案</h2>
      <p class="note">Supabaseのedit_requestsを参照します（SUPABASE_URL / SUPABASE_SERVICE_KEY が必要）。</p>
      <button id="loadEditRequests">編集提案を表示</button>
      <div class="row">
        <button id="applyEditRequests">SQLiteに反映</button>
        <button id="dismissEditRequests">却下（ステータス更新）</button>
      </div>
      <div id="editRequestResult"></div>
    </div>
    <div class="section" data-section="cloud" hidden>
      <h2>クラウド集計</h2>
      <p class="note">Supabaseの集計を表示します。環境変数 SUPABASE_URL と SUPABASE_SERVICE_KEY が必要です。</p>
      <label>取得件数（最大）</label>
      <input id="cloudLimit" type="number" value="1000" min="1" />
      <div class="row">
        <button id="loadCloudFeedback">報告一覧を表示</button>
        <button id="loadCloudAnswers">正答率集計を表示</button>
      </div>
      <div id="cloudFeedbackResult"></div>
      <div id="cloudAnswersResult"></div>
    </div>

    <script>
      function downloadText(filename, text) {
        const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      }

      function getSelectedPromptKinds() {
        const kinds = [];
        if (document.getElementById("promptExplain").checked) kinds.push("explanation");
        if (document.getElementById("promptTag").checked) kinds.push("tag");
        if (document.getElementById("promptSubtopic").checked) kinds.push("subtopic");
        return kinds;
      }

      document.getElementById("generate").addEventListener("click", async () => {
        const serials = document.getElementById("serials").value.trim();
        const limit = document.getElementById("limit").value;
        const unannotated = document.getElementById("unannotated").checked ? "1" : "0";
        const orderMode = document.querySelector("input[name='orderMode']:checked").value;
        const outputMode = document.querySelector("input[name='outputMode']:checked").value;
        const examType = document.getElementById("examType").value;
        const examSession = document.getElementById("examSession").value;
        const subjectFilter = document.getElementById("subjectFilter").value.trim();
        const promptKinds = getSelectedPromptKinds();
        if (!promptKinds.length) {
          document.getElementById("result").textContent = "プロンプト種別を選択してください。";
          return;
        }
        const params = new URLSearchParams();
        if (serials) params.set("serials", serials);
        params.set("limit", limit);
        params.set("unannotated", unannotated);
        params.set("order", orderMode);
        if (examType) params.set("exam_type", examType);
        if (examSession) params.set("exam_session", examSession);
        if (subjectFilter) params.set("subject", subjectFilter);
        params.set("kinds", promptKinds.join(","));

        const result = document.getElementById("result");
        result.textContent = "生成中...";
        const resp = await fetch("/api/prompts?" + params.toString());
        if (!resp.ok) {
          result.textContent = "エラー: " + resp.status;
          return;
        }
        const data = await resp.json();
        if (!data.count) {
          result.textContent = "対象の問題が見つかりませんでした。";
          return;
        }

        result.innerHTML = "";

        const info = document.createElement("div");
        info.textContent = "対象問題数: " + data.count;
        result.appendChild(info);

        const btns = document.createElement("div");
        btns.className = "row";

        if (outputMode === "download") {
          if (data.explanations.enabled) {
            const explainBtn = document.createElement("button");
            explainBtn.textContent = "解説プロンプトをダウンロード";
            explainBtn.className = "download";
            explainBtn.onclick = () => downloadText(data.explanations.filename, data.explanations.text);
            btns.appendChild(explainBtn);
          }

          if (data.tags.enabled) {
            const tagBtn = document.createElement("button");
            tagBtn.textContent = "タグプロンプトをダウンロード";
            tagBtn.className = "download";
            tagBtn.onclick = () => downloadText(data.tags.filename, data.tags.text);
            btns.appendChild(tagBtn);
          }

          if (data.subtopics.enabled) {
            const subBtn = document.createElement("button");
            subBtn.textContent = "小項目プロンプトをダウンロード";
            subBtn.className = "download";
            subBtn.onclick = () => downloadText(data.subtopics.filename, data.subtopics.text);
            btns.appendChild(subBtn);
          }

          const combinedBtn = document.createElement("button");
          combinedBtn.textContent = "解説・タグ・小項目プロンプトをダウンロード";
          combinedBtn.className = "download";
          combinedBtn.onclick = () => downloadText(data.combined.filename, data.combined.text);
          btns.appendChild(combinedBtn);
        } else {
          if (data.explanations.enabled) {
            const explainBtn = document.createElement("button");
            explainBtn.textContent = "解説プロンプトをコピー";
            explainBtn.className = "download";
            explainBtn.onclick = () => copyToClipboard(data.explanations.text);
            btns.appendChild(explainBtn);
          }

          if (data.tags.enabled) {
            const tagBtn = document.createElement("button");
            tagBtn.textContent = "タグプロンプトをコピー";
            tagBtn.className = "download";
            tagBtn.onclick = () => copyToClipboard(data.tags.text);
            btns.appendChild(tagBtn);
          }

          if (data.subtopics.enabled) {
            const subBtn = document.createElement("button");
            subBtn.textContent = "小項目プロンプトをコピー";
            subBtn.className = "download";
            subBtn.onclick = () => copyToClipboard(data.subtopics.text);
            btns.appendChild(subBtn);
          }

          const combinedBtn = document.createElement("button");
          combinedBtn.textContent = "解説・タグ・小項目プロンプトをコピー";
          combinedBtn.className = "download";
          combinedBtn.onclick = () => copyToClipboard(data.combined.text);
          btns.appendChild(combinedBtn);
        }

        result.appendChild(btns);
      });

      async function uploadFile(endpoint, file) {
        const form = new FormData();
        form.append("file", file);
        const resp = await fetch(endpoint, { method: "POST", body: form });
        if (!resp.ok) {
          throw new Error("エラー: " + resp.status);
        }
        return resp.json();
      }

      async function uploadText(endpoint, payload) {
        const resp = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!resp.ok) {
          throw new Error("エラー: " + resp.status);
        }
        return resp.json();
      }

      async function importFile(endpoint, fileInputId) {
        const fileInput = document.getElementById(fileInputId);
        const result = document.getElementById("importResult");
        if (!fileInput.files.length) {
          result.textContent = "ファイルを選択してください。";
          return;
        }
        result.textContent = "インポート中...";
        const data = await uploadFile(endpoint, fileInput.files[0]);
        result.textContent = data.message || "完了しました。";
      }

      document.getElementById("importExplanations").addEventListener("click", () => {
        const mode = document.getElementById("explanationMode").value;
        const version = document.getElementById("explanationVersion").value;
        const versionParam = version ? "&version=" + version : "&version=auto";
        importFile("/api/import/explanations?mode=" + mode + versionParam, "explanationsFile");
      });
      document.getElementById("importTags").addEventListener("click", () => {
        const mode = document.getElementById("tagMode").value;
        importFile("/api/import/tags?mode=" + mode, "tagsFile");
      });
      document.getElementById("importSubtopics").addEventListener("click", () => {
        const mode = document.getElementById("subtopicMode").value;
        importFile("/api/import/subtopics?mode=" + mode, "subtopicsFile");
      });

      document.getElementById("importCombined").addEventListener("click", () => {
        const modeExp = document.getElementById("explanationMode").value;
        const modeTag = document.getElementById("tagMode").value;
        const modeSub = document.getElementById("subtopicMode").value;
        const version = document.getElementById("explanationVersion").value;
        const versionParam = version ? "&version=" + version : "&version=auto";
        importFile(
          "/api/import/combined?modeExp=" + modeExp + versionParam + "&modeTag=" + modeTag + "&modeSub=" + modeSub,
          "combinedFile"
        );
      });

      function detectJsonlKind(text) {
        const lines = text.split("\\n").map(line => line.trim()).filter(line => line);
        let kind = "";
        for (const line of lines) {
          let obj = null;
          try {
            obj = JSON.parse(line);
          } catch (err) {
            return { kind: "", error: "JSONLの形式が正しくありません。" };
          }
          if (!obj || typeof obj !== "object") {
            return { kind: "", error: "JSONLの形式が正しくありません。" };
          }
          const hasExplanation = "explanation" in obj;
          const hasTags = "tags" in obj;
          const hasSubtopics = "subtopics" in obj;
          if (hasExplanation && hasTags && hasSubtopics) {
            if (kind && kind !== "combined") {
              return { kind: "", error: "複数種別が混在しています。" };
            }
            kind = "combined";
            continue;
          }
          if (hasExplanation && !hasTags && !hasSubtopics) {
            if (kind && kind !== "explanation") {
              return { kind: "", error: "複数種別が混在しています。" };
            }
            kind = "explanation";
            continue;
          }
          if (hasTags && !hasExplanation && !hasSubtopics) {
            if (kind && kind !== "tag") {
              return { kind: "", error: "複数種別が混在しています。" };
            }
            kind = "tag";
            continue;
          }
          if (hasSubtopics && !hasExplanation && !hasTags) {
            if (kind && kind !== "subtopic") {
              return { kind: "", error: "複数種別が混在しています。" };
            }
            kind = "subtopic";
            continue;
          }
          return { kind: "", error: "種別を判別できませんでした。" };
        }
        return { kind: kind, error: "" };
      }

      async function importJsonlText(text, resultElement, onDone) {
        const detected = detectJsonlKind(text);
        if (!detected.kind) {
          resultElement.textContent = detected.error;
          return;
        }
        resultElement.textContent = "インポート中...";
        if (detected.kind === "explanation") {
          const mode = document.getElementById("explanationMode").value;
          const version = document.getElementById("explanationVersion").value;
          const payload = { text: text, mode: mode, version: version || "auto" };
          const data = await uploadText("/api/import/explanations_text", payload);
          resultElement.textContent = data.message || "完了しました。";
          if (onDone) onDone();
          return;
        }
        if (detected.kind === "tag") {
          const mode = document.getElementById("tagMode").value;
          const payload = { text: text, mode: mode };
          const data = await uploadText("/api/import/tags_text", payload);
          resultElement.textContent = data.message || "完了しました。";
          if (onDone) onDone();
          return;
        }
        if (detected.kind === "subtopic") {
          const mode = document.getElementById("subtopicMode").value;
          const payload = { text: text, mode: mode };
          const data = await uploadText("/api/import/subtopics_text", payload);
          resultElement.textContent = data.message || "完了しました。";
          if (onDone) onDone();
          return;
        }
        if (detected.kind === "combined") {
          const modeExp = document.getElementById("explanationMode").value;
          const modeTag = document.getElementById("tagMode").value;
          const modeSub = document.getElementById("subtopicMode").value;
          const version = document.getElementById("explanationVersion").value;
          const payload = {
            text: text,
            modeExp: modeExp,
            modeTag: modeTag,
            modeSub: modeSub,
            version: version || "auto",
          };
          const data = await uploadText("/api/import/combined_text", payload);
          resultElement.textContent = data.message || "完了しました。";
          if (onDone) onDone();
          return;
        }
        resultElement.textContent = "種別を判別できませんでした。";
      }

      document.getElementById("importPaste").addEventListener("click", async () => {
        const textArea = document.getElementById("jsonlPaste");
        const text = textArea.value.trim();
        if (!text) {
          document.getElementById("importResult").textContent = "貼り付け内容が空です。";
          return;
        }
        await importJsonlText(text, document.getElementById("importResult"), () => {
          textArea.value = "";
        });
      });

      document.getElementById("importClipboard").addEventListener("click", async () => {
        const result = document.getElementById("clipboardImportResult");
        result.textContent = "クリップボードを読み取り中...";
        try {
          const text = await navigator.clipboard.readText();
          if (!text.trim()) {
            result.textContent = "クリップボードが空です。";
            return;
          }
          await importJsonlText(text, result);
        } catch (err) {
          result.textContent = "クリップボードを読み取れませんでした。";
        }
      });

      document.getElementById("runBackup").addEventListener("click", async () => {
        const result = document.getElementById("backupResult");
        result.textContent = "バックアップ中...";
        try {
          const resp = await fetch("/api/backup", { method: "POST" });
          const data = await resp.json();
          result.textContent = data.message || "完了しました。";
        } catch (err) {
          result.textContent = "バックアップに失敗しました。";
        }
      });

      document.getElementById("bulkImport").addEventListener("click", async () => {
        const folderInput = document.getElementById("bulkFolder");
        const result = document.getElementById("importResult");
        if (!folderInput.files.length) {
          result.textContent = "フォルダを選択してください。";
          return;
        }
        const files = Array.from(folderInput.files);
        const fileMap = {};
        files.forEach(file => {
          fileMap[file.name] = file;
        });
        result.textContent = "一括インポート中...";
        const modeExp = document.getElementById("explanationMode").value;
        const version = document.getElementById("explanationVersion").value;
        const versionParam = version ? "&version=" + version : "&version=auto";
        const modeTag = document.getElementById("tagMode").value;
        const modeSub = document.getElementById("subtopicMode").value;
        const combinedFile = fileMap["explanations_tags_subtopics_batch_filled.jsonl"];
        if (combinedFile) {
          const combinedRes = await uploadFile(
            "/api/import/combined?modeExp=" + modeExp + versionParam + "&modeTag=" + modeTag + "&modeSub=" + modeSub,
            combinedFile
          );
          result.textContent = combinedRes.message || "完了しました。";
          return;
        }
        const expFile = fileMap["explanations_batch_filled.jsonl"];
        const tagFile = fileMap["tags_batch_filled.jsonl"];
        const subFile = fileMap["subtopics_batch_filled.jsonl"];
        const missing = [];
        if (!expFile) missing.push("explanations_batch_filled.jsonl");
        if (!tagFile) missing.push("tags_batch_filled.jsonl");
        if (!subFile) missing.push("subtopics_batch_filled.jsonl");
        if (missing.length) {
          result.textContent = "不足ファイル: " + missing.join(", ");
          return;
        }
        const expRes = await uploadFile("/api/import/explanations?mode=" + modeExp + versionParam, expFile);
        const tagRes = await uploadFile("/api/import/tags?mode=" + modeTag, tagFile);
        const subRes = await uploadFile("/api/import/subtopics?mode=" + modeSub, subFile);
        result.textContent = [expRes.message, tagRes.message, subRes.message].join(" / ");
      });

      document.getElementById("importDownloads").addEventListener("click", async () => {
        const result = document.getElementById("importResult");
        result.textContent = "ダウンロードフォルダからインポート中...";
        const modeExp = document.getElementById("explanationMode").value;
        const version = document.getElementById("explanationVersion").value;
        const versionParam = version ? "&version=" + version : "&version=auto";
        const modeTag = document.getElementById("tagMode").value;
        const modeSub = document.getElementById("subtopicMode").value;
        const resp = await fetch(
          "/api/import/downloads?modeExp=" + modeExp + versionParam + "&modeTag=" + modeTag + "&modeSub=" + modeSub,
          { method: "POST" }
        );
        const data = await resp.json();
        result.textContent = data.message || "完了しました。";
      });

      async function loadSubjects() {
        const resp = await fetch("/api/subjects");
        const data = await resp.json();
        const select = document.getElementById("subjectFilter");
        data.forEach(name => {
          const opt = document.createElement("option");
          opt.value = name;
          opt.textContent = name;
          select.appendChild(opt);
        });
      }

      loadSubjects();

      document.getElementById("loadProgress").addEventListener("click", async () => {
        const resp = await fetch("/api/progress");
        const data = await resp.json();
        document.getElementById("progressResult").innerHTML = renderProgress(data);
      });

      document.getElementById("buildWeb").addEventListener("click", async () => {
        const result = document.getElementById("buildResult");
        result.textContent = "生成中...";
        const resp = await fetch("/api/build/web", { method: "POST" });
        const data = await resp.json();
        result.textContent = data.message || "完了しました。";
      });

      document.getElementById("buildAll").addEventListener("click", async () => {
        const result = document.getElementById("buildResult");
        result.textContent = "生成中...";
        const resp = await fetch("/api/build/all", { method: "POST" });
        const data = await resp.json();
        result.textContent = data.message || "完了しました。";
      });

      document.getElementById("loadHistory").addEventListener("click", async () => {
        const resp = await fetch("/api/history");
        const data = await resp.json();
        document.getElementById("historyResult").innerHTML = renderHistory(data);
      });

      function copyToClipboard(text) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text);
          return;
        }
        const textarea = document.createElement("textarea");
        textarea.value = text;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand("copy");
        textarea.remove();
      }

      document.getElementById("runPreview").addEventListener("click", async () => {
        const query = document.getElementById("previewQuery").value.trim();
        if (!query) {
          document.getElementById("previewResult").textContent = "検索語を入力してください。";
          return;
        }
        const resp = await fetch("/api/preview?q=" + encodeURIComponent(query));
        const data = await resp.json();
        document.getElementById("previewResult").innerHTML = renderPreview(data);
      });

      function getMissingParams() {
        const params = new URLSearchParams();
        if (document.getElementById("missingExplanations").checked) params.set("explanations", "1");
        if (document.getElementById("missingTags").checked) params.set("tags", "1");
        if (document.getElementById("missingSubtopics").checked) params.set("subtopics", "1");
        return params;
      }

      document.getElementById("loadMissing").addEventListener("click", async () => {
        const params = getMissingParams();
        const resp = await fetch("/api/missing?" + params.toString());
        const data = await resp.json();
        document.getElementById("missingResult").innerHTML = renderMissing(data);
      });

      document.getElementById("downloadMissingCsv").addEventListener("click", async () => {
        const params = getMissingParams();
        const resp = await fetch("/api/missing.csv?" + params.toString());
        const text = await resp.text();
        const blob = new Blob([text], { type: "text/csv;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "missing_items.csv";
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      });

      let reportItems = [];

      document.getElementById("loadReports").addEventListener("click", async () => {
        const resp = await fetch("/api/reports");
        const data = await resp.json();
        reportItems = data.items || [];
        document.getElementById("reportResult").innerHTML = renderReports(data);
      });

      let editItems = [];

      document.getElementById("loadEditRequests").addEventListener("click", async () => {
        const resp = await fetch("/api/edit_requests");
        const data = await resp.json();
        editItems = data.items || [];
        document.getElementById("editRequestResult").innerHTML = renderEditRequests(data);
      });

      document.getElementById("applyEditRequests").addEventListener("click", async () => {
        const selected = collectSelectedEdits();
        if (!selected.length) {
          document.getElementById("editRequestResult").textContent = "対象を選択してください。";
          return;
        }
        const resp = await fetch("/api/edit_requests/apply", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ items: selected }),
        });
        const data = await resp.json();
        document.getElementById("editRequestResult").textContent = data.message || "完了しました。";
      });

      document.getElementById("dismissEditRequests").addEventListener("click", async () => {
        const selected = collectSelectedEdits();
        if (!selected.length) {
          document.getElementById("editRequestResult").textContent = "対象を選択してください。";
          return;
        }
        const resp = await fetch("/api/edit_requests/dismiss", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ items: selected }),
        });
        const data = await resp.json();
        document.getElementById("editRequestResult").textContent = data.message || "完了しました。";
      });

      function getSelectedReportKinds() {
        const kinds = [];
        if (document.getElementById("reportExplain").checked) kinds.push("explanation");
        if (document.getElementById("reportTag").checked) kinds.push("tag");
        if (document.getElementById("reportSubtopic").checked) kinds.push("subtopic");
        return kinds;
      }

      document.getElementById("useReports").addEventListener("click", () => {
        if (!reportItems.length) {
          document.getElementById("reportResult").textContent = "報告がありません。";
          return;
        }
        const kinds = getSelectedReportKinds();
        if (!kinds.length) {
          document.getElementById("reportResult").textContent = "対象種別を選択してください。";
          return;
        }
        const serialSet = new Set();
        reportItems.forEach(item => {
          if (kinds.includes("explanation") && item.explanation) serialSet.add(item.serial);
          if (kinds.includes("tag") && item.tag) serialSet.add(item.serial);
          if (kinds.includes("subtopic") && item.subtopic) serialSet.add(item.serial);
        });
        const serials = Array.from(serialSet);
        if (!serials.length) {
          document.getElementById("reportResult").textContent = "対象の報告がありません。";
          return;
        }
        document.getElementById("serials").value = serials.join(",");
        document.getElementById("unannotated").checked = false;
        document.getElementById("promptExplain").checked = kinds.includes("explanation");
        document.getElementById("promptTag").checked = kinds.includes("tag");
        document.getElementById("promptSubtopic").checked = kinds.includes("subtopic");
      });

      document.getElementById("clearReports").addEventListener("click", async () => {
        const selected = {};
        document.querySelectorAll("input[data-report]").forEach(input => {
          if (!input.checked) return;
          const serial = input.getAttribute("data-serial");
          const kind = input.getAttribute("data-kind");
          if (!selected[serial]) selected[serial] = [];
          selected[serial].push(kind);
        });
        const items = Object.keys(selected).map(serial => ({
          serial: serial,
          kinds: selected[serial],
        }));
        if (!items.length) {
          document.getElementById("reportResult").textContent = "消去するフラグを選択してください。";
          return;
        }
        const resp = await fetch("/api/reports/clear", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ items: items }),
        });
        const data = await resp.json();
        reportItems = [];
        document.getElementById("reportResult").textContent = data.message || "消去しました。";
      });

      document.getElementById("checkAllReports").addEventListener("click", () => {
        document.querySelectorAll("input[data-report]").forEach(input => {
          input.checked = true;
        });
      });

      document.getElementById("uncheckAllReports").addEventListener("click", () => {
        document.querySelectorAll("input[data-report]").forEach(input => {
          input.checked = false;
        });
      });

      document.getElementById("loadCloudFeedback").addEventListener("click", async () => {
        const limit = document.getElementById("cloudLimit").value || "1000";
        const resp = await fetch("/api/supabase/feedback?limit=" + encodeURIComponent(limit));
        const data = await resp.json();
        document.getElementById("cloudFeedbackResult").innerHTML = renderCloudFeedback(data);
      });

      document.getElementById("loadCloudAnswers").addEventListener("click", async () => {
        const limit = document.getElementById("cloudLimit").value || "1000";
        const resp = await fetch("/api/supabase/answers?limit=" + encodeURIComponent(limit));
        const data = await resp.json();
        document.getElementById("cloudAnswersResult").innerHTML = renderCloudAnswers(data);
      });

      function renderProgress(data) {
        if (!data || !data.total_questions) return "<div>データがありません。</div>";
        var rows = "";
        (data.by_subject || []).forEach(function(item) {
          rows += "<tr>" +
            "<td>" + item.subject + "</td>" +
            "<td>" + item.total_questions + "</td>" +
            "<td>" + item.explained + "</td>" +
            "<td>" + item.tagged + "</td>" +
            "<td>" + item.subtopic_assigned + "</td>" +
            "</tr>";
        });
        return "<div>全体: " + data.total_questions +
          " / 解説 " + data.explained +
          " / タグ " + data.tagged +
          " / 小項目 " + data.subtopic_assigned + "</div>" +
          "<table border='1' cellspacing='0' cellpadding='4'>" +
            "<thead><tr><th>科目</th><th>総数</th><th>解説</th><th>タグ</th><th>小項目</th></tr></thead>" +
            "<tbody>" + rows + "</tbody>" +
          "</table>";
      }

      function renderHistory(data) {
        if (!data || !data.length) return "<div>履歴がありません。</div>";
        var rows = "";
        data.forEach(function(item) {
          rows += "<tr>" +
            "<td>" + item.type + "</td>" +
            "<td>" + item.serial + "</td>" +
            "<td><details><summary>" + escapeHtml(item.text).slice(0, 120) + "</summary>" +
            "<div>" + escapeHtml(item.text) + "</div></details></td>" +
            "</tr>";
        });
        return "<table border='1' cellspacing='0' cellpadding='4'>" +
          "<thead><tr><th>種別</th><th>シリアル</th><th>内容</th></tr></thead>" +
          "<tbody>" + rows + "</tbody>" +
          "</table>";
      }

      function renderPreview(data) {
        if (!data || !data.length) return "<div>該当なし</div>";
        var out = "";
        data.forEach(function(item) {
          var expHtml = "";
          if (item.explanations && item.explanations.length) {
            item.explanations.forEach(function(exp) {
              var label = exp.version ? "v" + exp.version : "";
              expHtml += "<div><strong>" + label + "</strong> " +
                escapeHtml(exp.body || "") + "</div>";
            });
          } else {
            expHtml = "<div>(未登録)</div>";
          }

          out += "<div class='preview-card'>" +
            "<div><strong>" + item.serial + "</strong> / " + item.subject + "</div>" +
            "<div>" + escapeHtml(item.stem) + "</div>" +
            "<div>解説:</div>" +
            "<div>" + expHtml + "</div>" +
            "<div>タグ: " + (item.tags.join(", ") || "(なし)") + "</div>" +
            "<div>小項目: " + (item.subtopics.join(", ") || "(なし)") + "</div>" +
          "</div>";
        });
        return out;
      }

      function renderMissing(data) {
        if (!data || !data.length) return "<div>該当なし</div>";
        var rows = "";
        data.forEach(function(item) {
          rows += "<tr>" +
            "<td>" + item.serial + "</td>" +
            "<td>" + (item.subject || "") + "</td>" +
            "<td>" + escapeHtml(item.stem).slice(0, 120) + "</td>" +
            "</tr>";
        });
        return "<table border='1' cellspacing='0' cellpadding='4'>" +
          "<thead><tr><th>シリアル</th><th>科目</th><th>問題文</th></tr></thead>" +
          "<tbody>" + rows + "</tbody>" +
          "</table>";
      }

      function renderReports(data) {
        if (!data || !data.items || !data.items.length) {
          if (data && data.message) {
            return "<div>" + data.message + "</div>";
          }
          return "<div>報告がありません。</div>";
        }
        var rows = "";
        data.items.forEach(function(item) {
          var explainBox = item.explanation
            ? "<input type='checkbox' data-report='1' data-serial='" + item.serial +
              "' data-kind='explanation' checked />"
            : "-";
          var tagBox = item.tag
            ? "<input type='checkbox' data-report='1' data-serial='" + item.serial +
              "' data-kind='tag' checked />"
            : "-";
          var subBox = item.subtopic
            ? "<input type='checkbox' data-report='1' data-serial='" + item.serial +
              "' data-kind='subtopic' checked />"
            : "-";
          rows += "<tr>" +
            "<td>" + item.serial + "</td>" +
            "<td>" + explainBox + "</td>" +
            "<td>" + tagBox + "</td>" +
            "<td>" + subBox + "</td>" +
            "<td>" + item.reported_at + "</td>" +
            "</tr>";
        });
        return "<div>件数: " + data.count + "</div>" +
          "<table border='1' cellspacing='0' cellpadding='4'>" +
            "<thead><tr><th>シリアル</th><th>解説</th><th>タグ</th><th>小項目</th><th>日時</th></tr></thead>" +
            "<tbody>" + rows + "</tbody>" +
          "</table>";
      }

      function collectSelectedEdits() {
        const selected = [];
        document.querySelectorAll("input[data-edit-id]").forEach(input => {
          if (!input.checked) return;
          selected.push({
            id: input.getAttribute("data-edit-id"),
            serial: input.getAttribute("data-edit-serial"),
            kind: input.getAttribute("data-edit-kind"),
          });
        });
        return selected;
      }

      function renderEditRequests(data) {
        if (!data || !data.items || !data.items.length) {
          if (data && data.message) {
            return "<div>" + data.message + "</div>";
          }
          return "<div>編集提案がありません。</div>";
        }
        function formatEditKind(kind) {
          if (kind === "explanation_edit") return "解説";
          if (kind === "explanation_confirm") return "解説（承認）";
          if (kind === "tags_edit") return "タグ";
          if (kind === "subtopics_edit") return "小項目";
          return kind || "";
        }
        function renderEditPayload(item) {
          var payload = item.payload || {};
          var kind = item.kind || "";
          if (kind === "explanation_edit") {
            var body = String(payload.body || "").trim();
            if (!body) return "<span>解説本文なし</span>";
            return "<details><summary>解説本文</summary><pre>" + escapeHtml(body) + "</pre></details>";
          }
          if (kind === "explanation_confirm") {
            return "<span>AI解説を承認</span>";
          }
          if (kind === "tags_edit" || kind === "subtopics_edit") {
            var add = (payload.add || []).map(String).filter(Boolean);
            var remove = (payload.remove || []).map(String).filter(Boolean);
            var html = "";
            if (add.length) html += "<div>追加: " + escapeHtml(add.join(", ")) + "</div>";
            if (remove.length) html += "<div>削除: " + escapeHtml(remove.join(", ")) + "</div>";
            if (!html) html = "<span>変更なし</span>";
            return html;
          }
          return "<details><summary>内容</summary><pre>" + escapeHtml(JSON.stringify(payload || {}, null, 2)) + "</pre></details>";
        }
        var rows = "";
        data.items.forEach(function(item) {
          rows += "<tr>" +
            "<td><input type='checkbox' data-edit-id='" + item.id +
            "' data-edit-serial='" + item.serial + "' data-edit-kind='" + item.kind + "' /></td>" +
            "<td>" + item.serial + "</td>" +
            "<td>" + formatEditKind(item.kind) + "</td>" +
            "<td>" + escapeHtml(item.note || "") + "</td>" +
            "<td>" + renderEditPayload(item) + "</td>" +
            "<td>" + (item.created_email || "") + "</td>" +
            "<td>" + item.created_at + "</td>" +
            "</tr>";
        });
        return "<div>件数: " + data.count + "</div>" +
          "<table border='1' cellspacing='0' cellpadding='4'>" +
            "<thead><tr><th>選択</th><th>シリアル</th><th>種別</th><th>コメント</th><th>内容</th><th>作成者</th><th>日時</th></tr></thead>" +
            "<tbody>" + rows + "</tbody>" +
          "</table>";
      }

      function renderCloudFeedback(data) {
        if (!data || !data.ok) return "<div>" + (data && data.message ? data.message : "取得に失敗しました。") + "</div>";
        if (!data.items || !data.items.length) return "<div>報告がありません。</div>";
        var rows = "";
        data.items.forEach(function(item) {
          rows += "<tr>" +
            "<td>" + item.serial + "</td>" +
            "<td>" + item.explanation_count + "</td>" +
            "<td>" + item.tag_count + "</td>" +
            "<td>" + item.subtopic_count + "</td>" +
            "<td>" + item.total + "</td>" +
            "<td>" + item.last_reported_at + "</td>" +
            "</tr>";
        });
        return "<div>件数: " + data.count + "</div>" +
          "<table border='1' cellspacing='0' cellpadding='4'>" +
            "<thead><tr><th>シリアル</th><th>解説</th><th>タグ</th><th>小項目</th><th>合計</th><th>最新日時</th></tr></thead>" +
            "<tbody>" + rows + "</tbody>" +
          "</table>";
      }

      function renderCloudAnswers(data) {
        if (!data || !data.ok) return "<div>" + (data && data.message ? data.message : "取得に失敗しました。") + "</div>";
        if (!data.items || !data.items.length) return "<div>回答データがありません。</div>";
        var rows = "";
        data.items.forEach(function(item) {
          rows += "<tr>" +
            "<td>" + item.serial + "</td>" +
            "<td>" + item.total + "</td>" +
            "<td>" + item.correct + "</td>" +
            "<td>" + item.accuracy + "%</td>" +
            "<td>" + item.last_answer_at + "</td>" +
            "</tr>";
        });
        return "<div>件数: " + data.count + "</div>" +
          "<table border='1' cellspacing='0' cellpadding='4'>" +
            "<thead><tr><th>シリアル</th><th>回答数</th><th>正解数</th><th>正答率</th><th>最新日時</th></tr></thead>" +
            "<tbody>" + rows + "</tbody>" +
          "</table>";
      }

      function escapeHtml(text) {
        return String(text || "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/\"/g, "&quot;");
      }

      document.querySelectorAll(".tab").forEach(tab => {
        tab.addEventListener("click", () => {
          document.querySelectorAll(".tab").forEach(t => {
            t.classList.remove("active");
            t.setAttribute("aria-selected", "false");
          });
          tab.classList.add("active");
          tab.setAttribute("aria-selected", "true");
          const target = tab.getAttribute("data-target");
          document.querySelectorAll(".section").forEach(section => {
            section.hidden = section.getAttribute("data-section") !== target;
          });
        });
      });

      document.querySelectorAll(".import-tab").forEach(tab => {
        tab.addEventListener("click", () => {
          document.querySelectorAll(".import-tab").forEach(t => {
            t.classList.remove("active");
            t.setAttribute("aria-selected", "false");
          });
          tab.classList.add("active");
          tab.setAttribute("aria-selected", "true");
          const target = tab.getAttribute("data-import-target");
          document.querySelectorAll("[data-import-section]").forEach(section => {
            section.hidden = section.getAttribute("data-import-section") !== target;
          });
        });
      });
    </script>
  </body>
</html>
"""


def parse_args():
    parser = argparse.ArgumentParser(description="Local admin server.")
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind.",
    )
    parser.add_argument(
        "--subtopics",
        default="config/subtopics_catalog.json",
        help="Path to subtopics catalog JSON.",
    )
    parser.add_argument(
        "--prompt-sample",
        default="resources/custum_prompt_sample.txt",
        help="Path to explanation prompt sample text.",
    )
    parser.add_argument(
        "--downloads",
        default="/Users/nishitani/Downloads",
        help="Downloads directory for batch import.",
    )
    return parser.parse_args()


def build_explanation_prompt(sample_text, jsonl_text):
    return (
        f"{sample_text.strip()}\n\n"
        "【指示】\n"
        "以下は【国家試験問題】のデータである。質問は不要で、そのまま解説を作成する。\n"
        "出力はJSONLのみとし、各行のexplanationを埋め、他のキーは変更しない。\n"
        "回答は画面表示ではなく、JSONLファイルとして保存して返す。\n"
        "ファイル名は explanations_batch_filled.jsonl とする。\n"
        "だ・である調で簡潔に、長くなりすぎない説明にする。\n"
        "書き方・文章量は【サンプルJSONL】に合わせる。\n"
        "解説は、対象の問題とその問題から考えられる類題に回答できる知識を端的に説明する。\n"
        "ただし、【国家試験問題】の科目内の文脈で必要とされる解説を行う。\n\n"
        "【国家試験問題(JSONL)】\n"
        f"{jsonl_text}\n"
    )


def build_tag_prompt(jsonl_text):
    return (
        "以下のJSONLを読み取り、各行のtagsに重要語句の配列を入れてください。\n"
        "対象は問題文と選択肢から抽出される医学・制度・概念・疾患・検査・解剖・症候など。\n"
        "1問につき3〜7個、重複や表記ゆれを避け、短い名詞で出力する。\n"
        "出力はJSONLのみで、tags以外のキーは変更しない。\n"
        "回答は画面表示ではなく、JSONLファイルとして保存して返す。\n"
        "ファイル名は tags_batch_filled.jsonl とする。\n\n"
        "【国家試験問題(JSONL)】\n"
        f"{jsonl_text}\n"
    )


def build_subtopic_prompt(jsonl_text):
    return (
        "以下のJSONLを読み取り、各行のsubtopicsに該当する小項目を配列で入れてください。\n"
        "候補はcandidate_subtopicsから選び、1問につき1〜3個に絞る。\n"
        "出力はJSONLのみで、subtopics以外のキーは変更しない。\n"
        "回答は画面表示ではなく、JSONLファイルとして保存して返す。\n"
        "ファイル名は subtopics_batch_filled.jsonl とする。\n\n"
        "【国家試験問題(JSONL)】\n"
        f"{jsonl_text}\n"
    )

def build_combined_prompt(sample_text, jsonl_text):
    return (
        f"{sample_text.strip()}\n\n"
        "【指示】\n"
        "以下は【国家試験問題】のデータである。質問は不要で、そのまま解説・タグ・小項目を作成する。\n"
        "出力はJSONLのみとし、各行のexplanation/tags/subtopicsを埋め、他のキーは変更しない。\n"
        "回答は画面表示ではなく、JSONLファイルとして保存して返す。\n"
        "ファイル名は explanations_tags_subtopics_batch_filled.jsonl とする。\n"
        "解説はだ・である調で簡潔に、長くなりすぎない説明にする。\n"
        "書き方・文章量は【サンプルJSONL】に合わせる。\n"
        "解説は、対象の問題とその問題から考えられる類題に回答できる知識を端的に説明する。\n"
        "ただし、【国家試験問題】の科目内の文脈で必要とされる解説を行う。\n"
        "tagsは問題文と選択肢から抽出される医学・制度・概念・疾患・検査・解剖・症候など。\n"
        "1問につき3〜7個、重複や表記ゆれを避け、短い名詞で出力する。\n"
        "subtopicsはcandidate_subtopicsから選び、1問につき1〜3個に絞る。\n\n"
        "【国家試験問題(JSONL)】\n"
        f"{jsonl_text}\n"
    )

def expand_serials(serials_text):
    serials = []
    for chunk in [s.strip() for s in serials_text.split(",") if s.strip()]:
        if ".." in chunk:
            start, end = [p.strip() for p in chunk.split("..", 1)]
            match_start = re.match(r"^([AB])(\d{2})-(\d{3})$", start)
            match_end = re.match(r"^([AB])(\d{2})-(\d{3})$", end)
            if not match_start or not match_end:
                continue
            if match_start.group(1) != match_end.group(1) or match_start.group(2) != match_end.group(2):
                continue
            prefix = f"{match_start.group(1)}{match_start.group(2)}-"
            start_num = int(match_start.group(3))
            end_num = int(match_end.group(3))
            if start_num > end_num:
                start_num, end_num = end_num, start_num
            for num in range(start_num, end_num + 1):
                serials.append(f"{prefix}{num:03}")
        else:
            serials.append(chunk)
    return serials


def select_questions(
    conn,
    serials,
    limit,
    unannotated,
    order_mode,
    exam_type,
    exam_session,
    subject,
    kinds,
):
    order_sql = "ORDER BY q.serial"
    if order_mode == "new":
        order_sql = "ORDER BY q.exam_session DESC, q.serial DESC"
    where = []
    params = []
    if serials:
        serial_list = expand_serials(serials)
        placeholders = ",".join("?" for _ in serial_list)
        where.append(f"q.serial IN ({placeholders})")
        params.extend(serial_list)
    if exam_type:
        where.append("q.exam_type_code = ?")
        params.append(exam_type)
    if exam_session:
        where.append("q.exam_session = ?")
        params.append(int(exam_session))
    if subject:
        where.append("s.name = ?")
        params.append(subject)

    if unannotated:
        kinds_set = set(kinds or ["explanation", "tag", "subtopic"])
        if "explanation" in kinds_set:
            where.append(
                "NOT EXISTS (SELECT 1 FROM explanations e WHERE e.question_id = q.id)"
            )
        if "tag" in kinds_set:
            where.append(
                "NOT EXISTS (SELECT 1 FROM question_tags qt WHERE qt.question_id = q.id)"
            )
        if "subtopic" in kinds_set:
            where.append(
                "NOT EXISTS (SELECT 1 FROM question_subtopics qs WHERE qs.question_id = q.id)"
            )
    where_sql = " AND ".join(where) if where else "1=1"
    query = f"""
            SELECT
                q.id,
                q.serial,
                s.name AS subject,
                q.case_text,
                q.stem,
                q.choices_json,
                q.answer_index,
                q.answer_text
            FROM questions q
            LEFT JOIN subjects s ON q.subject_id = s.id
            WHERE {where_sql}
            {order_sql}
            LIMIT ?
        """
    params.append(limit)
    return conn.execute(query, params).fetchall()


def build_jsonl(records, subtopic_catalog):
    explanation_rows = []
    tag_rows = []
    subtopic_rows = []
    combined_rows = []

    for row in records:
        _, serial, subject, case_text, stem, choices_json, answer_index, answer_text = row
        choices = json.loads(choices_json)

        explanation_rows.append(
            {
                "serial": serial,
                "subject": subject,
                "case_text": case_text,
                "stem": stem,
                "choices": choices,
                "answer_index": answer_index,
                "answer_text": answer_text,
                "explanation": "",
                "source": "llm",
            }
        )
        tag_rows.append(
            {
                "serial": serial,
                "subject": subject,
                "case_text": case_text,
                "stem": stem,
                "choices": choices,
                "tags": [],
                "source": "llm",
            }
        )
        subtopic_rows.append(
            {
                "serial": serial,
                "subject": subject,
                "case_text": case_text,
                "stem": stem,
                "choices": choices,
                "candidate_subtopics": subtopic_catalog.get(subject, []),
                "subtopics": [],
                "source": "llm",
            }
        )
        combined_rows.append(
            {
                "serial": serial,
                "subject": subject,
                "case_text": case_text,
                "stem": stem,
                "choices": choices,
                "answer_index": answer_index,
                "answer_text": answer_text,
                "explanation": "",
                "tags": [],
                "candidate_subtopics": subtopic_catalog.get(subject, []),
                "subtopics": [],
                "source": "llm",
            }
        )

    def to_jsonl(rows):
        return "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)

    return (
        to_jsonl(explanation_rows),
        to_jsonl(tag_rows),
        to_jsonl(subtopic_rows),
        to_jsonl(combined_rows),
    )


class Handler(BaseHTTPRequestHandler):
    def _set_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, payload, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._set_cors()
        self.end_headers()
        self.wfile.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        if not body:
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}

    def _read_multipart_file(self):
        content_type = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            return None
        boundary = content_type.split("boundary=")[-1]
        if not boundary:
            return None
        boundary_bytes = ("--" + boundary).encode("utf-8")
        data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        parts = data.split(boundary_bytes)
        for part in parts:
            if b"Content-Disposition" not in part:
                continue
            if b'name=\"file\"' not in part:
                continue
            header_end = part.find(b"\r\n\r\n")
            if header_end == -1:
                continue
            body = part[header_end + 4 :]
            if body.endswith(b"\r\n"):
                body = body[:-2]
            return body.decode("utf-8", errors="replace")
        return None

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self._set_cors()
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
            return

        if parsed.path == "/api/prompts":
            params = parse_qs(parsed.query)
            serials = params.get("serials", [""])[0]
            limit = int(params.get("limit", ["10"])[0])
            unannotated = params.get("unannotated", ["1"])[0] == "1"
            order_mode = params.get("order", ["new"])[0]
            exam_type = params.get("exam_type", [""])[0]
            exam_session = params.get("exam_session", [""])[0]
            subject = params.get("subject", [""])[0]
            kinds_raw = params.get("kinds", [""])[0]
            kinds = [k.strip() for k in kinds_raw.split(",") if k.strip()] or [
                "explanation",
                "tag",
                "subtopic",
            ]

            conn = sqlite3.connect(self.server.db_path)
            records = select_questions(
                conn,
                serials,
                limit,
                unannotated,
                order_mode,
                exam_type,
                exam_session,
                subject,
                kinds,
            )
            conn.close()

            if not records:
                self._send_json({"count": 0})
                return

            exp_jsonl, tag_jsonl, sub_jsonl, combined_jsonl = build_jsonl(
                records, self.server.subtopic_catalog
            )

            exp_enabled = "explanation" in kinds
            tag_enabled = "tag" in kinds
            sub_enabled = "subtopic" in kinds

            exp_prompt = (
                build_explanation_prompt(self.server.prompt_sample, exp_jsonl)
                if exp_enabled
                else ""
            )
            tag_prompt = build_tag_prompt(tag_jsonl) if tag_enabled else ""
            sub_prompt = build_subtopic_prompt(sub_jsonl) if sub_enabled else ""
            combined_prompt = build_combined_prompt(
                self.server.prompt_sample, combined_jsonl
            )

            payload = {
                "count": len(records),
                "explanations": {
                    "filename": "explanations_batch_prompt.txt",
                    "text": exp_prompt,
                    "enabled": exp_enabled,
                },
                "tags": {
                    "filename": "tags_batch_prompt.txt",
                    "text": tag_prompt,
                    "enabled": tag_enabled,
                },
                "subtopics": {
                    "filename": "subtopics_batch_prompt.txt",
                    "text": sub_prompt,
                    "enabled": sub_enabled,
                },
                "combined": {
                    "filename": "explanations_tags_subtopics_batch_prompt.txt",
                    "text": combined_prompt,
                    "enabled": True,
                },
            }
            self._send_json(payload)
            return
        if parsed.path == "/api/progress":
            payload = build_progress(self.server.db_path)
            self._send_json(payload)
            return
        if parsed.path == "/api/history":
            payload = build_history(self.server.db_path)
            self._send_json(payload)
            return
        if parsed.path == "/api/preview":
            params = parse_qs(parsed.query)
            query = params.get("q", [""])[0]
            payload = build_preview(self.server.db_path, query)
            self._send_json(payload)
            return
        if parsed.path == "/api/subjects":
            payload = load_subjects(self.server.db_path)
            self._send_json(payload)
            return
        if parsed.path == "/api/missing":
            params = parse_qs(parsed.query)
            payload = build_missing(self.server.db_path, params)
            self._send_json(payload)
            return
        if parsed.path == "/api/missing.csv":
            params = parse_qs(parsed.query)
            csv_text = build_missing_csv(self.server.db_path, params)
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self._set_cors()
            self.end_headers()
            self.wfile.write(csv_text.encode("utf-8"))
            return
        if parsed.path == "/api/report":
            params = parse_qs(parsed.query)
            serial = params.get("serial", [""])[0]
            kind = params.get("kind", [""])[0]
            payload = add_report_supabase(serial, kind)
            self._send_json(payload)
            return
        if parsed.path == "/api/reports":
            params = parse_qs(parsed.query)
            limit = int(params.get("limit", ["2000"])[0] or 2000)
            payload = list_reports_supabase(limit)
            self._send_json(payload)
            return
        if parsed.path == "/api/edit_requests":
            params = parse_qs(parsed.query)
            limit = int(params.get("limit", ["2000"])[0] or 2000)
            payload = list_edit_requests_supabase(limit)
            self._send_json(payload)
            return
        if parsed.path == "/api/supabase/feedback":
            params = parse_qs(parsed.query)
            limit = int(params.get("limit", ["1000"])[0] or 1000)
            payload = build_supabase_feedback(limit)
            self._send_json(payload)
            return
        if parsed.path == "/api/supabase/answers":
            params = parse_qs(parsed.query)
            limit = int(params.get("limit", ["1000"])[0] or 1000)
            payload = build_supabase_answers(limit)
            self._send_json(payload)
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/reports/clear":
            payload = self._read_json()
            items = payload.get("items", [])
            result = clear_reports_supabase(items)
            self._send_json(result)
            return
        if parsed.path == "/api/edit_requests/apply":
            payload = self._read_json()
            items = payload.get("items", [])
            result = apply_edit_requests(self.server.db_path, items)
            self._send_json(result)
            return
        if parsed.path == "/api/edit_requests/dismiss":
            payload = self._read_json()
            items = payload.get("items", [])
            result = update_edit_request_status(items, "dismissed")
            self._send_json(result)
            return
        if parsed.path == "/api/backup":
            message = run_backup(self.server.repo_root)
            self._send_json({"message": message})
            return
        parsed = urlparse(self.path)
        if parsed.path == "/api/import/explanations_text":
            payload = self._read_json()
            text = (payload.get("text") or "").strip()
            if not text:
                self._send_json({"message": "貼り付け内容が空です。"}, status=400)
                return
            mode = payload.get("mode") or "append"
            version_raw = payload.get("version") or "auto"
            version = None if version_raw == "auto" else int(version_raw)
            inserted = import_explanations(self.server.db_path, text, mode, version)
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {"message": f"解説を {inserted} 件インポートしました。 / {web_message}"}
            )
            return
        if parsed.path == "/api/import/tags_text":
            payload = self._read_json()
            text = (payload.get("text") or "").strip()
            if not text:
                self._send_json({"message": "貼り付け内容が空です。"}, status=400)
                return
            mode = payload.get("mode") or "append"
            inserted = import_tags(self.server.db_path, text, mode)
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {"message": f"タグを {inserted} 件インポートしました。 / {web_message}"}
            )
            return
        if parsed.path == "/api/import/subtopics_text":
            payload = self._read_json()
            text = (payload.get("text") or "").strip()
            if not text:
                self._send_json({"message": "貼り付け内容が空です。"}, status=400)
                return
            mode = payload.get("mode") or "append"
            inserted = import_subtopics(self.server.db_path, text, mode)
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {"message": f"小項目を {inserted} 件インポートしました。 / {web_message}"}
            )
            return
        if parsed.path == "/api/import/combined_text":
            payload = self._read_json()
            text = (payload.get("text") or "").strip()
            if not text:
                self._send_json({"message": "貼り付け内容が空です。"}, status=400)
                return
            mode_exp = payload.get("modeExp") or "append"
            mode_tag = payload.get("modeTag") or "append"
            mode_sub = payload.get("modeSub") or "append"
            version_raw = payload.get("version") or "auto"
            version = None if version_raw == "auto" else int(version_raw)
            counts = import_combined(
                self.server.db_path, text, mode_exp, version, mode_tag, mode_sub
            )
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {
                    "message": (
                        "同時インポート: 解説 {explanations} 件 / タグ {tags} 件 / 小項目 {subtopics} 件"
                        " / {web}"
                    ).format(
                        explanations=counts["explanations"],
                        tags=counts["tags"],
                        subtopics=counts["subtopics"],
                        web=web_message,
                    )
                }
            )
            return
        if parsed.path == "/api/import/explanations":
            content = self._read_multipart_file()
            if not content:
                self._send_json({"message": "ファイルを読み取れませんでした。"}, status=400)
                return
            params = parse_qs(parsed.query)
            mode = params.get("mode", ["append"])[0]
            version_raw = params.get("version", ["auto"])[0]
            version = None if version_raw == "auto" else int(version_raw)
            inserted = import_explanations(self.server.db_path, content, mode, version)
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {"message": f"解説を {inserted} 件インポートしました。 / {web_message}"}
            )
            return

        if parsed.path == "/api/import/tags":
            content = self._read_multipart_file()
            if not content:
                self._send_json({"message": "ファイルを読み取れませんでした。"}, status=400)
                return
            params = parse_qs(parsed.query)
            mode = params.get("mode", ["append"])[0]
            inserted = import_tags(self.server.db_path, content, mode)
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {"message": f"タグを {inserted} 件インポートしました。 / {web_message}"}
            )
            return

        if parsed.path == "/api/import/subtopics":
            content = self._read_multipart_file()
            if not content:
                self._send_json({"message": "ファイルを読み取れませんでした。"}, status=400)
                return
            params = parse_qs(parsed.query)
            mode = params.get("mode", ["append"])[0]
            inserted = import_subtopics(self.server.db_path, content, mode)
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {"message": f"小項目を {inserted} 件インポートしました。 / {web_message}"}
            )
            return
        if parsed.path == "/api/import/combined":
            content = self._read_multipart_file()
            if not content:
                self._send_json({"message": "ファイルを読み取れませんでした。"}, status=400)
                return
            params = parse_qs(parsed.query)
            mode_exp = params.get("modeExp", ["append"])[0]
            mode_tag = params.get("modeTag", ["append"])[0]
            mode_sub = params.get("modeSub", ["append"])[0]
            version_raw = params.get("version", ["auto"])[0]
            version = None if version_raw == "auto" else int(version_raw)
            counts = import_combined(
                self.server.db_path, content, mode_exp, version, mode_tag, mode_sub
            )
            web_message = run_build_web(self.server.repo_root)
            self._send_json(
                {
                    "message": (
                        "同時インポート: 解説 {explanations} 件 / タグ {tags} 件 / 小項目 {subtopics} 件"
                        " / {web}"
                    ).format(
                        explanations=counts["explanations"],
                        tags=counts["tags"],
                        subtopics=counts["subtopics"],
                        web=web_message,
                    )
                }
            )
            return

        if parsed.path == "/api/build/web":
            message = run_build_web(self.server.repo_root)
            self._send_json({"message": message})
            return

        if parsed.path == "/api/build/all":
            message = run_build_all(self.server.repo_root)
            self._send_json({"message": message})
            return

        if parsed.path == "/api/import/downloads":
            params = parse_qs(parsed.query)
            mode_exp = params.get("modeExp", ["append"])[0]
            version_raw = params.get("version", ["auto"])[0]
            version = None if version_raw == "auto" else int(version_raw)
            mode_tag = params.get("modeTag", ["append"])[0]
            mode_sub = params.get("modeSub", ["append"])[0]
            message = import_from_downloads(
                self.server.db_path,
                self.server.downloads_dir,
                mode_exp,
                version,
                mode_tag,
                mode_sub,
            )
            web_message = run_build_web(self.server.repo_root)
            self._send_json({"message": f"{message} / {web_message}"})
            return
        self.send_response(404)
        self.end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors()
        self.end_headers()


def import_explanations(db_path, jsonl_text, mode, version):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    inserted = 0
    for line in jsonl_text.splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        serial = record.get("serial")
        explanation = record.get("explanation", "").strip()
        source = record.get("source") or "llm"
        if not serial or not explanation:
            continue
        row = cursor.execute(
            "SELECT id FROM questions WHERE serial = ?",
            (serial,),
        ).fetchone()
        if not row:
            continue
        question_id = row[0]
        if mode == "skip":
            exists = cursor.execute(
                "SELECT 1 FROM explanations WHERE question_id = ? LIMIT 1",
                (question_id,),
            ).fetchone()
            if exists:
                continue
        if mode == "replace":
            cursor.execute("DELETE FROM explanations WHERE question_id = ?", (question_id,))
        if version is None:
            row = cursor.execute(
                "SELECT MAX(version) FROM explanations WHERE question_id = ?",
                (question_id,),
            ).fetchone()
            next_version = (row[0] or 0) + 1
        else:
            next_version = version
        cursor.execute(
            """
            INSERT INTO explanations(question_id, body, version, source)
            VALUES (?, ?, ?, ?)
            """,
            (question_id, explanation, next_version, source),
        )
        clear_feedback_flag(conn, serial, "explanation")
        clear_supabase_feedback(serial, "explanation")
        inserted += 1
    conn.commit()
    conn.close()
    return inserted


def import_tags(db_path, jsonl_text, mode):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    inserted = 0
    for line in jsonl_text.splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        serial = record.get("serial")
        tags = record.get("tags", [])
        if not serial or not tags:
            continue
        row = cursor.execute(
            "SELECT id FROM questions WHERE serial = ?",
            (serial,),
        ).fetchone()
        if not row:
            continue
        question_id = row[0]
        if mode == "skip":
            exists = cursor.execute(
                "SELECT 1 FROM question_tags WHERE question_id = ? LIMIT 1",
                (question_id,),
            ).fetchone()
            if exists:
                continue
        if mode == "replace":
            cursor.execute("DELETE FROM question_tags WHERE question_id = ?", (question_id,))
        updated = False
        for tag in tags:
            tag_label = " ".join(str(tag).split()).strip()
            if not tag_label:
                continue
            cursor.execute(
                "INSERT OR IGNORE INTO tags(label) VALUES (?)",
                (tag_label,),
            )
            tag_id = cursor.execute(
                "SELECT id FROM tags WHERE label = ?",
                (tag_label,),
            ).fetchone()[0]
            cursor.execute(
                """
                INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                VALUES (?, ?, ?)
                """,
                (question_id, tag_id, "llm"),
            )
            inserted += 1
            updated = True
        if updated or mode == "replace":
            clear_feedback_flag(conn, serial, "tag")
            clear_supabase_feedback(serial, "tag")
    conn.commit()
    conn.close()
    return inserted


def import_subtopics(db_path, jsonl_text, mode):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    inserted = 0
    for line in jsonl_text.splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        serial = record.get("serial")
        subtopics = record.get("subtopics", [])
        if not serial or not subtopics:
            continue
        row = cursor.execute(
            "SELECT id FROM questions WHERE serial = ?",
            (serial,),
        ).fetchone()
        if not row:
            continue
        question_id = row[0]
        if mode == "skip":
            exists = cursor.execute(
                "SELECT 1 FROM question_subtopics WHERE question_id = ? LIMIT 1",
                (question_id,),
            ).fetchone()
            if exists:
                continue
        if mode == "replace":
            cursor.execute("DELETE FROM question_subtopics WHERE question_id = ?", (question_id,))
        updated = False
        for item in subtopics:
            name = " ".join(str(item).split()).strip()
            if not name:
                continue
            cursor.execute(
                "INSERT OR IGNORE INTO subtopics(name) VALUES (?)",
                (name,),
            )
            subtopic_id = cursor.execute(
                "SELECT id FROM subtopics WHERE name = ?",
                (name,),
            ).fetchone()[0]
            cursor.execute(
                """
                INSERT OR IGNORE INTO question_subtopics(question_id, subtopic_id)
                VALUES (?, ?)
                """,
                (question_id, subtopic_id),
            )
            inserted += 1
            updated = True
        if updated or mode == "replace":
            clear_feedback_flag(conn, serial, "subtopic")
            clear_supabase_feedback(serial, "subtopic")
    conn.commit()
    conn.close()
    return inserted


def import_combined(db_path, jsonl_text, mode_exp, version, mode_tag, mode_sub):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    counts = {"explanations": 0, "tags": 0, "subtopics": 0}
    for line in jsonl_text.splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        serial = record.get("serial")
        if not serial:
            continue
        row = cursor.execute(
            "SELECT id FROM questions WHERE serial = ?",
            (serial,),
        ).fetchone()
        if not row:
            continue
        question_id = row[0]

        explanation = str(record.get("explanation", "")).strip()
        if explanation:
            if mode_exp == "skip":
                exists = cursor.execute(
                    "SELECT 1 FROM explanations WHERE question_id = ? LIMIT 1",
                    (question_id,),
                ).fetchone()
                if not exists:
                    next_version = version
                    if version is None:
                        row = cursor.execute(
                            "SELECT MAX(version) FROM explanations WHERE question_id = ?",
                            (question_id,),
                        ).fetchone()
                        next_version = (row[0] or 0) + 1
                    cursor.execute(
                        """
                        INSERT INTO explanations(question_id, body, version, source)
                        VALUES (?, ?, ?, ?)
                        """,
                        (question_id, explanation, next_version, "llm"),
                    )
                    counts["explanations"] += 1
                    clear_feedback_flag(conn, serial, "explanation")
                    clear_supabase_feedback(serial, "explanation")
            else:
                if mode_exp == "replace":
                    cursor.execute(
                        "DELETE FROM explanations WHERE question_id = ?",
                        (question_id,),
                    )
                next_version = version
                if version is None:
                    row = cursor.execute(
                        "SELECT MAX(version) FROM explanations WHERE question_id = ?",
                        (question_id,),
                    ).fetchone()
                    next_version = (row[0] or 0) + 1
                cursor.execute(
                    """
                    INSERT INTO explanations(question_id, body, version, source)
                    VALUES (?, ?, ?, ?)
                    """,
                    (question_id, explanation, next_version, "llm"),
                )
                counts["explanations"] += 1
                clear_feedback_flag(conn, serial, "explanation")
                clear_supabase_feedback(serial, "explanation")

        tags = record.get("tags", [])
        if tags:
            if mode_tag == "skip":
                exists = cursor.execute(
                    "SELECT 1 FROM question_tags WHERE question_id = ? LIMIT 1",
                    (question_id,),
                ).fetchone()
                if not exists:
                    for tag in tags:
                        tag_label = " ".join(str(tag).split()).strip()
                        if not tag_label:
                            continue
                        cursor.execute(
                            "INSERT OR IGNORE INTO tags(label) VALUES (?)",
                            (tag_label,),
                        )
                        tag_id = cursor.execute(
                            "SELECT id FROM tags WHERE label = ?",
                            (tag_label,),
                        ).fetchone()[0]
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                            VALUES (?, ?, ?)
                            """,
                            (question_id, tag_id, "llm"),
                        )
                        counts["tags"] += 1
                    clear_feedback_flag(conn, serial, "tag")
                    clear_supabase_feedback(serial, "tag")
            else:
                if mode_tag == "replace":
                    cursor.execute(
                        "DELETE FROM question_tags WHERE question_id = ?",
                        (question_id,),
                    )
                updated = False
                for tag in tags:
                    tag_label = " ".join(str(tag).split()).strip()
                    if not tag_label:
                        continue
                    cursor.execute(
                        "INSERT OR IGNORE INTO tags(label) VALUES (?)",
                        (tag_label,),
                    )
                    tag_id = cursor.execute(
                        "SELECT id FROM tags WHERE label = ?",
                        (tag_label,),
                    ).fetchone()[0]
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                        VALUES (?, ?, ?)
                        """,
                        (question_id, tag_id, "llm"),
                    )
                    counts["tags"] += 1
                    updated = True
                if updated or mode_tag == "replace":
                    clear_feedback_flag(conn, serial, "tag")
                    clear_supabase_feedback(serial, "tag")

        subtopics = record.get("subtopics", [])
        if subtopics:
            if mode_sub == "skip":
                exists = cursor.execute(
                    "SELECT 1 FROM question_subtopics WHERE question_id = ? LIMIT 1",
                    (question_id,),
                ).fetchone()
                if not exists:
                    for item in subtopics:
                        name = " ".join(str(item).split()).strip()
                        if not name:
                            continue
                        cursor.execute(
                            "INSERT OR IGNORE INTO subtopics(name) VALUES (?)",
                            (name,),
                        )
                        subtopic_id = cursor.execute(
                            "SELECT id FROM subtopics WHERE name = ?",
                            (name,),
                        ).fetchone()[0]
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO question_subtopics(question_id, subtopic_id)
                            VALUES (?, ?)
                            """,
                            (question_id, subtopic_id),
                        )
                        counts["subtopics"] += 1
                    clear_feedback_flag(conn, serial, "subtopic")
                    clear_supabase_feedback(serial, "subtopic")
            else:
                if mode_sub == "replace":
                    cursor.execute(
                        "DELETE FROM question_subtopics WHERE question_id = ?",
                        (question_id,),
                    )
                updated = False
                for item in subtopics:
                    name = " ".join(str(item).split()).strip()
                    if not name:
                        continue
                    cursor.execute(
                        "INSERT OR IGNORE INTO subtopics(name) VALUES (?)",
                        (name,),
                    )
                    subtopic_id = cursor.execute(
                        "SELECT id FROM subtopics WHERE name = ?",
                        (name,),
                    ).fetchone()[0]
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO question_subtopics(question_id, subtopic_id)
                        VALUES (?, ?)
                        """,
                        (question_id, subtopic_id),
                    )
                    counts["subtopics"] += 1
                    updated = True
                if updated or mode_sub == "replace":
                    clear_feedback_flag(conn, serial, "subtopic")
                    clear_supabase_feedback(serial, "subtopic")

    conn.commit()
    conn.close()
    return counts


def build_progress(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    total_questions = cursor.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
    explained = cursor.execute(
        "SELECT COUNT(DISTINCT question_id) FROM explanations"
    ).fetchone()[0]
    tagged = cursor.execute(
        "SELECT COUNT(DISTINCT question_id) FROM question_tags"
    ).fetchone()[0]
    subtopic_assigned = cursor.execute(
        "SELECT COUNT(DISTINCT question_id) FROM question_subtopics"
    ).fetchone()[0]

    subjects = cursor.execute("SELECT id, name FROM subjects ORDER BY name").fetchall()
    subject_rows = []
    for subject_id, name in subjects:
        subject_total = cursor.execute(
            "SELECT COUNT(*) FROM questions WHERE subject_id = ?",
            (subject_id,),
        ).fetchone()[0]
        subject_explained = cursor.execute(
            """
            SELECT COUNT(DISTINCT q.id)
            FROM questions q
            JOIN explanations e ON e.question_id = q.id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()[0]
        subject_tagged = cursor.execute(
            """
            SELECT COUNT(DISTINCT q.id)
            FROM questions q
            JOIN question_tags qt ON qt.question_id = q.id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()[0]
        subject_subtopics = cursor.execute(
            """
            SELECT COUNT(DISTINCT q.id)
            FROM questions q
            JOIN question_subtopics qs ON qs.question_id = q.id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()[0]
        subject_rows.append(
            {
                "subject": name,
                "total_questions": subject_total,
                "explained": subject_explained,
                "tagged": subject_tagged,
                "subtopic_assigned": subject_subtopics,
            }
        )
    conn.close()
    return {
        "total_questions": total_questions,
        "explained": explained,
        "tagged": tagged,
        "subtopic_assigned": subtopic_assigned,
        "by_subject": subject_rows,
    }


def build_history(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    history = []
    expl = cursor.execute(
        """
        SELECT e.id, q.serial, e.body
        FROM explanations e
        JOIN questions q ON q.id = e.question_id
        ORDER BY e.id DESC
        LIMIT 20
        """
    ).fetchall()
    for row in expl:
        history.append({"type": "explanation", "id": row[0], "serial": row[1], "text": row[2]})
    tags = cursor.execute(
        """
        SELECT qt.question_id, q.serial, t.label
        FROM question_tags qt
        JOIN questions q ON q.id = qt.question_id
        JOIN tags t ON t.id = qt.tag_id
        ORDER BY qt.rowid DESC
        LIMIT 20
        """
    ).fetchall()
    for row in tags:
        history.append({"type": "tag", "serial": row[1], "text": row[2]})
    subs = cursor.execute(
        """
        SELECT qs.question_id, q.serial, st.name
        FROM question_subtopics qs
        JOIN questions q ON q.id = qs.question_id
        JOIN subtopics st ON st.id = qs.subtopic_id
        ORDER BY qs.rowid DESC
        LIMIT 20
        """
    ).fetchall()
    for row in subs:
        history.append({"type": "subtopic", "serial": row[1], "text": row[2]})
    conn.close()
    return history[:20]


def build_preview(db_path, query):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if not query:
        conn.close()
        return []

    rows = cursor.execute(
        """
        SELECT q.id, q.serial, s.name, q.stem, q.choices_json, q.answer_index
        FROM questions q
        LEFT JOIN subjects s ON s.id = q.subject_id
        WHERE q.serial = ?
           OR q.stem LIKE ?
        ORDER BY q.serial
        LIMIT 20
        """,
        (query, f"%{query}%"),
    ).fetchall()

    results = []
    for row in rows:
        qid, serial, subject, stem, choices_json, answer_index = row
        explanations = cursor.execute(
            """
            SELECT body, version
            FROM explanations
            WHERE question_id = ?
            ORDER BY version DESC, id DESC
            LIMIT 3
            """,
            (qid,),
        ).fetchall()
        tags = cursor.execute(
            """
            SELECT t.label
            FROM question_tags qt
            JOIN tags t ON t.id = qt.tag_id
            WHERE qt.question_id = ?
            ORDER BY t.label
            """,
            (qid,),
        ).fetchall()
        subtopics = cursor.execute(
            """
            SELECT st.name
            FROM question_subtopics qs
            JOIN subtopics st ON st.id = qs.subtopic_id
            WHERE qs.question_id = ?
            ORDER BY st.name
            """,
            (qid,),
        ).fetchall()

        results.append(
            {
                "serial": serial,
                "subject": subject,
                "stem": stem,
                "choices": json.loads(choices_json),
                "answer_index": answer_index,
                "explanations": [{"body": e[0], "version": e[1]} for e in explanations],
                "tags": [t[0] for t in tags],
                "subtopics": [s[0] for s in subtopics],
            }
        )

    conn.close()
    return results


def build_missing(db_path, params):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    where_clauses = []
    if params.get("explanations"):
        where_clauses.append("NOT EXISTS (SELECT 1 FROM explanations e WHERE e.question_id = q.id)")
    if params.get("tags"):
        where_clauses.append("NOT EXISTS (SELECT 1 FROM question_tags qt WHERE qt.question_id = q.id)")
    if params.get("subtopics"):
        where_clauses.append("NOT EXISTS (SELECT 1 FROM question_subtopics qs WHERE qs.question_id = q.id)")
    if not where_clauses:
        conn.close()
        return []
    where_sql = " AND ".join(where_clauses)
    rows = cursor.execute(
        f"""
        SELECT q.serial, s.name, q.stem
        FROM questions q
        LEFT JOIN subjects s ON s.id = q.subject_id
        WHERE {where_sql}
        ORDER BY q.serial
        LIMIT 200
        """
    ).fetchall()
    conn.close()
    return [
        {"serial": row[0], "subject": row[1], "stem": row[2]}
        for row in rows
    ]


def build_missing_csv(db_path, params):
    rows = build_missing(db_path, params)
    lines = ["serial,subject,stem"]
    for row in rows:
        serial = row["serial"]
        subject = (row["subject"] or "").replace("\"", "\"\"")
        stem = (row["stem"] or "").replace("\"", "\"\"")
        lines.append(f"\"{serial}\",\"{subject}\",\"{stem}\"")
    return "\n".join(lines)


def load_subjects(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT name FROM subjects ORDER BY name").fetchall()
    conn.close()
    return [row[0] for row in rows]


def import_from_downloads(db_path, downloads_dir, mode_exp, version, mode_tag, mode_sub):
    from pathlib import Path

    downloads = Path(downloads_dir)
    if not downloads.exists():
        return f"ダウンロードフォルダが見つかりません: {downloads_dir}"

    combined_files = sorted(
        downloads.glob("explanations_tags_subtopics_batch_filled*.jsonl")
    )
    exp_files = sorted(downloads.glob("explanations_batch_filled*.jsonl"))
    tag_files = sorted(downloads.glob("tags_batch_filled*.jsonl"))
    sub_files = sorted(downloads.glob("subtopics_batch_filled*.jsonl"))

    messages = []
    if combined_files:
        for path in combined_files:
            text = path.read_text(encoding="utf-8")
            counts = import_combined(db_path, text, mode_exp, version, mode_tag, mode_sub)
            messages.append(
                f"{path.name}: 解説 {counts['explanations']} 件 / タグ {counts['tags']} 件 / 小項目 {counts['subtopics']} 件"
            )
            path.unlink()
    if exp_files:
        for path in exp_files:
            text = path.read_text(encoding="utf-8")
            inserted = import_explanations(db_path, text, mode_exp, version)
            messages.append(f"{path.name}: 解説 {inserted} 件")
            path.unlink()
    if tag_files:
        for path in tag_files:
            text = path.read_text(encoding="utf-8")
            inserted = import_tags(db_path, text, mode_tag)
            messages.append(f"{path.name}: タグ {inserted} 件")
            path.unlink()
    if sub_files:
        for path in sub_files:
            text = path.read_text(encoding="utf-8")
            inserted = import_subtopics(db_path, text, mode_sub)
            messages.append(f"{path.name}: 小項目 {inserted} 件")
            path.unlink()

    if not messages:
        return "対象ファイルが見つかりませんでした。"
    return " / ".join(messages)


def ensure_feedback_table(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feedback_reports (
            serial TEXT PRIMARY KEY,
            explain INTEGER DEFAULT 0,
            tag INTEGER DEFAULT 0,
            subtopic INTEGER DEFAULT 0,
            reported_at TEXT NOT NULL
        )
        """
    )
    cols = [row[1] for row in conn.execute("PRAGMA table_info(feedback_reports)").fetchall()]
    if "explain" not in cols:
        conn.execute("ALTER TABLE feedback_reports ADD COLUMN explain INTEGER DEFAULT 0")
    if "tag" not in cols:
        conn.execute("ALTER TABLE feedback_reports ADD COLUMN tag INTEGER DEFAULT 0")
    if "subtopic" not in cols:
        conn.execute("ALTER TABLE feedback_reports ADD COLUMN subtopic INTEGER DEFAULT 0")
    conn.commit()
    conn.close()


def add_report(db_path, serial, kind):
    if not serial:
        return "シリアルが指定されていません。"
    if kind not in {"explanation", "tag", "subtopic"}:
        return "種別が指定されていません。"
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT explain, tag, subtopic FROM feedback_reports WHERE serial = ?",
        (serial,),
    ).fetchone()
    explain, tag, subtopic = row if row else (0, 0, 0)
    if kind == "explanation":
        explain = 1
    elif kind == "tag":
        tag = 1
    elif kind == "subtopic":
        subtopic = 1
    conn.execute(
        """
        INSERT INTO feedback_reports(serial, explain, tag, subtopic, reported_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(serial) DO UPDATE SET
            explain = excluded.explain,
            tag = excluded.tag,
            subtopic = excluded.subtopic,
            reported_at = excluded.reported_at
        """,
        (serial, explain, tag, subtopic),
    )
    conn.commit()
    conn.close()
    return f"報告しました: {serial} ({kind})"


def list_reports(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT serial, explain, tag, subtopic, reported_at
        FROM feedback_reports
        ORDER BY reported_at DESC
        """
    ).fetchall()
    conn.close()
    return {
        "count": len(rows),
        "serials": [row[0] for row in rows],
        "items": [
            {
                "serial": row[0],
                "explanation": row[1],
                "tag": row[2],
                "subtopic": row[3],
                "reported_at": row[4],
            }
            for row in rows
        ],
    }


def clear_reports(db_path, items):
    conn = sqlite3.connect(db_path)
    if not items:
        conn.close()
        return "消去対象がありません。"
    for item in items:
        serial = item.get("serial")
        kinds = item.get("kinds", [])
        if not serial or not kinds:
            continue
        updates = []
        if "explanation" in kinds:
            updates.append("explain = 0")
        if "tag" in kinds:
            updates.append("tag = 0")
        if "subtopic" in kinds:
            updates.append("subtopic = 0")
        if not updates:
            continue
        conn.execute(
            f"UPDATE feedback_reports SET {', '.join(updates)} WHERE serial = ?",
            (serial,),
        )
        conn.execute(
            "DELETE FROM feedback_reports WHERE serial = ? AND explain = 0 AND tag = 0 AND subtopic = 0",
            (serial,),
        )
    conn.commit()
    conn.close()
    return "選択した報告フラグを消去しました。"


def clear_feedback_flag(conn, serial, kind):
    field = {"explanation": "explain", "tag": "tag", "subtopic": "subtopic"}.get(kind)
    if not field:
        return
    conn.execute(
        f"UPDATE feedback_reports SET {field} = 0 WHERE serial = ?",
        (serial,),
    )
    conn.execute(
        "DELETE FROM feedback_reports WHERE serial = ? AND explain = 0 AND tag = 0 AND subtopic = 0",
        (serial,),
    )


def supabase_config():
    url = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        os.environ.get("SUPABASE_SERVICE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    ).strip()
    if not url or not key:
        return None
    return {"url": url, "key": key}


def fetch_supabase_rows(table, select, limit):
    cfg = supabase_config()
    if not cfg:
        return None, "SUPABASE_URL と SUPABASE_SERVICE_KEY を設定してください。"
    endpoint = (
        f"{cfg['url']}/rest/v1/{table}"
        f"?select={quote(select)}&order=created_at.desc&limit={limit}"
    )
    req = Request(
        endpoint,
        headers={
            "apikey": cfg["key"],
            "Authorization": f"Bearer {cfg['key']}",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=10) as resp:
            data = resp.read().decode("utf-8")
        return json.loads(data), ""
    except URLError as exc:
        return None, f"Supabase接続に失敗しました: {exc}"


def supabase_request(method, table, query, body=None):
    cfg = supabase_config()
    if not cfg:
        return None, "SUPABASE_URL と SUPABASE_SERVICE_KEY を設定してください。"
    endpoint = f"{cfg['url']}/rest/v1/{table}{query}"
    headers = {
        "apikey": cfg["key"],
        "Authorization": f"Bearer {cfg['key']}",
        "Accept": "application/json",
    }
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
        headers["Prefer"] = "return=minimal"
    req = Request(endpoint, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=10) as resp:
            payload = resp.read().decode("utf-8")
        return payload, ""
    except URLError as exc:
        return None, f"Supabase接続に失敗しました: {exc}"


def build_supabase_feedback(limit):
    rows, error = fetch_supabase_rows("feedback", "serial,kind,created_at", limit)
    if error:
        return {"ok": False, "message": error, "items": []}
    if not rows:
        return {"ok": True, "count": 0, "items": []}
    summary = {}
    for row in rows:
        serial = row.get("serial")
        kind = row.get("kind")
        created = row.get("created_at") or ""
        if not serial or not kind:
            continue
        item = summary.get(serial)
        if not item:
            item = {
                "serial": serial,
                "explanation_count": 0,
                "tag_count": 0,
                "subtopic_count": 0,
                "total": 0,
                "last_reported_at": created,
            }
            summary[serial] = item
        if kind == "explanation":
            item["explanation_count"] += 1
        elif kind == "tag":
            item["tag_count"] += 1
        elif kind == "subtopic":
            item["subtopic_count"] += 1
        item["total"] += 1
        if created and created > item["last_reported_at"]:
            item["last_reported_at"] = created
    items = sorted(summary.values(), key=lambda x: (-x["total"], x["serial"]))
    return {"ok": True, "count": len(items), "items": items}


def build_supabase_answers(limit):
    rows, error = fetch_supabase_rows(
        "answers", "serial,is_correct,selected_index,created_at", limit
    )
    if error:
        return {"ok": False, "message": error, "items": []}
    if not rows:
        return {"ok": True, "count": 0, "items": []}
    summary = {}
    for row in rows:
        serial = row.get("serial")
        if not serial:
            continue
        created = row.get("created_at") or ""
        item = summary.get(serial)
        if not item:
            item = {
                "serial": serial,
                "total": 0,
                "correct": 0,
                "accuracy": 0.0,
                "last_answer_at": created,
            }
            summary[serial] = item
        item["total"] += 1
        if row.get("is_correct"):
            item["correct"] += 1
        if created and created > item["last_answer_at"]:
            item["last_answer_at"] = created
    for item in summary.values():
        if item["total"]:
            item["accuracy"] = round(item["correct"] * 100.0 / item["total"], 1)
    items = sorted(summary.values(), key=lambda x: (-x["total"], x["serial"]))
    return {"ok": True, "count": len(items), "items": items}


def list_reports_supabase(limit):
    rows, error = fetch_supabase_rows("feedback", "serial,kind,created_at", limit)
    if error:
        return {"count": 0, "serials": [], "items": [], "message": error}
    if not rows:
        return {"count": 0, "serials": [], "items": []}

    summary = {}
    for row in rows:
        serial = row.get("serial")
        kind = row.get("kind")
        created = row.get("created_at") or ""
        if not serial or not kind:
            continue
        item = summary.get(serial)
        if not item:
            item = {
                "serial": serial,
                "explanation": 0,
                "tag": 0,
                "subtopic": 0,
                "reported_at": created,
            }
            summary[serial] = item
        if kind == "explanation":
            item["explanation"] = 1
        elif kind == "tag":
            item["tag"] = 1
        elif kind == "subtopic":
            item["subtopic"] = 1
        if created and created > item["reported_at"]:
            item["reported_at"] = created

    items = sorted(summary.values(), key=lambda x: x["reported_at"], reverse=True)
    return {
        "count": len(items),
        "serials": [item["serial"] for item in items],
        "items": items,
    }


def list_edit_requests_supabase(limit):
    rows, error = fetch_supabase_rows(
        "edit_requests",
        "id,serial,kind,payload,note,status,created_at,created_by,created_email",
        limit,
    )
    if error:
        return {"count": 0, "items": [], "message": error}
    if not rows:
        return {"count": 0, "items": []}
    items = [row for row in rows if row.get("status") == "open"]
    return {"count": len(items), "items": items}


def update_edit_request_status(items, status):
    if not items:
        return {"message": "対象がありません。"}
    for item in items:
        edit_id = item.get("id")
        if not edit_id:
            continue
        query = f"?id=eq.{quote(str(edit_id))}"
        _, error = supabase_request(
            "PATCH",
            "edit_requests",
            query,
            {"status": status},
        )
        if error:
            return {"message": error}
    return {"message": f"{len(items)} 件を更新しました。"}


def apply_edit_requests(db_path, items):
    if not items:
        return {"message": "対象がありません。"}
    edits = []
    for item in items:
        edit_id = item.get("id")
        if not edit_id:
            continue
        query = f"?id=eq.{quote(str(edit_id))}"
        payload, error = supabase_request(
            "GET",
            "edit_requests",
            query + "&select=id,serial,kind,payload,note,status",
        )
        if error:
            return {"message": error}
        try:
            rows = json.loads(payload)
        except json.JSONDecodeError:
            rows = []
        edits.extend(rows)

    if not edits:
        return {"message": "対象の提案が見つかりません。"}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    applied = 0
    for edit in edits:
        if edit.get("status") != "open":
            continue
        serial = edit.get("serial")
        kind = edit.get("kind")
        payload = edit.get("payload") or {}
        row = cursor.execute(
            "SELECT id FROM questions WHERE serial = ?",
            (serial,),
        ).fetchone()
        if not row:
            continue
        question_id = row[0]
        if kind == "explanation_edit":
            body = str(payload.get("body") or "").strip()
            if not body:
                continue
            row = cursor.execute(
                "SELECT MAX(version) FROM explanations WHERE question_id = ?",
                (question_id,),
            ).fetchone()
            next_version = (row[0] or 0) + 1
            cursor.execute(
                """
                INSERT INTO explanations(question_id, body, version, source)
                VALUES (?, ?, ?, ?)
                """,
                (question_id, body, next_version, "human"),
            )
            applied += 1
        elif kind == "explanation_confirm":
            row = cursor.execute(
                """
                SELECT body FROM explanations
                WHERE question_id = ?
                ORDER BY id DESC LIMIT 1
                """,
                (question_id,),
            ).fetchone()
            if not row:
                continue
            body = row[0]
            row = cursor.execute(
                "SELECT MAX(version) FROM explanations WHERE question_id = ?",
                (question_id,),
            ).fetchone()
            next_version = (row[0] or 0) + 1
            cursor.execute(
                """
                INSERT INTO explanations(question_id, body, version, source)
                VALUES (?, ?, ?, ?)
                """,
                (question_id, body, next_version, "llm_checked"),
            )
            applied += 1
        elif kind == "tags_edit":
            add = payload.get("add") or []
            remove = payload.get("remove") or []
            for tag in remove:
                label = " ".join(str(tag).split()).strip()
                if not label:
                    continue
                row = cursor.execute(
                    "SELECT id FROM tags WHERE label = ?",
                    (label,),
                ).fetchone()
                if not row:
                    continue
                tag_id = row[0]
                cursor.execute(
                    "DELETE FROM question_tags WHERE question_id = ? AND tag_id = ?",
                    (question_id, tag_id),
                )
            for tag in add:
                label = " ".join(str(tag).split()).strip()
                if not label:
                    continue
                cursor.execute(
                    "INSERT OR IGNORE INTO tags(label) VALUES (?)",
                    (label,),
                )
                tag_id = cursor.execute(
                    "SELECT id FROM tags WHERE label = ?",
                    (label,),
                ).fetchone()[0]
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                    VALUES (?, ?, ?)
                    """,
                    (question_id, tag_id, "human"),
                )
            applied += 1
        elif kind == "subtopics_edit":
            add = payload.get("add") or []
            remove = payload.get("remove") or []
            for item in remove:
                label = " ".join(str(item).split()).strip()
                if not label:
                    continue
                row = cursor.execute(
                    "SELECT id FROM subtopics WHERE name = ?",
                    (label,),
                ).fetchone()
                if not row:
                    continue
                sub_id = row[0]
                cursor.execute(
                    "DELETE FROM question_subtopics WHERE question_id = ? AND subtopic_id = ?",
                    (question_id, sub_id),
                )
            for item in add:
                label = " ".join(str(item).split()).strip()
                if not label:
                    continue
                cursor.execute(
                    "INSERT OR IGNORE INTO subtopics(name) VALUES (?)",
                    (label,),
                )
                sub_id = cursor.execute(
                    "SELECT id FROM subtopics WHERE name = ?",
                    (label,),
                ).fetchone()[0]
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO question_subtopics(question_id, subtopic_id)
                    VALUES (?, ?)
                    """,
                    (question_id, sub_id),
                )
            applied += 1
        update_edit_request_status([{"id": edit.get("id")}], "applied")

    conn.commit()
    conn.close()
    web_message = run_build_web(Path(__file__).resolve().parent)
    return {"message": f"{applied} 件をSQLiteに反映しました。 / {web_message}"}


def add_report_supabase(serial, kind):
    if not serial or kind not in {"explanation", "tag", "subtopic"}:
        return {"message": "報告に失敗しました。"}
    payload, error = supabase_request(
        "POST",
        "feedback",
        "",
        [{"serial": serial, "kind": kind}],
    )
    if error:
        return {"message": error}
    return {"message": f"報告しました: {serial} ({kind})"}


def clear_reports_supabase(items):
    if not items:
        return {"message": "消去対象がありません。"}
    for item in items:
        serial = item.get("serial")
        kinds = item.get("kinds", [])
        if not serial or not kinds:
            continue
        for kind in kinds:
            if kind not in {"explanation", "tag", "subtopic"}:
                continue
            query = f"?serial=eq.{quote(serial)}&kind=eq.{quote(kind)}"
            _, error = supabase_request("DELETE", "feedback", query)
            if error:
                return {"message": error}
    return {"message": "選択した報告フラグを消去しました。"}


def clear_supabase_feedback(serial, kind):
    if kind not in {"explanation", "tag", "subtopic"}:
        return
    query = f"?serial=eq.{quote(serial)}&kind=eq.{quote(kind)}"
    supabase_request("DELETE", "feedback", query)


def run_command(repo_root, args):
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return f"失敗: {' '.join(args)}\n{result.stderr.strip()}"
    return f"完了: {' '.join(args)}"


def run_backup(repo_root):
    import subprocess

    script_path = Path(repo_root) / "scripts" / "backup_sqlite.sh"
    if not script_path.exists():
        return "失敗: scripts/backup_sqlite.sh が見つかりません。"
    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return f"失敗: {result.stderr.strip()}"
    output = result.stdout.strip()
    return output or "完了しました。"


def run_build_web(repo_root):
    message = run_command(
        repo_root,
        [
            "scripts/generate_web_json.py",
            "--out",
            "output/web/questions.json",
            "--index-dir",
            "output/web/index",
        ],
    )
    return message


def run_build_all(repo_root):
    messages = []
    messages.append(run_build_web(repo_root))
    messages.append(
        run_command(
            repo_root,
            [
                "scripts/generate_study_sets.py",
                "--out",
                "output/study_sets.json",
            ],
        )
    )
    messages.append(
        run_command(
            repo_root,
            [
                "scripts/generate_progress_report.py",
                "--out",
                "output/progress_report.json",
            ],
        )
    )
    return " / ".join(messages)


def main():
    args = parse_args()
    db_path = Path(args.db)
    catalog_path = Path(args.subtopics)
    prompt_sample_path = Path(args.prompt_sample)

    subtopic_catalog = {}
    if catalog_path.exists():
        subtopic_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))

    prompt_sample = ""
    if prompt_sample_path.exists():
        prompt_sample = prompt_sample_path.read_text(encoding="utf-8")

    server = HTTPServer((args.host, args.port), Handler)
    server.db_path = db_path
    server.subtopic_catalog = subtopic_catalog
    server.prompt_sample = prompt_sample
    server.repo_root = Path(__file__).resolve().parent
    server.downloads_dir = args.downloads
    ensure_feedback_table(db_path)

    print(f"Server running: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down.")


if __name__ == "__main__":
    main()
