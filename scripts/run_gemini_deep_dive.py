#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import request
from urllib.error import HTTPError, URLError


def load_env(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def normalize_digits(text):
    if text is None:
        return ""
    return str(text).translate(str.maketrans("０１２３４５６７８９", "0123456789"))


def normalize_label(value):
    return " ".join(str(value or "").split()).strip()


def parse_answer_meta(answer_text, answer_index, answer_indices_json, answer_none):
    answer_none_flag = bool(answer_none)
    indices = []
    if answer_indices_json:
        try:
            data = json.loads(answer_indices_json)
            if isinstance(data, list):
                indices = [int(x) for x in data if str(x).isdigit()]
        except (json.JSONDecodeError, TypeError, ValueError):
            indices = []
    if not indices and answer_text:
        normalized = normalize_digits(answer_text)
        if "なし" in normalized:
            answer_none_flag = True
        elif "すべて" in normalized:
            indices = [1, 2, 3, 4]
        else:
            digits = re.findall(r"[1-4]", normalized)
            indices = sorted({int(d) for d in digits})
    if not indices and answer_index:
        try:
            indices = [int(answer_index)]
        except (TypeError, ValueError):
            indices = []
    if answer_none_flag:
        indices = []
    return indices, answer_none_flag


def expand_serials(serials_text):
    serials = []
    for chunk in [s.strip() for s in str(serials_text or "").split(",") if s.strip()]:
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


def resolve_api_key(args):
    if args.api_key:
        return args.api_key, "cli", "custom"
    if args.api_key_source == "paid":
        return os.environ.get("GEMINI_API_KEY_PAID", ""), "GEMINI_API_KEY_PAID", "paid"
    if args.api_key_source == "free":
        return (
            os.environ.get("GEMINI_API_KEY_FREE", "") or os.environ.get("GEMINI_API_KEY", ""),
            "GEMINI_API_KEY_FREE/GEMINI_API_KEY",
            "free",
        )
    if args.api_key_source == "legacy":
        return os.environ.get("GEMINI_API_KEY", ""), "GEMINI_API_KEY", "legacy"

    paid = os.environ.get("GEMINI_API_KEY_PAID", "")
    if paid:
        return paid, "GEMINI_API_KEY_PAID", "paid"
    free = os.environ.get("GEMINI_API_KEY_FREE", "") or os.environ.get("GEMINI_API_KEY", "")
    if free:
        return free, "GEMINI_API_KEY_FREE/GEMINI_API_KEY", "free"
    return "", "auto", "free"


def resolve_model(args, mode):
    if args.model:
        return args.model
    default_model = "gemini-3-flash-preview"
    if mode == "paid":
        return os.environ.get("GEMINI_MODEL_PAID", "") or os.environ.get("GEMINI_MODEL", "") or default_model
    if mode == "legacy":
        return os.environ.get("GEMINI_MODEL", "") or default_model
    return os.environ.get("GEMINI_MODEL_FREE", "") or os.environ.get("GEMINI_MODEL", "") or default_model


def ensure_deep_dive_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS deep_dive_explanations (
          serial TEXT PRIMARY KEY,
          explanation TEXT,
          tags_json TEXT,
          updated_at TEXT,
          created_by TEXT
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(deep_dive_explanations)").fetchall()}
    required_columns = {
        "explanation": "TEXT",
        "tags_json": "TEXT",
        "updated_at": "TEXT",
        "created_by": "TEXT",
    }
    for name, column_type in required_columns.items():
        if name in columns:
            continue
        conn.execute(f"ALTER TABLE deep_dive_explanations ADD COLUMN {name} {column_type}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_deep_dive_explanations_updated_at ON deep_dive_explanations(updated_at)"
    )


def question_columns(conn):
    return {row[1] for row in conn.execute("PRAGMA table_info(questions)").fetchall()}


def select_targets(conn, args):
    serials = expand_serials(args.serials)
    where = []
    params = []
    if serials:
        placeholders = ",".join("?" for _ in serials)
        where.append(f"q.serial IN ({placeholders})")
        params.extend(serials)
    if args.exam_type:
        where.append("q.exam_type_code = ?")
        params.append(args.exam_type)
    if args.exam_session > 0:
        where.append("q.exam_session = ?")
        params.append(args.exam_session)
    if args.subject:
        where.append("s.name = ?")
        params.append(args.subject)
    where.append(
        "NOT EXISTS (SELECT 1 FROM deep_dive_explanations dd WHERE dd.serial = q.serial)"
    )
    where_sql = " AND ".join(where) if where else "1=1"

    q_columns = question_columns(conn)
    answer_indices_field = (
        "q.answer_indices_json"
        if "answer_indices_json" in q_columns
        else "NULL AS answer_indices_json"
    )
    answer_none_field = "q.answer_none" if "answer_none" in q_columns else "0 AS answer_none"
    order_sql = "ORDER BY q.serial"
    if args.order == "new":
        order_sql = "ORDER BY q.exam_session DESC, q.serial DESC"

    query = f"""
        SELECT
            q.serial,
            COALESCE(s.name, '') AS subject,
            q.case_text,
            q.stem,
            q.choices_json,
            q.answer_index,
            q.answer_text,
            {answer_indices_field},
            {answer_none_field},
            (
                SELECT e.body
                FROM explanations e
                WHERE e.question_id = q.id
                ORDER BY e.id DESC
                LIMIT 1
            ) AS latest_explanation
        FROM questions q
        LEFT JOIN subjects s ON s.id = q.subject_id
        WHERE {where_sql}
        {order_sql}
    """
    if not serials and args.limit > 0:
        query += "\nLIMIT ?"
        params.append(args.limit)

    rows = conn.execute(query, params).fetchall()
    out = []
    for row in rows:
        (
            serial,
            subject,
            case_text,
            stem,
            choices_json,
            answer_index,
            answer_text,
            answer_indices_json,
            answer_none,
            latest_explanation,
        ) = row
        try:
            choices = json.loads(choices_json or "[]")
            if not isinstance(choices, list):
                choices = []
        except json.JSONDecodeError:
            choices = []
        answer_indices, answer_none_flag = parse_answer_meta(
            answer_text, answer_index, answer_indices_json, answer_none
        )
        out.append(
            {
                "serial": str(serial or ""),
                "subject": str(subject or ""),
                "case_text": str(case_text or ""),
                "stem": str(stem or ""),
                "choices": choices,
                "answer_indices": answer_indices,
                "answer_none": answer_none_flag,
                "explanation_latest": str(latest_explanation or ""),
            }
        )
    return out


def format_answer_label(record):
    if record.get("answer_none"):
        return "なし"
    indices = [int(v) for v in record.get("answer_indices", []) if str(v).isdigit()]
    if not indices:
        return ""
    return "・".join(str(v) for v in indices)


def build_deep_dive_prompt(record):
    lines = []
    lines.append("あなたは医療系国家試験の深掘り解説生成AIです。")
    lines.append(
        "以下の情報をもとに、学習者の理解が深まり「なぜそうなるか」が腑に落ちるように、段階的に詳しく掘り下げた解説を作成してください。"
    )
    lines.append("")
    lines.append("【入力】")
    lines.append("症例文:")
    lines.append(record.get("case_text") or "（なし）")
    lines.append("")
    lines.append("問題文:")
    lines.append(record.get("stem") or "")
    lines.append("")
    lines.append("選択肢:")
    for idx, choice in enumerate(record.get("choices", []), start=1):
        lines.append(f"{idx}. {choice}")
    lines.append("")
    answer_label = format_answer_label(record)
    if answer_label:
        lines.append(f"解答: {answer_label}")
        lines.append("")
    lines.append("既存解説:")
    lines.append(record.get("explanation_latest") or "（なし）")
    lines.append("")
    lines.append("【出力形式（厳守）】")
    lines.append("explanation には Markdown 文字列を入れること。")
    lines.append("Markdownの推奨構成:")
    lines.append("## Level 1 要点（初学者向け）")
    lines.append("## Level 2 理由・機序（中級）")
    lines.append("## Level 3 鑑別・関連疾患・臨床応用（上級）")
    lines.append("## Level 4 落とし穴と試験対策（発展）")
    lines.append("## 1分復習チェック")
    lines.append("- 3〜5個の確認ポイントを箇条書きで示す")
    lines.append("")
    lines.append("関連タグ:")
    lines.append("- タグ1")
    lines.append("- タグ2")
    lines.append("- タグ3")
    lines.append("")
    lines.append("【ルール】")
    lines.append("- 既存解説と矛盾しないこと")
    lines.append("- 説明は長くても良い（疑問が解決し、興味が持てる内容を優先）")
    lines.append("- explanation は見出し・箇条書き・強調を使い、読みやすく構成する")
    lines.append("- 試験で問われやすい観点を盛り込むこと")
    lines.append("- 余計な断定を避け、根拠や理由を言語化すること")
    lines.append("- タグは8〜15個程度、具体的な医学用語で")
    lines.append("- 出力は日本語。目安 900〜2400 文字程度")
    lines.append("")
    lines.append("出力は次のJSONのみを返してください。")
    lines.append('{"explanation":"...","tags":["...","..."]}')
    return "\n".join(lines)


def call_gemini(api_key, model, prompt, max_output_tokens):
    model_name = model.replace("models/", "")
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model_name}:generateContent?key={api_key}"
    )
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": max_output_tokens},
    }
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8")), ""
    except HTTPError as err:
        payload = err.read().decode("utf-8", errors="replace")
        return {}, f"HTTP {err.code}: {payload}"
    except URLError as err:
        return {}, f"URL error: {err.reason}"


def extract_text(payload):
    parts = []
    for cand in payload.get("candidates", []):
        content = cand.get("content", {})
        for part in content.get("parts", []):
            text = part.get("text")
            if text:
                parts.append(text)
    return "\n".join(parts).strip()


def parse_deep_dive_response(text):
    if not text:
        return "", []
    try:
        json_start = text.index("{")
        json_end = text.rindex("}")
        if json_start >= 0 and json_end > json_start:
            payload = json.loads(text[json_start : json_end + 1])
            explanation = str(payload.get("explanation") or "").strip()
            tags = []
            raw_tags = payload.get("tags")
            if isinstance(raw_tags, list):
                for value in raw_tags:
                    tag = normalize_label(value)
                    if tag and tag not in tags:
                        tags.append(tag)
            return explanation, tags
    except Exception:
        pass
    return text.strip(), []


def validate_result(explanation, tags):
    if not explanation:
        return "explanation is empty"
    if not tags:
        return "tags is empty"
    return ""


def retry_wait_seconds(error_text, base_wait):
    text = str(error_text or "")
    marker = "retry in "
    idx = text.lower().find(marker)
    if idx < 0:
        return float(base_wait)
    tail = text[idx + len(marker) :]
    digits = []
    for ch in tail:
        if ch.isdigit() or ch == ".":
            digits.append(ch)
        else:
            break
    if not digits:
        return float(base_wait)
    try:
        return max(float(base_wait), float("".join(digits)) + 0.5)
    except Exception:
        return float(base_wait)


def upsert_deep_dive(conn, serial, explanation, tags, created_by):
    exists = conn.execute(
        "SELECT 1 FROM deep_dive_explanations WHERE serial = ? LIMIT 1",
        (serial,),
    ).fetchone()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        INSERT INTO deep_dive_explanations
          (serial, explanation, tags_json, updated_at, created_by)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(serial) DO UPDATE SET
          explanation = excluded.explanation,
          tags_json = excluded.tags_json,
          updated_at = excluded.updated_at,
          created_by = excluded.created_by
        """,
        (
            serial,
            explanation,
            json.dumps(tags, ensure_ascii=False),
            now,
            created_by,
        ),
    )
    return "updated" if exists else "inserted"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate deep-dive explanations/tags via Gemini and save into SQLite."
    )
    parser.add_argument("--db", default="output/ahaki.sqlite", help="Path to SQLite DB.")
    parser.add_argument("--limit", type=int, default=20, help="Max target questions when --serials is empty.")
    parser.add_argument("--serials", default="", help="Comma-separated serials or range (e.g. A01-001..A01-020).")
    parser.add_argument("--order", choices=["new", "serial"], default="new", help="Order for auto selection.")
    parser.add_argument("--exam-type", default="", help="Filter by exam type code (A/B).")
    parser.add_argument("--exam-session", type=int, default=0, help="Filter by exam session number.")
    parser.add_argument("--subject", default="", help="Filter by subject name.")
    parser.add_argument("--model", default="", help="Gemini model ID (optional).")
    parser.add_argument("--api-key", default="", help="Override API key.")
    parser.add_argument(
        "--api-key-source",
        default="free",
        choices=["auto", "paid", "free", "legacy"],
        help=(
            "API key source. paid=GEMINI_API_KEY_PAID, free=GEMINI_API_KEY_FREE fallback GEMINI_API_KEY, "
            "legacy=GEMINI_API_KEY, auto=PAID->FREE."
        ),
    )
    parser.add_argument("--max-output-tokens", type=int, default=8192, help="Gemini maxOutputTokens.")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per target when API/format error occurs.")
    parser.add_argument("--retry-sleep-seconds", type=float, default=20.0, help="Base wait seconds before retry.")
    parser.add_argument("--sleep-seconds", type=float, default=1.2, help="Sleep between successful requests.")
    parser.add_argument("--max-requests", type=int, default=0, help="Hard cap of API requests (0=all targets).")
    parser.add_argument("--output-dir", default="output/gemini_batches", help="Output directory for JSONL log.")
    parser.add_argument("--out", default="", help="Output JSONL path (overrides --output-dir).")
    parser.add_argument("--created-by", default="gemini_cli", help="created_by value saved in DB.")
    parser.add_argument("--dry-run", action="store_true", help="Print first prompt and exit without API calls.")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    load_env(repo_root / ".env")

    conn = sqlite3.connect(args.db)
    ensure_deep_dive_table(conn)
    targets = select_targets(conn, args)
    if args.max_requests > 0:
        targets = targets[: args.max_requests]
    if not targets:
        conn.close()
        print("No questions matched (already generated or filters returned none).")
        return 0

    print(f"targets={len(targets)}")
    if args.dry_run:
        sample = targets[0]
        print(f"sample_serial={sample['serial']}")
        print("----- prompt preview -----")
        print(build_deep_dive_prompt(sample))
        conn.close()
        return 0

    api_key, key_label, route_mode = resolve_api_key(args)
    if not api_key:
        conn.close()
        print(
            "API key is not set. Set GEMINI_API_KEY_PAID/GEMINI_API_KEY_FREE/GEMINI_API_KEY or pass --api-key.",
            file=sys.stderr,
        )
        return 1
    model = resolve_model(args, route_mode)
    print(f"API key source: {key_label}")
    print(f"Model: {model}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else Path(args.output_dir) / f"deep_dive_batch_filled_{ts}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_file = out_path.open("w", encoding="utf-8")
    inserted = 0
    updated = 0
    for idx, item in enumerate(targets, start=1):
        prompt = build_deep_dive_prompt(item)
        explanation = ""
        tags = []
        raw_text = ""
        error = ""

        for attempt in range(args.max_retries + 1):
            payload, api_error = call_gemini(api_key, model, prompt, args.max_output_tokens)
            if api_error:
                error = api_error
            else:
                raw_text = extract_text(payload)
                explanation, tags = parse_deep_dive_response(raw_text)
                validation_error = validate_result(explanation, tags)
                if not validation_error:
                    error = ""
                    break
                error = f"format error: {validation_error}"

            if attempt >= args.max_retries:
                out_file.close()
                conn.close()
                print(
                    f"Failed serial={item['serial']} after retries. last_error={error}",
                    file=sys.stderr,
                )
                print(
                    f"partial: inserted={inserted}, updated={updated}, saved={out_path}",
                    file=sys.stderr,
                )
                return 1
            wait = retry_wait_seconds(error, args.retry_sleep_seconds)
            print(
                f"[{idx}/{len(targets)}] {item['serial']} retry {attempt + 1}/{args.max_retries}: {error}"
            )
            print(f"  waiting {wait:.1f}s")
            time.sleep(wait)

        result = upsert_deep_dive(conn, item["serial"], explanation, tags, args.created_by)
        conn.commit()
        if result == "inserted":
            inserted += 1
        else:
            updated += 1
        out_row = {
            "serial": item["serial"],
            "subject": item["subject"],
            "explanation": explanation,
            "tags": tags,
            "model": model,
            "api_key_source": key_label,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "raw_text": raw_text,
        }
        out_file.write(json.dumps(out_row, ensure_ascii=False) + "\n")
        out_file.flush()
        print(
            f"[{idx}/{len(targets)}] saved serial={item['serial']} tags={len(tags)} chars={len(explanation)}"
        )
        if args.sleep_seconds > 0 and idx < len(targets):
            time.sleep(args.sleep_seconds)

    out_file.close()
    conn.close()

    print(f"done: inserted={inserted}, updated={updated}, saved={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
