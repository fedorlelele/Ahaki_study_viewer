#!/usr/bin/env python3
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from urllib.parse import quote


FULLWIDTH_TO_ASCII = str.maketrans("０１２３４５６７８９", "0123456789")


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


def log_stdout(message):
    print(message, flush=True)


def log_stderr(message):
    print(message, file=sys.stderr, flush=True)


def normalize_digits(text):
    if text is None:
        return ""
    return str(text).translate(FULLWIDTH_TO_ASCII)


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


def format_answer_label(record):
    if record.get("answer_none"):
        return "なし"
    indices = [int(v) for v in record.get("answer_indices", []) if str(v).isdigit()]
    if not indices:
        return ""
    return "・".join(str(v) for v in indices)


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


def question_columns(conn):
    return {row[1] for row in conn.execute("PRAGMA table_info(questions)").fetchall()}


def load_tags(conn):
    rows = conn.execute(
        """
        SELECT DISTINCT qt.question_id, t.label
        FROM question_tags qt
        JOIN tags t ON t.id = qt.tag_id
        ORDER BY t.label
        """
    ).fetchall()
    data = {}
    for question_id, label in rows:
        data.setdefault(question_id, []).append(str(label or "").strip())
    return data


def load_subtopics(conn):
    rows = conn.execute(
        """
        SELECT qs.question_id, st.name
        FROM question_subtopics qs
        JOIN subtopics st ON st.id = qs.subtopic_id
        ORDER BY st.name
        """
    ).fetchall()
    data = {}
    for question_id, name in rows:
        data.setdefault(question_id, []).append(str(name or "").strip())
    return data


def select_candidates(conn, args):
    serials = expand_serials(args.serials)
    where = []
    params = []
    if serials:
        placeholders = ",".join("?" for _ in serials)
        where.append(f"q.serial IN ({placeholders})")
        params.extend(serials)
    if args.exam_type:
        where.append("q.exam_type = ?")
        params.append(args.exam_type)
    if args.exam_session > 0:
        where.append("q.exam_session = ?")
        params.append(args.exam_session)
    if args.subject:
        where.append("s.name = ?")
        params.append(args.subject)
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
    elif args.order == "serial_desc":
        order_sql = "ORDER BY q.serial DESC"

    rows = conn.execute(
        f"""
        SELECT
            q.id,
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
        """,
        params,
    ).fetchall()

    tag_map = load_tags(conn)
    subtopic_map = load_subtopics(conn)
    out = []
    for row in rows:
        (
            question_id,
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
                "id": int(question_id),
                "serial": str(serial or ""),
                "subject": str(subject or ""),
                "case_text": str(case_text or ""),
                "stem": str(stem or ""),
                "choices": [str(item or "") for item in choices],
                "answer_indices": answer_indices,
                "answer_none": answer_none_flag,
                "explanation_latest": str(latest_explanation or ""),
                "tags": [tag for tag in tag_map.get(question_id, []) if tag],
                "subtopics": [name for name in subtopic_map.get(question_id, []) if name],
            }
        )
    return out


def build_beginner_qa_prompt(record):
    case_text = record.get("case_text") or "（なし）"
    stem = record.get("stem") or ""
    choices = record.get("choices") or []
    answer_label = format_answer_label(record) or "（不明）"
    explanation = record.get("explanation_latest") or "（なし）"
    subject = record.get("subject") or "（不明）"
    subtopics = record.get("subtopics") or []
    tags = record.get("tags") or []
    return "\n".join(
        [
            "あなたは医療系国家試験の初学者支援AIです。",
            "次の問題について、初学者や低学力者がつまずきやすい点を、やさしい日本語のQ&Aで5件だけ作成してください。",
            "",
            "【元問題】",
            f"シリアル: {record.get('serial') or '（不明）'}",
            f"科目: {subject}",
            f"小項目: {' / '.join(subtopics) if subtopics else '（なし）'}",
            f"タグ: {' / '.join(tags) if tags else '（なし）'}",
            "症例文:",
            case_text,
            "",
            "問題文:",
            stem,
            "",
            "選択肢:",
            *[f"{idx}. {choice}" for idx, choice in enumerate(choices, start=1)],
            "",
            f"正答: {answer_label}",
            "",
            "既存解説:",
            explanation,
            "",
            "【作るQ&Aの方針】",
            "- 5件ちょうど作る",
            "- 質問は学習者が実際に聞きそうな短い日本語にする",
            "- 回答は短すぎず長すぎず、2〜5文、目安80〜220文字にする",
            "- 難しい言葉には、かんたんな言い換えを添える",
            "- 上から順に、論点が重ならないようにする",
            "- 断定しすぎず、元問題と矛盾しないこと",
            "- 冗長な前置き、過度な敬語、長い箇条書きは避ける",
            "- Markdownは最小限なら使ってよいが、見出しは使わない",
            "",
            "【5件の担当論点】",
            "1. 正答の決め手",
            "2. 誤答しやすい選択肢・ひっかけ",
            "3. 用語の意味",
            "4. 問題文や症例の読み取り",
            "5. 覚え方・見分け方",
            "",
            "【出力形式（JSONのみ）】",
            '{"items":[',
            '  {"order":1,"focus":"正答の決め手","question":"...","answer":"..."},',
            '  {"order":2,"focus":"誤答しやすい選択肢・ひっかけ","question":"...","answer":"..."},',
            '  {"order":3,"focus":"用語の意味","question":"...","answer":"..."},',
            '  {"order":4,"focus":"問題文や症例の読み取り","question":"...","answer":"..."},',
            '  {"order":5,"focus":"覚え方・見分け方","question":"...","answer":"..."}',
            "]}",
        ]
    )


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
    except Exception as err:
        return {}, f"Request error: {err.__class__.__name__}: {err}"


def extract_text(payload):
    parts = []
    for cand in payload.get("candidates", []):
        content = cand.get("content", {})
        for part in content.get("parts", []):
            text = part.get("text")
            if text:
                parts.append(text)
    return "\n".join(parts).strip()


def parse_beginner_qa_response(text):
    if not text:
        return []
    try:
        json_start = text.index("{")
        json_end = text.rindex("}")
        if json_start >= 0 and json_end > json_start:
            payload = json.loads(text[json_start : json_end + 1])
        else:
            return []
    except Exception:
        return []
    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        return []
    items = []
    seen_questions = set()
    for index, raw in enumerate(raw_items):
        if not isinstance(raw, dict):
            continue
        question = normalize_label(raw.get("question"))
        answer = str(raw.get("answer") or "").strip()
        focus = normalize_label(raw.get("focus"))
        order_raw = raw.get("order")
        try:
            order = int(order_raw)
        except (TypeError, ValueError):
            order = index + 1
        key = question.lower()
        if not question or not answer or key in seen_questions:
            continue
        seen_questions.add(key)
        items.append(
            {
                "order": order,
                "focus": focus[:80],
                "question": question[:120],
                "answer": answer[:1200],
            }
        )
    items.sort(key=lambda item: item["order"])
    return items[:5]


def validate_beginner_qa_items(items):
    if len(items) != 5:
        return "items must contain exactly 5 entries"
    focus_seen = set()
    for item in items:
        question = str(item.get("question") or "").strip()
        answer = str(item.get("answer") or "").strip()
        focus = str(item.get("focus") or "").strip()
        if len(question) < 6:
            return "question is too short"
        if len(question) > 90:
            return "question is too long"
        if len(answer) < 40:
            return "answer is too short"
        if len(answer) > 400:
            return "answer is too long"
        if focus:
            normalized_focus = focus.lower()
            if normalized_focus in focus_seen:
                return "focus is duplicated"
            focus_seen.add(normalized_focus)
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


def build_supabase_text_in_filter(values):
    escaped = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        escaped_text = text.replace("\\", "\\\\").replace('"', '\\"')
        escaped.append(f'"{escaped_text}"')
    return f"in.({','.join(escaped)})"


def get_supabase_config(args):
    url = (args.supabase_url or os.environ.get("SUPABASE_URL") or "").strip()
    key = (args.supabase_service_key or os.environ.get("SUPABASE_SERVICE_KEY") or "").strip()
    if not url or not key:
        return None, "SUPABASE_URL と SUPABASE_SERVICE_KEY を設定してください。"
    return {"url": url.rstrip("/"), "key": key}, ""


def fetch_existing_serials(cfg, serials, batch_size=200):
    existing = set()
    for start in range(0, len(serials), batch_size):
        chunk = serials[start : start + batch_size]
        if not chunk:
            continue
        endpoint = (
            f"{cfg['url']}/rest/v1/question_beginner_qa"
            f"?select=serial&serial={quote(build_supabase_text_in_filter(chunk))}&is_active=eq.true"
        )
        req = request.Request(endpoint, method="GET")
        req.add_header("apikey", cfg["key"])
        req.add_header("Authorization", f"Bearer {cfg['key']}")
        req.add_header("Content-Type", "application/json")
        try:
            with request.urlopen(req, timeout=30) as resp:
                rows = json.loads(resp.read().decode("utf-8"))
        except HTTPError as err:
            payload = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase query failed: HTTP {err.code}: {payload}") from err
        except URLError as err:
            raise RuntimeError(f"Supabase query failed: {err.reason}") from err
        except Exception as err:
            raise RuntimeError(f"Supabase query failed: {err}") from err
        for row in rows or []:
            serial = str((row or {}).get("serial") or "").strip()
            if serial:
                existing.add(serial)
    return existing


def upsert_beginner_qa(cfg, serial, items, model, prompt_version):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = [
        {
            "serial": serial,
            "items": items,
            "model": model,
            "prompt_version": prompt_version,
            "is_active": True,
            "updated_at": now,
        }
    ]
    endpoint = f"{cfg['url']}/rest/v1/question_beginner_qa?on_conflict=serial"
    req = request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
    )
    req.add_header("apikey", cfg["key"])
    req.add_header("Authorization", f"Bearer {cfg['key']}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "resolution=merge-duplicates,return=minimal")
    try:
        with request.urlopen(req, timeout=30):
            return now, ""
    except HTTPError as err:
        payload = err.read().decode("utf-8", errors="replace")
        return "", f"HTTP {err.code}: {payload}"
    except URLError as err:
        return "", f"URL error: {err.reason}"
    except Exception as err:
        return "", f"Request error: {err.__class__.__name__}: {err}"


def write_progress_file(path, payload):
    Path(path).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def generate_beginner_qa_record(item, api_key, model, args, cfg):
    prompt = build_beginner_qa_prompt(item)
    items = []
    raw_text = ""
    error = ""
    for attempt in range(args.max_retries + 1):
        payload, api_error = call_gemini(api_key, model, prompt, args.max_output_tokens)
        if api_error:
            error = api_error
        else:
            raw_text = extract_text(payload)
            items = parse_beginner_qa_response(raw_text)
            validation_error = validate_beginner_qa_items(items)
            if not validation_error:
                error = ""
                break
            error = f"format error: {validation_error}"
        if attempt >= args.max_retries:
            return {
                "ok": False,
                "serial": item["serial"],
                "subject": item["subject"],
                "error": error,
                "raw_text": raw_text,
            }
        wait = retry_wait_seconds(error, args.retry_sleep_seconds)
        time.sleep(wait)

    _, save_error = upsert_beginner_qa(
        cfg,
        item["serial"],
        items,
        model=model,
        prompt_version=args.prompt_version,
    )
    if save_error:
        return {
            "ok": False,
            "serial": item["serial"],
            "subject": item["subject"],
            "error": f"save failed: {save_error}",
            "raw_text": raw_text,
        }
    if args.sleep_seconds > 0:
        time.sleep(args.sleep_seconds)
    return {
        "ok": True,
        "serial": item["serial"],
        "subject": item["subject"],
        "items": items,
        "model": model,
        "prompt_version": args.prompt_version,
        "raw_text": raw_text,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate beginner-friendly Q&A via Gemini and save to Supabase."
    )
    parser.add_argument("--db", default="output/ahaki.sqlite", help="Path to SQLite DB.")
    parser.add_argument("--limit", type=int, default=20, help="Max questions to generate when --serials is empty.")
    parser.add_argument("--serials", default="", help="Comma-separated serials or range (e.g. A01-001..A01-020).")
    parser.add_argument(
        "--order",
        choices=["new", "serial", "serial_desc"],
        default="new",
        help="Order for auto selection.",
    )
    parser.add_argument("--exam-type", default="", help="Filter by exam type code (A/B).")
    parser.add_argument("--exam-session", type=int, default=0, help="Filter by exam session number.")
    parser.add_argument("--subject", default="", help="Filter by subject name.")
    parser.add_argument("--force", action="store_true", help="Regenerate even if Supabase already has saved Q&A.")
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
    parser.add_argument("--supabase-url", default="", help="Override SUPABASE_URL.")
    parser.add_argument("--supabase-service-key", default="", help="Override SUPABASE_SERVICE_KEY.")
    parser.add_argument("--prompt-version", default="v1", help="Prompt version string saved to Supabase.")
    parser.add_argument("--max-output-tokens", type=int, default=4096, help="Gemini maxOutputTokens.")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per target when API/format error occurs.")
    parser.add_argument("--retry-sleep-seconds", type=float, default=20.0, help="Base wait seconds before retry.")
    parser.add_argument("--sleep-seconds", type=float, default=1.2, help="Sleep between successful requests.")
    parser.add_argument("--max-workers", type=int, default=1, help="Concurrent Gemini/Supabase workers.")
    parser.add_argument("--output-dir", default="output/gemini_batches", help="Output directory for JSONL log.")
    parser.add_argument("--out", default="", help="Output JSONL path (overrides --output-dir).")
    parser.add_argument("--progress-file", default="", help="Progress JSON path (default: alongside --out).")
    parser.add_argument("--dry-run", action="store_true", help="Print first prompt and exit without API calls.")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    load_env(repo_root / ".env")

    cfg, cfg_error = get_supabase_config(args)
    if cfg_error:
        print(cfg_error, file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    try:
        candidates = select_candidates(conn, args)
    finally:
        conn.close()
    if not candidates:
        print("No questions matched the requested filters.")
        return 0

    serials = [item["serial"] for item in candidates if item.get("serial")]
    existing = set()
    if not args.force:
        existing = fetch_existing_serials(cfg, serials)
    targets = [item for item in candidates if args.force or item["serial"] not in existing]
    if args.limit > 0 and not args.serials:
        targets = targets[: args.limit]
    if not targets:
        print("No questions matched (already generated or filters returned none).")
        return 0

    max_workers = max(1, int(args.max_workers or 1))
    log_stdout(f"targets={len(targets)} max_workers={max_workers}")
    if args.dry_run:
        sample = targets[0]
        log_stdout(f"sample_serial={sample['serial']}")
        log_stdout("----- prompt preview -----")
        log_stdout(build_beginner_qa_prompt(sample))
        return 0

    api_key, key_label, route_mode = resolve_api_key(args)
    if not api_key:
        print(
            "API key is not set. Set GEMINI_API_KEY_PAID/GEMINI_API_KEY_FREE/GEMINI_API_KEY or pass --api-key.",
            file=sys.stderr,
        )
        return 1
    model = resolve_model(args, route_mode)
    log_stdout(f"API key source: {key_label}")
    log_stdout(f"Model: {model}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else Path(args.output_dir) / f"beginner_qa_batch_filled_{ts}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = Path(args.progress_file) if args.progress_file else out_path.with_suffix(".progress.json")

    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    progress = {
        "started_at": started_at,
        "updated_at": started_at,
        "total": len(targets),
        "completed": 0,
        "succeeded": 0,
        "failed": 0,
        "remaining": len(targets),
        "max_workers": max_workers,
        "out_path": str(out_path),
        "last_serial": "",
        "last_status": "",
        "last_error": "",
    }
    write_progress_file(progress_path, progress)
    log_stdout(f"out_path={out_path}")
    log_stdout(f"progress_file={progress_path}")

    inserted = 0
    failed = []
    with out_path.open("w", encoding="utf-8") as out_file:
        if max_workers == 1:
            for idx, item in enumerate(targets, start=1):
                result = generate_beginner_qa_record(item, api_key, model, args, cfg)
                if not result.get("ok"):
                    failed.append({"serial": item["serial"], "error": result.get("error") or "unknown error"})
                    out_file.write(
                        json.dumps(
                            {
                                "serial": item["serial"],
                                "subject": item["subject"],
                                "error": result.get("error") or "unknown error",
                                "raw_text": result.get("raw_text") or "",
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    out_file.flush()
                    progress["completed"] += 1
                    progress["failed"] += 1
                    progress["remaining"] = max(0, progress["total"] - progress["completed"])
                    progress["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    progress["last_serial"] = item["serial"]
                    progress["last_status"] = "failed"
                    progress["last_error"] = result.get("error") or "unknown error"
                    write_progress_file(progress_path, progress)
                    log_stderr(
                        f"[{progress['completed']}/{progress['total']}] failed serial={item['serial']} "
                        f"ok={progress['succeeded']} fail={progress['failed']} error={result.get('error') or 'unknown error'}"
                    )
                    continue
                inserted += 1
                out_row = {
                    "serial": result["serial"],
                    "subject": result["subject"],
                    "items": result["items"],
                    "model": result["model"],
                    "prompt_version": result["prompt_version"],
                    "api_key_source": key_label,
                    "generated_at": result["generated_at"],
                    "raw_text": result["raw_text"],
                }
                out_file.write(json.dumps(out_row, ensure_ascii=False) + "\n")
                out_file.flush()
                progress["completed"] += 1
                progress["succeeded"] += 1
                progress["remaining"] = max(0, progress["total"] - progress["completed"])
                progress["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                progress["last_serial"] = result["serial"]
                progress["last_status"] = "saved"
                progress["last_error"] = ""
                write_progress_file(progress_path, progress)
                log_stdout(
                    f"[{progress['completed']}/{progress['total']}] saved serial={result['serial']} "
                    f"items={len(result['items'])} ok={progress['succeeded']} fail={progress['failed']}"
                )
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(generate_beginner_qa_record, item, api_key, model, args, cfg): (idx, item)
                    for idx, item in enumerate(targets, start=1)
                }
                for future in as_completed(future_map):
                    idx, item = future_map[future]
                    try:
                        result = future.result()
                    except Exception as err:  # pragma: no cover - defensive path
                        result = {
                            "ok": False,
                            "serial": item["serial"],
                            "subject": item["subject"],
                            "error": f"worker failed: {err}",
                            "raw_text": "",
                        }
                    if not result.get("ok"):
                        failed.append({"serial": item["serial"], "error": result.get("error") or "unknown error"})
                        out_file.write(
                            json.dumps(
                                {
                                    "serial": item["serial"],
                                    "subject": item["subject"],
                                    "error": result.get("error") or "unknown error",
                                    "raw_text": result.get("raw_text") or "",
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        out_file.flush()
                        progress["completed"] += 1
                        progress["failed"] += 1
                        progress["remaining"] = max(0, progress["total"] - progress["completed"])
                        progress["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        progress["last_serial"] = item["serial"]
                        progress["last_status"] = "failed"
                        progress["last_error"] = result.get("error") or "unknown error"
                        write_progress_file(progress_path, progress)
                        log_stderr(
                            f"[{progress['completed']}/{progress['total']}] failed serial={item['serial']} "
                            f"ok={progress['succeeded']} fail={progress['failed']} error={result.get('error') or 'unknown error'}"
                        )
                        continue
                    inserted += 1
                    out_row = {
                        "serial": result["serial"],
                        "subject": result["subject"],
                        "items": result["items"],
                        "model": result["model"],
                        "prompt_version": result["prompt_version"],
                        "api_key_source": key_label,
                        "generated_at": result["generated_at"],
                        "raw_text": result["raw_text"],
                    }
                    out_file.write(json.dumps(out_row, ensure_ascii=False) + "\n")
                    out_file.flush()
                    progress["completed"] += 1
                    progress["succeeded"] += 1
                    progress["remaining"] = max(0, progress["total"] - progress["completed"])
                    progress["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    progress["last_serial"] = result["serial"]
                    progress["last_status"] = "saved"
                    progress["last_error"] = ""
                    write_progress_file(progress_path, progress)
                    log_stdout(
                        f"[{progress['completed']}/{progress['total']}] saved serial={result['serial']} "
                        f"items={len(result['items'])} ok={progress['succeeded']} fail={progress['failed']}"
                    )

    log_stdout(f"done: inserted={inserted}, failed={len(failed)}, saved={out_path}")
    if failed:
        log_stderr("failed serials:")
        for item in failed[:20]:
            log_stderr(f"  {item['serial']}: {item['error']}")
        if len(failed) > 20:
            log_stderr(f"  ... and {len(failed) - 20} more")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
