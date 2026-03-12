#!/usr/bin/env python3
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import sys
import time
from urllib import request
from urllib.error import HTTPError, URLError
from urllib.parse import quote


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_gemini_beginner_qa import (  # noqa: E402
    build_supabase_text_in_filter,
    call_gemini,
    extract_retry_delay_seconds,
    extract_text,
    is_daily_quota_exhausted,
    load_env,
    log_stderr,
    log_stdout,
    normalize_label,
    resolve_api_key,
    resolve_model,
    retry_wait_seconds,
    select_candidates,
    write_progress_file,
)


FEATURE_ORDER = ("mcq", "tf", "short", "senryu")
PRACTICE_FEATURES = ("mcq", "tf", "short")
DEFAULT_FEATURES = ",".join(FEATURE_ORDER)


def normalize_true_false_answer(value):
    raw = (
        str("" if value is None else value)
        .strip()
        .lower()
        .replace("〇", "○")
        .replace("◯", "○")
        .replace("×", "✕")
        .replace("☓", "✕")
        .replace("o", "○")
        .replace("x", "✕")
    )
    if raw in ("○", "true", "t", "yes"):
        return "○"
    if raw in ("✕", "false", "f", "no"):
        return "✕"
    return ""


def normalize_practice_question_type(value):
    raw = str(value or "").strip().lower()
    if raw in ("tf", "true_false", "boolean", "marubatsu"):
        return "tf"
    if raw in ("short", "short_answer", "qa_short"):
        return "short"
    return "mcq"


def get_practice_label(question_type):
    normalized = normalize_practice_question_type(question_type)
    if normalized == "tf":
        return "大問II（○×）"
    if normalized == "short":
        return "大問III（一問一答）"
    return "大問I（4択）"


def parse_features(text):
    features = []
    for raw in str(text or "").split(","):
        feature = str(raw or "").strip().lower()
        if not feature:
            continue
        if feature in ("mcq", "tf", "short", "senryu"):
            if feature not in features:
                features.append(feature)
            continue
        raise ValueError(f"Unsupported feature: {feature}")
    return features or list(FEATURE_ORDER)


def get_expected_count(feature):
    return 3 if feature == "senryu" else 5


def build_requested_counts(selected_features, practice_counts, senryu_count, force=False):
    requested = {}
    counts = dict(practice_counts or {})
    for feature in selected_features:
        if feature == "senryu":
            current = int(senryu_count or 0)
        else:
            current = int(counts.get(feature, 0) or 0)
        remaining = get_expected_count(feature) if force else max(0, get_expected_count(feature) - current)
        if remaining > 0:
            requested[feature] = remaining
    return requested


def get_requested_feature_labels(requested_counts):
    ordered = [feature for feature in FEATURE_ORDER if requested_counts.get(feature)]
    return ",".join(ordered)


def build_bundle_prompt(record, requested_counts):
    case_text = str(record.get("case_text") or "").strip() or "（なし）"
    stem = str(record.get("stem") or "").strip() or "（なし）"
    choices = record.get("choices") or []
    answer_indices = [int(v) for v in (record.get("answer_indices") or []) if str(v).isdigit()]
    if record.get("answer_none"):
        answer_label = "なし"
    elif answer_indices:
        answer_label = "・".join(str(v) for v in answer_indices)
    else:
        answer_label = "（不明）"
    explanation = str(record.get("explanation_latest") or "").strip() or "（なし）"
    subject = str(record.get("subject") or "").strip() or "（不明）"
    subtopics = [str(v or "").strip() for v in (record.get("subtopics") or []) if str(v or "").strip()]
    tags = [str(v or "").strip() for v in (record.get("tags") or []) if str(v or "").strip()]

    sections = [
        "あなたは医療系国家試験の学習支援AIです。",
        "次の元問題を参照し、指定された種類の補助教材だけを JSON で生成してください。",
        "練習問題は元問題の焼き直しにせず、近縁概念・対比・鑑別・関連知識へ少しずらしてください。",
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
        "【共通ルール】",
        "- 出力は JSON のみ。前置き・Markdown・コードブロックは禁止",
        "- 依頼していないキーは出力しない",
        "- 各 items は指定件数ちょうどにする",
        "- 練習問題の explanation は2〜5文の短い解説にする",
        "- focus は出題の切り口を短く書く",
        "- 医学的に不確かな内容は断定しすぎない",
        "",
        "【依頼する出力キー】",
    ]

    if requested_counts.get("mcq"):
        sections.extend(
            [
                f'- "practice_mcq": 4択問題を{requested_counts["mcq"]}問',
                f"  - 過去問の {get_practice_label('mcq')} を意識した簡潔な文体",
                "  - choices は4個、answer_index は 1〜4、answer_text は空文字",
            ]
        )
    if requested_counts.get("tf"):
        sections.extend(
            [
                f'- "practice_tf": ○×問題を{requested_counts["tf"]}問',
                f"  - 過去問の {get_practice_label('tf')} を意識し、断定文1文で作る",
                '  - choices は空配列、answer_index は null、answer_text は「○」または「✕」',
            ]
        )
    if requested_counts.get("short"):
        sections.extend(
            [
                f'- "practice_short": 一問一答を{requested_counts["short"]}問',
                f"  - 過去問の {get_practice_label('short')} を意識する",
                "  - stem は「答えなさい。」で終える",
                "  - choices は空配列、answer_index は null、answer_text は短い模範解答",
            ]
        )
    if requested_counts.get("senryu"):
        sections.extend(
            [
                f'- "senryu": 問題に関連する学習川柳を{requested_counts["senryu"]}個',
                "  - senryu は1行、commentary は2〜4文",
                "  - それぞれ内容が被りすぎないようにする",
            ]
        )

    sections.extend(
        [
            "",
            "【JSONスキーマ】",
            '- practice_mcq: {"items":[{"focus":"...","stem":"...","choices":["...","...","...","..."],"answer_index":1,"answer_text":"","explanation":"..."}]}',
            '- practice_tf: {"items":[{"focus":"...","stem":"...。","choices":[],"answer_index":null,"answer_text":"○","explanation":"..."}]}',
            '- practice_short: {"items":[{"focus":"...","stem":"...答えなさい。","choices":[],"answer_index":null,"answer_text":"...","explanation":"..."}]}',
            '- senryu: {"items":[{"senryu":"...","commentary":"..."}]}',
        ]
    )
    return "\n".join(sections)


def extract_json_payload(text):
    if not text:
        return {}
    try:
        json_start = text.index("{")
        json_end = text.rindex("}")
    except ValueError:
        return {}
    if json_end <= json_start:
        return {}
    try:
        payload = json.loads(text[json_start : json_end + 1])
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def coerce_items_block(payload, key):
    raw = payload.get(key)
    if isinstance(raw, dict):
        items = raw.get("items")
        return items if isinstance(items, list) else []
    if isinstance(raw, list):
        return raw
    return []


def parse_practice_items(raw_items, question_type):
    items = []
    question_type = normalize_practice_question_type(question_type)
    for raw in raw_items or []:
        if not isinstance(raw, dict):
            continue
        focus = normalize_label(raw.get("focus"))[:80]
        stem = str(raw.get("stem") or "").strip()
        explanation = str(raw.get("explanation") or "").strip()[:1200]
        if not stem:
            continue
        if question_type == "mcq":
            choices = raw.get("choices")
            if not isinstance(choices, list):
                continue
            normalized_choices = [str(v or "").strip() for v in choices if str(v or "").strip()]
            try:
                answer_index = int(raw.get("answer_index"))
            except (TypeError, ValueError):
                continue
            # Gemini occasionally returns zero-based indices for 4-choice items.
            if len(normalized_choices) == 4 and answer_index in (0, 1, 2, 3):
                answer_index += 1
            if len(normalized_choices) != 4 or answer_index < 1 or answer_index > 4:
                continue
            items.append(
                {
                    "focus": focus,
                    "stem": stem[:240],
                    "choices": normalized_choices[:4],
                    "answer_index": answer_index,
                    "answer_text": "",
                    "explanation": explanation,
                }
            )
            continue
        if question_type == "tf":
            answer_text = normalize_true_false_answer(
                raw.get("answer_text") or raw.get("answer") or raw.get("answer_label") or raw.get("correct")
            )
            if answer_text not in ("○", "✕"):
                continue
            items.append(
                {
                    "focus": focus,
                    "stem": stem[:240],
                    "choices": [],
                    "answer_index": None,
                    "answer_text": answer_text,
                    "explanation": explanation,
                }
            )
            continue
        answer_text = str(raw.get("answer_text") or raw.get("answer") or "").strip()
        if not answer_text:
            continue
        items.append(
            {
                "focus": focus,
                "stem": stem[:240],
                "choices": [],
                "answer_index": None,
                "answer_text": answer_text[:200],
                "explanation": explanation,
            }
        )
    return items


def parse_senryu_items(raw_items):
    items = []
    for raw in raw_items or []:
        if not isinstance(raw, dict):
            continue
        senryu = str(raw.get("senryu") or raw.get("poem") or raw.get("senryu_text") or "").strip()
        commentary = str(raw.get("commentary") or raw.get("explanation") or raw.get("note") or "").strip()
        if not senryu or not commentary:
            continue
        items.append({"senryu": senryu[:120], "commentary": commentary[:1200]})
    return items


def parse_bundle_response(text):
    payload = extract_json_payload(text)
    return {
        "mcq": parse_practice_items(coerce_items_block(payload, "practice_mcq"), "mcq"),
        "tf": parse_practice_items(coerce_items_block(payload, "practice_tf"), "tf"),
        "short": parse_practice_items(coerce_items_block(payload, "practice_short"), "short"),
        "senryu": parse_senryu_items(coerce_items_block(payload, "senryu")),
    }


def validate_practice_items(items, question_type, expected_count):
    if len(items) != expected_count:
        return f"{question_type} items must contain exactly {expected_count} entries"
    seen_stems = set()
    for item in items:
        stem = str(item.get("stem") or "").strip()
        explanation = str(item.get("explanation") or "").strip()
        if len(stem) < 8:
            return f"{question_type} stem is too short"
        if len(stem) > 220:
            return f"{question_type} stem is too long"
        if explanation and len(explanation) > 1200:
            return f"{question_type} explanation is too long"
        key = stem.lower()
        if key in seen_stems:
            return f"{question_type} stem is duplicated"
        seen_stems.add(key)
        if question_type == "mcq":
            choices = item.get("choices") or []
            answer_index = item.get("answer_index")
            if len(choices) != 4:
                return "mcq choices must contain 4 entries"
            if len({str(choice).strip() for choice in choices}) != 4:
                return "mcq choices are duplicated"
            if answer_index not in (1, 2, 3, 4):
                return "mcq answer_index must be 1..4"
        elif question_type == "tf":
            if item.get("answer_text") not in ("○", "✕"):
                return "tf answer_text must be ○ or ✕"
        else:
            answer_text = str(item.get("answer_text") or "").strip()
            if len(answer_text) < 1:
                return "short answer_text is empty"
            if len(answer_text) > 200:
                return "short answer_text is too long"
    return ""


def validate_senryu_items(items, expected_count):
    if len(items) != expected_count:
        return f"senryu items must contain exactly {expected_count} entries"
    seen = set()
    for item in items:
        senryu = str(item.get("senryu") or "").strip()
        commentary = str(item.get("commentary") or "").strip()
        if len(senryu) < 5:
            return "senryu text is too short"
        if len(commentary) < 20:
            return "senryu commentary is too short"
        key = senryu.lower()
        if key in seen:
            return "senryu text is duplicated"
        seen.add(key)
    return ""


def validate_requested_sections(parsed_sections, requested_counts):
    for feature, expected_count in requested_counts.items():
        items = parsed_sections.get(feature) or []
        if feature in PRACTICE_FEATURES:
            error = validate_practice_items(items, feature, expected_count)
        else:
            error = validate_senryu_items(items, expected_count)
        if error:
            return error
    return ""


def get_supabase_config(args):
    url = str(args.supabase_url or "").strip()
    key = str(args.supabase_service_key or "").strip()
    if not url:
        from os import environ

        url = str(environ.get("SUPABASE_URL") or "").strip()
    if not key:
        from os import environ

        key = str(environ.get("SUPABASE_SERVICE_KEY") or "").strip()
    if not url or not key:
        return None, "SUPABASE_URL と SUPABASE_SERVICE_KEY を設定してください。"
    return {"url": url.rstrip("/"), "key": key}, ""


def fetch_existing_practice_counts(cfg, serials, batch_size=40):
    counts = {}
    for start in range(0, len(serials), batch_size):
        chunk = serials[start : start + batch_size]
        if not chunk:
            continue
        endpoint = (
            f"{cfg['url']}/rest/v1/practice_questions"
            f"?select=base_serial,question_type"
            f"&base_serial={quote(build_supabase_text_in_filter(chunk))}"
            "&is_public=eq.true"
            "&limit=1000"
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
            serial = str((row or {}).get("base_serial") or "").strip()
            question_type = normalize_practice_question_type((row or {}).get("question_type"))
            if not serial:
                continue
            counts.setdefault(serial, {"mcq": 0, "tf": 0, "short": 0})
            counts[serial][question_type] = int(counts[serial].get(question_type, 0) or 0) + 1
    return counts


def fetch_existing_senryu_counts(cfg, serials, batch_size=40):
    counts = {}
    for start in range(0, len(serials), batch_size):
        chunk = serials[start : start + batch_size]
        if not chunk:
            continue
        endpoint = (
            f"{cfg['url']}/rest/v1/question_senryu"
            f"?select=base_serial"
            f"&base_serial={quote(build_supabase_text_in_filter(chunk))}"
            "&limit=1000"
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
            serial = str((row or {}).get("base_serial") or "").strip()
            if not serial:
                continue
            counts[serial] = int(counts.get(serial, 0) or 0) + 1
    return counts


def insert_practice_questions(cfg, serial, question_type, items, model, mode):
    if not items:
        return "", ""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload = []
    for item in items:
        payload.append(
            {
                "base_serial": serial,
                "question_type": normalize_practice_question_type(question_type),
                "focus": item.get("focus") or "",
                "stem": item.get("stem") or "",
                "choices": item.get("choices") or [],
                "answer_index": item.get("answer_index"),
                "answer_text": item.get("answer_text") or "",
                "explanation": item.get("explanation") or "",
                "created_by": None,
                "model": model,
                "mode": mode,
                "is_public": True,
                "published_at": now,
                "published_by": None,
            }
        )
    req = request.Request(
        f"{cfg['url']}/rest/v1/practice_questions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
    )
    req.add_header("apikey", cfg["key"])
    req.add_header("Authorization", f"Bearer {cfg['key']}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "return=minimal")
    try:
        with request.urlopen(req, timeout=30):
            return now, ""
    except HTTPError as err:
        return "", f"HTTP {err.code}: {err.read().decode('utf-8', errors='replace')}"
    except URLError as err:
        return "", f"URL error: {err.reason}"
    except Exception as err:
        return "", f"Request error: {err.__class__.__name__}: {err}"


def insert_question_senryu(cfg, serial, items, model, mode):
    if not items:
        return "", ""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload = []
    for item in items:
        payload.append(
            {
                "base_serial": serial,
                "senryu": item.get("senryu") or "",
                "commentary": item.get("commentary") or "",
                "created_by": None,
                "model": model,
                "mode": mode,
            }
        )
    req = request.Request(
        f"{cfg['url']}/rest/v1/question_senryu",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
    )
    req.add_header("apikey", cfg["key"])
    req.add_header("Authorization", f"Bearer {cfg['key']}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "return=minimal")
    try:
        with request.urlopen(req, timeout=30):
            return now, ""
    except HTTPError as err:
        return "", f"HTTP {err.code}: {err.read().decode('utf-8', errors='replace')}"
    except URLError as err:
        return "", f"URL error: {err.reason}"
    except Exception as err:
        return "", f"Request error: {err.__class__.__name__}: {err}"


def generate_content_record(item, requested_counts, api_key, model, route_mode, args, cfg):
    prompt = build_bundle_prompt(item, requested_counts)
    raw_text = ""
    error = ""
    parsed_sections = {}
    for attempt in range(args.max_retries + 1):
        payload, api_error = call_gemini(api_key, model, prompt, args.max_output_tokens)
        if api_error:
            error = api_error
            if is_daily_quota_exhausted(error):
                return {
                    "ok": False,
                    "serial": item["serial"],
                    "subject": item["subject"],
                    "error": error,
                    "raw_text": raw_text,
                    "stop_run": True,
                    "retry_after_seconds": extract_retry_delay_seconds(error),
                }
        else:
            raw_text = extract_text(payload)
            parsed_sections = parse_bundle_response(raw_text)
            validation_error = validate_requested_sections(parsed_sections, requested_counts)
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
        time.sleep(retry_wait_seconds(error, args.retry_sleep_seconds))

    saved_counts = {}
    for feature in FEATURE_ORDER:
        items = parsed_sections.get(feature) or []
        if not items:
            continue
        if feature in PRACTICE_FEATURES:
            _, save_error = insert_practice_questions(cfg, item["serial"], feature, items, model=model, mode=route_mode)
        else:
            _, save_error = insert_question_senryu(cfg, item["serial"], items, model=model, mode=route_mode)
        if save_error:
            return {
                "ok": False,
                "serial": item["serial"],
                "subject": item["subject"],
                "error": f"save failed ({feature}): {save_error}",
                "raw_text": raw_text,
            }
        saved_counts[feature] = len(items)

    if args.sleep_seconds > 0:
        time.sleep(args.sleep_seconds)

    return {
        "ok": True,
        "serial": item["serial"],
        "subject": item["subject"],
        "requested_counts": requested_counts,
        "saved_counts": saved_counts,
        "model": model,
        "mode": route_mode,
        "raw_text": raw_text,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate practice questions and question senryu via Gemini and save to Supabase."
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
    parser.add_argument("--exam-type", default="", help="Filter by exam type code (A/B or full exam_type text).")
    parser.add_argument("--exam-session", type=int, default=0, help="Filter by exam session number.")
    parser.add_argument("--subject", default="", help="Filter by subject name.")
    parser.add_argument(
        "--features",
        default=DEFAULT_FEATURES,
        help="Comma-separated: mcq,tf,short,senryu",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Generate requested features even if enough rows already exist. Existing rows are not deleted.",
    )
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
    parser.add_argument("--max-output-tokens", type=int, default=8192, help="Gemini maxOutputTokens.")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per target when API/format error occurs.")
    parser.add_argument("--retry-sleep-seconds", type=float, default=20.0, help="Base wait seconds before retry.")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Sleep between successful requests.")
    parser.add_argument("--max-workers", type=int, default=1, help="Concurrent Gemini/Supabase workers.")
    parser.add_argument("--output-dir", default="output/gemini_batches", help="Output directory for JSONL log.")
    parser.add_argument("--out", default="", help="Output JSONL path (overrides --output-dir).")
    parser.add_argument("--progress-file", default="", help="Progress JSON path (default: alongside --out).")
    parser.add_argument("--dry-run", action="store_true", help="Print first prompt and exit without API calls.")
    return parser.parse_args()


def main():
    args = parse_args()
    load_env(REPO_ROOT / ".env")

    try:
        selected_features = parse_features(args.features)
    except ValueError as err:
        print(str(err), file=sys.stderr)
        return 1

    cfg, cfg_error = get_supabase_config(args)
    if cfg_error:
        print(cfg_error, file=sys.stderr)
        return 1

    import sqlite3

    conn = sqlite3.connect(args.db)
    try:
        candidates = select_candidates(conn, args)
    finally:
        conn.close()
    if not candidates:
        print("No questions matched the requested filters.")
        return 0

    serials = [item["serial"] for item in candidates if item.get("serial")]
    practice_counts = {}
    senryu_counts = {}
    if not args.dry_run:
        practice_counts = fetch_existing_practice_counts(cfg, serials)
        senryu_counts = fetch_existing_senryu_counts(cfg, serials)

    targets = []
    for item in candidates:
        requested_counts = build_requested_counts(
            selected_features,
            practice_counts.get(item["serial"], {}),
            senryu_counts.get(item["serial"], 0),
            force=args.force,
        )
        if not requested_counts:
            continue
        targets.append({"item": item, "requested_counts": requested_counts})

    if args.limit > 0 and not args.serials:
        targets = targets[: args.limit]

    if not targets:
        print("No questions matched (already generated or filters returned none).")
        return 0

    max_workers = max(1, int(args.max_workers or 1))
    total_requested_sections = sum(len(target["requested_counts"]) for target in targets)
    log_stdout(
        f"targets={len(targets)} max_workers={max_workers} requested_features={','.join(selected_features)} sections={total_requested_sections}"
    )
    if args.dry_run:
        sample = targets[0]
        log_stdout(f"sample_serial={sample['item']['serial']}")
        log_stdout(f"sample_features={get_requested_feature_labels(sample['requested_counts'])}")
        log_stdout("----- prompt preview -----")
        log_stdout(build_bundle_prompt(sample["item"], sample["requested_counts"]))
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

    ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    out_path = Path(args.out) if args.out else Path(args.output_dir) / f"practice_senryu_batch_filled_{ts}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = Path(args.progress_file) if args.progress_file else out_path.with_suffix(".progress.json")

    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    progress = {
        "started_at": started_at,
        "updated_at": started_at,
        "total": len(targets),
        "completed": 0,
        "succeeded": 0,
        "failed": 0,
        "remaining": len(targets),
        "max_workers": max_workers,
        "features": selected_features,
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
    stop_requested = None
    with out_path.open("w", encoding="utf-8") as out_file:
        if max_workers == 1:
            for target in targets:
                item = target["item"]
                requested_counts = target["requested_counts"]
                result = generate_content_record(item, requested_counts, api_key, model, route_mode, args, cfg)
                if not result.get("ok"):
                    failed.append({"serial": item["serial"], "error": result.get("error") or "unknown error"})
                    out_file.write(
                        json.dumps(
                            {
                                "serial": item["serial"],
                                "subject": item["subject"],
                                "requested_counts": requested_counts,
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
                    progress["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    progress["last_serial"] = item["serial"]
                    progress["last_status"] = "failed"
                    progress["last_error"] = result.get("error") or "unknown error"
                    if result.get("stop_run"):
                        progress["last_status"] = "stopped"
                        progress["stop_reason"] = "daily_quota_exhausted"
                    write_progress_file(progress_path, progress)
                    log_stderr(
                        f"[{progress['completed']}/{progress['total']}] failed serial={item['serial']} "
                        f"features={get_requested_feature_labels(requested_counts)} "
                        f"ok={progress['succeeded']} fail={progress['failed']} error={result.get('error') or 'unknown error'}"
                    )
                    if result.get("stop_run"):
                        stop_requested = result
                        retry_after = float(result.get("retry_after_seconds") or 0.0)
                        if retry_after > 0:
                            log_stderr(
                                f"stopping remaining targets: daily quota exhausted, retry after about {int(retry_after)} seconds"
                            )
                        else:
                            log_stderr("stopping remaining targets: daily quota exhausted")
                        break
                    continue
                inserted += 1
                out_file.write(json.dumps(result, ensure_ascii=False) + "\n")
                out_file.flush()
                progress["completed"] += 1
                progress["succeeded"] += 1
                progress["remaining"] = max(0, progress["total"] - progress["completed"])
                progress["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                progress["last_serial"] = result["serial"]
                progress["last_status"] = "saved"
                progress["last_error"] = ""
                write_progress_file(progress_path, progress)
                log_stdout(
                    f"[{progress['completed']}/{progress['total']}] saved serial={result['serial']} "
                    f"features={get_requested_feature_labels(result['saved_counts'])} "
                    f"ok={progress['succeeded']} fail={progress['failed']}"
                )
        else:
            executor = ThreadPoolExecutor(max_workers=max_workers)
            try:
                future_map = {
                    executor.submit(
                        generate_content_record,
                        target["item"],
                        target["requested_counts"],
                        api_key,
                        model,
                        route_mode,
                        args,
                        cfg,
                    ): target
                    for target in targets
                }
                for future in as_completed(future_map):
                    if future.cancelled():
                        continue
                    target = future_map[future]
                    item = target["item"]
                    requested_counts = target["requested_counts"]
                    try:
                        result = future.result()
                    except Exception as err:  # pragma: no cover - defensive
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
                                    "requested_counts": requested_counts,
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
                        progress["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        progress["last_serial"] = item["serial"]
                        progress["last_status"] = "failed"
                        progress["last_error"] = result.get("error") or "unknown error"
                        if result.get("stop_run"):
                            progress["last_status"] = "stopped"
                            progress["stop_reason"] = "daily_quota_exhausted"
                        write_progress_file(progress_path, progress)
                        log_stderr(
                            f"[{progress['completed']}/{progress['total']}] failed serial={item['serial']} "
                            f"features={get_requested_feature_labels(requested_counts)} "
                            f"ok={progress['succeeded']} fail={progress['failed']} error={result.get('error') or 'unknown error'}"
                        )
                        if result.get("stop_run"):
                            stop_requested = result
                            for other_future in future_map:
                                if other_future is future:
                                    continue
                                other_future.cancel()
                            retry_after = float(result.get("retry_after_seconds") or 0.0)
                            if retry_after > 0:
                                log_stderr(
                                    f"stopping remaining targets: daily quota exhausted, retry after about {int(retry_after)} seconds"
                                )
                            else:
                                log_stderr("stopping remaining targets: daily quota exhausted")
                            break
                        continue
                    inserted += 1
                    out_file.write(json.dumps(result, ensure_ascii=False) + "\n")
                    out_file.flush()
                    progress["completed"] += 1
                    progress["succeeded"] += 1
                    progress["remaining"] = max(0, progress["total"] - progress["completed"])
                    progress["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    progress["last_serial"] = result["serial"]
                    progress["last_status"] = "saved"
                    progress["last_error"] = ""
                    write_progress_file(progress_path, progress)
                    log_stdout(
                        f"[{progress['completed']}/{progress['total']}] saved serial={result['serial']} "
                        f"features={get_requested_feature_labels(result['saved_counts'])} "
                        f"ok={progress['succeeded']} fail={progress['failed']}"
                    )
            finally:
                executor.shutdown(wait=True, cancel_futures=True)

    log_stdout(f"done: inserted={inserted}, failed={len(failed)}, saved={out_path}")
    if stop_requested:
        retry_after = float(stop_requested.get("retry_after_seconds") or 0.0)
        if retry_after > 0:
            log_stderr(f"stopped early: daily quota exhausted. retry after about {int(retry_after)} seconds")
        else:
            log_stderr("stopped early: daily quota exhausted")
        return 1
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
