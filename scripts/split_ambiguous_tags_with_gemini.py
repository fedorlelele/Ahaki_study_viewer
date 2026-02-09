#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from urllib import request
from urllib.error import HTTPError, URLError


DEFAULT_TCM_SUBJECTS = {
    "東洋医学概論",
    "東洋医学臨床論",
    "経絡経穴概論",
    "あん摩マッサージ指圧理論",
    "はり理論",
    "きゅう理論",
}

DEFAULT_WESTERN_SUBJECTS = {
    "解剖学",
    "生理学",
    "病理学概論",
    "臨床医学総論",
    "臨床医学各論",
    "リハビリテーション医学",
    "医療概論",
    "衛生学・公衆衛生学",
    "総合問題",
    "関係法規",
}


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


def normalize_label(value):
    return " ".join(str(value or "").split()).strip()


def resolve_api_key(args):
    if args.api_key:
        return args.api_key, "cli"
    if args.api_key_source == "paid":
        return os.environ.get("GEMINI_API_KEY_PAID", ""), "GEMINI_API_KEY_PAID"
    if args.api_key_source == "free":
        return (
            os.environ.get("GEMINI_API_KEY_FREE", "") or os.environ.get("GEMINI_API_KEY", ""),
            "GEMINI_API_KEY_FREE/GEMINI_API_KEY",
        )
    if args.api_key_source == "legacy":
        return os.environ.get("GEMINI_API_KEY", ""), "GEMINI_API_KEY"
    return (
        os.environ.get("GEMINI_API_KEY_PAID", "")
        or os.environ.get("GEMINI_API_KEY_FREE", "")
        or os.environ.get("GEMINI_API_KEY", ""),
        "auto",
    )


def load_subject_groups(config_path):
    tcm = set(DEFAULT_TCM_SUBJECTS)
    western = set(DEFAULT_WESTERN_SUBJECTS)
    path = Path(config_path)
    if not path.exists():
        return tcm, western
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return tcm, western
    groups = payload.get("subject_groups") or {}
    tcm_values = groups.get("tcm") or []
    western_values = groups.get("western") or []
    if tcm_values:
        tcm = {normalize_label(v) for v in tcm_values if normalize_label(v)}
    if western_values:
        western = {normalize_label(v) for v in western_values if normalize_label(v)}
    return tcm, western


def collect_candidates(conn, tcm_subjects, western_subjects, min_count):
    usage = {}
    rows = conn.execute(
        """
        SELECT t.label, q.serial, COALESCE(s.name, ''), q.stem
        FROM question_tags qt
        JOIN tags t ON t.id = qt.tag_id
        JOIN questions q ON q.id = qt.question_id
        LEFT JOIN subjects s ON s.id = q.subject_id
        """
    ).fetchall()
    for raw_label, serial, subject_name, stem in rows:
        label = normalize_label(raw_label)
        if not label:
            continue
        if "（東洋医学）" in label or "（西洋医学）" in label:
            continue
        subject = normalize_label(subject_name)
        row = usage.setdefault(
            label,
            {
                "tcm_serials": set(),
                "western_serials": set(),
                "tcm_samples": [],
                "western_samples": [],
            },
        )
        sample = {
            "serial": str(serial or ""),
            "stem": normalize_label(stem)[:80],
        }
        if subject in tcm_subjects:
            row["tcm_serials"].add(str(serial or ""))
            if len(row["tcm_samples"]) < 2:
                row["tcm_samples"].append(sample)
        elif subject in western_subjects:
            row["western_serials"].add(str(serial or ""))
            if len(row["western_samples"]) < 2:
                row["western_samples"].append(sample)
    out = []
    for tag, row in usage.items():
        tcm_count = len(row["tcm_serials"])
        western_count = len(row["western_serials"])
        if tcm_count < min_count or western_count < min_count:
            continue
        out.append(
            {
                "tag": tag,
                "tcm_count": tcm_count,
                "western_count": western_count,
                "tcm_samples": row["tcm_samples"],
                "western_samples": row["western_samples"],
            }
        )
    out.sort(key=lambda x: (-(x["tcm_count"] + x["western_count"]), x["tag"]))
    return out


def build_prompt(batch):
    lines = [
        "あなたは医療系国家試験のタグ整備担当です。",
        "以下のタグについて、東洋医学と西洋医学で意味が分かれるなら分割対象にしてください。",
        "",
        "判定基準:",
        "- split_required=true: 同一表記だが学問体系が異なり意味が混同される。",
        "- split_required=false: 実質同一概念、または分割不要。",
        "- 新旧呼称・表記ゆれは分割不要（同義語扱い）。",
        "",
        "出力はJSONのみ。入力タグを必ず全件返すこと。",
        '{"items":[{"tag":"...","split_required":true,"confidence":0.0,"reason":"..."}]}',
        "",
        "対象タグ:",
    ]
    for idx, item in enumerate(batch, start=1):
        tcm_examples = " / ".join(
            f"{x['serial']}:{x['stem']}" for x in item.get("tcm_samples", [])
        )
        western_examples = " / ".join(
            f"{x['serial']}:{x['stem']}" for x in item.get("western_samples", [])
        )
        lines.append(
            f"{idx}. tag={item['tag']}; tcm_count={item['tcm_count']}; western_count={item['western_count']}; "
            f"tcm_examples={tcm_examples or '-'}; western_examples={western_examples or '-'}"
        )
    return "\n".join(lines)


def call_gemini(api_key, model, prompt, max_output_tokens):
    model_name = model.replace("models/", "")
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model_name}:generateContent?key={api_key}"
    )
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": max_output_tokens},
    }
    req = request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    try:
        with request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8")), ""
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        return {}, f"HTTP {exc.code}: {detail}"
    except URLError as exc:
        return {}, f"URL error: {exc.reason}"


def extract_text(payload):
    parts = []
    for cand in payload.get("candidates", []):
        content = cand.get("content", {})
        for part in content.get("parts", []):
            text = part.get("text")
            if text:
                parts.append(text)
    return "\n".join(parts).strip()


def parse_batch_result(text, expected_tags):
    expected = set(expected_tags)
    parsed = {
        tag: {"split_required": False, "confidence": 0.0, "reason": ""}
        for tag in expected_tags
    }
    if not text:
        return parsed
    try:
        start = text.index("{")
        end = text.rindex("}")
        payload = json.loads(text[start : end + 1])
    except Exception:
        return parsed

    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return parsed
    for item in items:
        if not isinstance(item, dict):
            continue
        tag = normalize_label(item.get("tag"))
        if tag not in expected:
            continue
        split_required = bool(item.get("split_required", False))
        try:
            confidence = float(item.get("confidence", 0.0))
        except Exception:
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        reason = normalize_label(item.get("reason", ""))
        parsed[tag] = {
            "split_required": split_required,
            "confidence": confidence,
            "reason": reason,
        }
    return parsed


def _retry_wait_seconds(error_text, base_wait):
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


def ensure_tag_id(conn, label):
    normalized = normalize_label(label)
    if not normalized:
        return None
    conn.execute("INSERT OR IGNORE INTO tags(label) VALUES (?)", (normalized,))
    row = conn.execute("SELECT id FROM tags WHERE label = ?", (normalized,)).fetchone()
    return int(row[0]) if row else None


def apply_split_to_db(conn, tag, tcm_subjects, western_subjects):
    source_label = normalize_label(tag)
    source_row = conn.execute("SELECT id FROM tags WHERE label = ?", (source_label,)).fetchone()
    if not source_row:
        return {"tag": source_label, "moved_tcm": 0, "moved_western": 0, "deep_dive": 0}
    source_id = int(source_row[0])
    tcm_label = f"{source_label}（東洋医学）"
    western_label = f"{source_label}（西洋医学）"
    tcm_id = ensure_tag_id(conn, tcm_label)
    western_id = ensure_tag_id(conn, western_label)

    moved_tcm = 0
    moved_western = 0
    rows = conn.execute(
        """
        SELECT q.id, q.serial, COALESCE(s.name, '')
        FROM question_tags qt
        JOIN questions q ON q.id = qt.question_id
        LEFT JOIN subjects s ON s.id = q.subject_id
        WHERE qt.tag_id = ?
        """,
        (source_id,),
    ).fetchall()
    for question_id, _serial, subject_name in rows:
        subject = normalize_label(subject_name)
        target_id = None
        if subject in tcm_subjects:
            target_id = tcm_id
            moved_tcm += 1
        elif subject in western_subjects:
            target_id = western_id
            moved_western += 1
        if not target_id:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
            VALUES (?, ?, ?)
            """,
            (question_id, target_id, "auto_split_domain"),
        )
        conn.execute(
            "DELETE FROM question_tags WHERE question_id = ? AND tag_id = ?",
            (question_id, source_id),
        )

    deep_dive = 0
    subject_by_serial = {
        str(serial): normalize_label(subject)
        for serial, subject in conn.execute(
            """
            SELECT q.serial, COALESCE(s.name, '')
            FROM questions q
            LEFT JOIN subjects s ON s.id = q.subject_id
            """
        ).fetchall()
    }
    try:
        dd_rows = conn.execute(
            """
            SELECT serial, tags_json
            FROM deep_dive_explanations
            WHERE tags_json IS NOT NULL AND tags_json != ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        dd_rows = []
    for serial, tags_json in dd_rows:
        try:
            values = json.loads(tags_json or "[]")
        except json.JSONDecodeError:
            values = []
        if not isinstance(values, list):
            continue
        subject = subject_by_serial.get(str(serial), "")
        replacement = None
        if subject in tcm_subjects:
            replacement = tcm_label
        elif subject in western_subjects:
            replacement = western_label
        if not replacement:
            continue
        changed = False
        new_values = []
        for value in values:
            label = normalize_label(value)
            if label == source_label:
                changed = True
                label = replacement
            if label and label not in new_values:
                new_values.append(label)
        if changed:
            conn.execute(
                "UPDATE deep_dive_explanations SET tags_json = ? WHERE serial = ?",
                (json.dumps(new_values, ensure_ascii=False), serial),
            )
            deep_dive += 1
    return {
        "tag": source_label,
        "moved_tcm": moved_tcm,
        "moved_western": moved_western,
        "deep_dive": deep_dive,
    }


def iter_chunks(values, size):
    chunk = max(1, int(size))
    for idx in range(0, len(values), chunk):
        yield values[idx : idx + chunk]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Detect and split ambiguous tags between TCM and Western medicine."
    )
    parser.add_argument("--db", default="output/ahaki.sqlite", help="SQLite path.")
    parser.add_argument(
        "--subject-group-config",
        default="config/tag_concept_rules.json",
        help="JSON file that contains subject_groups.tcm/western.",
    )
    parser.add_argument("--model", default="gemini-3-flash-preview", help="Gemini model.")
    parser.add_argument("--api-key", default="", help="Override API key directly.")
    parser.add_argument(
        "--api-key-source",
        default="free",
        choices=["auto", "paid", "free", "legacy"],
        help=(
            "API key source. auto=PAID->FREE->LEGACY, paid=GEMINI_API_KEY_PAID, "
            "free=GEMINI_API_KEY_FREE fallback GEMINI_API_KEY, legacy=GEMINI_API_KEY."
        ),
    )
    parser.add_argument("--min-count", type=int, default=3, help="Minimum count per domain.")
    parser.add_argument("--limit", type=int, default=0, help="Max candidate tags to evaluate.")
    parser.add_argument("--batch-size", type=int, default=10, help="Tags per API request.")
    parser.add_argument("--max-requests", type=int, default=0, help="Max API requests (0=all).")
    parser.add_argument("--sleep-seconds", type=float, default=1.2, help="Sleep between requests.")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per failed request.")
    parser.add_argument("--retry-sleep-seconds", type=float, default=20.0, help="Retry wait base.")
    parser.add_argument("--max-output-tokens", type=int, default=4096, help="Gemini maxOutputTokens.")
    parser.add_argument(
        "--out",
        default="",
        help="Suggestion JSON output path (default: output/tag_split_suggestions_<timestamp>.json)",
    )
    parser.add_argument("--apply", action="store_true", help="Apply split to DB.")
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.7,
        help="Apply only items with confidence >= this value.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    load_env(repo_root / ".env")
    api_key, key_label = resolve_api_key(args)
    if not api_key:
        raise SystemExit(
            "API key is not set. Set GEMINI_API_KEY_PAID/GEMINI_API_KEY_FREE/GEMINI_API_KEY or pass --api-key."
        )
    print(f"API key source: {key_label}")

    tcm_subjects, western_subjects = load_subject_groups(args.subject_group_config)
    conn = sqlite3.connect(args.db)

    candidates = collect_candidates(conn, tcm_subjects, western_subjects, args.min_count)
    if args.limit > 0:
        candidates = candidates[: args.limit]
    if args.max_requests > 0:
        candidates = candidates[: args.max_requests * args.batch_size]
    print(f"candidates={len(candidates)} (min_count={args.min_count})")
    if not candidates:
        conn.close()
        print("done: no candidates")
        return

    suggestions = []
    batches = list(iter_chunks(candidates, args.batch_size))
    for batch_idx, batch in enumerate(batches, start=1):
        prompt = build_prompt(batch)
        payload = {}
        error = ""
        for attempt in range(args.max_retries + 1):
            payload, error = call_gemini(api_key, args.model, prompt, args.max_output_tokens)
            if not error:
                break
            if attempt >= args.max_retries:
                break
            wait = _retry_wait_seconds(error, args.retry_sleep_seconds)
            print(f"[{batch_idx}/{len(batches)}] API error retry {attempt + 1}/{args.max_retries}: {error}")
            print(f"  waiting {wait:.1f}s")
            time.sleep(wait)
        if error:
            conn.close()
            raise SystemExit(f"APIエラーのため停止: {error}")
        text = extract_text(payload)
        expected_tags = [item["tag"] for item in batch]
        parsed = parse_batch_result(text, expected_tags)
        for item in batch:
            tag = item["tag"]
            judged = parsed.get(tag, {})
            suggestions.append(
                {
                    "tag": tag,
                    "tcm_count": item["tcm_count"],
                    "western_count": item["western_count"],
                    "split_required": bool(judged.get("split_required", False)),
                    "confidence": float(judged.get("confidence", 0.0)),
                    "reason": str(judged.get("reason", "")),
                }
            )
        yes = sum(1 for x in suggestions[-len(batch) :] if x["split_required"])
        print(f"[{batch_idx}/{len(batches)}] judged={len(batch)} split_yes={yes}")
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else (repo_root / "output" / f"tag_split_suggestions_{timestamp}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_payload = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "model": args.model,
        "api_key_source": key_label,
        "subject_groups": {
            "tcm": sorted(tcm_subjects),
            "western": sorted(western_subjects),
        },
        "items": suggestions,
    }
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"suggestions saved: {out_path}")

    if not args.apply:
        conn.close()
        return

    selected = [
        item
        for item in suggestions
        if item["split_required"] and float(item.get("confidence", 0.0)) >= float(args.min_confidence)
    ]
    print(f"apply targets={len(selected)} (min_confidence={args.min_confidence})")
    applied = []
    for item in selected:
        result = apply_split_to_db(conn, item["tag"], tcm_subjects, western_subjects)
        applied.append(result)
        print(
            f"  {result['tag']}: moved_tcm={result['moved_tcm']} moved_western={result['moved_western']} deep_dive={result['deep_dive']}"
        )
    conn.commit()
    conn.close()
    print(f"done: applied={len(applied)}")


if __name__ == "__main__":
    main()
