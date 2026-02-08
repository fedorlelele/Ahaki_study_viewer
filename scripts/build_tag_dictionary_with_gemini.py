#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from urllib import request
from urllib.error import HTTPError, URLError


def load_env(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def ensure_tables(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS tag_descriptions (
            canonical_label TEXT PRIMARY KEY,
            description TEXT NOT NULL DEFAULT '',
            source_model TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS tag_aliases (
            alias TEXT PRIMARY KEY,
            canonical_label TEXT NOT NULL,
            confidence REAL DEFAULT 0.0,
            approved INTEGER NOT NULL DEFAULT 1,
            source_model TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_tag_aliases_canonical
            ON tag_aliases(canonical_label);
        """
    )


def normalize_label(value):
    return " ".join(str(value or "").split()).strip()


def load_approved_alias_map(conn):
    try:
        rows = conn.execute(
            """
            SELECT alias, canonical_label
            FROM tag_aliases
            WHERE approved = 1
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    alias_map = {}
    for alias, canonical in rows:
        alias_label = normalize_label(alias)
        canonical_label = normalize_label(canonical)
        if not alias_label or not canonical_label or alias_label == canonical_label:
            continue
        alias_map[alias_label] = canonical_label
    return alias_map


def resolve_canonical(label, alias_map):
    current = normalize_label(label)
    visited = set()
    while current and current in alias_map and current not in visited:
        visited.add(current)
        nxt = normalize_label(alias_map.get(current))
        if not nxt or nxt == current:
            break
        current = nxt
    return current


def canonicalize_label_list(labels, alias_map):
    normalized = []
    seen = set()
    for value in labels:
        label = normalize_label(value)
        if not label:
            continue
        canonical = resolve_canonical(label, alias_map) if alias_map else label
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        normalized.append(canonical)
    return normalized


def collect_full_tags(conn, alias_map=None):
    tags = set()
    rows = conn.execute("SELECT label FROM tags ORDER BY label").fetchall()
    for (label,) in rows:
        cleaned = normalize_label(label)
        if cleaned:
            tags.add(cleaned)
    try:
        dd_rows = conn.execute(
            "SELECT tags_json FROM deep_dive_explanations WHERE tags_json IS NOT NULL AND tags_json != ''"
        ).fetchall()
    except sqlite3.OperationalError:
        dd_rows = []
    for (tags_json,) in dd_rows:
        try:
            values = json.loads(tags_json or "[]")
        except json.JSONDecodeError:
            values = []
        for value in values:
            cleaned = normalize_label(value)
            if cleaned:
                tags.add(cleaned)
    sorted_tags = sorted(tags)
    if alias_map:
        return canonicalize_label_list(sorted_tags, alias_map)
    return sorted_tags


def build_prompt(tags):
    tag_lines = [f"{idx}. {tag}" for idx, tag in enumerate(tags, start=1)]
    return "\n".join(
        [
            "あなたは医療系国家試験の用語辞書を作る編集者です。",
            "以下のタグ一覧について、各タグの説明と同義語候補を作成してください。",
            "",
            "タグ一覧:",
            *tag_lines,
            "",
            "制約:",
            "- 説明はMarkdownで作成する（見出し・箇条書き・強調を必要に応じて使用）。",
            "- 説明は日本語で2〜6行程度。冗長にしない。",
            "- 対象は医療系国家試験学習者。",
            "- 同義語候補は表記ゆれ・略称・同義語のみ。曖昧語は除く。",
            "- 無理に同義語を作らない。",
            "- 入力の全タグを必ず1件ずつ出力する。",
            "- tagは入力と同じ文字列を返す。",
            "",
            "出力はJSONのみ:",
            '{"items":[{"tag":"...","description":"...","aliases":["...","..."]}]}',
        ]
    )


def call_gemini(api_key, model, prompt, max_output_tokens):
    model_name = model.replace("models/", "")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
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


def _normalize_aliases(values):
    aliases_raw = values if isinstance(values, list) else []
    aliases = []
    seen = set()
    for value in aliases_raw:
        alias = " ".join(str(value or "").split()).strip()
        if not alias or alias in seen:
            continue
        seen.add(alias)
        aliases.append(alias)
    return aliases


def _normalize_markdown_description(value):
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text


def parse_result(text):
    if not text:
        return {"description": "", "aliases": []}
    try:
        start = text.index("{")
        end = text.rindex("}")
        data = json.loads(text[start : end + 1])
        description = _normalize_markdown_description(data.get("description") or "")
        aliases = _normalize_aliases(data.get("aliases") or [])
        return {"description": description, "aliases": aliases}
    except Exception:
        fallback = _normalize_markdown_description(text)
        return {"description": fallback, "aliases": []}


def parse_batch_result(text, expected_tags):
    expected_set = set(expected_tags)
    parsed = {tag: {"description": "", "aliases": []} for tag in expected_tags}
    if not text:
        return parsed
    try:
        start = text.index("{")
        end = text.rindex("}")
        payload = json.loads(text[start : end + 1])
    except Exception:
        if len(expected_tags) == 1:
            parsed[expected_tags[0]] = parse_result(text)
        return parsed

    items = []
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        items = payload["items"]
    elif isinstance(payload, dict):
        # Fallback: {"tagA": {...}, "tagB": "..."} format
        for key, value in payload.items():
            if key not in expected_set:
                continue
            if isinstance(value, dict):
                items.append(
                    {
                        "tag": key,
                        "description": value.get("description") or "",
                        "aliases": value.get("aliases") or [],
                    }
                )
            else:
                items.append({"tag": key, "description": value, "aliases": []})

    for item in items:
        tag = " ".join(str(item.get("tag") or "").split()).strip()
        if tag not in expected_set:
            continue
        description = _normalize_markdown_description(item.get("description") or "")
        aliases = _normalize_aliases(item.get("aliases") or [])
        parsed[tag] = {"description": description, "aliases": aliases}
    return parsed


def iter_chunks(values, size):
    chunk_size = max(1, int(size))
    for idx in range(0, len(values), chunk_size):
        yield values[idx : idx + chunk_size]


def save_result(conn, tag, description, aliases, model):
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        INSERT INTO tag_descriptions(canonical_label, description, source_model, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(canonical_label) DO UPDATE SET
          description=excluded.description,
          source_model=excluded.source_model,
          updated_at=excluded.updated_at
        """,
        (tag, description, model, now),
    )
    for alias in aliases:
        if alias == tag:
            continue
        conn.execute(
            """
            INSERT INTO tag_aliases(alias, canonical_label, confidence, approved, source_model, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(alias) DO UPDATE SET
              canonical_label=excluded.canonical_label,
              confidence=excluded.confidence,
              approved=excluded.approved,
              source_model=excluded.source_model,
              updated_at=excluded.updated_at
            """,
            (alias, tag, 0.7, model, now),
        )


def apply_alias_merge(conn, alias_map=None):
    alias_map = alias_map or load_approved_alias_map(conn)
    merged = 0
    for alias_label in sorted(alias_map.keys()):
        canonical_label = resolve_canonical(alias_map.get(alias_label), alias_map)
        if not alias_label or not canonical_label or alias_label == canonical_label:
            continue
        alias_row = conn.execute(
            "SELECT id FROM tags WHERE label = ?",
            (alias_label,),
        ).fetchone()
        if not alias_row:
            continue
        canonical_row = conn.execute(
            "SELECT id FROM tags WHERE label = ?",
            (canonical_label,),
        ).fetchone()
        if not canonical_row:
            conn.execute("INSERT OR IGNORE INTO tags(label) VALUES (?)", (canonical_label,))
            canonical_row = conn.execute(
                "SELECT id FROM tags WHERE label = ?",
                (canonical_label,),
            ).fetchone()
        alias_id = int(alias_row[0])
        canonical_id = int(canonical_row[0])
        if alias_id == canonical_id:
            continue
        question_rows = conn.execute(
            "SELECT DISTINCT question_id FROM question_tags WHERE tag_id = ?",
            (alias_id,),
        ).fetchall()
        for (question_id,) in question_rows:
            conn.execute(
                """
                INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                VALUES (?, ?, ?)
                """,
                (question_id, canonical_id, "alias_merge"),
            )
            merged += 1
        conn.execute("DELETE FROM question_tags WHERE tag_id = ?", (alias_id,))
        conn.execute("DELETE FROM tags WHERE id = ?", (alias_id,))

    deep_dive_updated = 0
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
            raw_tags = json.loads(tags_json or "[]")
        except json.JSONDecodeError:
            raw_tags = []
        before = canonicalize_label_list(raw_tags, {})
        after = canonicalize_label_list(raw_tags, alias_map)
        if after == before:
            continue
        conn.execute(
            "UPDATE deep_dive_explanations SET tags_json = ? WHERE serial = ?",
            (json.dumps(after, ensure_ascii=False), serial),
        )
        deep_dive_updated += 1
    return merged, deep_dive_updated


def parse_args():
    parser = argparse.ArgumentParser(description="Build tag descriptions and aliases with Gemini.")
    parser.add_argument("--db", default="output/ahaki.sqlite", help="SQLite path.")
    parser.add_argument("--model", default="gemini-3-flash-preview", help="Gemini model.")
    parser.add_argument("--api-key", default="", help="Override API key directly.")
    parser.add_argument(
        "--api-key-source",
        default="free",
        choices=["auto", "paid", "free", "legacy"],
        help=(
            "API key source. auto=GEMINI_API_KEY_PAID -> GEMINI_API_KEY_FREE -> GEMINI_API_KEY, "
            "paid=GEMINI_API_KEY_PAID, free=GEMINI_API_KEY_FREE (fallback GEMINI_API_KEY), "
            "legacy=GEMINI_API_KEY only."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process first N target tags after filtering (0=all).",
    )
    parser.add_argument("--batch-size", type=int, default=6, help="Tags per API request.")
    parser.add_argument(
        "--max-requests",
        type=int,
        default=0,
        help="Maximum Gemini API requests per run (0=unlimited).",
    )
    parser.add_argument("--sleep-seconds", type=float, default=1.5, help="Sleep between requests.")
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retries per batch when API request fails.",
    )
    parser.add_argument(
        "--retry-sleep-seconds",
        type=float,
        default=15.0,
        help="Base wait seconds before retrying failed API requests.",
    )
    parser.add_argument("--max-output-tokens", type=int, default=4096, help="Gemini maxOutputTokens.")
    parser.add_argument("--overwrite", action="store_true", help="Rebuild existing descriptions.")
    parser.add_argument("--apply-merge", action="store_true", help="Apply approved alias merge to tags/question_tags.")
    parser.add_argument("--dry-run", action="store_true", help="Print target tags only.")
    return parser.parse_args()


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
    # auto
    return (
        os.environ.get("GEMINI_API_KEY_PAID", "")
        or os.environ.get("GEMINI_API_KEY_FREE", "")
        or os.environ.get("GEMINI_API_KEY", ""),
        "auto",
    )


def _retry_wait_seconds(error_text, base_wait):
    text = str(error_text or "")
    match = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", text, flags=re.IGNORECASE)
    if match:
        try:
            return max(float(base_wait), float(match.group(1)) + 0.5)
        except Exception:
            pass
    return float(base_wait)


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    load_env(repo_root / ".env")
    api_key, api_key_label = resolve_api_key(args)
    if not api_key:
        raise SystemExit(
            "API key is not set. Set one of GEMINI_API_KEY_PAID / GEMINI_API_KEY_FREE / GEMINI_API_KEY "
            "or pass --api-key."
        )
    print(f"API key source: {api_key_label}")

    conn = sqlite3.connect(args.db)
    ensure_tables(conn)
    alias_map = load_approved_alias_map(conn)
    all_tags = collect_full_tags(conn, alias_map=alias_map)

    if not args.overwrite:
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT canonical_label FROM tag_descriptions WHERE description != ''"
            ).fetchall()
        }
        targets = [tag for tag in all_tags if tag not in existing]
    else:
        targets = all_tags

    if args.limit > 0:
        targets = targets[: args.limit]
    if args.max_requests > 0:
        targets = targets[: args.max_requests * args.batch_size]

    if args.dry_run:
        print(f"targets={len(targets)}")
        for tag in targets[:50]:
            print(tag)
        conn.close()
        return

    saved = 0
    batch_list = list(iter_chunks(targets, args.batch_size))
    api_error = ""
    for batch_idx, tag_batch in enumerate(batch_list, start=1):
        prompt = build_prompt(tag_batch)
        payload = {}
        error = ""
        for attempt in range(args.max_retries + 1):
            payload, error = call_gemini(api_key, args.model, prompt, args.max_output_tokens)
            if not error:
                break
            if attempt >= args.max_retries:
                break
            wait = _retry_wait_seconds(error, args.retry_sleep_seconds)
            print(
                f"[{batch_idx}/{len(batch_list)}] batch ERROR (retry {attempt + 1}/{args.max_retries}): {error}"
            )
            print(f"  waiting {wait:.1f}s before retry...")
            if wait > 0:
                time.sleep(wait)
        if error:
            api_error = error
            print(f"[{batch_idx}/{len(batch_list)}] batch ERROR: {error}")
            break
        text = extract_text(payload)
        parsed_map = parse_batch_result(text, tag_batch)
        batch_saved = 0
        for tag in tag_batch:
            parsed = parsed_map.get(tag, {"description": "", "aliases": []})
            description = parsed["description"]
            aliases = parsed["aliases"]
            if not description:
                continue
            save_result(conn, tag, description, aliases, args.model)
            batch_saved += 1
            saved += 1
        conn.commit()
        print(
            f"[{batch_idx}/{len(batch_list)}] saved {batch_saved}/{len(tag_batch)} tags"
        )
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    if api_error:
        conn.commit()
        conn.close()
        raise SystemExit(f"APIエラーのため停止しました: {api_error}")

    merged = 0
    deep_dive_tags_updated = 0
    if args.apply_merge:
        merged, deep_dive_tags_updated = apply_alias_merge(conn)
        conn.commit()

    conn.close()
    print(
        f"done: saved={saved}, targets={len(targets)}, merged={merged}, deep_dive_tags_updated={deep_dive_tags_updated}"
    )


if __name__ == "__main__":
    main()
