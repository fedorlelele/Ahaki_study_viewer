#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys
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


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Sync local SQLite deep_dive_explanations to Supabase "
            "(public.deep_dive_explanations) by upsert."
        )
    )
    parser.add_argument("--db", default="output/ahaki.sqlite", help="Path to local SQLite DB.")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per upsert request.")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to sync (0 = all).")
    parser.add_argument(
        "--since",
        default="",
        help="Only sync rows where updated_at >= this timestamp (ISO8601).",
    )
    parser.add_argument(
        "--serials",
        default="",
        help="Comma-separated serial list to sync only specific questions.",
    )
    parser.add_argument(
        "--created-by",
        default="",
        help="Override created_by UUID when local value is empty (optional).",
    )
    parser.add_argument(
        "--set-updated-now",
        action="store_true",
        help="Set updated_at to current UTC timestamp for all uploaded rows.",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Include rows with empty explanation (default: skip).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only; no HTTP request.")
    return parser.parse_args()


def get_supabase_config():
    url = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        os.environ.get("SUPABASE_SERVICE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    ).strip()
    return url, key


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def parse_serials(text):
    values = []
    for raw in str(text or "").split(","):
        serial = raw.strip()
        if serial:
            values.append(serial)
    return values


def normalize_uuid_or_none(value):
    text = str(value or "").strip()
    if not text:
        return None
    import re

    if re.fullmatch(
        r"[0-9a-fA-F]{8}-"
        r"[0-9a-fA-F]{4}-"
        r"[1-5][0-9a-fA-F]{3}-"
        r"[89abAB][0-9a-fA-F]{3}-"
        r"[0-9a-fA-F]{12}",
        text,
    ):
        return text.lower()
    return None


def parse_tags_json(value):
    if not value:
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out = []
    for item in data:
        label = str(item or "").strip()
        if label and label not in out:
            out.append(label)
    return out


def build_select_query(args, has_created_by):
    select_cols = [
        "serial",
        "explanation",
        "tags_json",
        "updated_at",
    ]
    if has_created_by:
        select_cols.append("created_by")

    where = []
    params = []
    if args.since:
        where.append("updated_at >= ?")
        params.append(args.since)
    serials = parse_serials(args.serials)
    if serials:
        placeholders = ",".join("?" for _ in serials)
        where.append(f"serial IN ({placeholders})")
        params.extend(serials)
    if not args.include_empty:
        where.append("TRIM(COALESCE(explanation,'')) <> ''")

    sql = f"SELECT {', '.join(select_cols)} FROM deep_dive_explanations"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY updated_at ASC, serial ASC"
    if args.limit > 0:
        sql += " LIMIT ?"
        params.append(args.limit)
    return sql, params


def fetch_local_rows(args):
    conn = sqlite3.connect(args.db)
    try:
        if not table_exists(conn, "deep_dive_explanations"):
            return []
        columns = {row[1] for row in conn.execute("PRAGMA table_info(deep_dive_explanations)")}
        has_created_by = "created_by" in columns
        query, params = build_select_query(args, has_created_by)
        rows = conn.execute(query, params).fetchall()
        payload_rows = []
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for row in rows:
            serial = str(row[0] or "").strip()
            explanation = str(row[1] or "")
            tags = parse_tags_json(row[2])
            updated_at = str(row[3] or "").strip() or now_utc
            created_by = (
                normalize_uuid_or_none(row[4]) if has_created_by and len(row) > 4 else None
            )
            if not created_by:
                created_by = normalize_uuid_or_none(args.created_by)
            if args.set_updated_now:
                updated_at = now_utc
            if not serial:
                continue
            payload_rows.append(
                {
                    "serial": serial,
                    "explanation": explanation,
                    "tags": tags,
                    "updated_at": updated_at,
                    "created_by": created_by,
                }
            )
        return payload_rows
    finally:
        conn.close()


def post_upsert(url, key, batch):
    endpoint = f"{url}/rest/v1/deep_dive_explanations?on_conflict=serial"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    data = json.dumps(batch, ensure_ascii=False).encode("utf-8")
    req = request.Request(endpoint, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=60) as resp:
            resp.read()
        return "", True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        return f"HTTP {exc.code}: {detail}", False
    except URLError as exc:
        return f"URL error: {exc.reason}", False
    except Exception as exc:
        return f"{exc.__class__.__name__}: {exc}", False


def chunked(items, size):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    load_env(repo_root / ".env")

    url, key = get_supabase_config()
    if not url or not key:
        print("SUPABASE_URL と SUPABASE_SERVICE_KEY を設定してください。", file=sys.stderr)
        return 1

    rows = fetch_local_rows(args)
    if not rows:
        print("同期対象はありません。")
        return 0

    print(f"targets={len(rows)} batch_size={args.batch_size}")
    if args.dry_run:
        preview = [row["serial"] for row in rows[:10]]
        print("dry-run mode (no upload)")
        print(f"sample serials: {preview}")
        return 0

    sent = 0
    total_batches = (len(rows) + args.batch_size - 1) // args.batch_size
    for batch_index, batch in enumerate(chunked(rows, args.batch_size), start=1):
        message, ok = post_upsert(url, key, batch)
        if not ok:
            print(
                f"[{batch_index}/{total_batches}] failed: {message}",
                file=sys.stderr,
            )
            print(f"partial success rows={sent}", file=sys.stderr)
            return 1
        sent += len(batch)
        print(f"[{batch_index}/{total_batches}] uploaded rows={len(batch)} total={sent}")

    print(f"done: uploaded={sent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
