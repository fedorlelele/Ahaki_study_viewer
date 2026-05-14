#!/usr/bin/env python3
"""Export all-time daily answer counts from Supabase answers."""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
OUT_ROOT = ROOT / "output"
JST = timezone(timedelta(hours=9))
PAGE_SIZE = 1000


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def parse_iso(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fetch_answer_created_at_rows(url: str, service_key: str) -> list[dict]:
    endpoint = url.rstrip("/") + "/rest/v1/answers"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Accept": "application/json",
    }
    rows: list[dict] = []
    offset = 0
    while True:
        params = urlencode(
            {
                "select": "created_at",
                "order": "created_at.asc",
                "limit": str(PAGE_SIZE),
                "offset": str(offset),
            }
        )
        request = Request(f"{endpoint}?{params}", headers=headers)
        try:
            with urlopen(request, timeout=30) as response:
                batch = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase HTTP {exc.code}: {body}") from exc
        except URLError as exc:
            raise RuntimeError(f"Supabase request failed: {exc}") from exc
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected Supabase response: {batch!r}")
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def build_daily(rows: list[dict]) -> tuple[list[dict], dict]:
    counts: Counter[str] = Counter()
    valid_timestamps: list[datetime] = []
    skipped = 0
    for row in rows:
        created_at = row.get("created_at")
        if not created_at:
            skipped += 1
            continue
        try:
            dt = parse_iso(str(created_at)).astimezone(JST)
        except ValueError:
            skipped += 1
            continue
        valid_timestamps.append(dt)
        counts[dt.date().isoformat()] += 1

    if not valid_timestamps:
        return [], {
            "total_answers": 0,
            "skipped_rows": skipped,
            "first_answer_at_jst": None,
            "last_answer_at_jst": None,
        }

    first = min(valid_timestamps)
    last = max(valid_timestamps)
    today = datetime.now(JST).date()
    end_date = max(last.date(), today)
    current = first.date()
    cumulative = 0
    daily: list[dict] = []
    while current <= end_date:
        date_key = current.isoformat()
        count = counts.get(date_key, 0)
        cumulative += count
        daily.append(
            {
                "date": date_key,
                "answer_count": count,
                "cumulative_answer_count": cumulative,
            }
        )
        current += timedelta(days=1)

    return daily, {
        "total_answers": len(valid_timestamps),
        "skipped_rows": skipped,
        "first_answer_at_jst": first.isoformat(timespec="seconds"),
        "last_answer_at_jst": last.isoformat(timespec="seconds"),
    }


def write_outputs(daily: list[dict], metadata: dict) -> Path:
    stamp = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    out_dir = OUT_ROOT / f"answer_daily_trend_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=False)

    csv_path = out_dir / "answer_daily_trend.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["date", "answer_count", "cumulative_answer_count"]
        )
        writer.writeheader()
        writer.writerows(daily)

    payload = {"metadata": metadata, "daily": daily}
    (out_dir / "answer_daily_trend.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    readme = [
        "# 生徒回答数 日次推移",
        "",
        f"- 集計日時: {metadata['exported_at_jst']}",
        "- 集計タイムゾーン: Asia/Tokyo (JST)",
        "- 対象: Supabase `answers` テーブル全体",
        "- 粒度: サイト全体の1日ごとの回答数",
        f"- 取得行数: {metadata['raw_rows_fetched']}",
        f"- 日付別に集計できた回答数: {metadata['total_answers']}",
        f"- 日付不明で日次推移から除外した行数: {metadata['skipped_rows']}",
        f"- 集計開始: {metadata['first_answer_at_jst'] or 'なし'}",
        f"- 最新回答: {metadata['last_answer_at_jst'] or 'なし'}",
        f"- 出力日数: {len(daily)}",
        "",
        "## ファイル",
        "",
        "- `answer_daily_trend.csv`: 日次集計データ",
        "- `answer_daily_trend.json`: メタデータ付きJSON",
        "",
    ]
    (out_dir / "README.md").write_text("\n".join(readme), encoding="utf-8")
    return out_dir


def main() -> int:
    load_dotenv(ENV_PATH)
    url = os.environ.get("SUPABASE_URL", "").strip()
    service_key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not service_key:
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY が見つかりません。", file=sys.stderr)
        return 2

    started = time.time()
    rows = fetch_answer_created_at_rows(url, service_key)
    daily, metadata = build_daily(rows)
    metadata.update(
        {
            "exported_at_jst": datetime.now(JST).isoformat(timespec="seconds"),
            "source_table": "answers",
            "timezone": "Asia/Tokyo",
            "raw_rows_fetched": len(rows),
            "duration_seconds": round(time.time() - started, 3),
        }
    )
    out_dir = write_outputs(daily, metadata)
    print(str(out_dir))
    print(json.dumps(metadata, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
