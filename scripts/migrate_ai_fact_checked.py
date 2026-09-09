"""One-time relabeling of pre-existing AI-checked normal explanations.

Teacher statuses keep their meaning after this migration. Existing text,
versions and model names are not changed; original source values are backed up.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

try:
    from scripts.backup_database import backup_database
    from scripts.explanation_metadata import derive_explanation_metadata, build_explanation_source
except ModuleNotFoundError:
    from backup_database import backup_database
    from explanation_metadata import derive_explanation_metadata, build_explanation_source

MIGRATION_ID = "20260909_existing_ai_fact_checks"


def migrate(conn):
    """Caller owns transaction; a durable marker prevents relabeling later teachers."""
    conn.execute("CREATE TABLE IF NOT EXISTS metadata_migrations (id TEXT PRIMARY KEY, applied_at TEXT NOT NULL, row_count INTEGER NOT NULL)")
    if conn.execute("SELECT 1 FROM metadata_migrations WHERE id=?", (MIGRATION_ID,)).fetchone():
        return 0
    rows = conn.execute("SELECT id, source, model_name, review_status FROM explanations").fetchall()
    changed = 0
    for row_id, source, model, status in rows:
        meta = derive_explanation_metadata(source, model, status)
        if meta["review_status"] not in {"teacher_approved", "teacher_edited"}:
            continue
        new_source = build_explanation_source(meta["model_name"], "ai_fact_checked")
        conn.execute("UPDATE explanations SET source=?, review_status='ai_fact_checked' WHERE id=?", (new_source, row_id))
        changed += 1
    conn.execute("INSERT INTO metadata_migrations VALUES (?,?,?)", (MIGRATION_ID, datetime.now(timezone.utc).isoformat(), changed))
    return changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("output/ahaki.sqlite"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", type=Path)
    args = parser.parse_args()
    if args.apply:
        if not args.backup:
            parser.error("--apply requires --backup")
        backup_database(args.db, args.backup)
    conn = sqlite3.connect(args.db.resolve().as_uri() + "?mode=rw", uri=True)
    try:
        conn.execute("BEGIN IMMEDIATE")
        changed = migrate(conn)
        if args.apply:
            conn.commit()
        else:
            conn.rollback()
        print(json.dumps({"applied": args.apply, "changed": changed}))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
