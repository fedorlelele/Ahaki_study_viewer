"""Deep-dive metadata uses the same review statuses as normal explanations."""
import argparse
import json
from pathlib import Path
import sqlite3

try:
    from scripts.backup_database import backup_database
except ModuleNotFoundError:
    from backup_database import backup_database

LEGACY_MODEL_NAME = "Gemini 3 Flash"


def ensure_deep_dive_metadata_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS deep_dive_explanations (
        serial TEXT PRIMARY KEY, explanation TEXT, tags_json TEXT,
        updated_at TEXT, created_by TEXT)""")
    columns = {row[1] for row in conn.execute("PRAGMA table_info(deep_dive_explanations)")}
    for name, definition in {"model_name": "TEXT", "review_status": "TEXT NOT NULL DEFAULT 'ai'"}.items():
        if name not in columns:
            conn.execute(f"ALTER TABLE deep_dive_explanations ADD COLUMN {name} {definition}")


def metadata_select_columns(columns):
    """Allow read-only consumers to read legacy databases before migration."""
    return ", ".join(name if name in columns else f"NULL AS {name}" for name in ("model_name", "review_status"))


def migrate(conn):
    ensure_deep_dive_metadata_schema(conn)
    changed = conn.execute(
        "UPDATE deep_dive_explanations SET model_name=? WHERE trim(coalesce(model_name,''))=''",
        (LEGACY_MODEL_NAME,),
    ).rowcount
    conn.execute("UPDATE deep_dive_explanations SET review_status='ai' WHERE trim(coalesce(review_status,''))=''")
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
    with sqlite3.connect(args.db.resolve().as_uri() + "?mode=rw", uri=True) as conn:
        conn.execute("BEGIN IMMEDIATE")
        changed = migrate(conn)
        if args.apply:
            conn.commit()
        else:
            conn.rollback()
    print(json.dumps({"applied": args.apply, "models_backfilled": changed}))


if __name__ == "__main__":
    main()
