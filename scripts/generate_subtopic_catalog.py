import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a subtopic catalog template per subject."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--out",
        default="config/subtopics_catalog.json",
        help="Output JSON path.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db)
    out_path = Path(args.out)

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT name FROM subjects ORDER BY name").fetchall()
    conn.close()

    catalog = {row[0]: [] for row in rows}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Catalog template saved: {out_path}")


if __name__ == "__main__":
    main()
