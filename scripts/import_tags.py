import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import tags from JSONL into SQLite."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--infile",
        default="output/tags_batch.jsonl",
        help="Input JSONL path.",
    )
    parser.add_argument(
        "--source",
        default="llm",
        help="Source label stored in question_tags.",
    )
    return parser.parse_args()


def normalize_tag(tag):
    return " ".join(str(tag).split()).strip()


def main():
    args = parse_args()
    db_path = Path(args.db)
    in_path = Path(args.infile)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted = 0
    with in_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            serial = record.get("serial")
            tags = record.get("tags", [])

            if not serial or not tags:
                continue

            row = cursor.execute(
                "SELECT id FROM questions WHERE serial = ?",
                (serial,),
            ).fetchone()
            if not row:
                continue
            question_id = row[0]

            for tag in tags:
                tag_label = normalize_tag(tag)
                if not tag_label:
                    continue

                cursor.execute(
                    "INSERT OR IGNORE INTO tags(label) VALUES (?)",
                    (tag_label,),
                )
                tag_id = cursor.execute(
                    "SELECT id FROM tags WHERE label = ?",
                    (tag_label,),
                ).fetchone()[0]

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                    VALUES (?, ?, ?)
                    """,
                    (question_id, tag_id, args.source),
                )
                inserted += 1

    conn.commit()
    conn.close()
    print(f"Imported {inserted} tags into {db_path}")


if __name__ == "__main__":
    main()
