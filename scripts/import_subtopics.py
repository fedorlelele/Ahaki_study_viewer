import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import subtopics from JSONL into SQLite."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--infile",
        default="output/subtopics_batch.jsonl",
        help="Input JSONL path.",
    )
    parser.add_argument(
        "--source",
        default="llm",
        help="Source label stored in subtopics (optional).",
    )
    return parser.parse_args()


def normalize_text(text):
    return " ".join(str(text).split()).strip()


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
            subtopics = record.get("subtopics", [])

            if not serial or not subtopics:
                continue

            row = cursor.execute(
                "SELECT id FROM questions WHERE serial = ?",
                (serial,),
            ).fetchone()
            if not row:
                continue
            question_id = row[0]

            for item in subtopics:
                name = normalize_text(item)
                if not name:
                    continue

                cursor.execute(
                    "INSERT OR IGNORE INTO subtopics(name) VALUES (?)",
                    (name,),
                )
                subtopic_id = cursor.execute(
                    "SELECT id FROM subtopics WHERE name = ?",
                    (name,),
                ).fetchone()[0]

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO question_subtopics(question_id, subtopic_id)
                    VALUES (?, ?)
                    """,
                    (question_id, subtopic_id),
                )
                inserted += 1

    conn.commit()
    conn.close()
    print(f"Imported {inserted} subtopics into {db_path}")


if __name__ == "__main__":
    main()
