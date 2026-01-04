import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import explanations from JSONL into SQLite."
    )
    parser.add_argument(
        "--db",
        default="kokushitxt/output/hikkei.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--infile",
        default="kokushitxt/output/explanations_batch.jsonl",
        help="Input JSONL path.",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=1,
        help="Explanation version number.",
    )
    return parser.parse_args()


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
            explanation = record.get("explanation", "").strip()
            source = record.get("source") or "llm"

            if not serial or not explanation:
                continue

            row = cursor.execute(
                "SELECT id FROM questions WHERE serial = ?",
                (serial,),
            ).fetchone()
            if not row:
                continue
            question_id = row[0]

            cursor.execute(
                """
                INSERT INTO explanations(question_id, body, version, source)
                VALUES (?, ?, ?, ?)
                """,
                (question_id, explanation, args.version, source),
            )
            inserted += 1

    conn.commit()
    conn.close()
    print(f"Imported {inserted} explanations into {db_path}")


if __name__ == "__main__":
    main()
