import argparse
import json
import sqlite3
from pathlib import Path

try:
    from scripts.explanation_metadata import (
        build_explanation_source,
        derive_explanation_metadata,
        ensure_explanation_metadata_schema,
        insert_explanation,
    )
except ModuleNotFoundError:
    from explanation_metadata import (
        build_explanation_source,
        derive_explanation_metadata,
        ensure_explanation_metadata_schema,
        insert_explanation,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import explanations from JSONL into SQLite."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--infile",
        default="output/explanations_batch.jsonl",
        help="Input JSONL path.",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=1,
        help="Explanation version number.",
    )
    parser.add_argument(
        "--model-name",
        default="",
        help="Default LLM model name for imported explanations.",
    )
    parser.add_argument(
        "--review-status",
        default="ai",
        choices=["ai", "teacher_approved", "teacher_edited"],
        help="Default review status for imported explanations.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db)
    in_path = Path(args.infile)

    conn = sqlite3.connect(db_path)
    ensure_explanation_metadata_schema(conn)
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
            meta = derive_explanation_metadata(
                record.get("source"),
                record.get("model_name") or args.model_name,
                record.get("review_status") or args.review_status,
            )
            source = record.get("source") or build_explanation_source(
                meta["model_name"],
                meta["review_status"],
            )

            if not serial or not explanation:
                continue

            row = cursor.execute(
                "SELECT id FROM questions WHERE serial = ?",
                (serial,),
            ).fetchone()
            if not row:
                continue
            question_id = row[0]

            insert_explanation(
                cursor,
                question_id,
                explanation,
                args.version,
                source,
                meta["model_name"],
                meta["review_status"],
            )
            inserted += 1

    conn.commit()
    conn.close()
    print(f"Imported {inserted} explanations into {db_path}")


if __name__ == "__main__":
    main()
