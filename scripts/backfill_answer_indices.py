import argparse
import json
import re
import sqlite3
from pathlib import Path

FULLWIDTH_TO_ASCII = str.maketrans("０１２３４５６７８９", "0123456789")


def normalize_digits(value):
    return value.translate(FULLWIDTH_TO_ASCII)


def parse_answer_text(text):
    if not text:
        return [], False
    normalized = normalize_digits(text)
    if "なし" in normalized:
        return [], True
    if "すべて" in normalized:
        return [1, 2, 3, 4], False
    digits = re.findall(r"[1-4]", normalized)
    indices = sorted({int(d) for d in digits})
    return indices, False


def ensure_column(conn, name, ddl):
    cols = [row[1] for row in conn.execute("PRAGMA table_info(questions)").fetchall()]
    if name in cols:
        return
    conn.execute(ddl)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill answer_indices_json and answer_none from answer_text."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)

    ensure_column(
        conn,
        "answer_indices_json",
        "ALTER TABLE questions ADD COLUMN answer_indices_json TEXT",
    )
    ensure_column(
        conn,
        "answer_none",
        "ALTER TABLE questions ADD COLUMN answer_none INTEGER DEFAULT 0",
    )

    rows = conn.execute(
        "SELECT id, answer_text FROM questions"
    ).fetchall()

    updated = 0
    for qid, answer_text in rows:
        indices, answer_none = parse_answer_text(answer_text or "")
        conn.execute(
            """
            UPDATE questions
            SET answer_indices_json = ?, answer_none = ?
            WHERE id = ?
            """,
            (json.dumps(indices, ensure_ascii=False), 1 if answer_none else 0, qid),
        )
        updated += 1

    conn.commit()
    conn.close()
    print(f"Updated {updated} questions in {db_path}")


if __name__ == "__main__":
    main()
