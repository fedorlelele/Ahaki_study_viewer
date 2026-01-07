import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate progress report for explanations/tags/subtopics."
    )
    parser.add_argument(
        "--db",
        default="output/hikkei.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--out",
        default="output/progress_report.json",
        help="Output JSON path.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db)
    out_path = Path(args.out)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    total_questions = cursor.execute("SELECT COUNT(*) FROM questions").fetchone()[0]

    explained = cursor.execute(
        "SELECT COUNT(DISTINCT question_id) FROM explanations"
    ).fetchone()[0]
    tagged = cursor.execute(
        "SELECT COUNT(DISTINCT question_id) FROM question_tags"
    ).fetchone()[0]
    subtopic_assigned = cursor.execute(
        "SELECT COUNT(DISTINCT question_id) FROM question_subtopics"
    ).fetchone()[0]

    subjects = cursor.execute("SELECT id, name FROM subjects ORDER BY name").fetchall()
    subject_rows = []
    for subject_id, name in subjects:
        subject_total = cursor.execute(
            "SELECT COUNT(*) FROM questions WHERE subject_id = ?",
            (subject_id,),
        ).fetchone()[0]
        subject_explained = cursor.execute(
            """
            SELECT COUNT(DISTINCT q.id)
            FROM questions q
            JOIN explanations e ON e.question_id = q.id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()[0]
        subject_tagged = cursor.execute(
            """
            SELECT COUNT(DISTINCT q.id)
            FROM questions q
            JOIN question_tags qt ON qt.question_id = q.id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()[0]
        subject_subtopics = cursor.execute(
            """
            SELECT COUNT(DISTINCT q.id)
            FROM questions q
            JOIN question_subtopics qs ON qs.question_id = q.id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()[0]

        subject_rows.append(
            {
                "subject": name,
                "total_questions": subject_total,
                "explained": subject_explained,
                "tagged": subject_tagged,
                "subtopic_assigned": subject_subtopics,
            }
        )

    report = {
        "total_questions": total_questions,
        "explained": explained,
        "tagged": tagged,
        "subtopic_assigned": subtopic_assigned,
        "by_subject": subject_rows,
    }

    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Report saved: {out_path}")


if __name__ == "__main__":
    main()
