import argparse
import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path


DEFAULT_DB = Path("output/ahaki.sqlite")
DEFAULT_CSV = Path("docs/output/clinical_general_final_smallitem_classification.csv")
DEFAULT_CATALOG = Path("config/subtopics_catalog.json")
SUBJECT_NAME = "臨床医学総論"

CATALOG_ORDER = [
    "診察の概要",
    "医療面接",
    "視診",
    "打診",
    "聴診",
    "触診",
    "測定法",
    "神経系の診察",
    "運動機能・整形外科的検査",
    "一般検査",
    "生化学的検査",
    "生理・画像検査",
    "治療法",
    "患者心理",
    "保留：循環器症候",
    "保留：呼吸器症候",
    "保留：消化器症候",
    "保留：泌尿生殖器・婦人科症候",
    "保留：血液・出血症候",
    "保留：感覚器症候",
    "保留：頭痛・睡眠・疲労",
    "保留：精神・心理・発達",
    "保留：運動器疼痛・外傷",
    "保留：感染・皮膚・免疫",
    "保留：治療各論・副作用",
    "保留：全身症候・病態",
    "保留：神経・筋の解剖/支配",
    "保留：神経症候・高次機能",
    "保留：理学所見・診察一般",
    "保留：内分泌・代謝・全身所見",
    "保留：病態生理・解剖",
    "保留：一般診断・評価知識",
    "保留：検査選択・検査運用",
    "保留：処置・対応手順",
    "保留：組合せ・鑑別",
    "保留：疫学・背景",
    "保留：各論疾患知識",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Apply final clinical general smallitems to SQLite and catalog."
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to SQLite database.")
    parser.add_argument(
        "--csv",
        default=str(DEFAULT_CSV),
        help="CSV containing serial and final_smallitem columns.",
    )
    parser.add_argument(
        "--catalog",
        default=str(DEFAULT_CATALOG),
        help="Path to subtopics catalog JSON.",
    )
    parser.add_argument(
        "--skip-catalog",
        action="store_true",
        help="Do not update config/subtopics_catalog.json.",
    )
    return parser.parse_args()


def load_assignments(csv_path: Path) -> dict[str, str]:
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        raise ValueError(f"CSV is empty: {csv_path}")

    assignments: dict[str, str] = {}
    for row in rows:
        serial = str(row.get("serial", "")).strip()
        smallitem = str(row.get("final_smallitem", "")).strip()
        if not serial or not smallitem:
            raise ValueError(f"Missing serial/final_smallitem in row: {row}")
        if serial in assignments and assignments[serial] != smallitem:
            raise ValueError(f"Duplicate serial with different smallitems: {serial}")
        assignments[serial] = smallitem
    return assignments


def fetch_subject_questions(conn: sqlite3.Connection) -> tuple[int, dict[str, int]]:
    row = conn.execute(
        "SELECT id FROM subjects WHERE name = ?",
        (SUBJECT_NAME,),
    ).fetchone()
    if not row:
        raise ValueError(f"Subject not found: {SUBJECT_NAME}")
    subject_id = int(row[0])

    question_rows = conn.execute(
        """
        SELECT serial, id
        FROM questions
        WHERE subject_id = ?
        ORDER BY serial
        """,
        (subject_id,),
    ).fetchall()
    question_map = {str(serial): int(question_id) for serial, question_id in question_rows}
    return subject_id, question_map


def sync_catalog(catalog_path: Path, active_smallitems: list[str]):
    data = json.loads(catalog_path.read_text(encoding="utf-8"))
    ordered = [name for name in CATALOG_ORDER if name in active_smallitems]
    unknown = [name for name in active_smallitems if name not in CATALOG_ORDER]
    ordered.extend(sorted(unknown))
    data[SUBJECT_NAME] = ordered
    catalog_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main():
    args = parse_args()
    db_path = Path(args.db)
    csv_path = Path(args.csv)
    catalog_path = Path(args.catalog)

    assignments = load_assignments(csv_path)
    assignment_counts = Counter(assignments.values())

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        subject_id, question_map = fetch_subject_questions(conn)
        if set(assignments) != set(question_map):
            missing_in_csv = sorted(set(question_map) - set(assignments))
            missing_in_db = sorted(set(assignments) - set(question_map))
            raise ValueError(
                "Serial mismatch between CSV and DB "
                f"(missing_in_csv={missing_in_csv[:10]}, missing_in_db={missing_in_db[:10]})"
            )

        before_links = conn.execute(
            """
            SELECT COUNT(*)
            FROM question_subtopics qs
            JOIN questions q ON q.id = qs.question_id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()[0]

        conn.execute("BEGIN")
        for smallitem in sorted(assignment_counts):
            conn.execute(
                "INSERT OR IGNORE INTO subtopics(name) VALUES (?)",
                (smallitem,),
            )

        subtopic_rows = conn.execute(
            "SELECT id, name FROM subtopics WHERE name IN ({})".format(
                ",".join("?" for _ in assignment_counts)
            ),
            tuple(sorted(assignment_counts)),
        ).fetchall()
        subtopic_ids = {str(name): int(subtopic_id) for subtopic_id, name in subtopic_rows}

        conn.execute(
            """
            DELETE FROM question_subtopics
            WHERE question_id IN (
                SELECT id FROM questions WHERE subject_id = ?
            )
            """,
            (subject_id,),
        )

        conn.executemany(
            """
            INSERT INTO question_subtopics(question_id, subtopic_id)
            VALUES (?, ?)
            """,
            [
                (question_map[serial], subtopic_ids[smallitem])
                for serial, smallitem in assignments.items()
            ],
        )
        conn.commit()

        if not args.skip_catalog:
            sync_catalog(catalog_path, list(assignment_counts))

        after_distinct_questions, after_links = conn.execute(
            """
            SELECT COUNT(DISTINCT q.id), COUNT(*)
            FROM question_subtopics qs
            JOIN questions q ON q.id = qs.question_id
            WHERE q.subject_id = ?
            """,
            (subject_id,),
        ).fetchone()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"Updated subject: {SUBJECT_NAME}")
    print(f"Questions: {len(assignments)}")
    print(f"Links before: {before_links}")
    print(f"Links after: {after_links}")
    print(f"Questions with subtopics after: {after_distinct_questions}")
    print(f"Unique final smallitems: {len(assignment_counts)}")
    for name, count in assignment_counts.most_common():
        print(f"{name}\t{count}")


if __name__ == "__main__":
    main()
