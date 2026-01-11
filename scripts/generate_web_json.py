import argparse
import json
import re
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate web-friendly JSON from SQLite."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--out",
        default="output/web/questions.json",
        help="Output JSON path.",
    )
    parser.add_argument(
        "--index-dir",
        default="output/web/index",
        help="Output directory for index JSON files.",
    )
    return parser.parse_args()


def load_questions(conn):
    rows = conn.execute(
        """
        SELECT
            q.id,
            q.serial,
            q.exam_type,
            q.exam_session,
            s.name AS subject,
            q.case_text,
            q.stem,
            q.choices_json,
            q.answer_index
        FROM questions q
        LEFT JOIN subjects s ON q.subject_id = s.id
        ORDER BY q.serial
        """
    ).fetchall()
    columns = [
        "id",
        "serial",
        "exam_type",
        "exam_session",
        "subject",
        "case_text",
        "stem",
        "choices_json",
        "answer_index",
    ]
    return [dict(zip(columns, row)) for row in rows]


def load_explanations(conn):
    rows = conn.execute(
        """
        SELECT question_id, body, version, source
        FROM explanations
        ORDER BY id
        """
    ).fetchall()
    data = {}
    for question_id, body, version, source in rows:
        data.setdefault(question_id, []).append(
            {"body": body, "version": version, "source": source}
        )
    return data


def load_tags(conn):
    rows = conn.execute(
        """
        SELECT qt.question_id, t.label
        FROM question_tags qt
        JOIN tags t ON t.id = qt.tag_id
        ORDER BY t.label
        """
    ).fetchall()
    data = {}
    for question_id, label in rows:
        data.setdefault(question_id, []).append(label)
    return data


def load_subtopics(conn):
    rows = conn.execute(
        """
        SELECT qs.question_id, st.name
        FROM question_subtopics qs
        JOIN subtopics st ON st.id = qs.subtopic_id
        ORDER BY st.name
        """
    ).fetchall()
    data = {}
    for question_id, name in rows:
        data.setdefault(question_id, []).append(name)
    return data


def load_explanation_update_log(conn):
    try:
        rows = conn.execute(
            """
            SELECT date, count
            FROM explanation_update_log
            ORDER BY date DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [{"date": row[0], "count": row[1]} for row in rows]


def load_update_notes(path):
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    notes = []
    for item in data:
        if not isinstance(item, dict):
            continue
        date = str(item.get("date") or "").strip()
        text = str(item.get("text") or "").strip()
        if date and text:
            notes.append({"date": date, "text": text})
    return notes


def parse_date_parts(value):
    parts = re.findall(r"\d+", value)
    if len(parts) >= 3:
        year = parts[0].zfill(4)
        month = parts[1].zfill(2)
        day = parts[2].zfill(2)
        return year, month, day
    return None


def normalize_date_key(value):
    parts = parse_date_parts(value)
    if parts:
        return "".join(parts)
    digits = "".join(re.findall(r"\d", value))
    if len(digits) >= 8:
        return digits[:8]
    return digits.ljust(8, "0")


def format_date_display(value):
    parts = parse_date_parts(value)
    if parts:
        return f"{parts[0]}/{parts[1]}/{parts[2]}"
    return value


def main():
    args = parse_args()
    db_path = Path(args.db)
    out_path = Path(args.out)
    index_dir = Path(args.index_dir)

    conn = sqlite3.connect(db_path)
    questions = load_questions(conn)
    explanations = load_explanations(conn)
    tags = load_tags(conn)
    subtopics = load_subtopics(conn)
    update_log = load_explanation_update_log(conn)
    conn.close()

    output = []
    for q in questions:
        qid = q["id"]
        exp_list = explanations.get(qid, [])
        exp_list_sorted = sorted(exp_list, key=lambda x: x.get("version", 0))
        latest_exp = exp_list_sorted[-1]["body"] if exp_list_sorted else None
        latest_source = exp_list_sorted[-1].get("source") if exp_list_sorted else None
        record = {
            "serial": q["serial"],
            "exam_type": q["exam_type"],
            "exam_session": q["exam_session"],
            "subject": q["subject"],
            "case_text": q["case_text"],
            "stem": q["stem"],
            "choices": json.loads(q["choices_json"]),
            "answer_index": q["answer_index"],
            "explanation_latest": latest_exp,
            "explanation_latest_source": latest_source,
            "explanations": exp_list_sorted,
            "tags": tags.get(qid, []),
            "subtopics": subtopics.get(qid, []),
        }
        output.append(record)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Web JSON saved: {out_path}")

    update_notes = load_update_notes(Path("config/update_notes.json"))
    update_entries = []
    for item in update_log:
        date = item.get("date", "")
        count = item.get("count", 0)
        if not date or count <= 0:
            continue
        update_entries.append(
            {
                "date": format_date_display(date),
                "text": f"解説を{count}件追加しました。",
                "kind": "explanation",
            }
        )
    for note in update_notes:
        update_entries.append(
            {
                "date": format_date_display(note["date"]),
                "text": note["text"],
                "kind": "note",
            }
        )
    update_entries.sort(key=lambda x: normalize_date_key(x["date"]), reverse=True)
    (out_path.parent / "update_log.json").write_text(
        json.dumps(update_entries, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    index_dir.mkdir(parents=True, exist_ok=True)
    index_by_subject = {}
    index_by_tag = {}
    index_by_subtopic = {}

    for record in output:
        serial = record["serial"]
        subject = record["subject"]
        index_by_subject.setdefault(subject, []).append(serial)

        for tag in record["tags"]:
            index_by_tag.setdefault(tag, []).append(serial)

        for subtopic in record["subtopics"]:
            index_by_subtopic.setdefault(subtopic, []).append(serial)

    (index_dir / "index_by_subject.json").write_text(
        json.dumps(index_by_subject, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (index_dir / "index_by_tag.json").write_text(
        json.dumps(index_by_tag, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (index_dir / "index_by_subtopic.json").write_text(
        json.dumps(index_by_subtopic, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Index JSON saved: {index_dir}")


if __name__ == "__main__":
    main()
