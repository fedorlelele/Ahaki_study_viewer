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


def load_question_columns(conn):
    rows = conn.execute("PRAGMA table_info(questions)").fetchall()
    return {row[1] for row in rows}


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
    columns = load_question_columns(conn)
    extra_cols = []
    if "answer_text" in columns:
        extra_cols.append("q.answer_text")
    if "answer_indices_json" in columns:
        extra_cols.append("q.answer_indices_json")
    if "answer_none" in columns:
        extra_cols.append("q.answer_none")
    rows = conn.execute(
        (
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
            {extra}
        FROM questions q
        LEFT JOIN subjects s ON q.subject_id = s.id
        ORDER BY q.serial
        """
        ).format(extra=(", " + ", ".join(extra_cols)) if extra_cols else "")
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
    columns.extend([col.replace("q.", "") for col in extra_cols])
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


def resolve_answer_meta(record):
    indices = []
    answer_none = False
    raw_json = record.get("answer_indices_json")
    if raw_json:
        try:
            indices = json.loads(raw_json)
        except json.JSONDecodeError:
            indices = []
    if record.get("answer_none"):
        answer_none = True
    if not indices and not answer_none:
        indices, answer_none = parse_answer_text(record.get("answer_text", ""))
    return indices, answer_none


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


def load_existing_update_log(path):
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
        if item.get("kind") != "note":
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
    max_session = 0
    for q in questions:
        qid = q["id"]
        if q.get("exam_session") and int(q["exam_session"]) > max_session:
            max_session = int(q["exam_session"])
        answer_indices, answer_none = resolve_answer_meta(q)
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
            "answer_indices": answer_indices,
            "answer_none": answer_none,
            "explanation_latest": latest_exp,
            "explanation_latest_source": latest_source,
            "explanations": exp_list_sorted,
            "tags": tags.get(qid, []),
            "subtopics": subtopics.get(qid, []),
        }
        output.append(record)

    if max_session <= 0:
        max_session = 1

    tag_scores = {}
    for record in output:
        subject = record["subject"]
        session = record.get("exam_session") or 0
        try:
            session_value = int(session)
        except (TypeError, ValueError):
            session_value = 0
        weight = 1.0 + (session_value / max_session)
        subtopics_list = record.get("subtopics") or []
        if not subtopics_list:
            subtopics_list = [None]
        for subtopic in subtopics_list:
            key = (subject, subtopic)
            tag_scores.setdefault(key, {})
            for tag in record.get("tags") or []:
                tag_scores[key][tag] = tag_scores[key].get(tag, 0.0) + weight

    top_tag_map = {}
    max_score_map = {}
    top_limit = 5
    for key, scores in tag_scores.items():
        ordered = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
        top_tag_map[key] = ordered[:top_limit]
        max_score_map[key] = sum(score for _, score in ordered[:top_limit])

    for record in output:
        subject = record["subject"]
        subtopics_list = record.get("subtopics") or []
        keys = []
        if subtopics_list:
            keys.extend((subject, subtopic) for subtopic in subtopics_list)
        else:
            keys.append((subject, None))
        best_score = 0.0
        best_tags = []
        best_scope = ""
        for key in keys:
            top_tags = top_tag_map.get(key, [])
            if not top_tags:
                continue
            top_tags_map = {tag: score for tag, score in top_tags}
            matched = [tag for tag in (record.get("tags") or []) if tag in top_tags_map]
            if not matched:
                continue
            score = sum(top_tags_map[tag] for tag in matched)
            if score > best_score:
                best_score = score
                best_tags = sorted(matched, key=lambda t: -top_tags_map.get(t, 0))
                scope_label = key[1] if key[1] is not None else "subject"
                best_scope = scope_label
        max_score = max_score_map.get(
            (subject, best_scope if best_scope != "subject" else None), 0.0
        )
        level = 0
        if best_score > 0 and max_score > 0:
            ratio = best_score / max_score
            if ratio >= 0.66:
                level = 3
            elif ratio >= 0.33:
                level = 2
            else:
                level = 1
        record["frequent_score"] = round(best_score, 3)
        record["frequent_level"] = level
        record["frequent_tags"] = best_tags[:2]
        record["frequent_scope"] = best_scope

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Web JSON saved: {out_path}")

    update_notes = load_update_notes(Path("config/update_notes.json"))
    existing_notes = load_existing_update_log(out_path.parent / "update_log.json")
    existing_set = {(note["date"], note["text"]) for note in update_notes}
    for note in existing_notes:
        key = (note["date"], note["text"])
        if key not in existing_set:
            update_notes.append(note)
            existing_set.add(key)
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
