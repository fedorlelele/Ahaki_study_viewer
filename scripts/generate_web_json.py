import argparse
from datetime import datetime, timezone
import json
import re
import sqlite3
from pathlib import Path

try:
    from scripts.explanation_metadata import (
        derive_explanation_metadata,
        explanation_columns,
    )
except ModuleNotFoundError:
    from explanation_metadata import (
        derive_explanation_metadata,
        explanation_columns,
    )

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
    parser.add_argument(
        "--embed-deep-dive",
        action="store_true",
        help=(
            "Embed deep-dive explanations in questions.json. "
            "Default is off to keep the output small."
        ),
    )
    parser.add_argument(
        "--question-chunk-dir",
        default="questions",
        help=(
            "Directory (relative to output/web) used for split question JSON files. "
            "Set empty string to disable chunk generation."
        ),
    )
    parser.add_argument(
        "--question-chunk-size",
        type=int,
        default=500,
        help="Number of questions per split JSON file.",
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
    columns = explanation_columns(conn)
    model_expr = "model_name" if "model_name" in columns else "NULL AS model_name"
    status_expr = (
        "review_status" if "review_status" in columns else "NULL AS review_status"
    )
    rows = conn.execute(
        f"""
        SELECT question_id, body, version, source, {model_expr}, {status_expr}
        FROM explanations
        ORDER BY id
        """
    ).fetchall()
    data = {}
    for question_id, body, version, source, model_name, review_status in rows:
        meta = derive_explanation_metadata(source, model_name, review_status)
        data.setdefault(question_id, []).append(
            {
                "body": body,
                "version": version,
                "source": source,
                "model_name": meta["model_name"],
                "review_status": meta["review_status"],
            }
        )
    return data


def load_tags(conn):
    rows = conn.execute(
        """
        SELECT DISTINCT qt.question_id, t.label
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


def load_tag_dictionary(conn):
    descriptions = {}
    alias_to_canonical = {}
    aliases_by_canonical = {}
    try:
        rows = conn.execute(
            """
            SELECT canonical_label, description
            FROM tag_descriptions
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for canonical_label, description in rows:
        label = str(canonical_label or "").strip()
        if not label:
            continue
        descriptions[label] = str(description or "").strip()
    try:
        alias_rows = conn.execute(
            """
            SELECT alias, canonical_label, approved
            FROM tag_aliases
            """
        ).fetchall()
    except sqlite3.OperationalError:
        alias_rows = []
    for alias, canonical_label, approved in alias_rows:
        if int(approved or 0) != 1:
            continue
        alias_label = str(alias or "").strip()
        canonical = str(canonical_label or "").strip()
        if not alias_label or not canonical:
            continue
        alias_to_canonical[alias_label] = canonical
        aliases_by_canonical.setdefault(canonical, set()).add(alias_label)
    aliases_by_canonical = {
        key: sorted(values) for key, values in aliases_by_canonical.items()
    }
    return descriptions, alias_to_canonical, aliases_by_canonical


def load_tag_view_stats(conn):
    try:
        rows = conn.execute(
            """
            SELECT tag_label, view_count
            FROM tag_view_stats
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    data = {}
    for tag_label, view_count in rows:
        label = str(tag_label or "").strip()
        if not label:
            continue
        data[label] = int(view_count or 0)
    return data


def load_tag_relations(conn):
    try:
        rows = conn.execute(
            """
            SELECT tag_label, related_tag_label, relation_type
            FROM tag_relations
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    data = {}
    for tag_label, related_tag_label, relation_type in rows:
        if str(relation_type or "").strip() not in {"", "related"}:
            continue
        left = str(tag_label or "").strip()
        right = str(related_tag_label or "").strip()
        if not left or not right or left == right:
            continue
        data.setdefault(left, set()).add(right)
    return {key: sorted(values) for key, values in data.items()}


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


def load_deep_dive(conn):
    try:
        rows = conn.execute(
            """
            SELECT serial, explanation, tags_json, updated_at
            FROM deep_dive_explanations
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    data = {}
    for serial, explanation, tags_json, updated_at in rows:
        tags = []
        if tags_json:
            try:
                tags = json.loads(tags_json)
            except json.JSONDecodeError:
                tags = []
        data[serial] = {
            "explanation": explanation or "",
            "tags": tags or [],
            "updated_at": updated_at or "",
        }
    return data


def load_question_qa(conn):
    try:
        rows = conn.execute(
            """
            SELECT id, serial, question, answer, view_count, like_count, created_at
            FROM question_qa
            WHERE status = 'ok'
            ORDER BY like_count DESC, view_count DESC, created_at DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    data = {}
    for qa_id, serial, question, answer, view_count, like_count, created_at in rows:
        if not serial:
            continue
        data.setdefault(serial, []).append(
            {
                "id": qa_id,
                "question": question or "",
                "answer": answer or "",
                "view_count": int(view_count or 0),
                "like_count": int(like_count or 0),
                "created_at": created_at or "",
            }
        )
    return data


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


def build_question_chunks(records, chunk_size):
    size = max(1, int(chunk_size or 1))
    chunks = []
    for start in range(0, len(records), size):
        end = min(start + size, len(records))
        chunks.append(records[start:end])
    return chunks


def write_question_chunks(records, out_base_dir, chunk_dir_name, chunk_size):
    chunk_dir = str(chunk_dir_name or "").strip().replace("\\", "/").strip("/")
    if not chunk_dir:
        return None
    chunk_rel_path = Path(chunk_dir)
    if chunk_rel_path.is_absolute():
        raise ValueError("--question-chunk-dir must be a relative path")

    out_base_dir.mkdir(parents=True, exist_ok=True)
    chunk_abs_dir = (out_base_dir / chunk_rel_path).resolve()
    chunk_abs_dir.mkdir(parents=True, exist_ok=True)
    for stale in chunk_abs_dir.glob("questions_*.json"):
        stale.unlink(missing_ok=True)

    chunk_rows = build_question_chunks(records, chunk_size)
    chunk_items = []
    for idx, rows in enumerate(chunk_rows, start=1):
        file_name = f"questions_{idx:04d}.json"
        rel_path = (chunk_rel_path / file_name).as_posix()
        abs_path = chunk_abs_dir / file_name
        abs_path.write_text(
            json.dumps(rows, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        first_serial = rows[0].get("serial") if rows else ""
        last_serial = rows[-1].get("serial") if rows else ""
        chunk_items.append(
            {
                "index": idx,
                "path": rel_path,
                "count": len(rows),
                "first_serial": first_serial,
                "last_serial": last_serial,
            }
        )

    manifest = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total": len(records),
        "chunk_size": max(1, int(chunk_size or 1)),
        "chunks": chunk_items,
    }
    manifest_path = out_base_dir / "questions_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_path


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
    deep_dive = load_deep_dive(conn) if args.embed_deep_dive else {}
    (
        tag_descriptions,
        tag_alias_to_canonical,
        tag_aliases_by_canonical,
    ) = load_tag_dictionary(conn)
    tag_view_stats = load_tag_view_stats(conn)
    tag_relations = load_tag_relations(conn)
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
        latest_model_name = (
            exp_list_sorted[-1].get("model_name") if exp_list_sorted else None
        )
        latest_review_status = (
            exp_list_sorted[-1].get("review_status") if exp_list_sorted else None
        )
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
            "explanation_latest_model_name": latest_model_name,
            "explanation_latest_review_status": latest_review_status,
            "explanations": exp_list_sorted,
            "tags": list(dict.fromkeys(tags.get(qid, []))),
            "subtopics": subtopics.get(qid, []),
            "deep_dive": (deep_dive.get(q["serial"]) or None) if args.embed_deep_dive else None,
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
    chunk_dir = str(args.question_chunk_dir or "").strip()
    if chunk_dir:
        manifest_path = write_question_chunks(
            output,
            out_path.parent,
            chunk_dir,
            args.question_chunk_size,
        )
        if manifest_path:
            print(f"Question chunk manifest saved: {manifest_path}")

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
    tag_catalog = {}

    for record in output:
        serial = record["serial"]
        subject = record["subject"]
        index_by_subject.setdefault(subject, []).append(serial)

        for tag in record["tags"]:
            index_by_tag.setdefault(tag, []).append(serial)
            canonical = tag_alias_to_canonical.get(tag, tag)
            item = tag_catalog.setdefault(
                tag,
                {
                    "tag": tag,
                    "canonical_tag": canonical,
                    "description": tag_descriptions.get(canonical, ""),
                    "aliases": tag_aliases_by_canonical.get(canonical, []),
                    "view_count": int(tag_view_stats.get(tag, 0)),
                    "related_count": 0,
                    "subjects": set(),
                    "subtopics": set(),
                    "related_tags": set(),
                },
            )
            item["related_count"] += 1
            if subject:
                item["subjects"].add(subject)
            for subtopic in record["subtopics"]:
                if subtopic:
                    item["subtopics"].add(subtopic)

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
    for item in tag_catalog.values():
        relation_labels = set()
        for key in {item.get("tag"), item.get("canonical_tag")}:
            if not key:
                continue
            for related in tag_relations.get(key, []):
                canonical_related = tag_alias_to_canonical.get(related, related)
                canonical_related = str(canonical_related or "").strip()
                if not canonical_related:
                    continue
                if canonical_related in {item.get("tag"), item.get("canonical_tag")}:
                    continue
                relation_labels.add(canonical_related)
        item["related_tags"] = sorted(relation_labels)
        item["subjects"] = sorted(item["subjects"])
        item["subtopics"] = sorted(item["subtopics"])
        item["related_serials"] = (index_by_tag.get(item["tag"], []) or [])[:5]
    ordered_tag_catalog = sorted(
        tag_catalog.values(),
        key=lambda x: (-int(x.get("view_count", 0)), -int(x.get("related_count", 0)), x["tag"]),
    )
    (index_dir / "tag_catalog.json").write_text(
        json.dumps(ordered_tag_catalog, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Index JSON saved: {index_dir}")


if __name__ == "__main__":
    main()
