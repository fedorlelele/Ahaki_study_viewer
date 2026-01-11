import json
import os
import re
import sqlite3
from pathlib import Path

import pandas as pd

from convert_ahaki_to_json import (
    add_subject_to_questions_and_rearrange_columns,
    clean_subject_names,
    generate_question_number,
    process_questions,
    remove_serial_number_from_questions,
    replace_question_with_serial,
    store_case_details_next_to_questions,
)

FULLWIDTH_TO_ASCII = str.maketrans("０１２３４５６７８９", "0123456789")


def normalize_digits(value):
    return value.translate(FULLWIDTH_TO_ASCII)


def add_exam_type_and_session(df):
    df["Exam Type"] = df["Serial Number"].str[0].map(
        {"A": "あん摩マッサージ指圧師", "B": "はり師・きゆう師"}
    )
    df["Exam Session"] = (
        df["Serial Number"].str.extract(r"[AB](\d{2})-").astype(float).astype("Int64")
    )
    return df


def parse_question_content(question_text):
    lines = [ln.strip() for ln in question_text.splitlines() if ln.strip()]
    answer_line = None
    content_lines = []
    for ln in lines:
        if ln.startswith("解答"):
            answer_line = ln.strip()
        else:
            content_lines.append(ln)

    choice_re = re.compile(r"^[ 　]*([0-9０-９]+)[\.．]\s*(.*)$")
    choices = []
    stem_lines = []
    in_choices = False
    for ln in content_lines:
        match = choice_re.match(ln)
        if match:
            in_choices = True
            choices.append(match.group(2).strip())
        else:
            if in_choices and choices:
                choices[-1] = choices[-1] + "\n" + ln
            else:
                stem_lines.append(ln)

    stem = "\n".join(stem_lines).strip()
    if not stem:
        stem = "\n".join(content_lines).strip()

    answer_index = None
    if answer_line:
        match = re.search(r"解答\s*([0-9０-９]+)", answer_line)
        if match:
            answer_index = int(normalize_digits(match.group(1)))

    return stem, choices, answer_index, answer_line


def build_dataframe(txt_path):
    df = (
        process_questions(txt_path)
        .pipe(generate_question_number)
        .pipe(replace_question_with_serial)
        .pipe(add_subject_to_questions_and_rearrange_columns)
        .pipe(store_case_details_next_to_questions)
    )

    df["Raw Text"] = df["Question"]

    df = (
        df.pipe(remove_serial_number_from_questions)
        .pipe(clean_subject_names)
        .pipe(add_exam_type_and_session)
    )

    return df


def init_db(conn):
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS subjects (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS questions (
            id INTEGER PRIMARY KEY,
            serial TEXT NOT NULL UNIQUE,
            exam_type_code TEXT NOT NULL,
            exam_type TEXT NOT NULL,
            exam_session INTEGER NOT NULL,
            subject_id INTEGER,
            case_text TEXT,
            stem TEXT NOT NULL,
            choices_json TEXT NOT NULL,
            answer_index INTEGER,
            answer_text TEXT,
            raw_text TEXT NOT NULL,
            FOREIGN KEY (subject_id) REFERENCES subjects(id)
        );

        CREATE TABLE IF NOT EXISTS explanations (
            id INTEGER PRIMARY KEY,
            question_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            source TEXT,
            FOREIGN KEY (question_id) REFERENCES questions(id)
        );

        CREATE TABLE IF NOT EXISTS explanation_update_log (
            date TEXT PRIMARY KEY,
            count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL UNIQUE,
            type TEXT
        );

        CREATE TABLE IF NOT EXISTS question_tags (
            question_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            PRIMARY KEY (question_id, tag_id, source),
            FOREIGN KEY (question_id) REFERENCES questions(id),
            FOREIGN KEY (tag_id) REFERENCES tags(id)
        );

        CREATE TABLE IF NOT EXISTS subtopics (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES subtopics(id)
        );

        CREATE TABLE IF NOT EXISTS question_subtopics (
            question_id INTEGER NOT NULL,
            subtopic_id INTEGER NOT NULL,
            PRIMARY KEY (question_id, subtopic_id),
            FOREIGN KEY (question_id) REFERENCES questions(id),
            FOREIGN KEY (subtopic_id) REFERENCES subtopics(id)
        );

        CREATE INDEX IF NOT EXISTS idx_questions_serial ON questions(serial);
        CREATE INDEX IF NOT EXISTS idx_questions_subject ON questions(subject_id);
        CREATE INDEX IF NOT EXISTS idx_question_tags_tag ON question_tags(tag_id);
        """
    )


def build_question_json(record):
    return {
        "serial": record["serial"],
        "exam_type": record["exam_type"],
        "exam_session": record["exam_session"],
        "subject": record["subject"],
        "case_text": record["case_text"],
        "stem": record["stem"],
        "choices": record["choices"],
        "answer_index": record["answer_index"],
        "answer_text": record["answer_text"],
        "explanations": [],
        "tags": [],
    }


def main():
    base_dir = Path(__file__).resolve().parent
    input_dir = base_dir / "kokushitxt"
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = output_dir / "ahaki.sqlite"
    json_dir = output_dir / "questions_json"
    json_dir.mkdir(parents=True, exist_ok=True)

    txt_files = sorted(
        [p for p in input_dir.iterdir() if p.suffix.lower() == ".txt"]
    )
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in {input_dir}")

    conn = sqlite3.connect(db_path)
    init_db(conn)

    subject_cache = {}

    for txt_path in txt_files:
        try:
            df = build_dataframe(str(txt_path))
        except Exception as exc:
            print(f"Error processing {txt_path.name}: {exc}")
            continue

        for _, row in df.iterrows():
            serial = row["Serial Number"]
            subject_name = row["Subject"]
            case_text = row["Case Details"]
            question_text = row["Question"]
            raw_text = row["Raw Text"]
            exam_type = row["Exam Type"]
            exam_session = int(row["Exam Session"])
            exam_type_code = serial[0]

            if subject_name not in subject_cache:
                conn.execute(
                    "INSERT OR IGNORE INTO subjects(name) VALUES (?)",
                    (subject_name,),
                )
                subject_id = conn.execute(
                    "SELECT id FROM subjects WHERE name = ?",
                    (subject_name,),
                ).fetchone()[0]
                subject_cache[subject_name] = subject_id
            subject_id = subject_cache[subject_name]

            stem, choices, answer_index, answer_text = parse_question_content(
                question_text
            )

            conn.execute(
                """
                INSERT INTO questions(
                    serial,
                    exam_type_code,
                    exam_type,
                    exam_session,
                    subject_id,
                    case_text,
                    stem,
                    choices_json,
                    answer_index,
                    answer_text,
                    raw_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(serial) DO UPDATE SET
                    exam_type_code = excluded.exam_type_code,
                    exam_type = excluded.exam_type,
                    exam_session = excluded.exam_session,
                    subject_id = excluded.subject_id,
                    case_text = excluded.case_text,
                    stem = excluded.stem,
                    choices_json = excluded.choices_json,
                    answer_index = excluded.answer_index,
                    answer_text = excluded.answer_text,
                    raw_text = excluded.raw_text
                """,
                (
                    serial,
                    exam_type_code,
                    exam_type,
                    exam_session,
                    subject_id,
                    case_text,
                    stem,
                    json.dumps(choices, ensure_ascii=False),
                    answer_index,
                    answer_text,
                    raw_text,
                ),
            )

            question_json = build_question_json(
                {
                    "serial": serial,
                    "exam_type": exam_type,
                    "exam_session": exam_session,
                    "subject": subject_name,
                    "case_text": case_text,
                    "stem": stem,
                    "choices": choices,
                    "answer_index": answer_index,
                    "answer_text": answer_text,
                }
            )
            json_path = json_dir / f"{serial}.json"
            json_path.write_text(
                json.dumps(question_json, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    conn.commit()
    conn.close()
    print(f"SQLite saved: {db_path}")
    print(f"Question JSON saved: {json_dir}")


if __name__ == "__main__":
    main()
