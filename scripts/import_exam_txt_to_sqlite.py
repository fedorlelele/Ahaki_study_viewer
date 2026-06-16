import argparse
import codecs
import json
import re
import sqlite3
from pathlib import Path


FULLWIDTH_TO_ASCII = str.maketrans("０１２３４５６７８９", "0123456789")


def normalize_digits(value):
    return str(value).translate(FULLWIDTH_TO_ASCII)


def read_exam_text(file_path):
    raw = Path(file_path).read_bytes()

    if raw.startswith(codecs.BOM_UTF8):
        return raw.decode("utf-8-sig")
    if raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return raw.decode("utf-16")

    if b"\x00" in raw[:256]:
        for encoding in ("utf-16", "utf-16-le", "utf-16-be"):
            try:
                return raw.decode(encoding)
            except UnicodeDecodeError:
                continue

    for encoding in ("utf-8", "utf-8-sig", "cp932"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError(
        "unknown",
        raw,
        0,
        len(raw),
        f"Unsupported text encoding: {file_path}",
    )


def clean_subject_name(value):
    return str(value or "").strip().replace("衛生学／公衆衛生学", "衛生学・公衆衛生学")


def extract_exam_info(lines, file_path):
    if not lines:
        raise ValueError(f"Empty exam file: {file_path}")

    header = lines[0]
    if "あん摩マッサージ指圧師試験" in header:
        exam_type_code = "A"
        exam_type = "あん摩マッサージ指圧師"
    elif "はり師・きゆう師試験" in header:
        exam_type_code = "B"
        exam_type = "はり師・きゆう師"
    else:
        raise ValueError(f"Unsupported exam header in {file_path}: {header}")

    match = re.search(r"第([０-９0-9]+)回", header)
    if not match:
        raise ValueError(f"Exam session not found in {file_path}: {header}")

    exam_session = int(normalize_digits(match.group(1)))
    return exam_type_code, exam_type, exam_session


def split_blocks(lines):
    blocks = []
    current = []
    in_question = False

    for line in lines:
        if line.startswith("問題"):
            if current:
                blocks.append("\n".join(current))
            current = [line]
            in_question = True
            continue

        if in_question:
            current.append(line)
            if line.startswith("解答"):
                blocks.append("\n".join(current))
                current = []
                in_question = False
            continue

        blocks.append(line)

    if current:
        blocks.append("\n".join(current))

    return blocks


def replace_question_references(text, serial_map):
    def repl(match):
        question_no = int(normalize_digits(match.group(1)))
        return serial_map.get(f"{question_no:03}", match.group(0))

    return re.sub(r"問題([０-９0-9]+)", repl, text)


def extract_serials_from_case_text(text):
    serial_re = re.compile(r"[AB]\d{2}-\d{3}")
    grouped_re = re.compile(r"([AB]\d{2})-(\d{3})(?:[、,](\d{1,3}))+")

    serials = set(serial_re.findall(text))
    for match in grouped_re.finditer(text):
        prefix = match.group(1)
        serials.add(f"{prefix}-{int(match.group(2)):03}")
        for num in re.findall(r"[、,](\d{1,3})", match.group(0)):
            serials.add(f"{prefix}-{int(num):03}")
    return sorted(serials)


def parse_question_content(question_text):
    lines = [line.strip() for line in question_text.splitlines() if line.strip()]
    answer_line = None
    content_lines = []
    for line in lines:
        if line.startswith("解答"):
            answer_line = line
        else:
            content_lines.append(line)

    choice_re = re.compile(r"^[ 　]*([0-9０-９]+)[\.．]\s*(.*)$")
    choices = []
    stem_lines = []
    in_choices = False
    for line in content_lines:
        match = choice_re.match(line)
        if match:
            in_choices = True
            choices.append(match.group(2).strip())
        else:
            if in_choices and choices:
                choices[-1] = choices[-1] + "\n" + line
            else:
                stem_lines.append(line)

    stem = "\n".join(stem_lines).strip()
    if not stem:
        stem = "\n".join(content_lines).strip()

    answer_index = None
    answer_indices = []
    answer_none = False
    if answer_line:
        normalized = normalize_digits(answer_line)
        if "なし" in normalized:
            answer_none = True
        elif "すべて" in normalized:
            answer_indices = [1, 2, 3, 4]
        else:
            digits = re.findall(r"[1-4]", normalized)
            answer_indices = sorted({int(digit) for digit in digits})
        if len(answer_indices) == 1:
            answer_index = answer_indices[0]

    return stem, choices, answer_index, answer_indices, answer_none, answer_line


def parse_exam_file(file_path):
    text = read_exam_text(file_path)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    exam_type_code, exam_type, exam_session = extract_exam_info(lines, file_path)
    blocks = split_blocks(lines)

    serial_map = {}
    for block in blocks:
        if not block.startswith("問題"):
            continue
        match = re.match(r"問題([０-９0-9]+)", block)
        if not match:
            continue
        question_no = int(normalize_digits(match.group(1)))
        serial_map[f"{question_no:03}"] = f"{exam_type_code}{exam_session:02}-{question_no:03}"

    normalized_blocks = [replace_question_references(block, serial_map) for block in blocks]

    entries = []
    current_subject = None
    for block in normalized_blocks:
        subject_match = re.search(r"《([^》]+)》", block)
        if subject_match:
            current_subject = clean_subject_name(subject_match.group(1))
            continue

        serial = None
        raw_text = None
        question_text = None
        if re.match(r"^[AB]\d{2}-\d{3}", block):
            serial = block.split()[0]
            raw_text = block
            question_text = re.sub(r"^\s*[AB]\d{2}-\d{3}\s+", "", block, count=1)

        entries.append(
            {
                "serial": serial,
                "subject": current_subject,
                "case_text": None,
                "raw_text": raw_text,
                "question_text": question_text,
                "block": block,
                "exam_type_code": exam_type_code,
                "exam_type": exam_type,
                "exam_session": exam_session,
            }
        )

    shared_context_re = re.compile(r"(答えよ|問いに答え|問に答え)")
    for index, entry in enumerate(entries):
        block = entry["block"]
        if entry["serial"]:
            continue
        if not shared_context_re.search(block) or not ("次の" in block or "下記" in block):
            continue

        serials = extract_serials_from_case_text(block)
        if not serials:
            continue

        case_lines = [block]
        next_index = index + 1
        while next_index < len(entries) and not entries[next_index]["serial"]:
            case_lines.append(entries[next_index]["block"])
            next_index += 1
        case_text = "\n".join(case_lines)

        for serial in serials:
            for question_entry in entries:
                if question_entry["serial"] == serial:
                    question_entry["case_text"] = case_text

    questions = []
    for entry in entries:
        if not entry["serial"]:
            continue
        if not entry["subject"]:
            raise ValueError(f"Subject not found for {entry['serial']} in {file_path}")
        stem, choices, answer_index, answer_indices, answer_none, answer_text = parse_question_content(
            entry["question_text"]
        )
        questions.append(
            {
                "serial": entry["serial"],
                "exam_type_code": entry["exam_type_code"],
                "exam_type": entry["exam_type"],
                "exam_session": entry["exam_session"],
                "subject": entry["subject"],
                "case_text": entry["case_text"],
                "stem": stem,
                "choices": choices,
                "answer_index": answer_index,
                "answer_indices": answer_indices,
                "answer_none": answer_none,
                "answer_text": answer_text,
                "raw_text": entry["raw_text"],
            }
        )

    return questions


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
            answer_indices_json TEXT,
            answer_none INTEGER DEFAULT 0,
            answer_text TEXT,
            raw_text TEXT NOT NULL,
            FOREIGN KEY (subject_id) REFERENCES subjects(id)
        );

        CREATE INDEX IF NOT EXISTS idx_questions_serial ON questions(serial);
        CREATE INDEX IF NOT EXISTS idx_questions_subject ON questions(subject_id);
        """
    )


def load_subject_cache(conn):
    rows = conn.execute("SELECT id, name FROM subjects").fetchall()
    return {name: subject_id for subject_id, name in rows}


def ensure_subject(conn, subject_cache, subject_name):
    if subject_name not in subject_cache:
        conn.execute("INSERT OR IGNORE INTO subjects(name) VALUES (?)", (subject_name,))
        row = conn.execute(
            "SELECT id FROM subjects WHERE name = ?",
            (subject_name,),
        ).fetchone()
        subject_cache[subject_name] = row[0]
    return subject_cache[subject_name]


def write_question_json(json_dir, question):
    output = {
        "serial": question["serial"],
        "exam_type": question["exam_type"],
        "exam_session": question["exam_session"],
        "subject": question["subject"],
        "case_text": question["case_text"],
        "stem": question["stem"],
        "choices": question["choices"],
        "answer_index": question["answer_index"],
        "answer_indices": question["answer_indices"],
        "answer_none": question["answer_none"],
        "answer_text": question["answer_text"],
        "explanations": [],
        "tags": [],
    }
    json_path = json_dir / f"{question['serial']}.json"
    json_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def import_exam_files(db_path, json_dir, file_paths):
    conn = sqlite3.connect(db_path)
    init_db(conn)
    subject_cache = load_subject_cache(conn)
    imported_count = 0

    for file_path in file_paths:
        questions = parse_exam_file(file_path)
        for question in questions:
            subject_id = ensure_subject(conn, subject_cache, question["subject"])
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
                    answer_indices_json,
                    answer_none,
                    answer_text,
                    raw_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(serial) DO UPDATE SET
                    exam_type_code = excluded.exam_type_code,
                    exam_type = excluded.exam_type,
                    exam_session = excluded.exam_session,
                    subject_id = excluded.subject_id,
                    case_text = excluded.case_text,
                    stem = excluded.stem,
                    choices_json = excluded.choices_json,
                    answer_index = excluded.answer_index,
                    answer_indices_json = excluded.answer_indices_json,
                    answer_none = excluded.answer_none,
                    answer_text = excluded.answer_text,
                    raw_text = excluded.raw_text
                """,
                (
                    question["serial"],
                    question["exam_type_code"],
                    question["exam_type"],
                    question["exam_session"],
                    subject_id,
                    question["case_text"],
                    question["stem"],
                    json.dumps(question["choices"], ensure_ascii=False),
                    question["answer_index"],
                    json.dumps(question["answer_indices"], ensure_ascii=False),
                    1 if question["answer_none"] else 0,
                    question["answer_text"],
                    question["raw_text"],
                ),
            )
            write_question_json(json_dir, question)
            imported_count += 1

    conn.commit()
    conn.close()
    return imported_count


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import specific exam TXT files into the existing SQLite database."
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="Exam TXT files to import.",
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--json-dir",
        default="output/questions_json",
        help="Directory for per-question JSON output.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    db_path = (root_dir / args.db).resolve()
    json_dir = (root_dir / args.json_dir).resolve()
    json_dir.mkdir(parents=True, exist_ok=True)

    file_paths = []
    for file_arg in args.files:
        file_path = Path(file_arg).expanduser().resolve()
        if not file_path.exists():
            raise FileNotFoundError(f"TXT file not found: {file_path}")
        file_paths.append(file_path)

    imported_count = import_exam_files(db_path, json_dir, file_paths)
    print(f"Imported {imported_count} questions into {db_path}")
    print(f"Question JSON saved: {json_dir}")


if __name__ == "__main__":
    main()
