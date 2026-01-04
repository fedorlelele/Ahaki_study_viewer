import argparse
import json
import re
import sqlite3
from pathlib import Path


SECTION_RE = re.compile(r"^\d{2}\.\s*(.+?)\s+\d+問\s*$")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Populate subtopic catalog from sample folder."
    )
    parser.add_argument(
        "--db",
        default="kokushitxt/output/hikkei.sqlite",
        help="Path to SQLite database (for subject validation).",
    )
    parser.add_argument(
        "--samples",
        default="科目ごと小項目サンプル",
        help="Path to subject subtopic sample folder.",
    )
    parser.add_argument(
        "--out",
        default="config/subtopics_catalog.json",
        help="Output JSON path.",
    )
    return parser.parse_args()


def normalize_subject_label(label):
    label = label.strip()
    label = label.replace("　", " ")
    return " ".join(label.split())


def map_subjects(label):
    label = normalize_subject_label(label)
    overrides = {
        "医療概論・関係法規": ["医療概論", "関係法規"],
        "療概論・関係法規": ["医療概論", "関係法規"],
        "衛生学・公衆衛生学": ["衛生学・公衆衛生学"],
        "東洋医学臨床論(はき師用)": ["東洋医学臨床論"],
        "東洋医学臨床論(あマ指師用)": ["東洋医学臨床論"],
        "はりきゅう理論": ["はり理論", "きゅう理論"],
    }
    if label in overrides:
        return overrides[label]
    if "・" in label:
        return [part.strip() for part in label.split("・") if part.strip()]
    return [label]


def extract_subject_label(lines, fallback_name):
    for ln in lines[:10]:
        match = re.search(r"^(.+?)\s+全\d+問\s*$", ln.strip())
        if match:
            return normalize_subject_label(match.group(1))
    return normalize_subject_label(fallback_name)


def extract_subtopics(lines):
    topics = []
    for ln in lines:
        match = SECTION_RE.match(ln.strip())
        if match:
            topic = normalize_subject_label(match.group(1))
            if topic and topic not in topics:
                topics.append(topic)
    return topics


def load_subjects(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT name FROM subjects").fetchall()
    conn.close()
    return {row[0] for row in rows}


def read_text(path):
    data = path.read_bytes()
    for enc in ("cp932", "utf-8"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def main():
    args = parse_args()
    sample_dir = Path(args.samples)
    out_path = Path(args.out)
    subject_set = load_subjects(Path(args.db))

    catalog = {}
    for path in sorted(sample_dir.glob("*.txt")):
        lines = read_text(path).splitlines()
        fallback = path.stem.split("_", 1)[-1]
        label = extract_subject_label(lines, fallback)
        subjects = map_subjects(label)
        subtopics = extract_subtopics(lines)

        for subject in subjects:
            if subject not in subject_set:
                print(f"Warning: subject not in DB: {subject} (from {path.name})")
            catalog.setdefault(subject, [])
            for topic in subtopics:
                if topic not in catalog[subject]:
                    catalog[subject].append(topic)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Catalog saved: {out_path}")


if __name__ == "__main__":
    main()
