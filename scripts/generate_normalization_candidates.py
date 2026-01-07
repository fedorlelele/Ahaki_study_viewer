import argparse
import json
import re
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate normalization candidates for tags/subtopics."
    )
    parser.add_argument(
        "--db",
        default="output/hikkei.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--out",
        default="output/normalization_candidates.json",
        help="Output JSON path.",
    )
    return parser.parse_args()


def normalize_key(text):
    text = str(text)
    text = text.replace("　", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_candidates(labels):
    groups = {}
    for label in labels:
        key = normalize_key(label)
        groups.setdefault(key, set()).add(label)
    candidates = {}
    for key, variants in groups.items():
        if len(variants) > 1:
            candidates[key] = sorted(variants)
    return candidates


def main():
    args = parse_args()
    db_path = Path(args.db)
    out_path = Path(args.out)

    conn = sqlite3.connect(db_path)
    tag_rows = conn.execute("SELECT label FROM tags ORDER BY label").fetchall()
    subtopic_rows = conn.execute("SELECT name FROM subtopics ORDER BY name").fetchall()
    conn.close()

    tag_labels = [row[0] for row in tag_rows]
    subtopic_labels = [row[0] for row in subtopic_rows]

    report = {
        "tags": build_candidates(tag_labels),
        "subtopics": build_candidates(subtopic_labels),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Candidates saved: {out_path}")


if __name__ == "__main__":
    main()
