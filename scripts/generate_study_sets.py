import argparse
import json
import random
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate study sets by subject/tag/subtopic."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--out",
        default="output/study_sets.json",
        help="Output JSON path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max questions per set (0 = no limit).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for sampling.",
    )
    parser.add_argument(
        "--include",
        default="subject,tag,subtopic",
        help="Comma-separated: subject,tag,subtopic",
    )
    return parser.parse_args()


def fetch_subject_sets(conn):
    rows = conn.execute(
        """
        SELECT s.name, q.serial
        FROM questions q
        JOIN subjects s ON s.id = q.subject_id
        ORDER BY s.name, q.serial
        """
    ).fetchall()
    data = {}
    for subject, serial in rows:
        data.setdefault(subject, []).append(serial)
    return data


def fetch_tag_sets(conn):
    rows = conn.execute(
        """
        SELECT t.label, q.serial
        FROM question_tags qt
        JOIN tags t ON t.id = qt.tag_id
        JOIN questions q ON q.id = qt.question_id
        ORDER BY t.label, q.serial
        """
    ).fetchall()
    data = {}
    for tag, serial in rows:
        data.setdefault(tag, []).append(serial)
    return data


def fetch_subtopic_sets(conn):
    rows = conn.execute(
        """
        SELECT st.name, q.serial
        FROM question_subtopics qs
        JOIN subtopics st ON st.id = qs.subtopic_id
        JOIN questions q ON q.id = qs.question_id
        ORDER BY st.name, q.serial
        """
    ).fetchall()
    data = {}
    for name, serial in rows:
        data.setdefault(name, []).append(serial)
    return data


def cap_sets(sets, limit, rng):
    if limit <= 0:
        return sets
    capped = {}
    for name, serials in sets.items():
        if len(serials) <= limit:
            capped[name] = serials
        else:
            capped[name] = rng.sample(serials, limit)
    return capped


def main():
    args = parse_args()
    include = {item.strip() for item in args.include.split(",") if item.strip()}
    rng = random.Random(args.seed)

    conn = sqlite3.connect(args.db)
    output = {}

    if "subject" in include:
        output["by_subject"] = cap_sets(fetch_subject_sets(conn), args.limit, rng)
    if "tag" in include:
        output["by_tag"] = cap_sets(fetch_tag_sets(conn), args.limit, rng)
    if "subtopic" in include:
        output["by_subtopic"] = cap_sets(fetch_subtopic_sets(conn), args.limit, rng)

    conn.close()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Study sets saved: {out_path}")


if __name__ == "__main__":
    main()
