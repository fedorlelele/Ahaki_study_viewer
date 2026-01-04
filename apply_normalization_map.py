import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Apply normalization map to tags/subtopics."
    )
    parser.add_argument(
        "--db",
        default="kokushitxt/output/hikkei.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--map",
        default="kokushitxt/output/normalization_map.json",
        help="Normalization map JSON path.",
    )
    return parser.parse_args()


def apply_map(conn, table, column, mapping):
    for src, dst in mapping.items():
        if src == dst:
            continue
        row = conn.execute(
            f"SELECT id FROM {table} WHERE {column} = ?",
            (src,),
        ).fetchone()
        if not row:
            continue
        src_id = row[0]

        row = conn.execute(
            f"SELECT id FROM {table} WHERE {column} = ?",
            (dst,),
        ).fetchone()
        if row:
            dst_id = row[0]
        else:
            conn.execute(
                f"INSERT INTO {table}({column}) VALUES (?)",
                (dst,),
            )
            dst_id = conn.execute(
                f"SELECT id FROM {table} WHERE {column} = ?",
                (dst,),
            ).fetchone()[0]

        if table == "tags":
            conn.execute(
                "UPDATE question_tags SET tag_id = ? WHERE tag_id = ?",
                (dst_id, src_id),
            )
        else:
            conn.execute(
                "UPDATE question_subtopics SET subtopic_id = ? WHERE subtopic_id = ?",
                (dst_id, src_id),
            )

        conn.execute(f"DELETE FROM {table} WHERE id = ?", (src_id,))


def main():
    args = parse_args()
    db_path = Path(args.db)
    map_path = Path(args.map)

    mapping = json.loads(map_path.read_text(encoding="utf-8"))
    tag_map = mapping.get("tags", {})
    subtopic_map = mapping.get("subtopics", {})

    conn = sqlite3.connect(db_path)
    apply_map(conn, "tags", "label", tag_map)
    apply_map(conn, "subtopics", "name", subtopic_map)
    conn.commit()
    conn.close()
    print(f"Normalization applied: {map_path}")


if __name__ == "__main__":
    main()
