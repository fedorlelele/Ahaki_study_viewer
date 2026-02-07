#!/usr/bin/env python3
import argparse
import json
import re
import sqlite3
from collections import defaultdict


FULLWIDTH_MAP = str.maketrans(
    "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
)


def normalize_text(value):
    text = str(value or "").translate(FULLWIDTH_MAP)
    text = text.lower()
    text = re.sub(r"\s+", "", text)
    return text


def ensure_tables(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS tag_descriptions (
            canonical_label TEXT PRIMARY KEY,
            description TEXT NOT NULL DEFAULT '',
            source_model TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS tag_aliases (
            alias TEXT PRIMARY KEY,
            canonical_label TEXT NOT NULL,
            confidence REAL DEFAULT 0.0,
            approved INTEGER NOT NULL DEFAULT 1,
            source_model TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );
        """
    )


def load_full_tags(conn):
    result = set()
    for (label,) in conn.execute("SELECT label FROM tags").fetchall():
        cleaned = " ".join(str(label or "").split()).strip()
        if cleaned:
            result.add(cleaned)
    for (canonical,) in conn.execute("SELECT canonical_label FROM tag_descriptions").fetchall():
        cleaned = " ".join(str(canonical or "").split()).strip()
        if cleaned:
            result.add(cleaned)
    for alias, canonical, approved in conn.execute(
        "SELECT alias, canonical_label, approved FROM tag_aliases"
    ).fetchall():
        if int(approved or 0) != 1:
            continue
        alias_clean = " ".join(str(alias or "").split()).strip()
        canonical_clean = " ".join(str(canonical or "").split()).strip()
        if alias_clean and canonical_clean:
            result.add(alias_clean)
            result.add(canonical_clean)
    try:
        rows = conn.execute(
            "SELECT tags_json FROM deep_dive_explanations WHERE tags_json IS NOT NULL AND tags_json != ''"
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for (tags_json,) in rows:
        try:
            values = json.loads(tags_json or "[]")
        except json.JSONDecodeError:
            values = []
        for value in values:
            cleaned = " ".join(str(value or "").split()).strip()
            if cleaned:
                result.add(cleaned)
    return sorted(result)


def load_alias_map(conn):
    mapping = {}
    rows = conn.execute(
        "SELECT alias, canonical_label, approved FROM tag_aliases"
    ).fetchall()
    for alias, canonical, approved in rows:
        if int(approved or 0) != 1:
            continue
        alias_clean = " ".join(str(alias or "").split()).strip()
        canonical_clean = " ".join(str(canonical or "").split()).strip()
        if alias_clean and canonical_clean:
            mapping[alias_clean] = canonical_clean
    return mapping


def load_questions(conn):
    return conn.execute(
        """
        SELECT id, serial, case_text, stem, choices_json
        FROM questions
        ORDER BY serial
        """
    ).fetchall()


def build_tag_candidates(tags, alias_map):
    candidates = []
    by_head = defaultdict(list)
    for raw in tags:
        canonical = alias_map.get(raw, raw)
        label = " ".join(str(canonical or "").split()).strip()
        if not label:
            continue
        normalized = normalize_text(label)
        if len(normalized) < 2:
            continue
        candidates.append((raw, label, normalized))
        by_head[normalized[0]].append((raw, label, normalized))
    return candidates, by_head


def ensure_tag_id(conn, label):
    conn.execute("INSERT OR IGNORE INTO tags(label) VALUES (?)", (label,))
    row = conn.execute("SELECT id FROM tags WHERE label = ?", (label,)).fetchone()
    return int(row[0])


def has_question_tag(conn, question_id, tag_id):
    row = conn.execute(
        "SELECT 1 FROM question_tags WHERE question_id = ? AND tag_id = ? LIMIT 1",
        (question_id, tag_id),
    ).fetchone()
    return bool(row)


def parse_choices(choices_json):
    try:
        data = json.loads(choices_json or "[]")
    except json.JSONDecodeError:
        data = []
    if not isinstance(data, list):
        return []
    return [str(item or "") for item in data]


def parse_args():
    parser = argparse.ArgumentParser(description="Auto assign tags from full tag strings.")
    parser.add_argument("--db", default="output/ahaki.sqlite", help="SQLite path.")
    parser.add_argument("--source", default="auto_text", help="source value for question_tags.")
    parser.add_argument("--replace-source", action="store_true", help="Delete existing source rows before insert.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of questions.")
    parser.add_argument("--min-tag-length", type=int, default=2, help="Minimum normalized tag length.")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db)
    ensure_tables(conn)
    alias_map = load_alias_map(conn)
    full_tags = load_full_tags(conn)
    candidates, by_head = build_tag_candidates(full_tags, alias_map)
    if args.min_tag_length > 2:
        filtered = []
        for raw, label, normalized in candidates:
            if len(normalized) >= args.min_tag_length:
                filtered.append((raw, label, normalized))
        candidates = filtered
        by_head = defaultdict(list)
        for item in candidates:
            by_head[item[2][0]].append(item)

    questions = load_questions(conn)
    if args.limit > 0:
        questions = questions[: args.limit]

    if args.replace_source:
        conn.execute("DELETE FROM question_tags WHERE source = ?", (args.source,))

    inserted = 0
    scanned = 0
    for qid, serial, case_text, stem, choices_json in questions:
        choices = parse_choices(choices_json)
        text = normalize_text(" ".join([case_text or "", stem or "", " ".join(choices)]))
        if not text:
            continue
        scanned += 1
        head_chars = {ch for ch in text[:5000]}
        possible = []
        for ch in head_chars:
            possible.extend(by_head.get(ch, []))
        if not possible:
            continue
        matched = set()
        for _, canonical, normalized in possible:
            if normalized in text:
                matched.add(canonical)
        if not matched:
            continue
        for canonical in sorted(matched):
            tag_id = ensure_tag_id(conn, canonical)
            if has_question_tag(conn, qid, tag_id):
                continue
            conn.execute(
                """
                INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                VALUES (?, ?, ?)
                """,
                (qid, tag_id, args.source),
            )
            inserted += 1
        if scanned % 500 == 0:
            conn.commit()
            print(f"scanned={scanned} serial={serial} inserted={inserted}")

    conn.commit()
    conn.close()
    print(f"done: scanned={scanned}, inserted={inserted}, candidate_tags={len(candidates)}")


if __name__ == "__main__":
    main()
