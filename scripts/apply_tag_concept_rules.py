#!/usr/bin/env python3
import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path


def normalize_label(value):
    return " ".join(str(value or "").split()).strip()


def now_utc():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_tables(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS tag_relations (
            tag_label TEXT NOT NULL,
            related_tag_label TEXT NOT NULL,
            relation_type TEXT NOT NULL DEFAULT 'related',
            updated_at TEXT DEFAULT '',
            PRIMARY KEY (tag_label, related_tag_label, relation_type)
        );
        CREATE INDEX IF NOT EXISTS idx_tag_relations_tag
            ON tag_relations(tag_label, relation_type);
        """
    )


def ensure_tag_id(conn, label):
    normalized = normalize_label(label)
    if not normalized:
        return None
    conn.execute("INSERT OR IGNORE INTO tags(label) VALUES (?)", (normalized,))
    row = conn.execute("SELECT id FROM tags WHERE label = ?", (normalized,)).fetchone()
    return int(row[0]) if row else None


def load_subject_by_serial(conn):
    rows = conn.execute(
        """
        SELECT q.serial, COALESCE(s.name, '')
        FROM questions q
        LEFT JOIN subjects s ON s.id = q.subject_id
        """
    ).fetchall()
    return {str(serial): str(subject or "") for serial, subject in rows}


def resolve_target_label(rule, subject_name, subject_groups):
    subject_name = str(subject_name or "")
    targets = rule.get("targets") or []
    for target in targets:
        label = normalize_label(target.get("label"))
        if not label:
            continue
        subjects = set()
        for value in target.get("subjects") or []:
            cleaned = normalize_label(value)
            if cleaned:
                subjects.add(cleaned)
        for group_name in target.get("groups") or []:
            for value in subject_groups.get(str(group_name), []):
                cleaned = normalize_label(value)
                if cleaned:
                    subjects.add(cleaned)
        if subject_name and subject_name in subjects:
            return label
        for needle in target.get("subject_contains") or []:
            needle_text = normalize_label(needle)
            if needle_text and needle_text in subject_name:
                return label
    default_label = normalize_label(rule.get("default_label"))
    if default_label:
        return default_label
    return normalize_label(rule.get("source_tag"))


def apply_split_rule(conn, rule, subject_groups, dry_run=False):
    source_tag = normalize_label(rule.get("source_tag"))
    if not source_tag:
        return {"source": "", "moved": 0, "deep_dive_updated": 0, "targets": {}}
    source_row = conn.execute("SELECT id FROM tags WHERE label = ?", (source_tag,)).fetchone()
    if not source_row:
        return {"source": source_tag, "moved": 0, "deep_dive_updated": 0, "targets": {}}
    source_tag_id = int(source_row[0])
    rows = conn.execute(
        """
        SELECT q.id, q.serial, COALESCE(s.name, '')
        FROM question_tags qt
        JOIN questions q ON q.id = qt.question_id
        LEFT JOIN subjects s ON s.id = q.subject_id
        WHERE qt.tag_id = ?
        ORDER BY q.serial
        """,
        (source_tag_id,),
    ).fetchall()
    moved = 0
    would_move = 0
    targets = {}
    for question_id, _serial, subject_name in rows:
        target_label = resolve_target_label(rule, subject_name, subject_groups)
        target_label = normalize_label(target_label)
        if not target_label:
            continue
        targets[target_label] = targets.get(target_label, 0) + 1
        if target_label == source_tag:
            continue
        would_move += 1
        if dry_run:
            continue
        target_id = ensure_tag_id(conn, target_label)
        if target_id is None:
            continue
        if target_id == source_tag_id:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
            VALUES (?, ?, ?)
            """,
            (question_id, target_id, "concept_split"),
        )
        conn.execute(
            "DELETE FROM question_tags WHERE question_id = ? AND tag_id = ?",
            (question_id, source_tag_id),
        )
        moved += 1

    deep_dive_updated = 0
    if not dry_run:
        subject_by_serial = load_subject_by_serial(conn)
        dd_rows = conn.execute(
            """
            SELECT serial, tags_json
            FROM deep_dive_explanations
            WHERE tags_json IS NOT NULL AND tags_json != ''
            """
        ).fetchall()
        for serial, tags_json in dd_rows:
            try:
                raw_tags = json.loads(tags_json or "[]")
            except json.JSONDecodeError:
                raw_tags = []
            if not isinstance(raw_tags, list):
                continue
            changed = False
            converted = []
            subject_name = subject_by_serial.get(str(serial), "")
            target_label = resolve_target_label(rule, subject_name, subject_groups)
            target_label = normalize_label(target_label)
            for value in raw_tags:
                tag = normalize_label(value)
                if tag != source_tag:
                    if tag and tag not in converted:
                        converted.append(tag)
                    continue
                changed = True
                if target_label and target_label not in converted:
                    converted.append(target_label)
            if changed:
                conn.execute(
                    "UPDATE deep_dive_explanations SET tags_json = ? WHERE serial = ?",
                    (json.dumps(converted, ensure_ascii=False), serial),
                )
                deep_dive_updated += 1

    return {
        "source": source_tag,
        "moved": moved if not dry_run else would_move,
        "deep_dive_updated": deep_dive_updated,
        "targets": targets,
    }


def apply_copy_rule(conn, rule, subject_groups, dry_run=False):
    source_tag = normalize_label(rule.get("source_tag"))
    target_tag = normalize_label(rule.get("target_tag"))
    copy_source = normalize_label(rule.get("source")) or "concept_copy"
    if not source_tag or not target_tag:
        return {"source": source_tag, "target": target_tag, "copied": 0}
    source_row = conn.execute("SELECT id FROM tags WHERE label = ?", (source_tag,)).fetchone()
    if not source_row:
        return {"source": source_tag, "target": target_tag, "copied": 0}
    source_tag_id = int(source_row[0])

    subjects = set()
    for value in rule.get("subjects") or []:
        cleaned = normalize_label(value)
        if cleaned:
            subjects.add(cleaned)
    for group_name in rule.get("groups") or []:
        for value in subject_groups.get(str(group_name), []):
            cleaned = normalize_label(value)
            if cleaned:
                subjects.add(cleaned)
    subject_contains = [normalize_label(v) for v in (rule.get("subject_contains") or []) if normalize_label(v)]

    rows = conn.execute(
        """
        SELECT q.id, q.serial, COALESCE(s.name, '')
        FROM question_tags qt
        JOIN questions q ON q.id = qt.question_id
        LEFT JOIN subjects s ON s.id = q.subject_id
        WHERE qt.tag_id = ?
        ORDER BY q.serial
        """,
        (source_tag_id,),
    ).fetchall()
    copied = 0
    target_id = None
    if not dry_run:
        target_id = ensure_tag_id(conn, target_tag)
        if target_id is None:
            return {"source": source_tag, "target": target_tag, "copied": 0}
    for question_id, _serial, subject_name in rows:
        subject_name = str(subject_name or "")
        if subjects and subject_name not in subjects:
            if not any(token and token in subject_name for token in subject_contains):
                continue
        elif subject_contains and not any(token and token in subject_name for token in subject_contains):
            continue
        if dry_run:
            copied += 1
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
            VALUES (?, ?, ?)
            """,
            (question_id, target_id, copy_source),
        )
        copied += 1
    return {"source": source_tag, "target": target_tag, "copied": copied}


def apply_rename_rule(conn, rule, dry_run=False):
    old_label = normalize_label(rule.get("old_label"))
    new_label = normalize_label(rule.get("new_label"))
    if not old_label or not new_label or old_label == new_label:
        return {"old": old_label, "new": new_label, "moved": 0, "deep_dive_updated": 0}

    old_row = conn.execute("SELECT id FROM tags WHERE label = ?", (old_label,)).fetchone()
    if not old_row:
        if not dry_run:
            ensure_tag_id(conn, new_label)
            conn.execute(
                """
                INSERT INTO tag_aliases(alias, canonical_label, confidence, approved, source_model, updated_at)
                VALUES (?, ?, 1.0, 1, ?, ?)
                ON CONFLICT(alias) DO UPDATE SET
                  canonical_label = excluded.canonical_label,
                  confidence = excluded.confidence,
                  approved = excluded.approved,
                  source_model = excluded.source_model,
                  updated_at = excluded.updated_at
                """,
                (old_label, new_label, "concept_rule", now_utc()),
            )
        return {"old": old_label, "new": new_label, "moved": 0, "deep_dive_updated": 0}

    moved = 0
    deep_dive_updated = 0
    if not dry_run:
        old_id = int(old_row[0])
        new_id = ensure_tag_id(conn, new_label)
        now = now_utc()
        conn.execute(
            """
            INSERT INTO tag_aliases(alias, canonical_label, confidence, approved, source_model, updated_at)
            VALUES (?, ?, 1.0, 1, ?, ?)
            ON CONFLICT(alias) DO UPDATE SET
              canonical_label = excluded.canonical_label,
              confidence = excluded.confidence,
              approved = excluded.approved,
              source_model = excluded.source_model,
              updated_at = excluded.updated_at
            """,
            (old_label, new_label, "concept_rule", now),
        )
        q_rows = conn.execute(
            "SELECT DISTINCT question_id FROM question_tags WHERE tag_id = ?",
            (old_id,),
        ).fetchall()
        for (question_id,) in q_rows:
            conn.execute(
                """
                INSERT OR IGNORE INTO question_tags(question_id, tag_id, source)
                VALUES (?, ?, ?)
                """,
                (question_id, new_id, "concept_rename"),
            )
            conn.execute(
                "DELETE FROM question_tags WHERE question_id = ? AND tag_id = ?",
                (question_id, old_id),
            )
            moved += 1
        dd_rows = conn.execute(
            """
            SELECT serial, tags_json
            FROM deep_dive_explanations
            WHERE tags_json IS NOT NULL AND tags_json != ''
            """
        ).fetchall()
        for serial, tags_json in dd_rows:
            try:
                raw_tags = json.loads(tags_json or "[]")
            except json.JSONDecodeError:
                raw_tags = []
            if not isinstance(raw_tags, list):
                continue
            converted = []
            changed = False
            for value in raw_tags:
                tag = normalize_label(value)
                if tag == old_label:
                    tag = new_label
                    changed = True
                if tag and tag not in converted:
                    converted.append(tag)
            if changed:
                conn.execute(
                    "UPDATE deep_dive_explanations SET tags_json = ? WHERE serial = ?",
                    (json.dumps(converted, ensure_ascii=False), serial),
                )
                deep_dive_updated += 1
        conn.execute("DELETE FROM tags WHERE id = ?", (old_id,))
    else:
        moved = conn.execute(
            "SELECT COUNT(DISTINCT question_id) FROM question_tags WHERE tag_id = ?",
            (int(old_row[0]),),
        ).fetchone()[0]

    return {
        "old": old_label,
        "new": new_label,
        "moved": int(moved),
        "deep_dive_updated": int(deep_dive_updated),
    }


def apply_unlink_alias_rule(conn, rule, dry_run=False):
    alias_label = normalize_label(rule.get("alias_label"))
    canonical_label = normalize_label(rule.get("canonical_label"))
    if not alias_label:
        return {"alias": "", "canonical": canonical_label, "deleted": 0}
    if canonical_label:
        row = conn.execute(
            "SELECT COUNT(*) FROM tag_aliases WHERE alias = ? AND canonical_label = ?",
            (alias_label, canonical_label),
        ).fetchone()
        deleted = int(row[0] or 0)
        if not dry_run and deleted:
            conn.execute(
                "DELETE FROM tag_aliases WHERE alias = ? AND canonical_label = ?",
                (alias_label, canonical_label),
            )
    else:
        row = conn.execute(
            "SELECT COUNT(*) FROM tag_aliases WHERE alias = ?",
            (alias_label,),
        ).fetchone()
        deleted = int(row[0] or 0)
        if not dry_run and deleted:
            conn.execute("DELETE FROM tag_aliases WHERE alias = ?", (alias_label,))
    return {"alias": alias_label, "canonical": canonical_label, "deleted": deleted}


def apply_related_rule(conn, rule, dry_run=False):
    left = normalize_label(rule.get("tag_label"))
    right = normalize_label(rule.get("related_tag_label"))
    relation_type = normalize_label(rule.get("relation_type")) or "related"
    bidirectional = bool(rule.get("bidirectional", True))
    if not left or not right:
        return {"left": left, "right": right, "inserted": 0}
    if dry_run:
        return {"left": left, "right": right, "inserted": 2 if bidirectional and left != right else 1}
    ensure_tag_id(conn, left)
    ensure_tag_id(conn, right)
    now = now_utc()
    inserted = 0
    conn.execute(
        """
        INSERT OR REPLACE INTO tag_relations(tag_label, related_tag_label, relation_type, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (left, right, relation_type, now),
    )
    inserted += 1
    if bidirectional and left != right:
        conn.execute(
            """
            INSERT OR REPLACE INTO tag_relations(tag_label, related_tag_label, relation_type, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (right, left, relation_type, now),
        )
        inserted += 1
    return {"left": left, "right": right, "inserted": inserted}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Apply tag concept split/rename/related rules to SQLite."
    )
    parser.add_argument("--db", default="output/ahaki.sqlite", help="SQLite file path.")
    parser.add_argument(
        "--rules",
        default="config/tag_concept_rules.json",
        help="JSON rules file path.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show expected changes only.")
    return parser.parse_args()


def main():
    args = parse_args()
    rules_path = Path(args.rules)
    if not rules_path.exists():
        raise SystemExit(f"rules file not found: {rules_path}")
    rules = json.loads(rules_path.read_text(encoding="utf-8"))
    subject_groups = rules.get("subject_groups") or {}
    split_rules = rules.get("split_rules") or []
    copy_rules = rules.get("copy_rules") or []
    rename_rules = rules.get("rename_rules") or []
    unlink_alias_rules = rules.get("unlink_alias_rules") or []
    related_rules = rules.get("related_rules") or []

    conn = sqlite3.connect(args.db)
    ensure_tables(conn)

    unlink_results = []
    for rule in unlink_alias_rules:
        unlink_results.append(apply_unlink_alias_rule(conn, rule, dry_run=args.dry_run))

    split_results = []
    for rule in split_rules:
        split_results.append(apply_split_rule(conn, rule, subject_groups, dry_run=args.dry_run))

    copy_results = []
    for rule in copy_rules:
        copy_results.append(apply_copy_rule(conn, rule, subject_groups, dry_run=args.dry_run))

    rename_results = []
    for rule in rename_rules:
        rename_results.append(apply_rename_rule(conn, rule, dry_run=args.dry_run))

    related_results = []
    for rule in related_rules:
        related_results.append(apply_related_rule(conn, rule, dry_run=args.dry_run))

    if args.dry_run:
        conn.rollback()
    else:
        conn.commit()
    conn.close()

    print("unlink_alias:")
    for item in unlink_results:
        print(f"  alias={item['alias']}, canonical={item['canonical']}: deleted={item['deleted']}")
    print("split:")
    for item in split_results:
        print(
            f"  {item['source']}: moved={item['moved']}, deep_dive_updated={item['deep_dive_updated']}, targets={item['targets']}"
        )
    print("copy:")
    for item in copy_results:
        print(f"  {item['source']} -> {item['target']}: copied={item['copied']}")
    print("rename:")
    for item in rename_results:
        print(
            f"  {item['old']} -> {item['new']}: moved={item['moved']}, deep_dive_updated={item['deep_dive_updated']}"
        )
    print("related:")
    for item in related_results:
        print(f"  {item['left']} <-> {item['right']}: inserted={item['inserted']}")
    print("done")


if __name__ == "__main__":
    main()
