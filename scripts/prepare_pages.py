#!/usr/bin/env python3
"""Validate a sibling staging tree before replacing the local Pages materials."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import tempfile


INDEX_HTML = """<!doctype html>
<html lang="ja">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Ahaki Study Viewer</title>
  </head>
  <body>
    <h1>Ahaki Study Viewer</h1>
    <p><a href="./web_app/">WebUIを開く</a></p>
  </body>
</html>
"""


def read_json(path: Path, expected_type):
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, expected_type):
        raise ValueError(f"Invalid JSON structure: {path}")
    return value


def question_count_from_db(db_path: Path) -> int:
    with sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        return connection.execute("SELECT COUNT(*) FROM questions").fetchone()[0]


def validate_pages(stage: Path, expected_count: int):
    if type(expected_count) is not int or expected_count < 1:
        raise ValueError("Expected question count must be a positive integer")
    for name in ("index.html", "web_app/index.html"):
        path = stage / name
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"Missing or empty Pages entry: {path}")
    web = stage / "output/web"
    questions = read_json(web / "questions.json", list)
    manifest = read_json(web / "questions_manifest.json", dict)
    if len(questions) != expected_count or manifest.get("total") != expected_count:
        raise ValueError(f"Question count must be {expected_count} in questions.json and manifest")
    serials = [row.get("serial") if isinstance(row, dict) else None for row in questions]
    if any(not isinstance(value, str) or not value for value in serials) or len(set(serials)) != expected_count:
        raise ValueError("Question serials must be present and unique")
    chunks = manifest.get("chunks")
    if not isinstance(chunks, list) or not chunks:
        raise ValueError("Question manifest has no chunks")
    offset, paths = 0, set()
    for index, item in enumerate(chunks, 1):
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise ValueError("Invalid question chunk entry")
        rel = Path(item["path"])
        path = (web / rel).resolve()
        if rel.is_absolute() or ".." in rel.parts or not path.is_relative_to(web.resolve()) or path in paths:
            raise ValueError(f"Unsafe or duplicate chunk path: {rel}")
        paths.add(path)
        rows = read_json(path, list)
        if not rows or item.get("index") != index or item.get("count") != len(rows):
            raise ValueError(f"Question chunk index/count mismatch: {rel}")
        if rows != questions[offset:offset + len(rows)]:
            raise ValueError(f"Question chunk contents/order mismatch: {rel}")
        if item.get("first_serial") != rows[0]["serial"] or item.get("last_serial") != rows[-1]["serial"]:
            raise ValueError(f"Question chunk serial range mismatch: {rel}")
        offset += len(rows)
    if offset != expected_count:
        raise ValueError(f"Question chunks contain {offset} records, expected {expected_count}")
    serial_set = set(serials)
    override_versions = read_json(web / "index/question_override_versions.json", dict)
    if set(override_versions) - serial_set:
        raise ValueError("Unknown question serials in question_override_versions.json")
    for name in ("index_by_subject", "index_by_subtopic", "index_by_tag"):
        catalog = read_json(web / "index" / f"{name}.json", dict)
        referenced = set()
        for values in catalog.values():
            if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                raise ValueError(f"Invalid serial list in {name}")
            referenced.update(values)
        if referenced - serial_set or (name == "index_by_subject" and referenced != serial_set):
            raise ValueError(f"Question references do not match in {name}")
    for name in ("tag_catalog", "tag_catalog_light"):
        read_json(web / "index" / f"{name}.json", list)
    read_json(web / "update_log.json", list)
    config = stage / "config/subtopics_catalog.json"
    if config.exists():
        read_json(config, dict)


def publish_stage(stage: Path, docs: Path):
    """Keep the old directory available for rollback until both renames finish."""
    backup = Path(tempfile.mkdtemp(prefix=".docs-backup-", dir=docs.parent))
    backup.rmdir()
    try:
        if docs.exists():
            os.replace(docs, backup)
        os.replace(stage, docs)
    except BaseException:
        if backup.exists():
            try:
                if docs.exists():
                    os.replace(docs, stage)
                os.replace(backup, docs)
            except BaseException as restore_error:
                raise RuntimeError(f"Pages rollback failed; previous materials remain at {backup}") from restore_error
        raise
    if backup.exists():
        # Publication is complete. A cleanup error must not discard the backup.
        try:
            shutil.rmtree(backup)
        except OSError:
            print(f"Docs prepared; old backup retained: {backup}")


def prepare_pages(root: Path, expected_count: int):
    root = root.resolve()
    docs = root / "docs"
    if docs.is_symlink() or (docs.exists() and not docs.is_dir()):
        raise ValueError(f"Pages destination must be a directory: {docs}")
    with tempfile.TemporaryDirectory(prefix=".docs-stage-", dir=root) as directory:
        stage = Path(directory)
        ignored = shutil.ignore_patterns(".DS_Store", "__pycache__", "*.pyc")
        shutil.copytree(root / "web_app", stage / "web_app", ignore=ignored)
        shutil.copytree(root / "output/web", stage / "output/web", ignore=ignored)
        (stage / "config").mkdir()
        config = root / "config/subtopics_catalog.json"
        if config.is_file():
            shutil.copy2(config, stage / "config" / config.name)
        (stage / "index.html").write_text(INDEX_HTML, encoding="utf-8")
        validate_pages(stage, expected_count)
        stage.chmod(docs.stat().st_mode & 0o777 if docs.exists() else 0o755)
        publish_stage(stage, docs)
    return docs


def interrupted(signum, frame):
    raise InterruptedError(f"Pages preparation interrupted by signal {signum}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--db", type=Path, help="Read-only question database (default: <root>/output/ahaki.sqlite)")
    parser.add_argument("--expected-count", type=int, help="Explicit expected question count instead of reading the DB")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    try:
        expected_count = args.expected_count
        if expected_count is None:
            expected_count = question_count_from_db(args.db or args.root / "output/ahaki.sqlite")
        docs = prepare_pages(args.root, expected_count)
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        parser.exit(1, f"Pages preparation failed: {exc}\n")
    print(f"Docs prepared: {docs}")


if __name__ == "__main__":
    main()
