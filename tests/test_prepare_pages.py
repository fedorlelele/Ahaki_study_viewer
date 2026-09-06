import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import prepare_pages as pages


class PreparePagesTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.web = self.root / "output/web"
        self.web.mkdir(parents=True)
        (self.root / "web_app").mkdir()
        (self.root / "web_app/index.html").write_text("new viewer")
        (self.root / "web_app/config.js").write_text("private fixture configuration")
        (self.root / "docs").mkdir()
        (self.root / "docs/old.txt").write_bytes(b"previous published materials")
        self.rows = [{"serial": f"A01-00{i}", "stem": f"question {i}"} for i in range(1, 4)]
        self.write_json("questions.json", self.rows)
        self.write_json("questions/part.json", self.rows)
        self.write_json("questions_manifest.json", {"total": 3, "chunks": [{
            "path": "questions/part.json", "index": 1, "count": 3,
            "first_serial": "A01-001", "last_serial": "A01-003",
        }]})
        for name in ("index_by_subject", "index_by_subtopic", "index_by_tag"):
            self.write_json(f"index/{name}.json", {"category": [row["serial"] for row in self.rows]})
        for name in ("index/tag_catalog.json", "index/tag_catalog_light.json", "update_log.json"):
            self.write_json(name, [])
        self.write_json("index/question_override_versions.json", {"A01-001": "2026-09-06T00:00:00Z"})

    def write_json(self, name, value):
        path = self.web / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def assert_old_docs(self):
        self.assertEqual((self.root / "docs/old.txt").read_bytes(), b"previous published materials")
        self.assertEqual(list(self.root.glob(".docs-stage-*")), [])
        self.assertEqual(list(self.root.glob(".docs-backup-*")), [])

    def database(self, count):
        path = self.root / "output/ahaki.sqlite"
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE questions(serial TEXT)")
            connection.executemany("INSERT INTO questions VALUES(?)", [(row["serial"],) for row in self.rows[:count]])
        return path

    def run_cli(self, *args):
        return subprocess.run([sys.executable, "-B", pages.__file__, "--root", str(self.root), *map(str, args)], capture_output=True, text=True)

    def test_success_replaces_whole_tree_after_validation(self):
        mode = (self.root / "docs").stat().st_mode & 0o777
        pages.prepare_pages(self.root, 3)
        self.assertFalse((self.root / "docs/old.txt").exists())
        self.assertEqual((self.root / "docs/web_app/config.js").read_bytes(), (self.root / "web_app/config.js").read_bytes())
        pages.validate_pages(self.root / "docs", 3)
        self.assertEqual((self.root / "docs").stat().st_mode & 0o777, mode)
        self.assertEqual(list(self.root.glob(".docs-*-*")), [])

    def test_missing_chunk_leaves_old_materials(self):
        (self.web / "questions/part.json").unlink()
        with self.assertRaises(FileNotFoundError):
            pages.prepare_pages(self.root, 3)
        self.assert_old_docs()

    def test_stale_chunk_with_same_count_leaves_old_materials(self):
        rows = [dict(row) for row in self.rows]
        rows[1]["stem"] = "outdated explanation"
        self.write_json("questions/part.json", rows)
        with self.assertRaisesRegex(ValueError, "contents/order"):
            pages.prepare_pages(self.root, 3)
        self.assert_old_docs()

    def test_partial_copy_failure_leaves_old_materials(self):
        original = pages.shutil.copytree
        def copy(source, destination, *args, **kwargs):
            if Path(source).resolve() == self.web.resolve():
                Path(destination).mkdir(parents=True)
                raise OSError("injected disk failure")
            return original(source, destination, *args, **kwargs)
        with patch.object(pages.shutil, "copytree", side_effect=copy), self.assertRaisesRegex(OSError, "disk failure"):
            pages.prepare_pages(self.root, 3)
        self.assert_old_docs()

    def test_failed_publication_restores_previous_directory(self):
        original = pages.os.replace
        for failure in (OSError("injected rename failure"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__):
                def replace(source, destination):
                    if Path(source).name.startswith(".docs-stage-"):
                        raise failure
                    return original(source, destination)
                with patch.object(pages.os, "replace", side_effect=replace), self.assertRaises(type(failure)):
                    pages.prepare_pages(self.root, 3)
                self.assert_old_docs()

    def test_cli_uses_default_database_count_without_changing_database(self):
        db = self.database(3)
        before = db.read_bytes()
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(db.read_bytes(), before)
        pages.validate_pages(self.root / "docs", 3)

    def test_database_count_mismatch_leaves_old_materials(self):
        db = self.database(2)
        result = self.run_cli("--db", db)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Question count must be 2", result.stderr)
        self.assert_old_docs()

    def test_missing_database_does_not_create_db_or_change_materials(self):
        db = self.root / "missing.sqlite"
        result = self.run_cli("--db", db)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(db.exists())
        self.assert_old_docs()

    def test_explicit_count_can_be_used_without_a_database(self):
        result = self.run_cli("--expected-count", "3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "output/ahaki.sqlite").exists())
        pages.validate_pages(self.root / "docs", 3)

    def test_shell_entrypoint_forwards_count_and_root_options(self):
        result = subprocess.run(["bash", str(Path(pages.__file__).with_suffix(".sh")), "--root", str(self.root), "--expected-count", "3"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        pages.validate_pages(self.root / "docs", 3)

    def test_override_versions_must_reference_existing_serials(self):
        self.write_json("index/question_override_versions.json", {"A99-999": "timestamp"})
        with self.assertRaisesRegex(ValueError, "Unknown question serials"):
            pages.prepare_pages(self.root, 3)
        self.assert_old_docs()

    def test_override_versions_is_a_required_json_object(self):
        path = self.web / "index/question_override_versions.json"
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            pages.prepare_pages(self.root, 3)
        self.assert_old_docs()
        self.write_json("index/question_override_versions.json", [])
        with self.assertRaisesRegex(ValueError, "Invalid JSON structure"):
            pages.prepare_pages(self.root, 3)
        self.assert_old_docs()

    def test_required_json_corruption_leaves_old_materials(self):
        (self.web / "index/tag_catalog_light.json").write_text("{")
        with self.assertRaises(ValueError):
            pages.prepare_pages(self.root, 3)
        self.assert_old_docs()


if __name__ == "__main__":
    unittest.main()
