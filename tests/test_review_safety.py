import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import local_admin_app as admin
from scripts.backup_database import backup_database
from scripts.generate_web_json import load_override_versions, load_explanations
from scripts.question_contract import resolve_question_answers, QuestionContractError


class AnswerContractTests(unittest.TestCase):
    def record(self, **kwargs):
        return {"serial": "B20-095", "choices": ["a", "b", "c", "d"], **kwargs}

    def test_media_answers_remain_distinct(self):
        result = resolve_question_answers(self.record(answer_index=1, answer_indices_json="[1,2]", answer_text="解答　１．(点字：１．２．)"))
        self.assertEqual(result["answer_indices"], [1])
        self.assertEqual(result["answer_variants"], {"default": [1], "braille": [1, 2]})
        self.assertEqual(result["answer_notes"], "(点字：１．２．)")

    def test_invalid_single_is_not_hidden_by_valid_array(self):
        with self.assertRaisesRegex(QuestionContractError, "33"):
            resolve_question_answers(self.record(answer_index=33, answer_indices_json="[3]"))

    def test_none_and_numeric_answer_conflict(self):
        with self.assertRaises(QuestionContractError):
            resolve_question_answers(self.record(answer_none=1, answer_indices=[2]))

    def test_exported_contract_is_idempotent(self):
        record = self.record(answer_index=2, answer_indices=[2, 4], answer_text="解答２・４")
        first = resolve_question_answers(record)
        self.assertEqual(resolve_question_answers({**record, **first}), first)

    def test_braille_contract_roundtrip(self):
        record = self.record(answer_index=1, answer_indices=[1, 2], answer_text="解答１（点字：１・２）")
        first = resolve_question_answers(record)
        self.assertEqual(resolve_question_answers({**record, **first}), first)


class TemporaryDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Path(self.directory.name) / "test.sqlite"
        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
              CREATE TABLE questions(id INTEGER PRIMARY KEY, serial TEXT UNIQUE, stem TEXT);
              CREATE TABLE explanations(id INTEGER PRIMARY KEY, question_id INTEGER,
                body TEXT, version INTEGER, source TEXT, model_name TEXT, review_status TEXT);
              INSERT INTO questions VALUES(1,'A01-001','old');
              INSERT INTO explanations VALUES(1,1,'old explanation',2,'model:OriginalModel','OriginalModel','ai');
            """)

    def test_reimport_becomes_latest_and_preserves_generator(self):
        path = Path(self.directory.name) / "import.jsonl"
        path.write_text(json.dumps({"serial": "A01-001", "explanation": "correction", "source": "teacher"}) + "\n")
        result = subprocess.run([sys.executable, "-B", "scripts/import_explanations.py", "--db", str(self.db), "--infile", str(path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        with sqlite3.connect(self.db) as conn:
            row = conn.execute("SELECT version,body,model_name,review_status FROM explanations ORDER BY version DESC LIMIT 1").fetchone()
            self.assertEqual(row, (3, "correction", "OriginalModel", "teacher_edited"))
            self.assertEqual(sorted(load_explanations(conn)[1], key=lambda item: item["version"])[-1]["body"], "correction")

    def test_stale_explicit_version_is_rejected_without_partial_import(self):
        path = Path(self.directory.name) / "import.jsonl"
        path.write_text(json.dumps({"serial": "A01-001", "explanation": "correction"}) + "\n")
        result = subprocess.run([sys.executable, "-B", "scripts/import_explanations.py", "--db", str(self.db), "--infile", str(path), "--version", "1"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM explanations").fetchone()[0], 1)

    def test_cloud_teacher_edit_updates_status_and_preserves_generator(self):
        with sqlite3.connect(self.db) as conn:
            self.assertTrue(admin.apply_override_explanation(conn.cursor(), 1, "teacher correction", "teacher"))
            self.assertEqual(conn.execute("SELECT body,model_name,review_status FROM explanations ORDER BY version DESC LIMIT 1").fetchone(), ("teacher correction", "OriginalModel", "teacher_edited"))

    def test_sync_records_receipt_without_hiding_cloud_override(self):
        row = {"serial": "A01-001", "stem": "corrected", "updated_at": "2026-09-06T00:00:00Z"}
        with patch.object(admin, "fetch_supabase_overrides", return_value=([row], "")), patch.object(admin, "mark_supabase_overrides_synced") as mark:
            result = admin.sync_supabase_overrides(self.db, "")
            mark.assert_not_called()
            self.assertEqual(result["counts"]["stem"], 1)
            repeated = admin.sync_supabase_overrides(self.db, "")
            self.assertEqual(repeated["counts"]["stem"], 0)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(load_override_versions(conn), {"A01-001": row["updated_at"]})
            self.assertEqual(conn.execute("SELECT stem FROM questions").fetchone()[0], "corrected")

    def test_cli_preserves_model_explicitly_named_in_source(self):
        path = Path(self.directory.name) / "import.jsonl"
        path.write_text(json.dumps({"serial": "A01-001", "explanation": "new reviewed text", "source": "model:NewModel:checked"}) + "\n")
        result = subprocess.run([sys.executable, "-B", "scripts/import_explanations.py", "--db", str(self.db), "--infile", str(path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT source,model_name,review_status FROM explanations ORDER BY version DESC LIMIT 1").fetchone(),
                             ("model:NewModel:checked", "NewModel", "teacher_approved"))

    def test_local_import_preserves_named_source_model_and_inherits_only_generic_markers(self):
        for source, expected_model, expected_source in [
            ("llm_checked", "OriginalModel", "model:OriginalModel:checked"),
            ("model:NewModel:checked", "NewModel", "model:NewModel:checked"),
        ]:
            with self.subTest(source=source):
                record = {"serial": "A01-001", "explanation": "reviewed " + source, "source": source}
                with patch.object(admin, "clear_feedback_flag"), patch.object(admin, "clear_supabase_feedback"):
                    self.assertEqual(admin.import_explanations(self.db, json.dumps(record), "append", None), 1)
                with sqlite3.connect(self.db) as conn:
                    self.assertEqual(conn.execute("SELECT source,model_name,review_status FROM explanations ORDER BY version DESC LIMIT 1").fetchone(),
                                     (expected_source, expected_model, "teacher_approved"))

    def test_cloud_review_preserves_explicit_source_model(self):
        with sqlite3.connect(self.db) as conn:
            self.assertTrue(admin.apply_override_explanation(conn.cursor(), 1, "reviewed by new model", "model:NewModel:checked"))
            self.assertEqual(conn.execute("SELECT source,model_name,review_status FROM explanations ORDER BY version DESC LIMIT 1").fetchone(),
                             ("model:NewModel:checked", "NewModel", "teacher_approved"))

    def test_empty_explanation_is_saved_before_sync_receipt_and_survives_export(self):
        row = {"serial": "A01-001", "explanation": "", "updated_at": "2026-09-06T00:00:00Z"}
        with patch.object(admin, "fetch_supabase_overrides", return_value=([row], "")):
            self.assertEqual(admin.sync_supabase_overrides(self.db, "")["counts"]["explanations"], 1)
            self.assertEqual(admin.sync_supabase_overrides(self.db, "")["counts"]["explanations"], 0)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT body,version FROM explanations ORDER BY version DESC LIMIT 1").fetchone(), ("", 3))
            self.assertEqual(sorted(load_explanations(conn)[1], key=lambda item: item["version"])[-1]["body"], "")
            self.assertEqual(load_override_versions(conn), {"A01-001": row["updated_at"]})

    def test_false_none_without_numeric_override_retains_existing_answer(self):
        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
                ALTER TABLE questions ADD COLUMN answer_index INTEGER;
                ALTER TABLE questions ADD COLUMN answer_indices_json TEXT;
                ALTER TABLE questions ADD COLUMN answer_none INTEGER;
                ALTER TABLE questions ADD COLUMN answer_text TEXT;
                UPDATE questions SET answer_index=2,answer_indices_json='[2]',answer_none=0,answer_text='解答２';
            """)
            changed = admin.apply_override_question_fields(conn.cursor(), 1, {"answer_none": False, "answer_indices": None, "answer_index": None})
            self.assertFalse(changed["answers"])
            self.assertEqual(conn.execute("SELECT answer_index,answer_indices_json,answer_none,answer_text FROM questions").fetchone(), (2, "[2]", 0, "解答２"))
            changed = admin.apply_override_question_fields(conn.cursor(), 1, {"answer_none": True})
            self.assertTrue(changed["answers"])
            self.assertEqual(conn.execute("SELECT answer_index,answer_indices_json,answer_none FROM questions").fetchone(), (None, "[]", 1))

    def test_first_sync_rejects_invalid_or_timezone_free_timestamps_without_writes(self):
        for value in [None, "", "invalid", "2026-09-06", "2026-09-06T01:00:00", 123]:
            with self.subTest(value=value), patch.object(admin, "fetch_supabase_overrides", return_value=([{"serial": "A01-001", "stem": "bad update", "updated_at": value}], "")):
                with self.assertRaisesRegex(ValueError, "更新日時"):
                    admin.sync_supabase_overrides(self.db, "")
                with sqlite3.connect(self.db) as conn:
                    self.assertEqual(conn.execute("SELECT stem FROM questions").fetchone()[0], "old")
                    self.assertEqual(load_override_versions(conn), {})

    def test_bad_stored_receipt_rolls_back_the_batch_and_releases_write_lock(self):
        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
                INSERT INTO questions VALUES(2,'A01-002','second old');
                CREATE TABLE question_override_sync(serial TEXT PRIMARY KEY,override_updated_at TEXT NOT NULL);
                INSERT INTO question_override_sync VALUES('A01-002','invalid');
            """)
        rows = [{"serial": serial, "stem": "new", "updated_at": "2026-09-06T00:00:00Z"} for serial in ["A01-001", "A01-002"]]
        with patch.object(admin, "fetch_supabase_overrides", return_value=(rows, "")):
            with self.assertRaisesRegex(ValueError, "更新日時"):
                admin.sync_supabase_overrides(self.db, "")
        with sqlite3.connect(self.db, timeout=0.1) as conn:
            self.assertEqual(conn.execute("SELECT stem FROM questions ORDER BY id").fetchall(), [("old",), ("second old",)])
            self.assertEqual(load_override_versions(conn), {"A01-002": "invalid"})
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE questions SET stem='retry works' WHERE id=1")

    def test_backup_includes_committed_wal_changes(self):
        conn = sqlite3.connect(self.db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("UPDATE questions SET stem='committed in WAL'")
        conn.commit()
        try:
            output = Path(self.directory.name) / "backup.sqlite"
            backup_database(self.db, output)
            with sqlite3.connect(output) as copy:
                self.assertEqual(copy.execute("SELECT stem FROM questions").fetchone()[0], "committed in WAL")
            with self.assertRaises(FileExistsError):
                backup_database(self.db, output)
        finally:
            conn.close()


class LocalAdminBoundaryTests(unittest.TestCase):
    def handler(self, headers=None, body=b"{}"):
        handler = admin.Handler.__new__(admin.Handler)
        handler.server = SimpleNamespace(server_port=8001, allowed_hosts={"127.0.0.1", "localhost"}, admin_token="test-session-token")
        handler.headers = {"Host": "127.0.0.1:8001", "Content-Type": "application/json", "Content-Length": str(len(body)), **(headers or {})}
        handler.path = "/api/admin/role"
        handler.rfile, handler.wfile = io.BytesIO(body), io.BytesIO()
        handler.statuses = []
        handler.sent_headers = {}
        handler.send_response = handler.statuses.append
        handler.send_header = lambda key, value: handler.sent_headers.update({key: value})
        handler.end_headers = lambda: None
        return handler

    def test_foreign_origin_cannot_mutate(self):
        handler = self.handler({"Origin": "https://foreign.invalid", "X-Ahaki-Admin-Token": "test-session-token"})
        with patch.object(handler, "_handle_post") as mutate:
            handler.do_POST()
            mutate.assert_not_called()
        self.assertEqual(handler.statuses, [403])
        self.assertNotIn("Access-Control-Allow-Origin", handler.sent_headers)

    def test_host_rebinding_is_rejected(self):
        handler = self.handler({"Host": "foreign.invalid:8001", "X-Ahaki-Admin-Token": "test-session-token"})
        self.assertFalse(handler._require_local_request())

    def test_token_required_even_without_origin_header(self):
        handler = self.handler()
        with patch.object(handler, "_handle_post") as mutate:
            handler.do_POST()
            mutate.assert_not_called()
        self.assertEqual(handler.statuses, [403])

    def test_valid_same_origin_request_works(self):
        handler = self.handler({"Origin": "http://127.0.0.1:8001", "X-Ahaki-Admin-Token": "test-session-token"})
        with patch.object(handler, "_handle_post") as mutate:
            handler.do_POST()
            mutate.assert_called_once()

    def test_plain_text_post_is_rejected(self):
        handler = self.handler({"Content-Type": "text/plain", "X-Ahaki-Admin-Token": "test-session-token"})
        handler.do_POST()
        self.assertEqual(handler.statuses, [415])

    def test_actor_id_is_not_taken_from_request_body(self):
        handler = self.handler({"X-Ahaki-Admin-Token": "test-session-token"}, json.dumps({"user_id": "user", "role": "admin", "actor_id": "spoofed"}).encode())
        with patch.object(admin, "set_admin_role", return_value={"ok": True}) as mutate:
            handler.do_POST()
            mutate.assert_called_once_with("user", "admin", None)

    def test_report_post_keeps_cloud_destination_and_comment(self):
        body = {"serial": "A01-001", "kind": "explanation", "comment": "説明を確認してください"}
        handler = self.handler({"X-Ahaki-Admin-Token": "test-session-token"}, json.dumps(body).encode())
        handler.path = "/api/report"
        with patch.object(admin, "add_report_supabase", return_value={"message": "reported"}) as cloud, patch.object(admin, "add_report") as local:
            handler.do_POST()
            cloud.assert_called_once_with(body["serial"], body["kind"], body["comment"])
            local.assert_not_called()
        self.assertEqual(handler.statuses, [200])
        self.assertEqual(json.loads(handler.wfile.getvalue()), {"message": "reported"})
