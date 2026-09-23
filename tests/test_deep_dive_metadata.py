import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.deep_dive_metadata import migrate
from scripts.generate_web_json import load_deep_dive
from scripts.run_gemini_deep_dive import ensure_deep_dive_table, upsert_deep_dive
from scripts.sync_deep_dive_to_supabase import fetch_local_rows
import local_admin_app as admin


class DeepDiveMetadataTests(unittest.TestCase):
    def test_legacy_migration_preserves_content_and_existing_reviews_on_repeat(self):
        with sqlite3.connect(':memory:') as conn:
            conn.execute('CREATE TABLE deep_dive_explanations(serial TEXT PRIMARY KEY, explanation TEXT, tags_json TEXT, updated_at TEXT, created_by TEXT)')
            original = ('A01-001', '既存の本文', '["タグ"]', '2026-09-01T00:00:00Z', 'author')
            conn.execute('INSERT INTO deep_dive_explanations VALUES (?,?,?,?,?)', original)
            self.assertEqual(load_deep_dive(conn)['A01-001']['model_name'], 'Gemini 3 Flash')
            self.assertEqual(migrate(conn), 1)
            conn.execute("UPDATE deep_dive_explanations SET model_name='GPT5.5', review_status='ai_fact_checked'")
            self.assertEqual(migrate(conn), 0)
            self.assertEqual(conn.execute('SELECT serial, explanation, tags_json, updated_at, created_by FROM deep_dive_explanations').fetchone(), original)
            record = load_deep_dive(conn)['A01-001']
            self.assertEqual((record['model_name'], record['review_status']), ('GPT5.5', 'ai_fact_checked'))

    def test_generation_export_and_both_sync_paths_preserve_actual_model_and_review(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'questions.sqlite'
            with sqlite3.connect(db) as conn:
                ensure_deep_dive_table(conn)
                upsert_deep_dive(conn, 'A01-001', '生成本文', ['タグ'], '', 'gemini-3.7-flash')
                generated = load_deep_dive(conn)['A01-001']
                self.assertEqual((generated['model_name'], generated['review_status']), ('gemini-3.7-flash', 'ai'))
            cloud = dict(serial='A01-001', explanation='教師の修正', tags=['タグ'], updated_at='2026-09-23T01:00:00Z', model_name='gemini-3.7-flash', review_status='teacher_edited')
            with patch.object(admin, 'fetch_supabase_deep_dive', return_value=([cloud], '')):
                admin.sync_supabase_deep_dive(db, '')
            args = SimpleNamespace(db=db, since='', serials='', include_empty=False, limit=0, created_by='', set_updated_now=False)
            for rows in (fetch_local_rows(args), admin.load_local_deep_dive_rows(db)):
                self.assertEqual(rows[0]['model_name'], cloud['model_name'])
                self.assertEqual(rows[0]['review_status'], 'teacher_edited')
                self.assertEqual(rows[0]['explanation'], '教師の修正')
            with sqlite3.connect(db) as conn:
                upsert_deep_dive(conn, 'A01-001', '再生成', [], '', 'new-model')
                self.assertEqual(load_deep_dive(conn)['A01-001']['review_status'], 'ai')

    def test_unmigrated_upload_does_not_invent_or_overwrite_review_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'legacy.sqlite'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE deep_dive_explanations(serial TEXT, explanation TEXT, tags_json TEXT, updated_at TEXT)')
                conn.execute("INSERT INTO deep_dive_explanations VALUES ('A01-001','本文','[]','2026-09-01T00:00:00Z')")
            args = SimpleNamespace(db=db, since='', serials='', include_empty=False, limit=0, created_by='', set_updated_now=False)
            for row in (fetch_local_rows(args)[0], admin.load_local_deep_dive_rows(db)[0]):
                self.assertNotIn('review_status', row)
                self.assertNotIn('model_name', row)
