import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.generate_web_json import (
    build_question_chunks,
    load_explanations,
    normalize_date_key,
    parse_answer_text,
    write_question_chunks,
)
from scripts.explanation_metadata import (
    build_explanation_source,
    derive_explanation_metadata,
    ensure_explanation_metadata_schema,
)


class GenerateWebJsonTests(unittest.TestCase):
    def test_parse_answer_text(self):
        indices, answer_none = parse_answer_text("正解は１と３")
        self.assertEqual(indices, [1, 3])
        self.assertFalse(answer_none)

        indices, answer_none = parse_answer_text("正解なし")
        self.assertEqual(indices, [])
        self.assertTrue(answer_none)

        indices, answer_none = parse_answer_text("すべて")
        self.assertEqual(indices, [1, 2, 3, 4])
        self.assertFalse(answer_none)

        indices, answer_none = parse_answer_text("２，４")
        self.assertEqual(indices, [2, 4])
        self.assertFalse(answer_none)

    def test_build_question_chunks(self):
        rows = [{"serial": f"A01-00{i}"} for i in range(1, 6)]
        chunks = build_question_chunks(rows, 2)
        self.assertEqual([len(c) for c in chunks], [2, 2, 1])
        self.assertEqual(chunks[0][0]["serial"], "A01-001")
        self.assertEqual(chunks[-1][-1]["serial"], "A01-005")

    def test_write_question_chunks(self):
        rows = [{"serial": f"A01-00{i}", "stem": f"q{i}"} for i in range(1, 6)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = write_question_chunks(rows, root, "questions", 2)
            self.assertIsNotNone(manifest_path)
            manifest = json.loads((root / "questions_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["total"], 5)
            self.assertEqual(manifest["chunk_size"], 2)
            self.assertEqual(len(manifest["chunks"]), 3)
            self.assertEqual(manifest["chunks"][0]["first_serial"], "A01-001")
            self.assertEqual(manifest["chunks"][2]["last_serial"], "A01-005")
            chunk_files = sorted((root / "questions").glob("questions_*.json"))
            self.assertEqual(len(chunk_files), 3)
            first_chunk = json.loads(chunk_files[0].read_text(encoding="utf-8"))
            self.assertEqual(len(first_chunk), 2)

    def test_normalize_date_key(self):
        self.assertEqual(normalize_date_key("2026/2/5"), "20260205")
        self.assertEqual(normalize_date_key("2026-02-14"), "20260214")

    def test_explanation_metadata_from_legacy_source(self):
        meta = derive_explanation_metadata("llm_checked")
        self.assertEqual(meta["model_name"], "Gemini3Flash")
        self.assertEqual(meta["review_status"], "teacher_approved")

        meta = derive_explanation_metadata("codex_case_text_rewrite_20260616")
        self.assertEqual(meta["model_name"], "GPT5.5")
        self.assertEqual(meta["review_status"], "ai")

    def test_explanation_metadata_for_arbitrary_model_source(self):
        source = build_explanation_source("Claude 3.5 Sonnet", "teacher_edited")
        self.assertEqual(source, "model:Claude 3.5 Sonnet:teacher")
        meta = derive_explanation_metadata(source)
        self.assertEqual(meta["model_name"], "Claude 3.5 Sonnet")
        self.assertEqual(meta["review_status"], "teacher_edited")

    def test_load_explanations_exports_model_and_review_status(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE explanations(
                id INTEGER PRIMARY KEY,
                question_id INTEGER NOT NULL,
                body TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                source TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO explanations(question_id, body, version, source)
            VALUES (1, 'body', 1, 'llm_checked')
            """
        )
        ensure_explanation_metadata_schema(conn)

        explanations = load_explanations(conn)
        self.assertEqual(explanations[1][0]["model_name"], "Gemini3Flash")
        self.assertEqual(explanations[1][0]["review_status"], "teacher_approved")


if __name__ == "__main__":
    unittest.main()
