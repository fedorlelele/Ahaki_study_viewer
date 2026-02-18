import json
import tempfile
import unittest
from pathlib import Path

from scripts.generate_web_json import (
    build_question_chunks,
    normalize_date_key,
    parse_answer_text,
    write_question_chunks,
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


if __name__ == "__main__":
    unittest.main()
