import sqlite3
import unittest

from scripts.explanation_metadata import build_explanation_source, derive_explanation_metadata
from scripts.migrate_ai_fact_checked import migrate


class AiFactCheckedTests(unittest.TestCase):
    def test_sources_roundtrip_without_reinterpreting_teacher_statuses(self):
        for model in ["Gemini3Flash", "GPT5.5", "GPT-6 Astra"]:
            for status in ["ai", "ai_fact_checked", "teacher_approved", "teacher_edited"]:
                source = build_explanation_source(model, status)
                self.assertEqual(derive_explanation_metadata(source)["review_status"], status)
                self.assertEqual(derive_explanation_metadata(source)["model_name"], model)
        self.assertEqual(derive_explanation_metadata("GPT-6 Astra_ai_fact_checked")["review_status"], "ai_fact_checked")

    def test_migration_preserves_content_and_does_not_relabel_future_teacher_work(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE explanations(id INTEGER PRIMARY KEY, body TEXT, version INTEGER, source TEXT, model_name TEXT, review_status TEXT)")
        rows = [(1, "unchanged", 3, "llm", "Original", "teacher_approved"), (2, "corrected", 4, "teacher", "Original", "teacher_edited"), (3, "raw", 1, "llm", "Original", "ai")]
        conn.executemany("INSERT INTO explanations VALUES(?,?,?,?,?,?)", rows)
        self.assertEqual(migrate(conn), 2)
        self.assertEqual(conn.execute("SELECT id,body,version,model_name FROM explanations ORDER BY id").fetchall(), [(r[0],r[1],r[2],r[4]) for r in rows])
        self.assertEqual(conn.execute("SELECT review_status FROM explanations ORDER BY id").fetchall(), [("ai_fact_checked",),("ai_fact_checked",),("ai",)])
        conn.execute("INSERT INTO explanations VALUES(4,'human',5,'teacher','Original','teacher_edited')")
        self.assertEqual(migrate(conn), 0)
        self.assertEqual(conn.execute("SELECT review_status FROM explanations WHERE id=4").fetchone()[0], "teacher_edited")
        conn.close()


if __name__ == "__main__":
    unittest.main()
