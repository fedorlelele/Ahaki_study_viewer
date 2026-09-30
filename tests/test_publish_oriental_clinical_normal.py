import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / 'scripts/publish_oriental_clinical_normal.py'
spec = importlib.util.spec_from_file_location('normal_publication', MODULE)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class NormalPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.db = self.directory / 'fixture.sqlite'
        with sqlite3.connect(self.db) as c:
            c.executescript('''CREATE TABLE questions(id INTEGER PRIMARY KEY,serial TEXT,answer_index INTEGER,answer_indices_json TEXT,answer_none INTEGER,answer_text TEXT,raw_text TEXT);
                CREATE TABLE explanations(id INTEGER PRIMARY KEY,question_id INTEGER,body TEXT,version INTEGER,source TEXT,model_name TEXT,review_status TEXT);
                CREATE TABLE deep_dive_explanations(serial TEXT,explanation TEXT);
                INSERT INTO questions VALUES(1,'A33-125',4,'[4]',0,'解答　４．','設問\n解答　４．');
                INSERT INTO questions VALUES(2,'B01-001',1,'[1]',0,'解答　１．','別問\n解答　１．');
                INSERT INTO explanations VALUES(1,1,'old',1,'llm','Gemini3Flash','ai');
                INSERT INTO explanations VALUES(2,2,'unrelated',1,'llm','Gemini3Flash','ai');
                INSERT INTO deep_dive_explanations VALUES('A33-125','preserve this deep dive');''')
        c = p.connect(self.db)
        q = dict(c.execute('SELECT * FROM questions WHERE id=1').fetchone())
        corrected = dict(q, answer_index=3, answer_indices_json='[3]', answer_text='解答　３．', raw_text='設問\n解答　３．')
        for file in ('release.jsonl', 'inputs.jsonl'):
            (self.directory / file).write_text('{}')
        self.plan = dict(db=str(self.db), source_tag='unique_release_test', rows={'A33-125': dict(explanation='new', model_name='GPT-6.1 Sol', review_status='ai_fact_checked')},
            inputs={'A33-125': dict(question=q, latest_explanation=p.latest(c, 1))}, corrections={'A33-125': corrected}, bound_files={},
            release_path=str(self.directory/'release.jsonl'), inputs_path=str(self.directory/'inputs.jsonl'), release_sha256=p.sha('{}'), inputs_sha256=p.sha('{}'), backup_path='fixture')
        self.plan['protected_hashes'] = p.protected_hashes(c, self.plan)
        c.close()
        self.path = self.directory / 'plan.json'
        p.write(self.path, self.plan)

    def tearDown(self): self.temp.cleanup()

    def test_atomic_answer_and_normal_version_and_idempotency(self):
        p.apply_local(self.path)
        p.apply_local(self.path)
        c = p.connect(self.db)
        self.assertEqual(c.execute('SELECT COUNT(*) FROM explanations').fetchone()[0], 3)
        self.assertEqual(p.latest(c, 1)['version'], 2)
        self.assertEqual(c.execute('SELECT answer_index FROM questions WHERE id=1').fetchone()[0], 3)
        self.assertEqual(c.execute('SELECT explanation FROM deep_dive_explanations').fetchone()[0], 'preserve this deep dive')
        self.assertEqual(p.latest(c, 2)['body'], 'unrelated')
        c.close()
        self.assertTrue(p.verify_local(p.load_plan(self.path)))

    def test_concurrent_unrelated_change_prevents_any_insert(self):
        c = p.connect(self.db)
        c.execute("UPDATE explanations SET body='other editor' WHERE id=2"); c.commit(); c.close()
        with self.assertRaises(p.SafetyError): p.apply_local(self.path)
        c = p.connect(self.db)
        self.assertEqual(c.execute('SELECT COUNT(*) FROM explanations').fetchone()[0], 2)
        self.assertEqual(c.execute('SELECT answer_index FROM questions WHERE id=1').fetchone()[0], 4)
        c.close()

    def test_provenance_bound_file_change_rejected(self):
        log = self.directory/'log.jsonl'; log.write_text('original')
        self.plan['bound_files'][str(log)] = p.sha('original'); p.write(self.path, self.plan)
        log.write_text('changed')
        with self.assertRaises(p.SafetyError): p.load_plan(self.path)

    def test_cloud_resume_does_not_accept_unrelated_metadata_change(self):
        before = dict(serial='A33-125', explanation='old', explanation_source='llm', stem='preserve', updated_at='old')
        desired = dict(explanation='new', explanation_source='model:GPT-6.1 Sol:ai_fact_checked')
        after = dict(before, **desired, updated_at='new')
        self.assertTrue(p.cloud_desired_match(before, after, desired))
        after['stem'] = 'another editor'
        self.assertFalse(p.cloud_desired_match(before, after, desired))

    def test_missing_db_cannot_silently_create_empty_database(self):
        missing = self.directory/'missing.sqlite'
        with self.assertRaises(p.SafetyError): p.connect(missing)
        self.assertFalse(missing.exists())

    def test_path_traversal_is_rejected(self):
        with self.assertRaises(p.SafetyError): p.inside(self.directory, '../elsewhere')


if __name__ == '__main__': unittest.main()
