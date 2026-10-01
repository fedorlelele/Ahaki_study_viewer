import importlib.util
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

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

    def test_bibliography_enrichment_is_reproducible_and_binds_source_index(self):
        index = self.directory/'efficiency/source_index.jsonl'
        index.parent.mkdir()
        index.write_text(json.dumps(dict(id='book-001', book='東洋医学臨床論',
            edition='第2版', headings=['第3章', '六部定位脈診']), ensure_ascii=False)+'\n')
        raw = [dict(type='textbook', source_id='book-001', excerpt_sha256='fixed-excerpt',
            passage='固定した根拠箇所', verdict='ok')]
        enriched = [dict(raw[0], title='東洋医学臨床論', edition='第2版',
            location='第3章 / 六部定位脈診',
            bibliographic_metadata_basis='immutable textbook source index')]
        bound = {}
        with patch.object(p, 'DEFAULT_ARTIFACTS', self.directory):
            self.assertTrue(p.same_audit_references(raw, enriched, bound))
        self.assertEqual(bound, {str(index.resolve()): p.sha(index.read_bytes())})
        self.assertNotIn('title', raw[0])
        self.plan['bound_files'] = bound
        p.write(self.path, self.plan)
        self.assertEqual(p.load_plan(self.path)['bound_files'], bound)
        index.write_text(index.read_text().replace('第2版', '第3版'))
        with self.assertRaises(p.SafetyError): p.load_plan(self.path)

    def test_bibliography_enrichment_rejects_medical_or_evidence_changes(self):
        index = self.directory/'efficiency/source_index.jsonl'
        index.parent.mkdir()
        index.write_text(json.dumps(dict(id='book-001', book='東洋医学臨床論',
            edition='第2版', headings=['第3章', '六部定位脈診']), ensure_ascii=False)+'\n')
        raw = [dict(type='textbook', source_id='book-001', excerpt_sha256='fixed-excerpt',
            passage='固定した根拠箇所', claim='右寸口の配当', verdict='ok')]
        expected = dict(raw[0], title='東洋医学臨床論', edition='第2版',
            location='第3章 / 六部定位脈診',
            bibliographic_metadata_basis='immutable textbook source index')
        with patch.object(p, 'DEFAULT_ARTIFACTS', self.directory):
            for field, changed in [('passage', '違う根拠'), ('claim', '別の医学的主張'),
                    ('verdict', 'uncertain'), ('excerpt_sha256', 'different-excerpt'),
                    ('source_id', 'other-book'), ('title', '推測した書名'),
                    ('evidence', '追加した根拠')]:
                with self.subTest(field=field):
                    bound = {}
                    self.assertFalse(p.same_audit_references(raw, [dict(expected, **{field: changed})], bound))
                    self.assertEqual(bound, {})

    def test_question_input_can_verify_only_fixed_question_structure(self):
        frozen = dict(input_sha256=p.sha('fixed-input'), question_sha256=p.sha('fixed-question'))
        audit = dict(references=[dict(id='Q1', type='question_input', serial='A27-140',
            input_sha256=frozen['input_sha256'], question_sha256=frozen['question_sha256'],
            passage='設問に病期の指定はない。'), dict(id='T1', type='textbook')],
            choice_audit=[dict(number=1, verdict='ok', evidence_ids=['T1'])],
            claim_audit=[dict(claim='設問に病期の指定はない。', claim_kind='question_structure',
                verdict='ok', evidence_ids=['Q1'])])
        p.validate_audit_evidence(audit, frozen, 'A27-140')
        for field, value in [('serial', 'A27-139'), ('input_sha256', 'changed-input'),
                ('question_sha256', 'changed-question'), ('passage', '')]:
            altered = copy.deepcopy(audit)
            altered['references'][0][field] = value
            with self.subTest(field=field), self.assertRaises(p.SafetyError):
                p.validate_audit_evidence(altered, frozen, 'A27-140')

    def test_question_input_cannot_replace_medical_or_choice_evidence(self):
        frozen = dict(input_sha256=p.sha('fixed-input'), question_sha256=p.sha('fixed-question'))
        audit = dict(references=[dict(id='Q1', type='question_input', serial='A27-140',
            input_sha256=frozen['input_sha256'], question_sha256=frozen['question_sha256'], passage='固定設問'),
            dict(id='T1', type='textbook')],
            choice_audit=[dict(number=1, verdict='ok', evidence_ids=['T1'])],
            claim_audit=[dict(claim='医学的主張', verdict='ok', evidence_ids=['Q1'])])
        with self.assertRaises(p.SafetyError):
            p.validate_audit_evidence(audit, frozen, 'A27-140')

        audit['claim_audit'][0]['claim_kind'] = 'medical'
        with self.assertRaises(p.SafetyError):
            p.validate_audit_evidence(audit, frozen, 'A27-140')
        audit['claim_audit'][0]['evidence_ids'] = ['T1']
        audit['choice_audit'][0]['evidence_ids'] = ['Q1']
        with self.assertRaises(p.SafetyError):
            p.validate_audit_evidence(audit, frozen, 'A27-140')
        audit['choice_audit'][0]['evidence_ids'] = ['T1']
        audit['claim_audit'][0]['evidence_ids'] = []
        with self.assertRaises(p.SafetyError):
            p.validate_audit_evidence(audit, frozen, 'A27-140')

    def test_truncated_web_hash_stops_publication(self):
        audit = {'references':[{'id':'R1', 'type':'web', 'sha256':'a'*50}],
                 'claim_audit':[{'claim':'原典の部位', 'verdict':'ok', 'evidence_ids':['R1']}]}
        with self.assertRaisesRegex(p.SafetyError, 'Malformed reference SHA-256'):
            p.validate_audit_evidence(audit, {}, 'A17-138')


if __name__ == '__main__': unittest.main()
