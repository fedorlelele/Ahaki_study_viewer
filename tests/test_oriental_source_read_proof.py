import copy
import hashlib
import json
from pathlib import Path
import shlex
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from oriental_source_read_proof import SourceReadProof


class ActualSourceReadProofTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.md = self.root / 'chapter.md'
        self.text = '## 脳卒中\n固定した完全な原文です。\n'
        self.md.write_text(self.text)
        self.unit = dict(id='u1', book='臨床医学', edition='第2版', file='chapter.md',
            line_start=1, line_end=2, headings=['脳卒中'],
            md_sha256=hashlib.sha256(self.md.read_bytes()).hexdigest())
        self.index = self.root / 'source_index.jsonl'
        self.index.write_text(json.dumps(self.unit)+'\n')
        self.helper = self.root / 'source_tools.py'
        self.helper.write_text('# immutable retrieval helper\n')
        self.bound = {}
        self.proof = SourceReadProof(self.index, [self.helper], self.bound)
        self.source = {k: self.unit[k] for k in ('id','book','edition','file','md_sha256')}
        self.source.update(location=self.unit['headings'], text=self.text,
            excerpt_sha256=hashlib.sha256(self.text.encode()).hexdigest())
        self.ref = dict(type='textbook', source_id='u1', excerpt_sha256=self.source['excerpt_sha256'])

    def tearDown(self): self.temp.cleanup()

    def event(self, output, exit_code=1, command=None):
        if command is None:
            command = '/bin/bash -lc ' + shlex.quote('python3 '+shlex.quote(str(self.helper))+' read u1; rg missing otherfile')
        return dict(type='item.completed', item=dict(type='command_execution',
            command=command, exit_code=exit_code, aggregated_output=output))

    def test_complete_first_read_survives_later_command_failure_and_binds_sources(self):
        output=json.dumps({'sources':[self.source]}, ensure_ascii=False)+'\nrg: missing: no match\n'
        received=self.proof.collect([self.event(output)])
        self.proof.require([self.ref], received)
        for path in (self.index,self.md,self.helper):
            self.assertEqual(self.bound[str(path.resolve())], hashlib.sha256(path.read_bytes()).hexdigest())

    def test_snippet_forged_hash_changed_text_and_untrusted_echo_are_rejected(self):
        bad=[]
        snippet=copy.deepcopy(self.source);snippet['text']=self.text[:8];bad.append(snippet)
        forged=copy.deepcopy(self.source);forged['excerpt_sha256']='a'*64;bad.append(forged)
        changed=copy.deepcopy(self.source);changed['text']=self.text.replace('原文','改変');changed['excerpt_sha256']=hashlib.sha256(changed['text'].encode()).hexdigest();bad.append(changed)
        preview=copy.deepcopy(self.source);preview.pop('text');preview['preview']=self.text;bad.append(preview)
        wrong_file=copy.deepcopy(self.source);wrong_file['file']='other.md';bad.append(wrong_file)
        wrong_md=copy.deepcopy(self.source);wrong_md['md_sha256']='a'*64;bad.append(wrong_md)
        for source in bad:
            for exit_code in (0,1):
                with self.subTest(source=source,exit_code=exit_code),self.assertRaises(ValueError):
                    self.proof.require([self.ref], self.proof.collect([self.event(json.dumps({'sources':[source]}),exit_code)]))
        output=json.dumps({'sources':[self.source]})
        for command in ('echo fake; false','python3 '+str(self.helper)+' read u1 | false',
                        'python3 '+str(self.helper)+' read u1 > hidden; false'):
            with self.subTest(command=command),self.assertRaises(ValueError):
                self.proof.require([self.ref],self.proof.collect([self.event(output,command=command)]))

    def test_partial_success_does_not_prove_later_unread_source_or_mutated_corpus(self):
        event=self.event('{}\n'+json.dumps({'sources':[self.source]}))
        with self.assertRaises(ValueError):self.proof.require([self.ref],self.proof.collect([event]))
        self.md.write_text('mutated after indexing')
        with self.assertRaises(ValueError):self.proof.collect([self.event(json.dumps({'sources':[self.source]}))])

    def legacy_roster(self, metadata):
        path=self.root/'legacy.json'
        fingerprint=hashlib.sha256(json.dumps(metadata,ensure_ascii=False,
            sort_keys=True,separators=(',',':')).encode()).hexdigest()
        entry=dict(metadata_canonical_sha256=fingerprint,
            **{k:metadata.get(k) for k in ('log_sha256','session_id','started_at','runner_sha256')})
        path.write_text(json.dumps({'entries':[entry]}))
        return path

    def test_frozen_legacy_missing_hash_allowed_only_with_exact_source_receipt(self):
        meta=dict(log_sha256='actual-log',session_id='actual-session',started_at='fixed-time')
        roster=self.legacy_roster(meta)
        original=copy.deepcopy(meta)
        basis=self.proof.validate_run_index(meta,roster)
        self.assertIn('unrecorded',basis)
        self.assertEqual(meta,original)  # no invented retrospective observation
        self.proof.require([self.ref],self.proof.collect([self.event(json.dumps({'sources':[self.source]}))]))
        self.assertIn(str(roster.resolve()),self.bound)
        changed=copy.deepcopy(self.source);changed['text']='partial'
        with self.assertRaises(ValueError):self.proof.require([self.ref],self.proof.collect([self.event(json.dumps({'sources':[changed]}))]))

    def test_recorded_mismatch_and_new_missing_hash_are_always_rejected(self):
        meta=dict(log_sha256='actual-log',session_id='actual-session',started_at='fixed-time')
        roster=self.legacy_roster(meta)
        for modified in (dict(meta,source_index_sha256='wrong'),dict(meta,source_index_sha256=None),
                         dict(meta,session_id='changed-session')):
            with self.subTest(meta=modified),self.assertRaises(ValueError):
                self.proof.validate_run_index(modified,roster)
        with self.assertRaises(ValueError):self.proof.validate_run_index(meta)
        self.assertIn('Recorded runtime',self.proof.validate_run_index(dict(meta,source_index_sha256=self.proof.index_sha256),roster))

    def test_successful_full_text_receipt_can_omit_unobserved_origin_metadata(self):
        minimal=copy.deepcopy(self.source);minimal.pop('file');minimal.pop('md_sha256')
        output=json.dumps(minimal)
        self.proof.require([self.ref],self.proof.collect([self.event(output,exit_code=0)]))
        self.assertEqual(self.bound[str(self.md.resolve())],self.unit['md_sha256'])
        # Failure recovery still requires a known first helper's full output.
        with self.assertRaises(ValueError):self.proof.require([self.ref],self.proof.collect([self.event(output)]))

    def test_successful_complete_top_level_array_receipt(self):
        for indent in (None, 2):
            output = json.dumps([self.source], ensure_ascii=False, indent=indent)
            with self.subTest(indent=indent):
                self.proof.require([self.ref], self.proof.collect([self.event(output, exit_code=0)]))
                self.assertEqual(len(self.proof.objects(output)), 1)

    def test_array_receipt_rejects_partial_quoted_and_modified_sources(self):
        complete = json.dumps([self.source], ensure_ascii=False, indent=2)
        changed = copy.deepcopy(self.source)
        changed['text'] = self.text[:8]
        outputs = [complete[:-1], json.dumps(complete),
                   json.dumps([changed]), json.dumps([{'sources': [self.source]}]),
                   '{"sources": ' + complete]  # incomplete enclosing object
        for output in outputs:
            with self.subTest(output=output), self.assertRaises(ValueError):
                self.proof.require([self.ref], self.proof.collect([self.event(output, exit_code=0)]))

    def test_nonzero_array_does_not_expand_failure_recovery_rules(self):
        output = json.dumps([self.source], ensure_ascii=False)
        with self.assertRaises(ValueError):
            self.proof.require([self.ref], self.proof.collect([self.event(output)]))

    def test_malformed_web_reference_hashes_are_rejected(self):
        for value in ('a' * 50, 'g' * 64, None, 123):
            for field in ('sha256', 'image_sha256'):
                with self.subTest(value=value, field=field), self.assertRaises(ValueError):
                    self.proof.require([dict(type='web', **{field:value})], set())

    def test_nested_hashes_are_checked_without_claiming_source_receipt(self):
        with self.assertRaises(ValueError):
            self.proof.require([{'type':'web', 'original':{'pdf_sha256':'a'*50}}], set())
        # A well-formed checksum alone does not prove any source was read.
        self.proof.require([{'type':'web', 'sha256':'a'*64}], set())
        with self.assertRaises(ValueError):
            self.proof.require([self.ref], set())


if __name__ == '__main__': unittest.main()
