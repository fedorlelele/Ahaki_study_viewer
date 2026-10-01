#!/usr/bin/env python3
"""Frozen, resumable publication of reviewed Oriental Clinical normal explanations.

prepare/local-apply/local-verify/cloud-prepare/cloud-apply/cloud-verify/export-verify.
Preparation writes private plans/backups only. No command commits or pushes Git.
"""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from oriental_source_read_proof import SourceReadProof
DEFAULT_ARTIFACTS = ROOT / 'reviews/2026-09-24/oriental-clinical-normal-claude'
MODELS = {'claude-opus-5-5': 'Claude Opus 5.5',
          'claude-sonnet-5-5': 'Claude Sonnet 5.5', 'gpt-6.1-sol': 'GPT-6.1 Sol'}
SUBJECT = '東洋医学臨床論'
ANSWER_COLUMNS = {'answer_index', 'answer_indices_json', 'answer_text', 'raw_text', 'answer_none'}
EXPORTED_EXPLANATION_FIELDS = {'explanation_latest', 'explanation_latest_source',
    'explanation_latest_model_name', 'explanation_latest_review_status', 'explanations'}
EXPORTED_ANSWER_FIELDS = {'answer_index', 'answer_indices', 'answer_none',
    'answer_text', 'answer_variants', 'answer_notes'}

class SafetyError(Exception): pass
def need(test, message):
    if not test: raise SafetyError(message)
def sha(value):
    return hashlib.sha256(value if isinstance(value, bytes) else value.encode('utf-8')).hexdigest()
def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
def digest(value): return sha(canonical(value))
def read(path): return json.loads(Path(path).read_text(encoding='utf-8'))
def jsonl(path): return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
def keyed(rows):
    result = {r['serial']: r for r in rows}
    need(len(result) == len(rows), 'duplicate serials')
    return result
def write(path, value, exclusive=False):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    if exclusive: need(not path.exists(), f'already exists: {path}')
    tmp = path.with_suffix(path.suffix + '.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
        stream.flush(); os.fsync(stream.fileno())
    tmp.replace(path)
def inside(base, relative):
    path = (base / relative).resolve()
    need(path.is_relative_to(base.resolve()) and not Path(relative).is_absolute(), 'unsafe artifact path')
    return path
def connect(path, readonly=False):
    need(path.is_file() and path.stat().st_size > 0, 'existing non-empty database required')
    c = sqlite3.connect(path.resolve().as_uri() + ('?mode=ro' if readonly else '?mode=rw'), uri=True)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA foreign_keys=ON')
    need(c.execute('PRAGMA quick_check').fetchone()[0] == 'ok', 'SQLite quick_check failed')
    return c
def latest(c, question_id):
    row = c.execute('SELECT * FROM explanations WHERE question_id=? ORDER BY version DESC,id DESC LIMIT 1', (question_id,)).fetchone()
    return dict(row) if row else None
def changes(a, b): return {k for k in set(a) | set(b) if a.get(k) != b.get(k)}

def same_audit_references(raw, enriched, bound):
    """Permit only reproducible bibliographic additions, never medical changes."""
    if raw == enriched: return True
    if not isinstance(raw, list) or not isinstance(enriched, list) or len(raw) != len(enriched): return False
    index_path = DEFAULT_ARTIFACTS / 'efficiency/source_index.jsonl'
    units = {x['id']: x for x in jsonl(index_path)}
    for original, filled in zip(raw, enriched):
        if original == filled: continue
        if original.get('type') != 'textbook' or original.get('source_id') not in units: return False
        unit = units[original['source_id']]
        expected = dict(original)
        expected.setdefault('title', unit['book'])
        expected.setdefault('edition', unit['edition'])
        expected.setdefault('location', ' / '.join(unit['headings']))
        expected['bibliographic_metadata_basis'] = 'immutable textbook source index'
        if filled != expected: return False
    bound[str(index_path.resolve())] = sha(index_path.read_bytes())
    return True

def validate_audit_evidence(audit, frozen_input, serial):
    """Question input proves literal structure only, never medical correctness."""
    references = audit.get('references', [])
    try:
        SourceReadProof.validate_hash_fields(references)
    except ValueError as error:
        raise SafetyError(f'{serial}: {error}') from error
    refmap = {ref['id']: ref for ref in references}
    need(bool(refmap) and len(refmap) == len(references) and audit.get('claim_audit'),
         f'{serial}: absent or duplicate evidence')
    for ref in references:
        if ref.get('type') != 'question_input': continue
        need(ref.get('serial') == serial and
             ref.get('input_sha256') == frozen_input['input_sha256'] and
             ref.get('question_sha256') == frozen_input['question_sha256'],
             f'{serial}: question evidence does not bind frozen input')
        need(isinstance(ref.get('passage'), str) and bool(ref['passage'].strip()),
             f'{serial}: question evidence lacks literal description')
    for kind, items in [('choice', audit.get('choice_audit', [])),
                        ('claim', audit['claim_audit'])]:
        for item in items:
            ids = item.get('evidence_ids')
            need(item.get('verdict') == 'ok' and ids and set(ids) <= set(refmap),
                 f'{serial}: unsupported claim/choice')
            question_only = all(refmap[eid].get('type') == 'question_input' for eid in ids)
            if question_only:
                need(kind == 'claim' and item.get('claim_kind') == 'question_structure',
                     f'{serial}: question evidence cannot verify medical claim or choice')

def validate_release(artifacts, release, inputs_path):
    inputs = keyed(jsonl(inputs_path))
    rows = keyed(jsonl(release)); need(bool(rows), 'empty release')
    bound, claude_log_evidence, codex_log_evidence = {}, {}, {}
    source_proof = SourceReadProof(DEFAULT_ARTIFACTS / 'efficiency/source_index.jsonl',
        [DEFAULT_ARTIFACTS / 'efficiency/efficient_tools.py', artifacts / 'source_tools.py'], bound)
    for s, r in rows.items():
        need(s in inputs, f'{s}: outside frozen inputs')
        i = inputs[s]; q = i['question']; le = i['latest_explanation']
        original = dict(i); original.pop('input_sha256')
        need(digest(original) == i['input_sha256'] and digest(q) == i['question_sha256'], f'{s}: immutable input hash differs')
        need(r.get('subject') == i['subject'] == SUBJECT and r.get('explanation_type') == 'normal', f'{s}: wrong subject/type')
        for key in ('input_sha256', 'question_sha256'):
            need(r[key] == i[key], f'{s}: {key} mismatch')
        need(r['expected_latest'] == {k: le[k] for k in ('id', 'version', 'body_sha256', 'row_sha256')}, f'{s}: latest baseline mismatch')
        body = r['explanation']
        need(isinstance(body, str) and body == body.strip() and len(body) >= 40, f'{s}: missing/unfinished body')
        need(not re.search(r'TODO|TBD|要執筆|執筆予定|作成中|以下略|仮の解説', body, re.I), f'{s}: unfinished body')
        need(sha(body) == r['body_sha256'], f'{s}: body SHA differs')
        draft = inside(artifacts, 'drafts/' + s + '.md')
        need(draft.read_bytes() == body.encode('utf-8'), f'{s}: draft differs from release')
        audit_path = inside(artifacts, r['audit_path']); a = read(audit_path)
        bound[str(audit_path)] = sha(audit_path.read_bytes()); bound[str(draft)] = sha(draft.read_bytes())
        need(a.get('serial') == s and a.get('audited_body_sha256') == r['body_sha256'], f'{s}: audit does not bind final body')
        need(a.get('verdict') in {'pass', 'uncertain'}, f'{s}: not independently accepted')
        need(r['review_status'] == a['review_status'] and r['review_status'] in {'ai', 'ai_fact_checked'}, f'{s}: invalid review status')
        if a['verdict'] == 'uncertain' or r['review_status'] == 'ai':
            need(r['review_status'] == 'ai' and re.search(r'留意|疑義|一意|曖昧|あいまい|出題当時', body), f'{s}: uncertainty must be visible')
        for role in ('generation', 'verification'):
            mid, model = a[f'{role}_model_id'], a[f'{role}_model_name']
            need(MODELS.get(mid) == model and r.get(f'{role}_model_id') == mid, f'{s}: unrecognized/different actual {role} model')
            namefield = 'model_name' if role == 'generation' else 'verification_model_name'
            need(r[namefield] == model, f'{s}: model label mismatch')
            paths, hashes = a[f'{role}_log_paths'], a[f'{role}_log_sha256']
            need(paths and len(paths) == len(hashes), f'{s}: absent provenance logs')
            for p, h in zip(paths, hashes):
                log = inside(artifacts, p)
                need(sha(log.read_bytes()) == h, f'{s}: provenance log SHA differs')
                bound[str(log)] = h
                if mid.startswith('claude-'):
                    if str(log) not in claude_log_evidence:
                        events = jsonl(log)
                        responses = [x for x in events if x.get('type') == 'assistant' and isinstance(x.get('message'), dict) and x['message'].get('model') != '<synthetic>']
                        claude_log_evidence[str(log)] = ({x['message'].get('model') for x in responses}, {x.get('agentId') for x in responses})
                    actual_models, actual_sessions = claude_log_evidence[str(log)]
                    need(actual_models == {mid} and bool(actual_sessions) and actual_sessions <= set(a[f'{role}_session_ids']), f'{s}: actual Claude model/session log mismatch')
                elif mid == 'gpt-6.1-sol':
                    if str(log) not in codex_log_evidence:
                        metadata_path, prompt_path = log.with_suffix('.run.json'), log.with_suffix('.prompt.txt')
                        meta, prompt = read(metadata_path), prompt_path.read_text()
                        try:
                            source_proof.validate_run_index(meta,
                                artifacts / 'LEGACY_SOURCE_INDEX_OBSERVATIONS.json')
                        except ValueError as error: raise SafetyError(str(error)) from error
                        events = jsonl(log); argv = meta.get('argv', [])
                        need(meta.get('returncode') == 0 and meta.get('requested_model_id') == mid and '-m' in argv and argv[argv.index('-m')+1] == mid, 'Codex run did not select requested model successfully')
                        need(meta.get('log_sha256') == h and meta.get('log_path') == p, 'Codex run metadata does not bind log')
                        starts = [x['thread_id'] for x in events if x.get('type') == 'thread.started']
                        need(starts == [meta.get('session_id')] and any(x.get('type') == 'turn.completed' for x in events), 'Codex session did not complete')
                        messages = [x['item']['text'] for x in events if x.get('type') == 'item.completed' and x.get('item', {}).get('type') == 'agent_message']
                        need(bool(messages), 'Codex final response missing')
                        final = json.loads(re.sub(r'^```(?:json)?\s*|\s*```$', '', messages[-1].strip()))
                        received_sources = source_proof.collect(events)
                        codex_log_evidence[str(log)] = (meta, prompt, keyed(final['items']), received_sources)
                    meta, prompt, final, received_sources = codex_log_evidence[str(log)]
                    bound[str(log.with_suffix('.run.json'))] = sha(log.with_suffix('.run.json').read_bytes())
                    bound[str(log.with_suffix('.prompt.txt'))] = sha(log.with_suffix('.prompt.txt').read_bytes())
                    need(a[f'{role}_session_ids'] == [meta['session_id']] and s in final, f'{s}: Codex session/final item differs')
                    need(i['input_sha256'] in prompt and s in prompt, f'{s}: Codex did not receive frozen input')
                    if role == 'generation':
                        need(final[s].get('explanation') == body, f'{s}: generated final response differs from published body')
                        try: source_proof.require(final[s].get('sources', []), received_sources)
                        except ValueError as error: raise SafetyError(f'{s}: generator {error}') from error
                    else:
                        need(body in prompt and r['body_sha256'] in prompt and final[s].get('audited_body_sha256') == r['body_sha256'], f'{s}: auditor did not receive fixed final body')
                        for field in ('verdict', 'choice_audit', 'claim_audit', 'references', 'issues'):
                            matches = same_audit_references(final[s].get(field), a.get(field), bound) if field == 'references' else final[s].get(field) == a.get(field)
                            need(matches, f'{s}: accepted audit differs from actual final response')
                        for ref in a.get('references', []):
                            if ref.get('type') == 'textbook':
                                need((ref['source_id'], ref['excerpt_sha256']) in received_sources,
                                     f'{s}: actual auditor complete textbook read not proved')
                            elif ref.get('type') == 'question_input':
                                need(i['question_sha256'] in prompt, f'{s}: auditor did not receive frozen question SHA')
        gs, vs = a['generation_session_ids'], a['verification_session_ids']
        need(gs and vs and not set(gs) & set(vs), f'{s}: generation and audit sessions must differ')
        need({x['number'] for x in a['choice_audit']} == set(range(1, len(i['choices']) + 1)), f'{s}: incomplete choice audit')
        validate_audit_evidence(a, i, s)
    return inputs, rows, bound

def overlay_rows(overlay, inputs, selected):
    result = {}
    if not overlay: return result
    data = read(overlay)
    for item in data['items']:
        s = item['serial']
        if s not in selected: continue
        i, q = inputs[s], dict(inputs[s]['question'])
        need(item['status'] == 'confirmed_discrepancy_not_applied' and item['expected_input_sha256'] == i['input_sha256'] and item['expected_question_sha256'] == i['question_sha256'], f'{s}: unconfirmed answer correction')
        need(all(item['identity_check'].get(k) is True for k in ('number_and_exam_match', 'stem_and_four_choices_match')), f'{s}: official question identity not verified')
        for field in ('official_key', 'official_question_paper'):
            ref = item[field]; path = overlay.parent / 'sources' / ref['file']
            need(path.is_file() and sha(path.read_bytes()) == ref['sha256'] and urlparse(ref['url']).hostname == 'ahaki.or.jp', f'{s}: official evidence hash/host differs')
        old, new = item['current_answers']['answer_text'], item['official_answers']['answer_text']
        need(q['answer_text'] == old and q['raw_text'].endswith(old), f'{s}: original answer text not an exact suffix')
        a = item['official_answers']
        q.update(answer_index=a['answer_index'], answer_indices_json=json.dumps(a['answer_indices']), answer_text=new, answer_none=int(a['answer_none']))
        q['raw_text'] = q['raw_text'][:-len(old)] + new
        result[s] = q
    return result

def protected_hashes(c, plan, after=False):
    selected = {i['question']['id']: s for s, i in plan['inputs'].items()}
    tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    out = {}
    for table in tables:
        need(re.fullmatch(r'[A-Za-z0-9_]+', table), 'unsafe table name')
        rows = [dict(r) for r in c.execute('SELECT * FROM "' + table + '" ORDER BY rowid')]
        if after and table == 'explanations':
            rows = [r for r in rows if r['source'] != plan['source_tag']]
        if after and table == 'questions':
            for n, r in enumerate(rows):
                s = selected.get(r['id'])
                if s in plan['corrections']:
                    need(r == plan['corrections'][s], f'{s}: corrected question differs')
                    rows[n] = plan['inputs'][s]['question']
        out[table] = digest(rows)
    return out

def prepare(args):
    need(not args.plan.exists(), 'plan exists; use verify/resume instead')
    inputs, rows, bound = validate_release(args.artifacts, args.release, args.inputs)
    selected_inputs = {s: inputs[s] for s in rows}
    corrections = overlay_rows(args.overlay, inputs, rows)
    for s, q in corrections.items():
        audit = read(inside(args.artifacts, rows[s]['audit_path']))
        reg = audit.get('answer_registration', {})
        need(reg.get('official_answer_indices') == json.loads(q['answer_indices_json']) and reg.get('registered_answer_indices') == inputs[s]['resolved_answers']['answer_indices'], f'{s}: audit not aligned with official correction')
    if corrections:
        bound[str(args.overlay.resolve())] = sha(args.overlay.read_bytes())
        for item in read(args.overlay)['items']:
            if item['serial'] in corrections:
                for field in ('official_key', 'official_question_paper'):
                    proof = args.overlay.parent / 'sources' / item[field]['file']
                    bound[str(proof.resolve())] = item[field]['sha256']
    now = datetime.now(timezone.utc).isoformat()
    plan = {'schema_version': 1, 'created_at': now, 'db': str(args.db.resolve()),
        'artifacts': str(args.artifacts.resolve()), 'release_path': str(args.release.resolve()),
        'inputs_path': str(args.inputs.resolve()), 'release_sha256': sha(args.release.read_bytes()),
        'inputs_sha256': sha(args.inputs.read_bytes()), 'bound_files': bound,
        'source_tag': 'oriental_clinical_normal_' + args.plan.parent.name,
        'rows': rows, 'inputs': selected_inputs, 'corrections': corrections, 'local_applied': False}
    c = connect(args.db, True)
    need(not c.execute('SELECT 1 FROM explanations WHERE source=? LIMIT 1', (plan['source_tag'],)).fetchone(), 'source_tag already used; choose a unique release directory')
    for s, i in selected_inputs.items():
        q = dict(c.execute('SELECT * FROM questions WHERE id=?', (i['question']['id'],)).fetchone())
        need(q == i['question'], f'{s}: question changed since immutable snapshot')
        le = latest(c, q['id'])
        need(digest(le) == i['latest_explanation']['row_sha256'], f'{s}: latest explanation changed')
        need(c.execute('SELECT name FROM subjects WHERE id=?', (q['subject_id'],)).fetchone()[0] == SUBJECT, f'{s}: subject differs')
    plan['protected_hashes'] = protected_hashes(c, plan)
    export = ROOT / 'docs/output/web/questions.json'
    plan['export_before'] = keyed(read(export))
    args.plan.parent.mkdir(parents=True, exist_ok=True); os.chmod(args.plan.parent, 0o700)
    backup = args.plan.parent / 'before.sqlite'
    need(not backup.exists(), 'unrecorded backup exists')
    with sqlite3.connect(backup) as b: c.backup(b)
    os.chmod(backup, 0o600); c.close()
    plan['backup_path'] = str(backup.resolve())
    plan['backup_sha256'] = sha(backup.read_bytes())
    write(args.plan, plan, True)
    return {'phase': 'prepared', 'selected': len(rows), 'answer_corrections': sorted(corrections), 'models': dict(Counter(r['model_name'] for r in rows.values()))}

def load_plan(path):
    p = read(path)
    need(sha(Path(p['release_path']).read_bytes()) == p['release_sha256'] and sha(Path(p['inputs_path']).read_bytes()) == p['inputs_sha256'], 'frozen input/release files changed')
    for file, h in p['bound_files'].items(): need(sha(Path(file).read_bytes()) == h, 'bound draft/audit/log changed: ' + file)
    if 'backup_sha256' in p:
        need(sha(Path(p['backup_path']).read_bytes()) == p['backup_sha256'], 'SQLite backup file changed')
    return p

def verify_local(p):
    c = connect(Path(p['db']), True)
    applied = [dict(r) for r in c.execute('SELECT * FROM explanations WHERE source=?', (p['source_tag'],))]
    need(len(applied) in {0, len(p['rows'])}, 'partial/unexpected local transaction')
    if applied:
        need(protected_hashes(c, p, True) == p['protected_hashes'], 'protected DB tables/old history changed')
        for s, r in p['rows'].items():
            i = p['inputs'][s]; le = latest(c, i['question']['id'])
            need(le['source'] == p['source_tag'] and le['version'] == i['latest_explanation']['version'] + 1 and le['body'] == r['explanation'] and le['model_name'] == r['model_name'] and le['review_status'] == r['review_status'], f'{s}: latest applied row differs')
    else: need(protected_hashes(c, p) == p['protected_hashes'], 'database changed since prepare')
    need(not c.execute('PRAGMA foreign_key_check').fetchall(), 'foreign key check failed'); c.close()
    return bool(applied)

def apply_local(path):
    p = load_plan(path)
    if not verify_local(p):
        c = connect(Path(p['db']))
        try:
            c.execute('BEGIN IMMEDIATE')
            need(protected_hashes(c, p) == p['protected_hashes'], 'concurrent DB changes; no writes performed')
            for s, r in p['rows'].items():
                i = p['inputs'][s]; qid = i['question']['id']
                if s in p['corrections']:
                    q = p['corrections'][s]
                    c.execute('UPDATE questions SET answer_index=?,answer_indices_json=?,answer_none=?,answer_text=?,raw_text=? WHERE id=?', (q['answer_index'], q['answer_indices_json'], q['answer_none'], q['answer_text'], q['raw_text'], qid))
                version = c.execute('SELECT MAX(version) FROM explanations WHERE question_id=?', (qid,)).fetchone()[0] + 1
                c.execute('INSERT INTO explanations(question_id,body,version,source,model_name,review_status) VALUES(?,?,?,?,?,?)', (qid, r['explanation'], version, p['source_tag'], r['model_name'], r['review_status']))
            need(protected_hashes(c, p, True) == p['protected_hashes'], 'protected data changed during apply')
            need(not c.execute('PRAGMA foreign_key_check').fetchall(), 'foreign key check failed')
            c.commit()
        except BaseException: c.rollback(); raise
        finally: c.close()
    verify_local(p); p['local_applied'] = True; write(path, p)
    return {'phase': 'local_verified', 'selected': len(p['rows']), 'backup': p['backup_path']}

def cfg():
    from sync_deep_dive_to_supabase import load_env, get_supabase_config
    load_env(ROOT / '.env'); url, key = get_supabase_config()
    need(urlparse(url).hostname == 'fnidnxgliwclhmvovkji.supabase.co' and key, 'expected Supabase credentials unavailable')
    return url, key
def request_cloud(config, method, query, body=None):
    need(method in {'GET', 'PATCH'}, 'unsupported cloud operation')
    if body is not None:
        need(method == 'PATCH' and set(body) <= {'explanation', 'explanation_source', 'answer_index', 'answer_indices', 'answer_none'} and query.get('serial', '').startswith('eq.') and query.get('updated_at', '').startswith('eq.'), 'unsafe cloud mutation')
    url, key = config
    headers = {'apikey': key, 'Authorization': 'Bearer ' + key, 'Accept': 'application/json'}
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode(); headers.update({'Content-Type': 'application/json', 'Prefer': 'return=representation'})
    req = Request(url + '/rest/v1/question_overrides?' + urlencode(query), headers=headers, data=data, method=method)
    try:
        with urlopen(req, timeout=40) as response: rows = json.load(response)
    except HTTPError as e: raise SafetyError(f'cloud {method} HTTP {e.code}; reread before retry') from None
    except URLError: raise SafetyError('cloud network failure; reread before retry') from None
    need(isinstance(rows, list), 'invalid cloud response'); return rows
def fetch_cloud(config, serials):
    rows = []
    for start in range(0, len(serials), 50):
        batch = serials[start:start+50]
        found = request_cloud(config, 'GET', {'select': '*', 'serial': 'in.(' + ','.join(batch) + ')', 'order': 'serial'})
        need(all(r.get('serial') in batch for r in found), 'out-of-scope cloud response'); rows.extend(found)
    return keyed(rows)
def active(base, row):
    def date(s):
        try:
            value = str(s).replace('Z', '+00:00')
            value = re.sub(r'\.(\d{1,6})(?=[+-]\d{2}:\d{2}$)', lambda m: '.' + m.group(1).ljust(6, '0'), value)
            return datetime.fromisoformat(value)
        except (ValueError, TypeError): return None
    a, b = date(row.get('updated_at')), date(base.get('override_updated_at'))
    return not (a and b and a <= b)
def cloud_desired_match(before, after, desired):
    return set(before) == set(after) and all(after.get(k) == v for k, v in desired.items()) and not (changes(before, after) - set(desired) - {'updated_at'})

def cloud_prepare(path):
    p = load_plan(path); need(verify_local(p), 'local application required first')
    need('cloud' not in p, 'cloud plan exists; verify/resume instead')
    before = fetch_cloud(cfg(), list(p['rows'])); desired = {}
    for s, row in before.items():
        base = p['export_before'][s]; r = p['rows'][s]; q = p['inputs'][s]['question']
        if not active(base, row): continue
        for field, expected in [('stem', q['stem']), ('case_text', q['case_text'] or ''), ('choices', p['inputs'][s]['choices'])]:
            actual = row.get(field)
            need(actual is None or (actual or '') == (expected or ''), f'{s}: active cloud {field} conflicts with frozen question')
        patch = {}
        if row.get('explanation') is not None or row.get('explanation_source'):
            need(row.get('explanation') in (None, base['explanation_latest'], r['explanation']), f'{s}: active cloud explanation changed independently')
            patch.update(explanation=r['explanation'], explanation_source='model:' + r['model_name'] + (':ai_fact_checked' if r['review_status'] == 'ai_fact_checked' else ''))
        if s in p['corrections'] and (row.get('answer_index') is not None or row.get('answer_indices') is not None or row.get('answer_none')):
            old = p['inputs'][s]['resolved_answers']; new = p['corrections'][s]
            numeric = row.get('answer_indices') if row.get('answer_indices') is not None else ([row['answer_index']] if row.get('answer_index') else [])
            need(numeric in (old['answer_indices'], json.loads(new['answer_indices_json'])) and not row.get('answer_none'), f'{s}: active cloud answer differs from reviewed correction')
            patch.update(answer_index=new['answer_index'], answer_indices=json.loads(new['answer_indices_json']), answer_none=bool(new['answer_none']))
        elif any(row.get(f) is not None for f in ('answer_index', 'answer_indices')) or row.get('answer_none'):
            numeric = row.get('answer_indices') if row.get('answer_indices') is not None else ([row['answer_index']] if row.get('answer_index') else [])
            need(numeric == p['inputs'][s]['resolved_answers']['answer_indices'] and bool(row.get('answer_none')) == p['inputs'][s]['resolved_answers']['answer_none'], f'{s}: active cloud answer conflicts with frozen question')
        if patch:
            need(row.get('updated_at'), f'{s}: CAS timestamp missing'); desired[s] = patch
    p['cloud'] = {'before': before, 'desired': desired, 'completed': {}}
    write(path, p)
    return {'phase': 'cloud_prepared', 'existing_override_rows': len(before), 'patch_count': len(desired), 'insertions': 0}

def cloud_verify_or_apply(path, apply=False):
    p = load_plan(path); need(verify_local(p), 'local application missing')
    cl = p['cloud']; config = cfg(); current = fetch_cloud(config, list(p['rows']))
    need(set(current) == set(cl['before']), 'cloud row set changed')
    for s, before in cl['before'].items():
        want = cl['desired'].get(s)
        if want and cloud_desired_match(before, current[s], want):
            cl['completed'][s] = current[s]
        elif current[s] != before:
            raise SafetyError(f'{s}: concurrent cloud change; no overwrite performed')
    write(path, p)
    pending = sorted(set(cl['desired']) - set(cl['completed']))
    if apply:
        for s in pending:
            before, desired = cl['before'][s], cl['desired'][s]
            out = request_cloud(config, 'PATCH', {'select': '*', 'serial': 'eq.' + s, 'updated_at': 'eq.' + before['updated_at']}, desired)
            need(len(out) == 1 and cloud_desired_match(before, out[0], desired), f'{s}: CAS/protected-field check failed')
            cl['completed'][s] = out[0]; write(path, p)
        return cloud_verify_or_apply(path, False)
    need(not pending, 'cloud publication incomplete: ' + ','.join(pending))
    cl['after'] = current; cl['verified'] = True; write(path, p)
    return {'phase': 'cloud_verified', 'patch_count': len(cl['desired']), 'protected_fields_preserved': True}

def export_verify(path):
    p = load_plan(path); need(verify_local(p), 'local apply missing')
    need(p.get('cloud', {}).get('verified'), 'cloud verification required')
    before, after = p['export_before'], keyed(read(ROOT / 'docs/output/web/questions.json'))
    need(set(before) == set(after), 'export question set changed')
    need({s for s in before if before[s] != after[s]} == set(p['rows']), 'export changed outside this frozen release')
    for s, r in p['rows'].items():
        allowed = EXPORTED_EXPLANATION_FIELDS | (EXPORTED_ANSWER_FIELDS if s in p['corrections'] else set())
        need(not changes(before[s], after[s]) - allowed, f'{s}: protected exported fields changed')
        for source, target in [('explanation', 'explanation_latest'), ('model_name', 'explanation_latest_model_name'), ('review_status', 'explanation_latest_review_status')]:
            need(after[s][target] == r[source], f'{s}: export differs')
        need(len(after[s]['explanations']) == len(before[s]['explanations']) + 1 and all(old in after[s]['explanations'] for old in before[s]['explanations']), f'{s}: explanation history not preserved')
        if s in p['corrections']:
            q = p['corrections'][s]
            need(after[s]['answer_index'] == q['answer_index'] and after[s]['answer_indices'] == json.loads(q['answer_indices_json']) and after[s]['answer_text'] == q['answer_text'], f'{s}: answer export not corrected')
    # Run the actual shipped merger; never infer effective production behavior.
    code = r'''const fs=require('fs'),assert=require('node:assert/strict'),Q=require(process.argv[1]+'/web_app/shared/questions.js');const p=JSON.parse(fs.readFileSync(process.argv[2])), after=JSON.parse(fs.readFileSync(process.argv[1]+'/docs/output/web/questions.json'));const old=Q.applyQuestionOverrides(Object.values(p.export_before),Object.values(p.cloud.before));const now=Q.applyQuestionOverrides(after,Object.values(p.cloud.after));const b=new Map(old.map(x=>[x.serial,x]));for(const q of now){const prev=b.get(q.serial),r=p.rows[q.serial];if(!r){assert.deepEqual(q,prev,q.serial);continue;}assert.equal(q.explanation_latest,r.explanation,q.serial);assert.equal(q.explanation_latest_model_name,r.model_name,q.serial);assert.equal(q.explanation_latest_review_status,r.review_status,q.serial);for(const k of ['stem','case_text','choices','tags','subtopics','subject'])assert.deepEqual(q[k],prev[k],q.serial+' '+k);if(p.corrections[q.serial]){assert.equal(q.answer_index,p.corrections[q.serial].answer_index);assert.deepEqual(q.answer_indices,JSON.parse(p.corrections[q.serial].answer_indices_json));}else for(const k of ['answer_index','answer_indices','answer_none','answer_text','answer_variants','answer_notes'])assert.deepEqual(q[k],prev[k],q.serial+' '+k);}'''
    result = subprocess.run(['node', '-e', code, str(ROOT), str(path.resolve())], text=True, capture_output=True)
    need(result.returncode == 0, 'effective ASV merge failed: ' + result.stderr[-1500:])
    from prepare_pages import validate_pages
    validate_pages(ROOT / 'docs', len(after))
    p['export_verified'] = True; write(path, p)
    return {'phase': 'export_verified', 'selected': len(p['rows']), 'unchanged_questions': len(after)-len(p['rows']), 'effective_frontend_verified': True}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'local-apply', 'local-verify', 'cloud-prepare', 'cloud-apply', 'cloud-verify', 'export-verify'])
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--db', type=Path, default=ROOT / 'output/ahaki.sqlite')
    parser.add_argument('--artifacts', type=Path, default=DEFAULT_ARTIFACTS)
    parser.add_argument('--inputs', type=Path, default=DEFAULT_ARTIFACTS / 'inputs.jsonl')
    parser.add_argument('--release', type=Path)
    parser.add_argument('--overlay', type=Path, default=DEFAULT_ARTIFACTS / 'efficiency/answer-audit/proposed_answer_overlay.json')
    args = parser.parse_args()
    try:
        if args.command == 'prepare':
            need(args.release is not None, '--release required for prepare'); result = prepare(args)
        elif args.command == 'local-apply': result = apply_local(args.plan)
        elif args.command == 'local-verify': result = {'phase': 'local_verified', 'applied': verify_local(load_plan(args.plan))}
        elif args.command == 'cloud-prepare': result = cloud_prepare(args.plan)
        elif args.command == 'export-verify': result = export_verify(args.plan)
        else: result = cloud_verify_or_apply(args.plan, args.command == 'cloud-apply')
    except (SafetyError, OSError, ValueError, KeyError, sqlite3.Error) as exc:
        parser.exit(1, 'Publication stopped: ' + str(exc) + '\n')
    print(json.dumps(result, ensure_ascii=False))

if __name__ == '__main__': main()
