import assert from 'node:assert/strict';
import {test} from 'node:test';
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import contract from '../../web_app/shared/questions.js';
import {createQuestionBank} from '../main/question-bank.mjs';

const questions = [
  {serial:'A01-001',exam_session:1,exam_type:'あん摩',subject:'医療概論',stem:'問題１',
    case_text:'症例',choices:['甲','乙'],answer_index:1,answer_indices:[1],answer_variants:{default:[1]},
    answer_none:false,explanation_latest:'公開時の解説',override_updated_at:'2026-10-01T00:00:00Z'},
  {serial:'A01-002',exam_session:1,exam_type:'あん摩',subject:'医療概論',stem:'問題２',
    choices:['甲','乙'],answer_index:2,explanation_latest:'２問目の解説'}
];
const manifest = {total:2,generated_at:'2026-10-16T00:00:00Z'};
async function setup(t) {
  const directory = await mkdtemp(join(tmpdir(),'asv-bank-'));
  t.after(() => rm(directory,{recursive:true,force:true}));
  const bundledPath = join(directory,'bundled.json');
  await writeFile(bundledPath, JSON.stringify({manifest,questions}));
  return {directory,bundledPath,contract};
}
function fixture() {
  const service = {rows:questions,manifest,overrides:[],baseRequests:0,failAtPage:false};
  service.fetcher = async (url,options) => {
    const target = new URL(url);
    if (target.pathname.endsWith('/questions_manifest.json')) return Response.json(service.manifest);
    if (target.pathname.endsWith('/questions.json')) {
      service.baseRequests++;
      if (options.headers['If-None-Match'] === '"unchanged"') return new Response(null,{status:304});
      return Response.json(service.rows,{headers:{etag:'"unchanged"'}});
    }
    assert.equal(target.pathname,'/rest/v1/question_overrides');
    assert.ok(options.headers.apikey.startsWith('sb_publishable_'));
    assert.ok(target.searchParams.get('select').includes('answer_none'));
    const offset = Number(target.searchParams.get('offset'));
    // The server can impose a smaller limit than the requested range.
    if (service.failAtPage && offset > 0) return new Response(null,{status:503});
    const page = service.overrides.slice(offset,offset+1);
    return Response.json(page,{headers:{'content-range':page.length ? offset + '-' + offset + '/' + service.overrides.length : '*/0'}});
  };
  return service;
}

test('load locally, merge current Web corrections, and undo deleted overrides after a 304 for the base', async t => {
  const options = await setup(t);
  const web = fixture();
  web.overrides = [
    {serial:'A01-001',updated_at:'2026-11-01T00:00:00Z',synced_at:'2026-11-02T00:00:00Z',
      explanation:'新しい解説',case_text:'',answer_indices:[2],explanation_source:'model:テスト:checked'},
    {serial:'A01-002',updated_at:'2026-11-01T00:00:00Z',explanation:''}
  ];
  const bank = createQuestionBank({...options,fetcher:web.fetcher});
  assert.equal((await bank.home()).questions[0].stem,'問題１');
  assert.equal(web.baseRequests,0);
  const request = bank.sync();
  assert.equal(bank.sync(),request,'concurrent refreshes share a single transaction');
  const {snapshot,syncedAt} = await request;
  assert.equal(snapshot.questions[0].case_text,'');
  assert.equal(snapshot.questions[0].explanation_latest,'新しい解説');
  assert.deepEqual(snapshot.questions[0].answer_variants.default,[2]);
  assert.equal(snapshot.questions[0].explanation_latest_model_name,'テスト');
  assert.equal(snapshot.questions[1].explanation_latest,'');
  const offline = createQuestionBank({...options,fetcher:() => {throw Error('offline');}});
  assert.equal((await offline.home()).syncedAt,syncedAt);
  await assert.rejects(offline.sync(),/offline/);
  assert.deepEqual((await offline.home()).questions,snapshot.questions);
  web.overrides = [{serial:'A01-001',updated_at:'2026-09-01T00:00:00Z',explanation:'古い訂正'}];
  assert.equal((await bank.sync()).snapshot.questions[0].explanation_latest,'公開時の解説');
  assert.equal((await bank.home()).questions[1].explanation_latest,'２問目の解説');
  assert.equal((await bank.sync()).snapshot,null);
});

test('an incomplete page, invalid new export or stale manifest cannot replace the usable disk cache', async t => {
  const options = await setup(t), web = fixture();
  const bank = createQuestionBank({...options,fetcher:web.fetcher});
  await bank.sync();
  const path = join(options.directory,'question-bank-v1.json');
  const cached = await readFile(path,'utf8');
  web.overrides = [{serial:'A01-001',explanation:'中間結果'}, {serial:'A01-002',case_text:'症例'}];
  web.failAtPage = true;
  await assert.rejects(bank.sync(),/503/);
  assert.equal(await readFile(path,'utf8'),cached);
  web.manifest = {total:3,generated_at:'2026-11-01T00:00:00Z'};
  await assert.rejects(bank.sync(),/件数/);
  web.manifest = {total:2,generated_at:'2024-11-01T00:00:00Z'};
  await assert.rejects(bank.sync(),/古い/);
  assert.equal((await bank.home()).questions[0].explanation_latest,'公開時の解説');
  assert.equal(await readFile(path,'utf8'),cached);
  await writeFile(path,'{interrupted');
  assert.deepEqual((await createQuestionBank(options).home()).questions,questions);
});
