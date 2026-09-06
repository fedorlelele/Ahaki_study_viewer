const test = require('node:test');
const assert = require('node:assert/strict');
const Q = require('../web_app/shared/questions.js');

const base = () => ({ serial: 'B20-095', subject: '生理学', stem: '旧問題', choices: ['1','2','3','4'], tags: ['旧タグ'], subtopics: ['旧項目'], answer_indices: [1], answer_index: 1, answer_none: false, answer_text: '1（点字問題は1、2）', answer_variants: { default: [1], braille: [1,2] }, answer_notes: ['点字問題は1、2'] });

test('only normal booklet answers are used even with legacy medium arguments', () => {
  const q=base();
  assert.equal(Q.isAnswerCorrect(q,2,'default'),false);
  assert.equal(Q.isAnswerCorrect(q,2,'braille'),false);
  assert.equal(Q.formatAnswerLabel(q,'braille',true),'１');
  assert.equal(Q.getAnswerNote(q),'');
  assert.deepEqual(Q.getAnswerIndices({choices:q.choices,answer_index:3},'braille'),[3]);
});
test('invalid selections never become correct for an excluded question', () => {
  const q={...base(), answer_none:true};
  for(const value of [null,0,5,-1,1.5,'bad']) assert.equal(Q.isAnswerCorrect(q,value),false);
  assert.equal(Q.isAnswerCorrect(q,1),true);
});
test('new cloud answer overrides replace stale variants and notes, without mutating static data', () => {
  const q=base();
  const result=Q.applyQuestionOverride(q,{serial:q.serial,answer_indices:[3],answer_none:false,stem:'訂正済',choices:['a','b','c','d'],updated_at:'2026-09-06T01:00:00Z',synced_at:'2026-09-06T02:00:00Z'});
  assert.equal(result.stem,'訂正済');
  assert.deepEqual(result.answer_variants,{default:[3]});
  assert.deepEqual(result.answer_notes,[]);
  assert.equal(Q.isAnswerCorrect(result,2,'braille'),false);
  assert.equal(q.stem,'旧問題');
});
test('synced cloud rows still apply until the exported version incorporates them', () => {
  const row={serial:'B20-095',stem:'訂正済',updated_at:'2026-09-06T01:00:00Z',synced_at:'2026-09-06T02:00:00Z'};
  assert.equal(Q.applyQuestionOverride(base(),row).stem,'訂正済');
  for(const version of ['2026-09-06T01:00:00Z','2026-09-06T03:00:00Z']) {
    const q={...base(),stem:'公開版',override_updated_at:version};
    assert.equal(Q.applyQuestionOverride(q,row),q);
  }
});
test('empty fields are meaningful overrides but unrelated false-only flags do not erase media answers', () => {
  const result=Q.applyQuestionOverride(base(),{tags:[],case_text:'',explanation:'',answer_none:false});
  assert.deepEqual(result.tags,[]);
  assert.equal(result.explanation_latest,'');
  assert.deepEqual(result.answer_variants,{default:[1],braille:[1,2]});
  const excluded=Q.applyQuestionOverride(base(),{answer_none:true});
  assert.deepEqual(excluded.answer_indices,[]);
  assert.equal(excluded.answer_text,'なし');
});
test('search indexes include cloud corrections before visiting a question', () => {
  const rows=Q.applyQuestionOverrides([base()],[{serial:'B20-095',tags:['新タグ'],subtopics:['新項目'],stem:'訂正済問題'}]);
  const index=Q.buildQuestionIndexes(rows);
  assert.deepEqual(index.byTag['新タグ'],['B20-095']);
  assert.equal(index.byTag['旧タグ'],undefined);
  assert.deepEqual(index.bySubtopic['新項目'],['B20-095']);
});
test('tag study updates related serials with the same exported timestamp contract', () => {
  const indexes={byTag:{old:['B20-095','A01-001']},bySubtopic:{old:['B20-095']}};
  Q.applyIndexOverrides(indexes,[{serial:'B20-095',tags:['new'],subtopics:[],updated_at:'2026-09-06T01:00:00Z'}],{});
  assert.deepEqual(indexes.byTag.old,['A01-001']);
  assert.deepEqual(indexes.byTag.new,['B20-095']);
  assert.deepEqual(indexes.bySubtopic.old,[]);
  Q.applyIndexOverrides(indexes,[{serial:'B20-095',tags:['obsolete'],updated_at:'2026-09-06T01:00:00Z'}],{'B20-095':'2026-09-06T02:00:00Z'});
  assert.equal(indexes.byTag.obsolete,undefined);
});
test('cloud loader paginates all rows and never filters out synced rows', async () => {
  const records=Array.from({length:1201},(_,i)=>({serial:`S${i}`,synced_at:'2026-09-06'}));
  const ranges=[];
  const client={from(table){assert.equal(table,'question_overrides');return this;},select(fields){assert.match(fields,/answer_indices/);assert.match(fields,/updated_at/);return this;},order(field){assert.equal(field,'serial');return this;},range(from,to){ranges.push([from,to]);return Promise.resolve({data:records.slice(from,to+1)});}};
  const rows=await Q.loadQuestionOverrides(client);
  assert.equal(rows.length,1201);
  assert.deepEqual(ranges,[[0,499],[500,999],[1000,1499]]);
});
test('a page error rejects the snapshot instead of returning partial corrections', async () => {
  let page=0;
  const client={from(){return this},select(){return this},order(){return this},range(){return Promise.resolve(page++ ? {error:new Error('outage')} : {data:[{serial:'A01-001'}]});}};
  await assert.rejects(Q.loadQuestionOverrides(client,{pageSize:1}),/outage/);
});
test('cloud loader is bounded when the server never answers', async () => {
  const client={from(){return this},select(){return this},order(){return this},range(){return new Promise(()=>{});}};
  await assert.rejects(Q.loadQuestionOverrides(client,{timeoutMs:10}),/タイムアウト/);
});
test('answer statistics use the aggregate API in batches, including more than 1000 source answers', async () => {
  const serials=Array.from({length:401},(_,i)=>`A01-${String(i+1).padStart(3,'0')}`);
  const calls=[];
  const items=await Q.loadAnswerStats('https://example.invalid',serials,{fetch:async url=>{
    calls.push(url);assert.match(url,/\/stats\/answers\?/);
    const requested=new URL(url).searchParams.get('serials').split(',');assert.ok(requested.length<=200);
    return {ok:true,json:async()=>({ok:true,items:requested.map(serial=>({serial,total:1201,correct:1001}))})};
  }});
  assert.equal(calls.length,3);assert.equal(items.length,401);assert.equal(items[0].total,1201);
});
test('unavailable aggregate API never falls back to raw answer data or invented zero counts', async () => {
  let calls=0;
  await assert.rejects(Q.loadAnswerStats('https://example.invalid',['A01-001'],{fetch:async()=>{calls++;return {ok:false,status:503};}}),/503/);
  assert.equal(calls,1);
  await assert.rejects(Q.loadAnswerStats('https://example.invalid',['A01-001'],{fetch:async()=>({ok:true,json:async()=>({ok:true,items:[]})})}),/不足/);
});
test('render generations reject late responses and cancellation invalidates a pending render', () => {
  const guard=Q.createRenderGuard();const first=guard.begin();const second=guard.begin();
  assert.equal(first(),false);assert.equal(second(),true);guard.cancel();assert.equal(second(),false);
});
test('optional storage failures do not affect normal booklet grading', () => {
  const previous=global.localStorage;
  global.localStorage={getItem(){throw Error('disabled')},setItem(){throw Error('quota')}};
  try { assert.equal(Q.getAnswerMedium(),'default');assert.equal(Q.setAnswerMedium('braille'),'default');assert.equal(Q.getAnswerMedium(),'default'); } finally { global.localStorage=previous;Q.setAnswerMedium('default'); }
});

test('generic teacher review sources keep the generator but replace stale AI review status', () => {
  const q={...base(),explanation_latest:'元解説',explanation_latest_source:'model:OldModel',explanation_latest_model_name:'OldModel',explanation_latest_review_status:'ai'};
  for(const [source,status] of [['llm_checked','teacher_approved'],['teacher','teacher_edited']]) {
    const result=Q.applyQuestionOverride(q,{serial:q.serial,explanation_source:source});
    assert.equal(result.explanation_latest,'元解説');
    assert.equal(result.explanation_latest_model_name,'OldModel');
    assert.equal(result.explanation_latest_review_status,status);
    assert.equal(Q.getExplanationMetadata(result.explanation_latest_source).model_name,'OldModel');
    assert.equal(Q.getExplanationMetadata(result.explanation_latest_source).review_status,status);
  }
});
test('an explicitly named cloud model takes precedence over metadata from the previous revision', () => {
  const q={...base(),explanation_latest:'old',explanation_latest_source:'model:OldModel',explanation_latest_model_name:'OldModel',explanation_latest_review_status:'ai'};
  for(const source of ['model:NewModel:checked','NewModel_checked']) {
    const result=Q.applyQuestionOverride(q,{serial:q.serial,explanation:'new',explanation_source:source});
    assert.equal(result.explanation_latest,'new');
    assert.equal(result.explanation_latest_model_name,'NewModel');
    assert.equal(result.explanation_latest_review_status,'teacher_approved');
  }
});
test('body-only cloud edits retain the model and become teacher edits; unchanged text keeps its status', () => {
  const q={...base(),explanation_latest:'old',explanation_latest_source:'model:OldModel:checked',explanation_latest_model_name:'OldModel',explanation_latest_review_status:'teacher_approved'};
  const result=Q.applyQuestionOverride(q,{serial:q.serial,explanation:'new',explanation_source:null});
  assert.equal(result.explanation_latest_model_name,'OldModel');
  assert.equal(result.explanation_latest_review_status,'teacher_edited');
  assert.equal(result.explanation_latest_source,'model:OldModel:teacher');
  assert.equal(Q.applyQuestionOverride(q,{explanation:'old'}).explanation_latest_review_status,'teacher_approved');
});
test('an already incorporated cloud revision preserves the published explanation metadata unchanged', () => {
  const q={...base(),explanation_latest:'同期後',explanation_latest_source:'model:PublishedModel:checked',explanation_latest_model_name:'PublishedModel',explanation_latest_review_status:'teacher_approved',override_updated_at:'2026-09-06T01:00:00Z'};
  const row={serial:q.serial,explanation:'クラウド履歴',explanation_source:'model:OldModel',updated_at:'2026-09-06T01:00:00Z'};
  assert.equal(Q.applyQuestionOverride(q,row),q);
});

test('saved braille preference is ignored and normal multiple answers and notes survive', () => {
  const previous=global.localStorage;
  global.localStorage={getItem:()=> 'braille'};
  try {
    assert.equal(Q.getAnswerMedium(),'default');
    assert.equal(Q.isAnswerCorrect(base(),2),false);
    const q={...base(),answer_variants:{default:[1,3],braille:[2]},answer_notes:['採点対象外','(点字：２．)']};
    assert.equal(Q.formatAnswerLabel(q),'1・3');
    assert.equal(Q.isAnswerCorrect(q,3),true);
    assert.equal(Q.getAnswerNote(q),'採点対象外');
  } finally { global.localStorage=previous; }
});
