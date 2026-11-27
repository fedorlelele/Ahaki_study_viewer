import assert from 'node:assert/strict';
import {test} from 'node:test';
import {randomUUID} from 'node:crypto';
import {createAnswerHistory} from '../renderer/answer-history.mjs';
import {uploadAnswers} from '../main/answer-upload.mjs';

const legacy = 'asv.windows.answers.v1';
const serial = 'A01-001';
const event = () => ({client_event_id: randomUUID(), serial, is_correct: false,
  selected_index: 2, created_at: '2026-01-02T03:04:05.006Z'});

test('old selections stay local; two offline attempts survive restart with their original identities and times', async () => {
  const entries = new Map([[legacy, JSON.stringify({[serial]:3})]]);
  const messages = [];
  const options = {storage:{getItem:key=>entries.get(key) ?? null, setItem:(key,value)=>entries.set(key,value)},
    uuid:randomUUID, notify:()=>{}, reportError:assert.fail,
    upload:async (anon,batch)=>{messages.push({anon,batch}); throw Error('offline');}};
  const first = createAnswerHistory(options);
  assert.equal(first.choices[serial],3);
  assert.equal(first.pendingCount,0);
  first.retry(serial);
  first.record(serial,2,false);
  first.retry(serial);
  first.record(serial,3,true);
  await new Promise(resolve=>setImmediate(resolve));
  let release;
  const resumed = createAnswerHistory({...options, upload:async (anon,batch)=>{
    messages.push({anon,batch});
    if (batch.length === 2) await new Promise(resolve=>{release=resolve;});
  }});
  assert.equal(resumed.pendingCount,2);
  assert.equal(resumed.choices[serial],3);
  const sending = resumed.flush();
  resumed.record('B02-001',1,false);
  release();
  await sending;
  assert.deepEqual(messages[1].batch[0], messages[0].batch[0]);
  assert.equal(messages[1].anon, messages[0].anon);
  assert.notEqual(messages[1].batch[0].client_event_id,messages[1].batch[1].client_event_id);
  assert.equal(messages[2].batch[0].serial,'B02-001');
  assert.equal(createAnswerHistory(options).pendingCount,0);
});

test('delivery recovers from a lost acknowledgement, overlapping batches and an occupied bigint slot', async () => {
  const rows = new Map();
  const anon = randomUUID(), a=event(), b=event();
  let loseResponse = true;
  const fetcher = async (url, options) => {
    assert.equal(url.pathname,'/rest/v1/answers');
    if (options.method === 'POST') {
      const batch = JSON.parse(options.body);
      assert.ok(batch.every(r=>/^-[1-9]\d*$/.test(r.id) && !('client_event_id' in r)));
      if (batch.some(r=>rows.has(r.id))) return Response.json({code:'23505'}, {status:409});
      for (const row of batch) rows.set(row.id,row);
      if (loseResponse) {loseResponse=false; throw Error('connection lost');}
      return new Response(null,{status:201});
    }
    assert.equal(url.searchParams.get('select'),'id');
    const filters = [...url.searchParams].filter(([k])=>k!=='select' && k!=='limit');
    const match = [...rows.values()].some(row=>filters.every(([k,v])=>'eq.' + row[k] === v));
    return Response.json(match ? [{id:1}] : []);
  };
  await assert.rejects(uploadAnswers(anon,[a],fetcher),/connection lost/);
  await uploadAnswers(anon,[a,b],fetcher);
  assert.equal(rows.size,2);
  assert.deepEqual([...rows.values()].map(r=>r.created_at),[a.created_at,b.created_at]);
  // A different attempt already owns the first ID. Preserve it and reuse the
  // same alternate ID if acknowledgement is lost again.
  const firstRow = rows.values().next().value;
  firstRow.serial='B02-001';
  await uploadAnswers(anon,[a],fetcher);
  await uploadAnswers(anon,[a],fetcher);
  assert.equal(rows.size,3);
  assert.equal(firstRow.serial,'B02-001');
});
