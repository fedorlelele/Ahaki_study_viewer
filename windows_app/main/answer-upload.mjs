import {createHash} from 'node:crypto';
import {publicKey, supabaseURL} from './cloud-config.mjs';

const endpoint = new URL('/rest/v1/answers', supabaseURL);
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const collision = Symbol('duplicate row');

function databaseRow(anonId, event, slot = 0) {
  const hash = createHash('sha256').update(event.client_event_id.toLowerCase() + ':' + slot).digest();
  // Web answers get positive sequence IDs. Avoid converting a bigint to a JS number.
  const id = -(BigInt.asUintN(63, hash.readBigUInt64BE()) || 1n);
  return {id: String(id), anon_id: anonId, serial: event.serial, selected_index: event.selected_index,
    is_correct: event.is_correct, created_at: event.created_at};
}

export async function uploadAnswers(anonId, events, fetcher = fetch) {
  if (typeof anonId !== 'string' || !uuidPattern.test(anonId) || !Array.isArray(events) ||
      !events.length || events.length > 100 || events.some(e => !e || !uuidPattern.test(e.client_event_id) ||
        typeof e.serial !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(e.serial) ||
        !Number.isInteger(e.selected_index) || e.selected_index < 1 || e.selected_index > 100 ||
        typeof e.is_correct !== 'boolean' || typeof e.created_at !== 'string' ||
        !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(e.created_at) || !Number.isFinite(Date.parse(e.created_at)))) {
    throw new Error('回答データを検証できません。');
  }
  async function insert(rows) {
    const response = await fetcher(endpoint, {method:'POST', redirect:'error', signal:AbortSignal.timeout(30000),
      headers:{apikey:publicKey, 'content-type':'application/json', Prefer:'return=minimal'}, body:JSON.stringify(rows)});
    if (response.ok) return;
    if (response.status === 409 && (await response.json()).code === '23505') return collision;
    throw new Error('回答を送信できません。');
  }
  async function exists(filters) {
    const url = new URL(endpoint);
    url.searchParams.set('select','id');
    url.searchParams.set('limit','1');
    for (const [key, value] of Object.entries(filters)) url.searchParams.set(key,'eq.' + value);
    const response = await fetcher(url, {headers:{apikey:publicKey}, cache:'no-store', redirect:'error',
      signal:AbortSignal.timeout(30000)});
    if (!response.ok) throw new Error('送信済みの回答を確認できません。');
    const result = await response.json();
    if (!Array.isArray(result)) throw new Error('回答の照合結果を取得できません。');
    return result.length === 1;
  }
  const rows = events.map(event => databaseRow(anonId, event));
  if (await insert(rows) !== collision) return;
  // A retried batch can mix previously committed attempts and brand-new ones.
  for (const [i, event] of events.entries()) {
    let row = rows[i];
    for (let slot = 0; ; slot++) {
      if (await exists(row)) break;
      if (await insert([row]) !== collision || await exists(row)) break;
      if (!await exists({id:row.id}) || slot >= 7) throw new Error('送信済みの回答を照合できません。');
      row = databaseRow(anonId, event, slot + 1);
    }
  }
}
