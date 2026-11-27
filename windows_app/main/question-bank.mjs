import {createHash, randomUUID} from 'node:crypto';
import {mkdir, readFile, rename, rm, writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {validateSnapshot} from '../renderer/study-core.mjs';
import {publicKey, supabaseURL} from './cloud-config.mjs';

const pages = 'https://fedorlelele.github.io/Ahaki_study_viewer/output/web/';
const webFields = ['serial','exam_type','exam_session','subject','case_text','stem','choices',
  'answer_index','answer_indices','answer_none','answer_variants','answer_notes','override_updated_at',
  'explanation_latest','explanation_latest_source','explanation_latest_model_name',
  'explanation_latest_review_status','tags','subtopics'];
const pageSize = 500;

function checkedManifest(manifest) {
  if (!Number.isSafeInteger(manifest?.total) || manifest.total < 1 ||
      !Number.isFinite(Date.parse(manifest.generated_at))) throw new Error('問題一覧の版情報が不正です。');
  return {generated_at:manifest.generated_at, total:manifest.total};
}
function checkedSnapshot(snapshot) {
  checkedManifest(snapshot?.manifest);
  const rows = validateSnapshot(snapshot);
  for (const q of rows) {
    if (typeof q.exam_type !== 'string' || !Number.isSafeInteger(q.exam_session) ||
        typeof q.subject !== 'string' || q.choices.some(value => typeof value !== 'string') ||
        [q.case_text,q.explanation_latest].some(value => value != null && typeof value !== 'string') ||
        [q.tags,q.subtopics].some(value => value != null && (!Array.isArray(value) ||
          value.some(label => typeof label !== 'string')))) throw new Error('問題データの形式が不正です。');
  }
  return snapshot;
}
async function jsonBody(response, maxBytes) {
  if (!response.ok) throw new Error('問題データを取得できません（HTTP ' + response.status + '）。');
  const chunks = [];
  let size = 0;
  for await (const chunk of response.body) {
    size += chunk.length;
    if (size > maxBytes) throw new Error('問題データのサイズが上限を超えました。');
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
async function fetchOverrides(fetcher, fields) {
  const rows = [];
  const url = new URL('/rest/v1/question_overrides',supabaseURL);
  url.searchParams.set('select',fields.replaceAll(' ',''));
  url.searchParams.set('order','serial.asc');
  let expected;
  do {
    url.searchParams.set('offset',String(rows.length));
    url.searchParams.set('limit',String(pageSize));
    const response = await fetcher(url, {headers:{apikey:publicKey, Prefer:'count=exact'},
      redirect:'error', signal:AbortSignal.timeout(60000)});
    const page = await jsonBody(response,16 * 1024 * 1024);
    if (!Array.isArray(page)) throw new Error('訂正データの形式が不正です。');
    const range = response.headers.get('content-range');
    const total = Number(range?.match(/\/(\d+)$/)?.[1]);
    if (!Number.isSafeInteger(total) || (expected !== undefined && expected !== total) ||
        rows.length + page.length > total || (!page.length && rows.length !== total)) {
      throw new Error('訂正一覧の全件を確認できませんでした。');
    }
    expected = total;
    for (const row of page) {
      if (typeof row?.serial !== 'string' || !row.serial || (rows.length && row.serial <= rows.at(-1).serial)) {
        throw new Error('訂正一覧の問題番号が不正です。');
      }
      rows.push(row);
    }
  } while (rows.length < expected);
  return rows;
}
function revision(snapshot) {
  return createHash('sha256').update(JSON.stringify(snapshot)).digest('hex');
}

export function createQuestionBank({directory, bundledPath, contract, fetcher = fetch}) {
  const path = join(directory,'question-bank-v1.json');
  let loading, syncing, base, etag, current, currentRevision, syncedAt;

  function apply(baseSnapshot, overrides) {
    if (!Array.isArray(overrides)) throw new Error('訂正データを読み込めません。');
    return checkedSnapshot({manifest:baseSnapshot.manifest,
      questions:contract.applyQuestionOverrides(baseSnapshot.questions,overrides)});
  }
  async function load() {
    try {
      const cached = JSON.parse(await readFile(path,'utf8'));
      const savedBase = checkedSnapshot(cached.base);
      const saved = apply(savedBase,cached.overrides);
      if (!Number.isFinite(Date.parse(cached.syncedAt)) || revision(saved) !== cached.revision) {
        throw new Error('保存済みの教材を確認できません。');
      }
      base = savedBase;
      etag = cached.etag;
      current = saved;
      currentRevision = cached.revision;
      syncedAt = cached.syncedAt;
    } catch {
      current = checkedSnapshot(JSON.parse(await readFile(bundledPath,'utf8')));
      currentRevision = revision(current);
    }
  }
  async function home() {
    loading ??= load().catch(e => { loading = undefined; throw e; });
    await loading;
    return {...current,syncedAt};
  }
  async function refresh() {
    await home();
    const manifest = checkedManifest(await jsonBody(await fetcher(pages + 'questions_manifest.json',
      {cache:'no-cache',signal:AbortSignal.timeout(30000)}),2 * 1024 * 1024));
    if (Date.parse(manifest.generated_at) < Date.parse(current.manifest.generated_at)) {
      throw new Error('公開データの版が保存済みの教材より古いため、更新を見送りました。');
    }
    const sameBase = base?.manifest.generated_at === manifest.generated_at && base.manifest.total === manifest.total;
    const url = new URL('questions.json',pages);
    url.searchParams.set('v',manifest.generated_at);
    const response = await fetcher(url,{headers:sameBase && etag ? {'If-None-Match':etag} : {},
      cache:'no-cache',signal:AbortSignal.timeout(180000)});
    let nextBase, nextEtag;
    if (response.status === 304 && sameBase && etag) {
      nextBase = base;
      nextEtag = etag;
    } else {
      const rows = await jsonBody(response,128 * 1024 * 1024);
      if (!Array.isArray(rows)) throw new Error('公開された問題一覧を読み込めません。');
      // Only keep fields used by the study reader and Web's correction rules.
      const questions = rows.map(q => Object.fromEntries(webFields.filter(k => Object.hasOwn(q,k)).map(k => [k,q[k]])));
      nextBase = checkedSnapshot({manifest,questions});
      nextEtag = response.headers.get('etag');
    }
    // Reapply the complete cloud table to the original export on every check.
    // An override removed on the server must also disappear from the offline bank.
    const overrides = await fetchOverrides(fetcher,contract.OVERRIDE_FIELDS);
    const next = apply(nextBase, overrides);
    const nextRevision = revision(next);
    const nextTime = new Date().toISOString();
    const tmp = path + '.' + randomUUID() + '.tmp';
    await mkdir(directory,{recursive:true});
    try {
      await writeFile(tmp,JSON.stringify({base:nextBase,etag:nextEtag,overrides,
        revision:nextRevision,syncedAt:nextTime}),{flag:'wx'});
      await rename(tmp,path);
    } finally { await rm(tmp,{force:true}).catch(() => {}); }
    const changed = currentRevision !== nextRevision;
    base = nextBase; etag = nextEtag; current = next; currentRevision = nextRevision; syncedAt = nextTime;
    return {snapshot:changed ? current : null, syncedAt};
  }
  return {
    home,
    sync() {
      syncing ??= refresh().finally(() => { syncing = undefined; });
      return syncing;
    }
  };
}
