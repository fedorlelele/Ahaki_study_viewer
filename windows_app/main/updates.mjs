import {createPublicKey, createHash, verify} from 'node:crypto';
import {mkdtemp, mkdir, open, rm} from 'node:fs/promises';
import {join} from 'node:path';

export const repository = 'https://github.com/fedorlelele/Ahaki_study_viewer';
export const feedUrl = repository + '/releases/download/windows-updates/windows-update.json';
export const publicKey = createPublicKey({
  key: Buffer.concat([Buffer.from('302a300506032b6570032100','hex'), Buffer.from('FGgpRm1KcSgyv2jNTy8Fj0ky8hvtP9/K7gs0lwh89MA=', 'base64')]),
  type:'spki', format:'der'
});
const versionPattern = /^(?:0|[1-9]\d{0,7})(?:\.(?:0|[1-9]\d{0,7})){2}$/;
export const filename = (version,arch) => 'ASV-Windows-' + version + '-' + arch + '-Setup.exe';
export function readFeed(text, arch, key = publicKey) {
  const {payload,signature} = JSON.parse(text);
  const bytes = Buffer.from(payload,'base64');
  if (!verify(null, bytes, key, Buffer.from(signature,'base64'))) throw new Error('更新情報の署名を確認できません。');
  const release = JSON.parse(bytes.toString('utf8'));
  const asset = release.installers?.[arch];
  if (release.product !== 'ASV-Windows' || !versionPattern.test(release.version) ||
      !['x64','arm64'].includes(arch) || !asset || !Number.isSafeInteger(asset.bytes) ||
      asset.bytes <= 0 || asset.bytes > 512*1024*1024 || !/^[a-f0-9]{64}$/.test(asset.sha256)) {
    throw new Error('このWindowsで使える更新情報を確認できません。');
  }
  return {version:release.version, bytes:asset.bytes, sha256:asset.sha256, name:filename(release.version, arch)};
}
export function isNewer(candidate,current) {
  if (![candidate,current].every(v => versionPattern.test(v))) return false;
  const old = current.split('.').map(Number);
  for (const [i,n] of candidate.split('.').map(Number).entries()) {
    if (n !== old[i]) return n > old[i];
  }
  return false;
}
export function createUpdater({version,arch,cache,notify,install,fetchImpl = fetch,key = publicKey}) {
  let state = {status:'idle',message:'現在のバージョン ' + version}, release = null, pending = null;
  function change(status,message,extra = {}) {
    state = {status,message,...extra}; notify(state); return state;
  }
  function perform(work) {
    if (pending) return pending;
    pending = work().catch(error => change('error',error.message || '更新を完了できませんでした。'))
      .finally(() => { pending = null; });
    return pending;
  }
  return {
    status: () => state,
    check: () => perform(async () => {
      release = null;
      change('checking','更新を確認しています。');
      const response = await fetchImpl(feedUrl,{signal:AbortSignal.timeout(30000)});
      if (response.status === 404) return change('current','Windows版の更新はまだ公開されていません。');
      if (!response.ok) throw new Error('更新情報を取得できません。インターネット接続を確認してください。');
      const buffers=[]; let size=0;
      for await (const bytes of response.body) {
        size += bytes.length;
        if (size > 64*1024) throw new Error('更新情報が大きすぎます。');
        buffers.push(bytes);
      }
      const candidate = readFeed(Buffer.concat(buffers).toString(),arch,key);
      if (!isNewer(candidate.version,version)) return change('current','現在のバージョンは最新です。');
      release = candidate;
      return change('available','バージョン ' + release.version + ' に更新できます。',{version:release.version});
    }),
    apply: () => perform(async () => {
      if (!release || state.status === 'installing') return state;
      const asset = release;
      change('downloading','更新をダウンロードしています。',{version:asset.version});
      await mkdir(cache,{recursive:true});
      const directory = await mkdtemp(join(cache, 'asv-'));
      try {
        const url = repository + '/releases/download/windows-v' + asset.version + '/' + asset.name;
        const response = await fetchImpl(url,{signal:AbortSignal.timeout(10*60*1000)});
        if (!response.ok) throw new Error('インストーラーを取得できません。更新を再確認してください。');
        const path = join(directory, asset.name), hash = createHash('sha256');
        const file = await open(path,'wx');
        let size=0, progress=0;
        try {
          for await (const bytes of response.body) {
            size += bytes.length;
            if (size > asset.bytes) throw new Error('更新ファイルのサイズが一致しません。');
            hash.update(bytes); await file.writeFile(bytes);
            const percent = Math.floor(size / asset.bytes * 100);
            if (percent >= progress + 10) { progress = percent; change('downloading','ダウンロード中 ' + percent + '%',{version:asset.version}); }
          }
        } finally { await file.close(); }
        if (size !== asset.bytes || hash.digest('hex') !== asset.sha256) throw new Error('更新ファイルの検証に失敗しました。');
        await install(path);
        return change('installing','インストーラーを起動しました。',{version:asset.version});
      } catch (error) {
        await rm(directory,{recursive:true,force:true}).catch(() => {});
        throw error;
      }
    })
  };
}
