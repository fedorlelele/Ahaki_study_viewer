import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync, sign, createHash} from 'node:crypto';
import {mkdtemp, writeFile, readFile, readdir, rm} from 'node:fs/promises';
import {basename, join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {createUpdater, filename, feedUrl, repository} from '../main/updates.mjs';
import {ReadingDictionary} from '../main/dictionary.mjs';
import {createSpeech} from '../main/speech.mjs';

async function workspace(t) {
  const dir = await mkdtemp(join(tmpdir(), 'asv-services-test-'));
  t.after(() => rm(dir, {recursive:true, force:true}));
  return dir;
}

const keys = generateKeyPairSync('ed25519');
const installers = {x64:Buffer.from('x64 installer fixture'), arm64:Buffer.from('arm installer fixture')};

function signedFeed(version = '0.10.0', signingKey = keys.privateKey) {
  const release = {product:'ASV-Windows', version, installers:{}};
  for (const [architecture, content] of Object.entries(installers)) {
    release.installers[architecture] = {bytes:content.length, sha256:createHash('sha256').update(content).digest('hex')};
  }
  const payload = Buffer.from(JSON.stringify(release));
  return JSON.stringify({payload:payload.toString('base64'), signature:sign(null, payload, signingKey).toString('base64')});
}

test('更新情報とアーキテクチャ別のファイルを照合してからインストーラーを一度だけ起動する', async t => {
  for (const arch of Object.keys(installers)) {
    const cache = await workspace(t), urls = [], states = [], launched = [];
    const asset = filename('0.10.0', arch);
    const updater = createUpdater({
      version:'0.9.9', arch, cache, key:keys.publicKey, notify:s => states.push(s.status),
      fetchImpl:async url => {
        urls.push(url);
        if (url === feedUrl) return new Response(signedFeed());
        assert.equal(url, repository + '/releases/download/windows-v0.10.0/' + asset);
        const data = installers[arch];
        return new Response(new ReadableStream({start(controller) {
          controller.enqueue(data.subarray(0, 5)); controller.enqueue(data.subarray(5)); controller.close();
        }}));
      },
      install:async local => {
        assert.equal(basename(local), asset);
        assert.deepEqual(await readFile(local), installers[arch]);
        launched.push(local);
      }
    });
    assert.equal((await updater.apply()).status, 'idle');
    assert.deepEqual((await Promise.all([updater.check(), updater.check()])).map(s => s.status), ['available', 'available']);
    assert.equal((await updater.apply()).status, 'installing');
    await updater.apply();
    assert.equal(launched.length, 1);
    assert.equal(urls.length, 2);
    assert.ok(states.includes('downloading'));
  }
});

test('未公開の更新、以前の版、署名を検証できないフィードを適用しない', async t => {
  const cache = await workspace(t);
  const tampered = JSON.parse(signedFeed());
  tampered.payload = Buffer.from(Buffer.from(tampered.payload, 'base64').toString().replace('0.10.0', '9.99.9')).toString('base64');
  for (const [getResponse, expected] of [
    [() => new Response(null, {status:404}), 'current'],
    [() => new Response(signedFeed('0.2.1')), 'current'],
    [() => new Response(signedFeed('0.9.9')), 'current'],
    [() => new Response(JSON.stringify(tampered)), 'error'],
    [() => new Response(signedFeed('0.10.0', generateKeyPairSync('ed25519').privateKey)), 'error']
  ]) {
    let installed = false;
    const updater = createUpdater({version:'0.9.9', arch:'x64', cache, key:keys.publicKey, notify() {},
      fetchImpl:async url => { assert.equal(url, feedUrl); return getResponse(); },
      install:async () => { installed = true; }});
    assert.equal((await updater.check()).status, expected);
    await updater.apply();
    assert.equal(installed, false);
  }
});

test('改変・不完全・取得失敗のインストーラーを起動せず、一時ファイルを削除する', async t => {
  const cache = await workspace(t), expected = installers.x64;
  for (const answer of [
    () => new Response(Buffer.alloc(expected.length, 1)),
    () => new Response(expected.subarray(0, expected.length - 1)),
    () => new Response(Buffer.concat([expected, Buffer.alloc(1)])),
    () => new Response(null, {status:404})
  ]) {
    let launched = false;
    const updater = createUpdater({version:'0.1.0', arch:'x64', cache, key:keys.publicKey, notify() {},
      fetchImpl:async url => url === feedUrl ? new Response(signedFeed()) : answer(),
      install:async () => { launched = true; }});
    assert.equal((await updater.check()).status, 'available');
    assert.equal((await updater.apply()).status, 'error');
    assert.equal(launched, false);
    assert.deepEqual(await readdir(cache), []);
  }
});

test('Windows読み辞書の文字コードと最長一致、英単語・単漢字の境界を判定する', () => {
  // CP932 with CRLF, exported in the same format as Jukugo.usr.
  const cp932 = Buffer.from('8fe398722c835783878345838f83930d0a8fe3987293f193aa8bd82c835783878345838f83938140836a83678345834c83930d0a8c6f8c8a2c83508343835083630d0a','hex');
  const dictionary = new ReadingDictionary(cp932);
  assert.equal(dictionary.count, 3);
  assert.equal(dictionary.apply('上腕二頭筋と経穴。\n上腕'), 'ジョウワン ニトウキンとケイケツ。\nジョウワン');
  const utf16 = Buffer.from('\ufeffRICE,仮\r\nRICE,ライス\r\n穴,ケツ\r\nケツ,再置換\r\n','utf16le');
  const revised = new ReadingDictionary(utf16);
  assert.equal(revised.count, 3);
  assert.equal(revised.apply('RICE・PRICE・RICED、穴と穴埋め、空穴'), 'ライス・PRICE・RICED、ケツと穴埋め、空穴');
});

test('辞書の読み込み直しが失敗しても既存ファイルとメモリ上の登録語数を保持する', async t => {
  const directory = await workspace(t), source = join(directory, 'external.usr'), userData = join(directory, 'user');
  let chosen = source;
  const service = createSpeech({userData, chooseFile:async () => chosen});
  const bytes = Buffer.from('経絡,ケイラク\r\n');
  await writeFile(source, bytes);
  assert.deepEqual(await service.importDictionary(), {count:1});
  await writeFile(source, '壊れた行');
  await assert.rejects(service.importDictionary(), /1行目/);
  assert.deepEqual(await service.dictionary(), {count:1});
  assert.deepEqual(await readFile(join(userData, 'Jukugo.usr')), bytes);
  assert.deepEqual(await readdir(userData), ['Jukugo.usr']);
  const restored = createSpeech({userData, chooseFile:async () => null});
  assert.deepEqual(await restored.dictionary(), {count:1});
  chosen = null;
  assert.deepEqual(await service.importDictionary(), {cancelled:true, count:1});
});

test('Windows標準のインストール済み音声で音声データを生成できる', {skip:process.platform !== 'win32', timeout:45000}, async t => {
  const directory = await workspace(t);
  const speech = createSpeech({userData:directory, chooseFile:async () => null});
  const voices = await speech.voices();
  assert.ok(Array.isArray(voices));
  if (!voices.length) { t.skip('このWindowsにSAPI音声がインストールされていません。'); return; }
  const voice = voices.find(v => v.language.startsWith('ja')) ?? voices[0];
  assert.equal(typeof voice.name, 'string');
  // Hosted Windows can have SAPI voices without an audio output device. Render to WAV for verification.
  const wav = join(directory, 'test.wav');
  const ps = join(process.env.SystemRoot, 'System32', 'WindowsPowerShell','v1.0','powershell.exe');
  const script = [
    "$ErrorActionPreference='Stop'",
    'Add-Type -AssemblyName System.Speech',
    '$s=New-Object System.Speech.Synthesis.SpeechSynthesizer',
    'try { $s.SelectVoice($env:ASV_TEST_VOICE); $s.SetOutputToWaveFile($env:ASV_TEST_WAV); $s.Speak($env:ASV_TEST_TEXT) } finally { $s.Dispose() }'
  ].join('\n');
  await promisify(execFile)(ps, ['-NoLogo','-NoProfile','-NonInteractive','-EncodedCommand',Buffer.from(script,'utf16le').toString('base64')], {
    windowsHide:true, timeout:25000, env:{...process.env, ASV_TEST_WAV:wav, ASV_TEST_VOICE:voice.id,
      ASV_TEST_TEXT:voice.language.startsWith('ja') ? '問題を読み上げます。' : 'Study question.'}
  });
  const audio = await readFile(wav);
  assert.equal(audio.toString('ascii', 0, 4), 'RIFF');
  assert.equal(audio.toString('ascii', 8, 12), 'WAVE');
  assert.ok(audio.length > 44);
});
