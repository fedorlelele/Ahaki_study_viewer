import {spawn} from 'node:child_process';
import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {ReadingDictionary, installDictionary} from './dictionary.mjs';

const script = readFileSync(new URL('./speech.ps1', import.meta.url), 'utf8');
const args = ['-NoLogo','-NoProfile','-NonInteractive','-Command',script];

export function createSpeech({userData, chooseFile}) {
  const destination = join(userData, 'Jukugo.usr');
  let dictionary = null, current = null, loading, importInProgress;
  function load() {
    loading ??= readFile(destination).then(bytes => { dictionary = new ReadingDictionary(bytes); })
      .catch(error => { if (error.code !== 'ENOENT') { loading = undefined; throw error; } });
    return loading;
  }
  function stop() {
    if (current) { current.stopped = true; current.child?.kill(); current = null; }
  }
  function run(request, utterance) {
    return new Promise((resolve, reject) => {
      if (process.platform !== 'win32') { reject(new Error('この読み上げ機能はWindowsで利用できます。')); return; }
      const executable = join(process.env.SystemRoot || 'C:\\Windows', 'System32', 'WindowsPowerShell','v1.0','powershell.exe');
      const child = spawn(executable, args, {windowsHide:true, stdio:['pipe','pipe','pipe']});
      if (utterance) utterance.child = child;
      let output = '', errorText = '';
      child.stdout.setEncoding('utf8').on('data', s => {
        output += s;
        if (utterance && output.includes('READY')) resolve();
      });
      child.stderr.setEncoding('utf8').on('data', s => { errorText = (errorText+s).slice(-1000); });
      child.once('error', reject);
      child.stdin.on('error', () => {}); // Stop may close the pipe during startup.
      child.once('close', code => {
        if (utterance && current === utterance) current = null;
        if (utterance?.stopped) return resolve();
        if (code !== 0) return reject(new Error('Windowsの読み上げを開始できません。音声と言語の設定を確認してください。 ' + errorText.trim()));
        try { resolve(utterance ? undefined : JSON.parse(output)); } catch { reject(new Error('Windowsの音声一覧を取得できません。')); }
      });
      child.stdin.end(JSON.stringify(request), 'utf8');
    });
  }
  return {
    stop,
    voices: () => run({action:'voices'}),
    async dictionary() { await load(); return {count:dictionary?.count ?? 0}; },
    async importDictionary() {
      if (importInProgress) return importInProgress;
      importInProgress = (async () => {
        const path = await chooseFile();
        if (!path) return {cancelled:true,count:dictionary?.count ?? 0};
        // Wait for an earlier read before replacing an existing or damaged dictionary.
        await load().catch(() => {});
        dictionary = await installDictionary(path, destination);
        loading = Promise.resolve();
        return {count:dictionary.count};
      })();
      try { return await importInProgress; } finally { importInProgress = undefined; }
    },
    async speak({text,rate = 0,voice = '',dictionary:enabled = true} = {}) {
      if (typeof text !== 'string' || !text.trim()) return;
      stop();
      const utterance = {stopped:false};
      current = utterance;
      if (enabled) await load();
      if (utterance.stopped) return;
      const spoken = enabled && dictionary ? dictionary.apply(text) : text;
      await run({action:'speak', text:spoken, rate: Math.max(-10,Math.min(10,Math.round(Number(rate)||0))), voice:String(voice)}, utterance);
    }
  };
}
