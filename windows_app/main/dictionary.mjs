import {readFile, mkdir, rename, writeFile, rm} from 'node:fs/promises';
import {dirname} from 'node:path';
import {randomUUID} from 'node:crypto';

function decode(bytes) {
  if (bytes[0] === 0xff && bytes[1] === 0xfe) return new TextDecoder('utf-16le', {fatal:true}).decode(bytes);
  if (bytes[0] === 0xfe && bytes[1] === 0xff) return new TextDecoder('utf-16be', {fatal:true}).decode(bytes);
  try { return new TextDecoder('utf-8', {fatal:true}).decode(bytes); }
  catch { return new TextDecoder('shift_jis', {fatal:true}).decode(bytes); }
}

const wordCharacter = /^[a-z0-9Ａ-Ｚａ-ｚ０-９]$/i;
const kanji = /^\p{Ideographic}$/u;

export class ReadingDictionary {
  root = {children: new Map()};
  count = 0;
  constructor(bytes) {
    let text;
    try { text = decode(bytes); } catch { throw new Error('読み辞書の文字コードを認識できません。'); }
    for (const [index, row] of text.split(/\r\n|[\n\r]/).entries()) {
      if (!row.trim()) continue;
      const fields = row.split(',').map(s => s.trim());
      if (fields.length !== 2 || !fields[0] || !fields[1]) throw new Error('読み辞書の' + (index+1) + '行目で見出し語と読みを認識できません。');
      let node = this.root;
      for (const char of fields[0]) {
        if (!node.children.has(char)) node.children.set(char, {children:new Map()});
        node = node.children.get(char);
      }
      if (!node.reading) this.count++;
      node.reading = fields[1].replaceAll('　', ' ');
    }
    if (!this.count) throw new Error('読み辞書に見出し語がありません。');
  }
  apply(text) {
    const chars = Array.from(text), output = [];
    for (let start = 0; start < chars.length;) {
      let node = this.root, end = start, reading;
      for (let i = start; i < chars.length; i++) {
        node = node.children.get(chars[i]);
        if (!node) break;
        if (!node.reading) continue;
        const before = chars[start-1] ?? '', after = chars[i+1] ?? '';
        if (wordCharacter.test(chars[start]) && wordCharacter.test(before)) continue;
        if (wordCharacter.test(chars[i]) && wordCharacter.test(after)) continue;
        if (i === start && kanji.test(chars[i]) && (kanji.test(before) || kanji.test(after))) continue;
        end = i + 1; reading = node.reading;
      }
      if (reading) { output.push(reading); start = end; }
      else output.push(chars[start++]);
    }
    return output.join('');
  }
}

export async function installDictionary(source, destination) {
  const bytes = await readFile(source);
  const dictionary = new ReadingDictionary(bytes);
  await mkdir(dirname(destination), {recursive:true});
  const temporary = destination + '.' + randomUUID() + '.tmp';
  try { await writeFile(temporary, bytes, {flag:'wx'}); await rename(temporary, destination); }
  finally { await rm(temporary, {force:true}); }
  return dictionary;
}
