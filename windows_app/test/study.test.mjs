import assert from 'node:assert/strict';
import {test} from 'node:test';
import {readFile} from 'node:fs/promises';
import {JSDOM} from 'jsdom';
import {answerLabel, answerNote, correctChoices, defaultShortcuts, isCorrect, keyChord,
  search, searchIndex, shortcutError, solutionText, validateSnapshot} from '../renderer/study-core.mjs';

const a = {serial:'A01-001', exam_type:'あん摩', exam_session:1, subject:'医療概論',
  case_text:'症例：朝に来院', stem:'多いのはどれか。', choices:['甲','乙','丙','丁'],
  answer_index:1, answer_indices:[1], answer_variants:{default:[2,3],braille:[1]},
  answer_notes:['通常の注記','点字問題の注記'], tags:['呼吸'], subtopics:['現代の医療'], explanation_latest:'学習用の解説。'};
const b = {serial:'B33-141', exam_type:'はり師', exam_session:33, subject:'総合問題',
  stem:'関節リウマチ', choices:['朝','昼'], answer_indices:[], answer_none:true, tags:['呼吸器']};
const dataset = {manifest:{generated_at:'2026-10-16',total:2},questions:[a,b]};

test('normal booklet, multiple answers, all-credit and invalid records', () => {
  assert.equal(validateSnapshot(dataset).length, 2);
  assert.deepEqual(correctChoices(a), [2,3]);
  assert.equal(isCorrect(a, 1), false);
  assert.equal(isCorrect(a, 2), true);
  assert.equal(isCorrect(a, 3), true);
  assert.equal(answerNote(a), '通常の注記');
  assert.match(solutionText(a), /正答 2・3/);
  assert.equal(answerLabel(b), 'なし（全員正解）');
  assert.equal(isCorrect(b, 2), true);
  assert.equal(isCorrect(b, 3), false);
  assert.throws(() => validateSnapshot({...dataset,questions:[a,a]}));
  assert.throws(() => validateSnapshot({...dataset,questions:[{...a,answer_variants:{default:[5]}},b]}));
});

test('fullwidth, serial shortcuts, exact tags, AND/OR and filters match Mac search', () => {
  const index = searchIndex([a,b]);
  const ids = criteria => search(index,criteria).map(q => q.serial);
  assert.deepEqual(ids({query:'Ａ１００１'}), ['A01-001']);
  assert.deepEqual(ids({query:'Ｂ３３－１４１'}), ['B33-141']);
  assert.deepEqual(ids({query:'＃呼吸'}), ['A01-001']);
  assert.deepEqual(ids({query:'朝 甲'}), ['A01-001']);
  assert.deepEqual(ids({query:'現代、リウマチ'}), ['A01-001','B33-141']);
  assert.deepEqual(ids({exam:'はり師',subject:'総合問題',session:'33'}), ['B33-141']);
  assert.deepEqual(ids({subject:'医療概論',session:33}), []);
});

test('JIS and US plus shortcuts share a chord, editing and IME commands are protected', () => {
  for (const key of ['+','=']) assert.equal(keyChord({key,ctrlKey:true,shiftKey:key==='+'}), 'Ctrl++');
  assert.equal(keyChord({key:'Add',code:'NumpadAdd',ctrlKey:true}), 'Ctrl++');
  assert.equal(keyChord({key:'A',ctrlKey:true,shiftKey:true}), 'Ctrl+Shift+A');
  assert.equal(keyChord({key:'f',ctrlKey:true,keyCode:229}), '');
  assert.match(shortcutError('search','Ctrl+C',defaultShortcuts), /標準操作/);
  assert.match(shortcutError('search','Ctrl+J',defaultShortcuts), /検索結果/);
  assert.equal(shortcutError('search','Ctrl+Alt+K',defaultShortcuts), '');
});

test('study renderer searches, grades, retries, navigates with keyboard and uses settings', async t => {
  const dom = new JSDOM(await readFile(new URL('../renderer/index.html',import.meta.url),'utf8'), {url:'https://asv.test/'});
  const w = dom.window, d = w.document;
  const $ = id => d.getElementById(id);
  w.HTMLDialogElement.prototype.showModal=function(){ this.open=true; };
  w.HTMLDialogElement.prototype.close=function(){ this.open=false; this.dispatchEvent(new w.Event('close')); };
  const spoken = [];
  let speechStops = 0;
  const revealed = [];
  for (const id of ['case','stem']) $(id).scrollIntoView = options => revealed.push([id, options.block]);
  const update = {status:'current',message:'最新版を使用中です。'};
  w.asv = {home:async()=>dataset,speech:{speak:async x=>spoken.push(x),stop:async()=>{ speechStops++; },voices:async()=>[],
    dictionary:async()=>({count:0}),importDictionary:async()=>({count:3})},
    updates:{subscribe(){},status:async()=>({status:'idle'}),check:async()=>update}};
  const names = ['window','document','localStorage','Option','CustomEvent'];
  for(const name of names){
    const previous = Object.getOwnPropertyDescriptor(globalThis,name);
    Object.defineProperty(globalThis,name,{configurable:true,value:w[name]});
    t.after(()=>{ if (previous) Object.defineProperty(globalThis,name,previous); else delete globalThis[name]; });
  }
  t.after(()=>dom.window.close());
  await import('../renderer/study.js');
  const key = (target,key,options={}) => target.dispatchEvent(new w.KeyboardEvent('keydown',{key,bubbles:true,cancelable:true,...options}));
  const change = (id,value) => { $(id).value=value; $(id).dispatchEvent(new w.Event('change')); };
  assert.equal($('serial').textContent,'A01-001');
  assert.equal($('results').children.length,2);
  const composition = new w.InputEvent('beforeinput', {bubbles:true, cancelable:true, inputType:'insertText'});
  assert.equal($('stem').dispatchEvent(composition), false);
  $('stem').value = '変換中の入力';
  $('stem').dispatchEvent(new w.InputEvent('input', {bubbles:true, inputType:'insertCompositionText'}));
  assert.equal($('stem').value, a.stem);
  $('read').click();
  key(w,'3',{ctrlKey:true});
  assert.equal(speechStops,1);
  assert.equal(d.activeElement.id,'feedback'); assert.equal($('feedback').textContent,'正解');
  assert.equal($('key-answer').value, '正答　2・3');
  assert.equal($('answer-note').value, '通常の注記');
  assert.equal($('answer-note').hidden, false);
  assert.equal(JSON.parse(w.localStorage.getItem('asv.windows.answers.v1'))[a.serial],3);
  $('read').click(); assert.match(spoken.at(-1).text,/正答 2・3/);
  $('retry').click(); assert.equal(d.activeElement.dataset.choice,'1'); assert.ok($('solution').hidden);
  key(w,'j',{ctrlKey:true}); key(d.activeElement,'ArrowDown');
  assert.equal($('serial').textContent,'B33-141'); assert.equal(d.activeElement.getAttribute('aria-selected'),'true');
  assert.deepEqual(revealed,[]);
  key(d.activeElement,'Enter'); assert.equal(d.activeElement.id,'stem');
  $('prev').focus(); $('prev').click();
  assert.equal($('serial').textContent,a.serial); assert.equal(d.activeElement.id,'case');
  key(d.activeElement,']',{ctrlKey:true});
  assert.equal($('serial').textContent,b.serial); assert.equal(d.activeElement.id,'stem');
  assert.deepEqual(revealed,[['stem','start'],['case','start'],['stem','start']]);
  change('subject','医療概論'); assert.equal($('serial').textContent,'A01-001');
  $('search').value = '#呼吸器'; $('session').dispatchEvent(new w.Event('change'));
  assert.ok($('problem').hidden); assert.equal($('result-count').textContent,'0 件の問題');
  $('reset-filters').click(); assert.equal($('results').children.length,2);
  let context; w.addEventListener('asv:chat',e=>{context=e.detail.context;});
  $('read').click();
  const stopsBeforeChat = speechStops;
  key(w,'a',{ctrlKey:true,shiftKey:true}); assert.equal($('chat-panel').hidden,false); assert.match(context,/正答 2・3/);
  assert.equal(speechStops,stopsBeforeChat + 1);
  assert.ok($('lookup').hidden, 'compact window makes space for the chat');
  w.dispatchEvent(new w.CustomEvent('asv:close-chat')); assert.equal(d.activeElement.id,'ask');
  assert.equal($('lookup').hidden,false);
  key(w,'+',{ctrlKey:true,shiftKey:true}); assert.equal($('size').value,'22');
  $('settings-open').click(); change('theme','dark'); change('size','40');
  assert.equal(d.documentElement.dataset.theme,'dark'); assert.ok($('larger').disabled);
  $('shortcut-list').querySelector('[data-action=search]').click(); key(w,'k',{ctrlKey:true,altKey:true});
  $('settings-close').click(); key(w,'k',{ctrlKey:true,altKey:true}); assert.equal(d.activeElement.id,'search');
  assert.equal(JSON.parse(w.localStorage.getItem('asv.windows.preferences.v1')).shortcuts.search,'Ctrl+Alt+K');
});
