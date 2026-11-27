import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {_electron} from 'playwright-core';
import electron from 'electron';

const executable = process.argv[2];
const profile = await mkdtemp(join(tmpdir(),'asv-window-qa-'));
async function launch() {
  const session = await _electron.launch({
    env: {...process.env, ASV_TEST_OFFLINE: '1'},
    executablePath:executable ? resolve(executable) : electron,
    args:[...(executable ? [] : ['.']), '--user-data-dir=' + profile],
    timeout:60000
  });
  try {
    const page = await session.firstWindow();
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.locator('#problem:not([hidden]) #stem').waitFor({timeout:60000});
    assert.equal(await session.evaluate(({app}) => app.isPackaged), Boolean(executable));
    return {session,page,errors};
  } catch (e) {
    for (const page of session.windows()) console.error('Startup', page.url(), await page.locator('body').innerText().catch(()=>''));
    await session.close(); throw e;
  }
}
async function checkReadingNavigation(session, page) {
  const clinical = await page.evaluate(async () => {
    const {questions} = await window.asv.home();
    const question = questions.find(q => q.case_text?.includes('\n') && q.explanation_latest);
    if (!question) throw new Error('A multiline clinical case with an explanation is required');
    return question;
  });
  // Observe the app's own speech requests independently of the OS screen reader.
  await session.evaluate(({BrowserWindow}) => {
    globalThis.asvSmokeSpeech = [];
    BrowserWindow.getAllWindows()[0].webContents.ipc.handle('speech:speak', (_event, request) => {
      globalThis.asvSmokeSpeech.push(request.text);
    });
  });

  await page.keyboard.press('Control+f');
  await page.locator('#search').fill(clinical.serial);
  await page.waitForFunction(serial => document.querySelector('#serial').textContent === serial, clinical.serial);
  await page.keyboard.press('Control+j');
  await page.keyboard.press('Enter');
  const caseField = page.getByRole('textbox', {name:'症例文',exact:true});
  const stem = page.getByRole('textbox', {name:'問題文',exact:true});
  const explanation = page.getByRole('textbox', {name:'解説',exact:true});
  const focusedReadOnly = el => el === document.activeElement &&
    (el.readOnly || el.getAttribute('aria-readonly') === 'true');
  assert.ok(await caseField.evaluate(focusedReadOnly));
  assert.equal(await caseField.inputValue(), clinical.case_text);
  const cdp = await page.context().newCDPSession(page);
  const {nodes} = await cdp.send('Accessibility.getFullAXTree');
  await cdp.detach();
  const accessibleCase = nodes.find(n => n.role?.value === 'textbox' && n.name?.value === '症例文');
  assert.equal(accessibleCase?.value?.value, clinical.case_text);
  assert.ok(accessibleCase.properties.some(p => p.name === 'readonly' && p.value.value));
  assert.ok(accessibleCase.properties.some(p => p.name === 'multiline' && p.value.value));
  assert.ok(await caseField.evaluate(el => el.scrollHeight <= el.clientHeight + 1),
    'the whole multiline case remains visible without an inner scrollbar');
  assert.equal(await caseField.evaluate(el => el.selectionStart), 0);
  await page.keyboard.press('ArrowRight');
  assert.equal(await caseField.evaluate(el => el.selectionStart), 1);
  await page.keyboard.press('ArrowDown');
  assert.ok(await caseField.evaluate(el => el.selectionStart > 1));
  await page.keyboard.press('Shift+ArrowRight');
  assert.ok(await caseField.evaluate(el => el.selectionEnd > el.selectionStart));
  await page.keyboard.type('x');
  assert.equal(await caseField.inputValue(), clinical.case_text);
  await page.keyboard.press('Tab');
  assert.ok(await stem.evaluate(focusedReadOnly));
  assert.equal(await stem.inputValue(), clinical.stem);
  await page.keyboard.press('Shift+Tab');
  assert.ok(await caseField.evaluate(el => el === document.activeElement));
  await page.locator('a.skip').focus();
  await page.keyboard.press('Enter');
  assert.ok(await caseField.evaluate(el => el === document.activeElement));
  assert.ok(await explanation.isHidden());
  await page.keyboard.press('Tab');
  await page.keyboard.press('Tab');
  assert.equal(await page.evaluate(() => document.activeElement.dataset.choice), '1');
  await page.keyboard.press('Enter');
  const feedback = page.locator('#feedback');
  assert.ok(await feedback.evaluate(el => el === document.activeElement));
  const selected = page.getByRole('button', {name:/選択肢1.*あなたの回答/});
  assert.equal(await selected.getAttribute('aria-disabled'), 'true');
  await page.keyboard.press('Shift+Tab');
  const gradedChoices = page.locator('#choices button');
  assert.ok(await gradedChoices.last().evaluate(el => el === document.activeElement));
  await page.keyboard.press('Enter');
  assert.equal(await selected.count(), 1);
  await page.keyboard.press('Tab');
  assert.ok(await feedback.evaluate(el => el === document.activeElement));
  await page.keyboard.press('Tab');
  assert.equal(await page.evaluate(() => document.activeElement.id), 'key-answer');
  await page.keyboard.press('Tab');
  if (await page.locator('#answer-note').isVisible()) {
    assert.equal(await page.evaluate(() => document.activeElement.id), 'answer-note');
    await page.keyboard.press('Tab');
  }
  assert.ok(await explanation.evaluate(focusedReadOnly));
  assert.equal(await explanation.inputValue(), clinical.explanation_latest);
  assert.deepEqual(await session.evaluate(() => globalThis.asvSmokeSpeech), []);
  await page.locator('#read').click();
  assert.ok(await session.evaluate(() => globalThis.asvSmokeSpeech.length === 1));
  await session.evaluate(({BrowserWindow}) => {
    BrowserWindow.getAllWindows()[0].webContents.ipc.removeHandler('speech:speak');
    delete globalThis.asvSmokeSpeech;
  });
}
let current;
try {
  current = await launch();
  const {page,errors} = current;
  assert.equal(await page.locator('#serial').innerText(),'A01-001');
  const count=await page.locator('#result-count').innerText();
  assert.ok(parseInt(count.replaceAll(',',''),10)>10000,count);
  const originalSize = await current.session.evaluate(({BrowserWindow}) => {
    const window = BrowserWindow.getAllWindows()[0];
    const size = window.getContentSize();
    window.setContentSize(700, 540);
    return size;
  });
  await page.waitForFunction(() => window.innerWidth <= 780);
  await page.locator('#next').focus();
  assert.ok(await page.locator('#workspace').evaluate(el => el.scrollTop > 0));
  await page.keyboard.press('Enter');
  await page.waitForFunction(() => document.activeElement.id === 'stem');
  assert.notEqual(await page.locator('#serial').innerText(),'A01-001');
  assert.ok(await page.locator('#stem').evaluate(heading => {
    const bounds = heading.getBoundingClientRect();
    const viewport = document.querySelector('#workspace').getBoundingClientRect();
    return bounds.top >= viewport.top - 1 && bounds.bottom <= viewport.bottom + 1;
  }), 'the new question heading is visible in the narrow scrolling workspace');
  await page.keyboard.press('Tab');
  assert.equal(await page.evaluate(() => document.activeElement.dataset.choice),'1');
  await current.session.evaluate(({BrowserWindow}, size) => BrowserWindow.getAllWindows()[0].setContentSize(...size),originalSize);
  await checkReadingNavigation(current.session, page);
  await page.keyboard.press('Control+f');
  assert.equal(await page.evaluate(() => document.activeElement.id),'search');
  await page.locator('#search').fill('A01-001');
  await page.waitForFunction(() => document.querySelector('#serial').textContent === 'A01-001' &&
    document.querySelectorAll('#results [role=option]').length === 1);
  assert.ok(await page.locator('#case').isHidden());
  await page.keyboard.press('Control+j');
  await page.keyboard.press('Enter');
  assert.equal(await page.evaluate(() => document.activeElement.id), 'stem');
  await page.locator('#choices button[data-choice="3"]').click();
  await page.locator('#feedback').waitFor();
  assert.match(await page.locator('#feedback').innerText(),/正解/);
  await page.keyboard.press('Control+Shift+a');
  await page.locator('#chat-panel').waitFor();
  assert.equal(await page.evaluate(() => document.activeElement.id),'chat-prompt');
  assert.equal(await page.evaluate(async () => (await window.asv.chat.invoke('state')).signedIn),false);
  await page.locator('#chat-close').click();
  await page.locator('#settings-open').click();
  await page.locator('#theme').selectOption('dark');
  await page.locator('#settings-close').click();
  if (process.platform === 'win32') {
    const voices = await page.evaluate(() => window.asv.speech.voices());
    assert.ok(Array.isArray(voices),'Windows speech engine returns a list');
    // The hosted runner may have no default audio device.
    console.log('Installed Windows voices:',voices.map(v=>v.language).join(', '));
    const encrypted = await current.session.evaluate(({safeStorage}) => {
      const sample=safeStorage.encryptString('ASV fixture');
      return safeStorage.decryptString(sample);
    });
    assert.equal(encrypted,'ASV fixture');
  }
  assert.deepEqual(errors,[]);
  await current.session.close(); current=null;
  current=await launch();
  await current.page.locator('#feedback').waitFor();
  assert.match(await current.page.locator('#feedback').innerText(),/正解/);
  await current.page.locator('#settings-open').click();
  assert.equal(await current.page.locator('#theme').inputValue(),'dark');
  console.log('ASV search, grading, chat, settings and persisted answers passed.');
} finally {
  if (current) await current.session.close();
  await rm(profile,{recursive:true,force:true,maxRetries:5,retryDelay:400});
}
