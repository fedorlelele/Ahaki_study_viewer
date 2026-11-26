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
let current;
try {
  current = await launch();
  const {page,errors} = current;
  assert.equal(await page.locator('#serial').innerText(),'A01-001');
  const count=await page.locator('#result-count').innerText();
  assert.ok(parseInt(count.replaceAll(',',''),10)>10000,count);
  await page.keyboard.press('Control+f');
  assert.equal(await page.evaluate(() => document.activeElement.id),'search');
  await page.locator('#search').fill('A01-001');
  await page.waitForFunction(() => document.querySelectorAll('#results [role=option]').length===1);
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
