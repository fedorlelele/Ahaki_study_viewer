const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');

const web = path.join(__dirname, '../web_app');
const script = fs.readFileSync(path.join(web, 'shared/desktop-downloads.js'), 'utf8');
const releases = 'https://github.com/fedorlelele/Ahaki_study_viewer/releases/download/';
const assets = [
  'mac-v0.4.0/ASV-0.4.0.zip',
  'windows-v0.2.0/ASV-Windows-0.2.0-x64-Setup.exe',
  'windows-v0.2.0/ASV-Windows-0.2.0-arm64-Setup.exe'
];

for (const page of ['index','simple','tag_study']) {
  test(page + ' offers the public releases without an authentication session', t => {
    const html = fs.readFileSync(path.join(web, page + '.html'), 'utf8');
    const dom = new JSDOM(html, {runScripts:'outside-only', url:'https://asv.example/web_app/'});
    t.after(() => dom.window.close());
    const w = dom.window;
    const d = w.document;
    const shared = d.querySelector('script[src="./shared/desktop-downloads.js?v=2"]');
    assert.ok(shared);
    const startup = [...d.scripts].find(el => el.textContent.includes('window.mountDesktopDownloads('));
    assert.ok(startup);
    w.eval(script);

    // Run the real page startup, stopping when it reaches the authentication client.
    const noAuth = new Error('Auth is unavailable');
    w.SUPABASE_URL = 'https://asv.example';
    w.SUPABASE_KEY = 'test';
    w.supabase = {createClient() { throw noAuth; }};
    assert.throws(() => w.eval(startup.textContent), error => error === noAuth);

    const menu = d.getElementById('desktopDownloads');
    assert.equal(menu.hidden, false);
    assert.equal(menu.closest('#adminMenu'), null);
    assert.equal(d.getElementById('analyticsMenuLink').hidden, true);
    if (page === 'index') assert.equal(d.getElementById('adminMenu').hidden, true);
    // The full and tag views expose the chooser through the ordinary menu button.
    if (page !== 'simple') {
      const panels = d.getElementById(d.getElementById('menuToggleBtn').getAttribute('aria-controls'));
      assert.ok(panels.contains(menu));
      panels.hidden = false;
      if (page === 'index') d.getElementById('topMenuContent').hidden = false;
    }
    assert.equal(menu.closest('[hidden]'), null);
    const summary = menu.querySelector('summary');
    assert.match(summary.textContent, /デスクトップ版をダウンロード/);
    summary.focus();
    assert.equal(d.activeElement, summary);
    summary.click();
    assert.ok(menu.open);

    const links = [...menu.querySelectorAll('a[href]')];
    assert.deepEqual(links.map(a => a.href), assets.map(asset => releases + asset));
    assert.match(links[0].textContent, /Mac.*Intel.*Apple Silicon/);
    assert.match(links[1].textContent, /Windows.*x64/);
    assert.match(links[2].textContent, /Windows.*ARM64/);
    for (const link of links) {
      assert.equal(link.tabIndex, 0);
      link.focus();
      assert.equal(d.activeElement, link);
    }
  });
}
