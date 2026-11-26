const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');

const web = path.join(__dirname, '../web_app');
const script = fs.readFileSync(path.join(web, 'shared/desktop-downloads.js'), 'utf8');
const releases = 'https://github.com/fedorlelele/Ahaki_study_viewer/releases/download/';
const assets = [
  'mac-v0.3.0/ASV-0.3.0.zip',
  'windows-v0.1.1/ASV-Windows-0.1.1-x64-Setup.exe',
  'windows-v0.1.1/ASV-Windows-0.1.1-arm64-Setup.exe'
];
const session = (id, role) => ({user:{id, app_metadata:{role}}, access_token:'test'});

for (const page of ['index','simple','tag_study']) {
  test(page + ' menu follows login, token refresh, role downgrade and sign-out', () => {
    const html = fs.readFileSync(path.join(web, page+'.html'), 'utf8');
    const dom = new JSDOM(html, {runScripts:'outside-only'});
    const w = dom.window;
    const menu = w.document.getElementById('desktopDownloads');
    const links = () => [...menu.querySelectorAll('a[href]')];
    assert.ok(menu.hidden);
    assert.equal(links().length,0);
    assert.match(menu.querySelector('summary').textContent, /デスクトップ版をダウンロード/);
    assert.ok(w.document.querySelector('script[src="./shared/desktop-downloads.js"]'));
    assert.match(html, /window\.mountDesktopDownloads\(supabaseClient, getUserRole\)/);
    if (page === 'index') assert.ok(menu.closest('#adminMenu'));

    let authChanged;
    w.eval(script);
    w.mountDesktopDownloads(null, () => 'admin');
    w.mountDesktopDownloads({auth:{onAuthStateChange(fn){ authChanged=fn; }}}, user => user?.app_metadata?.role);
    for (const value of [null, session('u',''), session('s','student'), session('t','teacher'),
      {access_token:'test', user:{id:'s',user_metadata:{role:'admin'}}}]) {
      authChanged('INITIAL_SESSION',value);
      assert.ok(menu.hidden);
      assert.equal(links().length,0);
    }
    authChanged('SIGNED_IN',session('a','admin'));
    assert.equal(menu.hidden,false);
    const initial = links();
    assert.deepEqual(initial.map(a=>a.href),assets.map(asset=>releases+asset));
    assert.ok(initial.every(a => a.tabIndex === 0));
    assert.match(initial[0].textContent,/Mac.*Intel.*Apple Silicon/);
    assert.match(initial[1].textContent,/Windows.*x64/);
    assert.match(initial[2].textContent,/Windows.*ARM64/);
    menu.open=true;
    authChanged('TOKEN_REFRESHED',session('a','admin'));
    assert.ok(menu.open);
    assert.equal(links()[0],initial[0], 'token refresh preserves link and keyboard focus');

    authChanged('TOKEN_REFRESHED',session('a','teacher'));
    assert.ok(menu.hidden);
    assert.equal(menu.open,false);
    assert.equal(links().length,0);
    authChanged('SIGNED_IN',session('b','admin'));
    assert.equal(links().length,3);
    authChanged('SIGNED_OUT',null);
    assert.ok(menu.hidden);
    assert.equal(links().length,0);
    dom.window.close();
  });
}
