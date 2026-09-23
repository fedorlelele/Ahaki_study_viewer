const test = require('node:test');
const assert = require('node:assert/strict');
const { JSDOM } = require('jsdom');
const createDOMPurify = require('dompurify');
const marked = require('marked');
const katex = require('katex');
const Markdown = require('../web_app/shared/markdown.js');

function setup() {
  const { window } = new JSDOM('<main><nav></nav><article></article></main>', { url: 'https://asv.example/web_app/' });
  const dependencies = { marked, katex, DOMPurify: createDOMPurify(window) };
  const body = window.document.querySelector('article');
  const outline = window.document.querySelector('nav');
  return { window, body, outline, dependencies };
}

test('Japanese punctuation emphasis, tables and bibliography links render as semantic HTML', () => {
  const ui = setup();
  Markdown.render(ui.body, '## 核心\nこれは**「筋の長さ」**です。\n\n| 線維 | 役割 |\n| --- | --- |\n| Ⅰa | **長さ** |\n\n## 参考資料\n[教科書の該当節](https://www.ncbi.nlm.nih.gov/books/NBK10809/)', { ...ui, headingPrefix: 'A34-029' });
  assert.equal(ui.body.querySelector('strong').textContent, '「筋の長さ」');
  assert.equal(ui.body.querySelector('table td strong').textContent, '長さ');
  assert.equal(ui.body.querySelector('th').getAttribute('scope'), 'col');
  assert.equal(ui.body.querySelector('.markdown-table-scroll').tabIndex, 0);
  const link = ui.body.querySelector('a');
  assert.equal(link.target, '_blank');
  assert.equal(link.rel, 'noopener noreferrer');
  assert.equal(ui.outline.querySelectorAll('a').length, 2);
  assert.equal(ui.outline.querySelectorAll('a')[1].hash, '#A34-029-section-2');
});

test('code, escaped markers, nesting and standard Markdown remain intact', () => {
  const ui = setup();
  Markdown.render(ui.body, '`**「コード」**`\n\n\\*\\*「文字」\\*\\*\n\n***italic and strong***\n\n**「*強調*」**\n\n> 引用', ui);
  assert.equal(ui.body.querySelector('code').textContent, '**「コード」**');
  assert.match(ui.body.textContent, /\*\*「文字」\*\*/);
  assert.equal(ui.body.querySelector('strong em').textContent, '強調');
  assert.equal(ui.body.querySelector('blockquote').textContent.trim(), '引用');
});

test('raw HTML and dangerous URLs never become executable content', () => {
  const ui = setup();
  Markdown.render(ui.body, '<script>alert(1)</script>\n\n<img src=x onerror=alert(1)>\n\n[unsafe](javascript:alert%281%29)\n\n[encoded](jav&#x61;script:alert%281%29)\n\n[local](file:///etc/passwd)\n\n![tracking](https://example.org/track)', ui);
  assert.equal(ui.body.querySelectorAll('script,img,iframe,[onerror]').length, 0);
  assert.equal(ui.body.querySelectorAll('a[href]').length, 0);
  assert.match(ui.body.textContent, /<script>/);
});

test('missing parser or sanitizer fails closed with readable original text', () => {
  const source = '<img src=x onerror=alert(1)>\n**「本文」**';
  for (const dependencies of [{}, { marked }, { DOMPurify: setup().dependencies.DOMPurify }]) {
    const ui = setup();
    Markdown.render(ui.body, source, { dependencies });
    assert.equal(ui.body.querySelectorAll('img,strong').length, 0);
    assert.match(ui.body.textContent, /\*\*「本文」\*\*/);
  }
});

test('outline is rebuilt without duplicate entries and moves keyboard focus to the section', () => {
  const ui = setup();
  Markdown.render(ui.body, '## はじめ\n本文\n\n## 参考資料\n出典', { ...ui, headingPrefix: 'B34-001' });
  const heading = ui.body.querySelectorAll('h2')[1];
  let scrolled = false;
  heading.scrollIntoView = () => { scrolled = true; };
  ui.outline.querySelectorAll('a')[1].click();
  assert.equal(ui.window.document.activeElement, heading);
  assert.equal(scrolled, true);
  Markdown.render(ui.body, '本文のみ', ui);
  assert.equal(ui.outline.hidden, true);
  assert.equal(ui.outline.children.length, 0);
});

test('legacy Japanese and English emphasis boundaries preserve every word', () => {
  const ui = setup();
  Markdown.render(ui.body, '比率は**「2：1」**です。**外転神経(VI)**の支配。**30℃**は境界。**S**ensory = **A**fferent。体は「**核**」と「**細胞質**」。', ui);
  assert.equal(ui.body.textContent.trim(), '比率は「2：1」です。外転神経(VI)の支配。30℃は境界。Sensory = Afferent。体は「核」と「細胞質」。');
  assert.deepEqual([...ui.body.querySelectorAll('strong')].map(x => x.textContent), ['「2：1」', '外転神経(VI)', '30℃', 'S', 'A', '核', '細胞質']);
  Markdown.render(ui.body, '** cobblestone appearance（敷石像）**。**コード `a**b` を含む**。**太字と *斜体***。', ui);
  assert.equal(ui.body.querySelector('strong').textContent, ' cobblestone appearance（敷石像）');
  assert.equal(ui.body.querySelector('strong code').textContent, 'a**b');
  assert.equal(ui.body.querySelector('strong em').textContent, '斜体');
});

test('inline chemistry and display equations render as accessible MathML within Markdown', () => {
  const ui = setup();
  Markdown.render(ui.body, '酸素は**$O_2$**、イオンは$Ca^{2+}$。\n\n$$\n\\frac{a}{b} \\rightleftharpoons c\n$$\n\n| 項目 | 式 |\n| --- | --- |\n| 気体 | $CO_2$ |\n\n\\(x^2\\)と\\[y_1\\]', ui);
  assert.equal(ui.body.querySelectorAll('math').length, 6);
  assert.equal(ui.body.querySelector('strong math msub').textContent, 'O2');
  assert.equal(ui.body.querySelector('math msup').textContent, 'a2+');
  assert.equal(ui.body.querySelectorAll('.markdown-math-display').length, 2);
  assert.ok(ui.body.querySelector('mfrac'));
  assert.ok(ui.body.querySelector('td math'));
  assert.equal(ui.body.querySelector('.markdown-math-fallback'), null);
});

test('code, escaped dollar signs and currency are not interpreted as equations', () => {
  const ui = setup();
  Markdown.render(ui.body, '`$O_2$ **強調**`\n\n```text\n$CO_2$\n```\n\n\\$O_2\\$\n\n$10 and $20\n\n途中の$も残す', ui);
  assert.equal(ui.body.querySelectorAll('math').length, 0);
  assert.equal(ui.body.querySelector('code').textContent, '$O_2$ **強調**');
  assert.match(ui.body.textContent, /\$10 and \$20/);
});

test('math errors or unavailable library keep original notation without blocking the article', () => {
  const ui = setup();
  Markdown.render(ui.body, '$\\notACommand{a}$の次に$O_2$。', ui);
  assert.equal(ui.body.querySelector('.markdown-math-fallback').textContent, '$\\notACommand{a}$');
  assert.equal(ui.body.querySelectorAll('math').length, 1);
  Markdown.render(ui.body, '**本文**と$O_2$', { dependencies: { marked, DOMPurify: ui.dependencies.DOMPurify } });
  assert.equal(ui.body.querySelector('strong').textContent, '本文');
  assert.match(ui.body.textContent, /\$O_2\$/);
});

test('legacy JSON control characters are repaired only inside math notation', () => {
  const ui = setup();
  Markdown.render(ui.body, '$\beta$受容体と$P = Q \times R$。本文の\tタブはそのまま。', ui);
  assert.equal(ui.body.querySelectorAll('math').length, 2);
  assert.equal(ui.body.querySelector('math mi').textContent, 'β');
  assert.match(ui.body.querySelectorAll('math')[1].textContent, /×/);
  assert.match(ui.body.textContent, /本文の\tタブはそのまま/);
});

test('math cannot introduce links, images, attributes or shared macros', () => {
  const ui = setup();
  Markdown.render(ui.body, '$\\href{javascript:alert(1)}{x}$ $\\includegraphics{https://example.org/x}$ $\\htmlStyle{color:red}{x}$\n\n$\\gdef\\custom{secret}\\custom$ $\\custom$', ui);
  assert.equal(ui.body.querySelectorAll('a,img,script,[style],[onerror]').length, 0);
  assert.ok(ui.body.querySelector('.markdown-math-fallback'));
  assert.match(ui.body.textContent, /\$\\custom\$/);
});
