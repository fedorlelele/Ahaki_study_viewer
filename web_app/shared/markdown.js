/* Shared Markdown presentation for explanations and study material. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.AhakiMarkdown = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function (root) {
  "use strict";
  const parsers = new WeakMap();
  const escapeHtml = value => String(value || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const escapeAttribute = value => escapeHtml(value).replace(/"/g, "&quot;");

  function mathToken(source, block = false) {
    const delimiters = block ? [["$$", "$$"], ["\\[", "\\]"]] : [["$$", "$$"], ["\\[", "\\]"], ["\\(", "\\)"], ["$", "$"]];
    for (const [open, close] of delimiters) {
      if (!source.startsWith(open)) continue;
      const display = open === "$$" || open === "\\[";
      for (let end = open.length; end < source.length; end += 1) {
        if (!display && source[end] === "\n") return;
        if (source[end] === "\\" && !source.startsWith(close, end)) { end += 1; continue; }
        if (!source.startsWith(close, end)) continue;
        const text = source.slice(open.length, end);
        if (!text.trim() || (open === "$" && (/^\s|\s$/.test(text) || /\d/.test(source[end + 1] || "")))) return;
        const raw = source.slice(0, end + close.length);
        if (block && !/^(?:[ \t]*\n|[ \t]*$)/.test(source.slice(raw.length))) return;
        return { type: block ? "displayMath" : "inlineMath", raw, text, display };
      }
    }
  }

  function mathPlaceholder(token) {
    return `<span data-math-source="${escapeAttribute(token.text)}" data-math-display="${token.display}">${escapeHtml(token.raw)}</span>`;
  }

  function getParser(marked) {
    if (parsers.has(marked)) return parsers.get(marked);
    const parser = new marked.Marked({ gfm: true, breaks: true, renderer: {
      html(token) { return escapeHtml(token.text); }
    }, extensions: [{
      // Japanese text often touches punctuation or an English abbreviation.
      // Accept paired ** at those boundaries; leave code, escapes and *** to Marked.
      name: "adjacentStrong", level: "inline",
      start(source) { const index = source.indexOf("**"); return index < 0 ? undefined : index; },
      tokenizer(source) {
        if (!source.startsWith("**") || source.startsWith("***")) return;
        for (let end = 2; end < source.length && source[end] !== "\n"; end += 1) {
          if (source[end] === "\\") { end += 1; continue; }
          if (source[end] === "`") {
            const fence = /^`+/.exec(source.slice(end))[0];
            const close = source.indexOf(fence, end + fence.length);
            if (close !== -1) { end = close + fence.length - 1; continue; }
          }
          if (!source.startsWith("**", end)) continue;
          const text = source.slice(2, end);
          if (!text.trim() || source[end + 2] === "*") return;
          return { type: "adjacentStrong", raw: source.slice(0, end + 2), tokens: this.lexer.inlineTokens(text) };
        }
      },
      renderer(token) { return `<strong>${this.parser.parseInline(token.tokens)}</strong>`; }
    }, {
      name: "inlineMath", level: "inline",
      start(source) { const index = source.search(/\$|\\[([]/); return index < 0 ? undefined : index; },
      tokenizer(source) { return mathToken(source); },
      renderer: mathPlaceholder
    }, {
      name: "displayMath", level: "block",
      start(source) { const index = source.search(/(?:^|\n)(?:\$\$|\\\[)/); return index < 0 ? undefined : index; },
      tokenizer(source) { return mathToken(source, true); },
      renderer(token) { return `${mathPlaceholder(token)}\n`; }
    }] });
    parsers.set(marked, parser);
    return parser;
  }

  function toHtml(text, dependencies = root) {
    const { marked, DOMPurify } = dependencies;
    // A CDN failure must leave readable text, never unsanitized generated HTML.
    if (!marked?.Marked || !DOMPurify?.sanitize) return escapeHtml(text).replace(/\n/g, "<br>");
    return DOMPurify.sanitize(getParser(marked).parse(String(text || "")), {
      ALLOWED_TAGS: ["p", "br", "span", "strong", "em", "del", "blockquote", "ul", "ol", "li", "h1", "h2", "h3", "h4", "h5", "h6", "pre", "code", "hr", "a", "table", "thead", "tbody", "tr", "th", "td"],
      ALLOWED_ATTR: ["href", "title", "data-math-source", "data-math-display"],
      ALLOW_DATA_ATTR: false
    });
  }

  function render(target, text, options = {}) {
    if (!target) return;
    const doc = target.ownerDocument;
    target.classList.add("markdown-body");
    target.innerHTML = toHtml(text, options.dependencies || root);
    const { katex, DOMPurify } = options.dependencies || root;
    if (katex?.renderToString && DOMPurify?.sanitize) {
      target.querySelectorAll("[data-math-source]").forEach(element => {
        const display = element.getAttribute("data-math-display") === "true";
        try {
          // Some saved JSON decoded TeX's \b / \t as control characters.
          // Repair only these two observed commands in the presentation copy.
          const source = element.getAttribute("data-math-source")
            .replace(/\u0008eta\b/g, "\\beta").replace(/\times\b/g, "\\times");
          const html = katex.renderToString(source, {
            displayMode: display, output: "mathml", throwOnError: true,
            trust: false, strict: "ignore", maxExpand: 1000, maxSize: 20,
            macros: {}
          });
          element.innerHTML = DOMPurify.sanitize(html, {
            USE_PROFILES: { html: true, mathMl: true },
            FORBID_TAGS: ["a", "img", "style"], FORBID_ATTR: ["style", "href"], ALLOW_DATA_ATTR: false
          });
          element.className = display ? "markdown-math markdown-math-display" : "markdown-math";
        } catch (_) {
          // Keep readable original notation if a formula or the library is unavailable.
          element.className = "markdown-math-fallback";
        }
      });
    }
    target.querySelectorAll("a[href]").forEach(link => {
      try {
        const url = new URL(link.getAttribute("href"), doc.baseURI);
        if (!["https:", "http:"].includes(url.protocol)) {
          link.removeAttribute("href");
        } else if (url.origin !== new URL(doc.baseURI).origin) {
          link.target = "_blank";
          link.rel = "noopener noreferrer";
        }
      } catch (_) { link.removeAttribute("href"); }
    });
    target.querySelectorAll("table").forEach((table, index) => {
      const wrap = doc.createElement("div");
      wrap.className = "markdown-table-scroll";
      wrap.tabIndex = 0;
      wrap.setAttribute("role", "region");
      const columns = Array.from(table.querySelectorAll("thead th"), cell => cell.textContent.trim()).join("・");
      wrap.setAttribute("aria-label", `${columns || `表${index + 1}`}の表（横にスクロールできます）`);
      table.querySelectorAll("thead th").forEach(cell => cell.setAttribute("scope", "col"));
      table.parentNode.insertBefore(wrap, table);
      wrap.appendChild(table);
    });
    const outline = options.outline;
    if (!outline) return;
    outline.replaceChildren();
    const headings = Array.from(target.querySelectorAll("h2"));
    outline.hidden = headings.length < 2;
    if (outline.hidden) return;
    const label = doc.createElement("p");
    label.className = "deep-dive-outline-label";
    label.textContent = "この解説でわかること";
    outline.appendChild(label);
    const list = doc.createElement("ul");
    headings.forEach((heading, index) => {
      heading.id = `${options.headingPrefix || "deep-dive"}-section-${index + 1}`;
      heading.tabIndex = -1;
      const item = doc.createElement("li");
      const link = doc.createElement("a");
      link.href = `#${heading.id}`;
      link.textContent = heading.textContent;
      link.addEventListener("click", event => {
        event.preventDefault();
        heading.focus({ preventScroll: true });
        heading.scrollIntoView({ block: "start", behavior: "instant" });
      });
      item.appendChild(link);
      list.appendChild(item);
    });
    outline.appendChild(list);
  }

  return { toHtml, render };
});
