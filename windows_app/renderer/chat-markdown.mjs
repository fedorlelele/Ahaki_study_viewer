import { marked } from '../node_modules/marked/lib/marked.esm.js'

const tags = {
  paragraph: 'p', strong: 'strong', em: 'em', del: 'del', blockquote: 'blockquote',
  list_item: 'li', br: 'br', hr: 'hr'
}

function decodeText(text) {
  // Decode character references as text. Never pass model output to an HTML parser.
  const named = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: '\u00a0' }
  return text.replace(/&(amp|lt|gt|quot|apos|nbsp|#(?:\d+|x[\da-f]+));/gi, (source, name) => {
    if (!name.startsWith('#')) return named[name.toLowerCase()]
    const hex = name[1].toLowerCase() === 'x'
    const scalar = parseInt(name.slice(hex ? 2 : 1), hex ? 16 : 10)
    if (!scalar || scalar > 0x10ffff || (scalar >= 0xd800 && scalar <= 0xdfff)) return '\ufffd'
    return String.fromCodePoint(scalar)
  })
}

function linkURL(href) {
  try {
    const url = new URL(decodeText(href))
    if (['https:', 'http:'].includes(url.protocol) && !url.username && !url.password) return url.href
  } catch { /* Relative links have no web base in the desktop app. */ }
  return null
}

export function renderMarkdown(document, source) {
  const fragment = document.createDocumentFragment()
  const append = (container, items) => {
    for (const item of items) {
      let element
      switch (item.type) {
        case 'space':
        case 'def':
          continue
        case 'html':
          if (/^<br\s*\/?\s*>$/i.test(item.raw)) container.append(document.createElement('br'))
          else container.append(document.createTextNode(item.raw))
          continue
        case 'table': {
          const wrapper = document.createElement('div')
          wrapper.className = 'chat-table-scroll'
          wrapper.tabIndex = 0
          wrapper.setAttribute('role', 'region')
          wrapper.setAttribute('aria-label', '回答の表')
          const table = document.createElement('table')
          for (const [section, rows] of [['thead', [item.header]], ['tbody', item.rows]]) {
            const group = document.createElement(section)
            for (const cells of rows) {
              const row = document.createElement('tr')
              for (const cell of cells) {
                const box = document.createElement(section === 'thead' ? 'th' : 'td')
                if (section === 'thead') box.scope = 'col'
                append(box, cell.tokens)
                row.append(box)
              }
              group.append(row)
            }
            table.append(group)
          }
          wrapper.append(table)
          container.append(wrapper)
          continue
        }
        case 'list':
          element = document.createElement(item.ordered ? 'ol' : 'ul')
          if (item.ordered && Number.isInteger(item.start)) element.start = item.start
          append(element, item.items)
          break
        case 'code':
        case 'codespan': {
          const code = document.createElement('code')
          code.textContent = item.text
          if (item.type === 'codespan') element = code
          else {
            element = document.createElement('pre')
            element.append(code)
          }
          break
        }
        case 'link': {
          const url = linkURL(item.href)
          element = document.createElement(url ? 'a' : 'span')
          if (url) element.href = url
          append(element, item.tokens)
          break
        }
        case 'image':
          container.append(document.createTextNode('画像：' + decodeText(item.text || '説明なし')))
          continue
        case 'checkbox':
          container.append(document.createTextNode(item.checked ? '［済］' : '［未］'))
          continue
        case 'heading':
          // Keep the app title, chat heading and speaker above headings inside an answer.
          element = document.createElement('h' + Math.min(6, item.depth + 3))
          append(element, item.tokens)
          break
        default:
          if (Object.hasOwn(tags, item.type)) {
            element = document.createElement(tags[item.type])
            if (item.tokens) append(element, item.tokens)
          } else if (item.tokens) {
            append(container, item.tokens)
            continue
          } else {
            container.append(document.createTextNode(decodeText(item.text ?? item.raw ?? '')))
            continue
          }
      }
      container.append(element)
    }
  }
  try {
    append(fragment, marked.lexer(source, { gfm: true, breaks: true }))
  } catch {
    fragment.replaceChildren(document.createTextNode(source))
  }
  return fragment
}

export function spokenMarkdown(document, source) {
  const content = renderMarkdown(document, source)
  for (const row of content.querySelectorAll('tr')) {
    const cells = Array.from(row.children, cell => cell.textContent)
    row.replaceChildren(document.createTextNode(cells.join('、') + '。\n'))
  }
  for (const block of content.querySelectorAll('p, pre, li, h4, h5, h6, blockquote, hr, br')) {
    block.after(document.createTextNode('\n'))
  }
  return content.textContent.replace(/\n\s*\n+/g, '\n').trim()
}
