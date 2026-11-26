import Foundation

/// Foundation parses Markdown into inline attributes and a hierarchy of block intents.
/// Blocks need their own views: a single SwiftUI Text loses paragraph and table boundaries.
struct ChatMarkdown {
    enum Content {
        case paragraph(AttributedString)
        case heading(Int, AttributedString)
        case code(String)
        case table(Table)
        case divider
    }

    struct Block {
        let content: Content
        let listDepth: Int
        let marker: String?
        let quoted: Bool

        var spokenText: String {
            let body: String
            switch content {
            case .paragraph(let text), .heading(_, let text): body = String(text.characters)
            case .code(let text): body = text
            case .table(let table): body = table.spokenRows.joined(separator: "\n")
            case .divider: return ""
            }
            if let marker, marker != "•" { return marker + " " + body }
            return body
        }
    }

    struct Table {
        let columns: [PresentationIntent.TableColumn]
        let rows: [[AttributedString]]  // Row 0 contains the column headings.

        func heading(at column: Int) -> String {
            let heading = String(rows[0][column].characters)
            return heading.isEmpty ? "列\(column + 1)" : heading
        }

        var spokenRows: [String] {
            if rows.count == 1 { return [rows[0].map { String($0.characters) }.joined(separator: "、")] }
            return rows.dropFirst().map { row in
                row.indices.map { column in
                    let value = String(row[column].characters)
                    return "\(heading(at: column))、" + (value.isEmpty ? "空欄" : value)
                }.joined(separator: "。")
            }
        }
    }

    let blocks: [Block]
    var spokenText: String { blocks.map(\.spokenText).filter { !$0.isEmpty }.joined(separator: "\n") }

    init(_ source: String) {
        let parsed = (try? AttributedString(markdown: source)) ?? AttributedString(source)
        let pieces = parsed.runs[\.presentationIntent].map { intent, range in
            Piece(text: Self.inlineText(parsed[range]), path: intent?.components ?? [])
        }
        var result: [Block] = []
        var seenItems = Set<Int>()
        var index = 0
        while index < pieces.count {
            let piece = pieces[index]
            let content: Content
            if let table = piece.table {
                let start = index
                repeat { index += 1 } while index < pieces.count && pieces[index].table?.id == table.id
                content = .table(Self.makeTable(columns: table.columns, cells: pieces[start..<index]))
            } else {
                index += 1
                switch piece.path.first?.kind {
                case .header(let level): content = .heading(level, piece.text)
                case .codeBlock:
                    var code = String(piece.text.characters)
                    if code.last == "\n" { code.removeLast() }
                    content = .code(code)
                case .thematicBreak: content = .divider
                default: content = .paragraph(piece.text)
                }
            }

            let items = piece.path.filter { if case .listItem = $0.kind { return true }; return false }
            var marker: String?
            if let item = items.first, case let .listItem(ordinal) = item.kind,
               seenItems.insert(item.identity).inserted {
                let list = piece.path.first { $0.kind == .orderedList || $0.kind == .unorderedList }
                marker = list?.kind == .orderedList ? "\(ordinal)." : "•"
            }
            result.append(Block(content: content, listDepth: items.count, marker: marker,
                                quoted: piece.path.contains { $0.kind == .blockQuote }))
        }
        blocks = result
    }

    private struct Piece {
        let text: AttributedString
        let path: [PresentationIntent.IntentType]
        var table: (id: Int, columns: [PresentationIntent.TableColumn])? {
            for part in path {
                if case let .table(columns) = part.kind { return (part.identity, columns) }
            }
            return nil
        }
    }

    private static func inlineText(_ substring: AttributedSubstring) -> AttributedString {
        var text = AttributedString(substring)
        text.presentationIntent = nil
        // Retain image descriptions as text, without requesting a remote resource.
        text.imageURL = nil
        for (link, range) in text.runs[\.link] {
            guard let link else { continue }
            let scheme = link.scheme?.lowercased() ?? ""
            if !["https", "http"].contains(scheme) || link.host?.isEmpty != false {
                text[range].link = nil
            }
        }
        return text
    }

    private static func makeTable(columns: [PresentationIntent.TableColumn], cells: ArraySlice<Piece>) -> Table {
        let blank = Array(repeating: AttributedString(), count: columns.count)
        var rows = [blank]
        for cell in cells {
            guard case let .tableCell(column) = cell.path.first?.kind, columns.indices.contains(column) else { continue }
            let row = cell.path.compactMap { part -> Int? in
                if case let .tableRow(index) = part.kind { return index }
                return nil
            }.first ?? 0
            while rows.count <= row { rows.append(blank) }
            rows[row][column] = cell.text
        }
        return Table(columns: columns, rows: rows)
    }
}
