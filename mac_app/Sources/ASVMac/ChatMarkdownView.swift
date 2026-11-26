import SwiftUI

struct ChatMarkdownView: View {
    let text: String
    @EnvironmentObject private var settings: Preferences
    @State private var document = ChatMarkdown("")

    static func spokenText(_ text: String) -> String { ChatMarkdown(text).spokenText }

    var body: some View {
        VStack(alignment: .leading, spacing: max(8, settings.size * 0.45)) {
            ForEach(document.blocks.indices, id: \.self) { index in
                let block = document.blocks[index]
                HStack(alignment: .firstTextBaseline, spacing: 6) {
                    if block.listDepth > 0 {
                        Text(verbatim: block.marker ?? "")
                            .frame(minWidth: settings.size * 0.8, alignment: .trailing)
                            .accessibilityHidden(block.marker == nil)
                    }
                    blockView(block.content)
                }
                .padding(.leading, settings.size * Double(min(4, max(0, block.listDepth - 1))) * 0.8)
                .padding(.leading, block.quoted ? 12 : 0)
                .overlay(alignment: .leading) {
                    if block.quoted {
                        Rectangle().fill(.secondary).frame(width: 3).accessibilityHidden(true)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .font(settings.font()).lineSpacing(settings.spacing)
        .lineLimit(nil).fixedSize(horizontal: false, vertical: true)
        .textSelection(.enabled)
        .onChange(of: text, initial: true) { _, value in document = ChatMarkdown(value) }
    }

    @ViewBuilder private func blockView(_ content: ChatMarkdown.Content) -> some View {
        switch content {
        case .paragraph(let text):
            Text(text)
        case .heading(let level, let text):
            Text(text)
                .font(settings.font(Double(max(1, 6 - level)), bold: true))
                .accessibilityHeading([.h1, .h2, .h3, .h4, .h5, .h6][min(5, max(0, level - 1))])
        case .code(let code):
            Text(verbatim: code)
                .font(.system(size: settings.size, design: .monospaced))
                .padding(8).frame(maxWidth: .infinity, alignment: .leading)
                .background(.quaternary, in: RoundedRectangle(cornerRadius: 5))
        case .table(let table):
            MarkdownTable(table: table)
        case .divider:
            Divider().accessibilityHidden(true)
        }
    }
}

private struct MarkdownTable: View {
    let table: ChatMarkdown.Table
    @EnvironmentObject private var settings: Preferences

    var body: some View {
        // Switch to labeled vertical rows when the columns cannot fit at the chosen text size.
        ViewThatFits(in: .horizontal) {
            Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 6) {
                ForEach(table.rows.indices, id: \.self) { row in
                    GridRow {
                        ForEach(table.columns.indices, id: \.self) { column in
                            Text(table.rows[row][column])
                                .fontWeight(row == 0 ? .semibold : .regular)
                                .gridColumnAlignment(alignment(for: column))
                                .accessibilityAddTraits(row == 0 ? .isHeader : [])
                        }
                    }
                    if row == 0 { Divider().gridCellUnsizedAxes(.horizontal) }
                }
            }
            VStack(alignment: .leading, spacing: 8) {
                if table.rows.count == 1 {
                    ForEach(table.columns.indices, id: \.self) { column in
                        Text(table.rows[0][column]).fontWeight(.semibold)
                    }
                }
                ForEach(table.rows.indices.dropFirst(), id: \.self) { row in
                    if row > 1 { Divider() }
                    ForEach(table.columns.indices, id: \.self) { column in
                        VStack(alignment: .leading, spacing: 2) {
                            Text(verbatim: table.heading(at: column)).fontWeight(.semibold)
                                .fixedSize(horizontal: false, vertical: true)
                            Text(table.rows[row][column]).fixedSize(horizontal: false, vertical: true)
                        }
                    }
                }
            }
            .fixedSize(horizontal: false, vertical: true)
        }
        .padding(8)
        .overlay(RoundedRectangle(cornerRadius: 5).strokeBorder(.secondary.opacity(0.5)))
        .accessibilityElement(children: .contain)
        .accessibilityLabel("表")
    }

    private func alignment(for column: Int) -> HorizontalAlignment {
        switch table.columns[column].alignment {
        case .center: return .center
        case .right: return .trailing
        default: return .leading
        }
    }
}
