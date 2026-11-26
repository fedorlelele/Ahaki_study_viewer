import Foundation
import XCTest
@testable import ASVMac

final class ChatMarkdownTests: XCTestCase {
    func testHeadingInlineFormattingAndNestedList() throws {
        let markdown = ChatMarkdown("""
        ## **ポイント**

        3. 酸素を *運ぶ*
           - 血液の成分
        4. 次の項目
        """)
        XCTAssertEqual(markdown.blocks.count, 4)
        guard case .heading(2, let heading) = markdown.blocks[0].content,
              case .paragraph(let item) = markdown.blocks[1].content else {
            return XCTFail("見出しとリストを別のブロックとして保持します")
        }
        XCTAssertTrue(heading.runs.contains { $0.inlinePresentationIntent?.contains(.stronglyEmphasized) == true })
        XCTAssertTrue(item.runs.contains { $0.inlinePresentationIntent?.contains(.emphasized) == true })
        XCTAssertEqual(markdown.blocks.map(\.listDepth), [0, 1, 2, 1])
        XCTAssertEqual(markdown.blocks.map(\.marker), [nil, "3.", "•", "4."])
        XCTAssertEqual(markdown.spokenText, "ポイント\n3. 酸素を 運ぶ\n血液の成分\n4. 次の項目")
    }

    func testQuotesCodeAndRuleDoNotChangeSpokenPunctuationInsideCode() {
        let markdown = ChatMarkdown("""
        > 既出の説明

        ---

        ```swift
        let formula = "a ** b"
        ```
        """)
        XCTAssertTrue(markdown.blocks[0].quoted)
        guard case .divider = markdown.blocks[1].content,
              case .code(let code) = markdown.blocks[2].content else { return XCTFail("引用・区切り・コード") }
        XCTAssertEqual(code, "let formula = \"a ** b\"")
        XCTAssertEqual(markdown.spokenText, "既出の説明\nlet formula = \"a ** b\"")
    }

    func testTableKeepsColumnsAndEmptyCellsForSpeech() {
        let markdown = ChatMarkdown("""
        | 部位 | 働き |
        | --- | --- |
        | **筋肉** | 運動 |
        | | 感覚 |
        """)
        guard case .table(let table) = markdown.blocks.first?.content else { return XCTFail("表を読めません") }
        XCTAssertEqual(table.rows.count, 3)
        XCTAssertEqual(String(table.rows[2][0].characters), "")
        XCTAssertEqual(String(table.rows[2][1].characters), "感覚")
        XCTAssertTrue(table.rows[1][0].runs.contains { $0.inlinePresentationIntent?.contains(.stronglyEmphasized) == true })
        XCTAssertEqual(markdown.spokenText, "部位、筋肉。働き、運動\n部位、空欄。働き、感覚")
    }

    func testSafeLinksAndImageDescriptions() {
        let markdown = ChatMarkdown("[公式](https://example.org) [ローカル](file:///tmp/test) [独自](asv://test) ![参考図](https://example.org/x.png)")
        guard case .paragraph(let text) = markdown.blocks.first?.content else { return XCTFail("段落を読めません") }
        XCTAssertEqual(text.runs.compactMap(\.link).map(\.absoluteString), ["https://example.org"])
        XCTAssertTrue(text.runs.allSatisfy { $0.imageURL == nil })
        XCTAssertEqual(markdown.spokenText, "公式 ローカル 独自 参考図")
    }
}
