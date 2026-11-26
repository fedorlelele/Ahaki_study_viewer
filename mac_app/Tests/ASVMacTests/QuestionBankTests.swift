import Foundation
import XCTest
import ASVData
@testable import ASVMac

final class QuestionBankTests: XCTestCase {
    private func row(_ serial: String = "A01-001") -> [String: Any] {
        ["serial": serial, "exam_type": "あん摩マッサージ指圧師", "exam_session": 1,
         "subject": "生理学", "case_text": "安静時", "stem": "該当するものを選べ。",
         "choices": ["心拍数", "呼吸数", "体温", "血圧"], "answer_index": 2,
         "tags": ["呼吸"], "subtopics": ["循環"]]
    }

    private func load(_ rows: [[String: Any]], total: Int? = nil) throws -> QuestionBank {
        let payload: [String: Any] = [
            "manifest": ["total": total ?? rows.count, "generated_at": "2026-01-01"],
            "questions": rows,
        ]
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try JSONSerialization.data(withJSONObject: payload).write(to: url)
        defer { try? FileManager.default.removeItem(at: url) }
        return try QuestionBank.load(from: url)
    }

    func testNormalBookletAndMultipleAcceptedAnswers() throws {
        var question = row()
        question["answer_variants"] = ["default": [1, 3], "braille": [2, 4]]
        question["answer_notes"] = ["点字冊子は２と４", "複数正答"]
        let result = try XCTUnwrap(load([question]).questions.first)
        XCTAssertEqual(result.answerLabel, "1・3")
        XCTAssertEqual(result.answerNote, "複数正答")
        XCTAssertTrue(result.isCorrect(1))
        XCTAssertTrue(result.isCorrect(3))
        XCTAssertFalse(result.isCorrect(2))
        XCTAssertFalse(result.isCorrect(4))
    }

    func testExplicitNoAnswerGradesEveryValidChoiceCorrect() throws {
        var question = row()
        question.removeValue(forKey: "answer_index")
        question["answer_none"] = true
        let result = try XCTUnwrap(load([question]).questions.first)
        for number in 1...4 {
            XCTAssertTrue(result.isCorrect(number))
            XCTAssertTrue(result.feedback(for: number).hasPrefix("正解"))
        }
        XCTAssertFalse(result.isCorrect(0))
        XCTAssertFalse(result.isCorrect(5))
        question["answer_none"] = false
        XCTAssertThrowsError(try load([question]))
        question["answer_index"] = 5
        XCTAssertThrowsError(try load([question]))
    }

    func testFilterAndSearchFullwidthSerialTagsAndCaseText() throws {
        let first = row()
        var second = row("B02-003")
        second["exam_type"] = "はり師・きゆう師"
        second["exam_session"] = 2
        second["subject"] = "解剖学"
        second["tags"] = ["呼吸器"]
        let bank = try load([first, second])
        XCTAssertEqual(bank.search("Ａ０１００１、B2-3").map(\.id), ["A01-001", "B02-003"])
        XCTAssertEqual(bank.search("#呼吸 安静時 呼吸数").map(\.id), ["A01-001"])
        XCTAssertEqual(bank.search("循環", subject: "解剖学", examType: "はり師・きゆう師", session: 2).map(\.id), ["B02-003"])
        XCTAssertTrue(bank.search("", subject: "解剖学", session: 1).isEmpty)
        XCTAssertEqual(bank.questions.first?.answerIndices, [2])
    }

    func testIncompleteAndDuplicateSnapshotsAreRejected() {
        XCTAssertThrowsError(try load([row()], total: 2))
        XCTAssertThrowsError(try load([row(), row()]))
    }
}
