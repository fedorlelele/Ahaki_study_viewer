import Foundation

/// Answer numbers are one-based, as printed on the normal examination booklet.
public struct ExamQuestion: Identifiable, Decodable, Sendable {
    public let id: String
    public let examType: String
    public let examSession: Int
    public let subject: String
    public let caseText: String?
    public let stem: String
    public let choices: [String]
    public let answerIndices: [Int]
    public let answerNone: Bool
    public let answerNote: String
    public let explanation: String
    public let explanationModel: String
    public let reviewStatus: String
    public let tags: [String]
    public let subtopics: [String]

    public var answerLabel: String {
        answerNone ? "なし（全員正解）" : answerIndices.map(String.init).joined(separator: "・")
    }

    public func isCorrect(_ choice: Int) -> Bool {
        (1...choices.count).contains(choice) && (answerNone || answerIndices.contains(choice))
    }

    private enum CodingKeys: String, CodingKey {
        case id = "serial", examType = "exam_type", examSession = "exam_session"
        case subject, caseText = "case_text", stem, choices
        case variants = "answer_variants", indices = "answer_indices", index = "answer_index"
        case answerNone = "answer_none", notes = "answer_notes"
        case explanation = "explanation_latest", model = "explanation_latest_model_name"
        case reviewStatus = "explanation_latest_review_status", tags, subtopics
    }

    public init(from decoder: Decoder) throws {
        let row = try decoder.container(keyedBy: CodingKeys.self)
        id = try row.decode(String.self, forKey: .id)
        examType = try row.decode(String.self, forKey: .examType)
        examSession = try row.decode(Int.self, forKey: .examSession)
        subject = try row.decode(String.self, forKey: .subject)
        caseText = try row.decodeIfPresent(String.self, forKey: .caseText)
        stem = try row.decode(String.self, forKey: .stem)
        choices = try row.decode([String].self, forKey: .choices)
        guard !choices.isEmpty else {
            throw DecodingError.dataCorruptedError(forKey: .choices, in: row, debugDescription: "\(id) に選択肢がありません。")
        }

        answerNone = try row.decodeIfPresent(Bool.self, forKey: .answerNone) ?? false
        let variants = try row.decodeIfPresent([String: [Int]].self, forKey: .variants)
        let legacy = try row.decodeIfPresent([Int].self, forKey: .indices) ?? []
        let first = try row.decodeIfPresent(Int.self, forKey: .index)
        let numbers = variants?["default"] ?? (legacy.isEmpty ? first.map { [$0] } ?? [] : legacy)
        let choiceCount = choices.count
        guard numbers.allSatisfy({ (1...choiceCount).contains($0) }),
              numbers.isEmpty == answerNone, Set(numbers).count == numbers.count else {
            throw DecodingError.dataCorruptedError(forKey: .indices, in: row, debugDescription: "\(id) の正答が選択肢と一致しません。")
        }
        answerIndices = numbers
        let notes: [String]
        if let entries = try? row.decode([String].self, forKey: .notes) {
            notes = entries
        } else {
            notes = try row.decodeIfPresent(String.self, forKey: .notes).map { [$0] } ?? []
        }
        answerNote = notes.filter { !$0.isEmpty && !$0.contains("点字") }.joined(separator: " / ")
        explanation = try row.decodeIfPresent(String.self, forKey: .explanation) ?? ""
        explanationModel = try row.decodeIfPresent(String.self, forKey: .model) ?? ""
        reviewStatus = try row.decodeIfPresent(String.self, forKey: .reviewStatus) ?? ""
        tags = try row.decodeIfPresent([String].self, forKey: .tags) ?? []
        subtopics = try row.decodeIfPresent([String].self, forKey: .subtopics) ?? []
    }
}
