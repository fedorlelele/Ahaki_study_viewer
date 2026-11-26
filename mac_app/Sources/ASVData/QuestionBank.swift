import Foundation

public struct QuestionBank: Sendable {
    public let questions: [ExamQuestion]
    public let subjects: [String]
    public let examTypes: [String]
    public let sessions: [Int]
    public let generatedAt: String

    private let index: [SearchEntry]

    /// A distributed .app contains a snapshot of the same data published by ASV.
    public static func load() throws -> Self {
        if let url = Bundle.main.url(forResource: "ASVQuestions", withExtension: "json") {
            return try load(from: url)
        }
        #if DEBUG
        // SwiftPM's command-line build runs directly from the source checkout.
        let project = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
            .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
        let web = project.appendingPathComponent("docs/output/web", isDirectory: true)
        if FileManager.default.fileExists(atPath: web.appendingPathComponent("questions.json").path) {
            let decoder = JSONDecoder()
            let manifest = try decoder.decode(Manifest.self, from: Data(contentsOf: web.appendingPathComponent("questions_manifest.json")))
            let questions = try decoder.decode([ExamQuestion].self, from: Data(contentsOf: web.appendingPathComponent("questions.json")))
            return try Self(manifest: manifest, questions: questions)
        }
        #endif
        throw BankError.missing
    }

    public static func load(from url: URL) throws -> Self {
        let snapshot = try JSONDecoder().decode(Snapshot.self, from: Data(contentsOf: url, options: .mappedIfSafe))
        return try Self(manifest: snapshot.manifest, questions: snapshot.questions)
    }

    private init(manifest: Manifest, questions: [ExamQuestion]) throws {
        guard !questions.isEmpty, !manifest.generatedAt.isEmpty,
              questions.count == manifest.total, Set(questions.map(\.id)).count == manifest.total else {
            throw BankError.incomplete
        }
        self.questions = questions
        generatedAt = manifest.generatedAt
        // Preserve the curriculum order used in the published examinations.
        var seen = Set<String>()
        subjects = questions.map(\.subject).filter { seen.insert($0).inserted }
        examTypes = Set(questions.map(\.examType)).sorted()
        sessions = Set(questions.map(\.examSession)).sorted(by: >)
        index = questions.map(SearchEntry.init)
    }

    /// Space-separated terms use AND. A comma separates alternative groups.
    /// A leading # matches an entire tag and a serial may be entered as A01001.
    public func search(_ query: String, subject: String? = nil, examType: String? = nil,
                       session: Int? = nil) -> [ExamQuestion] {
        let groups = normalized(query).split { $0 == "," || $0 == "、" }
            .map { $0.split(whereSeparator: \.isWhitespace).map { SearchTerm(String($0)) } }
            .filter { !$0.isEmpty }
        if groups.isEmpty && subject == nil && examType == nil && session == nil { return questions }
        return questions.indices.compactMap { offset in
            let question = questions[offset]
            if let subject, question.subject != subject { return nil }
            if let examType, question.examType != examType { return nil }
            if let session, question.examSession != session { return nil }
            if groups.isEmpty { return question }
            let entry = index[offset]
            return groups.contains { $0.allSatisfy { entry.matches($0) } } ? question : nil
        }
    }
}

private struct Manifest: Decodable {
    let generatedAt: String
    let total: Int
    enum CodingKeys: String, CodingKey { case generatedAt = "generated_at", total }
}

private struct Snapshot: Decodable {
    let manifest: Manifest
    let questions: [ExamQuestion]
}

private func normalized(_ value: String) -> String {
    value.precomposedStringWithCompatibilityMapping.lowercased()
}

private enum SearchTerm {
    case tag(String), serial(String), text(String)

    init(_ term: String) {
        if term.hasPrefix("#") {
            self = .tag(String(term.dropFirst()))
            return
        }
        if let prefix = term.first, prefix == "a" || prefix == "b" {
            let digits = term.dropFirst()
            let pair = digits.split(separator: "-", omittingEmptySubsequences: false)
            if pair.count == 2, (1...2).contains(pair[0].count), (1...3).contains(pair[1].count),
               let session = Int(pair[0]), let number = Int(pair[1]) {
                self = .serial(String(prefix) + String(format: "%02d-%03d", session, number))
                return
            }
            if (4...5).contains(digits.count), let session = Int(digits.dropLast(3)), let number = Int(digits.suffix(3)) {
                self = .serial(String(prefix) + String(format: "%02d-%03d", session, number))
                return
            }
        }
        self = .text(term)
    }
}

private struct SearchEntry: Sendable {
    let serial: String
    let text: String
    let tags: [String]

    init(_ question: ExamQuestion) {
        serial = normalized(question.id)
        tags = question.tags.map(normalized)
        let fields = [question.id, question.subject, question.caseText ?? "", question.stem]
        text = normalized((fields + question.choices + question.subtopics + question.tags).joined(separator: " "))
    }

    func matches(_ term: SearchTerm) -> Bool {
        switch term {
        case .tag(let tag): return tags.contains(tag)
        case .serial(let id): return serial == id
        case .text(let word): return text.range(of: word, options: .literal) != nil
        }
    }
}

private enum BankError: LocalizedError {
    case missing, incomplete

    var errorDescription: String? {
        switch self {
        case .missing: return "問題データを開けません。ASVをインストールし直してください。"
        case .incomplete: return "問題データの件数または版情報が正しくありません。"
        }
    }
}
