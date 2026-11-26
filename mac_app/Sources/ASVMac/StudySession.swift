import AppKit
import ASVData
import AVFoundation
import Combine

func announce(_ text: String) {
    guard let window = NSApp.keyWindow else { return }
    NSAccessibility.post(element: window, notification: .announcementRequested, userInfo: [
        .announcement: text, .priority: NSAccessibilityPriorityLevel.medium.rawValue
    ])
}

@MainActor final class Narrator: NSObject, ObservableObject, AVSpeechSynthesizerDelegate {
    private let engine = AVSpeechSynthesizer()
    private var current: AVSpeechUtterance?
    @Published private(set) var isSpeaking = false

    override init() {
        super.init()
        engine.delegate = self
    }

    func read(_ text: String, rate: Double) {
        stop()
        let utterance = AVSpeechUtterance(string: text)
        utterance.voice = AVSpeechSynthesisVoice(language: "ja-JP")
        utterance.rate = Float(rate)
        current = utterance
        isSpeaking = true
        engine.speak(utterance)
    }
    func stop() {
        current = nil
        engine.stopSpeaking(at: .immediate)
        isSpeaking = false
    }
    nonisolated func speechSynthesizer(_ synthesizer: AVSpeechSynthesizer, didFinish utterance: AVSpeechUtterance) {
        Task { @MainActor in
            if current === utterance { current = nil; isSpeaking = false }
        }
    }
}

extension ExamQuestion {
    var questionText: String {
        ([id, "第\(examSession)回 " + examType, subject] + [caseText, stem].compactMap { $0 } + choices.enumerated().map {
            "選択肢\($0.offset + 1)。\($0.element)"
        }).joined(separator: "\n")
    }
    var solutionText: String {
        ([answerLabel, answerNote, explanation].filter { !$0.isEmpty }).joined(separator: "\n")
    }
    func feedback(for choice: Int) -> String {
        return isCorrect(choice) ? "正解" : "不正解"
    }
}

@MainActor final class StudySession: ObservableObject {
    @Published private(set) var bank: QuestionBank?
    @Published private(set) var results: [ExamQuestion] = []
    @Published var selectedID: String?
    @Published var error: String?
    @Published var query = ""
    @Published var subject: String?
    @Published var examType: String?
    @Published var session: Int?
    @Published private(set) var responses: [String: Int]
    private var positions: [String: Int] = [:]
    private let defaults: UserDefaults

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        responses = defaults.dictionary(forKey: "answersBySerial") as? [String: Int] ?? [:]
    }
    func load() async {
        guard bank == nil else { return }
        error = nil
        do {
            bank = try await Task.detached(priority: .userInitiated) { try QuestionBank.load() }.value
            await search()
        } catch { self.error = error.localizedDescription }
    }
    func search() async {
        guard let bank else { return }
        let (query, subject, examType, session) = (self.query, self.subject, self.examType, self.session)
        let found = await Task.detached(priority: .userInitiated) {
            bank.search(query, subject: subject, examType: examType, session: session)
        }.value
        guard !Task.isCancelled, query == self.query, subject == self.subject,
              examType == self.examType, session == self.session else { return }
        positions = Dictionary(uniqueKeysWithValues: found.enumerated().map { ($0.element.id, $0.offset) })
        results = found
        if selectedID.flatMap({ positions[$0] }) == nil { selectedID = found.first?.id }
    }
    var index: Int? { selectedID.flatMap { positions[$0] } }
    var current: ExamQuestion? { index.map { results[$0] } }
    var choice: Int? { selectedID.flatMap { responses[$0] } }
    func canMove(_ distance: Int) -> Bool { index.map { results.indices.contains($0 + distance) } ?? false }
    func move(_ distance: Int) {
        if let index, canMove(distance) { selectedID = results[index + distance].id }
    }
    func respond(_ number: Int) {
        guard let current, choice == nil, current.choices.indices.contains(number - 1) else { return }
        responses[current.id] = number
        save()
        announce("\(current.feedback(for: number))。正答は\(current.answerLabel)")
    }
    func retry() {
        guard let selectedID else { return }
        responses.removeValue(forKey: selectedID)
        save()
        announce("もう一度、選択肢を選んでください。")
    }
    private func save() { defaults.set(responses, forKey: "answersBySerial") }
}
