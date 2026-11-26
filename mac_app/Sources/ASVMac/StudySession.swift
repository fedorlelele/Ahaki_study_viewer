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
    private var dictionary: ReadingDictionary?
    private var loadTask: Task<Void, Never>?
    private var pendingSpeech: Task<Void, Never>?
    @Published private(set) var isSpeaking = false
    @Published private(set) var dictionaryLoading = true
    @Published private(set) var dictionaryStatus = "読み辞書を確認中…"
    @Published var dictionaryEnabled: Bool {
        didSet { UserDefaults.standard.set(dictionaryEnabled, forKey: "readingDictionaryEnabled") }
    }

    override init() {
        dictionaryEnabled = UserDefaults.standard.object(forKey: "readingDictionaryEnabled") as? Bool ?? true
        super.init()
        engine.delegate = self
        loadDictionary()
    }

    func chooseDictionary() {
        guard !dictionaryLoading else { return }
        let panel = NSOpenPanel()
        panel.title = "理療科用読み辞書を選択"
        panel.message = "Jukugo.usrを選んでください。辞書はこのMacに保存されます。"
        panel.prompt = "読み込む"
        guard panel.runModal() == .OK, let source = panel.url else { return }
        loadDictionary(from: source)
    }

    private func loadDictionary(from source: URL? = nil) {
        dictionaryLoading = true
        dictionaryStatus = "読み辞書を確認中…"
        loadTask = Task {
            do {
                dictionary = try await Task.detached(priority: .userInitiated) {
                    if let source { return try ReadingDictionary.install(from: source) }
                    return try ReadingDictionary.loadInstalled()
                }.value
                if source != nil { dictionaryEnabled = true }
                if let dictionary {
                    dictionaryStatus = "理療科用読み辞書　登録語数 " + dictionary.count.formatted() + "語"
                } else {
                    dictionaryStatus = "読み辞書は未登録です。"
                }
            } catch {
                let prior = dictionary.map { "現在の辞書 " + $0.count.formatted() + "語を使います。" } ?? ""
                dictionaryStatus = error.localizedDescription + prior
            }
            dictionaryLoading = false
            if source != nil { announce(dictionaryStatus) }
        }
    }

    func read(_ text: String, rate: Double) {
        stop()
        isSpeaking = true
        pendingSpeech = Task {
            if dictionaryEnabled { await loadTask?.value }
            guard !Task.isCancelled else { return }
            let spoken = dictionaryEnabled ? dictionary?.pronunciations(in: text) ?? text : text
            let utterance = AVSpeechUtterance(string: spoken)
            utterance.voice = AVSpeechSynthesisVoice(language: "ja-JP")
            utterance.rate = Float(rate)
            current = utterance
            engine.speak(utterance)
        }
    }

    func stop() {
        pendingSpeech?.cancel()
        pendingSpeech = nil
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
