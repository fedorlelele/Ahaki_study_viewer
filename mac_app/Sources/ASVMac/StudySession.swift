import AppKit
import ASVData
import AVFoundation
import Combine
import Network

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
    private let catalog: QuestionSync
    private let journalFile: URL
    private var journal: AnswerJournal?
    private let service: () throws -> StudyWebService
    private var generation = 0
    private var loading = false
    private var online = false
    private var flushing = false
    private var nextAnswerAttempt = Date.distantPast
    @Published private(set) var refreshing = false
    @Published private(set) var catalogStatus = "このMacの問題データを使用します。"
    @Published private(set) var pendingCount = 0
    @Published private(set) var answerStatus = ""

    init(defaults: UserDefaults = .standard, directory: URL = ReadingDictionary.installedURL.deletingLastPathComponent()
        .appendingPathComponent("Study", isDirectory: true),
         service: @escaping () throws -> StudyWebService = StudyWebService.production) {
        catalog = QuestionSync(directory: directory)
        journalFile = directory.appendingPathComponent("answers.json")
        self.service = service
        let previous = defaults.dictionary(forKey: "answersBySerial") as? [String: Int] ?? [:]
        do {
            let local = try AnswerJournal.load(from: journalFile, migrating: previous)
            journal = local
            responses = local.responses
            pendingCount = local.pending.count
        } catch {
            responses = previous
            answerStatus = "保存済みの回答記録を読み込めません。" + error.localizedDescription
        }
    }

    func load() async {
        guard bank == nil, !loading else { return }
        loading = true
        defer { loading = false }
        error = nil
        do {
            let catalog = catalog
            let local = try await Task.detached(priority: .userInitiated) { try catalog.loadLocal() }.value
            if bank == nil {
                bank = local
                await search()
            }
        } catch { self.error = error.localizedDescription }
    }

    func monitor() async {
        await load()
        guard !Task.isCancelled else { return }
        let monitor = NWPathMonitor()
        monitor.pathUpdateHandler = { [weak self] path in
            Task { @MainActor in
                guard let self else { return }
                let connected = path.status == .satisfied
                let reconnected = connected && !self.online
                self.online = connected
                if reconnected {
                    self.nextAnswerAttempt = .distantPast
                    Task { await self.flushAnswers() }
                    await self.refresh()
                }
            }
        }
        monitor.start(queue: DispatchQueue(label: "ASV.study.network"))
        defer { monitor.cancel(); online = false }
        var minutes = 0
        while !Task.isCancelled {
            do { try await Task.sleep(for: .seconds(60)) } catch { return }
            minutes += 1
            if online {
                Task { await flushAnswers() }
                if minutes.isMultiple(of: 15) { await refresh() }
            }
        }
    }

    func refresh() async {
        guard !refreshing else { return }
        refreshing = true
        defer { refreshing = false }
        catalogStatus = "問題と解説の更新を確認しています…"
        do {
            if let updated = try await catalog.refresh(using: service(), current: bank) {
                generation += 1
                bank = updated
                error = nil
                await search()
            }
            catalogStatus = "確認日時　" + Date.now.formatted(date: .abbreviated, time: .shortened)
        } catch {
            catalogStatus = "更新を確認できません。このMacにある問題と解説を使用します。"
            if bank == nil { self.error = error.localizedDescription }
        }
    }

    func search() async {
        guard let bank else { return }
        let generation = generation
        let (query, subject, examType, session) = (self.query, self.subject, self.examType, self.session)
        let found = await Task.detached(priority: .userInitiated) {
            bank.search(query, subject: subject, examType: examType, session: session)
        }.value
        guard !Task.isCancelled, generation == self.generation, query == self.query, subject == self.subject,
              examType == self.examType, session == self.session else { return }
        positions = Dictionary(uniqueKeysWithValues: found.enumerated().map { ($0.element.id, $0.offset) })
        results = found
        if selectedID.flatMap({ positions[$0] }) == nil { selectedID = found.first?.id }
    }
    var index: Int? { selectedID.flatMap { positions[$0] } }
    var current: ExamQuestion? { index.map { results[$0] } }
    var choice: Int? {
        guard let current, let saved = responses[current.id], (1...current.choices.count).contains(saved) else { return nil }
        return saved
    }
    func canMove(_ distance: Int) -> Bool { index.map { results.indices.contains($0 + distance) } ?? false }
    func move(_ distance: Int) {
        if let index, canMove(distance) { selectedID = results[index + distance].id }
    }
    @discardableResult func respond(_ number: Int) -> Bool {
        guard let current, choice == nil, current.choices.indices.contains(number - 1) else { return false }
        guard var next = journal else { announce(answerStatus); return false }
        next.record(serial: current.id, choice: number, correct: current.isCorrect(number))
        guard save(next) else { return false }
        announce("\(current.feedback(for: number))。正答は\(current.answerLabel)")
        Task { await flushAnswers() }
        return true
    }
    func retry() {
        guard let selectedID, var next = journal else { return }
        next.retry(selectedID)
        guard save(next) else { return }
        announce("もう一度、選択肢を選んでください。")
    }
    private func save(_ next: AnswerJournal) -> Bool {
        do {
            try next.save(to: journalFile)
            journal = next
            responses = next.responses
            pendingCount = next.pending.count
            return true
        } catch {
            answerStatus = "回答記録を保存できません。" + error.localizedDescription
            announce(answerStatus)
            return false
        }
    }
    func flushAnswers() async {
        guard online, !flushing, Date.now >= nextAnswerAttempt, let local = journal, !local.pending.isEmpty else { return }
        flushing = true
        defer { flushing = false }
        do {
            let api = try service()
            while online, let event = journal?.pending.first {
                try await api.submit(event, anonymousID: local.anonymousID)
                guard var latest = journal else { return }
                latest.acknowledge(event.event)
                guard save(latest) else { return }
            }
            answerStatus = ""
        } catch {
            nextAnswerAttempt = Date.now.addingTimeInterval(60)
            answerStatus = "未送信の回答はこのMacに保存されています。接続時に再送します。"
        }
    }
}
