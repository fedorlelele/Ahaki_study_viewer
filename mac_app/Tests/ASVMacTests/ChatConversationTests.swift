import AppKit
import os
import XCTest
@testable import ASVChat
@testable import ASVMac

final class ChatConversationTests: XCTestCase {
    @MainActor func testEnterShiftEnterAndJapaneseConversionInNativeEditor() throws {
        _ = NSApplication.shared
        let editor = PromptTextView(frame: NSRect(x: 0, y: 0, width: 340, height: 100))
        var submissions = 0
        editor.send = { submissions += 1 }
        func pressEnter(_ modifier: NSEvent.ModifierFlags = []) throws {
            let event = try XCTUnwrap(NSEvent.keyEvent(with: .keyDown, location: .zero, modifierFlags: modifier,
                                                      timestamp: 0, windowNumber: 0, context: nil, characters: "\r",
                                                      charactersIgnoringModifiers: "\r", isARepeat: false, keyCode: 36))
            editor.keyDown(with: event)
        }
        editor.string = "質問"
        editor.setSelectedRange(NSRange(location: 2, length: 0))
        try pressEnter()
        XCTAssertEqual(submissions, 1)
        try pressEnter(.shift)
        XCTAssertEqual(editor.string, "質問\n")
        XCTAssertEqual(submissions, 1)
        editor.setMarkedText("へんかん", selectedRange: NSRange(location: 4, length: 0),
                             replacementRange: NSRange(location: NSNotFound, length: 0))
        XCTAssertTrue(editor.hasMarkedText())
        try pressEnter()
        XCTAssertEqual(submissions, 1, "確定前の読みをEnterで送信しない")
        editor.unmarkText()
        try pressEnter()
        XCTAssertEqual(submissions, 2)
    }

    @MainActor func testTwoFollowupsAndIsolationWithLocalModelResponses() async throws {
        let token = OAuthTokens(access: "fixture", refresh: nil, idToken: nil,
                                scopes: [OpenAIConnection.sharingScope], expires: .distantFuture)
        var vault = ChatVault(profiles: ["A", "B"].map { ChatProfile(clientID: $0, tokens: token) }, activeID: "A")
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [ChatFixtureTransport.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        let chat = StudyChat(loadVault: { vault }, saveVault: { vault = $0 }, inferenceSession: session)
        func ask(_ prompt: String, problem: String = "A01-001") async throws -> String {
            chat.setDraft(prompt, for: problem)
            chat.send(problemID: problem, context: "問題 " + problem)
            XCTAssertEqual(chat.answeringProblemID, problem)
            XCTAssertTrue(chat.draft(for: problem).isEmpty)
            let clock = ContinuousClock()
            let deadline = clock.now.advanced(by: .seconds(5))
            while chat.isBusy && clock.now < deadline { try await Task.sleep(for: .milliseconds(5)) }
            let turn = try XCTUnwrap(chat.turns(for: problem).last)
            XCTAssertEqual(turn.state, .complete)
            return turn.answer
        }
        let first = try await ask("最初の質問")
        XCTAssertEqual(first, "## 回答\n\n発言数 2")
        let second = try await ask("その理由は？")
        XCTAssertEqual(second, "## 回答\n\n発言数 4")
        let other = try await ask("別の問題", problem: "B01-001")
        XCTAssertEqual(other, first)
        XCTAssertEqual(chat.turns(for: "A01-001").count, 2)
        chat.setDraft("書きかけ", for: "A01-001")
        chat.selectAccount(id: "B")
        XCTAssertTrue(chat.turns(for: "A01-001").isEmpty)
        XCTAssertTrue(chat.draft(for: "A01-001").isEmpty)
        chat.selectAccount(id: "A")
        XCTAssertEqual(chat.draft(for: "A01-001"), "書きかけ")
        XCTAssertEqual(chat.turns(for: "A01-001").map(\.question), ["最初の質問", "その理由は？"])
    }

    @MainActor func testModelAndEffortApplyToNextTurnAndSurviveRelaunchPerAccount() async throws {
        let legacy = #"{"profiles":[{"clientID":"A","welcomed":true},{"clientID":"B","welcomed":true}],"activeID":"A"}"#
        var vault = try JSONDecoder().decode(ChatVault.self, from: Data(legacy.utf8))
        let token = OAuthTokens(access: "choices", refresh: nil, idToken: nil,
                                scopes: [OpenAIConnection.sharingScope], expires: .distantFuture)
        for index in vault.profiles.indices { vault.profiles[index].tokens = token }
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [ChatFixtureTransport.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        func launch() -> StudyChat {
            StudyChat(loadVault: { vault }, saveVault: { next in
                vault = try JSONDecoder().decode(ChatVault.self, from: JSONEncoder().encode(next))
            }, inferenceSession: session)
        }
        let chat = launch()
        await chat.loadModels()
        XCTAssertEqual(chat.selectedModelID, "fixture-model")
        XCTAssertNil(chat.selectedEffort)
        chat.selectModel(id: "advanced")
        chat.selectEffort("high")
        chat.selectModel(id: "fixture-model")
        XCTAssertNil(chat.selectedEffort)
        chat.selectEffort("high")
        XCTAssertNil(chat.selectedEffort)
        chat.selectModel(id: "advanced")
        XCTAssertEqual(chat.selectedEffort, "high")
        chat.setDraft("質問", for: "A01")
        chat.send(problemID: "A01", context: "症例")
        chat.selectModel(id: "fixture-model")
        chat.selectEffort("low")
        try await waitUntil { !chat.isBusy }
        XCTAssertEqual(chat.turns(for: "A01").last?.answer, "advanced/high/2")
        chat.selectModel(id: "fixture-model")
        chat.setDraft("もう少し説明して", for: "A01")
        chat.send(problemID: "A01", context: "症例")
        try await waitUntil { !chat.isBusy }
        XCTAssertEqual(chat.turns(for: "A01").map(\.answer), ["advanced/high/2", "fixture-model/auto/4"])
        chat.selectModel(id: "advanced")
        let restored = launch()
        await restored.loadModels()
        XCTAssertEqual(restored.selectedModelID, "advanced")
        XCTAssertEqual(restored.selectedEffort, "high")
        restored.selectAccount(id: "B")
        XCTAssertTrue(restored.availableModels.isEmpty)
        await restored.loadModels()
        XCTAssertEqual(restored.selectedModelID, "fixture-model")
        XCTAssertNil(restored.selectedEffort)
        restored.selectAccount(id: "A")
        await restored.loadModels()
        XCTAssertEqual(restored.selectedEffort, "high")
        restored.selectEffort(nil)
        restored.setDraft("もう一問", for: "A02")
        restored.send(problemID: "A02", context: "症例")
        try await waitUntil { !restored.isBusy }
        XCTAssertEqual(restored.turns(for: "A02").last?.answer, "advanced/auto/2")
    }

    @MainActor func testCatalogRetryAndFallbackForUnavailablePreferences() async {
        let token = OAuthTokens(access: "retry-" + UUID().uuidString, refresh: nil, idToken: nil,
                                scopes: [OpenAIConnection.sharingScope], expires: .distantFuture)
        let stale = ChatModelPreferences(modelID: "retired", efforts: ["advanced": "unsupported"])
        let profile = ChatProfile(clientID: "A", tokens: token, modelPreferences: stale)
        var vault = ChatVault(profiles: [profile], activeID: "A")
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [ChatFixtureTransport.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        let chat = StudyChat(loadVault: { vault }, saveVault: { vault = $0 }, inferenceSession: session)
        await chat.loadModels()
        XCTAssertNotNil(chat.modelLoadError)
        XCTAssertFalse(chat.isLoadingModels)
        XCTAssertNil(chat.selectedModelID)
        await chat.loadModels(force: true)
        XCTAssertNil(chat.modelLoadError)
        XCTAssertEqual(chat.selectedModelID, "fixture-model")
        chat.selectModel(id: "advanced")
        XCTAssertNil(chat.selectedEffort)
    }

    @MainActor func testCancelSendWhileSidebarLoadsCatalogAndSwitchAccount() async throws {
        func profile(_ id: String, access: String) -> ChatProfile {
            ChatProfile(clientID: id, tokens: OAuthTokens(access: access, refresh: nil, idToken: nil,
                                                          scopes: [OpenAIConnection.sharingScope], expires: .distantFuture))
        }
        var vault = ChatVault(profiles: [profile("A", access: "stalled"), profile("B", access: "choices")], activeID: "A")
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [ChatFixtureTransport.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        let chat = StudyChat(loadVault: { vault }, saveVault: { vault = $0 }, inferenceSession: session)
        let sidebar = Task { await chat.loadModels() }
        try await waitUntil { chat.isLoadingModels }
        chat.setDraft("中止する質問", for: "P")
        chat.send(problemID: "P", context: "問題")
        await Task.yield()
        chat.cancel()
        try await waitUntil { !chat.isBusy }
        XCTAssertEqual(chat.turns(for: "P").last?.state, .interrupted("回答を中止しました。"))
        XCTAssertTrue(chat.isLoadingModels)
        chat.selectAccount(id: "B")
        await chat.loadModels()
        await sidebar.value
        XCTAssertNil(chat.modelLoadError)
        XCTAssertEqual(chat.selectedModelID, "fixture-model")
    }

    @MainActor func testExpiryDuringFirstSendPreservesReconnectMessage() async throws {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [ChatFixtureTransport.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        for refresh: String? in [nil, "expired-refresh"] {
            let token = OAuthTokens(access: "expired", refresh: refresh, idToken: nil,
                                    scopes: [OpenAIConnection.sharingScope], expires: .distantPast)
            var vault = ChatVault(profiles: [ChatProfile(clientID: "A", tokens: token)], activeID: "A")
            let chat = StudyChat(loadVault: { vault }, saveVault: { vault = $0 }, inferenceSession: session)
            chat.setDraft("質問", for: "P")
            chat.send(problemID: "P", context: "問題")
            try await waitUntil { !chat.isBusy }
            XCTAssertNil(vault.selected?.tokens)
            XCTAssertFalse(chat.isSignedIn)
            XCTAssertEqual(chat.status, ChatError.expired.localizedDescription)
        }
    }

    @MainActor private func waitUntil(_ condition: () -> Bool) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(3))
        while !condition() && clock.now < deadline { try await Task.sleep(for: .milliseconds(5)) }
        XCTAssertTrue(condition())
    }

    func testPartialOrCanceledAnswersAreExcludedFromFollowups() {
        var partial = ChatTurn(question: "途中の質問")
        partial.answer = "未完"
        partial.state = .interrupted("中止")
        var complete = ChatTurn(question: "前の質問")
        complete.answer = "前の応答"
        complete.state = .complete
        let input = Conversation(turns: [partial, complete]).requestInput(context: "症例", question: "続き")
        XCTAssertEqual(input.map { $0["role"] }, ["user", "user", "assistant", "user"])
        XCTAssertEqual(input.compactMap { $0["content"] }.dropFirst(), ["前の質問", "前の応答", "続き"])
    }
}

/// Only this test URLSession uses the fixture, so account tokens never leave the process.
private final class ChatFixtureTransport: URLProtocol {
    private static let retryTokens = OSAllocatedUnfairLock(initialState: Set<String>())
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func stopLoading() {}

    override func startLoading() {
        let modelList = request.url?.lastPathComponent == "models"
        let refreshing = request.url?.lastPathComponent == "token"
        let authorization = request.value(forHTTPHeaderField: "Authorization")
        let data: Data
        if refreshing {
            data = Data(#"{"error":"invalid_grant"}"#.utf8)
        } else if modelList {
            if authorization == "Bearer stalled" { return }
            if let authorization, authorization.hasPrefix("Bearer retry-"),
               Self.retryTokens.withLock({ $0.insert(authorization).inserted }) {
                client?.urlProtocol(self, didFailWithError: URLError(.notConnectedToInternet))
                return
            }
            data = Data("""
            {"models":[
              {"visibility":"list","slug":"fixture-model"},
              {"visibility":"list","slug":"advanced","supported_reasoning_levels":[{"effort":"low"},{"effort":"high"}]}
            ]}
            """.utf8)
        } else {
            var body = request.httpBody ?? Data()
            if let stream = request.httpBodyStream {
                stream.open()
                defer { stream.close() }
                var buffer = [UInt8](repeating: 0, count: 1024)
                while true {
                    let n = stream.read(&buffer, maxLength: buffer.count)
                    if n <= 0 { break }
                    body.append(contentsOf: buffer.prefix(n))
                }
            }
            let object = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any]
            let input = object?["input"] as? [[String: String]] ?? []
            let answer: String
            if authorization == "Bearer choices" {
                let model = object?["model"] as? String ?? "?"
                let effort = (object?["reasoning"] as? [String: Any])?["effort"] as? String ?? "auto"
                answer = "\(model)/\(effort)/\(input.count)"
            } else {
                answer = "## 回答\n\n発言数 \(input.count)"
            }
            let delta = ["type": "response.output_text.delta", "delta": answer]
            let json = String(decoding: try! JSONEncoder().encode(delta), as: UTF8.self)
            data = Data(("data: " + json + "\n\ndata: {\"type\":\"response.completed\"}\n\n").utf8)
        }
        let response = HTTPURLResponse(url: request.url!, statusCode: refreshing ? 400 : 200, httpVersion: "HTTP/1.1",
                                       headerFields: ["Content-Type": modelList || refreshing ? "application/json" : "text/event-stream"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }
}
