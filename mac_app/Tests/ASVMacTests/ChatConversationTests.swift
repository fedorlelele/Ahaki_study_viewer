import AppKit
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
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func stopLoading() {}

    override func startLoading() {
        let modelList = request.url?.lastPathComponent == "models"
        let data: Data
        if modelList {
            data = Data(#"{"models":[{"visibility":"list","slug":"fixture-model"}]}"#.utf8)
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
            let json = ###"{"type":"response.output_text.delta","delta":"## 回答\n\n発言数 \###(input.count)"}"###
            data = Data(("data: " + json + "\n\ndata: {\"type\":\"response.completed\"}\n\n").utf8)
        }
        let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1",
                                       headerFields: ["Content-Type": modelList ? "application/json" : "text/event-stream"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }
}
