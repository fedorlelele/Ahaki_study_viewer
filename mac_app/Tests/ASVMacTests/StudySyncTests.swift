import Foundation
import XCTest
@testable import ASVData

private actor CatalogFixture {
    var version = 1
    var broken = false
    var removed = false
    var baseVersion = 1
    var baseDownloads = 0
    func editBase() { baseVersion += 1 }

    func change(broken: Bool = false, removed: Bool = false) {
        version += 1
        self.broken = broken
        self.removed = removed
    }

    func send(_ request: URLRequest) throws -> (Data, HTTPURLResponse) {
        let url = try XCTUnwrap(request.url)
        let body: Any
        var headers: [String: String] = [:]
        var status = 200
        switch url.lastPathComponent {
        case "questions_manifest.json":
            body = ["total": 1, "generated_at": "2026-01-01T00:00:00Z"] as [String: Any]
        case "questions.json":
            let etag = "\"base-\(baseVersion)\""
            headers["ETag"] = etag
            if request.value(forHTTPHeaderField: "If-None-Match") == etag {
                status = 304
                body = []
            } else {
                baseDownloads += 1
                body = [["serial": "A01-001", "exam_type": "あん摩", "exam_session": 1, "subject": "A",
                         "stem": "出題\(baseVersion)", "choices": ["一", "二"], "answer_index": 1,
                         "explanation_latest": "公開版"] as [String: Any]]
            }
        case "question_overrides":
            XCTAssertEqual(request.value(forHTTPHeaderField: "apikey"), "test")
            if request.value(forHTTPHeaderField: "Prefer") == "count=exact" {
                body = removed ? [] : [["updated_at": "2026-01-0\(version)T01:00:00Z"]]
                headers["Content-Range"] = removed ? "*/0" : "0-0/1"
            } else {
                XCTAssertFalse(removed)
                body = [["serial": "A01-001", "explanation": "訂正本文", "answer_indices": broken ? [9] : [2],
                         "updated_at": "2026-01-0\(version)T01:00:00Z"] as [String: Any]]
            }
        default:
            throw URLError(.unsupportedURL)
        }
        return (try JSONSerialization.data(withJSONObject: body),
                HTTPURLResponse(url: url, statusCode: status, httpVersion: nil, headerFields: headers)!)
    }
}

private actor AnswerFixture {
    var posts: [[String: Any]] = []
    var matches: [URLComponents] = []
    func send(_ request: URLRequest) throws -> (Data, HTTPURLResponse) {
        let url = try XCTUnwrap(request.url)
        let body: String
        let status: Int
        if request.httpMethod == "POST" {
            XCTAssertEqual(request.value(forHTTPHeaderField: "Prefer"), "return=minimal")
            let fields = try XCTUnwrap(JSONSerialization.jsonObject(with: XCTUnwrap(request.httpBody)) as? [String: Any])
            posts.append(fields)
            body = #"{"code":"23505"}"#
            status = 409
        } else {
            let query = try XCTUnwrap(URLComponents(url: url, resolvingAgainstBaseURL: false))
            matches.append(query)
            // First exact match is empty, then a colliding row is visible.
            // The second ID already holds the same event (lost HTTP response).
            body = matches.count == 1 ? "[]" : #"[{"id":-8112345678901234510}]"#
            status = 200
        }
        return (Data(body.utf8), HTTPURLResponse(url: url, statusCode: status, httpVersion: nil, headerFields: nil)!)
    }
    func validate(_ attempt: StudyAnswer, anonID: UUID) throws {
        XCTAssertEqual(posts.count, 2)
        XCTAssertNotEqual(posts.first?["id"] as? String, posts.last?["id"] as? String)
        XCTAssertLessThan(try XCTUnwrap(Int64(try XCTUnwrap(posts.first?["id"] as? String))), 0)
        XCTAssertEqual(posts[0]["selected_index"] as? Int, 2)
        XCTAssertEqual(posts[0]["is_correct"] as? Bool, true)
        XCTAssertEqual(posts[0]["created_at"] as? String, attempt.answeredAt)
        XCTAssertEqual(posts[0]["anon_id"] as? String, anonID.uuidString.lowercased())
        XCTAssertEqual(matches.count, 3)
        XCTAssertEqual(matches[0].queryItems?.first(where: { $0.name == "created_at" })?.value, "eq." + attempt.answeredAt)
        XCTAssertEqual(matches[1].queryItems?.count, 3)
    }
}

final class StudySyncTests: XCTestCase {
    func service(_ transport: @escaping StudyWebService.Transport) throws -> StudyWebService {
        let rules = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
            .deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("docs/web_app/shared/questions.js")
        return StudyWebService(pages: URL(string: "https://pages.invalid")!, database: URL(string: "https://db.invalid")!,
                               publishableKey: "test", rules: rules, transport: transport)
    }

    func testCorrectedBankWorksOfflineAndFailedRefreshPreservesSnapshot() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: dir) }
        let sync = QuestionSync(directory: dir)
        let server = CatalogFixture()
        let api = try service { try await server.send($0) }
        let first = try await sync.refresh(using: api, current: nil)
        let cached = try sync.loadLocal()
        XCTAssertEqual(first?.questions.first?.explanation, "訂正本文")
        XCTAssertEqual(cached.questions.first?.answerIndices, [2])
        let unchanged = try await sync.refresh(using: api, current: cached)
        XCTAssertNil(unchanged)
        let baseDownloads = await server.baseDownloads
        XCTAssertEqual(baseDownloads, 1)
        await server.editBase()
        let edited = try await sync.refresh(using: api, current: cached)
        XCTAssertEqual(edited?.questions.first?.stem, "出題2")

        await server.change(broken: true)
        do {
            _ = try await sync.refresh(using: api, current: cached)
            XCTFail("A bad answer index must not replace the cache")
        } catch {}
        XCTAssertEqual(try sync.loadLocal().questions.first?.answerIndices, [2])
        await server.change(removed: true)
        let removed = try await sync.refresh(using: api, current: cached)
        XCTAssertEqual(removed?.questions.first?.explanation, "公開版")
        XCTAssertEqual(removed?.questions.first?.answerIndices, [1])
    }

    func testOfflineAttemptsSurviveRetryAndRelaunchAndCheckIdempotentConflicts() async throws {
        let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: file) }
        var journal = try AnswerJournal.load(from: file, migrating: ["B01-001": 3])
        XCTAssertTrue(journal.pending.isEmpty)
        journal.record(serial: "A01-001", choice: 1, correct: false)
        journal.retry("A01-001")
        journal.record(serial: "A01-001", choice: 2, correct: true)
        try journal.save(to: file)
        var restored = try AnswerJournal.load(from: file, migrating: [:])
        XCTAssertEqual(restored.responses, ["A01-001": 2, "B01-001": 3])
        XCTAssertEqual(restored.pending, journal.pending)
        XCTAssertEqual(restored.anonymousID, journal.anonymousID)
        XCTAssertNotEqual(restored.pending[0].event, restored.pending[1].event)
        restored.acknowledge(restored.pending[0].event)
        let event = try XCTUnwrap(restored.pending.first)
        let backend = AnswerFixture()
        try await service { try await backend.send($0) }.submit(event, anonymousID: restored.anonymousID)
        try await backend.validate(event, anonID: restored.anonymousID)
        restored.acknowledge(event.event)
        XCTAssertTrue(restored.pending.isEmpty)
    }
}
