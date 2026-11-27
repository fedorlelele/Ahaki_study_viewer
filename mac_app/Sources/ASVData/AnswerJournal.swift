import CryptoKit
import Foundation

public struct StudyAnswer: Codable, Sendable, Equatable {
    public let event: UUID
    public let serial: String
    public let choice: Int
    public let correct: Bool
    public let answeredAt: String
}

/// Replacing one journal commits both the local answer and its pending upload.
/// Only freshly graded answers enter the queue; pre-sync history stays local.
public struct AnswerJournal: Codable {
    public private(set) var responses: [String: Int]
    public private(set) var pending: [StudyAnswer]
    public let anonymousID: UUID

    public init(migrating responses: [String: Int]) {
        self.responses = responses
        pending = []
        anonymousID = UUID()
    }

    public static func load(from file: URL, migrating responses: [String: Int]) throws -> Self {
        do { return try JSONDecoder().decode(Self.self, from: Data(contentsOf: file)) }
        catch let error as CocoaError where error.code == .fileReadNoSuchFile {
            return Self(migrating: responses)
        }
    }

    public mutating func record(serial: String, choice: Int, correct: Bool, now: Date = Date()) {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        pending.append(StudyAnswer(event: UUID(), serial: serial, choice: choice, correct: correct,
                                   answeredAt: formatter.string(from: now)))
        responses[serial] = choice
    }
    public mutating func retry(_ serial: String) { responses.removeValue(forKey: serial) }
    public mutating func acknowledge(_ event: UUID) {
        if pending.first?.event == event { pending.removeFirst() }
    }
    public func save(to file: URL) throws {
        try FileManager.default.createDirectory(at: file.deletingLastPathComponent(), withIntermediateDirectories: true)
        try JSONEncoder().encode(self).write(to: file, options: .atomic)
    }
}

extension StudyWebService {
    public func submit(_ answer: StudyAnswer, anonymousID: UUID) async throws {
        // The Web schema uses a positive identity sequence. An attempt always
        // reuses its own negative ID, including after a lost network response.
        let table = database.appendingPathComponent("answers")
        let anonymous = anonymousID.uuidString.lowercased()
        for collision in 0..<8 {
            let input = Data("\(answer.event.uuidString.lowercased()):\(collision)".utf8)
            let bits = SHA256.hash(data: input).prefix(8).reduce(UInt64(0)) { ($0 << 8) | UInt64($1) }
            let id = "-" + String(max(1, bits & UInt64(Int64.max)))
            let fields: [String: Any] = ["id": id, "anon_id": anonymous, "serial": answer.serial,
                                         "is_correct": answer.correct, "selected_index": answer.choice,
                                         "created_at": answer.answeredAt]
            var post = try request(table, databaseRequest: true)
            post.httpMethod = "POST"
            post.setValue("application/json", forHTTPHeaderField: "Content-Type")
            post.setValue("return=minimal", forHTTPHeaderField: "Prefer")
            post.httpBody = try JSONSerialization.data(withJSONObject: fields)
            let (data, status) = try await transport(post)
            if (200...299).contains(status.statusCode) { return }
            struct Failure: Decodable { let code: String }
            guard status.statusCode == 409, (try? JSONDecoder().decode(Failure.self, from: data).code) == "23505"
            else { throw StudySyncError.http(status.statusCode) }

            let byID = [URLQueryItem(name: "select", value: "id"), URLQueryItem(name: "limit", value: "1"),
                        URLQueryItem(name: "id", value: "eq." + id)]
            let exact = byID + [URLQueryItem(name: "anon_id", value: "eq." + anonymous),
                                URLQueryItem(name: "serial", value: "eq." + answer.serial),
                                URLQueryItem(name: "selected_index", value: "eq.\(answer.choice)"),
                                URLQueryItem(name: "is_correct", value: "eq.\(answer.correct)"),
                                URLQueryItem(name: "created_at", value: "eq." + answer.answeredAt)]
            if try await exists(table, matching: exact) { return }
            guard try await exists(table, matching: byID) else { throw StudySyncError.response }
        }
        throw StudySyncError.response
    }

    private func exists(_ table: URL, matching query: [URLQueryItem]) async throws -> Bool {
        let get = try request(table, query: query, databaseRequest: true)
        let bytes = try await fetch(get).0
        struct PrimaryKey: Decodable { let id: Int64 }
        let rows = try JSONDecoder().decode([PrimaryKey].self, from: bytes)
        return rows.count == 1
    }
}
