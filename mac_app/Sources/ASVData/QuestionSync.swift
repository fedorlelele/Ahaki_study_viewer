import CryptoKit
import Foundation
import JavaScriptCore

/// The merged bank is replaced only after the entire publication and its
/// corrections decode successfully. The original base is kept separately so
/// corrections removed on the server also disappear on the next refresh.
public struct QuestionSync: Sendable {
    public let directory: URL
    public init(directory: URL) { self.directory = directory }
    private var snapshotURL: URL { directory.appendingPathComponent("questions.json") }

    private struct Publication: Codable, Equatable {
        let generated_at: String
        let total: Int
    }

    public func loadLocal() throws -> QuestionBank {
        if let bank = try? QuestionBank.load(from: snapshotURL) {
            if let url = Bundle.main.url(forResource: "StudyBundledManifest", withExtension: "json"),
               let bundled = try? JSONDecoder().decode(Publication.self, from: Data(contentsOf: url)),
               bundled.generated_at > bank.generatedAt,
               let newer = try? QuestionBank.load() { return newer }
            return bank
        }
        return try QuestionBank.load()
    }

    public func refresh(using service: StudyWebService, current: QuestionBank?) async throws -> QuestionBank? {
        let manifest = try await publication(service)
        if let current, manifest.generated_at < current.generatedAt { throw StudySyncError.changingCatalog }
        let source = try await downloadBase(service, manifest, current: current)
        let (overrideCount, revision) = try await overrideRevision(service)
        if current?.generatedAt == manifest.generated_at, current?.questions.count == manifest.total,
           current?.cloudRevision == revision, current?.baseDigest == source.digest, current?.baseETag == source.etag,
           FileManager.default.fileExists(atPath: snapshotURL.path) { return nil }

        let corrections = try await overridePages(service, count: overrideCount)
        let (finalCount, finalRevision) = try await overrideRevision(service)
        guard finalCount == overrideCount, finalRevision == revision,
              try await publication(service) == manifest else { throw StudySyncError.changingCatalog }
        let (snapshot, bank) = try Self.merge(source, corrections: corrections, manifest: manifest,
                                              revision: revision, rules: service.rules)
        try Task.checkCancellation()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let sourceURL = directory.appendingPathComponent("base-" + source.digest + ".json")
        if !source.reused { try source.data.write(to: sourceURL, options: .atomic) }
        try snapshot.write(to: snapshotURL, options: .atomic)
        for url in (try? FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)) ?? []
            where url.lastPathComponent.hasPrefix("base-") && url.pathExtension == "json" && url.lastPathComponent != sourceURL.lastPathComponent {
            try? FileManager.default.removeItem(at: url)
        }
        return bank
    }

    private func publication(_ service: StudyWebService) async throws -> Publication {
        let request = try service.request(service.pages.appendingPathComponent("questions_manifest.json"))
        let (bytes, _) = try await service.fetch(request)
        let manifest = try JSONDecoder().decode(Publication.self, from: bytes)
        guard manifest.total > 0, !manifest.generated_at.isEmpty else { throw StudySyncError.catalog }
        return manifest
    }

    private struct Source {
        let data: Data
        let digest: String
        let etag: String?
        let reused: Bool

        init(_ data: Data, etag: String?, reused: Bool = false) {
            self.data = data
            digest = SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
            self.etag = etag
            self.reused = reused
        }
    }

    private func downloadBase(_ service: StudyWebService, _ manifest: Publication,
                              current: QuestionBank?) async throws -> Source {
        var request = try service.request(service.pages.appendingPathComponent("questions.json"),
                                          query: [URLQueryItem(name: "v", value: manifest.generated_at)])
        // A Pages correction can replace questions.json without changing the
        // manifest's timestamp. Revalidate the actual publication every time.
        if current?.generatedAt == manifest.generated_at, let tag = current?.baseETag,
           let digest = current?.baseDigest, digest.count == 64, digest.allSatisfy(\.isHexDigit) {
            request.setValue(tag, forHTTPHeaderField: "If-None-Match")
            let (data, response) = try await service.transport(request)
            if response.statusCode == 200 { return Source(data, etag: response.value(forHTTPHeaderField: "ETag")) }
            guard response.statusCode == 304 else { throw StudySyncError.http(response.statusCode) }
            if let disk = try? Data(contentsOf: directory.appendingPathComponent("base-" + digest + ".json"), options: .mappedIfSafe) {
                let source = Source(disk, etag: tag, reused: true)
                if source.digest == digest { return source }
            }
            request.setValue(nil, forHTTPHeaderField: "If-None-Match")
        }
        let (data, response) = try await service.fetch(request)
        return Source(data, etag: response.value(forHTTPHeaderField: "ETag"))
    }

    private func overrideRevision(_ service: StudyWebService) async throws -> (Int, String) {
        struct Stamp: Decodable { let updated_at: String }
        let query = [URLQueryItem(name: "select", value: "updated_at"),
                     URLQueryItem(name: "order", value: "updated_at.desc.nullslast"),
                     URLQueryItem(name: "limit", value: "1")]
        var request = try service.request(service.database.appendingPathComponent("question_overrides"),
                                          query: query, databaseRequest: true)
        request.setValue("count=exact", forHTTPHeaderField: "Prefer")
        let (data, http) = try await service.fetch(request)
        let rows = try JSONDecoder().decode([Stamp].self, from: data)
        guard let range = http.value(forHTTPHeaderField: "Content-Range"),
              let count = Int(range.components(separatedBy: "/").last ?? ""), count >= 0,
              rows.count == min(1, count) else { throw StudySyncError.catalog }
        return (count, "\(count):\(rows.first?.updated_at ?? "")")
    }

    private func overridePages(_ service: StudyWebService, count: Int) async throws -> [Data] {
        let fields = "serial,explanation,explanation_source,tags,subtopics,case_text,stem,choices,answer_indices,answer_index,answer_none,updated_at,synced_at"
        var pages: [Data] = []
        var seen = Set<String>()
        struct RowID: Decodable { let serial: String }
        while seen.count < count {
            let offset = seen.count
            let query = [URLQueryItem(name: "select", value: fields), URLQueryItem(name: "order", value: "serial.asc"),
                         URLQueryItem(name: "limit", value: "500"), URLQueryItem(name: "offset", value: String(offset))]
            let request = try service.request(service.database.appendingPathComponent("question_overrides"),
                                              query: query, databaseRequest: true)
            let bytes = try await service.fetch(request).0
            let ids = try JSONDecoder().decode([RowID].self, from: bytes)
            guard !ids.isEmpty, seen.count + ids.count <= count else { throw StudySyncError.changingCatalog }
            for row in ids where row.serial.isEmpty || !seen.insert(row.serial).inserted {
                throw StudySyncError.changingCatalog
            }
            pages.append(bytes)
        }
        return pages
    }

    private static func merge(_ source: Source, corrections: [Data], manifest: Publication,
                              revision: String, rules: URL) throws -> (Data, QuestionBank) {
        // Execute the same bundled merge rules used by ASV Web. Downloaded
        // strings are passed to JSON.parse as data, never evaluated as code.
        let context = JSContext()!
        context.evaluateScript(try String(contentsOf: rules, encoding: .utf8))
        guard let merge = context.objectForKeyedSubscript("AhakiQuestions")?.objectForKeyedSubscript("applyQuestionOverrides"),
              !merge.isUndefined, let json = context.objectForKeyedSubscript("JSON"),
              let rows = json.invokeMethod("parse", withArguments: [String(decoding: source.data, as: UTF8.self)]), rows.isArray,
              Int(rows.objectForKeyedSubscript("length")?.toUInt32() ?? 0) == manifest.total else { throw StudySyncError.catalog }
        let pages = JSValue(newArrayIn: context)!
        for (index, page) in corrections.enumerated() {
            let values = json.invokeMethod("parse", withArguments: [String(decoding: page, as: UTF8.self)])
            guard let values, values.isArray else { throw StudySyncError.catalog }
            pages.setValue(values, at: index)
        }
        var metadata: [String: Any] = ["generated_at": manifest.generated_at, "total": manifest.total,
                                       "cloud_revision": revision, "base_sha256": source.digest]
        metadata["base_etag"] = source.etag
        guard let overrides = pages.invokeMethod("flat", withArguments: []),
              let merged = merge.call(withArguments: [rows, overrides]),
              let result = json.invokeMethod("stringify", withArguments: [["manifest": metadata, "questions": merged]]),
              context.exception == nil, let string = result.toString() else { throw StudySyncError.catalog }
        let bytes = Data(string.utf8)
        return (bytes, try QuestionBank.load(data: bytes))
    }
}
