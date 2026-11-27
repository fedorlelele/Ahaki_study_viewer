import Foundation
import JavaScriptCore

/// Public ASV Web settings. No ASV or ChatGPT login is needed for guest statistics.
public struct StudyWebService: Sendable {
    public typealias Transport = @Sendable (URLRequest) async throws -> (Data, HTTPURLResponse)
    let pages: URL
    let database: URL
    let publishableKey: String
    let rules: URL
    let transport: Transport

    public init(pages: URL, database: URL, publishableKey: String, rules: URL,
                transport: @escaping Transport) {
        self.pages = pages
        self.database = database
        self.publishableKey = publishableKey
        self.rules = rules
        self.transport = transport
    }

    public static func production() throws -> Self {
        let configURL = try resource("StudyWebConfig", debugPath: "config.js")
        let rules = try resource("StudyQuestionRules", debugPath: "shared/questions.js")
        let context = JSContext()!
        context.evaluateScript("var window = {};")
        context.evaluateScript(try String(contentsOf: configURL, encoding: .utf8))
        guard context.exception == nil, let settings = context.objectForKeyedSubscript("window"),
              let url = settings.objectForKeyedSubscript("SUPABASE_URL")?.toString(),
              let project = URL(string: url), project.scheme == "https",
              project.host?.hasSuffix(".supabase.co") == true,
              let key = settings.objectForKeyedSubscript("SUPABASE_KEY")?.toString(),
              key.hasPrefix("sb_publishable_") else { throw StudySyncError.configuration }
        return Self(pages: URL(string: "https://fedorlelele.github.io/Ahaki_study_viewer/output/web")!,
                    database: project.appendingPathComponent("rest/v1"), publishableKey: key, rules: rules) { request in
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else { throw StudySyncError.response }
            return (data, http)
        }
    }

    private static func resource(_ name: String, debugPath: String) throws -> URL {
        if let url = Bundle.main.url(forResource: name, withExtension: "js") { return url }
        #if DEBUG
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
            .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("docs/web_app").appendingPathComponent(debugPath)
        if FileManager.default.fileExists(atPath: url.path) { return url }
        #endif
        throw StudySyncError.configuration
    }

    func request(_ url: URL, query: [URLQueryItem] = [], databaseRequest: Bool = false) throws -> URLRequest {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else { throw StudySyncError.configuration }
        if !query.isEmpty { components.queryItems = query }
        guard let target = components.url else { throw StudySyncError.configuration }
        var request = URLRequest(url: target, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 45)
        if databaseRequest { request.setValue(publishableKey, forHTTPHeaderField: "apikey") }
        return request
    }

    func fetch(_ request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await transport(request)
        guard (200...299).contains(response.statusCode) else {
            throw StudySyncError.http(response.statusCode)
        }
        return (data, response)
    }
}

enum StudySyncError: LocalizedError {
    case configuration, response, catalog, changingCatalog, http(Int)
    var errorDescription: String? {
        switch self {
        case .configuration: return "学習データの接続設定を読み込めません。"
        case .response: return "学習データのサーバーから応答を取得できません。"
        case .catalog: return "配信された問題・解説を確認できません。"
        case .changingCatalog: return "配信データが更新中です。しばらくしてから再確認してください。"
        case .http(let code): return "学習データのサーバーに接続できません（HTTP \(code)）。"
        }
    }
}
