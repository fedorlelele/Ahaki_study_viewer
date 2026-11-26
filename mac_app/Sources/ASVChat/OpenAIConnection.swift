import CryptoKit
import Foundation
import Security

private final class RefuseRedirect: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest, completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}

enum OpenAIConnection {
    static let issuer = "https://auth.openai.com"
    static let resource = "https://api.openai.com/v1"
    static let sharingScope = "chatgpt.tokens.use.direct"
    static let dynamicID = "dynamic_agent_client"
    static let http: URLSession = {
        let config = URLSessionConfiguration.ephemeral
        config.httpShouldSetCookies = false
        config.urlCredentialStorage = nil
        config.urlCache = nil
        config.timeoutIntervalForRequest = 90
        return URLSession(configuration: config, delegate: RefuseRedirect(), delegateQueue: nil)
    }()

    static func random() throws -> String {
        var bytes = [UInt8](repeating: 0, count: 32)
        guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else {
            throw ChatError.invalidLogin
        }
        return Data(bytes).base64URL
    }

    static func authorizationURL(client: String?, host: String, redirect: URL, state: String, nonce: String,
                                 verifier: String, hint: String?, consent: Bool) throws -> URL {
        var fields = [
            "client_id": client ?? dynamicID, "ext_agent_host_id": host, "redirect_uri": redirect.absoluteString,
            "response_type": "code", "scope": "openid profile email offline_access resource.invoke " + sharingScope,
            "resource": resource, "state": state, "nonce": nonce, "code_challenge_method": "S256",
            "code_challenge": Data(SHA256.hash(data: Data(verifier.utf8))).base64URL
        ]
        if client == nil { fields["agent_name_hint"] = "Ahaki Study Viewer" }
        if let hint { fields["id_token_hint"] = hint }
        if consent { fields["prompt"] = "consent" }
        var url = URLComponents(string: issuer + "/api/accounts/authorize")!
        url.percentEncodedQuery = form(fields)
        guard let result = url.url else { throw ChatError.invalidLogin }
        return result
    }

    static func token(_ fields: [String: String]) async throws -> [String: Any] {
        let request = postForm(path: "/api/accounts/oauth/token", fields: fields.merging(["resource": resource]) { _, new in new })
        return try await json(request)
    }

    static func tokens(from response: [String: Any], previous: OAuthTokens? = nil) throws -> OAuthTokens {
        let scope = (response["scope"] as? String).map { Set($0.split(separator: " ").map(String.init)) }
        let granted = scope ?? previous?.scopes ?? []
        let idToken = response["id_token"] as? String ?? previous?.idToken
        let access = response["access_token"] as? String ?? ""
        let seconds = response["expires_in"] as? Double ?? 0
        if granted.contains(sharingScope) {
            guard !access.isEmpty, (response["token_type"] as? String)?.lowercased() == "bearer",
                  seconds > 0 else { throw ChatError.malformed }
        } else if idToken == nil {
            throw ChatError.malformed
        }
        return OAuthTokens(access: access, refresh: response["refresh_token"] as? String ?? previous?.refresh,
                           idToken: idToken, scopes: granted, expires: Date().addingTimeInterval(seconds))
    }

    static func revoke(client: String, refresh: String) async -> Bool {
        let request = postForm(path: "/api/accounts/oauth/revoke",
                               fields: ["client_id": client, "token": refresh, "token_type_hint": "refresh_token"])
        for attempt in 0..<2 {
            do {
                let (_, response) = try await http.data(for: request)
                guard let status = (response as? HTTPURLResponse)?.statusCode else { return false }
                if status == 200 { return true }
                if status < 500 { return false }
            } catch { if Task.isCancelled { return false } }
            if attempt == 0 { try? await Task.sleep(nanoseconds: 700_000_000) }
        }
        return false
    }

    private static func form(_ fields: [String: String]) -> String {
        let safe = CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")
        return fields.sorted { $0.key < $1.key }.map { key, value in
            key + "=" + (value.addingPercentEncoding(withAllowedCharacters: safe) ?? "")
        }.joined(separator: "&")
    }

    private static func postForm(path: String, fields: [String: String]) -> URLRequest {
        var request = URLRequest(url: URL(string: issuer + path)!)
        request.httpMethod = "POST"
        request.timeoutInterval = 20
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        request.httpBody = Data(form(fields).utf8)
        return request
    }

    static func json(_ request: URLRequest, session: URLSession = http) async throws -> [String: Any] {
        let (data, response) = try await session.data(for: request)
        guard let status = (response as? HTTPURLResponse)?.statusCode else { throw ChatError.malformed }
        let body = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        guard (200..<300).contains(status) else { throw remoteError(status, body) }
        guard let body else { throw ChatError.malformed }
        return body
    }

    static func remoteError(_ status: Int, _ object: [String: Any]?) -> ChatError {
        let code = (object?["error"] as? [String: Any])?["code"] as? String
        return .remote(status, code ?? object?["error"] as? String ?? "")
    }

    static func availableModel(token: String, session: URLSession) async throws -> String {
        var request = URLRequest(url: URL(string: resource + "/models")!)
        request.setValue("Bearer " + token, forHTTPHeaderField: "Authorization")
        let body = try await json(request, session: session)
        let models = body["models"] as? [[String: Any]] ?? []
        guard let slug = models.first(where: { $0["visibility"] as? String == "list" && $0["slug"] is String })?["slug"] as? String else {
            throw ChatError.noModels
        }
        return slug
    }

    static func answer(token: String, model: String, input: [[String: String]], session: URLSession,
                       onPartial: @MainActor (String) -> Void) async throws -> String {
        var request = URLRequest(url: URL(string: resource + "/responses")!)
        request.httpMethod = "POST"
        request.setValue("Bearer " + token, forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        let body: [String: Any] = [
            "model": model, "input": input,
            "instructions": "あん摩マッサージ指圧師、はり師、きゅう師の国家試験の学習を助ける講師として、日本語で質問に答えてください。問題文・選択肢と解答の根拠を確認し、読み上げで理解しやすい簡潔な文章で説明してください。",
            "store": false, "stream": true
        ]
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (bytes, response) = try await session.bytes(for: request)
        guard let status = (response as? HTTPURLResponse)?.statusCode else { throw ChatError.malformed }
        guard (200..<300).contains(status) else {
            var data = Data()
            for try await byte in bytes {
                guard data.count < 32768 else { break }
                data.append(byte)
            }
            throw remoteError(status, (try? JSONSerialization.jsonObject(with: data)) as? [String: Any])
        }
        var stream = AnswerStream()
        var nextDisplay = ContinuousClock.now
        for try await line in bytes.lines {
            try Task.checkCancellation()
            if let result = try stream.consume(line) { return result }
            if !stream.text.isEmpty, ContinuousClock.now >= nextDisplay {
                await onPartial(stream.text)
                nextDisplay = ContinuousClock.now.advanced(by: .milliseconds(120))
            }
        }
        throw ChatError.incomplete
    }
}

struct AnswerStream {
    private(set) var text = ""

    mutating func consume(_ line: String) throws -> String? {
        guard line.hasPrefix("data:") else { return nil }
        let data = line.dropFirst(5).trimmingCharacters(in: .whitespaces)
        if data == "[DONE]" { throw ChatError.incomplete }
        guard let event = (try? JSONSerialization.jsonObject(with: Data(data.utf8))) as? [String: Any] else {
            throw ChatError.malformed
        }
        let response = event["response"] as? [String: Any]
        switch event["type"] as? String {
        case "response.output_text.delta", "response.refusal.delta":
            text += event["delta"] as? String ?? ""
        case "error":
            throw ChatError.remote(200, event["code"] as? String ?? "")
        case "response.failed":
            throw OpenAIConnection.remoteError(200, response)
        case "response.incomplete": throw ChatError.incomplete
        case "response.completed":
            let result = text.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !result.isEmpty else { throw ChatError.incomplete }
            return result
        default: break
        }
        return nil
    }
}

extension Data {
    var base64URL: String {
        base64EncodedString().replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "")
    }

    init?(base64URL value: String) {
        var base = value.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        base += String(repeating: "=", count: (4 - base.count % 4) % 4)
        self.init(base64Encoded: base)
    }
}
