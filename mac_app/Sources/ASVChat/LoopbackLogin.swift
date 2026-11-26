import Foundation
import Network

/// One IPv4 loopback callback, bound before the browser opens, for a single PKCE attempt.
@MainActor
final class LoopbackLogin {
    struct Callback {
        let redirect: URL
        let query: [String: String]
    }

    private var listener: NWListener?
    private var connections: [ObjectIdentifier: NWConnection] = [:]
    private var waiter: CheckedContinuation<Callback, Error>?
    private var timer: Task<Void, Never>?

    func receive(state: String, onReady: @escaping (URL) throws -> Void) async throws -> Callback {
        let settings = NWParameters.tcp
        settings.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
        let server = try NWListener(using: settings)
        listener = server
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                waiter = continuation
                guard !Task.isCancelled else { stop(.failure(CancellationError())); return }
                server.stateUpdateHandler = { [weak self] event in
                    MainActor.assumeIsolated {
                        guard let self, self.waiter != nil else { return }
                        switch event {
                        case .ready:
                            guard let redirect = self.redirectURL else { self.stop(.failure(ChatError.callback)); return }
                            do { try onReady(redirect) } catch { self.stop(.failure(error)) }
                        case .failed: self.stop(.failure(ChatError.callback))
                        default: break
                        }
                    }
                }
                server.newConnectionHandler = { [weak self] connection in
                    MainActor.assumeIsolated {
                        guard let self, self.waiter != nil, self.connections.count < 12 else {
                            connection.cancel()
                            return
                        }
                        self.connections[ObjectIdentifier(connection)] = connection
                        connection.start(queue: .main)
                        self.read(connection, state: state, buffer: Data())
                    }
                }
                server.start(queue: .main)
                timer = Task {
                    try? await Task.sleep(nanoseconds: 300_000_000_000)
                    if !Task.isCancelled { stop(.failure(ChatError.timeout)) }
                }
            }
        } onCancel: {
            Task { @MainActor in self.stop(.failure(CancellationError())) }
        }
    }

    private var redirectURL: URL? {
        guard let port = listener?.port else { return nil }
        return URL(string: "http://127.0.0.1:\(port.rawValue)/auth/callback")
    }

    private func read(_ connection: NWConnection, state: String, buffer: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 8192) { [weak self] data, _, ended, error in
            MainActor.assumeIsolated {
                guard let self, self.waiter != nil else { connection.cancel(); return }
                let request = buffer + (data ?? Data())
                if request.count <= 16_384,
                   let end = request.range(of: Data("\r\n\r\n".utf8)) {
                    self.respond(connection, request: Data(request[..<end.lowerBound]), state: state)
                } else if ended || error != nil || request.count > 16_384 {
                    self.connections.removeValue(forKey: ObjectIdentifier(connection))
                    connection.cancel()
                } else {
                    self.read(connection, state: state, buffer: request)
                }
            }
        }
    }

    private func respond(_ connection: NWConnection, request: Data, state: String) {
        let lines = String(decoding: request, as: UTF8.self).components(separatedBy: "\r\n")
        let fields = lines.first?.split(separator: " ") ?? []
        var result: Callback?
        if let redirect = redirectURL, fields.count == 3, fields[0] == "GET",
           let port = redirect.port,
           lines.dropFirst().contains(where: { $0.lowercased() == "host: 127.0.0.1:\(port)" }),
           let url = URLComponents(string: String(fields[1])), url.host == nil,
           url.scheme == nil, url.path == redirect.path {
            let items = url.queryItems ?? []
            var query = [String: String]()
            for item in items { query[item.name] = item.value }
            if items.count == query.count, query["state"] == state {
                result = Callback(redirect: redirect, query: query)
            }
        }
        let message = result == nil ? "このリクエストは無効です。" : "認証の応答を受け取りました。このタブを閉じて ASV に戻ってください。"
        let body = "<!doctype html><html lang=\"ja\"><meta charset=\"utf-8\"><title>ASV</title><p>\(message)</p></html>"
        let status = result == nil ? "400 Bad Request" : "200 OK"
        let header = "HTTP/1.1 \(status)\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Security-Policy: default-src 'none'\r\nContent-Length: \(body.utf8.count)\r\nConnection: close\r\n\r\n"
        connections.removeValue(forKey: ObjectIdentifier(connection))
        connection.send(content: Data((header + body).utf8), completion: .contentProcessed { _ in connection.cancel() })
        if let result { stop(.success(result)) }
    }

    private func stop(_ result: Result<Callback, Error>) {
        timer?.cancel()
        timer = nil
        listener?.cancel()
        listener = nil
        connections.values.forEach { $0.cancel() }
        connections.removeAll()
        waiter?.resume(with: result)
        waiter = nil
    }
}
