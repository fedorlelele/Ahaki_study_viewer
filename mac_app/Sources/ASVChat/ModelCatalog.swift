import Foundation

public struct ChatModel: Identifiable, Equatable, Sendable {
    public let id: String
    public let displayName: String
    public let supportedEfforts: [String]

    static func catalog(from body: [String: Any]) throws -> [ChatModel] {
        guard let rows = body["models"] as? [Any] else { throw ChatError.malformed }
        var seen = Set<String>()
        var models = [ChatModel]()
        for case let row as [String: Any] in rows {
            guard row["visibility"] as? String == "list",
                  let id = (row["slug"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !id.isEmpty, seen.insert(id).inserted else { continue }
            let name = (row["display_name"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let efforts = supportedEfforts(in: row, for: id)
            models.append(ChatModel(id: id, displayName: name.isEmpty ? id : name, supportedEfforts: efforts))
        }
        guard !models.isEmpty else { throw ChatError.noModels }
        return models
    }

    private static func supportedEfforts(in row: [String: Any], for id: String) -> [String] {
        // The Responses API accepts these wire values. Client-side modes such as Codex's
        // “ultra” cannot be passed through as reasoning.effort.
        let apiEfforts = ["none", "minimal", "low", "medium", "high", "xhigh", "max"]
        if let levels = row["supported_reasoning_levels"] {
            var seen = Set<String>()
            return (levels as? [Any] ?? []).compactMap { level in
                let value = (level as? [String: Any])?["effort"] as? String ?? level as? String
                guard let value, apiEfforts.contains(value), seen.insert(value).inserted else { return nil }
                return value
            }
        }

        // Sign in with ChatGPT only guarantees a display name and slug for listed models.
        // Use model-specific API capabilities when the account omits effort metadata.
        // https://developers.openai.com/api/docs/models
        let reasoningLevels = ["low", "medium", "high", "xhigh", "max"]
        switch id {
        case "gpt-6.2-astra", "gpt-6.2-sol", "gpt-6.1-sol", "gpt-6-astra":
            return reasoningLevels
        case "gpt-6.1-terra", "gpt-6-sol", "gpt-6-luna", "gpt-5.6", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna":
            return ["none"] + reasoningLevels
        case "gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano", "gpt-5.2":
            return ["none", "low", "medium", "high", "xhigh"]
        default:
            return []
        }
    }
}
