import Foundation
import Security

public struct ChatAccount: Identifiable {
    public let id: String
    public let label: String
}

struct OAuthTokens: Codable {
    let access: String
    let refresh: String?
    let idToken: String?
    let scopes: Set<String>
    let expires: Date
}

struct ChatProfile: Codable {
    let clientID: String
    var subject: String?
    var email: String?
    var tokens: OAuthTokens?
    var welcomed = false
}

struct ChatVault: Codable {
    var profiles = [ChatProfile]()
    var activeID: String?
    var pendingID: String?

    var selected: ChatProfile? { profiles.first { $0.clientID == activeID } }

    mutating func save(_ profile: ChatProfile) {
        if let index = profiles.firstIndex(where: { $0.clientID == profile.clientID }) {
            profiles[index] = profile
        } else {
            profiles.append(profile)
        }
    }
}

enum ChatKeychain {
    private static let query: [CFString: Any] = [
        kSecClass: kSecClassGenericPassword,
        kSecAttrService: "ASV.SignInWithChatGPT",
        kSecAttrAccount: "connections.v1"
    ]

    static func load() throws -> ChatVault {
        var read = query
        read[kSecReturnData] = true
        var result: CFTypeRef?
        let code = SecItemCopyMatching(read as CFDictionary, &result)
        if code == errSecItemNotFound { return ChatVault() }
        guard code == errSecSuccess, let data = result as? Data else { throw ChatError.keychain }
        return try JSONDecoder().decode(ChatVault.self, from: data)
    }

    static func save(_ vault: ChatVault) throws {
        let data = try JSONEncoder().encode(vault)
        let changes: [CFString: Any] = [kSecValueData: data]
        let status = SecItemUpdate(query as CFDictionary, changes as CFDictionary)
        if status == errSecSuccess { return }
        guard status == errSecItemNotFound else { throw ChatError.keychain }
        var item = query
        item[kSecValueData] = data
        item[kSecAttrAccessible] = kSecAttrAccessibleWhenUnlockedThisDeviceOnly
        guard SecItemAdd(item as CFDictionary, nil) == errSecSuccess else { throw ChatError.keychain }
    }
}

enum ChatError: LocalizedError {
    case keychain, invalidLogin, canceled, callback, timeout, malformed, incomplete, noModels, plan, expired
    case remote(Int, String)

    var errorDescription: String? {
        switch self {
        case .keychain: return "キーチェーンの接続情報を読み書きできません。キーチェーンのロックとアクセス権を確認してください。"
        case .invalidLogin: return "サインインの応答を検証できませんでした。アプリからもう一度サインインしてください。"
        case .canceled: return "サインインをキャンセルしました。"
        case .callback: return "サインイン用のブラウザまたは受け取り用ポートを開けませんでした。"
        case .timeout: return "サインインの有効時間が過ぎました。もう一度お試しください。"
        case .malformed: return "OpenAI の応答を読み取れませんでした。"
        case .incomplete: return "回答の受信が途中で終了しました。もう一度質問してください。"
        case .noModels: return "このアカウントで利用できるモデルが見つかりません。"
        case .plan: return "このアカウントで ChatGPT プランの利用を許可してください。"
        case .expired: return "接続の有効期限が切れました。もう一度サインインしてください。"
        case let .remote(status, code):
            switch (status, code) {
            case (429, _), (_, "subscription_sharing_usage_limit_exceeded"):
                return "利用上限に達しました。ChatGPT の利用状況を確認してください。"
            case (_, "access_denied"): return "ChatGPT との接続が許可されませんでした。"
            case (_, "subscription_sharing_user_not_eligible"), (403, _):
                return "このアカウントまたはワークスペースでは、この操作が許可されていません。"
            case (401, _): return "認証が認められませんでした。もう一度サインインしてください。"
            case (503, _): return "ChatGPT が一時的に利用できません。時間をおいてお試しください。"
            default: return "OpenAI へのリクエストを完了できませんでした（HTTP \(status)）。"
            }
        }
    }

    var isTerminalRefresh: Bool {
        guard case let .remote(_, code) = self else { return false }
        return ["invalid_grant", "invalid_refresh_token", "token_expired", "refresh_token_expired",
                "refresh_token_invalidated", "refresh_token_reused"].contains(code)
    }

    var isUsageLimit: Bool {
        guard case let .remote(status, code) = self else { return false }
        return status == 429 || code == "subscription_sharing_usage_limit_exceeded"
    }
}
