import AppKit
import Combine
import Foundation

@MainActor
public final class StudyChat: ObservableObject {
    @Published public private(set) var accounts: [ChatAccount] = []
    @Published public private(set) var activeAccountID: String?
    @Published public private(set) var isSignedIn = false
    @Published public private(set) var canAsk = false
    @Published public private(set) var isBusy = false
    @Published public private(set) var status = ""
    @Published public private(set) var reply = ""
    @Published public private(set) var usageLimitReached = false
    @Published public var showPlanWelcome = false

    private var vault = ChatVault()
    private var operation: Task<Void, Never>?
    private var model: String?

    public init() {
        do {
            vault = try ChatKeychain.load()
            updateAccounts()
        } catch { status = ChatError.keychain.localizedDescription }
    }

    public func cancel() { operation?.cancel() }

    public func clearReply() {
        reply = ""
        usageLimitReached = false
    }

    public func selectAccount(id: String) {
        guard !isBusy, vault.profiles.contains(where: { $0.clientID == id }) else { return }
        do {
            var next = vault
            next.activeID = id
            try persist(next)
            model = nil
            clearReply()
            status = isSignedIn ? "アカウントを切り替えました。" : "選んだアカウントでサインインしてください。"
        } catch { status = error.localizedDescription }
    }

    public func signIn(newAccount: Bool = false) async {
        await run(message: "ブラウザで ChatGPT へのサインインを完了してください。") {
            // A locked or previously inaccessible keychain must not be overwritten.
            self.vault = try ChatKeychain.load()
            let previous = newAccount ? nil : self.vault.selected
            let existing = previous?.clientID ?? self.vault.pendingID
            let state = try OpenAIConnection.random()
            let nonce = try OpenAIConnection.random()
            let verifier = try OpenAIConnection.random()
            let host = self.hostIdentifier()
            let callback = try await LoopbackLogin().receive(state: state) { redirect in
                let url = try OpenAIConnection.authorizationURL(
                    client: existing, host: host, redirect: redirect, state: state, nonce: nonce,
                    verifier: verifier, hint: previous?.tokens?.idToken,
                    consent: previous?.tokens != nil && !self.canAsk
                )
                guard NSWorkspace.shared.open(url) else { throw ChatError.callback }
            }
            NSApp?.activate()
            if let code = callback.query["error"] {
                if code == "access_denied" { throw ChatError.canceled }
                throw ChatError.invalidLogin
            }
            guard let code = callback.query["code"], !code.isEmpty,
                  let clientID = callback.query["client_id"] ?? existing, !clientID.isEmpty,
                  clientID != OpenAIConnection.dynamicID, existing == nil || existing == clientID else {
                throw ChatError.invalidLogin
            }
            if previous == nil {
                var pending = self.vault
                pending.pendingID = clientID
                try self.persist(pending)
            }
            let data = try await OpenAIConnection.token([
                "grant_type": "authorization_code", "code": code, "client_id": clientID,
                "code_verifier": verifier, "redirect_uri": callback.redirect.absoluteString
            ])
            guard let id = data["id_token"] as? String else { throw ChatError.invalidLogin }
            let identity = try await VerifiedIdentity.fetch(token: id, clientID: clientID, nonce: nonce)
            let known = self.vault.profiles.first { $0.clientID == clientID }
            guard known?.subject == nil || known?.subject == identity.subject else { throw ChatError.invalidLogin }
            let session = try OpenAIConnection.tokens(from: data)
            try Task.checkCancellation()
            var profile = known ?? ChatProfile(clientID: clientID)
            profile.email = identity.email
            profile.subject = identity.subject
            profile.tokens = session
            let firstUse = session.scopes.contains(OpenAIConnection.sharingScope) && !profile.welcomed
            if firstUse { profile.welcomed = true }
            var next = self.vault
            next.save(profile)
            next.activeID = clientID
            if next.pendingID == clientID { next.pendingID = nil }
            try self.persist(next)
            self.showPlanWelcome = firstUse
            self.clearReply()
            self.model = nil
            self.status = self.canAsk ? "ChatGPT に接続しました。" : "サインインしました。質問するには ChatGPT プランの利用を許可してください。"
        }
    }

    public func signOut() async {
        guard !isBusy, var profile = vault.selected else { return }
        await run(message: "サインアウトしています。") {
            var revoked = true
            if let refresh = profile.tokens?.refresh {
                revoked = await OpenAIConnection.revoke(client: profile.clientID, refresh: refresh)
            }
            profile.tokens = nil
            var next = self.vault
            next.save(profile)
            try self.persist(next)
            self.clearReply()
            self.model = nil
            self.status = revoked ? "サインアウトしました。" : "この Mac からサインアウトしました。サーバー側の解除は確認できませんでした。ChatGPT の設定で接続を確認してください。"
        }
    }

    public func ask(context: String, question: String) async {
        guard !isBusy, !context.isEmpty, !question.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard canAsk, let id = activeAccountID else { status = ChatError.plan.localizedDescription; return }
        clearReply()
        await run(message: "ChatGPT に質問しています。") {
            let access = try await self.accessToken(id: id)
            let slug: String
            if let cached = self.model { slug = cached }
            else {
                slug = try await OpenAIConnection.availableModel(token: access)
                self.model = slug
            }
            let answer = try await OpenAIConnection.answer(token: access, model: slug, context: context, question: question)
            try Task.checkCancellation()
            self.reply = answer
            self.status = "回答を受信しました。"
        }
    }

    private func accessToken(id: String) async throws -> String {
        guard var profile = vault.selected, profile.clientID == id,
              let tokens = profile.tokens, tokens.scopes.contains(OpenAIConnection.sharingScope) else {
            throw ChatError.plan
        }
        if tokens.expires.timeIntervalSinceNow > 60 { return tokens.access }
        guard let refresh = tokens.refresh else {
            try endExpiredSession(profile)
            throw ChatError.expired
        }
        do {
            let data = try await OpenAIConnection.token([
                "grant_type": "refresh_token", "client_id": id, "refresh_token": refresh
            ])
            let renewed = try OpenAIConnection.tokens(from: data, previous: tokens)
            profile.tokens = renewed
            var next = vault
            next.save(profile)
            try persist(next)
            guard renewed.scopes.contains(OpenAIConnection.sharingScope) else { throw ChatError.plan }
            return renewed.access
        } catch let error as ChatError where error.isTerminalRefresh {
            try endExpiredSession(profile)
            throw ChatError.expired
        }
    }

    private func endExpiredSession(_ profile: ChatProfile) throws {
        var signedOut = profile
        signedOut.tokens = nil
        var next = vault
        next.save(signedOut)
        try persist(next)
    }

    private func run(message: String, action: @escaping @MainActor () async throws -> Void) async {
        guard !isBusy else { return }
        isBusy = true
        status = message
        usageLimitReached = false
        let task = Task {
            do { try await action() }
            catch {
                if Task.isCancelled || error is CancellationError {
                    status = "操作をキャンセルしました。"
                } else {
                    status = error.localizedDescription
                    usageLimitReached = (error as? ChatError)?.isUsageLimit ?? false
                }
            }
        }
        operation = task
        await withTaskCancellationHandler { await task.value } onCancel: { task.cancel() }
        operation = nil
        isBusy = false
    }

    private func hostIdentifier() -> String {
        let key = "asv.chatgpt.installation"
        if let saved = UserDefaults.standard.string(forKey: key) { return saved }
        let id = "urn:uuid:" + UUID().uuidString.lowercased()
        UserDefaults.standard.set(id, forKey: key)
        return id
    }

    private func persist(_ next: ChatVault) throws {
        try ChatKeychain.save(next)
        vault = next
        updateAccounts()
    }

    private func updateAccounts() {
        activeAccountID = vault.activeID
        accounts = vault.profiles.enumerated().map { index, profile in
            let label = "接続 \(index + 1)" + (profile.email.map { "（\($0)）" } ?? "")
            return ChatAccount(id: profile.clientID, label: label)
        }
        isSignedIn = vault.selected?.tokens != nil
        canAsk = vault.selected?.tokens?.scopes.contains(OpenAIConnection.sharingScope) == true
    }
}
