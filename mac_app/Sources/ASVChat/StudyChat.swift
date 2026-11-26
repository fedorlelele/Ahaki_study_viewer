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
    @Published public private(set) var answeringProblemID: String?
    @Published private var conversations = [ThreadKey: Conversation]()
    @Published public private(set) var usageLimitReached = false
    @Published public var showPlanWelcome = false
    @Published public private(set) var availableModels: [ChatModel] = []
    @Published public private(set) var isLoadingModels = false
    @Published public private(set) var modelLoadError: String?
    @Published private var modelPreferences = ChatModelPreferences()

    private var selectedModel: ChatModel? { modelPreferences.model(in: availableModels) }
    public var selectedModelID: String? { selectedModel?.id }
    public var selectedEffort: String? { selectedModel.flatMap { modelPreferences.effort(for: $0) } }

    private var vault = ChatVault()
    private var operation: Task<Void, Never>?
    private var modelLoad: Task<[ChatModel], Error>?
    private var tokenRefreshes = [String: Task<String, Error>]()

    private let loadVault: () throws -> ChatVault
    private let saveVault: (ChatVault) throws -> Void
    private let inferenceSession: URLSession

    public convenience init() {
        self.init(loadVault: ChatKeychain.load, saveVault: ChatKeychain.save, inferenceSession: OpenAIConnection.http)
    }

    init(loadVault: @escaping () throws -> ChatVault, saveVault: @escaping (ChatVault) throws -> Void,
         inferenceSession: URLSession) {
        self.loadVault = loadVault
        self.saveVault = saveVault
        self.inferenceSession = inferenceSession
        do {
            vault = try loadVault()
            updateAccounts()
        } catch { status = ChatError.keychain.localizedDescription }
    }

    public func cancel() { operation?.cancel() }

    public func loadModels(force: Bool = false) async {
        guard canAsk, let id = activeAccountID else { return }
        // The shared task publishes failures for both the sidebar and a concurrent send.
        _ = try? await catalog(for: id, force: force)
    }

    private func catalog(for id: String, force: Bool = false) async throws -> [ChatModel] {
        try checkCatalogAccess(for: id)
        if let modelLoad { return try await waitForSharedTask(modelLoad) }
        if !force, !availableModels.isEmpty { return availableModels }
        isLoadingModels = true
        modelLoadError = nil
        let task = Task {
            defer {
                if !Task.isCancelled {
                    modelLoad = nil
                    isLoadingModels = false
                }
            }
            do {
                try Task.checkCancellation()
                let token = try await accessToken(id: id)
                try Task.checkCancellation()
                let models = try await OpenAIConnection.availableModels(token: token, session: inferenceSession)
                try Task.checkCancellation()
                availableModels = models
                return models
            } catch {
                // Losing access clears and cancels the catalog while a send may still await it.
                try checkCatalogAccess(for: id)
                if !Task.isCancelled {
                    if case ChatError.noModels = error { availableModels = [] }
                    modelLoadError = error.localizedDescription
                }
                throw error
            }
        }
        modelLoad = task
        return try await waitForSharedTask(task)
    }

    private func checkCatalogAccess(for id: String) throws {
        guard activeAccountID == id else { throw CancellationError() }
        guard isSignedIn else { throw ChatError.expired }
        guard canAsk else { throw ChatError.plan }
    }

    private func clearCatalog() {
        modelLoad?.cancel()
        modelLoad = nil
        availableModels = []
        isLoadingModels = false
        modelLoadError = nil
    }

    public func selectModel(id: String) {
        guard !isBusy, availableModels.contains(where: { $0.id == id }) else { return }
        var preferences = modelPreferences
        preferences.modelID = id
        savePreferences(preferences)
    }

    public func selectEffort(_ effort: String?) {
        guard !isBusy, let model = selectedModel else { return }
        if let effort, !model.supportedEfforts.contains(effort) { return }
        var preferences = modelPreferences
        preferences.modelID = model.id
        preferences.efforts[model.id] = effort
        savePreferences(preferences)
    }

    private func savePreferences(_ preferences: ChatModelPreferences) {
        guard canAsk, var profile = vault.selected else { return }
        profile.modelPreferences = preferences
        var next = vault
        next.save(profile)
        do {
            try persist(next)
            modelLoadError = nil
        } catch { modelLoadError = error.localizedDescription }
    }

    private func threadKey(_ problemID: String) -> ThreadKey {
        ThreadKey(registration: isSignedIn ? activeAccountID : nil, problem: problemID)
    }

    public func turns(for problemID: String) -> [ChatTurn] {
        conversations[threadKey(problemID)]?.turns ?? []
    }

    public func draft(for problemID: String) -> String {
        conversations[threadKey(problemID)]?.draft ?? ""
    }

    public func setDraft(_ value: String, for problemID: String) {
        let key = threadKey(problemID)
        guard conversations[key]?.draft != value else { return }
        conversations[key, default: Conversation()].draft = value
    }

    public func selectAccount(id: String) {
        guard !isBusy, vault.profiles.contains(where: { $0.clientID == id }) else { return }
        do {
            var next = vault
            next.activeID = id
            try persist(next)
            usageLimitReached = false
            status = isSignedIn ? "アカウントを切り替えました。" : "選んだアカウントでサインインしてください。"
        } catch { status = error.localizedDescription }
    }

    public func signIn(newAccount: Bool = false) async {
        await run(message: "ブラウザで ChatGPT へのサインインを完了してください。") {
            // A locked or previously inaccessible keychain must not be overwritten.
            self.vault = try self.loadVault()
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
            if previous?.tokens == nil {
                let drafts = self.conversations.filter { $0.key.registration == nil && !$0.value.draft.isEmpty }
                for (key, conversation) in drafts {
                    let target = ThreadKey(registration: clientID, problem: key.problem)
                    if self.conversations[target]?.draft.isEmpty != false {
                        self.conversations[target, default: Conversation()].draft = conversation.draft
                        self.conversations.removeValue(forKey: key)
                    }
                }
            }
            self.showPlanWelcome = firstUse
            self.usageLimitReached = false
            self.status = self.canAsk ? "ChatGPT に接続しました。" : "サインインしました。質問するには ChatGPT プランの利用を許可してください。"
        }
    }

    public func signOut() async {
        guard !isBusy, let id = activeAccountID else { return }
        await run(message: "サインアウトしています。") {
            self.clearCatalog()
            // Token rotation must finish before revoking the latest refresh token.
            if let refresh = self.tokenRefreshes[id] { _ = try? await refresh.value }
            guard var profile = self.vault.selected, profile.clientID == id else { throw ChatError.expired }
            var revoked = true
            if let refresh = profile.tokens?.refresh {
                revoked = await OpenAIConnection.revoke(client: profile.clientID, refresh: refresh)
            }
            profile.tokens = nil
            var next = self.vault
            next.save(profile)
            try self.persist(next)
            self.usageLimitReached = false
            self.status = revoked ? "サインアウトしました。" : "この Mac からサインアウトしました。サーバー側の解除は確認できませんでした。ChatGPT の設定で接続を確認してください。"
        }
    }

    /// Capture the question, account and transcript together before starting network work.
    public func send(problemID: String, context: String) {
        let key = threadKey(problemID)
        guard !isBusy, canAsk, let registration = key.registration,
              var thread = conversations[key], !context.isEmpty else { return }
        let question = thread.draft.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !question.isEmpty else { return }
        let input = thread.requestInput(context: context, question: question)
        let displayedModel = selectedModel
        let preferences = modelPreferences
        let index = thread.turns.count
        thread.turns.append(ChatTurn(question: question))
        thread.draft = ""
        conversations[key] = thread
        isBusy = true
        answeringProblemID = problemID
        usageLimitReached = false
        status = ""

        operation = Task {
            defer {
                operation = nil
                answeringProblemID = nil
                isBusy = false
            }
            do {
                let model: ChatModel
                if let displayedModel { model = displayedModel }
                else {
                    let models = try await catalog(for: registration)
                    guard let resolved = preferences.model(in: models) else { throw ChatError.noModels }
                    model = resolved
                }
                try Task.checkCancellation()
                let access = try await accessToken(id: registration)
                try Task.checkCancellation()
                let answer = try await OpenAIConnection.answer(token: access, model: model,
                                                               effort: preferences.effort(for: model),
                                                               input: input, session: inferenceSession) { partial in
                    self.conversations[key]?.turns[index].answer = partial
                }
                try Task.checkCancellation()
                conversations[key]?.turns[index].answer = answer
                conversations[key]?.turns[index].state = .complete
            } catch {
                let message = Task.isCancelled ? "回答を中止しました。" : error.localizedDescription
                conversations[key]?.turns[index].state = .interrupted(message)
                usageLimitReached = (error as? ChatError)?.isUsageLimit ?? false
                if !isSignedIn { status = message }
            }
        }
    }

    private func accessToken(id: String) async throws -> String {
        guard let profile = vault.selected, profile.clientID == id,
              let tokens = profile.tokens, tokens.scopes.contains(OpenAIConnection.sharingScope) else {
            throw ChatError.plan
        }
        if tokens.expires.timeIntervalSinceNow > 60 { return tokens.access }
        if let existing = tokenRefreshes[id] { return try await waitForSharedTask(existing) }
        guard let refresh = tokens.refresh else {
            try endExpiredSession(profile)
            throw ChatError.expired
        }
        let task = Task {
            defer { tokenRefreshes[id] = nil }
            @MainActor func currentProfile() throws -> ChatProfile {
                guard let current = vault.profiles.first(where: { $0.clientID == id }),
                      current.tokens?.access == tokens.access, current.tokens?.refresh == refresh else {
                    throw ChatError.expired
                }
                return current
            }
            do {
                let data = try await OpenAIConnection.token([
                    "grant_type": "refresh_token", "client_id": id, "refresh_token": refresh
                ], session: inferenceSession)
                let renewed = try OpenAIConnection.tokens(from: data, previous: tokens)
                var current = try currentProfile()
                current.tokens = renewed
                var next = vault
                next.save(current)
                try persist(next)
                guard renewed.scopes.contains(OpenAIConnection.sharingScope) else { throw ChatError.plan }
                return renewed.access
            } catch let error as ChatError where error.isTerminalRefresh {
                try endExpiredSession(currentProfile())
                throw ChatError.expired
            }
        }
        tokenRefreshes[id] = task
        return try await waitForSharedTask(task)
    }

    /// Cancel this wait independently of the sidebar's request or a rotating refresh token.
    private func waitForSharedTask<Value: Sendable>(_ task: Task<Value, Error>) async throws -> Value {
        let (results, continuation) = AsyncStream<Result<Value, Error>>.makeStream()
        Task {
            continuation.yield(await task.result)
            continuation.finish()
        }
        for await result in results {
            try Task.checkCancellation()
            return try result.get()
        }
        throw CancellationError()
    }

    private func endExpiredSession(_ profile: ChatProfile) throws {
        var signedOut = profile
        signedOut.tokens = nil
        var next = vault
        next.save(signedOut)
        try persist(next)
        if profile.clientID == activeAccountID { status = ChatError.expired.localizedDescription }
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
        try saveVault(next)
        vault = next
        updateAccounts()
    }

    private func updateAccounts() {
        let previousRegistration = canAsk ? activeAccountID : nil
        activeAccountID = vault.activeID
        accounts = vault.profiles.enumerated().map { index, profile in
            let label = "接続 \(index + 1)" + (profile.email.map { "（\($0)）" } ?? "")
            return ChatAccount(id: profile.clientID, label: label)
        }
        isSignedIn = vault.selected?.tokens != nil
        canAsk = vault.selected?.tokens?.scopes.contains(OpenAIConnection.sharingScope) == true
        modelPreferences = vault.selected?.modelPreferences ?? ChatModelPreferences()
        if previousRegistration != (canAsk ? activeAccountID : nil) { clearCatalog() }
    }
}
