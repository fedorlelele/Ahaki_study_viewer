import ASVChat
import ASVData
import SwiftUI

private let usagePage = URL(string: "https://chatgpt.com/settings/usage")!

struct AccountControls: View {
    @EnvironmentObject private var chat: StudyChat
    var showsWelcome = true

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            if !chat.accounts.isEmpty {
                Picker("アカウント", selection: Binding(get: { chat.activeAccountID ?? "" },
                                                   set: { chat.selectAccount(id: $0) })) {
                    ForEach(chat.accounts) { Text($0.label).tag($0.id) }
                }.disabled(chat.isBusy)
            }
            if chat.canAsk {
                Text("ChatGPTプランを使用中")
            } else {
                Text("質問には、対応するChatGPTプランと利用の許可が必要です。")
                Button("Continue with ChatGPT") { Task { await chat.signIn() } }
                    .buttonStyle(.borderedProminent).disabled(chat.isBusy)
            }
            if !chat.accounts.isEmpty {
                Button("別のアカウントを追加") { Task { await chat.signIn(newAccount: true) } }
                    .disabled(chat.isBusy)
                if chat.isSignedIn {
                    Button("サインアウト") { Task { await chat.signOut() } }.disabled(chat.isBusy)
                }
            }
            if chat.canAsk || chat.usageLimitReached { Link("ChatGPTの利用状況を管理", destination: usagePage) }
            if !chat.status.isEmpty { Text(chat.status).fixedSize(horizontal: false, vertical: true) }
            if chat.isBusy, chat.answeringProblemID == nil { Button("処理を中止") { chat.cancel() } }
        }
        .modifier(PlanWelcome(enabled: showsWelcome))
        .onChange(of: chat.status) { _, status in if !status.isEmpty { announce(status) } }
    }
}

private struct PlanWelcome: ViewModifier {
    @EnvironmentObject private var chat: StudyChat
    let enabled: Bool
    @ViewBuilder func body(content: Content) -> some View {
        if enabled {
            content.alert("ChatGPTプランを使用します", isPresented: $chat.showPlanWelcome) {
                Button("確認") {}
            } message: {
                Text("このアプリでの質問はChatGPTプランの利用量に含まれます。利用状況はChatGPTの設定で確認できます。")
            }
        } else { content }
    }
}

struct StudyChatSidebar: View {
    let problem: ExamQuestion
    let focusRequest: Int
    let close: () -> Void
    @EnvironmentObject private var chat: StudyChat
    @EnvironmentObject private var settings: Preferences
    @EnvironmentObject private var speech: Narrator
    @State private var showAccount = false
    @State private var readingTurn: UUID?
    @State private var followLatest = true
    @State private var previousTop: CGFloat = 0
    @State private var showKeyboardHelp = false

    private var turns: [ChatTurn] { chat.turns(for: problem.id) }
    private var draft: Binding<String> {
        Binding(get: { chat.draft(for: problem.id) }, set: { chat.setDraft($0, for: problem.id) })
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(spacing: 8) {
                VStack(alignment: .leading, spacing: 3) {
                    Text("ChatGPT").font(settings.font(0, bold: true)).accessibilityAddTraits(.isHeader)
                    Text(settings.size > 28 ? problem.id : "\(problem.id)　\(problem.subject)")
                        .font(settings.font(-4)).foregroundStyle(.secondary)
                        .accessibilityLabel("\(problem.id)、\(problem.subject)")
                }
                Spacer(minLength: 0)
                if chat.canAsk {
                    Button { showAccount.toggle() } label: {
                        Label("アカウント", systemImage: "person.crop.circle").labelStyle(.iconOnly)
                    }
                    .help("ChatGPTアカウント")
                    .popover(isPresented: $showAccount) {
                        AccountControls(showsWelcome: false).padding().frame(idealWidth: 300)
                    }
                }
                Button(action: close) {
                    Label("チャットを閉じる", systemImage: "xmark").labelStyle(.iconOnly)
                }.help("チャットを閉じる")
            }.font(.system(size: min(20, settings.size))).padding(12)
            Divider()
            transcript
            Divider()
            composer
        }
        .background(Color(nsColor: .windowBackgroundColor))
        .font(settings.font(-2)).lineSpacing(settings.spacing)
        .modifier(PlanWelcome(enabled: true))
        .onChange(of: turns.last?.state) { old, new in
            guard old == .receiving else { return }
            switch new {
            case .complete: announce("ChatGPTの回答を受信しました。")
            case let .interrupted(message): announce(message)
            default: break
            }
        }
        .onChange(of: problem.id) { _, _ in stopReading() }
        .onChange(of: chat.activeAccountID) { _, _ in stopReading() }
        .onChange(of: speech.isSpeaking) { _, active in if !active { readingTurn = nil } }
        .onDisappear { stopReading() }
    }

    private var transcript: some View {
        GeometryReader { viewport in
            ScrollViewReader { proxy in
                ScrollView {
                    VStack(alignment: .leading, spacing: 16) {
                        if !chat.canAsk { AccountControls(showsWelcome: false) }
                        if turns.isEmpty {
                            Text("送信すると、この問題の本文・選択肢・正答・解説と、この問題についての会話がOpenAIに送られます。")
                                .foregroundStyle(.secondary).font(settings.font(-4))
                        }
                        ForEach(turns) { turn in turnView(turn) }
                        Color.clear.frame(height: 1).id("chatEnd").accessibilityHidden(true)
                    }
                    .padding(12).frame(maxWidth: .infinity, alignment: .leading)
                    .background(GeometryReader { content in
                        Color.clear.preference(key: TranscriptBounds.self,
                                               value: content.frame(in: .named("chatTranscript")))
                    })
                }
                .coordinateSpace(name: "chatTranscript")
                .accessibilityLabel("この問題の会話")
                .onChange(of: turns.last?.id, initial: true) { _, id in
                    followLatest = true
                    if id != nil { proxy.scrollTo("chatEnd", anchor: .bottom) }
                }
                .onPreferenceChange(TranscriptBounds.self) { bounds in
                    if bounds.maxY <= viewport.size.height + 16 {
                        followLatest = true
                    } else if bounds.minY > previousTop + 1 {
                        // Content moved down in the viewport: the reader scrolled up.
                        followLatest = false
                    } else if followLatest, turns.last != nil {
                        proxy.scrollTo("chatEnd", anchor: .bottom)
                    }
                    previousTop = bounds.minY
                }
            }
        }
    }

    private var composer: some View {
        VStack(alignment: .leading, spacing: 7) {
            if chat.canAsk {
                Link(settings.size > 28 ? "プラン利用中" : "ChatGPTプラン使用中", destination: usagePage)
                    .font(settings.font(-6)).accessibilityHint("利用状況を開く")
                ChatModelControl()
            }
            ChatComposer(text: draft, problemID: problem.id,
                         accountID: chat.isSignedIn ? chat.activeAccountID : nil,
                         focusRequest: focusRequest, family: settings.family, size: settings.size, spacing: settings.spacing,
                         send: submit, close: close)
                .frame(height: min(88, settings.size * 2.5 + settings.spacing))
                .background(Color(nsColor: .textBackgroundColor), in: RoundedRectangle(cornerRadius: 7))
                .overlay(RoundedRectangle(cornerRadius: 7).strokeBorder(.secondary))
                .overlay(alignment: .topLeading) {
                    if draft.wrappedValue.isEmpty {
                        Text("質問を入力").font(settings.font()).foregroundStyle(.secondary)
                            .padding(.leading, 9).padding(.top, 5).allowsHitTesting(false).accessibilityHidden(true)
                    }
                }
            if let other = chat.answeringProblemID, other != problem.id {
                Text("\(other) に回答中").font(settings.font(-5))
            }
            ViewThatFits(in: .horizontal) {
                HStack { inputHelp.fixedSize(); Spacer(minLength: 4); sendButton }
                HStack {
                    Button { showKeyboardHelp.toggle() } label: {
                        Label("入力キーの使い方", systemImage: "keyboard").labelStyle(.iconOnly)
                    }.popover(isPresented: $showKeyboardHelp) { inputHelp.padding() }
                    Spacer()
                    sendButton
                }
            }
        }.padding(12)
    }

    private var inputHelp: some View {
        Text("Enterで送信・Shift＋Enterで改行")
            .foregroundStyle(.secondary).font(settings.font(-6))
    }

    @ViewBuilder private var sendButton: some View {
        if chat.answeringProblemID == problem.id {
            Button("中止") { chat.cancel() }.accessibilityLabel("回答を中止")
        } else {
            Button("送信", action: submit).accessibilityLabel("質問を送信")
                .buttonStyle(.borderedProminent)
                .disabled(!chat.canAsk || chat.isBusy || draft.wrappedValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        }
    }

    private func turnView(_ turn: ChatTurn) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            VStack(alignment: .leading, spacing: 5) {
                Text("あなた").font(settings.font(-4, bold: true)).accessibilityAddTraits(.isHeader)
                Text(verbatim: turn.question).font(settings.font()).textSelection(.enabled)
            }
            .padding(10).frame(maxWidth: .infinity, alignment: .leading)
            .background(Color.accentColor.opacity(0.11), in: RoundedRectangle(cornerRadius: 9))
            HStack {
                Text("ChatGPT").font(settings.font(-4, bold: true)).accessibilityAddTraits(.isHeader)
                Spacer()
                if turn.state == .complete {
                    let speaking = readingTurn == turn.id && speech.isSpeaking
                    Button {
                        if speaking { stopReading() }
                        else {
                            speech.read(ChatMarkdownView.spokenText(turn.answer), rate: settings.rate)
                            readingTurn = turn.id
                        }
                    } label: {
                        Label(speaking ? "読み上げを停止" : "回答を読み上げる",
                              systemImage: speaking ? "stop.circle" : "speaker.wave.2").labelStyle(.iconOnly)
                    }
                }
            }
            if !turn.answer.isEmpty { ChatMarkdownView(text: turn.answer) }
            switch turn.state {
            case .receiving:
                if turn.answer.isEmpty { ProgressView("回答を作成中").controlSize(.small) }
            case let .interrupted(message):
                Text(message)
                Button("質問を入力欄に戻す") { draft.wrappedValue = turn.question }.disabled(!draft.wrappedValue.isEmpty)
            case .complete: EmptyView()
            }
        }
    }

    private func stopReading() {
        if readingTurn != nil { speech.stop(); readingTurn = nil }
    }

    private func submit() {
        guard !draft.wrappedValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        if !chat.canAsk { announce("ChatGPTプランの利用を許可してから送信してください。"); return }
        guard !chat.isBusy else { return }
        stopReading()
        chat.send(problemID: problem.id, context: problem.questionText + "\n正答 " + problem.solutionText)
    }
}


private struct TranscriptBounds: PreferenceKey {
    static let defaultValue = CGRect.zero
    static func reduce(value: inout CGRect, nextValue: () -> CGRect) { value = nextValue() }
}
