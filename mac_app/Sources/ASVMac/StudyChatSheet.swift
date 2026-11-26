import ASVChat
import ASVData
import SwiftUI

private let usagePage = URL(string: "https://chatgpt.com/settings/usage")!

struct AccountControls: View {
    @EnvironmentObject private var chat: StudyChat

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
                HStack {
                    Button("別のアカウントを追加") { Task { await chat.signIn(newAccount: true) } }
                    if chat.isSignedIn { Button("サインアウト") { Task { await chat.signOut() } } }
                }.disabled(chat.isBusy)
            }
            if chat.canAsk || chat.usageLimitReached { Link("ChatGPTの利用状況を管理", destination: usagePage) }
            if !chat.status.isEmpty { Text(chat.status).fixedSize(horizontal: false, vertical: true) }
            if chat.isBusy { Button("処理を中止") { chat.cancel() } }
        }
        .alert("ChatGPTプランを使用します", isPresented: $chat.showPlanWelcome) {
            Button("確認") {}
        } message: {
            Text("このアプリでの質問はChatGPTプランの利用量に含まれます。利用状況はChatGPTの設定で確認できます。")
        }
        .onChange(of: chat.status) { _, status in if !status.isEmpty { announce(status) } }
    }
}

struct StudyChatSheet: View {
    let problem: ExamQuestion
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject private var chat: StudyChat
    @EnvironmentObject private var settings: Preferences
    @EnvironmentObject private var speech: Narrator
    @State private var question = ""
    @FocusState private var inputFocused: Bool
    @AccessibilityFocusState private var answerFocused: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack {
                Text("この問題について質問").font(settings.font(2, bold: true)).accessibilityAddTraits(.isHeader)
                Spacer()
                Button("閉じる") { dismiss() }.keyboardShortcut(.cancelAction)
            }.padding(20)
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 14) {
                    Text("\(problem.id)　\(problem.subject)").font(settings.font(-3, bold: true))
                    Text(problem.stem)
                    AccountControls().font(settings.font(-4))
                    Divider()
                    Text("質問").font(settings.font(0, bold: true)).accessibilityAddTraits(.isHeader)
                    TextField("例：選択肢2が誤りである理由を教えて", text: $question, axis: .vertical)
                        .lineLimit(2...6).textFieldStyle(.roundedBorder)
                        .accessibilityLabel("質問の内容").focused($inputFocused)
                    Text("送信すると、この問題の本文・選択肢・正答・解説と、入力した質問がOpenAIに送られます。")
                        .font(settings.font(-4)).foregroundStyle(.secondary)
                    Button("質問を送信") { submit() }
                        .keyboardShortcut(.return, modifiers: .command)
                        .buttonStyle(.borderedProminent).disabled(!canSubmit)
                    if !chat.reply.isEmpty {
                        Divider()
                        Text("ChatGPTの回答").font(settings.font(0, bold: true))
                            .accessibilityAddTraits(.isHeader).accessibilityFocused($answerFocused)
                        Text(chat.reply).textSelection(.enabled)
                        Button(speech.isSpeaking ? "読み上げを停止" : "回答を読み上げる") {
                            if speech.isSpeaking { speech.stop() } else { speech.read(chat.reply, rate: settings.rate) }
                        }
                    }
                }.padding(20).frame(maxWidth: .infinity, alignment: .leading)
            }
        }.font(settings.font()).lineSpacing(settings.spacing)
         .frame(minWidth: 420, idealWidth: 590, minHeight: 350, idealHeight: 560)
         .onAppear { chat.clearReply(); inputFocused = chat.canAsk }
         .onDisappear { chat.cancel(); speech.stop(); chat.clearReply() }
    }

    private var canSubmit: Bool {
        chat.canAsk && !chat.isBusy && !question.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }
    private func submit() {
        guard canSubmit else { return }
        speech.stop()
        let context = problem.questionText + "\n正答 " + problem.solutionText
        Task {
            await chat.ask(context: context, question: question)
            if !chat.reply.isEmpty {
                answerFocused = true
                announce("ChatGPTの回答が届きました。")
            }
        }
    }
}
