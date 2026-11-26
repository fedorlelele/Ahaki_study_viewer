import ASVData
import SwiftUI

struct QuestionReader: View {
    enum Anchor: Hashable { case choice(Int), result }

    @EnvironmentObject private var settings: Preferences
    @EnvironmentObject private var speech: Narrator
    @FocusState private var keyboardFocus: Anchor?
    @AccessibilityFocusState private var accessibilityFocus: Anchor?
    let problem: ExamQuestion
    let choice: Int?
    var headingFocused: AccessibilityFocusState<Bool>.Binding
    let respond: (Int) -> Void, retry: () -> Void, read: () -> Void, ask: () -> Void
    let reveal: (Anchor) -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            ViewThatFits(in: .horizontal) {
                HStack { metadata; Spacer(); audioButton }
                VStack(alignment: .leading, spacing: 10) { metadata; audioButton }
            }
            if let context = problem.caseText, !context.isEmpty {
                Text(context).textSelection(.enabled)
                    .padding(14).frame(maxWidth: .infinity, alignment: .leading)
                    .background(.quaternary.opacity(0.5), in: RoundedRectangle(cornerRadius: 8))
            }
            Text(problem.stem).font(settings.font(5, bold: true)).textSelection(.enabled)
                .accessibilityAddTraits(.isHeader).accessibilityFocused(headingFocused)
            VStack(alignment: .leading, spacing: 9) {
                ForEach(Array(problem.choices.enumerated()), id: \.offset) { index, text in
                    let number = index + 1
                    if choice == nil {
                        Button { respond(number) } label: { choiceRow(number, text) }
                            .buttonStyle(ChoiceButtonStyle())
                            .accessibilityLabel("選択肢\(number)。\(text)")
                            .focused($keyboardFocus, equals: .choice(number))
                            .accessibilityFocused($accessibilityFocus, equals: .choice(number))
                            .id(Anchor.choice(number))
                    } else {
                        choiceRow(number, text)
                            .background(.quaternary.opacity(0.4), in: RoundedRectangle(cornerRadius: 8))
                            .accessibilityElement(children: .combine)
                    }
                }
            }
            if let choice {
                Divider()
                VStack(alignment: .leading, spacing: 10) {
                    Text(problem.feedback(for: choice)).font(settings.font(2, bold: true))
                        .focusable().focused($keyboardFocus, equals: .result)
                        .accessibilityAddTraits(.isHeader)
                        .accessibilityLabel("\(problem.feedback(for: choice))。正答は\(problem.answerLabel)です。")
                        .accessibilityFocused($accessibilityFocus, equals: .result)
                        .id(Anchor.result)
                    Text("正答　\(problem.answerLabel)").font(settings.font(0, bold: true))
                    if !problem.answerNote.isEmpty { Text(problem.answerNote) }
                    if !problem.explanation.isEmpty {
                        Text("解説").font(settings.font(-2, bold: true)).accessibilityAddTraits(.isHeader)
                        Text(problem.explanation).textSelection(.enabled)
                    }
                    Button("もう一度解く", action: retry)
                }
            }
            Divider()
            Button(action: ask) { Label("この問題について質問", systemImage: "bubble.left.and.bubble.right") }
                .font(settings.font(-3))
        }.font(settings.font()).lineSpacing(settings.spacing)
        .onChange(of: choice) { _, selection in
            let target: Anchor = selection == nil ? .choice(1) : .result
            reveal(target)
            keyboardFocus = target
            accessibilityFocus = target
        }
    }

    private var metadata: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("\(problem.id)　\(problem.subject)").font(settings.font(-2, bold: true))
            Text("第\(problem.examSession)回　\(problem.examType)").font(settings.font(-5)).foregroundStyle(.secondary)
        }
    }
    private var audioButton: some View {
        Button {
            if speech.isSpeaking { speech.stop() } else { read() }
        } label: {
            Label(speech.isSpeaking ? "読み上げを停止" : "読み上げる",
                  systemImage: speech.isSpeaking ? "stop.circle" : "speaker.wave.2")
        }.font(settings.font(-4))
    }
    private func choiceRow(_ number: Int, _ text: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 12) {
            Text("\(number)").monospacedDigit().fontWeight(.semibold)
            VStack(alignment: .leading) {
                Text(text).fixedSize(horizontal: false, vertical: true)
                if choice == number { Text("あなたの回答").font(settings.font(-4, bold: true)) }
            }
            Spacer(minLength: 2)
        }.multilineTextAlignment(.leading)
         .padding(.horizontal, 15).padding(.vertical, 10)
         .frame(maxWidth: .infinity, alignment: .leading).contentShape(Rectangle())
    }
}

private struct ChoiceButtonStyle: ButtonStyle {
    @Environment(\.isFocused) private var focused
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .background(configuration.isPressed ? Color.accentColor.opacity(0.18) : Color(nsColor: .controlBackgroundColor),
                        in: RoundedRectangle(cornerRadius: 8))
            .overlay(RoundedRectangle(cornerRadius: 8).strokeBorder(focused ? Color.accentColor : .secondary.opacity(0.6),
                                                                  lineWidth: focused ? 3 : 1))
    }
}
