import AppKit
import ASVChat
import ASVUpdate
import SwiftUI

@main
@MainActor struct ASVMacApp: App {
    @StateObject private var study = StudySession()
    @StateObject private var settings = Preferences()
    @StateObject private var speech = Narrator()
    @StateObject private var chat = StudyChat()
    @StateObject private var updates = UpdateController()

    var body: some Scene {
        Window("Ahaki Study Viewer", id: "study") {
            StudyWindow().environmentObject(study).environmentObject(settings).environmentObject(speech).environmentObject(chat)
                .frame(minWidth: 680, minHeight: 440)
        }
        .defaultSize(width: 1080, height: 740)
        .commands {
            CommandGroup(after: .appInfo) {
                Button("アップデートを確認…", action: updates.checkForUpdates)
                    .disabled(!updates.canCheckForUpdates)
            }
            StudyCommands(settings: settings)
        }
        Settings {
            PreferencesView().environmentObject(study).environmentObject(settings).environmentObject(chat).environmentObject(speech)
        }
    }
}

private struct StudyCommands: Commands {
    @ObservedObject var settings: Preferences
    @FocusedValue(\.studyCommands) private var context

    var body: some Commands {
        CommandGroup(after: .toolbar) {
            ForEach(StudyAction.allCases.filter(\.isTextSizeAction)) { action in
                let larger = action == .largerText
                command(action, enabled: settings.canResizeText(larger: larger)) {
                    settings.resizeText(larger: larger)
                    announce("文字サイズ \(Int(settings.size))ポイント")
                }
            }
        }
        CommandMenu("学習") {
            ForEach(StudyAction.allCases.filter { !$0.isTextSizeAction }) { action in
                if [.previous, .choice1, .retry, .read, .ask].contains(action) { Divider() }
                command(action, enabled: context?.available(action) == true) { context?.run(action) }
            }
        }
    }

    @ViewBuilder
    private func command(_ action: StudyAction, enabled: Bool, perform: @escaping () -> Void) -> some View {
        let button = Button(action.title, action: perform).disabled(!enabled)
        let chord = settings.chord(action)
        if settings.shortcutsEnabled, let char = chord.key.first {
            button.keyboardShortcut(KeyEquivalent(char), modifiers: chord.modifier.flags)
        } else { button }
    }
}
