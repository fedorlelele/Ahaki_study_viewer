import AppKit
import ASVChat
import ASVUpdate
import SwiftUI

@main
@MainActor struct ASVMacApp: App {
    @StateObject private var settings = Preferences()
    @StateObject private var speech = Narrator()
    @StateObject private var chat = StudyChat()
    @StateObject private var updates = UpdateController()

    var body: some Scene {
        Window("Ahaki Study Viewer", id: "study") {
            StudyWindow().environmentObject(settings).environmentObject(speech).environmentObject(chat)
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
            PreferencesView().environmentObject(settings).environmentObject(chat)
        }
    }
}

private struct StudyCommands: Commands {
    @ObservedObject var settings: Preferences
    @FocusedValue(\.studyCommands) private var context

    var body: some Commands {
        CommandMenu("学習") {
            ForEach(StudyAction.allCases) { action in
                if [.previous, .choice1, .retry, .read, .ask].contains(action) { Divider() }
                let chord = settings.chord(action)
                let button = Button(action.title) { context?.run(action) }
                    .disabled(context?.available(action) != true)
                if settings.shortcutsEnabled, let char = chord.key.first {
                    button.keyboardShortcut(KeyEquivalent(char), modifiers: chord.modifier.flags)
                } else { button }
            }
        }
    }
}
