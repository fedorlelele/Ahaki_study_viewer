import SwiftUI

struct PreferencesView: View {
    @EnvironmentObject private var settings: Preferences
    @State private var editing: StudyAction?

    var body: some View {
        TabView {
            Form {
                Picker("外観", selection: $settings.appearance) {
                    ForEach(AppearanceMode.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                }
                Picker("フォント", selection: $settings.family) {
                    ForEach(ReadingFont.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                }
                Stepper("文字サイズ　\(Int(settings.size)) pt", value: $settings.size, in: 14...40, step: 1)
                Stepper("行間　\(Int(settings.spacing)) pt", value: $settings.spacing, in: 0...18, step: 1)
                Section("表示見本") {
                    Text("問題を読んで、選択肢から答えを選びます。\n回答すると、正答と解説が表示されます。")
                        .font(settings.font()).lineSpacing(settings.spacing).fixedSize(horizontal: false, vertical: true)
                }
            }.formStyle(.grouped).tabItem { Label("表示", systemImage: "textformat.size") }
            Form {
                Toggle("問題の切り替え時と回答時に自動で読み上げる", isOn: $settings.autoRead)
                Slider(value: $settings.rate, in: 0.35...0.65, step: 0.025) { Text("読み上げ速度") }
                    .accessibilityValue("\(Int((settings.rate / 0.5 * 100).rounded()))パーセント")
                Text("読み上げ速度　\(Int((settings.rate / 0.5 * 100).rounded()))%")
                Text("日本語の音声を使用します。学習メニューから手動で読み上げ・停止もできます。")
            }.formStyle(.grouped).tabItem { Label("読み上げ", systemImage: "speaker.wave.2") }
            Form {
                Toggle("学習のショートカットを使う", isOn: $settings.shortcutsEnabled)
                Section {
                    ForEach(StudyAction.allCases) { action in
                        HStack {
                            Text(action.title)
                            Spacer()
                            Button(settings.chord(action).label) { editing = action }
                                .accessibilityLabel("\(action.title)の割り当てを変更")
                                .accessibilityValue(settings.chord(action).spoken)
                        }
                    }
                }
                Button("標準の割り当てに戻す") { settings.resetShortcuts(); announce("標準の割り当てに戻しました。") }
            }.formStyle(.grouped).tabItem { Label("キーボード", systemImage: "keyboard") }
            Form {
                Section("ChatGPTとの接続") { AccountControls() }
            }.formStyle(.grouped).tabItem { Label("ChatGPT", systemImage: "bubble.left.and.bubble.right") }
        }
        .padding(10).frame(width: 540, height: 460)
        .sheet(item: $editing) { action in ShortcutEditor(action: action, chord: settings.chord(action)) }
    }
}

private struct ShortcutEditor: View {
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject private var settings: Preferences
    let action: StudyAction
    @State private var modifier: ChordModifier
    @State private var key: String
    @State private var error: String?

    init(action: StudyAction, chord: KeyChord) {
        self.action = action
        _modifier = State(initialValue: chord.modifier)
        _key = State(initialValue: chord.key)
    }
    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(action.title).font(.title2).accessibilityAddTraits(.isHeader)
            Picker("修飾キー", selection: $modifier) {
                ForEach(ChordModifier.allCases, id: \.self) { Text($0.spoken).tag($0) }
            }
            TextField("キー（英数字または角括弧1文字）", text: $key)
            if let error { Text(error).foregroundStyle(.primary) }
            HStack {
                Spacer()
                Button("キャンセル") { dismiss() }.keyboardShortcut(.cancelAction)
                Button("保存") {
                    error = settings.assign(KeyChord(key, modifier), to: action)
                    if let error { announce(error) } else { dismiss() }
                }.keyboardShortcut(.defaultAction)
            }
        }.padding(24).frame(width: 410)
    }
}
