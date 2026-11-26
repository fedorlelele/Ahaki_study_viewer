import AppKit
import SwiftUI

enum AppearanceMode: String, CaseIterable {
    case system = "システムに従う", light = "ライト", dark = "ダーク"

    var appearance: NSAppearance? {
        switch self {
        case .system: return nil
        case .light: return NSAppearance(named: .aqua)
        case .dark: return NSAppearance(named: .darkAqua)
        }
    }
}

enum ReadingFont: String, CaseIterable {
    case system = "システム標準", gothic = "ヒラギノ角ゴシック", mincho = "ヒラギノ明朝"

    func font(size: Double, bold: Bool = false) -> Font {
        switch self {
        case .system: return .system(size: size, weight: bold ? .semibold : .regular)
        case .gothic: return .custom(bold ? "HiraginoSans-W6" : "HiraginoSans-W3", size: size)
        case .mincho: return .custom(bold ? "HiraMinProN-W6" : "HiraMinProN-W3", size: size)
        }
    }
}

enum StudyAction: String, CaseIterable, Identifiable, Codable {
    case search, results, previous, next, choice1, choice2, choice3, choice4, retry, read, silence, ask
    var id: Self { self }

    var title: String {
        switch self {
        case .search: return "検索欄へ移動"
        case .results: return "検索結果へ移動"
        case .previous: return "前の問題"
        case .next: return "次の問題"
        case .choice1, .choice2, .choice3, .choice4: return "選択肢\(choiceNumber!)で回答"
        case .retry: return "もう一度解く"
        case .read: return "問題を読み上げる"
        case .silence: return "読み上げを停止"
        case .ask: return "この問題について質問"
        }
    }

    var choiceNumber: Int? {
        switch self {
        case .choice1: return 1
        case .choice2: return 2
        case .choice3: return 3
        case .choice4: return 4
        default: return nil
        }
    }

    var standard: KeyChord {
        switch self {
        case .search: return KeyChord("f")
        case .results: return KeyChord("j")
        case .previous: return KeyChord("[")
        case .next: return KeyChord("]")
        case .choice1, .choice2, .choice3, .choice4: return KeyChord(String(choiceNumber!))
        case .retry: return KeyChord("x", .commandShift)
        case .read: return KeyChord("r")
        case .silence: return KeyChord("r", .commandShift)
        case .ask: return KeyChord("a", .commandShift)
        }
    }
}

enum ChordModifier: String, CaseIterable, Codable {
    case command = "⌘", commandShift = "⇧⌘", commandOption = "⌥⌘"
    var flags: EventModifiers {
        switch self {
        case .command: return .command
        case .commandShift: return [.command, .shift]
        case .commandOption: return [.command, .option]
        }
    }
    var spoken: String {
        switch self {
        case .command: return "コマンド"
        case .commandShift: return "シフト・コマンド"
        case .commandOption: return "オプション・コマンド"
        }
    }
}

struct KeyChord: Codable, Equatable {
    let key: String
    let modifier: ChordModifier

    init(_ key: String, _ modifier: ChordModifier = .command) {
        self.key = key.lowercased()
        self.modifier = modifier
    }
    var label: String { modifier.rawValue + key.uppercased() }
    var spoken: String { modifier.spoken + " " + key.uppercased() }
    var isValid: Bool {
        guard key.count == 1, "abcdefghijklmnopqrstuvwxyz0123456789[]".contains(key) else { return false }
        let reserved: [ChordModifier: String] = [
            .command: "acghmnoqstvwxyz", .commandShift: "ghnpqstwz", .commandOption: "dfhimntvw"
        ]
        return !(reserved[modifier] ?? "").contains(key)
    }
}

@MainActor final class Preferences: ObservableObject {
    let defaults: UserDefaults
    @Published var appearance: AppearanceMode { didSet { defaults.set(appearance.rawValue, forKey: "appearance"); applyAppearance() } }
    @Published var family: ReadingFont { didSet { defaults.set(family.rawValue, forKey: "family") } }
    @Published var size: Double { didSet { defaults.set(size, forKey: "size") } }
    @Published var spacing: Double { didSet { defaults.set(spacing, forKey: "spacing") } }
    @Published var rate: Double { didSet { defaults.set(rate, forKey: "rate") } }
    @Published var autoRead: Bool { didSet { defaults.set(autoRead, forKey: "autoRead") } }
    @Published var shortcutsEnabled: Bool { didSet { defaults.set(shortcutsEnabled, forKey: "shortcutsEnabled") } }
    @Published private var overrides: [String: KeyChord] { didSet { defaults.set(try? JSONEncoder().encode(overrides), forKey: "shortcuts") } }

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        defaults.register(defaults: ["size": 20.0, "spacing": 5.0, "rate": 0.5, "shortcutsEnabled": true])
        appearance = AppearanceMode(rawValue: defaults.string(forKey: "appearance") ?? "") ?? .system
        family = ReadingFont(rawValue: defaults.string(forKey: "family") ?? "") ?? .system
        size = min(40, max(14, defaults.double(forKey: "size")))
        spacing = min(18, max(0, defaults.double(forKey: "spacing")))
        rate = min(0.65, max(0.35, defaults.double(forKey: "rate")))
        autoRead = defaults.bool(forKey: "autoRead")
        shortcutsEnabled = defaults.bool(forKey: "shortcutsEnabled")
        overrides = defaults.data(forKey: "shortcuts").flatMap { try? JSONDecoder().decode([String: KeyChord].self, from: $0) } ?? [:]
    }

    func applyAppearance() { NSApp.appearance = appearance.appearance }
    func font(_ offset: Double = 0, bold: Bool = false) -> Font { family.font(size: max(13, size + offset), bold: bold) }
    func chord(_ action: StudyAction) -> KeyChord { overrides[action.rawValue] ?? action.standard }

    func assign(_ chord: KeyChord, to action: StudyAction) -> String? {
        guard chord.isValid else { return "英数字・角括弧と、macOSの標準操作に使われていない組み合わせを選んでください。" }
        if let other = StudyAction.allCases.first(where: { $0 != action && self.chord($0) == chord }) {
            return "このキーは「\(other.title)」に割り当てられています。"
        }
        overrides[action.rawValue] = chord
        return nil
    }
    func resetShortcuts() { overrides.removeAll() }
}
