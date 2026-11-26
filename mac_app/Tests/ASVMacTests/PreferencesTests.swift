import XCTest
@testable import ASVMac

final class PreferencesTests: XCTestCase {
    @MainActor func testTextSizeShortcutsClampAndUseTheSavedReadingSize() throws {
        let suite = "ASV.Tests." + UUID().uuidString
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let settings = Preferences(defaults: defaults)
        XCTAssertEqual(settings.chord(.largerText), KeyChord("＋"))
        XCTAssertEqual(settings.chord(.smallerText), KeyChord("−"))
        settings.size = 39
        settings.resizeText(larger: true)
        XCTAssertEqual(settings.size, 40)
        XCTAssertFalse(settings.canResizeText(larger: true))
        settings.size = 15
        settings.resizeText(larger: false)
        XCTAssertEqual(settings.size, 14)
        XCTAssertFalse(settings.canResizeText(larger: false))
        settings.resizeText(larger: true)
        XCTAssertEqual(Preferences(defaults: defaults).size, 16)
        XCTAssertNotNil(settings.assign(KeyChord("+"), to: .search))
    }

    @MainActor func testAssignmentsPersistWithoutReplacingAnotherActionOrMacCommand() throws {
        let suite = "ASV.Tests." + UUID().uuidString
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let settings = Preferences(defaults: defaults)
        XCTAssertTrue(StudyAction.allCases.allSatisfy { $0.standard.isValid })
        XCTAssertNil(settings.assign(KeyChord("K", .commandOption), to: .read))
        XCTAssertNotNil(settings.assign(KeyChord("k", .commandOption), to: .ask))
        XCTAssertNotNil(settings.assign(KeyChord("w"), to: .read))
        XCTAssertNotNil(settings.assign(KeyChord(""), to: .read))
        settings.size = 32
        settings.spacing = 12
        settings.shortcutsEnabled = false

        let restored = Preferences(defaults: defaults)
        XCTAssertEqual(restored.chord(.read), KeyChord("k", .commandOption))
        XCTAssertEqual(restored.chord(.ask), StudyAction.ask.standard)
        XCTAssertEqual(restored.size, 32)
        XCTAssertEqual(restored.spacing, 12)
        XCTAssertFalse(restored.shortcutsEnabled)
        restored.resetShortcuts()
        XCTAssertEqual(restored.chord(.read), StudyAction.read.standard)
    }
}
