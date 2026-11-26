import XCTest
@testable import ASVMac

final class PreferencesTests: XCTestCase {
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
