import Foundation
import XCTest
@testable import ASVMac

final class ReadingDictionaryTests: XCTestCase {
    func testWindowsDictionaryPreservesNewlinesAndUsesLongestReadingOnce() throws {
        let rows = "上腕,ジョウワン\r\n上腕二頭筋,ジョウワン　ニトウキン\r\nニトウキン,サイチカン\r\n経穴,ケイケツ\r\n"
        let data = try XCTUnwrap(rows.data(using: .shiftJIS))
        let dictionary = try ReadingDictionary(data: data)
        XCTAssertEqual(dictionary.count, 4)
        XCTAssertEqual(dictionary.pronunciations(in: "上腕二頭筋と経穴。\n上腕"),
                       "ジョウワン ニトウキンとケイケツ。\nジョウワン")
    }

    func testEnglishWordBoundariesSingleKanjiAndDuplicateRows() throws {
        let rows = "RICE,アールアイシーイー\nRICE,ライス\n穴,ケツ\n"
        let dictionary = try ReadingDictionary(data: Data(rows.utf8))
        XCTAssertEqual(dictionary.count, 2)
        XCTAssertEqual(dictionary.pronunciations(in: "RICE・PRICE・RICED、穴と穴埋め、空穴"),
                       "ライス・PRICE・RICED、ケツと穴埋め、空穴")
    }

    func testInvalidReplacementKeepsPreviouslyImportedFile() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let source = dir.appendingPathComponent("export.usr")
        let saved = dir.appendingPathComponent("support/Jukugo.usr")
        try Data("経絡,ケイラク\r\n".utf8).write(to: source)
        XCTAssertEqual(try ReadingDictionary.install(from: source, at: saved).count, 1)
        try Data("壊れた行".utf8).write(to: source)
        XCTAssertThrowsError(try ReadingDictionary.install(from: source, at: saved))
        let installed = try XCTUnwrap(ReadingDictionary.loadInstalled(from: saved))
        XCTAssertEqual(installed.pronunciations(in: "経絡"), "ケイラク")
    }
}
