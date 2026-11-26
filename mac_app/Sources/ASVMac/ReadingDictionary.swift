import CoreFoundation
import Foundation

/// PC-Talker Jukugo.usr contains one unquoted "term,pronunciation" pair per line.
struct ReadingDictionary: Sendable {
    private struct Node: Sendable {
        var children = [Character: Int]()
        var reading: String?
    }

    private var nodes = [Node()]
    private(set) var count = 0

    init(data: Data) throws {
        let cp932 = String.Encoding(rawValue: CFStringConvertEncodingToNSStringEncoding(
            CFStringEncoding(CFStringEncodings.dosJapanese.rawValue)))
        let text: String?
        if data.starts(with: [0xFF, 0xFE]) || data.starts(with: [0xFE, 0xFF]) {
            text = String(data: data, encoding: .utf16)
        } else {
            text = String(data: data, encoding: .utf8) ?? String(data: data, encoding: cp932)
        }
        guard let text else { throw DictionaryError.encoding }
        for (index, raw) in text.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline).enumerated() {
            let line = raw.trimmingCharacters(in: .whitespaces)
            if line.isEmpty { continue }
            let fields = line.split(separator: ",", omittingEmptySubsequences: false)
            guard fields.count == 2 else { throw DictionaryError.row(index + 1) }
            let term = fields[0].trimmingCharacters(in: .whitespaces.union(CharacterSet(charactersIn: "\u{feff}")))
            let reading = fields[1].trimmingCharacters(in: .whitespaces)
                .replacingOccurrences(of: "　", with: " ")
            guard !term.isEmpty, !reading.isEmpty else { throw DictionaryError.row(index + 1) }
            var node = 0
            for character in term {
                if let existing = nodes[node].children[character] {
                    node = existing
                } else {
                    let next = nodes.count
                    nodes[node].children[character] = next
                    nodes.append(Node())
                    node = next
                }
            }
            if nodes[node].reading == nil { count += 1 }
            nodes[node].reading = reading
        }
        guard count > 0 else { throw DictionaryError.empty }
    }

    func pronunciations(in text: String) -> String {
        let characters = Array(text)
        var result = ""
        result.reserveCapacity(text.utf8.count)
        var start = 0
        while start < characters.count {
            var node = 0
            var cursor = start
            var match: (reading: String, end: Int)?
            let before = start > 0 ? characters[start - 1] : nil
            while cursor < characters.count, let next = nodes[node].children[characters[cursor]] {
                node = next
                cursor += 1
                guard let reading = nodes[node].reading else { continue }
                let after = cursor < characters.count ? characters[cursor] : nil
                let first = characters[start]
                let last = characters[cursor - 1]

                // e.g. an acronym in the middle of an English word isn't a term.
                if first.isLatinOrDigit && before?.isLatinOrDigit == true { continue }
                if last.isLatinOrDigit && after?.isLatinOrDigit == true { continue }
                // Avoid forcing a single kanji's reading inside an unknown compound.
                if cursor == start + 1 && first.isIdeograph &&
                    (before?.isIdeograph == true || after?.isIdeograph == true) { continue }
                match = (reading, cursor)
            }
            if let match {
                result += match.reading
                start = match.end
            } else {
                result.append(characters[start])
                start += 1
            }
        }
        return result
    }

    static var installedURL: URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("io.github.fedorlelele.ASV", isDirectory: true)
            .appendingPathComponent("Jukugo.usr")
    }

    static func loadInstalled(from url: URL = installedURL) throws -> Self? {
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        return try Self(data: Data(contentsOf: url))
    }

    static func install(from source: URL, at destination: URL = installedURL) throws -> Self {
        let data = try Data(contentsOf: source)
        let dictionary = try Self(data: data)
        try FileManager.default.createDirectory(at: destination.deletingLastPathComponent(), withIntermediateDirectories: true)
        try data.write(to: destination, options: .atomic)
        return dictionary
    }

    private enum DictionaryError: LocalizedError {
        case encoding, row(Int), empty

        var errorDescription: String? {
            switch self {
            case .encoding: return "読み辞書の文字コードを認識できませんでした。"
            case .row(let line): return "読み辞書の\(line)行目で見出し語と読みを認識できませんでした。"
            case .empty: return "読み辞書に見出し語が含まれていません。"
            }
        }
    }
}

private extension Character {
    var isLatinOrDigit: Bool {
        guard let value = unicodeScalars.first?.value else { return false }
        switch value {
        case 48...57, 65...90, 97...122, 0xFF10...0xFF19, 0xFF21...0xFF3A, 0xFF41...0xFF5A: return true
        default: return false
        }
    }

    var isIdeograph: Bool {
        unicodeScalars.first?.properties.isIdeographic == true
    }
}
