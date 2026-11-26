import Foundation

public struct ChatTurn: Identifiable, Equatable {
    public enum State: Equatable {
        case receiving
        case complete
        case interrupted(String)
    }

    public let id = UUID()
    public let question: String
    public internal(set) var answer = ""
    public internal(set) var state = State.receiving
}

/// A registration belongs to a particular ChatGPT account and workspace.
struct ThreadKey: Hashable {
    let registration: String?
    let problem: String
}

struct Conversation {
    var draft = ""
    var turns: [ChatTurn] = []

    func requestInput(context: String, question: String) -> [[String: String]] {
        var input = [["role": "user", "content": "【国家試験問題と解説】\n" + context]]
        for turn in turns where turn.state == .complete {
            input.append(["role": "user", "content": turn.question])
            input.append(["role": "assistant", "content": turn.answer])
        }
        input.append(["role": "user", "content": question])
        return input
    }
}
