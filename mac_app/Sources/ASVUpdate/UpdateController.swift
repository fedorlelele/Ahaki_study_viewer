import AppKit
import Combine
import Sparkle

@MainActor
public final class UpdateController: ObservableObject {
    private let controller: SPUStandardUpdaterController?
    private let feedURL: URL?
    private var observation: AnyCancellable?
    @Published private var sparkleReady = false
    @Published private var checkingFeed = false

    public var canCheckForUpdates: Bool {
        !checkingFeed && (controller == nil || sparkleReady)
    }

    public init() {
        feedURL = (Bundle.main.object(forInfoDictionaryKey: "SUFeedURL") as? String)
            .flatMap(URL.init(string:))
        guard Bundle.main.bundleURL.pathExtension == "app", feedURL != nil else {
            controller = nil
            return
        }
        let controller = SPUStandardUpdaterController(
            startingUpdater: true, updaterDelegate: nil, userDriverDelegate: nil
        )
        self.controller = controller
        observation = controller.updater.publisher(for: \.canCheckForUpdates)
            .sink { [weak self] in self?.sparkleReady = $0 }
    }

    public func checkForUpdates() {
        guard canCheckForUpdates else { return }
        guard let controller, let feedURL else {
            alert("アップデートの確認", "作成した ASV.app を開き、もう一度お試しください。")
            return
        }
        checkingFeed = true
        Task {
            defer { checkingFeed = false }
            var request = URLRequest(url: feedURL, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 15)
            request.httpMethod = "HEAD"
            do {
                let (_, response) = try await URLSession.shared.data(for: request)
                // GitHub returns 404 until the first Mac release is published.
                if (response as? HTTPURLResponse)?.statusCode == 404 {
                    alert("アップデートの確認", "現在、公開されているMac版の更新情報はありません。")
                    return
                }
                controller.checkForUpdates(nil)
            } catch {
                alert("更新情報を確認できませんでした", "インターネット接続を確認して、もう一度お試しください。\n\(error.localizedDescription)")
            }
        }
    }

    private func alert(_ title: String, _ message: String) {
        let alert = NSAlert()
        alert.messageText = title
        alert.informativeText = message
        alert.addButton(withTitle: "閉じる")
        alert.runModal()
    }
}
