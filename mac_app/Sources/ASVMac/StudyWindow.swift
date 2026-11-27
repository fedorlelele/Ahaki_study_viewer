import ASVData
import SwiftUI

struct StudyCommandContext {
    let run: (StudyAction) -> Void
    let available: (StudyAction) -> Bool
}

private struct StudyContextKey: FocusedValueKey { typealias Value = StudyCommandContext }
extension FocusedValues {
    var studyCommands: StudyCommandContext? {
        get { self[StudyContextKey.self] }
        set { self[StudyContextKey.self] = newValue }
    }
}

private struct SearchFilters: Equatable {
    let query: String
    let subject: String?
    let examType: String?
    let session: Int?
}

struct StudyWindow: View {
    @EnvironmentObject private var settings: Preferences
    @EnvironmentObject private var speech: Narrator
    @EnvironmentObject private var model: StudySession
    @State private var chatVisible = false
    @State private var chatFocusRequest = 0
    @State private var hidSearchForChat = false
    @State private var showFilters = false
    @State private var columns = NavigationSplitViewVisibility.all
    @FocusState private var focus: Focus?
    @AccessibilityFocusState private var headingFocused: Bool
    enum Focus { case search, results }

    var body: some View {
        GeometryReader { geometry in
            HSplitView {
                studyContent
                    .frame(minWidth: 320, maxWidth: .infinity, maxHeight: .infinity)
                if chatVisible, let problem = model.current {
                    StudyChatSidebar(problem: problem, focusRequest: chatFocusRequest, close: closeChat)
                        .frame(minWidth: min(355, max(280, geometry.size.width * 0.38)), idealWidth: 380,
                               maxWidth: max(380, geometry.size.width * 0.53),
                               maxHeight: .infinity)
                }
            }
            .onChange(of: chatVisible) { _, visible in
                if visible { makeRoomForChat(width: geometry.size.width) }
            }
            .onChange(of: geometry.size.width) { old, width in
                if chatVisible, old >= threeColumnWidth, width < threeColumnWidth { makeRoomForChat(width: width) }
            }
        }
        .navigationTitle("Ahaki Study Viewer")
        .toolbar {
            ToolbarItem {
                Button {
                    if chatVisible { closeChat() } else { perform(.ask) }
                } label: {
                    Label(chatVisible ? "質問サイドバーを閉じる" : "質問サイドバーを開く", systemImage: "sidebar.right")
                }
                .disabled(!chatVisible && model.current == nil)
                .help(chatVisible ? "質問サイドバーを閉じる" : "ChatGPTへの質問サイドバーを開く")
            }
            ToolbarItem {
                SettingsLink { Label("設定", systemImage: "slider.horizontal.3") }
                    .help("表示・読み上げ・キーボードの設定")
            }
        }
        .focusedSceneValue(\.studyCommands, StudyCommandContext(run: perform, available: available))
        .task { settings.applyAppearance(); await model.monitor() }
        .task(id: SearchFilters(query: model.query, subject: model.subject, examType: model.examType, session: model.session)) {
            do { try await Task.sleep(for: .milliseconds(160)) } catch { return }
            await model.search()
            if !Task.isCancelled, model.bank != nil { announce("検索結果 \(model.results.count)問") }
        }
        .onChange(of: model.selectedID) { _, _ in
            speech.stop()
            if settings.autoRead, let problem = model.current { speech.read(problem.questionText, rate: settings.rate) }
        }
        .onDisappear { speech.stop() }
    }

    private var studyContent: some View {
        NavigationSplitView(columnVisibility: $columns) {
            sidebar.navigationSplitViewColumnWidth(min: 245, ideal: 310, max: 520)
        } detail: {
            if let problem = model.current {
                ScrollViewReader { scroll in
                    ScrollView {
                        QuestionReader(problem: problem, choice: model.choice, headingFocused: $headingFocused,
                                       respond: answer, retry: { perform(.retry) }, read: { perform(.read) },
                                       ask: { perform(.ask) }, reveal: { scroll.scrollTo($0, anchor: .top) })
                            .id(problem.id)
                            .frame(maxWidth: 820, alignment: .leading)
                            .padding(.horizontal, 28).padding(.vertical, 24)
                            .frame(maxWidth: .infinity)
                            .id("questionTop")
                    }
                    .onChange(of: model.selectedID) { _, _ in scroll.scrollTo("questionTop", anchor: .top) }
                }
                .safeAreaInset(edge: .bottom, spacing: 0) { navigationBar }
            } else if let error = model.error {
                ContentUnavailableView {
                    Label("問題を開けません", systemImage: "exclamationmark.triangle")
                } description: { Text(error) } actions: {
                    Button("もう一度読み込む") { Task { await model.load() } }
                }
            } else if model.bank == nil {
                ProgressView("問題を読み込み中")
            } else {
                ContentUnavailableView {
                    Label("一致する問題がありません", systemImage: "magnifyingglass")
                } description: { Text("キーワードや絞り込み条件を変えて検索できます。") } actions: {
                    Button("検索条件をクリア", action: clearFilters)
                }
            }
        }
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 0) {
            VStack(alignment: .leading, spacing: 12) {
                Label("問題を探す", systemImage: "books.vertical")
                    .font(settings.font(1, bold: true)).accessibilityAddTraits(.isHeader)
                HStack {
                    Image(systemName: "magnifyingglass").foregroundStyle(.secondary).accessibilityHidden(true)
                    TextField("キーワード・問題番号", text: $model.query)
                        .font(settings.font(-4)).textFieldStyle(.plain)
                        .accessibilityLabel("問題を検索").focused($focus, equals: .search)
                    if !model.query.isEmpty {
                        Button { model.query = ""; focus = .search } label: {
                            Label("検索語を消去", systemImage: "xmark.circle.fill").labelStyle(.iconOnly)
                        }.buttonStyle(.borderless)
                    }
                }
                .padding(8).background(.background, in: RoundedRectangle(cornerRadius: 7))
                .overlay(RoundedRectangle(cornerRadius: 7).stroke(focus == .search ? Color.accentColor : .secondary.opacity(0.5),
                                                                  lineWidth: focus == .search ? 2 : 1))
                DisclosureGroup(isExpanded: $showFilters) {
                    VStack(alignment: .leading) {
                        Picker("試験", selection: $model.examType) {
                            Text("すべての試験").tag(nil as String?)
                            ForEach(model.bank?.examTypes ?? [], id: \.self) { Text($0).tag(Optional($0)) }
                        }
                        Picker("科目", selection: $model.subject) {
                            Text("すべての科目").tag(nil as String?)
                            ForEach(model.bank?.subjects ?? [], id: \.self) { Text($0).tag(Optional($0)) }
                        }
                        Picker("回数", selection: $model.session) {
                            Text("すべての回").tag(nil as Int?)
                            ForEach(model.bank?.sessions ?? [], id: \.self) { Text("第\($0)回").tag(Optional($0)) }
                        }
                        Button("条件をクリア", action: clearFilters)
                    }.padding(.top, 6)
                } label: {
                    Text(hasFilters ? "絞り込み（適用中）" : "絞り込み").font(settings.font(-5))
                }
                Text("検索結果  \(model.results.count.formatted())問")
                    .font(settings.font(-5, bold: true)).foregroundStyle(.secondary)
            }.padding(16)
            Divider()
            ScrollViewReader { proxy in
                List(selection: $model.selectedID) {
                    ForEach(model.results) { problem in
                        VStack(alignment: .leading, spacing: 4) {
                            Text("\(problem.id)　\(problem.subject)").font(settings.font(-6, bold: true))
                            Text(problem.stem).font(settings.font(-3)).lineLimit(2)
                        }
                        .accessibilityElement(children: .combine)
                        .accessibilityLabel("\(problem.id)、\(problem.subject)。\(problem.stem)")
                        .padding(.vertical, 4).tag(problem.id)
                    }
                }
                .listStyle(.sidebar).accessibilityLabel("問題の検索結果").focused($focus, equals: .results)
                .onChange(of: model.index) { _, _ in
                    if let id = model.selectedID { proxy.scrollTo(id) }
                }
            }
        }
    }

    private var navigationBar: some View {
        VStack(spacing: 0) {
            Divider()
            HStack {
                Button { navigate(-1) } label: { Label("前の問題", systemImage: "chevron.left") }
                    .disabled(!model.canMove(-1))
                Spacer()
                Text("\((model.index ?? 0) + 1) / \(model.results.count.formatted())")
                    .monospacedDigit().accessibilityLabel("検索結果\(model.results.count)問中、\((model.index ?? 0) + 1)問目")
                Spacer()
                Button { navigate(1) } label: { Label("次の問題", systemImage: "chevron.right") }
                    .disabled(!model.canMove(1))
            }.padding(.horizontal, 20).padding(.vertical, 12)
        }.background(.bar)
    }

    private var hasFilters: Bool { model.subject != nil || model.examType != nil || model.session != nil }
    private func clearFilters() {
        model.query = ""; model.subject = nil; model.examType = nil; model.session = nil
    }
    private func navigate(_ offset: Int) {
        model.move(offset)
        headingFocused = true
        if let current = model.current { announce("\(current.id)。\(current.stem)") }
    }
    private func answer(_ number: Int) {
        speech.stop()
        guard model.respond(number) else { return }
        if settings.autoRead, let current = model.current {
            speech.read(current.feedback(for: number) + "。正答 " + current.solutionText, rate: settings.rate)
        }
    }
    // Leave room for both the question and the conversation at the chosen reading size.
    private var threeColumnWidth: CGFloat { 310 + 355 + max(410, settings.size * 18) }

    private func makeRoomForChat(width: CGFloat) {
        if width < threeColumnWidth, columns != .detailOnly {
            columns = .detailOnly
            hidSearchForChat = true
        }
    }

    private func closeChat() {
        chatVisible = false
        if hidSearchForChat {
            columns = .all
            hidSearchForChat = false
        }
        headingFocused = true
    }

    private func available(_ action: StudyAction) -> Bool {
        if let number = action.choiceNumber {
            return model.choice == nil && (model.current?.choices.indices.contains(number - 1) == true)
        }
        switch action {
        case .search, .results: return true
        case .previous: return model.canMove(-1)
        case .next: return model.canMove(1)
        case .silence: return speech.isSpeaking
        case .retry: return model.choice != nil
        default: return model.current != nil
        }
    }
    private func perform(_ action: StudyAction) {
        guard available(action) else { return }
        if let number = action.choiceNumber { answer(number); return }
        switch action {
        case .search, .results:
            hidSearchForChat = false
            columns = .all
            Task { @MainActor in
                await Task.yield()
                focus = action == .search ? .search : .results
            }
        case .previous: navigate(-1)
        case .next: navigate(1)
        case .read:
            if let problem = model.current {
                let result = model.choice.map { problem.feedback(for: $0) + "。正答 " + problem.solutionText } ?? ""
                speech.read(problem.questionText + "\n" + result, rate: settings.rate)
            }
        case .silence: speech.stop()
        case .ask:
            speech.stop()
            focus = nil
            chatVisible = true
            chatFocusRequest += 1
        case .retry: speech.stop(); model.retry()
        default: break
        }
    }
}
