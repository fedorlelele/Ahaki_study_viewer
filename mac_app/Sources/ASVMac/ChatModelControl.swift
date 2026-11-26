import ASVChat
import SwiftUI

struct ChatModelControl: View {
    @EnvironmentObject private var chat: StudyChat
    @EnvironmentObject private var settings: Preferences
    @State private var isPresented = false
    @State private var showsModelList = false

    private var model: ChatModel? {
        chat.availableModels.first { $0.id == chat.selectedModelID }
    }

    private var summary: String {
        guard let model else {
            if chat.isLoadingModels { return "モデルを読み込み中" }
            if chat.modelLoadError != nil { return "モデル一覧を確認" }
            return "モデルとエフォート"
        }
        guard !model.supportedEfforts.isEmpty else { return model.displayName }
        return "\(model.displayName) · \(effortTitle(chat.selectedEffort))"
    }

    var body: some View {
        Button {
            showsModelList = false
            isPresented.toggle()
        } label: {
            HStack(spacing: 5) {
                Image(systemName: chat.modelLoadError == nil ? "slider.horizontal.3" : "exclamationmark.circle")
                    .accessibilityHidden(true)
                Text(verbatim: summary).lineLimit(1).truncationMode(.middle)
                Image(systemName: "chevron.up.chevron.down").imageScale(.small).accessibilityHidden(true)
            }
            .font(settings.font(-6))
        }
        .accessibilityLabel("モデルとエフォート")
        .accessibilityValue(summary)
        .help("モデルとエフォートを変更（\(summary)）")
        .popover(isPresented: $isPresented) {
            ViewThatFits(in: .vertical) {
                options.fixedSize(horizontal: false, vertical: true)
                ScrollView { options }
            }
            .frame(width: max(320, min(480, settings.size * 14)))
            .frame(maxHeight: 460)
        }
        .task(id: chat.activeAccountID) { await chat.loadModels() }
    }

    private var options: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("モデルとエフォート").font(settings.font(0, bold: true)).accessibilityAddTraits(.isHeader)
            if let model {
                DisclosureGroup(isExpanded: $showsModelList) {
                    Picker("回答モデル", selection: Binding(get: { chat.selectedModelID }, set: { id in
                        if let id { chat.selectModel(id: id) }
                    })) {
                        ForEach(chat.availableModels) { item in
                            Text(verbatim: item.displayName).tag(Optional(item.id))
                        }
                    }.pickerStyle(.radioGroup).labelsHidden().disabled(chat.isBusy || chat.isLoadingModels)
                } label: {
                    Text("回答モデル　\(model.displayName)")
                        .fixedSize(horizontal: false, vertical: true)
                }
                if model.supportedEfforts.isEmpty {
                    Text("このモデルではエフォートを選択できません。").foregroundStyle(.secondary)
                } else {
                    VStack(alignment: .leading, spacing: 4) {
                        Text("エフォート（推論の強さ）").accessibilityHidden(true)
                        Picker("エフォート（推論の強さ）", selection: Binding(get: { chat.selectedEffort }, set: chat.selectEffort)) {
                            Text(effortTitle(nil)).tag(nil as String?)
                            ForEach(model.supportedEfforts, id: \.self) { effort in
                                Text(effortTitle(effort)).tag(Optional(effort))
                            }
                        }.pickerStyle(.radioGroup).labelsHidden().disabled(chat.isBusy || chat.isLoadingModels)
                    }
                }
            }
            if chat.isLoadingModels {
                ProgressView("モデル一覧を読み込み中").controlSize(.small)
            }
            if let error = chat.modelLoadError {
                Text(verbatim: error).fixedSize(horizontal: false, vertical: true)
            }
            Button(chat.modelLoadError == nil ? "モデル一覧を更新" : "モデル一覧を再読み込み") {
                Task { await chat.loadModels(force: true) }
            }.disabled(chat.isLoadingModels || chat.isBusy)
        }
        .font(settings.font(-3))
        .controlSize(settings.size > 24 ? .large : .regular)
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(16)
    }

    private func effortTitle(_ effort: String?) -> String {
        guard let effort else { return "自動" }
        switch effort {
        case "none": return "なし"
        case "minimal": return "最小"
        case "low": return "低"
        case "medium": return "中"
        case "high": return "高"
        case "xhigh": return "非常に高い"
        case "max": return "最大"
        default: return effort
        }
    }
}
