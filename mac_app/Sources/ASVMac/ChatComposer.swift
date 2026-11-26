import AppKit
import SwiftUI

/// Let the input method handle Return while a Japanese reading is under conversion.
final class PromptTextView: NSTextView {
    var send: () -> Void = {}
    var close: () -> Void = {}
    private let localUndo = UndoManager()
    override var undoManager: UndoManager? { localUndo }

    override func keyDown(with event: NSEvent) {
        if hasMarkedText() { super.keyDown(with: event); return }
        let modifiers = event.modifierFlags.intersection([.shift, .option, .control, .command])
        switch (event.keyCode, modifiers) {
        case (36, []), (76, []), (36, .command), (76, .command):
            if !event.isARepeat { send() }
        case (36, .shift), (76, .shift):
            insertNewlineIgnoringFieldEditor(self)
        case (48, []): window?.selectNextKeyView(self)
        case (48, .shift): window?.selectPreviousKeyView(self)
        case (53, []): close()
        default: super.keyDown(with: event)
        }
    }
}

struct ChatComposer: NSViewRepresentable {
    @Binding var text: String
    let problemID: String
    let accountID: String?
    let focusRequest: Int
    let family: ReadingFont
    let size: Double
    let spacing: Double
    let send: () -> Void
    let close: () -> Void

    final class Coordinator: NSObject, NSTextViewDelegate {
        var parent: ChatComposer
        var focusRequest: Int?
        var updating = false
        init(_ parent: ChatComposer) { self.parent = parent }

        func textDidChange(_ notification: Notification) {
            guard !updating, let field = notification.object as? NSTextView else { return }
            parent.text = field.string
        }
    }

    func makeCoordinator() -> Coordinator { Coordinator(self) }

    func makeNSView(context: Context) -> NSScrollView {
        let field = PromptTextView()
        field.delegate = context.coordinator
        field.isRichText = false
        field.allowsUndo = true
        field.drawsBackground = false
        field.isHorizontallyResizable = false
        field.isVerticallyResizable = true
        field.minSize = .zero
        field.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        field.autoresizingMask = .width
        field.textContainer?.widthTracksTextView = true
        field.textContainerInset = NSSize(width: 4, height: 5)
        field.setAccessibilityLabel("質問の入力")
        field.setAccessibilityHelp("Enterで送信、ShiftとEnterで改行します。")
        let scroll = NSScrollView()
        scroll.drawsBackground = false
        scroll.hasVerticalScroller = true
        scroll.autohidesScrollers = true
        scroll.documentView = field
        return scroll
    }

    func updateNSView(_ scroll: NSScrollView, context: Context) {
        guard let field = scroll.documentView as? PromptTextView else { return }
        context.coordinator.updating = true
        defer { context.coordinator.updating = false }
        let previous = context.coordinator.parent
        context.coordinator.parent = self
        field.send = send
        field.close = close
        let changedThread = previous.problemID != problemID || previous.accountID != accountID
        if changedThread || field.string != text {
            // Discard composing state and undo for the old question when switching threads.
            if changedThread { field.inputContext?.discardMarkedText() }
            if changedThread || !field.hasMarkedText() {
                field.string = text
                field.undoManager?.removeAllActions()
                field.setSelectedRange(NSRange(location: (text as NSString).length, length: 0))
            }
        }
        let fontName: String?
        switch family {
        case .system: fontName = nil
        case .gothic: fontName = "HiraginoSans-W3"
        case .mincho: fontName = "HiraMinProN-W3"
        }
        let font = fontName.flatMap { NSFont(name: $0, size: size) } ?? .systemFont(ofSize: size)
        if field.font != font || field.defaultParagraphStyle?.lineSpacing != CGFloat(spacing) {
            let paragraph = NSMutableParagraphStyle()
            paragraph.lineSpacing = spacing
            field.font = font
            field.defaultParagraphStyle = paragraph
            field.typingAttributes = [.font: font, .paragraphStyle: paragraph, .foregroundColor: NSColor.textColor]
            if !field.string.isEmpty, !field.hasMarkedText() {
                field.textStorage?.addAttribute(.paragraphStyle, value: paragraph,
                                                range: NSRange(location: 0, length: (field.string as NSString).length))
            }
        }
        if context.coordinator.focusRequest != focusRequest {
            context.coordinator.focusRequest = focusRequest
            DispatchQueue.main.async { [weak field] in
                guard let field, let window = field.window else { return }
                window.makeFirstResponder(field)
            }
        }
    }
}
