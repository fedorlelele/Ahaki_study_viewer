window.mountDesktopDownloads = function () {
  const menu = document.getElementById("desktopDownloads");
  if (!menu) return;

  const releases = "https://github.com/fedorlelele/Ahaki_study_viewer/releases/download/";
  const choices = [
    ["Mac版（Intel・Apple Silicon共通）", "mac-v0.4.0/ASV-0.4.0.zip"],
    ["Windows版（Intel・AMD、x64）", "windows-v0.2.0/ASV-Windows-0.2.0-x64-Setup.exe"],
    ["Windows版（Snapdragonなど、ARM64）", "windows-v0.2.0/ASV-Windows-0.2.0-arm64-Setup.exe"]
  ];
  const list = document.createElement("ul");
  for (const [label, asset] of choices) {
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.href = releases + asset;
    link.className = "btn-secondary";
    link.textContent = label;
    item.append(link);
    if (asset.startsWith("mac-")) {
      const note = document.createElement("p");
      const title = document.createElement("strong");
      title.textContent = "Mac版を初めて開くとき";
      note.append(title);
      item.append(note);

      const steps = document.createElement("ol");
      for (const instruction of [
        "ZIPを展開し、ASVを「アプリケーション」フォルダに移してダブルクリックします。",
        "開発元を確認できないという警告で開けない場合は、警告を閉じ、アップルメニューの「システム設定」→「プライバシーとセキュリティ」を開きます。",
        "「セキュリティ」欄でASVの「このまま開く」を選びます。「開く」が表示される場合は先にそれを選び、画面に沿って認証します。次回からは通常どおり開けます。"
      ]) {
        const step = document.createElement("li");
        step.textContent = instruction;
        steps.append(step);
      }
      item.append(steps);
    }
    list.append(item);
  }
  menu.append(list);
  menu.hidden = false;
};
