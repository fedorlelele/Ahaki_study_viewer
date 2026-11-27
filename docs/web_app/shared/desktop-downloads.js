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
    list.append(item);
  }
  menu.append(list);
  menu.hidden = false;
};
