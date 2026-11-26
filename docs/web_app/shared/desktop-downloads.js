// Attach public release links only while the administrator's session is active.
window.mountDesktopDownloads = function (client, getUserRole) {
  const menu = document.getElementById("desktopDownloads");
  if (!menu || !client) return;

  const releases = "https://github.com/fedorlelele/Ahaki_study_viewer/releases/download/";
  const choices = [
    ["Mac版（Intel・Apple Silicon共通）", "mac-v0.3.0/ASV-0.3.0.zip"],
    ["Windows版（Intel・AMD、x64）", "windows-v0.1.2/ASV-Windows-0.1.2-x64-Setup.exe"],
    ["Windows版（Snapdragonなど、ARM64）", "windows-v0.1.2/ASV-Windows-0.1.2-arm64-Setup.exe"]
  ];
  const list = document.createElement("ul");
  menu.append(list);
  let owner = "";

  // Supabase also sends INITIAL_SESSION when opening the page already signed in.
  client.auth.onAuthStateChange((_event, session) => {
    const admin = session?.access_token && getUserRole(session.user) === "admin";
    const next = admin ? session.user.id : "";
    if (next === owner) return;
    owner = next;
    menu.hidden = !owner;
    menu.open = false;
    list.replaceChildren();
    if (owner) {
      for (const [label, asset] of choices) {
        const item = document.createElement("li");
        const link = document.createElement("a");
        link.href = releases + asset;
        link.className = "btn-secondary";
        link.textContent = label;
        item.append(link);
        list.append(item);
      }
    }
  });
};
