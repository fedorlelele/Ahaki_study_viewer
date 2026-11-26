const {contextBridge, ipcRenderer} = require('electron');

function subscribe(channel, listener) {
  if (typeof listener !== 'function') throw new TypeError('イベント受信関数が必要です。');
  const forward = (_event, data) => listener(data);
  ipcRenderer.on(channel, forward);
  return () => ipcRenderer.removeListener(channel, forward);
}

contextBridge.exposeInMainWorld('asv', {
  home: () => ipcRenderer.invoke('study:home'),
  openExternal: url => ipcRenderer.invoke('link:open',url),
  speech:{
    voices: () => ipcRenderer.invoke('speech:voices'),
    speak: request => ipcRenderer.invoke('speech:speak',request),
    stop: () => ipcRenderer.invoke('speech:stop'),
    dictionary: () => ipcRenderer.invoke('speech:dictionary'),
    importDictionary: () => ipcRenderer.invoke('speech:import')
  },
  updates:{
    status: () => ipcRenderer.invoke('updates:status'),
    check: () => ipcRenderer.invoke('updates:check'),
    apply: () => ipcRenderer.invoke('updates:apply'),
    subscribe: fn => subscribe('updates:changed',fn)
  },
  chat:{
    invoke: (action,payload) => ipcRenderer.invoke('chat:request',action,payload),
    subscribe: fn => subscribe('chat:changed',fn)
  }
});
