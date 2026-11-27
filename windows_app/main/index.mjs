import {app, BrowserWindow, dialog, ipcMain, Menu, protocol, safeStorage, session, shell} from 'electron';
import {readFile} from 'node:fs/promises';
import {join, extname} from 'node:path';
import {spawn} from 'node:child_process';
import {createRequire} from 'node:module';
import {createQuestionBank} from './question-bank.mjs';
import {createChatService} from './chat.mjs';
import {createSpeech} from './speech.mjs';
import {createUpdater} from './updates.mjs';
import {uploadAnswers} from './answer-upload.mjs';

const page = 'asv://local/renderer/index.html';
protocol.registerSchemesAsPrivileged([{scheme:'asv',privileges:{standard:true,secure:true,supportFetchAPI:true}}]);
const chatActions = new Set(['state','ackWelcome','history','signIn','selectAccount','signOut','models','selectModel','selectEffort','send','cancel','usage']);
let window, chat, speech;

async function openLink(raw) {
  if (typeof raw !== 'string') throw new Error('リンクを開けません。');
  const url = new URL(raw);
  if (!['https:','http:'].includes(url.protocol) || url.username || url.password) throw new Error('このリンクは開けません。');
  await shell.openExternal(url.href);
}

function start() {
  const root = app.getAppPath(), userData = app.getPath('userData');
  const rendererFiles = new Set(['index.html','study.css','study.js','study-core.mjs','answer-history.mjs','chat.mjs','chat.css','chat-markdown.mjs']);
  const mime = {'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'text/javascript; charset=utf-8','.mjs':'text/javascript; charset=utf-8'};
  protocol.handle('asv', async request => {
    const url = new URL(request.url);
    if (url.host !== 'local' || request.method !== 'GET') return new Response(null,{status:403});
    const path = url.pathname.slice(1);
    const [folder,file] = path.split('/');
    if (!(folder === 'renderer' && path === 'renderer/' + file && rendererFiles.has(file)) &&
        path !== 'node_modules/marked/lib/marked.esm.js') return new Response(null,{status:404});
    return new Response(await readFile(join(root,path)),{headers:{'Content-Type':mime[extname(path)],
      'X-Content-Type-Options':'nosniff','Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"}});
  });
  session.defaultSession.setPermissionRequestHandler((_contents,_perm,cb) => cb(false));
  session.defaultSession.setPermissionCheckHandler(() => false);
  Menu.setApplicationMenu(null);
  window = new BrowserWindow({width:1320,height:850,minWidth:640,minHeight:480,title:'ASV',show:false,
    backgroundColor:'#182529',webPreferences:{preload:join(root,'main','preload.cjs'),contextIsolation:true,
      nodeIntegration:false,sandbox:true,webSecurity:true,webviewTag:false}});
  window.webContents.setWindowOpenHandler(() => ({action:'deny'}));
  window.webContents.on('will-navigate', (event,url) => { if (url.split('#')[0] !== page) event.preventDefault(); });
  const send = (channel, data) => { if (window && !window.isDestroyed()) window.webContents.send(channel,data); };
  speech = createSpeech({userData, chooseFile:async () => {
    const result = await dialog.showOpenDialog(window,{title:'理療科用読み辞書を読み込む',properties:['openFile'],
      filters:[{name:'読み辞書（Jukugo.usr）',extensions:['usr']},{name:'すべてのファイル',extensions:['*']}]});
    return result.canceled ? null : result.filePaths[0];
  }});
  chat = createChatService({userData,safeStorage,openExternal:openLink,emit:e => send('chat:changed',e)});
  const updates = createUpdater({version:app.getVersion(),arch:process.arch,cache:join(app.getPath('temp'),'ASV-updates'),
    notify:s => send('updates:changed',s), install: async path => {
      if (!app.isPackaged || process.platform !== 'win32') throw new Error('更新はインストール済みのWindows版で実行してください。');
      await new Promise((resolve,reject) => {
        const installer = spawn(path,['--updated'],{detached:true,stdio:'ignore',windowsHide:false});
        installer.once('error',reject);
        installer.once('spawn',() => {installer.unref(); resolve();});
      });
      setImmediate(() => app.quit());
    }});
  function handle(channel,action) {
    ipcMain.handle(channel,(event,...args) => {
      const frame = event.senderFrame;
      if (!window || window.isDestroyed() || event.sender !== window.webContents || !frame ||
          frame !== event.sender.mainFrame || frame.url.split('#')[0] !== page) throw new Error('この操作を利用できません。');
      return action(...args);
    });
  }
  const contractPath = app.isPackaged ? join(process.resourcesPath,'web-questions.cjs') : join(root,'../web_app/shared/questions.js');
  const bank = createQuestionBank({directory:userData, contract:createRequire(import.meta.url)(contractPath),
    bundledPath:join(app.isPackaged ? process.resourcesPath : join(root,'resources'),'ASVQuestions.json')});
  handle('study:home',bank.home);
  handle('study:sync',() => process.env.ASV_TEST_OFFLINE === '1'
    ? Promise.reject(new Error('オフラインです。')) : bank.sync());
  handle('link:open',openLink);
  handle('answers:upload', (anonId, events) => {
    if (process.env.ASV_TEST_OFFLINE === '1') throw new Error('オフラインのテスト中です。');
    return uploadAnswers(anonId, events);
  });
  for (const method of ['voices','speak','stop','dictionary']) handle('speech:'+method,speech[method]);
  handle('speech:import',speech.importDictionary);
  for (const method of ['status','check','apply']) handle('updates:'+method,updates[method]);
  handle('chat:request',(action,payload) => {
    if (!chatActions.has(action)) throw new Error('チャット操作を認識できません。');
    return chat.handle(action,payload);
  });
  window.once('ready-to-show', () => window.show());
  window.loadURL(page).catch(error => dialog.showErrorBox('ASVを開けません',error.message));
}

if (app.requestSingleInstanceLock()) {
  app.on('second-instance',() => { if (window && !window.isDestroyed()) { if (window.isMinimized()) window.restore(); window.show(); window.focus(); } });
  app.on('window-all-closed',() => app.quit());
  app.on('before-quit',() => {speech?.stop(); chat?.close();});
  app.whenReady().then(start).catch(error => { dialog.showErrorBox('ASVを起動できません',error.message); app.quit(); });
} else app.quit();
