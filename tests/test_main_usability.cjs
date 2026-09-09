const test=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const vm=require('node:vm');
const html=fs.readFileSync('web_app/index.html','utf8');
function fn(name){const m=new RegExp('^([\\t ]+)(?:async )?function '+name+'\\(','m').exec(html);assert.ok(m,name);const close=/^[\t ]+}/gm;close.lastIndex=m.index+m[0].length;let end;while((end=close.exec(html))){const source=html.slice(m.index,end.index+end[0].length);try{new vm.Script(source);return source;}catch{}}throw Error(name);}
function add(ctx,names){for(const n of names)vm.runInContext(fn(n),ctx);}
test('handoff uses the complete displayed result order rather than pending filter text or page slice',()=>{
 let saved,navigation;const questions=Array.from({length:43},(_,i)=>({serial:`A01-${String(43-i).padStart(3,'0')}`}));
 const ctx=vm.createContext({state:{dataReady:true,resultQuestions:questions,resultTitle:'saved summary'},AhakiStudy:{saveHandoff:(serials,title)=>{saved={serials:Array.from(serials),title};return 'opaque-safe-token';}},window:{location:{assign:url=>navigation=url}},setSearchToolsMessage(){},encodeURIComponent});
 add(ctx,['openResultStudyView']);ctx.openResultStudyView('simple.html');assert.deepEqual(saved,{serials:questions.map(q=>q.serial),title:'saved summary'});assert.equal(navigation,'./simple.html?studySet=opaque-safe-token');
 ctx.state.resultQuestions=[];navigation='';ctx.openResultStudyView('print_export.html');assert.equal(navigation,'');
});
test('failed handoff storage does not navigate away or pretend all questions were sent',()=>{
 let message='',navigated=false;const ctx=vm.createContext({state:{dataReady:true,resultQuestions:[{serial:'B01-001'}]},AhakiStudy:{saveHandoff(){throw Error('保存できません');}},window:{location:{assign(){navigated=true;}}},setSearchToolsMessage:x=>message=x,encodeURIComponent});add(ctx,['openResultStudyView']);ctx.openResultStudyView('print_export.html');assert.equal(navigated,false);assert.equal(message,'保存できません');
});
test('copy captures result set, header and mode before asynchronous override loading',async()=>{
 let release,saved;const ctx=vm.createContext({state:{resultQuestions:[{serial:'A01-001'}],resultTitle:'original'},getCopyMode:()=>ctx.mode,mode:{showAnswer:false,showExplanation:false},ensureOverridesLoaded:()=>new Promise(r=>release=r),applyOverridesToQuestion:q=>q,formatQuestionForCopy:(q,mode)=>q.serial+(mode.showAnswer?' ANSWER':''),copyToClipboard:async text=>saved=text,setSearchToolsMessage(){}});add(ctx,['copyCurrentResults']);const promise=ctx.copyCurrentResults();ctx.state.resultQuestions=[{serial:'B01-002'}];ctx.state.resultTitle='changed';ctx.mode={showAnswer:true};release();await promise;assert.equal(saved,'original\n\nA01-001');
});
test('saved conditions work without a user account and clear old fields before restore',()=>{
 const calls=[];const ctx=vm.createContext({state:{dataReady:true},document:{getElementById:()=>({value:'preset-1'})},AhakiStudy:{listSearchPresets:()=>[{id:'preset-1',name:'循環',filters:{keyword:'循環',sort:'asc'}}]},applySearchStateFromUrl:x=>calls.push(x),resetResultViewState(){},closeSearchHistoryPanel(){},filterQuestions:()=>[],renderResults(){},syncSearchQueryToUrl(){},setSearchToolsMessage(){}});add(ctx,['loadSelectedSearchPreset']);ctx.loadSelectedSearchPreset();assert.equal(calls.length,2);assert.equal(calls[0].subject,'');assert.equal(calls[0].progress,'');assert.equal(calls[0].answered,'all');assert.equal(calls[1].keyword,'循環');assert.equal(calls[1].page,'1');
});
test('copy mode is independent of the answer/explanation display toggles',()=>{
 const ctx=vm.createContext({AhakiStudy:{getCopyMode:()=>({showAnswer:false,showExplanation:false})},document:{getElementById(){throw Error('display checkbox must not be consulted');}}});add(ctx,['getCopyMode']);assert.deepEqual(ctx.getCopyMode(),{showAnswer:false,showExplanation:false});
});
