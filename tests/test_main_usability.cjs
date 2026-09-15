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

test('pagination uses the displayed order after answers change live filtering',()=>{
 const original=Array.from({length:60},(_,i)=>({serial:`A01-${String(i+1).padStart(3,'0')}`}));
 let rendered;
 const ctx=vm.createContext({state:{dataReady:true,resultQuestions:original,currentResultPage:1,resultTotalPages:3,focusSerialOnRender:'A01-001'},
  filterQuestions(){throw Error('navigation must not recalculate answered/progress filters');},
  renderResults(list,options){rendered={list,options};ctx.state.resultQuestions=[];}});
 add(ctx,['jumpToResultPage']);
 assert.equal(ctx.jumpToResultPage(2),true);
 assert.deepEqual(Array.from(rendered.list.slice(20,40)),original.slice(20,40));
 assert.notEqual(rendered.list,original);
 assert.equal(rendered.options.preserveSearch,true);
 assert.equal(ctx.state.focusSerialOnRender,null);
 assert.equal(ctx.state.focusFirstResultOnRender,true);
 assert.equal(ctx.state.pendingResultPage,2);
});

test('pagination clamps to last page and rejects empty, loading and unchanged results',()=>{
 let renders=0;
 const ctx=vm.createContext({state:{dataReady:true,resultQuestions:[{serial:'A01-001'}],currentResultPage:1,resultTotalPages:3},renderResults(){renders++;}});
 add(ctx,['jumpToResultPage']);
 assert.equal(ctx.jumpToResultPage('invalid'),false);
 assert.equal(ctx.jumpToResultPage(1),false);
 assert.equal(ctx.jumpToResultPage(99),true);
 assert.equal(ctx.state.currentResultPage,3);
 ctx.state.dataReady=false;
 assert.equal(ctx.jumpToResultPage(2),false);
 ctx.state.dataReady=true;ctx.state.resultQuestions=[];
 assert.equal(ctx.jumpToResultPage(2),false);
 assert.equal(renders,1);
});

test('missing retry target falls back to first-result focus',()=>{
 let focused=0;
 const ctx=vm.createContext({state:{focusSerialOnRender:'previous-page',focusFirstResultOnRender:true}});
 add(ctx,['applyPendingResultFocus']);
 ctx.applyPendingResultFocus({querySelector:selector=>selector==='.card'?{focus(){focused++;}}:null});
 assert.equal(focused,1);
 assert.equal(ctx.state.focusFirstResultOnRender,false);
 assert.equal(ctx.state.focusSerialOnRender,null);
});

test('explicit search resets to page one and refreshes the result set',()=>{
 let rendered,filters=0;
 const ctx=vm.createContext({state:{currentResultPage:3,pendingResultPage:3,lastResultViewKey:'old',resultSearch:{},focusSerialOnRender:'old',focusFirstResultOnRender:true},
  getSearchHistoryElements:()=>({input:{value:'new'}}),addSearchHistoryTerm(){},closeSearchHistoryPanel(){},
  filterQuestions(){filters++;return [{serial:'new'}];},renderResults:list=>rendered=list,syncSearchQueryToUrl(){}});
 add(ctx,['resetResultViewState','runKeywordSearch']);ctx.runKeywordSearch();
 assert.equal(filters,1);assert.equal(rendered[0].serial,'new');
 assert.equal(ctx.state.currentResultPage,1);assert.equal(ctx.state.pendingResultPage,null);
 assert.equal(ctx.state.resultSearch,null);assert.equal(ctx.state.focusSerialOnRender,null);
});

test('page URL uses applied search metadata even while inputs have unsubmitted edits',()=>{
 let saved;
 const ctx=vm.createContext({state:{currentResultPage:2},URL,
  window:{location:{href:'https://example.test/web_app/index.html?q=old&sort=asc'}},history:{replaceState:(_a,_b,url)=>saved=url},
  getCurrentSearchSettings(){throw Error('must not read draft input');},isRoleAtLeast:()=>false,
  normalizeSessionParamValue:x=>x||'',normalizePositiveIntParamValue:x=>String(x),setPersistedRandomSeed(){}});
 add(ctx,['syncSearchQueryToUrl']);
 ctx.syncSearchQueryToUrl({keyword:'original',subject:'',subtopic:'',examType:'',progress:'',answered:'hide_answered',sort:'asc',randomSeed:'',sessionFrom:'',sessionTo:''});
 const u=new URL(saved,'https://example.test');
 assert.equal(u.searchParams.get('q'),'original');assert.equal(u.searchParams.get('answered'),'hide_answered');
 assert.equal(u.searchParams.get('page'),'2');assert.equal(u.searchParams.get('sort'),'asc');
});
