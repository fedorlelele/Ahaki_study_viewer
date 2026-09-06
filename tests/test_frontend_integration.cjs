const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Q = require('../web_app/shared/questions.js');
const web = path.join(__dirname,'../web_app');
function html(name){return fs.readFileSync(path.join(web,name),'utf8');}
function fn(name, functionName){
  const text=html(name);
  const match=new RegExp('^([\\t ]+)(?:async )?function '+functionName+'\\(', 'm').exec(text);
  if(!match) throw new Error(`missing ${name}:${functionName}`);
  const closing=/^[\t ]+}/gm;closing.lastIndex=match.index+match[0].length;
  let end;
  while((end=closing.exec(text))){
    const source=text.slice(match.index,end.index+end[0].length);
    try { new vm.Script(source);return source; } catch (_) { /* nested closing brace */ }
  }
  throw new Error(`missing end: ${functionName}`);
}
function addFunctions(context, name, names){for(const item of names)vm.runInContext(fn(name,item),context);}
function dom(){
 const document={activeElement:null};
 function element(tag='div'){
  const value={tagName:tag.toUpperCase(),textContent:'',hidden:false,disabled:false,value:'',dataset:{},attrs:{},children:[],listeners:{},className:'',tabIndex:0,
   setAttribute(k,v){this.attrs[k]=String(v);if(k==='tabindex')this.tabIndex=Number(v);},removeAttribute(k){delete this.attrs[k];},
   addEventListener(k,v){this.listeners[k]=v;},focus(){document.activeElement=this;},
   appendChild(child){if(child.isFragment)this.children.push(...child.children);else this.children.push(child);return child;},
   querySelectorAll(selector){return this.children.filter(child=>selector==='li'||selector==='button'||(selector==='.result-item'&&child.className.includes('result-item')));},
   get innerHTML(){return '';},set innerHTML(v){this.children=[];},
  };
  value.classList={add(){},remove(){},toggle(){},contains(){return false}};return value;
 }
 document.createElement=element;document.createTextNode=text=>({textContent:text});document.createDocumentFragment=()=>({...element(),isFragment:true});
 return {document,element};
}

test('every inline application script parses and question pages load the shared contract', () => {
 for(const name of fs.readdirSync(web).filter(name=>name.endsWith('.html'))){
  const text=html(name);
  for(const script of text.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g))if(!/src=/.test(script[1])&&script[2].trim())new vm.Script(script[2],{filename:name});
 }
 for(const name of ['index.html','simple.html','print_export.html','tag_study.html'])assert.match(html(name),/src="\.\/shared\/questions\.js"/);
});
test('all role-aware pages ignore user-editable metadata for privileges', () => {
 for(const name of ['index.html','simple.html','tag_study.html','admin.html','analytics.html']){
  const ctx=vm.createContext({});addFunctions(ctx,name,['normalizeRole','getUserRole']);
  assert.equal(ctx.getUserRole({user_metadata:{role:'admin'}}),'');
  assert.equal(ctx.getUserRole({app_metadata:{role:'student'},user_metadata:{role:'admin'}}),'student');
 }
});
test('simple study discards reversed requests and grades its displayed question exactly once', async () => {
 const {document,element}=dom();const waits={};const records=[];
 const a={serial:'A01-001',stem:'問題A',choices:['A1','A2'],answer_indices:[1]};
 const b={serial:'A01-002',stem:'問題B',choices:['B1','B2'],answer_indices:[2]};
 const ctx=vm.createContext({document,AhakiQuestions:Q,questionRenderGuard:Q.createRenderGuard(),state:{filtered:[a,b],currentIndex:0,sessionResults:[]},ensureOverridesLoaded:([serial])=>new Promise(resolve=>waits[serial]=resolve),applyOverridesToQuestion:q=>q,formatExamTypeLabel:()=>'',announce(){},saveSimpleSession(){},setResultButtonsEnabled(){},markAnswered:(q,selected,isCorrect)=>records.push({serial:q.serial,selected,isCorrect}),getAnonId:()=>'',supabaseClient:null,setTimeout(){}});
 for(const name of ['status','resultOverlay','resultText','resultStatus','resultBack','resultNext','questionSection','choices','progressIndicator','metaText','meta','caseText','stem','jumpSerial','confirmBtn'])ctx[name]=element();
 addFunctions(ctx,'simple.html',['renderQuestion','showResult','getAnswerIndices','isAnswerCorrect','formatAnswerLabel']);
 const first=ctx.renderQuestion();ctx.state.currentIndex=1;const second=ctx.renderQuestion();
 ctx.state.selectedIndex=1;ctx.showResult();assert.equal(records.length,0,'pending requests cannot be answered');
 waits[b.serial]();await second;waits[a.serial]();await first;
 assert.equal(ctx.stem.textContent,'問題B');assert.equal(ctx.progressIndicator.textContent,'問題 2 / 2');
 // Even a later list replacement must not change the grading target.
 ctx.state.filtered=[a,a];ctx.state.selectedIndex=2;ctx.showResult();ctx.showResult();
 assert.deepEqual(records,[{serial:b.serial,selected:2,isCorrect:true}]);
});
test('returning to the top invalidates an unfinished simple-study response', async () => {
 const {document,element}=dom();let release;
 const ctx=vm.createContext({document,AhakiQuestions:Q,questionRenderGuard:Q.createRenderGuard(),state:{filtered:[{serial:'A01-001'}],currentIndex:0,sessionResults:[]},ensureOverridesLoaded:()=>new Promise(r=>release=r),applyOverridesToQuestion:q=>q,shouldConfirmTopReset:()=>false,setStatus(){},showStep(){},updateState(){},updateResumeControls(){}});
 for(const name of ['confirmBtn','questionSection','summarySection','confirmOverlay','resultOverlay','topConfirmOverlay','subjectSelect','examTypeSelect','subtopicSelect','keywordInput','status'])ctx[name]=element();
 addFunctions(ctx,'simple.html',['renderQuestion','resetToTop']);
 const pending=ctx.renderQuestion();ctx.resetToTop({skipConfirm:true});release();await pending;
 assert.equal(ctx.state.displayedQuestion,null);assert.equal(ctx.questionSection.hidden,true);
});
test('main initial data load applies cloud text/tags before any search or serial visit', async () => {
 const staticQuestion={serial:'A01-001',subject:'生理学',stem:'旧問題',tags:['旧タグ'],subtopics:['旧項目'],choices:['1','2'],answer_indices:[1]};
 const row={serial:staticQuestion.serial,stem:'訂正済問題',tags:['新タグ'],subtopics:['新項目'],updated_at:'2026-09-06T01:00:00Z',synced_at:'2026-09-06T02:00:00Z'};
 const inputs={keyword:'訂正済問題',subjectSelect:'',subtopicSelect:'',examTypeSelect:'',sessionFrom:'',sessionTo:'',progressStateFilter:'',sortSession:'asc',answeredFilter:'all'};
 const ctx=vm.createContext({console,AhakiQuestions:Q,state:{qaSerials:new Set(),qaLatestBySerial:{},deepDiveLatestBySerial:{},tagViewCounts:{},overridesBySerial:{},disabledTags:new Set()},loadQuestionsWithFallback:async()=>[staticQuestion],loadAllQuestionOverrides:async()=>[row],loadTagCatalogLight:async()=>[],fetch:async()=>({ok:true,json:async()=>({})}),buildTagEquivalentMap:()=>({}),buildSubtopicsBySubject:()=>({}),resolvedQuestionCache:new Map(),filterDisabledTags:x=>x,getExplanationMetadata:()=>({}),normalizeAscii:x=>x,isRoleAtLeast:()=>false,document:{getElementById:id=>({value:inputs[id]})},buildAdvancedSearchState:()=>({commandSubjects:[],commandSubtopics:[],commandExamTypes:[],commandSessionRules:[],hasKeywordTerms:true,keywordRpn:[{type:'TERM',term:{kind:'text',value:'訂正済問題'}}]})});
 addFunctions(ctx,'index.html',['loadData','filterQuestions','uniqueQuestionsBySerial','applyOverridesToQuestion','evaluateKeywordRpn']);
 await ctx.loadData();
 assert.deepEqual(Array.from(ctx.filterQuestions(),q=>q.serial),['A01-001']);
 assert.deepEqual(Array.from(ctx.state.indexByTag['新タグ']),['A01-001']);
 assert.equal(ctx.state.indexByTag['旧タグ'],undefined);
});
test('simple and printable answer output retain the chosen medium and source note', () => {
 const q={serial:'B20-095',stem:'問',choices:['1','2'],answer_indices:[1],answer_variants:{default:[1],braille:[1,2]},answer_notes:['点字問題は1、2']};
 const ctx=vm.createContext({AhakiQuestions:Q,normalizeText:x=>String(x||'').trim(),getDeepDiveText:()=>''});
 addFunctions(ctx,'print_export.html',['formatAnswerLabel','buildQuestionBlock']);
 const normal=ctx.buildQuestionBlock(q,0,{includeAnswer:true,answerMedium:'default'});
 const braille=ctx.buildQuestionBlock(q,0,{includeAnswer:true,answerMedium:'braille'});
 assert.match(normal,/解答　１\n/);assert.match(braille,/解答　１・２/);assert.match(braille,/注記: 点字問題は1、2/);
});
test('print export snapshots the requested questions while deep explanations load', async () => {
 let release;let saved;
 const original={serial:'A01-001'};
 const ctx=vm.createContext({state:{filtered:[original]},console,applyFilters(){},buildFilterSummary:()=>ctx.state.filtered[0].serial,buildCurrentExportTitle:()=>ctx.state.filtered[0].serial,getExportMode:()=>({includeDeepDive:true,answerMedium:'default'}),ensureDeepDiveExportWithinLimit:async()=>({ok:true,count:1}),ensureDeepDiveContentLoaded:()=>new Promise(r=>release=r),setStatus(){},buildExportText:(title,list,mode,summary)=>({title,serials:Array.from(list,q=>q.serial),medium:mode.answerMedium,summary}),sanitizeFilename:x=>x,formatLocalTimestamp:()=>'',downloadText:(filename,text)=>saved=text});
 addFunctions(ctx,'print_export.html',['downloadCurrentTxt']);
 const pending=ctx.downloadCurrentTxt();await new Promise(resolve=>setImmediate(resolve));
 ctx.state.filtered=[{serial:'A01-002'}];release();await pending;
 assert.deepEqual(saved,{title:'A01-001',serials:['A01-001'],medium:'default',summary:'A01-001'});
});
test('tag candidates have one Tab entry per 30-result page and keyboard paging', () => {
 const {document,element}=dom();const elements={resultList:element(),searchMeta:element(),resultPagination:element()};document.getElementById=id=>elements[id];
 const ctx=vm.createContext({document,state:{selectedTag:'',searchResultPage:0},announce(){},trackAnalyticsEvent(){},escapeHtmlForMarkdown:x=>x,selectTag(){},getSearchTerms:()=>[],describeSingleHit:()=>''});
 addFunctions(ctx,'tag_study.html',['renderSearchResults']);
 const rows=Array.from({length:70},(_,i)=>({tag:`tag${i}`,related_count:1}));ctx.renderSearchResults(rows,[]);
 let buttons=elements.resultList.children;assert.equal(buttons.length,30);assert.equal(buttons.filter(b=>b.tabIndex===0).length,1);
 buttons[0].listeners.keydown({key:'End',preventDefault(){}});assert.equal(document.activeElement,buttons[29]);
 buttons[29].listeners.keydown({key:'PageDown',preventDefault(){}});
 buttons=elements.resultList.children;assert.equal(ctx.state.searchResultPage,1);assert.equal(buttons[0].dataset.tag,'tag30');assert.equal(document.activeElement,buttons[0]);assert.equal(buttons.filter(b=>b.tabIndex===0).length,1);
});
test('retries of one progress answer reuse its event ID and intentional re-answer gets a new ID', async () => {
 const bodies=[];const ctx=vm.createContext({AhakiQuestions:Q,state:{sessionAnswered:{},answeredMap:{},progressSelfBySerial:{},progressSelfList:[]},isRoleAtLeast:()=>true,trackAnalyticsEvent(){},saveAnsweredState(){},progressApiRequest:async(path,options)=>{bodies.push(options.body);return {};}});
 addFunctions(ctx,'index.html',['markAnswered','sendProgressAnswer']);
 const q={serial:'A01-001'};ctx.markAnswered(q,1,true);const first=ctx.state.sessionAnswered[q.serial].eventId;
 await ctx.sendProgressAnswer(q.serial,true);await ctx.sendProgressAnswer(q.serial,true);
 assert.equal(bodies[0].event_id,first);assert.equal(bodies[1].event_id,first);
 ctx.markAnswered(q,1,true);assert.notEqual(ctx.state.sessionAnswered[q.serial].eventId,first);
});

test('top reset and a new search clear old live-region feedback including queued announcements', () => {
 const {document,element}=dom();const live=element();document.getElementById=()=>live;
 const timers=new Map();let timerId=0;
 const ctx=vm.createContext({document,state:{filtered:[],sessionResults:[]},questionRenderGuard:Q.createRenderGuard(),shouldConfirmTopReset:()=>false,showStep(){},updateState(){},updateResumeControls(){},clearSavedSession(){},trackSimpleSearchSubmitted(){},filterQuestions(){},renderQuestion(){},setTimeout:callback=>{const id=++timerId;timers.set(id,callback);return id;},clearTimeout:id=>timers.delete(id)});
 for(const name of ['confirmBtn','questionSection','summarySection','confirmOverlay','resultOverlay','topConfirmOverlay','subjectSelect','examTypeSelect','subtopicSelect','keywordInput','status'])ctx[name]=element();
 addFunctions(ctx,'simple.html',['announce','setStatus','resetToTop','beginCurrentSearch']);
 ctx.setStatus('不正解です。正解は1です');
 for(const [id,callback] of timers){timers.delete(id);callback();}
 assert.equal(live.textContent,'不正解です。正解は1です');
 ctx.announce('遅延した古い採点通知');
 ctx.resetToTop({skipConfirm:true});
 assert.equal(ctx.status.textContent,'');assert.equal(live.textContent,'');assert.equal(timers.size,0);
 ctx.setStatus('前回の採点通知');ctx.beginCurrentSearch();
 assert.equal(ctx.status.textContent,'');assert.equal(live.textContent,'');assert.equal(timers.size,0);
 ctx.announce('新しい問題です');
 for(const [id,callback] of timers){timers.delete(id);callback();}
 assert.equal(live.textContent,'新しい問題です');
});

test('main initial cloud metadata and badges agree for approvals, named models and a synced static revision', async () => {
 const question=(serial)=>({serial,stem:'問',subject:'生理学',tags:[],subtopics:[],explanation_latest:'元解説',explanation_latest_source:'model:OldModel',explanation_latest_model_name:'OldModel',explanation_latest_review_status:'ai'});
 const published={...question('A01-003'),explanation_latest_source:'model:PublishedModel:checked',explanation_latest_model_name:'PublishedModel',explanation_latest_review_status:'teacher_approved',override_updated_at:'2026-09-06T01:00:00Z'};
 const rows=[{serial:'A01-001',explanation_source:'llm_checked'},{serial:'A01-002',explanation_source:'model:NewModel:checked'},{serial:'A01-003',explanation_source:'model:ObsoleteModel',updated_at:published.override_updated_at}];
 const ctx=vm.createContext({console,AhakiQuestions:Q,state:{qaSerials:new Set(),qaLatestBySerial:{},deepDiveLatestBySerial:{},tagViewCounts:{},overridesBySerial:Object.fromEntries(rows.map(row=>[row.serial,row])),disabledTags:new Set()},loadQuestionsWithFallback:async()=>[question('A01-001'),question('A01-002'),published],loadAllQuestionOverrides:async()=>rows,loadTagCatalogLight:async()=>[],fetch:async()=>({ok:true,json:async()=>({})}),buildTagEquivalentMap:()=>({}),buildSubtopicsBySubject:()=>({}),resolvedQuestionCache:new Map(),filterDisabledTags:x=>x});
 addFunctions(ctx,'index.html',['loadData','applyOverridesToQuestion','getExplanationMetadata','formatExplanationLabel']);
 await ctx.loadData();
 for(const [index,model] of ['OldModel','NewModel','PublishedModel'].entries()) {
   const q=ctx.applyOverridesToQuestion(ctx.state.questions[index]);
   assert.equal(q.explanation_latest_model_name,model);
   assert.equal(q.explanation_latest_review_status,'teacher_approved');
   assert.equal(ctx.formatExplanationLabel(q.explanation_latest_source,q.explanation_latest_model_name,q.explanation_latest_review_status),`（${model}・教師承認済み）`);
 }
});
test('a manual source-only approval follows the same metadata merge as initial cloud loading', async () => {
 const q={serial:'A01-001',explanation_latest:'元解説',explanation_latest_source:'model:OldModel',explanation_latest_model_name:'OldModel',explanation_latest_review_status:'ai'};
 const ctx=vm.createContext({AhakiQuestions:Q,state:{questions:[q],questionBySerial:{[q.serial]:q},teacherSession:{user:{id:'teacher'}},overridesBySerial:{},overridesLoaded:new Set()},supabaseClient:{from(){return {upsert:()=>Promise.resolve({error:null}),insert:()=>Promise.resolve({error:null})}}},window:{scrollY:0,scrollTo(){}},requestAnimationFrame:callback=>callback(),renderResults(){},filterQuestions:()=>[],alert:message=>{throw new Error(message)}});
 addFunctions(ctx,'index.html',['applyOverrideChange']);
 assert.equal(await ctx.applyOverrideChange(q.serial,'explanation',{}, {},{explanation_source:'llm_checked'},''),true);
 assert.equal(ctx.state.questions[0].explanation_latest_model_name,'OldModel');
 assert.equal(ctx.state.questions[0].explanation_latest_review_status,'teacher_approved');
 assert.equal(ctx.state.questionBySerial[q.serial],ctx.state.questions[0]);
});
