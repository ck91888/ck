// Deterministic UI-only fixtures. No worker, network, actual people, or live data.
// CK_JSDOM_MODULE=/tmp/ck-workforce-deps/node_modules/jsdom/lib/api.js node --test tests/workforce-refresh.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const domOptions={skip:!runtime};
const sources=['document-labels.js','attendance-ui.js','employee-import.js','employee-attendance.js'].map(name=>readFileSync(new URL('../shared/'+name,import.meta.url),'utf8'));
const DAY='2026-10-08',SERVER=Date.parse(DAY+'T10:00:00.123+09:00');
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
const settle=async()=>{for(let i=0;i<8;i++)await Promise.resolve();await new Promise(resolve=>setImmediate(resolve));};
function clock(w){
 let now=Date.parse('2026-11-12T12:00:00Z'),nextId=1;const start=now,timers=new Map();
 w.Date.now=()=>now;
 w.setInterval=(fn,delay)=>{const id=nextId++;timers.set(id,{fn,delay,next:now+delay});return id;};w.clearInterval=id=>timers.delete(id);
 return {get elapsed(){return now-start;},get count(){return timers.size;},async advance(ms){const end=now+ms;for(;;){let entry;for(const candidate of timers)if(candidate[1].next<=end&&(!entry||candidate[1].next<entry[1].next))entry=candidate;if(!entry)break;now=entry[1].next;entry[1].next+=entry[1].delay;entry[1].fn();await settle();}now=end;await settle();}};
}
function item(id='A',status='unassigned',kind='labor'){
 return {record:{id,name:'Fixture '+id,badgeId:(kind==='employee'?'EMP-':'DA-')+id,employeeNo:id,personType:kind==='employee'?'employee':'daily',department:'bulk',managementDepartment:'bulk',agency:'가온',day:DAY,inAt:DAY+'T08:00:00+09:00',outAt:'',version:1},status,currentJobs:status==='working'?[{id:'JOB-'+id,no:'TASK-'+id,department:'import'}]:[],canVoid:status==='unassigned',voidBlockedReason:'Fixture active work',totals:{presence:60,rest:0,bulk:0,import:0,direct_ship:0,unassigned:60,other:0,conflict:0,flags:[]},segments:[],breaks:[],events:[]};
}
async function fixture(t,{kind='labor',handler}={}){
 const errors=[],vc=new runtime.VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));
 const dom=new runtime.JSDOM('<!doctype html><body><main><section id="root"></section></main></body>',{url:'https://fixture.invalid/shuju/',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,root=w.document.querySelector('#root'),time=clock(w),calls=[];
 let hidden=false,current=[item('A','unassigned',kind)],x=0,y=0;
 Object.defineProperty(w.document,'hidden',{get:()=>hidden});Object.defineProperty(w,'scrollX',{get:()=>x});Object.defineProperty(w,'scrollY',{get:()=>y});w.scrollTo=(a,b)=>{x=a;y=b;};
 const listeners=new Map();for(const [host,prefix,names] of [[w,'window',['focus','pageshow','pagehide']],[w.document,'document',['visibilitychange']]]){
  const add=host.addEventListener.bind(host),remove=host.removeEventListener.bind(host);
  host.addEventListener=(type,fn,...rest)=>{if(names.includes(type)){const key=prefix+':'+type;if(!listeners.has(key))listeners.set(key,new Set());listeners.get(key).add(fn);}return add(type,fn,...rest);};
  host.removeEventListener=(type,fn,...rest)=>{listeners.get(prefix+':'+type)?.delete(fn);return remove(type,fn,...rest);};
 }
 w.HTMLDialogElement.prototype.showModal=function(){this.open=true;this.querySelector('input,button')?.focus({preventScroll:true});};
 w.HTMLDialogElement.prototype.close=function(){if(this.open){this.open=false;this.dispatchEvent(new w.Event('close'));}};
 w.CK_SOP_ROLLOUT={accessControl:true};
 const report=(items=current,date=DAY,asOf=new Date(SERVER+time.elapsed).toISOString())=>({date,asOf,items:structuredClone(items),deletedItems:[],stale:[]});
 const context={report,get current(){return current;},set current(v){current=v;},calls};
 w.CKSession={ready:Promise.resolve(),user:{role:'manager'},request:async(action,data={})=>{calls.push({action,data});const custom=handler?.(action,data,context);if(custom!==undefined)return await custom;if(action==='sop_attendance_config')return {day:DAY,asOf:new Date(SERVER+time.elapsed).toISOString(),agencies:['가온','포레인'],user:{role:'manager'}};if(action==='sop_attendance_summary')return report(current,data.date);if(action==='sop_attendance_employee_people')return {items:[{id:'P-A',name:'Fixture A',employeeNo:'A',department:'bulk',enabled:true}]};if(action==='sop_access_list')return {items:[{person_id:'P-A',enabled:true,default_grant:true}]};return {ok:true};}};
 for(const source of sources)w.eval(source);
 const mount=()=>kind==='employee'?w.CKEmployee.dashboard(root):w.CKLabor(root,{field:kind==='field'}),controller=await mount();await settle();
 const $=selector=>root.querySelector(selector),change=(selector,value)=>{const node=$(selector);node.value=value;node.dispatchEvent(new w.Event(node.tagName==='INPUT'&&node.type!=='date'?'input':'change',{bubbles:true}));};
 t.after(()=>{root.__ckLabor?.destroy();root.__ckEmployee?.destroy();w.close();});
 return {w,root,time,calls,errors,listeners,controller,mount,$,change,report,get current(){return current;},set current(v){current=v;},get reads(){return calls.filter(c=>c.action==='sop_attendance_summary').length;},get rows(){return $(kind==='employee'?'[data-table]':'[data-rows]');},async hidden(value){hidden=value;w.document.dispatchEvent(new w.Event('visibilitychange'));await settle();}};
}
for(const kind of ['labor','field','employee']){
 test(kind+': applies the next live summary after 15 seconds and reports exact KST asOf',domOptions,async t=>{
  const p=await fixture(t,{kind});assert.equal(p.reads,1);assert.match(p.$('[data-report-freshness]').textContent,/作业状态更新于 2026-10-08 10:00:00.123 KST \/ 작업 상태 갱신/);
  p.current=[item('A','working',kind)];await p.time.advance(14999);assert.equal(p.reads,1);assert.match(p.rows.textContent,/未分配作业/);await p.time.advance(1);
  assert.equal(p.reads,2);assert.match(p.rows.textContent,/作业中.*TASK-A/s);assert.match(p.$('[data-report-freshness]').textContent,/10:00:15.123 KST/);assert.equal(p.$('[data-report-freshness]').dataset.freshness,'current');assert.deepEqual(p.errors,[]);
  if(kind==='field')assert.match(p.$('[data-peoplecards]').textContent,/作业中.*TASK-A/s);
  if(kind==='employee'){await p.time.advance(30000);assert.equal(p.calls.filter(c=>c.action==='sop_attendance_employee_people').length,1);assert.equal(p.calls.filter(c=>c.action==='sop_access_list').length,1);}
 });
 test(kind+': focus or visibility return refreshes immediately, with no hidden or historical polling',domOptions,async t=>{
  const p=await fixture(t,{kind});p.current=[item('A','working',kind)];p.w.dispatchEvent(new p.w.Event('focus'));await settle();assert.equal(p.reads,2);assert.match(p.rows.textContent,/作业中/);
  await p.hidden(true);await p.time.advance(45000);assert.equal(p.reads,2);p.current=[item('A','rest',kind)];await p.hidden(false);assert.equal(p.reads,3);assert.match(p.rows.textContent,/休息中/);
  p.root.hidden=true;await settle();await p.time.advance(30000);p.w.dispatchEvent(new p.w.Event('focus'));await settle();assert.equal(p.reads,3);
  p.root.hidden=false;await settle();assert.equal(p.reads,4);
  p.change('[data-date]','2026-10-07');await settle();const reads=p.reads;await p.time.advance(60000);p.w.dispatchEvent(new p.w.Event('focus'));await settle();assert.equal(p.reads,reads);assert.match(p.$('[data-report-freshness]').textContent,/历史日期/);assert.deepEqual(p.errors,[]);
 });
 test(kind+': preserves filters, focused controls, and scroll across background redraws',domOptions,async t=>{
  const p=await fixture(t,{kind});p.change('[data-search]','Fixture A');p.change(kind==='employee'?'[data-work-status-filter]':'[data-status-filter]','unassigned');
  if(kind!=='employee'){p.change('[data-company]','가온');p.change('[data-department-filter]','bulk');}
  const selector=kind==='employee'?'[data-correct="A"]':'[data-person="A"]';p.$(selector).focus();p.$('.ck-table-wrap').scrollLeft=180;p.root.scrollTop=75;p.w.scrollTo(12,300);
  await p.time.advance(15000);assert.equal(p.$('[data-search]').value,'Fixture A');assert.equal(p.$(kind==='employee'?'[data-work-status-filter]':'[data-status-filter]').value,'unassigned');assert.equal(p.w.document.activeElement,p.$(selector));assert.equal(p.$('.ck-table-wrap').scrollLeft,180);assert.equal(p.root.scrollTop,75);assert.equal(p.w.scrollX,12);assert.equal(p.w.scrollY,300);
  const search=p.$('[data-search]');search.focus();search.setSelectionRange(2,5);await p.time.advance(15000);assert.equal(p.w.document.activeElement,search);assert.equal(search.selectionStart,2);assert.equal(search.selectionEnd,5);assert.deepEqual(p.errors,[]);
 });
 test(kind+': failed refresh retains working jobs and the last successful timestamp, then recovers manually',domOptions,async t=>{
  let fail=false;const p=await fixture(t,{kind,handler:a=>a==='sop_attendance_summary'&&fail?Promise.reject(Error('Fixture offline')):undefined});p.current=[item('A','working',kind)];await p.controller.refresh();const before=p.rows.textContent,stamp=p.$('[data-report-freshness]').textContent;
  fail=true;await p.time.advance(15000);assert.equal(p.rows.textContent,before);assert.match(p.rows.textContent,/TASK-A/);assert.match(p.$('[data-report-freshness]').textContent,/刷新失败.*状态可能已过时/s);assert.equal(p.$('[data-report-freshness]').dataset.freshness,'failed');assert.ok(p.$('[data-report-freshness]').textContent.startsWith(stamp));assert.equal(p.$('[data-refresh]').disabled,false);
  fail=false;p.current=[item('A','rest',kind)];p.$('[data-refresh]').click();await settle();assert.match(p.rows.textContent,/休息中/);assert.doesNotMatch(p.$('[data-report-freshness]').textContent,/刷新失败/);assert.equal(p.$('[data-report-freshness]').dataset.freshness,'current');assert.deepEqual(p.errors,[]);
 });
 test(kind+': interval/focus reads never overlap, while explicit newer dates supersede a pending response',domOptions,async t=>{
  const pending=deferred();let slow=false;const p=await fixture(t,{kind,handler:(a,b)=>a==='sop_attendance_summary'&&slow&&b.date===DAY?pending.promise:undefined});slow=true;await p.time.advance(15000);assert.equal(p.reads,2);assert.equal(p.$('[data-refresh]').disabled,false);
  await p.time.advance(45000);p.w.dispatchEvent(new p.w.Event('focus'));await p.hidden(true);await p.hidden(false);assert.equal(p.reads,2);assert.equal(p.$('[data-report-freshness]').dataset.freshness,'stale');
  p.current=[item('LATEST','rest',kind)];p.change('[data-date]','2026-10-07');await settle();assert.equal(p.reads,3);assert.match(p.rows.textContent,/LATEST/);const stamp=p.$('[data-report-freshness]').textContent;
  pending.resolve(p.report([item('OLD','working',kind)]));await settle();assert.match(p.rows.textContent,/LATEST/);assert.doesNotMatch(p.rows.textContent,/OLD/);assert.equal(p.$('[data-date]').value,'2026-10-07');assert.equal(p.$('[data-report-freshness]').textContent,stamp);assert.equal(p.$('[data-refresh]').disabled,false);assert.deepEqual(p.errors,[]);
 });
 test(kind+': destroys timers/listeners, remounts once, and suspends/resumes bfcache safely',domOptions,async t=>{
  const p=await fixture(t,{kind});assert.equal(p.time.count,1);await p.mount();await settle();assert.equal(p.time.count,1);assert.equal(p.listeners.get('window:focus').size,1);assert.equal(p.listeners.get('document:visibilitychange').size,1);
  p.w.dispatchEvent(new p.w.PageTransitionEvent('pagehide',{persisted:true}));assert.equal(p.time.count,0);assert.equal(p.listeners.get('window:focus').size,0);assert.equal(p.listeners.get('document:visibilitychange').size,0);const before=p.reads;await p.time.advance(45000);assert.equal(p.reads,before);
  p.w.dispatchEvent(new p.w.PageTransitionEvent('pageshow',{persisted:true}));await settle();assert.equal(p.time.count,1);assert.equal(p.reads,before+1);await p.time.advance(15000);assert.equal(p.reads,before+2);
  (p.root.__ckLabor||p.root.__ckEmployee).destroy();assert.equal(p.time.count,0);for(const callbacks of p.listeners.values())assert.equal(callbacks.size,0);const reads=p.reads;await p.time.advance(60000);p.w.dispatchEvent(new p.w.Event('focus'));p.w.dispatchEvent(new p.w.PageTransitionEvent('pageshow',{persisted:true}));await settle();assert.equal(p.reads,reads);assert.deepEqual(p.errors,[]);
 });
}
for(const kind of ['labor','field'])test(kind+': any open detail/editor pauses polling, keeps the review, and resumes once after the last close',domOptions,async t=>{
 const p=await fixture(t,{kind});p.$('[data-person="A"]').click();const detail=p.$('[data-detail]').innerHTML;p.current=[item('A','working',kind)];await p.time.advance(45000);assert.equal(p.reads,1);assert.equal(p.$('[data-detail]').innerHTML,detail);assert.match(p.$('[data-report-freshness]').textContent,/自动更新已暂停/);assert.equal(p.$('[data-report-freshness]').dataset.freshness,'stale');
 p.$('[data-correct]').click();const reason=p.$('[data-editor] textarea');reason.value='Keep this unsaved review';await p.time.advance(15000);assert.equal(p.reads,1);assert.equal(reason.value,'Keep this unsaved review');p.$('[data-cancel]').click();await settle();assert.equal(p.$('[data-detail-dialog]').open,true);assert.equal(p.reads,1);
 p.$('[data-close-detail]').click();await settle();assert.equal(p.reads,2);assert.match(p.rows.textContent,/作业中/);assert.equal(p.$('[data-editor]').open,false);assert.equal(p.$('[data-detail-dialog]').open,false);assert.equal(p.w.document.activeElement,p.$('[data-person="A"]'));assert.deepEqual(p.errors,[]);
});
for(const kind of ['labor','employee'])test(kind+': a background response arriving during review is not applied and closing reads afresh',domOptions,async t=>{
 const pending=deferred();let slow=false;const p=await fixture(t,{kind,handler:a=>a==='sop_attendance_summary'&&slow?pending.promise:undefined});slow=true;await p.time.advance(15000);p.$(kind==='employee'?'[data-correct="A"]':'[data-person="A"]').click();const popup=p.$(kind==='employee'?'dialog':'[data-detail-dialog]'),html=popup.innerHTML;
 pending.resolve(p.report([item('A','working',kind)]));await settle();assert.equal(popup.open,true);assert.equal(popup.innerHTML,html);assert.match(p.rows.textContent,/未分配作业/);
 slow=false;p.current=[item('A','rest',kind)];p.$(kind==='employee'?'[data-cancel]':'[data-close-detail]').click();await settle();assert.equal(p.reads,3);assert.match(p.rows.textContent,/休息中/);assert.equal(popup.open,false);assert.deepEqual(p.errors,[]);
});
test('employee editor and actual import panel pause/resume without discarding unsaved content',domOptions,async t=>{
 const p=await fixture(t,{kind:'employee'});p.$('[data-correct="A"]').click();p.$('dialog textarea').value='Unsaved reason';await p.time.advance(30000);assert.equal(p.reads,1);assert.equal(p.$('dialog textarea').value,'Unsaved reason');p.$('[data-cancel]').click();await settle();assert.equal(p.reads,2);
 p.$('[data-import]').click();const panel=p.$('[data-import-panel]'),html=panel.innerHTML;p.$('[data-view="attendance"]').click();await settle();await p.time.advance(45000);assert.equal(p.reads,2);assert.equal(panel.innerHTML,html);assert.equal(panel.hidden,false);p.$('[data-import-panel] [data-close]').click();await settle();assert.equal(panel.hidden,true);assert.equal(p.reads,3);assert.deepEqual(p.errors,[]);
});
test('employee mutation stays paused after cancel until save settles; successful save refreshes once',domOptions,async t=>{
 const pending=deferred();const p=await fixture(t,{kind:'employee',handler:a=>a==='sop_attendance_correct'?pending.promise:undefined});p.$('[data-correct="A"]').click();p.$('dialog textarea').value='Fixture correction';p.$('dialog form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));p.$('[data-cancel]').click();await p.time.advance(30000);assert.equal(p.reads,1);
 pending.resolve({ok:true});await settle();assert.equal(p.reads,2);assert.equal(p.$('dialog').open,false);assert.deepEqual(p.errors,[]);
});
test('current-day eligibility uses server offset and stops at the actual KST day boundary',domOptions,async t=>{
 const p=await fixture(t);assert.equal(p.$('[data-date]').value,DAY);await p.time.advance(15000);assert.equal(p.reads,2,'clock intentionally differs by more than a month, but server day is today');p.change('[data-date]','2026-10-07');await settle();const before=p.reads;await p.time.advance(14*3600000);assert.equal(p.reads,before);p.change('[data-date]',DAY);await settle();const after=p.reads;await p.time.advance(30000);assert.equal(p.reads,after,'yesterday in Korea is no longer polled');assert.match(p.$('[data-report-freshness]').textContent,/历史日期/);
});
test('deferred close while a read is completing resumes at end without overlapping or retrying in a loop',domOptions,async t=>{
 const p=await fixture(t);p.controller.destroy();let paused=false,reads=0,controller;
 controller=p.w.CKAttendance.reportRefresh({root:p.root,config:{asOf:new Date(SERVER).toISOString()},alive:()=>true,visible:()=>true,date:()=>DAY,paused:()=>paused,refresh(){reads++;controller.begin();}});
 t.after(()=>controller.destroy());controller.begin();paused=true;controller.defer();paused=false;controller.check();assert.equal(reads,0,'close waits for the pending read');controller.end();assert.equal(reads,1,'completion resumes a due close immediately');controller.end();assert.equal(reads,1,'the resumed request clears the pending flag');
});
test('closing review before the pending response arrives applies it once without another read',domOptions,async t=>{
 const pending=deferred();let slow=false;const p=await fixture(t,{handler:a=>a==='sop_attendance_summary'&&slow?pending.promise:undefined});slow=true;await p.time.advance(15000);p.$('[data-person="A"]').click();p.$('[data-close-detail]').click();await settle();assert.equal(p.reads,2);pending.resolve(p.report([item('A','working')]));await settle();assert.equal(p.reads,2);assert.match(p.rows.textContent,/作业中/);assert.equal(p.$('[data-detail-dialog]').open,false);assert.deepEqual(p.errors,[]);
});
test('ordinary response latency does not accidentally stretch the 15-second interval to 30 seconds',domOptions,async t=>{
 const pending=deferred();let slow=false;const p=await fixture(t,{handler:a=>a==='sop_attendance_summary'&&slow?pending.promise:undefined});slow=true;await p.time.advance(15000);assert.equal(p.reads,2);await p.time.advance(500);pending.resolve(p.report());await settle();slow=false;await p.time.advance(14500);assert.equal(p.reads,3);
});
test('a failed slow read waits for the next interval rather than retrying immediately in a loop',domOptions,async t=>{
 const pending=deferred();let slow=false;const p=await fixture(t,{handler:a=>a==='sop_attendance_summary'&&slow?pending.promise:undefined});slow=true;await p.time.advance(45000);assert.equal(p.reads,2);pending.reject(Error('Fixture offline'));await settle();assert.equal(p.reads,2);assert.equal(p.$('[data-report-freshness]').dataset.freshness,'failed');slow=false;await p.time.advance(15000);assert.equal(p.reads,3);
});
