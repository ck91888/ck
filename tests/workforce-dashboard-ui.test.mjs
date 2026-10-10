// Focused DOM behavior checks with fictional data, never a live endpoint.
// CK_JSDOM_MODULE=/path/to/jsdom/lib/api.js node --test tests/workforce-dashboard-ui.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const labelsSource=readFileSync(new URL('../shared/document-labels.js',import.meta.url),'utf8');
const laborSource=readFileSync(new URL('../shared/attendance-ui.js',import.meta.url),'utf8');
const employeeSource=readFileSync(new URL('../shared/employee-attendance.js',import.meta.url),'utf8');
const importSource=readFileSync(new URL('../shared/employee-import.js',import.meta.url),'utf8');
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const domOptions={skip:!runtime};
const DAY='2026-10-08';
const item=(id,status='unassigned',department='')=>({record:{id,name:'Fixture '+id,badgeId:'DA-'+id,agency:'가온',day:DAY,inAt:DAY+'T08:00:00+09:00',outAt:status==='out'?DAY+'T09:00:00+09:00':'',version:1,managementDepartment:department,personType:'daily'},status,currentJobs:status==='working'?[{id:'J-'+id,no:'TASK-'+id,department:'import'}]:[],canVoid:status==='unassigned',voidBlockedReason:status==='unassigned'?'':'已有作业或休息记录 / 작업·휴식 기록이 있습니다',totals:{presence:60,bulk:0,direct_ship:0,import:0,rest:0,unassigned:60,other:0,conflict:0,flags:[]},segments:[],breaks:[],events:[]});
const report=(items,date=DAY,deletedItems=[])=>({date,asOf:date+'T10:00:00+09:00',items,deletedItems,stale:[]});
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
const tick=()=>new Promise(r=>setTimeout(r,0));
async function until(fn){for(let n=0;n<100;n++){if(fn())return;await tick();}throw Error('Timed out waiting for fixture condition');}
function context(){const w={};w.window=w;vm.runInNewContext(labelsSource,w);vm.runInNewContext(laborSource,w);return w;}
test('shared current status uses live status, escapes task identifiers, and never infers from historic totals',()=>{
 const w=context(),x=item('A','working');x.currentJobs[0].no='<img src=x onerror=alert(1)>';x.totals.unassigned=500;
 const html=w.CKAttendance.statusMarkup(x);assert.match(html,/作业中 \/ 작업 중/);assert.match(html,/进口 \/ 수입/);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|未分配作业/);
 for(const state of ['out','rest','unassigned']){const html=w.CKAttendance.statusMarkup({...x,status:state});assert.match(html,new RegExp('data-status="'+state+'"'));assert.doesNotMatch(html,/TASK-|&lt;img/);}
 assert.match(w.CKAttendance.statusMarkup({status:'bad" onclick="x'}),/data-status="unknown"/);
});
async function fixture(t,{role='manager',kind='labor',field=false,initial=report([item('A'),item('B','working','bulk'),item('C','rest'),item('D','out')]),handler}={}){
 const {JSDOM,VirtualConsole}=runtime,errors=[],virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',x=>errors.push(x.message));
 const dom=new JSDOM('<!doctype html><body><main><section id="root"></section></main></body>',{url:'https://fixture.invalid/shuju/',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole}),w=dom.window,root=w.document.getElementById('root'),calls=[],scroll=[];
 t.after(()=>{root.__ckLabor?.destroy();root.__ckEmployee?.destroy();w.close();});
 w.scrollTo=(x,y)=>scroll.push([x,y]);w.eval(importSource);w.CK_SOP_ROLLOUT={};
 w.HTMLDialogElement.prototype.showModal=function(){this.open=true;this.querySelector('button,input,select,textarea')?.focus({preventScroll:true});};
 w.HTMLDialogElement.prototype.close=function(){if(!this.open)return;this.open=false;this.dispatchEvent(new w.Event('close'));};
 let current=initial;
 w.CKSession={ready:Promise.resolve(),user:{role},request:async(action,data={})=>{calls.push({action,data});const custom=handler?.(action,data,{get current(){return current;},set current(v){current=v;},calls});if(custom!==undefined)return await custom;if(action==='sop_attendance_config')return {day:DAY,agencies:['가온','포레인'],user:{role,id:'QA',name:'Fixture manager'}};if(action==='sop_attendance_summary')return structuredClone(current);if(action==='sop_attendance_employee_people'||action==='sop_attendance_people')return {items:[]};return {ok:true};}};
 w.eval(labelsSource);w.eval(laborSource.replace("import('/shared/labor-department.js')",'Promise.resolve({jobDefinitions:{}})'));w.eval(employeeSource);
 const mount=()=>kind==='employee'?w.CKEmployee.dashboard(root):w.CKLabor(root,{field});
 const controller=await mount();
 const $=s=>root.querySelector(s),all=s=>[...root.querySelectorAll(s)],change=(s,value)=>{const node=$(s);node.value=value;node.dispatchEvent(new w.Event(node.tagName==='INPUT'&&node.type!=='date'?'input':'change',{bubbles:true}));};
 return {w,root,$,all,calls,errors,scroll,mount,controller,change,get current(){return current;},set current(v){current=v;}};
}
test('labor combines company, department and current-unassigned filters, ignoring past unassigned minutes',domOptions,async t=>{
 const p=await fixture(t);p.change('[data-status-filter]','unassigned');assert.equal(p.all('[data-rows] tr').length,1);assert.match(p.$('[data-rows]').textContent,/Fixture A/);assert.doesNotMatch(p.$('[data-rows]').textContent,/Fixture B|Fixture C|Fixture D/);
 p.change('[data-department-filter]','bulk');assert.match(p.$('[data-rows]').textContent,/暂无/);p.change('[data-status-filter]','');assert.match(p.$('[data-rows]').textContent,/Fixture B/);
 p.change('[data-company]','포레인');assert.match(p.$('[data-rows]').textContent,/暂无/);assert.deepEqual(p.errors,[]);
});
test('details use a distinct modal; Close and Escape preserve filters, horizontal scroll and return focus',domOptions,async t=>{
 const p=await fixture(t);p.change('[data-search]','Fixture A');const origin=p.$('[data-person="A"]');origin.focus();p.$('.ck-table-wrap').scrollLeft=175;origin.click();
 assert.equal(p.$('[data-detail-dialog]').open,true);assert.equal(p.$('[data-editor]').open,false);assert.match(p.$('[data-detail]').textContent,/当日明细/);
 p.$('[data-close-detail]').click();assert.equal(p.$('[data-detail-dialog]').open,false);assert.equal(p.$('[data-search]').value,'Fixture A');assert.equal(p.$('.ck-table-wrap').scrollLeft,175);assert.equal(p.w.document.activeElement,origin);
 origin.click();p.$('[data-detail-dialog]').dispatchEvent(new p.w.Event('cancel',{cancelable:true}));assert.equal(p.$('[data-detail-dialog]').open,false);assert.equal(p.w.document.activeElement,origin);assert.deepEqual(p.errors,[]);
});
test('editing a department is independent of jobs; editor cancel leaves detail modal open',domOptions,async t=>{
 const p=await fixture(t,{handler:(action,data,ctx)=>{if(action==='sop_attendance_department'){ctx.current.items[0].record.managementDepartment=data.department;ctx.current.items[0].record.version++;return {ok:true};}}});
 p.$('[data-person="A"]').click();p.$('[data-detail] [data-department]').click();assert.equal(p.$('[data-editor]').open,true);p.$('[data-editor]').dispatchEvent(new p.w.Event('cancel',{cancelable:true}));assert.equal(p.$('[data-detail-dialog]').open,true);
 p.$('[data-detail] [data-department]').click();p.change('[data-editor] select','direct_ship');p.$('[data-editor] form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));await until(()=>!p.$('[data-editor]').open&&p.$('[data-detail]').textContent.includes('代发'));
 const call=p.calls.find(x=>x.action==='sop_attendance_department');assert.equal(call.data.id,'A');assert.equal(call.data.version,1);assert.equal(call.data.department,'direct_ship');assert.equal(p.current.items[0].status,'unassigned');assert.equal(p.current.items[0].totals.unassigned,60);assert.equal(p.current.items[0].record.inAt,DAY+'T08:00:00+09:00');
});
test('delete/restore confirm with a reason, preserve original identity, and exclude deleted rows from active counts',domOptions,async t=>{
 const p=await fixture(t,{handler:(action,data,ctx)=>{if(action==='sop_attendance_void'){const x=ctx.current.items.shift();x.record={...x.record,voided:true,voidReason:data.reason,voidedAt:DAY+'T10:00:00+09:00',version:2};ctx.current.deletedItems.push(x);return {ok:true,record:x.record};}if(action==='sop_attendance_restore'){const x=ctx.current.deletedItems.shift();x.record={...x.record,voided:false,version:3};ctx.current.items.unshift(x);return {ok:true,record:x.record};}}});
 p.$('[data-void="A"]').click();assert.match(p.$('[data-editor]').textContent,/不计入当天人数、工时和导出/);assert.equal(p.calls.some(x=>x.action==='sop_attendance_void'),false);
 p.change('[data-editor] textarea','Fixture mistaken checkin');p.$('[data-editor] form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));await until(()=>p.all('[data-rows] [data-person]').length===3);
 assert.equal(p.$('[data-summary] strong').textContent,'3 人');assert.equal(p.$('[data-rows] [data-person="A"]'),null);p.$('[data-show-deleted]').click();assert.equal(p.$('[data-deleted]').hidden,false);assert.match(p.$('[data-deleted]').textContent,/Fixture mistaken checkin/);
 p.$('[data-restore="A"]').click();p.change('[data-editor] textarea','Fixture restore original');p.$('[data-editor] form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));await until(()=>p.all('[data-rows] [data-person]').length===4);
 const restore=p.calls.find(x=>x.action==='sop_attendance_restore');assert.equal(restore.data.id,'A');assert.equal(restore.data.version,2);assert.equal(p.current.items[0].record.badgeId,'DA-A');assert.equal(p.current.items[0].record.inAt,DAY+'T08:00:00+09:00');
});
test('pending mutation locks repeated submissions; close or navigation cannot reopen a late dialog',domOptions,async t=>{
 const pending=deferred();const p=await fixture(t,{handler:action=>action==='sop_attendance_department'?pending.promise:undefined});p.$('[data-department="A"]').click();const form=p.$('[data-editor] form');p.change('[data-editor] select','bulk');form.dispatchEvent(new p.w.Event('submit',{cancelable:true}));form.dispatchEvent(new p.w.Event('submit',{cancelable:true}));assert.equal(p.calls.filter(x=>x.action==='sop_attendance_department').length,1);assert.equal(p.$('[data-editor] [type=submit]').disabled,true);
 p.$('[data-cancel]').click();p.root.hidden=true;await tick();pending.resolve({ok:true});await tick();await tick();assert.equal(p.$('[data-editor]').open,false);assert.equal(p.$('[data-detail-dialog]').open,false);assert.deepEqual(p.errors,[]);
});
test('lost-response retry reuses request identity while changed payload gets a fresh identity',domOptions,async t=>{
 let attempt=0;const p=await fixture(t,{handler:action=>{if(action==='sop_attendance_department'){attempt++;if(attempt===1)throw new Error('Fixture network response lost');return {ok:true};}}});p.$('[data-department="A"]').click();p.change('[data-editor] select','bulk');p.$('[data-editor] form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));await until(()=>!p.$('[data-editor] [role=alert]').hidden);
 p.$('[data-editor] form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));await until(()=>!p.$('[data-editor]').open);const calls=p.calls.filter(x=>x.action==='sop_attendance_department');assert.equal(calls.length,2);assert.equal(calls[0].data.client_req_id,calls[1].data.client_req_id);
});
test('permissions hide delete/restore from dispatchers/viewers and all management from field view',domOptions,async t=>{
 for(const [role,field,mark] of [['dispatcher',false,true],['viewer',false,false],['manager',true,false]]){const p=await fixture(t,{role,field});assert.equal(!!p.$('[data-department]'),mark);assert.equal(p.$('[data-void]'),null);assert.equal(p.$('[data-show-deleted]'),null);p.$('[data-person="A"]').click();assert.equal(p.$('[data-detail] [data-void]'),null);assert.equal(!!p.$('[data-detail] [data-department]'),mark);}
});
test('records with related work cannot be deleted and display the backend blocking reason in detail',domOptions,async t=>{
 const p=await fixture(t);assert.equal(p.$('[data-void="B"]').disabled,true);p.$('[data-person="B"]').click();assert.match(p.$('[data-detail]').textContent,/无法删除.*已有作业或休息记录/s);assert.equal(p.$('[data-detail] [data-void]').disabled,true);
});
test('a later labor date response wins over a slow earlier refresh',domOptions,async t=>{
 const old=deferred(),next=deferred();let active=false;const p=await fixture(t,{handler:(a,b)=>a==='sop_attendance_summary'&&active?(b.date==='2026-10-07'?old.promise:next.promise):undefined});active=true;
 p.change('[data-date]','2026-10-07');p.change('[data-date]','2026-10-06');next.resolve(report([item('NEW')],'2026-10-06'));await until(()=>p.$('[data-rows]').textContent.includes('NEW'));old.resolve(report([item('OLD')],'2026-10-07'));await tick();await tick();assert.match(p.$('[data-rows]').textContent,/NEW/);assert.doesNotMatch(p.$('[data-rows]').textContent,/OLD/);assert.equal(p.$('[data-date]').value,'2026-10-06');assert.equal(p.$('[data-refresh]').disabled,false);
});
test('employee shows actual current jobs separately from employment department, with status filtering',domOptions,async t=>{
 const fixtures=[item('A','working'),item('B','rest'),item('C','out'),item('D')];fixtures.forEach(x=>{x.record.personType='employee';x.record.employeeNo=x.record.id;x.record.department='bulk';});
 const p=await fixture(t,{kind:'employee',initial:report(fixtures)});const cells=p.all('[data-table] tbody tr')[0].querySelectorAll('td');assert.equal(cells[1].textContent,'大货 / 대량');assert.match(cells[2].textContent,/作业中.*进口.*TASK-A/s);assert.ok(p.$('[data-import-panel] [data-status]'));p.change('[data-work-status-filter]','unassigned');assert.equal(p.all('[data-table] tbody tr').length,1);assert.match(p.$('[data-table]').textContent,/Fixture D/);assert.deepEqual(p.errors,[]);
});
test('employee date races cannot overwrite the latest selected day',domOptions,async t=>{
 const old=deferred(),next=deferred();let active=false;const p=await fixture(t,{kind:'employee',handler:(a,b)=>a==='sop_attendance_summary'&&active?(b.date==='2026-10-07'?old.promise:next.promise):undefined});active=true;
 p.change('[data-date]','2026-10-07');p.change('[data-date]','2026-10-06');next.resolve(report([item('NEW')],'2026-10-06'));await until(()=>p.$('[data-table]').textContent.includes('NEW'));old.resolve(report([item('OLD')],'2026-10-07'));await tick();await tick();assert.match(p.$('[data-table]').textContent,/NEW/);assert.doesNotMatch(p.$('[data-table]').textContent,/OLD/);
});
test('CSV keeps original attendance values and appends management/current-work columns, excluding deleted rows',domOptions,async t=>{
 for(const kind of ['labor','employee']){const a=item('A','working','bulk'),z=item('DELETED');z.record.voided=true;const p=await fixture(t,{kind,initial:report([a],DAY,[z])});let exported;p.w.URL.createObjectURL=blob=>{exported=blob;return 'blob:fixture';};p.w.URL.revokeObjectURL=()=>{};p.w.HTMLAnchorElement.prototype.click=function(){};p.$('[data-export]').click();const text=await new Promise((resolve,reject)=>{const reader=new p.w.FileReader();reader.onload=()=>resolve(reader.result);reader.onerror=reject;reader.readAsText(exported);});assert.match(text,/当前作业状态/);assert.match(text,/TASK-A/);assert.doesNotMatch(text,/DELETED/);assert.match(text,/"60"/);assert.match(text,/2026-10-08T08:00:00\+09:00/);}
});
test('a closed editor can retry an uncertain identical mutation without a second operation identity',domOptions,async t=>{
 let attempt=0;const p=await fixture(t,{handler:action=>{if(action==='sop_attendance_department'){attempt++;throw Error('Fixture response lost '+attempt);}}});
 const save=async department=>{p.$('[data-department="A"]').click();p.change('[data-editor] select',department);p.$('[data-editor] form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));await until(()=>!p.$('[data-editor] [role=alert]').hidden);p.$('[data-cancel]').click();};
 await save('bulk');await save('bulk');await save('import');const calls=p.calls.filter(x=>x.action==='sop_attendance_department');assert.equal(calls.length,3);assert.equal(calls[0].data.client_req_id,calls[1].data.client_req_id);assert.notEqual(calls[1].data.client_req_id,calls[2].data.client_req_id);
});
test('reopening either dashboard invalidates an older config response and stale controls',domOptions,async t=>{
 for(const kind of ['labor','employee']){const old=deferred();let configCalls=0;const p=await fixture(t,{kind,handler:action=>{if(action==='sop_attendance_config'){configCalls++;if(configCalls===2)return old.promise;}}});
  const first=p.mount();await until(()=>configCalls===2);const latest=await p.mount();p.change('[data-search]','Fixture A');old.resolve({day:'2026-10-01',agencies:['가온'],user:{id:'OLD',name:'Old fixture',role:'manager'}});await first;assert.equal(p.$('[data-date]').value,DAY);assert.equal(p.$('[data-search]').value,'Fixture A');assert.ok(latest.refresh);assert.deepEqual(p.errors,[]);
 }
});
test('late failed save after remount cannot place an error into the new dashboard',domOptions,async t=>{
 const pending=deferred();const p=await fixture(t,{handler:action=>action==='sop_attendance_department'?pending.promise:undefined});p.$('[data-department="A"]').click();p.$('[data-editor] form').dispatchEvent(new p.w.Event('submit',{cancelable:true}));await p.mount();pending.reject(Error('Old fixture failure'));await tick();await tick();assert.equal(p.$('[data-editor]').open,false);assert.equal(p.$('[data-error]').hidden,true);assert.doesNotMatch(p.root.textContent,/Old fixture failure/);
});
test('employee correction locks duplicate submits and cannot reopen after dismissal while saving',domOptions,async t=>{
 const pending=deferred();const p=await fixture(t,{kind:'employee',handler:action=>action==='sop_attendance_correct'?pending.promise:undefined});p.$('[data-correct="A"]').click();p.change('dialog textarea','Fixture correction');const form=p.$('dialog form');form.dispatchEvent(new p.w.Event('submit',{cancelable:true}));form.dispatchEvent(new p.w.Event('submit',{cancelable:true}));assert.equal(p.calls.filter(x=>x.action==='sop_attendance_correct').length,1);p.$('[data-cancel]').click();pending.resolve({ok:true});await tick();await tick();assert.equal(p.$('dialog').open,false);assert.deepEqual(p.errors,[]);
});
test('business and dispatcher searches, timeline, and exports use the same safe job identity',domOptions,async t=>{
 const raw='JOB-01234567-89ab-4cde-8123-456789abcdef',other='JOB-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
 const a=item('A','working'),b=item('B','working');
 a.currentJobs=[{id:raw,no:raw,business_no:'EXT-7301',display_business_no:'EXT-7301',has_business_reference:true,dispatcher_name:'实际派审员 <甲>',job_type:'bulk_op',department:'bulk'}];
 a.segments=[{jobId:other,jobNo:other,business_no:'',display_business_no:'',has_business_reference:false,job_type:'inventory',job_title:'盘点',job_department:'bulk',job_started_at:DAY+'T07:30:00+09:00',dispatcher_name:'',start:DAY+'T08:00:00+09:00',end:DAY+'T08:30:00+09:00',reason:'job_completed',department:'bulk'}];
 b.currentJobs=[{id:'JOB-b',business_no:'EXT-7302',dispatcher_name:'另一位派审员',department:'import'}];
 const p=await fixture(t,{initial:report([a,b])});
 assert.match(p.$('[data-rows]').textContent,/EXT-7301.*派审员.*实际派审员 <甲>/s);assert.ok(!p.$('[data-rows]').textContent.includes(raw));assert.equal(p.$('[data-rows]').querySelector('甲'),null);
 p.change('[data-search]','实际派审员');assert.equal(p.all('[data-rows] [data-person]').length,1);assert.match(p.$('[data-rows]').textContent,/Fixture A/);
 p.$('[data-person="A"]').click();const timeline=p.$('.ck-timeline');assert.match(timeline.textContent,/盘点/);assert.equal((timeline.textContent.match(/盘点/g)||[]).length,1);assert.equal((timeline.textContent.match(/大货/g)||[]).length,1);assert.match(timeline.textContent,/2026-10-08 07:30 KST/);assert.match(timeline.textContent,/派审员：未记录 \/ 미기록/);assert.match(timeline.textContent,/作业已完成/);assert.ok(!timeline.textContent.includes(other));assert.ok(!timeline.textContent.includes('job_completed'));
 p.$('[data-close-detail]').click();p.change('[data-search]','EXT-7302');assert.equal(p.all('[data-rows] [data-person]').length,1);assert.match(p.$('[data-rows]').textContent,/Fixture B/);
 p.change('[data-search]','EXT-7301');let exported;p.w.URL.createObjectURL=blob=>{exported=blob;return 'blob:fixture';};p.w.URL.revokeObjectURL=()=>{};p.w.HTMLAnchorElement.prototype.click=function(){};p.$('[data-export]').click();const csv=await new Promise(resolve=>{const r=new p.w.FileReader();r.onload=()=>resolve(r.result);r.readAsText(exported);});assert.match(csv,/派审员 \/ 배정·검수 담당자/);assert.match(csv,/EXT-7301/);assert.match(csv,/实际派审员 <甲>/);assert.ok(!csv.includes(raw));assert.ok(!csv.includes('Fixture manager'));
});
test('employee attendance searches and CSV use business numbers and the saved dispatcher',domOptions,async t=>{
 const x=item('A','working');x.record.personType='employee';x.record.employeeNo='EMP-1';x.currentJobs=[{id:'JOB-employee',no:'JOB-employee',business_no:'EMP-WORK-900',department:'direct_ship',dispatcher_name:'保存的派审员'}];
 const p=await fixture(t,{kind:'employee',initial:report([x])});p.change('[data-search]','EMP-WORK-900');assert.match(p.$('[data-table]').textContent,/Fixture A/);p.change('[data-search]','保存的派审员');assert.match(p.$('[data-table]').textContent,/Fixture A/);
 let exported;p.w.URL.createObjectURL=blob=>{exported=blob;return 'blob:fixture';};p.w.URL.revokeObjectURL=()=>{};p.w.HTMLAnchorElement.prototype.click=function(){};p.$('[data-export]').click();const csv=await new Promise(resolve=>{const r=new p.w.FileReader();r.onload=()=>resolve(r.result);r.readAsText(exported);});assert.match(csv,/EMP-WORK-900/);assert.match(csv,/保存的派审员/);assert.ok(!csv.includes('JOB-employee'));
});
