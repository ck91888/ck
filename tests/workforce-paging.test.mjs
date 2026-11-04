// Local synthetic DOM tests. Pagination is presentation only; no worker/database access.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {report,DAY} from './workforce-paging-data.mjs';
const {JSDOM,VirtualConsole}=await import(process.env.CK_JSDOM_MODULE),source=name=>fs.readFileSync(new URL('../shared/'+name,import.meta.url),'utf8');
const tick=()=>new Promise(r=>setImmediate(r));
async function fixture(t,{count=121,field=false,role='manager',data=report(count),handler}={}){
 const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));const dom=new JSDOM('<!doctype html><main><section id="root"></section></main>',{url:'https://paging.invalid/shuju/',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,root=w.document.querySelector('#root'),calls=[],downloads=[];let current=data;
 t.after(()=>{root.__ckLabor?.destroy();w.close();});w.scrollTo=()=>{};w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;this.dispatchEvent(new w.Event('close'));};
 w.CKSession={ready:Promise.resolve(),user:{role,id:'SYN-MANAGER'},request:async(action,payload)=>{calls.push({action,payload});const custom=handler?.(action,payload);if(custom!==undefined)return custom;if(action==='sop_attendance_config')return {day:DAY,asOf:data.asOf,agencies:['가온','포레인'],user:{role,id:'SYN-MANAGER'}};if(action==='sop_attendance_summary')return structuredClone(current);throw Error('Unexpected action '+action);}};
 w.URL.createObjectURL=blob=>{downloads.push(blob);return 'blob:synthetic';};w.URL.revokeObjectURL=()=>{};w.HTMLAnchorElement.prototype.click=function(){};
 w.eval(source('document-labels.js'));w.eval(source('attendance-ui.js'));const controller=await w.CKLabor(root,{field});
 const $=name=>root.querySelector('[data-'+name+']'),ids=(kind='rows')=>[...$(kind).querySelectorAll('[data-person]')].map(b=>b.dataset.person),change=(name,value)=>{const n=$(name);n.value=value;n.dispatchEvent(new w.Event(name==='search'?'input':'change',{bubbles:true}));};
 return {w,root,$,ids,change,controller,calls,downloads,errors,get data(){return current;},set data(v){current=v;}};
}
test('all pages retain original order without duplicates, extra reads or page-sized aggregates',async t=>{
 const f=await fixture(t),all=[];do{all.push(...f.ids());if(f.$('people-next').disabled)break;f.$('people-next').click();}while(true);
 assert.deepEqual(all,f.data.items.map(x=>x.record.id));assert.equal(new Set(all).size,121);assert.equal(f.calls.length,2);assert.match(f.$('summary').textContent,/当天签到 \/ 출근121 人/);assert.equal(f.ids().length,21);
 assert.match(f.$('people-range').textContent,/101–121 \/ 121/);assert.equal(f.$('people-next').disabled,true);f.$('people-prev').click();assert.equal(f.ids()[0],'SYN-00050');assert.equal(f.$('people-prev').disabled,false);
});
test('company, department, status and raw case-insensitive search filter before paging and reset to first page',async t=>{
 const f=await fixture(t,{count:241});f.$('people-next').click();f.change('company','가온');assert.equal(f.ids()[0],'SYN-00000');assert.match(f.$('people-range').textContent,/\/ 121 人/);
 f.change('department-filter','bulk');f.change('status-filter','unassigned');const expected=f.data.items.filter(x=>x.record.agency==='가온'&&x.record.managementDepartment==='bulk'&&x.status==='unassigned');assert.deepEqual(f.ids(),expected.map(x=>x.record.id));
 f.change('search','SYNTHETIC PERSON 001');assert.deepEqual(f.ids(),expected.filter(x=>x.record.name.toLowerCase().includes('synthetic person 001')).map(x=>x.record.id));f.change('search',' synthetic person ');assert.deepEqual(f.ids(),[]);assert.equal(f.$('people-next').disabled,true);assert.match(f.$('people-range').textContent,/0–0 \/ 0/);
});
test('job references, historic segments, employee badges and escaped names remain searchable without changing stored values',async t=>{
 const data=report(105);data.items[103].record.name='Synthetic "<&\' 한글';data.items[103].segments=[{id:'H-SYN',no:'HISTORY-ONLY',department:'import',start:DAY+'T08:00:00+09:00',end:DAY+'T09:00:00+09:00'}];const snapshot=JSON.stringify(data),f=await fixture(t,{data});
 f.change('search','HISTORY-ONLY');assert.deepEqual(f.ids(),['SYN-00103']);assert.match(f.$('rows').textContent,/Synthetic "<&' 한글/);assert.equal(f.$('rows').querySelector('script,img'),null);f.change('search','TASK-SYN-00001');assert.deepEqual(f.ids(),['SYN-00001']);f.change('search','EMP-SYN-00099');assert.deepEqual(f.ids(),['SYN-00099']);assert.equal(JSON.stringify(f.data),snapshot);
});
test('field cards and table use the same page and keep field write/export boundaries',async t=>{
 const f=await fixture(t,{count:101,field:true,role:'dispatcher'});assert.deepEqual(f.ids('peoplecards'),f.ids());f.$('people-next').click();assert.deepEqual(f.ids('peoplecards'),f.data.items.slice(50,100).map(x=>x.record.id));assert.deepEqual(f.ids('peoplecards'),f.ids());
 for(const selector of ['[data-export]','[data-void]','[data-department]','[data-show-deleted]'])assert.equal(f.root.querySelector(selector),null);f.$('people-next').click();assert.deepEqual(f.ids(),['SYN-00100']);assert.equal(f.root.querySelectorAll('[data-rows] tr').length,1);
});
test('hidden deleted records have no rows, their independent pages preserve details and full deleted count',async t=>{
 const f=await fixture(t,{count:101,data:report(101,{deleted:121})});assert.equal(f.$('deleted-list').childElementCount,0);assert.match(f.$('deleted-count').textContent,/121/);f.$('people-next').click();const normal=f.ids();f.$('show-deleted').checked=true;f.$('show-deleted').dispatchEvent(new f.w.Event('change'));
 assert.equal(f.$('deleted').hidden,false);assert.equal(f.ids('deleted-list').length,50);f.$('deleted-next').click();assert.equal(f.ids('deleted-list')[0],'SYN-00151');assert.deepEqual(f.ids(),normal);assert.equal(f.ids('deleted-list')[0],'SYN-00151');
 f.$('deleted-list').querySelector('[data-person]').click();assert.match(f.$('detail').textContent,/Synthetic person 00151/);assert.equal(f.$('detail-dialog').open,true);f.$('close-detail').click();f.$('show-deleted').checked=false;f.$('show-deleted').dispatchEvent(new f.w.Event('change'));assert.equal(f.$('deleted-list').childElementCount,0);
});
test('details and mutation cancellation retain page, original person and focus',async t=>{
 const f=await fixture(t);f.$('people-next').click();const button=f.$('rows').querySelector('[data-person="SYN-00050"]');button.focus();button.click();assert.equal(f.$('detail-dialog').dataset.recordId,'SYN-00050');f.$('close-detail').click();assert.equal(f.w.document.activeElement.dataset.person,'SYN-00050');assert.equal(f.ids()[0],'SYN-00050');
 f.$('rows').querySelector('[data-department="SYN-00050"]').click();assert.equal(f.$('editor').open,true);assert.match(f.$('fields').textContent,/Synthetic person 00050/);f.$('cancel').click();assert.equal(f.ids()[0],'SYN-00050');assert.equal(f.calls.length,2);assert.deepEqual(f.errors,[]);
});
test('native pager buttons are keyboard focusable and crossing the last page keeps a usable focus target',async t=>{
 const f=await fixture(t,{count:51}),next=f.$('people-next');assert.equal(next.tagName,'BUTTON');assert.equal(next.type,'button');assert.equal(next.closest('nav').getAttribute('aria-label'),'签到人员分页 / 출근 인원 페이지');assert.equal(f.$('people-range').getAttribute('aria-live'),'polite');next.focus();next.click();assert.equal(f.w.document.activeElement,f.$('people-prev'));f.$('people-prev').click();assert.equal(f.w.document.activeElement,f.$('people-next'));
});
test('refresh preserves page, clamps after a smaller result and keeps previous rows on failure',async t=>{
 let fail=false;const f=await fixture(t,{handler:action=>{if(action==='sop_attendance_summary'&&fail)throw Error('Synthetic read failure');}});f.$('people-next').click();await f.controller.refresh();assert.equal(f.ids()[0],'SYN-00050');fail=true;assert.equal(await f.controller.refresh(),false);assert.equal(f.ids()[0],'SYN-00050');assert.match(f.$('error').textContent,/Synthetic read failure/);fail=false;f.data=report(3);await f.controller.refresh();assert.deepEqual(f.ids(),f.data.items.map(x=>x.record.id));assert.equal(f.$('people-prev').disabled,true);
});
test('new date resets both page cursors and late old results cannot replace current date',async t=>{
 let resolve;const old=new Promise(r=>resolve=r),f=await fixture(t);f.$('people-next').click();const request=f.w.CKSession.request;f.w.CKSession.request=(a,p)=>a==='sop_attendance_summary'&&p.date===DAY?old:request(a,p);const pending=f.controller.refresh();f.change('date','2026-11-03');await tick();assert.equal(f.ids()[0],'SYN-00000');resolve(report(1));await pending;assert.equal(f.ids().length,50);assert.match(f.$('people-range').textContent,/1–50/);
});
test('CSV includes every filtered person beyond the current page and excludes deleted records',async t=>{
 const f=await fixture(t,{data:report(121,{deleted:2})});f.$('people-next').click();f.$('export').click();const read=blob=>new Promise(resolve=>{const reader=new f.w.FileReader();reader.onload=()=>resolve(reader.result);reader.readAsText(blob);});const csv=await read(f.downloads[0]);assert.equal(csv.split('\r\n').length,122);assert.match(csv,/Synthetic person 00120/);assert.doesNotMatch(csv,/Synthetic person 00121/);f.change('status-filter','working');f.$('export').click();const expected=f.data.items.filter(x=>x.status==='working');assert.equal((await read(f.downloads[1])).split('\r\n').length,expected.length+1);assert.equal(f.calls.length,2);
});
for(const role of ['attendance_reader','dispatcher'])test(role+' paged rows retain existing action permissions',async t=>{
 const f=await fixture(t,{role});f.$('people-next').click();assert.equal(f.root.querySelector('[data-void],[data-show-deleted],[data-restore],[data-register]'),null);assert.equal(!!f.root.querySelector('[data-department]'),role==='dispatcher');assert.equal(f.ids().length,50);assert.equal(f.calls.length,2);
});
