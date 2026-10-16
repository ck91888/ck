// Legacy app rendering only, fictional data and stubbed API; no live endpoints.
// CK_JSDOM_MODULE=/tmp/ck-workforce-deps/node_modules/jsdom/lib/api.js node --test tests/job-display-apps.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const read=file=>fs.readFileSync(new URL('../'+file,import.meta.url),'utf8');
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const domOptions={skip:!runtime};
const ID='JOB-11111111-2222-4333-8444-555555555555';
const OTHER_ID='JOB-99999999-2222-4333-8444-555555555555';
const fixtureJob=(extra={})=>({id:ID,job_id:ID,job_type:'inbound_return',status:'working',job_status:'working',business_no:'IB-20261008-0042',job_label:'IB-20261008-0042',display_no:ID,department:'import',dispatcher_name:'真实派审员 <陈>',dispatcher_id:'MANAGER-REAL',created_by:'CREATOR-NOT-DISPATCHER',lead_id:'LEAD-NOT-DISPATCHER',worker_id:'EMP-FIXTURE',worker_name:'虚拟员工',segment_id:'SEG-FIXTURE',joined_at:'2026-10-07T23:00:00Z',started_at:'2026-10-07T23:00:00Z',created_at:'2026-10-07T23:00:00Z',...extra});
const tick=()=>new Promise(r=>setTimeout(r,0));
test('narrow dashboard header grows for wrapped identity controls instead of covering job tabs',()=>{
 const css=read('shuju/style.css');
 assert.match(css,/@media\s*\(max-width:\s*700px\)[\s\S]*?\.app-header\s*\{[^}]*height:\s*auto/);
 assert.match(css,/@media\s*\(max-width:\s*700px\)[\s\S]*?\.app-header \.header-right\s*\{[^}]*flex-wrap:\s*wrap/);
});
function fixture(t,app){
 const errors=[],virtualConsole=new runtime.VirtualConsole();virtualConsole.on('jsdomError',e=>errors.push(e.message));
 const dom=new runtime.JSDOM(read(app+'/index.html'),{url:'https://fixture.invalid/'+app+'/',runScripts:'outside-only',virtualConsole});
 const w=dom.window,alerts=[],calls=[],add=w.addEventListener;
 w.addEventListener=function(name,...rest){if(name!=='DOMContentLoaded')return add.call(this,name,...rest);};
 w.alert=x=>alerts.push(String(x));w.confirm=()=>true;w.fetch=()=>{throw Error('Fixture must not use network');};
 w.eval(read('shared/document-labels.js'));
 if(app!=='sop')w.eval(read(app+'/config.js'));else w.SOP_API='https://fixture.invalid/api';
 w.eval(read(app+'/app.js')+(app==='sop'?"\nObject.assign(window,{load,detail,renderDashboard,__setApi:fn=>{api=fn;},__setSopContext:()=>{user={role:'viewer'};tab='task';}});":''));w.addEventListener=add;
 w.api=async(...args)=>{calls.push(args);return {ok:true};};
 w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;};
 t.after(()=>{w.close();assert.deepEqual(errors,[]);});
 return {w,alerts,calls,$:id=>w.document.getElementById(id),setApi:fn=>{const api=async(...args)=>{calls.push(args);return fn(...args);};if(w.__setApi)w.__setApi(api);else w.api=api;}};
}
function checkLabel(node,job){assert.ok(node.textContent.includes(job.business_no));assert.ok(node.textContent.includes(job.dispatcher_name));assert.ok(!node.textContent.includes(ID));assert.equal(node.querySelector('img'),null);}
function missingJob(extra={}){return fixtureJob({business_no:'',job_label:'',dispatcher_name:'',dispatcher_id:'',...extra});}

test('every raw app loads the label helper before its app, and known UUID-only templates are removed',()=>{
 for(const app of ['001','002','shuju','sop']){const html=read(app+'/index.html');assert.ok(html.includes('document-labels.js'));assert.ok(html.indexOf('document-labels.js')<html.indexOf('src="app.js'),app);}
 for(const [app,patterns] of [['001',[/esc\(job\.id\)/,/res\.trip_no \|\| res\.display_no \|\| res\.job_id/,/res\.job\.display_no \|\| _activeJobId/,/esc\(t\.display_no \|\| t\.id\)/]],['sop',[/esc\(x\.job_id\)/,/\$\{esc\(x\.id\)\} · 版本/]]])for(const pattern of patterns)assert.doesNotMatch(read(app+'/app.js'),pattern);
});

test('return/import sessions render business + actual dispatcher, with safe missing-data and HTML text',domOptions,t=>{
 const p=fixture(t,'001'),j=fixtureJob();p.w.renderInboundReturnSession(j);p.w.renderImportDeliverySession(j);
 checkLabel(p.$('inboundReturnSessionInfo'),j);checkLabel(p.$('idSessionInfo'),j);
 const validBusiness=fixtureJob({business_no:'JOB-20261008-001',job_label:'JOB-20261008-001'});p.w.renderInboundReturnSession(validBusiness);checkLabel(p.$('inboundReturnSessionInfo'),validBusiness);
 const missing=missingJob({job_type:'pickup_delivery_import'});p.w.renderImportDeliverySession(missing);const text=p.$('idSessionInfo').textContent;
 assert.doesNotMatch(text,/JOB-|CREATOR-NOT|LEAD-NOT/);assert.match(text,/未记录|미기록/);assert.match(text,/取|送|픽업|배송/);
 p.w.renderInboundReturnSession(fixtureJob({business_no:'<img src=x onerror=alert(1)>',job_label:'<img src=x onerror=alert(1)>'}));assert.equal(p.$('inboundReturnSessionInfo').querySelector('img'),null);
});

test('active field task and stale notice show known business metadata while retaining the real active ID',domOptions,async t=>{
 const p=fixture(t,'001'),j=fixtureJob();p.setApi(()=>({ok:true,active:true,job:j,segment:{id:'SEG-FIXTURE'}}));
 await p.w.checkMyActiveJob();checkLabel(p.$('myTaskBar'),j);assert.equal(p.w._activeJobId,ID);
 p.w.notifyStaleSegmentsOnLogin([j],{active:false});assert.ok(p.alerts.at(-1).includes(j.business_no));assert.ok(p.alerts.at(-1).includes(j.dispatcher_name));assert.ok(!p.alerts.at(-1).includes(ID));
});

test('pick create, active trip list and working title never fall back to job UUID; requests still use IDs',domOptions,async t=>{
 const p=fixture(t,'001'),j=fixtureJob({job_type:'pick_direct',workers:[],pick_docs:[],created_by:'EMP-FIXTURE'});p.w.getWorkerId=()=> 'EMP-FIXTURE';p.w.getWorkerName=()=> '虚拟员工';
 p.w.withActionLock=(_key,_button,_label,fn)=>fn();p.w.renderPickDocList=()=>{};p.w.stopPickCreateScan=()=>{};p.w.switchPickMode=()=>{};p.w.loadPickActiveList=()=>{};
 p.w._pickCreateDocNos=['08766731'];p.setApi(()=>({ok:true,...j}));await p.w.submitCreatePickTrip();await tick();assert.ok(p.alerts[0].includes(j.business_no));assert.ok(p.alerts[0].includes(j.dispatcher_name));assert.ok(!p.alerts[0].includes(ID));assert.deepEqual(Array.from(p.calls[0][0].pick_doc_nos),['08766731']);
 // Restore the actual list function after suppressing the create handler's automatic refresh.
 const source=read('001/app.js');p.w.eval(source.slice(source.indexOf('async function loadPickActiveList()'),source.indexOf('async function finalizePickJob(')));
 p.setApi(()=>({ok:true,items:[j]}));await p.w.loadPickActiveList();checkLabel(p.$('pickActiveList'),j);assert.ok(p.$('pickActiveList').querySelector('button').getAttribute('onclick').includes(ID));
 p.w._activeJobId=ID;p.w.refreshPickWorkingDocs=()=>{};p.w.refreshPickWorkers=()=>{};p.w.startPickElapsedTimer=()=>{};
 p.setApi(()=>({ok:true,job:j,workers:[]}));p.w.enterPickWorkingSection({});await tick();checkLabel(p.$('pickWorkingTitle'),j);assert.equal(p.calls.at(-1)[0].job_id,ID);
});

test('field real-time active and off-duty worker rows use canonical enriched metadata',domOptions,async t=>{
 const p=fixture(t,'001'),j=fixtureJob(),off=fixtureJob({last_job_type:'inbound_return',last_display_no:ID,last_left_at:'2026-10-08T00:00:00Z'});
 p.setApi(()=>({ok:true,active_workers:[j],off_workers:[off]}));await p.w.loadRealtimeBoard();checkLabel(p.$('rtbActiveBody'),j);checkLabel(p.$('rtbOffBody'),off);
});

test('office job list and detail show business + dispatcher and preserve detail request ID',domOptions,async t=>{
 const p=fixture(t,'002'),j=fixtureJob();p.w.getPager=()=>({limit:50});p.w.getOffset=()=>0;p.w.renderPager=()=>'';
 p.setApi(()=>({ok:true,items:[j]}));await p.w.loadOrderOpsList();checkLabel(p.$('orderOpsListBody'),j);assert.ok(p.$('orderOpsListBody').querySelector('a').getAttribute('onclick').includes(ID));
 p.setApi(()=>({ok:true,job:j,workers:[],results:[]}));await p.w.loadOrderOpsDetail(ID);checkLabel(p.$('orderOpsDetailBody'),j);assert.equal(p.calls.at(-1)[0].job_id,ID);
});

test('manager live worker, force-exit and live-document displays are readable without changing action identifiers',domOptions,async t=>{
 const p=fixture(t,'shuju'),j=fixtureJob();p.w._liveWorkersCache=[j];p.w.renderLiveWorkers();checkLabel(p.$('workerLiveBody'),j);
 p.w.forceLeaveWorker(ID,j.worker_id,j.segment_id,j.worker_name,j.joined_at,j.status,j.created_at);checkLabel(p.$('forceLeaveOverlay'),j);assert.equal(p.$('forceLeaveOverlay').querySelector('[data-job]').dataset.job,ID);
 p.w.closeForceLeaveModal();assert.equal(p.$('forceLeaveOverlay'),null);
 p.w.forceLeaveWorker(OTHER_ID,'EMP-OTHER','SEG-OTHER','虚拟他人',j.joined_at,j.status,j.created_at);assert.doesNotMatch(p.$('forceLeaveOverlay').textContent,/JOB-|真实派审员|CREATOR-NOT|LEAD-NOT/);assert.match(p.$('forceLeaveOverlay').textContent,/未记录|미기록/);assert.equal(p.$('forceLeaveOverlay').querySelector('[data-job]').dataset.job,OTHER_ID);
 p.setApi(()=>({ok:true,docs:[j]}));await p.w.loadLiveDocs();checkLabel(p.$('liveDocsBody'),j);assert.ok(p.$('liveDocsBody').querySelector('button').getAttribute('onclick').includes(ID));
});

test('manager order list/detail/result dialog and export carry business + actual dispatcher',domOptions,async t=>{
 const p=fixture(t,'shuju'),j=fixtureJob();p.setApi(()=>({ok:true,items:[j],total:1}));await p.w.loadOrders();checkLabel(p.$('ordersBody'),j);
 p.setApi(()=>({ok:true,job:j,workers:[],results:[]}));await p.w.openOrderDetail(ID);checkLabel(p.$('orderDetailBody'),j);assert.equal(p.calls.at(-1)[0].job_id,ID);
 p.w.openManualFinalize(ID);checkLabel(p.$('opsResultOverlay'),j);assert.equal(p.$('opsResultOverlay').querySelector('[data-job]').dataset.job,ID);p.w.closeResultModal();
 let output;p.w.exportCsv=(name,rows)=>{output={name,rows};};p.setApi(()=>({ok:true,rows:[{...j,'日期':'2026-10-08','单号':ID}]}));await p.w.exportOrders();assert.equal(output.rows[0].单号,j.business_no);assert.equal(output.rows[0].派审员,j.dispatcher_name);assert.ok(!JSON.stringify(output.rows[0]).includes(ID));
});

test('work-hour segment table and CSV pair business label with dispatcher, including legacy missing source',domOptions,async t=>{
 const p=fixture(t,'shuju'),j=fixtureJob(),missing=missingJob();p.setApi(()=>({ok:true,summary:{},segments:[j,missing]}));await p.w.loadWorkhours();checkLabel(p.$('whSegmentsBody'),j);assert.match(p.$('whSegmentsBody').textContent,/未记录|미기록/);
 let rows;p.w.exportCsv=(_name,r)=>{rows=r;};p.w.exportWorkhoursSegments();assert.equal(rows[0].任务号,j.business_no);assert.equal(rows[0].派审员,j.dispatcher_name);assert.doesNotMatch(JSON.stringify(rows),/JOB-|CREATOR-NOT|LEAD-NOT/);
});

test('standalone SOP list, detail, history, live board and CSV use business identities; buttons retain record IDs',domOptions,async t=>{
 const p=fixture(t,'sop'),j=fixtureJob({kind:'task',title:'虚拟退件',owner:'真实派审员 <陈>',revision:1,workers:[],round:1,estimated_minutes:20});p.w.__setSopContext();
 p.setApi(()=>({ok:true,items:[j]}));await p.w.load();checkLabel(p.$('content'),j);
 p.setApi(()=>({ok:true,record:j,events:[{actor_name:'虚拟修改人',created_at:j.created_at,action:'test',before_json:JSON.stringify({job_id:ID}),after_json:JSON.stringify({job_id:ID,status:'working'})}]}));await p.w.detail(ID);checkLabel(p.$('content'),j);assert.equal(p.calls.at(-1)[1].id,ID);assert.doesNotMatch(p.$('content').textContent,/JOB-/);
 p.setApi(()=>({date:'2026-10-08',outputs:{},data_quality:[],live:[j],alerts:[j]}));await p.w.renderDashboard();checkLabel(p.$('livePeople'),j);assert.doesNotMatch(p.$('alerts').textContent,/JOB-/);
 let blob;p.w.URL.createObjectURL=b=>{blob=b;return 'blob:fixture';};p.w.URL.revokeObjectURL=()=>{};p.w.HTMLAnchorElement.prototype.click=function(){};
 [...p.$('content').querySelectorAll('button')].find(x=>x.textContent.includes('CSV')).click();await tick();const csv=await new Promise((resolve,reject)=>{const reader=new p.w.FileReader();reader.onload=()=>resolve(reader.result);reader.onerror=reject;reader.readAsText(blob);});assert.ok(csv.includes(j.business_no));assert.ok(csv.includes(j.dispatcher_name));assert.match(csv,/派审员/);assert.ok(!csv.includes(ID));
});
