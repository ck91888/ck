// Actual legacy app functions, synthetic DOM and deferred read responses only.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null,options={skip:!runtime};
const root=process.env.CK_FILTER_BASELINE_ROOT||new URL('../',import.meta.url).pathname,read=file=>fs.readFileSync(path.join(root,file),'utf8'),helper=fs.readFileSync(new URL('../shared/latest-list-read.js',import.meta.url),'utf8');
const specs={outbound:['002','v2_outbound_order_list','obFilterCustomer','outboundListBody','loadOutboundList'],inbound:['002','v2_inbound_plan_list','ibFilterCustomer','inboundListBody','loadInboundList'],orders:['shuju','v2_dashboard_order_list','ordersFilterDoc','ordersBody','loadOrders'],workhours:['shuju','v2_dashboard_workhour_summary','whFilterWorker','whByWorkerBody','loadWorkhours'],materials:['003','v2_003_material_list','materialSearch','materialList','loadMaterials'],ledger:['003','v2_003_ledger_list','ledgerSearch','ledgerList','loadLedger']};
function good(kind,tag,extra={}){
 const item={id:tag,display_no:tag,customer:tag,status:'pending_issue',biz_class:'bulk',created_at:'2026-11-03T00:00:00Z'};
 if(kind==='materials')return {ok:true,items:[{id:tag,material_code:tag,name_zh:tag,category:'合成',unit:'个',current_qty:1,status:'active'}]};
 if(kind==='ledger')return {ok:true,items:[{id:tag,kind:'material',item_code:tag,item_name:tag,action_type:'inbound',qty_delta:1,operator_name:'合成',note:tag}]};
 if(kind==='orders')return {ok:true,total:1,items:[{...item,job_no:tag,job_type:'bulk_op',status:'working',result_summary:tag}],metadata:{snapshot:tag},...extra};
 if(kind==='workhours')return {ok:true,total:1,summary:{},by_worker:[{worker_name:tag,total_minutes:1}],segments:[],metadata:{snapshot:tag},...extra};
 return {ok:true,total:1,limit:50,offset:0,items:[{...item,status:kind==='inbound'?'pending':'pending_issue',biz_classes:['bulk']}],...extra};
}
function fixture(t,kind){
 const [app,action,field,host,fn]=specs[kind],errors=[],vc=new runtime.VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));
 const dom=new runtime.JSDOM(read(app+'/index.html'),{url:'https://synthetic.invalid/'+app+'/',runScripts:'outside-only',virtualConsole:vc}),w=dom.window,add=w.addEventListener;w.addEventListener=function(event,...args){if(event!=='DOMContentLoaded')return add.call(this,event,...args);};
 const alerts=[],blobs=[],pending=[];w.alert=x=>alerts.push(x);w.confirm=()=>true;w.fetch=()=>{throw Error('Unexpected network in DOM fixture');};w.Blob=Blob;w.URL.createObjectURL=b=>{blobs.push(b);return 'blob:https://synthetic.invalid/'+blobs.length;};w.URL.revokeObjectURL=()=>{};w.HTMLAnchorElement.prototype.click=()=>{};
 w.eval(read('shared/document-labels.js'));w.eval(helper);w.eval(read(app+'/config.js'));w.eval(read(app+'/app.js'));w.addEventListener=add;
 w.CKSession={scope:'office',user:{id:'SYN-A',scope:'office',role:'manager',operation_context:{id:'SYN-CTX',name:'合成经办'}}};
 if(app==='003'){w.S003.role='admin';w.S003.operatorId='SYN-A';}
 w.api=(a,b)=>{const p=typeof a==='string'?{action:a,...b}:a;assert.equal(p.action,action);return new Promise((resolve,reject)=>pending.push({p,resolve,reject}));};
 t.after(()=>{w.close();assert.deepEqual(errors,[]);});
 const query=(value,extra)=>{w.document.getElementById(field).value=value;return w[fn](...(extra||[]));};
 const text=()=>w.document.getElementById(host).textContent;
 return {w,pending,query,text,alerts,blobs,host:w.document.getElementById(host),field,kind};
}
for(const kind of Object.keys(specs)){
 test(kind+': late old success cannot overwrite latest rows, cache, query or pager',options,async t=>{
  const f=fixture(t,kind),a=f.query('OLD'),b=f.query('NEW');f.pending[1].resolve(good(kind,'SYN-NEW'));await b;f.pending[0].resolve(good(kind,'SYN-OLD',{total:99}));await a;assert.match(f.text(),/SYN-NEW/);assert.doesNotMatch(f.text(),/SYN-OLD/);
  if(['outbound','inbound'].includes(kind))assert.equal(f.w.getPager(kind).total,1);
  if(kind==='orders'){assert.equal(f.w._ordersQuery.doc_no,'NEW');assert.equal(f.w._ordersQuery.snapshot,'SYN-NEW');}
  if(kind==='workhours'){assert.equal(f.w._whQuery.worker_name,'NEW');assert.equal(f.w._whSummary.by_worker[0].worker_name,'SYN-NEW');}
  if(kind==='materials')assert.equal(f.w.S003.materialRows[0].id,'SYN-NEW');if(kind==='ledger')assert.equal(f.w.S003.ledgerRows[0].id,'SYN-NEW');
 });
 for(const rejected of [false,true])test(kind+': stale '+(rejected?'rejection':'error response')+' cannot replace new success',options,async t=>{
  const f=fixture(t,kind),a=f.query('OLD'),b=f.query('NEW');f.pending[1].resolve(good(kind,'SYN-NEW'));await b;
  if(rejected||['materials','ledger'].includes(kind))f.pending[0].reject(Error('SYN-OLD-ERROR'));else f.pending[0].resolve({ok:false,error:'SYN-OLD-ERROR'});await a;assert.match(f.text(),/SYN-NEW/);assert.doesNotMatch(f.text(),/SYN-OLD-ERROR/);if(kind==='workhours')assert.doesNotMatch(f.w.document.getElementById('whSummaryRow').textContent,/SYN-OLD-ERROR/);
 });
 test(kind+': A to B to A keeps newest A even though filter text repeats',options,async t=>{
  const f=fixture(t,kind),a=f.query('A'),b=f.query('B'),last=f.query('A');f.pending[2].resolve(good(kind,'SYN-LATEST-A'));await last;f.pending[1].resolve(good(kind,'SYN-B'));await b;f.pending[0].resolve(good(kind,'SYN-OLD-A'));await a;assert.match(f.text(),/SYN-LATEST-A/);assert.doesNotMatch(f.text(),/SYN-OLD-A/);
 });
 for(const sameId of [false,true])test(kind+': pending old identity cannot commit after '+(sameId?'same-id relogin':'person change'),options,async t=>{
  const f=fixture(t,kind),a=f.query('OLD');f.w.CKSession.user={...f.w.CKSession.user,id:sameId?'SYN-A':'SYN-B'};f.pending[0].resolve(good(kind,'SYN-OLD'));await a;assert.doesNotMatch(f.text(),/SYN-OLD/);const b=f.query('NEW');f.pending[1].resolve(good(kind,'SYN-NEW'));await b;assert.match(f.text(),/SYN-NEW/);
 });
}
for(const kind of ['outbound','inbound'])test(kind+': old finally cannot unlock new request or replace its page',options,async t=>{
 const f=fixture(t,kind),button=f.w.document.getElementById(kind==='outbound'?'obQueryBtn':'ibQueryBtn'),a=f.query('OLD');f.w.getPager(kind).page=2;const b=f.query('NEW');f.pending[0].resolve(good(kind,'SYN-OLD'));await a;assert.equal(button.disabled,true);assert.doesNotMatch(f.text(),/SYN-OLD/);f.pending[1].resolve(good(kind,'SYN-PAGE-2',{offset:50,total:120}));await b;assert.equal(button.disabled,false);assert.equal(f.w.getPager(kind).page,2);assert.equal(f.w.getPager(kind).total,120);
});
test('ledger export is latest-only and blocked during pending, error and identity change',options,async t=>{
 const f=fixture(t,'ledger'),a=f.query('OLD');f.pending[0].resolve(good('ledger','SYN-OLD'));await a;const b=f.query('NEW');f.w.exportLedgerCsv();assert.equal(f.blobs.length,0);f.pending[1].resolve(good('ledger','SYN-NEW'));await b;f.w.exportLedgerCsv();assert.equal(f.blobs.length,1);assert.match(await f.blobs[0].text(),/SYN-NEW/);assert.doesNotMatch(await f.blobs[0].text(),/SYN-OLD/);f.w.CKSession.user={...f.w.CKSession.user};f.w.exportLedgerCsv();assert.equal(f.blobs.length,1);const c=f.query('FAIL');f.pending[2].reject(Error('SYN-FAIL'));await c;f.w.exportLedgerCsv();assert.equal(f.blobs.length,1);
});
test('workhour export uses latest snapshot and a changing query cancels delayed output',options,async t=>{
 const f=fixture(t,'workhours'),a=f.query('OLD'),b=f.query('NEW');f.pending[1].resolve(good('workhours','SYN-NEW'));await b;f.pending[0].resolve(good('workhours','SYN-OLD'));await a;
 const actual=f.w.exportActualReport;let captured;f.w.exportActualReport=async p=>{captured=p;};await f.w.exportWorkhoursSegments();assert.equal(captured.worker_name,'NEW');f.w.exportActualReport=actual;
 let release;f.w.collectReportPages=()=>new Promise(r=>release=r);let exports=0;f.w.exportCsv=()=>exports++;const pending=f.w.exportWorkhoursSegments(),c=f.query('CHANGED');release({ok:true,metadata:{},rows:[]});await pending;assert.equal(exports,0);f.pending[2].resolve(good('workhours','SYN-CHANGED'));await c;
});
for(const reject of [false,true])test('task export discards delayed '+(reject?'failure':'success')+' after identity changes without unlocking a new export',options,async t=>{
 const f=fixture(t,'orders');f.w.document.getElementById('ordersDateBasis').value='created_at';let release,fail;f.w.collectReportPages=()=>new Promise((r,j)=>{release=r;fail=j;});let exports=0;f.w.exportCsv=()=>exports++;const btn=f.w.document.createElement('button'),old=f.w.exportOrders(btn);assert.equal(btn.disabled,true);
 f.w.CKSession.user={...f.w.CKSession.user,id:'SYN-B'};btn.textContent='SYN-NEW-PENDING';if(reject)fail(Error('SYN-OLD-EXPORT'));else release({ok:true,rows:[{id:'SYN-OLD'}],metadata:{}});await old;assert.equal(exports,0);assert.deepEqual(f.alerts,[]);assert.equal(btn.disabled,true);assert.equal(btn.textContent,'SYN-NEW-PENDING');
});
test('task export retains current filter policy and normal CSV output',options,async t=>{
 const f=fixture(t,'orders');f.w.document.getElementById('ordersDateBasis').value='created_at';f.w.document.getElementById(f.field).value='CURRENT-CONTROLS';f.w._ordersQuery={doc_no:'OLD-LOADED'};let p,rows;f.w.collectReportPages=async x=>{p=x;return {ok:true,rows:[{id:'SYN-CSV',display_no:'SYN-CSV'}],metadata:{}};};f.w.exportCsv=(name,value)=>rows=value;const btn=f.w.document.createElement('button');await f.w.exportOrders(btn);assert.equal(p.doc_no,'CURRENT-CONTROLS');assert.equal(rows.length,1);assert.equal(btn.disabled,false);assert.deepEqual(f.alerts,[]);
});
test('workhour actual export keeps its loaded query and emits latest valid CSV rows',options,async t=>{
 const f=fixture(t,'workhours'),a=f.query('SAVED');f.pending[0].resolve(good('workhours','SYN-SAVED'));await a;f.w.document.getElementById(f.field).value='NOT-LOADED';let p,csv;f.w.collectReportPages=async x=>{p=x;return {ok:true,metadata:{snapshot:'SYN-SAVED'},rows:[{id:'SYN-SEGMENT',worker_name:'SYN-WORKER',job_type:'bulk_op'}]};};f.w.exportCsv=(name,rows)=>csv={name,rows};await f.w.exportWorkhoursSegments();assert.equal(p.worker_name,'SAVED');assert.equal(p.report_kind,'segments');assert.equal(csv.rows[0]['人员段ID'],'SYN-SEGMENT');assert.equal(csv.rows[0]['人员'],'SYN-WORKER');
});
for(const change of ['query','identity'])test('workhour stale export errors are ignored after '+change+' changes',options,async t=>{
 const f=fixture(t,'workhours'),a=f.query('SAVED');f.pending[0].resolve(good('workhours','SYN-SAVED'));await a;let fail;f.w.collectReportPages=()=>new Promise((r,j)=>fail=j);const old=f.w.exportWorkhoursSegments();let b;if(change==='identity')f.w.CKSession.user={...f.w.CKSession.user};else b=f.query('CHANGED');fail(Error('SYN-OLD-EXPORT'));await old;assert.deepEqual(f.alerts,[]);if(b){f.pending[1].resolve(good('workhours','SYN-CHANGED'));await b;}
});
for(const kind of ['orders','workhours'])test(kind+': latest query failure clears the previous snapshot and rows',options,async t=>{
 const f=fixture(t,kind),a=f.query('SUCCESS');f.pending[0].resolve(good(kind,'SYN-SUCCESS'));await a;const b=f.query('FAIL');f.pending[1].resolve({ok:false,error:'SYN-FAIL'});await b;if(kind==='orders'){assert.equal(f.w._ordersItems.length,0);assert.equal(f.w._ordersQuery,null);}else{assert.equal(f.w._whSummary,null);assert.equal(f.w._whQuery,null);f.w.exportWorkhoursSegments();assert.equal(f.blobs.length,0);}
});
for(const kind of Object.keys(specs))test(kind+': latest failure is retained when an older success arrives later',options,async t=>{
 const f=fixture(t,kind),a=f.query('OLD'),b=f.query('FAILED-NEW');if(['materials','ledger'].includes(kind))f.pending[1].reject(Error('SYN-NEW-FAIL'));else f.pending[1].resolve({ok:false,error:'SYN-NEW-FAIL'});await b;f.pending[0].resolve(good(kind,'SYN-OLD'));await a;assert.doesNotMatch(f.text(),/SYN-OLD/);assert.match(kind==='workhours'?f.w.document.getElementById('whSummaryRow').textContent:f.text(),/SYN-NEW-FAIL/);
});
for(const kind of ['orders','workhours']){
 test(kind+': in-flight old page cannot overwrite a fresh filter or snapshot',options,async t=>{
  const f=fixture(t,kind),a=f.query('ORIGINAL');f.pending[0].resolve(good(kind,'SYN-ORIGINAL',{total:401}));await a;const paging=f.w[kind==='orders'?'ordersPage':'workhoursPage'](1);assert.equal(f.pending[1].p.offset,200);assert.equal(f.pending[1].p.snapshot,'SYN-ORIGINAL');const fresh=f.query('FRESH');assert.equal(f.pending[2].p.offset,0);f.pending[2].resolve(good(kind,'SYN-FRESH'));await fresh;f.pending[1].resolve(good(kind,'SYN-OLD-PAGE',{total:401}));await paging;assert.match(f.text(),/SYN-FRESH/);assert.doesNotMatch(f.text(),/SYN-OLD-PAGE/);assert.equal(f.w[kind==='orders'?'_ordersOffset':'_whOffset'],0);assert.equal(f.w[kind==='orders'?'_ordersQuery':'_whQuery'].snapshot,'SYN-FRESH');
 });
 test(kind+': cached pagination cannot reuse another identity snapshot',options,async t=>{
  const f=fixture(t,kind),a=f.query('ORIGINAL');f.pending[0].resolve(good(kind,'SYN-ORIGINAL',{total:401}));await a;f.w.CKSession.user={...f.w.CKSession.user,id:'SYN-B'};await f.w[kind==='orders'?'ordersPage':'workhoursPage'](1);assert.equal(f.pending.length,1);
 });
}
