import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {documentCodeError,isStaffBadge} from '../shared/document-code.js';
function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,r.error||r.message);return r;};
 const staff=[{id:'EMP-TEST-A',name:'虚拟甲'},{id:'EMP-TEST-B',name:'虚拟乙'}];
 const payload=(numbers,key=crypto.randomUUID())=>({payload:{action:'v2_pick_job_start_by_docs',client_req_id:key,pick_doc_nos:numbers},workers:staff,lead_id:staff[0].id,estimated_minutes:15});
 return {DB,call,ok,staff,payload};
}
test('new external pick sheets dispatch directly with leading zeroes preserved; retry, resume and finish share one trip',async()=>{
 const {DB,ok,call,payload,staff}=setup(),request=payload(['649183761','08766731'],'scan-fixture');
 const j=await ok('sop_native_start',request);assert.equal(DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'working');
 assert.deepEqual(DB.raw.prepare('SELECT pick_doc_no FROM v2_ops_job_pick_docs ORDER BY rowid').all().map(x=>x.pick_doc_no),['649183761','08766731']);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,2);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_pick_worker_docs').get().n,4);
 assert.equal((await ok('sop_native_start',request)).job_id,j.job_id);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);
 const card=(await ok('sop_dispatch_list')).items[0];assert.equal(card.business_no,'649183761、08766731');assert.equal(card.workers.length,2);
 const duplicate=await call('sop_native_start',payload(['08766731']));assert.equal(duplicate.ok,false);assert.equal(duplicate.active_job_id,j.job_id);
 await ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:staff[0].id});assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,0);
 assert.equal((await call('sop_native_start',payload(['08766731']))).ok,false);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);
});
test('badges are rejected before creating or looking up documents, including historical badge-shaped sheets',async()=>{
 const {DB,call,ok,payload}=setup();
 for(const code of ['EMP-LIUJIAQI|刘佳奇','EMP-LIUJIAQI','DA-260929-9|测试日当','DAF-ABC|测试','TEST-A|虚拟','ＥＭＰ－Ａ｜测试']){
  assert.equal(isStaffBadge(code),true);
  for(const [action,data] of [['sop_native_start',payload([code])],['v2_pick_job_start',{pick_doc_nos:[code]}],['v2_pick_job_start_by_docs',{pick_doc_nos:[code],worker_id:'X'}],['v2_pick_doc_lookup',{pick_doc_no:code}],['v2_bulk_op_job_start',{work_order_no:code,worker_id:'X'}]]){
   const r=await call(action,data);assert.equal(r.ok,false,action);assert.match(r.error,/工牌/,action);
  }
 }
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,0);
 assert.equal(documentCodeError('08766731'),'');assert.equal(documentCodeError('EXT-PICK/2026-01'),'');
 await ok('sop_native_start',payload(['VALID']));
 assert.equal((await call('sop_native_start',payload(['VALID','NEW']))).ok,false);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_pick_docs').get().n,1);
});
test('home dispatch cards use external bulk work numbers and associated customer without changing job identity',async()=>{
 const {ok,DB,staff}=setup();const j=await ok('sop_native_start',{payload:{action:'v2_bulk_op_job_start',client_req_id:crypto.randomUUID(),work_order_no:'WMS-WORK-009',customer:'虚拟客户'},workers:staff,lead_id:staff[0].id,estimated_minutes:15});
 const [card]=(await ok('sop_dispatch_list')).items;assert.equal(card.id,j.job_id);assert.equal(card.business_no,'WMS-WORK-009');assert.equal(card.customer,'虚拟客户');assert.equal(card.workers[1].name,staff[1].name);
 assert.equal(DB.raw.prepare('SELECT related_doc_id FROM v2_ops_jobs WHERE id=?').get(j.job_id).related_doc_id,'WMS-WORK-009');
});
function scannerFixture(){
 const elements=new Map(),logs=[];let dialog=false,target=Promise.resolve('fixture');
 const node=id=>{if(!elements.has(id))elements.set(id,{id,value:'',textContent:'',innerHTML:'',after(n){elements.set(n.id,n);},setAttribute(){},focus(){}});return elements.get(id);};
 const context={Map,Promise,Array,console,_currentPage:'pick_direct',CKDocumentCode:{documentCodeError},document:{getElementById:node,createElement:()=>({setAttribute(){}}),querySelector:()=>dialog?{}:null},resolveCameraTarget:()=>target,ensureCameraSwitchButton(){},removeCameraSwitchButton(){},_pickCreateScanner:null,_pickStartScanner:null,_inboundScanner:null,_bulkScanner:null};context.window=context;
 const source=fs.readFileSync(new URL('../001/app.js',import.meta.url),'utf8');vm.runInNewContext(source.slice(source.indexOf('var _managedQrScanners'),source.indexOf('// ===== Action Lock')),context);
 const scanner={async start(t,c,callback){logs.push('start');this.decode=callback;},async stop(){logs.push('stop');}};
 return {context,scanner,logs,node,setDialog:v=>dialog=v,setTarget:p=>target=p};
}
test('document camera ignores badges and dialog scans; stopping a pending camera invalidates its callback',async()=>{
 const f=scannerFixture(),received=[];f.context._pickStartScanner=f.scanner;
 await f.context.startManagedQrScanner(f.scanner,'pickStartScanReader',{},v=>received.push(v));
 f.scanner.decode('08766731');f.scanner.decode('EMP-LIUJIAQI|刘佳奇');assert.deepEqual(received,['08766731']);
 assert.match(f.node('pickStartScanReader-scan-error').textContent,/工牌/);
 f.setDialog(true);f.scanner.decode('LATE-ORDER');f.setDialog(false);
 await f.context.stopAllManagedQrScanners();f.scanner.decode('AFTER-STOP');assert.deepEqual(received,['08766731']);assert.equal(f.context._pickStartScanner,null);
 const delayed=scannerFixture();let release;delayed.setTarget(new Promise(r=>release=r));
 const starting=delayed.context.startManagedQrScanner(delayed.scanner,'pickStartScanReader',{},()=>assert.fail('stale scan'));
 const stopped=delayed.context.stopAllManagedQrScanners();release('fixture');await Promise.all([starting,stopped]);assert.equal(delayed.logs.includes('start'),false);
});
