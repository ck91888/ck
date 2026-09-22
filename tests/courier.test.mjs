import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {handleCourier} from '../worker-v2/courier.js';import {courierOwners,trackingNumber,trackingNumbers} from '../shared/courier-rules.js';
import {TABLES} from '../worker-v2/test-data-reset.js';
const C1='301000000101',C2='50000000000102';
function setup(){const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={},ok=true)=>{const res=await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);const r=await res.json();if(ok)assert.equal(r.ok,true,r.message||r.error);return r;};
 const plan=(biz=['bulk'],codes=[C1,C2],extra={})=>call('v2_inbound_plan_create',{customer:'虚拟快递验收',biz_classes:biz,lines:[{unit_type:'courier',planned_qty:codes.length,tracking_nos:codes}],...extra});
 const scan=(code=C1,owner='8-1',extra={})=>call('sop_courier_receive',{owner,tracking_no:code,...extra});const detail=p=>call('v2_inbound_plan_detail',{id:p.id});return {DB,env,call,plan,scan,detail};}
test('owners match all eight workbook columns; full-string formats reject QR URLs, remarks and product codes',()=>{
 assert.deepEqual(courierOwners.map(x=>x[0]),['8-1','8-2','8-3','8-4','tent','supplies','purchase','unknown']);
 for(const c of ['45100000101',C1,'6080000000101',C2,'LP100000001CN','EZ100000002CN','JJD014600000000000101'])assert.equal(trackingNumber(c),c);
 assert.equal(trackingNumber(' ＥＺ１０００００００２ CN '),'EZ100000002CN');assert.equal(trackingNumber('3010-0000-0101'),C1);
 assert.deepEqual(trackingNumbers(C1+'\n'+C1+'，'+C2),[C1,C2]);
 for(const x of ['12345678','1234567890','123456789012345','https://x.test/'+C1,C1+'已认领','XYZ1234','000000000000',C1+'\n'+C2])assert.throws(()=>trackingNumber(x));
});
test('scan is idempotent and preserves first actor/time/owner; correction and handoff retain audit history',async()=>{
 const {scan,call,DB}=setup();const a=await scan();assert.equal(a.item.scanner_name,'测试负责人');
 const dupe=await scan(C1,'8-4');assert.equal(dupe.duplicate,true);assert.equal(dupe.owner_conflict,true);assert.equal(dupe.item.owner,'8-1');assert.equal(dupe.item.received_at,a.item.received_at);
 const changed=await call('sop_courier_update',{id:a.item.id,version:1,owner:'8-4',note:'面单归属核实'});assert.equal(changed.item.version,2);
 const stale=await call('sop_courier_update',{id:a.item.id,version:1,owner:'8-2',note:'过期页面'},false);assert.equal(stale.ok,false);
 const h=await call('sop_courier_update',{id:a.item.id,version:2,status:'handed_over',handed_to:'虚拟接收员',note:'按外包装交接'});assert.equal(h.item.status,'handed_over');assert.equal(h.item.received_at,a.item.received_at);
 const d=await call('sop_courier_detail',{id:a.item.id});assert.equal(d.events.length,3);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_courier_receipts').get().n,1);
});
test('three non-putaway classes complete only after all planned courier numbers are received, without artificial jobs',async()=>{
 for(const biz of ['bulk','return','change_order']){const {plan,scan,detail,DB}=setup(),p=await plan([biz]);const first=await scan();assert.equal(first.plan.progress.received,1);assert.equal((await detail(p)).plan.status,'pending');
 await scan(C2);const d=await detail(p);assert.equal(d.plan.status,'completed');assert.equal(d.plan.courier_progress.received,2);assert.equal(d.lines[0].actual_qty,2);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,0);}
});
test('courier receiving is distinct from WMS references; A1 and A2 must still complete separately',async()=>{
 const {plan,scan,detail,call}=setup(),p=await plan(['direct_ship'],[C1,C2],{external_inbound_nos:['WMS-A1','WMS-A2']});
 await scan();await scan(C2);assert.equal((await detail(p)).plan.status,'arrived_pending_putaway');
 for(const [i,no] of ['WMS-A1','WMS-A2'].entries()){const j=await call('v2_inbound_job_start',{source:'system',plan_id:p.id,external_inbound_no:no,biz_class:'direct_ship',worker_id:'TEST',worker_name:'虚拟人员'});await call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'TEST',complete_job:true,result_lines:[{unit_type:'courier',putaway_qty:1}]});assert.equal((await detail(p)).plan.status,i?'completed':'partially_completed');}
 assert.equal((await detail(p)).lines[0].putaway_qty,2);
});
test('mixed truck and parcel plans wait for both arrival paths, whichever comes first',async()=>{
 for(const courierFirst of [true,false]){const {plan,scan,call,detail}=setup();const p=await plan(['bulk'],[C1],{lines:[{unit_type:'courier',tracking_nos:[C1],planned_qty:1},{unit_type:'carton',planned_qty:20}]});
 const unload=async()=>{const j=await call('v2_unload_job_start',{plan_id:p.id,worker_id:'TEST',worker_name:'虚拟卸货员'});await call('v2_unload_job_finish',{job_id:j.job_id,worker_id:'TEST',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:20},{unit_type:'courier',actual_qty:999}]});};
 if(courierFirst){await scan();assert.equal((await detail(p)).plan.status,'pending');await unload();}else{await unload();assert.notEqual((await detail(p)).plan.status,'completed');await scan();}
 const d=await detail(p);assert.equal(d.plan.status,'completed');assert.equal(d.lines.find(x=>x.unit_type==='courier').actual_qty,1);}
});
test('tracking validation, cross-plan uniqueness and received-plan edit protection are enforced server-side',async()=>{
 const {plan,call,scan,DB}=setup();const bad=await call('v2_inbound_plan_create',{biz_classes:['bulk'],lines:[{unit_type:'courier',planned_qty:1}]},false);assert.equal(bad.ok,false);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,0);
 const p=await plan();const conflict=await call('v2_inbound_plan_create',{biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:[C1]}]},false);assert.equal(conflict.ok,false);
 await call('v2_inbound_plan_update',{id:p.id,biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:[C1],planned_qty:999}]});assert.equal(DB.raw.prepare('SELECT planned_qty FROM v2_inbound_plan_lines WHERE plan_id=?').get(p.id).planned_qty,1);
 await scan();const edit=await call('v2_inbound_plan_update',{id:p.id,biz_classes:['bulk'],customer:'不能更改'},false);assert.equal(edit.ok,false);
});
test('receipt-before-plan links on later plan creation and records remain queryable by owner, number and Korean day',async()=>{
 const {plan,scan,call,DB}=setup();const received=await scan();const p=await plan(['bulk'],[C1]);assert.equal((await call('v2_inbound_plan_detail',{id:p.id})).plan.status,'completed');
 DB.raw.prepare('UPDATE ck_courier_receipts SET received_at=? WHERE id=?').run('2026-09-21T15:00:00.000Z',received.item.id);
 const found=await call('sop_courier_list',{owner:'8-1',keyword:'000001',from:'2026-09-22',to:'2026-09-22'});assert.equal(found.total,1);assert.equal(found.items[0].display_no,p.display_no);
 assert.equal((await call('sop_courier_list',{owner:'8-2'})).total,0);assert.equal((await call('sop_courier_list',{to:'2026-09-21'})).total,0);
});
test('purchase lookup precedes warehouse destination; collection does not receive purchase inventory',async()=>{
 const {call,scan,DB}=setup();DB.raw.prepare("INSERT INTO v2_003_purchase_shipments(id,tracking_no,status) VALUES('P1',?,'pending')").run(C1);
 const bad=await call('sop_courier_receive',{tracking_no:C1,owner:'8-1'},false);assert.equal(bad.ok,false);const r=await scan(C1,'supplies');assert.equal(r.item.shipment_id,'P1');assert.equal(DB.raw.prepare("SELECT status FROM v2_003_purchase_shipments WHERE id='P1'").get().status,'pending');
});
test('unauthorized/readonly writes and cross-origin mutations are blocked; reset includes all courier records',async()=>{
 const {env}=setup();for(const role of ['viewer'])await assert.rejects(handleCourier({action:'sop_courier_receive',owner:'8-1',tracking_no:C1},{...env,SOP_REQUEST_USER:{id:'V',name:'只读',role}},()=>{}),/权限/);
 await assert.rejects(handleCourier({action:'sop_courier_list'},{...env,SOP_UPGRADE_ENABLED:'false'},()=>{}),/权限/);
 const r=await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:'https://outside.example'},body:JSON.stringify({action:'sop_courier_receive',owner:'8-1',tracking_no:C1})}),env);assert.equal(r.status,403);
 for(const table of ['ck_courier_receipts','ck_courier_plan_items','ck_courier_events'])assert.ok(TABLES.includes(table));
});

test('manual completion paths cannot bypass missing planned parcels',async()=>{
 const {plan,call,DB}=setup(),p=await plan();
 const forced=await call('v2_inbound_plan_force_complete',{id:p.id,reason:'test'},false);assert.equal(forced.error,'courier_not_received');
 DB.raw.prepare("UPDATE v2_inbound_plans SET status='arrived_pending_putaway' WHERE id=?").run(p.id);
 const marked=await call('v2_inbound_mark_completed',{inbound_plan_id:p.id},false);assert.equal(marked.error,'courier_not_received');
 assert.equal(DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(p.id).status,'arrived_pending_putaway');
});

test('wrong tracking correction reopens the old automatic arrival and completes the actual matching plan atomically',async()=>{
 const {plan,scan,call,detail}=setup();const oldPlan=await plan(['bulk'],[C1]),newPlan=await plan(['change_order'],[C2]);const first=await scan();
 assert.equal((await detail(oldPlan)).plan.status,'completed');
 const corrected=await call('sop_courier_update',{id:first.item.id,version:1,tracking_no:C2,owner:'8-2',note:'错扫相邻条码，核对实物更正'});
 assert.equal(corrected.item.received_at,first.item.received_at);assert.equal(corrected.item.scanner_name,first.item.scanner_name);assert.equal(corrected.item.tracking_no,C2);assert.equal(corrected.item.plan_id,newPlan.id);
 const old=await detail(oldPlan);assert.equal(old.plan.status,'pending');assert.equal(old.plan.courier_progress.received,0);assert.equal(old.lines[0].actual_qty,0);assert.equal(old.biz_tasks[0].status,'pending');
 assert.equal((await detail(newPlan)).plan.status,'completed');
 const audit=await call('sop_courier_detail',{id:first.item.id});const event=JSON.parse(audit.events.at(-1).detail);assert.equal(event.before.tracking_no,C1);assert.equal(event.after.tracking_no,C2);
 await scan(C1);assert.equal((await detail(oldPlan)).plan.status,'completed');
});

test('correction rejects duplicate numbers, missing reasons, invalid formats and stale versions without partial changes',async()=>{
 const {scan,call}=setup();const a=await scan(),b=await scan(C2);
 for(const change of [{tracking_no:C2,note:'错扫'},{tracking_no:'abc',note:'错扫'},{tracking_no:'301000000102'},{owner:'8-3'},{version:0,tracking_no:'301000000102',note:'旧页面'}]){
  const r=await call('sop_courier_update',{id:a.item.id,version:1,...change},false);assert.equal(r.ok,false);
 }
 const after=await call('sop_courier_detail',{id:a.item.id});assert.equal(after.item.tracking_no,C1);assert.equal(after.events.length,1);assert.equal(after.item.version,1);
 assert.equal((await call('sop_courier_detail',{id:b.item.id})).item.tracking_no,C2);
});

test('completed or active putaway blocks tracking rewrites but still allows audited ownership correction',async()=>{
 const {plan,scan,call,detail}=setup();const p=await plan(['direct_ship'],[C1],{external_inbound_no:'WMS-CORRECTION'}),r=await scan();
 const j=await call('v2_inbound_job_start',{source:'system',plan_id:p.id,external_inbound_no:'WMS-CORRECTION',biz_class:'direct_ship',worker_id:'TEST',worker_name:'虚拟人员'});
 for(const finished of [false,true]){
  if(finished)await call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'TEST',complete_job:true,result_lines:[{unit_type:'courier',putaway_qty:1}]});
  const bad=await call('sop_courier_update',{id:r.item.id,version:1,tracking_no:C2,note:'不得覆盖实际理货'},false);assert.equal(bad.ok,false);
 }
 const good=await call('sop_courier_update',{id:r.item.id,version:1,owner:'8-4',note:'所属纠错'});assert.equal(good.item.owner,'8-4');assert.equal((await detail(p)).plan.status,'completed');
});

test('mixed-plan tracking correction preserves genuine truck unloading while reopening parcel arrival',async()=>{
 const {plan,scan,call,detail}=setup();const p=await plan(['bulk'],[C1],{lines:[{unit_type:'courier',tracking_nos:[C1],planned_qty:1},{unit_type:'carton',planned_qty:5}]}),r=await scan();
 const j=await call('v2_unload_job_start',{plan_id:p.id,worker_id:'TEST',worker_name:'虚拟卸货员'});await call('v2_unload_job_finish',{job_id:j.job_id,worker_id:'TEST',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:5}]});const before=(await detail(p)).plan;
 await call('sop_courier_update',{id:r.item.id,version:1,tracking_no:C2,note:'更正实物单号'});const after=await detail(p);assert.equal(after.plan.status,'arrived_pending_putaway');assert.equal(after.plan.unload_completed_at,before.unload_completed_at);assert.equal(after.lines.find(x=>x.unit_type==='carton').actual_qty,5);assert.equal(after.lines.find(x=>x.unit_type==='courier').actual_qty,0);
});
