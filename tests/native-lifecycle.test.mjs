import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
 const workers=[{id:'A',name:'甲'},{id:'B',name:'乙'},{id:'C',name:'丙'}];
 const start=(action,data={},staff=workers)=>ok('sop_native_start',{payload:{action,client_req_id:crypto.randomUUID(),...data},workers:staff,lead_id:staff[0].id,estimated_minutes:15,labor_department:'bulk'});
 const count=id=>DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n;
 const status=id=>DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(id).status;
 return {DB,env,call,ok,start,workers,count,status};
}
test('three-person outbound loading finishes job and order once; failure rolls back crew and result',async()=>{
 const {DB,call,ok,start,count,status}=setup();const ob=await ok('v2_outbound_order_create',{customer:'装货测试',biz_class:'bulk',outbound_mode:'customer_pickup',planned_box_count:50});
 DB.raw.prepare("UPDATE v2_outbound_orders SET status='ready_to_ship' WHERE id=?").run(ob.id);
 const j=await start('v2_outbound_load_start',{order_id:ob.id});assert.equal(count(j.job_id),3);
 DB.raw.exec("CREATE TRIGGER fail_load BEFORE UPDATE ON v2_outbound_orders WHEN NEW.status='shipped' BEGIN SELECT RAISE(ABORT,'fixture_failure'); END");
 assert.equal((await call('v2_outbound_load_finish',{job_id:j.job_id,worker_id:'A',complete_job:true,box_count:50})).ok,false);assert.equal(count(j.job_id),3);assert.equal(status(j.job_id),'working');assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,0);
 DB.raw.exec('DROP TRIGGER fail_load');
 await ok('v2_outbound_load_finish',{job_id:j.job_id,worker_id:'A',complete_job:true,box_count:50});assert.equal(status(j.job_id),'completed');assert.equal(count(j.job_id),0);
 const saved=DB.raw.prepare('SELECT * FROM v2_outbound_orders WHERE id=?').get(ob.id);assert.equal(saved.status,'shipped');assert.equal(saved.actual_box_count,50);
 await ok('v2_outbound_load_finish',{job_id:j.job_id,worker_id:'A',complete_job:true,box_count:50});assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,1);
});
test('stock operation closes the entire team and waits for customer service outbound update',async()=>{
 const {DB,ok,start,count,status}=setup();const ob=await ok('v2_outbound_order_create',{customer:'库存操作',biz_class:'bulk',outbound_mode:'customer_pickup',uses_stock_operation:1});
 const j=await start('v2_outbound_stock_op_start',{outbound_order_id:ob.id});await ok('v2_outbound_stock_op_finish',{job_id:j.job_id,worker_id:'A',box_count:20,pallet_count:2});
 assert.equal(status(j.job_id),'completed');assert.equal(count(j.job_id),0);const order=DB.raw.prepare('SELECT * FROM v2_outbound_orders WHERE id=?').get(ob.id);assert.equal(order.status,'pending_outbound_update');assert.equal(JSON.parse(order.stock_operation_result_json).total_box_count,20);
});
test('picker team links all documents, finalizes atomically and counts personal time once per segment',async()=>{
 const {DB,ok,call,start,count,status}=setup();const j=await start('v2_pick_job_start',{pick_doc_nos:['P-A','P-B']});assert.equal(count(j.job_id),3);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_pick_worker_docs').get().n,6);
 DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(new Date(Date.now()-30*60000).toISOString(),j.job_id);
 DB.raw.exec("CREATE TRIGGER fail_pick BEFORE INSERT ON v2_ops_job_results BEGIN SELECT RAISE(ABORT,'fixture_failure'); END");assert.equal((await call('v2_pick_job_finalize',{job_id:j.job_id,worker_id:'A'})).ok,false);assert.equal(count(j.job_id),3);assert.equal(status(j.job_id),'working');DB.raw.exec('DROP TRIGGER fail_pick');
 const result=await ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:'A',result_note:'两单均完成'});assert.equal(result.total_minutes,90);assert.equal(result.pick_doc_count,2);assert.equal(count(j.job_id),0);assert.equal(status(j.job_id),'completed');assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_pick_worker_docs WHERE status='working'").get().n,0);
 const saved=JSON.parse(DB.raw.prepare('SELECT result_json FROM v2_ops_job_results').get().result_json);assert.equal(saved.total_pwd,6);assert.equal(saved.total_minutes,90);
 await ok('v2_pick_job_finish',{job_id:j.job_id,worker_id:'A'});assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,1);
});
test('invalid bulk outputs, missing customer and wrong endpoints do not stop any worker',async()=>{
 const {call,ok,start,count}=setup();const j=await start('v2_bulk_op_job_start',{work_order_no:'EXTERNAL-TEST'});
 for(const data of [{},{label_count:5},{customer:'客户',label_count:-1},{customer:'客户',label_count:1e10}]){assert.equal((await call('v2_bulk_op_job_finish',{job_id:j.job_id,worker_id:'A',...data})).ok,false);assert.equal(count(j.job_id),3);}
 assert.equal((await call('v2_ops_job_finish',{job_id:j.job_id,worker_id:'A'})).ok,false);assert.equal(count(j.job_id),3);
 await ok('v2_bulk_op_job_finish',{job_id:j.job_id,worker_id:'A',customer:'客户',label_count:5});assert.equal(count(j.job_id),0);
});
test('empty temporary unload receipt preserves all three clocks and completion timestamp does not drift on reads',async()=>{
 const {DB,call,ok,start,count}=setup();const j=await start('v2_unplanned_unload_start',{biz_class:'bulk'});
 assert.equal((await call('v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:'A',result_lines:[]})).ok,false);assert.equal(count(j.job_id),3);
 assert.equal((await call('v2_unload_job_finish',{job_id:j.job_id,worker_id:'A',result_lines:[{unit_type:'carton',actual_qty:2}]})).ok,false);assert.equal(count(j.job_id),3);
 await ok('v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:'A',result_lines:[{unit_type:'carton',actual_qty:30}]});
 const stamp='2026-09-20T12:00:00.000Z';DB.raw.prepare('UPDATE v2_ops_jobs SET updated_at=? WHERE id=?').run(stamp,j.job_id);for(let i=0;i<3;i++)await ok('v2_ops_job_detail',{job_id:j.job_id});assert.equal(DB.raw.prepare('SELECT updated_at FROM v2_ops_jobs WHERE id=?').get(j.job_id).updated_at,stamp);
});
test('dispatcher changes crew in-place: remaining segments continuous, retries safe, stale edits rejected',async()=>{
 const {DB,ok,call,start,count}=setup();const j=await start('v2_pick_job_start',{pick_doc_nos:['P1','P2']});const prior=DB.raw.prepare("SELECT * FROM v2_ops_job_workers WHERE worker_id='B'").get();
 const request={job_id:j.job_id,revision:1,client_req_id:'people-test',workers:[{id:'B',name:'乙'},{id:'C',name:'丙'},{id:'D',name:'丁'}],lead_id:'B'};
 await ok('sop_native_people',request);assert.equal(count(j.job_id),3);const after=DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE id=?').get(prior.id);assert.deepEqual(after,prior);assert.notEqual(DB.raw.prepare("SELECT left_at FROM v2_ops_job_workers WHERE worker_id='A'").get().left_at,'');
 assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_pick_worker_docs WHERE worker_id='D'").get().n,2);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_pick_worker_docs').get().n,8);
 await ok('sop_native_people',request);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,4);
 assert.equal((await call('sop_native_people',{...request,client_req_id:'stale'})).ok,false);
 await ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:'B'});assert.equal((await call('sop_native_people',{...request,revision:2,client_req_id:'after-finish'})).ok,false);assert.equal(count(j.job_id),0);
});
test('personnel update racing with completion cannot reopen a completed job',async()=>{
 const {DB,call,start,count,status}=setup();const j=await start('v2_ops_job_start',{job_type:'pack_direct',flow_stage:'outbound'}),batch=DB.batch;let once=true;
 DB.batch=async statements=>{if(once&&statements.some(s=>s.args?.includes('sop_native_people'))){once=false;DB.raw.prepare("UPDATE v2_ops_jobs SET status='completed' WHERE id=?").run(j.job_id);DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at='2026-09-22T00:00:00Z' WHERE job_id=?").run(j.job_id);}return batch(statements);};
 const r=await call('sop_native_people',{job_id:j.job_id,revision:1,workers:[{id:'D',name:'丁'}],lead_id:'D'});assert.equal(r.ok,false);assert.equal(status(j.job_id),'completed');assert.equal(count(j.job_id),0);assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id='D'").get().n,0);
});
test('resting or signed-out staff cannot be added; valid attendance rejoins without rewriting history',async()=>{
 const {DB,call,ok,start,count}=setup();const p=(await ok('sop_attendance_checkin',{name:'人员调整测试',agency:'가온'})).record;
 const j=await start('v2_ops_job_start',{job_type:'pack_direct',flow_stage:'outbound'});const request={job_id:j.job_id,revision:1,workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId};
 await ok('sop_attendance_break_start',{id:p.id});assert.equal((await call('sop_native_people',request)).ok,false);assert.equal(count(j.job_id),3);
 await ok('sop_attendance_break_end',{id:p.id});await ok('sop_native_people',request);assert.equal(count(j.job_id),1);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,4);
});
test('all generic job types finish a three-person crew under dispatch management',async()=>{
 const {start,ok,count,status}=setup();for(const job_type of ['pack_direct','change_order','load_import','scan_pallet','inventory','qc','disposal','other_internal']){
  const j=await start('v2_ops_job_start',{job_type,flow_stage:'internal'});await ok('v2_ops_job_finish',{job_id:j.job_id,worker_id:'A'});assert.equal(count(j.job_id),0,job_type);assert.equal(status(j.job_id),'completed',job_type);
 }
});
test('moving the whole crew preserves a finishable task and restores the correct lead on reassignment',async()=>{
 const {DB,ok,start,count,status}=setup();const j=await start('v2_unplanned_unload_start',{biz_class:'bulk'});
 await ok('sop_native_people',{job_id:j.job_id,revision:1,workers:[],lead_id:''});assert.equal(count(j.job_id),0);assert.equal(status(j.job_id),'awaiting_close');
 const list=await ok('sop_dispatch_list'),item=list.items.find(x=>x.id===j.job_id);assert.equal(item.last_lead.id,'A');assert.equal(item.workers.length,0);
 await ok('sop_native_people',{job_id:j.job_id,revision:2,workers:[{id:'D',name:'丁'}],lead_id:'D'});assert.equal(count(j.job_id),1);assert.equal(status(j.job_id),'working');
 await ok('sop_native_people',{job_id:j.job_id,revision:3,workers:[],lead_id:''});await ok('v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:'D',result_lines:[{unit_type:'carton',actual_qty:30}]});assert.equal(status(j.job_id),'completed');assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,4);
});
test('scanning an existing dispatched order cannot silently add only one of a new team',async()=>{
 const {DB,call,ok,start,count}=setup();const j=await start('v2_pick_job_start',{pick_doc_nos:['SAME-1','SAME-2']});
 const r=await call('sop_native_start',{payload:{action:'v2_pick_job_start_by_docs',pick_doc_nos:['SAME-1'],client_req_id:'new-crew'},workers:[{id:'D',name:'丁'},{id:'E',name:'戊'}],lead_id:'D',estimated_minutes:15});assert.equal(r.ok,false);assert.equal(r.active_job_id,j.job_id);assert.equal(count(j.job_id),3);assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id IN ('D','E')").get().n,0);
 await ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:'A'});
 const bulk=await start('v2_bulk_op_job_start',{work_order_no:'SAME-BULK'});const denied=await call('sop_native_start',{payload:{action:'v2_bulk_op_job_start',work_order_no:'SAME-BULK',client_req_id:'new-bulk-crew'},workers:[{id:'D',name:'丁'}],lead_id:'D',estimated_minutes:15});assert.equal(denied.ok,false);assert.equal(count(bulk.job_id),3);
});
test('independent import trips do not merge crews and both finish from the dispatcher page',async()=>{
 const {start,ok,count,status}=setup();const a=await start('v2_import_delivery_job_start'),b=await start('v2_import_delivery_job_start',{},[{id:'D',name:'丁'},{id:'E',name:'戊'}]);assert.notEqual(a.job_id,b.job_id);assert.equal(count(a.job_id),3);assert.equal(count(b.job_id),2);
 await ok('v2_import_delivery_job_finish',{job_id:a.job_id,worker_id:'A',complete_job:true});assert.equal(status(a.job_id),'completed');assert.equal(count(b.job_id),2);await ok('v2_import_delivery_job_finish',{job_id:b.job_id,worker_id:'D',complete_job:true});assert.equal(count(b.job_id),0);
});
test('return receiving and barcode verification both close the assigned three-person crew',async()=>{
 const {ok,start,count,status}=setup();const j=await start('v2_inbound_job_start',{job_type:'inbound_return',biz_class:'return'});await ok('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'A',complete_job:true});assert.equal(count(j.job_id),0);assert.equal(status(j.job_id),'completed');
 const seed=await ok('sop_demo_prepare');const v=await start('v2_verify_job_start',{batch_id:seed.batch_id});await ok('v2_verify_job_finish',{job_id:v.job_id,worker_id:'A',complete_job:true,complete_batch:false});assert.equal(count(v.job_id),0);assert.equal(status(v.job_id),'completed');
});
