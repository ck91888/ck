import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
// Copy into checkout/tests or set CK_REVIEW_REPO. Only in-memory synthetic data.
const repo=path.resolve(process.env.CK_REVIEW_REPO||fileURLToPath(new URL('../',import.meta.url)));
const {default:worker}=await import(pathToFileURL(path.join(repo,'worker-v2/index.js')));
const {database}=await import(pathToFileURL(path.join(repo,'tests/d1-adapter.mjs')));
function fixture(t){
  const DB=database();t.after(()=>DB.raw.close());
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
  const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
  const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
  const workers=[{id:'QA-ATOMIC-A',name:'QA atomic A'},{id:'QA-ATOMIC-B',name:'QA atomic B'}];
  const start=(action,data={},native=true)=>native?ok('sop_native_start',{payload:{action,client_req_id:crypto.randomUUID(),...data},workers,lead_id:workers[0].id,estimated_minutes:15,labor_department:'bulk'}):ok(action,{worker_id:workers[0].id,worker_name:workers[0].name,...data});
  const snap=(job,order)=>({job:DB.raw.prepare('SELECT status,active_worker_count,customer,finished_at FROM v2_ops_jobs WHERE id=?').get(job),workers:DB.raw.prepare('SELECT worker_id,left_at,minutes_worked,leave_reason FROM v2_ops_job_workers WHERE job_id=? ORDER BY worker_id').all(job),results:DB.raw.prepare('SELECT id,result_json FROM v2_ops_job_results WHERE job_id=? ORDER BY id').all(job),pick_docs:DB.raw.prepare('SELECT pick_doc_no,pick_status,pick_finished_at FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY rowid').all(job),worker_docs:DB.raw.prepare('SELECT worker_id,status,finished_at,minutes_worked FROM v2_pick_worker_docs WHERE job_id=? ORDER BY rowid').all(job),...(order?{order:DB.raw.prepare('SELECT status,actual_box_count,actual_pallet_count,stock_operation_status,stock_operation_result_json FROM v2_outbound_orders WHERE id=?').get(order)}:{})});
  return{DB,call,ok,start,snap,workers};
}
const bulkBody=job=>({job_id:job.job_id,worker_id:'QA-ATOMIC-A',total_operated_box_count:20,pallet_count:2,client_req_id:'QA-STABLE-FINISH'});
test('native bulk rankings recognize legacy counter fields and corrections replace prior quantities',async t=>{
 const f=fixture(t),j=await f.start('v2_bulk_op_job_start',{work_order_no:'QA-RANKING',customer:'QA synthetic customer'});
 await f.ok('v2_bulk_op_job_finish',{...bulkBody(j),packed_box_count:10,total_operated_box_count:10,pallet_count:0});
 const values=async expected=>{const dashboard=await f.ok('sop_dashboard');for(const metric of ['打包','操作箱数']){const items=dashboard.rankings.filter(x=>x.tasks.includes(j.job_id)&&x.metric===metric);assert.equal(items.length,2);assert.ok(items.every(x=>x.quantity===expected/2),JSON.stringify(items));}};
 await values(10);for(const n of [8,6])await f.ok('v2_ops_job_result_update',{job_id:j.job_id,reason:'QA correction',result_json:{packed_box_count:n,total_operated_box_count:n}});await values(6);
});
for(const native of [true,false])for(const point of ['result','job','linked_order'])test((native?'native':'in-flight legacy')+' bulk finish rolls back all crew, result, job and linked order if '+point+' write fails',async t=>{
  const f=fixture(t);let order;
  if(point==='linked_order')order=await f.ok('v2_outbound_order_create',{customer:'QA synthetic customer',biz_class:'bulk',outbound_mode:'customer_pickup',planned_box_count:20});
  const j=await f.start('v2_bulk_op_job_start',{work_order_no:order?.id||'QA-BULK-'+point,customer:'QA synthetic customer'},native),before=f.snap(j.job_id,order?.id);
  const target={result:'BEFORE INSERT ON v2_ops_job_results',job:"BEFORE UPDATE OF status ON v2_ops_jobs WHEN NEW.status='completed'",linked_order:"BEFORE UPDATE OF status ON v2_outbound_orders WHEN NEW.status='ready_to_ship'"}[point];
  f.DB.raw.exec("CREATE TRIGGER qa_failure "+target+" BEGIN SELECT RAISE(ABORT,'qa_failure'); END");
  const failed=await f.call('v2_bulk_op_job_finish',bulkBody(j));assert.equal(failed.ok,false);
  assert.deepEqual(f.snap(j.job_id,order?.id),before,'a rejected completion must preserve both active clocks and every business record');
  f.DB.raw.exec('DROP TRIGGER qa_failure');await f.ok('v2_bulk_op_job_finish',bulkBody(j));
  const after=f.snap(j.job_id,order?.id);assert.equal(after.job.status,'completed');assert.equal(after.workers.filter(w=>!w.left_at).length,0);assert.equal(after.results.length,1);
  if(order){assert.equal(after.order.status,'ready_to_ship');assert.equal(after.order.actual_box_count,20);assert.equal(after.order.actual_pallet_count,2);}
  await f.ok('v2_bulk_op_job_finish',bulkBody(j));assert.deepEqual(f.snap(j.job_id,order?.id),after);
});
for(const sameRequest of [false,true])test('native bulk concurrent completion claims one job once with '+(sameRequest?'same':'different')+' request keys',async t=>{
  const f=fixture(t),j=await f.start('v2_bulk_op_job_start',{work_order_no:'QA-BULK-CONCURRENT',customer:'QA synthetic customer'}),body=bulkBody(j);
  const replies=await Promise.all([f.call('v2_bulk_op_job_finish',body),f.call('v2_bulk_op_job_finish',{...body,client_req_id:sameRequest?body.client_req_id:'QA-OTHER-FINISH'})]);
  assert.ok(replies.every(r=>r.ok),JSON.stringify(replies));const after=f.snap(j.job_id);
  assert.equal(after.results.length,1,'one business completion produces one effective result across simultaneous retries');
  assert.equal(after.job.status,'completed');assert.equal(after.workers.filter(w=>!w.left_at).length,0);
  assert.equal((await f.ok('v2_dashboard_order_list',{job_type:'bulk_op'})).items[0].total_operated_box_count_sum,20);
});
test('native bulk sequential lost-reply replay preserves one result and clock timestamps',async t=>{
  const f=fixture(t),j=await f.start('v2_bulk_op_job_start',{work_order_no:'QA-BULK-REPLAY',customer:'QA synthetic customer'});
  await f.ok('v2_bulk_op_job_finish',bulkBody(j));const after=f.snap(j.job_id);await f.ok('v2_bulk_op_job_finish',bulkBody(j));assert.deepEqual(f.snap(j.job_id),after);assert.equal(after.results.length,1);
});
for(const native of [true,false])test((native?'native':'in-flight legacy')+' pick finalize is atomic after all document and result writes',async t=>{
  const f=fixture(t),numbers=['QA-PICK-A','QA-PICK-B'],j=await f.start('v2_pick_job_start',{pick_doc_nos:numbers},native);
  if(!native){await f.ok('v2_pick_job_start_by_docs',{pick_doc_nos:numbers,worker_id:f.workers[0].id,worker_name:f.workers[0].name});await f.ok('v2_pick_job_finish',{job_id:j.job_id,worker_id:f.workers[0].id});}
  const before=f.snap(j.job_id);f.DB.raw.exec("CREATE TRIGGER qa_failure BEFORE UPDATE OF status ON v2_ops_jobs WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'qa_failure'); END");
  const body={job_id:j.job_id,worker_id:f.workers[0].id,client_req_id:'QA-PICK-FINISH'};
  assert.equal((await f.call('v2_pick_job_finalize',body)).ok,false);assert.deepEqual(f.snap(j.job_id),before);
  f.DB.raw.exec('DROP TRIGGER qa_failure');await f.ok('v2_pick_job_finalize',body);const after=f.snap(j.job_id);
  assert.equal(after.job.status,'completed');assert.equal(after.results.filter(r=>JSON.parse(r.result_json).kind==='trip_finalize').length,1);assert.ok(after.pick_docs.every(d=>d.pick_status==='completed'));await f.ok('v2_pick_job_finalize',body);assert.deepEqual(f.snap(j.job_id),after);
});
for(const native of [true,false])test((native?'native':'in-flight legacy')+' stock completion atomically updates the customer-service milestone',async t=>{
  const f=fixture(t),order=await f.ok('v2_outbound_order_create',{customer:'QA synthetic stock customer',biz_class:'bulk',outbound_mode:'customer_pickup',uses_stock_operation:1}),j=await f.start('v2_outbound_stock_op_start',{outbound_order_id:order.id},native),before=f.snap(j.job_id,order.id);
  f.DB.raw.exec("CREATE TRIGGER qa_failure BEFORE UPDATE OF status ON v2_outbound_orders WHEN NEW.status='pending_outbound_update' BEGIN SELECT RAISE(ABORT,'qa_failure'); END");
  const body={job_id:j.job_id,worker_id:f.workers[0].id,box_count:20,pallet_count:2,client_req_id:'QA-STOCK-FINISH'};
  assert.equal((await f.call('v2_outbound_stock_op_finish',body)).ok,false);assert.deepEqual(f.snap(j.job_id,order.id),before);
  f.DB.raw.exec('DROP TRIGGER qa_failure');await f.ok('v2_outbound_stock_op_finish',body);const after=f.snap(j.job_id,order.id);
  assert.equal(after.job.status,'completed');assert.equal(after.results.length,1);assert.equal(after.order.status,'pending_outbound_update');assert.equal(JSON.parse(after.order.stock_operation_result_json).total_box_count,20);await f.ok('v2_outbound_stock_op_finish',body);assert.deepEqual(f.snap(j.job_id,order.id),after);
});
