import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {quantityCases,quantityValues,seedQuantityPlan} from './inbound-export-fixture.mjs';

async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>{
  const response=await worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
  const result=await response.json();assert.equal(result.ok,true,result.error||result.message);return result;
 };
 await call('v2_inbound_plan_list');
 return {DB,call,export:async()=> (await call('v2_inbound_plan_export')).rows};
}
for(const spec of quantityCases)test('inbound export persisted quantities: '+spec.id,async t=>{
 const f=await fixture(t),id=seedQuantityPlan(f.DB,spec);
 const before=f.DB.raw.prepare('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=?').all(id);
 const rows=await f.export();assert.equal(rows.length,1);
 assert.deepEqual(quantityValues(rows[0]),spec.expected);
 assert.ok(quantityValues(rows[0]).every(n=>typeof n==='number'&&Number.isFinite(n)));
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=?').all(id),before);
});

test('courier export uses detail quantities once, independent of tracking, receipts, jobs and linked outbound representations',async t=>{
 const f=await fixture(t),tracking=['99000000001','99000000002','99000000003'];
 const p=await f.call('v2_inbound_plan_create',{customer:'QA 合成快递客户',biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:tracking}]});
 const line=f.DB.raw.prepare('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=?').get(p.id);
 // Represent a persisted historical quantity that differs from tracking count.
 f.DB.raw.prepare('UPDATE v2_inbound_plan_lines SET planned_qty=8,actual_qty=8,putaway_qty=8 WHERE id=?').run(line.id);
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET cargo_summary='合成999箱' WHERE id=?").run(p.id);
 f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,display_no,source_inbound_plan_id,planned_box_count,actual_box_count) VALUES('QA-OB','QA-OB',?,999,888)").run(p.id);
 f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,related_doc_id,job_type,status) VALUES('QA-JOB',?,'inbound_bulk','completed')").run(p.id);
 f.DB.raw.prepare("INSERT INTO v2_ops_job_results(id,job_id,result_json,result_lines_json) VALUES('RES-FINAL-QA-JOB','QA-JOB',?,?)").run(JSON.stringify({box_count:8,actual_box_count:8,location_count:2}),JSON.stringify([{unit_type:'courier',actual_qty:8,putaway_qty:8}]));
 for(const [i,code] of tracking.slice(0,2).entries()){
  f.DB.raw.prepare("INSERT INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name) VALUES(?,?,'8-1','2026-10-01T00:00:00Z','QA','QA','QA','QA')").run('QA-R'+i,code);
  for(let j=0;j<3;j++)f.DB.raw.prepare("INSERT INTO ck_courier_events(id,receipt_id,kind,actor_id,actor_name,detail,created_at) VALUES(?,?,'update','QA','QA','{}','2026-10-01T00:00:00Z')").run('QA-E'+i+j,'QA-R'+i);
 }
 const detail=await f.call('v2_inbound_plan_detail',{id:p.id});
 assert.equal(detail.lines[0].planned_qty,8);assert.equal(detail.lines[0].actual_qty,8);
 assert.equal(detail.plan.courier_progress.total,3);assert.equal(detail.plan.courier_progress.received,2);
 const snapshot=()=>JSON.stringify(['v2_inbound_plans','v2_inbound_plan_lines','v2_outbound_orders','v2_ops_job_results','ck_courier_receipts','ck_courier_events','ck_courier_plan_items'].map(table=>f.DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()));
 const before=snapshot(),rows=await f.export();
 assert.deepEqual(quantityValues(rows[0]),[8,8,0,0]);assert.equal(rows[0]['关联出库单数量'],1);
 assert.equal(snapshot(),before);
});
