import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';

// Acceptance candidate: actual Worker, in-memory SQLite, no online access.
const repo=path.resolve(process.env.CK_REVIEW_REPO||fileURLToPath(new URL('../',import.meta.url)));
const {default:worker}=await import(pathToFileURL(path.join(repo,'worker-v2/index.js')));
const {database}=await import(pathToFileURL(path.join(repo,'tests/d1-adapter.mjs')));
async function fixture(t){
  const DB=database();t.after(()=>DB.raw.close());
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_WORK_CHAIN_ENABLED:'true'};
  const call=async(action,body={})=> (await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env)).json();
  assert.equal((await call('sop_identity')).ok,true);
  const body={payload:{action:'v2_ops_job_start',flow_stage:'internal',job_type:'inventory',client_req_id:'QA-START-ATOMIC-ONCE'},workers:[{id:'QA-A',name:'QA alpha'},{id:'QA-B',name:'QA beta'}],lead_id:'QA-A',estimated_minutes:10,labor_department:'direct_ship'};
  const snapshot=()=>({jobs:DB.raw.prepare('SELECT id,status FROM v2_ops_jobs').all(),workers:DB.raw.prepare('SELECT id,worker_id,job_id,left_at FROM v2_ops_job_workers').all(),dispatch:DB.raw.prepare("SELECT id,state FROM sop_records WHERE kind='dispatch'").all(),events:DB.raw.prepare("SELECT request_id FROM sop_events WHERE action='sop_native_start'").all(),idem:DB.raw.prepare('SELECT idem_key FROM v2_idempotency_keys WHERE idem_key=?').all(body.payload.client_req_id)});
  return{DB,env,call,body,snapshot};
}
for(const failure of ['event','second_worker']){
  test('native start rolls back every row if '+failure+' registration fails, then retries the same request',async t=>{
    const f=await fixture(t),before=f.snapshot();
    const target=failure==='event'?"BEFORE INSERT ON sop_events WHEN NEW.action='sop_native_start'":"BEFORE INSERT ON v2_ops_job_workers WHEN NEW.worker_id='QA-B'";
    f.DB.raw.exec("CREATE TRIGGER qa_dispatch_failure "+target+" BEGIN SELECT RAISE(ABORT,'qa_dispatch_failure'); END");
    const failed=await f.call('sop_native_start',f.body);assert.equal(failed.ok,false);
    assert.deepEqual(f.snapshot(),before,'failed assignment must not retain lead clock, job or cached successful start');
    f.DB.raw.exec('DROP TRIGGER qa_dispatch_failure');
    const retried=await f.call('sop_native_start',f.body);assert.equal(retried.ok,true,retried.error);
    const after=f.snapshot();assert.equal(after.jobs.length,1);assert.equal(after.workers.filter(w=>!w.left_at).length,2);assert.equal(after.dispatch.length,1);assert.equal(after.events.length,1);assert.equal(after.idem.length,1);
    const again=await f.call('sop_native_start',f.body);assert.equal(again.ok,true);assert.equal(again.job_id,retried.job_id);assert.deepEqual(f.snapshot(),after);
  });
}
test('native start successful lost reply can be retried without creating another crew',async t=>{
  const f=await fixture(t);const committed=await f.call('sop_native_start',f.body);assert.equal(committed.ok,true,committed.error);
  // Discard the first response; submit exactly the same original intent.
  const repeated=await f.call('sop_native_start',f.body);assert.equal(repeated.ok,true);assert.equal(repeated.job_id,committed.job_id);
  const after=f.snapshot();assert.equal(after.jobs.length,1);assert.equal(after.workers.filter(w=>!w.left_at).length,2);assert.equal(after.dispatch.length,1);assert.equal(after.events.length,1);
});
test('concurrent identical native start requests leave only one job and one full crew',async t=>{
  const f=await fixture(t),replies=await Promise.all([f.call('sop_native_start',f.body),f.call('sop_native_start',f.body)]);
  assert.ok(replies.some(r=>r.ok));
  const after=f.snapshot();assert.equal(after.jobs.length,1);assert.equal(after.workers.filter(w=>!w.left_at).length,2);assert.equal(after.dispatch.length,1);assert.equal(after.events.length,1);assert.equal(after.idem.length,1);
});
for(const action of ['v2_inbound_job_start','v2_bulk_op_job_start','v2_pick_job_start','v2_outbound_stock_op_start','v2_issue_handle_start','v2_verify_job_start','v2_unplanned_unload_start','v2_unload_job_start'])test(action+' rolls back its document graph when non-lead registration fails, then same request retries once',async t=>{
 const f=await fixture(t);let payload={action,client_req_id:'QA-DOMAIN-'+action};
 if(action==='v2_inbound_job_start')Object.assign(payload,{job_type:'inbound_return',biz_class:'return',customer_name:'QA returns'});
 if(action==='v2_bulk_op_job_start')Object.assign(payload,{work_order_no:'QA-DOMAIN-BULK',customer:'QA only'});
 if(action==='v2_pick_job_start')payload.pick_doc_nos=['QA-DOMAIN-PICK-A','QA-DOMAIN-PICK-B'];
 if(action==='v2_outbound_stock_op_start'){f.env.SOP_WORK_CHAIN_ENABLED='false';const ob=await f.call('v2_outbound_order_create',{customer:'QA stock only',biz_class:'bulk',outbound_mode:'customer_pickup',uses_stock_operation:1});f.env.SOP_WORK_CHAIN_ENABLED='true';assert.equal(ob.ok,true,ob.error);payload.outbound_order_id=ob.id;}
 if(action==='v2_issue_handle_start'){f.DB.raw.prepare("INSERT INTO v2_issue_tickets(id,status,biz_class) VALUES('QA-ISSUE','open','bulk')").run();payload.issue_id='QA-ISSUE';}
 if(action==='v2_verify_job_start'){f.DB.raw.prepare("INSERT INTO v2_verify_batches(id,status) VALUES('QA-VERIFY','pending')").run();payload.batch_id='QA-VERIFY';}
 if(action==='v2_unload_job_start'){const plan=await f.call('v2_inbound_plan_create',{customer:'QA unload only',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:5}]});assert.equal(plan.ok,true,plan.error);payload.plan_ids=[plan.id];}
 const tables=['v2_ops_jobs','v2_ops_job_workers','sop_records','sop_events','v2_idempotency_keys','v2_inbound_plans','v2_inbound_plan_biz_tasks','v2_ops_job_pick_docs','v2_pick_worker_docs','v2_outbound_orders','v2_issue_tickets','v2_issue_handle_runs','v2_verify_batches','v2_field_feedbacks','ck_unload_trips','ck_unload_plan_links'];
 const snapshot=()=>Object.fromEntries(tables.map(table=>[table,f.DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 const before=snapshot();f.DB.raw.exec("CREATE TRIGGER qa_domain_failure BEFORE INSERT ON v2_ops_job_workers WHEN NEW.worker_id='QA-B' BEGIN SELECT RAISE(ABORT,'qa_domain_failure'); END");
 const body={...f.body,payload};assert.equal((await f.call('sop_native_start',body)).ok,false);assert.deepEqual(snapshot(),before,'document graph and every clock must roll back');
 f.DB.raw.exec('DROP TRIGGER qa_domain_failure');const result=await f.call('sop_native_start',body);assert.equal(result.ok,true,result.error);const after=snapshot();const retry=await f.call('sop_native_start',body);assert.equal(retry.ok,true,retry.error);assert.equal(retry.job_id,result.job_id);assert.deepEqual(snapshot(),after);
});
test('different generic requests cannot both claim the same currently free workers',async t=>{
 const f=await fixture(t),replies=await Promise.all([f.call('sop_native_start',f.body),f.call('sop_native_start',{...f.body,payload:{...f.body.payload,client_req_id:'QA-OTHER-START'}})]);assert.equal(replies.filter(r=>r.ok).length,1);assert.equal(f.snapshot().jobs.length,1);assert.equal(f.snapshot().workers.length,2);
});
test('a request cannot silently replay a changed crew or changed job type',async t=>{
 const f=await fixture(t);assert.equal((await f.call('sop_native_start',f.body)).ok,true);const before=f.snapshot();for(const body of [{...f.body,workers:f.body.workers.slice(0,1)},{...f.body,payload:{...f.body.payload,job_type:'qc'}}])assert.equal((await f.call('sop_native_start',body)).ok,false);assert.deepEqual(f.snapshot(),before);
});
