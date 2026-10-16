import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';

// Regression candidates for FIELD-ATOMIC-01/02. No online access or disk DB.
// Copy to <checkout>/tests or set CK_REVIEW_REPO to an isolated checkout.
const repo=path.resolve(process.env.CK_REVIEW_REPO || fileURLToPath(new URL('../',import.meta.url)));
const {default:worker}=await import(pathToFileURL(path.join(repo,'worker-v2/index.js')));
const {database}=await import(pathToFileURL(path.join(repo,'tests/d1-adapter.mjs')));
function fixture(t){
  const DB=database();t.after(()=>DB.raw.close());
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_WORK_CHAIN_ENABLED:'true'};
  const call=async(action,body={})=> (await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env)).json();
  const ok=async(action,body)=>{const result=await call(action,body);assert.equal(result.ok,true,action+': '+(result.error||result.message));return result;};
  const start=(action,body={},workers=[{id:'QA-A',name:'验收甲'},{id:'QA-B',name:'验收乙'}])=>ok('sop_native_start',{payload:{action,client_req_id:crypto.randomUUID(),...body},workers,lead_id:workers[0].id,estimated_minutes:15,labor_department:'direct_ship'});
  const snap=(job,plan)=>({
    job:DB.raw.prepare('SELECT status,active_worker_count FROM v2_ops_jobs WHERE id=?').get(job),
    workers:DB.raw.prepare('SELECT worker_id,joined_at,left_at,minutes_worked,leave_reason FROM v2_ops_job_workers WHERE job_id=? ORDER BY worker_id').all(job),
    results:DB.raw.prepare('SELECT id,result_json FROM v2_ops_job_results WHERE job_id=? ORDER BY id').all(job),
    ...(plan?{plan:DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(plan),lines:DB.raw.prepare('SELECT unit_type,actual_qty,putaway_qty FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY id').all(plan),departments:DB.raw.prepare('SELECT biz_class,status,job_id FROM v2_inbound_plan_biz_tasks WHERE plan_id=? ORDER BY biz_class').all(plan)}:{})
  });
  return{DB,call,ok,start,snap};
}
async function inbound(f,codes=['QA-ATOMIC-A']){
  const plan=await f.ok('v2_inbound_plan_create',{customer:'QA atomic inbound',biz_classes:['direct_ship'],external_inbound_nos:codes,lines:[{unit_type:'carton',planned_qty:50}]});
  const unload=await f.ok('v2_unload_job_start',{plan_id:plan.id,worker_id:'QA-U',worker_name:'验收卸货',biz_class:'bulk'});
  await f.ok('v2_unload_job_finish',{job_id:unload.job_id,worker_id:'QA-U',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}],box_count:50});
  const jobs=[];
  for(const [i,code]of codes.entries())jobs.push(await f.start('v2_inbound_job_start',{plan_id:plan.id,external_inbound_no:code,biz_class:'direct_ship',job_type:'inbound_direct'},[{id:'QA-'+i+'A',name:'验收'+i+'甲'},{id:'QA-'+i+'B',name:'验收'+i+'乙'}]));
  const finish=(i,qty,request='QA-FINISH-'+i)=>f.call('v2_inbound_job_finish',{job_id:jobs[i].job_id,worker_id:'QA-'+i+'A',complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:qty}],client_req_id:request});
  return{plan,jobs,finish};
}

for(const point of ['line','department','plan']){
  test('inbound group finish rolls back result, all clocks, job and plan if '+point+' write fails',async t=>{
    const f=fixture(t),x=await inbound(f),before=f.snap(x.jobs[0].job_id,x.plan.id);
    const target={line:"BEFORE UPDATE OF putaway_qty ON v2_inbound_plan_lines WHEN NEW.putaway_qty!=OLD.putaway_qty",department:"BEFORE UPDATE OF status ON v2_inbound_plan_biz_tasks WHEN NEW.status='completed' AND NEW.biz_class='direct_ship'",plan:"BEFORE UPDATE OF status ON v2_inbound_plans WHEN NEW.status='completed'"}[point];
    f.DB.raw.exec("CREATE TRIGGER qa_finish_failure "+target+" BEGIN SELECT RAISE(ABORT,'qa_finish_failure'); END");
    const failed=await x.finish(0,50);
    assert.equal(failed.ok,false);
    assert.deepEqual(f.snap(x.jobs[0].job_id,x.plan.id),before,'failed transaction must leave original working state and both clocks open');
    f.DB.raw.exec('DROP TRIGGER qa_finish_failure');
    assert.equal((await x.finish(0,50)).ok,true);
    const after=f.snap(x.jobs[0].job_id,x.plan.id);
    assert.equal(after.job.status,'completed');assert.equal(after.job.active_worker_count,0);
    assert.equal(after.workers.filter(w=>!w.left_at).length,0);
    assert.equal(after.results.length,1);assert.equal(after.lines[0].putaway_qty,50);
    assert.equal(after.departments[0].status,'completed');assert.equal(after.plan.status,'completed');
    assert.equal((await x.finish(0,50)).ok,true);
    assert.deepEqual(f.snap(x.jobs[0].job_id,x.plan.id),after,'same-request replay must preserve all committed values');
  });
}

test('inbound concurrent completion of the same job with different request IDs commits quantity once',async t=>{
  const f=fixture(t),x=await inbound(f);
  const replies=await Promise.all([x.finish(0,50,'QA-CONCURRENT-ONE'),x.finish(0,50,'QA-CONCURRENT-TWO')]);
  assert.ok(replies.some(r=>r.ok),'one completion must succeed');
  const after=f.snap(x.jobs[0].job_id,x.plan.id);
  assert.equal(after.job.status,'completed');assert.equal(after.results.length,1);
  assert.equal(after.lines[0].putaway_qty,50);assert.equal(after.workers.filter(w=>!w.left_at).length,0);
  const third=await x.finish(0,50,'QA-CONCURRENT-THREE');assert.equal(third.ok,true);
  assert.deepEqual(f.snap(x.jobs[0].job_id,x.plan.id),after);
});

test('two external-number jobs finishing together close the department and parent plan from committed SQL state',async t=>{
  const f=fixture(t),x=await inbound(f,['QA-CODE-A','QA-CODE-B']);
  const replies=await Promise.all([x.finish(0,20),x.finish(1,30)]);
  for(const r of replies)assert.equal(r.ok,true,r.error||r.message);
  const after=f.snap(x.jobs[1].job_id,x.plan.id);
  assert.equal(after.lines[0].putaway_qty,50);
  assert.equal(after.departments[0].status,'completed');
  assert.equal(after.plan.status,'completed');
  const detail=await f.ok('v2_inbound_plan_detail',{id:x.plan.id});
  assert.equal(detail.plan.inbound_progress.completed,2);
  assert.match(detail.biz_tasks[0].worker_names,/验收0甲/);assert.match(detail.biz_tasks[0].worker_names,/验收1甲/);
});

test('time-only generic finish rolls back all crew clocks if job completion fails, then retries once',async t=>{
  const f=fixture(t),job=await f.start('v2_ops_job_start',{flow_stage:'internal',job_type:'qc'}),before=f.snap(job.job_id);
  f.DB.raw.exec("CREATE TRIGGER qa_generic_failure BEFORE UPDATE OF status ON v2_ops_jobs WHEN NEW.status='completed' AND NEW.job_type='qc' BEGIN SELECT RAISE(ABORT,'qa_generic_failure'); END");
  const body={job_id:job.job_id,worker_id:'QA-A',client_req_id:'QA-GENERIC-ONCE'};
  assert.equal((await f.call('v2_ops_job_finish',body)).ok,false);
  assert.deepEqual(f.snap(job.job_id),before);
  f.DB.raw.exec('DROP TRIGGER qa_generic_failure');
  assert.equal((await f.call('v2_ops_job_finish',body)).ok,true);
  const after=f.snap(job.job_id);assert.equal(after.job.status,'completed');assert.equal(after.workers.filter(w=>!w.left_at).length,0);
  // Time-only tasks must keep their existing no-quantity-result semantics.
  assert.equal(after.results.length,0);
  assert.equal((await f.call('v2_ops_job_finish',body)).ok,true);assert.deepEqual(f.snap(job.job_id),after);
});

for(const changed of ['cancelled','deleted','reference'])test('inbound completion rechecks '+changed+' plan immediately before the atomic batch',async t=>{
 const f=fixture(t),x=await inbound(f),batch=f.DB.batch.bind(f.DB);let first=true;
 f.DB.batch=async statements=>{if(first&&statements.some(s=>s.sql.includes('INSERT INTO v2_ops_job_results'))){first=false;f.DB.raw.prepare(changed==='cancelled'?"UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?":changed==='deleted'?"UPDATE v2_inbound_plans SET is_deleted=1 WHERE id=?":"UPDATE v2_inbound_plans SET external_inbound_no='QA-CHANGED' WHERE id=?").run(x.plan.id);}return batch(statements);};
 const response=await x.finish(0,50);assert.equal(response.ok,false);const after=f.snap(x.jobs[0].job_id,x.plan.id);assert.equal(after.job.status,'working');assert.equal(after.workers.filter(w=>!w.left_at).length,2);assert.equal(after.results.length,0);assert.equal(after.lines[0].putaway_qty,0);assert.notEqual(after.departments[0].status,'completed');
});
