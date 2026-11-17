import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {latestCutoff} from '../worker-v2/system-cutoff.js';
const tally='inbound_direct_tally',code='SYNTHETIC-CUTOFF-TALLY';
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(a,b={})=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+JSON.stringify(r));return r;};
 const plan=await ok('v2_inbound_plan_create',{customer:'Synthetic cutoff recovery',biz_classes:['direct_ship'],external_inbound_no:code,lines:[{unit_type:'carton',planned_qty:2}]});
 const unload=await ok('v2_unload_job_start',{plan_id:plan.id,worker_id:'SYNTHETIC-UNLOAD'});await ok('v2_unload_job_finish',{job_id:unload.job_id,worker_id:'SYNTHETIC-UNLOAD',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:2}]});
 const staff=n=>[{id:'SYNTHETIC-TALLY-'+n,name:'Synthetic tally '+n}];
 const payload=(n,type=tally)=>({payload:{action:'v2_inbound_job_start',plan_id:plan.id,external_inbound_no:code,job_type:type,biz_class:'direct_ship',client_req_id:crypto.randomUUID()},workers:staff(n),lead_id:staff(n)[0].id,estimated_minutes:15});
 const rows=(table,where='',...args)=>DB.raw.prepare('SELECT * FROM '+table+(where?' WHERE '+where:'')+' ORDER BY rowid').all(...args);
 const freeze=async job=>{const old=new Date(Date.parse(latestCutoff(Date.now()))-3600000).toISOString();DB.raw.prepare('UPDATE v2_ops_jobs SET created_at=?,updated_at=? WHERE id=?').run(old,old,job.job_id);DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(old,job.job_id);env.SOP_DAILY_CUTOFF_ENABLED='true';await ok('sop_identity');};
 return {DB,env,call,ok,plan,staff,payload,rows,freeze};
}
test('04:00 tally is recognized as its original job and explicit resume retains the cutoff evidence',async t=>{
 const f=await fixture(t),request=f.payload(1),job=await f.ok('sop_native_start',request);await f.freeze(job);
 const before=f.rows('v2_ops_job_workers','job_id=?',job.job_id),audit=f.rows('ck_system_cutoff_audit'),jobs=f.rows('v2_ops_jobs'),results=f.rows('v2_ops_job_results');
 const recognized=await f.ok('v2_inbound_resolve_code',{code,biz_class:'direct_ship',job_type:tally});assert.equal(recognized.kind,'system');assert.equal(recognized.plan.inbound_progress.completed,0);assert.equal(recognized.plan.inbound_progress.items[0].status,'system_closed');assert.equal(recognized.plan.inbound_progress.items[0].job_id,job.job_id);assert.equal(recognized.plan.inbound_progress.items[0].job_type,tally);
 const blocked=await f.call('sop_native_start',f.payload(2));assert.equal(blocked.ok,false);assert.match(blocked.error,/04:00.*原任务/);assert.doesNotMatch(blocked.error,/UNIQUE|SQLITE/);
 assert.deepEqual(f.rows('v2_ops_jobs'),jobs);assert.deepEqual(f.rows('v2_ops_job_workers','job_id=?',job.job_id),before);assert.deepEqual(f.rows('v2_ops_job_results'),results);
 const oldReplay=await f.ok('sop_native_start',request);assert.equal(oldReplay.job_id,job.job_id);assert.deepEqual(f.rows('v2_ops_jobs'),jobs,'an old successful start replay cannot reopen the forced job');
 const detail=await f.ok('v2_ops_job_detail',{job_id:job.job_id});const resume={job_id:job.job_id,revision:detail.dispatch.revision,workers:f.staff(2),lead_id:f.staff(2)[0].id,client_req_id:crypto.randomUUID()};
 const responses=await Promise.all([f.call('sop_native_resume',resume),f.call('sop_native_resume',resume)]);assert.ok(responses.some(x=>x.ok));assert.equal((await f.ok('sop_native_resume',resume)).job_id,job.job_id);
 const segments=f.rows('v2_ops_job_workers','job_id=?',job.job_id);assert.equal(segments.length,2);assert.deepEqual(segments.find(x=>x.id===before[0].id),before[0]);assert.equal(segments.filter(x=>!x.left_at).length,1);assert.deepEqual(f.rows('ck_system_cutoff_audit'),audit);
 const finish={job_id:job.job_id,worker_id:f.staff(2)[0].id,complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:2}]};await f.ok('v2_inbound_job_finish',finish);await f.ok('v2_inbound_job_finish',finish);assert.equal(f.rows('v2_ops_job_results','job_id=?',job.job_id).length,1);assert.equal(f.rows('v2_ops_jobs','job_type=?',tally).length,1);assert.equal(f.rows('v2_inbound_plan_lines','plan_id=?',f.plan.id)[0].putaway_qty,2);
});
test('paused and completed stages reject fresh starts; cancelled stage history permits one new stage',async t=>{
 const f=await fixture(t),job=await f.ok('sop_native_start',f.payload(1));await f.ok('sop_native_pause',{job_id:job.job_id,revision:1,reason:'Synthetic pause'});
 const paused=await f.call('sop_native_start',f.payload(2));assert.equal(paused.ok,false);assert.doesNotMatch(paused.error,/UNIQUE|SQLITE/);
 f.DB.raw.prepare("UPDATE v2_ops_jobs SET status='cancelled' WHERE id=?").run(job.job_id);f.DB.raw.prepare("UPDATE sop_records SET state=json_set(state,'$.status','cancelled') WHERE id=?").run(job.job_id);
 const replacement=await f.ok('sop_native_start',f.payload(2));assert.notEqual(replacement.job_id,job.job_id);assert.equal(f.rows('v2_ops_jobs','job_type=?',tally).length,2);
 await f.ok('v2_inbound_job_finish',{job_id:replacement.job_id,worker_id:f.staff(2)[0].id,complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:2}]});
 const completed=await f.call('sop_native_start',f.payload(3));assert.equal(completed.ok,false);assert.match(completed.error,/已完成/);assert.equal(f.rows('v2_ops_job_results','job_id=?',replacement.job_id).length,1);
});
test('concurrent new tally starts keep one stage and return a business explanation to the loser',async t=>{
 const f=await fixture(t),replies=await Promise.all([f.call('sop_native_start',f.payload(1)),f.call('sop_native_start',f.payload(2))]);assert.equal(replies.filter(x=>x.ok).length,1);assert.doesNotMatch(replies.find(x=>!x.ok).error,/UNIQUE|SQLITE/);assert.equal(f.rows('v2_ops_jobs','job_type=?',tally).length,1);
 const job=replies.find(x=>x.ok);assert.equal(f.rows('v2_ops_job_workers','job_id=?',job.job_id).length,1);
});
test('failed new tally transaction retries the same request without partial jobs or crew',async t=>{
 const f=await fixture(t),request=f.payload(1);f.DB.raw.exec("CREATE TRIGGER synthetic_tally_failure BEFORE INSERT ON v2_ops_job_workers WHEN NEW.worker_id='SYNTHETIC-TALLY-1' BEGIN SELECT RAISE(ABORT,'synthetic tally failure'); END");
 const failed=await f.call('sop_native_start',request);assert.equal(failed.ok,false);assert.match(failed.error,/synthetic tally failure/);assert.equal(f.rows('v2_ops_jobs','job_type=?',tally).length,0);assert.equal(f.rows('v2_idempotency_keys','idem_key=?',request.payload.client_req_id).length,0);
 f.DB.raw.exec('DROP TRIGGER synthetic_tally_failure');const saved=await f.ok('sop_native_start',request);assert.equal((await f.ok('sop_native_start',request)).job_id,saved.job_id);assert.equal(f.rows('v2_ops_job_workers','job_id=?',saved.job_id).length,1);
});
test('a system-closed putaway remains linked to its original stage and can resume without re-tallying',async t=>{
 const f=await fixture(t),tallied=await f.ok('sop_native_start',f.payload(1));await f.ok('v2_inbound_job_finish',{job_id:tallied.job_id,worker_id:f.staff(1)[0].id,complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:2}]});
 const putaway=await f.ok('sop_native_start',f.payload(2,'inbound_direct_putaway'));await f.freeze(putaway);
 const resolved=await f.ok('v2_inbound_resolve_code',{code,biz_class:'direct_ship',job_type:'inbound_direct_putaway'}),ref=resolved.plan.inbound_progress.items[0];assert.equal(ref.status,'system_closed');assert.equal(ref.job_id,putaway.job_id);assert.equal(ref.job_type,'inbound_direct_putaway');assert.equal(ref.tally_job_id,tallied.job_id);
 const duplicate=await f.call('sop_native_start',f.payload(3,'inbound_direct_putaway'));assert.equal(duplicate.ok,false);assert.doesNotMatch(duplicate.error,/UNIQUE|SQLITE/);
 const detail=await f.ok('v2_ops_job_detail',{job_id:putaway.job_id});await f.ok('sop_native_resume',{job_id:putaway.job_id,revision:detail.dispatch.revision,workers:f.staff(3),lead_id:f.staff(3)[0].id});
 await f.ok('v2_inbound_job_finish',{job_id:putaway.job_id,worker_id:f.staff(3)[0].id,complete_job:true,location_count:2,used_forklift:false});assert.equal(f.rows('v2_ops_jobs','job_type=?','inbound_direct_putaway').length,1);assert.equal(f.rows('v2_ops_job_results','job_id=?',putaway.job_id).length,1);assert.equal(f.rows('v2_inbound_plan_lines','plan_id=?',f.plan.id)[0].putaway_qty,2);
});
