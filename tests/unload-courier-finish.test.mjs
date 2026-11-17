import test from 'node:test';
import assert from 'node:assert/strict';
import {partialArrivalFixture} from './partial-arrival-fixture.mjs';
import {latestCutoff} from '../worker-v2/system-cutoff.js';
import worker from '../worker-v2/index.js';
const rows=async(f,sql,...args)=>(await f.DB.prepare(sql).bind(...args).all()).results;
async function scenario(t,{cutoff=true,biz='bulk',mixed=false,receipts=2}={}){
 const f=await partialArrivalFixture(t);const p=await f.create({biz_classes:[biz],...(mixed?{lines:[{unit_type:'courier',tracking_nos:f.codes},{unit_type:'carton',planned_qty:3}]}:{})});
 const j=await f.ok('sop_native_start',{payload:{action:'v2_unload_job_start',plan_ids:[p.id],client_req_id:crypto.randomUUID()},workers:[{id:'SYNTHETIC-TRUCK',name:'合成卸货员'}],lead_id:'SYNTHETIC-TRUCK',estimated_minutes:15});
 if(cutoff){const old=new Date(Date.parse(latestCutoff(Date.now()))-3600000).toISOString();await f.DB.prepare('UPDATE v2_ops_jobs SET created_at=?,updated_at=? WHERE id=?').bind(old,old,j.job_id).run();await f.DB.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').bind(old,j.job_id).run();f.env.SOP_DAILY_CUTOFF_ENABLED='true';await f.ok('sop_identity');}
 const closed=await rows(f,'SELECT * FROM v2_ops_job_workers WHERE job_id=?',j.job_id);for(let i=0;i<receipts;i++)await f.receive(i);const arrived=await f.detail(p);
 if(cutoff){const d=await f.ok('v2_ops_job_detail',{job_id:j.job_id});assert.equal(d.job.status,'system_closed');await f.ok('sop_native_resume',{job_id:j.job_id,revision:d.dispatch.revision,workers:[{id:'SYNTHETIC-RESUMED',name:'合成继续卸货员'}],lead_id:'SYNTHETIC-RESUMED'});}
 const finish={job_id:j.job_id,worker_id:cutoff?'SYNTHETIC-RESUMED':'SYNTHETIC-TRUCK',complete_job:true,plan_results:[{plan_id:p.id,lines:[],diff_note:'合成确认快递已收齐'}]};
 return {f,p,j,closed,arrived,finish};
}
test('courier-only truck can finish after cutoff, parcel arrival advanced plan, and explicit resume without rewinding plan or old labor',async t=>{
 const {f,p,j,closed,arrived,finish}=await scenario(t);assert.equal(arrived.plan.status,'completed');
 const before=await f.detail(p),receipts=await rows(f,'SELECT * FROM ck_courier_receipts ORDER BY id');const result=await f.call('v2_unload_job_finish',finish);assert.equal(result.ok,true,JSON.stringify(result));
 const after=await f.detail(p);assert.equal(after.plan.status,'completed');assert.equal(after.plan.unload_completed_at,before.plan.unload_completed_at);assert.equal(after.plan.unload_completed_by,before.plan.unload_completed_by);assert.deepEqual(after.lines,before.lines);assert.deepEqual(await rows(f,'SELECT * FROM ck_courier_receipts ORDER BY id'),receipts);
 for(const s of closed)assert.deepEqual((await rows(f,'SELECT * FROM v2_ops_job_workers WHERE id=?',s.id))[0],s);assert.equal((await rows(f,'SELECT status FROM v2_ops_jobs WHERE id=?',j.job_id))[0].status,'completed');assert.equal((await rows(f,'SELECT * FROM v2_ops_job_results WHERE job_id=?',j.job_id)).length,1);
 assert.equal((await f.call('v2_unload_job_finish',finish)).ok,true);assert.equal((await rows(f,'SELECT * FROM v2_ops_job_results WHERE job_id=?',j.job_id)).length,1);
});

const state=async(f,j,p)=>({job:await rows(f,'SELECT * FROM v2_ops_jobs WHERE id=?',j.job_id),workers:await rows(f,'SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id',j.job_id),results:await rows(f,'SELECT * FROM v2_ops_job_results WHERE job_id=?',j.job_id),plan:await rows(f,'SELECT * FROM v2_inbound_plans WHERE id=?',p.id),lines:await rows(f,'SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY id',p.id),trip:await rows(f,'SELECT * FROM ck_unload_trips WHERE job_id=?',j.job_id),links:await rows(f,'SELECT * FROM ck_unload_plan_links WHERE job_id=?',j.job_id)});
test('uninterrupted courier truck preserves actual scan arrival stamps and closes one team clock',async t=>{
 const {f,p,j,finish}=await scenario(t,{cutoff:false});const before=await f.detail(p);assert.equal(before.plan.status,'unloading');assert.ok(before.plan.unload_completed_at);assert.equal((await f.call('v2_unload_job_finish',finish)).ok,true);const after=await f.detail(p);assert.equal(after.plan.status,'completed');assert.equal(after.plan.unload_completed_at,before.plan.unload_completed_at);assert.equal((await rows(f,"SELECT * FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",j.job_id)).length,0);
});
for(const status of ['arrived_pending_putaway','tallied_pending_putaway','partially_completed','putting_away'])test('fully received courier plan '+status+' allows original unload closure with no fake receipt',async t=>{
 const {f,p,j,finish}=await scenario(t,{biz:'direct_ship'});await f.DB.prepare('UPDATE v2_inbound_plans SET status=? WHERE id=?').bind(status,p.id).run();const before=(await rows(f,'SELECT * FROM v2_inbound_plans WHERE id=?',p.id))[0];const reply=await f.call('v2_unload_job_finish',finish);assert.equal(reply.ok,true,JSON.stringify(reply));const after=(await rows(f,'SELECT * FROM v2_inbound_plans WHERE id=?',p.id))[0];assert.equal(after.unload_completed_at,before.unload_completed_at);assert.equal(after.unload_completed_by,before.unload_completed_by);assert.equal((await rows(f,'SELECT * FROM v2_ops_job_results WHERE job_id=?',j.job_id)).length,1);
});
for(const variant of ['missing-receipt','mixed-lines','cancelled','deleted','force-completed','manual-completed'])test(variant+' does not unlock advanced-plan completion',async t=>{
 const x=await scenario(t,{receipts:variant==='missing-receipt'?1:2,mixed:variant==='mixed-lines'}),{f,p,j,finish}=x;
 await f.DB.prepare("UPDATE v2_inbound_plans SET status='completed' WHERE id=?").bind(p.id).run();
 if(variant==='cancelled')await f.DB.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").bind(p.id).run();
 if(variant==='deleted')await f.DB.prepare('UPDATE v2_inbound_plans SET is_deleted=1 WHERE id=?').bind(p.id).run();
 if(variant==='force-completed')await f.DB.prepare('UPDATE v2_inbound_plans SET force_completed=1 WHERE id=?').bind(p.id).run();
 if(variant==='manual-completed')await f.DB.prepare("UPDATE v2_inbound_plans SET manual_completed_at='2026-11-01T00:00:00Z' WHERE id=?").bind(p.id).run();
 const before=await state(f,j,p),reply=await f.call('v2_unload_job_finish',finish);assert.equal(reply.ok,false,variant);assert.deepEqual(await state(f,j,p),before);
});
for(const mutation of ['cancel','remove-plan','quantity','new-line','receipt-time'])test('in-flight '+mutation+' atomically rejects completion before result or labor writes',async t=>{
 const {f,p,j,finish}=await scenario(t),batch=f.DB.batch.bind(f.DB);let armed=true,snapshot;
 f.DB.batch=async sql=>{if(armed&&sql.some(x=>x.sql?.startsWith('INSERT INTO v2_ops_job_results'))){armed=false;
 if(mutation==='cancel')await f.DB.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").bind(p.id).run();
 if(mutation==='remove-plan')await f.DB.prepare('UPDATE v2_inbound_plans SET is_deleted=1 WHERE id=?').bind(p.id).run();
 if(mutation==='quantity')await f.DB.prepare('UPDATE v2_inbound_plan_lines SET actual_qty=1 WHERE plan_id=?').bind(p.id).run();
 if(mutation==='new-line')await f.DB.prepare("INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty) VALUES('SYNTHETIC-EXTRA-LINE',?,99,'carton',3)").bind(p.id).run();
 if(mutation==='receipt-time')await f.DB.prepare("UPDATE ck_courier_receipts SET received_at='2026-11-01T00:00:00Z' WHERE tracking_no=?").bind(f.codes[0]).run();
 snapshot=await state(f,j,p);
 }return batch(sql);};
 const reply=await f.call('v2_unload_job_finish',finish);assert.equal(armed,false);assert.equal(reply.ok,false,JSON.stringify(reply));assert.match(reply.error,/记录已变化/);assert.deepEqual(await state(f,j,p),snapshot);assert.equal((await rows(f,'SELECT * FROM ck_unload_finish_checks')).length,0);
});
test('result insertion failure rolls back guard rows, all workers and receipts; same request can retry',async t=>{
 const {f,p,j,finish}=await scenario(t),before=await state(f,j,p);await f.DB.prepare("CREATE TRIGGER synthetic_unload_result_fail BEFORE INSERT ON v2_ops_job_results BEGIN SELECT RAISE(ABORT,'synthetic_result_failure'); END").run();assert.equal((await f.call('v2_unload_job_finish',finish)).ok,false);assert.deepEqual(await state(f,j,p),before);assert.equal((await rows(f,'SELECT * FROM ck_unload_finish_checks')).length,0);await f.DB.prepare('DROP TRIGGER synthetic_unload_result_fail').run();assert.equal((await f.call('v2_unload_job_finish',finish)).ok,true);
});
test('concurrent finish submissions save one result and release new crew once',async t=>{
 const {f,j,finish}=await scenario(t);const replies=await Promise.all([f.call('v2_unload_job_finish',finish),f.call('v2_unload_job_finish',finish)]);assert.ok(replies.every(r=>r.ok),JSON.stringify(replies));assert.equal((await rows(f,'SELECT * FROM v2_ops_job_results WHERE job_id=?',j.job_id)).length,1);assert.equal((await rows(f,"SELECT * FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",j.job_id)).length,0);assert.equal((await rows(f,'SELECT * FROM ck_unload_finish_checks')).length,0);
});
test('unrelated dispatcher cannot close the advanced courier plan job',async t=>{
 const {f,p,j,finish}=await scenario(t),before=await state(f,j,p);f.env.SOP_PUBLIC_TEST_ACCESS='false';f.env.SOP_USERS_JSON=JSON.stringify([{id:'SYNTH-OTHER',name:'合成无关派工员',role:'dispatcher',departments:['bulk'],key:'synthetic-other-only'}]);const send=(body,cookie='')=>worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie},body:JSON.stringify(body)}),f.env);const login=await send({action:'sop_login',sop_key:'synthetic-other-only'});assert.equal((await login.clone().json()).ok,true);const cookie=login.headers.get('set-cookie').split(';')[0];const reply=await(await send({action:'v2_unload_job_finish',client_req_id:crypto.randomUUID(),...finish},cookie)).json();assert.equal(reply.ok,false);assert.deepEqual(await state(f,j,p),before);
});
