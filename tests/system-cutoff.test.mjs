import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {runSystemCutoff,latestCutoff,guardSystemCutoffMutation,correctSystemCutoff} from '../worker-v2/system-cutoff.js';
import {nativePeople,ensureNativePauseGuards} from '../worker-v2/native-lifecycle.js';
import {handleSop} from '../worker-v2/sop.js';
import {ensureCrewBorrow,reconcileCrewReturns} from '../worker-v2/crew-borrow.js';
import {attendanceReport} from '../worker-v2/attendance-time.js';
import {measureSegments} from '../worker-v2/actual-work-report.js';
import {segmentMinutes} from '../worker-v2/dashboard-time.js';
const iso=n=>new Date(n).toISOString();
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const cut=Date.parse(latestCutoff(Date.now())),old=iso(cut-2*3600000),fresh=iso(cut+60000),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true'};
 const job=(id,status='working',start=old,type='change_order',kind='dispatch')=>{DB.raw.prepare('INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,created_at,updated_at,active_worker_count,shared_result_json) VALUES(?,?,?,?,?,?,?,?)').run(id,type,'bulk',status,start,start,2,'{"recorded":7}');if(kind)DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,1,?,?,?)').run(id,kind,'bulk',JSON.stringify({status,job_type:type,owner_id:'SYNTHETIC',owner:'Synthetic owner',created_at:start,workers:[{id:'SYNTHETIC-A',name:'Synthetic A'}],lead_id:'SYNTHETIC-A',result:{quantity:7}}),start);};
 const clock=(id,jobId,start=old,worker='SYNTHETIC-'+id)=>DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,minutes_worked) VALUES(?,?,?,?,?,17.5)').run(id,jobId,worker,worker,start);
 const get=id=>DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(id),state=id=>JSON.parse(DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(id).state),segments=()=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers ORDER BY id').all();
 return {DB,env,cut,old,fresh,job,clock,get,state,segments,run:()=>runSystemCutoff(env,cut+3600000)};
}
test('KST boundary is exact and a 04:00 execution belongs to the next operational day',()=>{
 assert.equal(latestCutoff(Date.parse('2026-10-23T03:59:59+09:00')),'2026-10-21T19:00:00.000Z');assert.equal(latestCutoff(Date.parse('2026-10-23T04:00:00+09:00')),'2026-10-22T19:00:00.000Z');
});
test('cutoff atomically ends execution and crew with full snapshots, never business completion or new work',async t=>{
 const f=await fixture(t);for(const status of ['working','paused','pending','awaiting_close']){f.job(status,status);f.clock('CLOCK-'+status,status);}
 f.job('OFFICE','working',f.old,'office_order_processing');f.clock('OFFICE-CLOCK','OFFICE');f.job('NEW','working',f.fresh);f.clock('NEW-CLOCK','NEW',f.fresh);f.job('EXACT','working',iso(f.cut));f.clock('EXACT-CLOCK','EXACT',iso(f.cut));f.job('DONE','completed');f.clock('DONE-REMNANT','DONE');
 f.DB.raw.prepare("INSERT INTO v2_inbound_plans(id,status,customer) VALUES('SYNTHETIC-IN','pending','Synthetic')").run();f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,status,planned_box_count) VALUES('SYNTHETIC-OUT','pending',7)").run();
 const before=f.segments(),newJob=f.get('NEW'),inbound=f.DB.raw.prepare('SELECT * FROM v2_inbound_plans').all(),outbound=f.DB.raw.prepare('SELECT * FROM v2_outbound_orders').all();
 await f.run();
 for(const id of ['working','paused','pending','awaiting_close','OFFICE']){assert.equal(f.get(id).status,'system_closed');assert.notEqual(f.get(id).finished_at,iso(f.cut));assert.equal(f.get(id).shared_result_json,'{"recorded":7}');assert.equal(f.state(id).status,'system_closed');assert.equal(f.state(id).result.quantity,7);}
 assert.deepEqual(f.get('NEW'),newJob);assert.equal(f.get('EXACT').status,'working');assert.equal(f.get('DONE').status,'completed');
 for(const s of f.segments()){const original=before.find(x=>x.id===s.id);assert.equal(s.joined_at,original.joined_at);assert.equal(s.minutes_worked,original.minutes_worked);if(!['NEW-CLOCK','EXACT-CLOCK'].includes(s.id)){assert.equal(s.left_at,iso(f.cut));assert.equal(s.leave_reason,'system_cutoff:unverified');const audit=f.DB.raw.prepare("SELECT * FROM ck_system_cutoff_audit WHERE kind='segment' AND row_id=?").get(s.id);assert.deepEqual(JSON.parse(audit.before_json),{...original});}}
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_inbound_plans').all(),inbound);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_outbound_orders').all(),outbound);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);
 const snapshot=f.DB.raw.prepare('SELECT * FROM ck_system_cutoff_audit ORDER BY kind,row_id').all();await Promise.all([f.run(),f.run()]);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM ck_system_cutoff_audit ORDER BY kind,row_id').all(),snapshot);
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_system_cutoff'").get().n,5);
});
test('missed-run recovery uses first elapsed boundary, excludes provisional work from all labor measurements',async t=>{
 const f=await fixture(t),start=iso(f.cut-2*86400000+3600000);f.job('HISTORY','working',start);f.clock('HISTORY-CLOCK','HISTORY',start);await f.run();const s=f.segments()[0],expected=iso(f.cut-86400000);
 assert.equal(s.left_at,expected);assert.equal(f.get('HISTORY').paused_at,expected);
 const range={start:Date.parse(start)-3600000,end:f.cut+3600000},measured=measureSegments([s],range,f.cut+3600000)[0];assert.equal(measured.seconds,0);assert.equal(measured.provisional,1);assert.match(measured.anomaly_reason,/强制结束/);assert.equal(segmentMinutes(s,range,f.cut).minutes,0);
 const day=iso(Date.parse(start)+9*3600000).slice(0,10),totals=attendanceReport({badgeId:s.worker_id,day,inAt:start,outAt:expected,breaks:[]},[{badgeId:s.worker_id,start,end:expected,department:'bulk',reason:s.leave_reason}],iso(f.cut),day);assert.equal(totals.bulk,0);assert.ok(totals.conflict>0);assert.ok(totals.flags.some(x=>x.includes('人效')));
});
test('loan closure never restores the source, and old requests cannot reopen or complete forced work',async t=>{
 const f=await fixture(t);f.job('SOURCE');f.job('DEST');f.clock('SOURCE-CLOCK','SOURCE',f.old,'SYNTHETIC-A');await ensureCrewBorrow(f.env);
 f.DB.raw.prepare("INSERT INTO ck_crew_borrows(id,request_id,actor_id,actor_name,destination_job_id,worker_id,worker_name,source_job_id,source_segment_id,source_kind,source_revision,source_owner_id,source_day,status,borrowed_at) VALUES('LOAN','LOAN-REQUEST','SYNTHETIC','Synthetic','DEST','SYNTHETIC-A','Synthetic A','SOURCE','SOURCE-CLOCK','dispatch',1,'SYNTHETIC','2026-10-22','borrowed',?)").run(f.old);
 f.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=? WHERE id='SOURCE-CLOCK'").run(f.old);f.clock('DEST-CLOCK','DEST',f.old,'SYNTHETIC-A');await f.run();assert.equal(f.DB.raw.prepare('SELECT status FROM ck_crew_borrows').get().status,'closed');await reconcileCrewReturns(f.env,'DEST');assert.equal(f.segments().filter(s=>!s.left_at).length,0);
 await assert.rejects(guardSystemCutoffMutation({action:'v2_ops_job_finish',job_id:'SOURCE'},f.env),/强制结束/);await guardSystemCutoffMutation({action:'sop_native_resume',job_id:'SOURCE'},f.env);
 assert.throws(()=>f.DB.raw.prepare("INSERT INTO v2_ops_job_results(id,job_id) VALUES('LATE-RESULT','SOURCE')").run(),/system_cutoff/);assert.throws(()=>f.DB.raw.prepare("UPDATE v2_ops_jobs SET status='completed' WHERE id='SOURCE'").run(),/system_cutoff/);
 assert.throws(()=>f.clock('LATE-CLOCK','SOURCE',iso(Date.now())),/system_cutoff/);assert.throws(()=>f.clock('STALE-CLOCK','NEW-UNKNOWN',f.old),/system_cutoff/);
});
test('failure rolls back snapshots and all execution changes, then an identical cutoff retries safely',async t=>{
 const f=await fixture(t);f.job('ROLLBACK');f.clock('ROLLBACK-CLOCK','ROLLBACK');f.DB.raw.exec("CREATE TRIGGER synthetic_fail_cutoff BEFORE UPDATE ON v2_ops_jobs WHEN NEW.status='system_closed' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END");
 const before=f.segments();await assert.rejects(f.run(),/synthetic failure/);assert.deepEqual(f.segments(),before);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').get().n,0);f.DB.raw.exec('DROP TRIGGER synthetic_fail_cutoff');await f.run();assert.equal(f.get('ROLLBACK').status,'system_closed');
});
test('production and unenabled environments never run the cutoff',async t=>{const f=await fixture(t);f.job('PROTECTED');f.clock('PROTECTED-CLOCK','PROTECTED');const before=f.segments();await runSystemCutoff({...f.env,SOP_ENVIRONMENT:'production'});await runSystemCutoff({...f.env,SOP_DAILY_CUTOFF_ENABLED:'false'});assert.deepEqual(f.segments(),before);});

test('authorized native and task resumes create fresh clocks and preserve the forced ones',async t=>{
 const f=await fixture(t);f.env.SOP_REQUEST_USER={id:'SYNTHETIC',name:'Synthetic manager',role:'manager',scope:'office'};
 f.job('NATIVE','working',f.old,'issue_handle');f.DB.raw.prepare("INSERT INTO v2_issue_handle_runs(id,job_id,issue_id,run_status) VALUES('SYNTHETIC-RUN','NATIVE','SYNTHETIC-ISSUE','working')").run();f.clock('OLD-NATIVE','NATIVE');f.job('TASK','working',f.old,'bulk_op','task');f.DB.raw.prepare("UPDATE sop_records SET state=json_set(state,'$.workers',json('[{\"id\":\"SYNTHETIC-B\",\"name\":\"Synthetic B\"}]'),'$.lead_id','SYNTHETIC-B') WHERE id='TASK'").run();f.clock('OLD-TASK','TASK');
 // Model the trigger already installed in an earlier staging release.
 f.DB.raw.exec("CREATE TRIGGER ck_native_pause_event BEFORE INSERT ON sop_events WHEN NEW.action='sop_native_resume' AND EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.record_id AND status!='paused') BEGIN SELECT RAISE(ABORT,'old_pause_guard'); END");
 await f.run();await ensureNativePauseGuards(f.env);const old=f.segments();
 const resumed=await nativePeople({job_id:'NATIVE',revision:2,client_req_id:crypto.randomUUID(),workers:[{id:'SYNTHETIC-A',name:'Synthetic A'}],lead_id:'SYNTHETIC-A'},f.env,{resume:true});assert.equal(resumed.ok,true);assert.equal(f.DB.raw.prepare("SELECT run_status FROM v2_issue_handle_runs WHERE id='SYNTHETIC-RUN'").get().run_status,'working');
 const task=await handleSop({action:'sop_task_start',id:'TASK',revision:2,client_req_id:crypto.randomUUID()},f.env);assert.equal(task.ok,true,JSON.stringify(task));
 for(const s of old)assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE id=?').get(s.id),s);assert.equal(f.segments().filter(s=>!s.left_at).length,2);
});

test('new post-boundary work committed during recovery is preserved; old concurrent writes cannot overwrite cutoff evidence',async t=>{
 const f=await fixture(t);f.job('OLD');f.clock('OLD-CLOCK','OLD');const batch=f.DB.batch.bind(f.DB);let injected=false;
 f.DB.batch=async sql=>{if(!injected&&sql.some(s=>s.sql.startsWith('INSERT OR IGNORE INTO ck_system_cutoff_audit'))){injected=true;f.job('RACING-NEW','working',f.fresh);f.clock('RACING-NEW-CLOCK','RACING-NEW',f.fresh);}return batch(sql);};await f.run();assert.equal(injected,true);assert.equal(f.get('RACING-NEW').status,'working');assert.equal(f.segments().find(s=>s.id==='RACING-NEW-CLOCK').left_at,'');
 assert.throws(()=>f.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=999,leave_reason='operation_finished' WHERE id='OLD-CLOCK'").run(iso(Date.now())),/audited_confirmation/);
});

test('only an audited manager confirmation can convert provisional labor to verified labor, idempotently',async t=>{
 const f=await fixture(t);f.job('VERIFY');f.clock('VERIFY-CLOCK','VERIFY');await f.run();const body={segment_id:'VERIFY-CLOCK',expected_end:iso(f.cut),end_at:iso(Date.parse(f.old)+1800000),reason:'Synthetic supervisor log confirms actual task end',client_req_id:crypto.randomUUID()};
 await assert.rejects(correctSystemCutoff(body,{...f.env,SOP_REQUEST_USER:{id:'S',role:'service',scope:'office'}}),/管理员/);
 f.env.SOP_REQUEST_USER={id:'SYNTHETIC-MANAGER',name:'Synthetic manager',role:'manager',scope:'office'};
 await assert.rejects(correctSystemCutoff({...body,end_at:iso(f.cut+1)},f.env),/原开始与系统截止/);
 const [a,b]=await Promise.all([correctSystemCutoff(body,f.env),correctSystemCutoff(body,f.env)]);assert.equal(a.ok,true);assert.equal(b.ok,true);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_corrections').get().n,1);
 const s=f.segments()[0];assert.equal(s.joined_at,f.old);assert.equal(s.left_at,body.end_at);assert.equal(s.minutes_worked,30);assert.equal(s.leave_reason,'system_cutoff_verified');assert.equal(f.get('VERIFY').status,'system_closed');
 const audit=f.DB.raw.prepare('SELECT * FROM ck_system_cutoff_corrections').get();assert.equal(JSON.parse(audit.before_json).minutes_worked,17.5);assert.equal(JSON.parse(audit.before_json).left_at,iso(f.cut));assert.equal(audit.actor_id,'SYNTHETIC-MANAGER');
 assert.equal(measureSegments([s],{start:f.cut-86400000,end:f.cut+86400000},Date.now())[0].minutes,30);
});
