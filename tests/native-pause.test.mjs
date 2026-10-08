import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+r.error);return r;};
 const start=(staff,action='v2_ops_job_start',extra={job_type:'pack_direct',flow_stage:'outbound'})=>ok('sop_native_start',{payload:{action,client_req_id:crypto.randomUUID(),...extra},workers:staff,lead_id:staff[0].id,estimated_minutes:15,labor_department:'bulk'});
 const active=id=>DB.raw.prepare("SELECT * FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").all(id);
 const status=id=>DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(id).status;
 const person=async name=>{const d=(await ok('sop_attendance_checkin',{name,agency:'가온'})).record;return {d,w:{id:d.badgeId,name:d.name}};};
 return {DB,env,call,ok,start,active,status,person};
}
test('explicit pause keeps assignment, closes only this task, resumes once and preserves effective time',async()=>{
 const f=setup(),a=await f.person('QA pause A'),b=await f.person('QA pause B'),other=await f.person('QA other');
 const j=await f.start([a.w,b.w]),k=await f.start([other.w]);
 f.DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(new Date(Date.now()-30*60000).toISOString(),j.job_id);
 const body={job_id:j.job_id,revision:1,reason:'午休等待 / 대기',client_req_id:'QA-PAUSE-ONCE'};
 const r=await f.ok('sop_native_pause',body);await f.ok('sop_native_pause',body);assert.equal(r.revision,2);assert.equal(f.status(j.job_id),'paused');assert.equal(f.active(j.job_id).length,0);assert.equal(f.active(k.job_id).length,1);
 const snapshot=JSON.parse(f.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(j.job_id).state);assert.equal(snapshot.workers.length,2);
 assert.equal(f.DB.raw.prepare('SELECT SUM(minutes_worked) n FROM v2_ops_job_workers WHERE job_id=?').get(j.job_id).n,60);
 assert.equal((await f.call('sop_native_people',{job_id:j.job_id,revision:2,workers:[a.w],lead_id:a.w.id})).ok,false);
 assert.equal((await f.call('v2_ops_job_finish',{job_id:j.job_id,worker_id:a.w.id})).ok,false);
 const resume={job_id:j.job_id,revision:2,workers:[a.w,b.w],lead_id:a.w.id,client_req_id:'QA-RESUME-ONCE'};
 await f.ok('sop_native_resume',resume);await f.ok('sop_native_resume',resume);assert.equal(f.active(j.job_id).length,2);assert.equal(f.status(j.job_id),'working');
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=?').get(j.job_id).n,4);
 await f.ok('v2_ops_job_finish',{job_id:j.job_id,worker_id:a.w.id});assert.equal((await f.call('sop_native_resume',{...resume,client_req_id:crypto.randomUUID(),revision:3})).ok,false);
});
test('ending personal rest cannot bypass whole-task pause; resume cannot include resting or signed-out people',async()=>{
 const f=setup(),a=await f.person('QA rest A'),b=await f.person('QA rest B'),j=await f.start([a.w,b.w]);
 await f.ok('sop_attendance_break_start',{id:a.d.id});assert.equal(f.active(j.job_id).length,1);
 await f.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'暂停整单'});
 assert.equal((await f.call('sop_native_resume',{job_id:j.job_id,revision:2,workers:[a.w,b.w],lead_id:b.w.id})).ok,false);
 const end=await f.ok('sop_attendance_break_end',{id:a.d.id});assert.deepEqual(end.resumedJobs,[]);assert.equal(f.active(j.job_id).length,0);assert.equal(f.status(j.job_id),'paused');
 await f.ok('sop_attendance_checkout',{id:b.d.id});assert.equal((await f.call('sop_native_resume',{job_id:j.job_id,revision:2,workers:[b.w],lead_id:b.w.id})).ok,false);
 await f.ok('sop_native_resume',{job_id:j.job_id,revision:2,workers:[a.w],lead_id:a.w.id});assert.equal(f.active(j.job_id).length,1);
});
test('rest end cannot rejoin removed staff, completed work, or duplicate on replay; pick documents mirror segments',async()=>{
 const f=setup(),a=await f.person('QA picker A'),b=await f.person('QA picker B'),j=await f.start([a.w,b.w],'v2_pick_job_start',{pick_doc_nos:['QA-P1','QA-P2']});
 const pwd=id=>f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_pick_worker_docs WHERE worker_id=? AND status='working'").get(id).n;
 await f.ok('sop_attendance_break_start',{id:a.d.id});assert.equal(pwd(a.w.id),0);assert.equal(pwd(b.w.id),2);
 const end={id:a.d.id,client_req_id:'QA-END-PICKER'};await f.ok('sop_attendance_break_end',end);await f.ok('sop_attendance_break_end',end);assert.equal(pwd(a.w.id),2);assert.equal(f.active(j.job_id).length,2);
 await f.ok('sop_attendance_break_start',{id:a.d.id});await f.ok('sop_native_people',{job_id:j.job_id,revision:1,workers:[b.w],lead_id:b.w.id});
 assert.deepEqual((await f.ok('sop_attendance_break_end',{id:a.d.id})).resumedJobs,[]);assert.equal(f.active(j.job_id).length,1);assert.equal(pwd(a.w.id),0);
 await f.ok('sop_attendance_break_start',{id:b.d.id});await f.ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:b.w.id});assert.deepEqual((await f.ok('sop_attendance_break_end',{id:b.d.id})).resumedJobs,[]);
});
test('pause and resume transaction guards reject state races without partially rewriting clocks or records',async()=>{
 for(const mode of ['pause','resume']){
  const f=setup(),a=await f.person('QA race '+mode),j=await f.start([a.w]);let rev=1;
  if(mode==='resume'){await f.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'test'});rev=2;}
  const batch=f.DB.batch;let once=true;
  f.DB.batch=async ss=>{if(once&&ss.some(s=>s.args?.includes('sop_native_'+mode))){once=false;f.DB.raw.prepare("UPDATE v2_ops_jobs SET status='completed' WHERE id=?").run(j.job_id);}return batch(ss);};
  const r=await f.call('sop_native_'+mode,{job_id:j.job_id,revision:rev,reason:'test',workers:[a.w],lead_id:a.w.id});assert.equal(r.ok,false);assert.equal(f.status(j.job_id),'completed');assert.equal(f.DB.raw.prepare('SELECT revision FROM sop_records WHERE id=?').get(j.job_id).revision,rev);
 }
});
test('rest return detects concurrent removal inside its transaction and leaves rest open for safe retry',async()=>{
 const f=setup(),a=await f.person('QA rest race'),j=await f.start([a.w]);await f.ok('sop_attendance_break_start',{id:a.d.id});
 const batch=f.DB.batch;let once=true;f.DB.batch=async ss=>{if(once&&ss.some(s=>s.args?.includes('sop_attendance_break_end'))){once=false;f.DB.raw.prepare("UPDATE sop_records SET state=json_set(state,'$.workers',json('[]')) WHERE id=?").run(j.job_id);}return batch(ss);};
 assert.equal((await f.call('sop_attendance_break_end',{id:a.d.id})).ok,false);assert.equal(f.active(j.job_id).length,0);assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_breaks WHERE ended_at=''").get().n,1);
});
