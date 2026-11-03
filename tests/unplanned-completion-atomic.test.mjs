// Authenticated synthetic requests against SQLite and, optionally, native workerd D1.
import test from 'node:test';
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';

async function fixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
 const f=await ownedHomeFixture();let native;
 f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';
 if(process.env.CK_P1_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 t.after(async()=>{await native?.close();f.DB.raw.close();});
 f.first=(sql,...args)=>f.env.DB.prepare(sql).bind(...args).first();
 f.rows=async(sql,...args)=>(await f.env.DB.prepare(sql).bind(...args).all()).results;
 return f;
}
async function scenario(f,kind='feedback'){
 const w=await f.person(),start=await f.ok('A','sop_native_start',{payload:{action:'v2_unplanned_unload_start',biz_class:'bulk',cargo_summary:'合成临时卸货',client_req_id:crypto.randomUUID()},workers:[{id:w.badgeId,name:w.name}],lead_id:w.badgeId,estimated_minutes:15,labor_department:'bulk'});
 const id=start.job_id,body={job_id:id,worker_id:w.badgeId,complete_job:true,result_lines:[{unit_type:'carton',actual_qty:3}],client_req_id:'SYN-UNPLANNED-'+id};
 // Synthetic historical shapes, retaining the real SOP owner and crew. No
 // deprecated start route is reopened or made available to current clients.
 let planId='';
 if(kind==='dynamic'){
  planId='SYN-DYNAMIC-'+id;
  await f.env.DB.prepare("INSERT INTO v2_inbound_plans(id,plan_date,source_type,status,updated_at) VALUES(?,'2026-11-03','field_dynamic','unloading',?)").bind(planId,new Date().toISOString()).run();
 }
 if(kind!=='feedback')await f.env.DB.prepare('UPDATE v2_ops_jobs SET related_doc_type=?,related_doc_id=? WHERE id=?').bind(planId?'inbound_plan':'',planId,id).run();
 const action=kind==='feedback'?'v2_unplanned_unload_finish':'v2_unload_job_finish';
 const snapshot=async()=>({job:await f.first('SELECT * FROM v2_ops_jobs WHERE id=?',id),workers:await f.rows('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id',id),results:await f.rows('SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY id',id),feedbacks:await f.rows("SELECT * FROM v2_field_feedbacks WHERE id=? OR (related_doc_type='ops_job' AND related_doc_id=?) ORDER BY id",start.feedback_id||'',id),plan:await f.first('SELECT * FROM v2_inbound_plans WHERE id=?',planId),lines:await f.rows('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY id',planId)});
 return {id,body,snapshot,action,planId};
}
async function holdFirstWrite(f,id){
 const db=f.env.DB,segments=await f.rows('SELECT id FROM v2_ops_job_workers WHERE job_id=?',id);let reached,release,held=false;
 const ready=new Promise(r=>reached=r),gate=new Promise(r=>release=r),ids=[id,...segments.map(s=>s.id)];
 f.env.DB={prepare:db.prepare.bind(db),async batch(ss){
  if(!held&&ss.some(s=>/UPDATE v2_ops_job_workers SET left_at|INSERT INTO v2_ops_job_results/.test(s.sql)&&s.args.some(a=>ids.includes(a)))){held=true;reached();await gate;}
  return db.batch(ss);
 }};
 return {ready,release,restore(){f.env.DB=db;}};
}
for(const kind of ['feedback','no-document','dynamic'])test(kind+' unload rejects an old owner before its first clock/result write after transfer',async t=>{
 const f=await fixture(t),s=await scenario(f,kind);t.mock.timers.tick(3*60000);
 const gate=await holdFirstWrite(f,s.id),pending=f.call('A',s.action,s.body);await gate.ready;let before;
 try{
  const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id);
  await f.ok('office','sop_owner_transfer',{id:s.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成临时卸货在途交接'});
  assert.equal((await f.call('A',s.action,s.body)).ok,false);
  before=await s.snapshot();
 }finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.deepEqual(await s.snapshot(),before);
 await f.ok('B',s.action,s.body);
 assert.equal((await f.first('SELECT status FROM v2_ops_jobs WHERE id=?',s.id)).status,'completed');
 assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,1);
 assert.equal((await f.first('SELECT SUM(minutes_worked) n FROM v2_ops_job_workers WHERE job_id=?',s.id)).n,3);
});
for(const kind of ['feedback','no-document','dynamic'])test(kind+' unload rolls back clocks and results if its last document write fails',async t=>{
 const f=await fixture(t),s=await scenario(f,kind);t.mock.timers.tick(3*60000);const before=await s.snapshot();
 const ddl=kind==='dynamic'?"CREATE TRIGGER synthetic_last_failure BEFORE UPDATE ON v2_inbound_plans WHEN NEW.status='unloaded_pending_info' BEGIN SELECT RAISE(ABORT,'synthetic last failure'); END":kind==='feedback'?"CREATE TRIGGER synthetic_last_failure BEFORE UPDATE ON v2_field_feedbacks WHEN NEW.status='unloaded_pending_info' BEGIN SELECT RAISE(ABORT,'synthetic last failure'); END":"CREATE TRIGGER synthetic_last_failure BEFORE INSERT ON v2_field_feedbacks WHEN NEW.feedback_type='unload_no_doc' BEGIN SELECT RAISE(ABORT,'synthetic last failure'); END";
 await f.env.DB.prepare(ddl).run();assert.equal((await f.call('A',s.action,s.body)).ok,false);assert.deepEqual(await s.snapshot(),before);
 await f.env.DB.prepare('DROP TRIGGER synthetic_last_failure').run();await f.ok('A',s.action,s.body);
 assert.equal((await f.first('SELECT SUM(minutes_worked) n FROM v2_ops_job_workers WHERE job_id=?',s.id)).n,3);
});
for(const kind of ['feedback','no-document','dynamic'])test(kind+' unload returns the same receipt to a held identical retry, rejecting changed content',async t=>{
 const f=await fixture(t),s=await scenario(f,kind),gate=await holdFirstWrite(f,s.id),pending=f.call('A',s.action,s.body);await gate.ready;let saved;
 try{saved=await f.ok('A',s.action,s.body);}finally{gate.release();}
 const reply=await pending;gate.restore();assert.deepEqual(reply,saved);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,1);
 const before=await s.snapshot();const conflict=await f.call('A',s.action,{...s.body,result_lines:[{unit_type:'carton',actual_qty:4}]});assert.equal(conflict.ok,false);assert.equal(conflict.error,'request_id_conflict');assert.deepEqual(await s.snapshot(),before);
});
test('unplanned manager can complete a three-person crew, with exact per-person time and preserved evidence',async t=>{
 const f=await fixture(t),s=await scenario(f),more=await Promise.all([f.person(),f.person()]);
 const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id);
 await f.ok('A','sop_native_people',{job_id:s.id,revision:row.revision,workers:[{id:s.body.worker_id,name:(await f.first('SELECT worker_name FROM v2_ops_job_workers WHERE job_id=?',s.id)).worker_name},...more.map(p=>({id:p.badgeId,name:p.name}))],lead_id:s.body.worker_id,reason:'合成增加人员'});
 t.mock.timers.tick(3*60000);await f.ok('office',s.action,s.body);
 assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=?',s.id)).n,3);
 assert.equal((await f.first("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",s.id)).n,0);
 assert.equal((await f.first('SELECT SUM(minutes_worked) n FROM v2_ops_job_workers WHERE job_id=?',s.id)).n,9);
});
test('unplanned completion cannot survive transfer away and back to its original owner',async t=>{
 const f=await fixture(t),s=await scenario(f),gate=await holdFirstWrite(f,s.id),pending=f.call('A',s.action,s.body);await gate.ready;
 try{for(const [from,to] of [['A','B'],['B','A']]){const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id);await f.ok(from,'sop_owner_transfer',{id:s.id,revision:row.revision,target_badge:f.people[to].badgeId,reason:'合成往返交接'});}}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,0);await f.ok('A',s.action,s.body);
});
test('unplanned empty output does not release anyone, and individual leave does not complete the task',async t=>{
 const f=await fixture(t),s=await scenario(f),before=await s.snapshot();
 for(const lines of [[],[{unit_type:'carton',actual_qty:-1}],[{unit_type:'carton',actual_qty:'invalid'}]]){assert.equal((await f.call('A',s.action,{...s.body,result_lines:lines})).ok,false);assert.deepEqual(await s.snapshot(),before);}
 await f.ok('A',s.action,{...s.body,client_req_id:crypto.randomUUID(),leave_only:true,result_lines:[]});
 assert.equal((await f.first('SELECT status FROM v2_ops_jobs WHERE id=?',s.id)).status,'working');assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,0);
 await f.ok('A',s.action,s.body);
});
test('an assigned dispatcher member keeps independent unplanned completion authority',async t=>{
 const f=await fixture(t),s=await scenario(f),row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id);
 await f.ok('A','sop_native_people',{job_id:s.id,revision:row.revision,workers:[{id:f.people.B.badgeId,name:f.people.B.name}],lead_id:f.people.B.badgeId,reason:'合成有权派审员亲自作业'});
 await f.ok('B',s.action,{...s.body,worker_id:f.people.B.badgeId});
 assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,1);
 assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',s.id)).state).owner_id,f.people.A.id);
});
test('unplanned manager completion cannot use a revoked office session after its first read',async t=>{
 const f=await fixture(t),s=await scenario(f),gate=await holdFirstWrite(f,s.id),pending=f.call('office',s.action,s.body);await gate.ready;const before=await s.snapshot();
 try{await f.ok('office','sop_logout');}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false);assert.deepEqual(await s.snapshot(),before);await f.ok('A',s.action,s.body);
});
test('historical unmanaged no-document unload releases only the leaving worker while others continue',async t=>{
 const f=await fixture(t),s=await scenario(f,'no-document'),other=await f.person(),row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id);
 const first=await f.first('SELECT worker_name FROM v2_ops_job_workers WHERE job_id=?',s.id);
 await f.ok('A','sop_native_people',{job_id:s.id,revision:row.revision,workers:[{id:s.body.worker_id,name:first.worker_name},{id:other.badgeId,name:other.name}],lead_id:s.body.worker_id,reason:'合成旧单两人'});
 await f.env.DB.prepare('DELETE FROM sop_records WHERE id=?').bind(s.id).run();t.mock.timers.tick(3*60000);
 const reply=await f.call('office',s.action,s.body);assert.equal(reply.ok,false);assert.equal(reply.error,'others_still_working');
 assert.equal((await f.first("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",s.id)).n,1);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,0);
 await f.ok('office','v2_ops_job_resume',{parent_job_id:s.id,worker_id:s.body.worker_id,worker_name:first.worker_name});
 const rejoined=await s.snapshot();assert.deepEqual(await f.call('office',s.action,s.body),reply);assert.deepEqual(await s.snapshot(),rejoined);
 await f.ok('office',s.action,{...s.body,client_req_id:crypto.randomUUID(),leave_only:true,result_lines:[]});
 await f.ok('office',s.action,{...s.body,client_req_id:crypto.randomUUID(),worker_id:other.badgeId});
 assert.equal((await f.first('SELECT SUM(minutes_worked) n FROM v2_ops_job_workers WHERE job_id=?',s.id)).n,6);assert.equal((await f.first("SELECT COUNT(*) n FROM v2_field_feedbacks WHERE related_doc_id=? AND feedback_type='unload_no_doc'",s.id)).n,1);
});
test('historical dynamic partial worker finish keeps its old individual release semantics',async t=>{
 const f=await fixture(t),s=await scenario(f,'dynamic');t.mock.timers.tick(3*60000);
 await f.ok('A',s.action,{...s.body,complete_job:false,result_lines:[],client_req_id:crypto.randomUUID()});
 assert.equal((await f.first("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",s.id)).n,0);assert.equal((await f.first('SELECT status FROM v2_ops_jobs WHERE id=?',s.id)).status,'working');assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,0);
 await f.ok('A',s.action,s.body);
});
for(const kind of ['feedback','dynamic'])test(kind+' exit retry does not close a later rejoined segment',async t=>{
 const f=await fixture(t),s=await scenario(f,kind),request={...s.body,...(kind==='feedback'?{leave_only:true}:{complete_job:false}),result_lines:[],client_req_id:'SYN-EXIT-'+s.id};
 const saved=await f.ok('A',s.action,request),row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id);
 const w=await f.first('SELECT worker_id,worker_name FROM v2_ops_job_workers WHERE job_id=?',s.id);t.mock.timers.tick(60000);
 await f.ok('A','sop_native_people',{job_id:s.id,revision:row.revision,workers:[{id:w.worker_id,name:w.worker_name}],lead_id:w.worker_id,reason:'合成退出后重新加入'});
 const before=await s.snapshot();assert.deepEqual(await f.ok('A',s.action,request),saved);assert.deepEqual(await s.snapshot(),before);
});
for(const kind of ['feedback','no-document','dynamic'])test(kind+' in-flight old exit cannot close a replacement work segment',async t=>{
 const f=await fixture(t),s=await scenario(f,kind),request={...s.body,leave_only:true,result_lines:[],client_req_id:'SYN-OLD-EXIT-'+s.id};
 t.mock.timers.tick(3*60000);const gate=await holdFirstWrite(f,s.id),pending=f.call('A',s.action,request);await gate.ready;let before;
 try{
  await f.ok('A',s.action,{...request,client_req_id:'SYN-NEW-EXIT-'+s.id});
  t.mock.timers.tick(60000);const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id),w=await f.first('SELECT worker_id,worker_name FROM v2_ops_job_workers WHERE job_id=?',s.id);
  await f.ok('A','sop_native_people',{job_id:s.id,revision:row.revision,workers:[{id:w.worker_id,name:w.worker_name}],lead_id:w.worker_id,reason:'合成另一请求退出后重新加入'});
  before=await s.snapshot();assert.equal(before.job.active_worker_count,1);
 }finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.deepEqual(await s.snapshot(),before);
 const rows=await f.rows('SELECT joined_at,left_at FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at',s.id);assert.equal(rows.filter(r=>r.left_at==='').length,1);assert.ok(rows.every(r=>!r.left_at||Date.parse(r.left_at)>=Date.parse(r.joined_at)));
});
