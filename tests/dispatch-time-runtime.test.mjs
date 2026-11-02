// Real authenticated Worker API, server-side clock, synthetic SQLite only.
import test from 'node:test';import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {dispatchTimeAttendance} from '../worker-v2/dispatch-time-runtime.js';
async function setup(t){
 const base=Date.now()-3600000;t.mock.timers.enable({apis:['Date'],now:base});
 const f=await ownedHomeFixture();t.after(()=>f.DB.raw.close());f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';f.env.SOP_DISPATCH_SHARE_ENABLED='true';
 const advance=m=>t.mock.timers.tick(m*60000),personId=k=>f.people[k].id,att=k=>f.people[k].record.id;
 const state=k=>f.DB.raw.prepare('SELECT * FROM ck_dispatch_time_days WHERE person_id=? ORDER BY activated_at DESC LIMIT 1').get(personId(k));
 const spans=k=>f.DB.raw.prepare('SELECT * FROM ck_dispatch_time_spans WHERE person_id=? ORDER BY started_at,rowid').all(personId(k));
 const seconds=k=>spans(k).filter(s=>!s.needs_review).reduce((n,s)=>n+(Date.parse(s.ended_at||new Date().toISOString())-Date.parse(s.started_at))/1000,0);
 const finish=async(k,id)=>{const task=f.record(id);return f.ok(k,'sop_task_complete_review',{id,revision:task.revision,result:{quantity:2,unit:'箱',label_count:2},decision:'pass',reason:'合成核对'});};
 const share=async()=>{const p=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});await f.ok('B','sop_dispatch_share_accept',{pair_id:p.pair.id});return p.pair.id;};
 return {...f,base,advance,personId,att,state,spans,seconds,finish,share};
}
test('successful first dispatch starts exactly once, multiple orders do not add time, and last completion pauses',async t=>{
 const f=await setup(t);const first=await f.task('A','合成首单');assert.equal(f.state('A').status,'dispatching');f.advance(10);const second=await f.task('A','合成二单');assert.equal(f.spans('A').length,1);f.advance(10);await f.finish('A',first.id);assert.equal(f.state('A').status,'dispatching');f.advance(10);await f.finish('A',second.id);assert.equal(f.state('A').status,'waiting_dispatch');assert.equal(f.seconds('A'),1800);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,2);
});
test('failed dispatch rolls all clock evidence back, and detail, list and successful draft recovery reads do not activate a person',async t=>{
 const f=await setup(t),n=await f.need('合成失败派工'),w=await f.person();await f.ok('A','sop_field_resolve',{code:n.id});assert.equal(f.state('A'),undefined);
 f.DB.raw.exec("CREATE TRIGGER synthetic_dispatch_failure BEFORE INSERT ON v2_ops_job_workers BEGIN SELECT RAISE(ABORT,'synthetic_failure'); END");const failed=await f.call('A','sop_task_dispatch',{need_id:n.id,title:'合成失败派工',department:'bulk',job_type:'bulk_op',workers:[{id:w.badgeId,name:w.name}],lead_id:w.badgeId,estimated_minutes:30});assert.equal(failed.ok,false);assert.equal(f.state('A'),undefined);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_dispatch_time_spans').get().n,0);
});
test('joining oneself pauses dispatch in the same transaction; leaving resumes while another order still needs management',async t=>{
 const f=await setup(t),job=await f.task('A','合成下场操作');f.advance(10);const s=f.record(job.id);await f.ok('A','sop_task_people',{id:job.id,revision:s.revision,workers:[...s.workers,{id:f.people.A.badgeId,name:f.people.A.name}],lead_id:s.lead_id,reason:'合成本人操作'});assert.equal(f.state('A').status,'operating');f.advance(20);assert.equal(f.seconds('A'),600);const r=f.record(job.id);await f.ok('A','sop_task_people',{id:job.id,revision:r.revision,workers:r.workers.filter(w=>w.id!==f.people.A.badgeId),lead_id:r.lead_id,reason:'合成退出操作'});assert.equal(f.state('A').status,'dispatching');f.advance(10);await f.finish('A',job.id);assert.equal(f.seconds('A'),1200);
});
test('rest and signout are authoritative; a closed page is never a timer event',async t=>{
 const f=await setup(t);await f.task('A','合成休息单');f.advance(10);await f.ok('office','sop_attendance_break_start',{id:f.att('A')});assert.equal(f.state('A').status,'rest');f.advance(15);assert.equal(f.seconds('A'),600);await f.ok('office','sop_attendance_break_end',{id:f.att('A')});assert.equal(f.state('A').status,'dispatching');f.advance(10);await f.ok('office','sop_attendance_checkout',{id:f.att('A')});assert.equal(f.state('A').status,'out');f.advance(30);assert.equal(f.seconds('A'),1200);
});
test('shared membership and detail reads do not start; first successful audited adjustment does, and leave ends only shared responsibility',async t=>{
 const f=await setup(t),a=await f.task('A','合成共享处理'),pair=await f.share();assert.equal(f.state('B'),undefined);await f.ok('B','sop_field_resolve',{code:a.id});assert.equal(f.state('B'),undefined);f.advance(5);const s=f.record(a.id);await f.ok('B','sop_task_people',{id:a.id,revision:s.revision,workers:s.workers,lead_id:s.lead_id,reason:'合成共享核对人员'});assert.equal(f.state('B').status,'dispatching');f.advance(10);await f.ok('B','sop_dispatch_share_leave',{pair_id:pair});assert.equal(f.state('B').status,'waiting_dispatch');assert.equal(f.seconds('B'),600);assert.equal(f.state('A').status,'dispatching');
});
test('shared leave does not stop a person who still owns a separately active order',async t=>{
 const f=await setup(t),a=await f.task('A','合成共享甲'),own=await f.task('B','合成自身单'),pair=await f.share(),s=f.record(a.id);await f.ok('B','sop_task_people',{id:a.id,revision:s.revision,workers:s.workers,lead_id:s.lead_id,reason:'合成共享处理'});f.advance(10);await f.ok('B','sop_dispatch_share_leave',{pair_id:pair});assert.equal(f.state('B').status,'dispatching');f.advance(10);await f.finish('B',own.id);assert.equal(f.seconds('B'),1200);assert.equal(f.state('B').status,'waiting_dispatch');
});
test('explicit transfer ends the former owner and starts the recipient at the successful server action',async t=>{
 const f=await setup(t),job=await f.task('A','合成转单');f.advance(12);await f.ok('A','sop_owner_transfer',{id:job.id,revision:f.record(job.id).revision,target_badge:f.people.B.badgeId,reason:'合成真实交接'});assert.equal(f.state('A').status,'waiting_dispatch');assert.equal(f.state('B').status,'dispatching');assert.equal(f.spans('B')[0].started_at,new Date().toISOString());f.advance(8);await f.finish('B',job.id);assert.equal(f.seconds('A'),720);assert.equal(f.seconds('B'),480);
});
test('an external order has the same automatic timer and formal output still records once',async t=>{
 const f=await setup(t),w=await f.person();const job=await f.ok('A','sop_native_start',{payload:{action:'v2_bulk_op_job_start',work_order_no:'SYNTH-EXTERNAL-DUTY',customer:'合成客户',client_req_id:crypto.randomUUID()},workers:[{id:w.badgeId,name:w.name}],lead_id:w.badgeId,estimated_minutes:30,labor_department:'bulk'});assert.equal(f.state('A').status,'dispatching');f.advance(10);await f.ok('A','v2_bulk_op_job_finish',{job_id:job.job_id,worker_id:w.badgeId,customer:'合成客户',complete_job:true,packed_box_count:2,total_operated_box_count:2,result_note:'合成核对'});assert.equal(f.state('A').status,'waiting_dispatch');assert.equal(f.seconds('A'),600);
});
