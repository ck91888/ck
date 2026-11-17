import test from 'node:test';
import assert from 'node:assert/strict';
import {officeDutyFixture} from './office-duty-fixture.mjs';
import {latestCutoff} from '../worker-v2/system-cutoff.js';
const NativeDate=Date;
test('daily cutoff releases office duty, excludes unverified seconds and permits an explicit fresh resume',async t=>{
 const f=await officeDutyFixture(t),started=await f.change('start'),cut=Date.parse(latestCutoff(Date.now())),old=new Date(cut-3600000).toISOString(),guard=f.DB.raw.prepare("SELECT sql FROM sqlite_master WHERE name='ck_daily_badge_join_update'").get().sql;
 f.DB.raw.exec('DROP TRIGGER ck_daily_badge_join_update');try{f.DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(old,started.job.id);}finally{f.DB.raw.exec(guard);}
 f.DB.raw.prepare('UPDATE v2_ops_jobs SET created_at=? WHERE id=?').run(old,started.job.id);f.env.SOP_DAILY_CUTOFF_ENABLED='true';
 let ended=await f.get();assert.equal(ended.job.status,'system_closed');assert.equal(ended.job.seconds,0);assert.equal(ended.job.needs_review,true);const pending=f.clocks()[0];assert.equal(pending.left_at,new Date(cut).toISOString());
 const date=new Date(Date.parse(old)+9*3600000).toISOString().slice(0,10),performance=await f.ok('office','v2_dashboard_management_summary',{start_date:date,end_date:date});assert.equal(performance.summary.qty_per_hour,null);assert.equal(performance.summary.boxes_per_hour,null);assert.equal(performance.summary.efficiency_needs_review,true);
 const correction={segment_id:pending.id,expected_end:pending.left_at,end_at:new Date(Date.parse(old)+1800000).toISOString(),reason:'Synthetic verified supervisor log'};assert.equal((await f.call('A','sop_system_cutoff_correct',correction)).ok,false);await f.ok('office','sop_system_cutoff_correct',correction);ended=await f.get();assert.equal(ended.job.seconds,1800);const before=f.clocks()[0];
 const resumed=await f.change('resume',ended);assert.equal(resumed.job.status,'working');assert.equal(f.clocks().filter(s=>!s.left_at).length,1);assert.deepEqual(f.clocks().find(s=>s.id===before.id),before);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);
});
function clock(t,value='2026-10-22T08:00:00+09:00'){
 let n=NativeDate.parse(value);globalThis.Date=class extends NativeDate{constructor(...args){super(...(args.length?args:[n]));}static now(){return n;}};t.after(()=>{globalThis.Date=NativeDate;});return value=>{n=NativeDate.parse(value);};
}
test('registered identity, attendance and office context enforced; no fabricated staff or department',async t=>{
 clock(t);const f=await officeDutyFixture(t),body={badge:f.p.badgeId,worker_name:'伪造姓名',labor_department:'bulk'};
 assert.equal((await f.call('office','sop_office_duty_start',{badge:'EMP-UNKNOWN'})).ok,false);
 const unchecked=await f.employee('UNCHECKED','office',false);
 assert.equal((await f.call('office','sop_office_duty_start',{badge:unchecked.badgeId})).ok,false);
 assert.equal((await f.call('A','sop_office_duty_start',body)).ok,false);
 const started=await f.ok('office','sop_office_duty_start',body);assert.equal(started.person.name,f.p.name);assert.equal(started.job.department,'order_processing');
 const w=f.clocks()[0];assert.equal(w.worker_id,f.p.badgeId);assert.equal(w.worker_name,f.p.name);
 assert.equal((await f.call('office','sop_native_pause',{job_id:started.job.id,revision:1,reason:'绕过'})).ok,false);
});
test('start pause resume finish, seconds, duplicate clicks, lost-response replay, reload and stale revisions',async t=>{
 const at=clock(t),f=await officeDutyFixture(t),startId=crypto.randomUUID();
 let s=await f.change('start',null,{client_req_id:startId});const job=s.job.id;
 assert.equal((await f.change('start',null,{client_req_id:startId})).replayed,true);
 assert.equal((await f.call('office','sop_office_duty_start',{badge:f.p.badgeId})).ok,false);
 assert.equal((await f.get()).job.id,job);
 at('2026-10-22T09:00:00+09:00');const pauseId=crypto.randomUUID(),prior=s;
 s=await f.change('pause',s,{client_req_id:pauseId});assert.equal(s.job.seconds,3600);assert.equal(s.job.status,'paused');
 assert.equal((await f.change('pause',prior,{client_req_id:pauseId})).replayed,true);
 assert.equal((await f.call('office','sop_office_duty_finish',{badge:f.p.badgeId,job_id:job,revision:1})).ok,false);
 at('2026-10-22T10:00:00+09:00');s=await f.change('resume',await f.get());
 at('2026-10-22T10:30:00+09:00');s=await f.change('finish',s);assert.equal(s.job.seconds,5400);assert.equal(s.job.status,'completed');
 assert.equal(f.clocks().length,2);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,0);
 const report=await f.ok('office','v2_dashboard_order_export',{date_basis:'actual_work',start_date:'2026-10-22',end_date:'2026-10-22'}),row=report.rows.find(r=>r.job_id===job);
 assert.equal(row.total_seconds,5400);assert.equal(row.quantity_state,'time_only');assert.equal(row.hour_state,'recorded');assert.deepEqual(row.data_gaps,[]);assert.equal(row.comparable,false);assert.match(row.efficiency_reason,/仅记录工时/);assert.equal(report.summary.missing_quantity_tasks,0);assert.equal(report.quantities.length,0);assert.equal(report.by_department[0].department,'order_processing');
 const detail=await f.ok('office','sop_attendance_employee_work_detail',{date:'2026-10-22',person_id:f.p.id});assert.equal(detail.workday.officeDuty.minutes,90);assert.equal(detail.sections.own[0].quantity_state,'time_only');assert.equal(detail.sections.own[0].job_department,'order_processing');
});
test('competing requests commit once, rollback preserves old segments and unconfirmed requests retry',async t=>{
 clock(t);const f=await officeDutyFixture(t);
 const inputs=[0,1].map(()=>({badge:f.p.badgeId,client_req_id:crypto.randomUUID()}));
 const results=await Promise.all(inputs.map(b=>f.call('office','sop_office_duty_start',b)));assert.equal(results.filter(r=>r.ok).length,1);assert.equal(f.clocks().length,1);
 let s=await f.get();const before=f.clocks();f.DB.raw.exec("CREATE TRIGGER synthetic_failure BEFORE UPDATE OF left_at ON v2_ops_job_workers BEGIN SELECT RAISE(ABORT,'synthetic_storage_failure'); END");
 const request=crypto.randomUUID();assert.equal((await f.call('office','sop_office_duty_pause',{badge:f.p.badgeId,job_id:s.job.id,revision:s.job.revision,client_req_id:request})).ok,false);assert.deepEqual(f.clocks(),before);assert.equal((await f.get()).job.revision,s.job.revision);
 f.DB.raw.exec('DROP TRIGGER synthetic_failure');s=await f.change('pause',s,{client_req_id:request});assert.equal(s.job.status,'paused');
 const copies=await Promise.all([0,1].map(()=>f.call('office','sop_office_duty_resume',{badge:f.p.badgeId,job_id:s.job.id,revision:s.job.revision})));assert.equal(copies.filter(r=>r.ok).length,1);assert.equal(f.clocks().filter(w=>!w.left_at).length,1);
});
test('office and field exclusion in both directions; existing unload borrowing returns one clock',async t=>{
 const at=clock(t),f=await officeDutyFixture(t);let s=await f.change('start');
 const workers=[{id:f.p.badgeId,name:f.p.name}],payload={action:'v2_unplanned_unload_start',client_req_id:crypto.randomUUID(),biz_class:'bulk',cargo_summary:'合成临时卸货'};
 const request={payload,workers,lead_id:f.p.badgeId,estimated_minutes:10,labor_department:'bulk'};
 assert.equal((await f.call('office','sop_native_start',request)).ok,false);
 const a=await f.ok('office','sop_crew_availability',{worker_id:f.p.badgeId,payload});assert.equal(a.can_borrow,true);
 at('2026-10-22T08:15:00+09:00');const field=await f.ok('office','sop_native_start',{...request,borrow_confirmations:[{worker_id:f.p.badgeId,source_job_id:a.source.job_id,source_segment_id:a.source.segment_id,source_revision:a.source.revision}]});
 await f.ok('office','v2_admin_cleanup_job_statuses',{dry_run:false});
 s=await f.get();assert.equal(s.job.borrowed,true);assert.equal(s.job.status,'awaiting_close');assert.equal(s.job.seconds,900);assert.equal((await f.call('office','sop_office_duty_resume',{badge:f.p.badgeId,job_id:s.job.id,revision:s.job.revision})).ok,false);
 assert.equal((await f.call('office','sop_office_duty_finish',{badge:f.p.badgeId,job_id:s.job.id,revision:s.job.revision})).ok,false);
 at('2026-10-22T08:30:00+09:00');await f.ok('office','v2_unplanned_unload_finish',{job_id:field.job_id,worker_id:f.p.badgeId,complete_job:true,result_lines:[{unit_type:'carton',actual_qty:1}]});
 s=await f.get();assert.equal(s.job.borrowed,false);assert.equal(s.job.status,'working');assert.equal(s.job.segments.length,2);assert.equal(f.clocks().filter(w=>!w.left_at).length,1);
 at('2026-10-22T09:00:00+09:00');s=await f.change('finish',s);assert.equal(s.job.seconds,2700);
 // A paused office duty may transfer to field work; resume stays blocked until field finishes.
 s=await f.change('start');s=await f.change('pause',s);const next=await f.ok('office','sop_native_start',{...request,payload:{...payload,client_req_id:crypto.randomUUID()}});assert.equal((await f.call('office','sop_office_duty_resume',{badge:f.p.badgeId,job_id:s.job.id,revision:s.job.revision})).ok,false);assert.ok(next.job_id);
});
test('KST midnight clips actual-date exports; finishing old duty is allowed and new starts require today attendance',async t=>{
 const at=clock(t,'2026-10-22T23:30:00+09:00'),f=await officeDutyFixture(t);let s=await f.change('start');
 at('2026-10-23T00:30:00+09:00');s=await f.change('finish',s);assert.equal(s.job.seconds,3600);
 for(const date of ['2026-10-22','2026-10-23']){const r=await f.ok('office','v2_dashboard_order_export',{date_basis:'actual_work',start_date:date,end_date:date,department:'order_processing'});assert.equal(r.summary.total_seconds,1800);assert.equal(r.rows[0].quantity_state,'time_only');const segments=await f.ok('office','v2_dashboard_workhour_summary',{date_basis:'actual_work',start_date:date,end_date:date,report_kind:'segments'});assert.equal(segments.segments[0].work_date,date);}
 assert.equal((await f.call('office','sop_office_duty_start',{badge:f.p.badgeId})).ok,false);
});

test('feature stays staging-only; historical jobs and results are preserved',async t=>{
 clock(t);const f=await officeDutyFixture(t);f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,created_at,shared_result_json) VALUES('SYNTH-HISTORY','old_type','completed','2024-01-01T00:00:00Z','{\"legacy\":true}')").run();const before=f.DB.raw.prepare("SELECT * FROM v2_ops_jobs WHERE id='SYNTH-HISTORY'").get();
 let s=await f.change('start');s=await f.change('pause',s);await f.ok('office','v2_admin_cleanup_job_statuses',{dry_run:false});assert.equal((await f.get()).job.status,'paused');await f.change('finish',s);assert.deepEqual(f.DB.raw.prepare("SELECT * FROM v2_ops_jobs WHERE id='SYNTH-HISTORY'").get(),before);
 f.env.SOP_ENVIRONMENT='production';const n=f.clocks().length;assert.equal((await f.call('office','sop_office_duty_start',{badge:f.p.badgeId})).ok,false);assert.equal(f.clocks().length,n);
});
