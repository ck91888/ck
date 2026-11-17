// Synthetic identities and clocks only. No remote services or business data.
import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {handleAttendance,guardAttendance} from '../worker-v2/attendance.js';

async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_REQUEST_USER:{id:'SYNTHETIC-OWNER',role:'manager',name:'Synthetic dispatcher'}};
 const call=(suffix,data={})=>handleAttendance({action:'sop_attendance_'+suffix,client_req_id:crypto.randomUUID(),...data},env);
 const daily=(name='Synthetic worker')=>call('checkin',{name,agency:'동인천'}).then(r=>r.record);
 const employee=async()=>{const p=(await call('employee_register',{employeeNo:'SYNTHETIC-MAPPED',name:'Synthetic employee',department:'direct_ship'})).person;return (await call('checkin',{badge:p.badgeId})).record;};
 const summary=(scope='all')=>call('summary',{scope});
 const guard=r=>guardAttendance({action:'sop_native_start',payload:{action:'v2_ops_job_start',job_type:'change_order'},workers:[{id:r.badgeId,name:r.name}]},env);
 const job=(id='SYNTHETIC-JOB',status='working')=>DB.raw.prepare('INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,display_no) VALUES(?,?,?,?,?)').run(id,'change_order','direct_ship',status,'SYNTHETIC-ORDER-01');
 const clock=(r,{id='SYNTHETIC-CLOCK',jobId='SYNTHETIC-JOB',start=new Date().toISOString(),end='',badge=r.badgeId}={})=>{
  // Historical inconsistencies cannot be created through today's guarded API.
  // Insert a closed fixture and disable only the update trigger while reproducing it.
  DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run(id,jobId,badge,r.name,start,end||'fixture-closed',7.5);
  if(!end){const trigger=DB.raw.prepare("SELECT sql FROM sqlite_master WHERE name='ck_daily_badge_join_update'").get().sql;DB.raw.exec('DROP TRIGGER ck_daily_badge_join_update');try{DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at='' WHERE id=?").run(id);}finally{DB.raw.exec(trigger);}}
 };
 const snapshot=()=>Object.fromEntries(['v2_ops_jobs','v2_ops_job_workers','ck_attendance_days','ck_attendance_events','ck_attendance_breaks'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 return {DB,env,call,daily,employee,summary,guard,job,clock,snapshot};
}

test('idle daily and mapped employee cards agree with change-order admission; same-name people remain separate',async t=>{
 const f=await fixture(t),a=await f.daily(),b=(await f.call('checkin',{name:a.name,agency:'가온',confirm_distinct_person:true,reason:'Synthetic distinct people'})).record,e=await f.employee();
 f.job();f.clock(a);const before=f.snapshot(),report=await f.summary();
 assert.equal(report.items.find(x=>x.record.id===a.id).status,'working');
 for(const r of [b,e]){assert.equal(report.items.find(x=>x.record.id===r.id).status,'unassigned');assert.equal(await f.guard(r),null);}
 const error=await f.guard(a);for(const value of ['另一任务','SYNTHETIC-ORDER-01','KST',a.badgeId])assert.ok(error.includes(value),error);
 assert.deepEqual(f.snapshot(),before);
});

for(const [status,reason] of [['completed','历史已完成'],['paused','暂停任务'],['cancelled','已取消任务'],['missing','任务资料']])test(status+' open clock is visible and still blocks without inventing an end',async t=>{
 const f=await fixture(t),r=await f.daily();if(status!=='missing')f.job('SYNTHETIC-JOB',status);f.clock(r);
 const before=f.snapshot(),report=await f.summary(),item=report.items.find(x=>x.record.id===r.id);
 assert.equal(item.status,'occupancy_conflict');assert.equal(item.currentJobs.length,0);assert.equal(item.occupancyWarnings.length,1);assert.match(item.occupancyWarnings[0].reason,new RegExp(reason));
 assert.ok(item.occupancyWarnings[0].startedAtLabel.includes('KST'));assert.equal(item.occupancyWarnings[0].jobId,'SYNTHETIC-JOB');
 if(status==='missing')assert.equal(item.totals.other,0,'surfacing an orphan must not add labor');
 assert.match(await f.guard(r),/另一任务/);assert.deepEqual(f.snapshot(),before);
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sqlite_master WHERE name='ck_completed_clock_repairs'").get().n,0);
});

test('previous-shift, invalid and future clocks cannot be hidden as idle or added to current labor',async t=>{
 for(const [start,reason] of [['2000-01-01T00:00:00Z','上个班次'],['invalid-start','归属待核实'],['2099-01-01T00:00:00Z','归属待核实']]){
  const f=await fixture(t),r=await f.employee();f.job();f.clock(r,{start});const before=f.snapshot();
  const item=(await f.summary('employee')).items[0];assert.equal(item.status,'occupancy_conflict');assert.equal(item.totals.direct_ship,0);assert.equal(item.workday.own.minutes,0);
  assert.match(item.occupancyWarnings[0].reason,new RegExp(reason));assert.match(await f.guard(r),/另一任务/);assert.deepEqual(f.snapshot(),before);
 }
});

test('reliable completed end uses the existing audited repair and never changes unrelated work',async t=>{
 const f=await fixture(t),r=await f.daily();f.job('SYNTHETIC-JOB','completed');f.clock(r);const end=new Date().toISOString();
 f.DB.raw.prepare('UPDATE v2_ops_jobs SET finished_at=? WHERE id=?').run(end,'SYNTHETIC-JOB');
 assert.equal((await f.summary()).items[0].status,'occupancy_conflict');assert.equal(await f.guard(r),null);
 const clock=f.DB.raw.prepare('SELECT * FROM v2_ops_job_workers').get(),audit=f.DB.raw.prepare('SELECT * FROM ck_completed_clock_repairs').get();
 assert.equal(clock.left_at,end);assert.equal(audit.closed_at,end);assert.equal(audit.previous_left_at,'');assert.equal(audit.previous_minutes,7.5);
 assert.equal((await f.summary()).items[0].status,'unassigned');
});

test('closed paused clocks release occupancy, while personal rest and case-sensitive staff identity still guard admission',async t=>{
 const f=await fixture(t),r=await f.employee();f.job('SYNTHETIC-JOB','paused');f.clock(r,{end:new Date().toISOString()});
 assert.equal((await f.summary()).items[0].status,'unassigned');assert.equal(await f.guard(r),null);
 assert.match(await f.guard({...r,badgeId:r.badgeId.toLowerCase()}),/未找到当天有效签到/);
 await f.call('break_start',{id:r.id});assert.equal((await f.summary()).items[0].status,'rest');assert.match(await f.guard(r),/休息/);
});

test('existing verified-departure correction can close the original older shift with an audit while preserving today',async t=>{
 const f=await fixture(t),r=await f.employee(),day=new Date(Date.parse(r.day+'T00:00:00Z')-86400000).toISOString().slice(0,10),arrival=day+'T20:00:00+09:00',start=day+'T21:14:00+09:00',departure=day+'T22:10:00+09:00';
 f.DB.raw.prepare("INSERT INTO ck_attendance_days(id,person_id,worker_id,name,agency,day,identity_key,signed_in) VALUES('SYNTHETIC-PRIOR-DAY',?,?,?,?,?,?,?)").run(r.personId,r.badgeId,r.name,r.agency,day,'SYNTHETIC-PRIOR-IDENTITY',new Date(arrival).toISOString());
 f.job();f.clock(r,{start:new Date(start).toISOString()});const today=f.DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(r.id);
 assert.equal((await f.summary()).items.find(x=>x.record.id===r.id).status,'occupancy_conflict');assert.match(await f.guard(r),/另一任务/);
 await f.call('correct',{id:'SYNTHETIC-PRIOR-DAY',version:1,inAt:arrival,outAt:departure,reason:'Synthetic evidence confirms actual prior-shift departure'});
 const clock=f.DB.raw.prepare('SELECT * FROM v2_ops_job_workers').get(),event=f.DB.raw.prepare("SELECT * FROM ck_attendance_events WHERE record_id='SYNTHETIC-PRIOR-DAY'").get();
 assert.equal(clock.joined_at,new Date(start).toISOString());assert.equal(clock.left_at,new Date(departure).toISOString());assert.equal(clock.minutes_worked,56);assert.equal(clock.leave_reason,'attendance:verified_departure');
 assert.equal(JSON.parse(event.after_json).corrected_segments[0].minutes_worked,7.5);assert.equal(event.actor,'SYNTHETIC-OWNER');assert.deepEqual(f.DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(r.id),today);
 assert.equal(f.DB.raw.prepare('SELECT status FROM v2_ops_jobs').get().status,'working');assert.equal(await f.guard(r),null);
});
