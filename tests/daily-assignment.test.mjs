import {pickStartFixture,historicalPickStart} from './pick-workload-fixture.mjs';
// Real Worker API, a controlled clock, and SQLite :memory: only.
import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';

const NativeDate=Date,day='2026-10-12';
const instant=(time,date=day)=>new NativeDate(`${date}T${time}:00+09:00`).toISOString();

async function setup(t,{permanent=false,arrival=instant('09:00')}={}){
  let clock=NativeDate.parse(arrival);
  globalThis.Date=class extends NativeDate{
    constructor(...args){super(...(args.length?args:[clock]));}
    static now(){return clock;}
  };
  const DB=database();
  t.after(()=>{DB.raw.close();globalThis.Date=NativeDate;});
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
  const at=(time,date=day)=>{clock=NativeDate.parse(instant(time,date));};
  const call=async(action,data={})=>(await worker.fetch(new Request('https://daily-assignment.fixture.invalid/api',{
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data}),
  }),env)).json();
  const ok=async(action,data={})=>{const result=await call(action,data);assert.equal(result.ok,true,action+': '+JSON.stringify(result));return result;};
  let record,job;
  if(permanent){
    const person=(await ok('sop_attendance_register',{badge:'DAF-SYNTHETIC-ASSIGNMENT',name:'合成固定拣货人员',agency:'가온'})).person;
    record=(await ok('sop_attendance_checkin',{badge:person.badgeId,agency:'가온'})).record;
  }else record=(await ok('sop_attendance_checkin',{name:'合成日当拣货人员',agency:'가온'})).record;
  const stored=()=>DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(record.id);
  const rows=table=>DB.raw.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
  const workSnapshot=()=>Object.fromEntries(['v2_ops_jobs','v2_ops_job_workers','v2_pick_worker_docs','v2_ops_job_results','sop_records','sop_events'].map(table=>[table,rows(table)]));
  const segments=()=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? ORDER BY joined_at,id').all(job.job_id,record.badgeId);
  const start=async(time='09:10',{mode='native',date=day}={})=>{
    at(time,date);const codes=['SYNTHETIC-PICK-ASSIGNMENT'];let original;
    if(mode!=='native')original=await ok('v2_pick_job_start',{pick_doc_nos:codes,worker_id:record.badgeId,worker_name:record.name});
    if(mode==='legacy')job=await historicalPickStart(env,ok,{pick_doc_nos:codes,worker_id:record.badgeId,worker_name:record.name});
    else job=await ok('sop_native_start',{
      payload:{action:'v2_pick_job_start_by_docs',...pickStartFixture('v2_pick_job_start_by_docs'),pick_doc_nos:codes,client_req_id:crypto.randomUUID()},
      workers:[{id:record.badgeId,name:record.name}],lead_id:record.badgeId,estimated_minutes:30,
    });
    if(original)assert.equal(job.job_id,original.job_id);
    return job;
  };
  const correct=async(time,{out='',preserveWork=!out}={})=>{
    const before=preserveWork?workSnapshot():null;
    record=(await ok('sop_attendance_correct',{id:record.id,version:stored().version,inAt:instant(time,record.day),outAt:out?instant(out,record.day):'',reason:'合成同日出勤更正'})).record;
    if(preserveWork)assert.deepEqual(workSnapshot(),before,'An arrival-only correction must not rewrite physical work records');
    return record;
  };
  const summary=async()=>(await ok('sop_attendance_summary',{date:record.day})).items.find(x=>x.record.id===record.id);
  const card=async()=>(await ok('sop_dispatch_list')).items.find(x=>x.id===job.job_id);
  const checkout=async()=>{const result=await ok('sop_attendance_checkout',{id:record.id});record=result.record;return result;};
  const finish=()=>ok('v2_pick_job_finalize',{job_id:job.job_id,worker_id:record.badgeId});
  const setRecord=r=>{record=r;};
  return {DB,env,at,call,ok,rows,stored,workSnapshot,segments,start,correct,summary,card,checkout,finish,setRecord,get record(){return record;},get job(){return job;}};
}

for(const permanent of [false,true])test(`${permanent?'DAF':'DA'} audited same-day arrival correction keeps exact-ID picking active and clips attendance minutes`,async t=>{
  const s=await setup(t,{permanent});await s.start();s.at('10:00');
  const before=await s.summary();assert.equal(before.status,'working');assert.equal(before.totals.direct_ship,50);
  const original=s.segments()[0];await s.correct('09:20');
  const after=await s.summary(),card=await s.card();
  assert.equal(after.status,'working');assert.equal(after.totals.direct_ship,40);assert.equal(after.totals.unassigned,0);
  assert.deepEqual(after.currentJobs.map(x=>x.id),[s.job.job_id]);assert.equal(card.workers[0].id,s.record.badgeId);
  assert.deepEqual(s.segments()[0],original);assert.equal(after.totals.flags.some(x=>x.includes('上个班次')),false);
  const event=s.rows('ck_attendance_events').find(e=>e.action==='sop_attendance_correct');
  assert.equal(event.record_id,s.record.id);assert.equal(JSON.parse(event.before_json).worker_id,s.record.badgeId);assert.equal(JSON.parse(event.after_json).worker_id,s.record.badgeId);
});

for(const [arrival,unassigned] of [['08:50',20],['09:05',5]])test(`same-day correction to ${arrival} before the work start preserves normal counting`,async t=>{
  const s=await setup(t);await s.start();s.at('10:00');await s.correct(arrival);
  const r=await s.summary();assert.equal(r.status,'working');assert.equal(r.totals.direct_ship,50);assert.equal(r.totals.unassigned,unassigned);assert.equal(r.currentJobs.length,1);
});

test('closed picking segments clip to the corrected arrival without changing original work or reviving assignment',async t=>{
  const s=await setup(t);await s.start();s.at('09:30');await s.finish();const original=s.segments()[0];
  s.at('10:00');await s.correct('09:20');const r=await s.summary();
  assert.equal(r.status,'unassigned');assert.equal(r.totals.direct_ship,10);assert.equal(r.totals.unassigned,30);assert.deepEqual(r.currentJobs,[]);
  assert.deepEqual(s.segments()[0],original);assert.equal(original.minutes_worked,20);assert.equal(await s.card(),undefined);
});

test('correction with a verified departure closes the original segment while attendance clips both ends',async t=>{
  const s=await setup(t);await s.start();s.at('12:00');await s.correct('09:47',{out:'12:00'});
  const segment=s.segments()[0],r=await s.summary();
  assert.equal(segment.joined_at,instant('09:10'));assert.equal(segment.left_at,instant('12:00'));assert.equal(segment.minutes_worked,170);
  assert.equal(r.status,'out');assert.equal(r.totals.direct_ship,133);assert.equal(r.totals.unassigned,0);assert.deepEqual(r.currentJobs,[]);
  assert.equal((await s.card()).workers.length,0);
});

test('checkout after an audited arrival correction closes the existing segment with original elapsed minutes',async t=>{
  const s=await setup(t);await s.start();s.at('10:00');await s.correct('09:47');s.at('12:00');const checked=await s.checkout();
  const segment=s.segments()[0],r=await s.summary();assert.equal(checked.closedSegments,1);
  assert.equal(segment.joined_at,instant('09:10'));assert.equal(segment.left_at,instant('12:00'));assert.equal(segment.minutes_worked,170);assert.equal(segment.leave_reason,'attendance:checkout');
  assert.equal(r.status,'out');assert.equal(r.totals.direct_ship,133);assert.deepEqual(r.currentJobs,[]);assert.equal((await s.card()).workers.length,0);
  assert.ok(s.rows('v2_pick_worker_docs').every(x=>x.status==='completed'&&x.finished_at===instant('12:00')));
});

test('personal break after correction closes the original segment and resumes a new segment once',async t=>{
  const s=await setup(t);await s.start();s.at('10:00');await s.correct('09:47');s.at('12:00');await s.ok('sop_attendance_break_start',{id:s.record.id});
  const original=s.segments()[0];assert.equal(original.joined_at,instant('09:10'));assert.equal(original.left_at,instant('12:00'));assert.equal(original.minutes_worked,170);assert.equal(original.leave_reason,'attendance:rest');
  let r=await s.summary();assert.equal(r.status,'rest');assert.equal(r.totals.direct_ship,133);assert.deepEqual(r.currentJobs,[]);
  s.at('12:15');const request={id:s.record.id,client_req_id:crypto.randomUUID()},ended=await s.ok('sop_attendance_break_end',request);
  assert.deepEqual(ended.resumedJobs,[s.job.job_id]);await s.ok('sop_attendance_break_end',request);
  const segments=s.segments();assert.equal(segments.length,2);assert.deepEqual(segments[0],original);assert.equal(segments[1].joined_at,instant('12:15'));assert.equal(segments[1].left_at,'');
  s.at('12:30');r=await s.summary();assert.equal(r.status,'working');assert.equal(r.totals.direct_ship,148);assert.equal(r.totals.rest,15);assert.equal(r.totals.unassigned,0);assert.equal(r.currentJobs.length,1);
  assert.equal(s.rows('v2_pick_worker_docs').filter(x=>x.status==='working').length,1);
});

test('clearing departure after a genuine checkout never reopens its closed work segment',async t=>{
  const s=await setup(t);await s.start();s.at('09:30');await s.checkout();const original=s.segments()[0];
  s.at('10:00');await s.correct('09:20');const r=await s.summary();
  assert.equal(r.record.outAt,'');assert.equal(r.status,'unassigned');assert.equal(r.totals.direct_ship,10);assert.deepEqual(r.currentJobs,[]);assert.deepEqual(s.segments()[0],original);assert.equal((await s.card()).workers.length,0);
});

test('a historical open segment with a later checkout audit cannot be requalified by clearing departure',async t=>{
  const s=await setup(t);await s.start();s.at('09:25');await s.correct('09:20');s.at('09:30');await s.checkout();
  // Model a legacy unclosed remnant only inside this synthetic database; retain all real audit events.
  const guard=s.DB.raw.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='ck_daily_badge_join_update'").get();assert.ok(guard);
  s.DB.raw.exec('DROP TRIGGER ck_daily_badge_join_update');
  try{s.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at='',minutes_worked=0 WHERE job_id=?").run(s.job.job_id);}
  finally{s.DB.raw.exec(guard.sql);}
  s.at('10:00');await s.correct('09:40');const r=await s.summary();
  assert.equal((await s.card()).workers[0].id,s.record.badgeId);assert.equal(r.segments.length,1);assert.equal(r.status,'occupancy_conflict');assert.equal(r.occupancyWarnings.length,1);assert.equal(r.totals.direct_ship,0);assert.deepEqual(r.currentJobs,[]);
  assert.equal(s.segments()[0].joined_at,instant('09:10'));assert.equal(s.segments()[0].left_at,'');
});

test('a same-day open remnant before arrival stays excluded without a qualifying correction audit',async t=>{
  const s=await setup(t);await s.start();s.DB.raw.prepare('UPDATE ck_attendance_days SET signed_in=? WHERE id=?').run(instant('09:20'),s.record.id);
  s.at('10:00');const before=s.workSnapshot(),r=await s.summary();
  assert.equal(r.status,'occupancy_conflict');assert.equal(r.occupancyWarnings.length,1);assert.equal(r.totals.direct_ship,0);assert.equal(r.totals.unassigned,40);assert.deepEqual(r.currentJobs,[]);assert.equal(r.segments.length,1);assert.equal((await s.card()).workers[0].id,s.record.badgeId);assert.deepEqual(s.workSnapshot(),before);
});

test('previous KST-day open work remains excluded for a fresh fixed-badge attendance day',async t=>{
  const s=await setup(t,{permanent:true,arrival:instant('23:40','2026-10-11')});await s.start('23:50',{date:'2026-10-11'});
  const priorRecord=s.record;s.at('09:00');s.setRecord((await s.ok('sop_attendance_checkin',{badge:priorRecord.badgeId,agency:'가온'})).record);
  assert.notEqual(s.record.id,priorRecord.id);assert.equal(s.record.badgeId,priorRecord.badgeId);s.at('10:00');await s.correct('09:20');
  const r=await s.summary();assert.equal(r.status,'occupancy_conflict');assert.equal(r.occupancyWarnings.length,1);assert.equal(r.totals.direct_ship,0);assert.equal(r.totals.unassigned,40);assert.deepEqual(r.currentJobs,[]);assert.equal(r.segments.length,1);assert.equal((await s.card()).workers[0].id,s.record.badgeId);
});

const invalidAuditCases=[
  ['different attendance record',(e)=>({...e,record_id:'SYNTHETIC-OTHER-ATTENDANCE'})],
  ['different prior record identity',(e)=>({...e,before_json:JSON.stringify({...JSON.parse(e.before_json),id:'SYNTHETIC-OTHER-ATTENDANCE'})})],
  ['different prior worker identity',(e)=>({...e,before_json:JSON.stringify({...JSON.parse(e.before_json),worker_id:'DA-SYNTHETIC-OTHER'})})],
  ['different resulting worker identity',(e)=>({...e,after_json:JSON.stringify({...JSON.parse(e.after_json),worker_id:'DA-SYNTHETIC-OTHER'})})],
  ['audit preceding the work segment',(e)=>({...e,at:instant('09:05')})],
  ['prior record already signed out',(e)=>({...e,before_json:JSON.stringify({...JSON.parse(e.before_json),signed_out:instant('09:15')})})],
  ['prior arrival already after the segment',(e)=>({...e,before_json:JSON.stringify({...JSON.parse(e.before_json),signed_in:instant('09:15')})})],
];
for(const [label,change] of invalidAuditCases)test(`correction evidence with ${label} cannot qualify a pre-arrival open segment`,async t=>{
  const s=await setup(t);await s.start();s.at('10:00');await s.correct('09:20');
  const original=s.rows('ck_attendance_events').find(e=>e.action==='sop_attendance_correct'),event=change(original);
  s.DB.raw.prepare('UPDATE ck_attendance_events SET record_id=?,before_json=?,after_json=?,at=? WHERE id=?').run(event.record_id,event.before_json,event.after_json,event.at,event.id);
  const r=await s.summary();assert.equal(r.status,'occupancy_conflict');assert.equal(r.occupancyWarnings.length,1);assert.equal(r.totals.direct_ship,0);assert.deepEqual(r.currentJobs,[]);assert.equal(s.segments()[0].joined_at,instant('09:10'));
});

test('same-name workers retain distinct assignments and correction evidence never moves between IDs',async t=>{
  const s=await setup(t),other=(await s.ok('sop_attendance_checkin',{name:s.record.name,agency:'포레인',confirm_distinct_person:true,reason:'合成同名不同人核实'})).record;
  assert.notEqual(other.badgeId,s.record.badgeId);await s.start();s.at('10:00');await s.correct('09:20');
  const report=await s.ok('sop_attendance_summary',{date:day}),own=report.items.find(x=>x.record.id===s.record.id),separate=report.items.find(x=>x.record.id===other.id);
  assert.equal(own.status,'working');assert.equal(own.totals.direct_ship,40);assert.equal(separate.status,'unassigned');assert.equal(separate.totals.direct_ship,0);assert.deepEqual(separate.currentJobs,[]);assert.deepEqual(separate.segments,[]);
});

for(const state of ['completed','paused','rest','removed'])test(`a corrected attendance does not turn ${state} work into an active assignment`,async t=>{
  const s=await setup(t);await s.start();s.at('09:25');await s.correct('09:20');s.at('09:30');
  if(state==='completed')await s.finish();
  else if(state==='paused')await s.ok('sop_native_pause',{job_id:s.job.job_id,revision:1,reason:'合成暂停核实'});
  else if(state==='rest')await s.ok('sop_attendance_break_start',{id:s.record.id});
  else await s.ok('sop_native_people',{job_id:s.job.job_id,revision:1,workers:[],lead_id:'',reason:'合成人员移出'});
  s.at('10:00');const r=await s.summary();assert.equal(r.status,state==='rest'?'rest':'unassigned');assert.equal(r.totals.direct_ship,10);assert.deepEqual(r.currentJobs,[]);assert.equal(s.segments()[0].left_at,instant('09:30'));assert.equal(s.segments()[0].minutes_worked,20);
});

for(const mode of ['legacy','legacy-pending-native'])test(`${mode} picking preserves the exact badge ID in attendance and original worker segments`,async t=>{
  const s=await setup(t,{permanent:mode!=='legacy'});await s.start('09:10',{mode});s.at('10:00');
  const r=await s.summary();assert.equal(r.status,'working');assert.equal(r.totals.direct_ship,50);assert.equal(s.segments().length,1);assert.equal(s.segments()[0].worker_id,s.record.badgeId);
  assert.equal(s.rows('v2_pick_worker_docs')[0].worker_id,s.record.badgeId);assert.equal(r.currentJobs[0].id,s.job.job_id);
});

for(const [label,start,asOf,encoded,minutes] of [
  ['SQLite UTC datetime','09:10','10:00','2026-10-12 00:10:00',50],
  ['explicit Korean timezone offset','16:00','17:00','2026-10-12T16:00:00+09:00',60],
])test(`historical ${label} encoding preserves the same active picking interval`,async t=>{
  const s=await setup(t);await s.start(start);s.at(asOf);
  const original=s.segments()[0];assert.equal(NativeDate.parse(encoded),NativeDate.parse(original.joined_at));
  // Current APIs emit ISO UTC. Only this isolated historical fixture changes its equivalent encoding.
  s.DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE id=?').run(encoded,original.id);
  const r=await s.summary();assert.equal(r.status,'working');assert.equal(r.totals.direct_ship,minutes);
  assert.equal(r.segments.length,1);assert.deepEqual(r.currentJobs.map(x=>x.id),[s.job.job_id]);
  assert.equal(r.totals.flags.some(x=>x.includes('上个班次')),false);assert.equal(s.segments()[0].joined_at,encoded);
  assert.equal((await s.card()).workers[0].id,s.record.badgeId);
});
