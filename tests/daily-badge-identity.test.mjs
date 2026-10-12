import {pickStartFixture} from './pick-workload-fixture.mjs';
// Synthetic identities and real Worker handlers against SQLite :memory: only.
import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {ATTENDANCE_SCHEMA} from '../worker-v2/attendance.js';
import {ATTENDANCE_MANAGEMENT_SCHEMA} from '../worker-v2/attendance-management.js';
import {CREW_BORROW_SCHEMA} from '../worker-v2/crew-borrow.js';
import {database} from './d1-adapter.mjs';

const NativeDate=Date,day='2026-10-12';
const instant=time=>new NativeDate(`${day}T${time}:00+09:00`).toISOString();
const identityError=/badge|identity|attendance|worker|conflict|ambiguous|canonical|签到|工牌|冲突|身份|作业|人员/i;
const oldRestGuard=`CREATE TRIGGER ck_rest_resume_guard BEFORE INSERT ON v2_ops_job_workers WHEN NEW.id LIKE 'WS-REST-%' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=NEW.job_id AND j.status IN ('working','awaiting_close')) OR EXISTS(SELECT 1 FROM sop_records s WHERE s.id=NEW.job_id AND s.kind IN ('task','dispatch') AND (json_extract(s.state,'$.status')='paused' OR NOT EXISTS(SELECT 1 FROM json_each(s.state,'$.workers') w WHERE json_extract(w.value,'$.id')=NEW.worker_id))) THEN RAISE(ABORT,'rest_assignment_changed') END; END`;
// Exact matching definitions from the prior attendance schema, kept as synthetic upgrade fixtures.
const oldRelatedWork=(w,d)=>`${w}.worker_id=${d}.worker_id AND julianday(${w}.joined_at)<julianday(${d}.day,'+1 day','-9 hours') AND (${w}.left_at='' OR julianday(${w}.left_at)>=julianday(${d}.day,'-9 hours'))`;
const oldManagementGuards=['INSERT','UPDATE'].flatMap(op=>[
  `CREATE TRIGGER ck_attendance_void_safe_${op.toLowerCase()} BEFORE ${op} ON ck_attendance_management WHEN NEW.voided_at!='' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d WHERE d.id=NEW.attendance_id AND d.worker_id NOT LIKE 'EMP-%') OR EXISTS(SELECT 1 FROM ck_attendance_days d JOIN v2_ops_job_workers w ON ${oldRelatedWork('w','d')} WHERE d.id=NEW.attendance_id) OR EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=NEW.attendance_id) THEN RAISE(ABORT,'Attendance has work or rest; cannot void') END; END`,
  `CREATE TRIGGER ck_attendance_void_work_${op.toLowerCase()} BEFORE ${op} ON v2_ops_job_workers WHEN EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_management m ON m.attendance_id=d.id WHERE m.voided_at!='' AND ${oldRelatedWork('NEW','d')}) BEGIN SELECT RAISE(ABORT,'Attendance voided; restore before work'); END`,
]);
const oldLoanGuards=['INSERT','UPDATE'].flatMap(op=>[
  `CREATE TRIGGER ck_attendance_void_loan_${op.toLowerCase()} BEFORE ${op} ON ck_attendance_management WHEN NEW.voided_at!='' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM ck_crew_borrows b JOIN ck_attendance_days d ON d.worker_id=b.worker_id WHERE d.id=NEW.attendance_id AND b.status IN ('borrowed','return_pending')) THEN RAISE(ABORT,'Attendance has pending loan; cannot void') END; END`,
  `CREATE TRIGGER ck_attendance_void_borrow_${op.toLowerCase()} BEFORE ${op} ON ck_crew_borrows WHEN NEW.status IN ('borrowed','return_pending') AND EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_management m ON m.attendance_id=d.id WHERE d.worker_id=NEW.worker_id AND d.day=NEW.source_day AND m.voided_at!='') BEGIN SELECT RAISE(ABORT,'Attendance voided; cannot borrow'); END`,
]);
function watchBatches(DB){const batches=[],batch=DB.batch.bind(DB);DB.batch=async statements=>{batches.push(statements.map(s=>s.sql));return batch(statements);};return batches;}
function assertAtomicTriggerUpgrade(DB,batches,names){
  for(const name of names){
    assert.match(DB.raw.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?").get(name).sql,/ck_badge_left/);
    assert.ok(batches.some(statements=>statements.some(sql=>sql.startsWith('DROP TRIGGER IF EXISTS '+name))&&statements.some(sql=>sql.includes('CREATE TRIGGER IF NOT EXISTS '+name+' '))),name+' must be dropped and replaced within one transactional batch');
  }
}

async function fixture(t){
  let clock=NativeDate.parse(instant('09:00'));
  globalThis.Date=class extends NativeDate{constructor(...args){super(...(args.length?args:[clock]));}static now(){return clock;}};
  const DB=database();t.after(()=>{DB.raw.close();globalThis.Date=NativeDate;});
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
  const at=time=>{clock=NativeDate.parse(instant(time));};
  const call=async(action,data={})=>(await worker.fetch(new Request('https://badge-identity.fixture.invalid/api',{
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data}),
  }),env)).json();
  const ok=async(action,data={})=>{const result=await call(action,data);assert.equal(result.ok,true,action+': '+JSON.stringify(result));return result;};
  let record=(await ok('sop_attendance_checkin',{name:'合成大小写归属人员',agency:'가온'})).record,job;
  const canonical=record.badgeId,lower=canonical.toLowerCase();assert.match(canonical,/^DA-\d{8}-[0-9A-F]{8}$/);
  const rows=table=>DB.raw.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
  const stored=()=>DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(record.id);
  const snapshot=()=>Object.fromEntries(['ck_attendance_people','ck_attendance_days','ck_attendance_events','ck_attendance_breaks','ck_attendance_management','v2_ops_jobs','v2_ops_job_workers','v2_pick_worker_docs','sop_records','sop_events'].map(table=>[table,rows(table)]));
  const work=()=>Object.fromEntries(['v2_ops_jobs','v2_ops_job_workers','v2_pick_worker_docs','sop_records','sop_events'].map(table=>[table,rows(table)]));
  // Only suspend newly introduced identity guards to represent preexisting historical rows.
  // Restore their original SQL immediately; attendance, void, and lifecycle guards remain installed.
  const historical=mutate=>{
    const guards=DB.raw.prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger'").all().filter(x=>/^ck_daily_badge_/.test(x.name));
    try{for(const guard of guards)DB.raw.exec('DROP TRIGGER "'+guard.name.replaceAll('"','""')+'"');mutate();}
    finally{for(const guard of guards)DB.raw.exec(guard.sql);}
  };
  const start=async(id=canonical,code='SYNTHETIC-DAILY-BADGE-PICK')=>{
    job=await ok('sop_native_start',{payload:{action:'v2_pick_job_start_by_docs',...pickStartFixture('v2_pick_job_start_by_docs'),pick_doc_nos:[code],client_req_id:crypto.randomUUID()},workers:[{id,name:record.name}],lead_id:id,estimated_minutes:30});return job;
  };
  const legacy=async()=>{
    at('09:10');await start();
    historical(()=>DB.raw.prepare('UPDATE v2_ops_job_workers SET worker_id=? WHERE job_id=?').run(lower,job.job_id));
    DB.raw.prepare('UPDATE v2_pick_worker_docs SET worker_id=? WHERE job_id=?').run(lower,job.job_id);
    DB.raw.prepare('UPDATE v2_ops_jobs SET created_by=? WHERE id=?').run(lower,job.job_id);
    const row=DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(job.job_id),state=JSON.parse(row.state);
    state.workers=state.workers.map(w=>({...w,id:lower}));state.lead_id=lower;
    DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(state),job.job_id);
    return job;
  };
  const segments=()=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at,id').all(job.job_id);
  const summary=async()=>(await ok('sop_attendance_summary',{date:day})).items.find(x=>x.record.id===record.id);
  const correct=async(time,{out=''}={})=>{
    const result=await ok('sop_attendance_correct',{id:record.id,version:stored().version,inAt:instant(time),outAt:out?instant(out):'',reason:'合成大小写历史记录出勤核实'});record=result.record;return result;
  };
  const sqlJob=()=>{const id='SYNTHETIC-SQL-JOB-'+crypto.randomUUID();DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,created_at,updated_at) VALUES(?,'pick_direct','direct_ship','working',?,?)").run(id,instant('09:10'),instant('09:10'));return id;};
  const insertSegment=(id,badge,jobId,{left='',joined=instant('09:10')}={})=>DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,0)').run(id,jobId,badge,'合成SQL原始姓名',joined,left);
  return {DB,env,at,call,ok,rows,stored,snapshot,work,historical,start,legacy,segments,summary,correct,sqlJob,insertSegment,canonical,lower,get record(){return record;},get job(){return job;}};
}

test('lowercase scan of a registered system daily badge stores canonical identity and reports active attendance',async t=>{
  const s=await fixture(t);s.at('09:10');await s.start(s.lower);s.at('10:00');
  assert.equal(s.segments()[0].worker_id,s.canonical);assert.equal(s.rows('v2_pick_worker_docs')[0].worker_id,s.canonical);
  const state=JSON.parse(s.rows('sop_records')[0].state);assert.equal(state.workers[0].id,s.canonical);assert.equal(state.lead_id,s.canonical);
  const r=await s.summary();assert.equal(r.record.badgeId,s.canonical);assert.equal(r.status,'working');assert.equal(r.totals.direct_ship,50);assert.equal(r.currentJobs.length,1);
});

test('historical lower-case work belongs to the canonical daily record while reads preserve every raw identity',async t=>{
  const s=await fixture(t);await s.legacy();s.at('10:00');const before=s.work(),r=await s.summary();
  assert.equal(r.status,'working');assert.equal(r.totals.direct_ship,50);assert.equal(r.segments.length,1);
  assert.equal(r.segments[0].badgeId,s.canonical);assert.equal(r.segments[0].sourceWorkerId,s.lower);assert.equal(r.currentJobs[0].id,s.job.job_id);
  assert.equal(s.segments()[0].worker_id,s.lower);assert.equal(s.rows('v2_pick_worker_docs')[0].worker_id,s.lower);assert.equal(JSON.parse(s.rows('sop_records')[0].state).lead_id,s.lower);
  assert.deepEqual(s.work(),before);
});

for(const exit of ['checkout','correct'])test(`${exit} closes a historical lower-case segment using original raw elapsed minutes`,async t=>{
  const s=await fixture(t);await s.legacy();s.at('10:00');const before=s.work();await s.correct('09:47');assert.deepEqual(s.work(),before);
  s.at('12:00');if(exit==='checkout')await s.ok('sop_attendance_checkout',{id:s.record.id});else await s.correct('09:47',{out:'12:00'});
  const segment=s.segments()[0],r=await s.summary();assert.equal(segment.worker_id,s.lower);assert.equal(segment.joined_at,instant('09:10'));assert.equal(segment.left_at,instant('12:00'));assert.equal(segment.minutes_worked,170);
  assert.equal(r.status,'out');assert.equal(r.totals.direct_ship,133);assert.deepEqual(r.currentJobs,[]);assert.equal(r.segments[0].sourceWorkerId,s.lower);
  assert.ok(s.rows('v2_pick_worker_docs').every(x=>x.worker_id===s.lower&&x.status==='completed'));
});

test('rest closes the raw lower-case segment and resumes one new canonical segment without rewriting history',async t=>{
  const s=await fixture(t);await s.legacy();s.at('10:00');await s.correct('09:47');s.at('12:00');await s.ok('sop_attendance_break_start',{id:s.record.id});
  const old=s.segments()[0];assert.equal(old.worker_id,s.lower);assert.equal(old.left_at,instant('12:00'));assert.equal(old.minutes_worked,170);
  assert.equal((await s.summary()).status,'rest');assert.equal((await s.summary()).totals.direct_ship,133);
  // An existing DB already has the old exact-ID resume guard; the real endpoint must upgrade it.
  s.DB.raw.exec(oldRestGuard);const batches=watchBatches(s.DB);
  s.at('12:15');const request={id:s.record.id,client_req_id:crypto.randomUUID()},resumed=await s.ok('sop_attendance_break_end',request);
  assertAtomicTriggerUpgrade(s.DB,batches,['ck_rest_resume_guard']);
  assert.deepEqual(resumed.resumedJobs,[s.job.job_id]);await s.ok('sop_attendance_break_end',request);
  assert.equal(s.segments().length,2);assert.deepEqual(s.segments()[0],old);assert.equal(s.segments()[1].worker_id,s.canonical);assert.equal(s.segments()[1].joined_at,instant('12:15'));
  assert.equal(s.rows('v2_pick_worker_docs').filter(x=>x.status==='working').length,1);assert.equal(s.rows('v2_pick_worker_docs').find(x=>x.status==='working').worker_id,s.canonical);
  s.at('12:30');const r=await s.summary();assert.equal(r.status,'working');assert.equal(r.totals.direct_ship,148);assert.equal(r.totals.rest,15);assert.equal(r.currentJobs.length,1);
});

test('existing lower-case labor blocks attendance void through both API and database guard',async t=>{
  const s=await fixture(t);await s.legacy();s.at('10:00');const before=s.snapshot(),r=await s.summary();
  assert.equal(r.canVoid,false);assert.match(r.voidBlockedReason,identityError);
  const denied=await s.call('sop_attendance_void',{id:s.record.id,version:s.stored().version,reason:'合成误签到删除尝试'});assert.equal(denied.ok,false);
  assert.throws(()=>s.DB.raw.prepare("INSERT INTO ck_attendance_management(attendance_id,voided_at,void_reason,voided_by) VALUES(?,?,?,'SYNTHETIC') ON CONFLICT(attendance_id) DO UPDATE SET voided_at=excluded.voided_at").run(s.record.id,instant('10:00'),'合成SQL删除尝试'),identityError);
  assert.deepEqual(s.snapshot(),before);
});

test('same-name daily records are not merged when only one has a historical case alias',async t=>{
  const s=await fixture(t),other=(await s.ok('sop_attendance_checkin',{name:s.record.name,agency:'포레인',confirm_distinct_person:true,reason:'合成同名不同人'})).record;
  await s.legacy();s.at('10:00');const report=await s.ok('sop_attendance_summary',{date:day});
  const own=report.items.find(x=>x.record.id===s.record.id),separate=report.items.find(x=>x.record.id===other.id);
  assert.notEqual(other.badgeId,s.canonical);assert.equal(own.status,'working');assert.equal(own.totals.direct_ship,50);assert.equal(separate.status,'unassigned');assert.equal(separate.totals.direct_ship,0);assert.deepEqual(separate.segments,[]);
});

for(const collision of ['registration','attendance-day','two-open-segments'])test(`${collision} case collision reports identity conflict and rejects attendance changes without choosing a name`,async t=>{
  const s=await fixture(t);await s.legacy();s.at('10:00');
  s.historical(()=>{
    if(collision==='registration')s.DB.raw.prepare("INSERT INTO ck_attendance_people(id,badge_id,name,agency,kind,enabled,created_at) VALUES(?,?,?,'포레인','daily',1,?)").run('SYNTHETIC-COLLIDING-PERSON',s.lower,'另一个合成姓名',instant('09:00'));
    else if(collision==='attendance-day')s.DB.raw.prepare('INSERT INTO ck_attendance_days(id,person_id,worker_id,name,agency,day,identity_key,signed_in,signed_out,version) VALUES(?,?,?,?,?,?,?, ?,\'\',1)').run('SYNTHETIC-COLLIDING-DAY',s.stored().person_id,s.lower,'另一个合成姓名','포레인',day,'synthetic-collision-identity',instant('09:00'));
    else s.insertSegment('SYNTHETIC-COLLIDING-SEGMENT',s.canonical,s.job.job_id);
  });
  const before=s.snapshot(),r=await s.summary();assert.equal(r.status,'identity_conflict');assert.equal(r.identityConflict,true);assert.equal(r.totals.direct_ship,0);assert.ok(r.totals.conflict>0);assert.match(r.totals.flags.join(' '),identityError);assert.deepEqual(r.currentJobs,[]);assert.deepEqual(s.snapshot(),before);
  for(const [action,body] of [
    ['sop_attendance_checkout',{}],['sop_attendance_break_start',{}],
    ['sop_attendance_correct',{inAt:instant('09:20'),outAt:'',reason:'合成冲突更正'}],
    ['sop_attendance_void',{reason:'合成冲突删除'}],
  ]){
    const result=await s.call(action,{id:s.record.id,version:s.stored().version,...body});assert.equal(result.ok,false,action+': '+JSON.stringify(result));assert.deepEqual(s.snapshot(),before);
  }
});

for(const badge of ['da-20261012-deadbeef','da-invalid','daf-synthetic-unknown','emp-synthetic-unknown'])test(`SQL insert cannot bypass attendance using ${badge}`,async t=>{
  const s=await fixture(t),jobId=s.sqlJob();s.at('10:00');
  assert.throws(()=>s.insertSegment('SYNTHETIC-DENIED-INSERT',badge,jobId),badge.startsWith('emp-')?/Employee disabled or unregistered/:identityError);
  assert.equal(s.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE id='SYNTHETIC-DENIED-INSERT'").get().n,0);
});

test('SQL insertion sees an existing raw lower-case segment as canonical occupancy',async t=>{
  const s=await fixture(t);await s.legacy();const other=s.sqlJob();
  assert.throws(()=>s.insertSegment('SYNTHETIC-DOUBLE-CASE',s.canonical,other),identityError);
  assert.equal(s.segments().length,1);assert.equal(s.segments()[0].worker_id,s.lower);
});

for(const change of ['reopen','replace-identity'])test(`SQL ${change} cannot open a segment under an unregistered lower-case daily badge`,async t=>{
  const s=await fixture(t);await s.legacy();const original=s.segments()[0];
  if(change==='reopen')s.DB.raw.prepare('UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=30 WHERE id=?').run(instant('09:40'),original.id);
  const before=s.segments();
  assert.throws(()=>s.DB.raw.prepare("UPDATE v2_ops_job_workers SET worker_id='da-20261012-deadbeef',left_at='' WHERE id=?").run(original.id),identityError);
  assert.deepEqual(s.segments(),before);
});

test('closing a historical unregistered lower-case segment remains possible after identity guards are installed',async t=>{
  const s=await fixture(t),jobId=s.sqlJob(),id='SYNTHETIC-HISTORICAL-UNKNOWN';
  s.historical(()=>s.insertSegment(id,'da-20261012-deadbeef',jobId));
  s.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=30,leave_reason='synthetic_verified_close' WHERE id=?").run(instant('09:40'),id);
  const row=s.DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE id=?').get(id);
  assert.equal(row.worker_id,'da-20261012-deadbeef');assert.equal(row.joined_at,instant('09:10'));assert.equal(row.left_at,instant('09:40'));assert.equal(row.minutes_worked,30);
});

test('real attendance startup upgrades an existing attendance schema and replaces its old join guard',async t=>{
  const DB=database();t.after(()=>DB.raw.close());
  for(const sql of ATTENDANCE_SCHEMA)DB.raw.exec(sql);
  // Seed the pre-upgrade guard and persisted registration before this DB binding receives any API request.
  DB.raw.exec("DROP TRIGGER IF EXISTS ck_attendance_guard_join; CREATE TRIGGER ck_attendance_guard_join BEFORE INSERT ON v2_ops_job_workers WHEN (NEW.worker_id GLOB 'DA-*' OR NEW.worker_id GLOB 'DAF-*') AND NEW.left_at='' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d WHERE d.worker_id=NEW.worker_id AND d.day=date(NEW.joined_at,'+9 hours') AND d.signed_out='' AND d.signed_in<=NEW.joined_at) THEN RAISE(ABORT,'Legacy attendance required') END; END");
  DB.raw.exec(ATTENDANCE_MANAGEMENT_SCHEMA[0]);for(const sql of oldManagementGuards)DB.raw.exec(sql);
  DB.raw.prepare("INSERT INTO ck_attendance_people VALUES('SYNTHETIC-UPGRADE-PERSON','DA-20261012-AB12CD34','合成升级保留人员','가온','daily',1,?)").run(instant('09:00'));
  DB.raw.prepare("INSERT INTO ck_attendance_days VALUES('SYNTHETIC-UPGRADE-DAY','SYNTHETIC-UPGRADE-PERSON','DA-20261012-AB12CD34','合成升级保留人员','가온',?,'synthetic-upgrade',?,'',1)").run(day,instant('09:00'));
  const before={people:DB.raw.prepare('SELECT * FROM ck_attendance_people').all(),days:DB.raw.prepare('SELECT * FROM ck_attendance_days').all()},batches=watchBatches(DB);
  assert.ok(DB.raw.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name='ck_attendance_guard_join'").get());
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
  const response=await worker.fetch(new Request('https://badge-upgrade.fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_attendance_config'})}),env);
  const result=await response.json();assert.equal(result.ok,true,JSON.stringify(result));
  assert.equal(DB.raw.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name='ck_attendance_guard_join'").get(),undefined);
  const upgraded=DB.raw.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name IN ('ck_daily_badge_join_insert','ck_daily_badge_join_update') ORDER BY name").all();
  assert.deepEqual(upgraded.map(x=>x.name),['ck_daily_badge_join_insert','ck_daily_badge_join_update']);
  assertAtomicTriggerUpgrade(DB,batches,oldManagementGuards.map(sql=>sql.match(/^CREATE TRIGGER (\w+)/)[1]));
  assert.deepEqual(DB.raw.prepare('SELECT * FROM ck_attendance_people').all(),before.people);assert.deepEqual(DB.raw.prepare('SELECT * FROM ck_attendance_days').all(),before.days);
  DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status) VALUES('SYNTHETIC-UPGRADE-JOB','pick_direct','working')").run();
  assert.throws(()=>DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES('SYNTHETIC-UPGRADE-BYPASS','SYNTHETIC-UPGRADE-JOB','da-20261012-deadbeef','合成未签到',?)").run(instant('09:10')),identityError);
  DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES('SYNTHETIC-UPGRADE-OLD-CASE','SYNTHETIC-UPGRADE-JOB','da-20261012-ab12cd34','合成升级保留人员',?)").run(instant('09:10'));
  assert.throws(()=>DB.raw.prepare("INSERT INTO ck_attendance_management(attendance_id,voided_at) VALUES('SYNTHETIC-UPGRADE-DAY',?)").run(instant('10:00')),identityError);
});

test('actual attendance mutation upgrades exact loan guards on an existing loan table atomically',async t=>{
  const s=await fixture(t);s.DB.raw.exec(CREW_BORROW_SCHEMA[0]);for(const sql of oldLoanGuards)s.DB.raw.exec(sql);
  s.DB.raw.prepare("INSERT INTO ck_crew_borrows(id,request_id,actor_id,actor_name,destination_job_id,worker_id,worker_name,source_job_id,source_segment_id,source_kind,source_revision,source_owner_id,source_day,status,borrowed_at) VALUES('SYNTHETIC-OLD-LOAN','SYNTHETIC-LOAN-REQUEST','SYNTHETIC-MANAGER','合成负责人','SYNTHETIC-DESTINATION',?,'合成借调人员','SYNTHETIC-SOURCE','SYNTHETIC-SOURCE-SEGMENT','legacy',1,'SYNTHETIC-OWNER',?,'return_pending',?)").run(s.lower,day,instant('09:10'));
  s.at('10:00');const before=s.snapshot(),loans=s.rows('ck_crew_borrows'),batches=watchBatches(s.DB);
  const denied=await s.call('sop_attendance_void',{id:s.record.id,version:s.stored().version,reason:'合成旧借调删除核实'});assert.equal(denied.ok,false);assert.match(denied.error,/借调|归还|loan|borrow/i);
  assertAtomicTriggerUpgrade(s.DB,batches,oldLoanGuards.map(sql=>sql.match(/^CREATE TRIGGER (\w+)/)[1]));
  assert.throws(()=>s.DB.raw.prepare('INSERT INTO ck_attendance_management(attendance_id,voided_at) VALUES(?,?)').run(s.record.id,instant('10:00')),/loan|borrow/i);
  assert.deepEqual(s.snapshot(),before);assert.deepEqual(s.rows('ck_crew_borrows'),loans);
});

test('a historical registration without a current attendance day never creates attendance or counts orphaned work',async t=>{
  const s=await fixture(t);await s.legacy();s.DB.raw.prepare('DELETE FROM ck_attendance_days WHERE id=?').run(s.record.id);s.at('10:00');
  const before=s.snapshot(),report=await s.ok('sop_attendance_summary',{date:day});assert.deepEqual(report.items,[]);
  assert.equal(report.identityWarnings.length,1);assert.equal(report.identityWarnings[0].worker_id,s.lower);assert.equal(report.identityWarnings[0].job_id,s.job.job_id);assert.match(report.identityWarnings[0].reason,/签到|身份|核实/);
  const denied=await s.call('sop_native_start',{payload:{action:'v2_pick_job_start_by_docs',...pickStartFixture('v2_pick_job_start_by_docs'),pick_doc_nos:['SYNTHETIC-ORPHAN-NEW'],client_req_id:crypto.randomUUID()},workers:[{id:s.lower,name:s.record.name}],lead_id:s.lower,estimated_minutes:15});
  assert.equal(denied.ok,false);assert.deepEqual(s.snapshot(),before);assert.equal(s.rows('ck_attendance_people').length,1);assert.equal(s.rows('ck_attendance_days').length,0);assert.equal(s.rows('v2_ops_jobs').length,1);assert.equal(s.segments()[0].worker_id,s.lower);
});

test('SQL admission rejects a strict generated daily badge attached to a permanent person',async t=>{
  const s=await fixture(t),jobId=s.sqlJob();
  s.DB.raw.prepare("UPDATE ck_attendance_people SET kind='permanent' WHERE id=?").run(s.stored().person_id);
  const before=s.snapshot();
  assert.throws(()=>s.insertSegment('SYNTHETIC-WRONG-PERSON-KIND',s.lower,jobId),identityError);
  assert.deepEqual(s.snapshot(),before);
});

test('SQL reopening a historical NULL end cannot bypass missing attendance',async t=>{
  const s=await fixture(t),jobId=s.sqlJob(),id='SYNTHETIC-HISTORICAL-NULL';
  assert.equal(s.DB.raw.prepare('PRAGMA table_info(v2_ops_job_workers)').all().find(x=>x.name==='left_at').notnull,0);
  s.historical(()=>s.insertSegment(id,'da-20261012-deadbeef',jobId,{left:null}));
  const before=s.snapshot();
  assert.throws(()=>s.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at='' WHERE id=?").run(id),identityError);
  assert.deepEqual(s.snapshot(),before);
});
