// Actual Worker/handlers with SQLite :memory:; candidate regressions, not live UI acceptance.
import test from 'node:test';
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const root=process.env.CK_REVIEW_REPO
 ?pathToFileURL(process.env.CK_REVIEW_REPO.replace(/\\/g,'/').replace(/\/$/,'')+'/')
 :new URL('../',import.meta.url);
const {default:worker}=await import(new URL('worker-v2/index.js',root));
const {database}=await import(new URL('tests/d1-adapter.mjs',root));
const {handleAttendance}=await import(new URL('worker-v2/attendance.js',root));

async function setup(t,{attendance=false}={}){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',
  ...(attendance?{SOP_ATTENDANCE_ENABLED:'true'}:{}),SOP_REQUEST_USER:{id:'QA-M',name:'QA only',role:'manager'}};
 const raw=async(action,data={})=>{
  const response=await worker.fetch(new Request('https://fixture.invalid/api',{
   method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})
  }),env);return {status:response.status,body:await response.json()};
 };
 const ok=async(action,data={})=>{const r=await raw(action,data);assert.equal(r.body.ok,true,JSON.stringify(r));return r.body;};
 const attendanceCall=(suffix,data={})=>handleAttendance({action:'sop_attendance_'+suffix,client_req_id:crypto.randomUUID(),...data},env);
 await ok('sop_identity');
 const employee=async()=>(await attendanceCall('employee_register',{name:'QA isolated employee',employeeNo:'QA-P1-ONLY',department:'bulk'})).person;
 const disable=p=>attendanceCall('employee_update',{id:p.id,version:p.version,name:p.name,department:p.department,enabled:false});
 function job(id,type='inventory'){
  DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,created_at) VALUES(?,?,'completed','2026-09-30T00:00:00.000Z')").run(id,type);
 }
 function segment(id,jobId,badge,name,start,end,minutes){
  DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run(id,jobId,badge,name,start,end,minutes);
 }
 const hours=data=>ok('v2_dashboard_workhour_summary',data),management=data=>ok('v2_dashboard_management_summary',data);
 return {DB,env,raw,ok,attendanceCall,employee,disable,job,segment,hours,management};
}

test('ordinary employee disable revalidates a checkin committed after its preflight',async t=>{
 const s=await setup(t,{attendance:true}),p=await s.employee(),batch=s.DB.batch.bind(s.DB);let injected=false;
 s.DB.batch=async statements=>{
  if(!injected&&statements[0]?.args[4]==='sop_attendance_employee_update'){
   injected=true;await s.attendanceCall('checkin',{badge:p.badgeId});
  }
  return batch(statements);
 };
 await assert.rejects(s.disable(p),/变化|冲突|签退|任务|changed|active/i);
 assert.equal(injected,true);
 assert.equal(s.DB.raw.prepare('SELECT enabled FROM ck_attendance_people WHERE id=?').get(p.id).enabled,1);
 assert.equal(s.DB.raw.prepare('SELECT version FROM ck_employee_profiles WHERE person_id=?').get(p.id).version,1);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action='sop_attendance_employee_update'").get().n,0);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_days WHERE person_id=? AND signed_out=''").get(p.id).n,1);
});

test('checkin revalidates an employee disabled after its enabled-profile preflight',async t=>{
 const s=await setup(t,{attendance:true}),p=await s.employee(),batch=s.DB.batch.bind(s.DB);let injected=false;
 s.DB.batch=async statements=>{
  if(!injected&&statements[0]?.args[4]==='sop_attendance_checkin'){
   injected=true;await s.disable(p);
  }
  return batch(statements);
 };
 await assert.rejects(s.attendanceCall('checkin',{badge:p.badgeId}),/变化|冲突|停用|在职|enabled|disabled|employee/i);
 assert.equal(injected,true);
 assert.equal(s.DB.raw.prepare('SELECT enabled FROM ck_attendance_people WHERE id=?').get(p.id).enabled,0);
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days WHERE person_id=?').get(p.id).n,0);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action='sop_attendance_checkin'").get().n,0);
});

async function disabledHistoricalPunch(t){
 const s=await setup(t,{attendance:true}),p=await s.employee();
 await s.attendanceCall('checkin',{badge:p.badgeId});
 // Seed a legacy inconsistent state only inside this isolated memory DB.
 s.DB.raw.prepare('UPDATE ck_attendance_people SET enabled=0 WHERE id=?').run(p.id);
 return {s,p};
}

test('disabled employee with a legacy open punch cannot enter a new native task',async t=>{
 const {s,p}=await disabledHistoricalPunch(t);
 const started=await s.raw('sop_native_start',{
  payload:{action:'v2_ops_job_start',job_type:'inventory',flow_stage:'internal',client_req_id:crypto.randomUUID()},
  labor_department:'bulk',workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId,estimated_minutes:5
 });
 assert.equal(started.body.ok,false,JSON.stringify(started));
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=?').get(p.badgeId).n,0);
});

test('SQL attendance guard rejects a disabled employee even after API preflight passed',async t=>{
 const {s,p}=await disabledHistoricalPunch(t);s.job('QA-SQL-GUARD');
 s.DB.raw.prepare("UPDATE v2_ops_jobs SET status='working' WHERE id='QA-SQL-GUARD'").run();
 assert.throws(()=>s.segment('QA-SQL-SEG','QA-SQL-GUARD',p.badgeId,p.name,new Date().toISOString(),'',0),/employee|enabled|disabled|在职|停用|attendance/i);
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=?').get(p.badgeId).n,0);
});

test('two same-name staff retain two identities and correct per-person minutes in both dashboards',async t=>{
 const s=await setup(t);s.job('QA-SAME-NAME');
 for(const id of ['A','B'])s.segment('QA-SAME-'+id,'QA-SAME-NAME','EMP-QA-'+id,'QA same name','2026-09-30T00:00:00.000Z','2026-09-30T01:00:00.000Z',60);
 for(const result of [await s.hours({start_date:'2026-09-30',end_date:'2026-09-30'}),await s.management({start_date:'2026-09-30',end_date:'2026-09-30'})]){
  assert.equal(result.summary.worker_count,2,JSON.stringify(result.summary));
  assert.equal(result.by_worker.length,2);
  assert.deepEqual(result.by_worker.map(x=>x.worker_id).sort(),['EMP-QA-A','EMP-QA-B']);
  assert.deepEqual(result.by_worker.map(x=>x.total_minutes).sort(),[60,60]);
 }
});

test('renaming one staff member does not create a second statistical identity',async t=>{
 const s=await setup(t);s.job('QA-RENAME');
 s.segment('QA-OLD-NAME','QA-RENAME','EMP-QA-SAME-ID','QA old name','2026-09-30T00:00:00.000Z','2026-09-30T01:00:00.000Z',60);
 s.segment('QA-NEW-NAME','QA-RENAME','EMP-QA-SAME-ID','QA new name','2026-09-30T01:00:00.000Z','2026-09-30T02:00:00.000Z',60);
 for(const result of [await s.hours({start_date:'2026-09-30',end_date:'2026-09-30'}),await s.management({start_date:'2026-09-30',end_date:'2026-09-30'})]){
  assert.equal(result.summary.worker_count,1,JSON.stringify(result.summary));
  assert.equal(result.by_worker.length,1);
  assert.equal(result.by_worker[0].worker_id,'EMP-QA-SAME-ID');
  assert.equal(result.by_worker[0].total_minutes,120);
 }
});

test('a closed 23:30 to 00:30 KST segment contributes thirty minutes to each Korean day',async t=>{
 const s=await setup(t);s.job('QA-MIDNIGHT');
 s.segment('QA-MIDNIGHT-SEG','QA-MIDNIGHT','EMP-QA-CROSS','QA midnight','2026-09-30T14:30:00.000Z','2026-09-30T15:30:00.000Z',60);
 for(const date of ['2026-09-30','2026-10-01']){
  for(const result of [await s.hours({start_date:date,end_date:date}),await s.management({start_date:date,end_date:date})]){
   assert.equal(result.summary.total_minutes,30,date+' '+JSON.stringify(result.summary));
  }
 }
 const entire=await s.hours({start_date:'2026-09-30',end_date:'2026-10-01'});
 assert.equal(entire.summary.total_minutes,60);
});

test('an unclosed historical segment is bounded by the requested Korean day end',async t=>{
 const s=await setup(t);s.job('QA-HISTORICAL-OPEN');
 s.DB.raw.prepare("UPDATE v2_ops_jobs SET status='working' WHERE id='QA-HISTORICAL-OPEN'").run();
 s.segment('QA-HISTORICAL-OPEN-SEG','QA-HISTORICAL-OPEN','EMP-QA-STALE','QA historical open','2026-09-29T14:30:00.000Z','',0);
 for(const result of [await s.hours({start_date:'2026-09-29',end_date:'2026-09-29'}),await s.management({start_date:'2026-09-29',end_date:'2026-09-29'})]){
  assert.equal(result.summary.total_minutes,30,JSON.stringify(result.summary));
 }
});
