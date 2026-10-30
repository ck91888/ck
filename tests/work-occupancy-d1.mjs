// Actual local workerd D1; no Cloudflare account or remote database.
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {handleAttendance,guardAttendance} from '../worker-v2/attendance.js';
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_REQUEST_USER:{id:'SYNTHETIC-D1-MANAGER',name:'Synthetic manager',role:'manager'}};
const request=(action,data={})=>({action:'sop_attendance_'+action,client_req_id:crypto.randomUUID(),...data});
const r=(await handleAttendance(request('checkin',{name:'Synthetic D1 worker',agency:'동인천'}),env)).record;
DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status) VALUES('SYNTHETIC-D1-JOB','change_order','direct_ship','paused')").run();
DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,minutes_worked) VALUES('SYNTHETIC-D1-CLOCK','SYNTHETIC-D1-JOB',?,?,?,7.5)").run(r.badgeId,r.name,new Date().toISOString());
const d1=await localD1(DB.raw);env.DB=d1;
try{
 const snapshot=async()=>Promise.all(['v2_ops_jobs','v2_ops_job_workers','ck_attendance_days','ck_attendance_breaks'].map(async table=>(await d1.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()).results));
 const before=await snapshot(),item=(await handleAttendance(request('summary',{scope:'all'}),env)).items[0];assert.equal(item.status,'occupancy_conflict');assert.match(item.occupancyWarnings[0].reason,/暂停任务/);
 const start={action:'sop_native_start',payload:{action:'v2_ops_job_start',job_type:'change_order'},workers:[{id:r.badgeId,name:r.name}]};assert.match(await guardAttendance(start,env),/另一任务/);assert.deepEqual(await snapshot(),before);
 const end=new Date().toISOString();await d1.prepare("UPDATE v2_ops_jobs SET status='completed',finished_at=? WHERE id='SYNTHETIC-D1-JOB'").bind(end).run();assert.equal(await guardAttendance(start,env),null);
 const clock=await d1.prepare('SELECT * FROM v2_ops_job_workers').first(),audit=await d1.prepare('SELECT * FROM ck_completed_clock_repairs').first();assert.equal(clock.left_at,end);assert.equal(audit.closed_at,end);assert.equal(audit.previous_minutes,7.5);
 assert.equal((await handleAttendance(request('summary',{scope:'all'}),env)).items[0].status,'unassigned');console.log('PASS local workerd D1: raw occupancy visible; paused guard unchanged; read/rejection writes no business rows; reliable completed-end audit repair preserved.');
}finally{await d1.close();DB.raw.close();}
