import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {handleAttendance,guardAttendance} from '../worker-v2/attendance.js';
import {kstDay} from '../worker-v2/attendance-time.js';

function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_ENABLED:'true',SOP_UPGRADE_ENABLED:'true',SOP_REQUEST_USER:{id:'SYNTHETIC-MANAGER',name:'Synthetic manager',role:'manager'}};
 const call=(action,data={})=>handleAttendance({action:'sop_attendance_'+action,client_req_id:crypto.randomUUID(),...data},env);
 return {DB,env,call};
}
test('dispatch identity diagnostics preserve strict exact-ID matching and expose only submitted identity',async t=>{
 const {DB,env,call}=fixture(t),record=(await call('checkin',{name:'合成人员甲',agency:'가온'})).record;
 const other=(await call('checkin',{name:'Private other synthetic',agency:'포레인'})).record;
 const guard=(id,action='sop_native_start')=>guardAttendance({action,payload:{action:'v2_pick_job_start_by_docs'},workers:[{id,name:record.name}]},env);
 assert.equal(await guard(record.badgeId),null);
 const count=DB.raw.prepare('SELECT count(*) n FROM ck_attendance_events').get().n;
 for(const bad of [record.badgeId+'\u200b',record.badgeId+'\u2060',record.badgeId+'|'+record.name,record.badgeId.replace(/-/,'-%2D')]){
  const message=await guard(bad);
  assert.match(message,/未找到当天有效签到/);
  assert.ok(message.includes(kstDay(new Date().toISOString())));
  assert.ok(message.includes('工牌 / 명찰: '));
  assert.equal(message.includes(other.badgeId),false);
  assert.equal(message.includes(other.name),false);
 }
 assert.match(await guard(record.badgeId+'\u200b'),/\\u\{200B\}/);
 assert.match(await guard(record.badgeId+'\u2060'),/\\u\{2060\}/);
 for(const action of ['sop_task_dispatch','v2_pick_job_start_by_docs','v2_ops_job_start'])assert.match(await guard(record.badgeId+'-UNKNOWN',action),/未找到当天有效签到/);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_events').get().n,count);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);
});
test('same-name distinct badges, rest, checkout, case and cross-day protection remain independent',async t=>{
 const {DB,env,call}=fixture(t),a=(await call('checkin',{name:'Synthetic same name',agency:'가온'})).record;
 const b=(await call('checkin',{name:a.name,agency:'포레인',confirm_distinct_person:true,reason:'Synthetic distinct identity'})).record;
 const p=(await call('register',{name:'Synthetic fixed',agency:'가온',badge:'DAF-Synthetic-Ab12'})).person;
 await call('checkin',{badge:p.badgeId,agency:p.agency});
 const guard=r=>guardAttendance({action:'sop_native_start',payload:{action:'v2_pick_job_start_by_docs'},workers:[{id:r.badgeId,name:r.name}]},env);
 assert.equal(await guard(a),null);assert.equal(await guard(b),null);assert.equal(await guard(p),null);
 assert.match(await guard({...p,badgeId:'DAF-Synthetic-ab12'}),/未找到当天有效签到/);
 await call('break_start',{id:a.id});assert.match(await guard(a),/休息/);assert.equal(await guard(b),null);
 await call('break_end',{id:a.id});await call('checkout',{id:a.id});assert.match(await guard(a),/签退/);
 DB.raw.prepare("UPDATE ck_attendance_days SET day='2000-01-01' WHERE id=?").run(b.id);
 assert.match(await guard(b),/未找到当天有效签到/);
 assert.equal(kstDay('2026-10-11T14:59:59.999Z'),'2026-10-11');assert.equal(kstDay('2026-10-11T15:00:00.000Z'),'2026-10-12');
});
