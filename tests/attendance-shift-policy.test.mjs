import test from 'node:test';
import assert from 'node:assert/strict';
import {attendanceReport} from '../worker-v2/attendance-time.js';
import {attendanceShift,shiftAttendanceReport,shiftPolicyEnabled} from '../worker-v2/attendance-shift-policy.js';
import {handleAttendance} from '../worker-v2/attendance.js';
import {database} from './d1-adapter.mjs';
const day='2026-10-15',at=t=>day+'T'+t+'+09:00';
const record=(inAt='08:50:00',outAt='18:00:00',breaks=[])=>({id:'SYNTHETIC-ATTENDANCE',badgeId:'DA-SHIFT',day,inAt:at(inAt),outAt:outAt?at(outAt):'',breaks});
const job=(start,end,department='bulk')=>({badgeId:'DA-SHIFT',start:at(start),end:end?at(end):'',department});
const rest=(start,end)=>({start:at(start),end:end?at(end):''});
const sum=r=>r.bulk+r.direct_ship+r.import+r.other+r.rest+r.unassigned+r.conflict;
const equalMinutes=(a,b)=>assert.ok(Math.abs(a-b)<1e-7,`${a} != ${b}`);
function report(r,s=[],asOf=at('19:00:00'),options){const out=shiftAttendanceReport(r,s,asOf,r.day,options);equalMinutes(sum(out),out.presence);for(const k of ['presence','bulk','direct_ship','import','other','rest','unassigned','conflict'])assert.ok(out[k]>=0,k);return out;}
test('Korean minute windows preserve raw punches and map their full endpoint minutes',()=>{
 for(const [input,expected,quota]of [['08:30:00','09:00',90],['09:20:59.999','09:00',90],['09:30:00','10:00',90],['10:20:59.999','10:00',90],['12:30:00','13:00',40],['13:20:59.999','13:00',40]]){const r=record(input),before=structuredClone(r),p=attendanceShift(r);assert.equal(p.rawInAt,r.inAt);assert.equal(p.effectiveInAt,new Date(at(expected+':00')).toISOString());assert.equal(p.shift,expected);assert.equal(p.restTargetMinutes,quota);assert.deepEqual(r,before);}
 for(const input of ['08:29:59.999','09:21:00','09:29:59.999','10:21:00','12:29:59.999','13:21:00']){const r=record(input),p=attendanceShift(r);assert.equal(p.effectiveInAt,r.inAt);assert.equal(p.matched,false);assert.equal(p.restTargetMinutes,Number(input.slice(0,2))<12?90:40);}
});
test('classification uses Korea time even when the timestamp has a different offset',()=>{
 const r=record('09:15:00'),p=attendanceShift({...r,inAt:'2026-10-15T00:15:00.000Z'});assert.equal(p.shift,'09:00');assert.equal(p.effectiveInAt,'2026-10-15T00:00:00.000Z');
});
test('morning quota tops actual merged rest up once using only unassigned minutes',()=>{
 const r=record('08:50:00','18:00:00',[rest('12:00:00','12:45:00'),rest('12:30:00','13:00:00')]),s=[job('09:00:00','12:00:00'),job('13:00:00','17:00:00','direct_ship')],before=JSON.stringify({r,s}),raw=attendanceReport(r,s,at('19:00:00')),out=report(r,s);
 assert.equal(raw.presence,550);assert.equal(out.presence,540);assert.equal(out.bulk,180);assert.equal(out.direct_ship,240);assert.equal(out.rest,90);assert.equal(out.unassigned,30);assert.equal(out.accounting.registeredRestMinutes,60);assert.equal(out.accounting.automaticRestMinutes,30);assert.equal(JSON.stringify({r,s}),before);assert.deepEqual(report(r,s),out);
});
test('late punch within window credits the requested effective start without changing work intervals',()=>{
 const r=record('09:20:00','18:00:00',[rest('12:00:00','13:00:00')]),s=[job('09:30:00','12:00:00'),job('13:00:00','17:00:00','import')],out=report(r,s);assert.equal(out.accounting.rawPresenceMinutes,520);assert.equal(out.presence,540);assert.equal(out.bulk,150);assert.equal(out.import,240);assert.equal(out.accounting.automaticRestMinutes,30);
});
test('afternoon quota is 40 minutes and already longer actual rest is never reduced',()=>{
 let out=report(record('12:40:00','17:00:00',[rest('16:00:00','16:15:00')]),[job('13:00:00','16:00:00')]);assert.equal(out.presence,240);assert.equal(out.rest,40);assert.equal(out.accounting.automaticRestMinutes,25);assert.equal(out.unassigned,20);
 out=report(record('12:40:00','17:00:00',[rest('15:00:00','16:00:00')]),[job('13:00:00','15:00:00')]);assert.equal(out.rest,60);assert.equal(out.accounting.automaticRestMinutes,0);
});
test('no automatic rest before actual checkout and repeating the report never accumulates it',()=>{
 const r=record('08:50:00','',[rest('12:00:00','13:00:00')]),s=[job('09:00:00','12:00:00')],out=report(r,s,at('18:00:00'));assert.equal(out.rest,60);assert.equal(out.accounting.automaticRestMinutes,0);assert.equal(out.accounting.status,'pending_checkout');const closed=report({...r,outAt:at('18:00:00')},s);assert.equal(closed.rest,90);assert.deepEqual(report({...r,outAt:at('18:00:00')},s),closed);
});
test('early departure converts only available unassigned time without negative or reduced work',()=>{
 const out=report(record('12:50:00','13:10:00'),[job('13:00:00','13:08:00')]);assert.equal(out.presence,10);assert.equal(out.bulk,8);assert.equal(out.rest,2);assert.equal(out.unassigned,0);assert.equal(out.accounting.restShortfallMinutes,38);assert.equal(out.accounting.status,'insufficient_unassigned');
 const empty=report(record('08:50:00','08:55:00'));assert.equal(empty.presence,0);assert.equal(empty.rest,0);assert.equal(empty.accounting.automaticRestMinutes,0);
});
test('real work before effective start is preserved and explicitly flagged instead of silently clipped',()=>{
 const r=record('08:30:00','10:00:00'),s=[job('08:40:00','09:10:00')],before=structuredClone(s),out=report(r,s);assert.equal(out.bulk,30);assert.equal(out.accounting.scheduledPresenceMinutes,60);assert.equal(out.accounting.retainedOutsideShiftMinutes,20);assert.equal(out.presence,80);assert.equal(out.rest,50);assert.equal(out.accounting.restShortfallMinutes,40);assert.ok(out.flags.some(x=>x.includes('原时段保留')));assert.deepEqual(s,before);
});
test('same-department overlaps count once; conflicting work or rest does not get automatic quota guessing',()=>{
 let out=report(record('08:50:00','12:00:00'),[job('09:00:00','10:00:00'),job('09:30:00','10:30:00')]);assert.equal(out.bulk,90);assert.equal(out.rest,90);assert.equal(out.unassigned,0);
 out=report(record('08:50:00','12:00:00'),[job('09:00:00','11:00:00'),job('10:00:00','12:00:00','import')]);assert.equal(out.conflict,60);assert.equal(out.accounting.automaticRestMinutes,0);assert.equal(out.accounting.status,'work_verification_pending');
 out=report(record('08:50:00','18:00:00'),[],undefined,{needsWorkVerification:true});assert.equal(out.accounting.automaticRestMinutes,0);assert.equal(out.accounting.status,'work_verification_pending');
});
test('outside-window actual start receives morning or afternoon rest quota after checkout',()=>{
 const r=record('11:05:00','18:00:00',[rest('12:00:00','12:20:00')]),out=report(r,[job('13:00:00','17:00:00')]);assert.equal(out.presence,415);assert.equal(out.rest,90);assert.equal(out.accounting.restTargetMinutes,90);assert.equal(out.accounting.automaticRestMinutes,70);assert.equal(out.unassigned,85);assert.equal(out.accounting.status,'settled');
 for(const [time,quota]of [['11:59:59.999',90],['12:00:00',40],['12:29:59.999',40],['13:21:00',40]]){const x=report(record(time,'18:00:00'));assert.equal(x.accounting.restTargetMinutes,quota,time);assert.equal(x.accounting.automaticRestMinutes,quota,time);assert.equal(x.accounting.effectiveInAt,x.accounting.rawInAt,time);}
});
test('cross-day unconfirmed or confirmed shifts stay flagged without automatic adjustment',()=>{
 const r=record('08:50:00',''),asOf='2026-10-16T01:00:00+09:00',raw=attendanceReport(r,[],asOf),out=report(r,[],asOf);assert.equal(out.presence,raw.presence);assert.equal(out.rest,raw.rest);assert.equal(out.accounting.applied,false);assert.equal(out.accounting.status,'cross_day_pending');
 const closed={...r,outAt:'2026-10-16T01:00:00+09:00'},finished=report(closed,[],closed.outAt);assert.equal(finished.accounting.automaticRestMinutes,0);assert.equal(finished.accounting.status,'cross_day_pending');
});
test('staging opt-in recalculates every valid historical date without requiring or honoring a since-date',()=>{
 const env={SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_SHIFT_ACCOUNTING:'true',SOP_ATTENDANCE_SHIFT_FROM:'2099-01-01'};
 for(const date of ['2024-01-01','2026-10-14',day])assert.equal(shiftPolicyEnabled(env,date),true,date);
 for(const [change,date]of [[{SOP_ENVIRONMENT:'production'},day],[{SOP_ATTENDANCE_SHIFT_ACCOUNTING:'false'},day],[{SOP_ATTENDANCE_SHIFT_ACCOUNTING:undefined},day],[{},'2026-02-30']])assert.equal(shiftPolicyEnabled({...env,...change},date),false);
});
async function fixture(t,start='08:50:00',date=day){
 const local=t0=>date+'T'+t0+'+09:00';
 const DB=database();t.after(()=>DB.raw.close());t.mock.timers.enable({apis:['Date'],now:Date.parse(local(start))});const env={DB,SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_ENABLED:'true',SOP_ATTENDANCE_SHIFT_ACCOUNTING:'true',SOP_REQUEST_USER:{id:'SYNTHETIC-SHIFT-MANAGER',name:'合成考勤管理员',role:'manager',scope:'office'}};
 const call=(action,data={})=>handleAttendance({action:'sop_attendance_'+action,client_req_id:crypto.randomUUID(),...data},env),set=t0=>t.mock.timers.setTime(Date.parse(local(t0))),snapshot=()=>Object.fromEntries(['ck_attendance_days','ck_attendance_breaks','v2_ops_job_workers'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));return {DB,env,call,set,snapshot};
}
test('real daily and employee summaries share policy while checkout/recheck/reopen preserve raw evidence and single top-up',async t=>{
 const f=await fixture(t),daily=(await f.call('checkin',{name:'合成日当',agency:'가온'})).record,person=(await f.call('employee_register',{employeeNo:'SHIFT-E1',name:'合成职员',department:'bulk'})).person,employee=(await f.call('checkin',{badge:person.badgeId})).record;
 for(const r of [daily,employee])f.DB.raw.prepare('INSERT INTO ck_attendance_breaks VALUES(?,?,?,?,?,?)').run('BREAK-'+r.id,r.id,at('12:00:00'),at('13:00:00'),'[]','SYNTHETIC');f.set('18:00:00');await f.call('checkout',{id:daily.id});await f.call('checkout',{id:employee.id});const before=f.snapshot(),dailyReport=await f.call('summary'),employeeReport=await f.call('summary',{scope:'employee'});assert.deepEqual(f.snapshot(),before);
 for(const x of [dailyReport.items[0],employeeReport.items[0]]){assert.equal(x.record.inAt,new Date(at('08:50:00')).toISOString());assert.equal(x.totals.presence,540);assert.equal(x.totals.rest,90);assert.equal(x.totals.accounting.registeredRestMinutes,60);assert.equal(x.totals.accounting.automaticRestMinutes,30);}
 assert.equal((await f.call('checkin',{badge:person.badgeId})).record.outAt,employeeReport.items[0].record.outAt);assert.equal((await f.call('checkout',{id:daily.id})).existing,true);assert.equal((await f.call('summary')).items[0].totals.rest,90);assert.deepEqual(f.snapshot(),before);
 const detail=await f.call('employee_work_detail',{person_id:person.id,date:day});assert.equal(detail.attendanceAccounting.automaticRestMinutes,30);assert.equal(detail.record.inAt,employee.inAt);const current=(await f.call('summary')).items[0].record;await f.call('correct',{id:daily.id,version:current.version,inAt:daily.inAt,outAt:'',reason:'合成核实撤回签退'});const opened=(await f.call('summary')).items[0];assert.equal(opened.totals.rest,60);assert.equal(opened.totals.accounting.automaticRestMinutes,0);f.set('18:10:00');await f.call('checkout',{id:daily.id});const closed=(await f.call('summary')).items[0];assert.equal(closed.totals.rest,90);assert.equal(closed.totals.accounting.automaticRestMinutes,30);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_attendance_breaks').get().n,2);assert.equal(f.DB.raw.prepare('SELECT signed_in FROM ck_attendance_days WHERE id=?').get(daily.id).signed_in,daily.inAt);
});
test('effective backdating never weakens physical employee check-in guard',async t=>{
 const f=await fixture(t,'09:20:00'),p=(await f.call('employee_register',{employeeNo:'SHIFT-LATE',name:'合成晚签到职员',department:'bulk'})).person,r=(await f.call('checkin',{badge:p.badgeId})).record;f.DB.raw.exec("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status) VALUES('SHIFT-JOB','bulk_op','bulk','working')");assert.throws(()=>f.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)').run('INVALID-EARLY-WORK','SHIFT-JOB',r.badgeId,r.name,at('09:10:00')),/attendance required/i);const row=(await f.call('summary',{scope:'employee'})).items[0];assert.equal(row.totals.accounting.effectiveInAt,'2026-10-15T00:00:00.000Z');assert.equal(row.record.inAt,'2026-10-15T00:20:00.000Z');
});
test('historical daily and employee reads recalculate morning and noon outside windows without rewriting source rows',async t=>{
 const date='2024-06-07',stamp=time=>new Date(date+'T'+time+'+09:00').toISOString(),f=await fixture(t,'11:15:00',date);f.env.SOP_ATTENDANCE_SHIFT_FROM='2099-01-01';
 const daily=(await f.call('checkin',{name:'合成历史日当',agency:'가온'})).record;
 const morningPerson=(await f.call('employee_register',{employeeNo:'HISTORY-AM',name:'合成历史上午职员',department:'bulk'})).person,morning=(await f.call('checkin',{badge:morningPerson.badgeId})).record;
 f.set('12:00:00');const afternoonPerson=(await f.call('employee_register',{employeeNo:'HISTORY-PM',name:'合成历史下午职员',department:'bulk'})).person,afternoon=(await f.call('checkin',{badge:afternoonPerson.badgeId})).record;
 for(const r of [daily,morning])f.DB.raw.prepare('INSERT INTO ck_attendance_breaks VALUES(?,?,?,?,?,?)').run('HISTORY-'+r.id,r.id,stamp('12:00:00'),stamp('12:30:00'),'[]','SYNTHETIC');
 f.DB.raw.prepare('INSERT INTO ck_attendance_breaks VALUES(?,?,?,?,?,?)').run('HISTORY-'+afternoon.id,afternoon.id,stamp('13:00:00'),stamp('13:10:00'),'[]','SYNTHETIC');
 f.set('18:00:00');for(const r of [daily,morning,afternoon])await f.call('checkout',{id:r.id});const before=f.snapshot();
 const dailyReport=await f.call('summary',{date}),employeeReport=await f.call('summary',{date,scope:'employee'});assert.equal(dailyReport.attendancePolicy.dateScope,'all');assert.equal(dailyReport.attendancePolicy.restCutoff,'12:00');
 for(const x of [dailyReport.items[0],employeeReport.items.find(x=>x.record.id===morning.id)]){assert.equal(x.record.inAt,stamp('11:15:00'));assert.equal(x.totals.accounting.effectiveInAt,x.record.inAt);assert.equal(x.totals.presence,405);assert.equal(x.totals.rest,90);assert.equal(x.totals.accounting.registeredRestMinutes,30);assert.equal(x.totals.accounting.automaticRestMinutes,60);}
 const pm=employeeReport.items.find(x=>x.record.id===afternoon.id);assert.equal(pm.record.inAt,stamp('12:00:00'));assert.equal(pm.totals.rest,40);assert.equal(pm.totals.accounting.registeredRestMinutes,10);assert.equal(pm.totals.accounting.automaticRestMinutes,30);
 const detail=await f.call('employee_work_detail',{person_id:afternoonPerson.id,date});assert.deepEqual(detail.attendanceAccounting,pm.totals.accounting);assert.deepEqual(f.snapshot(),before);
});
