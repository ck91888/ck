import test from 'node:test';import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';import {handleAttendance,guardAttendance} from '../worker-v2/attendance.js';
function setup(){const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_ENABLED:'true',SOP_UPGRADE_ENABLED:'true',SOP_REQUEST_USER:{id:'M',name:'Manager',role:'manager'}};const call=(a,b={})=>handleAttendance({action:'sop_attendance_'+a,client_req_id:crypto.randomUUID(),...b},env);return {DB,env,call};}
const registration={name:'职员甲',employeeNo:'CK001',department:'bulk'};
test('staff must be registered by manager, unique employee number, no agency or automatic checkin',async()=>{
 const {call,env,DB}=setup();env.SOP_REQUEST_USER.role='kiosk';await assert.rejects(call('employee_register',registration),/权限/);env.SOP_REQUEST_USER.role='manager';
 const request={...registration,client_req_id:crypto.randomUUID()},r=await call('employee_register',request);assert.deepEqual(await call('employee_register',request),r);assert.equal(r.person.badgeId,'EMP-CK001');assert.equal(r.person.personType,'employee');assert.equal(r.person.badgeType,'permanent');assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days').get().n,0);
 await assert.rejects(call('employee_register',{...registration,employeeNo:'ck001'}),/工号已登记/);await assert.rejects(call('employee_register',{...registration,employeeNo:'../bad'}),/工号限/);
 await assert.rejects(call('employee_register',{...registration,employeeNo:'CK002',department:'unapproved'}),/所属部门/);
 const auto=await call('employee_register',{name:'자동생성',department:'office'});assert.match(auto.person.employeeNo,/^E[A-F0-9]{8}$/);
 const off=await handleAttendance({action:'sop_attendance_employee_register'},{SOP_ENVIRONMENT:'production'});assert.equal(off.ok,false);
});
test('same-name staff are separate and daily labor reports and searches do not include staff',async()=>{
 const {call}=setup();const a=(await call('employee_register',registration)).person,b=(await call('employee_register',{...registration,employeeNo:'CK002',department:'office'})).person;
 const daily=(await call('checkin',{name:registration.name,agency:'가온'})).record;await call('checkin',{badge:a.badgeId});await call('checkin',{badge:b.badgeId});
 assert.equal((await call('employee_search',{term:registration.name})).items.length,2);assert.equal((await call('employee_search',{term:'ck001'})).items[0].id,a.id);assert.equal((await call('employee_search',{term:a.badgeId+'|职员甲'})).items[0].id,a.id);
 assert.deepEqual((await call('summary')).items.map(x=>x.record.id),[daily.id]);assert.equal((await call('summary',{scope:'employee'})).items.length,2);assert.equal((await call('summary',{scope:'all'})).items.length,3);assert.equal((await call('search',{name:registration.name})).items.length,1);assert.equal((await call('people')).items.length,0);
});
test('daily staff checkin is idempotent, repeat badge supports later dates and reprint preserves time',async()=>{
 const {call,DB}=setup();const p=(await call('employee_register',registration)).person;
 await assert.rejects(call('checkout',{badge:p.badgeId}),/当天签到/);const r=(await call('checkin',{badge:p.badgeId})).record;
 assert.equal((await call('checkin',{badge:p.badgeId})).record.id,r.id);assert.equal(r.department,'bulk');assert.equal(r.employeeNo,'CK001');assert.equal((await call('print',{id:r.id})).record.inAt,r.inAt);
 await assert.rejects(call('company',{id:r.id,version:2,agency:'가온'}),/职员无需/);const out=await call('checkout',{badge:p.badgeId});assert.equal((await call('checkout',{badge:p.badgeId})).record.outAt,out.record.outAt);assert.equal((await call('checkin',{badge:p.badgeId})).record.outAt,out.record.outAt);
 DB.raw.prepare('UPDATE ck_attendance_days SET day=? WHERE id=?').run('2020-01-01',r.id);const next=await call('checkin',{badge:p.badgeId});assert.notEqual(next.record.id,r.id);assert.equal(next.record.badgeId,p.badgeId);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days').get().n,2);
});
test('employee updates preserve historical attendance and require checkout before disabling',async()=>{
 const {call}=setup();const p=(await call('employee_register',registration)).person,r=(await call('checkin',{badge:p.badgeId})).record;
 await assert.rejects(call('employee_update',{id:p.id,version:1,name:p.name,department:'office',enabled:false}),/尚未签退/);
 const changed=await call('employee_update',{id:p.id,version:1,name:'更正姓名',department:'office',enabled:true});assert.equal(changed.person.version,2);assert.equal(changed.person.badgeId,p.badgeId);
 const saved=(await call('summary',{scope:'employee'})).items[0].record;assert.equal(saved.name,p.name);assert.equal(saved.department,'bulk');await assert.rejects(call('employee_update',{id:p.id,version:1,name:p.name,department:'office',enabled:true}),/已更新/);
 await call('checkout',{id:r.id});await call('employee_update',{id:p.id,version:2,name:'更正姓名',department:'office',enabled:false});assert.equal((await call('employee_search',{term:'CK001'})).items.length,0);await assert.rejects(call('checkin',{badge:p.badgeId}),/未登记/);assert.equal((await call('summary',{scope:'employee'})).items.length,1);
});
test('employee shifts enforce signed-in task participation, actual breaks and personal checkout closure',async()=>{
 const {call,DB,env}=setup();const p=(await call('employee_register',registration)).person;
 DB.raw.exec("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status) VALUES('J','bulk_op','bulk','working')");
 assert.match(await guardAttendance({action:'sop_task_dispatch',workers:[{id:p.badgeId}]},env),/当天签到/);
 const insert=id=>DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,joined_at) VALUES(?,?,?,?)').run(id,'J',p.badgeId,new Date().toISOString());assert.throws(()=>insert('PRE'),/Employee attendance/);
 const r=(await call('checkin',{badge:p.badgeId})).record;insert('W');assert.equal((await call('summary',{scope:'employee'})).items[0].totals.rest,0);
 await call('break_start',{id:r.id});assert.throws(()=>insert('REST'),/Employee attendance/);await call('break_end',{id:r.id});await call('checkout',{badge:p.badgeId});assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' ").get(p.badgeId).n,0);assert.equal(DB.raw.prepare("SELECT status FROM v2_ops_jobs WHERE id='J'").get().status,'working');assert.throws(()=>insert('AFTER'),/Employee attendance/);
});
