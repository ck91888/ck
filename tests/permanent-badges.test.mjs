import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {ATTENDANCE_SCHEMA,handleAttendance,guardAttendance} from '../worker-v2/attendance.js';
import worker from '../worker-v2/index.js';
import {digest} from '../worker-v2/access-control.js';

function setup(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_ENABLED:'true',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_REQUEST_USER:{id:'QA-MANAGER',name:'Synthetic manager',role:'manager'}};
 const call=(suffix,data={})=>handleAttendance({action:'sop_attendance_'+suffix,client_req_id:crypto.randomUUID(),...data},env);
 const register=(data={})=>call('register',{name:'Synthetic permanent',agency:'가온',...data}).then(r=>r.person);
 const update=(p,data={})=>call('permanent_update',{id:p.id,version:p.version,name:p.name,agency:p.agency,enabled:p.enabled,reason:'Synthetic verified change',...data});
 const api=async(action,data={})=>{const r=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);return r.json();};
 const count=table=>DB.raw.prepare('SELECT COUNT(*) n FROM '+table).get().n;
 return {DB,env,call,register,update,api,count};
}
const sameIdentity=(a,b)=>{assert.equal(a.id,b.id);assert.equal(a.badgeId,b.badgeId);};

test('rename, disable and restore preserve separate same-name identities and historical attendance/work snapshots',async t=>{
 const s=setup(t),a=await s.register({name:'Synthetic duplicate',badge:'DAF-QA-A'}),b=await s.register({name:'Synthetic duplicate',badge:'DAF-QA-B'});
 const day=(await s.call('checkin',{badge:a.badgeId,agency:a.agency})).record;
 s.DB.raw.exec("INSERT INTO v2_ops_jobs(id,job_type,status) VALUES('QA-HISTORY','inventory','working')");
 s.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)').run('QA-SEG','QA-HISTORY',a.badgeId,a.name,day.inAt);
 const snapshot={day:{...s.DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(day.id)},segment:{...s.DB.raw.prepare("SELECT * FROM v2_ops_job_workers WHERE id='QA-SEG'").get()}};
 const renamed=(await s.update(a,{name:b.name,agency:'포레인'})).person;sameIdentity(renamed,a);assert.equal(renamed.version,2);
 const disabled=(await s.update(renamed,{name:'Synthetic revised',enabled:false})).person;sameIdentity(disabled,a);
 assert.equal((await s.call('lookup',{badge:a.badgeId+'|Synthetic stale QR name'})).person.name,'Synthetic revised');
 assert.deepEqual({...s.DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(day.id)},snapshot.day);
 assert.deepEqual({...s.DB.raw.prepare("SELECT * FROM v2_ops_job_workers WHERE id='QA-SEG'").get()},snapshot.segment);
 let items=(await s.call('people')).items;assert.equal(items.length,2);assert.equal(items.find(p=>p.id===b.id).version,1);assert.equal(items.find(p=>p.id===b.id).enabled,true);
 const restored=(await s.update(disabled,{enabled:true})).person;sameIdentity(restored,a);assert.equal(restored.version,4);
 assert.equal(s.count('ck_attendance_people'),2);assert.equal(s.count('ck_attendance_days'),1);
 const events=s.DB.raw.prepare("SELECT * FROM ck_attendance_events WHERE record_id=? ORDER BY version").all(a.id);assert.equal(events.length,4);
 for(const event of events.slice(1)){assert.equal(event.actor,'QA-MANAGER');assert.equal(JSON.parse(event.after_json).change_reason,'Synthetic verified change');assert.equal(JSON.parse(event.after_json).audit_actor_name,'Synthetic manager');assert.ok(event.at);}
});

test('permanent edits validate reason, fields, version and badge family; role checks also reject cached replay',async t=>{
 const s=setup(t),p=await s.register(),body={client_req_id:crypto.randomUUID(),name:'Synthetic new name'};
 for(const data of [{reason:''},{reason:'  '},{reason:'a'.repeat(501)},{reason:'bad\u202e'},{name:'bad|name'},{agency:'invalid'},{enabled:'false'},{version:String(p.version)},{version:0}])await assert.rejects(s.update(p,data));
 assert.equal(s.count('ck_attendance_events'),1);
 const changed=await s.update(p,body);assert.equal(changed.person.version,2);assert.deepEqual(await s.update(p,body),changed);
 for(const role of ['dispatcher','reviewer','viewer','kiosk']){s.env.SOP_REQUEST_USER.role=role;await assert.rejects(s.call('people'),/权限/);await assert.rejects(s.update(p,body),/权限/);}
 s.env.SOP_REQUEST_USER.role='manager';
 const daily=(await s.call('checkin',{name:'Synthetic daily',agency:'가온'})).record;
 const employee=(await s.call('employee_register',{name:'Synthetic employee',employeeNo:'QA-EXCLUDED',department:'bulk'})).person;
 for(const id of [daily.personId,employee.id,'unknown'])await assert.rejects(s.update(p,{id}),/长期日当工牌不存在/);
 assert.equal((await s.call('people')).items.length,1);
});

test('concurrent profile edits have one committed version and audit, stale updates cannot overwrite',async t=>{
 const s=setup(t),p=await s.register();
 const results=await Promise.allSettled([s.update(p,{name:'Synthetic A'}),s.update(p,{name:'Synthetic B'})]);
 assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(results.filter(r=>r.status==='rejected').length,1);
 const current=(await s.call('people')).items[0];assert.equal(current.version,2);assert.equal(s.count('ck_attendance_events'),2);
 await assert.rejects(s.update(p,{enabled:false}),/更新/);assert.deepEqual((await s.call('people')).items[0],current);
});

test('disable blocks checkin and new assignments but permits retaining active crew, completion and checkout',async t=>{
 const s=setup(t),p=await s.register(),day=(await s.call('checkin',{badge:p.badgeId,agency:p.agency})).record;
 const started=await s.api('sop_native_start',{payload:{action:'v2_ops_job_start',job_type:'inventory',flow_stage:'internal',client_req_id:crypto.randomUUID()},labor_department:'bulk',workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId,estimated_minutes:5});assert.equal(started.ok,true,started.error);
 const disabled=(await s.update(p,{enabled:false})).person;
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=? AND left_at=''").get(p.badgeId).n,1);
 await assert.rejects(s.call('checkin',{badge:p.badgeId,agency:p.agency}),/停用/);
 assert.match(await guardAttendance({action:'sop_task_dispatch',workers:[{id:p.badgeId}]},s.env),/停用/);
 assert.equal(await guardAttendance({action:'sop_native_people',job_id:started.job_id,workers:[{id:p.badgeId}]},s.env),null);
 const retained=await s.api('sop_native_people',{job_id:started.job_id,revision:1,workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId});assert.equal(retained.ok,true,retained.error);
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=?').get(p.badgeId).n,1);
 assert.equal((await s.call('lookup',{badge:p.badgeId})).person.enabled,false);
 const finish=await s.api('v2_ops_job_finish',{job_id:started.job_id,worker_id:p.badgeId,complete_job:true});assert.equal(finish.ok,true,finish.error);
 const out=await s.call('checkout',{badge:p.badgeId});assert.equal(out.record.id,day.id);assert.ok(out.record.outAt);
 const restored=(await s.update(disabled,{enabled:true})).person;sameIdentity(restored,p);
 assert.equal((await s.call('checkin',{badge:p.badgeId,agency:p.agency})).record.id,day.id);
 assert.equal(s.count('ck_attendance_days'),1);
});

test('SQL guards reject checkin races and new/reopened/reassigned segments without preventing closure',async t=>{
 const s=setup(t),p=await s.register(),batch=s.DB.batch.bind(s.DB);let injected=false;
 s.DB.batch=async statements=>{if(!injected&&statements[0]?.args[4]==='sop_attendance_checkin'){injected=true;await s.update(p,{enabled:false});}return batch(statements);};
 await assert.rejects(s.call('checkin',{badge:p.badgeId,agency:p.agency}),/变化|冲突|disabled/);assert.equal(injected,true);assert.equal(s.count('ck_attendance_days'),0);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action='sop_attendance_checkin'").get().n,0);
 s.DB.batch=batch;let current=(await s.call('people')).items[0];current=(await s.update(current,{enabled:true})).person;
 const day=(await s.call('checkin',{badge:p.badgeId,agency:p.agency})).record;
 s.DB.raw.exec("INSERT INTO v2_ops_jobs(id,job_type,status) VALUES('QA-SQL','inventory','working'),('QA-SQL-OTHER','inventory','working')");
 s.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,joined_at) VALUES(?,?,?,?)').run('QA-OLD','QA-SQL',p.badgeId,day.inAt);
 assert.equal(await guardAttendance({action:'sop_native_people',job_id:'QA-SQL',workers:[{id:p.badgeId}]},s.env),null);
 await s.update(current,{enabled:false});
 assert.throws(()=>s.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,joined_at) VALUES(?,?,?,?)').run('QA-NEW','QA-SQL',p.badgeId,day.inAt),/disabled/);
 assert.throws(()=>s.DB.raw.exec("UPDATE v2_ops_job_workers SET job_id='QA-SQL-OTHER' WHERE id='QA-OLD'"),/disabled/);
 s.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=? WHERE id='QA-OLD'").run(day.inAt);
 assert.throws(()=>s.DB.raw.exec("UPDATE v2_ops_job_workers SET left_at='' WHERE id='QA-OLD'"),/disabled/);
 assert.equal(s.DB.raw.prepare("SELECT left_at FROM v2_ops_job_workers WHERE id='QA-OLD'").get().left_at,day.inAt);
});

test('disable after dispatch preflight rolls back native start and its audit',async t=>{
 const s=setup(t),p=await s.register();await s.call('checkin',{badge:p.badgeId,agency:p.agency});
 const batch=s.DB.batch.bind(s.DB);let injected=false;
 s.DB.batch=async statements=>{if(!injected&&statements.some(st=>/^INSERT INTO v2_ops_job_workers/i.test(st.sql))){injected=true;await s.update(p,{enabled:false});}return batch(statements);};
 const started=await s.api('sop_native_start',{payload:{action:'v2_ops_job_start',job_type:'inventory',flow_stage:'internal',client_req_id:crypto.randomUUID()},labor_department:'bulk',workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId,estimated_minutes:5});
 assert.equal(injected,true);assert.equal(started.ok,false);assert.equal(s.count('v2_ops_job_workers'),0);assert.equal(s.count('v2_ops_jobs'),0);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM sop_events WHERE action='sop_native_start'").get().n,0);
});

test('disabled worker may end rest and check out without restarting a task',async t=>{
 const s=setup(t),p=await s.register(),day=(await s.call('checkin',{badge:p.badgeId,agency:p.agency})).record;
 s.DB.raw.exec("INSERT INTO v2_ops_jobs(id,job_type,status) VALUES('QA-REST','inventory','working')");
 s.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,joined_at) VALUES(?,?,?,?)').run('QA-REST-SEG','QA-REST',p.badgeId,day.inAt);
 await s.call('break_start',{id:day.id});await s.update(p,{enabled:false});
 const ended=await s.call('break_end',{id:day.id});assert.deepEqual(ended.resumedJobs,[]);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_breaks WHERE ended_at=''").get().n,0);
 assert.equal(s.count('v2_ops_job_workers'),1);assert.ok((await s.call('checkout',{badge:p.badgeId})).record.outAt);
});

test('pre-existing permanent badges acquire audited versions without changing original people or punches',async t=>{
 const s=setup(t);for(const sql of ATTENDANCE_SCHEMA)s.DB.raw.exec(sql);
 s.DB.raw.exec("INSERT INTO ck_attendance_people VALUES('QA-LEGACY','DAF-QA-LEGACY','Synthetic legacy','가온','permanent',0,'2020-01-01T00:00:00.000Z')");
 const before={...s.DB.raw.prepare("SELECT * FROM ck_attendance_people WHERE id='QA-LEGACY'").get()};
 const p=(await s.call('people')).items[0];assert.equal(p.version,1);assert.equal(p.enabled,false);assert.deepEqual({...s.DB.raw.prepare("SELECT * FROM ck_attendance_people WHERE id='QA-LEGACY'").get()},before);
 const restored=(await s.update(p,{enabled:true})).person;sameIdentity(restored,p);assert.equal(restored.version,2);
 const checked=(await s.call('checkin',{badge:restored.badgeId+'|Synthetic old label',agency:restored.agency})).record;assert.equal(checked.personId,p.id);assert.equal(s.count('ck_attendance_people'),1);
});

test('real office route requires current confirmed operator context and records its name',async t=>{
 const s=setup(t);s.env.SOP_ACCESS_CONTROL='true';s.env.SOP_ADMIN_CODE_SHA256=await digest('synthetic-badge-admin-only');delete s.env.SOP_PUBLIC_TEST_ACCESS;
 let cookie='';const request=async(action,data={},context='')=>{const response=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:'https://synthetic.invalid',Cookie:cookie,'X-CK-Operation-Context':context},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),s.env);if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];return {status:response.status,...await response.json()};};
 assert.equal((await request('sop_login',{sop_key:'synthetic-badge-admin-only'})).ok,true);
 const p=(await request('sop_attendance_register',{name:'Synthetic secured',agency:'가온'})).person;
 const body={id:p.id,version:p.version,name:p.name,agency:p.agency,enabled:false,reason:'Synthetic secure change'};
 const context=(await request('sop_identity')).user.operation_context.id;
 assert.equal((await request('sop_attendance_permanent_update',body,context)).status,409);
 const confirmed=await request('sop_operator_confirm',{name:'Synthetic confirmed operator'},context),fresh=confirmed.user.operation_context.id;
 assert.equal((await request('sop_attendance_permanent_update',body,context)).status,409);
 assert.equal((await request('sop_attendance_permanent_update',body,fresh)).ok,true);
 const event=s.DB.raw.prepare("SELECT actor,after_json FROM ck_attendance_events WHERE record_id=? AND version=2").get(p.id);
 assert.equal(event.actor,'ck-office-admin');assert.equal(JSON.parse(event.after_json).audit_actor_name,'Synthetic confirmed operator');
});
