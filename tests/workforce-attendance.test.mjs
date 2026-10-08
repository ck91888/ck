// Isolated regression coverage: real attendance/Worker handlers and transactional SQLite.
// No live services, credentials, or production data are used.
import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {handleAttendance,guardAttendance} from '../worker-v2/attendance.js';
import {ensureCrewBorrow,crewAvailability} from '../worker-v2/crew-borrow.js';
import {digest} from '../worker-v2/access-control.js';

async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_REQUEST_USER:{id:'QA-WORKFORCE-MANAGER',name:'Isolated Manager',role:'manager'}};
 const call=(suffix,data={})=>handleAttendance({action:'sop_attendance_'+suffix,client_req_id:crypto.randomUUID(),...data},env);
 await call('config');await ensureCrewBorrow(env);
 const person=async(name='Isolated daily')=>(await call('checkin',{name,agency:'가온'})).record;
 const fixed=async(name='Isolated fixed')=>{const p=(await call('register',{name,agency:'가온'})).person;return (await call('checkin',{badge:p.badgeId,agency:'가온'})).record;};
 const summary=async(data={})=>call('summary',data);
 const item=async(id,deleted=false)=>(await summary())[deleted?'deletedItems':'items'].find(x=>x.record.id===id);
 const stored=id=>DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(id);
 const snapshot=()=>Object.fromEntries(['v2_ops_jobs','v2_ops_job_workers','ck_attendance_breaks','ck_crew_borrows'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 const voidDay=(r,data={})=>call('void',{id:r.id,version:r.version,reason:'Isolated mistaken registration',...data});
 const restore=(r,data={})=>call('restore',{id:r.id,version:r.version,reason:'Isolated verified restoration',...data});
 function job(id='QA-JOB',status='working'){
  DB.raw.prepare('INSERT INTO v2_ops_jobs(id,job_type,biz_class,status) VALUES(?,?,?,?)').run(id,'pack_direct','direct_ship',status);return id;
 }
 function segment(r,{id=crypto.randomUUID(),jobId='QA-JOB',start=new Date().toISOString(),end='',minutes=0}={}){
  DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run(id,jobId,r.badgeId,r.name,start,end,minutes);return id;
 }
 function breakRow(r,{id=crypto.randomUUID(),end=''}={}){
  DB.raw.prepare('INSERT INTO ck_attendance_breaks(id,attendance_id,started_at,ended_at,job_ids_json,actor) VALUES(?,?,?,?,?,?)').run(id,r.id,r.inAt,end,'[]','QA-WORKFORCE-MANAGER');return id;
 }
 function arm(action,inject){
  const batch=DB.batch.bind(DB);let fired=false;
  DB.batch=async statements=>{if(!fired&&statements.some(s=>s.sql.includes('ck_attendance_events')&&s.args.includes('sop_attendance_'+action))){fired=true;await inject();}return batch(statements);};
  return ()=>assert.equal(fired,true,'The interleaving must actually run');
 }
 async function loan(r,status='return_pending'){
  const id='QA-LOAN-'+crypto.randomUUID(),jobId=job('QA-SOURCE-'+crypto.randomUUID()),seg=segment(r,{jobId});
  DB.raw.prepare(`INSERT INTO ck_crew_borrows(id,request_id,actor_id,actor_name,destination_job_id,worker_id,worker_name,source_job_id,source_segment_id,source_kind,source_revision,source_owner_id,source_day,status,borrowed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(id,crypto.randomUUID(),'QA-WORKFORCE-MANAGER','Isolated Manager','QA-DESTINATION',r.badgeId,r.name,jobId,seg,'legacy',0,'',r.day,status,r.inAt);
  // Preserve a genuine historical completed source while testing the pending loan independently.
  const yesterday=new Date(Date.parse(r.day+'T00:00:00+09:00')-86400000).toISOString();
  DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=?,left_at=?,minutes_worked=0 WHERE id=?').run(yesterday,yesterday,seg);
  return id;
 }
 return {DB,env,call,person,fixed,summary,item,stored,snapshot,voidDay,restore,job,segment,breakRow,arm,loan};
}
const error=/权限|更新|变化|冲突|部门|职员|日当|签到|删除|恢复|作业|休息|借调|登记|reason|attendance|changed|deleted|restore|work|break|loan/i;

for(const role of ['manager','dispatcher'])test(role+' can set and clear only the daily management label',async t=>{
 const f=await fixture(t),r=await f.person();f.env.SOP_REQUEST_USER.role=role;
 let row=r;for(const department of ['bulk','direct_ship','import','']){
  row=(await f.call('department',{id:r.id,version:row.version,department})).record;
  assert.equal(row.managementDepartment,department);assert.equal(row.badgeId,r.badgeId);assert.equal(row.inAt,r.inAt);
  assert.equal((await f.item(r.id)).record.managementDepartment,department);
 }
 assert.equal(row.version,r.version+4);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days').get().n,1);
});

test('management labels do not relabel actual work or mutate clocks, breaks, jobs, or totals',async t=>{
 const f=await fixture(t),r=await f.person();f.job();
 const begin=r.day+'T08:00:00+09:00',end=r.day+'T09:00:00+09:00';
 f.DB.raw.prepare('UPDATE ck_attendance_days SET signed_in=?,signed_out=? WHERE id=?').run(begin,end,r.id);
 f.segment(r,{start:begin,end,minutes:60});
 const before=await f.item(r.id),work=f.snapshot();
 const result=await f.call('department',{id:r.id,version:r.version,department:'import'}),after=await f.item(r.id);
 assert.equal(result.record.managementDepartment,'import');assert.equal(after.totals.direct_ship,60);assert.equal(after.totals.import,0);
 assert.deepEqual(after.totals,before.totals);assert.deepEqual(after.segments,before.segments);assert.deepEqual(f.snapshot(),work);
 assert.equal(after.status,'out');assert.deepEqual(after.currentJobs,[]);
});

for(const role of ['reviewer','viewer','kiosk'])test(role+' cannot assign a daily management department',async t=>{
 const f=await fixture(t),r=await f.person();f.env.SOP_REQUEST_USER.role=role;
 await assert.rejects(f.call('department',{id:r.id,version:r.version,department:'bulk'}),/权限/);
 assert.equal(f.stored(r.id).version,r.version);
});

test('invalid departments, invalid versions, and employee rows fail without writes',async t=>{
 const f=await fixture(t),r=await f.person(),p=(await f.call('employee_register',{name:'Isolated employee',employeeNo:'QA-WORKFORCE',department:'bulk'})).person;
 const employee=(await f.call('checkin',{badge:p.badgeId})).record;
 const before=f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_events').get().n;
 for(const department of ['office','other','BULK',' bulk ',null,42])await assert.rejects(f.call('department',{id:r.id,version:r.version,department}));
 for(const version of [undefined,0,-1,1.5,2])await assert.rejects(f.call('department',{id:r.id,version,department:'bulk'}));
 await assert.rejects(f.call('department',{id:employee.id,version:employee.version,department:'import'}),error);
 await assert.rejects(f.voidDay(employee),error);await assert.rejects(f.restore(employee),error);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_events').get().n,before);
 assert.equal(f.stored(r.id).version,r.version);assert.equal(f.stored(employee.id).version,employee.version);
 const employees=await f.summary({scope:'employee'});assert.equal(employees.items.length,1);assert.equal(employees.items[0].record.department,'bulk');
});

test('fixed badge management label belongs to the attendance day, not the person',async t=>{
 const f=await fixture(t),r=await f.fixed();await f.call('department',{id:r.id,version:r.version,department:'import'});
 const yesterday=new Date(Date.parse(r.day+'T00:00:00Z')-86400000).toISOString().slice(0,10);
 f.DB.raw.prepare('UPDATE ck_attendance_days SET day=?,signed_out=? WHERE id=?').run(yesterday,r.inAt,r.id);
 const fresh=(await f.call('checkin',{badge:r.badgeId,agency:'포레인'})).record;
 assert.notEqual(fresh.id,r.id);assert.equal(fresh.managementDepartment,'');assert.equal(fresh.badgeId,r.badgeId);
 assert.equal((await f.summary({date:yesterday})).items[0].record.managementDepartment,'import');
});

test('department mutation is idempotent, rejects mismatched reuse, and resolves concurrent versions once',async t=>{
 const f=await fixture(t),r=await f.person(),payload={id:r.id,version:r.version,department:'bulk',client_req_id:crypto.randomUUID()};
 const first=await f.call('department',payload);assert.deepEqual(await f.call('department',payload),first);
 await assert.rejects(f.call('department',{...payload,department:'import'}),/请求/);
 await assert.rejects(f.call('department',{id:r.id,version:r.version,department:'import'}),/更新/);
 const results=await Promise.allSettled(['direct_ship','import'].map(department=>f.call('department',{id:r.id,version:first.record.version,department})));
 assert.equal(results.filter(x=>x.status==='fulfilled').length,1);assert.equal(results.filter(x=>x.status==='rejected').length,1);
 assert.equal(f.stored(r.id).version,r.version+2);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action='sop_attendance_department'").get().n,2);
});

test('void and restore preserve attendance identity/times and never mutate work records',async t=>{
 const f=await fixture(t),r=await f.person(),other=await f.person('Isolated unaffected');
 await f.call('department',{id:r.id,version:r.version,department:'bulk'});let current=(await f.item(r.id)).record;
 assert.equal((await f.item(r.id)).canVoid,true);assert.equal((await f.item(r.id)).voidBlockedReason,'');
 const original=f.stored(r.id),work=f.snapshot(),removed=await f.voidDay(current);current=removed.record;
 const summary=await f.summary();assert.deepEqual(summary.items.map(x=>x.record.id),[other.id]);assert.equal(summary.deletedItems.length,1);
 const deleted=summary.deletedItems[0];assert.equal(deleted.record.id,r.id);assert.equal(deleted.canVoid,false);
 for(const field of ['record','totals','segments','breaks','status','currentJobs','events'])assert.ok(field in deleted,field+' survives in the deleted list');
 assert.deepEqual(f.snapshot(),work);assert.equal(f.stored(r.id).signed_in,original.signed_in);assert.equal(f.stored(r.id).signed_out,original.signed_out);
 const restored=(await f.restore(current)).record;
 assert.equal(restored.id,r.id);assert.equal(restored.badgeId,r.badgeId);assert.equal(restored.day,r.day);assert.equal(restored.inAt,r.inAt);assert.equal(restored.outAt,r.outAt);assert.equal(restored.managementDepartment,'bulk');assert.equal(restored.version,current.version+1);
 assert.equal((await f.summary()).items.length,2);assert.deepEqual((await f.summary()).deletedItems,[]);assert.deepEqual(f.snapshot(),work);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n,2);
 const actions=(await f.item(r.id)).events.map(x=>x.action);assert.deepEqual(actions.slice(-3),['sop_attendance_department','sop_attendance_void','sop_attendance_restore']);
});

for(const role of ['dispatcher','reviewer','viewer','kiosk'])test(role+' cannot void or restore even by replaying manager requests',async t=>{
 const f=await fixture(t),r=await f.person(),request={id:r.id,version:r.version,reason:'Isolated mistake',client_req_id:crypto.randomUUID()};
 const removed=await f.voidDay(r,request);f.env.SOP_REQUEST_USER.role=role;
 await assert.rejects(f.voidDay(r,request),/权限/);await assert.rejects(f.restore(removed.record),/权限/);
 f.env.SOP_REQUEST_USER.role='manager';const restoreRequest={client_req_id:crypto.randomUUID()};await f.restore(removed.record,restoreRequest);
 f.env.SOP_REQUEST_USER.role=role;await assert.rejects(f.restore(removed.record,restoreRequest),/权限/);
});

test('void and restore require reasons, exact versions, and reject altered idempotency payloads',async t=>{
 const f=await fixture(t),r=await f.person();
 for(const reason of [undefined,'','   '])await assert.rejects(f.voidDay(r,{reason}));
 await assert.rejects(f.voidDay(r,{version:0}));
 const request={client_req_id:crypto.randomUUID()},removed=await f.voidDay(r,request);
 assert.deepEqual(await f.voidDay(r,request),removed);
 await assert.rejects(f.voidDay(r,{...request,reason:'Different reason'}),/请求/);
 await assert.rejects(f.restore(removed.record,{version:r.version}),/更新/);
 for(const reason of [undefined,'','   '])await assert.rejects(f.restore(removed.record,{reason}));
 const restoreRequest={client_req_id:crypto.randomUUID()},restored=await f.restore(removed.record,restoreRequest);
 assert.deepEqual(await f.restore(removed.record,restoreRequest),restored);
 await assert.rejects(f.restore(removed.record,{...restoreRequest,reason:'Different restoration'}),/请求/);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action IN ('sop_attendance_void','sop_attendance_restore')").get().n,2);
});

for(const role of ['dispatcher','reviewer','viewer'])test(role+' summary cannot expose deleted attendance rows',async t=>{
 const f=await fixture(t),r=await f.person();await f.voidDay(r);f.env.SOP_REQUEST_USER.role=role;
 const result=await f.summary();assert.deepEqual(result.items,[]);assert.deepEqual(result.deletedItems??[],[]);assert.deepEqual(result.stale,[]);
});

test('deleted badge is excluded from name search, lookup, print, company, checkout, corrections, and breaks',async t=>{
 const f=await fixture(t),r=await f.person(),removed=(await f.voidDay(r)).record;
 assert.deepEqual((await f.call('search',{name:r.name})).items,[]);
 await assert.rejects(f.call('lookup',{badge:r.badgeId}),error);
 for(const [action,data] of [['print',{}],['company',{agency:'포레인'}],['checkout',{}],['department',{department:'import'}],['correct',{inAt:r.inAt,outAt:r.outAt,reason:'Isolated correction'}],['break_start',{}],['break_end',{}]]){
  await assert.rejects(f.call(action,{id:r.id,version:removed.version,...data}),error,action);
 }
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_prints').get().n,0);assert.equal(f.stored(r.id).version,removed.version);
});

for(const kind of ['daily','fixed'])test('deleted '+kind+' checkin does not resurrect or duplicate the identity',async t=>{
 const f=await fixture(t),r=await (kind==='fixed'?f.fixed():f.person());await f.voidDay(r);
 const before=f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n;
 const payload=kind==='fixed'?{badge:r.badgeId,agency:'가온'}:{name:r.name,agency:'가온'};
 let response,thrown;try{response=await f.call('checkin',payload);}catch(e){thrown=e;}
 assert.ok(thrown||response?.ok===false||response?.restoreRequired,'Check-in must tell the caller a manager restoration is required');
 assert.match(thrown?.message||response?.error||response?.message||JSON.stringify(response),/恢复|restore/i);
 assert.deepEqual((await f.summary()).items,[]);assert.equal((await f.summary()).deletedItems.length,1);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n,before);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days').get().n,1);
});

test('two concurrent void requests commit once; audit insertion failure rolls back restoration',async t=>{
 const f=await fixture(t),r=await f.person();
 const result=await Promise.allSettled([f.voidDay(r),f.voidDay(r)]);
 assert.equal(result.filter(x=>x.status==='fulfilled').length,1);assert.equal(result.filter(x=>x.status==='rejected').length,1);
 const removed=(await f.item(r.id,true)).record;
 f.DB.raw.exec("CREATE TRIGGER qa_fail_restore_audit BEFORE INSERT ON ck_attendance_events WHEN NEW.action='sop_attendance_restore' BEGIN SELECT RAISE(ABORT,'isolated audit failure'); END");
 await assert.rejects(f.restore(removed));assert.equal((await f.summary()).deletedItems.length,1);assert.equal(f.stored(r.id).version,removed.version);
 f.DB.raw.exec('DROP TRIGGER qa_fail_restore_audit');await f.restore(removed);assert.equal((await f.summary()).items.length,1);
});

for(const scenario of ['closed work','open work','older open work','cross-midnight work','zero-length work','open break','closed break','borrowed loan','pending return'])test('void refuses '+scenario+' and leaves all work data unchanged',async t=>{
 const f=await fixture(t),r=await f.fixed();
 const start=r.day+'T00:00:00+09:00',before=new Date(Date.parse(start)-3600000).toISOString(),past=new Date(Date.parse(start)-7200000).toISOString();
 if(scenario.includes('work')){
  f.job();const options=scenario==='closed work'?{start:r.inAt,end:r.inAt}:scenario==='older open work'?{start:past,end:before}:scenario==='cross-midnight work'?{start:before,end:start}:scenario==='zero-length work'?{start,end:start}:{};
  const id=f.segment(r,options);if(scenario==='older open work')f.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at='' WHERE id=?").run(id);
 }else if(scenario.includes('break'))f.breakRow(r,{end:scenario==='closed break'?r.inAt:''});
 else await f.loan(r,scenario==='borrowed loan'?'borrowed':'return_pending');
 const snapshot=f.snapshot(),item=await f.item(r.id);
 assert.equal(item.canVoid,false);assert.ok(item.voidBlockedReason);
 await assert.rejects(f.voidDay(r),error);assert.deepEqual(f.snapshot(),snapshot);assert.equal(f.stored(r.id).version,r.version);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action='sop_attendance_void'").get().n,0);
});

test('closed work entirely before the Korean attendance date does not block a new fixed-badge day',async t=>{
 const f=await fixture(t),r=await f.fixed();f.job();
 const prior=new Date(Date.parse(r.day+'T00:00:00+09:00')-3600000).toISOString();f.segment(r,{start:prior,end:prior});
 const before=f.snapshot();assert.equal((await f.item(r.id)).canVoid,true);await f.voidDay(r);assert.deepEqual(f.snapshot(),before);
});

for(const scenario of ['work','break','loan'])test('void revalidates '+scenario+' committed after its preflight',async t=>{
 const f=await fixture(t),r=await f.person();let loan;
 if(scenario==='work')f.job();if(scenario==='loan')loan=await f.loan(r,'closed');
 const injected=f.arm('void',()=>scenario==='work'?f.segment(r):scenario==='break'?f.breakRow(r):f.DB.raw.prepare("UPDATE ck_crew_borrows SET status='return_pending' WHERE id=?").run(loan));
 await assert.rejects(f.voidDay(r),error);injected();assert.equal(f.stored(r.id).version,r.version);assert.equal((await f.summary()).items.length,1);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action='sop_attendance_void'").get().n,0);
});

test('SQL and JavaScript guards reject deleted badge joins and break insertion after stale preflight',async t=>{
 const f=await fixture(t),r=await f.person();f.job();await f.voidDay(r);
 assert.match(await guardAttendance({action:'sop_task_dispatch',workers:[{id:r.badgeId,name:r.name}]},f.env),/删除|恢复|签到|deleted|attendance/i);
 assert.throws(()=>f.segment(r),error);assert.throws(()=>f.breakRow(r),error);
 // A legacy closed segment cannot be moved into the deleted date by UPDATE either.
 const old=new Date(Date.parse(r.day+'T00:00:00+09:00')-3600000).toISOString();
 const id=f.segment(r,{start:old,end:old});
 assert.throws(()=>f.DB.raw.prepare("UPDATE v2_ops_job_workers SET joined_at=?,left_at='' WHERE id=?").run(new Date().toISOString(),id),error);
 const available=await crewAvailability(f.env,{worker_id:r.badgeId,payload:{action:'v2_unplanned_unload_start'}});assert.equal(available.can_borrow,false);assert.match(available.reason,/删除|恢复|签到|deleted|attendance/i);
});

test('a break request loses safely when void commits between read and write',async t=>{
 const f=await fixture(t),r=await f.person();const injected=f.arm('break_start',()=>f.voidDay(r));
 await assert.rejects(f.call('break_start',{id:r.id}),error);injected();
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_breaks').get().n,0);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_attendance_events WHERE action='sop_attendance_break_start'").get().n,0);
 assert.equal((await f.summary()).deletedItems.length,1);
});

async function officeFixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const secret='isolated-workforce-code',env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest(secret)};
 const cookies={office:'',field:'',kiosk:''},paths={office:'/api',field:'/001/api',kiosk:'/attendance/api'};
 async function call(action,data={},context='',scope='office'){
  const response=await worker.fetch(new Request('https://workforce.fixture'+paths[scope],{method:'POST',headers:{Origin:'https://workforce.fixture','Content-Type':'application/json',Cookie:cookies[scope],'X-CK-Operation-Context':context},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
  if(action==='sop_login'&&response.ok)cookies[scope]=response.headers.get('set-cookie').split(';')[0];
  return {status:response.status,...await response.json()};
 }
 const login=await call('sop_login',{sop_key:secret});assert.equal(login.ok,true);
 const confirm=async name=>{const identity=await call('sop_identity');const r=await call('sop_operator_confirm',{name},identity.user.operation_context.id);assert.equal(r.ok,true);return r.user.operation_context.id;};
 return {DB,env,call,confirm,secret};
}

test('real office API requires a current confirmed context for label, void, and restore',async t=>{
 const f=await officeFixture(t),r=(await f.call('sop_attendance_checkin',{name:'Isolated office daily',agency:'가온'})).record;
 const current=(await f.call('sop_identity')).user.operation_context.id;
 const denied=await f.call('sop_attendance_department',{id:r.id,version:r.version,department:'bulk'},current);
 assert.equal(denied.status,409);assert.equal(denied.operator_context_changed,true);
 const old=await f.confirm('Isolated Operator A'),fresh=await f.confirm('Isolated Operator B');
 for(const action of ['sop_attendance_department','sop_attendance_void','sop_attendance_restore']){
  const result=await f.call(action,{id:r.id,version:r.version,department:'bulk',reason:'Isolated reason'},old);
  assert.equal(result.status,409,action);assert.equal(result.operator_context_changed,true);
 }
 const changed=await f.call('sop_attendance_department',{id:r.id,version:r.version,department:'bulk',actor:'forged',actor_name:'forged',operator_name:'forged'},fresh);assert.equal(changed.ok,true,changed.error);
 const removed=await f.call('sop_attendance_void',{id:r.id,version:changed.record.version,reason:'Isolated reason',actor:'forged',actor_name:'forged',operator_name:'forged'},fresh);assert.equal(removed.ok,true,removed.error);
 const restored=await f.call('sop_attendance_restore',{id:r.id,version:removed.record.version,reason:'Isolated restoration',actor:'forged',actor_name:'forged',operator_name:'forged'},fresh);assert.equal(restored.ok,true,restored.error);
 const events=(await f.call('sop_attendance_summary')).items[0].events.slice(-3);
 assert.deepEqual(events.map(e=>[e.actor,e.actorName]),Array.from({length:3},()=>['ck-office-admin','Isolated Operator B']));
 assert.deepEqual(events.map(e=>e.action),['sop_attendance_department','sop_attendance_void','sop_attendance_restore']);
});

test('real field and kiosk scopes cannot access office-only management mutations',async t=>{
 const f=await officeFixture(t),context=await f.confirm('Isolated office supervisor');
 const employee=(await f.call('sop_attendance_employee_register',{name:'Isolated field supervisor',employeeNo:'QA-FIELD',department:'bulk'},context)).person;
 assert.equal((await f.call('sop_attendance_checkin',{badge:employee.badgeId})).ok,true);
 assert.equal((await f.call('sop_login',{badge:employee.badgeId},'','field')).ok,true);
 const r=(await f.call('sop_attendance_checkin',{name:'Isolated field daily',agency:'가온'})).record;
 const changed=await f.call('sop_attendance_department',{id:r.id,version:r.version,department:'import'},'','field');assert.equal(changed.status,403);assert.equal(changed.ok,false);
 const denied=await f.call('sop_attendance_void',{id:r.id,version:r.version,reason:'Isolated mistake'},'','field');assert.equal(denied.ok,false);assert.equal(denied.status,403);
 assert.equal((await f.call('sop_login',{sop_key:f.secret},'','kiosk')).ok,true);
 assert.equal((await f.call('sop_attendance_department',{id:r.id,version:r.version,department:'bulk'},'','kiosk')).status,403);
 assert.equal(f.DB.raw.prepare('SELECT version FROM ck_attendance_days WHERE id=?').get(r.id).version,r.version);
});

for(const operation of ['insert','update'])test('SQL '+operation+' cannot attach closed cross-midnight work to a voided attendance day',async t=>{
 const f=await fixture(t),r=await f.fixed();f.job();await f.voidDay(r);
 const boundary=Date.parse(r.day+'T00:00:00+09:00'),start=new Date(boundary-3600000).toISOString(),end=new Date(boundary+3600000).toISOString();
 if(operation==='insert')assert.throws(()=>f.segment(r,{start,end}),error);
 else{
  const id=f.segment(r,{start,end:start});
  assert.throws(()=>f.DB.raw.prepare('UPDATE v2_ops_job_workers SET left_at=? WHERE id=?').run(end,id),error);
 }
});

for(const action of ['checkin','print'])test('replaying cached '+action+' after void cannot return an apparently active badge',async t=>{
 const f=await fixture(t),checkin={name:'Isolated replay',agency:'가온',client_req_id:crypto.randomUUID()};
 let record=(await f.call('checkin',checkin)).record,payload=checkin;
 if(action==='print'){payload={id:record.id,client_req_id:crypto.randomUUID()};record=(await f.call('print',payload)).record;}
 await f.voidDay(record);let result,thrown;
 try{result=await f.call(action,payload);}catch(e){thrown=e;}
 assert.ok(thrown||result?.ok===false||result?.restoreRequired||result?.record?.voided,'A cached success must not describe a deleted badge as active');
 assert.match(thrown?.message||result?.error||result?.message||JSON.stringify(result),/删除|恢复|voided|restore/i);
 assert.equal((await f.summary()).items.length,0);assert.equal((await f.summary()).deletedItems.length,1);
});

test('manager can explicitly verify a distinct same-name person without reviving the deleted person',async t=>{
 const f=await fixture(t),r=await f.person('Isolated same name'),removed=(await f.voidDay(r)).record;
 const distinct=(await f.call('checkin',{name:r.name,agency:'포레인',confirm_distinct_person:true,reason:'Isolated verified different person'})).record;
 assert.notEqual(distinct.badgeId,r.badgeId);assert.notEqual(distinct.personId,r.personId);
 assert.equal((await f.summary()).items.length,1);assert.equal((await f.summary()).deletedItems[0].record.id,r.id);
 const restored=(await f.restore(removed)).record;
 assert.equal(restored.id,r.id);assert.equal((await f.summary()).items.length,2);assert.deepEqual((await f.summary()).deletedItems,[]);
 const search=await f.call('search',{name:r.name});assert.equal(search.items.length,2);
});

test('pending loan transition cannot revive eligibility after void',async t=>{
 const f=await fixture(t),r=await f.fixed(),loan=await f.loan(r,'closed');await f.voidDay(r);
 assert.throws(()=>f.DB.raw.prepare("UPDATE ck_crew_borrows SET status='return_pending' WHERE id=?").run(loan),error);
 assert.equal(f.DB.raw.prepare('SELECT status FROM ck_crew_borrows WHERE id=?').get(loan).status,'closed');
});

test('a voided prior-day fixed badge permits a separate new-day checkin and legitimate work',async t=>{
 const f=await fixture(t),r=await f.fixed(),yesterday=new Date(Date.parse(r.day+'T00:00:00Z')-86400000).toISOString().slice(0,10);
 f.DB.raw.prepare('UPDATE ck_attendance_days SET day=?,signed_in=?,signed_out=? WHERE id=?').run(yesterday,yesterday+'T08:00:00+09:00',yesterday+'T09:00:00+09:00',r.id);
 await f.voidDay(r);assert.deepEqual((await f.summary()).stale,[]);
 const today=(await f.call('checkin',{badge:r.badgeId,agency:'포레인'})).record;
 assert.notEqual(today.id,r.id);assert.equal(today.badgeId,r.badgeId);assert.equal(today.managementDepartment,'');assert.equal(today.voided,false);
 assert.equal((await f.summary({date:yesterday})).deletedItems.length,1);
 assert.equal(await guardAttendance({action:'sop_task_dispatch',workers:[{id:today.badgeId,name:today.name}]},f.env),null);
 f.job();f.segment(today);assert.equal((await f.item(today.id)).status,'working');
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n,1);
});

test('moving another attendance break into a voided day is blocked at the database layer',async t=>{
 const f=await fixture(t),r=await f.person(),other=await f.person('Isolated break source');
 const br=f.breakRow(other,{end:other.inAt});await f.voidDay(r);
 assert.throws(()=>f.DB.raw.prepare('UPDATE ck_attendance_breaks SET attendance_id=? WHERE id=?').run(r.id,br),error);
 assert.equal(f.DB.raw.prepare('SELECT attendance_id FROM ck_attendance_breaks WHERE id=?').get(br).attendance_id,other.id);
});

test('void and restore retain an already signed-out person’s original sign-in and sign-out',async t=>{
 const f=await fixture(t),r=await f.person(),out=(await f.call('checkout',{id:r.id})).record;
 const removed=(await f.voidDay(out)).record,restored=(await f.restore(removed)).record;
 assert.equal(removed.inAt,r.inAt);assert.equal(removed.outAt,out.outAt);
 assert.equal(restored.inAt,r.inAt);assert.equal(restored.outAt,out.outAt);assert.equal(restored.badgeId,r.badgeId);
 assert.equal((await f.item(r.id)).status,'out');
 assert.match(await guardAttendance({action:'sop_task_dispatch',workers:[{id:r.badgeId,name:r.name}]},f.env),/签退/);
});
