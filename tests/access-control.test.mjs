import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import entry from '../worker-v2/staging-entry.js';
import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';
const origin='https://fixture.local';
async function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCEPT_NEW:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ADMIN_CODE_SHA256:await digest('fixture-only-code'),ADMINKEY:'legacy-secret',OPSKEY:'legacy-ops',R2_BUCKET:{async get(){return {body:'fixture',httpMetadata:{contentType:'image/png'}};}},ASSETS:{async fetch(){return new Response('fixture asset');}}};
 const cookies={office:'',field:'',kiosk:''},paths={office:'/api',field:'/001/api',kiosk:'/attendance/api'};
 async function raw(scope,b,options={}){
  const path=options.path||paths[scope],method=options.method||'POST';
  return (options.entry?entry:worker).fetch(new Request(origin+path+(method==='GET'&&b?'?'+new URLSearchParams(b):''),{method,headers:{'Content-Type':'application/json',Origin:origin,Cookie:options.cookie??cookies[scope],...options.headers},...(method==='GET'?{}:{body:JSON.stringify({client_req_id:crypto.randomUUID(),...b})})}),env);
 }
 async function call(scope,action,b={},options){const r=await raw(scope,{action,...b},options);if(action==='sop_login'&&r.ok)cookies[scope]=r.headers.get('set-cookie')?.split(';')[0]||'';return r.json();}
 const ok=async(s,a,b={})=>{const r=await call(s,a,b);assert.equal(r.ok,true,r.error);return r;};
 await ok('office','sop_login',{sop_key:'fixture-only-code'});await ok('office','sop_attendance_config');
 const employee=async(no='FIXTURE-A')=>(await ok('office','sop_attendance_employee_register',{name:'虚拟职员',employeeNo:no,department:'bulk'})).person;
 const grant=(p,enabled=true,version=0)=>ok('office','sop_access_update',{person_id:p.id,enabled,version});
 const checkin=async p=>(await ok('office','sop_attendance_checkin',{badge:p.badgeId})).record;
 const login=p=>ok('field','sop_login',{badge:p.badgeId+'|伪造名称'});
 return {DB,env,cookies,raw,call,ok,employee,grant,checkin,login};
}
test('field requires employee, explicit authorization and today check-in; the server supplies the name',async()=>{
 const {call,employee,grant,checkin,login}=await setup();
 assert.equal((await call('field','sop_identity')).ok,false);
 assert.equal((await call('field','sop_login',{badge:'DA-20260101-X|临时工'})).ok,false);
 const p=await employee();await checkin(p);assert.equal((await call('field','sop_login',{badge:p.badgeId})).ok,false);
 await grant(p);const r=await login(p);assert.equal(r.user.name,p.name);assert.equal(r.user.role,'dispatcher');assert.equal(r.user.scope,'field');
 const other=await employee('FIXTURE-B');await grant(other);assert.equal((await call('field','sop_login',{badge:other.badgeId})).ok,false);
});
test('office, field and kiosk sessions cannot be exchanged; legacy/public access cannot bypass the gate',async()=>{
 const {call,ok,employee,grant,checkin,login,cookies}=await setup();const p=await employee();await grant(p);await checkin(p);await login(p);
 for(const action of ['sop_access_list','sop_access_update','sop_attendance_employee_register','sop_attendance_employee_import','sop_attendance_correct','v2_admin_dirty_data_cleanup','v2_inbound_plan_create','v2_outbound_order_create','v2_003_material_list','sop_dashboard','sop_demo_prepare'])assert.equal((await call('field',action,{k:'legacy-secret',sop_key:'fixture-only-code',role:'manager'})).ok,false,action);
 assert.equal((await call('office','sop_attendance_employee_people',{}, {cookie:cookies.field})).ok,false);
 assert.equal((await call('field','sop_identity',{}, {cookie:cookies.office})).ok,false);
 assert.equal((await call('office','v2_inbound_plan_list',{k:'legacy-secret'},{cookie:''})).ok,false);
 await ok('kiosk','sop_login',{sop_key:'fixture-only-code'});assert.equal((await ok('kiosk','sop_identity')).user.role,'kiosk');
 for(const action of ['sop_attendance_employee_people','sop_attendance_summary','sop_attendance_register','sop_access_list','sop_native_start'])assert.equal((await call('kiosk',action)).ok,false,action);
 await ok('kiosk','sop_attendance_employee_search',{term:p.badgeId});
});
test('checkout invalidates active field sessions permanently, even if office corrects a checkout',async()=>{
 const {DB,ok,call,employee,grant,checkin,login,cookies}=await setup();const p=await employee();await grant(p);const day=await checkin(p);await login(p);const old=cookies.field;
 await ok('office','sop_attendance_checkout',{id:day.id});assert.equal((await call('field','sop_dispatch_list')).ok,false);
 DB.raw.prepare("UPDATE ck_attendance_days SET signed_out='' WHERE id=?").run(day.id);
 assert.equal((await call('field','sop_identity',{}, {cookie:old})).ok,false);await login(p);
 await ok('field','sop_logout');assert.equal((await call('field','sop_identity')).ok,false);
});
test('revocation, disable, previous day and authorization version all invalidate field login',async()=>{
 const {DB,call,employee,grant,checkin,login,cookies}=await setup();const p=await employee();await grant(p);const day=await checkin(p);await login(p);const old=cookies.field;
 await grant(p,false,1);await grant(p,true,2);assert.equal((await call('field','sop_identity',{}, {cookie:old})).ok,false);await login(p);
 DB.raw.prepare('UPDATE ck_attendance_people SET enabled=0 WHERE id=?').run(p.id);assert.equal((await call('field','sop_dispatch_list')).ok,false);DB.raw.prepare('UPDATE ck_attendance_people SET enabled=1 WHERE id=?').run(p.id);await login(p);
 DB.raw.prepare("UPDATE ck_attendance_days SET day='2000-01-01' WHERE id=?").run(day.id);assert.equal((await call('field','sop_identity')).ok,false);assert.equal((await call('field','sop_login',{badge:p.badgeId})).ok,false);
});
test('field dispatcher can assign a crew and complete the original temporary unload, without office rights',async()=>{
 const {DB,ok,call,employee,grant,checkin,login}=await setup();const p=await employee();await grant(p);await checkin(p);await login(p);
 const daily=(await ok('office','sop_attendance_checkin',{name:'虚拟日当',agency:'가온'})).record;
 const j=await ok('field','sop_native_start',{payload:{action:'v2_unplanned_unload_start',cargo_summary:'虚拟验收卸货',client_req_id:crypto.randomUUID()},workers:[{id:daily.badgeId,name:daily.name}],lead_id:daily.badgeId,estimated_minutes:20});
 assert.equal((await ok('field','sop_dispatch_list')).items[0].id,j.job_id);
 const detail=await ok('field','v2_ops_job_detail',{job_id:j.job_id});assert.equal(detail.can_manage_dispatch,true);
 const other=await employee('FIXTURE-B');await grant(other);await checkin(other);await login(other);
 assert.equal((await ok('field','sop_dispatch_list')).items.length,0);
 assert.equal((await call('field','v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:daily.badgeId,complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}]})).ok,false);
 await login(p);await ok('field','v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:daily.badgeId,complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}]});
 assert.equal(DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'completed');
});
test('rejects cross-origin, unsafe GET, invalid content types and unregistered file access',async()=>{
 const {raw,cookies,employee,grant,checkin,login}=await setup();const p=await employee();await grant(p);await checkin(p);await login(p);
 assert.equal((await raw('office',{action:'sop_access_list'},{headers:{Origin:'https://unrelated.local'}})).status,403);
 assert.equal((await raw('office',{action:'sop_access_list'},{method:'GET'})).status,403);
 assert.equal((await raw('field',{action:'sop_identity'},{headers:{'Content-Type':'text/plain'}})).status,415);
 assert.equal((await raw('office',null,{method:'GET',path:'/file?key=fixture',cookie:''})).status,401);
 assert.equal((await raw('field',null,{method:'GET',path:'/001/api/file?key=unregistered',cookie:cookies.field})).status,401);
});

test('a checked-in authorized crew member can reopen and finish the same dispatch after switching login',async()=>{
 const {DB,ok,call,employee,grant,checkin,login}=await setup();
 const a=await employee('CREW-A'),b=await employee('CREW-B'),c=await employee('CREW-C');
 for(const p of [a,b,c]){await grant(p);await checkin(p);}await login(a);
 const crew=[a,b].map(p=>({id:p.badgeId,name:p.name}));
 const j=await ok('field','sop_native_start',{payload:{action:'v2_unplanned_unload_start',cargo_summary:'Fixture cargo',client_req_id:crypto.randomUUID()},workers:crew,lead_id:a.badgeId,estimated_minutes:20});
 const segment=()=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? AND worker_id=?').get(j.job_id,b.badgeId);
 const original=segment();
 await ok('field','sop_native_people',{job_id:j.job_id,revision:1,workers:[crew[1]],lead_id:b.badgeId});
 // Removing the original dispatcher's work segment does not remove ownership.
 assert.equal((await ok('field','v2_ops_job_detail',{job_id:j.job_id})).can_manage_dispatch,true);
 await ok('field','sop_logout');await login(b);
 for(let i=0;i<2;i++){
  const list=await ok('field','sop_dispatch_list'),item=list.items.find(x=>x.id===j.job_id);
  assert.ok(item);assert.equal(item.source_type,'field_feedback');assert.deepEqual(item.workers,[crew[1]]);
  assert.equal((await ok('field','v2_ops_job_detail',{job_id:j.job_id})).can_manage_dispatch,true);
 }
 assert.deepEqual(segment(),original,'reopening preserves the original timing segment');
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,2);
 // Forging badge/worker fields on an unrelated authorized session grants nothing.
 await login(c);assert.equal((await ok('field','sop_dispatch_list',{badge:b.badgeId,worker_id:b.badgeId})).items.length,0);
 assert.equal((await ok('field','v2_ops_job_detail',{job_id:j.job_id,worker_id:b.badgeId})).can_manage_dispatch,false);
 assert.equal((await call('field','sop_native_people',{job_id:j.job_id,revision:2,workers:[crew[1]],lead_id:b.badgeId})).ok,false);
 await login(b);await ok('field','sop_native_people',{job_id:j.job_id,revision:2,workers:[crew[1]],lead_id:b.badgeId});
 assert.deepEqual(segment(),original);
 await ok('field','v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:b.badgeId,complete_job:true,result_lines:[{unit_type:'carton',actual_qty:10}]});
 assert.equal(DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'completed');
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,1);
});

test('past crew snapshots do not grant access after a worker leaves; list filters before its limit',async()=>{
 const {DB,ok,employee,grant,checkin,login}=await setup();const a=await employee('OWNER'),b=await employee('CREW');
 for(const p of [a,b]){await grant(p);await checkin(p);}await login(a);
 const crew=[{id:b.badgeId,name:b.name}];
 const j=await ok('field','sop_native_start',{payload:{action:'v2_unplanned_unload_start',client_req_id:crypto.randomUUID()},workers:crew,lead_id:b.badgeId,estimated_minutes:20});
 // More than 200 unrelated newer jobs must not hide this person's ongoing work.
 const record=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(j.job_id);
 for(let i=0;i<205;i++){
  const id='UNRELATED-'+i;
  DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,updated_at) VALUES(?,'unload','working','2099-01-01')").run(id);
  DB.raw.prepare("INSERT INTO sop_records VALUES(?,'dispatch',1,'bulk',?,'2099-01-01')").run(id,JSON.stringify({...JSON.parse(record.state),owner_id:'unrelated'}));
 }
 await login(b);assert.equal((await ok('field','sop_dispatch_list')).items[0].id,j.job_id);
 DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at='2099-01-01' WHERE job_id=?").run(j.job_id);
 assert.equal((await ok('field','sop_dispatch_list')).items.length,0);
 assert.equal((await ok('field','v2_ops_job_detail',{job_id:j.job_id})).can_manage_dispatch,false);
});
test('static office pages require office login while the separate field and attendance entrances stay reachable',async()=>{
 const {raw,cookies}=await setup();for(const path of ['/','/002/','/003/','/shuju/','/templates/employees.xlsx']){const r=await raw('office',null,{path,method:'GET',cookie:'',entry:true});assert.equal(r.status,302,path);assert.match(r.headers.get('location'),/office-login/);}
 for(const path of ['/001/','/attendance/','/office-login/','/shared/sop-session.js'])assert.equal((await raw('office',null,{path,method:'GET',cookie:'',entry:true})).status,200);
 assert.equal((await raw('office',null,{path:'/shuju/',method:'GET',cookie:cookies.office,entry:true})).status,200);
});
test('switching a shared browser to field or kiosk removes its previous office session',async()=>{
 for(const scope of ['field','kiosk']){
  const {call,employee,grant,checkin,cookies}=await setup();const p=await employee();await grant(p);await checkin(p);const office=cookies.office;
  const r=await call(scope,'sop_login',scope==='field'?{badge:p.badgeId}:{sop_key:'fixture-only-code'},{cookie:office});assert.equal(r.ok,true,r.error);
  assert.equal((await call('office','sop_identity',{}, {cookie:office})).ok,false);
 }
});
test('field role can dispatch and review a requirement and cannot change office requirements',async()=>{
 const {call,ok,employee,grant,checkin,login}=await setup();const p=await employee();await grant(p);await checkin(p);await login(p);
 const daily=(await ok('office','sop_attendance_checkin',{name:'虚拟理货员',agency:'가온'})).record;
 const n=await ok('office','sop_need_create',{department:'bulk',title:'虚拟打托',customer:'虚拟客户',instructions:'打托并反馈',owner:'管理员'});
 const task=await ok('field','sop_task_dispatch',{need_id:n.id,department:'bulk',title:'虚拟打托',job_type:'bulk_op',workers:[{id:daily.badgeId,name:daily.name}],lead_id:daily.badgeId,estimated_minutes:15});
 const view=await ok('field','sop_field_resolve',{code:n.id});assert.equal(view.task.status,'working');
 assert.equal((await call('field','sop_need_change',{id:n.id,revision:2,instructions:'冒改要求'})).ok,false);
 await ok('field','sop_task_complete_review',{id:task.id,revision:1,decision:'pass',reason:'虚拟审核',result:{quantity:2,unit:'托',pallet_count:2,operated_box_count:20}});
 assert.equal((await ok('field','sop_field_resolve',{code:n.id})).task.status,'completed');
});
