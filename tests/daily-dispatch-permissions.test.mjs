import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
async function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCEPT_NEW:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-daily-permission')},cookies={office:'',field:''};
 async function call(scope,action,b={},cookie){const r=await worker.fetch(new Request('https://fixture.local'+(scope==='field'?'/001/api':'/api'),{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie??cookies[scope],...operationHeaders(cookie??cookies[scope])},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);if(action==='sop_login'&&r.ok)cookies[scope]=r.headers.get('set-cookie').split(';')[0];return rememberOperation(await r.json(),cookies[scope]);}
 const ok=async(s,a,b={},c)=>{const r=await call(s,a,b,c);assert.equal(r.ok,true,a+': '+r.error);return r;};await ok('office','sop_login',{sop_key:'synthetic-daily-permission'});await confirmOperation((a,b)=>call('office',a,b),'合成权限管理员');await ok('office','sop_attendance_config');
 const permanent=async(name='合成同名人员')=>(await ok('office','sop_attendance_register',{name,agency:'가온'})).person,checkin=async p=>(await ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency})).record;
 const grant=(p,enabled=true,version=0,departments=['bulk'])=>ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled,version,departments,reason:'合成核实授权'}),login=p=>ok('field','sop_login',{badge:p.badgeId});
 const start=(p,more={})=>call('field','sop_native_start',{payload:{action:'v2_ops_job_start',job_type:'inventory',flow_stage:'internal',client_req_id:crypto.randomUUID()},workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId,estimated_minutes:10,labor_department:'bulk',...more});
 return {DB,env,cookies,call,ok,permanent,checkin,grant,login,start};
}
test('daily dispatch defaults off, grants only explicit departments and no administrative authority',async()=>{
 const s=await setup(),p=await s.permanent();await s.checkin(p);assert.equal((await s.call('field','sop_login',{badge:p.badgeId})).ok,false);const list=await s.ok('office','sop_daily_dispatch_list');assert.equal(list.items[0].grant.enabled,false);
 await s.grant(p);await s.login(p);const u=(await s.ok('field','sop_identity')).user;assert.equal(u.id,p.id);assert.equal(u.daily_dispatch,true);assert.equal(u.can_dispatch,true);assert.deepEqual(u.dispatch_departments,['bulk']);
 for(const action of ['sop_daily_dispatch_update','sop_daily_dispatch_link','sop_access_update','sop_attendance_register','sop_task_delegate'])assert.equal((await s.call('field',action,{person_id:p.id,badge:p.badgeId,enabled:true,version:1,departments:['import'],reason:'forged'})).ok,false,action);
 assert.equal((await s.start(p,{labor_department:'import'})).ok,false);assert.equal((await s.start(p)).ok,true);
 const event=s.DB.raw.prepare('SELECT * FROM ck_daily_dispatch_events').get();assert.equal(event.actor_name,'合成权限管理员');assert.equal(JSON.parse(event.after_json).version,1);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_daily_dispatch_checks').get().n,0);
});
test('temporary badges require exact explicit stable identity, isolate same names and retain original history across days',async t=>{
 t.mock.timers.enable({apis:['Date'],now:new Date('2026-10-13T01:00:00Z')});const s=await setup(),p=await s.permanent(),other=await s.permanent();await s.grant(p);
 const d=(await s.ok('office','sop_attendance_checkin',{name:p.name,agency:'가온'})).record;assert.equal((await s.call('field','sop_login',{badge:d.badgeId})).ok,false);
 const preview=await s.ok('office','sop_daily_dispatch_link_preview',{person_id:p.id,daily_badge:d.badgeId.toLowerCase()});await s.ok('office','sop_daily_dispatch_link',{person_id:p.id,badge:p.badgeId,daily_badge:d.badgeId,daily_person_id:preview.daily.id,attendance_id:d.id,reason:'已当面核对同一本人'});
 const u=await s.login({badgeId:d.badgeId.toLowerCase()});assert.equal(u.user.id,p.id);assert.equal((await s.start({badgeId:d.badgeId,name:d.name})).ok,true);assert.equal(s.DB.raw.prepare('SELECT person_id FROM ck_attendance_days WHERE id=?').get(d.id).person_id,d.personId);
 assert.equal((await s.call('office','sop_daily_dispatch_link',{person_id:other.id,badge:other.badgeId,daily_badge:d.badgeId,daily_person_id:d.personId,attendance_id:d.id,reason:'不能继承'})).ok,false);
 t.mock.timers.tick(86400000);await s.ok('office','sop_login',{sop_key:'synthetic-daily-permission'});await confirmOperation((a,b)=>s.call('office',a,b),'合成权限管理员');await s.checkin(p);await s.login(p);assert.equal((await s.ok('field','sop_identity')).user.can_dispatch,true);assert.equal((await s.ok('office','sop_daily_dispatch_list')).items.find(x=>x.id===p.id).grant.version,1);
 const next=(await s.ok('office','sop_attendance_checkin',{name:p.name,agency:'가온',confirm_distinct_person:true,reason:'合成同名但未关联临时牌'})).record;assert.equal((await s.call('field','sop_login',{badge:next.badgeId})).ok,false);await s.checkin(other);assert.equal((await s.call('field','sop_login',{badge:other.badgeId})).ok,false);
});
test('revocation blocks old-session new dispatch immediately while keeping pause/resume and finish of existing crews',async()=>{
 const s=await setup(),p=await s.permanent();await s.checkin(p);await s.grant(p);await s.login(p);const j=await s.start(p);assert.equal(j.ok,true,j.error);const cookie=s.cookies.field;
 await s.ok('field','sop_native_pause',{job_id:j.job_id,revision:1,reason:'合成暂停'});await s.grant(p,false,1,['bulk']);assert.equal((await s.ok('field','sop_identity',{},cookie)).user.can_dispatch,false);assert.equal((await s.start(p)).ok,false);
 const detail=await s.ok('field','v2_ops_job_detail',{job_id:j.job_id});await s.ok('field','sop_native_resume',{job_id:j.job_id,revision:detail.dispatch.revision,workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId});
 await s.ok('field','v2_ops_job_finish',{job_id:j.job_id,worker_id:p.badgeId});assert.equal(s.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'completed');assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_daily_dispatch_events WHERE person_id=?').get(p.id).n,2);
});
test('grant updates are idempotent and concurrent versions cannot overwrite a revoke',async()=>{
 const s=await setup(),p=await s.permanent(),b={person_id:p.id,badge:p.badgeId,enabled:true,version:0,departments:['bulk'],reason:'合成授权',client_req_id:'SYNTH-GRANT-1'};
 const [a,c]=await Promise.all([s.call('office','sop_daily_dispatch_update',b),s.call('office','sop_daily_dispatch_update',b)]);assert.equal(a.ok,true,a.error);assert.equal(c.ok,true,c.error);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_daily_dispatch_events').get().n,1);
 const results=await Promise.all([s.call('office','sop_daily_dispatch_update',{...b,enabled:false,version:1,client_req_id:'SYNTH-REVOKE'}),s.call('office','sop_daily_dispatch_update',{...b,departments:['import'],version:1,client_req_id:'SYNTH-CHANGE'})]);assert.equal(results.filter(x=>x.ok).length,1);assert.equal(s.DB.raw.prepare('SELECT version FROM ck_daily_dispatch_grants').get().version,2);
});
test('revocation racing the business transaction rolls back job, segments and idempotency receipt',async()=>{
 const s=await setup(),p=await s.permanent();await s.checkin(p);await s.grant(p);await s.login(p);const batch=s.DB.batch.bind(s.DB);let raced=false;s.DB.batch=async statements=>{if(!raced&&statements.some(x=>x.sql.includes('INSERT INTO v2_ops_jobs'))){raced=true;s.DB.raw.prepare('UPDATE ck_daily_dispatch_grants SET enabled=0,version=version+1 WHERE person_id=?').run(p.id);}return batch(statements);};
 const r=await s.start(p);assert.equal(raced,true);assert.equal(r.ok,false);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,0);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_idempotency_keys').get().n,0);
});
test('dispatcher attendance is required even when dispatching only other workers; rest, disable/restore and checkout apply immediately',async()=>{
 const s=await setup(),p=await s.permanent(),member=await s.permanent('合成参与人');await s.checkin(member);await s.grant(p);assert.equal((await s.call('field','sop_login',{badge:p.badgeId})).ok,false);
 const day=await s.checkin(p);await s.login(p);await s.ok('office','sop_attendance_break_start',{id:day.id});assert.equal((await s.start(member)).ok,false);await s.ok('office','sop_attendance_break_end',{id:day.id});
 await s.ok('office','sop_attendance_permanent_update',{id:p.id,version:1,name:p.name,agency:p.agency,enabled:false,reason:'合成停用'});assert.equal((await s.start(member)).ok,false);await s.ok('office','sop_attendance_permanent_update',{id:p.id,version:2,name:p.name,agency:p.agency,enabled:true,reason:'合成恢复'});
 const job=await s.start(member);assert.equal(job.ok,true,job.error);assert.equal((await s.ok('office','sop_daily_dispatch_list')).items.find(x=>x.id===p.id).grant.version,1);
 await s.ok('office','sop_attendance_checkout',{id:day.id});assert.equal((await s.start(member)).ok,false);const again=await s.login(p);assert.equal(again.user.can_dispatch,false);await s.ok('field','v2_ops_job_finish',{job_id:job.job_id,worker_id:member.badgeId});assert.equal(s.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(job.job_id).status,'completed');
});
test('a valid old session crossing midnight cannot create a new dispatch before current-day attendance',async t=>{
 t.mock.timers.enable({apis:['Date'],now:new Date('2026-10-13T14:59:00Z')});const s=await setup(),p=await s.permanent();await s.checkin(p);await s.grant(p);await s.login(p);t.mock.timers.tick(120000);assert.equal((await s.ok('field','sop_identity')).user.can_dispatch,false);assert.equal((await s.start(p)).ok,false);await s.checkin(p);await s.login(p);assert.equal((await s.start(p)).ok,true);
});
test('temporary-link races, retries, wrong preview identities, and same-name profiles cannot overwrite associations',async()=>{
 const s=await setup(),p=await s.permanent(),other=await s.permanent();
 const d=(await s.ok('office','sop_attendance_checkin',{name:p.name,agency:'가온'})).record,d2=(await s.ok('office','sop_attendance_checkin',{name:p.name,agency:'가온',confirm_distinct_person:true,reason:'合成同名'})).record;
 const prev=await s.ok('office','sop_daily_dispatch_link_preview',{person_id:p.id,daily_badge:d.badgeId});const b={person_id:p.id,badge:p.badgeId,daily_badge:d.badgeId,daily_person_id:prev.daily.id,attendance_id:d.id,reason:'合成当面核对',client_req_id:'SYNTH-LINK-RACE'};
 assert.equal((await s.call('office','sop_daily_dispatch_link',{...b,daily_person_id:other.id})).ok,false);
 const same=await Promise.all([s.call('office','sop_daily_dispatch_link',b),s.call('office','sop_daily_dispatch_link',b)]);assert.equal(same.filter(x=>x.ok).length,2);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_daily_dispatch_links').get().n,1);
 assert.equal((await s.call('office','sop_daily_dispatch_link',{...b,person_id:other.id,badge:other.badgeId,client_req_id:'SYNTH-LINK-OTHER'})).ok,false);
 const prev2=await s.ok('office','sop_daily_dispatch_link_preview',{person_id:other.id,daily_badge:d2.badgeId});assert.equal((await s.call('office','sop_daily_dispatch_link',{...b,daily_badge:d2.badgeId,daily_person_id:prev2.daily.id,attendance_id:d2.id,client_req_id:'SYNTH-LINK-SECOND'})).ok,false);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_daily_dispatch_events').get().n,1);
});
test('requirement dispatch and courier use their explicit department and existing tasks finish after revocation',async()=>{
 const s=await setup(),p=await s.permanent(),d=await s.checkin(p);await s.grant(p);await s.login(p);
 const need=await s.ok('office','sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYNTH-PERMISSION-STOCK',title:'合成打托',customer:'合成',instructions:'合成',owner:'合成'}),task=await s.ok('field','sop_task_dispatch',{need_id:need.id,department:'bulk',title:'合成打托',job_type:'bulk_op',workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId,estimated_minutes:15});
 assert.equal((await s.call('field','sop_courier_receive',{operation:'start',workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId})).ok,false);
 await s.grant(p,false,1);await s.ok('field','sop_task_complete_review',{id:task.id,revision:1,decision:'pass',reason:'合成审核',result:{quantity:2,unit:'托',pallet_count:2,operated_box_count:20}});
 await s.grant(p,true,2,['direct_ship']);await s.ok('field','sop_courier_config');const batch=await s.ok('field','sop_courier_receive',{operation:'start',workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId});await s.grant(p,false,3,['direct_ship']);await s.ok('field','sop_courier_receive',{operation:'finish',batch_id:batch.batch.id,revision:batch.batch.revision});await s.ok('office','sop_attendance_checkout',{id:d.id});
});
test('revoked dispatcher may remove existing crew but cannot add a new worker',async()=>{
 const s=await setup(),p=await s.permanent(),member=await s.permanent('合成新参与人');await s.checkin(p);await s.checkin(member);await s.grant(p);await s.login(p);const job=await s.start(p);assert.equal(job.ok,true,job.error);await s.grant(p,false,1);
 assert.equal((await s.call('field','sop_native_people',{job_id:job.job_id,revision:1,workers:[{id:p.badgeId,name:p.name},{id:member.badgeId,name:member.name}],lead_id:p.badgeId})).ok,false);
 await s.ok('field','sop_native_people',{job_id:job.job_id,revision:1,workers:[],lead_id:''});await s.ok('field','v2_ops_job_finish',{job_id:job.job_id,worker_id:p.badgeId});
});
for(const race of ['checkout','disable','rest'])test(race+' racing dispatch transaction does not create a partial job',async()=>{
 const s=await setup(),p=await s.permanent(),d=await s.checkin(p);await s.grant(p);await s.login(p);const batch=s.DB.batch.bind(s.DB);let raced=false;
 s.DB.batch=async statements=>{if(!raced&&statements.some(x=>x.sql.includes('INSERT INTO v2_ops_jobs'))){raced=true;if(race==='checkout')s.DB.raw.prepare('UPDATE ck_attendance_days SET signed_out=? WHERE id=?').run(new Date().toISOString(),d.id);if(race==='disable')s.DB.raw.prepare('UPDATE ck_attendance_people SET enabled=0 WHERE id=?').run(p.id);if(race==='rest')s.DB.raw.prepare("INSERT INTO ck_attendance_breaks VALUES('SYNTH-RACE',?,?,'','[]','synthetic')").run(d.id,new Date().toISOString());}return batch(statements);};
 assert.equal((await s.start(p)).ok,false);assert.equal(raced,true);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,0);
});
test('permission changes require the existing confirmed office operator context',async()=>{
 const s=await setup(),p=await s.permanent();await s.ok('office','sop_daily_dispatch_list');s.DB.raw.prepare("UPDATE ck_office_operation_context SET operator_name='' ").run();assert.equal((await s.call('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,version:0,departments:['bulk'],reason:'未确认身份'})).ok,false);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_daily_dispatch_events').get().n,0);
});

test('legacy resume cannot bypass revoked permission to add new crew to an existing task',async()=>{
 const s=await setup(),p=await s.permanent(),member=await s.permanent('合成原成员'),other=await s.permanent('合成新成员');await s.checkin(p);await s.checkin(member);await s.checkin(other);await s.grant(p);await s.login(p);const job=await s.start(p,{workers:[{id:p.badgeId,name:p.name},{id:member.badgeId,name:member.name}]});assert.equal(job.ok,true,job.error);await s.grant(p,false,1);
 const r=await s.call('field','v2_ops_job_resume',{parent_job_id:job.job_id,worker_id:other.badgeId,worker_name:other.name});assert.equal(r.ok,false);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND worker_id=?').get(job.job_id,other.badgeId).n,0);
 await s.ok('field','v2_ops_job_leave',{job_id:job.job_id,worker_id:p.badgeId});await s.ok('field','v2_ops_job_resume',{parent_job_id:job.job_id,worker_id:p.badgeId,worker_name:p.name});await s.ok('field','v2_ops_job_finish',{job_id:job.job_id,worker_id:p.badgeId});
});

test('a second current-day permanent punch after a temporary link blocks ambiguous old-session dispatch',async()=>{
 const s=await setup(),p=await s.permanent(),d=(await s.ok('office','sop_attendance_checkin',{name:'合成临时本人',agency:'가온'})).record;await s.grant(p);const preview=await s.ok('office','sop_daily_dispatch_link_preview',{person_id:p.id,daily_badge:d.badgeId});await s.ok('office','sop_daily_dispatch_link',{person_id:p.id,badge:p.badgeId,daily_badge:d.badgeId,daily_person_id:preview.daily.id,attendance_id:d.id,reason:'合成本人核对'});await s.login(d);await s.checkin(p);assert.equal((await s.ok('field','sop_identity')).user.can_dispatch,false);assert.equal((await s.start(d)).ok,false);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);
});
