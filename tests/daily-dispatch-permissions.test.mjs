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
test('verify takeover requires an on-duty department dispatcher and preserves the original task',async()=>{
 const s=await setup(),a=await s.permanent('合成原派审员'),c=await s.permanent('合成接班派审员'),wrong=await s.permanent('合成跨部门派审员');
 const dayA=await s.checkin(a),dayC=await s.checkin(c);await s.checkin(wrong);await s.grant(a);await s.grant(c);await s.grant(wrong,true,0,['direct_ship']);await s.login(a);const cookieA=s.cookies.field;
 const check=await s.ok('office','sop_check_create',{ship_date:'2026-10-17'});await s.ok('office','sop_check_items',{id:check.id,revision:check.revision,items:[{barcode:'SYNTH-HANDOFF',customer:'合成',qty:2}]});
 const start=await s.ok('field','sop_native_start',{payload:{action:'v2_verify_job_start',batch_id:check.id,client_req_id:crypto.randomUUID()},workers:[{id:a.badgeId,name:a.name}],lead_id:a.badgeId,estimated_minutes:10,labor_department:'bulk'});
 const id=start.job_id,request={job_id:id,revision:1,client_req_id:'SYNTH-VERIFY-HANDOFF-1'};
 await s.login(c);assert.equal((await s.ok('field','sop_verify_active',{batch_id:check.id})).active.can_takeover,false,'active original crew blocks takeover');assert.equal((await s.call('field','sop_verify_takeover',request)).ok,false);
 await s.ok('office','sop_attendance_checkout',{id:dayA.id});assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n,0);
 await s.login(wrong);assert.equal((await s.call('field','sop_verify_active',{batch_id:check.id})).ok,false);assert.equal((await s.call('field','sop_verify_takeover',request)).ok,false);
 await s.login(c);const found=await s.ok('field','sop_verify_active',{batch_id:check.id});assert.equal(found.active.id,id);assert.equal(found.active.can_takeover,true);
 const transfer=await s.ok('field','sop_verify_takeover',request);assert.equal(transfer.revision,2);assert.equal((await s.ok('field','sop_verify_takeover',request)).revision,2,'same request replays');assert.equal((await s.call('field','sop_verify_takeover',{...request,client_req_id:'SYNTH-VERIFY-HANDOFF-STALE'})).ok,false);
 const state=JSON.parse(s.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(id).state);assert.equal(state.owner_id,c.id);assert.equal(state.last_owner_transfer.from_id,a.id);assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM sop_events WHERE record_id=? AND action='sop_verify_takeover'").get(id).n,1);assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_jobs WHERE related_doc_id=? AND job_type='verify_scan'").get(check.id).n,1);
 assert.equal((await s.call('field','sop_native_people',{job_id:id,revision:2,workers:[{id:a.badgeId,name:a.name}],lead_id:a.badgeId})).ok,false,'signed-out badge cannot resume a work clock');
 await s.ok('field','sop_native_people',{job_id:id,revision:2,workers:[{id:c.badgeId,name:c.name}],lead_id:c.badgeId});assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n,1);
 await s.ok('office','sop_attendance_break_start',{id:dayC.id});assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n,0);
 await s.grant(wrong,true,1,['bulk']);await s.login(wrong);const resting=await s.ok('field','sop_verify_active',{batch_id:check.id});assert.equal(resting.active.can_takeover,true);await s.ok('field','sop_verify_takeover',{job_id:id,revision:3});await s.ok('field','sop_native_people',{job_id:id,revision:4,workers:[{id:wrong.badgeId,name:wrong.name}],lead_id:wrong.badgeId});
 await s.ok('office','sop_attendance_break_end',{id:dayC.id});assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at=''").get(id,c.badgeId).n,0,'removed resting owner cannot silently rejoin');assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at=''").get(id,wrong.badgeId).n,1);
 assert.equal((await s.call('field','sop_verify_takeover',{job_id:id,revision:5},cookieA)).ok,false,'signed-out original session cannot take ownership back');
});
test('explicit owner transfer keeps the native job and crew while moving management rights',async()=>{
 const s=await setup(),old=await s.permanent('合成原负责人'),next=await s.permanent('合成新负责人'),wrong=await s.permanent('合成跨部门负责人'),member=await s.permanent('合成普通组员');
 await s.checkin(old);await s.checkin(next);await s.checkin(wrong);await s.checkin(member);await s.grant(old);await s.grant(next);await s.grant(wrong,true,0,['direct_ship']);await s.grant(member);
 await s.login(old);const oldCookie=s.cookies.field,started=await s.start(old,{workers:[{id:old.badgeId,name:old.name},{id:member.badgeId,name:member.name}]});assert.equal(started.ok,true,started.error);const id=started.job_id;
 const request={id,revision:1,target_badge:next.badgeId+'|'+next.name,reason:'合成换班，明确交接',client_req_id:'SYNTH-OWNER-TRANSFER'};
 await s.login(member);assert.equal((await s.call('field','sop_owner_transfer',{...request,client_req_id:'SYNTH-MEMBER-TAKEOVER'})).ok,false,'crew member cannot transfer ownership');await s.login(old);
 assert.equal((await s.call('field','sop_owner_transfer',{...request,target_badge:wrong.badgeId,client_req_id:'SYNTH-WRONG-DEPT'})).ok,false);
 assert.equal((await s.call('field','sop_owner_transfer',{...request,target_badge:old.badgeId,client_req_id:'SYNTH-SELF'})).ok,false);
 const moved=await s.ok('field','sop_owner_transfer',request);assert.equal(moved.revision,2);
 assert.equal((await s.ok('field','sop_owner_transfer',request)).revision,2,'same request replays');
 assert.equal((await s.call('field','sop_owner_transfer',{...request,client_req_id:'SYNTH-STALE'})).ok,false);
 let state=JSON.parse(s.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(id).state);
 assert.equal(state.owner_id,next.id);assert.equal(state.original_owner_id,old.id);assert.equal(state.owner_transfers.length,1);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n,2,'active worker segments preserved');
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs WHERE id=?').get(id).n,1);
 assert.equal((await s.ok('field','v2_ops_job_detail',{job_id:id})).can_manage_dispatch,false,'former owner is still a worker but cannot manage');
 assert.equal((await s.call('field','sop_native_pause',{job_id:id,revision:2,reason:'旧负责人不得暂停'},oldCookie)).ok,false);
 await s.login(next);assert.equal((await s.ok('field','v2_ops_job_detail',{job_id:id})).can_manage_dispatch,true);
 assert.equal((await s.ok('field','sop_dispatch_list')).items.some(x=>x.id===id),true);
 await s.ok('field','sop_native_pause',{job_id:id,revision:2,reason:'新负责人审核交接后暂停'});
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM sop_events WHERE record_id=? AND action='sop_owner_transfer'").get(id).n,1);
});
test('authorized successor can explicitly claim an off-duty owner task while another worker remains active',async()=>{
 const s=await setup(),old=await s.permanent('合成下班原派审员'),next=await s.permanent('合成在岗接班派审员'),member=await s.permanent('合成在岗普通组员'),wrong=await s.permanent('合成其他业务派审员');
 const dayOld=await s.checkin(old);await s.checkin(next);await s.checkin(member);await s.checkin(wrong);await s.grant(old);await s.grant(next);await s.grant(wrong,true,0,['direct_ship']);
 await s.login(old);const started=await s.start(old,{workers:[{id:old.badgeId,name:old.name},{id:member.badgeId,name:member.name}]});assert.equal(started.ok,true,started.error);const id=started.job_id;
 await s.login(next);assert.equal((await s.ok('field','sop_owner_transfer_options',{id})).can_claim,false,'owner on duty blocks self claim');assert.equal((await s.call('field','sop_owner_transfer',{id,revision:1,target_badge:next.badgeId,reason:'尚未离岗'})).ok,false);
 await s.ok('office','sop_attendance_checkout',{id:dayOld.id});assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n,1,'other worker still working');
 assert.equal((await s.call('field','sop_login',{badge:member.badgeId})).ok,false,'ordinary worker has no dispatcher grant');
 await s.login(wrong);assert.equal((await s.ok('field','sop_dispatch_list')).items.some(x=>x.id===id),false,'wrong business cannot see claim card');assert.equal((await s.call('field','sop_owner_transfer',{id,revision:1,target_badge:wrong.badgeId,reason:'其他业务'})).ok,false);
 await s.login(next);const listed=(await s.ok('field','sop_dispatch_list')).items.find(x=>x.id===id);assert.equal(listed?.can_claim,true,'authorized successor sees original task');const option=await s.ok('field','sop_owner_transfer_options',{id});assert.equal(option.can_claim,true);
 assert.equal((await s.call('field','sop_owner_transfer',{id,revision:1,target_badge:member.badgeId,reason:'不得指定普通成员'})).ok,false);
 const moved=await s.ok('field','sop_owner_transfer',{id,revision:1,target_badge:next.badgeId,reason:'原派审员下班，本人明确接班'});assert.equal(moved.revision,2);
 const state=JSON.parse(s.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(id).state);assert.equal(state.owner_id,next.id);assert.equal(state.original_owner_id,old.id);assert.equal(state.last_owner_transfer.mode,'claim');assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n,1);
 assert.equal((await s.ok('field','v2_ops_job_detail',{job_id:id})).can_manage_dispatch,true);await s.ok('field','sop_native_pause',{job_id:id,revision:2,reason:'新派审员确认后暂停'});
});
test('a returning original owner racing self claim aborts the entire transfer',async()=>{
 const s=await setup(),old=await s.permanent('合成临时离岗负责人'),next=await s.permanent('合成接班人');const day=await s.checkin(old);await s.checkin(next);await s.grant(old);await s.grant(next);await s.login(old);const started=await s.start(old);assert.equal(started.ok,true,started.error);await s.ok('office','sop_attendance_checkout',{id:day.id});await s.login(next);
 const batch=s.DB.batch.bind(s.DB);let raced=false;s.DB.batch=async statements=>{if(!raced&&statements.some(x=>x.sql.includes('INSERT INTO sop_events'))){raced=true;s.DB.raw.prepare("UPDATE ck_attendance_days SET signed_out='' WHERE id=?").run(day.id);}return batch(statements);};
 const r=await s.call('field','sop_owner_transfer',{id:started.job_id,revision:1,target_badge:next.badgeId,reason:'合成复岗竞态'});assert.equal(raced,true);assert.equal(r.ok,false);
 assert.equal(JSON.parse(s.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(started.job_id).state).owner_id,old.id);assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM sop_events WHERE record_id=? AND action='sop_owner_transfer'").get(started.job_id).n,0);
});
test('explicit transfer of a planned task gives the new owner its finish and review',async()=>{
 const s=await setup(),old=await s.permanent('合成计划原负责人'),next=await s.permanent('合成计划新负责人'),outsider=await s.permanent('合成普通参与人');
 await s.checkin(old);await s.checkin(next);await s.checkin(outsider);await s.grant(old);await s.grant(next);await s.login(old);const oldCookie=s.cookies.field;
 const need=await s.ok('office','sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYNTH-TRANSFER-STOCK',title:'合成计划任务',customer:'合成',instructions:'合成',owner:'合成'});
 const task=await s.ok('field','sop_task_dispatch',{need_id:need.id,department:'bulk',title:'合成计划任务',job_type:'bulk_op',workers:[{id:old.badgeId,name:old.name}],lead_id:old.badgeId,estimated_minutes:15});
 await s.login(next);assert.equal((await s.call('field','sop_owner_transfer',{id:task.id,revision:1,target_badge:next.badgeId,reason:'普通组员不得接管'})).ok,false);
 await s.login(old);await s.ok('field','sop_owner_transfer',{id:task.id,revision:1,target_badge:next.badgeId,reason:'合成移交计划任务'});
 assert.equal((await s.call('field','sop_task_complete_review',{id:task.id,revision:2,decision:'pass',reason:'旧负责人无审核权',result:{quantity:2,unit:'托',pallet_count:2,operated_box_count:20}},oldCookie)).ok,false);
 await s.login(next);await s.ok('field','sop_task_complete_review',{id:task.id,revision:2,decision:'pass',reason:'新负责人审核',result:{quantity:2,unit:'托',pallet_count:2,operated_box_count:20}});
 const state=JSON.parse(s.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(task.id).state);assert.equal(state.status,'completed');assert.equal(state.owner_id,next.id);assert.equal(state.original_owner_id,old.id);assert.equal(state.review.by,next.name);
});
test('off-duty planned-task owner can hand off review to an authorized successor with crew still working',async()=>{
 const s=await setup(),old=await s.permanent('合成计划下班负责人'),next=await s.permanent('合成计划接班负责人'),crew=await s.permanent('合成计划在岗组员');const day=await s.checkin(old);await s.checkin(next);await s.checkin(crew);await s.grant(old);await s.grant(next);await s.login(old);
 const need=await s.ok('office','sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYNTH-CLAIM-STOCK',title:'合成计划',customer:'合成',instructions:'合成',owner:'合成'});
 const task=await s.ok('field','sop_task_dispatch',{need_id:need.id,department:'bulk',title:'合成计划',job_type:'bulk_op',workers:[{id:old.badgeId,name:old.name},{id:crew.badgeId,name:crew.name}],lead_id:old.badgeId,estimated_minutes:15});await s.ok('office','sop_attendance_checkout',{id:day.id});
 await s.login(next);assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(task.id).n,1);const option=await s.ok('field','sop_owner_transfer_options',{id:task.id});assert.equal(option.can_claim,true);
 await s.ok('field','sop_owner_transfer',{id:task.id,revision:option.revision,target_badge:next.badgeId,reason:'原负责人下班，接续审核'});await s.ok('field','sop_task_complete_review',{id:task.id,revision:option.revision+1,decision:'pass',reason:'接班后核实成果',result:{quantity:2,unit:'托',pallet_count:2,operated_box_count:20}});
 const state=JSON.parse(s.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(task.id).state);assert.equal(state.owner_id,next.id);assert.equal(state.review.by,next.name);assert.equal(state.status,'completed');
});
test('courier batch transfer keeps the batch and lets the new dispatcher finish',async()=>{
 const s=await setup(),old=await s.permanent('合成快递原负责人'),next=await s.permanent('合成快递新负责人');await s.checkin(old);await s.checkin(next);await s.grant(old,true,0,['direct_ship']);await s.grant(next,true,0,['direct_ship']);await s.login(old);const oldCookie=s.cookies.field;
 await s.ok('field','sop_courier_config');const start=await s.ok('field','sop_courier_receive',{operation:'start',workers:[{id:old.badgeId,name:old.name}],lead_id:old.badgeId}),id=start.batch.id;
 await s.ok('field','sop_owner_transfer',{id,revision:start.batch.revision,target_badge:next.badgeId,reason:'合成快递换班'});
 assert.equal((await s.call('field','sop_courier_receive',{operation:'finish',batch_id:id,revision:start.batch.revision+1},oldCookie)).ok,false);
 await s.login(next);const active=await s.ok('field','sop_courier_config',{operation:'batches'});assert.equal(active.items.some(x=>x.id===id),true);const detail=await s.ok('field','sop_courier_detail',{batch_id:id});assert.equal(detail.batch.owner_id,next.id);
 await s.ok('field','sop_courier_receive',{operation:'finish',batch_id:id,revision:detail.batch.revision});assert.equal(s.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(id).status,'completed');
});
test('off-duty courier owner can be replaced by authorized in-department successor with crew still working',async()=>{
 const s=await setup(),old=await s.permanent('合成快递下班负责人'),next=await s.permanent('合成快递接班负责人'),crew=await s.permanent('合成快递在岗组员');const day=await s.checkin(old);await s.checkin(next);await s.checkin(crew);await s.grant(old,true,0,['direct_ship']);await s.grant(next,true,0,['direct_ship']);await s.login(old);await s.ok('field','sop_courier_config');const start=await s.ok('field','sop_courier_receive',{operation:'start',workers:[{id:old.badgeId,name:old.name},{id:crew.badgeId,name:crew.name}],lead_id:old.badgeId});
 await s.ok('office','sop_attendance_checkout',{id:day.id});await s.login(next);const id=start.batch.id;assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n,1);
 assert.equal((await s.ok('field','sop_dispatch_list')).items.find(x=>x.id===id)?.can_claim,true);const option=await s.ok('field','sop_owner_transfer_options',{id});assert.equal(option.can_claim,true);
 await s.ok('field','sop_owner_transfer',{id,revision:option.revision,target_badge:next.badgeId,reason:'原派审员下班，继续本批'});assert.equal((await s.ok('field','sop_courier_config',{operation:'batches'})).items.some(x=>x.id===id),true);
 const detail=await s.ok('field','sop_courier_detail',{batch_id:id});assert.equal(detail.batch.owner_id,next.id);await s.ok('field','sop_courier_receive',{operation:'finish',batch_id:id,revision:detail.batch.revision});assert.equal(s.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(id).status,'completed');
});
test('target losing its department grant during transfer cannot receive the task',async()=>{
 const s=await setup(),old=await s.permanent('合成原负责人'),next=await s.permanent('合成待接负责人');await s.checkin(old);await s.checkin(next);await s.grant(old);await s.grant(next);await s.login(old);const started=await s.start(old);assert.equal(started.ok,true,started.error);
 const batch=s.DB.batch.bind(s.DB);let raced=false;s.DB.batch=async statements=>{if(!raced&&statements.some(x=>x.sql.includes('INSERT INTO sop_events'))){raced=true;s.DB.raw.prepare('UPDATE ck_daily_dispatch_grants SET enabled=0,version=version+1 WHERE person_id=?').run(next.id);}return batch(statements);};
 const r=await s.call('field','sop_owner_transfer',{id:started.job_id,revision:1,target_badge:next.badgeId,reason:'合成权限竞态'});assert.equal(raced,true);assert.equal(r.ok,false);assert.equal(JSON.parse(s.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(started.job_id).state).owner_id,old.id);
 assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM sop_events WHERE record_id=? AND action='sop_owner_transfer'").get(started.job_id).n,0);
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
