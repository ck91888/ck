// Controlled ownership races through authenticated Worker requests, synthetic data only.
import test from 'node:test';import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';import {localD1} from './d1-local-runtime.mjs';
import {confirmOperation} from './office-operator-fixture.mjs';
async function fixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});const f=await ownedHomeFixture();let native;
 f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';
 if(process.env.CK_P1_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 t.after(async()=>{await native?.close();f.DB.raw.close();});
 const first=(s,...a)=>f.env.DB.prepare(s).bind(...a).first(),rows=async(s,...a)=>(await f.env.DB.prepare(s).bind(...a).all()).results;
 return {...f,first,rows};
}
async function scenario(f,kind){
 const w=await f.person(),workers=[{id:w.badgeId,name:w.name}];let job,action,body;
 if(kind==='ordinary'){
  job=await f.task('A','合成普通完成竞态',w);action='sop_task_complete_review';body={id:job.id,revision:job.revision,decision:'pass',reason:'合成完成',result:{quantity:3,unit:'箱',packed_count:3}};
 }else if(kind==='group'){
  const n=await f.ok('office','sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYN-GROUP-RACE',customer:'合成客户',title:'合成分组竞态',instructions:'合成',cargo_groups:{groups:[{range:'SYN-G1',box_count:3}]}}),need=(await f.ok('A','sop_get',{id:n.id})).record;
  job=await f.ok('A','sop_task_dispatch',{need_id:n.id,department:'bulk',title:'合成分组竞态',job_type:'bulk_op',workers,lead_id:w.badgeId,estimated_minutes:15,cargo_group_ids:need.cargo_groups.groups.map(g=>g.id),cargo_process_index:0});
  const updated=(await f.ok('A','sop_get',{id:n.id})).record;action='sop_cargo_group_complete';body={id:n.id,revision:updated.revision,task_id:job.id,group_id:updated.cargo_groups.groups[0].id,quantity:3,unit:'箱'};
 }else{
  if(kind==='multi-order'){
   const batch=await f.ok('A','sop_bulk_batch_create',{items:['ONE','TWO'].map(s=>({source_type:'external',work_order_no:'SYN-MULTI-'+s,customer:'合成客户'})),workers,lead_id:w.badgeId});
   const detail=await f.ok('A','sop_bulk_batch_get',{id:batch.batch_id});job=detail.items[0];
  }else{const started=await f.ok('A','sop_native_start',{payload:{action:'v2_bulk_op_job_start',work_order_no:'SYN-EXTERNAL-RACE',customer:'合成客户',client_req_id:crypto.randomUUID()},workers,lead_id:w.badgeId,estimated_minutes:15,labor_department:'bulk'});job={id:started.job_id};}
  action='v2_bulk_op_job_finish';body={job_id:job.id,worker_id:w.badgeId,complete_job:true,customer:'合成客户',packed_box_count:3,total_operated_box_count:3};
 }
 return{job,action,body};
}
async function hold(f,action,id){
 const db=f.env.DB;let reached,release,held=false;const ready=new Promise(r=>reached=r),gate=new Promise(r=>release=r);
 f.env.DB={prepare:db.prepare.bind(db),async batch(ss){const relevant=ss.some(s=>action.startsWith('v2_')?s.sql.includes('INSERT INTO v2_ops_job_results')&&(s.args.includes('RES-FINAL-'+id)||s.args.includes('R-'+id)||s.sql.includes("'manual_finalize'")):s.sql.includes('INSERT INTO sop_events')&&s.args[3]===action);if(!held&&relevant){held=true;reached();await gate;}return db.batch(ss);}};
 return{ready,release,restore(){f.env.DB=db;}};
}
for(const kind of ['ordinary','external','group','multi-order'])test(kind+' completion prepared by the old owner rolls back after transfer',async t=>{
 const f=await fixture(t),s=await scenario(f,kind),gate=await hold(f,s.action,s.job.id),pending=f.call('A',s.action,s.body);await gate.ready;
 try{const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.job.id);await f.ok('A','sop_owner_transfer',{id:s.job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成交接'});}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',s.job.id)).state).owner_id,f.people.B.id);
 assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);assert.notEqual((await f.first('SELECT status FROM v2_ops_jobs WHERE id=?',s.job.id)).status,'completed');
 if(kind==='group'){const n=(await f.ok('office','sop_get',{id:s.body.id})).record;assert.equal((n.cargo_groups.groups[0].outputs||[]).length,0);}
 const updated={...s.body,...(!s.action.startsWith('v2_')?{revision:(await f.first('SELECT revision FROM sop_records WHERE id=?',s.body.id)).revision}:{})};const current=await f.call('B',s.action,updated);assert.equal(current.ok,true,JSON.stringify(current));
});
test('transferring away and back still invalidates the old completion, even for the original owner',async t=>{
 const f=await fixture(t),s=await scenario(f,'external'),gate=await hold(f,s.action,s.job.id),pending=f.call('A',s.action,s.body);await gate.ready;
 try{for(const [from,to] of [['A','B'],['B','A']]){const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.job.id);await f.ok(from,'sop_owner_transfer',{id:s.job.id,revision:row.revision,target_badge:f.people[to].badgeId,reason:'合成交接往返'});}}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);assert.equal((await f.call('A',s.action,s.body)).ok,true);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_native_completion_checks')).n,0);
});
test('a prepared manager completion also requires reconfirmation after task ownership changes',async t=>{
 const f=await fixture(t),s=await scenario(f,'external'),gate=await hold(f,s.action,s.job.id),pending=f.call('office',s.action,s.body);await gate.ready;
 try{const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.job.id);await f.ok('A','sop_owner_transfer',{id:s.job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成经理提交期间交接'});}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);assert.equal((await f.call('office',s.action,s.body)).ok,true);
});
test('manual completion publishes neither a result nor job closure when ownership changes in flight',async t=>{
 const f=await fixture(t),s=await scenario(f,'external');await f.ok('A','v2_bulk_op_job_finish',{job_id:s.job.id,worker_id:s.body.worker_id,leave_only:true});
 const action='v2_ops_job_manual_finalize',body={job_id:s.job.id,reason:'合成离场补录',customer:'合成客户',result_json:{box_count:3}},gate=await hold(f,action,s.job.id),pending=f.call('office',action,body);await gate.ready;
 try{const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.job.id);await f.ok('A','sop_owner_transfer',{id:s.job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成补录在途交接'});}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);assert.notEqual((await f.first('SELECT status FROM v2_ops_jobs WHERE id=?',s.job.id)).status,'completed');assert.equal((await f.call('office',action,body)).ok,true);
});
for(const leave of [false,true])test('an active shared dispatcher '+(leave?'cannot commit after leaving the pair':'can complete without becoming the owner'),async t=>{
 const f=await fixture(t);f.env.SOP_DISPATCH_SHARE_ENABLED='true';for(const key of ['A','B']){const p=(await f.ok('office','sop_attendance_register',{name:'合成共享'+key,agency:'가온'})).person;await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk'],version:0,reason:'合成共享授权'});await f.ok(key,'sop_login',{badge:p.badgeId});f.people[key]=p;}
 const pair=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});await f.ok('B','sop_dispatch_share_accept',{pair_id:pair.pair.id});const s=await scenario(f,'external');
 let reply;if(leave){const gate=await hold(f,s.action,s.job.id),pending=f.call('B',s.action,s.body);await gate.ready;try{await f.ok('B','sop_dispatch_share_leave',{pair_id:pair.pair.id});}finally{gate.release();}reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);await f.ok('A',s.action,s.body);}else{reply=await f.call('B',s.action,s.body);assert.equal(reply.ok,true,JSON.stringify(reply));}
 assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',s.job.id)).state).owner_id,f.people.A.id);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
test('the same completion request held while its retry commits still reports one successful completion',async t=>{
 const f=await fixture(t),s=await scenario(f,'external'),body={...s.body,client_req_id:'SYN-FINISH-ONCE'},gate=await hold(f,s.action,s.job.id),pending=f.call('A',s.action,body);await gate.ready;
 try{await f.ok('A',s.action,body);}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,true,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
test('a concurrent pause invalidates the old completion without closing its replacement work state',async t=>{
 const f=await fixture(t),s=await scenario(f,'external'),gate=await hold(f,s.action,s.job.id),pending=f.call('A',s.action,s.body);await gate.ready;
 try{const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.job.id);await f.ok('A','sop_native_pause',{job_id:s.job.id,revision:row.revision,reason:'合成暂停'});}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT status FROM v2_ops_jobs WHERE id=?',s.job.id)).status,'paused');assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);
});
test('a rest round trip invalidates the prepared field actor evidence without ending the crew',async t=>{
 const f=await fixture(t),s=await scenario(f,'external'),gate=await hold(f,s.action,s.job.id),pending=f.call('A',s.action,s.body);await gate.ready;
 try{await f.ok('office','sop_attendance_break_start',{id:f.people.A.record.id});await f.ok('office','sop_attendance_break_end',{id:f.people.A.record.id});}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);assert.equal((await f.first("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",s.job.id)).n,1);await f.ok('A',s.action,s.body);
});
test('a failed completion transaction rolls back its result, worker release and authorization check',async t=>{
 const f=await fixture(t),s=await scenario(f,'external'),body={...s.body,client_req_id:'SYN-FINISH-ROLLBACK'};
 await f.env.DB.prepare("CREATE TRIGGER synthetic_completion_failure BEFORE UPDATE OF status ON v2_ops_jobs WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'synthetic completion failure'); END").run();
 assert.equal((await f.call('A',s.action,body)).ok,false);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);assert.equal((await f.first("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",s.job.id)).n,1);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_native_completion_checks')).n,0);
 await f.env.DB.prepare('DROP TRIGGER synthetic_completion_failure').run();await f.ok('A',s.action,body);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
async function logisticScenario(f,kind){
 for(const key of ['A','B']){const p=(await f.ok('office','sop_attendance_register',{name:'合成物流派审'+key,agency:'가온'})).person;await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk','direct_ship'],version:0,reason:'合成物流授权'});await f.ok(key,'sop_login',{badge:p.badgeId});f.people[key]=p;}
 const w=await f.person(),workers=[{id:w.badgeId,name:w.name}],start=payload=>f.ok('A','sop_native_start',{payload:{client_req_id:crypto.randomUUID(),...payload},workers,lead_id:w.badgeId,estimated_minutes:15,labor_department:payload.biz_class||'bulk'});
 const plans=[],orders=[];let job,action,body;
 if(kind==='outbound-load'){
  const n=await f.ok('office','sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYN-LOGISTIC-LOAD',customer:'合成装车客户',title:'合成装车准备',instructions:'合成',planned_quantity:3,planned_unit:'箱'}),o=await f.ok('office','v2_outbound_order_create',{customer:'合成装车客户',biz_class:'bulk',outbound_mode:'customer_pickup',expected_ship_at:'2026-11-03',sop_existing_need_id:n.id,sop_need_revision:n.revision,sop_link_quantity:3});orders.push(o.id);await f.ok('office','v2_outbound_order_update_status',{id:o.id,status:'issued'});
  const p=await f.person(),task=await f.ok('A','sop_task_dispatch',{need_id:n.id,department:'bulk',title:'合成装车准备',job_type:'bulk_op',workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId,estimated_minutes:15});await f.ok('A','sop_task_complete_review',{id:task.id,revision:task.revision,decision:'pass',reason:'合成确认准备',result:{quantity:3,unit:'箱'}});
  job=await start({action:'v2_outbound_load_start',order_ids:[o.id]});action='v2_outbound_load_finish';body={job_id:job.job_id,worker_id:w.badgeId,complete_job:true,order_results:[{order_id:o.id,box_count:3,pallet_count:0}]};
 }else{
  const p=await f.ok('office','v2_inbound_plan_create',{customer:'合成到仓客户',biz_classes:kind==='inbound'?['direct_ship']:['bulk'],external_inbound_nos:['SYN-LOGISTIC-INBOUND'],lines:[{unit_type:'carton',planned_qty:3}]});plans.push(p.id);
  const unloaded=await start({action:'v2_unload_job_start',plan_ids:[p.id]});const detail=await f.ok('A','v2_ops_job_detail',{job_id:unloaded.job_id}),plan_results=detail.unload_plans.map(({plan,lines})=>({plan_id:plan.id,lines:lines.map(x=>({line_id:x.id,actual_qty:x.planned_qty}))}));
  if(kind==='unload'){job=unloaded;action='v2_unload_job_finish';body={job_id:job.job_id,worker_id:w.badgeId,complete_job:true,plan_results};}
  else{await f.ok('A','v2_unload_job_finish',{job_id:unloaded.job_id,worker_id:w.badgeId,complete_job:true,plan_results});job=await start({action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:'SYN-LOGISTIC-INBOUND',biz_class:'direct_ship',job_type:'inbound_direct'});action='v2_inbound_job_finish';body={job_id:job.job_id,worker_id:w.badgeId,complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:3}]};}
 }
 const id=job.job_id,snapshot=async()=>({job:await f.first('SELECT * FROM v2_ops_jobs WHERE id=?',id),workers:await f.rows('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id',id),results:await f.rows('SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY id',id),plans:await f.rows('SELECT * FROM v2_inbound_plans WHERE id IN (SELECT value FROM json_each(?)) ORDER BY id',JSON.stringify(plans)),lines:await f.rows('SELECT * FROM v2_inbound_plan_lines WHERE plan_id IN (SELECT value FROM json_each(?)) ORDER BY id',JSON.stringify(plans)),departments:await f.rows('SELECT * FROM v2_inbound_plan_biz_tasks WHERE plan_id IN (SELECT value FROM json_each(?)) ORDER BY id',JSON.stringify(plans)),orders:await f.rows('SELECT * FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?)) ORDER BY id',JSON.stringify(orders))});return{id,action,body,snapshot};
}
for(const kind of ['inbound','outbound-load','unload'])test(kind+' ownership transfer rejects the held completion and preserves every business row',async t=>{
 const f=await fixture(t),s=await logisticScenario(f,kind),gate=await hold(f,s.action,s.id),pending=f.call('A',s.action,s.body);await gate.ready;let before;
 try{const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.id);await f.ok('office','sop_owner_transfer',{id:s.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成物流在途交接'});assert.equal((await f.call('A',s.action,s.body)).ok,false);before=await s.snapshot();}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.deepEqual(await s.snapshot(),before);await f.ok('B',s.action,s.body);assert.equal((await f.first('SELECT status FROM v2_ops_jobs WHERE id=?',s.id)).status,'completed');assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.id)).n,1);
});
for(const kind of ['external','ordinary','group','multi-order'])test(kind+' shared completion cannot reuse a replacement collaboration pair',async t=>{
 const f=await fixture(t);f.env.SOP_DISPATCH_SHARE_ENABLED='true';
 for(const key of ['A','B']){const p=(await f.ok('office','sop_attendance_register',{name:'合成配对轮换'+key,agency:'가온'})).person;await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk'],version:0,reason:'合成配对轮换授权'});await f.ok(key,'sop_login',{badge:p.badgeId});f.people[key]=p;}
 const pair=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});await f.ok('B','sop_dispatch_share_accept',{pair_id:pair.pair.id});const s=await scenario(f,kind),gate=await hold(f,s.action,s.job.id),pending=f.call('B',s.action,s.body);await gate.ready;
 try{await f.ok('B','sop_dispatch_share_leave',{pair_id:pair.pair.id});const next=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});assert.notEqual(next.pair.id,pair.pair.id);await f.ok('B','sop_dispatch_share_accept',{pair_id:next.pair.id});}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,0);
 if(kind==='group'){const n=(await f.ok('office','sop_get',{id:s.body.id})).record;assert.equal((n.cargo_groups.groups[0].outputs||[]).length,0);}
 await f.ok('B',s.action,s.body);
});
async function manualScenario(f){const s=await scenario(f,'external');await f.ok('A','v2_bulk_op_job_finish',{job_id:s.job.id,worker_id:s.body.worker_id,leave_only:true});return {...s,action:'v2_ops_job_manual_finalize',body:{job_id:s.job.id,reason:'合成同请求补录',customer:'合成客户',result_json:{box_count:3},client_req_id:'SYN-MANUAL-ONCE'}};}
test('an in-flight identical manual retry returns the original committed receipt',async t=>{
 const f=await fixture(t),s=await manualScenario(f),gate=await hold(f,s.action,s.job.id),pending=f.call('office',s.action,s.body);await gate.ready;let saved;
 try{saved=await f.ok('office',s.action,s.body);}finally{gate.release();}const reply=await pending;gate.restore();assert.deepEqual(reply,saved);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
test('manual receipt replay rejects changed content and verifies historical unbound results',async t=>{
 const f=await fixture(t),s=await manualScenario(f),saved=await f.ok('office',s.action,s.body);
 for(const change of [{result_json:{box_count:4}},{reason:'合成不同原因'},{customer:'合成不同客户'},{result_summary:'合成不同摘要'},{result_note:'合成不同说明'}]){const reply=await f.call('office',s.action,{...s.body,...change});assert.equal(reply.ok,false,JSON.stringify({change,reply}));assert.equal(reply.error,'request_id_conflict');}
 await f.env.DB.prepare('UPDATE v2_idempotency_keys SET response_json=? WHERE idem_key=?').bind(JSON.stringify(saved),s.body.client_req_id).run();assert.deepEqual(await f.ok('office',s.action,s.body),saved);assert.equal((await f.call('office',s.action,{...s.body,result_json:{box_count:9}})).ok,false);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
test('a matching manual receipt cannot override a changed ownership revision in an older in-flight request',async t=>{
 const f=await fixture(t),s=await manualScenario(f),gate=await hold(f,s.action,s.job.id),pending=f.call('office',s.action,s.body);await gate.ready;
 try{const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.job.id);await f.ok('A','sop_owner_transfer',{id:s.job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成同请求补录前交接'});await f.ok('office',s.action,s.body);}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
test('a conflicting manual retry cannot be substituted for an older request',async t=>{
 const f=await fixture(t),s=await manualScenario(f),gate=await hold(f,s.action,s.job.id),pending=f.call('office',s.action,s.body);await gate.ready;
 try{await f.ok('office',s.action,{...s.body,result_json:{box_count:4}});}finally{gate.release();}const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));assert.equal(reply.error,'request_id_conflict');assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);assert.equal((await f.first('SELECT box_count FROM v2_ops_job_results WHERE job_id=?',s.job.id)).box_count,4);
});
test('a committed manual receipt does not override a revoked session, while a fresh authorized session may replay it',async t=>{
 const f=await fixture(t),s=await manualScenario(f),gate=await hold(f,s.action,s.job.id),pending=f.call('office',s.action,s.body);await gate.ready;let saved;
 try{saved=await f.ok('office',s.action,s.body);await f.ok('office','sop_logout');}finally{gate.release();}const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));
 await f.ok('office','sop_login',{sop_key:'synthetic-owned-home-fixture'});await confirmOperation((a,b)=>f.ok('office',a,b),'合成任务归属验收员');assert.deepEqual(await f.ok('office',s.action,s.body),saved);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
for(const role of ['owner','member','manager'])test('replacement pairs do not revoke independent '+role+' completion authority',async t=>{
 const f=await fixture(t);f.env.SOP_DISPATCH_SHARE_ENABLED='true';
 for(const key of ['A','B']){const p=(await f.ok('office','sop_attendance_register',{name:'合成独立权限'+key,agency:'가온'})).person;await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk'],version:0,reason:'合成独立权限授权'});await f.ok(key,'sop_login',{badge:p.badgeId});f.people[key]=p;}
 const pair=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});await f.ok('B','sop_dispatch_share_accept',{pair_id:pair.pair.id});const s=await scenario(f,'external');
 if(role==='member'){const row=await f.first('SELECT revision FROM sop_records WHERE id=?',s.job.id);await f.ok('A','sop_native_people',{job_id:s.job.id,revision:row.revision,workers:[{id:f.people.B.badgeId,name:f.people.B.name}],lead_id:f.people.B.badgeId,reason:'合成本人下场'});s.body.worker_id=f.people.B.badgeId;}
 const actor=role==='manager'?'office':role==='member'?'B':'A',gate=await hold(f,s.action,s.job.id),pending=f.call(actor,s.action,s.body);await gate.ready;
 try{await f.ok('B','sop_dispatch_share_leave',{pair_id:pair.pair.id});const next=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});await f.ok('B','sop_dispatch_share_accept',{pair_id:next.pair.id});}finally{gate.release();}
 const reply=await pending;gate.restore();assert.equal(reply.ok,true,JSON.stringify(reply));assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
test('a legacy manual replay keeps office session revocation checks without requiring a SOP owner',async t=>{
 const f=await fixture(t),s=await manualScenario(f);await f.env.DB.prepare('DELETE FROM sop_records WHERE id=?').bind(s.job.id).run();const gate=await hold(f,s.action,s.job.id),pending=f.call('office',s.action,s.body);await gate.ready;let saved;
 try{saved=await f.ok('office',s.action,s.body);await f.ok('office','sop_logout');}finally{gate.release();}const reply=await pending;gate.restore();assert.equal(reply.ok,false,JSON.stringify(reply));
 await f.ok('office','sop_login',{sop_key:'synthetic-owned-home-fixture'});await confirmOperation((a,b)=>f.ok('office',a,b),'合成任务归属验收员');assert.deepEqual(await f.ok('office',s.action,s.body),saved);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?',s.job.id)).n,1);
});
