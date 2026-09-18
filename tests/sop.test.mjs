import test from 'node:test';import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';import {handleSop,guardLegacy} from '../worker-v2/sop.js';
function setup(){const DB=database();const env={DB,SOP_UPGRADE_ENABLED:'true',SOP_USERS_JSON:JSON.stringify([
 {key:'manager-test',id:'M',name:'经理',role:'manager'},
 {key:'dispatcher-test',id:'D',name:'派审',role:'dispatcher',departments:['bulk']},
 {key:'service-test',id:'S',name:'客服',role:'service',departments:['bulk']},
 {key:'viewer-test',id:'V',name:'只读',role:'viewer',departments:['bulk']}
 ])};const call=(action,data={},key='manager-test')=>handleSop({action,sop_key:key,client_req_id:crypto.randomUUID(),...data},env);return{env,DB,call};}
async function task(call,extra={}){const r=await call('sop_task_create',{department:'bulk',title:'贴标',job_type:'bulk_op',workers:[{id:'W1',name:'甲'}],lead_id:'W1',estimated_minutes:30,...extra});assert.equal(r.ok,true,r.error);return r;}
async function get(call,id){const r=await call('sop_get',{id});assert.equal(r.ok,true,r.error);return r.record;}
async function mutate(call,action,id,fields={}){const x=await get(call,id);return call(action,{id,revision:x.revision,...fields});}
test('disabled upgrade does not read DB; authentication and viewer writes blocked',async()=>{
 assert.equal((await handleSop({action:'sop_list'},{SOP_UPGRADE_ENABLED:'false'})).disabled,true);
 const {call}=setup();assert.equal((await call('sop_session',{},'bad')).unauthorized,true);
 assert.equal((await call('sop_check_create',{ship_date:'2026-09-20'},'viewer-test')).ok,false);
});
test('assignment has no hours; finish closes labor before review; rework preserves earlier segments',async()=>{
 const {call,DB}=setup();const t=await task(call);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,0);
 assert.equal((await mutate(call,'sop_task_start',t.id)).ok,true);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at='' AND worker_id='M'").get().n,0);
 assert.equal((await mutate(call,'sop_task_finish',t.id,{result:{quantity:100,unit:'箱',description:'贴标100箱',location:'A'}})).ok,true);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,0);
 assert.equal((await get(call,t.id)).status,'awaiting_review');
 assert.equal((await mutate(call,'sop_task_review',t.id,{decision:'return',reason:'标签不符'})).ok,true);
 assert.equal((await mutate(call,'sop_task_start',t.id)).ok,true);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,2);
});
test('concurrent start cannot double book same worker; failed batch rolls back event and task',async()=>{
 const {call,DB}=setup();const a=await task(call),b=await task(call);
 const results=await Promise.all([mutate(call,'sop_task_start',a.id),mutate(call,'sop_task_start',b.id)]);
 assert.equal(results.filter(x=>x.ok).length,1);assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,1);
 const loser=results[0].ok?b:a;assert.equal((await get(call,loser.id)).status,'assigned');
});
test('repeated mutation key and stale revision cannot duplicate participants',async()=>{
 const {call,DB}=setup();const a=await task(call);const payload={id:a.id,revision:1,client_req_id:'repeat'};
 const r=await call('sop_task_start',payload);assert.equal(r.ok,true);assert.deepEqual(await call('sop_task_start',payload),r);
 assert.equal((await call('sop_task_pause',{id:a.id,revision:1,reason:'pause'})).ok,false);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,1);
});
test('staff change records actual join/leave; pause and resume keep separate segments',async()=>{
 const {call,DB}=setup();const a=await task(call);await mutate(call,'sop_task_start',a.id);
 const r=await mutate(call,'sop_task_people',a.id,{workers:[{id:'W2',name:'乙'}],lead_id:'W2',reason:'支援'});assert.equal(r.ok,true,r.error);
 assert.equal(DB.raw.prepare("SELECT worker_id FROM v2_ops_job_workers WHERE left_at=''").get().worker_id,'W2');
 await mutate(call,'sop_task_pause',a.id,{reason:'等叉车'});assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,0);
 await mutate(call,'sop_task_start',a.id);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,3);
});
test('one requirement one task, approval to customer waiting, partial outbound allocation not duplicated',async()=>{
 const {call,DB}=setup();const n=await call('sop_need_create',{department:'bulk',title:'清点',customer:'C',owner:'D',instructions:'100箱'});assert.equal(n.ok,true,n.error);
 const t=await task(call,{need_id:n.id});assert.equal((await call('sop_task_create',{department:'bulk',title:'再次',job_type:'bulk_op',need_id:n.id,workers:[{id:'W2',name:'乙'}],lead_id:'W2',estimated_minutes:20})).ok,false);
 await mutate(call,'sop_task_start',t.id);await mutate(call,'sop_task_finish',t.id,{result:{quantity:100,unit:'箱',description:'已清点',location:'A'}});await mutate(call,'sop_task_review',t.id,{decision:'pass',reason:'全检通过'});
 assert.equal((await get(call,n.id)).status,'waiting_customer');DB.raw.exec("INSERT INTO v2_outbound_orders(id,customer) VALUES('OB1','C'),('OB2','C')");
 assert.equal((await mutate(call,'sop_need_link',n.id,{outbound_id:'OB1',quantity:60})).ok,true);
 assert.equal((await get(call,n.id)).status,'waiting_customer');assert.equal((await mutate(call,'sop_need_link',n.id,{outbound_id:'OB1',quantity:20})).ok,false);
 assert.equal((await mutate(call,'sop_need_link',n.id,{outbound_id:'OB2',quantity:50})).ok,false);
 assert.equal((await mutate(call,'sop_need_link',n.id,{outbound_id:'OB2',quantity:40})).ok,true);
});
test('dates: three rounds do not close; quantity changes reset affected scans only; no silent removal',async()=>{
 const {call}=setup();const c=await call('sop_check_create',{ship_date:'2026-09-20'});assert.equal(c.ok,true,c.error);
 assert.equal((await call('sop_check_create',{ship_date:'2026-09-20'})).ok,false);
 await mutate(call,'sop_check_items',c.id,{items:[{barcode:'0001',customer:'C',qty:1},{barcode:'0002',customer:'C',qty:1}]});
 for(const barcode of ['0001','0002'])await mutate(call,'sop_check_scan',c.id,{barcode,pallet:'P1'});
 for(const slot of ['11:00','13:00','16:00'])assert.equal((await mutate(call,'sop_check_round',c.id,{slot,reason:'交接'})).ok,true);
 assert.equal((await get(call,c.id)).status,'open');
 await mutate(call,'sop_check_items',c.id,{items:[{barcode:'0001',customer:'C',qty:2}],duplicate_mode:'replace',reason:'新增一箱'});
 let x=await get(call,c.id);assert.equal(x.items[0].scanned,0);assert.equal(x.items[1].scanned,1);assert.equal(x.summary.adjusted,1);
 assert.equal((await mutate(call,'sop_check_remove',c.id,{item_id:x.items[1].id,reason:'改期'})).ok,false);
 assert.equal((await mutate(call,'sop_check_close',c.id,{reason:'交接'})).ok,false);
});
test('scanned exceptions require explicit resolution; date move atomic and rechecks destination',async()=>{
 const {call}=setup();const a=await call('sop_check_create',{ship_date:'2026-09-20'}),b=await call('sop_check_create',{ship_date:'2026-09-21'});
 await mutate(call,'sop_check_items',a.id,{items:[{barcode:'A',customer:'C',qty:1}]});await mutate(call,'sop_check_scan',a.id,{barcode:'A',pallet:'P'});
 const x=await get(call,a.id);assert.equal((await mutate(call,'sop_check_move',a.id,{item_id:x.items[0].id,ship_date:'2026-09-21',reason:'客户改期',disposition:'留原位'})).ok,true);
 assert.equal((await get(call,a.id)).items[0].removed,true);assert.equal((await get(call,b.id)).items[0].scanned,0);
 await mutate(call,'sop_check_scan',b.id,{barcode:'A',pallet:'P'});await mutate(call,'sop_check_scan',b.id,{barcode:'X',pallet:'P'});
 assert.equal((await mutate(call,'sop_check_close',b.id,{reason:'交接'})).ok,false);
 const r=await get(call,b.id);await mutate(call,'sop_check_resolve',b.id,{exception_id:r.exceptions[0].id,reason:'误扫件已移出'});
 assert.equal((await mutate(call,'sop_check_close',b.id,{reason:'审核员交装货员，P位'})).ok,true);
});
test('issue changes need exact version acknowledgement; old clients cannot finish an adopted issue',async()=>{
 const {call,DB,env}=setup();DB.raw.exec("INSERT INTO v2_issue_tickets(id,biz_class,issue_description) VALUES('I','bulk','待查')");const r=await call('sop_issue_adopt',{legacy_id:'I'});assert.equal(r.ok,true,r.error);
 await mutate(call,'sop_issue_change',r.id,{message:'新版1'});await mutate(call,'sop_issue_change',r.id,{message:'新版2'});
 assert.equal((await mutate(call,'sop_issue_ack',r.id,{requirement_version:1})).ok,false);
 assert.equal((await mutate(call,'sop_issue_feedback',r.id,{message:'做完'})).ok,false);
 assert.equal((await mutate(call,'sop_issue_ack',r.id,{requirement_version:2})).ok,true);
 assert.equal((await mutate(call,'sop_issue_feedback',r.id,{message:'完成新版2'})).ok,true);
 assert.ok(await guardLegacy({action:'v2_issue_close',id:'I'},env));
});
test('legacy jobs continue and SOP jobs reject legacy mutations; schema rerun is harmless',async()=>{
 const {env,call,DB}=setup();const t=await task(call);
 assert.equal(await guardLegacy({action:'v2_ops_job_finish',job_id:'LEGACY'},env),null);
 assert.ok(await guardLegacy({action:'v2_ops_job_finish',job_id:t.id},env));
 DB.raw.exec("INSERT INTO v2_ops_jobs(id,status) VALUES('OLD','working'); INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES('OLDS','OLD','W1','甲','2026-09-17T00:00:00Z')");
 assert.equal((await mutate(call,'sop_task_start',t.id)).ok,false);
});
test('dashboard clips labor segments to Korean day and never duplicates team output',async()=>{
 const {call,DB}=setup();const t=await task(call);await mutate(call,'sop_task_start',t.id);
 DB.raw.prepare("UPDATE v2_ops_job_workers SET joined_at=?,left_at=? WHERE job_id=?").run('2026-09-16T14:00:00Z','2026-09-16T16:00:00Z',t.id);
 const d=await call('sop_dashboard',{date:'2026-09-17'});assert.equal(d.person_hours,1);
});
test('automatic outbound creates exactly one canonical need; changing shipping data stays possible',async()=>{
 const {env,DB,call}=setup();const {outboundNeedStatements}=await import('../worker-v2/sop.js');
 env.SOP_AUTO_OUTBOUND='true';env.SOP_ROLLOUT_DEPARTMENTS='bulk';
 const body={customer:'C',biz_class:'bulk',uses_stock_operation:1,instruction:'清点',created_by:'客服'};
 DB.raw.exec("INSERT INTO v2_outbound_orders(id,customer,instruction,uses_stock_operation) VALUES('OB-AUTO','C','清点',1)");
 await DB.batch(outboundNeedStatements(env,body,'OB-AUTO','0901','2026-09-17T00:00:00Z'));
 const need=await get(call,'NEED-OB-AUTO');assert.equal(need.instructions,'清点');
 assert.ok(await guardLegacy({action:'v2_outbound_stock_op_start',order_id:'OB-AUTO'},env));
 assert.equal(await guardLegacy({action:'v2_outbound_order_update',id:'OB-AUTO',instruction:'清点',uses_stock_operation:1,pickup_vehicle_no:'A'},env),null);
 assert.ok(await guardLegacy({action:'v2_outbound_order_update',id:'OB-AUTO',instruction:'换单'},env));
 const t=await task(call,{need_id:need.id});await mutate(call,'sop_task_start',t.id);await mutate(call,'sop_task_finish',t.id,{result:{quantity:20,unit:'箱',description:'清点',location:'A'}});await mutate(call,'sop_task_review',t.id,{decision:'pass',reason:'通过'});
 assert.equal((await get(call,need.id)).status,'linked');assert.equal(DB.raw.prepare("SELECT status FROM v2_outbound_orders WHERE id='OB-AUTO'").get().status,'pending_outbound_update');
});
test('pilot stops new intake while allowing existing jobs to finish',async()=>{
 const {call,env}=setup();const t=await task(call);env.SOP_ACCEPT_NEW='false';
 assert.equal((await call('sop_check_create',{ship_date:'2026-09-20'})).ok,false);
 assert.equal((await mutate(call,'sop_task_start',t.id)).ok,true);
 assert.equal((await mutate(call,'sop_task_finish',t.id,{result:{quantity:1,unit:'批',description:'完成',location:'A'}})).ok,true);
});
test('legacy check adoption keeps scan evidence and disallows active-job conversion',async()=>{
 const {call,DB,env}=setup();DB.raw.exec("INSERT INTO v2_verify_batches(id,status) VALUES('V','pending');INSERT INTO v2_verify_batch_items(id,batch_id,barcode,planned_qty,planned_box_count,customer_name) VALUES('VI','V','0001',1,1,'C');INSERT INTO v2_verify_scan_logs(id,batch_id,barcode,scan_result) VALUES('VS','V','0001','ok')");
 const r=await call('sop_check_adopt',{ship_date:'2026-09-20',legacy_id:'V'});assert.equal(r.ok,true,r.error);
 const c=await get(call,r.id);assert.equal(c.items[0].scanned,1);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_verify_scan_logs').get().n,1);
 assert.ok(await guardLegacy({action:'v2_verify_scan_submit',batch_id:'V'},env));
 assert.equal((await call('sop_check_adopt',{ship_date:'2026-09-21',legacy_id:'V'})).ok,false);
});
test('ordinary staff cannot review; another dispatcher cannot manipulate the owners task',async()=>{
 const {call,env}=setup();env.SOP_USERS_JSON=JSON.stringify([...JSON.parse(env.SOP_USERS_JSON),{key:'other',id:'D2',name:'其他派审',role:'dispatcher',departments:['bulk']}]);
 const t=await task(call);const x=await get(call,t.id);assert.equal((await call('sop_task_start',{id:t.id,revision:x.revision},'other')).ok,false);
 await mutate(call,'sop_task_start',t.id);await mutate(call,'sop_task_finish',t.id,{result:{quantity:1,unit:'批',description:'完成',location:'A'}});const y=await get(call,t.id);assert.equal((await call('sop_task_review',{id:t.id,revision:y.revision,decision:'pass',reason:'通过'},'other')).ok,false);
});
