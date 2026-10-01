import test from 'node:test';import assert from 'node:assert/strict';
import {loadFixture} from './outbound-load-fixture.mjs';import {ensureLoadSchema} from '../worker-v2/outbound-load-trip.js';import {database} from './d1-adapter.mjs';
const count=(s,table)=>s.DB.raw.prepare('SELECT COUNT(*) n FROM '+table).get().n;
test('one truck three orders: one labor job, per-order quantities and history, crew changes, replay and dashboard hours',async()=>{
 const s=await loadFixture(),orders=[await s.order('虚拟客户甲',10),await s.order('虚拟客户乙',2,'托'),await s.order('虚拟客户丙',8)],before=count(s,'v2_ops_jobs');
 const req=s.payload(orders),job=await s.ok('sop_native_start',req);assert.equal(count(s,'v2_ops_jobs'),before+1);assert.equal(count(s,'ck_load_order_links'),3);assert.equal(count(s,'ck_load_order_claims'),3);
 assert.equal((await s.ok('sop_native_start',req)).job_id,job.job_id);assert.equal(count(s,'v2_ops_jobs'),before+1);
 const home=(await s.ok('sop_dispatch_list')).items;assert.equal(home.length,1);for(const o of orders)assert.ok(home[0].business_no.includes(o.display_no));assert.match(home[0].customer,/甲.*乙.*丙/);
 const detail=await s.ok('v2_ops_job_detail',{job_id:job.job_id});assert.equal(detail.load_orders.length,3);assert.equal(detail.workers.length,2);assert.equal(detail.load_trip.vehicle_no,'测试车123');
 s.DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(new Date(Date.now()-30*60000).toISOString(),job.job_id);
 const a=s.DB.raw.prepare("SELECT id,joined_at FROM v2_ops_job_workers WHERE job_id=? AND worker_id='LOAD-A'").get(job.job_id),edit={job_id:job.job_id,revision:1,client_req_id:'crew-change-once',workers:[s.staff[0],{id:'LOAD-C',name:'虚拟装货丙'}],lead_id:'LOAD-A'};
 await s.ok('sop_native_people',edit);await s.ok('sop_native_people',edit);assert.deepEqual(s.DB.raw.prepare('SELECT id,joined_at FROM v2_ops_job_workers WHERE id=?').get(a.id),a);assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=?').get(job.job_id).n,3);
 s.DB.raw.prepare("UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=? AND worker_id='LOAD-C'").run(new Date(Date.now()-10*60000).toISOString(),job.job_id);
 const result=s.finishBody(job,orders);result.order_results[1].box_count=28;await s.ok('v2_outbound_load_finish',result);await s.ok('v2_outbound_load_finish',result);
 for(const [i,o]of orders.entries()){const d=await s.ok('v2_outbound_order_detail',{id:o.id});assert.equal(d.order.status,'shipped');assert.equal(d.order.actual_box_count,result.order_results[i].box_count);assert.equal(d.order.actual_pallet_count,result.order_results[i].pallet_count);assert.ok(d.jobs.some(j=>j.id===job.job_id));assert.equal(JSON.parse(d.jobs.find(j=>j.id===job.job_id).shared_result_json).box_count,result.order_results[i].box_count);assert.equal(d.load_history[0].result.box_count,result.order_results[i].box_count);}
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(job.job_id).n,1);assert.equal(count(s,'ck_load_order_claims'),0);assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at='' ").get(job.job_id).n,0);
 const minutes=s.DB.raw.prepare('SELECT SUM(minutes_worked) n FROM v2_ops_job_workers WHERE job_id=?').get(job.job_id).n;assert.ok(Math.abs(minutes-70)<.3);
 const dashboard=await s.ok('sop_dashboard');assert.equal(dashboard.person_hours,1.2,'70 person-minutes across three orders, not 210');assert.equal((await s.ok('sop_dispatch_list')).items.length,0);
});
test('an unreviewed, unissued, changed or occupied order names its reason and cannot be hidden in a multi-order request',async()=>{
 const s=await loadFixture(),a=await s.order('好单',5),bad=await s.order('未审核客户',2,'箱',{review:false}),unissued=await s.order('未下发客户',3,'箱',{issue:false});
 for(const [o,reason]of [[bad,/审核/],[unissued,/下发/]]){const r=await s.call('sop_native_start',s.payload([a,o]));assert.equal(r.ok,false);assert.ok(r.error.includes(o.display_no));assert.match(r.error,reason);assert.equal(count(s,'ck_load_trips'),0);}
 await s.ok('v2_outbound_order_update',{id:a.id,remark:'客服更新'});const d=await s.ok('v2_outbound_order_detail',{id:a.id});assert.equal(d.order.warehouse_ack_required,1);
 assert.match((await s.call('sop_native_start',s.payload([a]))).error,/最新变更/);await s.ok('v2_outbound_order_ack_change',{id:a.id,revision_no:d.order.revision_no});
 const j=await s.start([a]);const r=await s.call('sop_native_start',s.payload([unissued,a],[{id:'NEW-A',name:'其他人员'}]));assert.equal(r.ok,false);
 const occupied=await s.ok('v2_outbound_order_resolve_code',{scene:'load_trip',code:a.display_no});assert.equal(occupied.active_job_id,j.job_id);assert.match(occupied.reason,/原任务/);
 assert.equal((await s.call('v2_outbound_order_update_status',{id:a.id,status:'cancelled'})).ok,false);
});
test('no partial loads: missing, duplicate, short or invalid results preserve all order statuses and every clock',async()=>{
 const s=await loadFixture(),orders=[await s.order('全车甲',5),await s.order('全车乙',2,'托')],j=await s.start(orders),base=s.finishBody(j,orders);
 const cases=[{...base,order_results:base.order_results.slice(0,1)},{...base,order_results:[base.order_results[0],base.order_results[0]]},{...base,order_results:base.order_results.map((x,i)=>i===1?{...x,pallet_count:1}:x)},{...base,order_results:base.order_results.map((x,i)=>i===0?{...x,box_count:-1}:x)}];
 for(const body of cases){const r=await s.call('v2_outbound_load_finish',body);assert.equal(r.ok,false);assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at='' ").get(j.job_id).n,2);assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,0);for(const o of orders)assert.equal((await s.ok('v2_outbound_order_detail',{id:o.id})).order.status,'ready_to_ship');}
 assert.equal((await s.call('v2_ops_job_manual_finalize',{job_id:j.job_id,force:true})).ok,false);assert.equal((await s.call('v2_ops_job_result_update',{job_id:j.job_id,box_count:5})).ok,false);
 await s.ok('v2_outbound_load_finish',base);
});
test('per-order confirmation rechecks at finish and after a concurrent modification; no partially committed shipments',async()=>{
 const s=await loadFixture(),orders=[await s.order('确认甲',4),await s.order('确认乙',6)],j=await s.start(orders);
 await s.ok('v2_outbound_order_update',{id:orders[1].id,pickup_driver_name:'新司机'});let d=await s.ok('v2_outbound_order_detail',{id:orders[1].id});
 let r=await s.call('v2_outbound_load_finish',s.finishBody(j,orders));assert.equal(r.ok,false);assert.ok(r.error.includes(orders[1].display_no));
 await s.ok('v2_outbound_order_ack_change',{id:orders[1].id,revision_no:d.order.revision_no});assert.match((await s.call('v2_outbound_load_finish',s.finishBody(j,orders))).error,/提货信息/);await s.ok('v2_outbound_pickup_confirm',{id:orders[1].id,revision_no:d.order.revision_no});
 const batch=s.DB.batch.bind(s.DB);let injected=false;s.DB.batch=async statements=>{if(!injected&&statements.some(x=>x.sql.includes('UPDATE ck_load_order_links SET result_json'))){injected=true;s.DB.raw.prepare("UPDATE v2_outbound_orders SET updated_at='concurrent-change',revision_no=revision_no+1,warehouse_ack_required=1 WHERE id=?").run(orders[1].id);}return batch(statements);};
 r=await s.call('v2_outbound_load_finish',s.finishBody(j,orders));assert.equal(r.ok,false);assert.match(r.error,/修改|变更/);assert.ok(r.error.includes(orders[1].display_no));assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,0);
 for(const o of orders)assert.equal((await s.ok('v2_outbound_order_detail',{id:o.id})).order.status,'ready_to_ship');assert.equal(s.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at='' ").get(j.job_id).n,2);
 d=await s.ok('v2_outbound_order_detail',{id:orders[1].id});await s.ok('v2_outbound_order_ack_change',{id:orders[1].id,revision_no:d.order.revision_no});await s.ok('v2_outbound_load_finish',s.finishBody(j,orders));
});
test('overlapping concurrent starts and finishes remain atomic and idempotent, including lost-response replays',async()=>{
 const s=await loadFixture(),a=await s.order('并发甲',3),b=await s.order('并发乙',5),c=await s.order('并发丙',7),p=s.payload([a,b]);
 const first=await Promise.all([s.call('sop_native_start',p),s.call('sop_native_start',p)]);assert.ok(first.every(x=>x.ok));assert.equal(first[0].job_id,first[1].job_id);assert.equal(count(s,'ck_load_trips'),1);
 const blocked=await s.call('sop_native_start',s.payload([b,c],[{id:'OTHER',name:'另一车'}]));assert.equal(blocked.ok,false);assert.ok(blocked.error.includes(b.display_no));assert.equal(count(s,'ck_load_trips'),1);
 const mismatch=await s.call('sop_native_start',{...p,payload:{...p.payload,order_ids:[a.id]}});assert.equal(mismatch.ok,false);
 const j=first[0],f=s.finishBody(j,[a,b]),finish=await Promise.all([s.call('v2_outbound_load_finish',f),s.call('v2_outbound_load_finish',f)]);assert.ok(finish.every(x=>x.ok));assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,1);
 assert.equal((await s.ok('v2_outbound_order_detail',{id:c.id})).order.status,'issued');assert.equal((await s.ok('sop_native_start',p)).job_id,j.job_id);
});
test('start transaction failures, source-work races and worker conflicts create no half-truck or duplicate time',async()=>{
 const s=await loadFixture(),a=await s.order('回滚甲',4),b=await s.order('回滚乙',9),before=count(s,'v2_ops_jobs');
 s.DB.raw.exec("CREATE TRIGGER fail_load_fixture BEFORE INSERT ON ck_load_order_links WHEN NEW.position=1 BEGIN SELECT RAISE(ABORT,'fixture_storage_failure'); END");const denied=await s.call('sop_native_start',s.payload([a,b]));assert.equal(denied.ok,false);assert.equal(count(s,'ck_load_trips'),0);assert.equal(count(s,'v2_ops_jobs'),before);s.DB.raw.exec('DROP TRIGGER fail_load_fixture');
 const batch=s.DB.batch.bind(s.DB);let injected=false;s.DB.batch=async statements=>{if(!injected&&statements.some(x=>x.sql.startsWith('INSERT INTO ck_load_trips'))){injected=true;s.DB.raw.prepare('UPDATE sop_records SET revision=revision+1 WHERE id=?').run(b.need_id);}return batch(statements);};
 assert.equal((await s.call('sop_native_start',s.payload([a,b]))).ok,false);assert.equal(count(s,'ck_load_trips'),0);assert.equal(count(s,'ck_load_order_claims'),0);
 const j=await s.start([a]);assert.equal((await s.call('sop_native_start',s.payload([b]))).ok,false);assert.equal(count(s,'ck_load_trips'),1);await s.ok('v2_outbound_load_finish',s.finishBody(j,[a]));
});
test('single-order payloads and existing single-order history still use their original order and finish correctly',async()=>{
 const s=await loadFixture(),o=await s.order('单单兼容',6),p=s.payload([o]);delete p.payload.order_ids;p.payload.order_id=o.id;const j=await s.ok('sop_native_start',p);
 await s.ok('v2_outbound_load_finish',{job_id:j.job_id,worker_id:s.staff[0].id,box_count:6,pallet_count:0,complete_job:true});assert.equal((await s.ok('v2_outbound_order_detail',{id:o.id})).order.actual_box_count,6);
 const old=await s.order('旧任务兼容',4),id='LEGACY-LOAD';s.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,status,created_at,updated_at) VALUES(?,'load_outbound','outbound_order',?,'working',?,?)").run(id,old.id,new Date().toISOString(),new Date().toISOString());
 s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(id,'dispatch',1,'bulk',JSON.stringify({owner_id:'staging-demo-manager',owner:'测试负责人',workers:s.staff,lead_id:s.staff[0].id}),new Date().toISOString());
 s.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)').run('OLD-SEG',id,s.staff[0].id,s.staff[0].name,new Date().toISOString());
 const d=await s.ok('v2_ops_job_detail',{job_id:id});assert.equal(d.load_trip,null);assert.equal(d.load_orders[0].order.id,old.id);assert.equal((await s.call('v2_outbound_load_finish',{job_id:id,worker_id:s.staff[0].id,complete_job:true,box_count:3})).ok,false);await s.ok('v2_outbound_load_finish',{job_id:id,worker_id:s.staff[0].id,complete_job:true,box_count:4});assert.equal((await s.ok('v2_outbound_order_detail',{id:old.id})).order.status,'shipped');await s.ok('v2_outbound_load_finish',{job_id:id,worker_id:s.staff[0].id,complete_job:true,box_count:4});assert.equal(count(s,'ck_load_trips'),1);
});
test('new schema is staging-only and initializing it leaves existing data untouched',async()=>{
 const DB=database();DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status) VALUES('HISTORICAL','bulk_op','completed')").run();
 await ensureLoadSchema({DB,SOP_ENVIRONMENT:'production',SOP_UPGRADE_ENABLED:'true'});assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='ck_load_trips'").get().n,0);
 await ensureLoadSchema({DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true'});assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);assert.equal(DB.raw.prepare("SELECT status FROM v2_ops_jobs WHERE id='HISTORICAL'").get().status,'completed');
});
test('a legacy single-order completion race rolls back result and worker time, and reports the changed public number',async()=>{
 const s=await loadFixture(),o=await s.order('旧单并发兼容',4),id='LEGACY-RACE',t=new Date().toISOString();
 s.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,status,created_at,updated_at) VALUES(?,'load_outbound','outbound_order',?,'working',?,?)").run(id,o.id,t,t);
 s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(id,'dispatch',1,'bulk',JSON.stringify({owner_id:'staging-demo-manager',owner:'测试负责人',workers:s.staff,lead_id:s.staff[0].id}),t);
 s.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)').run('RACE-SEG',id,s.staff[0].id,s.staff[0].name,t);
 const batch=s.DB.batch.bind(s.DB);let changed=false;s.DB.batch=async sql=>{if(!changed&&sql.some(q=>q.sql.includes("UPDATE v2_outbound_orders SET status='shipped'"))){changed=true;s.DB.raw.prepare('UPDATE v2_outbound_orders SET warehouse_ack_required=1,revision_no=revision_no+1 WHERE id=?').run(o.id);}return batch(sql);};
 const result=await s.call('v2_outbound_load_finish',{job_id:id,worker_id:s.staff[0].id,complete_job:true,box_count:4});assert.equal(result.ok,false);assert.ok(result.error.includes(o.display_no));assert.match(result.error,/最新变更/);
 assert.equal(s.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(id).status,'working');assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(id).n,0);assert.equal(s.DB.raw.prepare('SELECT left_at FROM v2_ops_job_workers WHERE job_id=?').get(id).left_at,'');
});
