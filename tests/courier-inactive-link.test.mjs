import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
const A='301000008601',B='301000008602';
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data={})=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
 await ok('sop_identity');
 const person=(await ok('sop_attendance_checkin',{name:'Synthetic parcel scanner',agency:'가온'})).record;
 const batch=(await ok('sop_courier_receive',{operation:'start',workers:[{id:person.badgeId,name:person.name}],lead_id:person.badgeId})).batch;
 const payload=(codes=[A])=>({customer:'Synthetic inactive link',biz_classes:['direct_ship'],external_inbound_no:'SYNTHETIC-INACTIVE-'+codes.join('-'),lines:[{unit_type:'courier',tracking_nos:codes,planned_qty:codes.length}]});
 const create=async(codes=[A])=>ok('v2_inbound_plan_create',payload(codes));
 const cancel=id=>ok('v2_inbound_plan_cancel',{inbound_plan_id:id,reason:'Synthetic cancellation'});
 const scan=(code=A,extra={})=>call('sop_courier_receive',{owner:'8-1',tracking_no:code,batch_id:batch.id,scanner_badge:person.badgeId,...extra});
 const rows=(table,where='',...args)=>DB.raw.prepare('SELECT * FROM '+table+(where?' WHERE '+where:'')+(table==='ck_courier_active_plan_items'?' ORDER BY tracking_no':' ORDER BY rowid')).all(...args);
 const counts=()=>Object.fromEntries(['ck_courier_receipts','ck_courier_events','ck_courier_batch_items','v2_ops_job_results','v2_outbound_orders'].map(table=>[table,rows(table).length]));
 return {DB,env,call,ok,payload,create,cancel,scan,rows,counts,batch,person};
}
test('cancelled inbound association does not prevent a first parcel receipt or change the cancelled plan',async t=>{
 const f=await fixture(t),p=await f.create();await f.cancel(p.id);
 const before={plan:f.rows('v2_inbound_plans','id=?',p.id),lines:f.rows('v2_inbound_plan_lines','plan_id=?',p.id)};
 const received=await f.scan();assert.equal(received.ok,true,received.error);assert.equal(received.duplicate,false);assert.equal(received.item.plan_id,null);assert.equal(received.plan,null);
 assert.deepEqual({plan:f.rows('v2_inbound_plans','id=?',p.id),lines:f.rows('v2_inbound_plan_lines','plan_id=?',p.id)},before);
 assert.deepEqual(f.counts(),{ck_courier_receipts:1,ck_courier_events:1,ck_courier_batch_items:1,v2_ops_job_results:0,v2_outbound_orders:0});
 assert.equal(f.rows('ck_courier_active_plan_items','plan_id=?',p.id).length,0);
 const history=f.rows('ck_courier_plan_item_history','plan_id=?',p.id);assert.equal(history.length,1);assert.equal(history[0].replacement_plan_id,'');assert.equal(JSON.parse(history[0].receipt_json).receipt_id,undefined);
 assert.equal((await f.ok('v2_inbound_plan_detail',{id:p.id})).plan.courier_progress.received,0);
});
function legacyCancel(f,id){
 const sql=f.DB.raw.prepare("SELECT sql FROM sqlite_master WHERE name='ck_courier_inactive_snapshot'").get().sql;
 f.DB.raw.exec('DROP TRIGGER ck_courier_inactive_snapshot');try{f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").run(id);}finally{f.DB.raw.exec(sql);}
}
for(const kind of ['cancelled','soft-deleted','deleted-status','orphan'])test(kind+' historical association is ignored without discarding its history',async t=>{
 const f=await fixture(t);let id;
 if(kind==='orphan'){
  const sql=f.DB.raw.prepare("SELECT sql FROM sqlite_master WHERE name='ck_courier_claim_insert_guard'").get().sql;
  f.DB.raw.exec('DROP TRIGGER ck_courier_claim_insert_guard');try{f.DB.raw.prepare('INSERT INTO ck_courier_plan_items VALUES(?,?,?)').run(A,'SYNTHETIC-MISSING','SYNTHETIC-MISSING-LINE');}finally{f.DB.raw.exec(sql);}id='SYNTHETIC-MISSING';
 }else{const p=await f.create();id=p.id;if(kind==='cancelled')legacyCancel(f,id);else f.DB.raw.prepare(kind==='soft-deleted'?'UPDATE v2_inbound_plans SET is_deleted=1 WHERE id=?':"UPDATE v2_inbound_plans SET status='deleted' WHERE id=?").run(id);}
 const before=f.rows('v2_inbound_plans'),scan=await f.scan();assert.equal(scan.ok,true,scan.error);assert.equal(scan.item.plan_id,null);assert.equal(scan.plan,null);assert.deepEqual(f.rows('v2_inbound_plans'),before);
 assert.equal(f.rows('ck_courier_plan_item_history','plan_id=?',id).length,1);assert.equal(f.rows('ck_courier_plan_items','plan_id=?',id).length,1,'retain the historical reference and reconstruction safety guard');
 const duplicate=await f.scan();assert.equal(duplicate.ok,true,duplicate.error);assert.equal(duplicate.duplicate,true);assert.equal(duplicate.item.id,scan.item.id);assert.equal(f.rows('ck_courier_events').length,1);assert.equal(f.rows('ck_courier_batch_items').length,1);
});
test('a receipt saved before cancellation retains its identity, batch, owner and frozen historical progress',async t=>{
 const f=await fixture(t),p=await f.create(),first=await f.scan();assert.equal(first.ok,true,first.error);await f.cancel(p.id);
 const history=f.rows('ck_courier_plan_item_history'),receipt=f.rows('ck_courier_receipts');
 const second=await f.scan(A,{owner:'8-2'});assert.equal(second.ok,true,second.error);assert.equal(second.duplicate,true);assert.equal(second.owner_conflict,true);assert.equal(second.item.id,first.item.id);assert.equal(second.item.batch_id,first.item.batch_id);
 assert.deepEqual(f.rows('ck_courier_receipts'),receipt);assert.deepEqual(f.rows('ck_courier_plan_item_history'),history);assert.equal(f.rows('ck_courier_events').length,1);
 assert.equal((await f.ok('v2_inbound_plan_detail',{id:p.id})).plan.courier_progress.received,1);
 const list=await f.ok('sop_courier_list',{keyword:A});assert.equal(list.items[0].plan_id,null);
});
test('concurrent repeat scans after cancellation count one immutable receipt and one batch item',async t=>{
 const f=await fixture(t),p=await f.create();await f.cancel(p.id);
 const replies=await Promise.all(Array.from({length:8},(_,i)=>f.scan(A,{owner:i%2?'8-2':'8-1'})));assert.ok(replies.every(r=>r.ok),JSON.stringify(replies));assert.equal(replies.filter(r=>!r.duplicate).length,1);assert.equal(new Set(replies.map(r=>r.item.id)).size,1);
 assert.deepEqual(f.counts(),{ck_courier_receipts:1,ck_courier_events:1,ck_courier_batch_items:1,v2_ops_job_results:0,v2_outbound_orders:0});
 assert.equal((await f.ok('sop_courier_detail',{id:replies[0].item.id})).item.owner,replies.find(r=>!r.duplicate).item.owner);
 const batch=await f.ok('sop_courier_detail',{batch_id:f.batch.id});assert.equal(batch.batch.total,1);
});
test('historical execution and outbound facts do not block receipt and still forbid unsafe reconstruction',async t=>{
 const f=await fixture(t),p=await f.create();
 f.DB.raw.prepare('UPDATE v2_inbound_plans SET accounted=1 WHERE id=?').run(p.id);f.DB.raw.prepare('UPDATE v2_inbound_plan_lines SET putaway_qty=1 WHERE plan_id=?').run(p.id);
 f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,source_inbound_plan_id,status,customer) VALUES('SYNTHETIC-OUTBOUND',?,'pending','Synthetic historical order')").run(p.id);
 f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,status) VALUES('SYNTHETIC-HISTORY','unload','inbound_plan',?,'completed')").run(p.id);
 f.DB.raw.prepare("INSERT INTO v2_ops_job_results(id,job_id,result_json) VALUES('SYNTHETIC-RESULT','SYNTHETIC-HISTORY','{}')").run();
 legacyCancel(f,p.id);const tables=['v2_inbound_plans','v2_inbound_plan_lines','v2_ops_jobs','v2_ops_job_results','v2_outbound_orders'],before=Object.fromEntries(tables.map(x=>[x,f.rows(x)]));
 const scan=await f.scan();assert.equal(scan.ok,true,scan.error);assert.deepEqual(Object.fromEntries(tables.map(x=>[x,f.rows(x)])),before);
 const retry=await f.call('v2_inbound_plan_create',f.payload());assert.equal(retry.ok,false);assert.deepEqual(Object.fromEntries(tables.map(x=>[x,f.rows(x)])),before);
});
test('an active replacement receives the parcel while the cancelled history stays unreceived',async t=>{
 const f=await fixture(t),old=await f.create();await f.cancel(old.id);const received=await f.scan();assert.equal(received.ok,true,received.error);
 const replacement=await f.create(),d=await f.ok('v2_inbound_plan_detail',{id:replacement.id});assert.equal(d.plan.courier_progress.received,1);assert.equal(d.lines[0].actual_qty,1);
 const oldDetail=await f.ok('v2_inbound_plan_detail',{id:old.id});assert.equal(oldDetail.plan.status,'cancelled');assert.equal(oldDetail.plan.courier_progress.received,0);
 const duplicate=await f.scan();assert.equal(duplicate.duplicate,true);assert.equal(duplicate.item.plan_id,replacement.id);assert.equal(f.rows('ck_courier_receipts').length,1);
 assert.equal(f.rows('ck_courier_plan_item_history')[0].replacement_plan_id,replacement.id);
});
for(const recreate of [false,true])test('cancellation'+(recreate?' and recreation':'')+' after scanner preflight preserves the winner and permits receipt',async t=>{
 const f=await fixture(t),old=await f.create(),batch=f.DB.batch.bind(f.DB);let injected=false,replacement;
 f.DB.batch=async ss=>{if(!injected&&ss.some(s=>s.sql.includes('INSERT OR IGNORE INTO ck_courier_receipts'))){injected=true;await f.cancel(old.id);if(recreate)replacement=await f.create();}return batch(ss);};
 const received=await f.scan();assert.equal(injected,true);assert.equal(received.ok,true,received.error);assert.equal(received.item.plan_id,replacement?.id||null);
 const d=await f.ok('v2_inbound_plan_detail',{id:old.id});assert.equal(d.plan.status,'cancelled');assert.equal(d.lines[0].actual_qty,0);assert.equal(d.plan.courier_progress.received,0);
 assert.equal(f.rows('ck_courier_receipts').length,1);assert.equal(f.rows('ck_courier_batch_items').length,1);
});
test('receipt failure rolls back lazy history, receipt, event and batch count; retry succeeds once',async t=>{
 const f=await fixture(t),old=await f.create();legacyCancel(f,old.id);assert.equal(f.rows('ck_courier_plan_item_history').length,0);
 f.DB.raw.exec("CREATE TRIGGER synthetic_receipt_failure BEFORE INSERT ON ck_courier_events WHEN NEW.kind='receive' BEGIN SELECT RAISE(ABORT,'synthetic receive failure'); END");
 const request=crypto.randomUUID(),failed=await f.scan(A,{client_req_id:request});assert.equal(failed.ok,false);assert.equal(f.rows('ck_courier_plan_item_history').length,0);assert.equal(f.rows('ck_courier_receipts').length,0);assert.equal(f.rows('ck_courier_batch_items').length,0);
 f.DB.raw.exec('DROP TRIGGER synthetic_receipt_failure');const retry=await f.scan(A,{client_req_id:request});assert.equal(retry.ok,true,retry.error);assert.equal(retry.duplicate,false);assert.equal((await f.scan(A,{client_req_id:request})).duplicate,true);assert.equal(f.rows('ck_courier_events').length,1);
});
test('failed cancellation cannot leave a hidden link or a partial historical snapshot',async t=>{
 const f=await fixture(t),p=await f.create();f.DB.raw.exec("CREATE TRIGGER synthetic_cancel_failure BEFORE INSERT ON ck_courier_plan_item_history BEGIN SELECT RAISE(ABORT,'synthetic cancel failure'); END");
 assert.equal((await f.call('v2_inbound_plan_cancel',{inbound_plan_id:p.id})).ok,false);assert.equal(f.rows('v2_inbound_plans','id=?',p.id)[0].status,'pending');assert.equal(f.rows('ck_courier_active_plan_items').length,1);assert.equal(f.rows('ck_courier_plan_item_history').length,0);
 f.DB.raw.exec('DROP TRIGGER synthetic_cancel_failure');await f.cancel(p.id);assert.equal(f.rows('ck_courier_active_plan_items').length,0);assert.equal((await f.scan()).ok,true);
});
test('inactive plan does not mask purchase ownership checks or cause purchase inventory receipt',async t=>{
 const f=await fixture(t),p=await f.create();await f.cancel(p.id);f.DB.raw.prepare("INSERT INTO v2_003_purchase_shipments(id,tracking_no,status) VALUES('SYNTHETIC-PURCHASE',?,'pending')").run(A);
 assert.equal((await f.scan()).ok,false);const received=await f.scan(A,{owner:'supplies'});assert.equal(received.ok,true,received.error);assert.equal(received.item.plan_id,null);assert.equal(received.item.shipment_id,'SYNTHETIC-PURCHASE');assert.equal(f.rows('v2_003_purchase_shipments')[0].status,'pending');
});
for(const change of ["status='cancelled'","status='deleted'",'is_deleted=1'])test('plan becomes inactive between receipt and arrival synchronization: '+change,async t=>{
 const f=await fixture(t),p=await f.create(),prepare=f.DB.prepare.bind(f.DB);let injected=false,frozen;
 f.DB.prepare=sql=>{const s=prepare(sql);if(sql.startsWith('UPDATE v2_inbound_plan_lines SET actual_qty=')){const run=s.run;s.run=async()=>{if(!injected){injected=true;f.DB.raw.prepare('UPDATE v2_inbound_plans SET '+change+' WHERE id=?').run(p.id);frozen={plans:f.rows('v2_inbound_plans'),lines:f.rows('v2_inbound_plan_lines'),tasks:f.rows('v2_inbound_plan_biz_tasks')};}return run();};}return s;};
 const received=await f.scan();assert.equal(injected,true);assert.equal(received.ok,true,received.error);assert.equal(received.plan,null);assert.equal(received.item.plan_id,null);assert.deepEqual({plans:f.rows('v2_inbound_plans'),lines:f.rows('v2_inbound_plan_lines'),tasks:f.rows('v2_inbound_plan_biz_tasks')},frozen);assert.equal((await f.scan()).duplicate,true);assert.equal(f.rows('ck_courier_events').length,1);
});
