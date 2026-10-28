import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
const codes=['301000009401','301000009402'];
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json(),ok=async(a,b={})=>{const r=await call(a,b);assert.equal(r.ok,true,JSON.stringify(r));return r;};
 await ok('sop_identity');const person=(await ok('sop_attendance_checkin',{name:'Synthetic return scanner',agency:'가온'})).record,batch=(await ok('sop_courier_receive',{operation:'start',workers:[{id:person.badgeId,name:person.name}],lead_id:person.badgeId})).batch;
 const payload={customer:'Synthetic compatible return',biz_classes:['direct_ship'],external_inbound_no:'SYNTHETIC-COURIER-RETURN',lines:[{unit_type:'courier',tracking_nos:codes}]};
 const plan=await ok('v2_inbound_plan_create',payload),scan=code=>call('sop_courier_receive',{owner:'8-1',tracking_no:code,batch_id:batch.id,scanner_badge:person.badgeId});
 const rows=(table,where='',...args)=>DB.raw.prepare('SELECT * FROM '+table+(where?' WHERE '+where:'')+' ORDER BY rowid').all(...args);
 const freeze=async()=>{
  const first=await scan(codes[0]);assert.equal(first.ok,true,first.error);await ok('v2_inbound_plan_cancel',{inbound_plan_id:plan.id,reason:'Synthetic cancellation'});assert.equal(rows('ck_courier_plan_item_history').length,0,'stage A must not begin writing cancellation snapshots');
  const line=rows('v2_inbound_plan_lines','plan_id=?',plan.id)[0];
  for(const [i,code] of codes.entries())DB.raw.prepare('INSERT INTO ck_courier_plan_item_history VALUES(?,?,?,?,?,?,?,?)').run(code,plan.id,line.id,'2026-10-01T00:00:00.000Z','','SYNTHETIC-CANCEL','Synthetic cancellation',JSON.stringify(i?{}:{receipt_id:first.item.id,owner:first.item.owner,received_at:first.item.received_at,scanner_name:first.item.scanner_name,status:first.item.status,scanner_id:first.item.scanner_id,batch_id:first.item.batch_id,batch_no:first.item.batch_no}));
  const late={...rows('ck_courier_receipts')[0],id:'SYNTHETIC-LATE-RECEIPT',tracking_no:codes[1]},keys=Object.keys(late);DB.raw.prepare('INSERT INTO ck_courier_receipts('+keys.join(',')+') VALUES('+keys.map(()=>'?').join(',')+')').run(...Object.values(late));DB.raw.prepare('INSERT INTO ck_courier_batch_items VALUES(?,?)').run(late.id,batch.id);
  DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run('SYNTHETIC-PENDING-NEED','need',1,'direct_ship',JSON.stringify({id:'SYNTHETIC-PENDING-NEED',source_type:'inbound',source_id:plan.id,status:'pending',title:'Synthetic unstarted requirement'}),new Date().toISOString());
 };
 return {DB,env,call,ok,plan,payload,scan,rows,freeze};
}
test('compatibility stage preserves old cancellation behavior and creates no new snapshot schema',async t=>{
 const f=await fixture(t);await f.ok('v2_inbound_plan_cancel',{inbound_plan_id:f.plan.id});assert.equal(f.rows('ck_courier_plan_item_history').length,0);assert.equal((await f.scan(codes[0])).ok,false);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sqlite_master WHERE name IN ('ck_courier_active_plan_items','ck_courier_inactive_snapshot')").get().n,0);
});
test('return reader freezes 1/2 while safe reconstruction reaches 2/2 and archives the original need once',async t=>{
 const f=await fixture(t);await f.freeze();const original=f.rows('v2_inbound_plans','id=?',f.plan.id),lines=f.rows('v2_inbound_plan_lines','plan_id=?',f.plan.id),history=f.rows('ck_courier_plan_item_history'),receipts=f.rows('ck_courier_receipts');
 const progress=(await f.ok('v2_inbound_plan_detail',{id:f.plan.id})).plan.courier_progress;assert.equal(progress.total,2);assert.equal(progress.received,1);
 const replacement=await f.ok('v2_inbound_plan_create',f.payload);assert.equal((await f.ok('v2_inbound_plan_detail',{id:replacement.id})).lines[0].actual_qty,2);assert.equal((await f.ok('v2_inbound_plan_detail',{id:f.plan.id})).plan.courier_progress.received,1);
 assert.deepEqual(f.rows('v2_inbound_plans','id=?',f.plan.id),original);assert.deepEqual(f.rows('v2_inbound_plan_lines','plan_id=?',f.plan.id),lines);assert.deepEqual(f.rows('ck_courier_receipts'),receipts);
 assert.deepEqual(f.rows('ck_courier_plan_item_history').map(x=>({...x,replacement_plan_id:''})),history.map(x=>({...x})));assert.ok(f.rows('ck_courier_plan_item_history').every(x=>x.replacement_plan_id===replacement.id));assert.equal(JSON.parse(f.rows('sop_records','id=?','SYNTHETIC-PENDING-NEED')[0].state).status,'cancelled');assert.equal(f.rows('sop_events',"action='sop_inbound_replaced'").length,1);
});
test('future snapshot replacement rollback and same-request retry remain atomic',async t=>{
 const f=await fixture(t);await f.freeze();const history=f.rows('ck_courier_plan_item_history'),need=f.rows('sop_records','id=?','SYNTHETIC-PENDING-NEED'),request={...f.payload,client_req_id:crypto.randomUUID()};
 f.DB.raw.exec("CREATE TRIGGER synthetic_replacement_failure BEFORE INSERT ON ck_courier_plan_items BEGIN SELECT RAISE(ABORT,'synthetic replacement failure'); END");assert.equal((await f.call('v2_inbound_plan_create',request)).ok,false);assert.deepEqual(f.rows('ck_courier_plan_item_history'),history);assert.deepEqual(f.rows('sop_records','id=?','SYNTHETIC-PENDING-NEED'),need);assert.equal(f.rows('sop_events',"action='sop_inbound_replaced'").length,0);
 f.DB.raw.exec('DROP TRIGGER synthetic_replacement_failure');const replacement=await f.ok('v2_inbound_plan_create',request);assert.equal((await f.ok('v2_inbound_plan_create',request)).id,replacement.id);assert.equal(f.rows('ck_courier_receipts').length,2);
});
test('competing snapshot replacements have one winner and retain unsafe-history guards',async t=>{
 const f=await fixture(t);await f.freeze();f.DB.raw.prepare('UPDATE v2_inbound_plans SET accounted=1 WHERE id=?').run(f.plan.id);const before=f.rows('ck_courier_plan_item_history');assert.equal((await f.call('v2_inbound_plan_create',f.payload)).ok,false);assert.deepEqual(f.rows('ck_courier_plan_item_history'),before);
 f.DB.raw.prepare('UPDATE v2_inbound_plans SET accounted=0 WHERE id=?').run(f.plan.id);const replies=await Promise.all([f.call('v2_inbound_plan_create',f.payload),f.call('v2_inbound_plan_create',f.payload)]);assert.equal(replies.filter(x=>x.ok).length,1);assert.equal(f.rows('ck_courier_plan_items').length,2);assert.equal(f.rows('ck_courier_receipts').length,2);assert.equal(f.rows('sop_events',"action='sop_inbound_replaced'").length,1);
});
