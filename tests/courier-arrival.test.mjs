import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {handleCourier} from '../worker-v2/courier.js';
import {digest} from '../worker-v2/access-control.js';
import {database} from './d1-adapter.mjs';

const A='301000000101', B='50000000000102', C='EZ100000002CN';
const receiptFields=['receipt_id','owner','received_at','scanner_id','scanner_name','batch_id','batch_no'];
const planFields=['plan_id','display_no','plan_status'];
const businessTables=[
 'ck_courier_receipts','ck_courier_events','ck_courier_batches','ck_courier_batch_items','ck_courier_batch_requests',
 'ck_courier_plan_items','ck_courier_plan_item_history','v2_inbound_plans','v2_inbound_plan_lines',
 'v2_inbound_plan_biz_tasks','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','sop_records','sop_events'
];
const snapshot=DB=>Object.fromEntries(businessTables.map(table=>[table,DB.raw.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));

async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const request=async(action,data={})=>{
  const res=await worker.fetch(new Request('https://fixture.local/api',{
   method:'POST',headers:{'Content-Type':'application/json',Origin:'https://fixture.local'},
   body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})
  }),env);
  return res.json();
 };
 const ok=async(action,data={})=>{const r=await request(action,data);assert.equal(r.ok,true,r.message||r.error);return r;};
 const plan=(codes,extra={})=>ok('v2_inbound_plan_create',{
  customer:'Synthetic arrival lookup',biz_classes:['direct_ship'],external_inbound_no:'LOOKUP-'+crypto.randomUUID(),
  lines:[{unit_type:'courier',planned_qty:codes.length,tracking_nos:codes}],...extra
 });
 let batch,scanner;
 const scan=async(code,owner='8-1')=>{
  if(!batch){
   const person=(await ok('sop_attendance_checkin',{name:'Synthetic scanner',agency:'가온'})).record;
   scanner={id:person.badgeId,name:person.name};
   batch=(await ok('sop_courier_receive',{operation:'start',workers:[scanner],lead_id:scanner.id})).batch;
  }
  return ok('sop_courier_receive',{tracking_no:code,owner,batch_id:batch.id,scanner_badge:scanner.id});
 };
 await ok('sop_courier_config');
 return {DB,env,request,ok,plan,scan};
}

test('arrival lookup normalizes array and text input, deduplicates, preserves request order and never invents receipts',async()=>{
 const f=await fixture(),before=snapshot(f.DB);
 for(const input of [[B,'３０１０－００００－０１０１',B,' ez100000002 cn '],`${B}\n３０１０－００００－０１０１，${B}; ez100000002 cn `]){
  const r=await f.ok('sop_courier_lookup',{tracking_nos:input});
  assert.equal(r.total,3);assert.equal(r.received,0);assert.equal(r.arrival_status,'pending');
  assert.deepEqual(r.items.map(item=>item.tracking_no),[B,A,C]);
  for(const item of r.items){
   for(const field of [...receiptFields,...planFields])assert.equal(item[field],null,field);
   assert.equal(item.claim_status,'available');
  }
 }
 assert.deepEqual(snapshot(f.DB),before,'a lookup must not write business data');
});

test('arrival lookup returns receipt, scanner and batch metadata with an independent partial/all-arrived summary',async()=>{
 const f=await fixture(),p=await f.plan([A,B]),first=await f.scan(A,'8-3');
 f.DB.raw.prepare('UPDATE ck_courier_receipts SET received_at=? WHERE id=?').run('2026-09-21T15:00:00.000Z',first.item.id);
 const before=snapshot(f.DB),partial=await f.ok('sop_courier_lookup',{tracking_nos:[B,A,C]});
 assert.equal(partial.total,3);assert.equal(partial.received,1);assert.equal(partial.arrival_status,'partial');
 assert.deepEqual(partial.items.map(item=>item.tracking_no),[B,A,C]);
 const [missing,received,unknown]=partial.items;
 for(const field of receiptFields)assert.equal(missing[field],null,field);
 for(const item of [missing,received]){
  assert.equal(item.plan_id,p.id);assert.equal(item.display_no,p.display_no);
  assert.equal(item.plan_status,'pending');assert.equal(item.claim_status,'conflict');
 }
 assert.equal(received.receipt_id,first.item.id);assert.equal(received.owner,'8-3');
 assert.equal(received.received_at,'2026-09-21T15:00:00.000Z');
 assert.equal(received.scanner_id,first.item.scanner_id);assert.equal(received.scanner_name,first.item.scanner_name);
 assert.equal(received.batch_id,first.item.batch_id);assert.equal(received.batch_no,first.batch.batch_no);
 assert.equal(unknown.claim_status,'available');
 for(const field of [...receiptFields,...planFields])assert.equal(unknown[field],null,field);
 assert.deepEqual(snapshot(f.DB),before);
 await f.scan(B,'8-2');
 const all=await f.ok('sop_courier_lookup',{tracking_nos:[A,B]});
 assert.equal(all.received,2);assert.equal(all.arrival_status,'arrived');
 assert.equal(all.items[0].plan_status,'arrived_pending_putaway','arrival alone does not complete a plan that requires putaway');
});

test('arrival lookup includes historical receipts with no batch or plan without manufacturing associations',async()=>{
 const f=await fixture();
 f.DB.raw.prepare(`INSERT INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name)
  VALUES('HISTORICAL-LOOKUP',?,'8-4','2026-01-01T03:04:05.000Z','OLD-SCANNER','Historical scanner','OLD-ACTOR','Historical actor')`).run(A);
 const before=snapshot(f.DB),r=await f.ok('sop_courier_lookup',{tracking_nos:[A]});
 assert.equal(r.received,1);assert.equal(r.arrival_status,'arrived');
 assert.deepEqual(r.items[0],{
  tracking_no:A,receipt_id:'HISTORICAL-LOOKUP',owner:'8-4',received_at:'2026-01-01T03:04:05.000Z',
  scanner_id:'OLD-SCANNER',scanner_name:'Historical scanner',batch_id:null,batch_no:null,
  plan_id:null,display_no:null,plan_status:null,claim_status:'available'
 });
 assert.deepEqual(snapshot(f.DB),before);
});

test('arrival lookup follows a corrected tracking number and still counts handed-over physical receipts',async()=>{
 const f=await fixture(),scanned=await f.scan(A,'8-4');
 f.DB.raw.prepare('UPDATE ck_courier_receipts SET received_at=? WHERE id=?').run('2026-09-19T08:09:10.000Z',scanned.item.id);
 const original=(await f.ok('sop_courier_lookup',{tracking_nos:[A]})).items[0];
 const corrected=await f.ok('sop_courier_update',{id:scanned.item.id,version:scanned.item.version,tracking_no:B});
 assert.equal(corrected.item.tracking_no,B);
 const before=snapshot(f.DB),r=await f.ok('sop_courier_lookup',{tracking_nos:[A,B]});
 assert.equal(r.total,2);assert.equal(r.received,1);assert.equal(r.arrival_status,'partial');
 assert.deepEqual(r.items.map(item=>item.tracking_no),[A,B]);
 for(const field of receiptFields){
  assert.equal(r.items[0][field],null,'superseded tracking number: '+field);
  assert.equal(r.items[1][field],original[field],'corrected tracking number: '+field);
 }
 assert.equal(r.items[0].claim_status,'available');
 const oldOnly=await f.ok('sop_courier_lookup',{tracking_nos:[A]});
 assert.equal(oldOnly.received,0);assert.equal(oldOnly.arrival_status,'pending');
 assert.deepEqual(snapshot(f.DB),before);
 await f.ok('sop_courier_update',{id:scanned.item.id,version:corrected.item.version,status:'handed_over',handed_to:'Synthetic receiver'});
 assert.equal(f.DB.raw.prepare('SELECT status FROM ck_courier_receipts WHERE id=?').get(scanned.item.id).status,'handed_over');
 const handedOverSnapshot=snapshot(f.DB),handedOver=await f.ok('sop_courier_lookup',{tracking_nos:[B]});
 assert.equal(handedOver.total,1);assert.equal(handedOver.received,1);assert.equal(handedOver.arrival_status,'arrived');
 for(const field of receiptFields)assert.equal(handedOver.items[0][field],original[field],field);
 assert.deepEqual(snapshot(f.DB),handedOverSnapshot);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_courier_receipts').get().n,1);
});

test('arrival lookup distinguishes reusable cancelled claims, including receipt-only arrival, from live or unsafe claims',async()=>{
 const f=await fixture(),codes=[A,B,C,'301000000104','301000000105'];
 const live=await f.plan([codes[0]]),safe=await f.plan([codes[1]]),unsafe=await f.plan([codes[2]]),received=await f.plan([codes[3]]);
 for(const p of [safe,unsafe])await f.ok('v2_inbound_plan_cancel',{inbound_plan_id:p.id,reason:'Synthetic lookup cancellation'});
 f.DB.raw.prepare('UPDATE v2_inbound_plans SET accounted=1 WHERE id=?').run(unsafe.id);
 await f.scan(codes[3]);
 await f.ok('v2_inbound_plan_cancel',{inbound_plan_id:received.id,reason:'Synthetic received cancellation'});
 // Simulate an orphan predating the claim guard, then restore the exact production trigger.
 const insertGuard=f.DB.raw.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='ck_courier_claim_insert_guard'").get().sql;
 f.DB.raw.exec('DROP TRIGGER ck_courier_claim_insert_guard');
 try{f.DB.raw.prepare('INSERT INTO ck_courier_plan_items(tracking_no,plan_id,line_id) VALUES(?,?,?)').run(codes[4],'MISSING-PLAN','MISSING-LINE');}
 finally{f.DB.raw.exec(insertGuard);}
 assert.equal(f.DB.raw.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='ck_courier_claim_insert_guard'").get().sql,insertGuard);
 const before=snapshot(f.DB),r=await f.ok('sop_courier_lookup',{tracking_nos:codes});
 assert.deepEqual(r.items.map(item=>item.claim_status),['conflict','reusable','conflict','reusable','conflict']);
 assert.deepEqual(r.items.slice(0,4).map(item=>item.plan_id),[live.id,safe.id,unsafe.id,received.id]);
 assert.equal(r.items[1].plan_status,'cancelled');assert.equal(r.items[1].receipt_id,null);
 assert.equal(r.items[3].plan_status,'cancelled');assert.ok(r.items[3].receipt_id);
 assert.equal(r.items[4].plan_id,'MISSING-PLAN');
 assert.deepEqual(snapshot(f.DB),before,'checking reuse must not release any old claim');
});

test('arrival lookup rejects malformed input and more than 200 unique codes atomically, while all 200 are returned',async()=>{
 const f=await fixture(),codes=Array.from({length:201},(_,i)=>String(301000000000+i)),before=snapshot(f.DB);
 for(const input of [undefined,null,['not-a-tracking-number'],[A,'https://example.invalid/'+B],[A,B+'已收到'],[{}],{tracking_no:A},123,codes]){
  const r=await f.request('sop_courier_lookup',{tracking_nos:input});
  assert.equal(r.ok,false,JSON.stringify(input));assert.deepEqual(snapshot(f.DB),before);
 }
 const full=await f.ok('sop_courier_lookup',{tracking_nos:codes.slice(0,200)});
 assert.equal(full.total,200);assert.equal(full.received,0);assert.equal(full.arrival_status,'pending');
 assert.deepEqual(full.items.map(item=>item.tracking_no),codes.slice(0,200));
 const duplicates=await f.ok('sop_courier_lookup',{tracking_nos:[...codes.slice(0,200),codes[0]]});
 assert.equal(duplicates.total,200,'limit counts unique normalized tracking numbers');
 for(const input of ['',[]])assert.deepEqual(await f.ok('sop_courier_lookup',{tracking_nos:input}),{ok:true,total:0,received:0,arrival_status:'pending',items:[]});
 assert.deepEqual(snapshot(f.DB),before);
});

test('arrival lookup allows a viewer and requires an authenticated user and enabled courier environment',async()=>{
 const f=await fixture(),body={action:'sop_courier_lookup',tracking_nos:[A]},before=snapshot(f.DB);
 const r=await handleCourier(body,{...f.env,SOP_REQUEST_USER:{id:'LOOKUP-VIEWER',name:'Readonly lookup',role:'viewer'}},()=>assert.fail('lookup must not recalculate a plan'));
 assert.equal(r.ok,true);assert.equal(r.arrival_status,'pending');
 await assert.rejects(handleCourier(body,{...f.env,SOP_REQUEST_USER:null},()=>{}),/权限/);
 await assert.rejects(handleCourier(body,{...f.env,SOP_UPGRADE_ENABLED:'false',SOP_REQUEST_USER:{id:'LOOKUP-MANAGER',role:'manager'}},()=>{}),/权限/);
 assert.deepEqual(snapshot(f.DB),before);
});

test('arrival lookup is a read through the authenticated API without an office operation label, including field scope',async()=>{
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('lookup-fixture-only')},cookies={};
 const call=async(action,data={},scope='office',headers={})=>{
  const response=await worker.fetch(new Request('https://fixture.local/'+(scope==='office'?'':scope==='field'?'001/':'attendance/')+'api',{
   method:'POST',headers:{'Content-Type':'application/json',Origin:'https://fixture.local',Cookie:cookies[scope]||'',...headers},
   body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})
  }),env);
  const result=await response.json();
  if(action==='sop_login'&&result.ok)cookies[scope]=response.headers.get('set-cookie').split(';')[0];
  return result;
 };
 assert.equal((await call('sop_courier_lookup',{tracking_nos:[A]})).ok,false);
 assert.equal((await call('sop_login',{sop_key:'lookup-fixture-only'})).ok,true);
 const identity=await call('sop_identity');assert.equal(identity.ok,true);
 assert.equal(identity.user.operation_context.name,'');
 for(const headers of [{},{'X-CK-Operation-Context':'stale-context'}]){
  const r=await call('sop_courier_lookup',{tracking_nos:[A]},'office',headers);
  assert.equal(r.ok,true,r.error);assert.equal(r.received,0);
 }
 const personResult=await call('sop_attendance_employee_register',{name:'Lookup field reader',employeeNo:'LOOKUP-FIELD',department:'direct_ship'});
 assert.equal(personResult.ok,true,personResult.error);const person=personResult.person;
 assert.equal((await call('sop_attendance_checkin',{badge:person.badgeId})).ok,true);
 assert.equal((await call('sop_login',{badge:person.badgeId},'field')).ok,true);
 const field=await call('sop_courier_lookup',{tracking_nos:[A]},'field');assert.equal(field.ok,true,field.error);
 assert.equal((await call('sop_login',{sop_key:'lookup-fixture-only'},'kiosk')).ok,true);
 assert.equal((await call('sop_courier_lookup',{tracking_nos:[A]},'kiosk')).ok,false);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_courier_receipts').get().n,0);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,0);
});

test('creating a plan after preflight links original receipts and preserves their time, scanner, batch and receiving work',async()=>{
 const f=await fixture(),scanned=await f.scan(A,'8-2');
 f.DB.raw.prepare('UPDATE ck_courier_receipts SET received_at=? WHERE id=?').run('2026-09-20T10:11:12.000Z',scanned.item.id);
 const detail=await f.ok('sop_courier_detail',{batch_id:scanned.item.batch_id});
 await f.ok('sop_courier_receive',{operation:'finish',batch_id:detail.batch.id,revision:detail.batch.revision});
 const before=snapshot(f.DB),preview=await f.ok('sop_courier_lookup',{tracking_nos:[A,B]});
 assert.equal(preview.arrival_status,'partial');assert.equal(preview.items[0].claim_status,'available');
 assert.deepEqual(snapshot(f.DB),before);
 const p=await f.plan([A,B]),after=await f.ok('sop_courier_lookup',{tracking_nos:[A,B]});
 assert.equal(after.received,1);assert.equal(after.arrival_status,'partial');
 assert.equal(after.items[0].plan_id,p.id);assert.equal(after.items[1].plan_id,p.id);
 for(const field of receiptFields)assert.equal(after.items[0][field],preview.items[0][field],field);
 assert.equal(after.items[1].receipt_id,null);
 for(const table of ['ck_courier_receipts','ck_courier_events','ck_courier_batches','ck_courier_batch_items','ck_courier_batch_requests','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results']){
  assert.deepEqual(f.DB.raw.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),before[table],table);
 }
 assert.equal(f.DB.raw.prepare("SELECT actual_qty FROM v2_inbound_plan_lines WHERE plan_id=? AND unit_type='courier'").get(p.id).actual_qty,1);
});
