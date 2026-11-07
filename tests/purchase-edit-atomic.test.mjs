// Authenticated public Worker API; only synthetic memory/local workerd databases.
import test from 'node:test';
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {operationHeaders} from './office-operator-fixture.mjs';

async function fixture(t,{active=false}={}){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
 const f=await ownedHomeFixture();let native;
 if(active){f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';}
 const api=(action,body={})=>f.call('office',action,{operator_id:'SYN-PURCHASE-EDIT',operator_name:'合成采购验收员',...body});
 const ok=async(a,b)=>{const r=await api(a,b);assert.equal(r.ok,true,a+': '+r.error);return r;};
 const materials=[];
 for(const name of ['合成胶带','合成纸箱'])materials.push(await ok('v2_003_material_save',{name_zh:name,category:'包材',unit:'个',opening_qty:0}));
 await ok('v2_003_location_save',{location_code:'SYN-PURCHASE-A01'});
 const order=await ok('v2_003_purchase_request_create',{request_reason:'合成采购并发回归',lines:materials.map(m=>({material_id:m.id,requested_qty:10}))});
 const lines=(await ok('v2_003_purchase_order_detail',{id:order.id})).lines;
 const payload=(qty=10)=>({id:order.id,supplier:'合成供应商',lines:lines.map(l=>({id:l.id,ordered_qty:qty,unit_cost:100}))});
 await ok('v2_003_purchase_order_update',payload());
 if(process.env.CK_PURCHASE_EDIT_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 t.after(async()=>{await native?.close();f.DB.raw.close();});
 const rows=async(sql,...args)=>(await f.env.DB.prepare(sql).bind(...args).all()).results;
 const facts=['v2_003_purchase_shipments','v2_003_purchase_shipment_items','v2_003_purchase_receipts','v2_003_purchase_receipt_items','v2_003_materials','v2_003_material_txns'];
 const snapshot=async(tables=['v2_003_purchase_orders','v2_003_purchase_order_lines',...facts,'v2_idempotency_keys'])=>JSON.stringify(await Promise.all(tables.map(table=>rows('SELECT * FROM '+table+' ORDER BY rowid'))));
 const shipBody=(qty=8,index=0,extra={})=>({order_id:order.id,delivery_method:'express',tracking_no:'SYN-PURCHASE-'+crypto.randomUUID(),items:[{order_line_id:lines[index].id,expected_qty:qty}],...extra});
 const ship=(qty=8,index=0,extra={})=>ok('v2_003_purchase_shipment_create',shipBody(qty,index,extra));
 const detail=()=>ok('v2_003_purchase_order_detail',{id:order.id});
 return {...f,api,ok,materials,order,lines,payload,rows,facts,snapshot,shipBody,ship,detail};
}

function hold(f,predicate){
 const db=f.env.DB,original=db.batch.bind(db);let arrived,release,held=false;
 const at=new Promise(r=>arrived=r),gate=new Promise(r=>release=r);
 db.batch=async ss=>{if(!held&&ss.some(s=>predicate(s.sql))){held=true;arrived();await gate;}return original(ss);};
 return {at,release,restore(){db.batch=original;}};
}
const editSQL=s=>/UPDATE\s+v2_003_purchase_order_lines\s+SET ordered_qty=/s.test(s);
const shipSQL=s=>s.includes('INSERT INTO v2_003_purchase_shipments');
async function atGate(p){
 let timer;try{await Promise.race([p,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('synthetic transaction gate not reached')),10000);})]);}finally{clearTimeout(timer);}
}
async function staleEdit(f,interleave,{payload=f.payload(5),error='ordered_qty_below_shipped'}={}){
 const gate=hold(f,editSQL),pending=f.api('v2_003_purchase_order_update',payload);
 try{
  await atGate(gate.at);await interleave();const before=await f.snapshot();gate.release();const result=await pending;
  assert.equal(result.ok,false,JSON.stringify(result));assert.equal(result.error,error);assert.equal(await f.snapshot(),before,'failed edit must not alter any business row');
 }finally{gate.release();await pending;gate.restore();}
}

test('stale purchase edit cannot undercut a shipment committed after its validation',async t=>{
 const f=await fixture(t);await staleEdit(f,async()=>{await f.ship();assert.equal((await f.api('v2_003_purchase_order_update',f.payload(5))).error,'ordered_qty_below_shipped');});
 const d=await f.detail();assert.equal(d.lines[0].ordered_qty,10);assert.equal(d.lines[0].scheduled_qty,8);assert.equal(d.order.total_amount,2000);
});
for(const mode of ['cancel','complete'])test('stale purchase edit cannot reopen an order closed with '+mode,async t=>{
 const f=await fixture(t);await staleEdit(f,()=>f.ok('v2_003_purchase_order_close',{id:f.order.id,mode,reason:'合成关闭竞态'}),{error:'purchase_order_closed'});
 assert.equal((await f.detail()).order.status,mode==='cancel'?'cancelled':'completed');
});
test('reverse interleaving rejects a stale shipment and leaves no orphan shipment items',async t=>{
 const f=await fixture(t),gate=hold(f,shipSQL),pending=f.api('v2_003_purchase_shipment_create',f.shipBody());
 try{await atGate(gate.at);await f.ok('v2_003_purchase_order_update',f.payload(5));const before=await f.snapshot();gate.release();const r=await pending;assert.equal(r.error,'shipment_qty_exceeds_ordered');assert.equal(await f.snapshot(),before);}finally{gate.release();await pending;gate.restore();}
});
test('received and issued quantities and their original ledger survive a rejected stale edit',async t=>{
 const f=await fixture(t);await staleEdit(f,async()=>{
  const s=await f.ship(),r=await f.ok('v2_003_receiving_lookup',{code:s.shipment_no});
  await f.ok('v2_003_receipt_confirm',{shipment_id:s.id,items:r.items.map(x=>({shipment_item_id:x.id,received_qty:8,location_code:'SYN-PURCHASE-A01'}))});
  await f.ok('v2_003_material_txn',{material_id:f.lines[0].material_id,txn_type:'issue',qty:8,department:'大货',recipient_name:'合成领料员'});
 });
 assert.equal((await f.rows('SELECT current_qty FROM v2_003_materials WHERE id=?',f.lines[0].material_id))[0].current_qty,0);
 assert.deepEqual((await f.rows('SELECT txn_type,qty_delta FROM v2_003_material_txns WHERE material_id=? ORDER BY rowid',f.lines[0].material_id)).map(x=>[x.txn_type,x.qty_delta]),[['purchase_inbound',8],['issue',-8]]);
 assert.equal((await f.detail()).lines[0].received_qty,8);
});
test('a conflict on the last line rolls back earlier line amounts and supplier changes',async t=>{
 const f=await fixture(t),p=f.payload(5);p.lines[0].ordered_qty=7;p.lines[0].unit_cost=200;p.supplier='合成另一供应商';
 await staleEdit(f,()=>f.ship(8,1),{payload:p});
});

for(const change of ['add','delete','move','replace'])test('live line membership change rejects the complete edit: '+change,async t=>{
 const f=await fixture(t);
 await staleEdit(f,async()=>{
  if(change==='delete'||change==='replace')await f.env.DB.prepare('DELETE FROM v2_003_purchase_order_lines WHERE id=?').bind(f.lines[1].id).run();
  if(change==='add'||change==='replace'){
   const material=await f.ok('v2_003_material_save',{name_zh:'合成新增采购耗材',category:'包材',unit:'个',opening_qty:0});
   const row={...(await f.rows('SELECT * FROM v2_003_purchase_order_lines WHERE id=?',f.lines[0].id))[0],id:'SYN-LINE-'+crypto.randomUUID(),material_id:material.id};
   await f.env.DB.prepare('INSERT INTO v2_003_purchase_order_lines ('+Object.keys(row).join(',')+') VALUES ('+Object.keys(row).map(()=>'?').join(',')+')').bind(...Object.values(row)).run();
  }
  if(change==='move'){
   const other=await f.ok('v2_003_purchase_request_create',{request_reason:'合成另一采购',lines:[{material_id:f.lines[0].material_id,requested_qty:1}]});
   await f.env.DB.prepare('UPDATE v2_003_purchase_order_lines SET order_id=? WHERE id=?').bind(other.id,f.lines[1].id).run();
  }
 },{error:'purchase_order_lines_changed'});
 assert.equal((await f.rows('SELECT COUNT(*) n FROM ck_purchase_edit_checks'))[0].n,0);
});

test('two distinct simultaneous shipments are included in the live cumulative lower bound',async t=>{
 const f=await fixture(t);await staleEdit(f,()=>Promise.all([f.ship(3),f.ship(3)]));
 const d=await f.detail();assert.equal(d.lines[0].scheduled_qty,6);assert.equal(d.shipments.length,2);
});
test('concurrent retries of one shipment reserve once before rejecting a stale edit',async t=>{
 const f=await fixture(t),p=f.shipBody(8,0,{client_req_id:'SYN-SHIPMENT-ONCE'});
 await staleEdit(f,async()=>{const rs=await Promise.all([f.ok('v2_003_purchase_shipment_create',p),f.ok('v2_003_purchase_shipment_create',p)]);assert.equal(rs[0].id,rs[1].id);});
 const d=await f.detail();assert.equal(d.lines[0].scheduled_qty,8);assert.equal(d.shipments.length,1);
});
test('real shipment invariant also holds with attendance returns and automatic dispatch time enabled',async t=>{
 const f=await fixture(t,{active:true});await staleEdit(f,()=>f.ship());
});

test('an injected final header failure rolls back every line and cleans up all assertions',async t=>{
 const f=await fixture(t),before=await f.snapshot();
 await f.env.DB.prepare("CREATE TRIGGER synthetic_purchase_header_failure BEFORE UPDATE ON v2_003_purchase_orders BEGIN SELECT RAISE(ABORT,'synthetic purchase header failure'); END").run();
 const r=await f.api('v2_003_purchase_order_update',f.payload(7));assert.equal(r.ok,false);assert.match(r.error,/synthetic purchase header failure/);assert.equal(await f.snapshot(),before);
 assert.equal((await f.rows('SELECT COUNT(*) n FROM ck_purchase_edit_checks'))[0].n,0);
 await f.env.DB.prepare('DROP TRIGGER synthetic_purchase_header_failure').run();const saved=await f.ok('v2_003_purchase_order_update',f.payload(7));assert.equal(saved.total_amount,1400);
 assert.equal((await f.rows('SELECT COUNT(*) n FROM ck_purchase_edit_checks'))[0].n,0);
});

test('successful equal/larger edits and exact decimal bounds keep the original quantity policy',async t=>{
 const f=await fixture(t);await f.ship(2.25);await f.ok('v2_003_purchase_order_update',f.payload(2.25));
 let before=await f.snapshot();assert.equal((await f.api('v2_003_purchase_order_update',f.payload(2.25-1e-8))).error,'ordered_qty_below_shipped');assert.equal(await f.snapshot(),before);
 const p=f.payload(3.5);p.lines[1].ordered_qty=0;await f.ok('v2_003_purchase_order_update',p);const d=await f.detail();assert.deepEqual(d.lines.map(l=>l.ordered_qty),[3.5,0]);assert.equal(d.order.total_amount,350);
 before=await f.snapshot();assert.equal((await f.api('v2_003_purchase_order_update',f.payload(0))).error,'ordered_qty_below_shipped');assert.equal(await f.snapshot(),before);
});
test('the same strict decimal comparison is used for preflight and transaction checks',async t=>{
 const f=await fixture(t);await staleEdit(f,async()=>{await f.ship(0.1);await f.ship(0.2);},{payload:f.payload(0.3)});
 assert.equal((await f.api('v2_003_purchase_order_update',f.payload(0.3))).error,'ordered_qty_below_shipped');
});

test('cancelled shipment quantities stay excluded without changing historical shipment data',async t=>{
 const f=await fixture(t),s=await f.ship();
 // A historical cancelled shipment within an otherwise open order.
 await f.env.DB.prepare("UPDATE v2_003_purchase_shipments SET status='cancelled' WHERE id=?").bind(s.id).run();
 const facts=await f.snapshot(f.facts);await f.ok('v2_003_purchase_order_update',f.payload(5));
 assert.equal((await f.detail()).lines[0].scheduled_qty,0);assert.equal(await f.snapshot(f.facts),facts);
});
for(const actual of [2,12])test('actual receipt '+actual+' preserves discrepancy rules and expected-shipment lower bound',async t=>{
 const f=await fixture(t),s=await f.ship(),r=await f.ok('v2_003_receiving_lookup',{code:s.shipment_no});
 await f.ok('v2_003_receipt_confirm',{shipment_id:s.id,items:r.items.map(x=>({shipment_item_id:x.id,received_qty:actual,location_code:'SYN-PURCHASE-A01'}))});
 const before=await f.snapshot();assert.equal((await f.api('v2_003_purchase_order_update',f.payload(7))).error,'ordered_qty_below_shipped');assert.equal(await f.snapshot(),before);
 const facts=await f.snapshot(f.facts);await f.ok('v2_003_purchase_order_update',f.payload(8));assert.equal(await f.snapshot(f.facts),facts);
 const d=await f.detail();assert.equal(d.lines[0].received_qty,actual);assert.equal(d.lines[0].scheduled_qty,8);assert.equal((await f.rows('SELECT current_qty FROM v2_003_materials WHERE id=?',f.lines[0].material_id))[0].current_qty,actual);
});

test('field and unauthenticated callers cannot edit procurement or change any facts',async t=>{
 const f=await fixture(t),before=await f.snapshot();
 for(const actor of ['A','B'])assert.equal((await f.call(actor,'v2_003_purchase_order_update',f.payload(5))).ok,false);
 const response=await f.worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'v2_003_purchase_order_update',...f.payload(5)})}),f.env);
 assert.equal((await response.json()).ok,false);assert.equal(await f.snapshot(),before);
});

test('two cold bindings upgrade an old database without rewriting procurement or inventory facts',async t=>{
 const f=await fixture(t),db=f.env.DB;await db.prepare('DROP TABLE ck_purchase_edit_checks').run();
 const before=await f.snapshot(),bindings=[0,1].map(()=>({prepare:db.prepare.bind(db),batch:db.batch.bind(db)}));
 const replies=await Promise.all(bindings.map(DB=>f.worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:f.cookies.office,...operationHeaders(f.cookies.office)},body:JSON.stringify({action:'v2_003_purchase_order_update',...f.payload(10),operator_id:'SYN-PURCHASE-EDIT',operator_name:'合成采购验收员'})}),{...f.env,DB}).then(r=>r.json())));
 assert.ok(replies.every(r=>r.ok),JSON.stringify(replies));assert.equal(await f.snapshot(),before);assert.equal((await f.rows('SELECT COUNT(*) n FROM ck_purchase_edit_checks'))[0].n,0);
});
