// Authenticated public Worker API; only synthetic memory/local workerd databases.
import test from 'node:test';
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';

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
