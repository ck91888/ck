// Authenticated Worker with isolated synthetic SQLite/local workerd D1 only.
import test from 'node:test';import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';import {localD1} from './d1-local-runtime.mjs';
async function fixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});const f=await ownedHomeFixture();let native;
 const api=(a,b={})=>f.call('office',a,{operator_id:'SYN-SHIPMENT-REPLAY',operator_name:'合成发货重试核查员',...b}),ok=async(a,b)=>{const r=await api(a,b);assert.equal(r.ok,true,a+': '+r.error);return r;};
 const materials=[];for(const name of ['合成发货胶带','合成发货箱'])materials.push(await ok('v2_003_material_save',{name_zh:name,category:'包材',unit:'个',opening_qty:0}));
 const order=await ok('v2_003_purchase_request_create',{request_reason:'合成发货重试核查',lines:materials.map(x=>({material_id:x.id,requested_qty:10}))}),lines=(await ok('v2_003_purchase_order_detail',{id:order.id})).lines;
 await ok('v2_003_purchase_order_update',{id:order.id,supplier:'合成原供应商',lines:lines.map(x=>({id:x.id,ordered_qty:10,unit_cost:1}))});await ok('v2_003_location_save',{location_code:'SYN-SHIP-A01'});
 if(process.env.CK_SHIPMENT_REPLAY_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 t.after(async()=>{await native?.close();f.DB.raw.close();});
 const rows=async(sql,...args)=>(await f.env.DB.prepare(sql).bind(...args).all()).results;
 const body=(extra={})=>({order_id:order.id,delivery_method:'express',tracking_no:'SYN-SHIP-ONCE',client_req_id:'SYN-SHIP-ONCE',items:[{order_line_id:lines[0].id,expected_qty:8}],...extra});
 const snapshot=async()=>JSON.stringify(await Promise.all(['v2_003_purchase_orders','v2_003_purchase_order_lines','v2_003_purchase_shipments','v2_003_purchase_shipment_items','v2_003_purchase_receipts','v2_003_purchase_receipt_items','v2_003_materials','v2_003_material_txns','v2_idempotency_keys'].map(table=>rows('SELECT * FROM '+table+' ORDER BY rowid'))));
 return {...f,api,ok,order,lines,materials,rows,body,snapshot};
}
function hold(f,predicate,method='all'){
 const db=f.env.DB,prepare=db.prepare.bind(db);let entered,release,held=false;const at=new Promise(r=>entered=r),gate=new Promise(r=>release=r);
 db.prepare=sql=>{const s=prepare(sql);if(predicate(sql)){const read=s[method].bind(s);s[method]=async()=>{if(!held){held=true;entered();await gate;}return read();};}return s;};
 return {at,release,restore(){db.prepare=prepare;}};
}
async function waitFor(p){let timer;try{await Promise.race([p,new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('synthetic gate not reached')),10000))]);}finally{clearTimeout(timer);}}
const lineSQL=s=>s.includes('AS scheduled_qty')&&s.includes('WHERE l.order_id=?');
async function interleave(f,h,body,run){const pending=f.api('v2_003_purchase_shipment_create',body);try{await waitFor(h.at);const saved=await run();const before=await f.snapshot();h.release();const result=await pending;assert.deepEqual(result,saved);assert.equal(await f.snapshot(),before);}finally{h.release();await pending;h.restore();}}

test('same-key request paused before live quantity read returns its committed shipment instead of false excess',async t=>{
 const f=await fixture(t),p=f.body(),h=hold(f,lineSQL);await interleave(f,h,p,()=>f.ok('v2_003_purchase_shipment_create',p));
});
test('same-key request paused before duplicate tracking read returns its committed shipment',async t=>{
 const f=await fixture(t),p=f.body(),h=hold(f,s=>s.includes('SELECT id FROM v2_003_purchase_shipments WHERE tracking_no=? LIMIT 1'),'first');await interleave(f,h,p,()=>f.ok('v2_003_purchase_shipment_create',p));
});
for(const mode of ['cancel','complete'])test('same-key request can replay its committed shipment after purchase '+mode,async t=>{
 const f=await fixture(t),p=f.body(),h=hold(f,s=>s==='SELECT * FROM v2_003_purchase_orders WHERE id=?','first');await interleave(f,h,p,async()=>{const saved=await f.ok('v2_003_purchase_shipment_create',p);await f.ok('v2_003_purchase_order_close',{id:f.order.id,mode,reason:'合成关闭与重试'});return saved;});
});
test('a reused shipment request key rejects changed business content and another action receipt',async t=>{
 const f=await fixture(t),p=f.body();await f.ok('v2_003_purchase_shipment_create',p);const before=await f.snapshot();
 for(const extra of [{items:[{order_line_id:f.lines[0].id,expected_qty:7}]},{items:[{order_line_id:f.lines[1].id,expected_qty:8}]},{tracking_no:'SYN-DIFFERENT'},{delivery_method:'supplier',tracking_no:''},{supplier:'合成另一供应商'},{note:'合成另一备注'},{expected_date:'2026-11-05'},{order_id:'SYN-OTHER-ORDER'}]){
  const r=await f.api('v2_003_purchase_shipment_create',{...p,...extra});assert.equal(r.ok,false);assert.equal(r.error,'request_id_conflict');assert.equal(await f.snapshot(),before);
 }
 await f.env.DB.prepare("INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES('SYN-OTHER-ACTION','synthetic_other','{\"ok\":true,\"id\":\"SYN\"}','2026-11-03')").run();const otherBefore=await f.snapshot(),r=await f.api('v2_003_purchase_shipment_create',f.body({client_req_id:'SYN-OTHER-ACTION',tracking_no:'SYN-OTHER-ACTION'}));assert.equal(r.error,'request_id_conflict');assert.equal(await f.snapshot(),otherBefore);
});
