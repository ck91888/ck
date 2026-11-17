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

function holdBatch(f,count=1){
 const db=f.env.DB,batch=db.batch.bind(db);let release,entered,held=0;const at=new Promise(r=>entered=r),gate=new Promise(r=>release=r);
 db.batch=async ss=>{if(held<count&&ss.some(s=>s.sql.includes('INSERT INTO v2_003_purchase_shipments'))){if(++held===count)entered();await gate;}return batch(ss);};
 return {at,release,restore(){db.batch=batch;}};
}
async function released(f,h,bodies){const pending=Promise.all(bodies.map(body=>f.api('v2_003_purchase_shipment_create',body)));try{await waitFor(h.at);h.release();return await pending;}finally{h.release();await pending;h.restore();}}
async function reservation(f,quantity,count){
 const rows=await f.rows('SELECT SUM(expected_qty) qty,COUNT(*) n FROM v2_003_purchase_shipment_items WHERE order_line_id=?',f.lines[0].id);assert.equal(rows[0].qty,quantity);assert.equal(rows[0].n,count);
 assert.equal((await f.rows('SELECT SUM(current_qty) n FROM v2_003_materials'))[0].n,0);assert.equal((await f.rows('SELECT COUNT(*) n FROM v2_003_material_txns'))[0].n,0,'dispatch registration must not book physical stock');
}
test('same-key writes released together commit one shipment and exactly one identical receipt',async t=>{
 const f=await fixture(t),p=f.body(),r=await released(f,holdBatch(f,2),[p,p]);assert.ok(r.every(x=>x.ok),JSON.stringify(r));assert.deepEqual(r[0],r[1]);await reservation(f,8,1);
});
test('different content racing on one key returns conflict without publishing a second shipment',async t=>{
 const f=await fixture(t),p=f.body(),h=holdBatch(f),pending=f.api('v2_003_purchase_shipment_create',p);
 try{await waitFor(h.at);await f.ok('v2_003_purchase_shipment_create',f.body({items:[{order_line_id:f.lines[0].id,expected_qty:4}]}));const before=await f.snapshot();h.release();const r=await pending;assert.equal(r.error,'request_id_conflict');assert.equal(await f.snapshot(),before);await reservation(f,4,1);}finally{h.release();await pending;h.restore();}
});
test('different keys retain distinct legitimate supplier shipments and never touch stock',async t=>{
 const f=await fixture(t),r=await released(f,holdBatch(f,2),[3,4].map(q=>f.body({client_req_id:'SYN-DIFFERENT-'+q,delivery_method:'supplier',tracking_no:'',items:[{order_line_id:f.lines[0].id,expected_qty:q}]})));assert.ok(r.every(x=>x.ok),JSON.stringify(r));assert.notEqual(r[0].id,r[1].id);await reservation(f,7,2);
});
test('distinct keys competing for more than available quantity have one winner and no orphan receipt',async t=>{
 const f=await fixture(t),r=await released(f,holdBatch(f,2),[1,2].map(q=>f.body({client_req_id:'SYN-EXCESS-'+q,tracking_no:'SYN-EXCESS-'+q,items:[{order_line_id:f.lines[0].id,expected_qty:6}]})));assert.equal(r.filter(x=>x.ok).length,1);assert.equal(r.find(x=>!x.ok).error,'shipment_qty_exceeds_ordered');await reservation(f,6,1);assert.equal((await f.rows("SELECT COUNT(*) n FROM v2_idempotency_keys WHERE idem_key LIKE 'SYN-EXCESS-%'"))[0].n,1);
});
test('a different key cannot replay a conflicting tracking number as success',async t=>{
 const f=await fixture(t),p=f.body(),h=hold(f,s=>s.includes('SELECT id FROM v2_003_purchase_shipments WHERE tracking_no=? LIMIT 1'),'first'),pending=f.api('v2_003_purchase_shipment_create',p);
 try{await waitFor(h.at);await f.ok('v2_003_purchase_shipment_create',f.body({client_req_id:'SYN-OTHER-TRACK-KEY',items:[{order_line_id:f.lines[0].id,expected_qty:3}]}));const before=await f.snapshot();h.release();assert.equal((await pending).error,'duplicate_tracking_no');assert.equal(await f.snapshot(),before);await reservation(f,3,1);}finally{h.release();await pending;h.restore();}
});
for(const mode of ['cancel','complete'])test('uncommitted request remains rejected when purchase closes with '+mode,async t=>{
 const f=await fixture(t),h=holdBatch(f),pending=f.api('v2_003_purchase_shipment_create',f.body());
 try{await waitFor(h.at);await f.ok('v2_003_purchase_order_close',{id:f.order.id,mode,reason:'合成未提交请求关闭'});const before=await f.snapshot();h.release();assert.equal((await pending).error,'purchase_order_closed');assert.equal(await f.snapshot(),before);assert.equal((await f.rows('SELECT COUNT(*) n FROM v2_003_purchase_shipments'))[0].n,0);}finally{h.release();await pending;h.restore();}
});
test('receipt committed during a retry preserves exactly one real receipt and inbound inventory ledger',async t=>{
 const f=await fixture(t),p=f.body(),h=hold(f,lineSQL);await interleave(f,h,p,async()=>{
  const saved=await f.ok('v2_003_purchase_shipment_create',p),d=await f.ok('v2_003_receiving_lookup',{code:saved.shipment_no}),input={shipment_id:saved.id,items:d.items.map(x=>({shipment_item_id:x.id,received_qty:8,location_code:'SYN-SHIP-A01'}))};
  await f.ok('v2_003_receipt_confirm',input);assert.equal((await f.ok('v2_003_receipt_confirm',input)).duplicate,true);return saved;
 });
 assert.equal((await f.rows('SELECT current_qty FROM v2_003_materials WHERE id=?',f.lines[0].material_id))[0].current_qty,8);assert.deepEqual((await f.rows('SELECT txn_type,qty_delta FROM v2_003_material_txns')).map(x=>[x.txn_type,x.qty_delta]),[['purchase_inbound',8]]);
});
test('receipt persistence failure rolls back shipment, items and order state before clean retry',async t=>{
 const f=await fixture(t),p=f.body(),before=await f.snapshot();await f.env.DB.prepare("CREATE TRIGGER synthetic_shipment_receipt_failure BEFORE INSERT ON v2_idempotency_keys WHEN NEW.action='v2_003_purchase_shipment_create' BEGIN SELECT RAISE(ABORT,'synthetic shipment receipt failure'); END").run();
 const r=await f.api('v2_003_purchase_shipment_create',p);assert.equal(r.ok,false);assert.match(r.error,/synthetic shipment receipt failure/);assert.equal(await f.snapshot(),before);
 await f.env.DB.prepare('DROP TRIGGER synthetic_shipment_receipt_failure').run();const saved=await f.ok('v2_003_purchase_shipment_create',p);assert.deepEqual(await f.ok('v2_003_purchase_shipment_create',p),saved);await reservation(f,8,1);
});
test('keyless legacy caller still creates separate valid registrations instead of inventing deduplication',async t=>{
 const f=await fixture(t),p=f.body({client_req_id:'',delivery_method:'supplier',tracking_no:'',items:[{order_line_id:f.lines[0].id,expected_qty:2}]}),a=await f.ok('v2_003_purchase_shipment_create',p),b=await f.ok('v2_003_purchase_shipment_create',p);assert.notEqual(a.id,b.id);await reservation(f,4,2);
});
test('normalized quantities and reordered complete item sets replay one result without exposing the signature',async t=>{
 const f=await fixture(t),p=f.body({note:' 合成说明 ',items:f.lines.map((x,i)=>({order_line_id:x.id,expected_qty:i+2}))}),saved=await f.ok('v2_003_purchase_shipment_create',p),before=await f.snapshot();
 assert.deepEqual(await f.ok('v2_003_purchase_shipment_create',{...p,tracking_no:' '+p.tracking_no+' ',note:'合成说明',items:[...p.items].reverse().map(x=>({...x,expected_qty:x.expected_qty+'.0'}))}),saved);assert.equal(Object.hasOwn(saved,'_request_signature'),false);assert.equal(await f.snapshot(),before);
 await f.env.DB.prepare('UPDATE v2_003_purchase_orders SET supplier=? WHERE id=?').bind('合成后改默认供应商',f.order.id).run();assert.deepEqual(await f.ok('v2_003_purchase_shipment_create',p),saved,'bound default intent must not be rederived from mutable metadata');
});
test('old unsigned receipts replay only a provable matching shipment and reject changed defaults or quantities',async t=>{
 const f=await fixture(t),p=f.body(),saved=await f.ok('v2_003_purchase_shipment_create',p);await f.env.DB.prepare('UPDATE v2_idempotency_keys SET response_json=? WHERE idem_key=?').bind(JSON.stringify(saved),p.client_req_id).run();
 assert.deepEqual(await f.ok('v2_003_purchase_shipment_create',p),saved);const before=await f.snapshot();assert.equal((await f.api('v2_003_purchase_shipment_create',{...p,items:[{order_line_id:f.lines[0].id,expected_qty:7}]})).error,'request_id_conflict');assert.equal(await f.snapshot(),before);
 await f.env.DB.prepare('UPDATE v2_003_purchase_orders SET supplier=? WHERE id=?').bind('合成无法核实的默认变更',f.order.id).run();const latest=await f.snapshot();assert.equal((await f.api('v2_003_purchase_shipment_create',p)).error,'request_id_conflict');assert.equal(await f.snapshot(),latest);
});
test('old explicitly specified supplier stays provable after defaults and purchase status change',async t=>{
 const f=await fixture(t),p=f.body({supplier:'合成明确供应商'}),saved=await f.ok('v2_003_purchase_shipment_create',p);await f.env.DB.prepare('UPDATE v2_idempotency_keys SET response_json=? WHERE idem_key=?').bind(JSON.stringify(saved),p.client_req_id).run();await f.ok('v2_003_purchase_order_close',{id:f.order.id,mode:'cancel',reason:'合成旧回执关闭'});await f.env.DB.prepare('UPDATE v2_003_purchase_orders SET supplier=? WHERE id=?').bind('合成当前默认',f.order.id).run();const before=await f.snapshot();assert.deepEqual(await f.ok('v2_003_purchase_shipment_create',p),saved);assert.equal(await f.snapshot(),before);
});
test('malformed or unprovable historical receipts fail closed without creating another shipment',async t=>{
 const f=await fixture(t),p=f.body();await f.ok('v2_003_purchase_shipment_create',p);
 for(const value of ['null','[]','{invalid',JSON.stringify({ok:true,id:'SYN-MISSING',shipment_no:'SYN-MISSING'})]){await f.env.DB.prepare('UPDATE v2_idempotency_keys SET response_json=? WHERE idem_key=?').bind(value,p.client_req_id).run();const before=await f.snapshot();assert.equal((await f.api('v2_003_purchase_shipment_create',p)).error,'request_id_conflict');assert.equal(await f.snapshot(),before);}
});
test('a cached shipment cannot bypass field or anonymous permission checks',async t=>{
 const f=await fixture(t),p=f.body();await f.ok('v2_003_purchase_shipment_create',p);const before=await f.snapshot();for(const scope of ['A','guest'])assert.equal((await f.call(scope,'v2_003_purchase_shipment_create',p)).ok,false);assert.equal(await f.snapshot(),before);
});
test('attendance and automatic duty flags do not change the matching shipment replay contract',async t=>{
 const f=await fixture(t);f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';const p=f.body(),h=hold(f,lineSQL);await interleave(f,h,p,()=>f.ok('v2_003_purchase_shipment_create',p));
});
