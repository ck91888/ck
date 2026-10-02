import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';

async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const api=async(action,data={})=>(await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action,operator_id:'FIXTURE-P',operator_name:'Fixture purchaser',client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(a,d)=>{const r=await api(a,d);assert.equal(r.ok,true,r.error);return r;};
 const materials=[];for(const unit of ['卷','个'])materials.push(await ok('v2_003_material_save',{name_zh:'Fixture '+unit,category:'包材',unit,opening_qty:0}));
 await ok('v2_003_location_save',{location_code:'FIXTURE-A01'});
 const order=await ok('v2_003_purchase_request_create',{request_reason:'Fixture purchase',lines:materials.map(x=>({material_id:x.id,requested_qty:10}))});
 const detail=await ok('v2_003_purchase_order_detail',{id:order.id}),lines=detail.lines;
 await ok('v2_003_purchase_order_update',{id:order.id,lines:lines.map(x=>({id:x.id,ordered_qty:10,unit_cost:100}))});
 const ship=(indices=[0],qty=10,extra={})=>ok('v2_003_purchase_shipment_create',{order_id:order.id,delivery_method:'express',tracking_no:'FIXTURE-'+crypto.randomUUID(),items:indices.map(i=>({order_line_id:lines[i].id,expected_qty:qty})),...extra});
 const receive=async(s,quantities)=>{const d=await ok('v2_003_receiving_lookup',{code:s.shipment_no});return ok('v2_003_receipt_confirm',{shipment_id:s.id,items:d.items.map((x,i)=>({shipment_item_id:x.id,received_qty:quantities[i],location_code:'FIXTURE-A01'}))});};
 return{DB,api,ok,materials,order,lines,ship,receive};
}
function shipmentUI(f){
 const source=fs.readFileSync(new URL('../003/app.js',import.meta.url),'utf8'),submit=source.slice(source.indexOf('async function submitShipment('),source.indexOf('async function closePurchaseOrder('));
 const requestId=source.split('\n').find(x=>x.startsWith('function reqId(')),operator=source.split('\n').find(x=>x.startsWith('function operatorPayload('));
 const fields={shOrderId:f.order.id,shTracking:'',shSupplier:'Fixture supplier',shExpectedDate:'',shNote:''},qty={value:'4',focus(){}};
 const row={dataset:{lineId:f.lines[0].id,remain:'10'},querySelector:s=>s==='.sh-selected'?{checked:true}:qty};
 let handler=f.api;const requests=[],closed=[],notices=[];
 const context=vm.createContext({crypto,Date,Number,Object,Array,JSON,S003:{busy:false,shipmentSubmission:null,operatorId:'FIXTURE-P',operatorName:'Fixture purchaser'},val:id=>fields[id]||'',document:{querySelector:()=>({value:'supplier'})},E:()=>({querySelectorAll:()=>[row]}),toast:(...x)=>notices.push(x),T:x=>x,errorText:x=>x,closeModal:x=>closed.push(x),openPurchaseDetail(){},api:async(a,d)=>{requests.push({...d});return handler(a,d);}});
 vm.runInContext(requestId+'\n'+operator+'\n'+submit,context);
 return{context,requests,closed,notices,qty,fields,setHandler(fn){handler=fn;},submit:()=>context.submitShipment({preventDefault(){}}),count:()=>f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_shipments').get().n};
}
function materialUI(f,type){
 const source=fs.readFileSync(new URL('../003/app.js',import.meta.url),'utf8'),submit=source.slice(source.indexOf('async function submitMaterialTxn('),source.indexOf('var MATERIAL_IMPORT_HEADERS'));
 const requestId=source.split('\n').find(x=>x.startsWith('function reqId(')),operator=source.split('\n').find(x=>x.startsWith('function operatorPayload('));
 const fields={mtType:type,mtMaterialId:f.materials[0].id,mtQty:'3',mtRecipient:'Fixture worker',mtDepartment:'大货',mtPurpose:'Fixture usage',mtNote:'Fixture retry',mtDoc:'',mtCost:'0',mtSupplier:''};let handler=f.api;const requests=[],closed=[];
 const context=vm.createContext({crypto,Date,Number,Object,JSON,S003:{busy:false,materialTxnSubmission:null,operatorId:'FIXTURE-P',operatorName:'Fixture purchaser'},val:id=>String(fields[id]||'').trim(),num:id=>Number(fields[id]||0),toast(){},T:x=>x,errorText:x=>x,closeModal:x=>closed.push(x),openMaterialDetail(){},api:async(a,d)=>{requests.push({...d});return handler(a,d);}});vm.runInContext(requestId+'\n'+operator+'\n'+submit,context);
 return{context,fields,requests,closed,setHandler(fn){handler=fn;},submit:()=>context.submitMaterialTxn({preventDefault(){}})};
}
for(const [type,expected]of [['issue',7],['use',7],['return',13]])test('material '+type+' UI preserves a lost-response intent and changes stock once',async()=>{
 const f=await fixture(),id=f.materials[0].id;await f.ok('v2_003_material_txn',{material_id:id,txn_type:'stocktake',counted_qty:10});const ui=materialUI(f,type);let first=true;ui.setHandler(async(a,d)=>{const r=await f.api(a,d);if(first){first=false;throw Error('Fixture response lost after stock commit');}return r;});await ui.submit();assert.equal(ui.closed.length,0);assert.equal(ui.fields.mtQty,'3');await ui.submit();assert.equal(ui.requests[0].client_req_id,ui.requests[1].client_req_id);assert.equal(ui.closed.length,1);assert.equal(f.DB.raw.prepare('SELECT current_qty FROM v2_003_materials WHERE id=?').get(id).current_qty,expected);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_material_txns WHERE material_id=? AND txn_type=?').get(id,type).n,1);
});
test('material retry before commit retains the request key, while editing quantity starts a new intent',async()=>{const f=await fixture();await f.ok('v2_003_material_txn',{material_id:f.materials[0].id,txn_type:'stocktake',counted_qty:10});const ui=materialUI(f,'issue');ui.setHandler(async()=>{throw Error('Fixture offline');});await ui.submit();await ui.submit();assert.equal(ui.requests[0].client_req_id,ui.requests[1].client_req_id);ui.fields.mtQty='4';ui.setHandler(f.api);await ui.submit();assert.notEqual(ui.requests[1].client_req_id,ui.requests[2].client_req_id);assert.equal(f.DB.raw.prepare('SELECT current_qty FROM v2_003_materials WHERE id=?').get(f.materials[0].id).current_qty,6);});
test('material pending duplicate click sends once and a completed operation permits another intent',async()=>{const f=await fixture();await f.ok('v2_003_material_txn',{material_id:f.materials[0].id,txn_type:'stocktake',counted_qty:10});const ui=materialUI(f,'issue');let release;ui.setHandler((a,d)=>new Promise(resolve=>release=async()=>resolve(await f.api(a,d))));const pending=ui.submit();await ui.submit();assert.equal(ui.requests.length,1);await release();await pending;ui.setHandler(f.api);await ui.submit();assert.notEqual(ui.requests[0].client_req_id,ui.requests[1].client_req_id);assert.equal(f.DB.raw.prepare('SELECT current_qty FROM v2_003_materials WHERE id=?').get(f.materials[0].id).current_qty,4);});
test('one purchase assigns selected materials to separate tracking numbers and receipts maintain unit/department ledger',async()=>{
 const f=await fixture(),s1=await f.ship([0],10,{tracking_no:'FIXTURE-T1'}),s2=await f.ship([1],10,{tracking_no:'FIXTURE-T2'});
 for(const [code,index]of [['FIXTURE-T1',0],['FIXTURE-T2',1]]){const r=await f.ok('v2_003_receiving_lookup',{code});assert.equal(r.items.length,1);assert.equal(r.items[0].material_id,f.lines[index].material_id);}
 assert.equal((await f.receive(s1,[10])).order_status,'partial_received');assert.equal((await f.receive(s2,[10])).order_status,'completed');
 const id=f.lines[0].material_id;await f.ok('v2_003_material_txn',{material_id:id,txn_type:'issue',qty:3,department:'大货',recipient_name:'Fixture worker'});await f.ok('v2_003_material_txn',{material_id:id,txn_type:'return',qty:1,department:'大货',recipient_name:'Fixture worker'});
 const r=await f.ok('v2_003_material_detail',{id});assert.equal(r.item.current_qty,8);const logs=f.DB.raw.prepare('SELECT * FROM v2_003_material_txns WHERE material_id=? ORDER BY rowid').all(id);for(let i=1;i<logs.length;i++)assert.equal(logs[i].qty_before,logs[i-1].qty_after);assert.equal(logs.at(-1).department,'大货');
});
test('same shipment request replays before tracking validation, including after its purchase is closed',async()=>{
 const f=await fixture(),p={order_id:f.order.id,delivery_method:'express',tracking_no:'FIXTURE-REPLAY',client_req_id:'FIXTURE-SHIP-ONCE',items:f.lines.map(x=>({order_line_id:x.id,expected_qty:10}))};
 const s=await f.ok('v2_003_purchase_shipment_create',p);await f.receive(s,[10,10]);const retry=await f.ok('v2_003_purchase_shipment_create',p);assert.deepEqual(retry,s);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_shipments').get().n,1);
});
test('supplier shipment UI retry after a lost committed response preserves intent and creates one batch',async()=>{
 const f=await fixture(),ui=shipmentUI(f);let first=true;ui.setHandler(async(a,d)=>{const r=await f.api(a,d);if(first){first=false;throw Error('Fixture response lost after commit');}return r;});
 await ui.submit();assert.equal(ui.count(),1);assert.equal(ui.closed.length,0);assert.equal(ui.qty.value,'4');await ui.submit();assert.equal(ui.count(),1);assert.equal(ui.requests[0].client_req_id,ui.requests[1].client_req_id);assert.equal(ui.closed.length,1);assert.equal(ui.context.S003.shipmentSubmission,null);
});
test('shipment UI offline before commit retries the same intent and editing starts a new intent',async()=>{
 const f=await fixture(),ui=shipmentUI(f);ui.setHandler(async()=>{throw Error('Fixture offline');});await ui.submit();assert.equal(ui.count(),0);await ui.submit();assert.equal(ui.requests[0].client_req_id,ui.requests[1].client_req_id);ui.qty.value='5';ui.setHandler(f.api);await ui.submit();assert.notEqual(ui.requests[1].client_req_id,ui.requests[2].client_req_id);assert.equal(ui.count(),1);assert.equal(f.DB.raw.prepare('SELECT expected_qty FROM v2_003_purchase_shipment_items').get().expected_qty,5);
});
test('shipment UI blocks clicks while pending and success permits a distinct next batch',async()=>{
 const f=await fixture(),ui=shipmentUI(f);let release;ui.setHandler((a,d)=>new Promise(resolve=>release=async()=>resolve(await f.api(a,d))));const pending=ui.submit();await ui.submit();assert.equal(ui.requests.length,1);await release();await pending;ui.setHandler(f.api);await ui.submit();assert.equal(ui.count(),2);assert.notEqual(ui.requests[0].client_req_id,ui.requests[1].client_req_id);
});
async function race(f,payloads){const original=f.DB.prepare.bind(f.DB);let arrived=0,release;const gate=new Promise(r=>release=r);f.DB.prepare=sql=>{const s=original(sql);if(sql.includes('AS scheduled_qty')&&sql.includes('WHERE l.order_id=?')){const all=s.all.bind(s);s.all=async()=>{const r=await all();if(++arrived===payloads.length)release();await gate;return r;};}return s;};return Promise.all(payloads.map(x=>f.api('v2_003_purchase_shipment_create',x)));}
test('concurrent distinct shipments reserve at most the purchased quantity and leave no orphan items',async()=>{
 const f=await fixture(),results=await race(f,['A','B'].map(x=>({order_id:f.order.id,delivery_method:'express',tracking_no:'FIXTURE-RACE-'+x,items:[{order_line_id:f.lines[0].id,expected_qty:7}]})));
 assert.equal(results.filter(x=>x.ok).length,1);assert.equal(results.find(x=>!x.ok).error,'shipment_qty_exceeds_ordered');assert.equal(f.DB.raw.prepare('SELECT SUM(expected_qty) n FROM v2_003_purchase_shipment_items').get().n,7);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_shipments').get().n,1);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_shipment_items').get().n,1);
});
test('concurrent supplier retries with the same request key return one atomically committed batch',async()=>{
 const f=await fixture(),p={order_id:f.order.id,delivery_method:'supplier',client_req_id:'FIXTURE-SUPPLIER-RACE',items:[{order_line_id:f.lines[0].id,expected_qty:4}]};const r=await race(f,[p,p]);assert.equal(r.every(x=>x.ok),true);assert.equal(r[0].id,r[1].id);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_shipments').get().n,1);assert.equal(f.DB.raw.prepare('SELECT SUM(expected_qty) n FROM v2_003_purchase_shipment_items').get().n,4);
});
test('shipment transaction rechecks a cancellation made after validation',async()=>{
 const f=await fixture(),batch=f.DB.batch.bind(f.DB);let injected=false;f.DB.batch=async statements=>{if(!injected&&statements[0].sql.includes('INSERT INTO v2_003_purchase_shipments')){injected=true;f.DB.raw.prepare("UPDATE v2_003_purchase_orders SET status='cancelled' WHERE id=?").run(f.order.id);}return batch(statements);};
 const r=await f.api('v2_003_purchase_shipment_create',{order_id:f.order.id,delivery_method:'express',tracking_no:'FIXTURE-CANCEL-RACE',items:[{order_line_id:f.lines[0].id,expected_qty:5}]});assert.equal(r.error,'purchase_order_closed');assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_shipments').get().n,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_shipment_items').get().n,0);
});
test('surplus of one item cannot complete a purchase while another item is short',async()=>{
 const f=await fixture(),s=await f.ship([0,1]);const r=await f.receive(s,[20,0]);assert.equal(r.has_discrepancy,true);assert.equal(r.order_status,'partial_received');
});
test('duplicate receipt commits actual inventory only once even with changed retry quantities',async()=>{
 const f=await fixture(),s=await f.ship();await f.receive(s,[6]);assert.equal((await f.receive(s,[20])).duplicate,true);assert.equal(f.DB.raw.prepare('SELECT current_qty FROM v2_003_materials WHERE id=?').get(f.lines[0].material_id).current_qty,6);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_receipts').get().n,1);
});
test('incomplete or invalid-location receipt is rejected without stock writes',async()=>{
 const f=await fixture(),s=await f.ship(),d=await f.ok('v2_003_receiving_lookup',{code:s.shipment_no});assert.equal((await f.api('v2_003_receipt_confirm',{shipment_id:s.id,items:[]})).error,'receipt_lines_incomplete');assert.equal((await f.api('v2_003_receipt_confirm',{shipment_id:s.id,items:[{shipment_item_id:d.items[0].id,received_qty:5,location_code:'UNKNOWN'}]})).error,'invalid_putaway_location');assert.equal(f.DB.raw.prepare('SELECT SUM(current_qty) n FROM v2_003_materials').get().n,0);
});
test('receipt batch failure rolls back inventory, ledger and receipt before a clean retry',async()=>{
 const f=await fixture(),s=await f.ship(),d=await f.ok('v2_003_receiving_lookup',{code:s.shipment_no}),p={shipment_id:s.id,items:[{shipment_item_id:d.items[0].id,received_qty:5,location_code:'FIXTURE-A01'}]},batch=f.DB.batch.bind(f.DB);let fail=true;
 f.DB.batch=async statements=>{if(fail&&statements[0].sql.includes('INSERT INTO v2_003_purchase_receipts')){fail=false;return batch([...statements,f.DB.prepare('INSERT INTO nonexistent_fixture_table VALUES(1)')]);}return batch(statements);};
 assert.equal((await f.api('v2_003_receipt_confirm',p)).ok,false);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_receipts').get().n,0);assert.equal(f.DB.raw.prepare('SELECT SUM(current_qty) n FROM v2_003_materials').get().n,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_material_txns').get().n,0);await f.ok('v2_003_receipt_confirm',p);assert.equal(f.DB.raw.prepare('SELECT current_qty FROM v2_003_materials WHERE id=?').get(f.lines[0].material_id).current_qty,5);
});
test('supplier arrival photo prerequisite and cancelled purchase cannot create receipts',async()=>{
 const f=await fixture(),s=await f.ship([0],5,{delivery_method:'supplier',tracking_no:''}),d=await f.ok('v2_003_receiving_lookup',{code:s.shipment_no});assert.equal((await f.api('v2_003_receipt_confirm',{shipment_id:s.id,items:[{shipment_item_id:d.items[0].id,received_qty:5,location_code:'FIXTURE-A01'}]})).error,'arrival_photo_required');await f.ok('v2_003_purchase_order_close',{id:f.order.id,mode:'cancel',reason:'Fixture cancel'});assert.equal((await f.receive(s,[5])).shipment_status,'cancelled');assert.equal(f.DB.raw.prepare('SELECT SUM(current_qty) n FROM v2_003_materials').get().n,0);
});
