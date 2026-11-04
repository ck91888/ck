import assert from 'node:assert/strict';
import {locationFixture} from './location-picker-fixture.mjs';
export async function receiptFixture(t,options){
 const f=await locationFixture(t,options),ok=(action,data={})=>f.ok('office',action,data);
 const material=await ok('v2_003_material_save',{name_zh:'Synthetic receipt lifecycle material',category:'包材',unit:'个',opening_qty:0});
 const count=options?.shipments||3,quantity=count*10;
 const order=await ok('v2_003_purchase_request_create',{request_reason:'Synthetic receipt lifecycle order',lines:[{material_id:material.id,requested_qty:quantity}]});
 const detail=await ok('v2_003_purchase_order_detail',{id:order.id}),line=detail.lines[0];await ok('v2_003_purchase_order_update',{id:order.id,lines:[{id:line.id,ordered_qty:quantity,unit_cost:1}]});
 const shipments=[];for(const n of ['A','B','C','D','E'].slice(0,count)){const s=await ok('v2_003_purchase_shipment_create',{order_id:order.id,delivery_method:'express',tracking_no:'SYN-LIFECYCLE-'+n,items:[{order_line_id:line.id,expected_qty:10}],client_req_id:'SYN-SHIP-'+n});shipments.push({...s,detail:await ok('v2_003_receiving_lookup',{code:s.shipment_no})});}
 const receive=(s,qty=7)=>({shipment_id:s.id,items:s.detail.items.map(x=>({shipment_item_id:x.id,received_qty:qty,location_code:'FE01-A01',note:'Synthetic receipt line'})),discrepancy_note:'Synthetic short receipt',client_req_id:'SYN-RECEIPT-'+s.id});
 const stock=async()=>Number((await f.rows('SELECT current_qty FROM v2_003_materials WHERE id=?',material.id))[0].current_qty);
 assert.equal(await stock(),0);return{...f,ok,material,order,line,shipments,receive,stock};
}
