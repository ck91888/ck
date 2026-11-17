import {intentSignature,boundReceiptJSON,readBoundReceipt,canonicalIntent} from './transaction-receipt.js';

// New receipts bind normalized business input; legacy receipts must still match
// their persisted shipment and exact item set before they can be replayed.
export async function purchaseShipmentReceipt(env,key,input){
 const signature=key?await intentSignature(input):'';
 const legacyMatch=async saved=>{
  const shipment=await env.DB.prepare('SELECT * FROM v2_003_purchase_shipments WHERE id=?').bind(String(saved.id||'')).first();
  if(!shipment||saved.shipment_no!==shipment.shipment_no||shipment.order_id!==input.order_id||shipment.delivery_method!==input.delivery_method||
   String(shipment.tracking_no||'')!==input.tracking_no||String(shipment.expected_date||'')!==input.expected_date||String(shipment.note||'')!==input.note||String(shipment.created_by||'')!==input.operator.name)return false;
  let supplier=input.supplier;
  if(supplier===null){const order=await env.DB.prepare('SELECT supplier FROM v2_003_purchase_orders WHERE id=?').bind(input.order_id).first();if(!order)return false;supplier=String(order.supplier||'');}
  if(String(shipment.supplier||'')!==supplier)return false;
  const rows=(await env.DB.prepare('SELECT order_line_id,expected_qty FROM v2_003_purchase_shipment_items WHERE shipment_id=? ORDER BY order_line_id').bind(shipment.id).all()).results||[];
  return canonicalIntent(rows.map(x=>({order_line_id:x.order_line_id,expected_qty:Number(x.expected_qty)})))===canonicalIntent(input.items);
 };
 return {read:()=>readBoundReceipt(env,{key,action:'v2_003_purchase_shipment_create',signature,legacyMatch}),encode:response=>boundReceiptJSON(response,signature)};
}
