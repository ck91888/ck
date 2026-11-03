import {ensureSchema} from './schema-ready.js';

// Ephemeral assertions run in the SAME batch as all line/header writes. A failed
// CHECK aborts the whole transaction, rather than silently skipping one UPDATE.
export async function purchaseEditGuard(env,orderId,lines){
 await ensureSchema(env.DB,'purchase-edit-checks-v1',()=>env.DB.prepare(`CREATE TABLE IF NOT EXISTS ck_purchase_edit_checks (
  id TEXT PRIMARY KEY,
  order_open INTEGER NOT NULL CONSTRAINT ck_purchase_edit_order_open CHECK(order_open=1),
  lines_current INTEGER NOT NULL CONSTRAINT ck_purchase_edit_lines_current CHECK(lines_current=1),
  quantity_valid INTEGER NOT NULL CONSTRAINT ck_purchase_edit_quantity_valid CHECK(quantity_valid=1))`).run());
 const id=crypto.randomUUID();
 const before=[env.DB.prepare(`INSERT INTO ck_purchase_edit_checks(id,order_open,lines_current,quantity_valid)
  SELECT ?,CASE WHEN EXISTS(SELECT 1 FROM v2_003_purchase_orders WHERE id=? AND status NOT IN ('completed','cancelled')) THEN 1 ELSE 0 END,
   CASE WHEN (SELECT COUNT(*) FROM v2_003_purchase_order_lines WHERE order_id=?)=? THEN 1 ELSE 0 END,1`)
  .bind(id,orderId,orderId,lines.length)];
 for(const line of lines){
  before.push(env.DB.prepare(`UPDATE ck_purchase_edit_checks SET
   lines_current=CASE WHEN EXISTS(SELECT 1 FROM v2_003_purchase_order_lines WHERE id=? AND order_id=?) THEN 1 ELSE 0 END,
   quantity_valid=CASE WHEN ?>=COALESCE((SELECT SUM(si.expected_qty)
    FROM v2_003_purchase_shipment_items si JOIN v2_003_purchase_shipments s ON s.id=si.shipment_id
    WHERE si.order_line_id=? AND s.status!='cancelled'),0) THEN 1 ELSE 0 END WHERE id=?`)
   .bind(line.id,orderId,line.orderedQty,line.id,id));
 }
 return {before,after:[env.DB.prepare('DELETE FROM ck_purchase_edit_checks WHERE id=?').bind(id)]};
}

export function purchaseEditError(error){
 const message=String(error?.message||'');
 for(const [constraint,code] of [['order_open','purchase_order_closed'],['lines_current','purchase_order_lines_changed'],['quantity_valid','ordered_qty_below_shipped']]){
  if(message.includes('CHECK constraint failed: ck_purchase_edit_'+constraint))return code;
 }
 return null;
}
