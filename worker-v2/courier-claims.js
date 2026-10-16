import {ensureSchema} from './schema-ready.js';
import {inboundFlowEnabled} from './inbound-flow.js';
import {ensureCourierBatches} from './courier-batches.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
export const COURIER_CLAIM_HISTORY_SQL=`CREATE TABLE IF NOT EXISTS ck_courier_plan_item_history(tracking_no TEXT NOT NULL,plan_id TEXT NOT NULL,line_id TEXT NOT NULL,released_at TEXT NOT NULL,replacement_plan_id TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,receipt_json TEXT NOT NULL,PRIMARY KEY(plan_id,tracking_no))`;

// A cancelled plan can release an expected parcel only before any real work.
// Keep this predicate identical in the helpful preflight and the write transaction.
export const reusableCourierPlan=p=>`${p}.status='cancelled' AND COALESCE(${p}.is_deleted,0)=0
 AND COALESCE(${p}.accounted,0)=0 AND COALESCE(${p}.force_completed,0)=0
 AND COALESCE(${p}.manual_completed_at,'')=''
 AND (COALESCE(${p}.unload_completed_at,'')='' OR NOT EXISTS(SELECT 1 FROM v2_inbound_plan_lines l WHERE l.plan_id=${p}.id AND l.unit_type!='courier' AND l.planned_qty>0))
 AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_lines l WHERE l.plan_id=${p}.id AND (COALESCE(l.putaway_qty,0)!=0
  OR (COALESCE(l.actual_qty,0)!=0 AND (l.unit_type!='courier' OR l.actual_qty!=((SELECT COUNT(*) FROM ck_courier_plan_items ci JOIN ck_courier_receipts cr ON cr.tracking_no=ci.tracking_no WHERE ci.line_id=l.id)+(SELECT COUNT(*) FROM ck_courier_plan_item_history h WHERE h.line_id=l.id AND json_extract(h.receipt_json,'$.receipt_id') IS NOT NULL AND NOT EXISTS(SELECT 1 FROM ck_courier_plan_items ci WHERE ci.plan_id=h.plan_id AND ci.tracking_no=h.tracking_no)))))))
 AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=${p}.id AND (j.status!='cancelled'
   OR EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.job_id=j.id)
   OR EXISTS(SELECT 1 FROM v2_ops_job_results r WHERE r.job_id=j.id)))
 AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_biz_tasks t WHERE t.plan_id=${p}.id AND t.status!='pending')
 AND NOT EXISTS(SELECT 1 FROM v2_outbound_orders o WHERE o.source_inbound_plan_id=${p}.id)
 AND NOT EXISTS(SELECT 1 FROM sop_records s WHERE s.kind='need' AND json_extract(s.state,'$.source_type')='inbound' AND json_extract(s.state,'$.source_id')=${p}.id AND COALESCE(json_extract(s.state,'$.status'),'')!='cancelled')`;

export async function ensureCourierClaims(env){
 if(!inboundFlowEnabled(env))return;
 await ensureCourierBatches(env);
 await ensureSchema(env.DB,'courier-claims-v1',()=>env.DB.batch([
  q(env,COURIER_CLAIM_HISTORY_SQL),
  q(env,`CREATE TRIGGER IF NOT EXISTS ck_courier_released_plan_guard BEFORE UPDATE OF status ON v2_inbound_plans
   WHEN NEW.status!='cancelled' AND EXISTS(SELECT 1 FROM ck_courier_plan_item_history h WHERE h.plan_id=NEW.id)
   BEGIN SELECT RAISE(ABORT,'已释放快递占用的取消计划不能恢复，请使用新的入库计划 / 취소된 계획은 복원할 수 없습니다'); END`),
  q(env,`CREATE TRIGGER IF NOT EXISTS ck_courier_history_delete_guard BEFORE DELETE ON v2_inbound_plans
   WHEN EXISTS(SELECT 1 FROM ck_courier_plan_item_history h WHERE h.plan_id=OLD.id)
   BEGIN SELECT RAISE(ABORT,'此计划保留快递重建历史，不能删除 / 택배 이력이 있어 삭제할 수 없습니다'); END`),
  q(env,`CREATE TRIGGER IF NOT EXISTS ck_courier_history_edit_guard BEFORE UPDATE OF plan_date,customer,biz_class,biz_classes_json,cargo_summary,expected_arrival,purpose,remark,external_inbound_no,bulk_external_inbound_no ON v2_inbound_plans
   WHEN OLD.status='cancelled' AND EXISTS(SELECT 1 FROM ck_courier_plan_item_history h WHERE h.plan_id=OLD.id)
   BEGIN SELECT RAISE(ABORT,'此计划已取消并保留快递历史，不能修改 / 취소된 계획은 수정할 수 없습니다'); END`),
  q(env,`CREATE TRIGGER IF NOT EXISTS ck_courier_claim_insert_guard BEFORE INSERT ON ck_courier_plan_items
   WHEN NOT EXISTS(SELECT 1 FROM v2_inbound_plans p WHERE p.id=NEW.plan_id AND p.status='pending' AND COALESCE(p.is_deleted,0)=0)
   BEGIN SELECT RAISE(ABORT,'入库计划状态已变化，请刷新 / 계획 상태가 변경되었습니다'); END`),
  q(env,`CREATE TRIGGER IF NOT EXISTS ck_courier_claim_edit_guard BEFORE UPDATE ON ck_courier_plan_items
   WHEN NOT EXISTS(SELECT 1 FROM v2_inbound_plans p WHERE p.id=OLD.plan_id AND p.status='pending' AND COALESCE(p.is_deleted,0)=0)
    OR EXISTS(SELECT 1 FROM ck_courier_plan_items ci JOIN ck_courier_receipts r ON r.tracking_no=ci.tracking_no WHERE ci.plan_id=OLD.plan_id)
    OR EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=OLD.plan_id AND j.status!='cancelled')
   BEGIN SELECT RAISE(ABORT,'计划状态或收货作业已变化，不能修改 / 계획 또는 수령 상태가 변경되었습니다'); END`),
  ...['INSERT','UPDATE OF status,related_doc_type,related_doc_id'].map((operation,i)=>q(env,`CREATE TRIGGER IF NOT EXISTS ck_courier_cancelled_job_${i} BEFORE ${operation} ON v2_ops_jobs
   WHEN NEW.related_doc_type='inbound_plan' AND NEW.status NOT IN ('cancelled','completed')
    AND EXISTS(SELECT 1 FROM v2_inbound_plans p WHERE p.id=NEW.related_doc_id AND (p.status='cancelled' OR COALESCE(p.is_deleted,0)!=0)
     AND (EXISTS(SELECT 1 FROM ck_courier_plan_items ci WHERE ci.plan_id=p.id) OR EXISTS(SELECT 1 FROM ck_courier_plan_item_history h WHERE h.plan_id=p.id)))
   BEGIN SELECT RAISE(ABORT,'关联快递入库计划已取消或删除，请刷新 / 취소된 계획입니다'); END`))
 ]));
}

export function claimCourierStatements(env,planId,lineId,code){
 const user=env.SOP_REQUEST_USER||{},safe=`EXISTS(SELECT 1 FROM v2_inbound_plans p WHERE p.id=ck_courier_plan_items.plan_id AND ${reusableCourierPlan('p')})`;
 return [
  q(env,`INSERT INTO ck_courier_plan_item_history(tracking_no,plan_id,line_id,released_at,replacement_plan_id,actor_id,actor_name,receipt_json)
   SELECT tracking_no,plan_id,line_id,?,?,?,?,COALESCE((SELECT json_object('receipt_id',r.id,'owner',r.owner,'received_at',r.received_at,'scanner_id',r.scanner_id,'scanner_name',r.scanner_name,'status',r.status,'handed_to',r.handed_to,'handed_at',r.handed_at,'batch_id',bi.batch_id,'batch_no',b.batch_no) FROM ck_courier_receipts r LEFT JOIN ck_courier_batch_items bi ON bi.receipt_id=r.id LEFT JOIN ck_courier_batches b ON b.id=bi.batch_id WHERE r.tracking_no=ck_courier_plan_items.tracking_no),'{}') FROM ck_courier_plan_items WHERE tracking_no=? AND plan_id!=? AND ${safe}`,
   new Date().toISOString(),planId,String(user.id||''),String(user.name||''),code,planId),
  q(env,`DELETE FROM ck_courier_plan_items WHERE tracking_no=? AND plan_id!=? AND ${safe}
   AND EXISTS(SELECT 1 FROM ck_courier_plan_item_history h WHERE h.plan_id=ck_courier_plan_items.plan_id AND h.tracking_no=ck_courier_plan_items.tracking_no AND h.replacement_plan_id=?)`,code,planId,planId),
  q(env,'INSERT INTO ck_courier_plan_items(tracking_no,plan_id,line_id) VALUES(?,?,?)',code,planId,lineId)
 ];
}
