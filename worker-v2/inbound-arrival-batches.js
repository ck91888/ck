// Partial courier arrivals are physical receipts, never planned quantities.
// A batch records the available quantities at dispatch; later receipts cannot
// silently enlarge an earlier job's output allowance.
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
export const arrivalBatchTypes=['inbound_direct_tally','inbound_bulk'];
export async function inboundArrivalState(e,plan){
 if(typeof plan==='string')plan=await q(e,'SELECT * FROM v2_inbound_plans WHERE id=?',plan).first();
 if(!plan)return null;
 const receipt=await q(e,`SELECT COUNT(*) AS total,COUNT(r.id) AS received FROM ck_courier_plan_items i LEFT JOIN ck_courier_receipts r ON r.tracking_no=i.tracking_no WHERE i.plan_id=?`,plan.id).first();
 if(!receipt.total)return null;
 const lines=(await q(e,`SELECT l.id,l.unit_type,l.planned_qty,COALESCE(l.putaway_qty,0) AS tallied,
  CASE WHEN l.unit_type='courier' THEN (SELECT COUNT(*) FROM ck_courier_plan_items i JOIN ck_courier_receipts r ON r.tracking_no=i.tracking_no WHERE i.plan_id=l.plan_id AND i.line_id=l.id)
   WHEN COALESCE(?, '')!='' THEN COALESCE(l.actual_qty,0) ELSE 0 END AS arrived
  FROM v2_inbound_plan_lines l WHERE l.plan_id=? ORDER BY l.line_no,l.id`,plan.unload_completed_at||'',plan.id).all()).results||[];
 const batched=!!await q(e,'SELECT 1 FROM ck_inbound_arrival_batches WHERE plan_id=? LIMIT 1',plan.id).first();
 const truck=lines.some(l=>l.unit_type!=='courier'&&l.planned_qty>0);
 const fully_arrived=receipt.received===receipt.total&&(!truck||!!plan.unload_completed_at);
 const caps=lines.map(l=>({line_id:l.id,unit_type:l.unit_type,arrived:Number(l.arrived),tallied:Number(l.tallied),available:Math.max(0,Number(l.arrived)-Number(l.tallied))}));
 return {...receipt,batched,fully_arrived,lines:caps,remaining:caps.reduce((s,l)=>s+l.available,0)};
}
export async function inboundArrivalScope(e,plan,code,type){
 if(!arrivalBatchTypes.includes(type))return null;
 const state=await inboundArrivalState(e,plan);
 if(!state||state.fully_arrived&&!state.batched)return null;
 if(!state.received)throw Error('尚无已到仓快递，不能开始分批理货 / 도착한 택배가 없습니다');
 const previous=await q(e,`SELECT 1 FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND inbound_external_no=? AND job_type=? AND status='completed' LIMIT 1`,plan.id,code,type).first();
 if(!state.remaining&&(!state.fully_arrived||previous))throw Error('已到仓部分已理货，请等待后续到货 / 도착분 검수 완료·추가 입고 대기');
 return {plan_id:plan.id,external_no:code,fully_arrived:state.fully_arrived,received:state.received,total:state.total,lines:state.lines};
}
export const arrivalBatchRead=(e,id)=>q(e,'SELECT * FROM ck_inbound_arrival_batches WHERE job_id=?',id).first();
export function arrivalBatchStatement(e,id,scope,t){
 return q(e,'INSERT INTO ck_inbound_arrival_batches(job_id,plan_id,external_no,scope_json,created_at) VALUES(?,?,?,?,?)',id,scope.plan_id,scope.external_no,JSON.stringify(scope),t);
}
export function arrivalBatchOutput(scope,body){
 const input=body.result_lines??[],allowed=new Map();
 for(const line of scope.lines)allowed.set(line.unit_type,(allowed.get(line.unit_type)||0)+line.available);
 if(!Array.isArray(input))throw Error('请填写本批实际理货数量 / 이번 검수 수량을 입력하세요');
 const quantities=new Map();
 for(const line of input){
  const qty=Number(line?.putaway_qty??0),unit=line?.unit_type;
  if(quantities.has(unit)||!allowed.has(unit)||!Number.isFinite(qty)||qty<0||qty>(allowed.get(unit)||0))throw Error('理货数量超过本批已到仓未理货数量，请刷新核对 / 도착 미검수 수량을 확인하세요');
  quantities.set(unit,qty);
 }
 if(!scope.fully_arrived&&![...quantities.values()].some(n=>n>0))throw Error('请填写本批实际理货数量 / 이번 검수 수량을 입력하세요');
 // Distribute a unit total once across original lines, rather than multiplying
 // the same output into every line with that unit.
 const remaining=new Map(quantities);
 return scope.lines.map(line=>{const qty=Math.min(line.available,remaining.get(line.unit_type)||0);remaining.set(line.unit_type,(remaining.get(line.unit_type)||0)-qty);return {...line,putaway_qty:qty};});
}
// Used inside the completing transaction, after quantities have been applied.
// Missing courier receipts or the truck milestone keep the whole plan pending.
export function arrivalSettlementSQL(planId){
 return `(NOT EXISTS(SELECT 1 FROM ck_inbound_arrival_batches ab WHERE ab.plan_id=${planId}) OR (
  NOT EXISTS(SELECT 1 FROM ck_courier_plan_items ci WHERE ci.plan_id=${planId} AND NOT EXISTS(SELECT 1 FROM ck_courier_receipts cr WHERE cr.tracking_no=ci.tracking_no))
  AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_lines al JOIN v2_inbound_plans ap ON ap.id=al.plan_id WHERE al.plan_id=${planId} AND ((al.unit_type!='courier' AND al.planned_qty>0 AND COALESCE(ap.unload_completed_at,'')='') OR COALESCE(al.putaway_qty,0)<COALESCE(al.actual_qty,0)))
 ))`;
}
