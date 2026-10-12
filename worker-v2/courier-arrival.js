import {trackingNumbers} from '../shared/courier-rules.js';
import {reusableCourierPlan} from './courier-claims.js';

// Exact, read-only preflight. Saving still validates and claims within its own transaction.
// One JSON binding also keeps the 200-waybill limit below D1's bound-variable limit.
export async function lookupCourierArrival(env,body){
 if(typeof body.tracking_nos!=='string'&&!Array.isArray(body.tracking_nos))throw Error('请填写快递单号 / 송장번호를 입력하세요');
 const codes=trackingNumbers(body.tracking_nos);
 if(!codes.length)return {ok:true,total:0,received:0,arrival_status:'pending',items:[]};
 const result=await env.DB.prepare(`SELECT requested.value AS tracking_no,
  r.id AS receipt_id,r.owner,r.received_at,r.scanner_id,r.scanner_name,
  bi.batch_id,cb.batch_no,i.plan_id,p.display_no,p.status AS plan_status,
  CASE WHEN i.plan_id IS NULL THEN 'available'
   WHEN p.id IS NOT NULL AND (${reusableCourierPlan('p')}) THEN 'reusable'
   ELSE 'conflict' END AS claim_status
  FROM json_each(?) requested
  LEFT JOIN ck_courier_receipts r ON r.tracking_no=requested.value
  LEFT JOIN ck_courier_batch_items bi ON bi.receipt_id=r.id
  LEFT JOIN ck_courier_batches cb ON cb.id=bi.batch_id
  LEFT JOIN ck_courier_plan_items i ON i.tracking_no=requested.value
  LEFT JOIN v2_inbound_plans p ON p.id=i.plan_id
  ORDER BY CAST(requested.key AS INTEGER)`).bind(JSON.stringify(codes)).all();
 const items=result.results||[],received=items.filter(x=>x.receipt_id).length;
 return {ok:true,total:codes.length,received,arrival_status:received===codes.length?'arrived':received?'partial':'pending',items};
}
