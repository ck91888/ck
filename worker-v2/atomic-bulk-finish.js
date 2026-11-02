import {bulkOutputFinalGuard} from './bulk-output-draft.js';
import {partnerShareGuard,shareConflict,notedShareGrant} from './dispatch-share.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
export async function atomicBulkFinish(e,b,job,data){
 const id=job.id,t=new Date().toISOString(),token=crypto.randomUUID(),resultId='RES-FINAL-'+id,by=String(b.worker_id||e.SOP_REQUEST_USER?.id||''),actor=e.SOP_REQUEST_USER||{},gate="EXISTS(SELECT 1 FROM v2_ops_job_results WHERE id=? AND json_extract(result_json,'$._completion_request')=?)",g=[resultId,token],result={ok:true};
 const reviewed={...data,_completion_request:token,dispatch_actor_id:String(actor.id||''),dispatch_actor_name:String(actor.name||'')};
 const finalGuard=e.SOP_GROUP_FINISH?'':" AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND worker_id!=? AND left_at='')",args=e.SOP_GROUP_FINISH?[]:[id,by];
 const record=await q(e,"SELECT * FROM sop_records WHERE id=? AND kind='dispatch'",id).first();
 const sql=[...bulkOutputFinalGuard(e,b,record,actor),...await partnerShareGuard(e,record,actor,notedShareGrant(b)),q(e,`INSERT INTO v2_ops_job_results(id,job_id,remark,result_json,result_lines_json,created_by,created_at) SELECT ?,?,?,?,?,?,? FROM v2_ops_jobs WHERE id=? AND status IN ('pending','working','awaiting_close')${finalGuard}`,resultId,id,String(b.remark||''),JSON.stringify(reviewed),'[]',by,t,id,...args),
 q(e,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='job_completed' WHERE job_id=? AND left_at='' AND ${gate}`,t,t,id,...g),
 q(e,`UPDATE v2_ops_jobs SET status='completed',finished_at=?,active_worker_count=0,customer=?,updated_at=? WHERE id=? AND ${gate}`,t,data.customer,t,id,...g)];
 if(job.linked_outbound_order_id){let meta={};try{meta=JSON.parse(job.shared_result_json||'{}');}catch{}const reopen=meta.started_from_reopen_pending?1:0;
  sql.push(q(e,`UPDATE v2_outbound_orders SET actual_box_count=CASE WHEN ?=1 THEN COALESCE(actual_box_count,0)+? ELSE ? END,actual_pallet_count=CASE WHEN ?=1 THEN COALESCE(actual_pallet_count,0)+? ELSE ? END,status='ready_to_ship',updated_at=? WHERE id=? AND ${gate}`,reopen,data.total_operated_box_count,data.total_operated_box_count,reopen,data.pallet_count,data.pallet_count,t,job.linked_outbound_order_id,...g));
 }
 if(b.client_req_id)sql.push(q(e,`INSERT OR IGNORE INTO v2_idempotency_keys(idem_key,action,response_json,created_at) SELECT ?,'v2_bulk_op_job_finish',?,? WHERE ${gate}`,b.client_req_id,JSON.stringify(result),t,...g));
 try{await e.DB.batch(sql);}catch(error){if(shareConflict.test(error.message||''))throw Error('记录已更新或协作已结束，请刷新后再操作');if((await q(e,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first())?.status==='completed')return {ok:true,already_completed:true};throw error;}
 const saved=await q(e,'SELECT result_json FROM v2_ops_job_results WHERE id=?',resultId).first();if(!saved)throw Error('任务或参与人员已变化，请刷新核对 / 작업 상태를 확인하세요');return JSON.parse(saved.result_json)._completion_request===token?result:{ok:true,already_completed:true};
}
