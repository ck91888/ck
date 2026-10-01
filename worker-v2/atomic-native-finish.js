import {departmentCodes,inboundCodes,needsPutaway} from './inbound-flow.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const active="('pending','working','awaiting_close')";
// The stable result primary key claims one completion; a failed batch rolls back the claim too.
export async function atomicNativeFinish(env,body,job,{inbound=false,biz=''}={}){
 const t=new Date().toISOString(),id=job.id,resultId='RES-FINAL-'+id,token=crypto.randomUUID(),by=String(body.worker_id||env.SOP_REQUEST_USER?.id||'');
 const isReturn=job.job_type==='inbound_return',lines=isReturn?[]:(body.result_lines||[]),data=inbound?{remark:String(body.remark||''),result_note:String(body.result_note||''),result_lines:lines,...(isReturn?{is_return:true}:{extra_ops:body.extra_ops||{}})}:(body.shared_result||{});
 const plan=inbound&&job.related_doc_id?await q(env,'SELECT * FROM v2_inbound_plans WHERE id=?',job.related_doc_id).first():null;
 const json=JSON.stringify({...data,_completion_request:token}),result=inbound?{ok:true,result_id:resultId}:{ok:true},claimKey='NATIVE-FINISH:'+id;
 const gate=inbound?"EXISTS(SELECT 1 FROM v2_ops_job_results WHERE id=? AND json_extract(result_json,'$._completion_request')=?)":"EXISTS(SELECT 1 FROM v2_idempotency_keys WHERE idem_key=? AND json_extract(response_json,'$._completion_request')=?)",g=[inbound?resultId:claimKey,token],sql=[];
 const planGuard=plan?` AND EXISTS(SELECT 1 FROM v2_inbound_plans p WHERE p.id=? AND COALESCE(p.is_deleted,0)=0 AND ${isReturn?"p.status!='cancelled'":"p.status IN ('putting_away','partially_completed')"} AND COALESCE(p.external_inbound_no,'')=? AND COALESCE(p.bulk_external_inbound_no,'')=?)${isReturn?'':` AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs uj WHERE uj.plan_id=? AND uj.related_doc_type='inbound_plan' AND uj.job_type='unload' AND uj.status IN ${active}) AND NOT EXISTS(SELECT 1 FROM ck_courier_plan_items ci WHERE ci.plan_id=? AND NOT EXISTS(SELECT 1 FROM ck_courier_receipts cr WHERE cr.tracking_no=ci.tracking_no))`}`:'';
 const planArgs=plan?[plan.id,plan.external_inbound_no||'',plan.bulk_external_inbound_no||'',...(isReturn?[]:[plan.id,plan.id])]:[];
 if(inbound)sql.push(q(env,`INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,result_lines_json,created_by,created_at)
 SELECT ?,?,?,?,?,?,?,?,? FROM v2_ops_jobs WHERE id=? AND status IN ${active}${planGuard}`,resultId,id,0,0,String(body.remark||''),json,JSON.stringify(lines),by,t,id,...planArgs));
 else{
  sql.push(q(env,`INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) SELECT ?,'v2_ops_job_finish',?,? FROM v2_ops_jobs WHERE id=? AND status IN ${active}`,claimKey,JSON.stringify({...result,_completion_request:token}),t,id));
  if(body.box_count!=null||body.pallet_count!=null||body.remark)sql.push(q(env,`INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,created_by,created_at) SELECT ?,?,?,?,?,?,?,? WHERE ${gate}`,resultId,id,Number(body.box_count||0),Number(body.pallet_count||0),String(body.remark||''),JSON.stringify(data),by,t,...g));
 }
 sql.push(q(env,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='job_completed' WHERE job_id=? AND left_at='' AND ${gate}`,t,t,id,...g));
 sql.push(q(env,`UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,shared_result_json=?,finished_at=?,updated_at=? WHERE id=? AND ${gate}`,JSON.stringify(data),t,t,id,...g));
 if(inbound&&job.related_doc_id){
  const pid=job.related_doc_id;
  if(plan){
   for(const line of lines){if(line?.unit_type&&Number(line.putaway_qty||0)>0)sql.push(q(env,`UPDATE v2_inbound_plan_lines SET putaway_qty=COALESCE(putaway_qty,0)+?,putaway_remark=? WHERE plan_id=? AND unit_type=? AND ${gate}`,Number(line.putaway_qty),String(line.putaway_remark||''),pid,String(line.unit_type),...g));}
   const codes=departmentCodes(plan,biz),allCodes=inboundCodes(plan.external_inbound_no),codesJson=JSON.stringify(codes),single=allCodes.length===1?1:0;
   // Evaluate references after this job's completion, inside the same batch, for concurrent departmental finishes.
   const referencesDone=`NOT EXISTS(SELECT 1 FROM json_each(?) c WHERE NOT EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.related_doc_type='inbound_plan' AND j.related_doc_id=? AND j.job_type IN ('inbound_direct','inbound_bulk') AND j.status='completed' AND (j.inbound_external_no=c.value OR (?=1 AND COALESCE(j.inbound_external_no,'')=''))))`;
   const jobMatch=`j.related_doc_type='inbound_plan' AND j.related_doc_id=? AND (j.id=? OR (j.job_type IN ('inbound_direct','inbound_bulk') AND (j.inbound_external_no IN (SELECT value FROM json_each(?)) OR (?=1 AND COALESCE(j.inbound_external_no,'')=''))))`;
   const workers=`SELECT w.* FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE ${jobMatch}`;
   const taskBiz=isReturn?'return':biz;
   if(taskBiz){
    sql.push(q(env,`INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks(id,plan_id,biz_class,job_type,status,created_at,updated_at) SELECT ?,?,?,?,'pending',?,? WHERE NOT EXISTS(SELECT 1 FROM v2_inbound_plan_biz_tasks WHERE plan_id=? AND biz_class=?) AND ${gate}`,'IBT-'+crypto.randomUUID(),pid,taskBiz,job.job_type,t,t,pid,taskBiz,...g));
    sql.push(q(env,`UPDATE v2_inbound_plan_biz_tasks SET status='completed',job_id=?,started_at=COALESCE(NULLIF(started_at,''),(SELECT MIN(joined_at) FROM (${workers})),?),completed_at=?,completed_by=?,
     worker_names=COALESCE((SELECT group_concat(worker_name,'、') FROM (SELECT DISTINCT worker_name FROM (${workers}) WHERE worker_name!='')),''),total_minutes=ROUND(COALESCE((SELECT SUM(minutes_worked) FROM (${workers})),0)),updated_at=?
     WHERE plan_id=? AND biz_class=? AND status!='completed' AND ${referencesDone} AND ${gate}`,id,pid,id,codesJson,single,job.created_at||t,t,by,pid,id,codesJson,single,pid,id,codesJson,single,t,pid,taskBiz,codesJson,pid,single,...g));
   }
   if(plan.source_type==='return_session')sql.push(q(env,`UPDATE v2_inbound_plans SET status='completed',updated_at=? WHERE id=? AND ${gate}`,t,pid,...g));
   else{
    // Non-putaway dispositions keep their existing unload milestone semantics.
    if(!['external_inbound'].includes(plan.source_type))sql.push(q(env,`UPDATE v2_inbound_plan_biz_tasks SET status='completed',completed_at=?,completed_by=?,completion_source='unload',completion_note='卸货完成后自动入库',updated_at=? WHERE plan_id=? AND biz_class IN ('bulk','return','change_order') AND status!='completed'
     AND EXISTS(SELECT 1 FROM v2_inbound_plans p WHERE p.id=? AND p.unload_completed_at!='')
     AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=? AND j.related_doc_type='inbound_plan' AND j.job_type='unload' AND j.status IN ${active})
     AND NOT EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.related_doc_type='inbound_plan' AND j.related_doc_id=? AND j.job_type=v2_inbound_plan_biz_tasks.job_type AND j.status IN ${active} AND (?=0 OR v2_inbound_plan_biz_tasks.biz_class!='bulk')) AND ${gate}`,plan.unload_completed_at||t,plan.unload_completed_by||'',t,pid,pid,pid,pid,needsPutaway(plan)?1:0,...g));
    const allJson=JSON.stringify(allCodes),allDone=referencesDone;
    sql.push(q(env,`UPDATE v2_inbound_plans SET status=CASE
      WHEN EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=? AND j.related_doc_type='inbound_plan' AND j.job_type='unload' AND j.status IN ${active}) THEN CASE WHEN EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=? AND j.related_doc_type='inbound_plan' AND j.job_type LIKE 'inbound%' AND j.status IN ${active}) THEN 'unloading_putting_away' ELSE 'unloading' END
      WHEN NOT EXISTS(SELECT 1 FROM v2_inbound_plan_biz_tasks b WHERE b.plan_id=? AND b.status!='completed') AND ${allDone} AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=? AND j.related_doc_type='inbound_plan' AND j.job_type LIKE 'inbound%' AND j.status IN ${active}) THEN 'completed'
      WHEN EXISTS(SELECT 1 FROM v2_inbound_plan_biz_tasks b WHERE b.plan_id=? AND b.status='completed') OR EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.related_doc_type='inbound_plan' AND j.related_doc_id=? AND j.job_type IN ('inbound_direct','inbound_bulk') AND j.status='completed' AND j.inbound_external_no IN (SELECT value FROM json_each(?))) THEN 'partially_completed'
      WHEN EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=? AND j.related_doc_type='inbound_plan' AND j.job_type LIKE 'inbound%' AND j.status IN ${active}) THEN 'putting_away' ELSE 'arrived_pending_putaway' END,updated_at=? WHERE id=? AND status!='cancelled' AND ${gate}`,pid,pid,pid,allJson,pid,single,pid,pid,pid,allJson,pid,t,pid,...g));
   }
  }
 }
 if(body.client_req_id)sql.push(q(env,`INSERT OR IGNORE INTO v2_idempotency_keys(idem_key,action,response_json,created_at) SELECT ?,?,?,? WHERE ${gate}`,body.client_req_id,body.action|| (inbound?'v2_inbound_job_finish':'v2_ops_job_finish'),JSON.stringify(result),t,...g));
 try{await env.DB.batch(sql);}catch(error){if((await q(env,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first())?.status==='completed')return {ok:true,already_completed:true,result_id:resultId};throw error;}
 const saved=inbound?await q(env,'SELECT result_json FROM v2_ops_job_results WHERE id=?',resultId).first():await q(env,'SELECT response_json AS result_json FROM v2_idempotency_keys WHERE idem_key=?',claimKey).first();
 if(!saved)throw Error('任务状态已变化，请刷新后核对');
 return JSON.parse(saved.result_json)._completion_request===token?result:{ok:true,already_completed:true,result_id:resultId};
}
