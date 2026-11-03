import {nativeCompletionGuard,completionConflict,completionChanged} from './native-completion-guard.js';
import {workerBadgeMatch,canonicalWorkerSQL} from './daily-badge-identity.js';
import {intentSignature,canonicalIntent,boundReceiptJSON,readBoundReceipt} from './transaction-receipt.js';
import {ensureSchema} from './schema-ready.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const active="('pending','working','awaiting_close')";

// A single claim owns the entire completion, including the first clock close.
// Reads before this transaction never authorize any part of its business writes.
export async function finishUnplannedUnload(env,body,job,{legacy=false,plan=null}={}){
 if(!job)throw Error('job not found');
 const id=job.id,by=String(body.worker_id||''),t=new Date().toISOString(),leave=body.leave_only===true;
 const finish=!leave&&(!legacy||body.complete_job===true),key=String(body.client_req_id||'').trim(),action=body.action;
 const lines=Array.isArray(body.result_lines)?body.result_lines:[],note=String(body.diff_note||'').trim(),remark=String(body.remark||'').trim();
 const modernFeedback=!legacy&&job.related_doc_type==='field_feedback'?job.related_doc_id:'';
 const badge=(a,b)=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_ATTENDANCE_ENABLED==='true'?workerBadgeMatch(a,b):`${a}=${b}`;
 const column=env.SOP_ENVIRONMENT==='staging'&&env.SOP_ATTENDANCE_ENABLED==='true'?canonicalWorkerSQL('worker_id'):'worker_id';
 const ownExit=leave||!finish||!env.SOP_GROUP_FINISH;
 // Capture before receipt/network awaits. A later request can exit and rejoin
 // this badge with a different segment while this request is still in flight.
 // This schema has no row revision; the ID plus original clock fields provide
 // the compare-and-swap evidence without rewriting historical rows.
 const exitSQL=`SELECT json_group_array(json_object('id',id,'worker_id',worker_id,'joined_at',joined_at,'minutes_worked',minutes_worked,'leave_reason',leave_reason)) AS evidence
  FROM (SELECT id,worker_id,joined_at,minutes_worked,leave_reason FROM v2_ops_job_workers WHERE job_id=? AND ${badge('worker_id','?')} AND left_at='' ORDER BY id)`;
 const exitState=ownExit?(await q(env,exitSQL,id,by).first()).evidence:'[]';
 const exitIds=JSON.stringify(JSON.parse(exitState).map(row=>row.id));
 const signature=await intentSignature({action,job_id:id,worker_id:by,leave,finish,lines,note,remark,box_count:Number(body.box_count||0),pallet_count:Number(body.pallet_count||0),actor:env.SOP_REQUEST_USER?.id||''});
 const receipt=async()=>{
  if(!key)return null;
  const rs=await env.DB.batch([...await nativeCompletionGuard(env,body,{replay:true}),q(env,'SELECT action,response_json FROM v2_idempotency_keys WHERE idem_key=?',key)]);
  const row=rs.at(-1)?.results?.[0]||null;
  // Legacy individual finish releases this worker while reporting that others
  // continue. That mutating response also needs a bound, durable receipt.
  if(row?.action===action){
   let saved;try{saved=JSON.parse(row.response_json);}catch{}
   if(saved?.ok===false&&saved.error==='others_still_working'&&saved._request_signature===signature){const {_request_signature,...reply}=saved;return reply;}
  }
  return readBoundReceipt(env,{key,action,signature,load:async()=>row,legacyMatch:async saved=>{
  if(!saved.result_id||!finish)return false;
  const result=await q(env,'SELECT * FROM v2_ops_job_results WHERE id=? AND job_id=?',saved.result_id,id).first();
  if(!result||result.created_by!==by||result.remark!==remark||result.diff_note!==note)return false;
  try{return canonicalIntent(JSON.parse(result.result_lines_json))===canonicalIntent(lines)&&(!legacy||Number(result.box_count)===Number(body.box_count||0)&&Number(result.pallet_count)===Number(body.pallet_count||0));}catch{return false;}
 }});};
 try{
  const saved=await receipt();if(saved)return saved;
  if(job.status==='completed'){
   // Preserve the old completed-job repair, but apply it under the same grant.
   const sql=[...await nativeCompletionGuard(env,body),q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='already_completed_cleanup' WHERE job_id=? AND left_at='' AND EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=? AND status='completed')",t,t,id,id),q(env,"UPDATE v2_ops_jobs SET active_worker_count=0 WHERE id=? AND status='completed'",id)];
   if(modernFeedback)sql.push(q(env,"UPDATE v2_field_feedbacks SET status='unloaded_pending_info',updated_at=? WHERE id=? AND status='field_working' AND EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=? AND status='completed')",t,modernFeedback,id));
   const rs=await env.DB.batch(sql);
   return {ok:true,already_completed:true,error:'already_completed',cleaned_open_segments:rs[sql.length-(modernFeedback?3:2)]?.meta?.changes||0,message:'任务已完成'};
  }
  if(!['pending','working','awaiting_close'].includes(job.status))throw Error('任务状态已变化，请刷新后核对');
  if(finish&&(!Array.isArray(body.result_lines)||!lines.some(l=>Number(l.actual_qty)>0)))throw Error('至少填写一项实际数量');
  if(finish&&lines.some(l=>!l||!Number.isFinite(Number(l.actual_qty||0))||Number(l.actual_qty||0)<0||Number(l.actual_qty||0)>1e8))throw Error('实际数量无效');
  if(plan&&finish&&!['unloading','unloading_putting_away'].includes(plan.status))throw Error('当前卸货计划状态已变化，不能继续完成');
  const resultId='RES-FINAL-'+id,token=crypto.randomUUID();
  const gate="EXISTS(SELECT 1 FROM v2_ops_job_results WHERE id=? AND json_extract(result_json,'$._completion_request')=?)",g=[resultId,token];
  const data={...(legacy?{box_count:Number(body.box_count||0),pallet_count:Number(body.pallet_count||0)}:{}),result_lines:lines,diff_note:note,remark};
  const sql=[];let hadPlanLines=false;
  let result=finish?{ok:true,result_id:resultId,...(!legacy?{feedback_id:modernFeedback}:plan?{dynamic_plan:true,plan_id:plan.id}:{no_doc:true})}:leave?{ok:true,left:true}:{ok:true};
  const link=modernFeedback?" AND related_doc_type='field_feedback' AND related_doc_id=? AND EXISTS(SELECT 1 FROM v2_field_feedbacks f WHERE f.id=? AND f.status='field_working')":plan?" AND related_doc_type='inbound_plan' AND related_doc_id=? AND EXISTS(SELECT 1 FROM v2_inbound_plans p WHERE p.id=? AND p.source_type='field_dynamic' AND p.status=? AND p.updated_at=?)":'';
  const linkArgs=modernFeedback?[modernFeedback,modernFeedback]:plan?[plan.id,plan.id,plan.status,plan.updated_at]:[];
  if(finish&&legacy&&!plan){
   const old=await q(env,"SELECT id FROM v2_field_feedbacks WHERE related_doc_type='ops_job' AND related_doc_id=? AND feedback_type='unload_no_doc' ORDER BY created_at,id LIMIT 1",id).first();
   result.feedback_id=old?.id||'FB-UNLOAD-'+id;if(old)result.fb_reused=true;
  }
  if(finish){
   if(plan){
    const planned=(await q(env,'SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY line_no',plan.id).all()).results;
    data.has_diff=planned.some(p=>Number(lines.find(l=>l.unit_type===p.unit_type)?.actual_qty||0)!==Number(p.planned_qty||0))||lines.some(l=>Number(l.actual_qty||0)!==Number(planned.find(p=>p.unit_type===l.unit_type)?.planned_qty||0));
    hadPlanLines=planned.length>0;
   }
   const canFinish=env.SOP_GROUP_FINISH?'1':`NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.job_id=v2_ops_jobs.id AND w.left_at='' AND NOT ${badge('w.worker_id','?')})`;
   sql.push(q(env,`INSERT OR IGNORE INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,result_lines_json,diff_note,created_by,created_at)
    SELECT ?,?,?,?,?,?,?,?,?,? FROM v2_ops_jobs WHERE id=? AND status IN ${active} AND ${canFinish}${link}`,resultId,id,Number(body.box_count||0),Number(body.pallet_count||0),remark,JSON.stringify({...data,_completion_request:token}),JSON.stringify(lines),note,by,t,id,...(env.SOP_GROUP_FINISH?[]:[by]),...linkArgs));
  }
  let exitCheck=null;
  if(ownExit){
   await ensureSchema(env.DB,'unload-exit-checks-v1',()=>q(env,`CREATE TABLE IF NOT EXISTS ck_unload_exit_checks(id TEXT PRIMARY KEY,allowed INTEGER NOT NULL CONSTRAINT ck_unload_exit_checks CHECK(allowed=1))`).run());
   exitCheck=crypto.randomUUID();
  }
  const checks=exitCheck?[q(env,`INSERT INTO ck_unload_exit_checks(id,allowed) SELECT ?,CASE WHEN (${exitSQL})=? AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND ${badge('worker_id','?')} AND left_at='' AND (julianday(joined_at) IS NULL OR julianday(joined_at)>julianday(?))) THEN 1 ELSE 0 END`,exitCheck,id,by,exitState,id,by,t)]:[];
  sql.push(q(env,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason=CASE WHEN ${badge('worker_id','?')} THEN ? ELSE 'job_completed' END
   WHERE job_id=? AND left_at='' AND (${finish?gate:'0'}${ownExit?` OR (id IN (SELECT value FROM json_each(?)) AND ${badge('worker_id','?')} AND EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=? AND status IN ${active}${link}))`:''})`,t,t,by,leave?'leave':'finished',id,...(finish?g:[]),...(ownExit?[exitIds,by,id,...linkArgs]:[])));
  sql.push(q(env,`UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(DISTINCT ${column}) FROM v2_ops_job_workers WHERE job_id=? AND left_at='') WHERE id=? AND status IN ${active}`,id,id));
  if(finish){
   sql.push(q(env,`UPDATE v2_ops_jobs SET status='completed',shared_result_json=?,active_worker_count=0,updated_at=? WHERE id=? AND ${gate}`,JSON.stringify(data),t,id,...g));
   if(modernFeedback)sql.push(q(env,`UPDATE v2_field_feedbacks SET status='unloaded_pending_info',result_lines_json=?,diff_note=?,remark=?,completed_at=?,completed_by=?,title=?,updated_at=? WHERE id=? AND ${gate}`,JSON.stringify(lines),note,remark,t,by,'计划外卸货完成: '+lines.map(l=>(l.unit_type||'')+' '+(l.actual_qty||0)).join(' / '),t,modernFeedback,...g));
   if(legacy&&plan){
    for(const [i,l] of lines.entries()){
     if(!hadPlanLines)sql.push(q(env,`INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty,actual_qty) SELECT ?,?,?,?,?,? WHERE ${gate}`,crypto.randomUUID(),plan.id,i+1,String(l.unit_type||''),Number(l.actual_qty||0),Number(l.actual_qty||0),...g));
     else sql.push(q(env,`UPDATE v2_inbound_plan_lines SET actual_qty=? WHERE plan_id=? AND unit_type=? AND ${gate}`,Number(l.actual_qty||0),plan.id,String(l.unit_type||''),...g));
    }
    sql.push(q(env,`UPDATE v2_inbound_plans SET status='unloaded_pending_info',cargo_summary=?,unload_completed_at=COALESCE(NULLIF(unload_completed_at,''),?),unload_completed_by=COALESCE(NULLIF(unload_completed_by,''),COALESCE(NULLIF((SELECT group_concat(worker_name,', ') FROM (SELECT DISTINCT worker_name FROM v2_ops_job_workers WHERE job_id=? AND worker_name!='')),''),?)),updated_at=? WHERE id=? AND ${gate}`,lines.map(l=>(l.unit_type||'')+' '+(l.actual_qty||0)).join(' / ')||'现场无单卸货',t,id,by,t,plan.id,...g));
   }else if(legacy)sql.push(q(env,`INSERT INTO v2_field_feedbacks(id,feedback_type,related_doc_type,related_doc_id,title,content,submitted_by,status,created_at,updated_at)
    SELECT ?,'unload_no_doc','ops_job',?,'无单卸货结果待转正',?,?,'open',?,? WHERE ${gate} AND NOT EXISTS(SELECT 1 FROM v2_field_feedbacks WHERE id=?)`,result.feedback_id,id,'卸货数量: '+JSON.stringify(lines)+(note?' | 备注: '+note:''),by,t,t,...g,result.feedback_id));
   if(key)sql.push(q(env,`INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) SELECT ?,?,?,? WHERE ${gate}`,key,action,boundReceiptJSON(result,signature),t,...g));
   if(key&&!env.SOP_GROUP_FINISH)sql.push(q(env,`INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at)
    SELECT ?,?,json_object('ok',json('false'),'error','others_still_working','message','您已退出此任务，还有 '||active_worker_count||' 人继续作业','active_worker_count',active_worker_count,'_request_signature',?),?
    FROM v2_ops_jobs WHERE id=? AND status IN ${active} AND active_worker_count>0 AND NOT ${gate}${link}`,key,action,signature,t,id,...g,...linkArgs));
  }else if(key){
   sql.push(q(env,`INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) SELECT ?,?,?,? FROM v2_ops_jobs WHERE id=? AND status IN ${active}${link}`,key,action,boundReceiptJSON(result,signature),t,id,...linkArgs));
  }
  const rs=await env.DB.batch([...await nativeCompletionGuard(env,body),...checks,...sql,...(exitCheck?[q(env,'DELETE FROM ck_unload_exit_checks WHERE id=?',exitCheck)]:[]),q(env,'SELECT status,active_worker_count FROM v2_ops_jobs WHERE id=?',id),q(env,'SELECT result_json FROM v2_ops_job_results WHERE id=?',resultId)]);
  const current=rs.at(-2)?.results?.[0],savedResult=rs.at(-1)?.results?.[0];
  if(!finish){if(current&&['pending','working','awaiting_close'].includes(current.status))return result;throw Error('任务状态已变化，请刷新后核对');}
  if(savedResult&&JSON.parse(savedResult.result_json)._completion_request===token)return result;
  if(current?.status==='completed'){const replay=await receipt();if(replay)return replay;return {ok:true,already_completed:true,result_id:resultId};}
  if(!env.SOP_GROUP_FINISH&&current?.active_worker_count>0)return {ok:false,error:'others_still_working',message:'您已退出此任务，还有 '+current.active_worker_count+' 人继续作业',active_worker_count:current.active_worker_count};
  throw Error('任务或卸货资料已变化，请刷新后核对');
 }catch(error){
  if(completionConflict.test(error.message||''))throw Error(completionChanged);
  // A competing identical request may have committed the receipt while this
  // transaction waited. Verify its original grant before returning that result.
  const saved=await receipt();if(saved)return saved;
  if(/ck_unload_exit_checks/.test(error.message||''))throw Error('作业计时段已变化，请刷新后重新确认退出 / 작업 시간이 변경되었습니다');
  throw error;
 }
}
