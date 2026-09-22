// Staging dispatch ownership and validation shared by the original operation routes.
import {nativeOwner} from './sop-dispatch.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const fail=m=>{throw Error(m);};
const types={v2_unload_job_finish:['unload'],v2_unplanned_unload_finish:['unload'],v2_inbound_job_finish:['inbound_direct','inbound_bulk','inbound_return','inbound_change_order'],v2_import_delivery_job_finish:['pickup_delivery_import'],v2_outbound_load_finish:['load_outbound'],v2_outbound_stock_op_finish:['outbound_stock_op'],v2_bulk_op_job_finish:['bulk_op'],v2_pick_job_finish:['pick_direct'],v2_pick_job_finalize:['pick_direct'],v2_verify_job_finish:['verify_scan']};
const quantityKeys=['box_count','pallet_count','packed_sku_count','packed_box_count','used_carton_large_count','used_carton_small_count','repaired_box_count','reboxed_count','label_count','total_operated_box_count','forklift_location_count'];
export async function validateNativeMutation(body,env){
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true')return;
 if(!body.job_id||!(/_(finish|finalize|leave|resume)$/.test(body.action)||body.action==='v2_ops_job_result_update'))return;
 const record=await q(env,"SELECT state FROM sop_records WHERE id=? AND kind='dispatch'",body.job_id).first();if(!record)return;
 if(!await nativeOwner(body,env))fail('请由本任务派审员操作 / 이 작업의 배정 담당자가 처리하세요');
 const job=await q(env,'SELECT * FROM v2_ops_jobs WHERE id=?',body.job_id).first();if(!job||job.status==='cancelled')fail('任务已取消或不存在 / 작업이 없거나 취소되었습니다');
 if(types[body.action]&&!types[body.action].includes(job.job_type))fail('作业类型与操作入口不符，请重新打开原任务 / 작업 종류가 일치하지 않습니다');
 if(body.action==='v2_ops_job_finish'&&Object.values(types).flat().includes(job.job_type))fail('请从原任务填写产出并结束 / 원래 작업에서 산출을 입력하고 종료하세요');
 if(body.action==='v2_unplanned_unload_finish'&&job.related_doc_type!=='field_feedback')fail('请从原卸货任务完成 / 원래 하차 작업에서 완료하세요');
 if(body.action==='v2_unload_job_finish'&&job.related_doc_type==='field_feedback')fail('临时卸货请从原任务完成 / 임시 하차 작업에서 완료하세요');
 if(body.leave_only||body.action.endsWith('_leave')||body.action.endsWith('_resume')||job.status==='completed')return;
 for(const key of quantityKeys){const n=Number(body[key]??0);if(!Number.isFinite(n)||n<0||n>1e8)fail('产出数量不能为负数或无效数字 / 올바른 산출 수량을 입력하세요');}
 for(const key of ['sort_qty','label_qty','repair_box_qty']){const n=Number(body.extra_ops?.[key]??0);if(!Number.isFinite(n)||n<0||n>1e8)fail('附加操作数量无效 / 추가 작업 수량 오류');}
 for(const line of body.result_lines||[]){for(const key of ['actual_qty','putaway_qty'])if(line[key]!=null&&(!Number.isFinite(Number(line[key]))||Number(line[key])<0||Number(line[key])>1e8))fail('实际数量无效 / 실제 수량 오류');}
 if(['v2_unplanned_unload_finish','v2_unload_job_finish'].includes(body.action)&&body.complete_job!==false&&!body.plan_results&&!(body.result_lines||[]).some(x=>Number(x.actual_qty)>0))fail('至少填写一项实收数量 / 실제 수량을 입력하세요');
 if(body.action==='v2_bulk_op_job_finish'){
  if(!quantityKeys.filter(k=>k!=='box_count').some(k=>Number(body[k])>0)&&!body.used_forklift)fail('请先记录操作产出后再完成 / 산출을 기록한 후 완료하세요');
  if(!job.linked_outbound_order_id&&!String(body.customer||job.customer||'').trim())fail('请填写客户名称 / 고객명을 입력하세요');
 }
}
export function pickTeamStatements(env,jobId,t){
 return [q(env,`INSERT OR IGNORE INTO v2_pick_worker_docs(id,job_id,segment_id,worker_id,worker_name,pick_doc_no,started_at,status,created_at)
  SELECT 'PWD-'||?||'-'||w.id||'-'||d.id,w.job_id,w.id,w.worker_id,w.worker_name,d.pick_doc_no,w.joined_at,'working',?
  FROM v2_ops_job_workers w JOIN v2_ops_job_pick_docs d ON d.job_id=w.job_id
  WHERE w.job_id=? AND w.left_at='' AND d.pick_status!='completed'`,crypto.randomUUID(),t,jobId),
 q(env,"UPDATE v2_ops_job_pick_docs SET pick_status='working',pick_started_at=COALESCE(NULLIF(pick_started_at,''),?) WHERE job_id=? AND pick_status='pending'",t,jobId)];
}
export async function nativePeople(body,env){
 if(env.SOP_ENVIRONMENT!=='staging'||!await nativeOwner({job_id:body.job_id},env))fail('请由本任务派审员调整人员 / 담당자가 인원을 변경하세요');
 const request=String(body.client_req_id||'');if(!request||request.length>100)fail('缺少请求编号');
 const old=await q(env,'SELECT action,actor_id,result_json FROM sop_events WHERE request_id=?',request).first();
 if(old){if(old.action!=='sop_native_people'||old.actor_id!==env.SOP_REQUEST_USER.id)fail('请求已被使用');return JSON.parse(old.result_json);}
 const row=await q(env,"SELECT * FROM sop_records WHERE id=? AND kind='dispatch'",body.job_id).first();
 const job=await q(env,'SELECT * FROM v2_ops_jobs WHERE id=?',body.job_id).first();
 if(!row||!job||!['pending','working','awaiting_close'].includes(job.status))fail('任务已结束，请刷新 / 작업이 종료되었습니다');
 if(Number(body.revision)!==row.revision)fail('人员已被其他操作更新，请重新打开 / 인원 정보가 변경되었습니다');
 const workers=body.workers;
 if(!Array.isArray(workers)||workers.length>50||workers.some(w=>!w.id||!w.name||String(w.id).length>100||String(w.name).length>100)||new Set(workers.map(w=>w.id)).size!==workers.length)fail('工牌无效或重复 / 명찰을 확인하세요');
 if(workers.length&&!workers.some(w=>w.id===body.lead_id))fail('请选择本次主操作员 / 주 작업자를 선택하세요');
 const t=new Date().toISOString(),prior=JSON.parse(row.state),next={...prior,workers,lead_id:workers.length?body.lead_id:prior.lead_id,last_lead:workers.find(w=>w.id===body.lead_id)||prior.workers.find(w=>w.id===prior.lead_id)||prior.last_lead};
 const active=await rows(env,"SELECT * FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",job.id);
 const result={ok:true,job_id:job.id,revision:row.revision+1,lead:next.last_lead};
 const sql=[q(env,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request,job.id,row.revision,'sop_native_people',env.SOP_REQUEST_USER.id,env.SOP_REQUEST_USER.name,row.state,JSON.stringify(next),JSON.stringify(result),t),q(env,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(next),t,job.id)];
 for(const s of active.filter(s=>!workers.some(w=>w.id===s.worker_id))){
  const minutes=Math.max(0,Math.round((Date.parse(t)-Date.parse(s.joined_at))/6000)/10);
  sql.push(q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=?,leave_reason='dispatcher_change' WHERE id=? AND left_at=''",t,minutes,s.id));
  if(job.job_type==='pick_direct')sql.push(q(env,"UPDATE v2_pick_worker_docs SET status='completed',finished_at=?,minutes_worked=? WHERE segment_id=? AND status='working'",t,minutes,s.id));
 }
 for(const w of workers.filter(w=>!active.some(s=>s.worker_id===w.id))){sql.push(q(env,"INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)",'WS-'+crypto.randomUUID(),job.id,w.id,w.name,t));}
 if(job.job_type==='pick_direct')sql.push(...pickTeamStatements(env,job.id,t));
 sql.push(q(env,"UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(DISTINCT worker_id) FROM v2_ops_job_workers WHERE job_id=? AND left_at=''),status=?,updated_at=? WHERE id=?",job.id,workers.length?'working':'awaiting_close',t,job.id));
 // Install after the SOP schema exists (legacy migrations run before SOP tables).
 await env.DB.prepare("CREATE TRIGGER IF NOT EXISTS ck_native_people_active BEFORE INSERT ON sop_events WHEN NEW.action='sop_native_people' AND NOT EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.record_id AND status IN ('pending','working','awaiting_close')) BEGIN SELECT RAISE(ABORT,'job_already_finished'); END").run();
 await env.DB.batch(sql);return result;
}

// One transaction closes the complete crew and writes exactly one trip result.
export async function finishNativePick(body,env){
 const id=body.job_id,t=new Date().toISOString(),resultId='RES-FINAL-'+id;
 const completed=await q(env,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first();
 if(completed?.status==='completed')return {ok:true,already_completed:true};
 const minutes="MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0)";
 const sql=[q(env,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=${minutes},leave_reason='dispatcher_finish' WHERE job_id=? AND left_at=''`,t,t,id),
 q(env,`UPDATE v2_pick_worker_docs SET status='completed',finished_at=COALESCE(NULLIF(finished_at,''),?),minutes_worked=COALESCE((SELECT minutes_worked FROM v2_ops_job_workers WHERE id=segment_id),minutes_worked) WHERE job_id=? AND status='working'`,t,id),
 q(env,"UPDATE v2_ops_job_pick_docs SET pick_status='completed',pick_finished_at=COALESCE(NULLIF(pick_finished_at,''),?) WHERE job_id=?",t,id),
 q(env,`INSERT INTO v2_ops_job_results(id,job_id,remark,result_json,result_lines_json,created_by,created_at)
 SELECT ?,?,?,json_object('kind','trip_finalize','pick_doc_nos',json((SELECT json_group_array(pick_doc_no) FROM v2_ops_job_pick_docs WHERE job_id=?)),
 'worker_count',COUNT(DISTINCT worker_id),'total_pwd',(SELECT COUNT(*) FROM v2_pick_worker_docs WHERE job_id=?),'total_minutes',ROUND(COALESCE(SUM(minutes_worked),0),1),'result_note',?,'finalized_by',?), '[]',?,? FROM v2_ops_job_workers WHERE job_id=?`,resultId,id,String(body.remark||''),id,id,String(body.result_note||''),env.SOP_REQUEST_USER.id,env.SOP_REQUEST_USER.id,t,id),
 q(env,"UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,updated_at=? WHERE id=?",t,id)];
 try{await env.DB.batch(sql);}catch(e){const j=await q(env,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first();if(j?.status==='completed')return {ok:true,already_completed:true};throw e;}
 const r=JSON.parse((await q(env,'SELECT result_json FROM v2_ops_job_results WHERE id=?',resultId).first()).result_json);
 return {ok:true,job_id:id,finalized_at:t,pick_doc_count:r.pick_doc_nos.length,worker_count:r.worker_count,total_minutes:r.total_minutes};
}

export async function finishNativeOutbound(body,env){
 const id=body.job_id,t=new Date().toISOString(),job=await q(env,'SELECT * FROM v2_ops_jobs WHERE id=?',id).first();
 if(job.status==='completed')return {ok:true,already_completed:true};
 const stock=job.job_type==='outbound_stock_op',box=Number(body.box_count||0),pallet=Number(body.pallet_count||0),remark=String(body.remark||''),by=env.SOP_REQUEST_USER.id;
 const resultId='RES-FINAL-'+id,resultJson=stock&&body.result_json?String(body.result_json):JSON.stringify({box_count:box,pallet_count:pallet,remark});
 const sql=[q(env,`INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)`,resultId,id,box,pallet,remark,resultJson,by,t),
 q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='dispatcher_finish' WHERE job_id=? AND left_at=''",t,t,id),
 q(env,"UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,shared_result_json=?,updated_at=? WHERE id=?",resultJson,t,id)];
 if(job.related_doc_id){
  if(stock){
   const prior=await rows(env,'SELECT box_count,pallet_count,remark,created_by,created_at FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at',id);
   const all=[...prior,{box_count:box,pallet_count:pallet,remark,created_by:by,created_at:t}];
   const summary={total_box_count:all.reduce((n,r)=>n+Number(r.box_count||0),0),total_pallet_count:all.reduce((n,r)=>n+Number(r.pallet_count||0),0),last_box_count:box,last_pallet_count:pallet,last_remark:remark,results:all};
   sql.push(q(env,"UPDATE v2_outbound_orders SET status='pending_outbound_update',stock_operation_status='completed',stock_operation_completed_at=?,stock_operation_completed_by=?,stock_operation_result_json=?,updated_at=? WHERE id=?",t,env.SOP_REQUEST_USER.name,JSON.stringify(summary),t,job.related_doc_id));
  }else sql.push(q(env,"UPDATE v2_outbound_orders SET status='shipped',actual_box_count=?,actual_pallet_count=?,updated_at=? WHERE id=?",box,pallet,t,job.related_doc_id));
 }
 try{await env.DB.batch(sql);}catch(e){if((await q(env,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first())?.status==='completed')return {ok:true,already_completed:true};throw e;}
 return {ok:true,result_id:resultId,status:'completed'};
}
