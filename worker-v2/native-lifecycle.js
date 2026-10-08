import {ensureSchema} from './schema-ready.js';
// Staging dispatch ownership and validation shared by the original operation routes.
import {nativeOwner} from './sop-dispatch.js';
import {borrowedOut,crewBorrowStatements} from './crew-borrow.js';
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
  if(job.status==='paused')fail('任务已暂停，请先核对人员并恢复 / 작업을 먼저 재개하세요');
  if(body.leave_only||body.action.endsWith('_leave')||body.action.endsWith('_resume')||job.status==='completed')return;
  if(body.action==='v2_import_delivery_job_finish'){
   if(!String(body.destination_note||'').trim())fail('请填写去向 / 목적지를 입력하세요');
   const count=Number(body.estimated_piece_count??0);if(!Number.isFinite(count)||count<0||count>1e8)fail('大概件数不能为负数或无效数字 / 올바른 수량을 입력하세요');
  }
 for(const key of quantityKeys){const n=Number(body[key]??0);if(!Number.isFinite(n)||n<0||n>1e8)fail('产出数量不能为负数或无效数字 / 올바른 산출 수량을 입력하세요');}
 for(const key of ['sort_qty','label_qty','repair_box_qty']){const n=Number(body.extra_ops?.[key]??0);if(!Number.isFinite(n)||n<0||n>1e8)fail('附加操作数量无效 / 추가 작업 수량 오류');}
 for(const line of body.result_lines||[]){for(const key of ['actual_qty','putaway_qty'])if(line[key]!=null&&(!Number.isFinite(Number(line[key]))||Number(line[key])<0||Number(line[key])>1e8))fail('实际数量无效 / 실제 수량 오류');}
 if(['v2_unplanned_unload_finish','v2_unload_job_finish'].includes(body.action)&&body.complete_job!==false&&!body.plan_results&&!(body.result_lines||[]).some(x=>Number(x.actual_qty)>0))fail('至少填写一项实收数量 / 실제 수량을 입력하세요');
 if(body.action==='v2_bulk_op_job_finish'){
  if(!quantityKeys.filter(k=>k!=='box_count').some(k=>Number(body[k])>0)&&!body.used_forklift)fail('请先记录操作产出后再完成 / 산출을 기록한 후 완료하세요');
  if(!job.linked_outbound_order_id&&!String(body.customer||job.customer||'').trim())fail('请填写客户名称 / 고객명을 입력하세요');
  const ids=body.location_photos||[];
  if(!Array.isArray(ids)||ids.length>8||new Set(ids).size!==ids.length||ids.some(id=>typeof id!=='string'))fail('货位照片无效');
  const photos=ids.length?await rows(env,"SELECT id,file_key,file_name,content_type FROM v2_attachments WHERE related_doc_type='ops_job' AND related_doc_id=? AND attachment_category='location_photo' AND id IN (SELECT value FROM json_each(?))",job.id,JSON.stringify(ids)):[];
  if(photos.length!==ids.length||photos.some(p=>!['image/jpeg','image/png','image/webp'].includes(p.content_type)))fail('货位照片不存在或不属于本任务');
  body.location_photos=photos;
 }
}
export function pickTeamStatements(env,jobId,t){
 return [q(env,`INSERT OR IGNORE INTO v2_pick_worker_docs(id,job_id,segment_id,worker_id,worker_name,pick_doc_no,started_at,status,created_at)
  SELECT 'PWD-'||?||'-'||w.id||'-'||d.id,w.job_id,w.id,w.worker_id,w.worker_name,d.pick_doc_no,w.joined_at,'working',?
  FROM v2_ops_job_workers w JOIN v2_ops_job_pick_docs d ON d.job_id=w.job_id
  WHERE w.job_id=? AND w.left_at='' AND d.pick_status!='completed'`,crypto.randomUUID(),t,jobId),
 q(env,"UPDATE v2_ops_job_pick_docs SET pick_status='working',pick_started_at=COALESCE(NULLIF(pick_started_at,''),?) WHERE job_id=? AND pick_status='pending'",t,jobId)];
}
export async function nativePeople(body,env,{resume=false}={}){
 const action=resume?'sop_native_resume':'sop_native_people',resumeSignature=JSON.stringify({job_id:body.job_id,revision:body.revision,workers:body.workers,lead_id:body.lead_id});
 if(env.SOP_ENVIRONMENT!=='staging'||!await nativeOwner({job_id:body.job_id},env))fail('请由本任务派审员调整人员 / 담당자가 인원을 변경하세요');
 const request=String(body.client_req_id||'');if(!request||request.length>100)fail('缺少请求编号');
 const old=await q(env,'SELECT action,actor_id,record_id,after_json,result_json FROM sop_events WHERE request_id=?',request).first();
 if(old){if(old.action!==action||old.actor_id!==env.SOP_REQUEST_USER.id||old.record_id!==body.job_id||resume&&JSON.parse(old.after_json)._resume_signature!==resumeSignature)fail('请求已被使用');return JSON.parse(old.result_json);}
 const row=await q(env,"SELECT * FROM sop_records WHERE id=? AND kind='dispatch'",body.job_id).first();
 const job=await q(env,'SELECT * FROM v2_ops_jobs WHERE id=?',body.job_id).first();
 if(!row||!job||!(resume?['paused']:['pending','working','awaiting_close']).includes(job.status))fail('任务已结束，请刷新 / 작업이 종료되었습니다');
 if(Number(body.revision)!==row.revision)fail('人员已被其他操作更新，请重新打开 / 인원 정보가 변경되었습니다');
 const workers=body.workers;
 if(resume&&(!Array.isArray(workers)||!workers.length))fail('请确认实际恢复作业的人员 / 재개할 인원을 확인하세요');
 if(!Array.isArray(workers)||workers.length>50||workers.some(w=>!w.id||!w.name||String(w.id).length>100||String(w.name).length>100)||new Set(workers.map(w=>w.id)).size!==workers.length)fail('工牌无效或重复 / 명찰을 확인하세요');
 if(workers.length&&!workers.some(w=>w.id===body.lead_id))fail('请选择本次主操作员 / 주 작업자를 선택하세요');
 const t=new Date().toISOString(),prior=JSON.parse(row.state),next={...prior,workers,lead_id:workers.length?body.lead_id:prior.lead_id,last_lead:workers.find(w=>w.id===body.lead_id)||prior.workers.find(w=>w.id===prior.lead_id)||prior.last_lead,last_adjustment:String(body.reason||''),...(resume?{status:'working',pause_reason:'',paused_workers:[],_resume_signature:resumeSignature}:{} )};
 if(resume)next.workers=[...(prior.workers||[]).filter(w=>!workers.some(n=>n.id===w.id)),...workers];
 const active=await rows(env,"SELECT * FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",job.id);
 const out=await borrowedOut(env,job.id),away=new Set(out.map(b=>b.worker_id));
 const result={ok:true,job_id:job.id,revision:row.revision+1,lead:next.last_lead,...(env.SOP_CREW_BORROW?{has_crew_borrows:true}:{})};
 const sql=[q(env,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request,job.id,row.revision,action,env.SOP_REQUEST_USER.id,env.SOP_REQUEST_USER.name,row.state,JSON.stringify(next),JSON.stringify(result),t),q(env,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(next),t,job.id)];
 if(resume)sql.push(q(env,"UPDATE v2_ops_jobs SET status='working',resumed_at=?,updated_at=? WHERE id=? AND status='paused'",t,t,job.id));
 for(const s of active.filter(s=>!workers.some(w=>w.id===s.worker_id))){
  const minutes=Math.max(0,Math.round((Date.parse(t)-Date.parse(s.joined_at))/6000)/10);
  sql.push(q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=?,leave_reason='dispatcher_change' WHERE id=? AND left_at=''",t,minutes,s.id));
  if(job.job_type==='pick_direct')sql.push(q(env,"UPDATE v2_pick_worker_docs SET status='completed',finished_at=?,minutes_worked=? WHERE segment_id=? AND status='working'",t,minutes,s.id));
 }
 sql.push(...crewBorrowStatements(env,env.SOP_CREW_BORROW,job.id,t));
 for(const w of workers.filter(w=>!away.has(w.id)&&!active.some(s=>s.worker_id===w.id))){sql.push(q(env,"INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)",'WS-'+crypto.randomUUID(),job.id,w.id,w.name,t));}
 if(job.job_type==='pick_direct')sql.push(...pickTeamStatements(env,job.id,t));
 sql.push(q(env,"UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(DISTINCT worker_id) FROM v2_ops_job_workers WHERE job_id=? AND left_at=''),status=CASE WHEN EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND left_at='') THEN 'working' ELSE 'awaiting_close' END,updated_at=? WHERE id=?",job.id,job.id,t,job.id));
 // Install after the SOP schema exists (legacy migrations run before SOP tables).
 await env.DB.prepare("CREATE TRIGGER IF NOT EXISTS ck_native_people_active BEFORE INSERT ON sop_events WHEN NEW.action='sop_native_people' AND NOT EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.record_id AND status IN ('pending','working','awaiting_close')) BEGIN SELECT RAISE(ABORT,'job_already_finished'); END").run();
 await ensureNativePauseGuards(env);
 await env.DB.batch(sql);return result;
}

// One transaction closes the complete crew and writes exactly one trip result.
export async function finishNativePick(body,env,{legacy=false}={}){
  const id=body.job_id,t=new Date().toISOString(),resultId='RES-FINAL-'+id,token=crypto.randomUUID(),claim='PICK-FINAL:'+id,gate="EXISTS(SELECT 1 FROM v2_idempotency_keys WHERE idem_key=? AND json_extract(response_json,'$._completion_request')=?)",g=[claim,token];
 const completed=await q(env,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first();
 if(completed?.status==='completed')return {ok:true,already_completed:true};
  const minutes="MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0)",actor=legacy?String(body.worker_id||env.SOP_REQUEST_USER?.id||''):env.SOP_REQUEST_USER.id;
  const sql=[q(env,`INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) SELECT ?,'v2_pick_job_finalize',?,? FROM v2_ops_jobs WHERE id=? AND status IN ('pending','working','awaiting_close')${legacy?" AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND left_at='')":''}`,claim,JSON.stringify({_completion_request:token}),t,id,...(legacy?[id]:[])),
  q(env,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=${minutes},leave_reason='dispatcher_finish' WHERE job_id=? AND left_at='' AND ${gate}`,t,t,id,...g),
  q(env,`UPDATE v2_pick_worker_docs SET status='completed',finished_at=COALESCE(NULLIF(finished_at,''),?),minutes_worked=COALESCE((SELECT minutes_worked FROM v2_ops_job_workers WHERE id=segment_id),minutes_worked) WHERE job_id=? AND status='working' AND ${gate}`,t,id,...g),
  q(env,`UPDATE v2_ops_job_pick_docs SET pick_status='completed',pick_finished_at=COALESCE(NULLIF(pick_finished_at,''),?) WHERE job_id=? AND ${gate}`,t,id,...g),
 q(env,`INSERT INTO v2_ops_job_results(id,job_id,remark,result_json,result_lines_json,created_by,created_at)
 SELECT ?,?,?,json_object('kind','trip_finalize','pick_doc_nos',json((SELECT json_group_array(pick_doc_no) FROM v2_ops_job_pick_docs WHERE job_id=?)),
  'worker_count',COUNT(DISTINCT worker_id),'total_pwd',(SELECT COUNT(*) FROM v2_pick_worker_docs WHERE job_id=?),'total_minutes',ROUND(COALESCE(SUM(minutes_worked),0),1),'result_note',?,'finalized_by',?), '[]',?,? FROM v2_ops_job_workers WHERE job_id=? HAVING ${gate}`,resultId,id,String(body.remark||''),id,id,String(body.result_note||''),actor,actor,t,id,...g),
  q(env,`UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,finished_at=?,updated_at=? WHERE id=? AND ${gate}`,t,t,id,...g)];
 try{await env.DB.batch(sql);}catch(e){const j=await q(env,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first();if(j?.status==='completed')return {ok:true,already_completed:true};throw e;}
  const saved=await q(env,'SELECT result_json FROM v2_ops_job_results WHERE id=?',resultId).first();if(!saved)throw Error('人员或任务状态已变化，请先核实人员完成状态');const r=JSON.parse(saved.result_json);
 return {ok:true,job_id:id,finalized_at:t,pick_doc_count:r.pick_doc_nos.length,worker_count:r.worker_count,total_minutes:r.total_minutes};
}

export async function finishNativeOutbound(body,env,{legacy=false}={}){
 const id=body.job_id,t=new Date().toISOString(),job=await q(env,'SELECT * FROM v2_ops_jobs WHERE id=?',id).first();
 if(job.status==='completed')return {ok:true,already_completed:true};
  const stock=job.job_type==='outbound_stock_op',box=Number(body.box_count||0),pallet=Number(body.pallet_count||0),remark=String(body.remark||''),by=legacy?String(body.worker_id||env.SOP_REQUEST_USER?.id||''):env.SOP_REQUEST_USER.id;
  let lines;try{lines=body.result_lines_json?JSON.parse(body.result_lines_json):undefined;}catch{}
  const resultId='RES-FINAL-'+id,resultJson=stock&&body.result_json?String(body.result_json):JSON.stringify({box_count:box,pallet_count:pallet,remark,...(lines?{result_lines:lines}:{})}),claim='OUTBOUND-FINAL:'+id,token=crypto.randomUUID(),gate="EXISTS(SELECT 1 FROM v2_idempotency_keys WHERE idem_key=? AND json_extract(response_json,'$._completion_request')=?)",g=[claim,token];
  const sql=[q(env,`INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) SELECT ?,?,?,? FROM v2_ops_jobs WHERE id=? AND status IN ('pending','working','awaiting_close')${legacy?" AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND worker_id!=? AND left_at='')":''}`,claim,body.action||'v2_outbound_stock_op_finish',JSON.stringify({_completion_request:token}),t,id,...(legacy?[id,by]:[])),
  q(env,`INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,created_by,created_at) SELECT ?,?,?,?,?,?,?,? WHERE ${gate}`,resultId,id,box,pallet,remark,resultJson,by,t,...g),
  q(env,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='dispatcher_finish' WHERE job_id=? AND left_at='' AND ${gate}`,t,t,id,...g),
  q(env,`UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,shared_result_json=?,finished_at=?,updated_at=? WHERE id=? AND ${gate}`,resultJson,t,t,id,...g)];
 if(job.related_doc_id){
  if(stock){
    sql.push(q(env,`UPDATE v2_outbound_orders SET status='pending_outbound_update',stock_operation_status='completed',stock_operation_completed_at=?,stock_operation_completed_by=?,
     stock_operation_result_json=(SELECT json_object('total_box_count',COALESCE(SUM(box_count),0),'total_pallet_count',COALESCE(SUM(pallet_count),0),'last_box_count',?,'last_pallet_count',?,'last_remark',?,'results',json_group_array(json_object('box_count',box_count,'pallet_count',pallet_count,'remark',remark,'created_by',created_by,'created_at',created_at))) FROM v2_ops_job_results WHERE job_id=?),updated_at=? WHERE id=? AND ${gate}`,t,legacy?String(body.worker_name||by):env.SOP_REQUEST_USER.name,box,pallet,remark,id,t,job.related_doc_id,...g));
  }else{
    const ship=q(env,`UPDATE v2_outbound_orders SET status='shipped',actual_box_count=?,actual_pallet_count=?,updated_at=? WHERE id=? AND ${gate}`,box,pallet,t,job.related_doc_id,...g);
   // In staging, the order guard must still see the active loading job during shipment.
    if(env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true')sql.splice(3,0,ship);else sql.push(ship);
  }
 }
 try{await env.DB.batch(sql);}catch(e){if((await q(env,'SELECT status FROM v2_ops_jobs WHERE id=?',id).first())?.status==='completed')return {ok:true,already_completed:true};throw e;}
  if(!await q(env,'SELECT id FROM v2_ops_job_results WHERE id=?',resultId).first())throw Error('人员或任务状态已变化，请先核实人员完成状态');return {ok:true,result_id:resultId,status:'completed'};
}

// Native pause preserves assignment; an empty crew edit is not a task pause.
export async function ensureNativePauseGuards(env){
 await ensureSchema(env.DB,'native-pause-v1',()=>env.DB.batch([
 q(env,`CREATE TRIGGER IF NOT EXISTS ck_native_pause_event BEFORE INSERT ON sop_events WHEN NEW.action IN ('sop_native_pause','sop_native_resume') BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.record_id AND ((NEW.action='sop_native_pause' AND status IN ('working','awaiting_close')) OR (NEW.action='sop_native_resume' AND status='paused'))) THEN RAISE(ABORT,'pause_state_changed') END; END`),
 q(env,`CREATE TRIGGER IF NOT EXISTS ck_native_paused_clock BEFORE INSERT ON v2_ops_job_workers WHEN NEW.left_at='' AND EXISTS(SELECT 1 FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id WHERE j.id=NEW.job_id AND s.kind='dispatch' AND j.status IN ('paused','completed','cancelled')) BEGIN SELECT RAISE(ABORT,'task_not_running'); END`)
 ]));
}
export async function pauseNative(body,env){
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true'||!await nativeOwner(body,env))fail('请由本任务派审员暂停 / 담당자가 작업을 중지하세요');
 const request=String(body.client_req_id||''),reason=String(body.reason||'').trim();
 if(!request||request.length>100||!reason||reason.length>500)fail('请填写暂停原因 / 중지 사유를 입력하세요');
 const old=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();
 if(old){if(old.action!=='sop_native_pause'||old.record_id!==body.job_id||old.actor_id!==env.SOP_REQUEST_USER.id||JSON.parse(old.after_json).pause_reason!==reason)fail('请求已被使用');return JSON.parse(old.result_json);}
 const row=await q(env,"SELECT * FROM sop_records WHERE id=? AND kind='dispatch'",body.job_id).first(),job=await q(env,'SELECT * FROM v2_ops_jobs WHERE id=?',body.job_id).first();
 if(!row||!job||!['working','awaiting_close'].includes(job.status)||row.revision!==Number(body.revision))fail('任务或人员已变化，请刷新 / 작업 상태가 변경되었습니다');
 const t=new Date().toISOString(),before=JSON.parse(row.state),active=await rows(env,"SELECT worker_id AS id,worker_name AS name FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",job.id);
 const after={...before,status:'paused',pause_reason:reason,paused_at:t,pause_actor_id:env.SOP_REQUEST_USER.id,paused_workers:active,last_lead:before.last_lead||(before.workers||[]).find(w=>w.id===before.lead_id)};
 const result={ok:true,job_id:job.id,revision:row.revision+1,status:'paused'};
 await ensureNativePauseGuards(env);
 await env.DB.batch([
 q(env,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request,job.id,row.revision,'sop_native_pause',env.SOP_REQUEST_USER.id,env.SOP_REQUEST_USER.name,row.state,JSON.stringify(after),JSON.stringify(result),t),
 q(env,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(after),t,job.id),
 q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='task_pause' WHERE job_id=? AND left_at=''",t,t,job.id),
 q(env,"UPDATE v2_pick_worker_docs SET status='completed',finished_at=?,minutes_worked=COALESCE((SELECT minutes_worked FROM v2_ops_job_workers WHERE id=segment_id),minutes_worked) WHERE job_id=? AND status='working'",t,job.id),
 q(env,"UPDATE v2_ops_jobs SET status='paused',paused_at=?,active_worker_count=0,updated_at=? WHERE id=?",t,t,job.id)
 ]);return result;
}
