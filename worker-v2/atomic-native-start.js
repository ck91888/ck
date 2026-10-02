import {staffedDispatchStatements} from './crew-borrow.js';
import {ensureSchema} from './schema-ready.js';
import {resolveInboundPlan,putawayTaskBiz,selectInboundReference,planClasses} from './inbound-flow.js';
import {documentCodeError} from '../shared/document-code.js';
import {pickTeamStatements} from './native-lifecycle.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
export async function ensureDispatchStartGuard(e){
 await ensureSchema(e.DB,'native-start-atomic-v1',()=>e.DB.batch([
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_dispatch_worker_exclusive BEFORE INSERT ON v2_ops_job_workers WHEN NEW.left_at='' AND EXISTS(SELECT 1 FROM sop_records WHERE id=NEW.job_id AND kind IN ('dispatch','task')) BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.worker_id=NEW.worker_id AND w.left_at='') THEN RAISE(ABORT,'worker_has_active_job') END; END`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_dispatch_document_exclusive BEFORE INSERT ON sop_records WHEN NEW.kind='dispatch' AND COALESCE(json_extract(NEW.state,'$.source_id'),'')!='' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM sop_records s JOIN v2_ops_jobs j ON j.id=s.id WHERE s.kind='dispatch' AND s.id!=NEW.id AND j.status IN ('pending','working','awaiting_close') AND json_extract(s.state,'$.source_type')=json_extract(NEW.state,'$.source_type') AND json_extract(s.state,'$.source_id')=json_extract(NEW.state,'$.source_id') AND json_extract(s.state,'$.job_type')=json_extract(NEW.state,'$.job_type') AND (j.job_type NOT IN ('inbound_direct','inbound_bulk') OR COALESCE(j.inbound_external_no,'')=COALESCE((SELECT inbound_external_no FROM v2_ops_jobs WHERE id=NEW.id),''))) THEN RAISE(ABORT,'document_has_active_dispatch') END; END`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_dispatch_active_job BEFORE INSERT ON sop_records WHEN NEW.kind='dispatch' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.id AND status IN ('pending','working','awaiting_close')) THEN RAISE(ABORT,'document_or_job_changed') END; END`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_dispatch_pick_doc_exclusive BEFORE INSERT ON v2_ops_job_pick_docs BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM v2_ops_job_pick_docs d JOIN v2_ops_jobs j ON j.id=d.job_id WHERE d.pick_doc_no=NEW.pick_doc_no AND d.job_id!=NEW.job_id AND j.status IN ('pending','working','awaiting_close')) THEN RAISE(ABORT,'pick_document_has_active_trip') END; END`)
 ]));
}
async function commitPrepared(e,p,staff,{id,t,sql,after=[],sourceType,sourceId,jobType,extra={}}){
 const lead=staff.workers.find(w=>w.id===staff.lead_id),signature=dispatchSignature(e,p,staff),result={ok:true,job_id:id,worker_seg_id:'WS-'+crypto.randomUUID(),is_new_job:true,lead,assigned_workers:staff.workers,...extra,_dispatch_signature:signature};
 const statements=[q(e,'INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES(?,?,?,?)',p.client_req_id,p.action,JSON.stringify(result),t),...sql,...staffedDispatchStatements(e,{...p,job_type:jobType,source_type:sourceType,source_id:sourceId},staff,id,t,result),...after];
 await ensureDispatchStartGuard(e);try{await e.DB.batch(statements);}catch(error){const prior=await q(e,'SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?',p.client_req_id).first();if(prior){const saved=JSON.parse(prior.response_json);if(saved._dispatch_signature===signature)return saved;throw Error('同一请求的人员或作业要求已变化，请重新提交');}throw error;}return result;
}
export async function startAtomicDocument(e,p,staff,pickNumber){
 const t=new Date().toISOString(),lead=staff.workers.find(w=>w.id===staff.lead_id),sql=[],after=[],extra={};let id='JOB-'+crypto.randomUUID(),type,flow,biz='',doc,source;
 if(['v2_pick_job_start','v2_pick_job_start_by_docs'].includes(p.action)){
  type='pick_direct';flow='order_op';biz='direct_ship';doc='';source='';const codes=p.pick_doc_nos;
  if(p.action==='v2_pick_job_start_by_docs'){
   const docs=(await q(e,`SELECT d.*,j.status AS job_status,j.is_temporary_interrupt FROM v2_ops_job_pick_docs d JOIN v2_ops_jobs j ON j.id=d.job_id WHERE d.pick_doc_no IN (SELECT value FROM json_each(?)) ORDER BY d.created_at DESC`,JSON.stringify(codes)).all()).results||[],selected=codes.map(code=>docs.find(d=>d.pick_doc_no===code));
   if(selected.some(d=>!d)||new Set(selected.map(d=>d.job_id)).size!==1||selected.some(d=>['completed','cancelled'].includes(d.job_status)||d.is_temporary_interrupt))return {ok:false,error:'拣货单不存在、跨趟次或原趟次已关闭，请核对原任务'};
   id=selected[0].job_id;const live=await q(e,"SELECT id FROM v2_ops_job_workers WHERE job_id=? AND left_at='' LIMIT 1",id).first();if(live)return {ok:false,error:'原趟次有人作业，请由负责人交接原任务'};
   sql.push(q(e,"UPDATE v2_ops_jobs SET status='working',active_worker_count=?,updated_at=? WHERE id=? AND status IN ('pending','working','awaiting_close')",staff.workers.length,t,id));extra.is_new_job=false;
  }else{
   const number=await pickNumber();sql.push(q(e,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count,display_no) VALUES(?,'order_op','direct_ship','pick_direct','','','working',?,?,?,?,?)`,id,lead.id,t,t,staff.workers.length,number));
   for(const code of codes)sql.push(q(e,"INSERT INTO v2_ops_job_pick_docs(id,job_id,pick_doc_no,pick_status,created_at) VALUES(?,?,?,'pending',?)",'PD-'+crypto.randomUUID(),id,code,t));Object.assign(extra,{trip_no:number,display_no:number,pick_doc_nos:codes});
  }
  after.push(...pickTeamStatements(e,id,t));
 }else{
  let table,column,status;
  if(p.action==='v2_outbound_stock_op_start'){source=String(p.outbound_order_id||p.order_id||'');doc='outbound_order';type='outbound_stock_op';flow='outbound';table='v2_outbound_orders';column='status';}
  else if(p.action==='v2_issue_handle_start'){source=String(p.issue_id||'');doc='issue_ticket';type='issue_handle';flow='issue_handle';table='v2_issue_tickets';column='status';}
  else if(p.action==='v2_verify_job_start'){source=String(p.batch_id||'');doc='verify_batch';type='verify_scan';flow='order_op';table='v2_verify_batches';column='status';}
  else return {ok:false,error:'不支持的单据派工'};
  const record=source?await q(e,'SELECT * FROM '+table+' WHERE id=?',source).first():null;if(!record)return {ok:false,error:'单据不存在'};status=record.status;biz=record.biz_class||'';
  if(type==='outbound_stock_op'&&(Number(record.uses_stock_operation)!==1||!['operation_reserved','stock_operating'].includes(status)))return {ok:false,error:'当前出库单不是可开始的库内操作单'};
  if(['completed','cancelled','closed'].includes(status))return {ok:false,error:'该单据已关闭，不能开始'};
  const old=await q(e,"SELECT id FROM v2_ops_jobs WHERE job_type=? AND related_doc_type=? AND related_doc_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1",type,doc,source).first();if(old)return {ok:false,error:'已有在途任务，请继续原任务',active_job_id:old.id};
  sql.push(q(e,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count) SELECT ?,?,?,?,?,?,'working',?,?,?,? FROM ${table} WHERE id=? AND ${column}=?`,id,flow,biz,type,doc,source,lead.id,t,t,staff.workers.length,source,status));
  if(type==='outbound_stock_op')after.push(q(e,"UPDATE v2_outbound_orders SET status='stock_operating',stock_operation_status='working',stock_operation_job_id=?,updated_at=? WHERE id=?",id,t,source));
  else if(type==='verify_scan'){extra.batch_id=source;after.push(q(e,"UPDATE v2_verify_batches SET status='verifying',updated_at=? WHERE id=? AND status='pending'",t,source));}
  else{extra.run_id='RUN-'+crypto.randomUUID();after.push(q(e,"INSERT INTO v2_issue_handle_runs(id,issue_id,job_id,handler_id,handler_name,started_at,run_status,created_at) VALUES(?,?,?,?,?,?,'working',?)",extra.run_id,source,id,lead.id,lead.name,t,t),q(e,"UPDATE v2_issue_tickets SET status='processing',updated_at=? WHERE id=?",t,source));}
 }
 return commitPrepared(e,p,staff,{id,t,sql,after,jobType:type,sourceType:doc,sourceId:source,extra});
}
export async function startAtomicInbound(e,p,staff,number){
 const id='JOB-'+crypto.randomUUID(),t=new Date().toISOString(),lead=staff.workers.find(w=>w.id===staff.lead_id),type=p.job_type||'inbound_direct',returned=type==='inbound_return',biz=String(p.biz_class||''),sql=[];let planId=String(p.plan_id||''),code='';
 if(type==='inbound_change_order')return {ok:false,error:'换单计划卸货后自动入库，无需再次理货'};
 if(!['inbound_direct','inbound_bulk','inbound_return'].includes(type)||(!returned&&biz!==(type==='inbound_bulk'?'bulk':'direct_ship'))||(returned&&biz&&biz!=='return'))return {ok:false,error:'入库业务类型不匹配'};
 if(returned&&!planId){planId='IB-'+crypto.randomUUID();const display=await number(),day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);sql.push(q(e,`INSERT INTO v2_inbound_plans(id,plan_date,customer,biz_class,biz_classes_json,cargo_summary,purpose,remark,status,created_by,created_at,updated_at,display_no,source_type) VALUES(?,?,?,'return','["return"]','退件入库会话','',?,'putting_away',?,?,?,?,'return_session')`,planId,day,String(p.customer_name||'未指定'),String(p.start_remark||''),lead.id,t,t,display));}
 else{
  if(!planId){const found=await resolveInboundPlan(e,p.external_inbound_no,biz);if(found.kind!=='system')return {ok:false,error:found.message};planId=found.plan.id;}
  const plan=await q(e,'SELECT * FROM v2_inbound_plans WHERE id=?',planId).first();if(!plan||plan.is_deleted)return {ok:false,error:'入库计划不存在或已删除'};
  if(returned){if(plan.source_type!=='return_session'||plan.biz_class!=='return'||plan.status!=='putting_away')return {ok:false,error:'退件会话状态不允许开工'};}
  else{
   if(!['unloading','unloading_putting_away','arrived_pending_putaway','putting_away','partially_completed'].includes(plan.status))return {ok:false,error:'当前计划状态不可开始理货'};
   const taskBiz=putawayTaskBiz(e,plan,biz);if(!taskBiz||!planClasses(plan).includes(taskBiz))return {ok:false,error:'此计划没有本部门的理货上架'};
   code=await selectInboundReference(e,plan,p.external_inbound_no,taskBiz);const task=await q(e,'SELECT status FROM v2_inbound_plan_biz_tasks WHERE plan_id=? AND biz_class=?',planId,taskBiz).first();if(task?.status==='completed')return {ok:false,error:'本部门理货已完成'};
   sql.push(q(e,`INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks(id,plan_id,biz_class,job_type,status,created_at,updated_at) VALUES(?,?,?,?,'pending',?,?)`,'IBT-'+crypto.randomUUID(),planId,taskBiz,type,t,t));
   sql.push(q(e,"UPDATE v2_inbound_plans SET status=CASE WHEN status='unloading' THEN 'unloading_putting_away' WHEN status='arrived_pending_putaway' THEN 'putting_away' ELSE status END,updated_at=? WHERE id=?",t,planId));
  }
  const active=await q(e,"SELECT id FROM v2_inbound_plan_jobs WHERE plan_id=? AND related_doc_type='inbound_plan' AND job_type IN ('inbound_direct','inbound_bulk','inbound_return') AND status IN ('pending','working','awaiting_close') AND (?='' OR inbound_external_no=? OR COALESCE(inbound_external_no,'')='') LIMIT 1",planId,code,code).first();if(active)return {ok:false,error:'此单已有在途任务，请继续原任务并核对人员',active_job_id:active.id};
 }
 if(returned)sql.push(q(e,`INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks(id,plan_id,biz_class,job_type,status,created_at,updated_at) VALUES(?,?,'return','inbound_return','pending',?,?)`,'IBT-'+crypto.randomUUID(),planId,t,t));
 sql.push(q(e,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count,inbound_external_no) VALUES(?,'inbound',?,?,'inbound_plan',?,'working',?,?,?,?,?)`,id,biz,type,planId,lead.id,t,t,staff.workers.length,code));
 return commitPrepared(e,p,staff,{id,t,sql,jobType:type,sourceType:'inbound_plan',sourceId:planId,extra:{plan_id:planId,external_inbound_no:code}});
}
export async function startAtomicBulk(e,p,staff,findOutbound,needs){
 const no=String(p.work_order_no||'').trim();if(!no||documentCodeError(no))return {ok:false,error:documentCodeError(no)||'缺少作业单号'};
 if(/^(CKWORK\||NEED-|SOPJOB-|(?:ZY|RW)-\d{8}-\d+)/i.test(no))return {ok:false,error:'这是需求作业单，请从需求作业单入口打开'};
 const order=await findOutbound(no);if(order&&(await needs(order.id)).length)return {ok:false,error:'此出库计划已关联作业需求，请打开原需求'};
 if(order&&['completed','cancelled'].includes(order.status))return {ok:false,error:'该工单已完成或取消，请办公室核实'};
 const prior=await q(e,"SELECT id,status FROM v2_ops_jobs WHERE job_type='bulk_op' AND (related_doc_id=? OR (?!='' AND linked_outbound_order_id=?)) ORDER BY created_at DESC LIMIT 1",no,order?.id||'',order?.id||'').first();
 if(prior&&(['pending','working','awaiting_close'].includes(prior.status)||(prior.status==='completed'&&order?.status!=='reopen_pending')))return {ok:false,error:'此单已有任务或已完成，请继续原任务或由办公室安排返工',active_job_id:prior.id};
 const id='JOB-'+crypto.randomUUID(),t=new Date().toISOString(),lead=staff.workers.find(w=>w.id===staff.lead_id),customer=String(order?.customer||p.customer||'').trim();
 const values=[id,no,JSON.stringify(order?.status==='reopen_pending'?{started_from_reopen_pending:true}:{}),order?.id||'',customer,lead.id,t,t,staff.workers.length],tail=order?' FROM v2_outbound_orders WHERE id=? AND status=? AND updated_at=? AND COALESCE(revision_no,0)=?':'';
 const sql=[q(e,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,shared_result_json,linked_outbound_order_id,customer,created_by,created_at,updated_at,active_worker_count) SELECT ?,'order_op','bulk','bulk_op','work_order',?,'working',?,?,?,?,?,?,?${tail}`,...values,...(order?[order.id,order.status,order.updated_at,Number(order.revision_no||0)]:[]))];
 if(order&&['pending_issue','issued','reopen_pending'].includes(order.status))sql.push(q(e,"UPDATE v2_outbound_orders SET status='working',updated_at=? WHERE id=?",t,order.id));
 return commitPrepared(e,p,staff,{id,t,sql,jobType:'bulk_op',sourceType:'work_order',sourceId:order?.id||no,extra:order?{linked_outbound:{...order}}:{}});
}
export function dispatchSignature(e,p,staff){const payload={...p};if(['v2_pick_job_start','v2_pick_job_start_by_docs'].includes(payload.action))payload.action='v2_pick_job_start';for(const key of ['client_req_id','k','worker_id','worker_name','handler_id','handler_name'])delete payload[key];return JSON.stringify({actor:e.SOP_REQUEST_USER.id,payload,workers:staff.workers,lead_id:staff.lead_id,department:staff.department,estimated_minutes:staff.estimated_minutes});}
export async function startAtomicImportDelivery(e,p,staff){
 const id='JOB-'+crypto.randomUUID(),t=new Date().toISOString(),lead=staff.workers.find(w=>w.id===staff.lead_id);
 return commitPrepared(e,p,staff,{id,t,sql:[q(e,"INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count) VALUES(?,'import','import','pickup_delivery_import','','','working',?,?,?,?)",id,lead.id,t,t,staff.workers.length)],sourceType:'',sourceId:'',jobType:'pickup_delivery_import'});
}
export async function startAtomicGeneric(e,p,staff){
 const id='JOB-'+crypto.randomUUID(),t=new Date().toISOString(),lead=staff.workers.find(w=>w.id===staff.lead_id),seg='WS-'+crypto.randomUUID(),signature=dispatchSignature(e,p,staff),result={ok:true,job_id:id,worker_seg_id:seg,is_new_job:true,lead,assigned_workers:staff.workers,_dispatch_signature:signature};
 if(p.related_doc_type&&p.related_doc_id){const old=await q(e,"SELECT id FROM v2_ops_jobs WHERE related_doc_type=? AND related_doc_id=? AND job_type=? AND status IN ('pending','working','awaiting_close')",p.related_doc_type,p.related_doc_id,p.job_type).first();if(old)return {ok:false,error:'此单已有旧任务，请继续原任务并核对人员',active_job_id:old.id};}
 const sql=[q(e,`INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES(?,?,?,?)`,p.client_req_id,p.action,JSON.stringify(result),t),
 q(e,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,parent_job_id,is_temporary_interrupt,interrupt_type,created_by,created_at,updated_at,active_worker_count) VALUES(?,?,?,?,?,?,'working',?,?,?,?,?,?,?)`,id,String(p.flow_stage||''),String(p.biz_class||''),String(p.job_type||''),String(p.related_doc_type||''),String(p.related_doc_id||''),String(p.parent_job_id||''),p.is_temporary_interrupt?1:0,String(p.interrupt_type||''),lead.id,t,t,staff.workers.length),
 ...staffedDispatchStatements(e,{...p,source_type:p.related_doc_type||'',source_id:p.related_doc_id||''},staff,id,t,result)];
 await ensureDispatchStartGuard(e);
 try{await e.DB.batch(sql);}catch(error){const old=await q(e,'SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?',p.client_req_id).first();if(old){const saved=JSON.parse(old.response_json);if(saved._dispatch_signature===signature)return saved;throw Error('同一请求的人员或作业要求已变化，请重新提交');}throw error;}return result;
}
