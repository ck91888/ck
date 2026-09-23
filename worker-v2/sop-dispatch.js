import {pickTeamStatements} from './native-lifecycle.js';
import {dispatchAccess} from './dispatch-access.js';
// Responsible-person assignment around existing operation handlers. Documents,
// result forms and status transitions remain owned by those handlers.
import { laborDepartment,startDepartment,departments } from '../shared/labor-department.js';
const starts=new Set(['v2_unload_job_start','v2_unplanned_unload_start','v2_inbound_job_start','v2_import_delivery_job_start','v2_outbound_load_start','v2_outbound_stock_op_start','v2_issue_handle_start','v2_pick_job_start','v2_pick_job_start_by_docs','v2_bulk_op_job_start','v2_ops_job_start','v2_verify_job_start']);
export async function startNative(body,env,invoke,guard){
 const u=env.SOP_REQUEST_USER;
 if(env.SOP_ENVIRONMENT!=='staging'||!['manager','dispatcher'].includes(u?.role))return {ok:false,error:'请以已授权的派审员登录 / 배정 담당자로 로그인하세요'};
 const p={...body.payload};if(!starts.has(p.action))return {ok:false,error:'无效作业类型'};
 if(p.action==='v2_ops_job_start'&&!String(p.biz_class??'').trim()){
  const department=laborDepartment(p);if(department!=='other')p.biz_class=department;
 }
 const workers=body.workers,ids=new Set();
 if(!Array.isArray(workers)||!workers.length||workers.length>50)return {ok:false,error:'请扫描操作人员工牌'};
 for(const w of workers){if(!w.id||!w.name||ids.has(w.id))return {ok:false,error:'工牌不完整或重复'};ids.add(w.id);}
 const lead=workers.find(x=>x.id===body.lead_id);if(!lead)return {ok:false,error:'请选择实际参与的主操作员'};
 const minutes=Number(body.estimated_minutes);if(!Number.isSafeInteger(minutes)||minutes<1)return {ok:false,error:'预计分钟数必须为正整数'};
 if(!p.client_req_id)return {ok:false,error:'缺少请求编号'};
 const prior=await env.DB.prepare('SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?').bind(p.client_req_id).first();
 const allowed=prior?JSON.parse(prior.response_json).job_id:null;
 if(allowed){const existing=await env.DB.prepare("SELECT state FROM sop_records WHERE id=? AND kind='dispatch'").bind(allowed).first();if(existing){const saved=JSON.parse(existing.state);if(saved.owner_id!==u.id)return {ok:false,error:'此任务已有其他负责人'};return {...JSON.parse(prior.response_json),lead:saved.workers.find(w=>w.id===saved.lead_id),assigned_workers:saved.workers};}}
 const managed=await existingDispatch(env,p);
 if(managed)return {ok:false,error:'该单已有在途任务，请从现场首页打开原任务后调整人员 / 진행 중인 작업을 열어 인원을 변경하세요',active_job_id:managed.id};
 let department=body.labor_department||startDepartment(p);
 if(!body.labor_department&&department==='other'&&p.action==='v2_issue_handle_start'){
  const issue=await env.DB.prepare('SELECT biz_class FROM v2_issue_tickets WHERE id=?').bind(p.issue_id).first();department=laborDepartment(issue||{});
 }
 if(!Object.hasOwn(departments,department))return {ok:false,error:'请选择本次用工部门 / 작업 부서를 선택하세요'};
 for(const w of workers){const busy=await env.DB.prepare("SELECT job_id FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' AND job_id!=? LIMIT 1").bind(w.id,allowed||'').first();if(busy)return {ok:false,error:w.name+'仍在另一任务中，请先办理人员交接'};}
 p.worker_id=lead.id;p.worker_name=lead.name;p.handler_id=lead.id;p.handler_name=lead.name;
 const blocked=await guard(p,env);if(blocked)return {ok:false,error:blocked};
 env.SOP_NATIVE_START=true;
 const result=await invoke(p);if(!result.ok||!result.job_id)return result;
 const job=await env.DB.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').bind(result.job_id).first();
 const existing=await env.DB.prepare('SELECT * FROM sop_records WHERE id=?').bind(result.job_id).first();
 if(existing){if(existing.kind!=='dispatch'||JSON.parse(existing.state).owner_id!==u.id)return {ok:false,error:'此任务已有其他负责人，请交接后操作'};const saved=JSON.parse(existing.state);return {...result,lead:saved.workers.find(w=>w.id===saved.lead_id),assigned_workers:saved.workers};}
 const t=new Date().toISOString(),data={title:job.job_type,owner_id:u.id,owner:u.name,lead_id:lead.id,workers,estimated_minutes:minutes,job_type:job.job_type,status:job.status,created_at:t,source_type:job.related_doc_type,source_id:job.related_doc_id,labor_department:department};
 const sql=[env.DB.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').bind('DISPATCH-'+p.client_req_id,job.id,0,'sop_native_start',u.id,u.name,'{}',JSON.stringify(data),JSON.stringify({ok:true,id:job.id,revision:1}),t),env.DB.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').bind(job.id,'dispatch',1,department,JSON.stringify(data),t)];
 for(const w of workers){const joined=await env.DB.prepare("SELECT id FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at='' LIMIT 1").bind(job.id,w.id).first();if(!joined)sql.push(env.DB.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)').bind('WS-'+crypto.randomUUID(),job.id,w.id,w.name,t));}
 if(job.job_type==='pick_direct')sql.push(...pickTeamStatements(env,job.id,t));
 sql.push(env.DB.prepare("UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(*) FROM v2_ops_job_workers WHERE job_id=? AND left_at=''),status='working',updated_at=? WHERE id=?").bind(job.id,t,job.id));
 try{await env.DB.batch(sql);}catch(e){return {ok:false,job_id:job.id,error:'任务已建立，但人员登记未全部成功。请保留当前任务并重试同一次派工：'+e.message};}
 return {...result,lead,assigned_workers:workers};
}
export async function nativeOwner(body,env){
 if(env.SOP_ENVIRONMENT!=='staging'||!env.SOP_REQUEST_USER||!body.job_id)return false;
 const access=dispatchAccess(env.SOP_REQUEST_USER);
 return !!await env.DB.prepare("SELECT s.id FROM sop_records s WHERE s.id=? AND s.kind='dispatch' AND "+access.sql).bind(body.job_id,...access.args).first();
}

// A fresh dispatch must not silently join only the new lead to an existing crew.
async function existingDispatch(env,p){
 let where='',args=[];
 if(p.action==='v2_pick_job_start_by_docs'){
  const docs=Array.isArray(p.pick_doc_nos)?p.pick_doc_nos:String(p.pick_doc_nos||'').split(',');
  if(docs.length){where='j.id IN (SELECT job_id FROM v2_ops_job_pick_docs WHERE pick_doc_no IN ('+docs.map(()=>'?').join(',')+'))';args=docs.map(x=>String(x).trim());}
 }else if(p.action==='v2_bulk_op_job_start'){
  where="j.job_type='bulk_op' AND (j.related_doc_id=? OR j.linked_outbound_order_id IN (SELECT id FROM v2_outbound_orders WHERE id=? OR display_no=? OR wms_work_order_no=?))";args=Array(4).fill(String(p.work_order_no||'').trim());
 }else if(['v2_outbound_load_start','v2_outbound_stock_op_start'].includes(p.action)&& (p.order_id||p.outbound_order_id)){
  where="j.related_doc_type='outbound_order' AND j.related_doc_id=? AND j.job_type=?";args=[p.order_id||p.outbound_order_id,p.action==='v2_outbound_load_start'?'load_outbound':'outbound_stock_op'];
 }else if(p.action==='v2_inbound_job_start'&&p.external_inbound_no){where='j.inbound_external_no=?';args=[String(p.external_inbound_no).trim().toUpperCase()];
 }else if(p.plan_id){where="j.related_doc_type='inbound_plan' AND j.related_doc_id=? AND j.job_type=?";args=[p.plan_id,p.action==='v2_unload_job_start'?'unload':p.job_type];
 }else if(p.action==='v2_verify_job_start'&&p.batch_id){where="j.related_doc_id=? AND j.job_type='verify_scan'";args=[p.batch_id];
 }else if(p.action==='v2_ops_job_start'&&p.related_doc_id){where='j.related_doc_id=? AND j.related_doc_type=? AND j.job_type=?';args=[p.related_doc_id,p.related_doc_type,p.job_type];}
 if(!where)return null;
 return env.DB.prepare("SELECT j.id FROM v2_ops_jobs j JOIN sop_records r ON r.id=j.id AND r.kind='dispatch' WHERE j.status IN ('pending','working','awaiting_close') AND "+where+' LIMIT 1').bind(...args).first();
}
