// Responsible-person assignment around existing operation handlers. Documents,
// result forms and status transitions remain owned by those handlers.
const starts=new Set(['v2_unload_job_start','v2_unplanned_unload_start','v2_inbound_job_start','v2_import_delivery_job_start','v2_outbound_load_start','v2_outbound_stock_op_start','v2_issue_handle_start','v2_pick_job_start','v2_pick_job_start_by_docs','v2_bulk_op_job_start','v2_ops_job_start','v2_verify_job_start']);
export async function startNative(body,env,invoke,guard){
 const u=env.SOP_REQUEST_USER;
 if(env.SOP_ENVIRONMENT!=='staging'||u?.role!=='manager')return {ok:false,error:'请以测试负责人登录'};
 const p={...body.payload};if(!starts.has(p.action))return {ok:false,error:'无效作业类型'};
 const workers=body.workers,ids=new Set();
 if(!Array.isArray(workers)||!workers.length||workers.length>50)return {ok:false,error:'请扫描操作人员工牌'};
 for(const w of workers){if(!w.id||!w.name||ids.has(w.id))return {ok:false,error:'工牌不完整或重复'};ids.add(w.id);}
 const lead=workers.find(x=>x.id===body.lead_id);if(!lead)return {ok:false,error:'请选择实际参与的主操作员'};
 const minutes=Number(body.estimated_minutes);if(!Number.isSafeInteger(minutes)||minutes<1)return {ok:false,error:'预计分钟数必须为正整数'};
 if(!p.client_req_id)return {ok:false,error:'缺少请求编号'};
 const prior=await env.DB.prepare('SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?').bind(p.client_req_id).first();
 const allowed=prior?JSON.parse(prior.response_json).job_id:null;
 for(const w of workers){const busy=await env.DB.prepare("SELECT job_id FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' AND job_id!=? LIMIT 1").bind(w.id,allowed||'').first();if(busy)return {ok:false,error:w.name+'仍在另一任务中，请先办理人员交接'};}
 p.worker_id=lead.id;p.worker_name=lead.name;p.handler_id=lead.id;p.handler_name=lead.name;
 const blocked=await guard(p,env);if(blocked)return {ok:false,error:blocked};
 const result=await invoke(p);if(!result.ok||!result.job_id)return result;
 const job=await env.DB.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').bind(result.job_id).first();
 const existing=await env.DB.prepare('SELECT * FROM sop_records WHERE id=?').bind(result.job_id).first();
 if(existing){if(existing.kind!=='dispatch'||JSON.parse(existing.state).owner_id!==u.id)return {ok:false,error:'此任务已有其他负责人，请交接后操作'};const saved=JSON.parse(existing.state);return {...result,lead:saved.workers.find(w=>w.id===saved.lead_id),assigned_workers:saved.workers};}
 const t=new Date().toISOString(),data={title:job.job_type,owner_id:u.id,owner:u.name,lead_id:lead.id,workers,estimated_minutes:minutes,job_type:job.job_type,status:job.status,created_at:t,source_type:job.related_doc_type,source_id:job.related_doc_id};
 const sql=[env.DB.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').bind('DISPATCH-'+p.client_req_id,job.id,0,'sop_native_start',u.id,u.name,'{}',JSON.stringify(data),JSON.stringify({ok:true,id:job.id,revision:1}),t),env.DB.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').bind(job.id,'dispatch',1,job.biz_class||'bulk',JSON.stringify(data),t)];
 for(const w of workers){const joined=await env.DB.prepare("SELECT id FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at='' LIMIT 1").bind(job.id,w.id).first();if(!joined)sql.push(env.DB.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)').bind('WS-'+crypto.randomUUID(),job.id,w.id,w.name,t));}
 sql.push(env.DB.prepare("UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(*) FROM v2_ops_job_workers WHERE job_id=? AND left_at=''),status='working',updated_at=? WHERE id=?").bind(job.id,t,job.id));
 try{await env.DB.batch(sql);}catch(e){return {ok:false,job_id:job.id,error:'任务已建立，但人员登记未全部成功。请保留当前任务并重试同一次派工：'+e.message};}
 return {...result,lead,assigned_workers:workers};
}
export async function nativeOwner(body,env){
 if(env.SOP_ENVIRONMENT!=='staging'||!env.SOP_REQUEST_USER||!body.job_id)return false;
 const row=await env.DB.prepare("SELECT state FROM sop_records WHERE id=? AND kind='dispatch'").bind(body.job_id).first();
 return !!row&&(env.SOP_REQUEST_USER.role==='manager'||JSON.parse(row.state).owner_id===env.SOP_REQUEST_USER.id);
}
