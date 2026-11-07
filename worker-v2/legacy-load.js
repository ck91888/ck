import {laborDepartment} from '../shared/labor-department.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
// Only a continuing, single-order job can be explicitly claimed; no new order or clock is created.
export async function adoptLegacyLoad(body,env){
 const u=env.SOP_REQUEST_USER;
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true'||!['manager','dispatcher'].includes(u?.role))throw Error('请由授权派审员接续原任务');
 const job=await q(env,"SELECT * FROM v2_ops_jobs WHERE id=? AND job_type='load_outbound' AND related_doc_type='outbound_order' AND status IN ('pending','working','awaiting_close')",String(body.job_id||'')).first();
 if(!job||await q(env,'SELECT job_id FROM ck_load_trips WHERE job_id=?',job.id).first())throw Error('请打开原装货任务');
 const department=laborDepartment(job);if(u.role!=='manager'&&!(u.departments||[]).includes(department))throw Error('无此部门任务权限');
 const existing=await q(env,'SELECT * FROM sop_records WHERE id=?',job.id).first();
 if(existing){if(existing.kind==='dispatch'&&JSON.parse(existing.state).owner_id===u.id)return {ok:true,job_id:job.id};throw Error('原任务已由其他负责人接续，请办理交接');}
 const segments=(await q(env,'SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at',job.id).all()).results||[];
 const active=segments.filter(w=>!w.left_at),last=segments.at(-1),workers=[...new Map(active.map(w=>[w.worker_id,{id:w.worker_id,name:w.worker_name}])).values()],lead=workers[0]||(last?{id:last.worker_id,name:last.worker_name}:null);
 if(!lead)throw Error('原任务缺少工时人员，请管理员核实');
 const t=new Date().toISOString(),data={title:job.job_type,owner_id:u.id,owner:u.name,lead_id:lead.id,last_lead:lead,workers,estimated_minutes:0,job_type:job.job_type,status:job.status,created_at:job.created_at,source_type:job.related_doc_type,source_id:job.related_doc_id,labor_department:department,legacy_native:true};
 const sql=[q(env,"INSERT INTO sop_events SELECT ?,?,0,'sop_native_adopt',?,?,'{}',?,?,? WHERE EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=? AND status IN ('pending','working','awaiting_close')) AND NOT EXISTS(SELECT 1 FROM sop_records WHERE id=?)",'LEGACY-ADOPT-'+job.id,job.id,u.id,u.name,JSON.stringify(data),JSON.stringify({ok:true,job_id:job.id}),t,job.id,job.id),q(env,"INSERT INTO sop_records(id,kind,revision,department,state,updated_at) SELECT ?,'dispatch',1,?,?,? WHERE EXISTS(SELECT 1 FROM sop_events WHERE request_id=? AND actor_id=?)",job.id,department,JSON.stringify(data),t,'LEGACY-ADOPT-'+job.id,u.id)];
 try{await env.DB.batch(sql);}catch(error){const own=await q(env,"SELECT state FROM sop_records WHERE id=? AND kind='dispatch'",job.id).first();if(own&&JSON.parse(own.state).owner_id===u.id)return {ok:true,job_id:job.id};throw error;}
 if(!await q(env,"SELECT id FROM sop_records WHERE id=? AND kind='dispatch'",job.id).first())throw Error('原任务状态已变化，请刷新后核对');
 return {ok:true,job_id:job.id};
}
export async function legacyLoadClaim(env,jobId){
 if(env.SOP_ENVIRONMENT!=='staging'||!jobId)return false;
 return !!await q(env,`SELECT j.id FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id AND s.kind='dispatch' WHERE j.id=? AND j.job_type='load_outbound' AND j.related_doc_type='outbound_order' AND json_extract(s.state,'$.legacy_native')=1 AND NOT EXISTS(SELECT 1 FROM ck_load_trips WHERE job_id=j.id)`,jobId).first();
}
