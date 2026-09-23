// Attribute a completed physical unload to an existing plan. Never copy labor/output.
import {inboundFlowEnabled,planClasses} from './inbound-flow.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const fail=m=>{throw Error(m);};
const parse=s=>{try{return JSON.parse(s||'[]');}catch{return [];}};
const unit=s=>s==='box'?'carton':String(s||'');
function actor(env){const u=env.SOP_REQUEST_USER;if(!inboundFlowEnabled(env)||!u||!['manager','service','reviewer'].includes(u.role))fail('请由办公室或负责人关联入库计划 / 사무실·관리자 권한이 필요합니다');return u;}
const eligible=`p.status='pending' AND COALESCE(p.is_deleted,0)=0 AND COALESCE(p.accounted,0)=0 AND COALESCE(p.unload_completed_at,'')='' AND p.source_type NOT IN ('return_session','external_inbound')
 AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=p.id AND j.status IN ('pending','working','awaiting_close','completed'))
 AND NOT EXISTS(SELECT 1 FROM ck_feedback_plan_links l WHERE l.plan_id=p.id)`;
async function source(env,id){
 const fb=await q(env,'SELECT * FROM v2_field_feedbacks WHERE id=?',id).first();
 if(!fb||fb.is_deleted||!['unplanned_unload','unload_no_doc'].includes(fb.feedback_type))fail('不是可关联的计划外卸货反馈 / 연결할 수 없는 피드백입니다');
 if(fb.inbound_plan_id||!['unloaded_pending_info','open'].includes(fb.status))fail('反馈尚未完成卸货，或已处理，请刷新 / 하차 미완료 또는 이미 처리된 피드백입니다');
 if(await q(env,'SELECT id FROM v2_inbound_plans WHERE source_feedback_id=? LIMIT 1',id).first())fail('此反馈已生成入库计划，请勿重复关联');
 const job=await q(env,"SELECT * FROM v2_ops_jobs WHERE id=? AND job_type='unload' AND related_doc_type='field_feedback' AND related_doc_id=? AND status='completed'",fb.related_doc_id,id).first();
 if(!job||await q(env,"SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND COALESCE(left_at,'')='' LIMIT 1",job.id).first())fail('请先完成现场卸货并结束人员计时 / 먼저 하차 작업을 완료하세요');
 const latest=await q(env,'SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at DESC LIMIT 1',job.id).first();
 let result=parse(fb.result_lines_json);if(!result.length)result=parse(latest?.result_lines_json);if(!result.length)result=parse(job.shared_result_json)?.result_lines||[];
 if(!Array.isArray(result)||!result.length)fail('缺少实际卸货数量，请先核实原任务');
 const totals=new Map();for(const l of result){const n=Number(l.actual_qty),u=unit(l.unit_type);if(!u||u==='courier'||!Number.isFinite(n)||n<0||n>1e8)fail('原卸货明细无效，请先核实原任务');totals.set(u,(totals.get(u)||0)+n);}
 if(![...totals.values()].some(n=>n>0))fail('原任务没有有效的卸货数量');
 return {fb,job,totals};
}
async function target(env,id){
 const p=await q(env,`SELECT p.* FROM v2_inbound_plans p WHERE p.id=? AND ${eligible}`,id).first();
 if(!p)fail('该计划已收货、已开始作业或已记账，不能重复关联 / 이미 처리 중인 계획입니다');
 const lines=await rows(env,'SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY line_no,id',id);
 if(lines.length>150)fail('明细过多，请先整理入库计划');
 return {p,lines};
}
function receipt(src,lines){
 const out=lines.filter(l=>l.unit_type!=='courier').map(l=>({...l,actual_qty:lines.filter(x=>unit(x.unit_type)===unit(l.unit_type)).length===1?(src.totals.get(unit(l.unit_type))||0):null}));
 for(const [u,n] of src.totals)if(n>0&&!out.some(l=>unit(l.unit_type)===u))out.push({id:'',unit_type:u,planned_qty:0,actual_qty:n,remark:'计划外卸货实收 / 현장 하차 수량'});
 return out;
}
async function reconcile(env,link,hooks){
 const p=await q(env,'SELECT * FROM v2_inbound_plans WHERE id=?',link.plan_id).first();
 await hooks.ensureTasks(env,p);await hooks.syncCourier(env,p.id,hooks.recalc);await hooks.recalc(env,p.id,new Date().toISOString());
 const latest=await q(env,'SELECT status FROM v2_inbound_plans WHERE id=?',p.id).first();
 return {ok:true,inbound_plan_id:p.id,display_no:p.display_no,status:latest.status,job_id:link.job_id};
}
export async function feedbackLinkDetail(env,id){
 return q(env,`SELECT l.feedback_id,l.plan_id,l.job_id,l.linked_by,l.linked_name,l.linked_at,p.display_no,p.customer,f.display_no AS feedback_no FROM ck_feedback_plan_links l JOIN v2_inbound_plans p ON p.id=l.plan_id JOIN v2_field_feedbacks f ON f.id=l.feedback_id WHERE l.feedback_id=? OR l.plan_id=? LIMIT 1`,id,id).first();
}
export async function handleFeedbackLink(b,env,hooks){
 const u=actor(env),id=String(b.feedback_id||'').trim();if(!id||id.length>120)fail('缺少反馈编号');
 if(b.action==='sop_feedback_link_save'){
  const old=await q(env,'SELECT * FROM ck_feedback_plan_links WHERE feedback_id=?',id).first();
  if(old){if(old.plan_id!==b.plan_id)fail('此反馈已关联其他计划，不能重复关联');return {...await reconcile(env,old,hooks),already_linked:true};}
 }
 const src=await source(env,id);
 if(b.action==='sop_feedback_link_candidates'){
  const term=String(b.keyword||'').trim().slice(0,120),offset=Math.max(0,Math.min(100000,Math.floor(Number(b.offset)||0))),like='%'+term.replace(/[\\%_]/g,'\\$&')+'%';
  const items=await rows(env,`SELECT p.id,p.display_no,p.customer,p.expected_arrival,p.cargo_summary,p.biz_class,p.biz_classes_json,p.external_inbound_no,
   (SELECT json_group_array(json_object('unit_type',unit_type,'planned_qty',planned_qty)) FROM v2_inbound_plan_lines WHERE plan_id=p.id) AS lines_json
   FROM v2_inbound_plans p WHERE ${eligible} AND (?='' OR p.customer LIKE ? ESCAPE '\\' OR p.display_no LIKE ? ESCAPE '\\' OR p.external_inbound_no LIKE ? ESCAPE '\\' OR p.id=?) ORDER BY p.created_at DESC,p.id LIMIT 21 OFFSET ?`,term,like,like,like,term,offset);
  return {ok:true,items:items.slice(0,20).map(p=>({...p,lines:parse(p.lines_json)})),has_more:items.length>20};
 }
 if(!['sop_feedback_link_preview','sop_feedback_link_save'].includes(b.action))fail('未知关联操作');
 const {p,lines}=await target(env,String(b.plan_id||''));const mapped=receipt(src,lines);
 if(b.action==='sop_feedback_link_preview')return {ok:true,plan:p,feedback_version:src.fb.updated_at,job_version:src.job.updated_at,plan_version:p.updated_at,lines:mapped,actual_totals:[...src.totals].map(([unit_type,actual_qty])=>({unit_type,actual_qty})),needs_putaway:planClasses(p).includes('direct_ship'),completed_at:src.fb.completed_at||src.job.updated_at};
 if(b.feedback_version!==src.fb.updated_at||b.plan_version!==p.updated_at||b.job_version!==src.job.updated_at)fail('记录已变化，请重新选择并核对 / 새로고침 후 다시 확인하세요');
 if(!Array.isArray(b.lines)||b.lines.length!==mapped.length||new Set(b.lines.map(l=>l.line_id||'new:'+unit(l.unit_type))).size!==mapped.length)fail('请核对所有实收明细');
 const allocated=new Map();const actual=mapped.map(l=>{const input=b.lines.find(x=>l.id?x.line_id===l.id:!x.line_id&&unit(x.unit_type)===unit(l.unit_type)),n=input?.actual_qty;if(typeof n!=='number'||!Number.isFinite(n)||n<0||n>1e8)fail('请填写每行实际数量 / 각 행의 실제 수량을 입력하세요');allocated.set(unit(l.unit_type),(allocated.get(unit(l.unit_type))||0)+n);return {line_id:l.id||'IPL-'+crypto.randomUUID(),unit_type:l.unit_type,actual_qty:n,new_line:!l.id,planned_qty:l.planned_qty};});
 for(const key of new Set([...allocated.keys(),...src.totals.keys()]))if(Math.abs((allocated.get(key)||0)-(src.totals.get(key)||0))>0.000001)fail('各类型分配合计必须等于原卸货实收，不能改写现场产出 / 원래 하차 수량과 일치해야 합니다');
 const t=new Date().toISOString(),completed=src.fb.completed_at||src.job.updated_at;
 const own={plan_id:p.id,result_lines:actual.map(({line_id,unit_type,actual_qty})=>({line_id,unit_type,actual_qty})),has_diff:actual.some(l=>l.actual_qty!==l.planned_qty),diff_note:src.fb.diff_note||''};
 const workers=await rows(env,'SELECT DISTINCT worker_name FROM v2_ops_job_workers WHERE job_id=?',src.job.id),names=workers.map(w=>w.worker_name).filter(Boolean).join('、');
 const sql=[q(env,`INSERT INTO ck_feedback_plan_links(feedback_id,plan_id,job_id,feedback_version,plan_version,job_version,linked_by,linked_name,linked_at,snapshot_json) VALUES(?,?,?,?,?,?,?,?,?,?)`,id,p.id,src.job.id,src.fb.updated_at,p.updated_at,src.job.updated_at,u.id,u.name||u.id,t,JSON.stringify({feedback:src.fb,plan:p,lines,result:own})),
  q(env,'INSERT INTO ck_unload_plan_links(job_id,plan_id,position,plan_version,lines_snapshot,result_json) VALUES(?,?,0,?,?,?)',src.job.id,p.id,p.updated_at,JSON.stringify(lines),JSON.stringify(own)),
  q(env,"UPDATE v2_ops_jobs SET related_doc_type='inbound_plan',related_doc_id=? WHERE id=?",p.id,src.job.id)];
 for(const [i,l] of actual.entries())sql.push(l.new_line?q(env,'INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty,actual_qty,remark) VALUES(?,?,?,?,0,?,?)',l.line_id,p.id,Math.max(0,...lines.map(x=>Number(x.line_no)||0))+i+1,l.unit_type,l.actual_qty,'计划外卸货实收 / 현장 하차 수량'):q(env,'UPDATE v2_inbound_plan_lines SET actual_qty=? WHERE id=? AND plan_id=?',l.actual_qty,l.line_id,p.id));
 sql.push(q(env,"UPDATE v2_inbound_plans SET status='arrived_pending_putaway',unload_completed_at=?,unload_completed_by=?,updated_at=? WHERE id=?",completed,names||src.fb.completed_by||'',t,p.id),
  q(env,"UPDATE v2_field_feedbacks SET status='converted',inbound_plan_id=?,updated_at=? WHERE id=?",p.id,t,id));
 try{await env.DB.batch(sql);}catch(error){
  const old=await q(env,'SELECT * FROM ck_feedback_plan_links WHERE feedback_id=?',id).first();
  if(old?.plan_id===p.id)return {...await reconcile(env,old,hooks),already_linked:true};
  if(/feedback_link_changed|unload_plan_changed|unload_plan_busy|UNIQUE constraint/.test(error.message))fail('反馈或计划已被其他人处理，请刷新后核对 / 다른 담당자가 처리했습니다');
  throw error;
 }
 return reconcile(env,{plan_id:p.id,job_id:src.job.id},hooks);
}
