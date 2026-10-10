// Inbound dispositions use the existing storage keys for compatibility.
// Cross-dock retains `bulk`; only `bulk_putaway` denotes bulk putaway.
import {referenceWriteBatch} from './inbound-reference-history.js';
export const inboundLabels={direct_ship:'代发理货上架',bulk_putaway:'大货理货上架',bulk:'直进直出',return:'整托退回',change_order:'换单'};
export const inboundFlowEnabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true';
export const inboundCode=value=>String(value??'').normalize('NFKC').trim();
export const inboundReferenceValue=(body,fallback='')=>body.external_inbound_nos??body.external_inbound_no??fallback;
export function inboundCodes(value){return [...new Set((Array.isArray(value)?value:[value]).flatMap(x=>inboundCode(x).split(/[\n\r,;，；\t]+/)).map(inboundCode).filter(Boolean))];}
export const putawayClasses=['direct_ship','bulk_putaway'];
export const needsPutaway=plan=>planClasses(plan).some(x=>putawayClasses.includes(x));
export function departmentCodes(plan,biz){
 const all=inboundCodes(plan.external_inbound_no),bulk=inboundCodes(plan.bulk_external_inbound_no);
 if(biz==='bulk_putaway')return bulk;
 if(biz==='direct_ship')return all.filter(x=>!bulk.includes(x));
 return all;
}
export async function inboundReferenceData(env,classes,body,plan={}){
 const all=inboundCodes(inboundReferenceValue(body,plan.external_inbound_no));
 let bulk=inboundCodes(body.bulk_external_inbound_no??plan.bulk_external_inbound_no);
 if(classes.includes('bulk_putaway')&&!classes.includes('direct_ship')&&body.bulk_external_inbound_no===undefined)bulk=all;
 if(!classes.includes('bulk_putaway'))bulk=[];
 const combined=[...new Set([...all,...bulk])];
 if(inboundFlowEnabled(env)){
  if(body.direct_external_inbound_no!==undefined&&inboundCodes(body.direct_external_inbound_no).some(x=>bulk.includes(x)))throw Error('两部门不能使用同一外部入库单号');
  if(classes.includes('bulk_putaway')&&departmentCodes(plan,'bulk_putaway').length&&!bulk.length)throw Error('已有的大货外部入库单号不能清空');
  if(classes.includes('direct_ship')&&departmentCodes(plan,'direct_ship').length&&!combined.some(x=>!bulk.includes(x)))throw Error('已有的代发外部入库单号不能清空');
  if(!classes.includes('direct_ship')&&classes.includes('bulk_putaway')&&combined.some(x=>!bulk.includes(x)))throw Error('存在未分配理货部门的入库单号');
 }
 return {externalNo:await validateInboundCode(env,classes,combined,plan.id||''),bulkExternalNo:bulk.join('\n')};
}
export async function inboundCodeProgress(env,plan,knownJobs,biz){
 if(!inboundFlowEnabled(env)||!plan||!needsPutaway(plan))return null;
 const codes=departmentCodes(plan,biz),allCodes=inboundCodes(plan.external_inbound_no);
 const jobs=knownJobs ? knownJobs.filter(j=>['inbound_direct','inbound_bulk'].includes(j.job_type)&&j.status!=='cancelled') : (await env.DB.prepare("SELECT id,job_type,status,inbound_external_no,created_at,updated_at FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type IN ('inbound_direct','inbound_bulk') AND status!='cancelled' ORDER BY created_at DESC").bind(plan.id).all()).results||[];
 const items=codes.map(code=>{
  const matches=jobs.filter(j=>j.inbound_external_no===code||(!j.inbound_external_no&&allCodes.length===1));
  const job=matches.find(j=>j.status==='completed')||matches.find(j=>['pending','working','awaiting_close'].includes(j.status));
  const historicalComplete=codes.length===1&&plan.status==='completed';
  return {external_no:code,biz_class:inboundCodes(plan.bulk_external_inbound_no).includes(code)?'bulk_putaway':'direct_ship',status:job?.status==='completed'||historicalComplete?'completed':job?'working':'pending',job_id:job?.id||'',completed_at:job?.status==='completed'?job.updated_at:historicalComplete?(plan.manual_completed_at||plan.updated_at||''):''};
 });
 const missing_departments=planClasses(plan).filter(x=>putawayClasses.includes(x)&&(!biz||x===biz)&&!departmentCodes(plan,x).length);
 return {total:items.length,completed:items.filter(x=>x.status==='completed').length,items,missing_departments};
}
export async function selectInboundReference(env,plan,value,biz){
 const progress=await inboundCodeProgress(env,plan,undefined,biz);const codes=departmentCodes(plan,biz);let code=inboundCode(value);
 if(!codes.length)throw Error('本部门外部入库单号待补充，请办公室在原计划补填后再开始理货 / 외부 입고번호 보완 후 작업을 시작하세요');
 if(!code||code===plan.id||code===plan.display_no){if(codes.length!==1)throw Error('此计划有多个外部入库单号，请扫描或选择本次操作的单号 / 작업할 외부 입고번호를 선택하세요');code=codes[0];}
 if(!codes.includes(code))throw Error('外部单号不属于本部门的理货计划，请确认代发／大货入口 / 해당 부서의 입고번호가 아닙니다');
 if(progress?.items.some(x=>x.external_no===code&&x.status==='completed'))throw Error('此外部入库单已完成，请勿重复开工 / 이미 완료된 입고번호입니다');
 return code;
}
export function planClasses(plan){try{const list=JSON.parse(plan.biz_classes_json||'[]');if(Array.isArray(list)&&list.length)return list;}catch{}return plan.biz_class?[plan.biz_class]:[];}
export function putawayTaskBiz(env,plan,biz){
 if(!inboundFlowEnabled(env)||!plan||plan.source_type==='external_inbound')return biz;
 const task=biz==='bulk'?'bulk_putaway':biz;return putawayClasses.includes(task)&&planClasses(plan).includes(task)?task:'';
}
export async function validateInboundCode(env,classes,value,id=''){
 if(!inboundFlowEnabled(env))return inboundCode(value);
 const codes=inboundCodes(value);
 if(codes.length>50)throw Error('一张计划最多关联50个外部入库单号');
 for(const code of codes){
  if(code.length>120||/[\u0000-\u001f]/.test(code))throw Error('外部入库单号格式无效 / 외부 입고번호 형식 오류');
  const duplicate=await env.DB.prepare("SELECT id,display_no FROM v2_inbound_plans WHERE id!=? AND (instr(char(10)||COALESCE(external_inbound_no,'')||char(10),char(10)||?||char(10))>0 OR display_no=? OR id=?) AND status!='cancelled' AND COALESCE(is_deleted,0)=0 LIMIT 1").bind(id,code,code,code).first();
  if(duplicate)throw Error(code+' 已关联 '+(duplicate.display_no||duplicate.id)+'，请核对 / 이미 연결된 입고번호입니다');
 }
 return codes.join('\n');
}
export async function resolveInboundPlan(env,value,biz){
 const code=inboundCode(value);
 const rows=(await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE (instr(char(10)||COALESCE(external_inbound_no,'')||char(10),char(10)||?||char(10))>0 OR display_no=? OR id=?) AND COALESCE(is_deleted,0)=0 AND status!='cancelled' AND source_type!='return_session' LIMIT 2").bind(code,code,code).all()).results||[];
 const blocked=message=>({ok:true,kind:'status_not_allowed',message});
 if(rows.length>1)return blocked('此单号匹配多张入库计划，请办公室核实后再开始 / 여러 입고계획과 일치합니다. 사무실 확인이 필요합니다');
 if(!rows.length)return blocked('未找到对应入库计划，请客服填写或核对外部系统入库单号 / 입고계획을 찾을 수 없습니다. 외부 입고번호를 확인하세요');
 const plan=rows[0],taskBiz=putawayTaskBiz(env,plan,biz||'direct_ship');
 if(!taskBiz)return blocked('此计划没有本部门的理货上架，请确认代发／大货入口 / 해당 부서의 입고계획이 아닙니다');
 const tasks=(await env.DB.prepare('SELECT * FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').bind(plan.id).all()).results||[];
 if(plan.status==='completed'||tasks.some(x=>x.biz_class===taskBiz&&x.status==='completed'))return {ok:true,kind:'biz_already_completed',plan,message:'该计划已完成入库，请勿重复开工 / 이미 입고 완료된 계획입니다'};
 const progress=await inboundCodeProgress(env,plan,undefined,taskBiz);let selected;
 try{selected=await selectInboundReference(env,plan,code,taskBiz);}catch(error){return progress?.items.some(x=>x.external_no===code&&x.status==='completed')?{ok:true,kind:'biz_already_completed',plan,message:error.message}:blocked(error.message);}
 if(!['unloading','unloading_putting_away','arrived_pending_putaway','putting_away','partially_completed'].includes(plan.status))return blocked('该计划尚未开始卸货，暂不能理货 / 하차 시작 후 입고 작업이 가능합니다');
 return {ok:true,kind:'system',plan:{...plan,biz_classes:planClasses(plan),external_inbound_nos:departmentCodes(plan,taskBiz),selected_external_inbound_no:selected,inbound_progress:progress},biz_classes:planClasses(plan),pending_biz_classes:tasks.filter(x=>x.status!=='completed').map(x=>x.biz_class),completed_biz_classes:tasks.filter(x=>x.status==='completed').map(x=>x.biz_class)};
}
export async function completeUnloadedDispositions(env,planId,t){
 if(!inboundFlowEnabled(env))return;
 const plan=await env.DB.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').bind(planId).first();
 if(!plan||['return_session','external_inbound'].includes(plan.source_type)||['pending','cancelled','completed'].includes(plan.status)||plan.is_deleted)return;
 const active=await env.DB.prepare("SELECT id FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type='unload' AND status IN ('pending','working','awaiting_close') LIMIT 1").bind(planId).first();
 if(active)return;
 const unloaded=await env.DB.prepare("SELECT id,updated_at FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type='unload' AND status='completed' ORDER BY updated_at DESC LIMIT 1").bind(planId).first();
 if(!plan.unload_completed_at&&!unloaded)return;
 const completedAt=plan.unload_completed_at||unloaded.updated_at;
 // No synthetic putaway jobs, output, or labor time: this is only a plan milestone.
 // A bulk crew can perform the putaway part of a mixed plan; that job must not
 // keep its separate cross-dock disposition pending after unloading.
 await env.DB.prepare(`UPDATE v2_inbound_plan_biz_tasks SET status='completed',completed_at=?,completed_by=?,completion_source='unload',completion_note='卸货完成后自动入库',updated_at=?
  WHERE plan_id=? AND biz_class IN ('bulk','return','change_order') AND status!='completed'
  AND NOT EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.related_doc_type='inbound_plan' AND j.related_doc_id=? AND j.job_type=v2_inbound_plan_biz_tasks.job_type AND j.status IN ('pending','working','awaiting_close')
    AND (?=0 OR v2_inbound_plan_biz_tasks.biz_class!='bulk'))`).bind(completedAt,plan.unload_completed_by||'',t,planId,planId,needsPutaway(plan)?1:0).run();
}
export async function bindInboundCode(env,body){
 const plan=await env.DB.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').bind(String(body.id||'')).first();
 if(!plan||plan.is_deleted||['completed','cancelled'].includes(plan.status)||plan.source_type==='return_session')throw Error('此计划不能修改外部入库单号');
 const active=await env.DB.prepare("SELECT id FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close') LIMIT 1").bind(plan.id).first();
 if(active)throw Error('理货任务正在进行，不能更换关联单号 / 진행 중에는 입고번호를 변경할 수 없습니다');
 if(String(body.previous_code??'')!==String(plan.external_inbound_no||''))throw Error('单号已被其他人修改，请刷新 / 새로고침 후 다시 시도하세요');
 const {externalNo:code,bulkExternalNo}=await inboundReferenceData(env,planClasses(plan),body,plan),t=new Date().toISOString();
 if(body.previous_bulk_code!==undefined&&String(body.previous_bulk_code)!==String(plan.bulk_external_inbound_no||''))throw Error('单号归属已变化，请刷新后重试');
 const progress=await inboundCodeProgress(env,plan),nextCodes=inboundCodes(code);
 for(const item of progress?.items||[])if(item.status==='completed'&&(!nextCodes.includes(item.external_no)||(inboundCodes(bulkExternalNo).includes(item.external_no)!==(item.biz_class==='bulk_putaway'))))throw Error('已完成的外部入库单号不能删除、更换或调整部门：'+item.external_no);
 const statements=[];
 // Attribute the one historical completed job before expanding a single-code plan.
 if(progress?.total===1&&progress.completed===1)statements.push(env.DB.prepare("UPDATE v2_ops_jobs SET inbound_external_no=? WHERE id=? AND COALESCE(inbound_external_no,'')='' AND EXISTS(SELECT 1 FROM v2_inbound_plans WHERE id=? AND COALESCE(external_inbound_no,'')=?)").bind(progress.items[0].external_no,progress.items[0].job_id,plan.id,plan.external_inbound_no||''));
 for(const biz of putawayClasses){
  const next=departmentCodes({...plan,external_inbound_no:code,bulk_external_inbound_no:bulkExternalNo},biz);
  if(next.some(x=>!progress?.items.some(i=>i.external_no===x&&i.status==='completed')))statements.push(env.DB.prepare("UPDATE v2_inbound_plan_biz_tasks SET status='pending',completed_at='',completed_by='',updated_at=? WHERE plan_id=? AND biz_class=? AND EXISTS(SELECT 1 FROM v2_inbound_plans WHERE id=? AND COALESCE(external_inbound_no,'')=? AND COALESCE(bulk_external_inbound_no,'')=?) AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close'))").bind(t,plan.id,biz,plan.id,plan.external_inbound_no||'',plan.bulk_external_inbound_no||'',plan.id));
 }
  statements.push(env.DB.prepare("UPDATE v2_inbound_plans SET external_inbound_no=?,bulk_external_inbound_no=?,updated_at=? WHERE id=? AND COALESCE(external_inbound_no,'')=? AND COALESCE(bulk_external_inbound_no,'')=? AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close')) RETURNING id").bind(code,bulkExternalNo,t,plan.id,plan.external_inbound_no||'',plan.bulk_external_inbound_no||'',plan.id));
 const results=await referenceWriteBatch(env,plan.id,statements),updated=results[results.length-1];
  if(!updated.results?.some(row=>row.id===plan.id))throw Error('记录已变化，请刷新后重试');
 return {ok:true,id:plan.id,external_inbound_no:code};
}
