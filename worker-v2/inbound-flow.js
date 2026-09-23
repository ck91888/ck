// Inbound dispositions use the existing storage keys for compatibility.
// They describe the goods' path, not the department providing labor.
export const inboundLabels={direct_ship:'理货上架',bulk:'直进直出',return:'整托退回',change_order:'换单'};
export const inboundFlowEnabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true';
export const inboundCode=value=>String(value??'').normalize('NFKC').trim();
export const inboundReferenceValue=(body,fallback='')=>body.external_inbound_nos??body.external_inbound_no??fallback;
export function inboundCodes(value){return [...new Set((Array.isArray(value)?value:[value]).flatMap(x=>inboundCode(x).split(/[\n\r,;，；\t]+/)).map(inboundCode).filter(Boolean))];}
export async function inboundCodeProgress(env,plan,knownJobs){
 if(!inboundFlowEnabled(env)||!plan||!planClasses(plan).includes('direct_ship'))return null;
 const codes=inboundCodes(plan.external_inbound_no);
 const jobs=knownJobs ? knownJobs.filter(j=>['inbound_direct','inbound_bulk'].includes(j.job_type)&&j.status!=='cancelled') : (await env.DB.prepare("SELECT id,job_type,status,inbound_external_no,created_at,updated_at FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type IN ('inbound_direct','inbound_bulk') AND status!='cancelled' ORDER BY created_at DESC").bind(plan.id).all()).results||[];
 const items=codes.map(code=>{
  const matches=jobs.filter(j=>j.inbound_external_no===code||(!j.inbound_external_no&&codes.length===1));
  const job=matches.find(j=>j.status==='completed')||matches.find(j=>['pending','working','awaiting_close'].includes(j.status));
  const historicalComplete=codes.length===1&&plan.status==='completed';
  return {external_no:code,status:job?.status==='completed'||historicalComplete?'completed':job?'working':'pending',job_id:job?.id||'',completed_at:job?.status==='completed'?job.updated_at:historicalComplete?(plan.manual_completed_at||plan.updated_at||''):''};
 });
 return {total:items.length,completed:items.filter(x=>x.status==='completed').length,items};
}
export async function selectInboundReference(env,plan,value){
 const progress=await inboundCodeProgress(env,plan);const codes=inboundCodes(plan.external_inbound_no);let code=inboundCode(value);
 if(!code||code===plan.id||code===plan.display_no){if(codes.length!==1)throw Error('此计划有多个外部入库单号，请扫描或选择本次操作的单号 / 작업할 외부 입고번호를 선택하세요');code=codes[0];}
 if(!codes.includes(code))throw Error('外部单号与所选计划不一致 / 입고번호가 계획과 일치하지 않습니다');
 if(progress?.items.some(x=>x.external_no===code&&x.status==='completed'))throw Error('此外部入库单已完成，请勿重复开工 / 이미 완료된 입고번호입니다');
 return code;
}
export function planClasses(plan){try{const list=JSON.parse(plan.biz_classes_json||'[]');if(Array.isArray(list)&&list.length)return list;}catch{}return plan.biz_class?[plan.biz_class]:[];}
export function putawayTaskBiz(env,plan,biz){
 if(!inboundFlowEnabled(env)||!plan||plan.source_type==='external_inbound')return biz;
 return ['direct_ship','bulk'].includes(biz)&&planClasses(plan).includes('direct_ship')?'direct_ship':'';
}
export async function validateInboundCode(env,classes,value,id=''){
 if(!inboundFlowEnabled(env))return inboundCode(value);
 const codes=inboundCodes(value);
 if(classes.includes('direct_ship')&&!codes.length)throw Error('理货上架必须填写外部系统入库单号 / 검수·적치는 외부 입고번호가 필요합니다');
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
 if(!taskBiz)return blocked('此计划无需理货上架，卸货完成后自动入库 / 하차 완료 시 자동 입고 처리됩니다');
 const tasks=(await env.DB.prepare('SELECT * FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').bind(plan.id).all()).results||[];
 if(plan.status==='completed'||tasks.some(x=>x.biz_class===taskBiz&&x.status==='completed'))return {ok:true,kind:'biz_already_completed',plan,message:'该计划已完成入库，请勿重复开工 / 이미 입고 완료된 계획입니다'};
 const progress=await inboundCodeProgress(env,plan);let selected;
 try{selected=await selectInboundReference(env,plan,code);}catch(error){return progress?.items.some(x=>x.external_no===code&&x.status==='completed')?{ok:true,kind:'biz_already_completed',plan,message:error.message}:blocked(error.message);}
 if(!['unloading','unloading_putting_away','arrived_pending_putaway','putting_away','partially_completed'].includes(plan.status))return blocked('该计划尚未开始卸货，暂不能理货 / 하차 시작 후 입고 작업이 가능합니다');
 return {ok:true,kind:'system',plan:{...plan,biz_classes:planClasses(plan),external_inbound_nos:inboundCodes(plan.external_inbound_no),selected_external_inbound_no:selected,inbound_progress:progress},biz_classes:planClasses(plan),pending_biz_classes:tasks.filter(x=>x.status!=='completed').map(x=>x.biz_class),completed_biz_classes:tasks.filter(x=>x.status==='completed').map(x=>x.biz_class)};
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
    AND (?=0 OR v2_inbound_plan_biz_tasks.biz_class!='bulk'))`).bind(completedAt,plan.unload_completed_by||'',t,planId,planId,planClasses(plan).includes('direct_ship')?1:0).run();
}
export async function bindInboundCode(env,body){
 const plan=await env.DB.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').bind(String(body.id||'')).first();
 if(!plan||plan.is_deleted||['completed','cancelled'].includes(plan.status)||plan.source_type==='return_session')throw Error('此计划不能修改外部入库单号');
 const active=await env.DB.prepare("SELECT id FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close') LIMIT 1").bind(plan.id).first();
 if(active)throw Error('理货任务正在进行，不能更换关联单号 / 진행 중에는 입고번호를 변경할 수 없습니다');
 if(String(body.previous_code??'')!==String(plan.external_inbound_no||''))throw Error('单号已被其他人修改，请刷新 / 새로고침 후 다시 시도하세요');
 const code=await validateInboundCode(env,planClasses(plan),inboundReferenceValue(body),plan.id),t=new Date().toISOString();
 const progress=await inboundCodeProgress(env,plan),nextCodes=inboundCodes(code);
 for(const item of progress?.items||[])if(item.status==='completed'&&!nextCodes.includes(item.external_no))throw Error('已完成的外部入库单号不能删除或更换：'+item.external_no);
 const statements=[];
 // Attribute the one historical completed job before expanding a single-code plan.
 if(progress?.total===1&&progress.completed===1)statements.push(env.DB.prepare("UPDATE v2_ops_jobs SET inbound_external_no=? WHERE id=? AND COALESCE(inbound_external_no,'')='' AND EXISTS(SELECT 1 FROM v2_inbound_plans WHERE id=? AND COALESCE(external_inbound_no,'')=?)").bind(progress.items[0].external_no,progress.items[0].job_id,plan.id,plan.external_inbound_no||''));
 if(progress?.completed&&nextCodes.some(x=>!progress.items.some(i=>i.external_no===x&&i.status==='completed')))statements.push(env.DB.prepare("UPDATE v2_inbound_plan_biz_tasks SET status='pending',completed_at='',completed_by='',updated_at=? WHERE plan_id=? AND biz_class='direct_ship' AND EXISTS(SELECT 1 FROM v2_inbound_plans WHERE id=? AND COALESCE(external_inbound_no,'')=?) AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close'))").bind(t,plan.id,plan.id,plan.external_inbound_no||'',plan.id));
 statements.push(env.DB.prepare("UPDATE v2_inbound_plans SET external_inbound_no=?,updated_at=? WHERE id=? AND COALESCE(external_inbound_no,'')=? AND NOT EXISTS(SELECT 1 FROM v2_inbound_plan_jobs WHERE related_doc_type='inbound_plan' AND plan_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close'))").bind(code,t,plan.id,plan.external_inbound_no||'',plan.id));
 const results=await env.DB.batch(statements),updated=results[results.length-1];
 if(updated.meta?.changes!==1)throw Error('记录已变化，请刷新后重试');
 return {ok:true,id:plan.id,external_inbound_no:code};
}
