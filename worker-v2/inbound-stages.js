// New direct-ship jobs are separate stages. Historical inbound_direct jobs keep
// their combined completion contract; no existing results or clocks are rewritten.
export const tallyType='inbound_direct_tally';
export const putawayType='inbound_direct_putaway';
export const stagedInbound=type=>[tallyType,putawayType].includes(type);
export function putawayMetrics(body){
 const count=body.location_count;
 if(count===undefined||count===null||typeof count==='boolean'||String(count).trim()===''||!Number.isSafeInteger(Number(count))||Number(count)<0||Number(count)>1e8)throw Error('货位数必填，请填写 0–100000000 的整数 / 적치 위치 수를 입력하세요');
 if(typeof body.used_forklift!=='boolean')throw Error('请选择是否使用叉车：是或否 / 지게차 사용 여부를 선택하세요');
 return {location_count:Number(count),used_forklift:body.used_forklift};
}
export function stagedPlanStatement(env,id,t,gate='1',args=[]){
 // Evaluate committed jobs inside the same transaction. Arrival/unload states
 // and terminal plans retain their established gates and historical meaning.
 return env.DB.prepare(`UPDATE v2_inbound_plans SET status=CASE
  WHEN EXISTS(SELECT 1 FROM v2_inbound_references j WHERE j.plan_id=v2_inbound_plans.id AND j.job_type='inbound_direct_putaway' AND j.status IN ('pending','working','awaiting_close','paused')) THEN 'direct_putting_away'
  WHEN EXISTS(SELECT 1 FROM v2_inbound_references j WHERE j.plan_id=v2_inbound_plans.id AND j.job_type='inbound_direct_tally' AND j.status IN ('pending','working','awaiting_close','paused')) THEN 'putting_away'
  WHEN EXISTS(SELECT 1 FROM v2_inbound_references j WHERE j.plan_id=v2_inbound_plans.id AND j.job_type='inbound_direct_tally' AND j.status='completed' AND NOT EXISTS(SELECT 1 FROM v2_inbound_references p WHERE p.plan_id=j.plan_id AND p.inbound_external_no=j.inbound_external_no AND p.job_type='inbound_direct_putaway' AND p.status='completed')) THEN 'tallied_pending_putaway'
  ELSE status END,updated_at=? WHERE id=? AND status IN ('arrived_pending_putaway','putting_away','tallied_pending_putaway','direct_putting_away','partially_completed') AND ${gate}`).bind(t,id,...args);
}
export async function validateInboundStage(env,plan,code,type){
 if(!stagedInbound(type))return;
 const jobs=(await env.DB.prepare("SELECT id,job_type,status FROM v2_inbound_references WHERE related_doc_type='inbound_plan' AND plan_id=? AND (inbound_external_no=? OR COALESCE(inbound_external_no,'')='') AND status!='cancelled'").bind(plan.id,code).all()).results||[];
 if(jobs.some(j=>j.job_type==='inbound_direct'))throw Error('此单已有旧版理货上架任务，请继续原任务 / 기존 검수·적치 작업을 계속하세요');
 const closed=jobs.find(j=>j.status==='system_closed');
 if(closed)throw Error('已有系统04:00结束的原任务，请从现场首页打开原任务，核对人员后恢复 / 04:00 종료된 기존 작업을 열어 인원 확인 후 재개하세요');
 const tally=jobs.find(j=>j.job_type===tallyType&&j.status==='completed');
 if(type===tallyType&&tally)throw Error('本单理货已完成，请从上架入口派工 / 검수 완료, 적치를 시작하세요');
 if(type===putawayType&&!tally)throw Error('本单理货尚未完成，不能开始上架 / 검수 완료 후 적치를 시작하세요');
 if(jobs.some(j=>j.job_type===type))throw Error('此单已有原任务，请打开原任务核对状态与人员 / 기존 작업의 상태와 인원을 확인하세요');
}
