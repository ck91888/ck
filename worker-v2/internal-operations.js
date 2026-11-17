import {isInternal,readOperation,validateOperationFinish} from '../shared/internal-operations.js';
export const internalEnabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true';
export async function protectInternalMutation(body,env){
 if(!internalEnabled(env)||!body.job_id||!(/_(finish|finalize)$/.test(body.action)||body.action==='v2_ops_job_result_update'))return;
 const job=await env.DB.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').bind(body.job_id).first();
 if(!isInternal(job?.job_type))return;
 const finishAction=job.job_type==='inbound_return'?'v2_inbound_job_finish':'v2_ops_job_finish';
 if(body.action!==finishAction&&readOperation(job))throw Error('请从库内原任务按规定时点填写并完成；开始记录不可补改 / 원래 창고 작업에서 완료하세요');
 if(body.action===finishAction){
  if(body.leave_only)return;
  if(readOperation(job)&&job.job_type==='inbound_return'&&body.complete_job!==true)throw Error('请通过完成退件入库按钮提交本批次结果 / 반품 입고 완료로 제출하세요');
  if(job.status==='cancelled'||job.status==='paused')throw Error('任务已取消或暂停，请先核对原任务 / 작업 상태를 확인하세요');
  if(job.status!=='completed')validateOperationFinish(job,body);
 }
}
