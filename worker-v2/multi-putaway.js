import {resolveInboundEntry} from './independent-inbound.js';
import {stagedPlanStatement,putawayType} from './inbound-stages.js';
import {nativeOwner} from './sop-dispatch.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
export async function preparePutaway(e,p,staff){
 if(p.biz_class!=='direct_ship'||staff.department!=='direct_ship')throw Error('代发上架仅限代发部门 / 출고대행 부서만 적치 가능');
 const input=p.putaway_refs??[{plan_id:p.plan_id,external_inbound_no:p.external_inbound_no}];
 if(!Array.isArray(input)||!input.length||input.length>50)throw Error('请选择 1–50 张已理货入库单 / 검수 완료 입고번호 1–50개 선택');
 const refs=[],seen=new Set();
 for(const item of input){
  const code=typeof item==='string'?item:item?.external_inbound_no||item?.plan_id;
  const found=await resolveInboundEntry(e,code,'direct_ship',putawayType),plan=found.plan;
  if(!['system','independent'].includes(found.kind)||!plan)throw Error(String(code||'')+'：'+(found.message||'入库单不可上架 / 적치 불가'));
  if(item?.plan_id&&item.plan_id!==plan.id)throw Error('单号关联已变化，请重新识别 / 입고번호 연결을 다시 확인하세요');
  const no=plan.selected_external_inbound_no,ref=plan.inbound_progress?.items.find(x=>x.external_no===no),key=JSON.stringify([plan.id,no]);
  if(seen.has(key))continue;
  if(ref?.status!=='tallied_pending_putaway'||!ref.tally_job_id)throw Error(no+'：须已完成理货且未占用、未上架 / 검수 완료·적치 대기 필요');
  refs.push({plan_id:plan.id,external_inbound_no:no,tally_job_id:ref.tally_job_id,display_no:plan.display_no||'',customer:plan.customer||'',source_type:plan.source_type});seen.add(key);
 }
 const id='JOB-'+crypto.randomUUID(),t=new Date().toISOString(),lead=staff.workers.find(x=>x.id===staff.lead_id),first=refs[0];
 const sql=[q(e,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count,inbound_external_no) VALUES(?,'inbound','direct_ship','inbound_direct_putaway','inbound_plan',?,'working',?,?,?,?,?)`,id,first.plan_id,lead.id,t,t,staff.workers.length,first.external_inbound_no)];
 for(const [position,ref] of refs.entries())sql.push(q(e,'INSERT INTO ck_putaway_links(job_id,plan_id,external_no,tally_job_id,position) VALUES(?,?,?,?,?)',id,ref.plan_id,ref.external_inbound_no,ref.tally_job_id,position));
 return {id,t,sql,jobType:putawayType,sourceType:'inbound_plan',sourceId:first.plan_id,after:[...new Set(refs.map(x=>x.plan_id))].map(pid=>stagedPlanStatement(e,pid,t)),extra:{plan_id:first.plan_id,external_inbound_no:first.external_inbound_no,putaway_refs:refs}};
}
export async function putawayReferences(e,id){
 return (await q(e,`SELECT l.plan_id,l.external_no AS external_inbound_no,l.tally_job_id,p.display_no,p.customer,p.source_type,p.status FROM ck_putaway_links l JOIN v2_inbound_plans p ON p.id=l.plan_id WHERE l.job_id=? ORDER BY l.position`,id).all()).results||[];
}
// Cancel only this assignment, preserving tally evidence, original clocks and membership history.
export async function cancelPutaway(e,b){
 if(e.SOP_ENVIRONMENT!=='staging'||e.SOP_UPGRADE_ENABLED!=='true'||!await nativeOwner(b,e))throw Error('请由本任务派审员取消 / 담당자가 취소하세요');
 const request=String(b.client_req_id||''),reason=String(b.reason||'').trim();
 if(!request||request.length>100||!reason||reason.length>500)throw Error('请填写取消原因 / 취소 사유를 입력하세요');
 const old=await q(e,'SELECT * FROM sop_events WHERE request_id=?',request).first();
 if(old){if(old.action!=='sop_putaway_cancel'||old.record_id!==b.job_id||old.actor_id!==e.SOP_REQUEST_USER.id||JSON.parse(old.after_json).cancel_reason!==reason)throw Error('请求已被使用');return JSON.parse(old.result_json);}
 const row=await q(e,"SELECT * FROM sop_records WHERE id=? AND kind='dispatch'",b.job_id).first(),job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',b.job_id).first();
 if(!row||job?.job_type!==putawayType||!['pending','working','awaiting_close','paused'].includes(job.status)||row.revision!==Number(b.revision))throw Error('任务状态已变化，请刷新 / 작업 상태 변경');
 const refs=await putawayReferences(e,job.id),ids=[...new Set(refs.length?refs.map(x=>x.plan_id):[job.related_doc_id])],t=new Date().toISOString(),after={...JSON.parse(row.state),status:'cancelled',cancel_reason:reason,cancelled_at:t},result={ok:true,job_id:job.id,status:'cancelled',revision:row.revision+1};
 // Event expected revision is enforced by the existing SOP event trigger; the
 // job predicate also closes the finish/cancel race before any clock is changed.
 await q(e,`CREATE TRIGGER IF NOT EXISTS ck_putaway_cancel_guard BEFORE INSERT ON sop_events WHEN NEW.action='sop_putaway_cancel' AND NOT EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.record_id AND job_type='inbound_direct_putaway' AND status IN ('pending','working','awaiting_close','paused')) BEGIN SELECT RAISE(ABORT,'上架任务已结束 / 적치 작업 종료'); END`).run();
 await e.DB.batch([
  q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request,job.id,row.revision,'sop_putaway_cancel',e.SOP_REQUEST_USER.id,e.SOP_REQUEST_USER.name,row.state,JSON.stringify(after),JSON.stringify(result),t),
  q(e,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(after),t,job.id),
  q(e,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='putaway_cancelled' WHERE job_id=? AND left_at=''",t,t,job.id),
  q(e,"UPDATE v2_ops_jobs SET status='cancelled',active_worker_count=0,updated_at=? WHERE id=?",t,job.id),
  ...ids.map(pid=>stagedPlanStatement(e,pid,t))
 ]);return result;
}
