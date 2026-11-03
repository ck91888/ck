import {ensureSchema} from './schema-ready.js';
import {dispatchAccess} from './dispatch-access.js';
import {notedShareGrant} from './dispatch-share.js';

// Server-owned evidence only: request fields cannot supply or replace a grant.
const grants=new WeakMap();
export const completionConflict=/ck_native_completion_checks|completion_authorization_changed/;
export const completionChanged='任务归属、人员或权限已变化，请刷新后重新确认完成 / 작업 담당·권한이 변경되었습니다';
export function noteCompletionAuthorization(env,body,record,job){
 if(body.leave_only||!/_(finish|finalize)$/.test(body.action)||grants.has(body))return;
 if(!record||!env.SOP_REQUEST_USER)return;
 const actor={...env.SOP_REQUEST_USER};
 grants.set(body,{id:record.id,revision:record.revision,status:job.status,jobType:job.job_type,actor,shared:notedShareGrant(body)});
}
export async function nativeCompletionGuard(env,body){
 const grant=grants.get(body);
 // Legacy jobs without a staged dispatch retain their existing completion rules.
 if(!grant)return [];
 if(grant.id!==body.job_id||grant.actor.id!==env.SOP_REQUEST_USER?.id)throw Error('completion_authorization_changed');
 await ensureSchema(env.DB,'native-completion-checks-v1',()=>env.DB.prepare(`CREATE TABLE IF NOT EXISTS ck_native_completion_checks(
  id TEXT PRIMARY KEY,allowed INTEGER NOT NULL CONSTRAINT ck_native_completion_checks CHECK(allowed=1))`).run());
 const access=dispatchAccess(grant.actor),id=crypto.randomUUID(),day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);
 // Bulk completion already claims a stable result ID and reconciles retries.
 // Manual finalization has no stable result claim, so also compare its status.
 const manual=body.action==='v2_ops_job_manual_finalize';
 const shared=grant.shared?` OR EXISTS(SELECT 1 FROM ck_dispatch_share_pairs p WHERE p.status='active' AND p.day=? AND (
  (p.person_lo=? AND p.person_hi=json_extract(s.state,'$.owner_id')) OR
  (p.person_hi=? AND p.person_lo=json_extract(s.state,'$.owner_id'))))`:'';
 return [env.DB.prepare(`INSERT INTO ck_native_completion_checks(id,allowed)
  SELECT ?,CASE WHEN EXISTS(SELECT 1 FROM sop_records s WHERE s.id=? AND s.kind='dispatch'
   AND s.revision=? AND (${access.sql}${shared})
   AND EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=s.id AND j.job_type=?${manual?' AND j.status=?':''})) THEN 1 ELSE 0 END`)
  .bind(id,grant.id,grant.revision,...access.args,...(grant.shared?[day,grant.actor.id,grant.actor.id]:[]),grant.jobType,...(manual?[grant.status]:[])),
  env.DB.prepare('DELETE FROM ck_native_completion_checks WHERE id=?').bind(id)];
}
