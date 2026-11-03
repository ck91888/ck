import {ensureSchema} from './schema-ready.js';
import {dispatchAccess} from './dispatch-access.js';
import {notedShareGrant,shareGrantFor} from './dispatch-share.js';

// Server-owned evidence only: request fields cannot supply or replace a grant.
const grants=new WeakMap();
export const completionConflict=/ck_native_completion_checks|completion_authorization_changed/;
export const completionChanged='任务归属、人员或权限已变化，请刷新后重新确认完成 / 작업 담당·권한이 변경되었습니다';
export function noteCompletionAuthorization(env,body,record,job){
 if(body.leave_only||!/_(finish|finalize)$/.test(body.action)||grants.has(body))return;
 if(!record||!env.SOP_REQUEST_USER)return;
 const actor={...env.SOP_REQUEST_USER};
 let owner=record.data?.owner_id;try{owner??=JSON.parse(record.state||'{}').owner_id;}catch{}
 grants.set(body,{id:record.id,revision:record.revision,status:job.status,jobType:job.job_type,actor,shared:notedShareGrant(body),pairId:shareGrantFor(env,actor.id,owner)?.id||'',office:env.SOP_OPERATION_CONTEXT?{...env.SOP_OPERATION_CONTEXT}:null});
}
export async function nativeCompletionGuard(env,body,{replay=false}={}){
 const grant=grants.get(body),actor=grant?.actor||env.SOP_REQUEST_USER,context=grant?.office||env.SOP_OPERATION_CONTEXT;
 const office=actor?.scope==='office'&&context?.session_hash;
 // Non-SOP legacy jobs keep their original ownership policy. A real office
 // session still has to be current, including when reading a committed replay.
 if(!grant&&!office)return [];
 if(grant&&(grant.id!==body.job_id||grant.actor.id!==env.SOP_REQUEST_USER?.id))throw Error('completion_authorization_changed');
 await ensureSchema(env.DB,'native-completion-checks-v1',()=>env.DB.prepare(`CREATE TABLE IF NOT EXISTS ck_native_completion_checks(
  id TEXT PRIMARY KEY,allowed INTEGER NOT NULL CONSTRAINT ck_native_completion_checks CHECK(allowed=1))`).run());
 const id=crypto.randomUUID(),conditions=[],args=[id];
 if(grant){
  const access=dispatchAccess(grant.actor),day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);
  // Stable result claims reconcile bulk retries; manual completion additionally
  // compares its original status, except while verifying a committed replay.
  const manual=body.action==='v2_ops_job_manual_finalize'&&!replay;
  const shared=grant.shared?` OR EXISTS(SELECT 1 FROM ck_dispatch_share_pairs p WHERE p.id=? AND p.status='active' AND p.day=? AND (
   (p.person_lo=? AND p.person_hi=json_extract(s.state,'$.owner_id')) OR
   (p.person_hi=? AND p.person_lo=json_extract(s.state,'$.owner_id'))))`:'';
  conditions.push(`EXISTS(SELECT 1 FROM sop_records s WHERE s.id=? AND s.kind='dispatch' AND s.revision=?
   AND (${access.sql}${shared}) AND EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=s.id AND j.job_type=?${manual?' AND j.status=?':''}))`);
  args.push(grant.id,grant.revision,...access.args,...(grant.shared?[grant.pairId,day,grant.actor.id,grant.actor.id]:[]),grant.jobType,...(manual?[grant.status]:[]));
 }
 if(office){
  conditions.push(`EXISTS(SELECT 1 FROM ck_access_sessions a JOIN ck_office_operation_context o ON o.session_hash=a.token_hash
   WHERE a.token_hash=? AND a.scope='office' AND a.user_id=? AND a.role=? AND a.expires_at>?
   AND o.context_id=? AND o.operator_name=?)`);
  args.push(context.session_hash,actor.id,actor.role,Date.now(),context.context_id,context.operator_name);
 }
 return [env.DB.prepare(`INSERT INTO ck_native_completion_checks(id,allowed) SELECT ?,CASE WHEN ${conditions.join(' AND ')} THEN 1 ELSE 0 END`).bind(...args),
  env.DB.prepare('DELETE FROM ck_native_completion_checks WHERE id=?').bind(id)];
}
