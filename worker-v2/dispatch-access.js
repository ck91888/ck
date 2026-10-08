// Used by both task discovery and mutation authorization. `s` is a dispatch
// record alias; membership comes from live work segments, never request fields
// or the historical crew snapshot. Only verified field sessions carry a badge.
export function dispatchAccess(user) {
 const badge=user?.scope==='field'?user.badge||'':'';
 return {
  sql:`(?='manager' OR (?='dispatcher' AND (
   json_extract(s.state,'$.owner_id')=? OR (?<>'' AND EXISTS(
    SELECT 1 FROM v2_ops_job_workers w
    WHERE w.job_id=s.id AND w.worker_id=? AND w.left_at=''
   )) OR (?<>'' AND json_extract(s.state,'$.pause_actor_id')=? AND EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=s.id AND j.status='paused') AND EXISTS(SELECT 1 FROM json_each(s.state,'$.workers') w WHERE json_extract(w.value,'$.id')=?) AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.worker_id=? AND w.left_at='' AND w.job_id!=s.id)))))`,
  args:[user?.role||'',user?.role||'',user?.id||'',badge,badge,badge,user?.id||'',badge,badge]
 };
}

// A resting member may view their own still-assigned task, not manage its crew.
// Only the verified session's current attendance break confers this narrow right.
export function restingDispatchAccess(user){
 if(user?.scope!=='field'||!user.badge||!user.attendance_id)return {sql:'0',args:[]};
 return {sql:`(EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=s.id AND j.status IN ('working','awaiting_close','paused'))
 AND EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_breaks b ON b.attendance_id=d.id WHERE d.id=? AND d.worker_id=? AND d.signed_out='' AND b.ended_at='' AND EXISTS(SELECT 1 FROM json_each(b.job_ids_json) x WHERE x.value=s.id))
 AND EXISTS(SELECT 1 FROM json_each(s.state,'$.workers') w WHERE json_extract(w.value,'$.id')=?)
 AND NOT EXISTS(SELECT 1 FROM json_each(s.state,'$.borrowed_worker_ids') w WHERE w.value=?)
 AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.worker_id=? AND w.left_at='' AND w.job_id!=s.id))`,args:[user.attendance_id,user.badge,user.badge,user.badge,user.badge]};
}
export async function restingDispatch(env,id){
 const a=restingDispatchAccess(env.SOP_REQUEST_USER);if(!a.args.length)return false;
 return !!await env.DB.prepare("SELECT s.id FROM sop_records s WHERE s.kind='dispatch' AND s.id=? AND "+a.sql).bind(id,...a.args).first();
}
