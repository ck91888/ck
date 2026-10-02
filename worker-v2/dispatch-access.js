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
   )))))`,
  args:[user?.role||'',user?.role||'',user?.id||'',badge,badge]
 };
}
