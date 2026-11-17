// Creation identity belongs to the work plan itself, never its source or editor.
const name=value=>{const s=String(value||'').trim();return /^(管理员|系统管理员|协同管理员|出库计划自动生成|admin(?:istrator)?|system|office)$/i.test(s)?'':s;};
export async function workNeedCreators(env,records){
 const needs=records.filter(r=>r.kind==='need');if(!needs.length)return records;
 const events=(await env.DB.prepare("SELECT record_id,actor_name FROM sop_events WHERE expected_revision=0 AND action IN ('sop_need_create','sop_need_from_outbound','sop_need_auto') AND record_id IN (SELECT value FROM json_each(?)) ORDER BY created_at,request_id").bind(JSON.stringify(needs.map(r=>r.id))).all()).results;
 const creators=new Map();for(const e of events)if(!creators.has(e.record_id))creators.set(e.record_id,name(e.actor_name));
 for(const r of needs)r.creator_name=creators.get(r.id)||name(r.created_by);
 return records;
}
