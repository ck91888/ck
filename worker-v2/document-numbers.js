import {ensureSchema} from './schema-ready.js';
// Display numbers are separate from immutable IDs, revisions and scan payloads.
const prefix=alias=>`CASE ${alias}.kind WHEN 'need' THEN 'ZY' WHEN 'task' THEN 'RW' WHEN 'dispatch' THEN 'RW' WHEN 'check' THEN 'HD' WHEN 'issue' THEN 'WT' END`;
const day=alias=>`COALESCE(strftime('%Y%m%d',json_extract(${alias}.state,'$.created_at'),'+9 hours'),strftime('%Y%m%d',${alias}.updated_at,'+9 hours'),'19700101')`;
export const NUMBER_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS sop_document_numbers(record_id TEXT PRIMARY KEY,prefix TEXT NOT NULL,day TEXT NOT NULL,sequence INTEGER NOT NULL,display_no TEXT NOT NULL UNIQUE,UNIQUE(prefix,day,sequence))`,
 `CREATE TRIGGER IF NOT EXISTS sop_document_number_insert AFTER INSERT ON sop_records
 WHEN NEW.kind IN ('need','task','dispatch','check','issue') AND NOT EXISTS(SELECT 1 FROM sop_document_numbers WHERE record_id=NEW.id)
 BEGIN
 INSERT INTO sop_document_numbers(record_id,prefix,day,sequence,display_no)
 SELECT NEW.id,${prefix('NEW')},${day('NEW')},COALESCE(MAX(sequence),0)+1,
 ${prefix('NEW')}||'-'||${day('NEW')}||'-'||printf('%03d',COALESCE(MAX(sequence),0)+1)
 FROM sop_document_numbers WHERE prefix=${prefix('NEW')} AND day=${day('NEW')};
 END`,
 // One set-based backfill; reruns preserve every assigned number, including deleted records.
 `INSERT OR IGNORE INTO sop_document_numbers(record_id,prefix,day,sequence,display_no)
 SELECT id,prefix,day,base+position,prefix||'-'||day||'-'||printf('%03d',base+position) FROM (
 SELECT pending.*,COALESCE((SELECT MAX(n.sequence) FROM sop_document_numbers n WHERE n.prefix=pending.prefix AND n.day=pending.day),0) AS base,
 ROW_NUMBER() OVER(PARTITION BY prefix,day ORDER BY created_at,id) AS position FROM (
 SELECT s.id,${prefix('s')} AS prefix,${day('s')} AS day,COALESCE(json_extract(s.state,'$.created_at'),s.updated_at) AS created_at
 FROM sop_records s WHERE s.kind IN ('need','task','dispatch','check','issue') AND NOT EXISTS(SELECT 1 FROM sop_document_numbers n WHERE n.record_id=s.id)
 ) pending)`
];
export async function ensureDocumentNumbers(env){
 if(env.SOP_UPGRADE_ENABLED!=='true')return;
 await ensureSchema(env.DB,'document-numbers-v1',()=>env.DB.batch(NUMBER_SCHEMA.map(sql=>env.DB.prepare(sql))));
}
export async function recordNumbers(env,items){
 if(!items.length||env.SOP_UPGRADE_ENABLED!=='true')return items;
 const rows=(await env.DB.prepare(`SELECT s.id,n.display_no,wn.display_no AS work_plan_no,
 COALESCE(ib.display_no,ob.display_no,'') AS source_display_no,
 COALESCE(json_extract(w.state,'$.source_type'),json_extract(s.state,'$.source_type'),'') AS source_type
 FROM sop_records s LEFT JOIN sop_document_numbers n ON n.record_id=s.id
 LEFT JOIN sop_records w ON w.id=CASE WHEN s.kind='need' THEN s.id ELSE json_extract(s.state,'$.need_id') END
 LEFT JOIN sop_document_numbers wn ON wn.record_id=w.id
 LEFT JOIN v2_inbound_plans ib ON json_extract(w.state,'$.source_type')='inbound' AND ib.id=json_extract(w.state,'$.source_id')
 LEFT JOIN v2_outbound_orders ob ON json_extract(w.state,'$.source_type')='outbound' AND ob.id=json_extract(w.state,'$.source_id')
 WHERE s.id IN (SELECT value FROM json_each(?))`).bind(JSON.stringify(items.map(x=>x.id))).all()).results;
 const byId=new Map(rows.map(r=>[r.id,r]));
 return items.map(x=>{const n=byId.get(x.id);return n?{...x,display_no:n.display_no||'',work_plan_no:n.work_plan_no||'',source_display_no:n.source_display_no||'',source_type:x.source_type||n.source_type}:x;});
}
export async function resolveRecordId(env,code){
 if(!/^(?:ZY|RW|HD|WT)-\d{8}-\d{3,}$/i.test(code))return code;
 const r=await env.DB.prepare('SELECT record_id FROM sop_document_numbers WHERE display_no=?').bind(code.toUpperCase()).first();
 return r?.record_id||code;
}
// Batch business references for task lists, details, live boards and exports.
const tripNumbers=`(SELECT group_concat(display_no,'、') FROM (SELECT p.display_no FROM ck_unload_plan_links l JOIN v2_inbound_plans p ON p.id=l.plan_id WHERE l.job_id=j.id ORDER BY l.position))`;
export const JOB_NUMBER_SQL=`SELECT j.id,CASE WHEN j.job_type='pick_direct' THEN j.display_no ELSE '' END AS trip_no,
 n.display_no AS work_plan_no,COALESCE(${tripNumbers},ib.display_no) AS inbound_plan_no,ob.display_no AS outbound_plan_no,
 CASE WHEN n.display_no IS NOT NULL THEN n.display_no
 WHEN j.related_doc_type='work_order' THEN j.related_doc_id
 WHEN j.job_type='pick_direct' THEN COALESCE((SELECT group_concat(pick_doc_no,'、') FROM v2_ops_job_pick_docs WHERE job_id=j.id),j.display_no)
 ELSE COALESCE(${tripNumbers},NULLIF(j.inbound_external_no,''),NULLIF(ib.display_no,''),NULLIF(ob.display_no,''),NULLIF(fb.display_no,''),NULLIF(it.related_doc_no,''),NULLIF(vb.batch_no,''),NULLIF(j.display_no,''),tn.display_no,'') END AS business_no
 FROM v2_ops_jobs j
 LEFT JOIN sop_document_numbers tn ON tn.record_id=j.id
 LEFT JOIN sop_records s ON s.id=j.id AND s.kind='task'
 LEFT JOIN sop_document_numbers n ON n.record_id=COALESCE(json_extract(s.state,'$.need_id'),CASE WHEN j.related_doc_type='sop_need' THEN j.related_doc_id END)
 LEFT JOIN v2_inbound_plans ib ON j.related_doc_type='inbound_plan' AND ib.id=j.related_doc_id
 LEFT JOIN v2_outbound_orders ob ON ob.id=CASE WHEN j.linked_outbound_order_id!='' THEN j.linked_outbound_order_id WHEN j.related_doc_type IN ('outbound','outbound_order') THEN j.related_doc_id END
 LEFT JOIN v2_field_feedbacks fb ON j.related_doc_type='field_feedback' AND fb.id=j.related_doc_id
 LEFT JOIN v2_issue_tickets it ON j.related_doc_type='issue_ticket' AND it.id=j.related_doc_id
 LEFT JOIN v2_verify_batches vb ON j.related_doc_type='verify_batch' AND vb.id=j.related_doc_id`;
export async function jobNumbers(env,items,key='id'){
 if(!items.length||env.SOP_UPGRADE_ENABLED!=='true')return items;
 const rows=(await env.DB.prepare(JOB_NUMBER_SQL+' WHERE j.id IN (SELECT value FROM json_each(?))').bind(JSON.stringify([...new Set(items.map(x=>x[key]))])).all()).results;
 const byId=new Map(rows.map(r=>[r.id,r]));
 return items.map(x=>{const n=byId.get(x[key]);return n?{...x,business_no:n.business_no||'',display_no:n.business_no||x.display_no||'',trip_no:n.trip_no||'',work_plan_no:n.work_plan_no||'',inbound_plan_no:n.inbound_plan_no||'',outbound_plan_no:n.outbound_plan_no||''}:x;});
}
