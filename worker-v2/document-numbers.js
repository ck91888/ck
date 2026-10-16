import {ensureSchema} from './schema-ready.js';
import {ensureLoadSchema,loadTripEnabled} from './outbound-load-trip.js';
import {jobDefinitions,departments,laborDepartment} from '../shared/labor-department.js';
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
 COALESCE(ib.status,ob.status,'') AS source_status,
 CASE WHEN ib.status='cancelled' OR COALESCE(ib.is_deleted,0)=1 OR ob.status='cancelled' THEN 1 ELSE 0 END AS source_cancelled,
 COALESCE(json_extract(w.state,'$.source_type'),json_extract(s.state,'$.source_type'),'') AS source_type
 FROM sop_records s LEFT JOIN sop_document_numbers n ON n.record_id=s.id
 LEFT JOIN sop_records w ON w.id=CASE WHEN s.kind='need' THEN s.id ELSE json_extract(s.state,'$.need_id') END
 LEFT JOIN sop_document_numbers wn ON wn.record_id=w.id
 LEFT JOIN v2_inbound_plans ib ON json_extract(w.state,'$.source_type')='inbound' AND ib.id=json_extract(w.state,'$.source_id')
 LEFT JOIN v2_outbound_orders ob ON json_extract(w.state,'$.source_type')='outbound' AND ob.id=json_extract(w.state,'$.source_id')
 WHERE s.id IN (SELECT value FROM json_each(?))`).bind(JSON.stringify(items.map(x=>x.id))).all()).results;
 const byId=new Map(rows.map(r=>[r.id,r]));
 const numbered=items.map(x=>{const n=byId.get(x.id);return n?{...x,display_no:n.display_no||'',work_plan_no:n.work_plan_no||'',source_display_no:n.source_display_no||'',source_type:x.source_type||n.source_type,source_status:n.source_status,source_cancelled:!!n.source_cancelled,...(x.kind==='need'&&n.source_cancelled&&!x.result?{status:'cancelled'}:{})}:x;});
 const jobs=await jobNumbers(env,numbered.filter(x=>['task','dispatch'].includes(x.kind)));
 const jobsById=new Map(jobs.map(x=>[x.id,x]));
 // A SOP record keeps its public RW number and scan identity. Its job label uses
 // the linked ZY/business document, independently of that record number.
 return numbered.map(x=>{const job=jobsById.get(x.id);return job?{...job,display_no:x.display_no,source_type:x.source_type}:x;});
}
export async function resolveRecordId(env,code){
 if(!/^(?:ZY|RW|HD|WT)-\d{8}-\d{3,}$/i.test(code))return code;
 const r=await env.DB.prepare('SELECT record_id FROM sop_document_numbers WHERE display_no=?').bind(code.toUpperCase()).first();
 return r?.record_id||code;
}
// Batch business references for task lists, details, attendance, live boards and exports.
// Identifiers and scan payloads are never rewritten. The same linked job is the
// sole authority for its dispatcher, even when a caller supplies a worker/creator.
const tripNumbers=`(SELECT group_concat(display_no,'、') FROM (SELECT p.display_no FROM ck_unload_plan_links l JOIN v2_inbound_plans p ON p.id=l.plan_id WHERE l.job_id=j.id AND NULLIF(p.display_no,'') IS NOT NULL ORDER BY l.position,p.id))`;
const loadNumbers=`(SELECT group_concat(display_no,'、') FROM (SELECT o.display_no FROM ck_load_order_links l JOIN v2_outbound_orders o ON o.id=l.order_id WHERE l.job_id=j.id AND NULLIF(o.display_no,'') IS NOT NULL ORDER BY l.position,o.id))`;
const pickNumbers=`(SELECT group_concat(pick_doc_no,'、') FROM (SELECT pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id=j.id AND NULLIF(pick_doc_no,'') IS NOT NULL GROUP BY pick_doc_no ORDER BY MIN(rowid)))`;
function numberSQL(loadTrips=false){return `SELECT j.id,j.job_type,j.biz_class,j.created_at AS job_created_at,
 COALESCE(NULLIF(json_extract(s.state,'$.started_at'),''),j.created_at) AS job_started_at,
 s.kind AS assignment_kind,s.department AS assigned_department,json_extract(s.state,'$.labor_department') AS labor_department,
 COALESCE(NULLIF(json_extract(need.state,'$.title'),''),json_extract(s.state,'$.title'),'') AS job_title,
 COALESCE(json_extract(s.state,'$.owner'),'') AS dispatcher_name,COALESCE(json_extract(s.state,'$.owner_id'),'') AS dispatcher_id,
 CASE WHEN j.job_type='pick_direct' THEN j.display_no ELSE '' END AS trip_no,
 tn.display_no AS task_no,n.display_no AS work_plan_no,COALESCE(${tripNumbers},ib.display_no) AS inbound_plan_no,
 ${loadTrips?`COALESCE(${loadNumbers},ob.display_no)`:'ob.display_no'} AS outbound_plan_no,
 CASE WHEN n.display_no IS NOT NULL THEN n.display_no
 WHEN j.related_doc_type='work_order' THEN j.related_doc_id
 WHEN j.job_type='pick_direct' THEN COALESCE(${pickNumbers},NULLIF(j.display_no,''),'')
 ELSE COALESCE(${loadTrips?loadNumbers+',':''}${tripNumbers},NULLIF(j.inbound_external_no,''),NULLIF(ib.display_no,''),NULLIF(ob.display_no,''),NULLIF(fb.display_no,''),NULLIF(it.related_doc_no,''),NULLIF(vb.batch_no,''),NULLIF(j.display_no,''),'') END AS business_no
 FROM v2_ops_jobs j
 LEFT JOIN sop_document_numbers tn ON tn.record_id=j.id
 LEFT JOIN sop_records s ON s.id=j.id AND s.kind IN ('task','dispatch')
 LEFT JOIN sop_records need ON need.id=COALESCE(json_extract(s.state,'$.need_id'),CASE WHEN j.related_doc_type='sop_need' THEN j.related_doc_id END) AND need.kind='need'
 LEFT JOIN sop_document_numbers n ON n.record_id=need.id
 LEFT JOIN v2_inbound_plans ib ON j.related_doc_type IN ('inbound_plan','inbound') AND ib.id=j.related_doc_id
 LEFT JOIN v2_outbound_orders ob ON ob.id=CASE WHEN j.linked_outbound_order_id!='' THEN j.linked_outbound_order_id WHEN j.related_doc_type IN ('outbound','outbound_order') THEN j.related_doc_id END
 LEFT JOIN v2_field_feedbacks fb ON j.related_doc_type='field_feedback' AND fb.id=j.related_doc_id
 LEFT JOIN v2_issue_tickets it ON j.related_doc_type='issue_ticket' AND it.id=j.related_doc_id
 LEFT JOIN v2_verify_batches vb ON j.related_doc_type='verify_batch' AND vb.id=j.related_doc_id`;}
export const JOB_NUMBER_SQL=numberSQL();
// Optional load-trip tables exist only in the isolated staging rollout.
export const jobNumberSQL=env=>numberSQL(loadTripEnabled(env));
const clean=value=>String(value??'').trim();
const internalReference=value=>/^(?:(?:JOB(?:-(?:LD|CB))?|SOPJOB|NEED|TASK|DISPATCH)-)?[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(value)||/^JOB-[a-z0-9]{8,10}-[a-z0-9]{6,10}$/i.test(value);
export function businessReference(value,internalIds=[]){return [...new Set(clean(value).split('、').map(clean).filter(x=>x&&!internalIds.includes(x)&&!internalReference(x)))].join('、');}
function startLabel(value){const ms=Date.parse(value||'');return Number.isFinite(ms)?new Date(ms+9*3600000).toISOString().slice(0,16).replace('T',' ')+' KST':'';}
export function jobDisplayMetadata(row={}){
 const business_no=businessReference(row.business_no,[row.id,row.job_id]),display_business_no=businessReference(row.display_business_no??row.business_no,[row.id,row.job_id]),jobType=clean(row.job_type),definition=jobDefinitions[jobType];
 const type=definition?.[0]||(jobType==='courier_receiving'?'快递收货 / 택배 수령':'作业 / 작업');
 const rawTitle=clean(row.job_title),title=rawTitle&&!internalReference(rawTitle)&&rawTitle!==row.id&&rawTitle!==row.job_id&&rawTitle!==jobType?rawTitle:type;
 const department=Object.hasOwn(departments,row.job_department)?row.job_department:laborDepartment(row),departmentLabel=departments[department]||'用工部门未记录';
 const job_label=display_business_no?[display_business_no,title].filter(Boolean).join(' · '):[...new Set([type,title,departmentLabel,startLabel(row.job_started_at||row.job_created_at||row.created_at)].filter(Boolean))].join(' · ');
 return {business_no,display_business_no,has_business_reference:!!display_business_no,job_label,dispatcher_name:clean(row.dispatcher_name),dispatcher_id:clean(row.dispatcher_id),job_type:jobType,job_title:title,job_department:department,job_started_at:clean(row.job_started_at||row.job_created_at||row.created_at)};
}
export function decorateJobNumber(item,number){
 if(!number)return {...item,display_no:'',...jobDisplayMetadata({...item,business_no:'',dispatcher_name:'',dispatcher_id:''})};
 const metadata=jobDisplayMetadata(number),task_no=businessReference(number.task_no),business_no=metadata.display_business_no||task_no;
 return {...item,...metadata,business_no,display_no:business_no,task_no,trip_no:businessReference(number.trip_no),work_plan_no:businessReference(number.work_plan_no),inbound_plan_no:businessReference(number.inbound_plan_no),outbound_plan_no:businessReference(number.outbound_plan_no)};
}
export async function jobNumbers(env,items,key='id'){
 if(!items.length||env.SOP_UPGRADE_ENABLED!=='true')return items;
 const ids=[...new Set(items.map(x=>x[key]).filter(Boolean))];if(!ids.length)return items;
 await ensureLoadSchema(env);
 const rows=(await env.DB.prepare(jobNumberSQL(env)+' WHERE j.id IN (SELECT value FROM json_each(?))').bind(JSON.stringify(ids)).all()).results||[];
 const byId=new Map(rows.map(r=>[r.id,r]));
 return items.map(x=>x[key]?decorateJobNumber(x,byId.get(x[key])):x);
}

// Dashboard list and export use the same searchable business sources as labels.
// Keep existing ID lookup compatibility, but never substitute IDs as labels.
export function jobBusinessFilter(env,pattern){
 const clauses=['j.display_no LIKE ?','j.related_doc_id LIKE ?','j.linked_outbound_order_id LIKE ?'];
 if(env.SOP_UPGRADE_ENABLED==='true')clauses.push(
  "j.inbound_external_no LIKE ?",
  "EXISTS(SELECT 1 FROM sop_document_numbers dn LEFT JOIN sop_records sr ON sr.id=j.id AND sr.kind='task' WHERE (dn.record_id=j.id OR dn.record_id=json_extract(sr.state,'$.need_id') OR dn.record_id=j.related_doc_id) AND dn.display_no LIKE ?)",
  "EXISTS(SELECT 1 FROM v2_inbound_plans ip WHERE (ip.id=j.related_doc_id OR EXISTS(SELECT 1 FROM ck_unload_plan_links l WHERE l.job_id=j.id AND l.plan_id=ip.id)) AND ip.display_no LIKE ?)",
  "EXISTS(SELECT 1 FROM v2_outbound_orders op WHERE (op.id=j.related_doc_id OR op.id=j.linked_outbound_order_id"+(loadTripEnabled(env)?" OR EXISTS(SELECT 1 FROM ck_load_order_links l WHERE l.job_id=j.id AND l.order_id=op.id)":"")+") AND op.display_no LIKE ?)",
  "EXISTS(SELECT 1 FROM v2_ops_job_pick_docs pd WHERE pd.job_id=j.id AND pd.pick_doc_no LIKE ?)",
  "EXISTS(SELECT 1 FROM v2_field_feedbacks f WHERE j.related_doc_type='field_feedback' AND f.id=j.related_doc_id AND f.display_no LIKE ?)",
  "EXISTS(SELECT 1 FROM v2_issue_tickets t WHERE j.related_doc_type='issue_ticket' AND t.id=j.related_doc_id AND t.related_doc_no LIKE ?)",
  "EXISTS(SELECT 1 FROM v2_verify_batches b WHERE j.related_doc_type='verify_batch' AND b.id=j.related_doc_id AND b.batch_no LIKE ?)"
 );
 return {sql:'('+clauses.join(' OR ')+')',args:clauses.map(()=>pattern)};
}
