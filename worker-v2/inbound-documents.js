import {ensureSchema} from './schema-ready.js';
import {inboundFlowEnabled} from './inbound-flow.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
export async function ensureInboundDocuments(env){
 if(!inboundFlowEnabled(env))return;
 await ensureSchema(env.DB,'inbound-documents-v1',async()=>{
  const planFields=['plan_date','customer','cargo_summary','expected_arrival','purpose','remark','biz_class','biz_classes_json','external_inbound_no','bulk_external_inbound_no'];
  const bump=expression=>`UPDATE ck_inbound_document_versions SET revision=revision+1 WHERE plan_id=${expression};`;
  const needsPlan=alias=>`json_extract(${alias}.state,'$.source_id')`;
  const isInbound=alias=>`${alias}.kind='need' AND json_extract(${alias}.state,'$.source_type')='inbound'`;
  const changed=planFields.map(f=>`NEW.${f} IS NOT OLD.${f}`).join(' OR ');
  const schema=[
   'CREATE TABLE IF NOT EXISTS ck_inbound_document_versions(plan_id TEXT PRIMARY KEY,revision INTEGER NOT NULL DEFAULT 1)',
   'CREATE TABLE IF NOT EXISTS ck_inbound_document_issues(plan_id TEXT NOT NULL,revision INTEGER NOT NULL,confirmed_at TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,PRIMARY KEY(plan_id,revision))',
   'INSERT OR IGNORE INTO ck_inbound_document_versions(plan_id) SELECT id FROM v2_inbound_plans',
   'CREATE TRIGGER IF NOT EXISTS ck_inbound_document_created AFTER INSERT ON v2_inbound_plans BEGIN INSERT OR IGNORE INTO ck_inbound_document_versions(plan_id) VALUES(NEW.id); END',
   `CREATE TRIGGER IF NOT EXISTS ck_inbound_document_plan_changed AFTER UPDATE OF ${planFields.join(',')} ON v2_inbound_plans WHEN ${changed} BEGIN ${bump('NEW.id')} END`,
   `CREATE TRIGGER IF NOT EXISTS ck_inbound_document_need_created AFTER INSERT ON sop_records WHEN ${isInbound('NEW')} BEGIN ${bump(needsPlan('NEW'))} END`,
   `CREATE TRIGGER IF NOT EXISTS ck_inbound_document_need_removed AFTER DELETE ON sop_records WHEN ${isInbound('OLD')} BEGIN ${bump(needsPlan('OLD'))} END`,
   `CREATE TRIGGER IF NOT EXISTS ck_inbound_document_need_changed AFTER UPDATE OF state ON sop_records WHEN (${isInbound('NEW')} OR ${isInbound('OLD')}) AND (json_extract(NEW.state,'$.requirement_version') IS NOT json_extract(OLD.state,'$.requirement_version') OR json_extract(NEW.state,'$.source_id') IS NOT json_extract(OLD.state,'$.source_id') OR (json_extract(NEW.state,'$.status')='cancelled') IS NOT (json_extract(OLD.state,'$.status')='cancelled')) BEGIN ${bump(needsPlan('OLD'))} ${bump(needsPlan('NEW'))} END`
  ];
  for(const [table,key,fields]of [
   ['v2_inbound_plan_lines','plan_id',['unit_type','planned_qty','remark','line_no']],
   ['ck_courier_plan_items','plan_id',['tracking_no']],
   ['v2_attachments','related_doc_id',['file_name','file_key','attachment_category']]
  ]){
   const condition=table==='v2_attachments'?" AND NEW.related_doc_type='inbound_plan'":'';
   const oldCondition=table==='v2_attachments'?" AND OLD.related_doc_type='inbound_plan'":'';
   schema.push(`CREATE TRIGGER IF NOT EXISTS ck_document_${table}_insert AFTER INSERT ON ${table} WHEN 1${condition} BEGIN ${bump('NEW.'+key)} END`);
   schema.push(`CREATE TRIGGER IF NOT EXISTS ck_document_${table}_delete AFTER DELETE ON ${table} WHEN 1${oldCondition} BEGIN ${bump('OLD.'+key)} END`);
   schema.push(`CREATE TRIGGER IF NOT EXISTS ck_document_${table}_update AFTER UPDATE OF ${fields.join(',')} ON ${table} WHEN (${fields.map(f=>`NEW.${f} IS NOT OLD.${f}`).join(' OR ')})${condition} BEGIN ${bump('NEW.'+key)} END`);
  }
  await env.DB.batch(schema.map(sql=>env.DB.prepare(sql)));
 });
}
export async function inboundDocumentStates(env,ids){
 if(!inboundFlowEnabled(env)||!ids.length)return new Map();
 await ensureInboundDocuments(env);
 const rows=(await inboundDocumentStateRead(env,ids).all()).results||[];
 return documentStatesFromRows(rows);
}
export const inboundDocumentStateRead=(env,ids)=>q(env,`SELECT v.plan_id,v.revision,p.status,p.is_deleted,i.confirmed_at,i.actor_id,i.actor_name,
  (SELECT MAX(revision) FROM ck_inbound_document_issues old WHERE old.plan_id=v.plan_id) AS previous_issued_revision
  FROM ck_inbound_document_versions v JOIN v2_inbound_plans p ON p.id=v.plan_id
  LEFT JOIN ck_inbound_document_issues i ON i.plan_id=v.plan_id AND i.revision=v.revision
  WHERE v.plan_id IN (${ids.map(()=>'?').join(',')})`,...ids);
export const documentStatesFromRows=rows=>new Map(rows.map(r=>[r.plan_id,{revision:r.revision,state:r.status==='cancelled'||r.is_deleted?'cancelled':r.confirmed_at?'issued':r.previous_issued_revision?'needs_reissue':'unissued',confirmed_at:r.confirmed_at||'',actor_name:r.actor_name||'',previous_issued_revision:r.previous_issued_revision||0}]));
export async function confirmInboundIssue(env,body){
 if(!inboundFlowEnabled(env))throw Error('打印下发功能未启用');
 const user=env.SOP_REQUEST_USER;
 if(!user||user.scope==='field'||!['manager','service'].includes(user.role))throw Error('请由办公室客服或管理员确认打印下发');
 const id=String(body.id||''),revision=Number(body.revision);
 if(!id||!Number.isSafeInteger(revision)||revision<1)throw Error('请先打印当前入库计划版本');
 await ensureInboundDocuments(env);const t=new Date().toISOString();
 await q(env,`INSERT OR IGNORE INTO ck_inbound_document_issues(plan_id,revision,confirmed_at,actor_id,actor_name)
  SELECT p.id,v.revision,?,?,? FROM v2_inbound_plans p JOIN ck_inbound_document_versions v ON v.plan_id=p.id
  WHERE p.id=? AND v.revision=? AND p.status!='cancelled' AND COALESCE(p.is_deleted,0)=0`,t,user.id,user.name,id,revision).run();
 const state=(await inboundDocumentStates(env,[id])).get(id);
 if(!state||state.state==='cancelled')throw Error('已取消或删除的入库计划不能下发');
 if(state.revision!==revision)throw Error('计划内容已更新，请重新打印后确认下发');
 if(state.state!=='issued')throw Error('未能确认下发，请重试');
 return {ok:true,id,issue_state:state};
}
