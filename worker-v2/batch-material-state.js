import {ensureSchema} from './schema-ready.js';
import {ensureInboundDocuments} from './inbound-documents.js';
export const batchStateEnabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true'&&env.SOP_WORK_CHAIN_ENABLED==='true';
export const batchVisibleSql=(env,alias='a')=>batchStateEnabled(env)?` AND NOT EXISTS(SELECT 1 FROM ck_batch_material_state bs WHERE bs.attachment_id=${alias}.id AND bs.removed=1)`:'';
// Match cargoReferencesFile's document arrays in groups/history and frozen links.
// Only string attachment IDs count; unrelated metadata and old invalid JSON do not.
const referenceIds=state=>{
 const json=`CASE WHEN json_valid(${state}) THEN ${state} ELSE '{}' END`;
 const file="CASE WHEN f.type='object' THEN f.value ELSE '{}' END";
 return `SELECT json_extract(${file},'$.id') FROM (
  SELECT type,key,value FROM json_tree(${json},'$.cargo_groups')
  UNION ALL SELECT type,key,value FROM json_tree(${json},'$.links')
 ) c JOIN json_each(CASE WHEN c.type='array' AND c.key IN ('documents','public_documents') THEN c.value ELSE '[]' END) f
 WHERE json_type(${file},'$.id')='text'`;
};
export async function ensureBatchMaterialState(env){
 if(!batchStateEnabled(env))return;
 await ensureInboundDocuments(env);
 await ensureSchema(env.DB,'batch-material-state-v1',async()=>{
  const sql=[
   'CREATE TABLE IF NOT EXISTS ck_batch_material_state(attachment_id TEXT PRIMARY KEY,plan_id TEXT NOT NULL,removed INTEGER NOT NULL CHECK(removed IN (0,1)),revision INTEGER NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,changed_at TEXT NOT NULL)',
   'CREATE TABLE IF NOT EXISTS ck_batch_material_events(request_id TEXT PRIMARY KEY,attachment_id TEXT NOT NULL,plan_id TEXT NOT NULL,need_id TEXT NOT NULL,expected_revision INTEGER NOT NULL,revision INTEGER NOT NULL,removed INTEGER NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,created_at TEXT NOT NULL,result_json TEXT NOT NULL,UNIQUE(attachment_id,revision))',
   `CREATE TRIGGER IF NOT EXISTS ck_batch_material_event_guard BEFORE INSERT ON ck_batch_material_events
    WHEN NEW.expected_revision!=COALESCE((SELECT revision FROM ck_batch_material_state WHERE attachment_id=NEW.attachment_id),0)
    OR NEW.revision!=NEW.expected_revision+1 OR NEW.removed=COALESCE((SELECT removed FROM ck_batch_material_state WHERE attachment_id=NEW.attachment_id),0)
    OR NOT EXISTS(SELECT 1 FROM v2_attachments a JOIN v2_inbound_plans p ON p.id=a.related_doc_id WHERE a.id=NEW.attachment_id AND a.related_doc_type='inbound_plan' AND a.attachment_category='batch_work_material' AND p.id=NEW.plan_id AND p.status!='cancelled' AND COALESCE(p.is_deleted,0)=0)
    BEGIN SELECT RAISE(ABORT,'batch_material_stale'); END`,
   `CREATE TRIGGER IF NOT EXISTS ck_batch_material_document_insert AFTER INSERT ON ck_batch_material_state WHEN NEW.removed=1 BEGIN UPDATE ck_inbound_document_versions SET revision=revision+1 WHERE plan_id=NEW.plan_id; END`,
   `CREATE TRIGGER IF NOT EXISTS ck_batch_material_document_update AFTER UPDATE OF removed ON ck_batch_material_state WHEN NEW.removed IS NOT OLD.removed BEGIN UPDATE ck_inbound_document_versions SET revision=revision+1 WHERE plan_id=NEW.plan_id; END`
  ];
  await env.DB.batch(sql.map(s=>env.DB.prepare(s)));
 });
 // Separate additive upgrade: an already-initialized v1 database must also get
 // these commit-time guards. No historical states/events or objects are changed.
 await ensureSchema(env.DB,'batch-material-reference-guards-v1',()=>env.DB.batch([
  `CREATE TRIGGER IF NOT EXISTS ck_batch_material_reference_remove_v1 BEFORE INSERT ON ck_batch_material_events
   WHEN NEW.removed=1 AND EXISTS(SELECT 1 FROM sop_records n WHERE n.kind='need'
    AND json_extract(n.state,'$.source_type')='inbound' AND json_extract(n.state,'$.source_id')=NEW.plan_id
    AND NEW.attachment_id IN (${referenceIds('n.state')}))
   BEGIN SELECT RAISE(ABORT,'batch_material_in_use'); END`,
  `CREATE TRIGGER IF NOT EXISTS ck_batch_material_reference_insert_v1 BEFORE INSERT ON sop_records
   WHEN NEW.kind='need' AND EXISTS(SELECT 1 FROM ck_batch_material_state b WHERE b.removed=1 AND b.attachment_id IN (${referenceIds('NEW.state')}))
   BEGIN SELECT RAISE(ABORT,'batch_material_reference_removed'); END`,
  `CREATE TRIGGER IF NOT EXISTS ck_batch_material_reference_update_v1 BEFORE UPDATE OF state ON sop_records
   WHEN NEW.kind='need' AND EXISTS(SELECT 1 FROM ck_batch_material_state b WHERE b.removed=1
    AND b.attachment_id IN (${referenceIds('NEW.state')}) AND b.attachment_id NOT IN (${referenceIds('OLD.state')}))
   BEGIN SELECT RAISE(ABORT,'batch_material_reference_removed'); END`
 ].map(sql=>env.DB.prepare(sql))));
}
