import {ensureSchema} from './schema-ready.js';
const enabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true';
const directPresent=alias=>`EXISTS(WITH RECURSIVE codes(code,rest) AS (SELECT '',COALESCE(${alias}.external_inbound_no,'')||char(10) UNION ALL SELECT substr(rest,1,instr(rest,char(10))-1),substr(rest,instr(rest,char(10))+1) FROM codes WHERE rest!='') SELECT 1 FROM codes WHERE code!='' AND instr(char(10)||COALESCE(${alias}.bulk_external_inbound_no,'')||char(10),char(10)||code||char(10))=0)`;
const directValue=alias=>`(WITH RECURSIVE codes(code,rest) AS (SELECT '',COALESCE(${alias}.external_inbound_no,'')||char(10) UNION ALL SELECT substr(rest,1,instr(rest,char(10))-1),substr(rest,instr(rest,char(10))+1) FROM codes WHERE rest!='') SELECT group_concat(code,char(10)) FROM codes WHERE code!='' AND instr(char(10)||COALESCE(${alias}.bulk_external_inbound_no,'')||char(10),char(10)||code||char(10))=0)`;
export async function ensureReferenceHistory(env){
 if(!enabled(env))return;
 await ensureSchema(env.DB,'inbound-reference-history-v1',()=>env.DB.batch([
  env.DB.prepare('CREATE TABLE IF NOT EXISTS ck_inbound_reference_context(plan_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL)'),
  env.DB.prepare('CREATE TABLE IF NOT EXISTS ck_inbound_reference_history(plan_id TEXT NOT NULL,biz_class TEXT NOT NULL,external_nos TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(plan_id,biz_class))'),
  env.DB.prepare(`CREATE TRIGGER IF NOT EXISTS ck_inbound_reference_backfill AFTER UPDATE OF external_inbound_no,bulk_external_inbound_no ON v2_inbound_plans BEGIN
   INSERT OR IGNORE INTO ck_inbound_reference_history SELECT NEW.id,'direct_ship',${directValue('NEW')},c.actor_id,c.actor_name,NEW.updated_at FROM ck_inbound_reference_context c WHERE c.plan_id=NEW.id AND NOT ${directPresent('OLD')} AND ${directPresent('NEW')};
   INSERT OR IGNORE INTO ck_inbound_reference_history SELECT NEW.id,'bulk_putaway',NEW.bulk_external_inbound_no,c.actor_id,c.actor_name,NEW.updated_at FROM ck_inbound_reference_context c WHERE c.plan_id=NEW.id AND COALESCE(OLD.bulk_external_inbound_no,'')='' AND COALESCE(NEW.bulk_external_inbound_no,'')!='';
  END`)
 ]));
}
// The request principal and the reference update live in the same D1 transaction.
// The trigger sees the actual prior row; failed or repeated writes add no history.
export async function referenceWriteBatch(env,planId,statements){
 if(!enabled(env))return env.DB.batch(statements);
 await ensureReferenceHistory(env);const actor=env.SOP_REQUEST_USER||{};
 const results=await env.DB.batch([
  env.DB.prepare('INSERT OR REPLACE INTO ck_inbound_reference_context(plan_id,actor_id,actor_name) VALUES(?,?,?)').bind(planId,String(actor.id||''),String(actor.name||'')),
  ...statements,
  env.DB.prepare('DELETE FROM ck_inbound_reference_context WHERE plan_id=?').bind(planId)
 ]);
 return results.slice(1,-1);
}
export async function referenceHistory(env,planId){
 if(!enabled(env))return [];
 await ensureReferenceHistory(env);
 return (await env.DB.prepare('SELECT * FROM ck_inbound_reference_history WHERE plan_id=? ORDER BY created_at,biz_class').bind(planId).all()).results||[];
}
