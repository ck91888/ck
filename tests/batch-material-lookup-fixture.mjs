// Synthetic history only. Preserve insertion order, permissions and audit rows.
import {createHash} from 'node:crypto';
export const lookupIndex='ck_batch_material_inbound_needs_v1';
export const inboundChildren="kind='need' AND json_extract(state,'$.source_type')='inbound' AND json_extract(state,'$.source_id')=?";
export const lookupHash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
export async function seedMaterialHistory(f,history){
 const raw=f.DB.raw,original=await f.first('SELECT * FROM sop_records WHERE id=?',f.a.id),base=JSON.parse(original.state),values=[];
 const sql="INSERT INTO sop_records(id,kind,revision,department,state,updated_at) VALUES(?,'need',?,?,?,?)";
 raw?.exec('BEGIN');
 try{
  // Include different departments, identical timestamps, closed and cancelled
  // children. The old kind index orders by department/time then rowid, not ID.
  for(const [i,department,status,at]of [[9,'bulk','pending','02'],[2,'bulk','closed','01'],[7,'direct_ship','cancelled','01'],[1,'bulk','pending','01'],[8,'bulk','pending','01']])
   values.push(['SYN-TARGET-'+i,original.revision,department,JSON.stringify({...base,status,title:'合成关联作业'+i}),'2026-11-03T00:00:'+at+'Z']);
  for(let i=0;i<history;i++)values.push(['SYN-UNRELATED-'+String(i).padStart(6,'0'),1,['bulk','direct_ship','return'][i%3],JSON.stringify({source_type:i%5===0?'outbound':'inbound',source_id:'SYN-OTHER-PLAN-'+i,status:['closed','cancelled','pending'][i%3],links:[]}),'2026-01-01T00:00:00Z']);
  if(raw){const insert=raw.prepare(sql);for(const args of values)insert.run(...args);raw.exec('COMMIT');}
  else for(let i=0;i<values.length;i+=100)await f.env.DB.batch(values.slice(i,i+100).map(args=>f.env.DB.prepare(sql).bind(...args)));
 }catch(e){if(raw)raw.exec('ROLLBACK');throw e;}
}
export async function materialLookupSnapshot(db){
 const tables=['sop_records','sop_events','v2_inbound_plans','v2_inbound_plan_lines','v2_attachments','ck_batch_material_state','ck_batch_material_events','ck_batch_material_need_set_guards','ck_inbound_document_versions','v2_outbound_orders','v2_outbound_order_change_logs','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results'];
 const data={};for(const table of tables)data[table]=(await db.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()).results;return lookupHash(data);
}
