// All six real relationship shapes, with duplicate labels on different links.
export function reportCustomers(f,ids){
 const db=f.DB.raw;
 // Seed historical association shapes in the isolated memory database, then
 // restore every admission guard before reads (these are not lifecycle tests).
 const guards=db.prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('ck_unload_plan_links','ck_load_order_links','ck_putaway_links')").all();
 for(const g of guards)db.exec('DROP TRIGGER "'+g.name+'"');
 try{
 for(const [id,customer] of [['SCALE-IN','合成直接入库'],['SCALE-UNLOAD','合成共同卸货'],['SCALE-PUT','合成共同上架'],['SCALE-PUT-2','合成第二上架客户']])db.prepare('INSERT INTO v2_inbound_plans(id,customer,display_no) VALUES(?,?,?)').run(id,customer,id);
 for(const [id,customer] of [['SCALE-OUT','合成关联出库'],['SCALE-LOAD','合成共同装货']])db.prepare('INSERT INTO v2_outbound_orders(id,customer,display_no) VALUES(?,?,?)').run(id,customer,id);
 for(const id of ids){
  db.prepare("UPDATE v2_ops_jobs SET related_doc_type='inbound_plan',related_doc_id='SCALE-IN',linked_outbound_order_id='SCALE-OUT' WHERE id=?").run(id);
  db.prepare("INSERT INTO sop_records(id,kind,revision,department,state,updated_at) VALUES(?,'dispatch',1,'direct_ship',?,'2026-10-13T00:00:00Z')").run(id,JSON.stringify({customer:'合成派工客户',labor_department:'direct_ship'}));
  db.prepare("INSERT INTO ck_unload_plan_links(job_id,plan_id,position,plan_version,lines_snapshot) VALUES(?,'SCALE-UNLOAD',0,'1','[]')").run(id);
  db.prepare("INSERT INTO ck_load_order_links(job_id,order_id,position,snapshot_json) VALUES(?,'SCALE-LOAD',0,'{}')").run(id);
  for(const [position,plan] of ['SCALE-PUT','SCALE-PUT','SCALE-PUT-2'].entries())db.prepare('INSERT INTO ck_putaway_links(job_id,plan_id,external_no,tally_job_id,position) VALUES(?,?,?,?,?)').run(id,plan,'SYNTH-REF-'+position,'SYNTH-TALLY',position);
 }
 }finally{for(const g of guards)db.exec(g.sql);}
}
