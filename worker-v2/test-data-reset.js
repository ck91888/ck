// Manager-triggered maintenance for the isolated staging database only.
// Every table is archived and cleared in one D1 transaction. No scheduled reset.
export const STAGING_HOST = 'ck-v2-api-sop-staging.ck91888.workers.dev';
export const STAGING_DATABASE = 'ba1eba33-bcdd-4de2-8927-d37dc3788396';
export const TABLES = [
 'ck_unload_plan_links','ck_unload_trips','ck_courier_events','ck_courier_plan_items','ck_courier_receipts',
 'ck_employee_profiles','ck_attendance_prints','ck_attendance_events','ck_attendance_breaks','ck_attendance_days','ck_attendance_people',
 'sop_events','sop_records','v2_verify_scan_logs','v2_verify_batch_items','v2_verify_batches','v2_scan_batch_items','v2_scan_batches',
 'v2_issue_rework_requests','v2_issue_handle_runs','v2_issue_tickets','v2_pick_worker_docs','v2_ops_job_pick_docs','v2_ops_job_results','v2_ops_job_workers','v2_ops_jobs',
 'v2_outbound_order_change_logs','v2_outbound_order_lines','v2_outbound_orders','v2_inbound_plan_biz_tasks','v2_inbound_plan_lines','v2_inbound_plans','v2_field_feedbacks','v2_attachments',
 'v2_wms_import_rows','v2_wms_import_batches','v2_correction_requests','v2_ops_login_events','v2_admin_cleanup_logs',
 'v2_003_purchase_receipt_items','v2_003_purchase_receipts','v2_003_purchase_shipment_items','v2_003_purchase_shipments','v2_003_purchase_order_lines','v2_003_purchase_orders',
 'v2_003_asset_txns','v2_003_material_txns','v2_003_assets','v2_003_materials','v2_idempotency_keys'
];
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results;
const fail=(message,status=400)=>{throw Object.assign(Error(message),{status});};
export const enabled=(request,env)=>new URL(request.url).hostname===STAGING_HOST&&env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true'&&env.SOP_TEST_RESET_ENABLED==='true'&&env.SOP_TEST_RESET_DATABASE===STAGING_DATABASE;
export async function ensureResetSchema(env){
 await env.DB.batch([
  env.DB.prepare("CREATE TABLE IF NOT EXISTS ck_test_resets(id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,actor TEXT NOT NULL,started_at TEXT NOT NULL,completed_at TEXT NOT NULL DEFAULT '',tables_json TEXT NOT NULL,last_error TEXT NOT NULL DEFAULT '')"),
  env.DB.prepare("CREATE UNIQUE INDEX IF NOT EXISTS ck_test_reset_single_active ON ck_test_resets((1)) WHERE completed_at=''"),
  env.DB.prepare('CREATE TABLE IF NOT EXISTS ck_test_reset_items(run_id TEXT NOT NULL,table_name TEXT NOT NULL,archive_name TEXT NOT NULL,row_count INTEGER NOT NULL,PRIMARY KEY(run_id,table_name))')
 ]);
}
export const activeReset=env=>q(env,"SELECT * FROM ck_test_resets WHERE completed_at='' LIMIT 1").first();
const knownTables=async env=>{const names=(await rows(env,"SELECT name FROM sqlite_master WHERE type='table'")).map(x=>x.name);return TABLES.filter(x=>names.includes(x));};
async function counts(env,tables){
 const result=[];
 // Scalar subqueries avoid D1's low compound-SELECT limit.
 for(let i=0;i<tables.length;i+=10){const part=tables.slice(i,i+10);const row=await q(env,'SELECT '+part.map((name,j)=>`(SELECT COUNT(*) FROM "${name}") AS c${j}`).join(',')).first();result.push(...part.map((name,j)=>({table_name:name,count:row['c'+j]})));}
 return result;
}
async function view(env,run){
 if(!run)return null;
 const progress=await q(env,'SELECT COUNT(*) AS done,COALESCE(SUM(row_count),0) AS archived FROM ck_test_reset_items WHERE run_id=?',run.id).first();
 return {id:run.id,startedAt:run.started_at,completedAt:run.completed_at,total:JSON.parse(run.tables_json).length,done:progress.done,archived:progress.archived,error:run.last_error};
}
export async function resetAction(action,body,env,user){
 if(user?.role!=='manager')fail('仅负责人可清空测试数据 / 관리자만 삭제할 수 있습니다',403);
 await ensureResetSchema(env);
 if(action==='status'){
  const active=await activeReset(env);
  const latest=active||await q(env,'SELECT * FROM ck_test_resets ORDER BY started_at DESC LIMIT 1').first();
  return {ok:true,run:await view(env,latest),counts:active?[]:await counts(env,await knownTables(env))};
 }
 if(action==='start'){
  if(body.confirmation!=='CLEAR_TEST_DATA'||!/^[-a-zA-Z0-9]{16,80}$/.test(body.requestId||''))fail('请先确认清空范围 / 삭제 범위를 확인하세요');
  const previous=await q(env,'SELECT * FROM ck_test_resets WHERE request_id=?',body.requestId).first();
  if(previous)return {ok:true,run:await view(env,previous)};
  const active=await activeReset(env);if(active)return {ok:true,run:await view(env,active)};
  const tables=await knownTables(env);
  if(!['v2_inbound_plans','v2_ops_jobs','sop_records','ck_attendance_people'].every(x=>tables.includes(x)))fail('测试库尚未就绪，未清空数据 / 테스트 데이터베이스가 준비되지 않았습니다',409);
  const id=crypto.randomUUID().replaceAll('-','');
  try{await q(env,'INSERT INTO ck_test_resets(id,request_id,actor,started_at,tables_json) VALUES(?,?,?,?,?)',id,body.requestId,user.id,new Date().toISOString(),JSON.stringify(tables)).run();}
  catch(error){const concurrent=await q(env,'SELECT * FROM ck_test_resets WHERE request_id=?',body.requestId).first()||await activeReset(env);if(concurrent)return {ok:true,run:await view(env,concurrent)};throw error;}
  return {ok:true,run:await view(env,await q(env,'SELECT * FROM ck_test_resets WHERE id=?',id).first())};
 }
 if(action!=='step')fail('Unknown maintenance action',404);
 const run=await q(env,'SELECT * FROM ck_test_resets WHERE id=?',String(body.runId||'')).first();
 if(!run)fail('清理记录不存在 / 삭제 기록이 없습니다',404);
 if(run.completed_at)return {ok:true,run:await view(env,run)};
 const tables=JSON.parse(run.tables_json);
 if(!/^[a-f0-9]{32}$/.test(run.id)||!tables.every(name=>TABLES.includes(name)))fail('Invalid maintenance scope',409);
 try{
  const done=(await rows(env,'SELECT table_name FROM ck_test_reset_items WHERE run_id=?',run.id)).map(x=>x.table_name);
  for(const name of tables.filter(x=>!done.includes(x)).slice(0,4)){
   const archive=`ck_backup_${run.id}_${name}`;
   try{await env.DB.batch([
    q(env,`INSERT INTO ck_test_reset_items SELECT ?,?,?,COUNT(*) FROM "${name}"`,run.id,name,archive),
    env.DB.prepare(`CREATE TABLE "${archive}" AS SELECT * FROM "${name}"`),
    env.DB.prepare(`DELETE FROM "${name}"`)
   ]);}catch(error){if(!await q(env,'SELECT table_name FROM ck_test_reset_items WHERE run_id=? AND table_name=?',run.id,name).first())throw error;}
  }
  const progress=await view(env,run);
  if(progress.done===tables.length){
   if((await counts(env,tables)).some(x=>x.count!==0))fail('清理期间出现新增记录，已保留，请联系维护人员核实 / 새 기록이 있어 보존했습니다',409);
   await q(env,"UPDATE ck_test_resets SET completed_at=?,last_error='' WHERE id=? AND completed_at=''",new Date().toISOString(),run.id).run();
  }else await q(env,"UPDATE ck_test_resets SET last_error='' WHERE id=?",run.id).run();
  return {ok:true,run:await view(env,await q(env,'SELECT * FROM ck_test_resets WHERE id=?',run.id).first())};
 }catch(error){await q(env,'UPDATE ck_test_resets SET last_error=? WHERE id=?',String(error.message).slice(0,400),run.id).run();throw error;}
}
