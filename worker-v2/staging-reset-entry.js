// Temporary, one-time maintenance entry for the user's 2026-09-21 reset request.
// Remove this entry from the staging config as soon as verification is complete.
import app from './index.js';
export const RESET_ID='ck-staging-clear-test-data-20260921';
export const RESET_DATABASE='ba1eba33-bcdd-4de2-8927-d37dc3788396';
const HOST='ck-v2-api-sop-staging.ck91888.workers.dev';
const EXPIRES='2026-09-21T15:00:00.000Z';
export const RESET_TABLES=[
 'ck_attendance_prints','ck_attendance_events','ck_attendance_breaks','ck_attendance_days','ck_attendance_people',
 'sop_events','sop_records',
 'v2_verify_scan_logs','v2_verify_batch_items','v2_verify_batches','v2_scan_batch_items','v2_scan_batches',
 'v2_issue_rework_requests','v2_issue_handle_runs','v2_issue_tickets',
 'v2_pick_worker_docs','v2_ops_job_pick_docs','v2_ops_job_results','v2_ops_job_workers','v2_ops_jobs',
 'v2_outbound_order_change_logs','v2_outbound_order_lines','v2_outbound_orders',
 'v2_inbound_plan_biz_tasks','v2_inbound_plan_lines','v2_inbound_plans','v2_field_feedbacks','v2_attachments',
 'v2_wms_import_rows','v2_wms_import_batches','v2_correction_requests','v2_ops_login_events','v2_admin_cleanup_logs',
 'v2_003_purchase_receipt_items','v2_003_purchase_receipts','v2_003_purchase_shipment_items','v2_003_purchase_shipments',
 'v2_003_purchase_order_lines','v2_003_purchase_orders','v2_003_asset_txns','v2_003_material_txns','v2_003_assets','v2_003_materials',
 'v2_idempotency_keys'
];
// Explicitly preserved: v2_schema_meta, v2_003_locations, server-side accounts,
// agency choices, app code/configuration, and R2 files referenced by the archive.
const reply=(value,status=503)=>Response.json(value,{status,headers:{'Cache-Control':'no-store','Retry-After':'2'}});
const query=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
async function tableCounts(env,tables){const result=[];for(let i=0;i<tables.length;i+=10){const part=tables.slice(i,i+10);result.push(...(await query(env,part.map(name=>`SELECT '${name}' AS table_name,COUNT(*) AS count FROM "${name}"`).join(' UNION ALL ')).all()).results);}return result;}
export async function resetGate(request,env,time=new Date().toISOString()){
 const url=new URL(request.url);
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true'||
    env.SOP_TEST_RESET_RUN!==RESET_ID||env.SOP_TEST_RESET_DATABASE!==RESET_DATABASE||url.hostname!==HOST)return null;
 const report=url.pathname==='/api/maintenance-20260921';
 await env.DB.batch([
  env.DB.prepare('CREATE TABLE IF NOT EXISTS ck_staging_reset_runs(id TEXT PRIMARY KEY,started_at TEXT NOT NULL,completed_at TEXT NOT NULL DEFAULT \'\')'),
  env.DB.prepare('CREATE TABLE IF NOT EXISTS ck_staging_reset_tables(run_id TEXT NOT NULL,table_name TEXT NOT NULL,archive_name TEXT NOT NULL,row_count INTEGER NOT NULL,cleared_at TEXT NOT NULL,PRIMARY KEY(run_id,table_name))')
 ]);
 const run=await query(env,'SELECT * FROM ck_staging_reset_runs WHERE id=?',RESET_ID).first();
 if(run?.completed_at&&!report)return null;
 if(!run?.completed_at&&time>=EXPIRES)return reply({ok:false,error:'One-time staging maintenance window expired; no further records were cleared.'});
 const existing=(await query(env,"SELECT name FROM sqlite_master WHERE type='table'").all()).results.map(x=>x.name);
 if(!['v2_inbound_plans','v2_ops_jobs','sop_records','ck_attendance_people'].every(x=>existing.includes(x)))throw Error('Unexpected staging schema; reset refused');
 const tables=RESET_TABLES.filter(x=>existing.includes(x));
 if(!run)await query(env,'INSERT OR IGNORE INTO ck_staging_reset_runs(id,started_at) VALUES(?,?)',RESET_ID,time).run();
 let done=(await query(env,'SELECT * FROM ck_staging_reset_tables WHERE run_id=?',RESET_ID).all()).results;
 if(!run?.completed_at){
  // A bounded batch stays below the Free-plan query limit. Each table's archive,
  // deletion and unique completion marker commit atomically; retries never delete
  // data from a table which has already been processed.
  const pending=tables.filter(name=>!done.some(x=>x.table_name===name)).slice(0,4);
  for(const name of pending){
   const archive='ck_archive_20260921_'+name;
   try{await env.DB.batch([
    query(env,`INSERT INTO ck_staging_reset_tables SELECT ?,?,?,COUNT(*),? FROM "${name}"`,RESET_ID,name,archive,time),
    env.DB.prepare(`CREATE TABLE "${archive}" AS SELECT * FROM "${name}"`),
    env.DB.prepare(`DELETE FROM "${name}"`)
   ]);}catch(error){
    // A concurrent request may have finished this exact table. Anything else
    // remains a hard failure and keeps normal application requests blocked.
    const processed=await query(env,'SELECT table_name FROM ck_staging_reset_tables WHERE run_id=? AND table_name=?',RESET_ID,name).first();
    if(!processed)throw error;
   }
  }
  done=(await query(env,'SELECT * FROM ck_staging_reset_tables WHERE run_id=?',RESET_ID).all()).results;
  if(tables.every(name=>done.some(x=>x.table_name===name))){
   const counts=await tableCounts(env,tables);
   if(counts.some(x=>x.count!==0))throw Error('Unexpected writes during reset; preserved and require review');
   await query(env,"UPDATE ck_staging_reset_runs SET completed_at=? WHERE id=? AND completed_at=''",time,RESET_ID).run();
  }
 }
 const latest=await query(env,'SELECT * FROM ck_staging_reset_runs WHERE id=?',RESET_ID).first();
 if(report){
  const counts=await tableCounts(env,tables);
  return reply({ok:true,maintenance:!latest.completed_at,run:latest,tables_done:done.length,tables_total:tables.length,archived_rows:done.reduce((n,x)=>n+x.row_count,0),tables:done,remaining:counts,preserved:['accounts','agencies','v2_003_locations','v2_schema_meta','archived attachment files']},latest.completed_at?200:503);
 }
 if(latest.completed_at)return null;
 return reply({ok:false,maintenance:true,error:'正在清空测试记录，请稍后刷新 / 테스트 데이터 정리 중입니다',tables_done:done.length,tables_total:tables.length});
}
export default {async fetch(request,env,ctx){
 try{const response=await resetGate(request,env);if(response)return response;}
 catch(error){console.error('Staging reset stopped',error.message);return reply({ok:false,maintenance:true,error:'测试数据清理未完成，请等待核实。正常业务操作暂未执行。',detail:new URL(request.url).pathname==='/api/maintenance-20260921'?error.message:undefined});}
 return app.fetch(request,env,ctx);
}};
