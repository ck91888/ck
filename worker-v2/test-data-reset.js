import {ensureSchema} from './schema-ready.js';
// Execution is intentionally unavailable. Existing runs and backups stay intact
// pending an independently reviewed recovery/isolation design.
export const STAGING_HOST = 'ck-v2-api-sop-staging.ck91888.workers.dev';
export const STAGING_HOSTS = Object.freeze([STAGING_HOST, 'sop-test.ck91888.cn']);
export const STAGING_DATABASE = 'ba1eba33-bcdd-4de2-8927-d37dc3788396';
export const RESET_DISABLED_REASON='重置功能已暂停，现有记录和备份保留；活动清理请联系维护人员核实 / 초기화가 중지되었습니다. 기존 기록과 백업은 유지됩니다.';
export const TABLES = [
 'ck_courier_batch_items','ck_courier_batch_requests','ck_courier_batches',
 'ck_courier_plan_item_history',
 'ck_inbound_create_receipts','ck_putaway_links',
 'ck_bulk_labor_finishes','ck_bulk_work_batch_items',
 'ck_crew_borrows','ck_crew_requests',
 'ck_load_order_claims','ck_load_order_links','ck_load_trips',
 'ck_field_access','ck_access_events','ck_feedback_plan_links','ck_unload_plan_links','ck_unload_trips','ck_courier_events','ck_courier_plan_items','ck_courier_receipts',
 'ck_employee_profiles','ck_permanent_badge_profiles','ck_attendance_management','ck_attendance_prints','ck_attendance_events','ck_attendance_breaks','ck_attendance_days','ck_attendance_people',
 'sop_document_numbers','sop_events','sop_records','v2_verify_scan_logs','v2_verify_batch_items','v2_verify_batches','v2_scan_batch_items','v2_scan_batches',
 'v2_issue_rework_requests','v2_issue_handle_runs','v2_issue_tickets','v2_pick_worker_docs','v2_ops_job_pick_docs','v2_ops_job_results','v2_ops_job_workers','v2_ops_jobs',
 'v2_outbound_order_change_logs','v2_outbound_order_lines','v2_outbound_orders','ck_outbound_display_sequences','v2_inbound_plan_biz_tasks','v2_inbound_plan_lines','v2_inbound_plans','v2_field_feedbacks','v2_attachments',
 'v2_wms_import_rows','v2_wms_import_batches','v2_correction_requests','v2_ops_login_events','v2_admin_cleanup_logs',
 'v2_003_purchase_receipt_items','v2_003_purchase_receipts','v2_003_purchase_shipment_items','v2_003_purchase_shipments','v2_003_purchase_order_lines','v2_003_purchase_orders',
 'v2_003_asset_txns','v2_003_material_txns','v2_003_assets','v2_003_materials','v2_idempotency_keys'
];
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results;
const fail=(message,status=400)=>{throw Object.assign(Error(message),{status});};
export const enabled=(request,env)=>STAGING_HOSTS.includes(new URL(request.url).hostname)&&env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true'&&env.SOP_TEST_RESET_ENABLED==='true'&&env.SOP_TEST_RESET_DATABASE===STAGING_DATABASE;
export async function ensureResetSchema(env){
 return ensureSchema(env.DB,'ensureResetSchema-v1',async()=>{
 await env.DB.batch([
  env.DB.prepare("CREATE TABLE IF NOT EXISTS ck_test_resets(id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,actor TEXT NOT NULL,started_at TEXT NOT NULL,completed_at TEXT NOT NULL DEFAULT '',tables_json TEXT NOT NULL,last_error TEXT NOT NULL DEFAULT '')"),
  env.DB.prepare("CREATE UNIQUE INDEX IF NOT EXISTS ck_test_reset_single_active ON ck_test_resets((1)) WHERE completed_at=''"),
  env.DB.prepare('CREATE TABLE IF NOT EXISTS ck_test_reset_items(run_id TEXT NOT NULL,table_name TEXT NOT NULL,archive_name TEXT NOT NULL,row_count INTEGER NOT NULL,PRIMARY KEY(run_id,table_name))')
 ]);
 });
}
export const activeReset=env=>q(env,"SELECT * FROM ck_test_resets WHERE completed_at='' LIMIT 1").first();
const hasResetTable=env=>q(env,"SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='ck_test_resets'").first();
// Safety is independent of the old feature/upgrade switches and request host.
// Never cache a negative result: another binding may have an existing run.
export async function safetyActiveReset(env){
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_TEST_RESET_DATABASE!==STAGING_DATABASE)return null;
 return await hasResetTable(env)?activeReset(env):null;
}
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
 if(action==='start'||action==='step')throw Object.assign(Error(RESET_DISABLED_REASON),{status:503,reset_disabled:true});
 if(action==='status'){
  const exists=await hasResetTable(env),active=exists?await activeReset(env):null;
  const latest=active||(exists?await q(env,'SELECT * FROM ck_test_resets ORDER BY started_at DESC LIMIT 1').first():null);
  return {ok:true,disabled:true,reason:RESET_DISABLED_REASON,run:await view(env,latest),counts:active?[]:await counts(env,await knownTables(env))};
 }
 fail('Unknown maintenance action',404);
}
