// Daily declarations are independent of task output and clocks. Never sum the two.
import {ensureSchema} from './schema-ready.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const fail=(message,status=400)=>{throw Object.assign(Error(message),{status});};
export const scanToday=(at=Date.now())=>new Date(Number(new Date(at))+9*3600000).toISOString().slice(0,10);
export function scanDate(value){
 if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value)||value<'0001-01-01'||!Number.isFinite(Date.parse(value+'T00:00:00Z'))||new Date(value+'T00:00:00Z').toISOString().slice(0,10)!==value)fail('业务日期无效 / 업무 날짜를 확인하세요');
 return value;
}
export function scanTotal(value){
 if(typeof value!=='number'||!Number.isSafeInteger(value)||value<0)fail('扫描总量须为非负整数，未填不等于0 / 스캔 수량은 0 이상의 정수로 입력하세요');
 return value;
}
export function scanAccess(env){
 const u=env.SOP_REQUEST_USER;
 if(!u?.id||!['manager','dispatcher'].includes(u.role)||u.scope==='kiosk'||u.role!=='manager'&&(!(u.departments||[]).includes('import')||u.daily_dispatch&&(!u.can_dispatch||!(u.dispatch_departments||[]).includes('import'))))fail('仅管理员或获进口权限的派审员可录入及查询 / 관리자 또는 수입 권한이 있는 배정 담당자만 이용할 수 있습니다',403);
 return u;
}
export async function ensureDailyScan(env){
 return ensureSchema(env.DB,'import-daily-scan-v1',()=>env.DB.batch([
  q(env,`CREATE TABLE IF NOT EXISTS ck_import_daily_scans (
   business_date TEXT PRIMARY KEY,total INTEGER NOT NULL CHECK(total>=0 AND typeof(total)='integer'),
   version INTEGER NOT NULL CHECK(version>0),created_by TEXT NOT NULL,created_name TEXT NOT NULL,created_at TEXT NOT NULL,
   updated_by TEXT NOT NULL,updated_name TEXT NOT NULL,updated_at TEXT NOT NULL)`),
  q(env,`CREATE TABLE IF NOT EXISTS ck_import_daily_scan_events (
   request_id TEXT PRIMARY KEY,business_date TEXT NOT NULL,version INTEGER NOT NULL,
   before_total INTEGER,after_total INTEGER NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,
   actor_scope TEXT NOT NULL,created_at TEXT NOT NULL,fingerprint TEXT NOT NULL,attempt TEXT NOT NULL,
   UNIQUE(business_date,version))`)
 ]));
}
const itemQuery='SELECT * FROM ck_import_daily_scans WHERE business_date=?';
const eventFields='business_date,version,before_total,after_total,actor_name,actor_scope,created_at';
export async function handleDailyScan(body,env){
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true')fail('Not available',404);
 const u=scanAccess(env),action=body.action;
 if(!['sop_import_daily_scan_get','sop_import_daily_scan_save'].includes(action))fail('Unknown action',404);
 const date=body.business_date===undefined?scanToday():scanDate(body.business_date);
 if(action==='sop_import_daily_scan_get'){
  const before=body.before_version??null;
  if(before!==null&&(!Number.isSafeInteger(before)||before<1))fail('Invalid history cursor');
  await ensureDailyScan(env);
  const results=await env.DB.batch([
   q(env,itemQuery,date),
   q(env,`SELECT ${eventFields} FROM ck_import_daily_scan_events WHERE business_date=? AND (? IS NULL OR version<?) ORDER BY version DESC LIMIT 21`,date,before,before),
   q(env,'SELECT * FROM ck_import_daily_scans ORDER BY business_date DESC LIMIT 30')
  ]);
  const events=results[1].results||[];
  return {ok:true,business_date:date,timezone:'Asia/Seoul',item:results[0].results[0]||null,events:events.slice(0,20),next_before_version:events.length>20?events[19].version:null,recent:results[2].results,efficiency:null,efficiency_status:'scope_unconfirmed'};
 }
 // Saving always requires an explicit date and known version; no accidental "today" write.
 scanDate(body.business_date);const total=scanTotal(body.total),version=body.version;
 if(!Number.isSafeInteger(version)||version<0)fail('请先读取当天记录 / 먼저 당일 기록을 조회하세요');
 const request=body.client_req_id;
 if(typeof request!=='string'||!/^[a-zA-Z0-9_-]{8,100}$/.test(request))fail('Invalid request ID');
 const fingerprint=JSON.stringify([u.id,u.operation_context?.id||'',date,total,version]),at=new Date().toISOString(),attempt=crypto.randomUUID();
 await ensureDailyScan(env);
 // One serialized D1 batch claims the expected revision, appends its audit and replaces
 // the daily value. Replays return the current value even if later corrections exist.
 const result=await env.DB.batch([
  q(env,`INSERT INTO ck_import_daily_scan_events
   SELECT ?,?,?,(SELECT total FROM ck_import_daily_scans WHERE business_date=?),?,?,?,?,?,?,?
   WHERE COALESCE((SELECT version FROM ck_import_daily_scans WHERE business_date=?),0)=?
   ON CONFLICT(request_id) DO NOTHING`,request,date,version+1,date,total,u.id,u.name||u.id,u.scope||'',at,fingerprint,attempt,date,version),
  q(env,`INSERT INTO ck_import_daily_scans
   SELECT business_date,after_total,version,actor_id,actor_name,created_at,actor_id,actor_name,created_at
   FROM ck_import_daily_scan_events WHERE request_id=? AND attempt=?
   ON CONFLICT(business_date) DO UPDATE SET total=excluded.total,version=excluded.version,
   updated_by=excluded.updated_by,updated_name=excluded.updated_name,updated_at=excluded.updated_at`,request,attempt),
  q(env,'SELECT * FROM ck_import_daily_scan_events WHERE request_id=?',request),
  q(env,itemQuery,date)
 ]);
 const event=result[2].results[0];
 if(!event)fail('记录已被更正，请读取最新值并核对后再保存；输入已保留 / 기록이 변경되었습니다. 최신 값을 확인 후 저장하세요',409);
 if(event.fingerprint!==fingerprint)fail('请求已用于其他内容，请重新核对 / 요청 내용이 일치하지 않습니다',409);
 return {ok:true,item:result[3].results[0],saved_version:event.version,replayed:event.attempt!==attempt};
}
