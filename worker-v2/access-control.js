import {ensureSchema} from './schema-ready.js';
// Separate, revocable sessions for office, field dispatch and the fixed attendance terminal.
// Raw session tokens and administrator codes are never stored in D1 or sent to assets.
import { kstDay } from './attendance-time.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
export const accessEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_ACCESS_CONTROL==='true';
export const accessScope=request=>new URL(request.url).pathname.startsWith('/001/')?'field':new URL(request.url).pathname.startsWith('/attendance/')?'kiosk':'office';
const paths={field:'/001/',kiosk:'/attendance/',office:'/'};
const cookieName=s=>'ck_'+s+'_access';
const cookie=(s,t,age)=>`${cookieName(s)}=${t}; Path=${paths[s]}; HttpOnly; Secure; SameSite=Strict; Max-Age=${age}`;
const token=()=>Array.from(crypto.getRandomValues(new Uint8Array(32)),x=>x.toString(16).padStart(2,'0')).join('');
export const digest=async s=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(s))),x=>x.toString(16).padStart(2,'0')).join('');
const response=(b,status=200,headers={})=>Response.json(b,{status,headers:{'Cache-Control':'no-store',...headers}});
const denied=(error='请重新登录 / 다시 로그인하세요',status=401)=>response({ok:false,unauthorized:status===401,error},status);
export const ACCESS_SCHEMA=[
 'CREATE TABLE IF NOT EXISTS ck_access_sessions(token_hash TEXT PRIMARY KEY,scope TEXT NOT NULL,user_id TEXT NOT NULL,name TEXT NOT NULL,role TEXT NOT NULL,departments TEXT NOT NULL,attendance_id TEXT NOT NULL DEFAULT \'\',grant_version INTEGER NOT NULL DEFAULT 0,credential_hash TEXT NOT NULL DEFAULT \'\',expires_at INTEGER NOT NULL,created_at TEXT NOT NULL)',
 'CREATE INDEX IF NOT EXISTS ck_access_expiry ON ck_access_sessions(expires_at)',
 'CREATE TABLE IF NOT EXISTS ck_field_access(person_id TEXT PRIMARY KEY,enabled INTEGER NOT NULL,version INTEGER NOT NULL,updated_by TEXT NOT NULL,updated_at TEXT NOT NULL)',
 'CREATE TABLE IF NOT EXISTS ck_access_events(id TEXT PRIMARY KEY,person_id TEXT NOT NULL,action TEXT NOT NULL,actor_id TEXT NOT NULL,detail TEXT NOT NULL,created_at TEXT NOT NULL)',
 'CREATE TABLE IF NOT EXISTS ck_access_attempts(bucket TEXT PRIMARY KEY,attempts INTEGER NOT NULL,expires_at INTEGER NOT NULL)'
];
export async function ensureAccess(env){
 return ensureSchema(env.DB,'ensureAccess-v1',async()=>{
 const present=await q(env,"SELECT COUNT(*) n FROM sqlite_master WHERE type='table' AND name IN ('ck_access_sessions','ck_field_access','ck_access_events','ck_access_attempts')").first();
 if(present?.n!==4)await env.DB.batch(ACCESS_SCHEMA.map(s=>env.DB.prepare(s)));
 });
}
function rawToken(request,scope){return (request.headers.get('Cookie')||'').split(';').map(s=>s.trim()).find(s=>s.startsWith(cookieName(scope)+'='))?.slice(cookieName(scope).length+1)||'';}
function safeUser(s){return {id:s.user_id,name:s.name,role:s.role,departments:JSON.parse(s.departments),scope:s.scope};}
async function adminByCode(code,env){
 if(typeof code!=='string'||code.length>256||!code)return null;
 const hash=await digest(code.trim());
 // This optional verifier is for a randomly generated 256-bit recovery code, not a human password.
 if(env.SOP_ADMIN_CODE_SHA256&&hash===env.SOP_ADMIN_CODE_SHA256)return {id:'ck-office-admin',name:'管理员',role:'manager',departments:['bulk','direct_ship','import'],hash};
 let users=[];try{users=JSON.parse(env.SOP_USERS_JSON||'[]');}catch{}
 for(const u of Array.isArray(users)?users:[])if(u.role==='manager'&&u.key&&await digest(u.key)===hash)return {...u,hash};
 return null;
}
async function credentialCurrent(s,env){
 if(s.credential_hash===env.SOP_ADMIN_CODE_SHA256&&s.user_id==='ck-office-admin')return true;
 let users=[];try{users=JSON.parse(env.SOP_USERS_JSON||'[]');}catch{}
 for(const u of Array.isArray(users)?users:[])if(u.id===s.user_id&&u.role==='manager'&&u.key&&await digest(u.key)===s.credential_hash)return true;
 return false;
}
export async function accessUser(request,env){
 const scope=accessScope(request),raw=rawToken(request,scope);if(!/^[a-f0-9]{64}$/.test(raw))return null;await ensureAccess(env);
 const s=await q(env,'SELECT * FROM ck_access_sessions WHERE token_hash=? AND scope=? AND expires_at>?',await digest(raw),scope,Date.now()).first();if(!s)return null;
 if(scope==='field'){
  const row=await q(env,`SELECT p.name,p.badge_id FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id LEFT JOIN ck_field_access a ON a.person_id=p.id JOIN ck_attendance_days d ON d.person_id=p.id
   WHERE p.id=? AND p.enabled=1 AND COALESCE(a.enabled,1)=1 AND COALESCE(a.version,0)=? AND d.id=? AND d.day=? AND d.signed_out='' AND d.signed_in<=?`,s.user_id,s.grant_version,s.attendance_id,kstDay(new Date().toISOString()),new Date().toISOString()).first();
  if(!row){await q(env,'DELETE FROM ck_access_sessions WHERE token_hash=?',s.token_hash).run();return null;}
  return {...safeUser(s),name:row.name,badge:row.badge_id,attendance_id:s.attendance_id};
 }
 if(!await credentialCurrent(s,env)){await q(env,'DELETE FROM ck_access_sessions WHERE token_hash=?',s.token_hash).run();return null;}
 return safeUser(s);
}
export async function accessSessionAction(b,env,request){
 if(!['sop_login','sop_identity','sop_logout'].includes(b.action))return null;
 const scope=accessScope(request);await ensureAccess(env);
 if(b.action==='sop_identity')return env.SOP_REQUEST_USER?response({ok:true,user:env.SOP_REQUEST_USER}):denied();
 if(b.action==='sop_logout'){const raw=rawToken(request,scope);if(raw)await q(env,'DELETE FROM ck_access_sessions WHERE token_hash=? AND scope=?',await digest(raw),scope).run();return response({ok:true},200,{'Set-Cookie':cookie(scope,'',0)});}
 const bucket=await digest(scope+':'+(request.headers.get('CF-Connecting-IP')||'local')+':'+Math.floor(Date.now()/600000));
 const attempt=await q(env,'INSERT INTO ck_access_attempts VALUES(?,1,?) ON CONFLICT(bucket) DO UPDATE SET attempts=attempts+1 RETURNING attempts',bucket,Date.now()+600000).first();
 if(attempt.attempts>30)return denied('尝试次数较多，请稍后再试 / 잠시 후 다시 시도하세요',429);
 let u,attendance='',version=0,credential='';
 if(scope==='field'){
  const badge=String(b.badge||'').trim().split('|')[0].toUpperCase();
  if(!/^EMP-[A-Z0-9_-]{1,32}$/.test(badge))return denied('请扫描已授权的职员工牌；日当工牌用于加入作业 / 권한이 있는 직원 명찰을 스캔하세요',403);
  // Registered employees have field access by default. An explicit revocation
  // remains authoritative across edits, imports and deployments.
  const p=await q(env,`SELECT p.id,p.name,COALESCE(a.version,0) AS version FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id LEFT JOIN ck_field_access a ON a.person_id=p.id WHERE p.badge_id=? AND p.enabled=1 AND COALESCE(a.enabled,1)=1`,badge).first();
  if(!p)return denied('职员未登记、已停用或现场权限已关闭，请联系管理员 / 직원 등록·재직 상태와 현장 권한을 확인하세요',403);
  const day=await q(env,"SELECT id FROM ck_attendance_days WHERE person_id=? AND day=? AND signed_out='' AND signed_in<=?",p.id,kstDay(new Date().toISOString()),new Date().toISOString()).first();
  if(!day)return denied('今天尚未上班签到或已经签退，请先在签到点打卡 / 오늘 출근 등록을 먼저 해주세요',403);
  u={id:p.id,name:p.name,role:'dispatcher',departments:['bulk','direct_ship','import','other']};attendance=day.id;version=p.version;
 }else{
  u=await adminByCode(b.sop_key,env);if(!u)return denied('管理员授权码不正确 / 관리자 인증 코드를 확인하세요',403);credential=u.hash;
 }
 const raw=token(),seconds=scope==='kiosk'?30*86400:12*3600;
 const s={user_id:u.id,name:scope==='kiosk'?'签到点 / 출퇴근 등록':u.name,role:scope==='kiosk'?'kiosk':u.role,departments:JSON.stringify(u.departments||[]),scope};
 const old=rawToken(request,scope),statements=[q(env,'DELETE FROM ck_access_sessions WHERE expires_at<=? OR (token_hash=? AND scope=?)',Date.now(),await digest(old),scope),q(env,'DELETE FROM ck_access_attempts WHERE bucket=? OR expires_at<=?',bucket,Date.now()),q(env,'INSERT INTO ck_access_sessions VALUES(?,?,?,?,?,?,?,?,?,?,?)',await digest(raw),scope,s.user_id,s.name,s.role,s.departments,attendance,version,credential,Date.now()+seconds*1000,new Date().toISOString())];
 // A shared PDA / kiosk must not inherit an administrator's earlier browser session.
 const headers=new Headers({'Set-Cookie':cookie(scope,raw,seconds)});
 if(scope!=='office'){
  const office=rawToken(request,'office');if(office)statements.push(q(env,"DELETE FROM ck_access_sessions WHERE token_hash=? AND scope='office'",await digest(office)));
  headers.append('Set-Cookie',cookie('office','',0));
 }
 await env.DB.batch(statements);
 headers.set('Cache-Control','no-store');headers.set('Content-Type','application/json; charset=utf-8');
 return new Response(JSON.stringify({ok:true,user:safeUser(s)}),{headers});
}

// A field session has no office, employee-registration, import, reset or billing authority.
export const FIELD_ACTIONS=new Set(`
sop_identity sop_logout sop_login sop_native_start sop_native_people sop_dispatch_list sop_field_resolve sop_list sop_get sop_linked sop_source_detail sop_updates sop_check_for_batch
sop_task_dispatch sop_task_start sop_task_pause sop_task_people sop_task_complete_review sop_task_review sop_task_finish
sop_issue_adopt sop_issue_ack sop_issue_append sop_issue_feedback sop_check_scan sop_check_resolve sop_check_round sop_check_close
sop_courier_config sop_courier_list sop_courier_detail sop_courier_receive sop_courier_update sop_courier_handover
sop_attendance_config sop_attendance_lookup sop_attendance_summary sop_attendance_break_start sop_attendance_break_end
v2_inbound_plan_list v2_inbound_plan_detail v2_inbound_plan_ops_candidates v2_inbound_plan_find_by_code v2_inbound_resolve_code
v2_outbound_order_list v2_outbound_order_detail v2_outbound_order_resolve_code v2_outbound_stock_op_list v2_outbound_order_ack_change v2_outbound_pickup_confirm
v2_ops_job_detail v2_ops_job_leave v2_ops_job_resume v2_ops_job_finish v2_ops_my_active_job
v2_unload_job_finish v2_unplanned_unload_finish v2_unplanned_unload_active_list
v2_inbound_job_finish v2_import_delivery_job_finish v2_outbound_load_finish v2_outbound_stock_op_finish v2_bulk_op_job_finish
v2_pick_doc_lookup v2_pick_job_active_list v2_pick_job_docs_list v2_pick_job_add_docs v2_pick_job_finish v2_pick_job_finalize
v2_issue_ops_list v2_issue_tickets v2_issue_detail v2_issue_create v2_issue_handle_finish v2_issue_handle_resume
v2_verify_batch_detail v2_verify_batch_list v2_verify_job_finish v2_verify_scan_submit
v2_attachment_list v2_attachment_upload v2_correction_request_create
`.trim().split(/\s+/));
const KIOSK_ACTIONS=new Set('sop_identity sop_logout sop_login sop_attendance_config sop_attendance_employee_search sop_attendance_lookup sop_attendance_search sop_attendance_checkin sop_attendance_checkout sop_attendance_company sop_attendance_print'.split(' '));
export function accessGuard(b,env,request){
 if(!accessEnabled(env))return null;
 const url=new URL(request.url),scope=accessScope(request),action=String(b.action||'').trim();
 if(request.method!=='POST'||request.headers.get('Origin')&&request.headers.get('Origin')!==url.origin||request.headers.get('Sec-Fetch-Site')==='cross-site')return denied('请从本系统页面操作 / 시스템 화면에서 조작하세요',403);
 if(!request.headers.get('Content-Type')?.includes('application/json')&&!request.headers.get('Content-Type')?.includes('multipart/form-data'))return denied('Invalid content type',415);
 if(['sop_login','sop_identity','sop_logout'].includes(action))return null;
 const u=env.SOP_REQUEST_USER;if(!u||u.scope!==scope)return denied();
 if(scope==='field'){
  if(!FIELD_ACTIONS.has(action))return denied('现场工牌无此权限 / 현장 권한으로 이용할 수 없습니다',403);
  if(action==='sop_list'&&!['need','task','issue','check'].includes(b.kind))return denied('无此权限',403);
  if(action==='sop_attendance_summary'&&b.date&&b.date!==kstDay(new Date().toISOString()))return denied('现场只显示当天人员 / 오늘 인원만 조회할 수 있습니다',403);
  if(action==='v2_attachment_list'&&!['inbound_plan','outbound_order','field_feedback','ops_job','issue_ticket','sop_task','sop_need'].includes(b.related_doc_type))return denied('无此附件权限',403);
 }
 if(scope==='kiosk'&&!KIOSK_ACTIONS.has(action))return denied('签到点仅支持打卡和工牌操作 / 출퇴근 등록만 이용할 수 있습니다',403);
 return null;
}
export async function accessAdminAction(b,env){
 if(!b.action?.startsWith('sop_access_'))return null;
 const u=env.SOP_REQUEST_USER;if(u?.scope!=='office'||u.role!=='manager')return denied('仅办公室管理员可授权',403);
 if(b.action==='sop_access_list')return response({ok:true,items:(await q(env,`SELECT p.id AS person_id,
  CASE WHEN p.enabled=1 THEN COALESCE(a.enabled,1) ELSE 0 END AS enabled,
  COALESCE(a.version,0) AS version,CASE WHEN a.person_id IS NULL THEN 1 ELSE 0 END AS default_grant,
  COALESCE(a.updated_by,'') AS updated_by,COALESCE(a.updated_at,'') AS updated_at
  FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id LEFT JOIN ck_field_access a ON a.person_id=p.id`).all()).results});
 if(b.action!=='sop_access_update')return denied('Unknown action',404);
 if(typeof b.enabled!=='boolean'||!Number.isSafeInteger(b.version)||b.version<0)return denied('权限设置无效',400);
 const p=await q(env,'SELECT p.* FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id WHERE p.id=?',b.person_id).first();
 if(!p||b.enabled&&!p.enabled)return denied('请先登记在职职员',400);
 const old=await q(env,'SELECT * FROM ck_field_access WHERE person_id=?',p.id).first();if((old?.version||0)!==b.version)return denied('权限已变化，请刷新后重试',409);
 const t=new Date().toISOString(),next=b.version+1;
 try{await env.DB.batch([
  q(env,"INSERT INTO ck_access_events VALUES(?,?,?,?,?,?)",p.id+':'+next,p.id,b.enabled?'field_grant':'field_revoke',u.id,JSON.stringify({before:!!old?.enabled,after:b.enabled,version:next}),t),
  q(env,'INSERT INTO ck_field_access VALUES(?,?,?,?,?) ON CONFLICT(person_id) DO UPDATE SET enabled=excluded.enabled,version=excluded.version,updated_by=excluded.updated_by,updated_at=excluded.updated_at',p.id,b.enabled?1:0,next,u.id,t),
  q(env,"DELETE FROM ck_access_sessions WHERE user_id=? AND scope='field'",p.id)
 ]);}catch{return denied('权限已变化，请刷新后重试',409);}
 return response({ok:true,version:next});
}
export async function accessFileAllowed(request,env,key){
 const u=await accessUser(request,env);if(!u||u.scope==='kiosk')return false;
 if(u.scope==='office')return true;
 const a=await q(env,'SELECT related_doc_type FROM v2_attachments WHERE file_key=?',key).first();
 return !!a&&['inbound_plan','outbound_order','field_feedback','ops_job','issue_ticket','sop_task','sop_need'].includes(a.related_doc_type);
}
