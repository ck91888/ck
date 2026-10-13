import {ensureSchema} from './schema-ready.js';
import {kstDay} from './attendance-time.js';
import {activeAttendanceSQL} from './attendance-management.js';
import {resolveDailyBadge} from './daily-badge-identity.js';
import {canonicalDailyBadge,sameWorkerBadge} from '../shared/badge-identity.js';
import {startDepartment,laborDepartment} from '../shared/labor-department.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const fail=m=>{throw Error(m);},scopes=['bulk','direct_ship','import'];
export const dailyDispatchEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_ACCESS_CONTROL==='true'&&e.SOP_ATTENDANCE_ENABLED==='true';
export async function ensureDailyDispatch(e){await ensureSchema(e.DB,'daily-dispatch-v1',()=>e.DB.batch([
 q(e,`CREATE TABLE IF NOT EXISTS ck_daily_dispatch_grants(person_id TEXT PRIMARY KEY,enabled INTEGER NOT NULL,departments TEXT NOT NULL,version INTEGER NOT NULL,updated_by TEXT NOT NULL,updated_name TEXT NOT NULL,updated_at TEXT NOT NULL)`),
 q(e,`CREATE TABLE IF NOT EXISTS ck_daily_dispatch_links(daily_person_id TEXT PRIMARY KEY,permanent_person_id TEXT NOT NULL,day TEXT NOT NULL,version INTEGER NOT NULL,created_by TEXT NOT NULL,created_name TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(permanent_person_id,day))`),
 q(e,`CREATE TABLE IF NOT EXISTS ck_daily_dispatch_events(request_id TEXT PRIMARY KEY,person_id TEXT NOT NULL,action TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,at TEXT NOT NULL,before_json TEXT NOT NULL,after_json TEXT NOT NULL,reason TEXT NOT NULL,signature TEXT NOT NULL,response_json TEXT NOT NULL)`),
 q(e,`CREATE TABLE IF NOT EXISTS ck_daily_dispatch_checks(id TEXT PRIMARY KEY,allowed INTEGER NOT NULL CHECK(allowed=1))`)
]));}
const stable=(e,id)=>q(e,"SELECT * FROM ck_attendance_people WHERE id=? AND kind='permanent' AND badge_id GLOB 'DAF-*'",id).first();
const pub=p=>({id:p.id,badge:p.badge_id,name:p.name,agency:p.agency,enabled:!!p.enabled});
const grantPublic=g=>({enabled:!!g?.enabled,departments:g?JSON.parse(g.departments):[],version:g?.version||0,updatedBy:g?.updated_by||'',updatedName:g?.updated_name||'',updatedAt:g?.updated_at||''});
async function dayRows(e,personId,day){return rows(e,`SELECT d.*,p.enabled AS person_enabled FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE d.person_id=? AND d.day=? AND ${activeAttendanceSQL('d')}`,personId,day);}
async function linkedDay(e,p,day){
 const direct=await dayRows(e,p.id,day),links=await rows(e,'SELECT daily_person_id FROM ck_daily_dispatch_links WHERE permanent_person_id=? AND day=?',p.id,day);
 let all=[...direct];for(const link of links)all.push(...await dayRows(e,link.daily_person_id,day));
 const active=all.filter(d=>!d.signed_out);if(active.length>1)fail('长期与临时工牌存在多份有效签到，请先核实 / 중복 출근 기록을 확인하세요');return active[0]||all[0]||null;
}
async function validAttendance(e,p,d,t){if(!(p?.enabled&&d?.person_enabled&&d.day===kstDay(t)&&!d.signed_out&&d.signed_in<=t))return false;const actual=await linkedDay(e,p,d.day).catch(()=>null);return actual?.id===d.id&&!await q(e,"SELECT 1 FROM ck_attendance_breaks WHERE attendance_id=? AND ended_at=''",d.id).first();}
export async function dailyFieldLogin(e,value){
 await ensureDailyDispatch(e);const badge=canonicalDailyBadge(String(value||'').trim().split('|')[0]),day=kstDay(new Date().toISOString());let p,d;
 if(badge.startsWith('DAF-')){p=await q(e,"SELECT * FROM ck_attendance_people WHERE badge_id=? AND kind='permanent'",badge).first();if(p)d=await linkedDay(e,p,day);}
 else{const found=await resolveDailyBadge(e,badge,{day});if(found.person){const link=await q(e,'SELECT * FROM ck_daily_dispatch_links WHERE daily_person_id=? AND day=?',found.person.id,day).first();if(link){p=await stable(e,link.permanent_person_id);if(p)d=await linkedDay(e,p,day);if(d?.person_id!==found.person.id)d=null;}}}
 if(!p||!d)fail('日当派工须先由管理员授权长期档案；临时牌须明确关联本人档案，并完成当天签到 / 고정 인물 권한·임시 명찰 연결·당일 출근을 확인하세요');
 const g=await q(e,'SELECT * FROM ck_daily_dispatch_grants WHERE person_id=?',p.id).first(),valid=await validAttendance(e,p,d,new Date().toISOString());
 const unfinished=await q(e,"SELECT 1 FROM sop_records s JOIN v2_ops_jobs j ON j.id=s.id WHERE s.kind IN ('dispatch','task') AND json_extract(s.state,'$.owner_id')=? AND j.status IN ('pending','working','paused','awaiting_close') LIMIT 1",p.id).first();
 if((!g?.enabled||!valid)&&!unfinished)fail('派工权限未开启，或工牌/当天签到无效 / 배정 권한 또는 당일 출근 상태를 확인하세요');
 if(!g)fail('派工权限默认关闭，请联系管理员 / 관리자에게 권한을 요청하세요');
 return {id:p.id,name:p.name,role:'dispatcher',departments:JSON.parse(g.departments),attendance_id:d.id,grant_version:g.version,badge:d.worker_id,daily_dispatch:true,can_dispatch:!!g.enabled&&valid};
}
export async function dailyFieldSession(e,s){
 await ensureDailyDispatch(e);const p=await stable(e,s.user_id);if(!p)return null;
 const d=await q(e,`SELECT d.*,p.enabled AS person_enabled FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE d.id=? AND ${activeAttendanceSQL('d')}`,s.attendance_id).first();
 if(!d||d.person_id!==p.id&&!await q(e,'SELECT 1 FROM ck_daily_dispatch_links WHERE daily_person_id=? AND permanent_person_id=? AND day=?',d.person_id,p.id,d.day).first())return null;
 const g=await q(e,'SELECT * FROM ck_daily_dispatch_grants WHERE person_id=?',p.id).first();
 return {id:p.id,name:p.name,role:'dispatcher',scope:'field',departments:[...new Set([...JSON.parse(s.departments),...JSON.parse(g?.departments||'[]')])],badge:d.worker_id,attendance_id:d.id,daily_dispatch:true,can_dispatch:!!g?.enabled&&await validAttendance(e,p,d,new Date().toISOString()),dispatch_departments:JSON.parse(g?.departments||'[]'),dispatch_version:g?.version||0};
}
async function temporary(e,b,p){
 const day=kstDay(new Date().toISOString()),found=await resolveDailyBadge(e,canonicalDailyBadge(String(b.daily_badge||'').trim().split('|')[0]),{day});
 if(!found.person||found.person.kind!=='daily'||!found.attendance)fail('请扫描当天实际临时工牌，不能按姓名关联 / 오늘 임시 명찰을 스캔하세요');
 const d=(await dayRows(e,found.person.id,day)).find(x=>x.id===found.attendance.id);
 if(!d||!d.person_enabled||d.signed_out||!p.enabled)fail('工牌停用、已签退或签到已删除，请先核实');
 const direct=await dayRows(e,p.id,day);if(direct.some(x=>!x.signed_out))fail('长期档案今天已签到，请先核实重复签到，不能合并历史记录');
 const old=await q(e,'SELECT * FROM ck_daily_dispatch_links WHERE daily_person_id=? OR (permanent_person_id=? AND day=?)',found.person.id,p.id,day).first();
 if(old&&old.permanent_person_id!==p.id||old&&old.daily_person_id!==found.person.id)fail('此临时牌或长期档案当天已有其他关联，请核实；不能覆盖历史关联');
 return {person:found.person,day:d,old};
}
export async function dailyDispatchAdmin(b,e){
 const u=e.SOP_REQUEST_USER;if(u?.scope!=='office'||u.role!=='manager')fail('仅办公室管理员可管理派工权限 / 사무실 관리자만 변경할 수 있습니다');
 await ensureDailyDispatch(e);
 if(b.action==='sop_daily_dispatch_list'){
  const people=await rows(e,"SELECT * FROM ck_attendance_people WHERE kind='permanent' AND badge_id GLOB 'DAF-*' ORDER BY name,badge_id"),grants=await rows(e,'SELECT * FROM ck_daily_dispatch_grants'),links=await rows(e,'SELECT l.*,p.name,p.badge_id FROM ck_daily_dispatch_links l JOIN ck_attendance_people p ON p.id=l.daily_person_id ORDER BY l.created_at DESC');
  return {ok:true,items:people.map(p=>({...pub(p),grant:grantPublic(grants.find(g=>g.person_id===p.id)),links:links.filter(l=>l.permanent_person_id===p.id)}))};
 }
 const p=await stable(e,String(b.person_id||''));if(!p)fail('长期档案不存在，请刷新');
 if(b.action==='sop_daily_dispatch_history')return {ok:true,items:(await rows(e,'SELECT action,actor_name,at,before_json,after_json,reason FROM ck_daily_dispatch_events WHERE person_id=? ORDER BY at DESC LIMIT 100',p.id)).map(x=>({...x,before:JSON.parse(x.before_json),after:JSON.parse(x.after_json)}))};
 if(b.action==='sop_daily_dispatch_link_preview'){const x=await temporary(e,b,p);return {ok:true,permanent:pub(p),daily:pub(x.person),day:x.day.day,attendance_id:x.day.id,linked:!!x.old};}
 if(!['sop_daily_dispatch_update','sop_daily_dispatch_link'].includes(b.action))fail('未知权限操作');
 if(b.badge!==p.badge_id)fail('工牌与档案不一致，请刷新');
 if(u.id===p.id)fail('不能自授权');
 const reason=String(b.reason||'').trim();if(!reason||reason.length>500)fail('请填写1–500字核实原因');
 const request=String(b.client_req_id||'');if(!request||request.length>100)fail('缺少请求编号');
 const signature=JSON.stringify({actor:u.id,...Object.fromEntries(Object.entries(b).filter(([k])=>!['client_req_id','sop_key'].includes(k)).sort(([a],[b])=>a.localeCompare(b)))});
 const cached=await q(e,'SELECT * FROM ck_daily_dispatch_events WHERE request_id=?',request).first();if(cached){if(cached.signature!==signature)fail('请求编号已用于其他内容');return JSON.parse(cached.response_json);}
 const t=new Date().toISOString(),sql=[];let before,after;
 if(b.action==='sop_daily_dispatch_update'){
  before=await q(e,'SELECT * FROM ck_daily_dispatch_grants WHERE person_id=?',p.id).first();if(typeof b.enabled!=='boolean'||!Number.isSafeInteger(b.version)||b.version!==(before?.version||0))fail('权限版本已变化，请刷新后重试');
  const ds=Array.isArray(b.departments)?[...new Set(b.departments)].sort():[];if(ds.some(d=>!scopes.includes(d))||b.enabled&&(!ds.length||!p.enabled))fail('请启用长期工牌并明确选择派工部门');
  after={person_id:p.id,enabled:b.enabled?1:0,departments:JSON.stringify(ds),version:b.version+1,updated_by:u.id,updated_name:u.name,updated_at:t};
  sql.push(q(e,`INSERT INTO ck_daily_dispatch_grants VALUES(?,?,?,?,?,?,?) ON CONFLICT(person_id) DO UPDATE SET enabled=excluded.enabled,departments=excluded.departments,version=CASE WHEN ck_daily_dispatch_grants.version=? THEN excluded.version ELSE NULL END,updated_by=excluded.updated_by,updated_name=excluded.updated_name,updated_at=excluded.updated_at`,...Object.values(after),b.version));
  // The initial INSERT is also guarded against a concurrently created first grant.
  sql.unshift(q(e,`INSERT INTO ck_daily_dispatch_checks VALUES(?,CASE WHEN COALESCE((SELECT version FROM ck_daily_dispatch_grants WHERE person_id=?),0)=? AND (?=0 OR EXISTS(SELECT 1 FROM ck_attendance_people WHERE id=? AND enabled=1)) THEN 1 ELSE 0 END)`,request,p.id,b.version,b.enabled?1:0,p.id));
 }else{
  const x=await temporary(e,b,p);if(b.daily_person_id!==x.person.id||b.attendance_id!==x.day.id)fail('临时牌信息已变化，请重新核对');if(x.old)fail('此临时工牌已明确关联，无需重复保存');before={};after={daily_person_id:x.person.id,permanent_person_id:p.id,day:x.day.day,version:1,created_by:u.id,created_name:u.name,created_at:t};
  sql.push(q(e,`INSERT INTO ck_daily_dispatch_links VALUES(?,?,?,?,?,?,?)`,...Object.values(after)));
  sql.unshift(q(e,`INSERT INTO ck_daily_dispatch_checks VALUES(?,CASE WHEN EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE d.id=? AND d.person_id=? AND d.day=? AND d.signed_out='' AND p.enabled=1 AND ${activeAttendanceSQL('d')}) AND EXISTS(SELECT 1 FROM ck_attendance_people WHERE id=? AND enabled=1) AND NOT EXISTS(SELECT 1 FROM ck_attendance_days d WHERE d.person_id=? AND d.day=? AND d.signed_out='' AND ${activeAttendanceSQL('d')}) THEN 1 ELSE 0 END)`,request,x.day.id,x.person.id,x.day.day,p.id,p.id,x.day.day));
 }
 const response={ok:true,person_id:p.id,version:after.version};sql.push(q(e,'INSERT INTO ck_daily_dispatch_events VALUES(?,?,?,?,?,?,?,?,?,?,?)',request,p.id,b.action,u.id,u.name,t,JSON.stringify(before||{}),JSON.stringify(after),reason,signature,JSON.stringify(response)),q(e,'DELETE FROM ck_daily_dispatch_checks WHERE id=?',request));
 try{await e.DB.batch(sql);}catch{const saved=await q(e,'SELECT signature,response_json FROM ck_daily_dispatch_events WHERE request_id=?',request).first();if(saved?.signature===signature)return JSON.parse(saved.response_json);fail('资料或权限已变化，请刷新后重试 / 정보가 변경되었습니다. 새로고침하세요');}return response;
}

// Only new dispatch/crew expansion consumes the grant. Existing crews can pause,
// resume and finish after revocation; these existing-task handlers still enforce ownership.
export async function guardDailyDispatch(b,e){
 const u=e.SOP_REQUEST_USER;if(!dailyDispatchEnabled(e)||!u?.daily_dispatch)return;
 let needs=false,department='',record;
 if(b.action==='sop_native_start'){needs=true;department=b.labor_department||startDepartment(b.payload||{});}
 else if(b.action==='sop_task_dispatch'){needs=true;department=b.department;}
 else if(b.action==='sop_courier_receive'&&b.operation==='start'){needs=true;department='direct_ship';}
 else if(['sop_native_adopt','v2_pick_job_add_docs'].includes(b.action)){needs=true;}
 const legacyResume=b.action==='v2_ops_job_resume',submitted=legacyResume?[{id:b.worker_id}]:b.workers||[];
 const people=legacyResume||['sop_native_people','sop_native_resume','sop_task_people'].includes(b.action)||b.action==='sop_courier_receive'&&b.operation==='people';
 if(people||needs&&!department){record=await q(e,"SELECT * FROM sop_records WHERE id=? AND kind IN ('dispatch','task')",b.job_id||b.parent_job_id||b.batch_id||b.id||'').first();department=record?.department;if(people){const crew=record?JSON.parse(record.state).workers||[]:[];needs=submitted.some(w=>!crew.some(old=>sameWorkerBadge(w.id,old.id)));}}
 if(!needs)return;
 if(!department&&(b.action==='sop_native_adopt'||legacyResume)){const job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',b.job_id||b.parent_job_id).first();if(job)department=laborDepartment(job);}
 await ensureDailyDispatch(e);const g=await q(e,'SELECT * FROM ck_daily_dispatch_grants WHERE person_id=?',u.id).first();
 if(!u.can_dispatch||!g?.enabled||!JSON.parse(g.departments).includes(department))fail('派工权限未开启、已撤销、部门未授权或当天签到无效；已有任务仍可收尾 / 배정 권한·부서·출근 상태를 확인하세요');
 const original=e.DB,version=g.version;
 // Recheck admission within every transaction of this new dispatch. A revoke,
 // sign-out or disable racing the request rolls the entire business batch back.
 const guardedBatch=async statements=>{const t=new Date().toISOString(),id=crypto.randomUUID();const guard=original.prepare(`INSERT INTO ck_daily_dispatch_checks VALUES(?,CASE WHEN EXISTS(
 SELECT 1 FROM ck_daily_dispatch_grants g JOIN ck_attendance_people p ON p.id=g.person_id JOIN ck_attendance_days d ON d.id=? JOIN ck_attendance_people a ON a.id=d.person_id
 WHERE g.person_id=? AND g.enabled=1 AND g.version=? AND p.enabled=1 AND a.enabled=1 AND EXISTS(SELECT 1 FROM json_each(g.departments) WHERE value=?)
 AND d.day=? AND d.signed_out='' AND d.signed_in<=? AND ${activeAttendanceSQL('d')} AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks r WHERE r.attendance_id=d.id AND r.ended_at='')
 AND NOT EXISTS(SELECT 1 FROM ck_attendance_days other WHERE other.id!=d.id AND other.day=d.day AND other.signed_out='' AND ${activeAttendanceSQL('other')} AND (other.person_id=p.id OR EXISTS(SELECT 1 FROM ck_daily_dispatch_links duplicate WHERE duplicate.daily_person_id=other.person_id AND duplicate.permanent_person_id=p.id AND duplicate.day=other.day)))
 AND (d.person_id=p.id OR EXISTS(SELECT 1 FROM ck_daily_dispatch_links l WHERE l.daily_person_id=d.person_id AND l.permanent_person_id=p.id AND l.day=d.day))) THEN 1 ELSE 0 END)`).bind(id,u.attendance_id,u.id,version,department,kstDay(t),t);
 try{const result=await original.batch([guard,...statements,original.prepare('DELETE FROM ck_daily_dispatch_checks WHERE id=?').bind(id)]);return result.slice(1,-1);}catch(error){if(/CHECK constraint failed: allowed/.test(error.message))fail('派工权限或签到刚刚变化，本次未派工，请刷新 / 권한·출근 상태가 변경되었습니다');throw error;}};
 // Legacy additions sometimes use run(); guard those writes as well. Reads stay unchanged.
 e.DB={prepare(sql){const statement=original.prepare(sql);const wrap=s=>new Proxy(s,{get(target,key){if(key==='bind')return (...args)=>wrap(target.bind(...args));if(key==='run')return async()=> (await guardedBatch([target]))[0];const value=target[key];return typeof value==='function'?value.bind(target):value;}});return wrap(statement);},batch:guardedBatch};
}
