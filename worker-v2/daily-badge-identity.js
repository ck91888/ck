import {canonicalDailyBadge,isSystemDailyBadge} from '../shared/badge-identity.js';
import {ensureSchema} from './schema-ready.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results||[];
const enabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_ATTENDANCE_ENABLED==='true';
const conflict=()=>{throw Error('工牌大小写身份记录冲突，请负责人核对 / 명찰 식별 기록이 충돌합니다');};
const dateSQL=x=>`substr(${x},4,4)||'-'||substr(${x},8,2)||'-'||substr(${x},10,2)`;
const dailySQL=x=>`length(${x})=20 AND lower(substr(${x},1,3))='da-' AND substr(${x},4,8) NOT GLOB '*[^0-9]*' AND substr(${x},12,1)='-' AND substr(${x},13,8) NOT GLOB '*[^0-9A-Fa-f]*' AND date(${dateSQL(x)},'+0 days')=(${dateSQL(x)})`;
const canonicalSQL=x=>`CASE WHEN ${dailySQL(x)} THEN upper(${x}) ELSE ${x} END`;
// Each input appears once, including a bound placeholder. Never apply NOCASE to
// custom DA, DAF, or EMP identities.
export const canonicalWorkerSQL=expr=>`(SELECT ${canonicalSQL('ck_badge_value')} FROM (SELECT ${expr} AS ck_badge_value))`;
export const workerBadgeMatch=(left,right)=>`(SELECT ${canonicalSQL('ck_badge_left')}=${canonicalSQL('ck_badge_right')} FROM (SELECT ${left} AS ck_badge_left,${right} AS ck_badge_right))`;

export async function resolveDailyBadge(env,value,{day}={}){
 const raw=String(value??''),id=canonicalDailyBadge(raw);
 if(!enabled(env)||!isSystemDailyBadge(raw))return {id:raw,person:null,attendance:null};
 const people=await rows(env,`SELECT * FROM ck_attendance_people p WHERE ${workerBadgeMatch('p.badge_id','?')}`,id);
 if(people.length>1||people.length===1&&people[0].kind!=='daily')conflict();
 let attendance=null;
 if(day){const days=await rows(env,`SELECT * FROM ck_attendance_days d WHERE d.day=? AND ${workerBadgeMatch('d.worker_id','?')}`,day,id);
  if(days.length>1||days.length===1&&(!people[0]||days[0].person_id!==people[0].id))conflict();attendance=days[0]||null;}
 return {id,person:people[0]||null,attendance};
}

export async function normalizeDailyBadgeRequest(env,body){
 if(!enabled(env))return body;
 env.SOP_BADGE_REQUEST_ORIGINAL=structuredClone(body);
 const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10),seen=new Map();
 const resolve=async value=>{if(typeof value!=='string'||!isSystemDailyBadge(value))return value;if(!seen.has(value))seen.set(value,(await resolveDailyBadge(env,value,{day})).id);return seen.get(value);};
 const visit=async object=>{
  if(!object||typeof object!=='object')return;
  for(const key of ['worker_id','handler_id','lead_id'])if(key in object)object[key]=await resolve(object[key]);
  if(Array.isArray(object.workers)){for(const person of object.workers)if(person&&typeof person==='object')person.id=await resolve(person.id);if(new Set(object.workers.map(w=>w?.id)).size!==object.workers.length)throw Error('同一工牌重复，请保留一名人员 / 동일 명찰이 중복되었습니다');}
  if(Array.isArray(object.borrow_confirmations))for(const row of object.borrow_confirmations)if(row&&typeof row==='object')row.worker_id=await resolve(row.worker_id);
 };
 await visit(body);await visit(body.payload);return body;
}

export async function dailyIdentityMap(env,records){
 const identities=new Map(),people=await rows(env,'SELECT id,badge_id,kind FROM ck_attendance_people');
 for(const record of records){const id=canonicalDailyBadge(record.worker_id);
  if(!isSystemDailyBadge(record.worker_id)){identities.set(record.id,{id,conflict:false});continue;}
  const registered=people.filter(p=>canonicalDailyBadge(p.badge_id)===id),days=records.filter(d=>d.day===record.day&&canonicalDailyBadge(d.worker_id)===id);
  identities.set(record.id,{id,conflict:registered.length!==1||registered[0].kind!=='daily'||registered[0].id!==record.person_id||days.length!==1});
 }
 return identities;
}

const managed=x=>`(lower(${x}) GLOB 'da-*' OR lower(${x}) GLOB 'daf-*' OR lower(${x}) GLOB 'emp-*')`;
const validAttendance=`SELECT d.id FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id
 WHERE ${workerBadgeMatch('d.worker_id','NEW.worker_id')} AND ${workerBadgeMatch('p.badge_id','d.worker_id')}
 AND d.day=date(NEW.joined_at,'+9 hours') AND d.signed_out='' AND julianday(d.signed_in)<=julianday(NEW.joined_at)
 AND p.enabled=1 AND (NOT (${dailySQL('NEW.worker_id')}) OR p.kind='daily') AND (lower(NEW.worker_id) NOT GLOB 'emp-*' OR EXISTS(SELECT 1 FROM ck_employee_profiles e WHERE e.person_id=p.id))
 AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=d.id AND b.ended_at='')
 AND NOT EXISTS(SELECT 1 FROM ck_attendance_management m WHERE m.attendance_id=d.id AND m.voided_at!='')`;
export const DAILY_BADGE_IDENTITY_SCHEMA=[
 'DROP TRIGGER IF EXISTS ck_attendance_guard_join',
 'DROP TRIGGER IF EXISTS ck_daily_badge_join_insert','DROP TRIGGER IF EXISTS ck_daily_badge_join_update',
 ...['INSERT','UPDATE'].map(operation=>`CREATE TRIGGER IF NOT EXISTS ck_daily_badge_join_${operation.toLowerCase()} BEFORE ${operation} ON v2_ops_job_workers
 WHEN NEW.left_at='' AND ${managed('NEW.worker_id')}${operation==='UPDATE'?" AND (OLD.left_at IS NOT '' OR OLD.worker_id IS NOT NEW.worker_id OR OLD.job_id IS NOT NEW.job_id OR OLD.joined_at IS NOT NEW.joined_at)":''}
 BEGIN
 SELECT CASE WHEN lower(NEW.worker_id) GLOB 'emp-*' AND NOT EXISTS(SELECT 1 FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id WHERE p.badge_id=NEW.worker_id AND p.enabled=1) THEN RAISE(ABORT,'Employee disabled or unregistered') END;
 SELECT CASE WHEN lower(NEW.worker_id) GLOB 'daf-*' AND NOT EXISTS(SELECT 1 FROM ck_attendance_people p WHERE p.badge_id=NEW.worker_id AND p.kind='permanent' AND p.enabled=1) THEN RAISE(ABORT,'Permanent badge disabled or unregistered') END;
 SELECT CASE WHEN lower(NEW.worker_id) GLOB 'emp-*' AND NOT EXISTS(${validAttendance}) THEN RAISE(ABORT,'Employee attendance required: signed out, resting, voided, or not signed in') END;
 SELECT CASE WHEN (SELECT COUNT(*) FROM ck_attendance_people p WHERE ${workerBadgeMatch('p.badge_id','NEW.worker_id')})!=1 OR (SELECT COUNT(*) FROM ck_attendance_days d WHERE d.day=date(NEW.joined_at,'+9 hours') AND ${workerBadgeMatch('d.worker_id','NEW.worker_id')})!=1 THEN RAISE(ABORT,'Attendance badge identity missing or ambiguous') END;
 SELECT CASE WHEN NOT EXISTS(${validAttendance}) THEN RAISE(ABORT,'Attendance required: signed out, resting, voided, or not signed in') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.left_at='' AND w.id!=NEW.id AND ${workerBadgeMatch('w.worker_id','NEW.worker_id')}) THEN RAISE(ABORT,'Attendance worker already assigned') END;
 END`)
];
export async function ensureDailyBadgeIdentity(env){if(enabled(env))await ensureSchema(env.DB,'daily-badge-identity-v2',()=>env.DB.batch(DAILY_BADGE_IDENTITY_SCHEMA.map(sql=>q(env,sql))));}
