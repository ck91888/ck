// Explicit read model of the current attendance child; original punch evidence stays in the parent.
import {ensureSchema,shareSchemaBinding} from './schema-ready.js';
import {attendanceReturnEnabled,attendanceReturnsRequested,ensureAttendanceReturnSchema} from './attendance-return-schema.js';
export const attendanceCutoffSQL=t=>`strftime('%Y-%m-%dT%H:%M:%fZ',julianday(${t}),'+5 hours','start of day','+1 day','-5 hours')`;
export const attendanceSource=e=>attendanceReturnEnabled(e)?'ck_attendance_current_days':'ck_attendance_days';
export const attendanceDaySQL=(e,d,day)=>attendanceReturnEnabled(e)?`date(${day}) BETWEEN date(${d}.signed_in,'+9 hours') AND date(${attendanceCutoffSQL(d+'.signed_in')},'+9 hours','-0.001 seconds')`:`${d}.day=${day}`;
export const attendanceAtSQL=(e,d,at)=>attendanceReturnEnabled(e)?`${d}.signed_out='' AND ${d}.span_status='open' AND julianday(${d}.signed_in)<=julianday(${at}) AND julianday(${attendanceCutoffSQL(d+'.signed_in')})>julianday(${at})`:`${d}.signed_out='' AND julianday(${d}.signed_in)<=julianday(${at})`;
const configDDL='CREATE TABLE IF NOT EXISTS ck_attendance_return_config(id INTEGER PRIMARY KEY CHECK(id=1),enabled INTEGER NOT NULL CHECK(enabled IN (0,1)))';
const configSQL='INSERT INTO ck_attendance_return_config VALUES(1,?) ON CONFLICT(id) DO UPDATE SET enabled=excluded.enabled WHERE enabled<>excluded.enabled';
export async function ensureAttendanceCurrentView(e){
 // Dispatch initialization may create an empty child schema with the switch off;
 // that is metadata only and never generates historical attendance or labor.
 await ensureAttendanceReturnSchema({...e,SOP_ATTENDANCE_RETURNS_ENABLED:'true'});
 await ensureSchema(e.DB,'attendance-current-view-v4',()=>e.DB.batch([
 e.DB.prepare('CREATE TABLE IF NOT EXISTS ck_attendance_return_migration_guard(id INTEGER PRIMARY KEY CHECK(id=1))'),e.DB.prepare(configDDL),e.DB.prepare('INSERT OR IGNORE INTO ck_attendance_return_config VALUES(1,0)'),e.DB.prepare('DROP VIEW IF EXISTS ck_attendance_current_days'),
 e.DB.prepare(`CREATE VIEW IF NOT EXISTS ck_attendance_current_days AS
 SELECT d.*,d.day AS origin_day,d.person_id AS origin_person_id,'' AS attendance_span_id,CASE WHEN d.signed_out='' THEN 'open' ELSE 'closed' END AS span_status FROM ck_attendance_return_config cfg CROSS JOIN ck_attendance_days d WHERE cfg.id=1 AND cfg.enabled=0
 UNION ALL
 SELECT d.id,s.source_person_id AS person_id,s.badge_id AS worker_id,d.name,d.agency,date(s.started_at,'+9 hours') AS day,d.identity_key,s.started_at AS signed_in,CASE WHEN s.status='pending_review' AND s.ended_at='' THEN 'pending_review' ELSE s.ended_at END AS signed_out,d.version,d.day AS origin_day,d.person_id AS origin_person_id,s.id AS attendance_span_id,s.status AS span_status
 FROM ck_attendance_return_config cfg CROSS JOIN ck_attendance_spans s JOIN ck_attendance_days d ON d.id=s.attendance_id
 WHERE cfg.id=1 AND cfg.enabled=1 AND s.ordinal=(SELECT MAX(latest.ordinal) FROM ck_attendance_spans latest WHERE latest.attendance_id=d.id)`)

 ]));
}
export async function attendanceReturnEnvironment(e){
 const next={...e,SOP_ATTENDANCE_RETURNS_COMPAT:'false'};
 if(next.SOP_ENVIRONMENT!=='staging'||next.SOP_ATTENDANCE_ENABLED!=='true'||next.SOP_ACCESS_CONTROL!=='true')return next;
 let configExists,currentConfig;
 if(!attendanceReturnsRequested(next)){
  const tables=new Set((await e.DB.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ck_attendance_spans','ck_attendance_return_config','ck_attendance_span_events')").all()).results.map(r=>r.name));configExists=tables.has('ck_attendance_return_config');
  if(!configExists&&!tables.has('ck_attendance_spans'))return next;
  const evidence=await e.DB.prepare(`SELECT ${configExists?'(SELECT enabled FROM ck_attendance_return_config WHERE id=1)':'NULL'} AS enabled,${tables.has('ck_attendance_spans')?"EXISTS(SELECT 1 FROM ck_attendance_spans WHERE kind<>'normal')":'0'} AS facts,${tables.has('ck_attendance_span_events')?"EXISTS(SELECT 1 FROM ck_attendance_span_events WHERE action='mistaken_checkout')":'0'} AS correction`).first();
  currentConfig=evidence.enabled==null?null:{enabled:evidence.enabled};if(evidence.facts||evidence.correction)next.SOP_ATTENDANCE_RETURNS_COMPAT='true';
  if(!configExists&&!tables.has('ck_attendance_spans'))return next;
 }
 return {...next,DB:await attendanceReturnDatabase(next,{configExists,currentConfig})};
}
export async function attendanceReturnDatabase(e,{configExists,currentConfig}={}){
 if(e.SOP_ENVIRONMENT!=='staging')return e.DB;const db=e.DB,on=attendanceReturnEnabled(e);
 const exists=on||(configExists===undefined?!!await db.prepare("SELECT name FROM sqlite_master WHERE name='ck_attendance_return_config' AND type='table'").first():configExists);
 if(!exists)return db;await ensureSchema(db,'attendance-return-config-v1',()=>db.prepare(configDDL).run());
 const sync=()=>db.prepare(configSQL).bind(on?1:0),current=currentConfig===undefined?await db.prepare('SELECT enabled FROM ck_attendance_return_config WHERE id=1').first():currentConfig;if(!current||current.enabled!==(on?1:0))await sync().run();
 const originals=new WeakMap(),sqls=new WeakMap(),writes=s=>!s||!/^\s*(SELECT|EXPLAIN|PRAGMA)\b/i.test(s);
 const batch=async statements=>{const source=statements.map(s=>originals.get(s)||s);return statements.some(s=>writes(sqls.get(s)))?(await db.batch([sync(),...source])).slice(1):db.batch(source);};
 const prepare=sql=>{const wrap=s=>{sqls.set(s,sql);const proxy=new Proxy(s,{get(target,key){if(key==='bind')return(...a)=>wrap(target.bind(...a));if(key==='run'&&writes(sql))return async()=>(await batch([target]))[0];if(['all','first'].includes(key)&&writes(sql))return async column=>{const r=(await batch([target]))[0],row=r.results?.[0]||null;return key==='first'?(column&&row?row[column]:row):r;};const value=target[key];return typeof value==='function'?value.bind(target):value;}});originals.set(proxy,s);sqls.set(proxy,sql);return proxy;};return wrap(db.prepare(sql));};
 const wrapper=new Proxy(db,{get(target,key){if(key==='prepare')return prepare;if(key==='batch')return batch;const value=target[key];return typeof value==='function'?value.bind(target):value;}});shareSchemaBinding(wrapper,db);return wrapper;
}
export const attendanceCurrentSQL=(e,d,at="'"+new Date().toISOString()+"'")=>attendanceReturnEnabled(e)?attendanceAtSQL(e,d,at):'1';
// Private request evidence cannot be forged through JSON or leaked by sop_identity.
const sessionEvidence=new WeakMap();
export function rememberFieldSession(user,hash){if(user)sessionEvidence.set(user,hash);return user;}
export function guardAttendanceSession(e){
 const hash=sessionEvidence.get(e.SOP_REQUEST_USER);if(!attendanceReturnEnabled(e)||e.SOP_REQUEST_USER?.scope!=='field'||!hash)return;
 const db=e.DB,originals=new WeakMap(),sqls=new WeakMap(),writes=s=>!s||!/^\s*(SELECT|EXPLAIN|PRAGMA)\b/i.test(s);
 const batch=async statements=>{const source=statements.map(s=>originals.get(s)||s);if(!statements.some(s=>writes(sqls.get(s))))return db.batch(source);const id='session:'+crypto.randomUUID(),guard=db.prepare("INSERT INTO ck_attendance_span_checks VALUES(?,CASE WHEN EXISTS(SELECT 1 FROM ck_access_sessions WHERE token_hash=? AND scope='field' AND expires_at>?) THEN 1 ELSE 0 END)").bind(id,hash,Date.now());return(await db.batch([guard,...source,db.prepare('DELETE FROM ck_attendance_span_checks WHERE id=?').bind(id)])).slice(1,-1);};
 const prepare=sql=>{const wrap=s=>{sqls.set(s,sql);const proxy=new Proxy(s,{get(target,key){if(key==='bind')return(...a)=>wrap(target.bind(...a));if(key==='run'&&writes(sql))return async()=>(await batch([target]))[0];if(['all','first'].includes(key)&&writes(sql))return async column=>{const r=(await batch([target]))[0],row=r.results?.[0]||null;return key==='first'?(column&&row?row[column]:row):r;};const value=target[key];return typeof value==='function'?value.bind(target):value;}});originals.set(proxy,s);sqls.set(proxy,sql);return proxy;};return wrap(db.prepare(sql));};
 const wrapper=new Proxy(db,{get(target,key){if(key==='prepare')return prepare;if(key==='batch')return batch;const value=target[key];return typeof value==='function'?value.bind(target):value;}});shareSchemaBinding(wrapper,db);e.DB=wrapper;
}
// Metadata association only. Never alter labor start/end, status, needs_review or minutes.
export function attendanceDispatchBindingSQL(table){
 if(!['ck_dispatch_time_days','ck_dispatch_time_spans','ck_dispatch_time_links'].includes(table))throw Error('Unknown attendance ledger');
 const time=table==='ck_dispatch_time_links'?'engaged_at':table==='ck_dispatch_time_spans'?'started_at':'';
 const candidate=`s.attendance_id=${table}.attendance_id AND s.person_id=${table}.person_id AND ${time?`julianday(s.started_at)<=julianday(${table}.${time}) AND julianday(COALESCE(NULLIF(s.ended_at,''),NULLIF(s.provisional_end_at,''),${attendanceCutoffSQL('s.started_at')}))>julianday(${table}.${time})`:"s.ordinal=1 AND s.kind='normal'"}`;
 return `UPDATE ${table} SET attendance_span_id=(SELECT s.id FROM ck_attendance_spans s WHERE ${candidate} LIMIT 1) WHERE attendance_span_id='' AND (SELECT COUNT(*) FROM ck_attendance_spans s WHERE ${candidate})=1`;
}
