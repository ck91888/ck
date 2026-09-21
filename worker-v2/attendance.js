// Attendance is enabled only in the isolated staging rollout. Production routes stay unchanged.
import { attendanceReport, kstDay } from './attendance-time.js';
import { laborDepartment } from '../shared/labor-department.js';
export const ATTENDANCE_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS ck_attendance_people(id TEXT PRIMARY KEY,badge_id TEXT NOT NULL UNIQUE,name TEXT NOT NULL,agency TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('daily','permanent')),enabled INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL)`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_days(id TEXT PRIMARY KEY,person_id TEXT NOT NULL,worker_id TEXT NOT NULL,name TEXT NOT NULL,agency TEXT NOT NULL,day TEXT NOT NULL,identity_key TEXT NOT NULL,signed_in TEXT NOT NULL,signed_out TEXT NOT NULL DEFAULT '',version INTEGER NOT NULL DEFAULT 1,UNIQUE(day,worker_id),UNIQUE(day,identity_key))`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_events(id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,record_id TEXT NOT NULL,version INTEGER NOT NULL,action TEXT NOT NULL,actor TEXT NOT NULL,at TEXT NOT NULL,before_json TEXT NOT NULL,after_json TEXT NOT NULL,response_json TEXT NOT NULL,fingerprint TEXT NOT NULL,UNIQUE(record_id,version))`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_breaks(id TEXT PRIMARY KEY,attendance_id TEXT NOT NULL,started_at TEXT NOT NULL,ended_at TEXT NOT NULL DEFAULT '',job_ids_json TEXT NOT NULL,actor TEXT NOT NULL)`,
 `CREATE UNIQUE INDEX IF NOT EXISTS ck_attendance_one_break ON ck_attendance_breaks(attendance_id) WHERE ended_at=''`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_day ON ck_attendance_days(day,agency)`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_worker ON ck_attendance_days(worker_id,day)`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_name_lookup ON ck_attendance_people(name,enabled,kind)`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_person_day ON ck_attendance_days(person_id,day)`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_prints(id TEXT PRIMARY KEY,attendance_id TEXT NOT NULL,requested_at TEXT NOT NULL,requested_by TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'requested')`,
 // Enforce attendance inside the same SQLite transaction as any legacy or new task join.
 `CREATE TRIGGER IF NOT EXISTS ck_attendance_guard_join BEFORE INSERT ON v2_ops_job_workers
 WHEN (NEW.worker_id GLOB 'DA-*' OR NEW.worker_id GLOB 'DAF-*') AND NEW.left_at=''
 BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d WHERE d.worker_id=NEW.worker_id AND d.day=date(NEW.joined_at,'+9 hours') AND d.signed_out='' AND d.signed_in<=NEW.joined_at AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=d.id AND b.ended_at='')) THEN RAISE(ABORT,'Attendance required: signed out, resting, or not signed in') END; END`
];
export const attendanceEnabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_ATTENDANCE_ENABLED==='true';
const agencies=['가온','포레인','동인천'],roles=['manager','dispatcher','reviewer','viewer','kiosk'];
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results||[];
const uid=p=>p+'-'+crypto.randomUUID();
const fail=message=>{throw Error(message);};
const text=(value,max=120)=>String(value??'').normalize('NFC').trim().replace(/\s+/g,' ').slice(0,max);
const nameOf=value=>{const n=text(value,41);if(!n||n.length>40||/[|<>\u0000-\u001f]/u.test(n))fail('请填写正确姓名 / 이름을 확인하세요');return n;};
const agencyOf=value=>{if(!agencies.includes(value))fail('请选择人力公司 / 인력회사를 선택하세요');return value;};
const badgeOf=value=>text(String(value||'').split('|')[0]);
function dateOf(value){const d=String(value||'');if(!/^\d{4}-\d{2}-\d{2}$/.test(d)||Number.isNaN(Date.parse(d+'T00:00:00+09:00'))||new Date(d+'T00:00:00Z').toISOString().slice(0,10)!==d)fail('日期无效');return d;}
const publicPerson=p=>({id:p.id,badgeId:p.badge_id,name:p.name,agency:p.agency,badgeType:p.kind,enabled:!!p.enabled});
const publicDay=r=>({id:r.id,personId:r.person_id,badgeId:r.worker_id,name:r.name,agency:r.agency,day:r.day,inAt:r.signed_in,outAt:r.signed_out,version:r.version,badgeType:r.worker_id.startsWith('DAF-')?'permanent':'daily'});
export async function ensureAttendance(env){if(!attendanceEnabled(env))return;await env.DB.batch(ATTENDANCE_SCHEMA.map(sql=>env.DB.prepare(sql)));}
function access(env,allowed=roles){const u=env.SOP_REQUEST_USER;if(!u||!allowed.includes(u.role))fail('无此操作权限 / 권한이 없습니다');return u;}
const readDay=(env,id)=>q(env,'SELECT * FROM ck_attendance_days WHERE id=?',id).first();
const todayRecord=(env,badge,t)=>q(env,'SELECT * FROM ck_attendance_days WHERE worker_id=? AND day=?',badge,kstDay(t)).first();
function canonical(value){if(Array.isArray(value))return value.map(canonical);if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().filter(k=>k!=='sop_key'&&k!=='client_req_id').map(k=>[k,canonical(value[k])]));return value;}
const fingerprint=b=>JSON.stringify(canonical(b));
async function cached(env,b){const r=await q(env,'SELECT response_json,fingerprint FROM ck_attendance_events WHERE request_id=?',b.client_req_id).first();if(!r)return null;if(r.fingerprint!==fingerprint(b))fail('请求编号已使用，请刷新后重试');return JSON.parse(r.response_json);}
async function commit(env,b,u,before,after,statements,result){
 const version=after.version||1,recordId=after.id,t=new Date().toISOString();
 const event=q(env,'INSERT INTO ck_attendance_events VALUES(?,?,?,?,?,?,?,?,?,?,?)',uid('ATE'),b.client_req_id,recordId,version,b.action,u.id,t,JSON.stringify(before||{}),JSON.stringify(after),JSON.stringify(result),fingerprint(b));
 try{await env.DB.batch([event,...statements]);}catch(e){const prior=await cached(env,b);if(prior)return prior;throw Error('记录已变化或请求冲突，请刷新后重试 / 기록이 변경되었습니다');}
 return result;
}
function updateDay(env,before,after){return q(env,'UPDATE ck_attendance_days SET agency=?,signed_in=?,signed_out=?,version=? WHERE id=? AND version=?',after.agency,after.signed_in,after.signed_out,after.version,before.id,before.version);}
async function findRecord(env,b,t){let r=b.id?await readDay(env,b.id):await todayRecord(env,badgeOf(b.badge),t);if(!r)fail('没有当天签到记录，请核对工牌 / 오늘 출근 기록이 없습니다');return r;}
function currentDay(r,t){if(r.day!==kstDay(t))fail('仅能操作当天工牌；补卡请联系办公室 / 오늘 명찰만 사용할 수 있습니다');}
function validVersion(b,r){if(Number(b.version)!==r.version)fail('记录已更新，请重新打开 / 기록이 변경되었습니다');}
const countJobs=(env,jobIds,t)=>jobIds.map(id=>q(env,"UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(*) FROM v2_ops_job_workers WHERE job_id=? AND left_at=''),updated_at=? WHERE id=?",id,t,id));
async function closePerson(env,r,t,reason){
 const open=await rows(env,"SELECT w.* FROM v2_ops_job_workers w WHERE w.worker_id=? AND w.left_at=''",r.worker_id);
 // Do not invent a historical finish time for a stale shift. Current segments stop now;
 // older-day remnants are cut off at this day's arrival and remain explicitly disputed.
 const sql=[q(env,"UPDATE v2_ops_job_workers SET left_at=CASE WHEN joined_at<? THEN joined_at ELSE ? END,minutes_worked=CASE WHEN joined_at<? THEN 0 ELSE MAX(0,ROUND((julianday(?)-julianday(joined_at))*1440,1)) END,leave_reason=CASE WHEN joined_at<? THEN ? ELSE ? END WHERE worker_id=? AND left_at=''",r.signed_in,t,r.signed_in,t,r.signed_in,'attendance_stale:'+reason,'attendance:'+reason,r.worker_id),q(env,"UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(*) FROM v2_ops_job_workers w WHERE w.job_id=v2_ops_jobs.id AND w.left_at=''),updated_at=? WHERE id IN (SELECT job_id FROM v2_ops_job_workers WHERE worker_id=?)",t,r.worker_id)];
 const ids=[...new Set(open.map(s=>s.job_id))];return {sql,open,ids};
}
async function overview(env,date,t){
 const start=new Date(date+'T00:00:00+09:00').toISOString(),end=new Date(Date.parse(start)+86400000).toISOString();
 const days=await rows(env,'SELECT * FROM ck_attendance_days WHERE day=? ORDER BY signed_in,name,id',date);
 const segments=await rows(env,`SELECT w.*,j.biz_class,j.job_type,j.status AS job_status,j.display_no,s.kind AS assignment_kind,s.department AS assigned_department,s.state AS assignment_state FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id LEFT JOIN sop_records s ON s.id=j.id AND s.kind IN ('task','dispatch') WHERE w.joined_at<? AND (w.left_at='' OR w.left_at>?)`,end,start);
 for(const s of segments){let assignment={};try{assignment=JSON.parse(s.assignment_state||'{}');}catch{}s.labor_department=assignment.labor_department||'';}
 const breaks=await rows(env,'SELECT b.* FROM ck_attendance_breaks b JOIN ck_attendance_days d ON d.id=b.attendance_id WHERE d.day=?',date);
 const history=await rows(env,'SELECT e.* FROM ck_attendance_events e JOIN ck_attendance_days d ON d.id=e.record_id WHERE d.day=? ORDER BY e.version',date);
 const output=[];
 for(const d of days){
  const record=publicDay(d),br=breaks.filter(x=>x.attendance_id===d.id);record.breaks=br.map(x=>({id:x.id,start:x.started_at,end:x.ended_at}));
  const own=segments.filter(x=>x.worker_id===d.worker_id).map(x=>({id:x.id,jobId:x.job_id,jobNo:x.display_no||x.job_id,jobType:x.job_type,jobStatus:x.job_status,badgeId:x.worker_id,department:laborDepartment(x),start:x.joined_at,end:x.left_at,reason:x.leave_reason}));
  const verifiedWindow=own.filter(s=>!(s.start<record.inAt&&!s.end));
  const totals=attendanceReport(record,verifiedWindow,t,date);
  if(own.some(s=>s.department==='other'))totals.flags.push('有作业尚未确认用工部门');
  if(own.some(s=>s.start<record.inAt&&!s.end))totals.flags.push('上个班次任务未退出，本日不计入');
  if(own.some(s=>((s.reason||'').startsWith('attendance:checkout')&&s.jobStatus!=='completed')||(s.reason||'').startsWith('attendance_stale:')))totals.flags.push('签退/跨日关闭的作业段，待负责人核实');
  const live=own.filter(s=>!s.end&&s.start>=record.inAt&&['working','pending','awaiting_close'].includes(s.jobStatus));
  const resting=br.some(x=>!x.ended_at),status=record.outAt?'out':resting?'rest':live.length?'working':'unassigned';
  const events=history.filter(e=>e.record_id===d.id);
  output.push({record,totals,segments:own,breaks:record.breaks,status,currentJobs:live.map(s=>({id:s.jobId,no:s.jobNo,department:s.department})),events:events.map(e=>({action:e.action,actor:e.actor,at:e.at,before:JSON.parse(e.before_json),after:JSON.parse(e.after_json)}))});
 }
 const stale=await rows(env,"SELECT id,worker_id,name,agency,day,signed_in FROM ck_attendance_days WHERE day<? AND signed_out='' ORDER BY day DESC LIMIT 100",date);
 return {ok:true,date,asOf:t,agencies,items:output,stale,publicTest:!!env.SOP_REQUEST_USER?.public_test};
}
export async function handleAttendance(b,env){
 if(!attendanceEnabled(env))return {ok:false,error:'签到尚未启用'};
 await ensureAttendance(env);const u=access(env),t=new Date().toISOString(),day=kstDay(t),action=b.action;
 if(action==='sop_attendance_config')return {ok:true,day,asOf:t,agencies,user:{id:u.id,name:u.name,role:u.role},publicTest:!!u.public_test};
 if(action==='sop_attendance_summary'){access(env,['manager','dispatcher','reviewer','viewer']);return overview(env,dateOf(b.date||day),t);}
 if(action==='sop_attendance_lookup'){
  const badge=badgeOf(b.badge),p=await q(env,'SELECT * FROM ck_attendance_people WHERE badge_id=? AND enabled=1',badge).first();
  if(!p)fail('工牌未登记，请联系工作人员 / 등록되지 않은 명찰입니다');
  const r=await todayRecord(env,badge,t);return {ok:true,person:publicPerson(p),record:r?publicDay(r):null};
 }
 if(action==='sop_attendance_search'){
  // Exact normalized names: all same-name matches must be explicitly confirmed.
  // Expired daily badges cannot be recovered as if they were today's identity.
  const name=nameOf(b.name),agency=b.agency?agencyOf(b.agency):'';
  const people=await rows(env,`SELECT p.* FROM ck_attendance_people p
   LEFT JOIN ck_attendance_days d ON d.person_id=p.id AND d.day=?
   WHERE p.enabled=1 AND p.name=? AND (p.kind='permanent' OR d.id IS NOT NULL)
   AND (?='' OR COALESCE(d.agency,p.agency)=?)
   ORDER BY CASE WHEN d.id IS NULL THEN 1 ELSE 0 END,d.signed_in,p.badge_id LIMIT 51`,day,name,agency,agency);
  if(people.length>50)fail('同名记录过多，请选择人力公司或联系工作人员 / 인력회사를 선택하거나 담당자에게 문의하세요');
  const items=[];for(const p of people){const r=await todayRecord(env,p.badge_id,t);items.push({person:publicPerson(p),record:r?publicDay(r):null});}
  return {ok:true,day,items};
 }
 if(action==='sop_attendance_people'){access(env,['manager']);return {ok:true,items:(await rows(env,"SELECT * FROM ck_attendance_people WHERE kind='permanent' ORDER BY name")).map(publicPerson)};}
 access(env,['manager','dispatcher','reviewer','kiosk']);
 if(!/^[A-Za-z0-9_-]{8,100}$/.test(String(b.client_req_id||'')))fail('缺少有效请求编号');
 const prior=await cached(env,b);if(prior)return prior;
 if(action==='sop_attendance_register'){
  access(env,['manager']);const name=nameOf(b.name),agency=agencyOf(b.agency),badge=b.badge?badgeOf(b.badge):'DAF-'+crypto.randomUUID().slice(0,8).toUpperCase();
  if(!/^DAF-[A-Za-z0-9_-]{1,60}$/.test(badge))fail('固定工牌必须使用DAF工号');
  const p={id:uid('ATP'),badge_id:badge,name,agency,kind:'permanent',enabled:1,created_at:t,version:1};
  return commit(env,b,u,null,p,[q(env,'INSERT INTO ck_attendance_people(id,badge_id,name,agency,kind,enabled,created_at) VALUES(?,?,?,?,?,1,?)',p.id,badge,name,agency,'permanent',t)],{ok:true,person:publicPerson(p)});
 }
 if(action==='sop_attendance_checkin'){
  let person=null,agency=agencyOf(b.agency),name,identity;
  if(b.badge){person=await q(env,"SELECT * FROM ck_attendance_people WHERE badge_id=? AND kind='permanent' AND enabled=1",badgeOf(b.badge)).first();if(!person)fail('固定工牌未登记 / 고정 명찰을 확인하세요');name=person.name;identity=person.badge_id;
   const existing=await todayRecord(env,person.badge_id,t);if(existing)return {ok:true,existing:true,record:publicDay(existing)};
  }else{
   name=nameOf(b.name);const existing=await rows(env,'SELECT * FROM ck_attendance_days WHERE name=? AND day=?',name,day);
   if(existing.length&&!b.confirm_distinct_person)return {ok:true,existing:true,identityRequired:true,records:existing.map(publicDay),record:null};
   if(b.confirm_distinct_person){access(env,['manager']);if(!text(b.reason))fail('请填写同名不同人的核实说明');}
   identity='daily:'+name+(b.confirm_distinct_person?':'+crypto.randomUUID():'');
   const badge='DA-'+day.replaceAll('-','')+'-'+crypto.randomUUID().slice(0,8).toUpperCase();person={id:uid('ATP'),badge_id:badge,name,agency,kind:'daily',enabled:1,created_at:t};
  }
  const r={id:uid('ATD'),person_id:person.id,worker_id:person.badge_id,name,agency,day,identity_key:identity,signed_in:t,signed_out:'',version:1};
  const statements=[];if(person.kind==='daily')statements.push(q(env,'INSERT INTO ck_attendance_people VALUES(?,?,?,?,?,1,?)',person.id,person.badge_id,name,agency,person.kind,t));
  statements.push(q(env,'INSERT INTO ck_attendance_days VALUES(?,?,?,?,?,?,?,?,?,1)',r.id,r.person_id,r.worker_id,name,agency,day,identity,t,''));
  return commit(env,b,u,null,r,statements,{ok:true,record:publicDay(r),existing:false});
 }
 const before=await findRecord(env,b,t),after={...before,version:before.version+1};
 if(action==='sop_attendance_checkout'){
  currentDay(before,t);if(before.signed_out)return {ok:true,existing:true,record:publicDay(before)};
  after.signed_out=t;const closed=await closePerson(env,before,t,'checkout');
  return commit(env,b,u,before,after,[updateDay(env,before,after),...closed.sql,q(env,"UPDATE ck_attendance_breaks SET ended_at=? WHERE attendance_id=? AND ended_at=''",t,before.id)],{ok:true,record:publicDay(after),closedSegments:closed.open.length,needsReview:closed.open.length>0});
 }
 if(action==='sop_attendance_company'){
  currentDay(before,t);validVersion(b,before);after.agency=agencyOf(b.agency);
  return commit(env,b,u,before,after,[updateDay(env,before,after)],{ok:true,record:publicDay(after)});
 }
 if(action==='sop_attendance_print'){
  currentDay(before,t);const id=uid('PRINT');return commit(env,b,u,before,after,[updateDay(env,before,after),q(env,'INSERT INTO ck_attendance_prints VALUES(?,?,?,?,?)',id,before.id,t,u.id,'requested')],{ok:true,record:publicDay(after),printId:id,status:'requested'});
 }
 if(action==='sop_attendance_break_start'){
  access(env,['manager','dispatcher']);currentDay(before,t);if(before.signed_out)fail('已签退，不能登记休息');
  const open=await q(env,"SELECT * FROM ck_attendance_breaks WHERE attendance_id=? AND ended_at=''",before.id).first();if(open)return {ok:true,existing:true,record:publicDay(before)};
  const closed=await closePerson(env,before,t,'rest');const validJobs=closed.open.filter(s=>s.joined_at>=before.signed_in).map(s=>s.job_id);
  return commit(env,b,u,before,after,[updateDay(env,before,after),...closed.sql,q(env,'INSERT INTO ck_attendance_breaks VALUES(?,?,?,?,?,?)',uid('BREAK'),before.id,t,'',JSON.stringify([...new Set(validJobs)]),u.id)],{ok:true,record:publicDay(after)});
 }
 if(action==='sop_attendance_break_end'){
  access(env,['manager','dispatcher']);currentDay(before,t);if(before.signed_out)fail('已签退，不能恢复作业');
  const open=await q(env,"SELECT * FROM ck_attendance_breaks WHERE attendance_id=? AND ended_at=''",before.id).first();if(!open)fail('没有进行中的休息');
  const busy=await q(env,"SELECT id FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' LIMIT 1",before.worker_id).first();
  const eligible=[];if(!busy)for(const id of JSON.parse(open.job_ids_json)){const j=await q(env,"SELECT * FROM v2_ops_jobs WHERE id=? AND status IN ('working','awaiting_close')",id).first();if(j)eligible.push(j);}
  // Never restore multiple disputed concurrent jobs. A dispatcher must select one.
  const resume=eligible.length===1?eligible:[],statements=[updateDay(env,before,after),q(env,"UPDATE ck_attendance_breaks SET ended_at=? WHERE id=? AND ended_at=''",t,open.id)];
  for(const j of resume)statements.push(q(env,"INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)",uid('WS'),j.id,before.worker_id,before.name,t));
  statements.push(...countJobs(env,resume.map(j=>j.id),t));return commit(env,b,u,before,after,statements,{ok:true,record:publicDay(after),resumedJobs:resume.map(j=>j.id)});
 }
 if(action==='sop_attendance_correct'){
  access(env,['manager']);validVersion(b,before);const reason=text(b.reason,500);if(!reason)fail('请填写补卡/更正原因');
  const parse=value=>{const n=Date.parse(value);if(!Number.isFinite(n)||n>Date.parse(t))fail('时间无效或晚于现在');return new Date(n).toISOString();};
  after.signed_in=parse(b.inAt);after.signed_out=b.outAt?parse(b.outAt):'';
  if(kstDay(after.signed_in)!==before.day||after.signed_out&&Date.parse(after.signed_out)<Date.parse(after.signed_in))fail('上下班时间或日期不一致');
  if(after.signed_out&&Date.parse(after.signed_out)-Date.parse(after.signed_in)>86400000)fail('跨度超过24小时，请先核对班次');
  after.correction_reason=reason;
  const statements=[updateDay(env,before,after)];
  // Explicitly verified departure closes only that shift's personal segments.
  if(after.signed_out){const open=await rows(env,"SELECT * FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' AND joined_at>=? AND joined_at<=?",before.worker_id,after.signed_in,after.signed_out);after.corrected_segments=open;
   for(const w of open)statements.push(q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*1440,1)),leave_reason='attendance:verified_departure' WHERE id=? AND left_at=''",after.signed_out,after.signed_out,w.id));statements.push(...countJobs(env,[...new Set(open.map(w=>w.job_id))],t));}
  return commit(env,b,u,before,after,statements,{ok:true,record:publicDay(after),needsReview:true});
 }
 fail('未知签到操作');
}
export async function guardAttendance(body,env){
 if(!attendanceEnabled(env))return null;
 const action=body.action||'',native=action==='sop_native_start',source=native?body.payload||{}:body;
 if(!(native||['sop_task_start','sop_task_people','sop_task_dispatch'].includes(action)||/^v2_.*_(start|resume|join)$/.test(action)||action==='v2_pick_job_start_by_docs'))return null;
 // A committed retry must reach the original action's idempotent response.
 if(body.client_req_id){const done=await q(env,'SELECT action,actor_id FROM sop_events WHERE request_id=?',body.client_req_id).first();if(done&&done.action===action&&done.actor_id===env.SOP_REQUEST_USER?.id)return null;}
 let workers=body.workers||[],jobId=body.id||source.job_id;
 if(native&&source.client_req_id){const old=await q(env,'SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?',source.client_req_id).first();if(old){jobId=JSON.parse(old.response_json).job_id;const saved=await q(env,"SELECT state FROM sop_records WHERE id=? AND kind='dispatch'",jobId).first();if(saved&&JSON.parse(saved.state).owner_id===env.SOP_REQUEST_USER?.id)return null;}}
 if(action==='sop_task_people'){const row=await q(env,"SELECT state FROM sop_records WHERE id=? AND kind='task'",body.id).first();if(row&&JSON.parse(row.state).status!=='working')return null;}

 if(action==='sop_task_start'){const row=await q(env,"SELECT state FROM sop_records WHERE id=? AND kind='task'",body.id).first();workers=row?JSON.parse(row.state).workers||[]:[];}
 if(!workers.length&&(source.worker_id||source.handler_id))workers=[{id:source.worker_id||source.handler_id}];
 const daily=workers.filter(w=>/^DA(?:F)?-/.test(w.id||''));if(!daily.length)return null;
 await ensureAttendance(env);const t=new Date().toISOString();
 for(const w of daily){const record=await todayRecord(env,w.id,t);if(!record)return (w.name||w.id)+' 请先办理当天签到 / 먼저 출근 등록하세요';if(record.signed_out)return record.name+' 已签退，不能开始作业 / 이미 퇴근했습니다';const rest=await q(env,"SELECT id FROM ck_attendance_breaks WHERE attendance_id=? AND ended_at=''",record.id).first();if(rest)return record.name+' 正在休息，请先结束休息 / 휴식을 먼저 종료하세요';const busy=await q(env,"SELECT job_id FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' AND job_id!=? LIMIT 1",w.id,jobId||'').first();if(busy)return record.name+' 已在另一任务，请先办理交接';}
 return null;
}
