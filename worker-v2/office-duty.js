// Staging office duty uses the same jobs, personal segments and crew loans as field work.
import {activeAttendanceSQL} from './attendance-management.js';
import {ensureAttendance} from './attendance.js';
import {employeeDepartments} from './employee-attendance.js';
import {ensureSchema} from './schema-ready.js';
import {ensureDispatchStartGuard} from './atomic-native-start.js';
import {ensureCrewBorrow,borrowedOut} from './crew-borrow.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const fail=m=>{const error=Error(m);error.status=409;throw error;};
export const officeDutyEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_UPGRADE_ENABLED==='true'&&e.SOP_ATTENDANCE_ENABLED==='true';
async function schema(e){
 await ensureAttendance(e);await ensureDispatchStartGuard(e);await ensureCrewBorrow(e);
 await ensureSchema(e.DB,'office-duty-v1',()=>e.DB.batch([
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_office_one_duty BEFORE INSERT ON sop_records WHEN json_extract(NEW.state,'$.job_type')='office_order_processing' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM sop_records s JOIN v2_ops_jobs j ON j.id=s.id WHERE j.job_type='office_order_processing' AND j.status NOT IN ('completed','cancelled') AND json_extract(s.state,'$.worker_id')=json_extract(NEW.state,'$.worker_id')) THEN RAISE(ABORT,'office_duty_already_open') END; END`),
  ...['INSERT','UPDATE OF left_at,worker_id,job_id'].map((op,i)=>q(e,`CREATE TRIGGER IF NOT EXISTS ck_office_running_${i} BEFORE ${op} ON v2_ops_job_workers WHEN COALESCE(NEW.left_at,'')='' AND EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.job_id AND job_type='office_order_processing' AND status NOT IN ('working','awaiting_close')) BEGIN SELECT RAISE(ABORT,'office_duty_not_running'); END`)),
  ...['INSERT','UPDATE OF left_at,worker_id,job_id'].map((op,i)=>q(e,`CREATE TRIGGER IF NOT EXISTS ck_office_exclusive_${i} BEFORE ${op} ON v2_ops_job_workers WHEN COALESCE(NEW.left_at,'')='' AND EXISTS(SELECT 1 FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.id!=NEW.id AND w.worker_id=NEW.worker_id AND COALESCE(w.left_at,'')='' AND (j.job_type='office_order_processing' OR EXISTS(SELECT 1 FROM v2_ops_jobs n WHERE n.id=NEW.job_id AND n.job_type='office_order_processing'))) BEGIN SELECT RAISE(ABORT,'office_worker_has_active_job'); END`))
 ]));
}
async function snapshot(e,p,id){
 const job=id?await q(e,"SELECT j.*,s.revision,s.state FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id WHERE j.id=? AND j.job_type='office_order_processing'",id).first():await q(e,"SELECT j.*,s.revision,s.state FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id WHERE j.job_type='office_order_processing' AND json_extract(s.state,'$.worker_id')=? ORDER BY CASE WHEN j.status IN ('completed','cancelled') THEN 1 ELSE 0 END,j.created_at DESC,j.id DESC LIMIT 1",p.badge_id).first();
 if(job&&JSON.parse(job.state).worker_id!==p.badge_id)fail('职员与任务不一致，请重新选择');
 const segments=job?await rows(e,'SELECT id,joined_at,left_at,leave_reason FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at,id',job.id):[];
 const loans=job?await borrowedOut(e,job.id):[];
 const asOf=new Date().toISOString();
 return {ok:true,person:{id:p.id,badgeId:p.badge_id,name:p.name,department:p.department,department_label:employeeDepartments[p.department],enabled:!!p.enabled},job:job?{id:job.id,status:job.status,revision:job.revision,department:JSON.parse(job.state).labor_department,segments,needs_review:segments.some(s=>String(s.leave_reason||'').startsWith('system_cutoff:')),borrowed:loans.length>0,seconds:segments.filter(s=>!String(s.leave_reason||'').startsWith('system_cutoff:')).reduce((n,s)=>n+Math.max(0,(Date.parse(s.left_at||asOf)-Date.parse(s.joined_at))/1000),0)}:null,as_of:asOf};
}
export async function handleOfficeDuty(b,e){
 const u=e.SOP_REQUEST_USER;
 if(!officeDutyEnabled(e)||u?.scope!=='office'||!['manager','service','reviewer'].includes(u.role))fail('请使用现有办公室身份登录 / 사무실 로그인 필요');
 await schema(e);
 const p=await q(e,'SELECT p.*,x.department FROM ck_attendance_people p JOIN ck_employee_profiles x ON x.person_id=p.id WHERE p.badge_id=?',String(b.badge||'')).first();
 if(!p||!Object.hasOwn(employeeDepartments,p.department))fail('请选择已登记职员工牌 / 등록된 직원 명찰을 선택하세요');
 if(b.action==='sop_office_duty_get')return snapshot(e,p,b.job_id);
 if(!['sop_office_duty_start','sop_office_duty_pause','sop_office_duty_resume','sop_office_duty_finish'].includes(b.action))fail('无效操作');
 const request=String(b.client_req_id||'');if(!request||request.length>100)fail('缺少请求编号');
 const signature=JSON.stringify({actor:u.id,action:b.action,badge:p.badge_id,job:b.job_id||'',revision:b.revision??null});
 const replay=async()=>{const old=await q(e,'SELECT action,response_json FROM v2_idempotency_keys WHERE idem_key=?',request).first();if(!old)return null;const saved=JSON.parse(old.response_json);if(old.action!==b.action||saved.signature!==signature)fail('请求编号已用于其他操作，请重新核对');return saved;};
 const old=await replay();if(old)return {...await snapshot(e,p,old.job_id),replayed:true};
 const start=b.action==='sop_office_duty_start',open=start||b.action==='sop_office_duty_resume',t=new Date().toISOString();
 if(open&&!p.enabled)fail('职员已停用');
 if(open){
  const day=new Date(Date.parse(t)+9*3600000).toISOString().slice(0,10);
  const attendance=await q(e,`SELECT d.id FROM ck_attendance_days d WHERE d.person_id=? AND d.worker_id=? AND d.day=? AND d.signed_in<=? AND d.signed_out='' AND ${activeAttendanceSQL('d')} AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks r WHERE r.attendance_id=d.id AND r.ended_at='')`,p.id,p.badge_id,day,t).first();
  if(!attendance)fail('请先核对韩国当天有效签到并结束休息 / 당일 출근·휴식 상태를 확인하세요');
  if(await q(e,"SELECT id FROM v2_ops_job_workers WHERE worker_id=? AND COALESCE(left_at,'')=''",p.badge_id).first())fail('该职员仍在另一任务计时，请先暂停原任务或按装卸借调规则办理');
 }
 let record,job,id=start?'OFFICE-'+crypto.randomUUID():String(b.job_id||''),before={};
 if(!start){
  record=await q(e,"SELECT * FROM sop_records WHERE id=? AND kind='dispatch'",id).first();job=await q(e,"SELECT * FROM v2_ops_jobs WHERE id=? AND job_type='office_order_processing'",id).first();
  if(!record||!job||JSON.parse(record.state).worker_id!==p.badge_id)fail('职员与任务不一致，请重新选择');
  before=JSON.parse(record.state);
  if(record.revision!==Number(b.revision))fail('任务已变化，请刷新后核对');
  if((await borrowedOut(e,id)).length)fail('该职员正在借调或待归还，请先在原装卸任务办理归还');
  const allowed=b.action==='sop_office_duty_resume'?['paused','system_closed','awaiting_close']:b.action==='sop_office_duty_pause'?['working']:['working','paused','awaiting_close'];
  if(!allowed.includes(job.status))fail('当前状态不能执行此操作，请刷新');
 }
 const status=open?'working':b.action==='sop_office_duty_pause'?'paused':'completed',revision=start?0:record.revision;
 const after=start?{title:'订单处理',job_type:'office_order_processing',worker_id:p.badge_id,workers:[{id:p.badge_id,name:p.name}],lead_id:p.badge_id,owner_id:u.id,owner:u.name,labor_department:p.department,status,created_at:t,time_only:true}:{...before,status};
 const saved={ok:true,job_id:id,signature};
 const sql=[q(e,'INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES(?,?,?,?)',request,b.action,JSON.stringify(saved),t),q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request,id,revision,b.action,u.id,u.name,JSON.stringify(before),JSON.stringify(after),JSON.stringify(saved),t)];
 if(start)sql.push(q(e,"INSERT INTO v2_ops_jobs(id,job_type,flow_stage,biz_class,status,created_by,created_at,updated_at,active_worker_count) VALUES(?,'office_order_processing','office',?,'working',?,?,?,1)",id,p.department,u.id,t,t),q(e,"INSERT INTO sop_records VALUES(?,'dispatch',1,?,?,?)",id,p.department,JSON.stringify(after),t));
 else sql.push(q(e,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(after),t,id),q(e,"UPDATE v2_ops_jobs SET status=?,active_worker_count=?,finished_at=CASE WHEN ?='completed' THEN ? ELSE finished_at END,updated_at=? WHERE id=?",status,open?1:0,status,t,t,id));
 if(open)sql.push(q(e,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)','WS-'+crypto.randomUUID(),id,p.badge_id,p.name,t));
 else sql.push(q(e,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason=? WHERE job_id=? AND left_at=''",t,t,status==='paused'?'office_pause':'office_finish',id));
 try{await e.DB.batch(sql);}catch(error){const saved=await replay();if(saved)return {...await snapshot(e,p,saved.job_id),replayed:true};if(/office_duty_already_open|office_worker_has_active_job|revision_conflict|worker_busy/.test(error.message))fail('任务或人员状态已变化，请刷新核对');throw error;}
 return snapshot(e,p,id);
}
export async function protectOfficeDuty(b,e){
 if(officeDutyEnabled(e)&&(b.job_type==='office_order_processing'||b.payload?.job_type==='office_order_processing'))fail('请从协同中心「订单处理」记录工时');
 if(!officeDutyEnabled(e)||!b.job_id||b.action.startsWith('sop_office_duty_')||/_(get|detail|list|status|export)$/.test(b.action))return;
 if(await q(e,"SELECT id FROM v2_ops_jobs WHERE id=? AND job_type='office_order_processing'",b.job_id).first())fail('请从协同中心「订单处理」记录工时');
}
