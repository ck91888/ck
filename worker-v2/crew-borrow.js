// Staging-only personal loans. The destination dispatcher authorizes a loan;
// source task ownership and every other person's clock remain unchanged.
import {ensureSchema} from './schema-ready.js';
import {activeAttendanceSQL} from './attendance-management.js';
import {kstDay} from './attendance-time.js';
import {dispatchAccess} from './dispatch-access.js';
import {jobNumbers} from './document-numbers.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const parse=s=>JSON.parse(s||'{}'),fail=m=>{throw Error(m);},active=['working','awaiting_close'],pending="('borrowed','return_pending')";
export const crewBorrowEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_UPGRADE_ENABLED==='true';
export const CREW_BORROW_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS ck_crew_borrows(id TEXT PRIMARY KEY,request_id TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,destination_job_id TEXT NOT NULL,worker_id TEXT NOT NULL,worker_name TEXT NOT NULL,source_job_id TEXT NOT NULL,source_segment_id TEXT NOT NULL,source_kind TEXT NOT NULL,source_revision INTEGER NOT NULL,source_owner_id TEXT NOT NULL,source_day TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'borrowed',borrowed_at TEXT NOT NULL,released_at TEXT NOT NULL DEFAULT '',returned_at TEXT NOT NULL DEFAULT '',return_segment_id TEXT NOT NULL DEFAULT '',blocked_reason TEXT NOT NULL DEFAULT '')`,
 `CREATE UNIQUE INDEX IF NOT EXISTS ck_crew_one_loan ON ck_crew_borrows(worker_id) WHERE status IN ${pending}`,
 'CREATE INDEX IF NOT EXISTS ck_crew_destination ON ck_crew_borrows(destination_job_id,status)',
 'CREATE INDEX IF NOT EXISTS ck_crew_source ON ck_crew_borrows(source_job_id,status)',
 'CREATE TABLE IF NOT EXISTS ck_crew_requests(request_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,signature_json TEXT NOT NULL,destination_job_id TEXT NOT NULL,created_at TEXT NOT NULL)',
 `CREATE TRIGGER IF NOT EXISTS ck_crew_source_guard BEFORE INSERT ON ck_crew_borrows BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.id=NEW.source_segment_id AND w.worker_id=NEW.worker_id AND w.job_id=NEW.source_job_id AND w.left_at='' AND j.status IN ('working','awaiting_close')) OR EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.worker_id=NEW.worker_id AND w.left_at='' AND w.id!=NEW.source_segment_id) THEN RAISE(ABORT,'borrow_source_changed') END;
 SELECT CASE WHEN NEW.source_kind!='legacy' AND NOT EXISTS(SELECT 1 FROM sop_records s WHERE s.id=NEW.source_job_id AND s.kind=NEW.source_kind AND s.revision=NEW.source_revision AND (s.kind!='task' OR json_extract(s.state,'$.status')='working')) THEN RAISE(ABORT,'borrow_source_changed') END;
 END`,
 // Extends the existing SOP occupancy guard to legacy sources with loan history.
 `CREATE TRIGGER IF NOT EXISTS ck_crew_one_clock BEFORE INSERT ON v2_ops_job_workers WHEN NEW.left_at='' AND EXISTS(SELECT 1 FROM ck_crew_borrows b WHERE b.worker_id=NEW.worker_id) AND EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.worker_id=NEW.worker_id AND w.left_at='') BEGIN SELECT RAISE(ABORT,'borrow_worker_busy'); END`,
 `CREATE TRIGGER IF NOT EXISTS ck_crew_return_guard BEFORE INSERT ON v2_ops_job_workers WHEN NEW.id LIKE 'WS-RETURN-%' BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_crew_borrows b JOIN v2_ops_jobs j ON j.id=b.source_job_id WHERE 'WS-RETURN-'||b.id=NEW.id AND b.worker_id=NEW.worker_id AND b.source_job_id=NEW.job_id AND b.status='return_pending' AND j.status IN ('working','awaiting_close') AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.job_id=b.destination_job_id AND w.worker_id=b.worker_id AND w.left_at='') AND (b.source_kind='legacy' OR EXISTS(SELECT 1 FROM sop_records s WHERE s.id=b.source_job_id AND s.kind=b.source_kind AND COALESCE(json_extract(s.state,'$.owner_id'),'')=b.source_owner_id AND (s.kind!='task' OR json_extract(s.state,'$.status')='working') AND EXISTS(SELECT 1 FROM json_each(s.state,'$.workers') w WHERE json_extract(w.value,'$.id')=b.worker_id)))) THEN RAISE(ABORT,'borrow_return_changed') END;
 END`
];
const CREW_ATTENDANCE_SCHEMA=[
 `CREATE TRIGGER IF NOT EXISTS ck_crew_source_attendance BEFORE INSERT ON ck_crew_borrows WHEN NEW.worker_id GLOB 'DA-*' OR NEW.worker_id GLOB 'DAF-*' OR NEW.worker_id GLOB 'EMP-*' BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id JOIN v2_ops_job_workers w ON w.id=NEW.source_segment_id WHERE d.worker_id=NEW.worker_id AND d.day=NEW.source_day AND p.enabled=1 AND d.signed_out='' AND ${activeAttendanceSQL('d')} AND d.signed_in<=w.joined_at AND date(w.joined_at,'+9 hours')=NEW.source_day AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks r WHERE r.attendance_id=d.id AND r.ended_at='')) THEN RAISE(ABORT,'borrow_source_attendance_changed') END; END`,
 `CREATE TRIGGER IF NOT EXISTS ck_crew_return_attendance BEFORE INSERT ON v2_ops_job_workers WHEN NEW.id LIKE 'WS-RETURN-%' AND (NEW.worker_id GLOB 'DA-*' OR NEW.worker_id GLOB 'DAF-*' OR NEW.worker_id GLOB 'EMP-*') BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE d.worker_id=NEW.worker_id AND d.day=date(NEW.joined_at,'+9 hours') AND p.enabled=1 AND d.signed_out='' AND ${activeAttendanceSQL('d')} AND d.signed_in<=NEW.joined_at AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks r WHERE r.attendance_id=d.id AND r.ended_at='')) THEN RAISE(ABORT,'borrow_return_attendance_changed') END; END`
];
export async function ensureCrewBorrow(e){
 if(!crewBorrowEnabled(e))return;
 await ensureSchema(e.DB,'crew-borrow-v1',()=>e.DB.batch(CREW_BORROW_SCHEMA.map(s=>e.DB.prepare(s))));
 if(e.SOP_ATTENDANCE_ENABLED==='true')await ensureSchema(e.DB,'crew-borrow-attendance-v2',()=>e.DB.batch(['DROP TRIGGER IF EXISTS ck_crew_source_attendance','DROP TRIGGER IF EXISTS ck_crew_return_attendance',...CREW_ATTENDANCE_SCHEMA].map(s=>e.DB.prepare(s))));
}
const hasLoanTable=async e=>!!await q(e,"SELECT name FROM sqlite_master WHERE type='table' AND name='ck_crew_borrows'").first();
export function borrowablePayload(p={}){return ['v2_unload_job_start','v2_unplanned_unload_start','v2_outbound_load_start'].includes(p.action)||p.action==='v2_ops_job_start'&&p.job_type==='load_outbound'&&!p.related_doc_id;}
async function destination(e,b){
 if(!crewBorrowEnabled(e)||!['manager','dispatcher'].includes(e.SOP_REQUEST_USER?.role))fail('请由有装卸管理资格的负责人确认借调');
 if(b.job_id){const access=dispatchAccess(e.SOP_REQUEST_USER),j=await q(e,"SELECT j.* FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id AND s.kind='dispatch' WHERE j.id=? AND "+access.sql,b.job_id,...access.args).first();if(!j||!['unload','load_outbound'].includes(j.job_type))fail('仅本装卸任务的管理人员可借调或归还');return j;}
 if(!borrowablePayload(b.payload))fail('借调仅用于到仓卸货或出库装货');return null;
}
export async function canCrewDestination(e,b){try{await destination(e,b);return !!b.job_id;}catch{return false;}}
async function eligibility(e,id,t){
 if(!/^(?:DA(?:F)?|EMP)-/.test(id))return '';
 if(e.SOP_ATTENDANCE_ENABLED!=='true')return '须先启用当天考勤核验';
 const d=await q(e,'SELECT d.*,p.enabled FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE d.worker_id=? AND d.day=? AND '+activeAttendanceSQL('d'),id,kstDay(t)).first();
 if(!d||!d.enabled)return '人员已停用或没有韩国当天签到';if(d.signed_out)return '人员已签退';
 if(await q(e,"SELECT id FROM ck_attendance_breaks WHERE attendance_id=? AND ended_at=''",d.id).first())return '人员正在休息';return '';
}
export async function crewAvailability(e,b){
 await ensureCrewBorrow(e);const target=await destination(e,b),id=String(b.worker_id||'');if(!id||id.length>100)fail('请扫描有效工牌');
 const t=new Date().toISOString(),why=await eligibility(e,id,t);if(why)return {ok:true,worker_id:id,can_borrow:false,reason:why};
 const segments=await rows(e,'SELECT w.*,j.status,j.job_type FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.worker_id=? AND w.left_at=\'\'',id);
 if(target&&segments.length===1&&segments[0].job_id===target.id)return {ok:true,worker_id:id,already_here:true};
 const loan=await q(e,`SELECT id,status,destination_job_id FROM ck_crew_borrows WHERE worker_id=? AND status IN ${pending}`,id).first();
 if(loan)return {ok:true,worker_id:id,can_borrow:false,reason:'该人仍有借调或待归还记录，请先归还；本版不支持多层借调'};
 if(!segments.length)return {ok:true,worker_id:id,available:true};
 if(segments.length!==1||!active.includes(segments[0].status))return {ok:true,worker_id:id,can_borrow:false,reason:'原计时状态异常，请由负责人先核实'};
 const s=segments[0],record=await q(e,"SELECT * FROM sop_records WHERE id=? AND kind IN ('task','dispatch')",s.job_id).first();
 if(/^(?:DA(?:F)?|EMP)-/.test(id)){
  const day=await q(e,'SELECT signed_in FROM ck_attendance_days WHERE worker_id=? AND day=?',id,kstDay(t)).first();
  if(kstDay(s.joined_at)!==kstDay(t)||s.joined_at<day.signed_in)return {ok:true,worker_id:id,can_borrow:false,reason:'原计时属于旧班次，请负责人先核实结束'};
 }
 if(record?.kind==='task'&&parse(record.state).status!=='working')return {ok:true,worker_id:id,can_borrow:false,reason:'原作业计划当前不能借调'};
 const [display]=await jobNumbers(e,[{id:s.job_id}]);
 return {ok:true,worker_id:id,worker_name:s.worker_name,can_borrow:true,source:{job_id:s.job_id,segment_id:s.id,revision:record?.revision||0,kind:record?.kind||'legacy',business_no:display.business_no||s.job_id,job_type:s.job_type,department:record?.department||'',owner:record?parse(record.state).owner||'':''}};
}
const signature=b=>JSON.stringify({action:b.action,payload:b.payload||null,job_id:b.job_id||'',workers:[...(b.workers||[])].map(w=>({id:w.id,name:w.name})).sort((a,b)=>a.id.localeCompare(b.id)),lead_id:b.lead_id||'',department:b.labor_department||'',minutes:b.estimated_minutes||0,revision:b.revision||0,confirmations:[...(b.borrow_confirmations||[])].sort((a,b)=>a.worker_id.localeCompare(b.worker_id))});
export async function prepareCrewBorrow(e,b){
 await ensureCrewBorrow(e);const request=String(b.action==='sop_native_start'?b.payload?.client_req_id:b.client_req_id||''),confirm=b.borrow_confirmations||[];
 const cached=request?await q(e,'SELECT * FROM ck_crew_requests WHERE request_id=?',request).first():null;
 if(cached){if(cached.actor_id!==e.SOP_REQUEST_USER?.id||cached.signature_json!==signature(b))fail('本次借调请求已变更，请重新确认人员');return {request,signature:signature(b),plans:[],replay:true};}
 if(!confirm.length)return null;
 await destination(e,b);if(!request||request.length>100)fail('缺少借调请求编号');
 if(!Array.isArray(confirm)||confirm.length>50||new Set(confirm.map(x=>x.worker_id)).size!==confirm.length)fail('借调确认无效或重复');
 const plans=[];
 for(const c of confirm){
  const w=(b.workers||[]).find(w=>w.id===c.worker_id);if(!w)fail('借调人员已不在本次名单中');
  const a=await crewAvailability(e,{job_id:b.job_id,payload:b.payload,worker_id:w.id});
  if(!a.can_borrow||a.source.job_id!==c.source_job_id||a.source.segment_id!==c.source_segment_id||a.source.revision!==c.source_revision||a.worker_name!==w.name)fail(a.reason||'人员原任务已变化，请重新扫描确认借调');
  const record=await q(e,"SELECT * FROM sop_records WHERE id=? AND kind IN ('task','dispatch')",c.source_job_id).first();
  plans.push({worker:w,source:a.source,record,id:'CB-'+crypto.randomUUID()});
 }
 return {request,signature:signature(b),plans,replay:false};
}
export async function borrowedOut(e,id){if(!crewBorrowEnabled(e)||!await hasLoanTable(e))return [];return rows(e,`SELECT * FROM ck_crew_borrows WHERE source_job_id=? AND status IN ${pending}`,id);}
export async function personHasPendingReturn(e,id){if(!crewBorrowEnabled(e)||!await hasLoanTable(e))return false;return !!await q(e,"SELECT id FROM ck_crew_borrows WHERE worker_id=? AND status='return_pending'",id).first();}
export async function sourceActiveWorkers(e,id,workers){const out=await borrowedOut(e,id);return workers.filter(w=>!out.some(b=>b.worker_id===w.id));}
export function crewBorrowStatements(e,prepared,destinationId,t){
 if(!prepared?.plans.length)return [];
 const sql=[q(e,'INSERT INTO ck_crew_requests VALUES(?,?,?,?,?)',prepared.request,e.SOP_REQUEST_USER.id,prepared.signature,destinationId,t)],sources=new Map();
 for(const p of prepared.plans){const s=p.source,record=p.record,d=record?parse(record.state):{};
  sql.push(q(e,'INSERT INTO ck_crew_borrows(id,request_id,actor_id,actor_name,destination_job_id,worker_id,worker_name,source_job_id,source_segment_id,source_kind,source_revision,source_owner_id,source_day,borrowed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',p.id,prepared.request,e.SOP_REQUEST_USER.id,e.SOP_REQUEST_USER.name,destinationId,p.worker.id,p.worker.name,s.job_id,s.segment_id,s.kind,s.revision,d.owner_id||'',kstDay(t),t));
  sql.push(q(e,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*1440,1)),leave_reason=? WHERE id=? AND left_at=''",t,t,'borrow:'+p.id,s.segment_id));
  if(s.job_type==='pick_direct')sql.push(q(e,"UPDATE v2_pick_worker_docs SET status='completed',finished_at=?,minutes_worked=(SELECT minutes_worked FROM v2_ops_job_workers WHERE id=?) WHERE segment_id=? AND status='working'",t,s.segment_id,s.segment_id));
  if(!sources.has(s.job_id))sources.set(s.job_id,{record,ids:[]});sources.get(s.job_id).ids.push(p.worker.id);
 }
 for(const [id,{record,ids}] of sources){
  sql.push(q(e,"UPDATE v2_ops_jobs SET active_worker_count=(SELECT COUNT(*) FROM v2_ops_job_workers WHERE job_id=? AND left_at=''),status=CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND left_at='') AND ?!='task' THEN 'awaiting_close' ELSE status END,updated_at=? WHERE id=?",id,id,record?.kind||'legacy',t,id));
  if(record){const before=parse(record.state),after={...before,borrowed_worker_ids:[...new Set([...(before.borrowed_worker_ids||[]),...ids])]};
   sql.push(q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',prepared.request+':loan:'+id,id,record.revision,'crew_borrow_out',e.SOP_REQUEST_USER.id,e.SOP_REQUEST_USER.name,record.state,JSON.stringify(after),JSON.stringify({ok:true,destination_job_id:destinationId}),t),q(e,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(after),t,id));
  }
 }
 return sql;
}
export function staffedDispatchStatements(e,p,staff,id,t,result){
 const lead=staff.workers.find(w=>w.id===staff.lead_id),data={title:p.job_type||'unload',owner_id:e.SOP_REQUEST_USER.id,owner:e.SOP_REQUEST_USER.name,lead_id:lead.id,workers:staff.workers,estimated_minutes:staff.estimated_minutes,job_type:p.job_type||'unload',status:'working',created_at:t,source_type:p.source_type||'',source_id:p.source_id||'',labor_department:staff.department};
 return [q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)','DISPATCH-'+p.client_req_id,id,0,'sop_native_start',e.SOP_REQUEST_USER.id,e.SOP_REQUEST_USER.name,'{}',JSON.stringify(data),JSON.stringify(result),t),q(e,'INSERT INTO sop_records VALUES(?,?,?,?,?,?)',id,'dispatch',1,staff.department,JSON.stringify(data),t),...crewBorrowStatements(e,e.SOP_CREW_BORROW,id,t),...staff.workers.map(w=>q(e,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)',w.id===staff.lead_id&&result.worker_seg_id?result.worker_seg_id:'WS-'+crypto.randomUUID(),id,w.id,w.name,t))];
}
export async function startBorrowableSimple(e,p,staff,feedbackNo){
  const id='JOB-CB-'+crypto.randomUUID(),t=new Date().toISOString(),unload=p.action==='v2_unplanned_unload_start',fb=unload?'FB-'+crypto.randomUUID():'',lead=staff.workers.find(w=>w.id===staff.lead_id),result={ok:true,job_id:id,worker_seg_id:'WS-'+crypto.randomUUID(),is_new_job:true,lead,assigned_workers:staff.workers,...(staff.signature?{_dispatch_signature:staff.signature}:{}),...(unload?{feedback_id:fb,display_no:feedbackNo}:{})};
 const sql=[];
 if(unload){const content=[p.customer&&'客户/고객: '+p.customer,p.vehicle_info&&'车辆/차량: '+p.vehicle_info,p.driver_info&&'司机/기사: '+p.driver_info,p.source_info&&'来源/출처: '+p.source_info].filter(Boolean).join('\n')||'现场操作人员发起计划外卸货';sql.push(q(e,"INSERT INTO v2_field_feedbacks(id,feedback_type,related_doc_type,related_doc_id,title,content,submitted_by,status,display_no,remark,created_at,updated_at) VALUES(?,'unplanned_unload','ops_job',?,?,?,?,'field_working',?,?,?,?)",fb,id,p.cargo_summary||p.title||'计划外到货-现场卸货中',content,lead.name,feedbackNo,p.remark||'',t,t));}
 sql.push(q(e,"INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count) VALUES(?,?,?,?,?,?,'working',?,?,?,?)",id,unload?'unload':'outbound',p.biz_class||'',unload?'unload':'load_outbound',unload?'field_feedback':'',fb,lead.id,t,t,staff.workers.length));
 sql.push(q(e,'INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES(?,?,?,?)',p.client_req_id,p.action,JSON.stringify(result),t),...staffedDispatchStatements(e,{...p,job_type:unload?'unload':'load_outbound',source_type:unload?'field_feedback':'',source_id:fb},staff,id,t,result));
  try{await e.DB.batch(sql);}catch(error){const old=await q(e,'SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?',p.client_req_id).first(),request=await q(e,'SELECT * FROM ck_crew_requests WHERE request_id=?',p.client_req_id).first();if(old){const saved=parse(old.response_json);if((staff.signature&&saved._dispatch_signature===staff.signature)||(request?.actor_id===e.SOP_REQUEST_USER.id&&request.signature_json===e.SOP_CREW_BORROW?.signature))return saved;}throw error;}return result;
}
async function mark(e,b,status,reason,t){await q(e,'UPDATE ck_crew_borrows SET status=?,blocked_reason=?,released_at=CASE WHEN released_at=\'\' THEN ? ELSE released_at END WHERE id=? AND status IN '+pending,status,reason,t,b.id).run();}
export async function reconcileCrewReturns(e,id){
 if(!crewBorrowEnabled(e))return [];await ensureCrewBorrow(e);const loans=await rows(e,`SELECT * FROM ck_crew_borrows WHERE destination_job_id=? AND status IN ${pending}`,id),out=[];
 for(const b of loans){
  if(await q(e,"SELECT id FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at=''",id,b.worker_id).first())continue;
  const t=new Date().toISOString();await mark(e,b,'return_pending','',t);
  const job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',b.source_job_id).first(),record=await q(e,"SELECT * FROM sop_records WHERE id=? AND kind IN ('task','dispatch')",b.source_job_id).first(),data=record?parse(record.state):{};
  let terminal='',wait='';
  if(!job||['completed','cancelled'].includes(job.status))terminal='原任务已结束或取消';
  else if(record?.kind==='task'&&!['working','paused'].includes(data.status))terminal='原作业段已完成或状态已变更';
  else if(b.source_day!==kstDay(t))terminal='已跨韩国工作日，请重新确认派工';
  else if(record&&(data.owner_id!==b.source_owner_id||!(data.workers||[]).some(w=>w.id===b.worker_id)))terminal='原任务已正式改派该人';
  else if(!active.includes(job.status)||record?.kind==='task'&&data.status!=='working')wait='原任务已暂停，需负责人恢复';
  const own=await rows(e,"SELECT * FROM v2_ops_job_workers WHERE worker_id=? AND left_at=''",b.worker_id);
  if(!terminal&&own.some(s=>s.job_id!==b.source_job_id))terminal='人员已另派任务，停止自动归还';
  const why=await eligibility(e,b.worker_id,t);if(why.includes('休息'))wait=why;else if(why)terminal=why;
  if(terminal||wait){await mark(e,b,terminal?'closed':'return_pending',terminal||wait,t);out.push({id:b.id,worker_id:b.worker_id,worker_name:b.worker_name,status:terminal?'closed':'return_pending',reason:terminal||wait});continue;}
  const existing=own.find(s=>s.job_id===b.source_job_id),seg=existing?.id||'WS-RETURN-'+b.id,sql=[];
  if(!existing)sql.push(q(e,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)',seg,b.source_job_id,b.worker_id,b.worker_name,t));
  if(record){const after={...data,borrowed_worker_ids:(data.borrowed_worker_ids||[]).filter(x=>x!==b.worker_id)};
   sql.push(q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',b.id+':return',record.id,record.revision,'crew_borrow_return',b.actor_id,b.actor_name,record.state,JSON.stringify(after),JSON.stringify({ok:true,worker_segment_id:seg}),t),q(e,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(after),t,record.id));}
  sql.push(q(e,"UPDATE v2_ops_jobs SET status='working',active_worker_count=(SELECT COUNT(*) FROM v2_ops_job_workers WHERE job_id=? AND left_at=''),updated_at=? WHERE id=? AND status IN ('working','awaiting_close')",b.source_job_id,t,b.source_job_id));
  if(job.job_type==='pick_direct')sql.push(q(e,`INSERT OR IGNORE INTO v2_pick_worker_docs(id,job_id,segment_id,worker_id,worker_name,pick_doc_no,started_at,status,created_at) SELECT 'PWD-RETURN-'||?||'-'||d.id,?,?,?,?,d.pick_doc_no,?,'working',? FROM v2_ops_job_pick_docs d WHERE d.job_id=? AND d.pick_status!='completed'`,b.id,b.source_job_id,seg,b.worker_id,b.worker_name,t,t,b.source_job_id));
  sql.push(q(e,"UPDATE ck_crew_borrows SET status='returned',blocked_reason='',returned_at=?,return_segment_id=? WHERE id=? AND status='return_pending'",t,seg,b.id));
  try{await e.DB.batch(sql);out.push({id:b.id,worker_id:b.worker_id,worker_name:b.worker_name,status:'returned'});}catch(error){const now=await q(e,'SELECT status FROM ck_crew_borrows WHERE id=?',b.id).first();if(now?.status==='returned')out.push({id:b.id,worker_id:b.worker_id,status:'returned'});else{await mark(e,b,'return_pending','归还时状态变化，可重新核对后重试',t);out.push({id:b.id,worker_id:b.worker_id,worker_name:b.worker_name,status:'return_pending',reason:'归还时状态变化，可重试'});}}
 }
 return out.map(x=>({...x,destination_job_id:id}));
}
export async function crewStatus(e,b){await ensureCrewBorrow(e);await destination(e,b);return {ok:true,items:await rows(e,'SELECT * FROM ck_crew_borrows WHERE destination_job_id=? ORDER BY borrowed_at',b.job_id)};}
export async function crewRetry(e,b){await ensureCrewBorrow(e);await destination(e,b);const returns=await reconcileCrewReturns(e,b.job_id);return {ok:true,returns,crew_returns:returns};}
export async function reconcilePersonReturns(e,workerId){if(!workerId||!crewBorrowEnabled(e))return [];await ensureCrewBorrow(e);const items=await rows(e,`SELECT DISTINCT destination_job_id FROM ck_crew_borrows WHERE worker_id=? AND status IN ${pending}`,workerId),out=[];for(const b of items)out.push(...await reconcileCrewReturns(e,b.destination_job_id));return out;}
