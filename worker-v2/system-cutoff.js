import {ensureSchema} from './schema-ready.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
export const cutoffEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_UPGRADE_ENABLED==='true'&&e.SOP_DAILY_CUTOFF_ENABLED==='true';
export const forcedClock=s=>String(s.leave_reason??s.reason??'').startsWith('system_cutoff:');
export const cutoffLabel='系统04:00强制结束，待核实 / 04:00 시스템 종료 · 확인 필요';
export function latestCutoff(value){const n=Number(value);if(!Number.isFinite(n))throw Error('Invalid cutoff clock');return new Date(Math.floor((n+5*3600000)/86400000)*86400000-5*3600000).toISOString();}
// The first 04:00 KST strictly after an execution began. A 04:00 start belongs
// to the new operational day. Missing timestamps use the latest system boundary
// and remain explicitly unverified, never a claimed actual departure.
const nextSQL=(jul,latest)=>`CASE WHEN (${jul})>0 THEN strftime('%Y-%m-%dT%H:%M:%fZ',(${jul}),'+5 hours','start of day','+1 day','-5 hours') ELSE '${latest}' END`;
const jobStart=a=>`MAX(COALESCE(julianday(${a}.created_at),0),COALESCE(julianday(${a}.resumed_at),0),COALESCE((SELECT MAX(julianday(sw.joined_at)) FROM v2_ops_job_workers sw WHERE sw.job_id=${a}.id),0))`;
export async function ensureCutoffSchema(e){
 if(!cutoffEnabled(e))return;
 await ensureSchema(e.DB,'system-cutoff-v1',()=>e.DB.batch([
  q(e,`CREATE TABLE IF NOT EXISTS ck_system_cutoff_audit(kind TEXT NOT NULL,row_id TEXT NOT NULL,cutoff_at TEXT NOT NULL,job_id TEXT NOT NULL,run_token TEXT NOT NULL,processed_at TEXT NOT NULL,before_json TEXT NOT NULL,provisional_minutes REAL,PRIMARY KEY(kind,row_id,cutoff_at))`),
  q(e,'CREATE INDEX IF NOT EXISTS ck_system_cutoff_token ON ck_system_cutoff_audit(run_token,kind,row_id)'),
  q(e,`CREATE TABLE IF NOT EXISTS ck_system_cutoff_runs(cutoff_at TEXT PRIMARY KEY,processed_at TEXT NOT NULL,run_token TEXT NOT NULL,policy TEXT NOT NULL)`),
  q(e,`CREATE TABLE IF NOT EXISTS ck_system_cutoff_corrections(request_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL,cutoff_at TEXT NOT NULL,before_json TEXT NOT NULL,after_json TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,reason TEXT NOT NULL,confirmed_at TEXT NOT NULL,signature TEXT NOT NULL)`),
  // A request prepared before the boundary must retry with a fresh server time.
  ...['INSERT','UPDATE OF joined_at,left_at,job_id,worker_id'].map((op,i)=>q(e,`CREATE TRIGGER IF NOT EXISTS ck_cutoff_join_${i} BEFORE ${op} ON v2_ops_job_workers WHEN NEW.left_at='' AND (julianday(NEW.joined_at) IS NULL OR julianday(NEW.joined_at)<julianday('now','+5 hours','start of day','-5 hours') OR EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.job_id AND status='system_closed')) BEGIN SELECT RAISE(ABORT,'system_cutoff_refresh_and_resume'); END`)),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_cutoff_job_start BEFORE INSERT ON v2_ops_jobs WHEN NEW.status NOT IN ('completed','cancelled') AND julianday(NEW.created_at)<julianday('now','+5 hours','start of day','-5 hours') BEGIN SELECT RAISE(ABORT,'system_cutoff_refresh_and_resume'); END`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_cutoff_completion BEFORE UPDATE OF status ON v2_ops_jobs WHEN OLD.status='system_closed' AND NEW.status NOT IN ('system_closed','working','cancelled') BEGIN SELECT RAISE(ABORT,'system_cutoff_resume_before_completion'); END`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_cutoff_preserve_clock BEFORE UPDATE ON v2_ops_job_workers WHEN OLD.leave_reason='system_cutoff:unverified' AND (NEW.joined_at IS NOT OLD.joined_at OR NEW.left_at IS NOT OLD.left_at OR NEW.minutes_worked IS NOT OLD.minutes_worked OR NEW.leave_reason IS NOT OLD.leave_reason) AND NOT (NEW.joined_at IS OLD.joined_at AND NEW.leave_reason='system_cutoff_verified' AND EXISTS(SELECT 1 FROM ck_system_cutoff_corrections c WHERE c.segment_id=OLD.id AND json_extract(c.before_json,'$.left_at')=OLD.left_at AND json_extract(c.after_json,'$.left_at')=NEW.left_at AND json_extract(c.after_json,'$.minutes_worked')=NEW.minutes_worked)) BEGIN SELECT RAISE(ABORT,'system_cutoff_clock_requires_audited_confirmation'); END`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_cutoff_result BEFORE INSERT ON v2_ops_job_results WHEN EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=NEW.job_id AND (j.status='system_closed' OR (${jobStart('j')})<julianday('now','+5 hours','start of day','-5 hours'))) BEGIN SELECT RAISE(ABORT,'system_cutoff_resume_before_result'); END`)
 ]));
}
const quote=x=>'"'+x.replaceAll('"','""')+'"';
async function jsonRow(e,table,alias){const columns=await rows(e,'PRAGMA table_info('+quote(table)+')');return 'json_object('+columns.flatMap(c=>["'"+c.name.replaceAll("'","''")+"'",alias+'.'+quote(c.name)]).join(',')+')';}
export async function runSystemCutoff(e,asOf=Date.now()){
 if(!cutoffEnabled(e))return {enabled:false};
 await ensureCutoffSchema(e);const latest=latestCutoff(asOf);
 if(await q(e,'SELECT cutoff_at FROM ck_system_cutoff_runs WHERE cutoff_at=?',latest).first())return {ok:true,replayed:true,cutoff_at:latest};
 const token=crypto.randomUUID(),at=new Date(asOf).toISOString(),tables=new Set((await rows(e,"SELECT name FROM sqlite_master WHERE type='table'")).map(r=>r.name)),sql=[];
 const selected=(kind,id)=>`EXISTS(SELECT 1 FROM ck_system_cutoff_audit ca WHERE ca.run_token='${token}' AND ca.kind='${kind}' AND ca.row_id=${id})`;
 const savedCut=(kind,id)=>`(SELECT cutoff_at FROM ck_system_cutoff_audit ca WHERE ca.run_token='${token}' AND ca.kind='${kind}' AND ca.row_id=${id})`;
 const snapshot=async(kind,table,id,job,cut,where,minutes='NULL')=>{
  sql.push(q(e,`INSERT OR IGNORE INTO ck_system_cutoff_audit SELECT ?,${id},${cut},${job},?,?,${await jsonRow(e,table,'x')},${minutes} FROM ${quote(table)} x WHERE ${where}`,kind,token,at));
 };
 const segmentCut=nextSQL('julianday(x.joined_at)',latest),jobCut=nextSQL(jobStart('x'),latest);
 await snapshot('segment','v2_ops_job_workers','x.id',"COALESCE(x.job_id,'')",segmentCut,`COALESCE(x.left_at,'')='' AND julianday(${segmentCut})<=julianday('${latest}')`,`CASE WHEN julianday(x.joined_at) IS NOT NULL THEN MAX(0,(julianday(${segmentCut})-julianday(x.joined_at))*1440) END`);
 await snapshot('job','v2_ops_jobs','x.id','x.id',jobCut,`COALESCE(x.status,'') NOT IN ('completed','cancelled','system_closed') AND julianday(${jobCut})<=julianday('${latest}')`);
 if(tables.has('sop_records')){
  await snapshot('record','sop_records','x.id','x.id',savedCut('job','x.id'),`x.kind IN ('task','dispatch','bulk_batch') AND ${selected('job','x.id')}`);
  if(tables.has('ck_bulk_work_batch_items'))await snapshot('record','sop_records','x.id','x.id',`'${latest}'`,`x.kind='bulk_batch' AND json_extract(x.state,'$.status') NOT IN ('completed','cancelled','system_closed') AND EXISTS(SELECT 1 FROM ck_bulk_work_batch_items i WHERE i.batch_id=x.id AND ${selected('job','i.job_id')}) AND NOT EXISTS(SELECT 1 FROM ck_bulk_work_batch_items i JOIN v2_ops_jobs j ON j.id=i.job_id WHERE i.batch_id=x.id AND julianday(j.created_at)>=julianday('${latest}'))`);
 }
 if(tables.has('v2_pick_worker_docs'))await snapshot('pick_clock','v2_pick_worker_docs','x.id','x.job_id',savedCut('segment','x.segment_id'),`${selected('segment','x.segment_id')} AND x.status='working'`);
 if(tables.has('ck_crew_borrows'))await snapshot('loan','ck_crew_borrows','x.id','x.destination_job_id',`'${latest}'`,`x.status IN ('borrowed','return_pending') AND (julianday(x.borrowed_at)<julianday('${latest}') OR julianday(x.borrowed_at) IS NULL) AND (${selected('job','x.source_job_id')} OR ${selected('job','x.destination_job_id')} OR ${selected('segment','x.source_segment_id')})`);
 for(const [table,kind,status]of [['v2_inbound_plan_jobs','inbound_execution','status'],['v2_issue_handle_runs','issue_execution','run_status']])if(tables.has(table))await snapshot(kind,table,'x.id',table==='v2_inbound_plan_jobs'?'x.id':'x.job_id',savedCut('job',table==='v2_inbound_plan_jobs'?'x.id':'x.job_id'),`${selected('job',table==='v2_inbound_plan_jobs'?'x.id':'x.job_id')} AND x.${status} NOT IN ('completed','cancelled')`);
 // All snapshots and mutations share one D1 transaction; no business finish
 // handler, inventory update, result insertion or document completion is called.
 sql.push(q(e,`UPDATE v2_ops_job_workers SET left_at=${savedCut('segment','v2_ops_job_workers.id')},leave_reason='system_cutoff:unverified' WHERE ${selected('segment','v2_ops_job_workers.id')} AND COALESCE(left_at,'')=''`));
 if(tables.has('v2_pick_worker_docs'))sql.push(q(e,`UPDATE v2_pick_worker_docs SET status='system_closed',finished_at=${savedCut('pick_clock','v2_pick_worker_docs.id')} WHERE ${selected('pick_clock','v2_pick_worker_docs.id')}`));
 if(tables.has('ck_crew_borrows'))sql.push(q(e,`UPDATE ck_crew_borrows SET status='closed',released_at=CASE WHEN released_at='' THEN ${savedCut('loan','ck_crew_borrows.id')} ELSE released_at END,blocked_reason='system_cutoff:unverified; no automatic return' WHERE ${selected('loan','ck_crew_borrows.id')}`));
 const after=alias=>`json_set(${alias}.state,'$.status','system_closed','$.pause_reason','${cutoffLabel}','$.system_cutoff',json_object('cutoff_at',${savedCut('record',alias+'.id')},'needs_review',json('true'),'policy','kst-0400-v1','previous_status',json_extract(${alias}.state,'$.status')),'$.paused_workers',COALESCE(json_extract(${alias}.state,'$.workers'),json('[]')))`;
 if(tables.has('sop_records')){
  if(tables.has('sop_events'))sql.push(q(e,`INSERT OR IGNORE INTO sop_events SELECT 'SYSTEM-CUTOFF:'||${savedCut('record','s.id')}||':'||s.id,s.id,s.revision,'sop_system_cutoff','SYSTEM-0400','系统04:00收尾',s.state,${after('s')},json_object('ok',json('true'),'status','system_closed','needs_review',json('true')),? FROM sop_records s WHERE ${selected('record','s.id')}`,at));
  sql.push(q(e,`UPDATE sop_records SET state=${after('sop_records')},revision=revision+1,updated_at=? WHERE ${selected('record','sop_records.id')}`,at));
 }
 sql.push(q(e,`UPDATE v2_ops_jobs SET status='system_closed',paused_at=${savedCut('job','v2_ops_jobs.id')},active_worker_count=0,updated_at=? WHERE ${selected('job','v2_ops_jobs.id')}`,at));
 for(const [table,kind,status]of [['v2_inbound_plan_jobs','inbound_execution','status'],['v2_issue_handle_runs','issue_execution','run_status']])if(tables.has(table))sql.push(q(e,`UPDATE ${table} SET ${status}='system_closed' WHERE ${selected(kind,table+'.id')}`));
 sql.push(q(e,`INSERT OR IGNORE INTO ck_system_cutoff_runs VALUES(?,?,?,'kst-0400-v1; earliest boundary per execution; unverified clocks excluded from effective labor')`,latest,at,token));
 await e.DB.batch(sql);
 return {ok:true,cutoff_at:latest,changes:await rows(e,'SELECT kind,COUNT(*) AS count FROM ck_system_cutoff_audit WHERE run_token=? GROUP BY kind',token)};
}
export async function guardSystemCutoffMutation(b,e){
 if(!cutoffEnabled(e))return;
 const id=b.job_id||b.id;if(!id||/_(get|detail|list|status|export|options)$/.test(b.action))return;
 const row=await q(e,"SELECT status FROM v2_ops_jobs WHERE id=?",id).first();
 if(row?.status==='system_closed'&&!['sop_native_resume','sop_task_start','sop_task_people','sop_bulk_batch_resume','sop_office_duty_resume'].includes(b.action))throw Error(cutoffLabel+'；请核对人员后从原任务续作 / 인원 확인 후 원래 작업을 재개하세요');
}
export async function correctSystemCutoff(b,e){
 const u=e.SOP_REQUEST_USER;
 if(!cutoffEnabled(e)||u?.scope!=='office'||u.role!=='manager')throw Error('仅办公室管理员可核实系统截止工时 / 관리자 확인 필요');
 await ensureCutoffSchema(e);
 const request=String(b.client_req_id||''),reason=String(b.reason||'').trim(),signature=JSON.stringify({segment:b.segment_id,expected:b.expected_end,end:b.end_at,reason,actor:u.id});
 if(!request||request.length>100||!reason||reason.length>500)throw Error('请填写核实依据和请求编号');
 const replay=async()=>{const old=await q(e,'SELECT signature,after_json FROM ck_system_cutoff_corrections WHERE request_id=?',request).first();if(!old)return null;if(old.signature!==signature)throw Error('请求内容已变化，请重新核对');return {ok:true,replayed:true,segment:JSON.parse(old.after_json)};};
 const old=await replay();if(old)return old;
 const s=await q(e,'SELECT * FROM v2_ops_job_workers WHERE id=?',String(b.segment_id||'')).first(),end=Date.parse(b.end_at);
 if(!s||!forcedClock(s)||s.left_at!==b.expected_end)throw Error('该段已变化或不属于待核实系统截止，请刷新');
 if(!Number.isFinite(end)||!Number.isFinite(Date.parse(s.joined_at))||end<Date.parse(s.joined_at)||end>Date.parse(s.left_at)||end>Date.now())throw Error('实际结束须在原开始与系统截止之间；截止后续作请另开计时段');
 const after={...s,left_at:new Date(end).toISOString(),minutes_worked:Math.round((end-Date.parse(s.joined_at))/6000)/10,leave_reason:'system_cutoff_verified'},at=new Date().toISOString();
 try{await e.DB.batch([
  q(e,`INSERT INTO ck_system_cutoff_corrections SELECT ?,id,left_at,?,?,?, ?,?,?,? FROM v2_ops_job_workers WHERE id=? AND left_at=? AND leave_reason='system_cutoff:unverified'`,request,JSON.stringify(s),JSON.stringify(after),u.id,u.name||u.id,reason,at,signature,s.id,s.left_at),
  q(e,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=?,leave_reason='system_cutoff_verified' WHERE id=? AND EXISTS(SELECT 1 FROM ck_system_cutoff_corrections WHERE request_id=?)`,after.left_at,after.minutes_worked,s.id,request),
  q(e,`SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_system_cutoff_corrections WHERE request_id=?) THEN json('system_cutoff_correction_changed') ELSE 1 END`,request)
 ]);}catch(error){const done=await replay();if(done)return done;throw error;}
 return {ok:true,segment:after};
}
