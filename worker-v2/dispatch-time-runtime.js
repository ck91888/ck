// Event-driven dispatch labor. SQL triggers share the transaction of the real business write.
// No browser presence, synthetic jobs, historical backfill, or client-supplied person names.
import {ensureSchema} from './schema-ready.js';
import {ensureAttendance} from './attendance.js';
import {ensureAccess} from './access-control.js';
import {ensureDailyDispatch} from './daily-dispatch-permissions.js';
import {ensureCrewBorrow} from './crew-borrow.js';
import {ensureDispatchShare} from './dispatch-share.js';
import {workerBadgeMatch} from './daily-badge-identity.js';
import {activeAttendanceSQL} from './attendance-management.js';
import {dispatchTimeEnabled} from './dispatch-time.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),seen=new WeakMap();
const open="('pending','assigned','working','paused','awaiting_close','awaiting_review','rework')";
const now="strftime('%Y-%m-%dT%H:%M:%fZ','now')";
const cutoff=t=>`strftime('%Y-%m-%dT%H:%M:%fZ',julianday(${t}),'+5 hours','start of day','-5 hours')`;
const boundary=s=>`MIN(strftime('%Y-%m-%dT%H:%M:%fZ',julianday(${s}.started_at),'+9 hours','start of day','+1 day','-9 hours'),strftime('%Y-%m-%dT%H:%M:%fZ',julianday(${s}.started_at),'+5 hours','start of day','+1 day','-5 hours'))`;
const pair=(person,owner,day)=>`EXISTS(SELECT 1 FROM ck_dispatch_share_pairs pair WHERE (SELECT sharing_enabled FROM ck_dispatch_time_config WHERE id=1)=1 AND pair.status='active' AND pair.day=${day} AND ((pair.person_lo=${person} AND pair.person_hi=${owner}) OR (pair.person_hi=${person} AND pair.person_lo=${owner})))`;
const delegate=(person,state)=>`EXISTS(SELECT 1 FROM json_each(${state},'$.delegates') WHERE value=${person}) AND NOT EXISTS(SELECT 1 FROM json_each(${state},'$.owner_transfers') WHERE json_extract(value,'$.from_id')=${person})`;
const aliases=(person,worker)=>`EXISTS(SELECT 1 FROM ck_dispatch_time_aliases alias WHERE alias.person_id=${person} AND ${workerBadgeMatch(worker,'alias.worker_id')})`;
const duties=(s,t)=>`EXISTS(SELECT 1 FROM ck_dispatch_time_links link JOIN sop_records record ON record.id=link.job_id JOIN v2_ops_jobs job ON job.id=link.job_id WHERE link.person_id=${s}.person_id AND link.attendance_id=${s}.attendance_id AND job.job_type='bulk_op' AND job.status IN ${open} AND json_extract(record.state,'$.status') IN ${open} AND julianday(link.engaged_at)>=julianday(${cutoff(t)}) AND (json_extract(record.state,'$.owner_id')=link.person_id OR (link.source='shared' AND ${pair('link.person_id',"json_extract(record.state,'$.owner_id')",`${s}.day`)}) OR (link.source='delegate' AND ${delegate('link.person_id','record.state')})))`;
const identity="SELECT d.id,d.worker_id,d.name,d.agency,d.day,d.identity_key,d.signed_in,d.signed_out,d.version,d.person_id AS attendance_person_id,actual.enabled AS actual_enabled,p.id AS person_id,p.enabled AS person_enabled,p.badge_id AS permanent_badge,CASE WHEN p.badge_id LIKE 'EMP-%' THEN EXISTS(SELECT 1 FROM ck_employee_profiles employee WHERE employee.person_id=p.id AND p.badge_id='EMP-'||employee.employee_no) AND COALESCE((SELECT enabled FROM ck_field_access access WHERE access.person_id=p.id),1)=1 ELSE p.badge_id GLOB 'DAF-*' AND EXISTS(SELECT 1 FROM ck_daily_dispatch_grants grant_row WHERE grant_row.person_id=p.id AND grant_row.enabled=1 AND EXISTS(SELECT 1 FROM json_each(grant_row.departments) WHERE value='bulk')) END AS qualified FROM ck_attendance_days d JOIN ck_attendance_people actual ON actual.id=d.person_id JOIN ck_attendance_people p ON p.id=COALESCE((SELECT permanent_person_id FROM ck_daily_dispatch_links link WHERE link.daily_person_id=d.person_id AND link.day=d.day),d.person_id) WHERE "+activeAttendanceSQL('d');
function status(s,t){return `(SELECT CASE
 WHEN attendance.id IS NULL OR attendance.day<>date(${t},'+9 hours') THEN 'pending_review'
 WHEN attendance.signed_out<>'' THEN 'out'
 WHEN ${s}.requires_action=1 THEN 'waiting_dispatch'
 WHEN (SELECT COUNT(*) FROM ck_dispatch_time_identity duplicate WHERE duplicate.person_id=${s}.person_id AND duplicate.day=${s}.day AND duplicate.signed_out='')<>1 THEN 'pending_review'
 WHEN attendance.person_enabled<>1 OR attendance.actual_enabled<>1 OR attendance.qualified<>1 THEN 'blocked'
 WHEN EXISTS(SELECT 1 FROM ck_attendance_breaks rest WHERE rest.attendance_id=attendance.id AND rest.ended_at='') THEN 'rest'
 WHEN EXISTS(SELECT 1 FROM v2_ops_job_workers worker LEFT JOIN v2_ops_jobs job ON job.id=worker.job_id WHERE COALESCE(worker.left_at,'')='' AND ${aliases(`${s}.person_id`,'worker.worker_id')} AND (job.id IS NULL OR job.status NOT IN ('working','pending','awaiting_close') OR julianday(worker.joined_at)<julianday(attendance.signed_in) OR date(worker.joined_at,'+9 hours')<>attendance.day)) THEN 'pending_review'
 WHEN EXISTS(SELECT 1 FROM v2_ops_job_workers worker WHERE COALESCE(worker.left_at,'')='' AND ${aliases(`${s}.person_id`,'worker.worker_id')}) THEN 'operating'
 WHEN EXISTS(SELECT 1 FROM ck_crew_borrows loan WHERE loan.status IN ('borrowed','return_pending') AND ${aliases(`${s}.person_id`,'loan.worker_id')}) THEN 'blocked'
 WHEN NOT ${duties(s,t)} THEN CASE WHEN ${s}.needs_review=1 THEN 'pending_review' ELSE 'waiting_dispatch' END
 ELSE 'dispatching' END FROM (SELECT 1) singleton LEFT JOIN ck_dispatch_time_identity attendance ON attendance.id=${s}.attendance_id AND attendance.person_id=${s}.person_id)`;}
function tick(person,t,reason,key){return `INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT ${person},${t},'${reason}',${key} WHERE ${person} IS NOT NULL;`;}
const businessActions=['sop_task_dispatch','sop_native_start','sop_task_start','sop_task_people','sop_task_pause','sop_task_finish','sop_task_complete_review','sop_task_review','sop_native_people','sop_native_resume','sop_native_pause','sop_owner_transfer','sop_cargo_group_complete','sop_cargo_group_review','sop_cargo_job_finish','sop_cargo_groups_ack','sop_bulk_output_draft_save','sop_bulk_batch_resume','sop_bulk_batch_pause','sop_bulk_batch_people','sop_bulk_batch_cancel','sop_bulk_batch_switch'];
function capture(eventKey,actor,job,t,state,action){
 const owner=`json_extract(${state},'$.owner_id')`;
 return `INSERT OR IGNORE INTO ck_dispatch_time_actions(event_key,person_id,actor_id,job_id,at,source,action) SELECT ${eventKey},${actor},${actor},${job},${t},CASE WHEN ${owner}=${actor} THEN 'owner' WHEN ${delegate(actor,state)} THEN 'delegate' ELSE 'shared' END,${action} WHERE ${owner}=${actor} OR (${delegate(actor,state)}) OR ${pair(actor,owner,`date(${t},'+9 hours')`)};`;
}
function schema(){
 const enabled="(SELECT enabled FROM ck_dispatch_time_config WHERE id=1)=1",t="MAX(NEW.at,COALESCE((SELECT MAX(updated_at) FROM ck_dispatch_time_days WHERE person_id=NEW.person_id),NEW.at))";
 const sql=[
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_config(id INTEGER PRIMARY KEY CHECK(id=1),enabled INTEGER NOT NULL,sharing_enabled INTEGER NOT NULL)`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_days(person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,day TEXT NOT NULL,activated_at TEXT NOT NULL,updated_at TEXT NOT NULL,status TEXT NOT NULL,needs_review INTEGER NOT NULL DEFAULT 0,requires_action INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(person_id,attendance_id))`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_links(person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,job_id TEXT NOT NULL,source TEXT NOT NULL,engaged_at TEXT NOT NULL,PRIMARY KEY(person_id,attendance_id,job_id))`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_spans(id TEXT PRIMARY KEY,person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,day TEXT NOT NULL,started_at TEXT NOT NULL,ended_at TEXT NOT NULL DEFAULT '',start_reason TEXT NOT NULL,end_reason TEXT NOT NULL DEFAULT '',needs_review INTEGER NOT NULL DEFAULT 0,source_event TEXT NOT NULL)`,
 `CREATE UNIQUE INDEX IF NOT EXISTS ck_dispatch_time_one_open_span ON ck_dispatch_time_spans(person_id) WHERE ended_at=''`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_span_attendance ON ck_dispatch_time_spans(attendance_id,started_at)`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_actions(event_key TEXT PRIMARY KEY,person_id TEXT NOT NULL,actor_id TEXT NOT NULL,job_id TEXT NOT NULL,at TEXT NOT NULL,source TEXT NOT NULL,action TEXT NOT NULL)`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_ticks(id INTEGER PRIMARY KEY,person_id TEXT NOT NULL,at TEXT NOT NULL,reason TEXT NOT NULL,event_key TEXT NOT NULL)`,
 `CREATE VIEW IF NOT EXISTS ck_dispatch_time_identity AS ${identity}`,
 `CREATE VIEW IF NOT EXISTS ck_dispatch_time_aliases AS SELECT DISTINCT person_id,worker_id FROM ck_dispatch_time_identity UNION SELECT id AS person_id,badge_id AS worker_id FROM ck_attendance_people WHERE kind='permanent'`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_tick AFTER INSERT ON ck_dispatch_time_ticks BEGIN
 UPDATE ck_dispatch_time_days SET needs_review=1 WHERE person_id=NEW.person_id AND EXISTS(SELECT 1 FROM ck_dispatch_time_spans span WHERE span.person_id=NEW.person_id AND span.attendance_id=ck_dispatch_time_days.attendance_id AND span.ended_at='' AND julianday(${boundary('span')})<=julianday(${t}));
 UPDATE ck_dispatch_time_spans AS span SET ended_at=${boundary('span')},end_reason='system_cutoff:unverified',needs_review=1 WHERE span.person_id=NEW.person_id AND span.ended_at='' AND julianday(${boundary('span')})<=julianday(${t});
 UPDATE ck_dispatch_time_days SET status=CASE WHEN (SELECT enabled FROM ck_dispatch_time_config WHERE id=1)=0 THEN 'blocked' ELSE ${status('ck_dispatch_time_days',t)} END WHERE person_id=NEW.person_id;
 UPDATE ck_dispatch_time_spans SET ended_at=MAX(started_at,${t}),end_reason=NEW.reason WHERE person_id=NEW.person_id AND ended_at='' AND NOT EXISTS(SELECT 1 FROM ck_dispatch_time_days state WHERE state.person_id=NEW.person_id AND state.attendance_id=ck_dispatch_time_spans.attendance_id AND state.status='dispatching');
 INSERT INTO ck_dispatch_time_spans SELECT 'DTS-'||lower(hex(randomblob(16))),state.person_id,state.attendance_id,state.day,${t},'',NEW.reason,'',0,NEW.event_key FROM ck_dispatch_time_days state WHERE state.person_id=NEW.person_id AND state.status='dispatching' AND NOT EXISTS(SELECT 1 FROM ck_dispatch_time_spans span WHERE span.person_id=NEW.person_id AND span.ended_at='');
 UPDATE ck_dispatch_time_days SET updated_at=${t} WHERE person_id=NEW.person_id;
 DELETE FROM ck_dispatch_time_ticks WHERE id=NEW.id;
 END`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_engage AFTER INSERT ON ck_dispatch_time_actions WHEN ${enabled} BEGIN
 INSERT OR IGNORE INTO ck_dispatch_time_days(person_id,attendance_id,day,activated_at,updated_at,status) SELECT NEW.person_id,attendance.id,attendance.day,NEW.at,NEW.at,'waiting_dispatch' FROM ck_dispatch_time_identity attendance WHERE attendance.person_id=NEW.person_id AND attendance.day=date(NEW.at,'+9 hours') AND attendance.signed_out='' AND attendance.person_enabled=1 AND attendance.actual_enabled=1 AND attendance.qualified=1 AND julianday(attendance.signed_in)<=julianday(NEW.at) AND (SELECT COUNT(*) FROM ck_dispatch_time_identity duplicate WHERE duplicate.person_id=NEW.person_id AND duplicate.day=attendance.day AND duplicate.signed_out='')=1;
 UPDATE ck_dispatch_time_days SET requires_action=0 WHERE person_id=NEW.person_id AND day=date(NEW.at,'+9 hours') AND EXISTS(SELECT 1 FROM ck_dispatch_time_identity attendance WHERE attendance.id=ck_dispatch_time_days.attendance_id AND attendance.signed_out='');
 INSERT INTO ck_dispatch_time_links SELECT state.person_id,state.attendance_id,NEW.job_id,NEW.source,NEW.at FROM ck_dispatch_time_days state WHERE state.person_id=NEW.person_id AND state.day=date(NEW.at,'+9 hours') AND EXISTS(SELECT 1 FROM ck_dispatch_time_identity attendance WHERE attendance.id=state.attendance_id AND attendance.signed_out='') ON CONFLICT(person_id,attendance_id,job_id) DO UPDATE SET source=excluded.source,engaged_at=MAX(engaged_at,excluded.engaged_at);
 ${tick('NEW.person_id','NEW.at','business_action','NEW.event_key')}
 END`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_sop_action AFTER INSERT ON sop_events WHEN ${enabled} AND json_extract(NEW.after_json,'$.job_type')='bulk_op' AND NEW.action IN (${businessActions.map(x=>"'"+x+"'").join(',')}) BEGIN
 ${capture('NEW.request_id','NEW.actor_id','NEW.record_id','NEW.created_at','NEW.after_json','NEW.action')}
 INSERT OR IGNORE INTO ck_dispatch_time_actions SELECT NEW.request_id||':transfer-target',json_extract(NEW.after_json,'$.owner_id'),NEW.actor_id,NEW.record_id,NEW.created_at,'owner',NEW.action FROM (SELECT 1) WHERE NEW.action='sop_owner_transfer' AND json_extract(NEW.after_json,'$.owner_id')<>json_extract(NEW.before_json,'$.owner_id');
 END`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_native_result AFTER INSERT ON v2_ops_job_results WHEN ${enabled} AND COALESCE(json_extract(NEW.result_json,'$.dispatch_actor_id'),'')<>'' AND EXISTS(SELECT 1 FROM v2_ops_jobs WHERE id=NEW.job_id AND job_type='bulk_op') BEGIN
 ${capture("'result:'||NEW.id","json_extract(NEW.result_json,'$.dispatch_actor_id')",'NEW.job_id','NEW.created_at',"(SELECT state FROM sop_records WHERE id=NEW.job_id)","'v2_bulk_op_job_finish'")}
 END`
 ];
 // Mutations below only reconcile existing sessions; they never create a day or responsibility.
 function trigger(name,op,table,body,when=enabled){sql.push(`CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_${name} AFTER ${op} ON ${table} WHEN ${when} BEGIN ${body} END`);}
 const recordTick=`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,NEW.updated_at,'assignment_change',NEW.id FROM ck_dispatch_time_links WHERE job_id=NEW.id;`;
 trigger('record_insert','INSERT','sop_records',recordTick);trigger('record_update','UPDATE OF state','sop_records',recordTick);
 const jobTime="MAX(COALESCE(NEW.updated_at,''),COALESCE((SELECT MAX(created_at) FROM sop_events WHERE record_id=NEW.id),''),COALESCE((SELECT MAX(created_at) FROM v2_ops_job_results WHERE job_id=NEW.id),''))";
 const jobTick=`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,${jobTime},'job_change',NEW.id FROM ck_dispatch_time_links WHERE job_id=NEW.id;`;
 trigger('job_insert','INSERT','v2_ops_jobs',jobTick);trigger('job_update','UPDATE OF status','v2_ops_jobs',jobTick);
 const workerTick=`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,CASE WHEN COALESCE(NEW.left_at,'')='' THEN NEW.joined_at ELSE NEW.left_at END,'operation_change',NEW.id FROM ck_dispatch_time_aliases alias WHERE ${workerBadgeMatch('NEW.worker_id','alias.worker_id')};`;
 trigger('worker_insert','INSERT','v2_ops_job_workers',workerTick);trigger('worker_update','UPDATE OF left_at,worker_id,joined_at','v2_ops_job_workers',workerTick);
 trigger('attendance','UPDATE OF signed_out,signed_in','ck_attendance_days',`UPDATE ck_dispatch_time_days SET requires_action=1 WHERE attendance_id=NEW.id AND NEW.signed_out<>''; INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,CASE WHEN NEW.signed_out<>'' THEN NEW.signed_out ELSE COALESCE((SELECT MAX(at) FROM ck_attendance_events WHERE record_id=NEW.id),${now}) END,'attendance_change',NEW.id FROM ck_dispatch_time_days WHERE attendance_id=NEW.id;`);
 for(const op of ['INSERT','UPDATE'])trigger('rest_'+op.toLowerCase(),op,'ck_attendance_breaks',`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,CASE WHEN NEW.ended_at='' THEN NEW.started_at ELSE NEW.ended_at END,'rest_change',NEW.id FROM ck_dispatch_time_days WHERE attendance_id=NEW.attendance_id;`);
 trigger('grant','UPDATE','ck_daily_dispatch_grants',tick('NEW.person_id','NEW.updated_at','qualification_change',"'grant:'||NEW.person_id"));
 for(const op of ['INSERT','UPDATE'])trigger('access_'+op.toLowerCase(),op,'ck_field_access',tick('NEW.person_id','NEW.updated_at','qualification_change',"'access:'||NEW.person_id"));
 trigger('person','UPDATE OF enabled','ck_attendance_people',`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,${now},'person_change',NEW.id FROM ck_dispatch_time_identity WHERE person_id=NEW.id OR attendance_person_id=NEW.id;`);
 trigger('share','UPDATE OF status','ck_dispatch_share_pairs',tick('NEW.person_lo',`COALESCE(NULLIF(NEW.ended_at,''),NULLIF(NEW.accepted_at,''),NEW.created_at)`,'sharing_change',"'pair:'||NEW.id")+tick('NEW.person_hi',`COALESCE(NULLIF(NEW.ended_at,''),NULLIF(NEW.accepted_at,''),NEW.created_at)`,'sharing_change',"'pair:'||NEW.id"));
 trigger('loan','UPDATE OF status','ck_crew_borrows',`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,MAX(COALESCE(NEW.returned_at,''),COALESCE(NEW.released_at,''),NEW.borrowed_at),'loan_change',NEW.id FROM ck_dispatch_time_aliases alias WHERE ${workerBadgeMatch('NEW.worker_id','alias.worker_id')};`);
 for(const op of ['INSERT','UPDATE'])trigger('attendance_management_'+op.toLowerCase(),op,'ck_attendance_management',`UPDATE ck_dispatch_time_days SET requires_action=1 WHERE attendance_id=NEW.attendance_id AND NEW.voided_at<>''; INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT person_id,CASE WHEN NEW.voided_at<>'' THEN NEW.voided_at ELSE ${now} END,'attendance_management',NEW.attendance_id FROM ck_dispatch_time_days WHERE attendance_id=NEW.attendance_id;`);
 for(const op of ['INSERT','UPDATE'])sql.push(`CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_void_${op.toLowerCase()} BEFORE ${op} ON ck_attendance_management WHEN NEW.voided_at<>'' AND EXISTS(SELECT 1 FROM ck_dispatch_time_days WHERE attendance_id=NEW.attendance_id) BEGIN SELECT RAISE(ABORT,'Attendance has dispatch labor; cannot void'); END`);
 trigger('config','UPDATE','ck_dispatch_time_config',`UPDATE ck_dispatch_time_days SET requires_action=1 WHERE NEW.enabled=0; INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,${now},'feature_config','config' FROM ck_dispatch_time_days;`,'OLD.enabled<>NEW.enabled OR OLD.sharing_enabled<>NEW.sharing_enabled');
 return sql;
}
export async function ensureAutoDispatchTime(e){
 const enabled=dispatchTimeEnabled(e),sharing=e.SOP_DISPATCH_SHARE_ENABLED==='true';
 const signature=enabled+':'+sharing;if(seen.get(e.DB)===signature)return;
 if(enabled){
  await ensureAttendance(e);await ensureAccess(e);await ensureDailyDispatch(e);await ensureCrewBorrow(e);
  // Schema initialization does not turn on shared permissions; the actual flag remains authoritative.
  await ensureDispatchShare({...e,SOP_DISPATCH_SHARE_ENABLED:'true'});
  await ensureSchema(e.DB,'auto-dispatch-runtime-v1',()=>e.DB.batch(schema().map(s=>e.DB.prepare(s))));
 }else if(!await q(e,"SELECT name FROM sqlite_master WHERE type='table' AND name='ck_dispatch_time_config'").first()){seen.set(e.DB,signature);return;}
 await q(e,'INSERT INTO ck_dispatch_time_config VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET enabled=excluded.enabled,sharing_enabled=excluded.sharing_enabled WHERE enabled<>excluded.enabled OR sharing_enabled<>excluded.sharing_enabled',enabled?1:0,sharing?1:0).run();seen.set(e.DB,signature);
}
export async function dispatchTimeAttendance(e,date,asOf){
 if(!dispatchTimeEnabled(e))return new Map();
 const result=await e.DB.batch([q(e,'SELECT * FROM ck_dispatch_time_days WHERE day=?',date),q(e,'SELECT * FROM ck_dispatch_time_spans WHERE day=? ORDER BY started_at,id',date)]);
 const items=new Map(result[0].results.map(row=>[row.attendance_id,{...row,segments:[],seconds:0,pendingSeconds:0}]));
 for(const span of result[1].results){const item=items.get(span.attendance_id);if(!item)continue;const start=Date.parse(span.started_at);
  // Calculate the first 04:00 KST strictly after start without extending an unclosed historical span.
  const four=Math.floor((start+5*3600000)/86400000)*86400000-5*3600000+86400000,bound=Math.min(Date.parse(date+'T00:00:00+09:00')+86400000,four),until=Date.parse(asOf),pending=!!span.needs_review||!span.ended_at&&until>=bound,end=span.ended_at?Math.min(Date.parse(span.ended_at),until):Math.min(until,bound),seconds=Math.max(0,(end-start)/1000);
  if(!seconds)continue;item.segments.push({id:span.id,start:span.started_at,end:span.ended_at||(!pending?'':new Date(end).toISOString()),endReason:pending?'system_cutoff:unverified':span.end_reason,startReason:span.start_reason,needsVerification:pending,seconds});item[pending?'pendingSeconds':'seconds']+=seconds;if(pending&&item.status==='dispatching')item.status='pending_review';
 }
 return items;
}
