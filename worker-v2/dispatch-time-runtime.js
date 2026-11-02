import {attendanceReturnEnabled} from './attendance-return-schema.js';
import {ensureAttendanceCurrentView,attendanceCutoffSQL} from './attendance-context.js';
// Event-driven dispatch labor. SQL triggers share the transaction of the real business write.
// No browser presence, synthetic jobs, historical backfill, or client-supplied person names.
import {ensureSchema,shareSchemaBinding} from './schema-ready.js';
import {ensureAttendance} from './attendance.js';
import {ensureAccess} from './access-control.js';
import {ensureDailyDispatch} from './daily-dispatch-permissions.js';
import {ensureCrewBorrow} from './crew-borrow.js';
import {ensureDispatchShare} from './dispatch-share.js';
import {workerBadgeMatch} from './daily-badge-identity.js';
import {activeAttendanceSQL} from './attendance-management.js';
import {dispatchTimeEnabled} from './dispatch-time.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),wrappers=new WeakMap(),bases=new WeakMap(),configExists=new WeakSet();
const configDDL=`CREATE TABLE IF NOT EXISTS ck_dispatch_time_config(id INTEGER PRIMARY KEY CHECK(id=1),enabled INTEGER NOT NULL,sharing_enabled INTEGER NOT NULL)`;
const configSQL='INSERT INTO ck_dispatch_time_config VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET enabled=excluded.enabled,sharing_enabled=excluded.sharing_enabled WHERE enabled<>excluded.enabled OR sharing_enabled<>excluded.sharing_enabled';
const open="('pending','assigned','working','paused','awaiting_close','awaiting_review','rework')";
const now="strftime('%Y-%m-%dT%H:%M:%fZ','now')";
const cutoff=t=>`strftime('%Y-%m-%dT%H:%M:%fZ',julianday(${t}),'+5 hours','start of day','-5 hours')`;
const boundary=s=>`MIN(strftime('%Y-%m-%dT%H:%M:%fZ',julianday(${s}.started_at),'+9 hours','start of day','+1 day','-9 hours'),strftime('%Y-%m-%dT%H:%M:%fZ',julianday(${s}.started_at),'+5 hours','start of day','+1 day','-5 hours'))`;
const pair=(person,owner,day,grant='')=>`EXISTS(SELECT 1 FROM ck_dispatch_share_pairs pair WHERE (SELECT sharing_enabled FROM ck_dispatch_time_config WHERE id=1)=1 AND pair.status='active' AND pair.day=${day} AND ((pair.person_lo=${person} AND pair.person_hi=${owner}) OR (pair.person_hi=${person} AND pair.person_lo=${owner}))${grant?` AND pair.id=${grant}`:''})`;
const delegate=(person,state)=>`EXISTS(SELECT 1 FROM json_each(${state},'$.delegates') WHERE value=${person}) AND NOT EXISTS(SELECT 1 FROM json_each(${state},'$.owner_transfers') WHERE json_extract(value,'$.from_id')=${person})`;
const aliases=(person,worker)=>`EXISTS(SELECT 1 FROM ck_dispatch_time_aliases alias WHERE alias.person_id=${person} AND ${workerBadgeMatch(worker,'alias.worker_id')})`;
// Registered badge -> stable person is a keyed reverse lookup. The NOCASE
// prefilter only narrows candidates; the existing identity predicate still
// enforces case-sensitive EMP/DAF/custom badges and canonical system-daily IDs.
const workerPeople=worker=>`SELECT COALESCE(link.permanent_person_id,person.id) AS person_id FROM ck_attendance_people person LEFT JOIN ck_daily_dispatch_links link ON link.daily_person_id=person.id WHERE person.badge_id=${worker} COLLATE NOCASE AND ${workerBadgeMatch(worker,'person.badge_id')}`;
const duties=(s,t)=>`EXISTS(SELECT 1 FROM ck_dispatch_time_links link JOIN sop_records record ON record.id=link.job_id JOIN v2_ops_jobs job ON job.id=link.job_id WHERE link.person_id=${s}.person_id AND link.attendance_id=${s}.attendance_id AND job.job_type='bulk_op' AND job.status IN ${open} AND json_extract(record.state,'$.status') IN ${open} AND julianday(link.engaged_at)>=julianday(${cutoff(t)}) AND (json_extract(record.state,'$.owner_id')=link.person_id OR (link.source='shared' AND ${pair('link.person_id',"json_extract(record.state,'$.owner_id')",`${s}.day`,'link.share_pair_id')}) OR (link.source='delegate' AND ${delegate('link.person_id','record.state')})))`;
const returnOn="COALESCE((SELECT enabled FROM ck_attendance_return_config WHERE id=1),0)=1";
const currentAt=(a,t)=>`((${returnOn} AND ${a}.span_status='open' AND julianday(${attendanceCutoffSQL(a+'.signed_in')})>julianday(${t})) OR (NOT (${returnOn}) AND ${a}.day=date(${t},'+9 hours')))`;
const rawIdentity="SELECT d.id,'' AS attendance_span_id,CASE WHEN d.signed_out='' THEN 'open' ELSE 'closed' END AS span_status,d.worker_id,d.name,d.agency,d.day,d.identity_key,d.signed_in,d.signed_out,d.version,d.person_id AS attendance_person_id,actual.enabled AS actual_enabled,p.id AS person_id,p.enabled AS person_enabled,p.badge_id AS permanent_badge,CASE WHEN p.badge_id LIKE 'EMP-%' THEN EXISTS(SELECT 1 FROM ck_employee_profiles employee WHERE employee.person_id=p.id AND p.badge_id='EMP-'||employee.employee_no) AND COALESCE((SELECT enabled FROM ck_field_access access WHERE access.person_id=p.id),1)=1 ELSE p.badge_id GLOB 'DAF-*' AND EXISTS(SELECT 1 FROM ck_daily_dispatch_grants grant_row WHERE grant_row.person_id=p.id AND grant_row.enabled=1 AND EXISTS(SELECT 1 FROM json_each(grant_row.departments) WHERE value='bulk')) END AS qualified FROM ck_attendance_days d JOIN ck_attendance_people actual ON actual.id=d.person_id JOIN ck_attendance_people p ON p.id=COALESCE((SELECT permanent_person_id FROM ck_daily_dispatch_links link WHERE link.daily_person_id=d.person_id AND link.day=d.day),d.person_id) WHERE "+activeAttendanceSQL('d')+' AND '+workerBadgeMatch('d.worker_id','actual.badge_id');
const childIdentity="SELECT d.id,s.id AS attendance_span_id,s.status AS span_status,s.badge_id AS worker_id,d.name,d.agency,date(s.started_at,'+9 hours') AS day,d.identity_key,s.started_at AS signed_in,CASE WHEN s.status='pending_review' AND s.ended_at='' THEN 'pending_review' ELSE s.ended_at END AS signed_out,d.version,s.source_person_id AS attendance_person_id,actual.enabled AS actual_enabled,p.id AS person_id,p.enabled AS person_enabled,p.badge_id AS permanent_badge,CASE WHEN p.badge_id LIKE 'EMP-%' THEN EXISTS(SELECT 1 FROM ck_employee_profiles employee WHERE employee.person_id=p.id AND p.badge_id='EMP-'||employee.employee_no) AND COALESCE((SELECT enabled FROM ck_field_access access WHERE access.person_id=p.id),1)=1 ELSE p.badge_id GLOB 'DAF-*' AND EXISTS(SELECT 1 FROM ck_daily_dispatch_grants grant_row WHERE grant_row.person_id=p.id AND grant_row.enabled=1 AND EXISTS(SELECT 1 FROM json_each(grant_row.departments) WHERE value='bulk')) END AS qualified FROM ck_attendance_spans s JOIN ck_attendance_days d ON d.id=s.attendance_id JOIN ck_attendance_people actual ON actual.id=s.source_person_id JOIN ck_attendance_people p ON p.id=s.person_id WHERE s.ordinal=(SELECT MAX(latest.ordinal) FROM ck_attendance_spans latest WHERE latest.attendance_id=d.id) AND "+activeAttendanceSQL('d')+' AND '+workerBadgeMatch('s.badge_id','actual.badge_id');
const identity=rawIdentity;
function status(s,t){return `CASE WHEN ${returnOn} THEN ${statusFor(s,t,'ck_dispatch_time_return_identity',true)} ELSE ${statusFor(s,t,'ck_dispatch_time_identity',false)} END`;}
function statusFor(s,t,view,returns){return `COALESCE((SELECT CASE
 WHEN attendance.signed_out<>'' THEN 'out'
 WHEN NOT ${returns?`(attendance.span_status='open' AND julianday(attendance.signed_in)<=julianday(${t}) AND julianday(${attendanceCutoffSQL('attendance.signed_in')})>julianday(${t}))`:`attendance.day=date(${t},'+9 hours')`} THEN 'pending_review'
 WHEN ${s}.requires_action=1 THEN 'waiting_dispatch'
 WHEN (SELECT COUNT(*) FROM ${view} duplicate WHERE duplicate.person_id=${s}.person_id AND ${returns?'1':`duplicate.day=${s}.day`} AND duplicate.signed_out='')<>1 THEN 'pending_review'
 WHEN attendance.person_enabled<>1 OR attendance.actual_enabled<>1 OR attendance.qualified<>1 THEN 'blocked'
 WHEN EXISTS(SELECT 1 FROM ck_attendance_breaks rest WHERE rest.attendance_id=attendance.id AND rest.ended_at='') THEN 'rest'
 WHEN EXISTS(SELECT 1 FROM v2_ops_job_workers worker LEFT JOIN v2_ops_jobs job ON job.id=worker.job_id WHERE worker.left_at='' AND ${aliases(`${s}.person_id`,'worker.worker_id')} AND (job.id IS NULL OR job.status NOT IN ('working','pending','awaiting_close') OR julianday(worker.joined_at)<julianday(attendance.signed_in) OR (${returns?'0':`date(worker.joined_at,'+9 hours')<>attendance.day`}))) THEN 'pending_review'
 WHEN EXISTS(SELECT 1 FROM v2_ops_job_workers worker WHERE worker.left_at='' AND ${aliases(`${s}.person_id`,'worker.worker_id')}) THEN 'operating'
 WHEN EXISTS(SELECT 1 FROM ck_crew_borrows loan WHERE loan.status IN ('borrowed','return_pending') AND ${aliases(`${s}.person_id`,'loan.worker_id')}) THEN 'blocked'
 WHEN NOT ${duties(s,t)} THEN CASE WHEN ${s}.needs_review=1 THEN 'pending_review' ELSE 'waiting_dispatch' END
 ELSE 'dispatching' END FROM ${view} attendance WHERE attendance.id=${s}.attendance_id AND attendance.person_id=${s}.person_id),'pending_review')`;}
function tick(person,t,reason,key){return `INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT ${person},${t},'${reason}',${key} WHERE ${person} IS NOT NULL;`;}
const businessActions=['sop_task_dispatch','sop_native_start','sop_task_start','sop_task_people','sop_task_pause','sop_task_finish','sop_task_complete_review','sop_task_review','sop_native_people','sop_native_resume','sop_native_pause','sop_owner_transfer','sop_cargo_group_complete','sop_cargo_group_review','sop_cargo_job_finish','sop_cargo_groups_ack','sop_bulk_output_draft_save','sop_bulk_batch_resume','sop_bulk_batch_pause','sop_bulk_batch_people','sop_bulk_batch_cancel','sop_bulk_batch_switch'];
function capture(eventKey,actor,job,t,state,action,admitted='1'){
 const owner=`json_extract(${state},'$.owner_id')`;
 return `INSERT OR IGNORE INTO ck_dispatch_time_actions(event_key,person_id,actor_id,job_id,at,source,action) SELECT ${eventKey},${actor},${actor},${job},${t},CASE WHEN ${owner}=${actor} THEN 'owner' WHEN ${delegate(actor,state)} THEN 'delegate' ELSE 'shared' END,${action} WHERE (${admitted}) AND (${owner}=${actor} OR (${delegate(actor,state)}) OR ${pair(actor,owner,`date(${t},'+9 hours')`)});`;
}
function engage(view,returns){return ` INSERT OR IGNORE INTO ck_dispatch_time_days(person_id,attendance_id,day,activated_at,updated_at,status,attendance_span_id) SELECT NEW.person_id,attendance.id,date(NEW.at,'+9 hours'),NEW.at,NEW.at,'waiting_dispatch',attendance.attendance_span_id FROM ${view} attendance WHERE ${returns?returnOn:`NOT (${returnOn})`} AND attendance.person_id=NEW.person_id AND ${returns?`(attendance.span_status='open' AND julianday(attendance.signed_in)<=julianday(NEW.at) AND julianday(${attendanceCutoffSQL('attendance.signed_in')})>julianday(NEW.at))`:"attendance.day=date(NEW.at,'+9 hours')"} AND attendance.signed_out='' AND attendance.person_enabled=1 AND attendance.actual_enabled=1 AND attendance.qualified=1 AND julianday(attendance.signed_in)<=julianday(NEW.at) AND (SELECT COUNT(*) FROM ${view} duplicate WHERE duplicate.person_id=NEW.person_id AND ${returns?'1':'duplicate.day=attendance.day'} AND duplicate.signed_out='')=1;
 UPDATE ck_dispatch_time_days SET requires_action=0,day=date(NEW.at,'+9 hours'),attendance_span_id=COALESCE((SELECT attendance_span_id FROM ${view} attendance WHERE attendance.id=ck_dispatch_time_days.attendance_id),'') WHERE ${returns?returnOn:`NOT (${returnOn})`} AND person_id=NEW.person_id AND EXISTS(SELECT 1 FROM ${view} attendance WHERE attendance.id=ck_dispatch_time_days.attendance_id AND attendance.signed_out='' AND ${returns?`(attendance.span_status='open' AND julianday(attendance.signed_in)<=julianday(NEW.at) AND julianday(${attendanceCutoffSQL('attendance.signed_in')})>julianday(NEW.at))`:"attendance.day=date(NEW.at,'+9 hours')"});
 INSERT INTO ck_dispatch_time_links(person_id,attendance_id,job_id,source,engaged_at,share_pair_id) SELECT state.person_id,state.attendance_id,NEW.job_id,NEW.source,NEW.at,CASE WHEN NEW.source='shared' THEN COALESCE((SELECT pair.id FROM ck_dispatch_share_pairs pair WHERE pair.status='active' AND pair.day=state.day AND ((pair.person_lo=NEW.person_id AND pair.person_hi=(SELECT json_extract(state,'$.owner_id') FROM sop_records WHERE id=NEW.job_id)) OR (pair.person_hi=NEW.person_id AND pair.person_lo=(SELECT json_extract(state,'$.owner_id') FROM sop_records WHERE id=NEW.job_id))) LIMIT 1),'') ELSE '' END FROM ck_dispatch_time_days state WHERE ${returns?returnOn:`NOT (${returnOn})`} AND state.person_id=NEW.person_id AND state.day=date(NEW.at,'+9 hours') AND EXISTS(SELECT 1 FROM ${view} attendance WHERE attendance.id=state.attendance_id AND attendance.signed_out='' AND ${returns?`(attendance.span_status='open' AND julianday(attendance.signed_in)<=julianday(NEW.at) AND julianday(${attendanceCutoffSQL('attendance.signed_in')})>julianday(NEW.at))`:"attendance.day=date(NEW.at,'+9 hours')"}) ON CONFLICT(person_id,attendance_id,job_id) DO UPDATE SET source=excluded.source,engaged_at=MAX(engaged_at,excluded.engaged_at),share_pair_id=excluded.share_pair_id;
`;}
function schema(){
 const enabled="(SELECT enabled FROM ck_dispatch_time_config WHERE id=1)=1",t="MAX(NEW.at,COALESCE((SELECT MAX(updated_at) FROM ck_dispatch_time_days WHERE person_id=NEW.person_id),NEW.at))";
 const sql=[
 configDDL,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_days(person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,day TEXT NOT NULL,activated_at TEXT NOT NULL,updated_at TEXT NOT NULL,status TEXT NOT NULL,needs_review INTEGER NOT NULL DEFAULT 0,requires_action INTEGER NOT NULL DEFAULT 0,attendance_span_id TEXT NOT NULL DEFAULT '',PRIMARY KEY(person_id,attendance_id))`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_links(person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,job_id TEXT NOT NULL,source TEXT NOT NULL,engaged_at TEXT NOT NULL,share_pair_id TEXT NOT NULL DEFAULT '',PRIMARY KEY(person_id,attendance_id,job_id))`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_spans(id TEXT PRIMARY KEY,person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,day TEXT NOT NULL,started_at TEXT NOT NULL,ended_at TEXT NOT NULL DEFAULT '',start_reason TEXT NOT NULL,end_reason TEXT NOT NULL DEFAULT '',needs_review INTEGER NOT NULL DEFAULT 0,source_event TEXT NOT NULL,attendance_span_id TEXT NOT NULL DEFAULT '')`,
 `CREATE UNIQUE INDEX IF NOT EXISTS ck_dispatch_time_one_open_span ON ck_dispatch_time_spans(person_id) WHERE ended_at=''`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_span_attendance ON ck_dispatch_time_spans(attendance_id,started_at)`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_days_date ON ck_dispatch_time_days(day,attendance_id)`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_days_attendance ON ck_dispatch_time_days(attendance_id)`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_links_job ON ck_dispatch_time_links(job_id,person_id)`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_spans_date ON ck_dispatch_time_spans(day,started_at,id)`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_open_attendance ON ck_attendance_days(day,person_id) WHERE signed_out=''`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_open_workers ON v2_ops_job_workers(worker_id,job_id,joined_at) WHERE left_at=''`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_badge_person ON ck_attendance_people(badge_id COLLATE NOCASE,id)`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_pending_loans ON ck_crew_borrows(worker_id) WHERE status IN ('borrowed','return_pending')`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_active_pairs ON ck_dispatch_share_pairs(day,person_lo,person_hi) WHERE status='active'`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_job_events ON sop_events(record_id,created_at)`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_job_results ON v2_ops_job_results(job_id,created_at)`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_actions(event_key TEXT PRIMARY KEY,person_id TEXT NOT NULL,actor_id TEXT NOT NULL,job_id TEXT NOT NULL,at TEXT NOT NULL,source TEXT NOT NULL,action TEXT NOT NULL)`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_ticks(id INTEGER PRIMARY KEY,person_id TEXT NOT NULL,at TEXT NOT NULL,reason TEXT NOT NULL,event_key TEXT NOT NULL)`,
 `CREATE VIEW IF NOT EXISTS ck_dispatch_time_identity AS ${identity}`,
 `CREATE VIEW IF NOT EXISTS ck_dispatch_time_return_identity AS ${childIdentity}`,
 // UNION ALL permits predicate pushdown to the indexed person/attendance/link keys.
 `CREATE VIEW IF NOT EXISTS ck_dispatch_time_aliases AS SELECT d.person_id,d.worker_id FROM ck_attendance_days d WHERE NOT EXISTS(SELECT 1 FROM ck_daily_dispatch_links link WHERE link.daily_person_id=d.person_id AND link.day=d.day) UNION ALL SELECT link.permanent_person_id,d.worker_id FROM ck_daily_dispatch_links link JOIN ck_attendance_days d ON d.person_id=link.daily_person_id AND d.day=link.day UNION ALL SELECT id,badge_id FROM ck_attendance_people WHERE kind='permanent'`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_tick AFTER INSERT ON ck_dispatch_time_ticks BEGIN
 UPDATE ck_dispatch_time_days SET needs_review=1 WHERE person_id=NEW.person_id AND EXISTS(SELECT 1 FROM ck_dispatch_time_spans span WHERE span.person_id=NEW.person_id AND span.attendance_id=ck_dispatch_time_days.attendance_id AND span.ended_at='' AND julianday(${boundary('span')})<=julianday(${t}));
 UPDATE ck_dispatch_time_spans SET ended_at=${boundary('ck_dispatch_time_spans')},end_reason='system_cutoff:unverified',needs_review=1 WHERE person_id=NEW.person_id AND ended_at='' AND julianday(${boundary('ck_dispatch_time_spans')})<=julianday(${t});
 UPDATE ck_dispatch_time_days SET status=CASE WHEN (SELECT enabled FROM ck_dispatch_time_config WHERE id=1)=0 THEN 'blocked' ELSE ${status('ck_dispatch_time_days',t)} END WHERE person_id=NEW.person_id;
 UPDATE ck_dispatch_time_spans SET ended_at=MAX(started_at,${t}),end_reason=NEW.reason WHERE person_id=NEW.person_id AND ended_at='' AND NOT EXISTS(SELECT 1 FROM ck_dispatch_time_days state WHERE state.person_id=NEW.person_id AND state.attendance_id=ck_dispatch_time_spans.attendance_id AND state.status='dispatching' AND state.attendance_span_id=ck_dispatch_time_spans.attendance_span_id);
 INSERT INTO ck_dispatch_time_spans SELECT 'DTS-'||lower(hex(randomblob(16))),state.person_id,state.attendance_id,state.day,${t},'',NEW.reason,'',0,NEW.event_key,state.attendance_span_id FROM ck_dispatch_time_days state WHERE state.person_id=NEW.person_id AND state.status='dispatching' AND NOT EXISTS(SELECT 1 FROM ck_dispatch_time_spans span WHERE span.person_id=NEW.person_id AND span.ended_at='');
 UPDATE ck_dispatch_time_days SET updated_at=${t} WHERE person_id=NEW.person_id;
 DELETE FROM ck_dispatch_time_ticks WHERE id=NEW.id;
 END`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_engage AFTER INSERT ON ck_dispatch_time_actions WHEN ${enabled} BEGIN
 ${engage('ck_dispatch_time_identity',false)}
 ${engage('ck_dispatch_time_return_identity',true)}
 ${tick('NEW.person_id','NEW.at','business_action','NEW.event_key')}
 END`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_sop_action AFTER INSERT ON sop_events WHEN ${enabled} AND json_extract(NEW.after_json,'$.job_type')='bulk_op' AND NEW.action IN (${businessActions.map(x=>"'"+x+"'").join(',')}) BEGIN
 ${capture('NEW.request_id','NEW.actor_id','NEW.record_id','NEW.created_at','NEW.after_json','NEW.action',"NEW.action<>'sop_owner_transfer'")}
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
 const workerTick=`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,CASE WHEN COALESCE(NEW.left_at,'')='' THEN NEW.joined_at ELSE NEW.left_at END,'operation_change',NEW.id FROM (${workerPeople('NEW.worker_id')});`;
 trigger('worker_insert','INSERT','v2_ops_job_workers',workerTick);trigger('worker_update','UPDATE OF left_at,worker_id,joined_at','v2_ops_job_workers',workerTick);
 const stablePerson=(person,day)=>`COALESCE((SELECT permanent_person_id FROM ck_daily_dispatch_links WHERE daily_person_id=${person} AND day=${day}),${person})`;
 for(const op of ['INSERT','UPDATE OF ended_at,status,started_at,person_id'])trigger('attendance_span_'+op.slice(0,6).toLowerCase(),op,'ck_attendance_spans',`UPDATE ck_dispatch_time_days SET requires_action=1 WHERE attendance_id=NEW.attendance_id AND (NEW.ended_at<>'' OR attendance_span_id<>NEW.id); INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT NEW.person_id,CASE WHEN NEW.ended_at<>'' THEN NEW.ended_at ELSE NEW.started_at END,'attendance_span',NEW.id;`);
 const attendancePeople=`SELECT person_id FROM ck_dispatch_time_days WHERE attendance_id=NEW.id UNION SELECT ${stablePerson('NEW.person_id','NEW.day')}`;
 trigger('attendance_insert','INSERT','ck_attendance_days',`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT person_id,NEW.signed_in,'attendance_insert',NEW.id FROM (${attendancePeople});`);
 trigger('attendance','UPDATE OF signed_out,signed_in,person_id,worker_id,day','ck_attendance_days',`UPDATE ck_dispatch_time_days SET requires_action=1 WHERE attendance_id=NEW.id AND NEW.signed_out<>''; INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT person_id,CASE WHEN NEW.person_id<>OLD.person_id OR NEW.worker_id<>OLD.worker_id OR NEW.day<>OLD.day THEN ${now} WHEN NEW.signed_out<>'' THEN NEW.signed_out ELSE COALESCE((SELECT MAX(at) FROM ck_attendance_events WHERE record_id=NEW.id),${now}) END,'attendance_change',NEW.id FROM (${attendancePeople});`);
 trigger('link_insert','INSERT','ck_daily_dispatch_links',tick('NEW.permanent_person_id','NEW.created_at','identity_link','NEW.daily_person_id'));
 trigger('link_update','UPDATE','ck_daily_dispatch_links',tick('OLD.permanent_person_id',now,'identity_link','NEW.daily_person_id')+tick('NEW.permanent_person_id',now,'identity_link','NEW.daily_person_id'));
 for(const op of ['INSERT','UPDATE'])trigger('rest_'+op.toLowerCase(),op,'ck_attendance_breaks',`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,CASE WHEN NEW.ended_at='' THEN NEW.started_at ELSE NEW.ended_at END,'rest_change',NEW.id FROM ck_dispatch_time_days WHERE attendance_id=NEW.attendance_id;`);
 trigger('grant','UPDATE','ck_daily_dispatch_grants',tick('NEW.person_id','NEW.updated_at','qualification_change',"'grant:'||NEW.person_id"));
 for(const op of ['INSERT','UPDATE'])trigger('access_'+op.toLowerCase(),op,'ck_field_access',tick('NEW.person_id','NEW.updated_at','qualification_change',"'access:'||NEW.person_id"));
 trigger('person','UPDATE OF enabled','ck_attendance_people',`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT person_id,${now},'person_change',NEW.id FROM (SELECT NEW.id AS person_id UNION SELECT permanent_person_id FROM ck_daily_dispatch_links WHERE daily_person_id=NEW.id);`);
 trigger('share','UPDATE OF status','ck_dispatch_share_pairs',tick('NEW.person_lo',`COALESCE(NULLIF(NEW.ended_at,''),NULLIF(NEW.accepted_at,''),NEW.created_at)`,'sharing_change',"'pair:'||NEW.id")+tick('NEW.person_hi',`COALESCE(NULLIF(NEW.ended_at,''),NULLIF(NEW.accepted_at,''),NEW.created_at)`,'sharing_change',"'pair:'||NEW.id"));
 trigger('loan','UPDATE OF status','ck_crew_borrows',`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,MAX(COALESCE(NEW.returned_at,''),COALESCE(NEW.released_at,''),NEW.borrowed_at),'loan_change',NEW.id FROM (${workerPeople('NEW.worker_id')});`);
 for(const op of ['INSERT','UPDATE'])trigger('attendance_management_'+op.toLowerCase(),op,'ck_attendance_management',`UPDATE ck_dispatch_time_days SET requires_action=1 WHERE attendance_id=NEW.attendance_id AND NEW.voided_at<>''; INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT person_id,CASE WHEN NEW.voided_at<>'' THEN NEW.voided_at ELSE ${now} END,'attendance_management',NEW.attendance_id FROM (SELECT person_id FROM ck_dispatch_time_days WHERE attendance_id=NEW.attendance_id UNION SELECT ${stablePerson('d.person_id','d.day')} FROM ck_attendance_days d WHERE d.id=NEW.attendance_id);`);
 for(const op of ['INSERT','UPDATE'])sql.push(`CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_void_${op.toLowerCase()} BEFORE ${op} ON ck_attendance_management WHEN NEW.voided_at<>'' AND EXISTS(SELECT 1 FROM ck_dispatch_time_days WHERE attendance_id=NEW.attendance_id) BEGIN SELECT RAISE(ABORT,'Attendance has dispatch labor; cannot void'); END`);
 trigger('config','UPDATE','ck_dispatch_time_config',`UPDATE ck_dispatch_time_days SET requires_action=1 WHERE NEW.enabled=0; INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,${now},'feature_config','config' FROM ck_dispatch_time_days;`,'OLD.enabled<>NEW.enabled OR OLD.sharing_enabled<>NEW.sharing_enabled');
 return sql;
}
export async function ensureAutoDispatchTime(e){
 if(e.SOP_ENVIRONMENT!=='staging')return e.DB;
 const db=bases.get(e.DB)||e.DB;
 const enabled=dispatchTimeEnabled(e),sharing=e.SOP_DISPATCH_SHARE_ENABLED==='true';
 const source={...e,DB:db};
 // Only DDL may be cached. Every request and every business transaction reapplies
 // its own flags; another isolate/binding wrapper cannot leave this request enabled.
 let exists=configExists.has(db);
 if(enabled){await ensureSchema(db,'auto-dispatch-config-v1',()=>db.prepare(configDDL).run());exists=true;configExists.add(db);}
 else if(!exists&&await db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='ck_dispatch_time_config'").first()){exists=true;configExists.add(db);}
 if(enabled){
  await ensureAttendance(source);await ensureAttendanceCurrentView(source);await ensureAccess(source);await ensureDailyDispatch(source);await ensureCrewBorrow(source);
  // Schema initialization does not turn on shared permissions; the actual flag remains authoritative.
  await ensureDispatchShare({...source,SOP_DISPATCH_SHARE_ENABLED:'true'});
  await ensureSchema(db,'auto-dispatch-runtime-v3',async()=>{
   const migrate=async()=>{
   const info=await db.batch([db.prepare('PRAGMA table_info(ck_dispatch_time_days)'),db.prepare('PRAGMA table_info(ck_dispatch_time_links)'),db.prepare('PRAGMA table_info(ck_dispatch_time_spans)')]),columns=info[0].results,linkColumns=info[1].results,spanColumns=info[2].results;
   const sql=schema(),triggers=sql.filter(s=>s.startsWith('CREATE TRIGGER')).map(s=>s.match(/EXISTS (\w+)/)[1]);
   await db.batch([
    ...(columns.length&&!columns.some(c=>c.name==='requires_action')?[db.prepare('ALTER TABLE ck_dispatch_time_days ADD COLUMN requires_action INTEGER NOT NULL DEFAULT 0')]:[]),
    ...(linkColumns.length&&!linkColumns.some(c=>c.name==='share_pair_id')?[db.prepare("ALTER TABLE ck_dispatch_time_links ADD COLUMN share_pair_id TEXT NOT NULL DEFAULT ''")]:[]),
    ...(columns.length&&!columns.some(c=>c.name==='attendance_span_id')?[db.prepare("ALTER TABLE ck_dispatch_time_days ADD COLUMN attendance_span_id TEXT NOT NULL DEFAULT ''")]:[]),
    ...(spanColumns.length&&!spanColumns.some(c=>c.name==='attendance_span_id')?[db.prepare("ALTER TABLE ck_dispatch_time_spans ADD COLUMN attendance_span_id TEXT NOT NULL DEFAULT ''")]:[]),
    ...triggers.map(name=>db.prepare('DROP TRIGGER IF EXISTS '+name)),
    db.prepare('DROP VIEW IF EXISTS ck_dispatch_time_return_identity'),db.prepare('DROP VIEW IF EXISTS ck_dispatch_time_aliases'),db.prepare('DROP VIEW IF EXISTS ck_dispatch_time_identity'),
    ...sql.map(s=>db.prepare(s)),db.prepare(`INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) SELECT DISTINCT person_id,${now},'ledger_upgrade','runtime-v2' FROM ck_dispatch_time_days`)
   ]);
   };
   try{await migrate();}catch(error){if(!/duplicate column name:\s*(requires_action|share_pair_id|attendance_span_id)/i.test(error.message||''))throw error;await migrate();}
  });
 }
 const sync=()=>db.prepare(configSQL).bind(enabled?1:0,sharing?1:0);
 if(exists){const current=await db.prepare('SELECT enabled,sharing_enabled FROM ck_dispatch_time_config WHERE id=1').first();if(!current||current.enabled!==(enabled?1:0)||current.sharing_enabled!==(sharing?1:0))await sync().run();}
 const signature=enabled+':'+sharing;let variants=wrappers.get(db);if(!variants)wrappers.set(db,variants=new Map());if(variants.has(signature))return variants.get(signature);
 const originals=new WeakMap(),statementsSQL=new WeakMap(),writes=sql=>!sql||!/^\s*(SELECT|EXPLAIN|PRAGMA)\b/i.test(sql);
 const batch=async statements=>{const source=statements.map(s=>originals.get(s)||s);if(!statements.some(s=>writes(statementsSQL.get(s))))return db.batch(source);await ensureSchema(db,'auto-dispatch-config-v1',()=>db.prepare(configDDL).run());configExists.add(db);return(await db.batch([sync(),...source])).slice(1);};
 const prepare=sql=>{const wrap=statement=>{statementsSQL.set(statement,sql);const proxy=new Proxy(statement,{get(target,key){if(key==='bind')return(...args)=>wrap(target.bind(...args));if(key==='run'&&writes(sql))return async()=>(await batch([target]))[0];if(typeof key==='string'&&/^(all|first)$/.test(key)&&writes(sql))return async column=>{const r=(await batch([target]))[0],row=r.results?.[0]||null;return key==='first'?(column&&row?row[column]:row):r;};const value=target[key];return typeof value==='function'?value.bind(target):value;}});originals.set(proxy,statement);statementsSQL.set(proxy,sql);return proxy;};return wrap(db.prepare(sql));};
 const wrapper=new Proxy(db,{get(target,key){if(key==='prepare')return prepare;if(key==='batch')return batch;const value=target[key];return typeof value==='function'?value.bind(target):value;}});bases.set(wrapper,db);shareSchemaBinding(wrapper,db);variants.set(signature,wrapper);return wrapper;
}
export async function dispatchTimeAttendance(e,date,asOf){
 if(!dispatchTimeEnabled(e))return new Map();
 const result=await e.DB.batch([attendanceReturnEnabled(e)?q(e,'SELECT * FROM ck_dispatch_time_days d WHERE day=? OR EXISTS(SELECT 1 FROM ck_dispatch_time_spans s WHERE s.attendance_id=d.attendance_id AND s.day=?)',date,date):q(e,'SELECT * FROM ck_dispatch_time_days WHERE day=?',date),q(e,'SELECT * FROM ck_dispatch_time_spans WHERE day=? ORDER BY started_at,id',date)]);
 const items=new Map(result[0].results.map(row=>[row.attendance_id,{...row,segments:[],seconds:0,pendingSeconds:0}]));
 for(const span of result[1].results){let item=items.get(span.attendance_id);if(!item&&attendanceReturnEnabled(e)){item={attendance_id:span.attendance_id,person_id:span.person_id,day:date,status:'out',segments:[],seconds:0,pendingSeconds:0};items.set(span.attendance_id,item);}if(!item)continue;const start=Date.parse(span.started_at);
  // Calculate the first 04:00 KST strictly after start without extending an unclosed historical span.
  const four=Math.floor((start+5*3600000)/86400000)*86400000-5*3600000+86400000,bound=Math.min(Date.parse(date+'T00:00:00+09:00')+86400000,four),until=Date.parse(asOf),pending=!!span.needs_review||!span.ended_at&&until>=bound,end=span.ended_at?Math.min(Date.parse(span.ended_at),until):Math.min(until,bound),seconds=Math.max(0,(end-start)/1000);
  if(!seconds)continue;item.segments.push({id:span.id,attendanceSpanId:span.attendance_span_id||'',start:span.started_at,end:span.ended_at||(!pending?'':new Date(end).toISOString()),endReason:pending?'system_cutoff:unverified':span.end_reason,startReason:span.start_reason,needsVerification:pending,seconds});item[pending?'pendingSeconds':'seconds']+=seconds;if(pending&&item.status==='dispatching')item.status='pending_review';
 }
 return items;
}
