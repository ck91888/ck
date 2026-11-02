// Additive attendance children. Integration is opt-in and never relaxes parent uniqueness.
import {ensureSchema} from './schema-ready.js';
const eligible=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_ATTENDANCE_ENABLED==='true'&&e.SOP_ACCESS_CONTROL==='true';
export const attendanceReturnsRequested=e=>eligible(e)&&e.SOP_ATTENDANCE_RETURNS_ENABLED==='true';
export const attendanceReturnEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_ATTENDANCE_ENABLED==='true'&&e.SOP_ACCESS_CONTROL==='true'&&(e.SOP_ATTENDANCE_RETURNS_ENABLED==='true'||e.SOP_ATTENDANCE_RETURNS_COMPAT==='true');
export const ATTENDANCE_RETURN_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS ck_attendance_spans(id TEXT PRIMARY KEY,attendance_id TEXT NOT NULL,person_id TEXT NOT NULL,source_person_id TEXT NOT NULL,badge_id TEXT NOT NULL,shift_id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('normal','return','correction_resume','continuation')),started_at TEXT NOT NULL,ended_at TEXT NOT NULL DEFAULT '',status TEXT NOT NULL CHECK(status IN ('open','closed','pending_review')),provisional_end_at TEXT NOT NULL DEFAULT '',overtime_state TEXT NOT NULL DEFAULT 'none' CHECK(overtime_state IN ('none','pending','approved','denied')),version INTEGER NOT NULL DEFAULT 1,started_by TEXT NOT NULL,ended_by TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,ordinal INTEGER NOT NULL DEFAULT 0,resume_authorized INTEGER NOT NULL DEFAULT 0,resume_authorized_by TEXT NOT NULL DEFAULT '',resume_authorized_at TEXT NOT NULL DEFAULT '',identity_verified_by TEXT NOT NULL DEFAULT '',identity_verified_at TEXT NOT NULL DEFAULT '')`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_return_facts ON ck_attendance_spans(person_id) WHERE kind<>'normal'`,
 `CREATE UNIQUE INDEX IF NOT EXISTS ck_attendance_span_one_open ON ck_attendance_spans(person_id) WHERE status='open' AND ended_at=''`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_span_cut_day ON ck_attendance_spans(date(strftime('%Y-%m-%dT%H:%M:%fZ',julianday(started_at),'+5 hours','start of day','+1 day','-5 hours'),'+9 hours','-0.001 seconds'))`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_span_parent ON ck_attendance_spans(attendance_id,started_at)`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_span_open_badge ON ck_attendance_spans(badge_id COLLATE NOCASE,attendance_id) WHERE status='open' AND ended_at=''`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_span_person_time ON ck_attendance_spans(person_id,started_at,ended_at)`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_span_events(request_id TEXT PRIMARY KEY,attendance_id TEXT NOT NULL,span_id TEXT NOT NULL,expected_version INTEGER NOT NULL,action TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,at TEXT NOT NULL,before_json TEXT NOT NULL,after_json TEXT NOT NULL,reason TEXT NOT NULL,signature TEXT NOT NULL,response_json TEXT NOT NULL)`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_span_event_action ON ck_attendance_span_events(action,attendance_id)`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_span_event_parent ON ck_attendance_span_events(attendance_id,at,request_id)`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_return_identities(request_id TEXT PRIMARY KEY,attendance_id TEXT NOT NULL,person_id TEXT NOT NULL,badge_id TEXT NOT NULL,day TEXT NOT NULL,cutoff_at TEXT NOT NULL,parent_version INTEGER NOT NULL,verified_by TEXT NOT NULL,verified_name TEXT NOT NULL,verified_at TEXT NOT NULL,reason TEXT NOT NULL,used_by TEXT NOT NULL DEFAULT '')`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_span_checks(id TEXT PRIMARY KEY,allowed INTEGER NOT NULL CHECK(allowed=1))`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_span_workers(segment_id TEXT PRIMARY KEY,span_id TEXT NOT NULL)`,
 `CREATE INDEX IF NOT EXISTS ck_attendance_span_worker_owner ON ck_attendance_span_workers(span_id,segment_id)`,
 `CREATE TABLE IF NOT EXISTS ck_attendance_span_breaks(break_id TEXT PRIMARY KEY,span_id TEXT NOT NULL)`,
 `CREATE TRIGGER IF NOT EXISTS ck_attendance_span_version BEFORE UPDATE ON ck_attendance_spans WHEN NEW.version<>OLD.version+1 BEGIN SELECT RAISE(ABORT,'attendance_span_version_conflict'); END`,
 `CREATE TRIGGER IF NOT EXISTS ck_attendance_span_time_insert BEFORE INSERT ON ck_attendance_spans WHEN NEW.status<>'pending_review' AND (julianday(NEW.started_at) IS NULL OR (NEW.ended_at<>'' AND (julianday(NEW.ended_at) IS NULL OR julianday(NEW.ended_at)<julianday(NEW.started_at))) OR (NEW.status='closed' AND NEW.ended_at='') OR (NEW.status='open' AND NEW.ended_at<>'')) BEGIN SELECT RAISE(ABORT,'attendance_span_invalid_time'); END`,
 `CREATE TRIGGER IF NOT EXISTS ck_attendance_span_time_update BEFORE UPDATE ON ck_attendance_spans WHEN NEW.status<>'pending_review' AND (julianday(NEW.started_at) IS NULL OR (NEW.ended_at<>'' AND (julianday(NEW.ended_at) IS NULL OR julianday(NEW.ended_at)<julianday(NEW.started_at))) OR (NEW.status='closed' AND NEW.ended_at='') OR (NEW.status='open' AND NEW.ended_at<>'')) BEGIN SELECT RAISE(ABORT,'attendance_span_invalid_time'); END`
];
export async function ensureAttendanceReturnSchema(e){if(!attendanceReturnEnabled(e))return false;await ensureSchema(e.DB,'attendance-return-spans-v5',async()=>{
 const migrate=async()=>{const old=(await e.DB.prepare('PRAGMA table_info(ck_attendance_spans)').all()).results,extras={ordinal:'INTEGER NOT NULL DEFAULT 0',resume_authorized:'INTEGER NOT NULL DEFAULT 0',resume_authorized_by:"TEXT NOT NULL DEFAULT ''",resume_authorized_at:"TEXT NOT NULL DEFAULT ''",identity_verified_by:"TEXT NOT NULL DEFAULT ''",identity_verified_at:"TEXT NOT NULL DEFAULT ''"};await e.DB.batch([
 e.DB.prepare('DROP TRIGGER IF EXISTS ck_attendance_span_version'),
 ...Object.entries(extras).filter(([name])=>old.length&&!old.some(c=>c.name===name)).map(([name,type])=>e.DB.prepare('ALTER TABLE ck_attendance_spans ADD COLUMN '+name+' '+type)),
 ...ATTENDANCE_RETURN_SCHEMA.map(sql=>e.DB.prepare(sql)),
 e.DB.prepare(`UPDATE ck_attendance_spans SET ordinal=(SELECT COUNT(*) FROM ck_attendance_spans prior WHERE prior.attendance_id=ck_attendance_spans.attendance_id AND (prior.started_at<ck_attendance_spans.started_at OR (prior.started_at=ck_attendance_spans.started_at AND prior.id<=ck_attendance_spans.id))),version=version+1 WHERE ordinal=0`),
 e.DB.prepare('CREATE UNIQUE INDEX IF NOT EXISTS ck_attendance_span_order ON ck_attendance_spans(attendance_id,ordinal)')
 ]);};try{await migrate();}catch(error){if(!/duplicate column name:\s*(ordinal|resume_authorized|resume_authorized_by|resume_authorized_at|identity_verified_by|identity_verified_at)/.test(error.message||''))throw error;await migrate();}
 });return true;}
