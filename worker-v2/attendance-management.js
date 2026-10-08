// Staging attendance labels and reversible voids. No work or break record is deleted.
import {ensureSchema} from './schema-ready.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
export const activeAttendanceSQL=alias=>`NOT EXISTS(SELECT 1 FROM ck_attendance_management m WHERE m.attendance_id=${alias}.id AND m.voided_at!='')`;
export const attendanceDaySelect=`SELECT d.*,COALESCE(m.department,'') AS management_department,COALESCE(m.voided_at,'') AS voided_at,COALESCE(m.void_reason,'') AS void_reason FROM ck_attendance_days d LEFT JOIN ck_attendance_management m ON m.attendance_id=d.id`;
// julianday handles canonical ISO strings and historical offsets consistently.
const relatedWork=(w,d)=>`${w}.worker_id=${d}.worker_id AND julianday(${w}.joined_at)<julianday(${d}.day,'+1 day','-9 hours') AND (${w}.left_at='' OR julianday(${w}.left_at)>=julianday(${d}.day,'-9 hours'))`;
export const ATTENDANCE_MANAGEMENT_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS ck_attendance_management(attendance_id TEXT PRIMARY KEY,department TEXT NOT NULL DEFAULT '' CHECK(department IN ('','bulk','direct_ship','import')),voided_at TEXT NOT NULL DEFAULT '',void_reason TEXT NOT NULL DEFAULT '',voided_by TEXT NOT NULL DEFAULT '')`,
 ...['INSERT','UPDATE'].map(operation=>`CREATE TRIGGER IF NOT EXISTS ck_attendance_void_safe_${operation.toLowerCase()} BEFORE ${operation} ON ck_attendance_management WHEN NEW.voided_at!='' BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d WHERE d.id=NEW.attendance_id AND d.worker_id NOT LIKE 'EMP-%') OR EXISTS(SELECT 1 FROM ck_attendance_days d JOIN v2_ops_job_workers w ON ${relatedWork('w','d')} WHERE d.id=NEW.attendance_id) OR EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=NEW.attendance_id) THEN RAISE(ABORT,'Attendance has work or rest; cannot void') END; END`),
 ...['INSERT','UPDATE'].map(operation=>`CREATE TRIGGER IF NOT EXISTS ck_attendance_void_work_${operation.toLowerCase()} BEFORE ${operation} ON v2_ops_job_workers WHEN EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_management m ON m.attendance_id=d.id WHERE m.voided_at!='' AND ${relatedWork('NEW','d')}) BEGIN SELECT RAISE(ABORT,'Attendance voided; restore before work'); END`),
 ...['INSERT','UPDATE'].map(operation=>`CREATE TRIGGER IF NOT EXISTS ck_attendance_void_break_${operation.toLowerCase()} BEFORE ${operation} ON ck_attendance_breaks WHEN EXISTS(SELECT 1 FROM ck_attendance_management m WHERE m.attendance_id=NEW.attendance_id AND m.voided_at!='') BEGIN SELECT RAISE(ABORT,'Attendance voided; restore before rest'); END`),
 `CREATE TRIGGER IF NOT EXISTS ck_attendance_void_day_update BEFORE UPDATE ON ck_attendance_days WHEN EXISTS(SELECT 1 FROM ck_attendance_management m WHERE m.attendance_id=OLD.id AND m.voided_at!='') AND (NEW.signed_in!=OLD.signed_in OR NEW.signed_out!=OLD.signed_out OR NEW.worker_id!=OLD.worker_id OR NEW.day!=OLD.day OR NEW.agency!=OLD.agency) BEGIN SELECT RAISE(ABORT,'Attendance voided; restore before correction'); END`
];
export async function ensureAttendanceManagement(env){await ensureSchema(env.DB,'attendance-management-v1',()=>env.DB.batch(ATTENDANCE_MANAGEMENT_SCHEMA.map(s=>q(env,s))));}
export async function ensureAttendanceLoanGuard(env){
 const table=await q(env,"SELECT name FROM sqlite_master WHERE type='table' AND name='ck_crew_borrows'").first();if(!table)return false;
 await ensureSchema(env.DB,'attendance-void-loan-v1',()=>env.DB.batch([
  ...['INSERT','UPDATE'].map(operation=>q(env,`CREATE TRIGGER IF NOT EXISTS ck_attendance_void_loan_${operation.toLowerCase()} BEFORE ${operation} ON ck_attendance_management WHEN NEW.voided_at!='' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM ck_crew_borrows b JOIN ck_attendance_days d ON d.worker_id=b.worker_id WHERE d.id=NEW.attendance_id AND b.status IN ('borrowed','return_pending')) THEN RAISE(ABORT,'Attendance has pending loan; cannot void') END; END`)),
  ...['INSERT','UPDATE'].map(operation=>q(env,`CREATE TRIGGER IF NOT EXISTS ck_attendance_void_borrow_${operation.toLowerCase()} BEFORE ${operation} ON ck_crew_borrows WHEN NEW.status IN ('borrowed','return_pending') AND EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_management m ON m.attendance_id=d.id WHERE d.worker_id=NEW.worker_id AND d.day=NEW.source_day AND m.voided_at!='') BEGIN SELECT RAISE(ABORT,'Attendance voided; cannot borrow'); END`))
 ]));return true;
}
export async function attendanceVoidBlockers(env,date){
 const work=(await q(env,`SELECT DISTINCT d.id FROM ck_attendance_days d JOIN v2_ops_job_workers w ON ${relatedWork('w','d')} WHERE d.day=?`,date).all()).results;
 const rest=(await q(env,'SELECT DISTINCT d.id FROM ck_attendance_days d JOIN ck_attendance_breaks b ON b.attendance_id=d.id WHERE d.day=?',date).all()).results;
 const hasLoans=await q(env,"SELECT name FROM sqlite_master WHERE type='table' AND name='ck_crew_borrows'").first();
 const loans=hasLoans?(await q(env,"SELECT DISTINCT d.id FROM ck_attendance_days d JOIN ck_crew_borrows b ON b.worker_id=d.worker_id WHERE d.day=? AND b.status IN ('borrowed','return_pending')",date).all()).results:[];
 const result=new Map();for(const row of work)result.set(row.id,'已有作业计时，不能删除；请核实原作业 / 작업 기록이 있어 삭제할 수 없습니다');for(const row of rest)result.set(row.id,'已有休息记录，不能删除；请先核实 / 휴식 기록이 있어 삭제할 수 없습니다');for(const row of loans)result.set(row.id,'仍有借调或待归还记录，不能删除 / 차출·복귀 대기 기록이 있습니다');return result;
}
export function attendanceManagementStatement(env,after){return q(env,`INSERT INTO ck_attendance_management(attendance_id,department,voided_at,void_reason,voided_by) VALUES(?,?,?,?,?) ON CONFLICT(attendance_id) DO UPDATE SET department=excluded.department,voided_at=excluded.voided_at,void_reason=excluded.void_reason,voided_by=excluded.voided_by`,after.id,after.management_department||'',after.voided_at||'',after.void_reason||'',after.voided_by||'');}
