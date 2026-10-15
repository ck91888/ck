import {ensureSchema} from './schema-ready.js';
import {canonicalDailyBadge} from '../shared/badge-identity.js';
import {canonicalWorkerSQL} from './daily-badge-identity.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
// Repair only this requested crew's clocks on provably completed jobs. No
// ownership, assignment, result, document or active job is changed. The original
// clock values remain in a separate audit, and no end time is invented from now.
export async function retireCompletedWorkClocks(e,workers){
 if(e.SOP_ENVIRONMENT!=='staging'||e.SOP_UPGRADE_ENABLED!=='true'||!['manager','dispatcher'].includes(e.SOP_REQUEST_USER?.role))return [];
 const ids=JSON.stringify([...new Set(workers.map(w=>canonicalDailyBadge(w.id)))]);
 const end="NULLIF(j.finished_at,'')";
 const stale=await q(e,`SELECT w.id FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.left_at='' AND j.status='completed' AND ${canonicalWorkerSQL('w.worker_id')} IN (SELECT value FROM json_each(?)) AND julianday(${end})>=julianday(w.joined_at) AND julianday(${end})<=julianday(?)`,ids,new Date().toISOString()).all();
 if(!stale.results.length)return [];
 await ensureSchema(e.DB,'completed-work-clock-audit-v1',()=>q(e,"CREATE TABLE IF NOT EXISTS ck_completed_clock_repairs(segment_id TEXT PRIMARY KEY,repair_token TEXT NOT NULL,job_id TEXT NOT NULL,worker_id TEXT NOT NULL,previous_left_at TEXT NOT NULL,previous_minutes REAL,closed_at TEXT NOT NULL,actor_id TEXT NOT NULL,repaired_at TEXT NOT NULL,reason TEXT NOT NULL)").run());
 const token=crypto.randomUUID(),at=new Date().toISOString(),actor=e.SOP_REQUEST_USER.id;
 await e.DB.batch([
  q(e,`INSERT OR IGNORE INTO ck_completed_clock_repairs SELECT w.id,?,w.job_id,w.worker_id,w.left_at,w.minutes_worked,${end},?,?,'completed_job_open_clock' FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.id IN (SELECT value FROM json_each(?)) AND w.left_at='' AND j.status='completed' AND julianday(${end})>=julianday(w.joined_at) AND julianday(${end})<=julianday(?)`,token,actor,at,JSON.stringify(stale.results.map(r=>r.id)),at),
  q(e,`UPDATE v2_ops_job_workers SET left_at=(SELECT closed_at FROM ck_completed_clock_repairs WHERE segment_id=v2_ops_job_workers.id AND repair_token=?),minutes_worked=MAX(0,ROUND((julianday((SELECT closed_at FROM ck_completed_clock_repairs WHERE segment_id=v2_ops_job_workers.id AND repair_token=?))-julianday(joined_at))*14400)/10.0),leave_reason='auto_cleanup_completed_job' WHERE left_at='' AND id IN (SELECT segment_id FROM ck_completed_clock_repairs WHERE repair_token=?)`,token,token,token)
 ]);
 return (await q(e,'SELECT segment_id,job_id,closed_at FROM ck_completed_clock_repairs WHERE repair_token=?',token).all()).results;
}
