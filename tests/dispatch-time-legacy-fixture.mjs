// Intended v1 normalized schema (before requires_action), synthetic historical labor only.
export function legacyDispatchLedger(db,person){
 db.exec(`CREATE TABLE ck_dispatch_time_days(person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,day TEXT NOT NULL,activated_at TEXT NOT NULL,updated_at TEXT NOT NULL,status TEXT NOT NULL,needs_review INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(person_id,attendance_id));
 CREATE TABLE ck_dispatch_time_links(person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,job_id TEXT NOT NULL,source TEXT NOT NULL,engaged_at TEXT NOT NULL,PRIMARY KEY(person_id,attendance_id,job_id));
 CREATE TABLE ck_dispatch_time_spans(id TEXT PRIMARY KEY,person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,day TEXT NOT NULL,started_at TEXT NOT NULL,ended_at TEXT NOT NULL DEFAULT '',start_reason TEXT NOT NULL,end_reason TEXT NOT NULL DEFAULT '',needs_review INTEGER NOT NULL DEFAULT 0,source_event TEXT NOT NULL);
 CREATE TRIGGER ck_dispatch_time_attendance AFTER UPDATE OF signed_in,signed_out ON ck_attendance_days BEGIN SELECT RAISE(ABORT,'synthetic_obsolete'); END;`);
 const end=new Date().toISOString(),start=new Date(Date.now()-600000).toISOString();db.prepare('INSERT INTO ck_dispatch_time_days VALUES(?,?,?,?,?,?,0)').run(person.id,person.record.id,person.record.day,start,end,'waiting_dispatch');db.prepare('INSERT INTO ck_dispatch_time_spans VALUES(?,?,?,?,?,?,?,?,?,?)').run('DTS-SYNTHETIC-V1',person.id,person.record.id,person.record.day,start,end,'business_action','assignment_end',0,'SYNTHETIC-V1-EVENT');
}
export function dispatchHistory(db,count=10000){
 const nums=`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<${count})`;
 // Construct pre-existing, already-closed history before testing the new feature.
 // Restore every guard in the same transaction; this helper is :memory: fixture only.
 const triggers=db.prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger'").all();db.exec('BEGIN');
 try{for(const t of triggers)db.exec('DROP TRIGGER "'+t.name.replaceAll('"','""')+'"');db.exec(`
 ${nums} INSERT INTO ck_attendance_people(id,badge_id,name,agency,kind,enabled,created_at) SELECT 'HIST-P-'||i,'DAF-HIST-'||i,'合成历史'||i,'가온','permanent',1,'2026-01-05T00:00:00.000Z' FROM n;
 ${nums} INSERT INTO ck_attendance_days(id,person_id,worker_id,name,agency,day,identity_key,signed_in,signed_out) SELECT 'HIST-A-'||i,'HIST-P-'||i,'DAF-HIST-'||i,'合成历史'||i,'가온','2026-01-05','HIST-IDENTITY-'||i,'2026-01-05T00:00:00.000Z','2026-01-05T09:00:00.000Z' FROM n;
 INSERT INTO v2_ops_jobs(id,job_type,biz_class,flow_stage,status,created_at,updated_at,created_by) VALUES('HIST-JOB','bulk_op','bulk','order_op','completed','2026-01-05T00:00:00.000Z','2026-01-05T09:00:00.000Z','SYNTHETIC');
 ${nums} INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) SELECT 'HIST-W-'||i,'HIST-JOB','DAF-HIST-'||i,'合成历史'||i,'2026-01-05T00:00:00.000Z','2026-01-05T09:00:00.000Z',540 FROM n;
 `);for(const t of triggers)db.exec(t.sql);db.exec('COMMIT');}catch(e){db.exec('ROLLBACK');throw e;}
}
