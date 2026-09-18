-- Additive only. Apply to an isolated staging D1 before enabling SOP_UPGRADE_ENABLED.
CREATE TABLE IF NOT EXISTS sop_records (
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
 department TEXT NOT NULL, state TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sop_records_kind ON sop_records(kind, department, updated_at);
CREATE TABLE IF NOT EXISTS sop_events (
 request_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, expected_revision INTEGER NOT NULL,
 action TEXT NOT NULL, actor_id TEXT NOT NULL, actor_name TEXT NOT NULL,
 before_json TEXT NOT NULL, after_json TEXT NOT NULL, result_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sop_events_record ON sop_events(record_id, created_at);
CREATE TRIGGER IF NOT EXISTS sop_revision_guard BEFORE INSERT ON sop_events
BEGIN
 SELECT CASE WHEN COALESCE((SELECT revision FROM sop_records WHERE id=NEW.record_id),0) != NEW.expected_revision
 THEN RAISE(ABORT,'revision_conflict') END;
END;
-- Only SOP-owned tasks are protected; existing historic duplicate segments are not changed.
CREATE TRIGGER IF NOT EXISTS sop_worker_busy_guard BEFORE INSERT ON v2_ops_job_workers
WHEN NEW.left_at='' OR NEW.left_at IS NULL
BEGIN
 SELECT CASE WHEN EXISTS (
  SELECT 1 FROM v2_ops_job_workers w
  WHERE w.worker_id=NEW.worker_id AND w.left_at=''
  AND (EXISTS(SELECT 1 FROM sop_records WHERE id=NEW.job_id AND kind='task')
       OR EXISTS(SELECT 1 FROM sop_records WHERE id=w.job_id AND kind='task'))
 ) THEN RAISE(ABORT,'worker_busy') END;
END;
