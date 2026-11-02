-- Dispatch collaboration pairs. Apply only to the staging D1 when enabling
-- SOP_DISPATCH_SHARE_ENABLED. Do not run this against the abandoned production
-- database. The Worker also creates these tables on first use while the flag is on.
CREATE TABLE IF NOT EXISTS ck_dispatch_share_pairs(
  id TEXT PRIMARY KEY,
  person_lo TEXT NOT NULL,
  person_hi TEXT NOT NULL,
  invited_by TEXT NOT NULL,
  invitee_id TEXT NOT NULL,
  status TEXT NOT NULL,
  day TEXT NOT NULL,
  created_at TEXT NOT NULL,
  accepted_at TEXT NOT NULL DEFAULT '',
  ended_at TEXT NOT NULL DEFAULT '',
  ended_by TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS ck_dispatch_share_open_pair ON ck_dispatch_share_pairs(person_lo,person_hi) WHERE status IN ('pending','active');
CREATE TABLE IF NOT EXISTS ck_dispatch_share_events(
  request_id TEXT PRIMARY KEY,
  pair_id TEXT NOT NULL,
  action TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  actor_name TEXT NOT NULL,
  signature TEXT NOT NULL,
  response_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ck_dispatch_share_checks(
  id TEXT PRIMARY KEY,
  allowed INTEGER NOT NULL CHECK(allowed=1)
);
