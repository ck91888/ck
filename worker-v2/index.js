/**
 * CK Warehouse V2 — Backend Workerer
 * Independent from V1. Uses v2_ table prefix in same D1 database.
 * Modules: issue_tickets, outbound_orders, inbound_plans, ops_jobs, attachments
 */

// ===== CORS =====
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...CORS }
  });
}

function err(msg, status = 400) {
  return json({ ok: false, error: msg }, status);
}

// ===== Helpers =====
function uid() {
  return Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 10);
}

function now() {
  return new Date().toISOString();
}

function kstToday() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.toISOString().slice(0, 10);
}

function isAuth(body, env) {
  const k = String(body.k || "").trim();
  const secret = String(env.ADMINKEY || "").trim();
  if (secret && k && k === secret) return true;
  const view = String(env.VIEWKEY || "").trim();
  if (view && k && k === view) return true;
  return false;
}

function isAdmin(body, env) {
  const k = String(body.k || "").trim();
  const secret = String(env.ADMINKEY || "").trim();
  return !!(secret && k && k === secret);
}

// OPS key — 现场执行系统专用，只允许用于 ops 相关接口
function isOpsKey(body, env) {
  const k = String(body.k || "").trim();
  const opsKey = String(env.OPSKEY || "").trim();
  return !!(opsKey && k && k === opsKey);
}

// isOpsAuth = ADMINKEY | VIEWKEY | OPSKEY（ops 接口用）
function isOpsAuth(body, env) {
  return isAuth(body, env) || isOpsKey(body, env);
}

// ===== Worker dedup helpers =====
// 查找某 worker 在某 job 中是否有未关闭的参与段
async function findOpenSeg(env, jobId, workerId) {
  return env.DB.prepare(
    "SELECT * FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at='' ORDER BY joined_at DESC LIMIT 1"
  ).bind(jobId, workerId).first();
}

// 关闭某 worker 在某 job 中的所有 open segments（自愈）
async function closeAllOpenSegs(env, jobId, workerId, t, reason) {
  const segs = await env.DB.prepare(
    "SELECT * FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at=''"
  ).bind(jobId, workerId).all();
  const rows = segs.results || [];
  for (const seg of rows) {
    const minutes = Math.round((new Date(t).getTime() - new Date(seg.joined_at).getTime()) / 60000 * 10) / 10;
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason=? WHERE id=?"
    ).bind(t, Math.max(0, minutes), reason, seg.id).run();
  }
  return rows.length;
}

// 从表中重算某 job 的 active_worker_count
async function recalcActiveCount(env, jobId, t) {
  const cnt = await env.DB.prepare(
    "SELECT COUNT(DISTINCT worker_id) as c FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
  ).bind(jobId).first();
  const real = cnt ? cnt.c : 0;
  await env.DB.prepare(
    "UPDATE v2_ops_jobs SET active_worker_count=?, updated_at=? WHERE id=?"
  ).bind(real, t, jobId).run();
  return real;
}

// ===== Display No helper =====
// 查当日最大序号 +1，唯一索引兜底重试
async function nextDisplayNo(env, planDate) {
  const dateStr = String(planDate || kstToday()).replace(/-/g, '');
  const prefix = 'RU-' + dateStr + '-';
  for (let attempt = 0; attempt < 3; attempt++) {
    const row = await env.DB.prepare(
      "SELECT display_no FROM v2_inbound_plans WHERE plan_date=? AND display_no LIKE ? ORDER BY display_no DESC LIMIT 1"
    ).bind(planDate, prefix + '%').first();
    let seq = 1;
    if (row && row.display_no) {
      const tail = row.display_no.split('-').pop();
      seq = (parseInt(tail, 10) || 0) + 1;
    }
    const no = prefix + String(seq).padStart(3, '0');
    // 验证唯一：如果后续 INSERT 因唯一索引失败会重试
    const dup = await env.DB.prepare(
      "SELECT 1 FROM v2_inbound_plans WHERE display_no=? LIMIT 1"
    ).bind(no).first();
    if (!dup) return no;
    // 有冲突，下一轮循环会重查最大值
  }
  // 极端情况：3 次都冲突，用时间戳兜底
  return 'RU-' + dateStr + '-' + Date.now().toString(36).slice(-4);
}

// ===== Auto-migration =====
const MIGRATIONS = [
  // v2_inbound_plans
  `CREATE TABLE IF NOT EXISTS v2_inbound_plans (
    id TEXT PRIMARY KEY,
    plan_date TEXT,
    customer TEXT DEFAULT '',
    biz_class TEXT DEFAULT '',
    cargo_summary TEXT DEFAULT '',
    expected_arrival TEXT DEFAULT '',
    purpose TEXT DEFAULT '',
    remark TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
  )`,

  // v2_outbound_orders
  `CREATE TABLE IF NOT EXISTS v2_outbound_orders (
    id TEXT PRIMARY KEY,
    order_date TEXT,
    customer TEXT DEFAULT '',
    biz_class TEXT DEFAULT '',
    operation_mode TEXT DEFAULT '',
    outbound_mode TEXT DEFAULT '',
    instruction TEXT DEFAULT '',
    remark TEXT DEFAULT '',
    status TEXT DEFAULT 'draft',
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
  )`,

  // v2_outbound_order_lines
  `CREATE TABLE IF NOT EXISTS v2_outbound_order_lines (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    line_no INTEGER DEFAULT 0,
    wms_order_no TEXT DEFAULT '',
    sku TEXT DEFAULT '',
    quantity INTEGER DEFAULT 0,
    remark TEXT DEFAULT ''
  )`,

  // v2_field_feedbacks
  `CREATE TABLE IF NOT EXISTS v2_field_feedbacks (
    id TEXT PRIMARY KEY,
    feedback_type TEXT DEFAULT '',
    related_doc_type TEXT DEFAULT '',
    related_doc_id TEXT DEFAULT '',
    title TEXT DEFAULT '',
    content TEXT DEFAULT '',
    submitted_by TEXT DEFAULT '',
    status TEXT DEFAULT 'open',
    created_at TEXT,
    updated_at TEXT
  )`,

  // v2_scan_batches
  `CREATE TABLE IF NOT EXISTS v2_scan_batches (
    id TEXT PRIMARY KEY,
    batch_type TEXT DEFAULT '',
    related_doc_type TEXT DEFAULT '',
    related_doc_id TEXT DEFAULT '',
    status TEXT DEFAULT 'open',
    total_expected INTEGER DEFAULT 0,
    total_scanned INTEGER DEFAULT 0,
    created_by TEXT DEFAULT '',
    created_at TEXT,
    closed_at TEXT
  )`,

  // v2_scan_batch_items
  `CREATE TABLE IF NOT EXISTS v2_scan_batch_items (
    id TEXT PRIMARY KEY,
    batch_id TEXT,
    barcode TEXT DEFAULT '',
    scanned_by TEXT DEFAULT '',
    scanned_at TEXT,
    remark TEXT DEFAULT ''
  )`,

  // v2_issue_tickets
  `CREATE TABLE IF NOT EXISTS v2_issue_tickets (
    id TEXT PRIMARY KEY,
    biz_class TEXT DEFAULT '',
    customer TEXT DEFAULT '',
    related_doc_no TEXT DEFAULT '',
    issue_type TEXT DEFAULT '',
    issue_summary TEXT DEFAULT '',
    issue_description TEXT DEFAULT '',
    priority TEXT DEFAULT 'normal',
    submitted_by TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    latest_feedback_text TEXT DEFAULT '',
    total_minutes_worked REAL DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
  )`,

  // v2_ops_jobs
  `CREATE TABLE IF NOT EXISTS v2_ops_jobs (
    id TEXT PRIMARY KEY,
    flow_stage TEXT DEFAULT '',
    biz_class TEXT DEFAULT '',
    job_type TEXT DEFAULT '',
    related_doc_type TEXT DEFAULT '',
    related_doc_id TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    shared_result_json TEXT DEFAULT '{}',
    parent_job_id TEXT DEFAULT '',
    is_temporary_interrupt INTEGER DEFAULT 0,
    interrupt_type TEXT DEFAULT '',
    paused_at TEXT DEFAULT '',
    resumed_at TEXT DEFAULT '',
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT,
    active_worker_count INTEGER DEFAULT 0
  )`,

  // v2_ops_job_results
  `CREATE TABLE IF NOT EXISTS v2_ops_job_results (
    id TEXT PRIMARY KEY,
    job_id TEXT,
    box_count INTEGER DEFAULT 0,
    pallet_count INTEGER DEFAULT 0,
    remark TEXT DEFAULT '',
    result_json TEXT DEFAULT '{}',
    created_by TEXT DEFAULT '',
    created_at TEXT
  )`,

  // v2_ops_job_workers
  `CREATE TABLE IF NOT EXISTS v2_ops_job_workers (
    id TEXT PRIMARY KEY,
    job_id TEXT,
    worker_id TEXT DEFAULT '',
    worker_name TEXT DEFAULT '',
    joined_at TEXT,
    left_at TEXT DEFAULT '',
    minutes_worked REAL DEFAULT 0,
    leave_reason TEXT DEFAULT ''
  )`,

  // v2_issue_handle_runs
  `CREATE TABLE IF NOT EXISTS v2_issue_handle_runs (
    id TEXT PRIMARY KEY,
    issue_id TEXT,
    job_id TEXT DEFAULT '',
    handler_id TEXT DEFAULT '',
    handler_name TEXT DEFAULT '',
    started_at TEXT,
    ended_at TEXT DEFAULT '',
    minutes_worked REAL DEFAULT 0,
    feedback_text TEXT DEFAULT '',
    run_status TEXT DEFAULT 'working',
    created_at TEXT
  )`,

  // v2_attachments
  `CREATE TABLE IF NOT EXISTS v2_attachments (
    id TEXT PRIMARY KEY,
    related_doc_type TEXT DEFAULT '',
    related_doc_id TEXT DEFAULT '',
    attachment_category TEXT DEFAULT '',
    file_name TEXT DEFAULT '',
    file_key TEXT DEFAULT '',
    file_size INTEGER DEFAULT 0,
    content_type TEXT DEFAULT '',
    uploaded_by TEXT DEFAULT '',
    created_at TEXT
  )`,

  // indexes
  `CREATE INDEX IF NOT EXISTS idx_v2_issue_status ON v2_issue_tickets(status)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_jobs_status ON v2_ops_jobs(status)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_jobs_related ON v2_ops_jobs(related_doc_type, related_doc_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_workers_job ON v2_ops_job_workers(job_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_workers_worker ON v2_ops_job_workers(worker_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_handle_runs_issue ON v2_issue_handle_runs(issue_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_attachments_doc ON v2_attachments(related_doc_type, related_doc_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_date ON v2_inbound_plans(plan_date)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_date ON v2_outbound_orders(order_date)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_scan_batch_items_batch ON v2_scan_batch_items(batch_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_lines_order ON v2_outbound_order_lines(order_id)`,

  // ---- Round 2 migrations ----
  // v2_inbound_plan_lines
  `CREATE TABLE IF NOT EXISTS v2_inbound_plan_lines (
    id TEXT PRIMARY KEY,
    plan_id TEXT,
    line_no INTEGER DEFAULT 0,
    unit_type TEXT DEFAULT '',
    planned_qty REAL DEFAULT 0,
    actual_qty REAL DEFAULT 0,
    remark TEXT DEFAULT ''
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ipl_plan ON v2_inbound_plan_lines(plan_id)`,

  // ALTER — each wrapped in try-catch by ensureMigrated
  `ALTER TABLE v2_inbound_plans ADD COLUMN source_feedback_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN source_inbound_plan_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_results ADD COLUMN diff_note TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_results ADD COLUMN result_lines_json TEXT DEFAULT '[]'`,

  // ---- display_no for inbound plans ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN display_no TEXT DEFAULT ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_inbound_display_no ON v2_inbound_plans(display_no) WHERE display_no != ''`,

  // ---- source_type for dynamic plans ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN source_type TEXT DEFAULT 'manual'`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN needs_info_update INTEGER DEFAULT 0`,
];

let _migrated = false;
async function ensureMigrated(db) {
  if (_migrated) return;
  for (const sql of MIGRATIONS) {
    try {
      await db.prepare(sql).run();
    } catch (e) {
      // ALTER TABLE may fail if column already exists — ignore
      if (!sql.trim().toUpperCase().startsWith("ALTER")) throw e;
    }
  }
  _migrated = true;
}

// ===== Route dispatcher =====
const HANDLERS = {};
function route(action, fn) { HANDLERS[action] = fn; }

// =====================================================
// Health check
// =====================================================
route("v2_health_check", async (body, env) => {
  return json({ ok: true, version: "2.0.0", time: now() });
});

// =====================================================
// ISSUE TICKETS — Collab side
// =====================================================
route("v2_issue_create", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = "ISS-" + uid();
  const t = now();
  await env.DB.prepare(`
    INSERT INTO v2_issue_tickets(id, biz_class, customer, related_doc_no, issue_type,
      issue_summary, issue_description, priority, submitted_by, status, created_at, updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,'pending',?,?)
  `).bind(
    id,
    String(body.biz_class || ""),
    String(body.customer || ""),
    String(body.related_doc_no || ""),
    String(body.issue_type || ""),
    String(body.issue_summary || ""),
    String(body.issue_description || ""),
    String(body.priority || "normal"),
    String(body.submitted_by || ""),
    t, t
  ).run();
  return json({ ok: true, id });
});

route("v2_issue_list", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const status = String(body.status || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  let sql = "SELECT * FROM v2_issue_tickets WHERE 1=1";
  const binds = [];
  if (status) { sql += " AND status=?"; binds.push(status); }
  if (biz_class) { sql += " AND biz_class=?"; binds.push(biz_class); }
  sql += " ORDER BY created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  return json({ ok: true, items: rs.results || [] });
});

route("v2_issue_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const row = await env.DB.prepare("SELECT * FROM v2_issue_tickets WHERE id=?").bind(id).first();
  if (!row) return err("not found", 404);
  // get handle runs
  const runs = await env.DB.prepare(
    "SELECT * FROM v2_issue_handle_runs WHERE issue_id=? ORDER BY started_at DESC"
  ).bind(id).all();
  // get attachments
  const atts = await env.DB.prepare(
    "SELECT * FROM v2_attachments WHERE related_doc_type='issue_ticket' AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(id).all();
  return json({
    ok: true,
    issue: row,
    handle_runs: runs.results || [],
    attachments: atts.results || []
  });
});

route("v2_issue_close", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  await env.DB.prepare(
    "UPDATE v2_issue_tickets SET status='closed', updated_at=? WHERE id=?"
  ).bind(now(), id).run();
  return json({ ok: true });
});

route("v2_issue_cancel", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  await env.DB.prepare(
    "UPDATE v2_issue_tickets SET status='cancelled', updated_at=? WHERE id=?"
  ).bind(now(), id).run();
  return json({ ok: true });
});

// =====================================================
// ISSUE TICKETS — Ops side (field execution)
// =====================================================
route("v2_issue_ops_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const status = String(body.status || "").trim();
  let sql = "SELECT * FROM v2_issue_tickets WHERE 1=1";
  const binds = [];
  if (status) { sql += " AND status=?"; binds.push(status); }
  sql += " ORDER BY CASE priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END, created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  return json({ ok: true, items: rs.results || [] });
});

route("v2_issue_handle_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const issue_id = String(body.issue_id || "").trim();
  const handler_id = String(body.handler_id || "").trim();
  const handler_name = String(body.handler_name || "").trim();
  if (!issue_id || !handler_id) return err("missing issue_id or handler_id");

  const issue = await env.DB.prepare("SELECT * FROM v2_issue_tickets WHERE id=?").bind(issue_id).first();
  if (!issue) return err("issue not found", 404);
  if (issue.status === "closed" || issue.status === "cancelled") return err("issue already " + issue.status);

  const t = now();
  const job_id = "JOB-" + uid();
  const run_id = "RUN-" + uid();

  // Create ops job
  await env.DB.prepare(`
    INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
      status, created_by, created_at, updated_at, active_worker_count)
    VALUES(?, 'issue_handle', ?, 'issue_handle', 'issue_ticket', ?, 'working', ?, ?, ?, 1)
  `).bind(job_id, issue.biz_class || "", issue_id, handler_id, t, t).run();

  // Create worker participation segment
  const worker_seg_id = "WS-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
    VALUES(?,?,?,?,?)
  `).bind(worker_seg_id, job_id, handler_id, handler_name, t).run();

  // Create handle run
  await env.DB.prepare(`
    INSERT INTO v2_issue_handle_runs(id, issue_id, job_id, handler_id, handler_name, started_at, run_status, created_at)
    VALUES(?,?,?,?,?,?,'working',?)
  `).bind(run_id, issue_id, job_id, handler_id, handler_name, t, t).run();

  // Update issue status
  await env.DB.prepare(
    "UPDATE v2_issue_tickets SET status='processing', updated_at=? WHERE id=?"
  ).bind(t, issue_id).run();

  return json({ ok: true, job_id, run_id, worker_seg_id });
});

route("v2_issue_handle_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const run_id = String(body.run_id || "").trim();
  const feedback_text = String(body.feedback_text || "").trim();
  if (!run_id) return err("missing run_id");

  const run = await env.DB.prepare("SELECT * FROM v2_issue_handle_runs WHERE id=?").bind(run_id).first();
  if (!run) return err("run not found", 404);
  if (run.run_status === "completed") return err("already completed");

  const t = now();
  const started = new Date(run.started_at).getTime();
  const ended = new Date(t).getTime();
  const minutes = Math.round((ended - started) / 60000 * 10) / 10;

  // Update run
  await env.DB.prepare(`
    UPDATE v2_issue_handle_runs SET ended_at=?, minutes_worked=?, feedback_text=?, run_status='completed' WHERE id=?
  `).bind(t, minutes, feedback_text, run_id).run();

  // Update job
  if (run.job_id) {
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', updated_at=? WHERE id=?"
    ).bind(t, run.job_id).run();

    // Close worker segment
    await env.DB.prepare(`
      UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=? WHERE job_id=? AND left_at=''
    `).bind(t, minutes, run.job_id).run();
  }

  // Update issue
  const allRuns = await env.DB.prepare(
    "SELECT minutes_worked FROM v2_issue_handle_runs WHERE issue_id=? AND run_status='completed'"
  ).bind(run.issue_id).all();
  const totalMin = (allRuns.results || []).reduce((s, r) => s + (r.minutes_worked || 0), 0);

  await env.DB.prepare(`
    UPDATE v2_issue_tickets SET status='responded', latest_feedback_text=?, total_minutes_worked=?, updated_at=? WHERE id=?
  `).bind(feedback_text, totalMin, t, run.issue_id).run();

  return json({ ok: true, minutes_worked: minutes, total_minutes: totalMin });
});

// =====================================================
// OUTBOUND ORDERS — Collab side
// =====================================================
route("v2_outbound_order_create", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = "OB-" + uid();
  const t = now();
  await env.DB.prepare(`
    INSERT INTO v2_outbound_orders(id, order_date, customer, biz_class, operation_mode,
      outbound_mode, instruction, remark, status, created_by, created_at, updated_at)
    VALUES(?,?,?,?,?,?,?,?,'draft',?,?,?)
  `).bind(
    id,
    String(body.order_date || kstToday()),
    String(body.customer || ""),
    String(body.biz_class || ""),
    String(body.operation_mode || ""),
    String(body.outbound_mode || ""),
    String(body.instruction || ""),
    String(body.remark || ""),
    String(body.created_by || ""),
    t, t
  ).run();

  // Insert lines
  const lines = body.lines || [];
  for (let i = 0; i < lines.length; i++) {
    const ln = lines[i];
    await env.DB.prepare(`
      INSERT INTO v2_outbound_order_lines(id, order_id, line_no, wms_order_no, sku, quantity, remark)
      VALUES(?,?,?,?,?,?,?)
    `).bind("OBL-" + uid(), id, i + 1, String(ln.wms_order_no || ""), String(ln.sku || ""), Number(ln.quantity || 0), String(ln.remark || "")).run();
  }

  return json({ ok: true, id });
});

route("v2_outbound_order_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const start = String(body.start_date || "").trim();
  const end = String(body.end_date || "").trim();
  const status = String(body.status || "").trim();
  let sql = "SELECT * FROM v2_outbound_orders WHERE 1=1";
  const binds = [];
  if (start) { sql += " AND order_date>=?"; binds.push(start); }
  if (end) { sql += " AND order_date<=?"; binds.push(end); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  sql += " ORDER BY order_date DESC, created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  return json({ ok: true, items: rs.results || [] });
});

route("v2_outbound_order_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const row = await env.DB.prepare("SELECT * FROM v2_outbound_orders WHERE id=?").bind(id).first();
  if (!row) return err("not found", 404);
  const lines = await env.DB.prepare(
    "SELECT * FROM v2_outbound_order_lines WHERE order_id=? ORDER BY line_no"
  ).bind(id).all();
  // Get related jobs
  const jobs = await env.DB.prepare(
    "SELECT * FROM v2_ops_jobs WHERE related_doc_type='outbound_order' AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(id).all();
  const atts = await env.DB.prepare(
    "SELECT * FROM v2_attachments WHERE related_doc_type='outbound_order' AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(id).all();
  return json({
    ok: true,
    order: row,
    lines: lines.results || [],
    jobs: jobs.results || [],
    attachments: atts.results || []
  });
});

route("v2_outbound_order_update_status", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const status = String(body.status || "").trim();
  if (!id || !status) return err("missing id or status");
  await env.DB.prepare(
    "UPDATE v2_outbound_orders SET status=?, updated_at=? WHERE id=?"
  ).bind(status, now(), id).run();
  return json({ ok: true });
});

// =====================================================
// OUTBOUND LOAD — Ops side
// =====================================================
route("v2_outbound_load_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const order_id = String(body.order_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!worker_id) return err("missing worker_id");

  const t = now();

  // Check if there's already an active job for this order
  let job = null;
  if (order_id) {
    const existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE related_doc_type='outbound_order' AND related_doc_id=? AND status IN ('pending','working') LIMIT 1"
    ).bind(order_id).first();
    if (existing) job = existing;
  }

  let job_id, is_new_job = false;
  if (job) {
    job_id = job.id;
    // 防重：同一 worker 已有 open segment 则直接返回
    const dup = await findOpenSeg(env, job_id, worker_id);
    if (dup) return json({ ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, updated_at=?, status='working' WHERE id=?"
    ).bind(t, job_id).run();
  } else {
    job_id = "JOB-" + uid();
    is_new_job = true;
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, created_by, created_at, updated_at, active_worker_count)
      VALUES(?, 'outbound', ?, 'load_outbound', 'outbound_order', ?, 'working', ?, ?, ?, 1)
    `).bind(job_id, String(body.biz_class || ""), order_id, worker_id, t, t).run();

    if (order_id) {
      await env.DB.prepare(
        "UPDATE v2_outbound_orders SET status='working', updated_at=? WHERE id=? AND status IN ('draft','issued')"
      ).bind(t, order_id).run();
    }
  }

  const seg_id = "WS-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
    VALUES(?,?,?,?,?)
  `).bind(seg_id, job_id, worker_id, worker_name, t).run();

  return json({ ok: true, job_id, worker_seg_id: seg_id, is_new_job });
});

route("v2_outbound_load_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  const t = now();
  const box_count = Number(body.box_count || 0);
  const pallet_count = Number(body.pallet_count || 0);
  const remark = String(body.remark || "");
  const complete_job = body.complete_job === true;

  // 自愈：关闭该 worker 全部 open segments + 重算 count
  await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');
  const realCount = await recalcActiveCount(env, job_id, t);

  // Save shared result
  if (box_count > 0 || pallet_count > 0 || remark) {
    const resultJson = JSON.stringify({ box_count, pallet_count, remark });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET shared_result_json=?, updated_at=? WHERE id=?"
    ).bind(resultJson, t, job_id).run();
  }

  // Save result record
  const result_id = "RES-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, created_by, created_at)
    VALUES(?,?,?,?,?,?,?)
  `).bind(result_id, job_id, box_count, pallet_count, remark, worker_id, t).run();

  // Complete job if requested — 基于 realCount 判断
  if (complete_job) {
    if (realCount <= 0) {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', updated_at=? WHERE id=?"
      ).bind(t, job_id).run();
      const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
      if (job && job.related_doc_id) {
        await env.DB.prepare(
          "UPDATE v2_outbound_orders SET status='completed', updated_at=? WHERE id=?"
        ).bind(t, job.related_doc_id).run();
      }
    } else {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='awaiting_close', updated_at=? WHERE id=?"
      ).bind(t, job_id).run();
    }
  }

  return json({ ok: true, result_id });
});

// =====================================================
// INBOUND PLANS — Collab side
// =====================================================
route("v2_inbound_plan_create", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = "IB-" + uid();
  const t = now();
  const plan_date = String(body.plan_date || kstToday());
  const customer = String(body.customer || "");
  const biz_class = String(body.biz_class || "");
  const created_by = String(body.created_by || "");
  const display_no = await nextDisplayNo(env, plan_date);

  await env.DB.prepare(`
    INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, cargo_summary,
      expected_arrival, purpose, remark, status, created_by, created_at, updated_at, display_no)
    VALUES(?,?,?,?,?,?,?,?,'pending',?,?,?,?)
  `).bind(
    id, plan_date,
    customer, biz_class,
    String(body.cargo_summary || ""),
    String(body.expected_arrival || ""),
    String(body.purpose || ""),
    String(body.remark || ""),
    created_by, t, t, display_no
  ).run();

  // Insert plan lines
  const lines = body.lines || [];
  for (let i = 0; i < lines.length; i++) {
    const ln = lines[i];
    await env.DB.prepare(`
      INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, remark)
      VALUES(?,?,?,?,?,?)
    `).bind("IPL-" + uid(), id, i + 1, String(ln.unit_type || ""), Number(ln.planned_qty || 0), String(ln.remark || "")).run();
  }

  // Auto-create outbound order if requested
  let outbound_id = null;
  if (body.auto_create_outbound) {
    outbound_id = "OB-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_outbound_orders(id, order_date, customer, biz_class, operation_mode,
        outbound_mode, instruction, remark, status, source_inbound_plan_id, created_by, created_at, updated_at)
      VALUES(?,?,?,?,?,?,?,?,'draft',?,?,?,?)
    `).bind(
      outbound_id,
      String(body.plan_date || kstToday()),
      customer, biz_class,
      String(body.ob_operation_mode || ""),
      String(body.ob_outbound_mode || ""),
      String(body.ob_instruction || ""),
      String(body.ob_remark || ""),
      id, created_by, t, t
    ).run();
  }

  return json({ ok: true, id, display_no, outbound_id });
});

route("v2_inbound_plan_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const start = String(body.start_date || "").trim();
  const end = String(body.end_date || "").trim();
  const status = String(body.status || "").trim();
  let sql = "SELECT * FROM v2_inbound_plans WHERE 1=1";
  const binds = [];
  if (start) { sql += " AND plan_date>=?"; binds.push(start); }
  if (end) { sql += " AND plan_date<=?"; binds.push(end); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  sql += " ORDER BY plan_date DESC, created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  return json({ ok: true, items: rs.results || [] });
});

route("v2_inbound_plan_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const row = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
  if (!row) return err("not found", 404);
  const planLines = await env.DB.prepare(
    "SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY line_no"
  ).bind(id).all();
  const jobs = await env.DB.prepare(
    "SELECT * FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(id).all();
  const atts = await env.DB.prepare(
    "SELECT * FROM v2_attachments WHERE related_doc_type='inbound_plan' AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(id).all();

  // Enrich each job with workers + results summary
  const enrichedJobs = [];
  for (const job of (jobs.results || [])) {
    const workers = await env.DB.prepare(
      "SELECT worker_name, minutes_worked, left_at FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at"
    ).bind(job.id).all();
    const workerRows = workers.results || [];
    const names = [...new Set(workerRows.map(w => w.worker_name).filter(Boolean))];
    const totalMin = workerRows.reduce((s, w) => s + (Number(w.minutes_worked) || 0), 0);
    const maxLeft = workerRows.reduce((m, w) => (w.left_at && w.left_at > m ? w.left_at : m), "");

    const latestResult = await env.DB.prepare(
      "SELECT result_lines_json, diff_note, created_at FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at DESC LIMIT 1"
    ).bind(job.id).first();

    let resultLines = [];
    if (latestResult && latestResult.result_lines_json) {
      try { resultLines = JSON.parse(latestResult.result_lines_json); } catch(e) {}
    }

    enrichedJobs.push({
      ...job,
      worker_names: names,
      worker_names_text: names.join(", "),
      total_minutes_worked: Math.round(totalMin),
      completed_at: maxLeft || job.updated_at || "",
      result_lines: resultLines,
      diff_note: (latestResult && latestResult.diff_note) || ""
    });
  }

  return json({
    ok: true,
    plan: row,
    lines: planLines.results || [],
    jobs: enrichedJobs,
    attachments: atts.results || []
  });
});

// Upcoming inbound plans (next 3 working days, skip Sundays)
route("v2_inbound_plan_list_upcoming", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const today = kstToday();
  // Compute next 3 working days (skip Sundays)
  const dates = [today];
  let d = new Date(today + "T00:00:00+09:00");
  while (dates.length < 4) {
    d.setDate(d.getDate() + 1);
    if (d.getDay() !== 0) { // 0=Sunday
      dates.push(d.toISOString().slice(0, 10));
    }
  }
  const first = dates[0];
  const last = dates[dates.length - 1];
  const rs = await env.DB.prepare(
    "SELECT * FROM v2_inbound_plans WHERE plan_date>=? AND plan_date<=? AND status NOT IN ('completed','cancelled') ORDER BY plan_date ASC, created_at ASC"
  ).bind(first, last).all();
  return json({ ok: true, items: rs.results || [], dates });
});

route("v2_inbound_plan_update_status", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const status = String(body.status || "").trim();
  if (!id || !status) return err("missing id or status");
  await env.DB.prepare(
    "UPDATE v2_inbound_plans SET status=?, updated_at=? WHERE id=?"
  ).bind(status, now(), id).run();
  return json({ ok: true });
});

// ===== Dynamic plan finalize: fill info and convert to formal inbound =====
route("v2_inbound_dynamic_finalize", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
  if (!plan) return err("not found", 404);
  if (plan.source_type !== "field_dynamic") return err("not a dynamic plan");
  if (plan.status !== "unloaded_pending_info") return err("status must be unloaded_pending_info, current: " + plan.status);

  const t = now();
  const customer = String(body.customer || plan.customer || "").trim();
  const biz_class = String(body.biz_class || plan.biz_class || "").trim();
  const cargo_summary = String(body.cargo_summary || plan.cargo_summary || "").trim();
  const expected_arrival = String(body.expected_arrival || plan.expected_arrival || "").trim();
  const purpose = String(body.purpose || plan.purpose || "").trim();
  const remark = String(body.remark || plan.remark || "").trim();

  await env.DB.prepare(`
    UPDATE v2_inbound_plans SET customer=?, biz_class=?, cargo_summary=?,
      expected_arrival=?, purpose=?, remark=?, status='completed',
      needs_info_update=0, updated_at=? WHERE id=?
  `).bind(customer, biz_class, cargo_summary, expected_arrival, purpose, remark, t, id).run();

  // Update lines if provided
  const newLines = body.lines || [];
  if (newLines.length > 0) {
    await env.DB.prepare("DELETE FROM v2_inbound_plan_lines WHERE plan_id=?").bind(id).run();
    for (let i = 0; i < newLines.length; i++) {
      const ln = newLines[i];
      await env.DB.prepare(
        "INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, actual_qty, remark) VALUES(?,?,?,?,?,?,?)"
      ).bind("IPL-" + uid(), id, i + 1, String(ln.unit_type || ""), Number(ln.planned_qty || 0), Number(ln.actual_qty || 0), String(ln.remark || "")).run();
    }
  }

  return json({ ok: true, id, display_no: plan.display_no });
});

// =====================================================
// UNLOAD / INBOUND JOBS — Ops side
// =====================================================
// ===== Dynamic no-doc unload: create plan + job in one shot =====
route("v2_unload_dynamic_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!worker_id) return err("missing worker_id");

  const t = now();
  const plan_date = kstToday();
  const plan_id = "IB-" + uid();
  const display_no = await nextDisplayNo(env, plan_date);

  // 1. Create dynamic inbound plan
  await env.DB.prepare(`
    INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, cargo_summary,
      expected_arrival, purpose, remark, status, created_by, created_at, updated_at, display_no, source_type, needs_info_update)
    VALUES(?,?,'待补充','','现场无单卸货','','','','field_working',?,?,?,?,'field_dynamic',1)
  `).bind(plan_id, plan_date, worker_name || worker_id, t, t, display_no).run();

  // 2. Create unload job bound to this plan
  const job_id = "JOB-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
      status, created_by, created_at, updated_at, active_worker_count)
    VALUES(?, 'unload', '', 'unload', 'inbound_plan', ?, 'working', ?, ?, ?, 1)
  `).bind(job_id, plan_id, worker_id, t, t).run();

  // 3. Create worker segment
  const seg_id = "WS-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
    VALUES(?,?,?,?,?)
  `).bind(seg_id, job_id, worker_id, worker_name, t).run();

  return json({ ok: true, plan_id, display_no, job_id, worker_seg_id: seg_id });
});

route("v2_unload_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const plan_id = String(body.plan_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  if (!worker_id) return err("missing worker_id");

  const t = now();

  // Check existing job for this plan
  let job = null;
  if (plan_id) {
    const existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status IN ('pending','working') LIMIT 1"
    ).bind(plan_id).first();
    if (existing) job = existing;
  }

  let job_id, is_new_job = false;
  if (job) {
    job_id = job.id;
    // 防重：同一 worker 已有 open segment 则直接返回
    const dup = await findOpenSeg(env, job_id, worker_id);
    if (dup) return json({ ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, updated_at=?, status='working' WHERE id=?"
    ).bind(t, job_id).run();
  } else {
    job_id = "JOB-" + uid();
    is_new_job = true;
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, created_by, created_at, updated_at, active_worker_count)
      VALUES(?, 'unload', ?, 'unload', 'inbound_plan', ?, 'working', ?, ?, ?, 1)
    `).bind(job_id, biz_class, plan_id, worker_id, t, t).run();
    if (plan_id) {
      await env.DB.prepare(
        "UPDATE v2_inbound_plans SET status='processing', updated_at=? WHERE id=? AND status IN ('pending','arrived')"
      ).bind(t, plan_id).run();
    }
  }

  const seg_id = "WS-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
    VALUES(?,?,?,?,?)
  `).bind(seg_id, job_id, worker_id, worker_name, t).run();

  return json({ ok: true, job_id, worker_seg_id: seg_id, is_new_job });
});

route("v2_unload_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  const t = now();
  const leave_only = body.leave_only === true;
  const complete_job = body.complete_job === true;
  const result_lines = body.result_lines || [];
  const diff_note = String(body.diff_note || "").trim();
  const remark = String(body.remark || "");

  // 1. 自愈：关闭该 worker 全部 open segments + 重算 count
  await closeAllOpenSegs(env, job_id, worker_id, t, leave_only ? 'leave' : 'finished');
  const realCount = await recalcActiveCount(env, job_id, t);

  // 3. If leave_only → done, no result validation
  if (leave_only) {
    return json({ ok: true, left: true });
  }

  // 4. complete_job logic
  if (complete_job) {
    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!job) return err("job not found", 404);

    // 4a. Check if others still working — 基于 realCount
    if (realCount > 0) {
      return json({ ok: false, error: "others_still_working", active_count: realCount });
    }

    // 4b. Validate result_lines: at least one actual_qty > 0
    const hasAnyQty = result_lines.some(ln => Number(ln.actual_qty || 0) > 0);
    if (!hasAnyQty) {
      return json({ ok: false, error: "empty_result", message: "至少填写一项实际数量" });
    }

    // 4c. Check diff vs plan and require diff_note
    const plan_id = job.related_doc_id || "";
    let hasDiff = false;
    if (plan_id) {
      const planLines = await env.DB.prepare(
        "SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY line_no"
      ).bind(plan_id).all();
      const plMap = {};
      for (const pl of (planLines.results || [])) {
        plMap[pl.unit_type] = pl.planned_qty || 0;
      }
      for (const rl of result_lines) {
        const planned = plMap[rl.unit_type] || 0;
        const actual = Number(rl.actual_qty || 0);
        if (actual !== planned) { hasDiff = true; break; }
      }
      // Also check if plan has types not in result
      for (const pl of (planLines.results || [])) {
        const found = result_lines.find(r => r.unit_type === pl.unit_type);
        if (!found && (pl.planned_qty || 0) > 0) { hasDiff = true; break; }
      }
    }

    if (hasDiff && !diff_note) {
      return json({ ok: false, error: "diff_note_required", message: "计划与实际有差异，请填写差异说明" });
    }

    // 4d. Write result record
    const result_id = "RES-" + uid();
    const box_count = Number(body.box_count || 0);
    const pallet_count = Number(body.pallet_count || 0);
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, result_lines_json, diff_note, created_by, created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)
    `).bind(result_id, job_id, box_count, pallet_count, remark,
        JSON.stringify({ box_count, pallet_count, remark }),
        JSON.stringify(result_lines), diff_note, worker_id, t).run();

    // 4e. Write back actual_qty to plan lines
    if (plan_id) {
      for (const rl of result_lines) {
        await env.DB.prepare(
          "UPDATE v2_inbound_plan_lines SET actual_qty=? WHERE plan_id=? AND unit_type=?"
        ).bind(Number(rl.actual_qty || 0), plan_id, String(rl.unit_type || "")).run();
      }
    }

    // 4f. Complete job
    const sharedResult = JSON.stringify({ box_count, pallet_count, remark, result_lines, diff_note });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', shared_result_json=?, updated_at=? WHERE id=?"
    ).bind(sharedResult, t, job_id).run();

    // Close any remaining open worker segments
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
    ).bind(t, job_id).run();

    // Update inbound plan status
    if (plan_id) {
      const planRow = await env.DB.prepare("SELECT source_type FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
      if (planRow && planRow.source_type === "field_dynamic") {
        // Dynamic plan: set to unloaded_pending_info, auto-create lines from result
        const existingLines = await env.DB.prepare(
          "SELECT COUNT(*) as c FROM v2_inbound_plan_lines WHERE plan_id=?"
        ).bind(plan_id).first();
        if (!existingLines || existingLines.c === 0) {
          for (let i = 0; i < result_lines.length; i++) {
            const rl = result_lines[i];
            await env.DB.prepare(
              "INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, actual_qty) VALUES(?,?,?,?,?,?)"
            ).bind("IPL-" + uid(), plan_id, i + 1, String(rl.unit_type || ""), Number(rl.actual_qty || 0), Number(rl.actual_qty || 0)).run();
          }
        } else {
          for (const rl of result_lines) {
            await env.DB.prepare(
              "UPDATE v2_inbound_plan_lines SET actual_qty=? WHERE plan_id=? AND unit_type=?"
            ).bind(Number(rl.actual_qty || 0), plan_id, String(rl.unit_type || "")).run();
          }
        }
        // Build cargo summary from result
        const cargoSummary = result_lines.map(rl => (rl.unit_type || "") + " " + (rl.actual_qty || 0)).join(" / ");
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='unloaded_pending_info', cargo_summary=?, updated_at=? WHERE id=?"
        ).bind(cargoSummary || "现场无单卸货", t, plan_id).run();
        return json({ ok: true, result_id, dynamic_plan: true, plan_id });
      } else {
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='completed', updated_at=? WHERE id=?"
        ).bind(t, plan_id).run();
      }
    }

    // 4g. No-doc unload (legacy: no plan_id at all) → auto-create feedback
    if (!plan_id || plan_id === "") {
      const fb_id = "FB-" + uid();
      await env.DB.prepare(`
        INSERT INTO v2_field_feedbacks(id, feedback_type, related_doc_type, related_doc_id,
          title, content, submitted_by, status, created_at, updated_at)
        VALUES(?,?,?,?,?,?,?,'open',?,?)
      `).bind(fb_id, "unload_no_doc", "ops_job", job_id,
          "无单卸货结果待转正",
          "卸货数量: " + JSON.stringify(result_lines) + (diff_note ? " | 备注: " + diff_note : ""),
          worker_id, t, t).run();
      return json({ ok: true, result_id, feedback_id: fb_id, no_doc: true });
    }

    return json({ ok: true, result_id });
  }

  // Neither leave_only nor complete_job — just left
  return json({ ok: true });
});

route("v2_inbound_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const plan_id = String(body.plan_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  const job_type = String(body.job_type || "inbound_direct").trim();
  if (!worker_id) return err("missing worker_id");

  const t = now();

  let job = null;
  if (plan_id) {
    const existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type=? AND status IN ('pending','working') LIMIT 1"
    ).bind(plan_id, job_type).first();
    if (existing) job = existing;
  }

  let job_id, is_new_job = false;
  if (job) {
    job_id = job.id;
    // 防重：同一 worker 已有 open segment 则直接返回
    const dup = await findOpenSeg(env, job_id, worker_id);
    if (dup) return json({ ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, updated_at=?, status='working' WHERE id=?"
    ).bind(t, job_id).run();
  } else {
    job_id = "JOB-" + uid();
    is_new_job = true;
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, created_by, created_at, updated_at, active_worker_count)
      VALUES(?, 'inbound', ?, ?, 'inbound_plan', ?, 'working', ?, ?, ?, 1)
    `).bind(job_id, biz_class, job_type, plan_id, worker_id, t, t).run();
  }

  const seg_id = "WS-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
    VALUES(?,?,?,?,?)
  `).bind(seg_id, job_id, worker_id, worker_name, t).run();

  return json({ ok: true, job_id, worker_seg_id: seg_id, is_new_job });
});

route("v2_inbound_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const complete_job = body.complete_job === true;
  if (!job_id) return err("missing job_id");

  const t = now();
  const remark = String(body.remark || "");

  // 自愈：关闭该 worker 全部 open segments + 重算 count
  await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');
  const realCount = await recalcActiveCount(env, job_id, t);

  if (remark) {
    const resultJson = JSON.stringify({ remark });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET shared_result_json=?, updated_at=? WHERE id=?"
    ).bind(resultJson, t, job_id).run();
  }

  if (complete_job) {
    if (realCount <= 0) {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', updated_at=? WHERE id=?"
      ).bind(t, job_id).run();
      const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
      if (job && job.related_doc_id) {
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='completed', updated_at=? WHERE id=?"
        ).bind(t, job.related_doc_id).run();
      }
    }
  }

  return json({ ok: true });
});

// =====================================================
// GENERIC OPS JOB — for flexible use
// =====================================================
route("v2_ops_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const flow_stage = String(body.flow_stage || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  const job_type = String(body.job_type || "").trim();
  const related_doc_type = String(body.related_doc_type || "").trim();
  const related_doc_id = String(body.related_doc_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const parent_job_id = String(body.parent_job_id || "").trim();
  const is_temporary_interrupt = body.is_temporary_interrupt ? 1 : 0;
  const interrupt_type = String(body.interrupt_type || "").trim();
  if (!worker_id) return err("missing worker_id");

  const t = now();

  // Check for existing job on same doc
  let job = null;
  if (related_doc_type && related_doc_id) {
    const existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE related_doc_type=? AND related_doc_id=? AND job_type=? AND status IN ('pending','working') AND is_temporary_interrupt=0 LIMIT 1"
    ).bind(related_doc_type, related_doc_id, job_type).first();
    if (existing) job = existing;
  }

  let job_id, is_new_job = false;
  if (job) {
    job_id = job.id;
    // 防重：同一 worker 已有 open segment 则直接返回
    const dup = await findOpenSeg(env, job_id, worker_id);
    if (dup) return json({ ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, updated_at=?, status='working' WHERE id=?"
    ).bind(t, job_id).run();
  } else {
    job_id = "JOB-" + uid();
    is_new_job = true;
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, parent_job_id, is_temporary_interrupt, interrupt_type, created_by, created_at, updated_at, active_worker_count)
      VALUES(?,?,?,?,?,?,'working',?,?,?,?,?,?,1)
    `).bind(job_id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        parent_job_id, is_temporary_interrupt, interrupt_type, worker_id, t, t).run();
  }

  // If this is an interrupt, pause the parent job for this worker — 自愈式关闭
  if (is_temporary_interrupt && parent_job_id) {
    await closeAllOpenSegs(env, parent_job_id, worker_id, t, 'interrupted');
    await recalcActiveCount(env, parent_job_id, t);
  }

  const seg_id = "WS-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
    VALUES(?,?,?,?,?)
  `).bind(seg_id, job_id, worker_id, worker_name, t).run();

  return json({ ok: true, job_id, worker_seg_id: seg_id, is_new_job });
});

route("v2_ops_job_leave", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id || !worker_id) return err("missing job_id or worker_id");

  const t = now();
  // 自愈：关闭该 worker 全部 open segments + 重算 count
  await closeAllOpenSegs(env, job_id, worker_id, t, String(body.leave_reason || 'leave'));
  const realCount = await recalcActiveCount(env, job_id, t);

  // Check if job should go to awaiting_close — 基于 realCount
  if (realCount <= 0) {
    const job = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (job && job.status === "working") {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='awaiting_close', updated_at=? WHERE id=?"
      ).bind(t, job_id).run();
    }
  }

  return json({ ok: true });
});

route("v2_ops_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  const t = now();

  // 自愈：关闭该 worker 全部 open segments
  await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');

  // Save shared result
  const shared = body.shared_result || {};
  if (Object.keys(shared).length > 0) {
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET shared_result_json=?, updated_at=? WHERE id=?"
    ).bind(JSON.stringify(shared), t, job_id).run();
  }

  // Save result record if provided
  if (body.box_count != null || body.pallet_count != null || body.remark) {
    const result_id = "RES-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, created_by, created_at)
      VALUES(?,?,?,?,?,?,?,?)
    `).bind(result_id, job_id, Number(body.box_count || 0), Number(body.pallet_count || 0),
        String(body.remark || ""), JSON.stringify(shared), worker_id, t).run();
  }

  // Complete the job — 关闭所有剩余 open segments + 重算归零
  await env.DB.prepare(
    "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
  ).bind(t, job_id).run();
  await env.DB.prepare(
    "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
  ).bind(t, job_id).run();

  return json({ ok: true });
});

route("v2_ops_job_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  if (!job_id) return err("missing job_id");
  // 实时校正 active_worker_count
  await recalcActiveCount(env, job_id, now());
  const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
  if (!job) return err("not found", 404);
  const workers = await env.DB.prepare(
    "SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at DESC"
  ).bind(job_id).all();
  const results = await env.DB.prepare(
    "SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at DESC"
  ).bind(job_id).all();
  const atts = await env.DB.prepare(
    "SELECT * FROM v2_attachments WHERE related_doc_type='ops_job' AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(job_id).all();
  return json({
    ok: true, job,
    workers: workers.results || [],
    results: results.results || [],
    attachments: atts.results || []
  });
});

// Get worker's current active job
route("v2_ops_my_active_job", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  if (!worker_id) return err("missing worker_id");

  const seg = await env.DB.prepare(
    "SELECT * FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' ORDER BY joined_at DESC LIMIT 1"
  ).bind(worker_id).first();
  if (!seg) return json({ ok: true, active: false });

  // 实时校正 active_worker_count，确保首页人数与任务页一致
  const t = now();
  await recalcActiveCount(env, seg.job_id, t);
  const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(seg.job_id).first();
  return json({ ok: true, active: true, segment: seg, job });
});

// Resume parent job after interrupt
route("v2_ops_job_resume", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const parent_job_id = String(body.parent_job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!parent_job_id || !worker_id) return err("missing parent_job_id or worker_id");

  const t = now();
  const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(parent_job_id).first();
  if (!job) return err("parent job not found", 404);

  // 防重：同一 worker 已有 open segment 则不重复插入
  const dup = await findOpenSeg(env, parent_job_id, worker_id);
  if (dup) {
    // 仍需确保 status 从 awaiting_close 恢复
    if (job.status === "awaiting_close") {
      await recalcActiveCount(env, parent_job_id, t);
      const rc = await env.DB.prepare("SELECT active_worker_count as c FROM v2_ops_jobs WHERE id=?").bind(parent_job_id).first();
      if (rc && rc.c > 0) {
        await env.DB.prepare("UPDATE v2_ops_jobs SET status='working', resumed_at=?, updated_at=? WHERE id=?").bind(t, t, parent_job_id).run();
      }
    }
    return json({ ok: true, worker_seg_id: dup.id, already_joined: true });
  }

  const seg_id = "WS-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
    VALUES(?,?,?,?,?)
  `).bind(seg_id, parent_job_id, worker_id, worker_name, t).run();

  // 重算 count + 恢复 status
  const realCount = await recalcActiveCount(env, parent_job_id, t);
  if (job.status === "awaiting_close" && realCount > 0) {
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='working', resumed_at=?, updated_at=? WHERE id=?"
    ).bind(t, t, parent_job_id).run();
  }

  return json({ ok: true, worker_seg_id: seg_id });
});

// =====================================================
// ATTACHMENTS
// =====================================================
route("v2_attachment_upload", async (body, env, request) => {
  if (!request) return err("upload requires multipart POST");
  const formData = await request.formData();
  const k = formData.get("k") || "";
  if (!isAuth({ k }, env)) return err("unauthorized", 401);

  const file = formData.get("file");
  if (!file) return err("missing file");

  const related_doc_type = formData.get("related_doc_type") || "";
  const related_doc_id = formData.get("related_doc_id") || "";
  const attachment_category = formData.get("attachment_category") || "";
  const uploaded_by = formData.get("uploaded_by") || "";

  const id = "ATT-" + uid();
  const fileKey = `v2/${related_doc_type}/${related_doc_id}/${id}-${file.name}`;
  const t = now();

  // Upload to R2
  await env.R2_BUCKET.put(fileKey, file.stream(), {
    httpMetadata: { contentType: file.type }
  });

  await env.DB.prepare(`
    INSERT INTO v2_attachments(id, related_doc_type, related_doc_id, attachment_category,
      file_name, file_key, file_size, content_type, uploaded_by, created_at)
    VALUES(?,?,?,?,?,?,?,?,?,?)
  `).bind(id, related_doc_type, related_doc_id, attachment_category,
      file.name, fileKey, file.size, file.type, uploaded_by, t).run();

  return json({ ok: true, id, file_key: fileKey });
});

route("v2_attachment_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const doc_type = String(body.related_doc_type || "").trim();
  const doc_id = String(body.related_doc_id || "").trim();
  if (!doc_type || !doc_id) return err("missing related_doc_type or related_doc_id");
  const rs = await env.DB.prepare(
    "SELECT * FROM v2_attachments WHERE related_doc_type=? AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(doc_type, doc_id).all();
  return json({ ok: true, items: rs.results || [] });
});

route("v2_attachment_get", async (body, env) => {
  const file_key = String(body.file_key || "").trim();
  if (!file_key) return err("missing file_key");
  const obj = await env.R2_BUCKET.get(file_key);
  if (!obj) return err("file not found", 404);
  return new Response(obj.body, {
    headers: {
      "Content-Type": obj.httpMetadata?.contentType || "application/octet-stream",
      ...CORS
    }
  });
});

// =====================================================
// FIELD FEEDBACKS — basic CRUD
// =====================================================
route("v2_feedback_create", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = "FB-" + uid();
  const t = now();
  await env.DB.prepare(`
    INSERT INTO v2_field_feedbacks(id, feedback_type, related_doc_type, related_doc_id,
      title, content, submitted_by, status, created_at, updated_at)
    VALUES(?,?,?,?,?,?,?,'open',?,?)
  `).bind(id, String(body.feedback_type || ""), String(body.related_doc_type || ""),
      String(body.related_doc_id || ""), String(body.title || ""), String(body.content || ""),
      String(body.submitted_by || ""), t, t).run();
  return json({ ok: true, id });
});

route("v2_feedback_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const feedback_type = String(body.feedback_type || "").trim();
  const status = String(body.status || "").trim();
  let sql = "SELECT * FROM v2_field_feedbacks WHERE 1=1";
  const binds = [];
  if (feedback_type) { sql += " AND feedback_type=?"; binds.push(feedback_type); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  sql += " ORDER BY created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  return json({ ok: true, items: rs.results || [] });
});

route("v2_feedback_detail", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const row = await env.DB.prepare("SELECT * FROM v2_field_feedbacks WHERE id=?").bind(id).first();
  if (!row) return err("not found", 404);
  // Get related job results if linked to a job
  let jobResults = [];
  if (row.related_doc_type === "ops_job" && row.related_doc_id) {
    const jr = await env.DB.prepare(
      "SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at DESC"
    ).bind(row.related_doc_id).all();
    jobResults = jr.results || [];
  }
  return json({ ok: true, feedback: row, job_results: jobResults });
});

route("v2_feedback_convert_to_inbound", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const feedback_id = String(body.feedback_id || "").trim();
  if (!feedback_id) return err("missing feedback_id");

  const fb = await env.DB.prepare("SELECT * FROM v2_field_feedbacks WHERE id=?").bind(feedback_id).first();
  if (!fb) return err("feedback not found", 404);

  const t = now();
  const id = "IB-" + uid();
  const plan_date = kstToday();
  const customer = String(body.customer || "");
  const biz_class = String(body.biz_class || "");
  const created_by = String(body.created_by || "");
  const display_no = await nextDisplayNo(env, plan_date);

  await env.DB.prepare(`
    INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, cargo_summary,
      expected_arrival, purpose, remark, status, source_feedback_id, created_by, created_at, updated_at, display_no)
    VALUES(?,?,?,?,?,?,?,?,'pending',?,?,?,?,?)
  `).bind(
    id, plan_date, customer, biz_class,
    String(body.cargo_summary || fb.title || ""),
    String(body.expected_arrival || ""),
    String(body.purpose || ""),
    String(body.remark || fb.content || ""),
    feedback_id, created_by, t, t, display_no
  ).run();

  // Insert lines if provided
  const lines = body.lines || [];
  for (let i = 0; i < lines.length; i++) {
    const ln = lines[i];
    await env.DB.prepare(`
      INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, remark)
      VALUES(?,?,?,?,?,?)
    `).bind("IPL-" + uid(), id, i + 1, String(ln.unit_type || ""), Number(ln.planned_qty || 0), String(ln.remark || "")).run();
  }

  // Update feedback status to converted
  await env.DB.prepare(
    "UPDATE v2_field_feedbacks SET status='converted', updated_at=? WHERE id=?"
  ).bind(t, feedback_id).run();

  return json({ ok: true, inbound_plan_id: id, display_no });
});

// =====================================================
// SCAN BATCHES — basic CRUD
// =====================================================
route("v2_scan_batch_create", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = "SB-" + uid();
  const t = now();
  await env.DB.prepare(`
    INSERT INTO v2_scan_batches(id, batch_type, related_doc_type, related_doc_id,
      total_expected, status, created_by, created_at)
    VALUES(?,?,?,?,?,'open',?,?)
  `).bind(id, String(body.batch_type || ""), String(body.related_doc_type || ""),
      String(body.related_doc_id || ""), Number(body.total_expected || 0),
      String(body.created_by || ""), t).run();
  return json({ ok: true, id });
});

route("v2_scan_batch_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const rs = await env.DB.prepare(
    "SELECT * FROM v2_scan_batches ORDER BY created_at DESC LIMIT 200"
  ).all();
  return json({ ok: true, items: rs.results || [] });
});

// =====================================================
// Worker fetch entry
// =====================================================
export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS });
    }

    try {
      await ensureMigrated(env.DB);
    } catch (e) {
      return json({ ok: false, error: "migration failed: " + e.message }, 500);
    }

    const url = new URL(request.url);

    // Handle attachment file GET
    if (url.pathname === "/file" && request.method === "GET") {
      const fileKey = url.searchParams.get("key") || "";
      if (!fileKey) return err("missing key");
      const obj = await env.R2_BUCKET.get(fileKey);
      if (!obj) return err("not found", 404);
      return new Response(obj.body, {
        headers: {
          "Content-Type": obj.httpMetadata?.contentType || "application/octet-stream",
          "Cache-Control": "public, max-age=86400",
          ...CORS
        }
      });
    }

    // Parse body
    let body = {};
    let isMultipart = false;
    const ct = request.headers.get("content-type") || "";

    if (request.method === "GET") {
      body = Object.fromEntries(url.searchParams);
    } else if (ct.includes("multipart/form-data")) {
      isMultipart = true;
      // For multipart, we pass the request to the handler directly
      const formData = await request.formData();
      body = { action: formData.get("action") || "", k: formData.get("k") || "" };
      // Re-create formData-capable request is tricky, so handle in route
    } else if (ct.includes("application/json")) {
      body = await request.json().catch(() => ({}));
    } else {
      const txt = await request.text().catch(() => "");
      try { body = JSON.parse(txt); } catch { body = Object.fromEntries(new URLSearchParams(txt)); }
    }

    const action = String(body.action || "").trim();

    // Special handling for multipart upload
    if (action === "v2_attachment_upload" || isMultipart) {
      // Re-fetch from original request
      return await handleMultipartUpload(request, env);
    }

    const handler = HANDLERS[action];
    if (!handler) {
      return err("unknown action: " + action, 404);
    }

    try {
      return await handler(body, env, request);
    } catch (e) {
      return json({ ok: false, error: e.message || "internal error" }, 500);
    }
  }
};

// Special multipart handler
async function handleMultipartUpload(request, env) {
  try {
    await ensureMigrated(env.DB);
    const formData = await request.formData();
    const k = formData.get("k") || "";
    if (!isOpsAuth({ k }, env)) return err("unauthorized", 401);

    const file = formData.get("file");
    if (!file) return err("missing file");

    const related_doc_type = formData.get("related_doc_type") || "";
    const related_doc_id = formData.get("related_doc_id") || "";
    const attachment_category = formData.get("attachment_category") || "";
    const uploaded_by = formData.get("uploaded_by") || "";

    const id = "ATT-" + uid();
    const fileKey = `v2/${related_doc_type}/${related_doc_id}/${id}-${file.name}`;
    const t = now();

    await env.R2_BUCKET.put(fileKey, file.stream(), {
      httpMetadata: { contentType: file.type }
    });

    await env.DB.prepare(`
      INSERT INTO v2_attachments(id, related_doc_type, related_doc_id, attachment_category,
        file_name, file_key, file_size, content_type, uploaded_by, created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)
    `).bind(id, related_doc_type, related_doc_id, attachment_category,
        file.name, fileKey, file.size, file.type, uploaded_by, t).run();

    return json({ ok: true, id, file_key: fileKey });
  } catch (e) {
    return json({ ok: false, error: e.message }, 500);
  }
}
