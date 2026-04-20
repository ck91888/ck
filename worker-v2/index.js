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

// ===== Idempotency helper =====
// 用法：
//   route("v2_xxx_create", async (body, env) => {
//     if (!isAuth(body, env)) return err("unauthorized", 401);
//     return withIdem(env, body, "v2_xxx_create", async () => {
//       // ... 业务逻辑 ...
//       return { ok: true, id };   // 返回 plain object，withIdem 会包 json()
//     });
//   });
// client_req_id 由前端生成，一次点击一份。同 key 重入直接返回上次结果，防止网络重发 / 双击穿透前端锁产生多记录。
async function withIdem(env, body, action, fn) {
  const key = String((body && body.client_req_id) || "").trim();
  if (key) {
    try {
      const row = await env.DB.prepare(
        "SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?"
      ).bind(key).first();
      if (row && row.response_json) {
        try {
          const cached = JSON.parse(row.response_json);
          return json(cached);
        } catch (e) { /* fall through */ }
      }
    } catch (e) { /* table may not exist yet on first run */ }
  }
  let result;
  try {
    result = await fn();
  } catch (e) {
    // 异常不缓存，让客户端可以重试
    return json({ ok: false, error: e.message || "internal error" }, 500);
  }
  if (key && result && typeof result === 'object') {
    try {
      await env.DB.prepare(
        "INSERT OR IGNORE INTO v2_idempotency_keys(idem_key, action, response_json, created_at) VALUES(?,?,?,?)"
      ).bind(key, action, JSON.stringify(result), now()).run();
    } catch (e) { /* ignore idem write failures */ }
  }
  return json(result);
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

// ===== Feedback Display No helper =====
async function nextFeedbackDisplayNo(env, date, prefix) {
  const dateStr = String(date || kstToday()).replace(/-/g, '');
  const pfx = prefix + '-' + dateStr + '-';
  for (let attempt = 0; attempt < 3; attempt++) {
    const row = await env.DB.prepare(
      "SELECT display_no FROM v2_field_feedbacks WHERE display_no LIKE ? ORDER BY display_no DESC LIMIT 1"
    ).bind(pfx + '%').first();
    let seq = 1;
    if (row && row.display_no) {
      const tail = row.display_no.split('-').pop();
      seq = (parseInt(tail, 10) || 0) + 1;
    }
    const no = pfx + String(seq).padStart(3, '0');
    const dup = await env.DB.prepare(
      "SELECT 1 FROM v2_field_feedbacks WHERE display_no=? LIMIT 1"
    ).bind(no).first();
    if (!dup) return no;
  }
  return prefix + '-' + dateStr + '-' + Date.now().toString(36).slice(-4);
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

// ===== Pick Trip No helper =====
// PK-YYYYMMDD-001 format, based on v2_ops_jobs.display_no
async function nextPickTripNo(env) {
  const dateStr = kstToday().replace(/-/g, '');
  const prefix = 'PK-' + dateStr + '-';
  for (let attempt = 0; attempt < 3; attempt++) {
    const row = await env.DB.prepare(
      "SELECT display_no FROM v2_ops_jobs WHERE job_type='pick_direct' AND display_no LIKE ? ORDER BY display_no DESC LIMIT 1"
    ).bind(prefix + '%').first();
    let seq = 1;
    if (row && row.display_no) {
      const tail = row.display_no.split('-').pop();
      seq = (parseInt(tail, 10) || 0) + 1;
    }
    const no = prefix + String(seq).padStart(3, '0');
    const dup = await env.DB.prepare(
      "SELECT 1 FROM v2_ops_jobs WHERE display_no=? LIMIT 1"
    ).bind(no).first();
    if (!dup) return no;
  }
  return 'PK-' + dateStr + '-' + Date.now().toString(36).slice(-4);
}

// ===== Outbound Display No helper =====
// CHU-YYYYMMDD-001 format
async function nextOutboundDisplayNo(env, orderDate) {
  const dateStr = String(orderDate || kstToday()).replace(/-/g, '');
  const prefix = 'CHU-' + dateStr + '-';
  for (let attempt = 0; attempt < 3; attempt++) {
    const row = await env.DB.prepare(
      "SELECT display_no FROM v2_outbound_orders WHERE display_no LIKE ? ORDER BY display_no DESC LIMIT 1"
    ).bind(prefix + '%').first();
    let seq = 1;
    if (row && row.display_no) {
      const tail = row.display_no.split('-').pop();
      seq = (parseInt(tail, 10) || 0) + 1;
    }
    const no = prefix + String(seq).padStart(3, '0');
    const dup = await env.DB.prepare(
      "SELECT 1 FROM v2_outbound_orders WHERE display_no=? LIMIT 1"
    ).bind(no).first();
    if (!dup) return no;
  }
  return 'CHU-' + dateStr + '-' + Date.now().toString(36).slice(-4);
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

  // ---- unplanned_unload: feedback-first flow columns ----
  `ALTER TABLE v2_field_feedbacks ADD COLUMN result_lines_json TEXT DEFAULT '[]'`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN diff_note TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN remark TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN completed_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN completed_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN inbound_plan_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN parent_job_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN interrupt_type TEXT DEFAULT ''`,

  // ---- display_no for feedbacks ----
  `ALTER TABLE v2_field_feedbacks ADD COLUMN display_no TEXT DEFAULT ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_feedback_display_no ON v2_field_feedbacks(display_no) WHERE display_no != ''`,

  // ---- manual completion tracking for inbound plans ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN manual_completed_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN manual_completed_at TEXT DEFAULT ''`,

  // ---- performance indexes for inbound plans ----
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_status ON v2_inbound_plans(status)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_plan_date_status ON v2_inbound_plans(plan_date, status)`,

  // ---- putaway tracking on plan lines ----
  `ALTER TABLE v2_inbound_plan_lines ADD COLUMN putaway_qty REAL DEFAULT 0`,
  `ALTER TABLE v2_inbound_plan_lines ADD COLUMN putaway_remark TEXT DEFAULT ''`,

  // ---- external WMS inbound number (for standard inbound started from external no) ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN external_inbound_no TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_external_no ON v2_inbound_plans(external_inbound_no) WHERE external_inbound_no != ''`,

  // ---- idempotency keys for create/start/convert class writes ----
  `CREATE TABLE IF NOT EXISTS v2_idempotency_keys (
    idem_key TEXT PRIMARY KEY,
    action TEXT,
    response_json TEXT,
    created_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_idem_created ON v2_idempotency_keys(created_at)`,

  // ---- v2_ops_login_events: 记录每次现场系统登录 ----
  `CREATE TABLE IF NOT EXISTS v2_ops_login_events (
    id TEXT PRIMARY KEY,
    worker_id TEXT DEFAULT '',
    worker_name TEXT DEFAULT '',
    login_at TEXT,
    login_date TEXT DEFAULT '',
    page_source TEXT DEFAULT '',
    device_info TEXT DEFAULT ''
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_login_date ON v2_ops_login_events(login_date)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_login_worker ON v2_ops_login_events(worker_id, login_date)`,

  // ---- v2_ops_job_pick_docs: 拣货任务关联的拣货单号 ----
  `CREATE TABLE IF NOT EXISTS v2_ops_job_pick_docs (
    id TEXT PRIMARY KEY,
    job_id TEXT DEFAULT '',
    pick_doc_no TEXT DEFAULT '',
    created_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pick_docs_job ON v2_ops_job_pick_docs(job_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pick_docs_no ON v2_ops_job_pick_docs(pick_doc_no)`,

  // ---- display_no on v2_ops_jobs for trip numbers (PK-YYYYMMDD-NNN) ----
  `ALTER TABLE v2_ops_jobs ADD COLUMN display_no TEXT DEFAULT ''`,

  // ---- v2_correction_requests: 主管修正申请（由看板发起，不直接修改业务数据） ----
  `CREATE TABLE IF NOT EXISTS v2_correction_requests (
    id TEXT PRIMARY KEY,
    type TEXT DEFAULT '',
    target_id TEXT DEFAULT '',
    target_label TEXT DEFAULT '',
    reporter TEXT DEFAULT '',
    reason TEXT DEFAULT '',
    status TEXT DEFAULT 'open',
    handled_by TEXT DEFAULT '',
    handled_at TEXT DEFAULT '',
    handle_note TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_corr_status ON v2_correction_requests(status, created_at)`,

  // ---- v2_admin_cleanup_logs: 脏数据清理操作审计 ----
  `CREATE TABLE IF NOT EXISTS v2_admin_cleanup_logs (
    id TEXT PRIMARY KEY,
    operator TEXT DEFAULT '',
    action_type TEXT DEFAULT '',
    target_job_id TEXT DEFAULT '',
    target_worker_id TEXT DEFAULT '',
    reason TEXT DEFAULT '',
    detail_json TEXT DEFAULT '',
    created_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_cleanup_log_time ON v2_admin_cleanup_logs(created_at)`,

  // ---- 出库作业单口径调整：单头字段扩充（destination/po_no/wms_work_order_no + 计划/实际 箱托）----
  `ALTER TABLE v2_outbound_orders ADD COLUMN destination TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN po_no TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN wms_work_order_no TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN planned_box_count INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN planned_pallet_count INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN actual_box_count INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN actual_pallet_count INTEGER DEFAULT 0`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_wms_wo ON v2_outbound_orders(wms_work_order_no) WHERE wms_work_order_no != ''`,

  // ---- display_no for outbound orders (CHU-YYYYMMDD-NNN) ----
  `ALTER TABLE v2_outbound_orders ADD COLUMN display_no TEXT DEFAULT ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_outbound_display_no ON v2_outbound_orders(display_no) WHERE display_no != ''`,
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
  return withIdem(env, body, "v2_issue_create", async () => {
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
    return { ok: true, id };
  });
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

  return withIdem(env, body, "v2_issue_handle_start", async () => {
    const issue = await env.DB.prepare("SELECT * FROM v2_issue_tickets WHERE id=?").bind(issue_id).first();
    if (!issue) return { ok: false, error: "issue not found" };
    if (issue.status === "closed" || issue.status === "cancelled") return { ok: false, error: "issue already " + issue.status };

    const t = now();
    const job_id = "JOB-" + uid();
    const run_id = "RUN-" + uid();

    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, created_by, created_at, updated_at, active_worker_count)
      VALUES(?, 'issue_handle', ?, 'issue_handle', 'issue_ticket', ?, 'working', ?, ?, ?, 1)
    `).bind(job_id, issue.biz_class || "", issue_id, handler_id, t, t).run();

    const worker_seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(worker_seg_id, job_id, handler_id, handler_name, t).run();

    await env.DB.prepare(`
      INSERT INTO v2_issue_handle_runs(id, issue_id, job_id, handler_id, handler_name, started_at, run_status, created_at)
      VALUES(?,?,?,?,?,?,'working',?)
    `).bind(run_id, issue_id, job_id, handler_id, handler_name, t, t).run();

    await env.DB.prepare(
      "UPDATE v2_issue_tickets SET status='processing', updated_at=? WHERE id=?"
    ).bind(t, issue_id).run();

    return { ok: true, job_id, run_id, worker_seg_id };
  });
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
  return withIdem(env, body, "v2_outbound_order_create", async () => {
    const outbound_mode = String(body.outbound_mode || "").trim();
    const VALID_MODES = ['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'];
    if (!outbound_mode || !VALID_MODES.includes(outbound_mode)) return err("invalid outbound_mode");
    const id = "OB-" + uid();
    const t = now();
    const order_date = String(body.order_date || kstToday());
    const display_no = await nextOutboundDisplayNo(env, order_date);
    // 口径调整：所有出库单均联动大货操作，biz_class 固定 'bulk'；
    // 不再从前端接收 biz_class / operation_mode / remark
    await env.DB.prepare(`
      INSERT INTO v2_outbound_orders(id, order_date, customer, biz_class, operation_mode,
        outbound_mode, instruction, remark, status, created_by, created_at, updated_at,
        destination, po_no, wms_work_order_no,
        planned_box_count, planned_pallet_count, actual_box_count, actual_pallet_count, display_no)
      VALUES(?,?,?,'bulk','',?,?,'','draft',?,?,?,?,?,?,?,?,0,0,?)
    `).bind(
      id,
      order_date,
      String(body.customer || ""),
      outbound_mode,
      String(body.instruction || ""),
      String(body.created_by || ""),
      t, t,
      String(body.destination || ""),
      String(body.po_no || ""),
      String(body.wms_work_order_no || ""),
      Number(body.planned_box_count || 0),
      Number(body.planned_pallet_count || 0),
      display_no
    ).run();

    const lines = body.lines || [];
    for (let i = 0; i < lines.length; i++) {
      const ln = lines[i];
      // 行级 wms_order_no 已废弃；单头承载 wms_work_order_no，这里写空保留兼容列
      await env.DB.prepare(`
        INSERT INTO v2_outbound_order_lines(id, order_id, line_no, wms_order_no, sku, quantity, remark)
        VALUES(?,?,?,'',?,?,?)
      `).bind("OBL-" + uid(), id, i + 1, String(ln.sku || ""), Number(ln.quantity || 0), String(ln.remark || "")).run();
    }

    return { ok: true, id, display_no };
  });
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

  return withIdem(env, body, "v2_outbound_load_start", async () => {
    const t = now();
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
      const dup = await findOpenSeg(env, job_id, worker_id);
      if (dup) return { ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true };
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

    return { ok: true, job_id, worker_seg_id: seg_id, is_new_job };
  });
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

  // 终态幂等保护：已完成的 job 不再重复写
  const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
  if (jobCheck && jobCheck.status === 'completed') {
    return json({ ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" });
  }

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
        // 口径联动：完成时回写 actual_box_count / actual_pallet_count
        await env.DB.prepare(
          "UPDATE v2_outbound_orders SET status='completed', actual_box_count=?, actual_pallet_count=?, updated_at=? WHERE id=?"
        ).bind(box_count, pallet_count, t, job.related_doc_id).run();
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
  return withIdem(env, body, "v2_inbound_plan_create", async () => {
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

    const lines = body.lines || [];
    for (let i = 0; i < lines.length; i++) {
      const ln = lines[i];
      await env.DB.prepare(`
        INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, remark)
        VALUES(?,?,?,?,?,?)
      `).bind("IPL-" + uid(), id, i + 1, String(ln.unit_type || ""), Number(ln.planned_qty || 0), String(ln.remark || "")).run();
    }

    let outbound_id = null;
    let outbound_display_no = null;
    if (body.auto_create_outbound) {
      outbound_id = "OB-" + uid();
      const ob_date = String(body.plan_date || kstToday());
      outbound_display_no = await nextOutboundDisplayNo(env, ob_date);
      // 口径调整：auto-create outbound 同步新字段；biz_class 固定 'bulk'，不再接 op_mode/remark
      await env.DB.prepare(`
        INSERT INTO v2_outbound_orders(id, order_date, customer, biz_class, operation_mode,
          outbound_mode, instruction, remark, status, source_inbound_plan_id, created_by, created_at, updated_at,
          destination, po_no, wms_work_order_no,
          planned_box_count, planned_pallet_count, actual_box_count, actual_pallet_count, display_no)
        VALUES(?,?,?,'bulk','',?,?,'','draft',?,?,?,?,?,?,?,?,?,0,0,?)
      `).bind(
        outbound_id,
        ob_date,
        customer,
        String(body.ob_outbound_mode || ""),
        String(body.ob_instruction || ""),
        id, created_by, t, t,
        String(body.ob_destination || ""),
        String(body.ob_po_no || ""),
        String(body.ob_wms_work_order_no || ""),
        Number(body.ob_planned_box_count || 0),
        Number(body.ob_planned_pallet_count || 0),
        outbound_display_no
      ).run();
    }

    return { ok: true, id, display_no, outbound_id, outbound_display_no };
  });
});

// ===== Helper: check if an inbound plan is fully completed =====
// Returns { allDone: bool, unloadDone: bool, putawayDone: bool }
async function checkPlanFullyCompleted(env, plan_id) {
  // 1. Check unload is done: no active unload jobs
  const activeUnload = await env.DB.prepare(
    "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status IN ('pending','working') LIMIT 1"
  ).bind(plan_id).first();
  const unloadDone = !activeUnload;

  // 2. Check all lines have putaway_qty >= actual_qty (fallback to planned_qty)
  const lines = await env.DB.prepare(
    "SELECT planned_qty, actual_qty, putaway_qty FROM v2_inbound_plan_lines WHERE plan_id=?"
  ).bind(plan_id).all();
  let putawayDone = true;
  for (const ln of (lines.results || [])) {
    const target = (ln.actual_qty != null && ln.actual_qty > 0) ? ln.actual_qty : (ln.planned_qty || 0);
    if (target > 0 && (ln.putaway_qty || 0) < target) {
      putawayDone = false;
      break;
    }
  }

  return { allDone: unloadDone && putawayDone, unloadDone, putawayDone };
}

route("v2_inbound_plan_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const start = String(body.start_date || "").trim();
  const end = String(body.end_date || "").trim();
  const status = String(body.status || "").trim();
  // 排除退件入库会话：return_session 不属于正式入库计划口径
  let sql = "SELECT * FROM v2_inbound_plans WHERE source_type != 'return_session'";
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
  // 退件入库会话不属于正式入库计划口径，协同中心不应打开
  if (row.source_type === 'return_session') return err("not found", 404);
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
      "SELECT result_lines_json, diff_note, remark, result_json, created_at FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at DESC LIMIT 1"
    ).bind(job.id).first();

    let resultLines = [];
    if (latestResult && latestResult.result_lines_json) {
      try { resultLines = JSON.parse(latestResult.result_lines_json); } catch(e) {}
    }
    let resultNote = "";
    let extraOps = null;
    let isReturnFlag = false;
    if (latestResult && latestResult.result_json) {
      try {
        const rj = JSON.parse(latestResult.result_json);
        resultNote = rj.result_note || "";
        if (rj.extra_ops && typeof rj.extra_ops === 'object') extraOps = rj.extra_ops;
        if (rj.is_return === true) isReturnFlag = true;
      } catch(e) {}
    }

    enrichedJobs.push({
      ...job,
      worker_names: names,
      worker_names_text: names.join(", "),
      total_minutes_worked: Math.round(totalMin),
      completed_at: maxLeft || job.updated_at || "",
      result_lines: resultLines,
      diff_note: (latestResult && latestResult.diff_note) || "",
      remark: (latestResult && latestResult.remark) || "",
      result_note: resultNote,
      extra_ops: extraOps,
      is_return: isReturnFlag || (job.job_type === 'inbound_return')
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

// Find inbound plan by display_no or id (for QR scan)
route("v2_inbound_plan_find_by_code", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const code = String(body.code || "").trim();
  if (!code) return err("missing code");
  // Prefer display_no, fallback to id
  let row = await env.DB.prepare(
    "SELECT id, display_no, status, customer, cargo_summary, biz_class FROM v2_inbound_plans WHERE display_no=? AND status!='cancelled'"
  ).bind(code).first();
  if (!row) {
    row = await env.DB.prepare(
      "SELECT id, display_no, status, customer, cargo_summary, biz_class FROM v2_inbound_plans WHERE id=? AND status!='cancelled'"
    ).bind(code).first();
  }
  if (!row) return err("not found", 404);
  return json({ ok: true, plan: row });
});

// ===== Ops candidates: filtered list for putaway/unload scene =====
route("v2_inbound_plan_ops_candidates", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const scene = String(body.scene || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  const keyword = String(body.keyword || "").trim();
  const limit = Math.min(Number(body.limit) || 100, 200);

  let statusFilter;
  if (scene === 'putaway') {
    statusFilter = "('unloading','unloading_putting_away','arrived_pending_putaway','putting_away')";
  } else if (scene === 'unload') {
    statusFilter = "('pending','unloading','unloading_putting_away')";
  } else {
    return err("scene must be putaway or unload");
  }

  let sql = `SELECT id, display_no, external_inbound_no, customer, cargo_summary, status, biz_class, plan_date
    FROM v2_inbound_plans WHERE status IN ${statusFilter}`;
  const binds = [];

  if (biz_class) {
    sql += " AND biz_class=?";
    binds.push(biz_class);
  }
  if (keyword) {
    sql += " AND (display_no LIKE ? OR external_inbound_no LIKE ? OR customer LIKE ?)";
    const kw = "%" + keyword + "%";
    binds.push(kw, kw, kw);
  }
  sql += " ORDER BY created_at DESC LIMIT ?";
  binds.push(limit);

  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  return json({ ok: true, items: rs.results || [] });
});

// ===== Resolve inbound code: identify system plan vs external no =====
route("v2_inbound_resolve_code", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const code = String(body.code || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  if (!code) return err("missing code");

  // Try to find system plan by display_no, external_inbound_no, or id
  let plan = await env.DB.prepare(
    "SELECT id, display_no, external_inbound_no, status, customer, cargo_summary, biz_class, plan_date FROM v2_inbound_plans WHERE display_no=? AND status!='cancelled'"
  ).bind(code).first();
  if (!plan) {
    plan = await env.DB.prepare(
      "SELECT id, display_no, external_inbound_no, status, customer, cargo_summary, biz_class, plan_date FROM v2_inbound_plans WHERE external_inbound_no=? AND status!='cancelled' ORDER BY created_at DESC LIMIT 1"
    ).bind(code).first();
  }
  if (!plan) {
    plan = await env.DB.prepare(
      "SELECT id, display_no, external_inbound_no, status, customer, cargo_summary, biz_class, plan_date FROM v2_inbound_plans WHERE id=? AND status!='cancelled'"
    ).bind(code).first();
  }

  if (!plan) {
    // Not a system plan → treat as external inbound number
    return json({ ok: true, kind: 'external', code });
  }

  // Found system plan — check biz_class match
  if (biz_class && plan.biz_class && plan.biz_class !== biz_class) {
    return json({ ok: true, kind: 'biz_mismatch', plan, message: "该入库单不属于当前业务（" + plan.biz_class + "），不能在此页面开始入库" });
  }

  // Check status is putaway-able
  const putawayStatuses = ['unloading', 'unloading_putting_away', 'arrived_pending_putaway', 'putting_away'];
  if (putawayStatuses.indexOf(plan.status) === -1) {
    return json({ ok: true, kind: 'status_not_allowed', plan, message: "该系统入库单当前状态（" + plan.status + "）不可开始入库" });
  }

  return json({ ok: true, kind: 'system', plan });
});

// Upcoming inbound plans (next 3 working days, skip Sundays)
route("v2_inbound_plan_list_upcoming", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const today = kstToday();
  // Compute next 3 working days strictly after today (skip Sundays)
  const dates = [];
  const kstMs = Date.now() + 9 * 3600 * 1000;
  let d = new Date(kstMs);
  d.setUTCHours(0, 0, 0, 0);
  while (dates.length < 3) {
    d.setUTCDate(d.getUTCDate() + 1);
    if (d.getUTCDay() !== 0) { // 0=Sunday
      const ds = d.toISOString().slice(0, 10);
      if (ds !== today && dates.indexOf(ds) === -1) dates.push(ds);
    }
  }
  const first = dates[0];
  const last = dates[dates.length - 1];
  const rs = await env.DB.prepare(
    "SELECT * FROM v2_inbound_plans WHERE plan_date>=? AND plan_date<=? AND status NOT IN ('completed','cancelled') AND source_type != 'return_session' ORDER BY plan_date ASC, created_at ASC"
  ).bind(first, last).all();
  return json({ ok: true, items: rs.results || [], dates });
});

route("v2_inbound_plan_update_status", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const status = String(body.status || "").trim();
  if (!id || !status) return err("missing id or status");
  // 禁止通过此接口设置 cancelled，必须走专用取消接口
  if (status === 'cancelled') return err("请使用 v2_inbound_plan_cancel 取消入库计划");
  await env.DB.prepare(
    "UPDATE v2_inbound_plans SET status=?, updated_at=? WHERE id=?"
  ).bind(status, now(), id).run();
  return json({ ok: true });
});

// ===== 入库计划专用取消接口 =====
route("v2_inbound_plan_cancel", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.inbound_plan_id || "").trim();
  if (!id) return err("missing inbound_plan_id");
  const operator = String(body.operator_name || "").trim();
  const reason = String(body.reason || "").trim();

  const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
  if (!plan) return err("plan not found", 404);

  // 只允许 pending / arrived_pending_putaway 取消
  const allowCancel = ['pending', 'arrived_pending_putaway'];
  if (!allowCancel.includes(plan.status)) {
    return json({ ok: false, error: "cancel_not_allowed", message: "当前状态（" + plan.status + "）不允许取消，只有待到库和已到库待入库可以取消" });
  }

  // 检查是否有进行中的 unload 或 inbound job
  const activeJob = await env.DB.prepare(
    "SELECT id, job_type FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1"
  ).bind(id).first();
  if (activeJob) {
    return json({ ok: false, error: "active_job_exists", message: "当前仍有进行中的现场任务（" + (activeJob.job_type || "") + "），不能取消" });
  }

  const t = now();
  let updateSql = "UPDATE v2_inbound_plans SET status='cancelled', updated_at=?";
  const binds = [t];
  if (reason) {
    updateSql += ", remark=CASE WHEN remark='' THEN ? ELSE remark||' | 取消原因: '||? END";
    binds.push('取消原因: ' + reason, reason);
  }
  updateSql += " WHERE id=?";
  binds.push(id);
  await env.DB.prepare(updateSql).bind(...binds).run();

  return json({ ok: true, operator, cancelled_at: t });
});

// ===== [DEPRECATED] Dynamic plan finalize: fill info and convert to formal inbound =====
// This route is kept for backward compatibility with old field_dynamic plans only.
// New flow uses v2_feedback_finalize_to_inbound instead.
route("v2_inbound_dynamic_finalize", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_inbound_dynamic_finalize", async () => {
    const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
    if (!plan) return { ok: false, error: "not found" };
    if (plan.source_type !== "field_dynamic") return { ok: false, error: "not a dynamic plan" };
    if (plan.status !== "unloaded_pending_info") return { ok: false, error: "status must be unloaded_pending_info, current: " + plan.status };

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

    return { ok: true, id, display_no: plan.display_no };
  });
});

// =====================================================
// UNLOAD / INBOUND JOBS — Ops side
// =====================================================
// =====================================================
// UNPLANNED UNLOAD — feedback-first flow (new)
// =====================================================

// Step 1: Start unplanned unload — creates feedback + unload job, NO inbound_plan
route("v2_unplanned_unload_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const parent_job_id = String(body.parent_job_id || "").trim();
  const interrupt_type = String(body.interrupt_type || "").trim();
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_unplanned_unload_start", async () => {
    const t = now();
    const fb_id = "FB-" + uid();
    const fb_display_no = await nextFeedbackDisplayNo(env, kstToday(), 'XCXH');

    await env.DB.prepare(`
      INSERT INTO v2_field_feedbacks(id, feedback_type, related_doc_type, related_doc_id,
        title, content, submitted_by, status, parent_job_id, interrupt_type, display_no, created_at, updated_at)
      VALUES(?,'unplanned_unload','ops_job','',?,?,?,'field_working',?,?,?,?,?)
    `).bind(fb_id,
        "计划外到货-现场卸货中",
        "现场操作人员发起计划外卸货",
        worker_name || worker_id,
        parent_job_id, interrupt_type, fb_display_no, t, t).run();

    const job_id = "JOB-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, parent_job_id, is_temporary_interrupt, interrupt_type, created_by, created_at, updated_at, active_worker_count)
      VALUES(?, 'unload', '', 'unload', 'field_feedback', ?, 'working', ?, ?, ?, ?, ?, ?, 1)
    `).bind(job_id, fb_id, parent_job_id, parent_job_id ? 1 : 0, interrupt_type, worker_id, t, t).run();

    await env.DB.prepare(
      "UPDATE v2_field_feedbacks SET related_doc_id=? WHERE id=?"
    ).bind(job_id, fb_id).run();

    if (parent_job_id) {
      await closeAllOpenSegs(env, parent_job_id, worker_id, t, 'interrupted');
      await recalcActiveCount(env, parent_job_id, t);
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    return { ok: true, feedback_id: fb_id, display_no: fb_display_no, job_id, worker_seg_id: seg_id };
  });
});

// Step 2: Finish unplanned unload — save result to feedback, do NOT create inbound_plan
route("v2_unplanned_unload_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_unplanned_unload_finish", async () => {
    const t = now();
    const result_lines = body.result_lines || [];
    const diff_note = String(body.diff_note || "").trim();
    const remark = String(body.remark || "").trim();
    const leave_only = body.leave_only === true;

    if (!leave_only) {
      const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
      if (jobCheck && jobCheck.status === 'completed') {
        return { ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" };
      }
    }

    await closeAllOpenSegs(env, job_id, worker_id, t, leave_only ? 'leave' : 'finished');
    const realCount = await recalcActiveCount(env, job_id, t);

    if (leave_only) {
      return { ok: true, left: true };
    }

    if (realCount > 0) {
      return { ok: false, error: "others_still_working",
        message: "您已退出此任务，还有 " + realCount + " 人继续作业",
        active_worker_count: realCount };
    }
    const hasAnyQty = result_lines.some(ln => Number(ln.actual_qty || 0) > 0);
    if (!hasAnyQty) {
      return { ok: false, error: "empty_result", message: "至少填写一项实际数量" };
    }

    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!job) return { ok: false, error: "job not found" };

    const result_id = "RES-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, result_lines_json, diff_note, created_by, created_at)
      VALUES(?,?,0,0,?,?,?,?,?,?)
    `).bind(result_id, job_id, remark,
        JSON.stringify({ result_lines, diff_note, remark }),
        JSON.stringify(result_lines), diff_note, worker_id, t).run();

    const sharedResult = JSON.stringify({ result_lines, diff_note, remark });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', shared_result_json=?, active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(sharedResult, t, job_id).run();
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
    ).bind(t, job_id).run();

    const fb_id = (job.related_doc_type === 'field_feedback') ? job.related_doc_id : '';
    if (fb_id) {
      const cargoSummary = result_lines.map(rl => (rl.unit_type || "") + " " + (rl.actual_qty || 0)).join(" / ");
      await env.DB.prepare(`
        UPDATE v2_field_feedbacks SET status='unloaded_pending_info',
          result_lines_json=?, diff_note=?, remark=?,
          completed_at=?, completed_by=?,
          title=?, updated_at=? WHERE id=?
      `).bind(
        JSON.stringify(result_lines), diff_note, remark,
        t, worker_id,
        "计划外卸货完成: " + (cargoSummary || "无明细"), t, fb_id
      ).run();
    }

    return { ok: true, result_id, feedback_id: fb_id };
  });
});

// List active (field_working) unplanned unload feedbacks for join
route("v2_unplanned_unload_active_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const fbs = await env.DB.prepare(
    "SELECT * FROM v2_field_feedbacks WHERE feedback_type='unplanned_unload' AND status='field_working' ORDER BY created_at DESC LIMIT 50"
  ).all();
  const items = [];
  for (const fb of (fbs.results || [])) {
    const jobId = fb.related_doc_id || '';
    let activeCount = 0, workerNames = [];
    if (jobId) {
      const ws = await env.DB.prepare(
        "SELECT worker_id, worker_name FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
      ).bind(jobId).all();
      const rows = ws.results || [];
      activeCount = rows.length;
      workerNames = rows.map(r => r.worker_name || r.worker_id);
    }
    items.push({
      feedback_id: fb.id,
      display_no: fb.display_no || fb.id,
      title: fb.title,
      submitted_by: fb.submitted_by,
      created_at: fb.created_at,
      related_job_id: jobId,
      active_worker_count: activeCount,
      worker_names: workerNames
    });
  }
  return json({ ok: true, items });
});

// Join an existing unplanned unload task
route("v2_unplanned_unload_join", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const feedback_id = String(body.feedback_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!feedback_id || !worker_id) return err("missing feedback_id or worker_id");

  return withIdem(env, body, "v2_unplanned_unload_join", async () => {
    const fb = await env.DB.prepare("SELECT * FROM v2_field_feedbacks WHERE id=?").bind(feedback_id).first();
    if (!fb) return { ok: false, error: "feedback not found" };
    if (fb.status !== 'field_working') return { ok: false, error: "feedback is not in field_working status" };

    const job_id = fb.related_doc_id || '';
    if (!job_id) return { ok: false, error: "no related job found" };

    const t = now();

    const existing = await findOpenSeg(env, job_id, worker_id);
    if (existing) {
      return { ok: true, feedback_id, display_no: fb.display_no || fb.id, job_id, worker_seg_id: existing.id, already_joined: true };
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(
      "INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at) VALUES(?,?,?,?,?)"
    ).bind(seg_id, job_id, worker_id, worker_name, t).run();
    await recalcActiveCount(env, job_id, t);

    return { ok: true, feedback_id, display_no: fb.display_no || fb.id, job_id, worker_seg_id: seg_id, already_joined: false };
  });
});

// Step 3: Finalize feedback → create formal inbound plan with lines
route("v2_feedback_finalize_to_inbound", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const feedback_id = String(body.feedback_id || "").trim();
  if (!feedback_id) return err("missing feedback_id");
  const customer = String(body.customer || "").trim();
  if (!customer) return err("customer is required");

  return withIdem(env, body, "v2_feedback_finalize_to_inbound", async () => {
    const fb = await env.DB.prepare("SELECT * FROM v2_field_feedbacks WHERE id=?").bind(feedback_id).first();
    if (!fb) return { ok: false, error: "feedback not found" };
    if (fb.status !== 'unloaded_pending_info') return { ok: false, error: "feedback status must be unloaded_pending_info, current: " + fb.status };

    const t = now();
    const plan_date = kstToday();
    const plan_id = "IB-" + uid();
    const display_no = await nextDisplayNo(env, plan_date);

    const biz_class = String(body.biz_class || "").trim();
    const cargo_summary = String(body.cargo_summary || "").trim();
    const expected_arrival = String(body.expected_arrival || "").trim();
    const purpose = String(body.purpose || "").trim();
    const remark = String(body.remark || "").trim();
    const created_by = String(body.created_by || "").trim();

    await env.DB.prepare(`
      INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, cargo_summary,
        expected_arrival, purpose, remark, status, source_feedback_id, created_by, created_at, updated_at, display_no, source_type)
      VALUES(?,?,?,?,?,?,?,?,'arrived_pending_putaway',?,?,?,?,?,'from_feedback')
    `).bind(plan_id, plan_date, customer, biz_class, cargo_summary,
        expected_arrival, purpose, remark,
        feedback_id, created_by, t, t, display_no).run();

    let lines = body.lines || [];
    if (lines.length === 0) {
      try { lines = JSON.parse(fb.result_lines_json || "[]"); } catch(e) { lines = []; }
    }
    for (let i = 0; i < lines.length; i++) {
      const ln = lines[i];
      const actual = Number(ln.actual_qty || ln.planned_qty || 0);
      await env.DB.prepare(
        "INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, actual_qty, remark) VALUES(?,?,?,?,?,?,?)"
      ).bind("IPL-" + uid(), plan_id, i + 1, String(ln.unit_type || ""), actual, actual, String(ln.remark || "")).run();
    }

    await env.DB.prepare(`
      UPDATE v2_field_feedbacks SET status='converted', inbound_plan_id=?, updated_at=? WHERE id=?
    `).bind(plan_id, t, feedback_id).run();

    return { ok: true, inbound_plan_id: plan_id, display_no };
  });
});

// ===== [DEPRECATED] Dynamic no-doc unload: create plan + job in one shot =====
// This route is kept for backward compatibility but should NOT be called from frontend.
// Use v2_unplanned_unload_start instead.
route("v2_unload_dynamic_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_unload_dynamic_start", async () => {
    const t = now();
    const plan_date = kstToday();
    const plan_id = "IB-" + uid();
    const display_no = await nextDisplayNo(env, plan_date);

    await env.DB.prepare(`
      INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, cargo_summary,
        expected_arrival, purpose, remark, status, created_by, created_at, updated_at, display_no, source_type, needs_info_update)
      VALUES(?,?,'待补充','','现场无单卸货','','','','field_working',?,?,?,?,'field_dynamic',1)
    `).bind(plan_id, plan_date, worker_name || worker_id, t, t, display_no).run();

    const job_id = "JOB-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, created_by, created_at, updated_at, active_worker_count)
      VALUES(?, 'unload', '', 'unload', 'inbound_plan', ?, 'working', ?, ?, ?, 1)
    `).bind(job_id, plan_id, worker_id, t, t).run();

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    return { ok: true, plan_id, display_no, job_id, worker_seg_id: seg_id };
  });
});

route("v2_unload_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const plan_id = String(body.plan_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_unload_job_start", async () => {
    const t = now();

    if (plan_id) {
      const plan = await env.DB.prepare("SELECT status FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
      if (!plan) return { ok: false, error: "plan not found" };
      const unloadAllowed = ['pending', 'unloading', 'unloading_putting_away'];
      if (unloadAllowed.indexOf(plan.status) === -1) {
        return { ok: false, error: "unload_not_allowed_for_status", message: "当前状态不可继续卸货 / 현재 상태에서 하차 불가", current_status: plan.status };
      }
    }

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
      const dup = await findOpenSeg(env, job_id, worker_id);
      if (dup) return { ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true };
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, updated_at=?, status='working' WHERE id=?"
      ).bind(t, job_id).run();
    } else {
      if (plan_id) {
        const plan2 = await env.DB.prepare("SELECT status FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
        if (plan2 && (plan2.status === 'unloading' || plan2.status === 'unloading_putting_away')) {
          return { ok: false, error: "unload_status_inconsistent", message: "状态为卸货中但无活跃卸货任务，请联系管理员检查" };
        }
      }
      job_id = "JOB-" + uid();
      is_new_job = true;
      await env.DB.prepare(`
        INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
          status, created_by, created_at, updated_at, active_worker_count)
        VALUES(?, 'unload', ?, 'unload', 'inbound_plan', ?, 'working', ?, ?, ?, 1)
      `).bind(job_id, biz_class, plan_id, worker_id, t, t).run();
      if (plan_id) {
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='unloading', updated_at=? WHERE id=? AND status='pending'"
        ).bind(t, plan_id).run();
      }
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    return { ok: true, job_id, worker_seg_id: seg_id, is_new_job };
  });
});

route("v2_unload_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_unload_job_finish", async () => {
  const t = now();
  const leave_only = body.leave_only === true;
  const complete_job = body.complete_job === true;
  const result_lines = body.result_lines || [];
  const diff_note = String(body.diff_note || "").trim();
  const remark = String(body.remark || "");

  if (!leave_only) {
    const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (jobCheck && jobCheck.status === 'completed') {
      return { ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" };
    }
  }

  if (complete_job && !leave_only) {
    const preJob = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!preJob) return { ok: false, error: "job not found" };

    const hasAnyQty = result_lines.some(ln => Number(ln.actual_qty || 0) > 0);
    if (!hasAnyQty) {
      return { ok: false, error: "empty_result", message: "至少填写一项实际数量" };
    }

    if (preJob.related_doc_type === 'inbound_plan' && preJob.related_doc_id) {
      const planCheck = await env.DB.prepare("SELECT status FROM v2_inbound_plans WHERE id=?").bind(preJob.related_doc_id).first();
      const unloadFinishAllowed = ['unloading', 'unloading_putting_away'];
      if (planCheck && unloadFinishAllowed.indexOf(planCheck.status) === -1) {
        return { ok: false, error: "unload_plan_status_invalid", message: "当前卸货计划状态已变化（" + planCheck.status + "），不能继续完成" };
      }
    }
  }

  await closeAllOpenSegs(env, job_id, worker_id, t, leave_only ? 'leave' : 'finished');
  const realCount = await recalcActiveCount(env, job_id, t);

  if (leave_only) {
    return { ok: true, left: true };
  }

  if (complete_job) {
    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!job) return { ok: false, error: "job not found" };

    if (realCount > 0) {
      return { ok: false, error: "others_still_working",
        message: "您已退出此任务，还有 " + realCount + " 人继续作业",
        active_worker_count: realCount };
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

    // diff_note is optional — warehouse records observations, not reasons

    // 4d. Write result record
    const result_id = "RES-" + uid();
    const box_count = Number(body.box_count || 0);
    const pallet_count = Number(body.pallet_count || 0);
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, result_lines_json, diff_note, created_by, created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)
    `).bind(result_id, job_id, box_count, pallet_count, remark,
        JSON.stringify({ box_count, pallet_count, remark, has_diff: hasDiff }),
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
    const sharedResult = JSON.stringify({ box_count, pallet_count, remark, result_lines, diff_note, has_diff: hasDiff });
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
        return { ok: true, result_id, dynamic_plan: true, plan_id };
      } else {
        // Check if inbound (putaway) jobs are currently active for this plan
        const activeInboundJob = await env.DB.prepare(
          "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type IN ('inbound_direct','inbound_bulk') AND status IN ('pending','working') LIMIT 1"
        ).bind(plan_id).first();
        if (activeInboundJob) {
          // Parallel: inbound still running → putting_away
          await env.DB.prepare(
            "UPDATE v2_inbound_plans SET status='putting_away', updated_at=? WHERE id=?"
          ).bind(t, plan_id).run();
        } else {
          await env.DB.prepare(
            "UPDATE v2_inbound_plans SET status='arrived_pending_putaway', updated_at=? WHERE id=?"
          ).bind(t, plan_id).run();
        }
      }
    }

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
      return { ok: true, result_id, feedback_id: fb_id, no_doc: true };
    }

    return { ok: true, result_id };
  }

  return { ok: true };
  });
});

route("v2_inbound_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  const job_type = String(body.job_type || "inbound_direct").trim();
  const external_inbound_no = String(body.external_inbound_no || "").trim();
  const customer_name = String(body.customer_name || "").trim();
  const start_remark = String(body.start_remark || "").trim();
  if (!worker_id) return err("missing worker_id");

  const VALID_JOB_TYPES = ['inbound_direct', 'inbound_bulk', 'inbound_return'];
  if (VALID_JOB_TYPES.indexOf(job_type) === -1) return err("invalid job_type: " + job_type);
  const isReturn = (job_type === 'inbound_return');
  const isStandard = (job_type === 'inbound_direct' || job_type === 'inbound_bulk');

  if (isReturn && biz_class && biz_class !== 'return') {
    return err("biz_class mismatch for inbound_return: " + biz_class);
  }
  if (isStandard && biz_class !== 'direct_ship' && biz_class !== 'bulk') {
    return err("biz_class must be direct_ship or bulk for standard inbound, got: " + biz_class);
  }

  return withIdem(env, body, "v2_inbound_job_start", async () => {
    let plan_id = String(body.plan_id || "").trim();
    const t = now();
    const today = kstToday();

    // ===== Path A: start from existing system inbound plan (standard only) =====
    if (plan_id && isStandard) {
      const plan = await env.DB.prepare("SELECT status, biz_class FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
      if (!plan) return { ok: false, error: "plan not found" };
      const inboundStartAllowed = ['unloading', 'unloading_putting_away', 'arrived_pending_putaway', 'putting_away'];
      if (inboundStartAllowed.indexOf(plan.status) === -1) {
        return { ok: false, error: "plan_status_invalid", message: "当前状态不可开始理货 / 현재 상태에서 입고 불가, current: " + plan.status };
      }
      if (plan.biz_class && biz_class && plan.biz_class !== biz_class) {
        return { ok: false, error: "biz_class_mismatch", message: "plan biz_class mismatch: plan=" + plan.biz_class + " req=" + biz_class };
      }
    }
    // ===== Path B: start from external WMS inbound number (standard only) =====
    else if (!plan_id && isStandard) {
      if (!external_inbound_no) return { ok: false, error: "missing plan_id or external_inbound_no" };
      if (!customer_name) return { ok: false, error: "missing customer_name for external inbound" };
      const dupPlan = await env.DB.prepare(
        "SELECT id FROM v2_inbound_plans WHERE source_type='external_inbound' AND external_inbound_no=? AND status='putting_away' ORDER BY created_at DESC LIMIT 1"
      ).bind(external_inbound_no).first();
      if (dupPlan) {
        plan_id = dupPlan.id;
      } else {
        plan_id = "IB-" + uid();
        const display_no = await nextDisplayNo(env, today);
        await env.DB.prepare(`
          INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, cargo_summary,
            expected_arrival, purpose, remark, status, created_by, created_at, updated_at,
            display_no, source_type, external_inbound_no)
          VALUES(?,?,?,?,'外部WMS入库单','','',?, 'putting_away',?,?,?,?,'external_inbound',?)
        `).bind(plan_id, today, customer_name, biz_class, start_remark, worker_id, t, t, display_no, external_inbound_no).run();
      }
    }
    // ===== Path C: return inbound lightweight session =====
    else if (isReturn) {
      if (!plan_id) {
        plan_id = "IB-" + uid();
        const display_no = await nextDisplayNo(env, today);
        await env.DB.prepare(`
          INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, cargo_summary,
            expected_arrival, purpose, remark, status, created_by, created_at, updated_at,
            display_no, source_type)
          VALUES(?,?,?,'return','退件入库会话','','',?, 'putting_away',?,?,?,?,'return_session')
        `).bind(plan_id, today, customer_name || '未指定', start_remark, worker_id, t, t, display_no).run();
      } else {
        const rp = await env.DB.prepare(
          "SELECT status, source_type, biz_class FROM v2_inbound_plans WHERE id=?"
        ).bind(plan_id).first();
        if (!rp) return { ok: false, error: "return session not found" };
        if (rp.status !== 'putting_away') return { ok: false, error: "return session status invalid: " + rp.status };
        if (rp.biz_class !== 'return') return { ok: false, error: "not a return session" };
      }
    } else {
      return { ok: false, error: "missing plan_id" };
    }

    // ===== Find / create job bound to plan_id =====
    let job = null;
    const existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type=? AND status IN ('pending','working') LIMIT 1"
    ).bind(plan_id, job_type).first();
    if (existing) job = existing;

    let job_id, is_new_job = false;
    if (job) {
      job_id = job.id;
      const dup = await findOpenSeg(env, job_id, worker_id);
      if (dup) return { ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true, plan_id };
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
      if (isStandard) {
        // Parallel: if unloading → unloading_putting_away; if arrived_pending_putaway → putting_away
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='unloading_putting_away', updated_at=? WHERE id=? AND status='unloading'"
        ).bind(t, plan_id).run();
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='putting_away', updated_at=? WHERE id=? AND status='arrived_pending_putaway'"
        ).bind(t, plan_id).run();
      }
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    return { ok: true, job_id, worker_seg_id: seg_id, is_new_job, plan_id };
  });
});

route("v2_inbound_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const complete_job = body.complete_job === true;
  const leave_only = body.leave_only === true;
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_inbound_job_finish", async () => {
    const t = now();
    const remark = String(body.remark || "");
    const result_note = String(body.result_note || "");
    const result_lines = Array.isArray(body.result_lines) ? body.result_lines : [];

    const rawExtra = body.extra_ops || {};
    const extra_ops = {
      sort_qty: Number(rawExtra.sort_qty || 0) || 0,
      label_qty: Number(rawExtra.label_qty || 0) || 0,
      repair_box_qty: Number(rawExtra.repair_box_qty || 0) || 0,
      other_op_remark: String(rawExtra.other_op_remark || "")
    };

    if (!leave_only) {
      const jobCheck = await env.DB.prepare("SELECT status, job_type FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
      if (jobCheck && jobCheck.status === 'completed') {
        return { ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" };
      }
    }

    const jobRow = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!jobRow) return { ok: false, error: "job not found" };
    const isReturnJob = (jobRow.job_type === 'inbound_return');

    if (complete_job && !leave_only && !isReturnJob) {
      if (jobRow.related_doc_id) {
        const planCheck = await env.DB.prepare("SELECT status FROM v2_inbound_plans WHERE id=?").bind(jobRow.related_doc_id).first();
        const inboundFinishAllowed = ['putting_away', 'unloading_putting_away'];
        if (planCheck && inboundFinishAllowed.indexOf(planCheck.status) === -1) {
          return { ok: false, error: "inbound_plan_status_invalid", message: "当前入库计划状态不允许完成入库（当前: " + planCheck.status + "）" };
        }
      }
    }

    await closeAllOpenSegs(env, job_id, worker_id, t, leave_only ? 'leave' : 'finished');
    const realCount = await recalcActiveCount(env, job_id, t);

    if (leave_only) {
      return { ok: true, left: true };
    }

    const resultData = isReturnJob
      ? { remark, result_note, result_lines: [], is_return: true }
      : { remark, result_note, result_lines, extra_ops };
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET shared_result_json=?, updated_at=? WHERE id=?"
    ).bind(JSON.stringify(resultData), t, job_id).run();

    if (complete_job) {
      if (realCount > 0) {
        return { ok: false, error: "others_still_working",
          message: "您已退出此任务，还有 " + realCount + " 人继续作业",
          active_worker_count: realCount };
      }

      const result_id = "RES-" + uid();
      await env.DB.prepare(
        "INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, result_lines_json, created_by, created_at) VALUES(?,?,0,0,?,?,?,?,?)"
      ).bind(result_id, job_id, remark, JSON.stringify(resultData),
             JSON.stringify(isReturnJob ? [] : result_lines), worker_id, t).run();

      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', updated_at=? WHERE id=?"
      ).bind(t, job_id).run();
      await env.DB.prepare(
        "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
      ).bind(t, job_id).run();

      if (!isReturnJob && jobRow.related_doc_id) {
        const pid = jobRow.related_doc_id;
        // Accumulate putaway_qty (not overwrite) — idempotent via result_id check
        for (const rl of result_lines) {
          if (rl && rl.unit_type && Number(rl.putaway_qty || 0) > 0) {
            await env.DB.prepare(
              "UPDATE v2_inbound_plan_lines SET putaway_qty = COALESCE(putaway_qty, 0) + ?, putaway_remark=? WHERE plan_id=? AND unit_type=?"
            ).bind(Number(rl.putaway_qty || 0), String(rl.putaway_remark || ""), pid, String(rl.unit_type)).run();
          }
        }

        // Determine next plan status based on parallel state
        const activeUnloadJob = await env.DB.prepare(
          "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status IN ('pending','working') LIMIT 1"
        ).bind(pid).first();
        const otherInboundJob = await env.DB.prepare(
          "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type IN ('inbound_direct','inbound_bulk') AND status IN ('pending','working') AND id!=? LIMIT 1"
        ).bind(pid, job_id).first();

        if (activeUnloadJob) {
          // Unload still running
          if (otherInboundJob) {
            // Other inbound workers still active → keep parallel
            await env.DB.prepare(
              "UPDATE v2_inbound_plans SET status='unloading_putting_away', updated_at=? WHERE id=?"
            ).bind(t, pid).run();
          } else {
            // No other inbound workers → back to unloading only
            await env.DB.prepare(
              "UPDATE v2_inbound_plans SET status='unloading', updated_at=? WHERE id=?"
            ).bind(t, pid).run();
          }
        } else {
          // Unload done — check if all putaway is complete
          const completion = await checkPlanFullyCompleted(env, pid);
          if (completion.allDone) {
            await env.DB.prepare(
              "UPDATE v2_inbound_plans SET status='completed', updated_at=? WHERE id=?"
            ).bind(t, pid).run();
          } else if (otherInboundJob) {
            // Still have other inbound workers
            await env.DB.prepare(
              "UPDATE v2_inbound_plans SET status='putting_away', updated_at=? WHERE id=?"
            ).bind(t, pid).run();
          } else {
            // No workers, not fully done
            await env.DB.prepare(
              "UPDATE v2_inbound_plans SET status='arrived_pending_putaway', updated_at=? WHERE id=?"
            ).bind(t, pid).run();
          }
        }
      }
      if (isReturnJob && jobRow.related_doc_id) {
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='completed', updated_at=? WHERE id=? AND source_type='return_session'"
        ).bind(t, jobRow.related_doc_id).run();
      }
      return { ok: true, result_id };
    }

    return { ok: true };
  });
});

// ===== Clerk direct mark completed =====
route("v2_inbound_mark_completed", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const plan_id = String(body.inbound_plan_id || "").trim();
  if (!plan_id) return err("missing inbound_plan_id");

  return withIdem(env, body, "v2_inbound_mark_completed", async () => {
    const operator = String(body.operator_name || "").trim();
    const remark = String(body.remark || "").trim();

    const plan = await env.DB.prepare("SELECT status FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
    if (!plan) return { ok: false, error: "plan not found" };
    const markCompletedAllowed = ['arrived_pending_putaway', 'putting_away'];
    if (markCompletedAllowed.indexOf(plan.status) === -1) {
      return { ok: false, error: "status_invalid", message: "only arrived_pending_putaway or putting_away can be marked completed, current: " + plan.status };
    }

    const activeJob = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(plan_id).first();
    if (activeJob) {
      return { ok: false, error: "inbound_job_still_active", message: "当前仍有进行中的入库任务，不能直接完结" };
    }

    const t = now();
    let updateSql = "UPDATE v2_inbound_plans SET status='completed', updated_at=?, manual_completed_by=?, manual_completed_at=?";
    const binds = [t, operator, t];
    if (remark) { updateSql += ", remark=?"; binds.push(remark); }
    updateSql += " WHERE id=?";
    binds.push(plan_id);
    await env.DB.prepare(updateSql).bind(...binds).run();

    return { ok: true, operator, completed_at: t };
  });
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

  return withIdem(env, body, "v2_ops_job_start", async () => {
    const t = now();

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
      const dup = await findOpenSeg(env, job_id, worker_id);
      if (dup) return { ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true };
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

    if (is_temporary_interrupt && parent_job_id) {
      await closeAllOpenSegs(env, parent_job_id, worker_id, t, 'interrupted');
      await recalcActiveCount(env, parent_job_id, t);
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    return { ok: true, job_id, worker_seg_id: seg_id, is_new_job };
  });
});

route("v2_ops_job_leave", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id || !worker_id) return err("missing job_id or worker_id");

  return withIdem(env, body, "v2_ops_job_leave", async () => {
    const t = now();
    await closeAllOpenSegs(env, job_id, worker_id, t, String(body.leave_reason || 'leave'));
    const realCount = await recalcActiveCount(env, job_id, t);

    if (realCount <= 0) {
      const job = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
      if (job && job.status === "working") {
        await env.DB.prepare(
          "UPDATE v2_ops_jobs SET status='awaiting_close', updated_at=? WHERE id=?"
        ).bind(t, job_id).run();
      }
    }

    return { ok: true };
  });
});

route("v2_ops_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_ops_job_finish", async () => {
    const t = now();

    const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (jobCheck && jobCheck.status === 'completed') {
      return { ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" };
    }

    await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');

    const shared = body.shared_result || {};
    if (Object.keys(shared).length > 0) {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET shared_result_json=?, updated_at=? WHERE id=?"
      ).bind(JSON.stringify(shared), t, job_id).run();
    }

    if (body.box_count != null || body.pallet_count != null || body.remark) {
      const result_id = "RES-" + uid();
      await env.DB.prepare(`
        INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, created_by, created_at)
        VALUES(?,?,?,?,?,?,?,?)
      `).bind(result_id, job_id, Number(body.box_count || 0), Number(body.pallet_count || 0),
          String(body.remark || ""), JSON.stringify(shared), worker_id, t).run();
    }

    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(t, job_id).run();
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
    ).bind(t, job_id).run();

    return { ok: true };
  });
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

  return withIdem(env, body, "v2_ops_job_resume", async () => {
    const t = now();
    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(parent_job_id).first();
    if (!job) return { ok: false, error: "parent job not found" };

    const dup = await findOpenSeg(env, parent_job_id, worker_id);
    if (dup) {
      if (job.status === "awaiting_close") {
        await recalcActiveCount(env, parent_job_id, t);
        const rc = await env.DB.prepare("SELECT active_worker_count as c FROM v2_ops_jobs WHERE id=?").bind(parent_job_id).first();
        if (rc && rc.c > 0) {
          await env.DB.prepare("UPDATE v2_ops_jobs SET status='working', resumed_at=?, updated_at=? WHERE id=?").bind(t, t, parent_job_id).run();
        }
      }
      return { ok: true, worker_seg_id: dup.id, already_joined: true };
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, parent_job_id, worker_id, worker_name, t).run();

    const realCount = await recalcActiveCount(env, parent_job_id, t);
    if (job.status === "awaiting_close" && realCount > 0) {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='working', resumed_at=?, updated_at=? WHERE id=?"
      ).bind(t, t, parent_job_id).run();
    }

    return { ok: true, worker_seg_id: seg_id };
  });
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
  // Parse result_lines from feedback itself (unplanned_unload flow)
  let feedbackResultLines = [];
  try { feedbackResultLines = JSON.parse(row.result_lines_json || "[]"); } catch(e) {}
  return json({ ok: true, feedback: row, job_results: jobResults, feedback_result_lines: feedbackResultLines });
});

// ===== [DEPRECATED] Generic feedback-to-inbound conversion =====
// New flow for unplanned_unload feedbacks must use v2_feedback_finalize_to_inbound.
// This route is kept only for backward compatibility with old open/unload_no_doc feedback data.
route("v2_feedback_convert_to_inbound", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const feedback_id = String(body.feedback_id || "").trim();
  if (!feedback_id) return err("missing feedback_id");

  const fb = await env.DB.prepare("SELECT * FROM v2_field_feedbacks WHERE id=?").bind(feedback_id).first();
  if (!fb) return err("feedback not found", 404);

  // Block unplanned_unload — must use v2_feedback_finalize_to_inbound
  if (fb.feedback_type === 'unplanned_unload') {
    return err("unplanned_unload feedbacks must use v2_feedback_finalize_to_inbound instead");
  }

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
// PICK DIRECT — 代发拣货
// =====================================================
route("v2_pick_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!worker_id) return err("missing worker_id");

  // pick_doc_nos: array of pick document numbers
  let pick_doc_nos = body.pick_doc_nos || [];
  if (typeof pick_doc_nos === 'string') {
    pick_doc_nos = pick_doc_nos.split(',').map(s => s.trim()).filter(Boolean);
  }
  if (pick_doc_nos.length === 0) return err("missing pick_doc_nos");

  return withIdem(env, body, "v2_pick_job_start", async () => {
    const t = now();

    // Cross-trip worker exclusion: same worker cannot be in 2 active pick trips
    const workerConflict = await env.DB.prepare(
      `SELECT j.id, j.display_no FROM v2_ops_jobs j
       JOIN v2_ops_job_workers w ON w.job_id = j.id
       WHERE j.job_type='pick_direct' AND j.status IN ('pending','working')
       AND j.is_temporary_interrupt=0 AND w.worker_id=? AND w.left_at=''
       LIMIT 1`
    ).bind(worker_id).first();
    if (workerConflict) {
      return { ok: false, error: "worker_already_in_pick_trip",
        message: "您已在趟次 " + (workerConflict.display_no || workerConflict.id) + " 中，请先完成后再开始新趟次",
        conflict_trip: workerConflict.display_no || workerConflict.id };
    }

    // Conflict check: reject if any doc_no is already in an active trip
    for (const docNo of pick_doc_nos) {
      const conflict = await env.DB.prepare(
        `SELECT j.id, j.display_no FROM v2_ops_jobs j
         JOIN v2_ops_job_pick_docs pd ON pd.job_id = j.id
         WHERE j.job_type='pick_direct' AND j.status IN ('pending','working')
         AND j.is_temporary_interrupt=0 AND pd.pick_doc_no=?
         LIMIT 1`
      ).bind(docNo).first();
      if (conflict) {
        return { ok: false, error: "doc_conflict",
          message: "拣货单 " + docNo + " 已在活跃趟次 " + (conflict.display_no || conflict.id) + " 中",
          conflict_doc_no: docNo, conflict_trip: conflict.display_no || conflict.id };
      }
    }

    // Generate trip number
    const trip_no = await nextPickTripNo(env);
    const job_id = "JOB-" + uid();

    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, parent_job_id, is_temporary_interrupt, interrupt_type, created_by, created_at, updated_at,
        active_worker_count, display_no)
      VALUES(?,'order_op','direct_ship','pick_direct','','','working','',0,'',?,?,?,1,?)
    `).bind(job_id, worker_id, t, t, trip_no).run();

    // Insert pick doc nos
    for (const docNo of pick_doc_nos) {
      await env.DB.prepare(
        "INSERT INTO v2_ops_job_pick_docs(id, job_id, pick_doc_no, created_at) VALUES(?,?,?,?)"
      ).bind("PD-" + uid(), job_id, docNo, t).run();
    }

    // Add creator as first worker
    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    return { ok: true, job_id, worker_seg_id: seg_id, trip_no, is_new_job: true };
  });
});

route("v2_pick_job_docs_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  if (!job_id) return err("missing job_id");
  const allDocs = await env.DB.prepare(
    "SELECT pick_doc_no, created_at FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at"
  ).bind(job_id).all();
  return json({ ok: true, docs: allDocs.results || [] });
});

route("v2_pick_job_add_docs", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  if (!job_id) return err("missing job_id");

  let pick_doc_nos = body.pick_doc_nos || [];
  if (typeof pick_doc_nos === 'string') {
    pick_doc_nos = pick_doc_nos.split(',').map(s => s.trim()).filter(Boolean);
  }
  if (pick_doc_nos.length === 0) return err("missing pick_doc_nos");

  return withIdem(env, body, "v2_pick_job_add_docs", async () => {
    const t = now();

    // Conflict check: reject if any doc_no is in another active trip
    for (const docNo of pick_doc_nos) {
      const conflict = await env.DB.prepare(
        `SELECT j.id, j.display_no FROM v2_ops_jobs j
         JOIN v2_ops_job_pick_docs pd ON pd.job_id = j.id
         WHERE j.job_type='pick_direct' AND j.status IN ('pending','working')
         AND j.is_temporary_interrupt=0 AND pd.pick_doc_no=? AND j.id!=?
         LIMIT 1`
      ).bind(docNo, job_id).first();
      if (conflict) {
        return { ok: false, error: "doc_conflict",
          message: "拣货单 " + docNo + " 已在趟次 " + (conflict.display_no || conflict.id) + " 中",
          conflict_doc_no: docNo, conflict_trip: conflict.display_no || conflict.id };
      }
    }

    const existingDocs = await env.DB.prepare(
      "SELECT pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id=?"
    ).bind(job_id).all();
    const existingSet = new Set((existingDocs.results || []).map(r => r.pick_doc_no));
    let added = 0;
    for (const docNo of pick_doc_nos) {
      if (!existingSet.has(docNo)) {
        await env.DB.prepare(
          "INSERT INTO v2_ops_job_pick_docs(id, job_id, pick_doc_no, created_at) VALUES(?,?,?,?)"
        ).bind("PD-" + uid(), job_id, docNo, t).run();
        added++;
      }
    }
    // Return all docs for this job
    const allDocs = await env.DB.prepare(
      "SELECT pick_doc_no, created_at FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at"
    ).bind(job_id).all();
    return { ok: true, added, docs: allDocs.results || [] };
  });
});

route("v2_pick_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_pick_job_finish", async () => {
    const t = now();

    // Idempotency fallback: already completed
    const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (jobCheck && jobCheck.status === 'completed') {
      return { ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" };
    }

    // 1. Close this worker's segments only
    await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');

    // 2. Recalc active count
    const realCount = await recalcActiveCount(env, job_id, t);

    // 3. If others still working, block completion
    if (realCount > 0) {
      return { ok: false, error: "others_still_working",
        message: "您已退出此趟次，还有 " + realCount + " 人继续作业",
        active_worker_count: realCount };
    }

    // 4. Last person — save result and complete
    const remark = String(body.remark || "").trim();
    const result_note = String(body.result_note || "").trim();

    const allDocs = await env.DB.prepare(
      "SELECT pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at"
    ).bind(job_id).all();
    const docNos = (allDocs.results || []).map(r => r.pick_doc_no);

    const result_id = "RES-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, remark, result_json, result_lines_json, created_by, created_at)
      VALUES(?,?,?,?,?,?,?)
    `).bind(result_id, job_id, remark, JSON.stringify({
      result_note,
      pick_doc_nos: docNos
    }), '[]', worker_id, t).run();

    // Complete job — close any stale open segments (safety net)
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(t, job_id).run();
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
    ).bind(t, job_id).run();

    return { ok: true };
  });
});

// =====================================================
// PICK DIRECT — 活跃趟次列表
// =====================================================
route("v2_pick_job_active_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);

  const rs = await env.DB.prepare(
    `SELECT * FROM v2_ops_jobs
     WHERE job_type='pick_direct' AND status IN ('pending','working')
     AND is_temporary_interrupt=0
     ORDER BY created_at DESC LIMIT 50`
  ).all();

  const items = [];
  for (const job of (rs.results || [])) {
    const pds = await env.DB.prepare(
      "SELECT pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at"
    ).bind(job.id).all();
    const workers = await env.DB.prepare(
      "SELECT worker_id, worker_name FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
    ).bind(job.id).all();
    items.push({
      id: job.id,
      display_no: job.display_no || '',
      status: job.status,
      active_worker_count: job.active_worker_count || 0,
      created_by: job.created_by || '',
      created_at: job.created_at,
      pick_doc_nos: (pds.results || []).map(r => r.pick_doc_no),
      workers: (workers.results || []).map(w => ({ id: w.worker_id, name: w.worker_name }))
    });
  }
  return json({ ok: true, items });
});

// =====================================================
// PICK DIRECT — 加入已有趟次
// =====================================================
route("v2_pick_job_join", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!job_id || !worker_id) return err("missing job_id or worker_id");

  return withIdem(env, body, "v2_pick_job_join", async () => {
    const t = now();
    const job = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE id=? AND job_type='pick_direct'"
    ).bind(job_id).first();
    if (!job) return { ok: false, error: "trip not found" };
    if (job.status === 'completed') return { ok: false, error: "trip_already_completed" };

    // Dedup: already has open segment in this same trip
    const dup = await findOpenSeg(env, job_id, worker_id);
    if (dup) {
      return { ok: true, job_id, worker_seg_id: dup.id, already_joined: true,
        trip_no: job.display_no || '' };
    }

    // Cross-trip worker exclusion: same worker cannot be in 2 active pick trips
    const workerConflict = await env.DB.prepare(
      `SELECT j.id, j.display_no FROM v2_ops_jobs j
       JOIN v2_ops_job_workers w ON w.job_id = j.id
       WHERE j.job_type='pick_direct' AND j.status IN ('pending','working')
       AND j.is_temporary_interrupt=0 AND w.worker_id=? AND w.left_at='' AND j.id!=?
       LIMIT 1`
    ).bind(worker_id, job_id).first();
    if (workerConflict) {
      return { ok: false, error: "worker_already_in_pick_trip",
        message: "您已在趟次 " + (workerConflict.display_no || workerConflict.id) + " 中，请先完成后再加入其他趟次",
        conflict_trip: workerConflict.display_no || workerConflict.id };
    }

    // Add worker segment
    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, updated_at=?, status='working' WHERE id=?"
    ).bind(t, job_id).run();

    return { ok: true, job_id, worker_seg_id: seg_id, trip_no: job.display_no || '' };
  });
});

// =====================================================
// BULK OP — 大货操作
// =====================================================
route("v2_bulk_op_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const work_order_no = String(body.work_order_no || "").trim();
  if (!worker_id) return err("missing worker_id");
  if (!work_order_no) return err("missing work_order_no");

  return withIdem(env, body, "v2_bulk_op_job_start", async () => {
    const t = now();

    // Try to join existing bulk_op job for same work_order_no
    let job = null;
    if (work_order_no) {
      const existing = await env.DB.prepare(
        "SELECT * FROM v2_ops_jobs WHERE job_type='bulk_op' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') AND is_temporary_interrupt=0 LIMIT 1"
      ).bind(work_order_no).first();
      if (existing) job = existing;
    }

    // Cross-job guard: worker must not be active in ANOTHER bulk_op job
    const targetJobId = job ? job.id : null;
    const otherActive = await env.DB.prepare(
      `SELECT j.id, j.related_doc_id FROM v2_ops_job_workers w
       JOIN v2_ops_jobs j ON j.id = w.job_id
       WHERE w.worker_id=? AND w.left_at=''
       AND j.job_type='bulk_op'
       AND j.status IN ('pending','working','awaiting_close')
       LIMIT 5`
    ).bind(worker_id).all();
    const otherRows = (otherActive && otherActive.results) || [];
    const blocking = otherRows.find(r => r.id !== targetJobId);
    if (blocking) {
      return { ok: false, error: "worker_already_in_other_bulk_job",
        message: "当前已在其他大货工单作业中，请先退出或完成当前工单",
        other_job_id: blocking.id,
        other_work_order_no: blocking.related_doc_id || "" };
    }

    let job_id, is_new_job = false;
    if (job) {
      job_id = job.id;
      const dup = await findOpenSeg(env, job_id, worker_id);
      if (dup) return { ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true };
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, updated_at=?, status='working' WHERE id=?"
      ).bind(t, job_id).run();
    } else {
      job_id = "JOB-" + uid();
      is_new_job = true;
      await env.DB.prepare(`
        INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
          status, parent_job_id, is_temporary_interrupt, interrupt_type, created_by, created_at, updated_at, active_worker_count)
        VALUES(?,'order_op','bulk','bulk_op','work_order',?,'working','',0,'',?,?,?,1)
      `).bind(job_id, work_order_no, worker_id, t, t).run();
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    // 口径联动：若 work_order_no 匹配某出库单（id 或 wms_work_order_no），返回该出库单参考信息
    const linkedOb = await findOutboundByWorkOrder(env, work_order_no);
    const ret = { ok: true, job_id, worker_seg_id: seg_id, is_new_job };
    if (linkedOb) {
      ret.linked_outbound = {
        id: linkedOb.id,
        display_no: linkedOb.display_no || linkedOb.id,
        customer: linkedOb.customer || "",
        destination: linkedOb.destination || "",
        po_no: linkedOb.po_no || "",
        wms_work_order_no: linkedOb.wms_work_order_no || "",
        planned_box_count: Number(linkedOb.planned_box_count || 0),
        planned_pallet_count: Number(linkedOb.planned_pallet_count || 0),
        instruction: linkedOb.instruction || ""
      };
    }
    return ret;
  });
});

// 出库单查找 helper：优先匹配 id，其次匹配 wms_work_order_no
async function findOutboundByWorkOrder(env, workOrderNo) {
  if (!workOrderNo) return null;
  let row = await env.DB.prepare(
    "SELECT * FROM v2_outbound_orders WHERE id=? LIMIT 1"
  ).bind(workOrderNo).first();
  if (row) return row;
  row = await env.DB.prepare(
    "SELECT * FROM v2_outbound_orders WHERE wms_work_order_no=? ORDER BY created_at DESC LIMIT 1"
  ).bind(workOrderNo).first();
  return row || null;
}

route("v2_bulk_op_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_bulk_op_job_finish", async () => {
    const t = now();

    const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (jobCheck && jobCheck.status === 'completed') {
      return { ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" };
    }

    // Pre-check: if this user is the only active worker, they MUST record output
    const othersRow = await env.DB.prepare(
      "SELECT COUNT(*) as c FROM v2_ops_job_workers WHERE job_id=? AND worker_id!=? AND left_at=''"
    ).bind(job_id, worker_id).first();
    const willBeLastPerson = (Number((othersRow && othersRow.c) || 0) === 0);

    if (willBeLastPerson) {
      const numFields = [
        Number(body.packed_sku_count || 0),
        Number(body.packed_box_count || 0),
        Number(body.used_carton_large_count || 0),
        Number(body.used_carton_small_count || 0),
        Number(body.repaired_box_count || 0),
        Number(body.reboxed_count || 0),
        Number(body.label_count || 0),
        Number(body.total_operated_box_count || 0),
        Number(body.pallet_count || 0),
        Number(body.forklift_location_count || 0)
      ];
      const hasOutput = numFields.some(v => v > 0) || !!body.used_forklift;
      if (!hasOutput) {
        return { ok: false, error: "missing_bulk_output",
          message: "请先记录操作产出后再完成" };
      }
    }

    // 1. Close this worker's segments only
    await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');

    // 2. Recalc active count
    const realCount = await recalcActiveCount(env, job_id, t);

    // 3. If others still working, this worker has been kicked out
    if (realCount > 0) {
      return { ok: false, error: "others_still_working",
        message: "您已退出此工单，还有 " + realCount + " 人继续作业",
        active_worker_count: realCount };
    }

    // 4. Last person — save result and complete
    const resultData = {
      packed_sku_count: Number(body.packed_sku_count || 0),
      packed_box_count: Number(body.packed_box_count || 0),
      used_carton_large_count: Number(body.used_carton_large_count || 0),
      used_carton_small_count: Number(body.used_carton_small_count || 0),
      repaired_box_count: Number(body.repaired_box_count || 0),
      reboxed_count: Number(body.reboxed_count || 0),
      label_count: Number(body.label_count || 0),
      total_operated_box_count: Number(body.total_operated_box_count || 0),
      pallet_count: Number(body.pallet_count || 0),
      used_forklift: body.used_forklift ? 1 : 0,
      forklift_location_count: Number(body.forklift_location_count || 0),
      result_note: String(body.result_note || "")
    };

    const result_id = "RES-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, remark, result_json, result_lines_json, created_by, created_at)
      VALUES(?,?,?,?,?,?,?)
    `).bind(result_id, job_id, String(body.remark || ""), JSON.stringify(resultData), '[]', worker_id, t).run();

    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(t, job_id).run();
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
    ).bind(t, job_id).run();

    // 口径联动：大货操作完成 → 回写关联出库单的 actual_box_count / actual_pallet_count
    const finishedJob = await env.DB.prepare("SELECT related_doc_id FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (finishedJob && finishedJob.related_doc_id) {
      const linkedOb = await findOutboundByWorkOrder(env, finishedJob.related_doc_id);
      if (linkedOb) {
        await env.DB.prepare(
          "UPDATE v2_outbound_orders SET actual_box_count=?, actual_pallet_count=?, status='completed', updated_at=? WHERE id=?"
        ).bind(resultData.total_operated_box_count, resultData.pallet_count, t, linkedOb.id).run();
      }
    }

    return { ok: true };
  });
});

// =====================================================
// ORDER OPS — 按单操作 job 列表查询（协同中心/看板用）
// =====================================================
route("v2_order_ops_job_list", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const start = String(body.start_date || "").trim();
  const end = String(body.end_date || "").trim();
  const job_type = String(body.job_type || "").trim();

  let sql = "SELECT * FROM v2_ops_jobs WHERE flow_stage='order_op'";
  const binds = [];
  if (job_type) { sql += " AND job_type=?"; binds.push(job_type); }
  if (start) { sql += " AND created_at>=?"; binds.push(start + "T00:00:00.000Z"); }
  if (end) { sql += " AND created_at<=?"; binds.push(end + "T23:59:59.999Z"); }
  sql += " ORDER BY created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();

  // Enrich with worker names, pick docs, result
  const items = [];
  for (const job of (rs.results || [])) {
    const workers = await env.DB.prepare(
      "SELECT worker_name, minutes_worked, left_at FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at"
    ).bind(job.id).all();
    const workerRows = workers.results || [];
    const names = [...new Set(workerRows.map(w => w.worker_name).filter(Boolean))];
    const totalMin = workerRows.reduce((s, w) => s + (Number(w.minutes_worked) || 0), 0);

    // Pick docs (if pick_direct)
    let pickDocs = [];
    if (job.job_type === 'pick_direct') {
      const pds = await env.DB.prepare(
        "SELECT pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at"
      ).bind(job.id).all();
      pickDocs = (pds.results || []).map(r => r.pick_doc_no);
    }

    // Latest result
    const latestResult = await env.DB.prepare(
      "SELECT remark, result_json, created_at FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at DESC LIMIT 1"
    ).bind(job.id).first();

    let resultData = null, remark = "";
    if (latestResult) {
      remark = latestResult.remark || "";
      try { resultData = JSON.parse(latestResult.result_json); } catch(e) {}
    }

    items.push({
      ...job,
      worker_names: names,
      worker_names_text: names.join(", "),
      total_minutes_worked: Math.round(totalMin),
      pick_doc_nos: pickDocs,
      result_data: resultData,
      result_remark: remark
    });
  }

  return json({ ok: true, items });
});

// =====================================================
// OPS LOGIN EVENT — 现场系统登录事件记录
// =====================================================
route("v2_ops_login_mark", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!worker_id) return err("missing worker_id");

  const t = now();
  const login_date = kstToday();
  const id = "LOGIN-" + uid();
  await env.DB.prepare(`
    INSERT INTO v2_ops_login_events(id, worker_id, worker_name, login_at, login_date, page_source, device_info)
    VALUES(?,?,?,?,?,?,?)
  `).bind(id, worker_id, worker_name, t, login_date,
      String(body.page_source || ""), String(body.device_info || "")).run();
  return json({ ok: true, id });
});

// =====================================================
// DASHBOARD — 仓库数据看板接口
// =====================================================
route("v2_dashboard_realtime_overview", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const today = kstToday();

  // 1. 当前在岗人数 = distinct worker_id with open segments (left_at='')
  const activeWorkers = await env.DB.prepare(
    "SELECT COUNT(DISTINCT worker_id) as c FROM v2_ops_job_workers WHERE left_at=''"
  ).first();

  // 2. 今日上岗人数 = distinct worker_id from login events today
  const todayLogins = await env.DB.prepare(
    "SELECT COUNT(DISTINCT worker_id) as c FROM v2_ops_login_events WHERE login_date=?"
  ).bind(today).first();

  // 3. 当前活跃任务数 = working/awaiting_close jobs with active_worker_count > 0
  const activeJobs = await env.DB.prepare(
    "SELECT COUNT(*) as c FROM v2_ops_jobs WHERE status IN ('working','awaiting_close') AND active_worker_count > 0"
  ).first();

  // 4. 当前活跃单数 = distinct related_doc_id from active jobs (non-empty)
  const activeDocs = await env.DB.prepare(
    "SELECT COUNT(DISTINCT related_doc_id) as c FROM v2_ops_jobs WHERE status IN ('working','awaiting_close') AND active_worker_count > 0 AND related_doc_id != ''"
  ).first();

  // 5. Worker live status — each open segment joined with its job + best display_no
  const liveWorkers = await env.DB.prepare(`
    SELECT w.worker_id, w.worker_name, w.joined_at, w.job_id,
           j.flow_stage, j.biz_class, j.job_type, j.related_doc_type, j.related_doc_id,
           j.display_no as job_display_no, j.status as job_status,
           p.display_no as plan_display_no
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON w.job_id = j.id
    LEFT JOIN v2_inbound_plans p ON j.related_doc_type='inbound_plan' AND j.related_doc_id = p.id
    WHERE w.left_at=''
    ORDER BY w.joined_at DESC
  `).all();
  // Inject unified display_no: priority = plan_display_no > job_display_no > related_doc_id > job_id
  const liveWorkerRows = (liveWorkers.results || []).map(function(r) {
    r.display_no = r.plan_display_no || r.job_display_no || r.related_doc_id || r.job_id || '';
    return r;
  });

  // 6. Biz breakdown — group active workers by job_type
  const bizBreak = await env.DB.prepare(`
    SELECT j.job_type, j.flow_stage,
           COUNT(DISTINCT w.worker_id) as worker_count,
           COUNT(DISTINCT j.id) as job_count
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON w.job_id = j.id
    WHERE w.left_at='' AND j.status IN ('working','awaiting_close')
    GROUP BY j.job_type, j.flow_stage
    ORDER BY worker_count DESC
  `).all();

  return json({
    ok: true,
    current_active_workers: (activeWorkers && activeWorkers.c) || 0,
    today_login_workers: (todayLogins && todayLogins.c) || 0,
    current_active_jobs: (activeJobs && activeJobs.c) || 0,
    current_active_docs: (activeDocs && activeDocs.c) || 0,
    worker_live_status: liveWorkerRows,
    biz_breakdown: bizBreak.results || []
  });
});

route("v2_dashboard_live_docs", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);

  // Active jobs that have related_doc_id, grouped by doc
  const jobs = await env.DB.prepare(`
    SELECT j.id as job_id, j.flow_stage, j.biz_class, j.job_type,
           j.related_doc_type, j.related_doc_id, j.status, j.created_at, j.active_worker_count,
           j.display_no as job_display_no,
           p.display_no as plan_display_no
    FROM v2_ops_jobs j
    LEFT JOIN v2_inbound_plans p ON j.related_doc_type='inbound_plan' AND j.related_doc_id = p.id
    WHERE j.status IN ('working','awaiting_close') AND j.active_worker_count > 0
    ORDER BY j.created_at DESC
    LIMIT 100
  `).all();

  const docs = [];
  for (const job of (jobs.results || [])) {
    // Get current worker names
    const ws = await env.DB.prepare(
      "SELECT DISTINCT worker_name FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
    ).bind(job.job_id).all();
    const names = (ws.results || []).map(function(r) { return r.worker_name; }).filter(Boolean);

    // Unified display_no: plan_display_no > job_display_no > related_doc_id > job_id
    const display_no = job.plan_display_no || job.job_display_no || job.related_doc_id || job.job_id || '';

    docs.push({
      job_id: job.job_id,
      flow_stage: job.flow_stage,
      biz_class: job.biz_class,
      job_type: job.job_type,
      related_doc_type: job.related_doc_type,
      related_doc_id: job.related_doc_id,
      display_no: display_no,
      status: job.status,
      created_at: job.created_at,
      active_worker_count: job.active_worker_count,
      worker_names: names.join(", ")
    });
  }

  return json({ ok: true, docs });
});

// =====================================================
// ADMIN — 脏数据诊断 + 清理
// =====================================================
route("v2_admin_dirty_data_diagnose", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);

  // 1. Worker with multiple open segments
  const multiSegs = await env.DB.prepare(`
    SELECT worker_id, worker_name, COUNT(*) as seg_count,
           GROUP_CONCAT(job_id) as job_ids
    FROM v2_ops_job_workers
    WHERE left_at=''
    GROUP BY worker_id
    HAVING seg_count > 1
  `).all();

  // 2. Same worker in multiple active jobs of same non-parallel type (bulk_op, inbound, unload)
  // pick_direct is parallel — allow multiple trips under legacy data but flagged via cross-trip check
  const crossJob = await env.DB.prepare(`
    SELECT w.worker_id, w.worker_name, j.job_type, COUNT(DISTINCT j.id) as job_count,
           GROUP_CONCAT(DISTINCT j.id) as job_ids
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON w.job_id = j.id
    WHERE w.left_at='' AND j.status IN ('working','awaiting_close','pending')
      AND j.job_type IN ('bulk_op','inbound_direct','inbound_return','unload')
    GROUP BY w.worker_id, j.job_type
    HAVING job_count > 1
  `).all();

  // 3. Open segments on completed/cancelled jobs
  const orphanSegs = await env.DB.prepare(`
    SELECT w.id as seg_id, w.worker_id, w.worker_name, w.job_id, w.joined_at, j.status as job_status
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON w.job_id = j.id
    WHERE w.left_at='' AND j.status IN ('completed','cancelled')
    ORDER BY w.joined_at DESC LIMIT 200
  `).all();

  // 4. Jobs with active_worker_count > 0 but no open segments (count drift)
  const countDrift = await env.DB.prepare(`
    SELECT j.id as job_id, j.job_type, j.status, j.active_worker_count,
           (SELECT COUNT(*) FROM v2_ops_job_workers w WHERE w.job_id=j.id AND w.left_at='') as real_count
    FROM v2_ops_jobs j
    WHERE j.active_worker_count > 0 AND j.status IN ('working','awaiting_close')
  `).all();
  const drifts = (countDrift.results || []).filter(function(r) { return r.real_count !== r.active_worker_count; });

  return json({
    ok: true,
    multi_open_segments: multiSegs.results || [],
    cross_job_workers: crossJob.results || [],
    orphan_open_segments: orphanSegs.results || [],
    count_drift_jobs: drifts
  });
});

route("v2_admin_dirty_data_cleanup", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const operator = String(body.operator || "").trim();
  const reason = String(body.reason || "").trim();
  const action_type = String(body.action_type || "").trim();
  if (!operator) return err("missing operator");
  if (!reason) return err("missing reason");
  if (!action_type) return err("missing action_type");

  return withIdem(env, body, "v2_admin_dirty_data_cleanup", async () => {
    const t = now();
    const log_id = "CLN-" + uid();
    const detail = {};

    if (action_type === "close_orphan_segment") {
      const seg_id = String(body.seg_id || "").trim();
      if (!seg_id) return { ok: false, error: "missing seg_id" };
      const seg = await env.DB.prepare("SELECT * FROM v2_ops_job_workers WHERE id=?").bind(seg_id).first();
      if (!seg) return { ok: false, error: "segment not found" };
      await env.DB.prepare(
        "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='admin_cleanup' WHERE id=?"
      ).bind(t, seg_id).run();
      await recalcActiveCount(env, seg.job_id, t);
      detail.seg = seg;
    } else if (action_type === "recalc_job_count") {
      const job_id = String(body.job_id || "").trim();
      if (!job_id) return { ok: false, error: "missing job_id" };
      const rc = await recalcActiveCount(env, job_id, t);
      detail.job_id = job_id;
      detail.recalc_result = rc;
    } else if (action_type === "close_worker_all_open") {
      const worker_id = String(body.worker_id || "").trim();
      if (!worker_id) return { ok: false, error: "missing worker_id" };
      const segs = await env.DB.prepare(
        "SELECT id, job_id FROM v2_ops_job_workers WHERE worker_id=? AND left_at=''"
      ).bind(worker_id).all();
      const rows = segs.results || [];
      for (const s of rows) {
        await env.DB.prepare(
          "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='admin_cleanup' WHERE id=?"
        ).bind(t, s.id).run();
        await recalcActiveCount(env, s.job_id, t);
      }
      detail.closed_count = rows.length;
      detail.worker_id = worker_id;
    } else {
      return { ok: false, error: "unknown action_type" };
    }

    await env.DB.prepare(`
      INSERT INTO v2_admin_cleanup_logs(id, operator, action_type, target_job_id, target_worker_id, reason, detail_json, created_at)
      VALUES(?,?,?,?,?,?,?,?)
    `).bind(log_id, operator, action_type,
        String(body.job_id || ""), String(body.worker_id || ""),
        reason, JSON.stringify(detail), t).run();

    return { ok: true, log_id, detail };
  });
});

route("v2_admin_cleanup_log_list", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const rs = await env.DB.prepare(
    "SELECT * FROM v2_admin_cleanup_logs ORDER BY created_at DESC LIMIT 100"
  ).all();
  return json({ ok: true, items: rs.results || [] });
});

// =====================================================
// CORRECTION REQUESTS — 看板主管修正申请（不直接改业务数据）
// =====================================================
route("v2_correction_request_create", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const type = String(body.type || "").trim();
  const target_id = String(body.target_id || "").trim();
  const reporter = String(body.reporter || "").trim();
  const reason = String(body.reason || "").trim();
  if (!type || !target_id) return err("missing type or target_id");
  if (!reason) return err("missing reason");

  return withIdem(env, body, "v2_correction_request_create", async () => {
    const t = now();
    const id = "CR-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_correction_requests(id, type, target_id, target_label, reporter, reason, status, created_at, updated_at)
      VALUES(?,?,?,?,?,?,'open',?,?)
    `).bind(id, type, target_id, String(body.target_label || ""), reporter, reason, t, t).run();
    return { ok: true, id };
  });
});

route("v2_correction_request_list", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const status = String(body.status || "").trim();
  let sql = "SELECT * FROM v2_correction_requests";
  const binds = [];
  if (status) { sql += " WHERE status=?"; binds.push(status); }
  sql += " ORDER BY created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
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
