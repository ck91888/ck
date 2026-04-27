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

function round1(n) {
  n = Number(n);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 10) / 10;
}

function kstDateOf(iso) {
  if (!iso) return '';
  const t = new Date(iso).getTime();
  if (isNaN(t)) return '';
  return new Date(t + 9 * 3600 * 1000).toISOString().slice(0, 10);
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

// 列表分页参数：默认 limit=50，最大 200；offset 默认 0
function pageParams(body) {
  let limit = parseInt(body && body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 50;
  if (limit > 200) limit = 200;
  let offset = parseInt(body && body.offset, 10);
  if (!Number.isFinite(offset) || offset < 0) offset = 0;
  return { limit, offset };
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

async function checkWorkerBusy(env, workerId, allowJobId) {
  const seg = await env.DB.prepare(
    "SELECT w.job_id, j.job_type, j.flow_stage FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.worker_id=? AND w.left_at='' AND j.status IN ('pending','working','awaiting_close') ORDER BY w.joined_at DESC LIMIT 1"
  ).bind(workerId).first();
  if (!seg) return null;
  if (allowJobId && seg.job_id === allowJobId) return null;
  return seg;
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
    status TEXT DEFAULT 'pending_issue',
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

  // ---- 强关联：bulk_op job → 出库单主键 ----
  `ALTER TABLE v2_ops_jobs ADD COLUMN linked_outbound_order_id TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_jobs_linked_ob ON v2_ops_jobs(linked_outbound_order_id) WHERE linked_outbound_order_id != ''`,

  // ---- 按单操作列表查询优化索引 ----
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_jobs_flow_created ON v2_ops_jobs(flow_stage, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_jobs_flow_type_created ON v2_ops_jobs(flow_stage, job_type, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_results_job_created ON v2_ops_job_results(job_id, created_at)`,

  // ---- issue rework_note 字段 ----
  `ALTER TABLE v2_issue_tickets ADD COLUMN rework_note TEXT DEFAULT ''`,

  // ---- 核对中心：扫码核对批次（客服上传，不含托盘号） ----
  `CREATE TABLE IF NOT EXISTS v2_verify_batches (
    id TEXT PRIMARY KEY,
    batch_no TEXT DEFAULT '',
    customer_name TEXT DEFAULT '',
    planned_qty INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    remark TEXT DEFAULT '',
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT,
    completed_at TEXT DEFAULT '',
    completed_by TEXT DEFAULT '',
    cancelled_at TEXT DEFAULT '',
    cancelled_by TEXT DEFAULT ''
  )`,

  // ---- 核对中心：批次内计划条码（不含托盘号） ----
  `CREATE TABLE IF NOT EXISTS v2_verify_batch_items (
    id TEXT PRIMARY KEY,
    batch_id TEXT DEFAULT '',
    barcode TEXT DEFAULT '',
    planned_qty INTEGER DEFAULT 1,
    created_at TEXT
  )`,

  // ---- 核对中心：现场扫码流水（托盘号仅出现在扫码记录） ----
  `CREATE TABLE IF NOT EXISTS v2_verify_scan_logs (
    id TEXT PRIMARY KEY,
    batch_id TEXT DEFAULT '',
    job_id TEXT DEFAULT '',
    worker_id TEXT DEFAULT '',
    worker_name TEXT DEFAULT '',
    pallet_no TEXT DEFAULT '',
    barcode TEXT DEFAULT '',
    scan_result TEXT DEFAULT '',
    message TEXT DEFAULT '',
    scanned_at TEXT
  )`,

  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_verify_batch_no ON v2_verify_batches(batch_no) WHERE batch_no != ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_verify_batches_status ON v2_verify_batches(status, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_verify_batch_items_batch ON v2_verify_batch_items(batch_id, barcode)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_verify_scan_logs_batch_barcode ON v2_verify_scan_logs(batch_id, barcode)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_verify_scan_logs_batch_pallet ON v2_verify_scan_logs(batch_id, pallet_no)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_verify_scan_logs_job ON v2_verify_scan_logs(job_id)`,

  // ---- 核对口径修正：按"条码对应的计划箱数"核对，客户名落到条码级 ----
  `ALTER TABLE v2_verify_batch_items ADD COLUMN planned_box_count INTEGER DEFAULT 1`,
  `ALTER TABLE v2_verify_batch_items ADD COLUMN customer_name TEXT DEFAULT ''`,

  // ---- 记账标记：入库计划 / 出库作业单 ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN accounted INTEGER DEFAULT 0`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN accounted_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN accounted_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN accounted INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN accounted_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN accounted_at TEXT DEFAULT ''`,

  // ---- 性能索引（v2.20260424f）：列表接口高频过滤路径 ----
  `CREATE INDEX IF NOT EXISTS idx_v2_issue_status_biz_created ON v2_issue_tickets(status, biz_class, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_status_date_created ON v2_outbound_orders(status, order_date, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_feedback_status_created ON v2_field_feedbacks(status, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_status_date_created ON v2_inbound_plans(status, plan_date, created_at)`,

  // ---- 代发拣货（v2.20260427a）：拣货单级状态字段（pick_status 表示"整张单总状态"，非个人独占）----
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN pick_status TEXT DEFAULT 'pending'`,
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN pick_started_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN pick_finished_at TEXT DEFAULT ''`,
  // legacy informational 字段（多人共拣后不再代表归属，仅留首位拣货人参考）
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN picked_by_worker_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN picked_by_worker_name TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN picker_segment_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN assigned_worker_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_pick_docs ADD COLUMN assigned_worker_name TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pick_docs_status ON v2_ops_job_pick_docs(pick_status)`,

  // ---- 代发拣货（v2.20260427b）：人-单 多对多明细（同一单可多人共拣，同 segment 可多单）----
  `CREATE TABLE IF NOT EXISTS v2_pick_worker_docs (
    id TEXT PRIMARY KEY,
    job_id TEXT DEFAULT '',
    segment_id TEXT DEFAULT '',
    worker_id TEXT DEFAULT '',
    worker_name TEXT DEFAULT '',
    pick_doc_no TEXT DEFAULT '',
    started_at TEXT DEFAULT '',
    finished_at TEXT DEFAULT '',
    minutes_worked REAL DEFAULT 0,
    status TEXT DEFAULT 'working',
    created_at TEXT DEFAULT ''
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pwd_job ON v2_pick_worker_docs(job_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pwd_segment ON v2_pick_worker_docs(segment_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pwd_worker ON v2_pick_worker_docs(worker_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pwd_pick_doc ON v2_pick_worker_docs(pick_doc_no)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_pwd_job_doc ON v2_pick_worker_docs(job_id, pick_doc_no)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_pwd_seg_doc ON v2_pick_worker_docs(segment_id, pick_doc_no)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_accounted_date ON v2_inbound_plans(accounted, plan_date, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_accounted_date ON v2_outbound_orders(accounted, order_date, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_accounted_status_date ON v2_inbound_plans(accounted, status, plan_date, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_accounted_status_date ON v2_outbound_orders(accounted, status, order_date, created_at)`,

  // ===== 数据看板 — WMS 导入（独立数据源，不污染 v2_ops_jobs 等现场工时表）=====
  `CREATE TABLE IF NOT EXISTS v2_wms_import_batches (
    id TEXT PRIMARY KEY,
    import_type TEXT DEFAULT '',
    file_name TEXT DEFAULT '',
    row_count INTEGER DEFAULT 0,
    date_from TEXT DEFAULT '',
    date_to TEXT DEFAULT '',
    uploaded_by TEXT DEFAULT '',
    status TEXT DEFAULT 'imported',
    raw_headers_json TEXT DEFAULT '[]',
    created_at TEXT DEFAULT ''
  )`,
  `CREATE TABLE IF NOT EXISTS v2_wms_import_rows (
    id TEXT PRIMARY KEY,
    batch_id TEXT DEFAULT '',
    import_type TEXT DEFAULT '',
    work_date TEXT DEFAULT '',
    operated_at TEXT DEFAULT '',
    worker_name TEXT DEFAULT '',
    worker_id TEXT DEFAULT '',
    customer TEXT DEFAULT '',
    doc_no TEXT DEFAULT '',
    order_no TEXT DEFAULT '',
    sku TEXT DEFAULT '',
    qty REAL DEFAULT 0,
    box_count REAL DEFAULT 0,
    operation_type TEXT DEFAULT '',
    raw_json TEXT DEFAULT '{}',
    matched_job_id TEXT DEFAULT '',
    matched_worker_id TEXT DEFAULT '',
    match_confidence REAL DEFAULT 0,
    created_at TEXT DEFAULT ''
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_wms_rows_batch ON v2_wms_import_rows(batch_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_wms_rows_type_date ON v2_wms_import_rows(import_type, work_date)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_wms_rows_worker_date ON v2_wms_import_rows(worker_name, work_date)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_wms_rows_doc ON v2_wms_import_rows(doc_no)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_wms_batches_created ON v2_wms_import_batches(created_at)`,
];

// 每次发布迁移变化时手动 +1（patch 段），冷启动只比对一次字符串即可跳过整段 MIGRATIONS
const CURRENT_SCHEMA_VERSION = 'v2.20260427g';

let _migrated = false;
async function ensureMigrated(db) {
  if (_migrated) return;
  // 1. 先确保 v2_schema_meta 存在（轻量幂等 DDL）
  try {
    await db.prepare(`CREATE TABLE IF NOT EXISTS v2_schema_meta (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT
    )`).run();
  } catch (e) { /* 容忍并发 */ }

  // 2. 比对版本号，命中即跳过整段 MIGRATIONS
  try {
    const row = await db.prepare(
      "SELECT value FROM v2_schema_meta WHERE key='schema_version'"
    ).first();
    if (row && row.value === CURRENT_SCHEMA_VERSION) {
      _migrated = true;
      return;
    }
  } catch (e) { /* 表刚建好/读失败一律走完整迁移 */ }

  // 3. 版本不匹配（首次部署 / 升级），跑全量迁移
  for (const sql of MIGRATIONS) {
    try {
      await db.prepare(sql).run();
    } catch (e) {
      // ALTER TABLE may fail if column already exists — ignore
      if (!sql.trim().toUpperCase().startsWith("ALTER")) throw e;
    }
  }

  // 4. 写入当前版本号
  try {
    await db.prepare(
      "INSERT OR REPLACE INTO v2_schema_meta(key, value, updated_at) VALUES('schema_version', ?, ?)"
    ).bind(CURRENT_SCHEMA_VERSION, now()).run();
  } catch (e) { /* 写入失败不影响功能 */ }

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

// 轻量鉴权探测：登录 / 自动登录用，避免触发业务 SQL
route("v2_auth_check", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  return json({ ok: true });
});

// 协同中心首页聚合：一次拉齐 5 张卡片所需数据，替代前端 5 次并发
// 每组返回 { count, items(<=3) }；upcoming 额外含 dates。
// 用 SELECT * 兜底，避免后续表新增列时这里 SELECT 列名不一致而 500。
route("v2_dashboard_summary", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);

  // 未来 3 个工作日（跳过周日，与 v2_inbound_plan_list_upcoming 保持一致）
  const today = kstToday();
  const dates = [];
  const kstMs = Date.now() + 9 * 3600 * 1000;
  let d = new Date(kstMs);
  d.setUTCHours(0, 0, 0, 0);
  while (dates.length < 3) {
    d.setUTCDate(d.getUTCDate() + 1);
    if (d.getUTCDay() !== 0) {
      const ds = d.toISOString().slice(0, 10);
      if (ds !== today && dates.indexOf(ds) === -1) dates.push(ds);
    }
  }
  const first = dates[0];
  const last = dates[dates.length - 1];

  // 每个类别：count(*) + items(<=3) 两个查询并发
  const [
    issuesCntRs, issuesItemsRs,
    obCntRs, obItemsRs,
    ibCntRs, ibItemsRs,
    fbCntRs, fbItemsRs,
    upcomingRs
  ] = await Promise.all([
    // ---- issues：FIFO 排序，最早的最先看到 ----
    env.DB.prepare(
      `SELECT COUNT(*) AS c FROM v2_issue_tickets
        WHERE status IN ('pending','processing','responded','rework_required')`
    ).first(),
    env.DB.prepare(
      `SELECT * FROM v2_issue_tickets
        WHERE status IN ('pending','processing','responded','rework_required')
        ORDER BY created_at ASC LIMIT 3`
    ).all(),

    // ---- outbounds：按 order_date / created_at 顺序 ----
    env.DB.prepare(
      `SELECT COUNT(*) AS c FROM v2_outbound_orders
        WHERE status IN ('pending_issue','issued','working','ready_to_ship')`
    ).first(),
    env.DB.prepare(
      `SELECT * FROM v2_outbound_orders
        WHERE status IN ('pending_issue','issued','working','ready_to_ship')
        ORDER BY order_date ASC, created_at ASC LIMIT 3`
    ).all(),

    // ---- inbounds（待执行入库）----
    env.DB.prepare(
      `SELECT COUNT(*) AS c FROM v2_inbound_plans
        WHERE source_type != 'return_session'
          AND status IN ('pending','unloading','unloading_putting_away','arrived_pending_putaway','putting_away')`
    ).first(),
    env.DB.prepare(
      `SELECT * FROM v2_inbound_plans
        WHERE source_type != 'return_session'
          AND status IN ('pending','unloading','unloading_putting_away','arrived_pending_putaway','putting_away')
        ORDER BY plan_date ASC, created_at ASC LIMIT 3`
    ).all(),

    // ---- feedbacks（现场反馈进行中）----
    env.DB.prepare(
      `SELECT COUNT(*) AS c FROM v2_field_feedbacks
        WHERE status IN ('field_working','unloaded_pending_info')`
    ).first(),
    env.DB.prepare(
      `SELECT * FROM v2_field_feedbacks
        WHERE status IN ('field_working','unloaded_pending_info')
        ORDER BY created_at ASC LIMIT 3`
    ).all(),

    // ---- upcoming（未来 3 工作日入库计划，按日期分组）----
    env.DB.prepare(
      `SELECT * FROM v2_inbound_plans
        WHERE plan_date>=? AND plan_date<=?
          AND source_type != 'return_session'
          AND status NOT IN ('completed','cancelled')
        ORDER BY plan_date ASC, created_at ASC`
    ).bind(first, last).all()
  ]);

  const upcomingItems = upcomingRs.results || [];

  return json({
    ok: true,
    issues:    { count: Number((issuesCntRs && issuesCntRs.c) || 0), items: issuesItemsRs.results || [] },
    outbounds: { count: Number((obCntRs && obCntRs.c) || 0),         items: obItemsRs.results || [] },
    inbounds:  { count: Number((ibCntRs && ibCntRs.c) || 0),         items: ibItemsRs.results || [] },
    feedbacks: { count: Number((fbCntRs && fbCntRs.c) || 0),         items: fbItemsRs.results || [] },
    upcoming:  { count: upcomingItems.length, items: upcomingItems, dates }
  });
});

// =====================================================
// ISSUE TICKETS — Collab side
// =====================================================
route("v2_issue_create", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  return withIdem(env, body, "v2_issue_create", async () => {
    const desc = String(body.issue_description || "").trim();
    if (!desc) return { ok: false, error: "issue_description is required" };
    const id = "ISS-" + uid();
    const t = now();
    // issue_type / issue_summary 字段保留（schema DEFAULT ''），不再写入；前端不再传
    await env.DB.prepare(`
      INSERT INTO v2_issue_tickets(id, biz_class, customer, related_doc_no,
        issue_description, priority, submitted_by, status, created_at, updated_at)
      VALUES(?,?,?,?,?,?,?,'pending',?,?)
    `).bind(
      id,
      String(body.biz_class || ""),
      String(body.customer || ""),
      String(body.related_doc_no || ""),
      desc,
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
  const sort = String(body.sort || "").trim();
  const { limit, offset } = pageParams(body);
  let sql = "SELECT * FROM v2_issue_tickets WHERE 1=1";
  const binds = [];
  if (status) { sql += " AND status=?"; binds.push(status); }
  if (biz_class) { sql += " AND biz_class=?"; binds.push(biz_class); }
  // 默认 newest_first（002 客服侧看最新）；oldest_first 给需要 FIFO 的视角
  sql += sort === "oldest_first" ? " ORDER BY created_at ASC" : " ORDER BY created_at DESC";
  sql += " LIMIT ? OFFSET ?";
  binds.push(limit, offset);
  const rs = await env.DB.prepare(sql).bind(...binds).all();
  return json({ ok: true, items: rs.results || [], limit, offset });
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
    "UPDATE v2_issue_tickets SET status='completed', updated_at=? WHERE id=?"
  ).bind(now(), id).run();
  return json({ ok: true });
});

route("v2_issue_rework", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const rework_note = String(body.rework_note || "").trim();
  if (!id) return err("missing id");
  if (!rework_note) return err("missing rework_note");
  await env.DB.prepare(
    "UPDATE v2_issue_tickets SET status='rework_required', rework_note=?, updated_at=? WHERE id=?"
  ).bind(rework_note, now(), id).run();
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
  const biz_class = String(body.biz_class || "").trim();
  const sort = String(body.sort || "").trim();
  let sql = "SELECT * FROM v2_issue_tickets WHERE 1=1";
  const binds = [];
  if (status) { sql += " AND status=?"; binds.push(status); }
  if (biz_class) { sql += " AND biz_class=?"; binds.push(biz_class); }
  // 现场默认 oldest_first（FIFO，最早等待最先看到）；显式 newest_first 才反过来
  sql += sort === "newest_first" ? " ORDER BY created_at DESC LIMIT 200" : " ORDER BY created_at ASC LIMIT 200";
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
    const busy = await checkWorkerBusy(env, handler_id);
    if (busy) return { ok: false, error: "worker_busy", busy_job_type: busy.job_type };

    const issue = await env.DB.prepare("SELECT * FROM v2_issue_tickets WHERE id=?").bind(issue_id).first();
    if (!issue) return { ok: false, error: "issue not found" };
    if (issue.status === "closed" || issue.status === "cancelled" || issue.status === "completed") return { ok: false, error: "issue already " + issue.status };

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
  if (!feedback_text) return err("missing feedback_text");

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
      VALUES(?,?,?,'bulk','',?,?,'','pending_issue',?,?,?,?,?,?,?,?,0,0,?)
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
  const accounted = String(body.accounted == null ? "" : body.accounted).trim();
  const { limit, offset } = pageParams(body);
  let sql = "SELECT * FROM v2_outbound_orders WHERE 1=1";
  const binds = [];
  if (start) { sql += " AND order_date>=?"; binds.push(start); }
  if (end) { sql += " AND order_date<=?"; binds.push(end); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  if (accounted === "1") { sql += " AND accounted=1"; }
  else if (accounted === "0") { sql += " AND (accounted IS NULL OR accounted=0)"; }
  sql += " ORDER BY order_date DESC, created_at DESC LIMIT ? OFFSET ?";
  binds.push(limit, offset);
  const rs = await env.DB.prepare(sql).bind(...binds).all();
  return json({ ok: true, items: rs.results || [], limit, offset });
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
  // Get related jobs（含 outbound_load 和 bulk_op 两种关联方式）
  const jobs = await env.DB.prepare(
    "SELECT * FROM v2_ops_jobs WHERE (related_doc_type='outbound_order' AND related_doc_id=?) OR linked_outbound_order_id=? ORDER BY created_at DESC"
  ).bind(id, id).all();
  const jobIds = (jobs.results || []).map(j => j.id);
  let allAtts = [];
  const orderAtts = await env.DB.prepare(
    "SELECT * FROM v2_attachments WHERE related_doc_type='outbound_order' AND related_doc_id=? ORDER BY created_at DESC"
  ).bind(id).all();
  allAtts = allAtts.concat(orderAtts.results || []);
  for (const jid of jobIds) {
    const jAtts = await env.DB.prepare(
      "SELECT * FROM v2_attachments WHERE related_doc_id=? ORDER BY created_at DESC"
    ).bind(jid).all();
    allAtts = allAtts.concat(jAtts.results || []);
  }
  return json({
    ok: true,
    order: row,
    lines: lines.results || [],
    jobs: jobs.results || [],
    attachments: allAtts
  });
});

route("v2_outbound_order_update_status", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const newStatus = String(body.status || "").trim();
  if (!id || !newStatus) return err("missing id or status");

  const order = await env.DB.prepare("SELECT status FROM v2_outbound_orders WHERE id=?").bind(id).first();
  if (!order) return err("not found", 404);
  const cur = order.status || "";

  // 状态迁移白名单（前端只允许这些手动迁移）
  const allowed = {
    "pending_issue": ["issued", "cancelled"],
    "issued":        ["cancelled"],
    "working":       ["cancelled"],
    "ready_to_ship": ["reopen_pending", "cancelled"],
    "shipped":       [],
    "reopen_pending": ["cancelled"],
  };
  const validTargets = allowed[cur] || [];
  if (!validTargets.includes(newStatus)) {
    return json({ ok: false, error: "invalid_status_transition",
      message: "不允许从 " + cur + " 变更为 " + newStatus });
  }

  // 硬拦截：shipped 状态绝不允许 reopen（已装车出库）
  if (newStatus === "reopen_pending") {
    const loadedJob = await env.DB.prepare(
      `SELECT id FROM v2_ops_jobs
       WHERE job_type='load_outbound' AND status='completed'
         AND (related_doc_id=? OR linked_outbound_order_id=?)
       LIMIT 1`
    ).bind(id, id).first();
    if (loadedJob) {
      return json({ ok: false, error: "outbound_already_shipped_cannot_reopen",
        message: "该出库作业单已完成出库，不能设为待再操作 / 해당 출고작업단은 이미 출고 완료되어 재작업 대기로 변경할 수 없습니다" });
    }
  }

  // 如果要取消，先检查是否有活跃 job（覆盖全部关联口径）
  if (newStatus === "cancelled") {
    const activeJob = await env.DB.prepare(
      `SELECT id FROM v2_ops_jobs
       WHERE status IN ('working','awaiting_close','pending')
         AND (linked_outbound_order_id=?
           OR (related_doc_type='outbound_order' AND related_doc_id=?))
       LIMIT 1`
    ).bind(id, id).first();
    if (activeJob) {
      return json({ ok: false, error: "has_active_job",
        message: "当前有进行中的现场作业，不能取消" });
    }
  }

  await env.DB.prepare(
    "UPDATE v2_outbound_orders SET status=?, updated_at=? WHERE id=?"
  ).bind(newStatus, now(), id).run();
  return json({ ok: true });
});

// ===== 出库作业单：记账标记 =====
route("v2_outbound_order_mark_accounted", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const operator = String(body.operator_name || "").trim();
  const accounted = Number(body.accounted) === 1 ? 1 : 0;
  if (!id) return err("missing id");
  if (accounted === 1 && !operator) return err("missing operator_name");
  const order = await env.DB.prepare("SELECT id FROM v2_outbound_orders WHERE id=?").bind(id).first();
  if (!order) return err("not found", 404);
  const t = now();
  if (accounted === 1) {
    await env.DB.prepare(
      "UPDATE v2_outbound_orders SET accounted=1, accounted_by=?, accounted_at=?, updated_at=? WHERE id=?"
    ).bind(operator, t, t, id).run();
  } else {
    await env.DB.prepare(
      "UPDATE v2_outbound_orders SET accounted=0, accounted_by='', accounted_at='', updated_at=? WHERE id=?"
    ).bind(t, id).run();
  }
  return json({ ok: true, accounted, accounted_by: accounted ? operator : '', accounted_at: accounted ? t : '' });
});

// =====================================================
// OUTBOUND LOAD — Ops side
// =====================================================
route("v2_outbound_order_resolve_code", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const code = String(body.code || "").trim();
  if (!code) return err("missing code");

  const cols = "id, display_no, wms_work_order_no, status, customer, outbound_mode, planned_box_count, planned_pallet_count, order_date";
  let order = await env.DB.prepare(
    `SELECT ${cols} FROM v2_outbound_orders WHERE display_no=? LIMIT 1`
  ).bind(code).first();
  if (!order) {
    order = await env.DB.prepare(
      `SELECT ${cols} FROM v2_outbound_orders WHERE wms_work_order_no=? AND wms_work_order_no!='' ORDER BY created_at DESC LIMIT 1`
    ).bind(code).first();
  }
  if (!order) {
    order = await env.DB.prepare(
      `SELECT ${cols} FROM v2_outbound_orders WHERE id=? LIMIT 1`
    ).bind(code).first();
  }

  if (!order) {
    return json({ ok: true, kind: 'not_found', message: "未找到匹配的出库作业单 / 일치하는 출고작업단을 찾을 수 없습니다" });
  }

  const loadableStatuses = ['issued', 'working', 'ready_to_ship'];
  if (order.status === 'shipped') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "该出库单已出库，不能再装货 / 이미 출고 완료되어 상차할 수 없습니다" });
  }
  if (order.status === 'cancelled') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "该出库单已取消 / 해당 출고단은 취소되었습니다" });
  }
  if (order.status === 'pending_issue') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "该出库单尚未下发，请先打印下发 / 아직 배정되지 않았습니다. 먼저 인쇄하세요" });
  }
  if (loadableStatuses.indexOf(order.status) === -1) {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "当前状态（" + order.status + "）不允许装货" });
  }

  return json({ ok: true, kind: 'system', order });
});

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

    const busy = await checkWorkerBusy(env, worker_id, job ? job.id : null);
    if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };

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
          "UPDATE v2_outbound_orders SET status='ready_to_ship', updated_at=? WHERE id=? AND status IN ('ready_to_ship')"
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
          "UPDATE v2_outbound_orders SET status='shipped', actual_box_count=?, actual_pallet_count=?, updated_at=? WHERE id=?"
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
        VALUES(?,?,?,'bulk','',?,?,'','pending_issue',?,?,?,?,?,?,?,?,?,0,0,?)
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
  const accounted = String(body.accounted == null ? "" : body.accounted).trim();
  const { limit, offset } = pageParams(body);
  // 排除退件入库会话：return_session 不属于正式入库计划口径
  let sql = "SELECT * FROM v2_inbound_plans WHERE source_type != 'return_session'";
  const binds = [];
  if (start) { sql += " AND plan_date>=?"; binds.push(start); }
  if (end) { sql += " AND plan_date<=?"; binds.push(end); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  if (accounted === "1") { sql += " AND accounted=1"; }
  else if (accounted === "0") { sql += " AND (accounted IS NULL OR accounted=0)"; }
  sql += " ORDER BY plan_date DESC, created_at DESC LIMIT ? OFFSET ?";
  binds.push(limit, offset);
  const rs = await env.DB.prepare(sql).bind(...binds).all();
  return json({ ok: true, items: rs.results || [], limit, offset });
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

// ===== 入库计划：记账标记 =====
route("v2_inbound_plan_mark_accounted", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const operator = String(body.operator_name || "").trim();
  const accounted = Number(body.accounted) === 1 ? 1 : 0;
  if (!id) return err("missing id");
  if (accounted === 1 && !operator) return err("missing operator_name");
  const plan = await env.DB.prepare("SELECT id FROM v2_inbound_plans WHERE id=?").bind(id).first();
  if (!plan) return err("plan not found", 404);
  const t = now();
  if (accounted === 1) {
    await env.DB.prepare(
      "UPDATE v2_inbound_plans SET accounted=1, accounted_by=?, accounted_at=?, updated_at=? WHERE id=?"
    ).bind(operator, t, t, id).run();
  } else {
    await env.DB.prepare(
      "UPDATE v2_inbound_plans SET accounted=0, accounted_by='', accounted_at='', updated_at=? WHERE id=?"
    ).bind(t, id).run();
  }
  return json({ ok: true, accounted, accounted_by: accounted ? operator : '', accounted_at: accounted ? t : '' });
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
    if (!parent_job_id) {
      const busy = await checkWorkerBusy(env, worker_id, null);
      if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };
    }
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

    const busy = await checkWorkerBusy(env, worker_id, job ? job.id : null);
    if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };

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

    const busy = await checkWorkerBusy(env, worker_id, job ? job.id : null);
    if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };

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
        // Hard block: unload not done → cannot finish inbound
        const unloadStillRunning = ['unloading', 'unloading_putting_away'];
        if (planCheck && unloadStillRunning.indexOf(planCheck.status) !== -1) {
          return { ok: false, error: "unload_not_finished", message: "卸货未完成，无法完成理货 / 하차가 아직 완료되지 않아 입고 완료 처리할 수 없습니다" };
        }
        const inboundFinishAllowed = ['putting_away'];
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

// ===== Import Delivery (外出取/送货) =====
route("v2_import_delivery_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_import_delivery_job_start", async () => {
    const t = now();
    const job_type = "pickup_delivery_import";

    const existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE job_type=? AND status IN ('pending','working') LIMIT 1"
    ).bind(job_type).first();

    const busy = await checkWorkerBusy(env, worker_id, existing ? existing.id : null);
    if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };

    let job_id, is_new_job = false;
    if (existing) {
      job_id = existing.id;
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
        VALUES(?, 'import', 'import', ?, '', '', 'working', ?, ?, ?, 1)
      `).bind(job_id, job_type, worker_id, t, t).run();
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    return { ok: true, job_id, worker_seg_id: seg_id, is_new_job };
  });
});

route("v2_import_delivery_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const complete_job = body.complete_job === true;
  const leave_only = body.leave_only === true;
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_import_delivery_job_finish", async () => {
    const t = now();

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

    const destination_note = String(body.destination_note || "").trim();
    const estimated_piece_count = Number(body.estimated_piece_count || 0) || 0;
    const remark = String(body.remark || "").trim();
    const resultData = { destination_note, estimated_piece_count, remark };

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
      ).bind(result_id, job_id, remark, JSON.stringify(resultData), '[]', worker_id, t).run();

      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', updated_at=? WHERE id=?"
      ).bind(t, job_id).run();
      await env.DB.prepare(
        "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
      ).bind(t, job_id).run();

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

    if (!is_temporary_interrupt) {
      const busy = await checkWorkerBusy(env, worker_id, job ? job.id : null);
      if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };
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

  // 优先取 job 仍在 working 的 segment；若有历史脏数据（多个 open seg），仍只返回 1 条最合理项
  const seg = await env.DB.prepare(
    `SELECT w.* FROM v2_ops_job_workers w
     JOIN v2_ops_jobs j ON j.id = w.job_id
     WHERE w.worker_id=? AND w.left_at='' AND j.status IN ('pending','working','awaiting_close')
     ORDER BY w.joined_at DESC LIMIT 1`
  ).bind(worker_id).first();
  if (!seg) return json({ ok: true, active: false });

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
  const { limit, offset } = pageParams(body);
  let sql = "SELECT * FROM v2_field_feedbacks WHERE 1=1";
  const binds = [];
  if (feedback_type) { sql += " AND feedback_type=?"; binds.push(feedback_type); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?";
  binds.push(limit, offset);
  const rs = await env.DB.prepare(sql).bind(...binds).all();
  return json({ ok: true, items: rs.results || [], limit, offset });
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
// 流程语义（v2.20260427a 重构）：
//   1) v2_pick_job_start          : 仅创建趟次（pending 态），录入 pick_doc_nos，不计任何工时
//   2) v2_pick_job_start_by_docs  : 实际拣货人扫码 N 个拣货单 → 同一趟次内开 segment，开始计时
//   3) v2_pick_job_finish         : 当前拣货人完成自己这一段单，趟次内多人各自独立结算
//   4) v2_pick_doc_lookup         : 现场扫码识别单号归属/状态/可否开始
//   created_by 仅代表趟次录入人，不代表拣货人
// =====================================================
route("v2_pick_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();   // creator id（仅审计，不计时）
  const worker_name = String(body.worker_name || "").trim();

  // pick_doc_nos: array of pick document numbers
  let pick_doc_nos = body.pick_doc_nos || [];
  if (typeof pick_doc_nos === 'string') {
    pick_doc_nos = pick_doc_nos.split(',').map(s => s.trim()).filter(Boolean);
  }
  pick_doc_nos = Array.from(new Set(
    pick_doc_nos.map(s => String(s || '').trim()).filter(Boolean)
  ));
  if (pick_doc_nos.length === 0) return err("missing pick_doc_nos");

  return withIdem(env, body, "v2_pick_job_start", async () => {
    const t = now();

    // 单号占用冲突：拒绝重复录入到第二个未完成趟次
    for (const docNo of pick_doc_nos) {
      const conflict = await env.DB.prepare(
        `SELECT j.id, j.display_no FROM v2_ops_jobs j
         JOIN v2_ops_job_pick_docs pd ON pd.job_id = j.id
         WHERE j.job_type='pick_direct' AND j.status IN ('pending','working','awaiting_close')
         AND j.is_temporary_interrupt=0 AND pd.pick_doc_no=?
         LIMIT 1`
      ).bind(docNo).first();
      if (conflict) {
        return { ok: false, error: "doc_conflict",
          message: "拣货单 " + docNo + " 已在活跃趟次 " + (conflict.display_no || conflict.id) + " 中",
          conflict_doc_no: docNo, conflict_trip: conflict.display_no || conflict.id };
      }
    }

    const trip_no = await nextPickTripNo(env);
    const job_id = "JOB-" + uid();

    // 趟次仅 pending，等拣货人扫码切换到 working；active_worker_count=0
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, parent_job_id, is_temporary_interrupt, interrupt_type, created_by, created_at, updated_at,
        active_worker_count, display_no)
      VALUES(?,'order_op','direct_ship','pick_direct','','','pending','',0,'',?,?,?,0,?)
    `).bind(job_id, worker_id, t, t, trip_no).run();

    // 写 pick docs（pick_status 默认 'pending'）
    const docRows = [];
    for (const docNo of pick_doc_nos) {
      const pd_id = "PD-" + uid();
      await env.DB.prepare(
        "INSERT INTO v2_ops_job_pick_docs(id, job_id, pick_doc_no, pick_status, created_at) VALUES(?,?,?, 'pending', ?)"
      ).bind(pd_id, job_id, docNo, t).run();
      docRows.push({ id: pd_id, pick_doc_no: docNo, pick_status: 'pending' });
    }

    return {
      ok: true,
      job_id,
      trip_no,
      display_no: trip_no,
      pick_doc_nos: pick_doc_nos,
      pick_docs: docRows,
      created_by: worker_id,
      is_new_job: true
    };
  });
});

// =====================================================
// 实际拣货人扫码开始 — 一次可扫 N 个拣货单一起开拣
// 多对多语义：同一个 pick_doc 允许多人共同参与；通过 v2_pick_worker_docs 记录每人每单的明细
// =====================================================
route("v2_pick_job_start_by_docs", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!worker_id) return err("missing worker_id");

  let pick_doc_nos = body.pick_doc_nos || [];
  if (typeof pick_doc_nos === 'string') {
    pick_doc_nos = pick_doc_nos.split(',').map(s => s.trim()).filter(Boolean);
  }
  pick_doc_nos = Array.from(new Set(
    pick_doc_nos.map(s => String(s || '').trim()).filter(Boolean)
  ));
  if (pick_doc_nos.length === 0) return err("missing pick_doc_nos");

  return withIdem(env, body, "v2_pick_job_start_by_docs", async () => {
    const t = now();

    // 当前 worker 不能有其他活跃任务（业务规则：一人同时只能在一个 active job）
    const busy = await checkWorkerBusy(env, worker_id, null);
    if (busy) {
      return { ok: false, error: "worker_has_active_job",
        message: "您当前已有进行中的任务，请先完成或暂离后再开始拣货",
        active_job_id: busy.job_id, active_job_type: busy.job_type };
    }

    // 解析每个拣货单 → 所属 job + 当前状态
    const docs = [];
    for (const docNo of pick_doc_nos) {
      const row = await env.DB.prepare(
        `SELECT pd.*, j.id as j_id, j.display_no as j_display_no, j.status as j_status,
                j.is_temporary_interrupt as j_interrupt
         FROM v2_ops_job_pick_docs pd
         JOIN v2_ops_jobs j ON j.id = pd.job_id
         WHERE j.job_type='pick_direct' AND pd.pick_doc_no=?
         ORDER BY pd.created_at DESC LIMIT 1`
      ).bind(docNo).first();
      if (!row) {
        return { ok: false, error: "doc_not_found",
          message: "拣货单 " + docNo + " 不存在，请确认是否已创建趟次",
          conflict_doc_no: docNo };
      }
      if (row.j_status === 'completed') {
        return { ok: false, error: "trip_already_completed",
          message: "拣货单 " + docNo + " 所属趟次已完成",
          conflict_doc_no: docNo };
      }
      if (row.j_status === 'cancelled') {
        return { ok: false, error: "trip_cancelled",
          message: "拣货单 " + docNo + " 所属趟次已取消",
          conflict_doc_no: docNo };
      }
      if (row.j_interrupt) {
        return { ok: false, error: "trip_interrupted",
          message: "拣货单 " + docNo + " 所属趟次正处于临时挂起",
          conflict_doc_no: docNo };
      }
      // 注意：不再检查 pick_status='working' 的"独占"，允许多人共拣
      docs.push(row);
    }

    // 跨趟次拒绝：所有扫描单必须属于同一个趟次
    const jobIds = Array.from(new Set(docs.map(d => d.j_id)));
    if (jobIds.length > 1) {
      const tripNos = Array.from(new Set(docs.map(d => d.j_display_no || d.j_id)));
      return { ok: false, error: "cross_trip_not_allowed",
        message: "不能跨趟次同时拣货（涉及趟次：" + tripNos.join(", ") + "），请确认拣货单号",
        trips: tripNos };
    }

    const job_id = jobIds[0];

    // 创建拣货人 segment（开始计时）
    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    // 写多对多明细 v2_pick_worker_docs（每个单一条）
    for (const d of docs) {
      const pwd_id = "PWD-" + uid();
      await env.DB.prepare(`
        INSERT INTO v2_pick_worker_docs(id, job_id, segment_id, worker_id, worker_name,
          pick_doc_no, started_at, status, created_at)
        VALUES(?,?,?,?,?,?,?, 'working', ?)
      `).bind(pwd_id, job_id, seg_id, worker_id, worker_name, d.pick_doc_no, t, t).run();

      // pick_doc 总状态：pending → working（"至少一人开始过"），不绑定独占
      // pick_started_at 仅在首次开始时写入；首位 picker 信息仅作参考，不代表独占
      if ((d.pick_status || 'pending') === 'pending') {
        await env.DB.prepare(
          `UPDATE v2_ops_job_pick_docs
           SET pick_status='working',
               pick_started_at=COALESCE(NULLIF(pick_started_at,''), ?),
               picked_by_worker_id=COALESCE(NULLIF(picked_by_worker_id,''), ?),
               picked_by_worker_name=COALESCE(NULLIF(picked_by_worker_name,''), ?)
           WHERE id=?`
        ).bind(t, worker_id, worker_name, d.id).run();
      }
    }

    // 趟次 → working；重算 active_worker_count
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='working', updated_at=? WHERE id=?"
    ).bind(t, job_id).run();
    await recalcActiveCount(env, job_id, t);

    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    const allDocs = await env.DB.prepare(
      "SELECT * FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at"
    ).bind(job_id).all();

    return {
      ok: true,
      job_id,
      worker_seg_id: seg_id,
      trip_no: job ? (job.display_no || '') : '',
      display_no: job ? (job.display_no || '') : '',
      started_at: t,
      picked_doc_nos: docs.map(d => d.pick_doc_no),
      job_pick_docs: allDocs.results || []
    };
  });
});

// =====================================================
// 扫码识别拣货单 — 返回趟次/总状态/已参与人员；不做写操作
// =====================================================
route("v2_pick_doc_lookup", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const docNo = String(body.pick_doc_no || "").trim();
  if (!docNo) return err("missing pick_doc_no");

  const row = await env.DB.prepare(
    `SELECT pd.*, j.id as j_id, j.display_no as j_display_no, j.status as j_status,
            j.is_temporary_interrupt as j_interrupt,
            j.created_by as j_created_by, j.created_at as j_created_at
     FROM v2_ops_job_pick_docs pd
     JOIN v2_ops_jobs j ON j.id = pd.job_id
     WHERE j.job_type='pick_direct' AND pd.pick_doc_no=?
     ORDER BY pd.created_at DESC LIMIT 1`
  ).bind(docNo).first();
  if (!row) {
    return json({ ok: true, found: false, pick_doc_no: docNo });
  }
  const pickStatus = row.pick_status || 'pending';
  // 参与人员明细（多对多）
  const partsRs = await env.DB.prepare(
    `SELECT worker_id, worker_name, started_at, finished_at, minutes_worked, status
     FROM v2_pick_worker_docs WHERE job_id=? AND pick_doc_no=? ORDER BY started_at`
  ).bind(row.j_id, row.pick_doc_no).all();
  const participants = partsRs.results || [];
  const can_join = (
    row.j_status !== 'completed' &&
    row.j_status !== 'cancelled' &&
    !row.j_interrupt
  );
  return json({
    ok: true,
    found: true,
    pick_doc_no: row.pick_doc_no,
    pick_status: pickStatus,
    pick_started_at: row.pick_started_at || '',
    pick_finished_at: row.pick_finished_at || '',
    job_id: row.j_id,
    job_display_no: row.j_display_no || '',
    job_status: row.j_status || '',
    job_interrupted: !!row.j_interrupt,
    job_created_by: row.j_created_by || '',
    job_created_at: row.j_created_at || '',
    participants,
    active_picker_count: participants.filter(p => p.status === 'working').length,
    can_join,
    can_start: can_join  // 新语义：可加入即可开始
  });
});

// =====================================================
// PICK BREAKDOWN — 按单/按人双视角明细（供 002 详情、看板使用）
// =====================================================
route("v2_pick_job_breakdown", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  if (!job_id) return err("missing job_id");

  const docsRs = await env.DB.prepare(
    `SELECT id, pick_doc_no, pick_status, pick_started_at, pick_finished_at, created_at
     FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at`
  ).bind(job_id).all();
  const pwdRs = await env.DB.prepare(
    `SELECT id, segment_id, worker_id, worker_name, pick_doc_no,
            started_at, finished_at, minutes_worked, status, created_at
     FROM v2_pick_worker_docs WHERE job_id=? ORDER BY started_at, created_at`
  ).bind(job_id).all();
  const segsRs = await env.DB.prepare(
    `SELECT id, worker_id, worker_name, joined_at, left_at, minutes_worked, leave_reason
     FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at`
  ).bind(job_id).all();

  const docs = docsRs.results || [];
  const pwds = pwdRs.results || [];
  const segs = segsRs.results || [];

  // 按单分组：每张单的参与人员明细
  const byDoc = {};
  for (const d of docs) {
    byDoc[d.pick_doc_no] = {
      pick_doc_no: d.pick_doc_no,
      pick_status: d.pick_status || 'pending',
      pick_started_at: d.pick_started_at || '',
      pick_finished_at: d.pick_finished_at || '',
      participants: []
    };
  }
  for (const p of pwds) {
    if (!byDoc[p.pick_doc_no]) {
      // 有可能 pick_doc 被删但 pwd 还在（理论上不该）
      byDoc[p.pick_doc_no] = {
        pick_doc_no: p.pick_doc_no, pick_status: 'unknown',
        pick_started_at: '', pick_finished_at: '', participants: []
      };
    }
    byDoc[p.pick_doc_no].participants.push({
      worker_id: p.worker_id,
      worker_name: p.worker_name,
      segment_id: p.segment_id,
      started_at: p.started_at,
      finished_at: p.finished_at,
      minutes_worked: Number(p.minutes_worked) || 0,
      status: p.status
    });
  }

  // 按人分组：每人参与的单 + 总耗时
  const byWorker = {};
  for (const s of segs) {
    if (!byWorker[s.worker_id]) {
      byWorker[s.worker_id] = {
        worker_id: s.worker_id,
        worker_name: s.worker_name,
        segments: [],
        pick_doc_nos: [],
        total_minutes: 0
      };
    }
    const segPwds = pwds.filter(p => p.segment_id === s.id);
    byWorker[s.worker_id].segments.push({
      segment_id: s.id,
      joined_at: s.joined_at,
      left_at: s.left_at || '',
      minutes_worked: Number(s.minutes_worked) || 0,
      leave_reason: s.leave_reason || '',
      pick_doc_nos: segPwds.map(p => p.pick_doc_no)
    });
    byWorker[s.worker_id].total_minutes += (Number(s.minutes_worked) || 0);
    for (const p of segPwds) {
      if (byWorker[s.worker_id].pick_doc_nos.indexOf(p.pick_doc_no) === -1) {
        byWorker[s.worker_id].pick_doc_nos.push(p.pick_doc_no);
      }
    }
  }
  // 圆整 total_minutes
  Object.values(byWorker).forEach(w => {
    w.total_minutes = Math.round(w.total_minutes * 10) / 10;
  });

  return json({
    ok: true,
    job_id,
    docs_view: Object.values(byDoc),
    workers_view: Object.values(byWorker),
    segments: segs,
    pick_worker_docs: pwds
  });
});

route("v2_pick_job_docs_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  if (!job_id) return err("missing job_id");
  const allDocs = await env.DB.prepare(
    `SELECT id, job_id, pick_doc_no, pick_status, pick_started_at, pick_finished_at, created_at
     FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at`
  ).bind(job_id).all();
  const pwds = await env.DB.prepare(
    `SELECT pick_doc_no, worker_id, worker_name, segment_id, started_at, finished_at,
            minutes_worked, status
     FROM v2_pick_worker_docs WHERE job_id=? ORDER BY started_at`
  ).bind(job_id).all();
  const partsByDoc = {};
  for (const p of (pwds.results || [])) {
    if (!partsByDoc[p.pick_doc_no]) partsByDoc[p.pick_doc_no] = [];
    partsByDoc[p.pick_doc_no].push(p);
  }
  const docs = (allDocs.results || []).map(d => {
    const parts = partsByDoc[d.pick_doc_no] || [];
    return Object.assign({}, d, {
      participants: parts,
      active_picker_count: parts.filter(p => p.status === 'working').length,
      total_picker_count: new Set(parts.map(p => p.worker_id)).size
    });
  });
  return json({ ok: true, docs });
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

// =====================================================
// PICK FINISH — 当前拣货人完成"自己这一段"
// 多对多语义：
//   - 仅关闭当前 worker 的 open segment（同单仍可被其他人继续拣）
//   - 把该 segment 对应的 v2_pick_worker_docs → status='completed' + finished_at + minutes_worked
//   - 不自动把整张拣货单标记 completed，整趟次也不自动 completed
//   - 趟次完成由 v2_pick_job_finalize 显式触发
//   - 当 active=0 且仍有 working 中的 pwd 时：视为"全员暂离"，趟次仍 pending（等待恢复或 finalize）
// =====================================================
route("v2_pick_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_pick_job_finish", async () => {
    const t = now();

    const jobCheck = await env.DB.prepare(
      "SELECT id, status FROM v2_ops_jobs WHERE id=?"
    ).bind(job_id).first();
    if (!jobCheck) return { ok: false, error: "job_not_found", message: "趟次不存在" };
    if (jobCheck.status === 'cancelled') {
      return { ok: false, error: "already_cancelled", message: "趟次已取消" };
    }

    // 找到当前 worker 的 open segment（最近一段）
    const openSeg = await findOpenSeg(env, job_id, worker_id);
    if (!openSeg) {
      if (jobCheck.status === 'completed') {
        return { ok: false, error: "already_completed", message: "趟次已完成，请勿重复提交" };
      }
      return { ok: false, error: "no_open_segment",
        message: "您未在该趟次中拣货，无法完成" };
    }

    // 1) 关闭 segment（计算 minutes_worked）
    const minutes = Math.round(
      (new Date(t).getTime() - new Date(openSeg.joined_at).getTime()) / 60000 * 10
    ) / 10;
    const minutesSafe = Math.max(0, minutes);
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason='finished' WHERE id=?"
    ).bind(t, minutesSafe, openSeg.id).run();

    // 2) 关闭该 segment 名下所有 v2_pick_worker_docs（多对多明细）
    const pwdRs = await env.DB.prepare(
      "SELECT * FROM v2_pick_worker_docs WHERE segment_id=? AND status='working'"
    ).bind(openSeg.id).all();
    const segPwds = pwdRs.results || [];
    for (const pwd of segPwds) {
      await env.DB.prepare(
        "UPDATE v2_pick_worker_docs SET status='completed', finished_at=?, minutes_worked=? WHERE id=?"
      ).bind(t, minutesSafe, pwd.id).run();
    }
    const segDocNos = segPwds.map(p => p.pick_doc_no);

    // 3) 写本拣货人段独立的结果记录（便于按拣货人分组展示）
    const remark = String(body.remark || "").trim();
    const result_note = String(body.result_note || "").trim();
    const result_id = "RES-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, remark, result_json, result_lines_json, created_by, created_at)
      VALUES(?,?,?,?,?,?,?)
    `).bind(result_id, job_id, remark, JSON.stringify({
      segment_id: openSeg.id,
      worker_id,
      worker_name: openSeg.worker_name || '',
      pick_doc_nos: segDocNos,
      minutes_worked: minutesSafe,
      result_note,
      kind: 'segment_finish'
    }), '[]', worker_id, t).run();

    // 4) 重算 active count；趟次状态：active>0 → working，否则 → pending（等待 finalize）
    const realCount = await recalcActiveCount(env, job_id, t);
    const newStatus = (realCount > 0) ? 'working' : 'pending';
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status=?, updated_at=? WHERE id=?"
    ).bind(newStatus, t, job_id).run();

    return {
      ok: true,
      job_id,
      segment_id: openSeg.id,
      minutes_worked: minutesSafe,
      finished_pick_doc_nos: segDocNos,
      job_status: newStatus,
      active_worker_count: realCount
    };
  });
});

// =====================================================
// PICK FINALIZE — 趟次整体完结（由创建人/主管/最后一位拣货人触发）
// 关闭所有残留 segment、所有未完结的 pwd、所有 pick_docs → completed；趟次 → completed
// =====================================================
route("v2_pick_job_finalize", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_pick_job_finalize", async () => {
    const t = now();
    const job = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE id=? AND job_type='pick_direct'"
    ).bind(job_id).first();
    if (!job) return { ok: false, error: "job_not_found", message: "趟次不存在" };
    if (job.status === 'completed') {
      return { ok: false, error: "already_completed", message: "趟次已完成，请勿重复提交" };
    }
    if (job.status === 'cancelled') {
      return { ok: false, error: "already_cancelled", message: "趟次已取消" };
    }

    // ---- 权限收口：仅 ADMINKEY 或 趟次创建人 可整趟完成 ----
    // OPSKEY 调用时必须 worker_id === job.created_by，否则拒绝
    const isAdminCall = isAdmin(body, env);
    if (!isAdminCall) {
      if (!worker_id) {
        return { ok: false, error: "missing_worker_id",
          message: "请提供拣货人ID / worker_id 필요" };
      }
      if (worker_id !== (job.created_by || '')) {
        return { ok: false, error: "forbidden_not_creator",
          message: "只有趟次创建人或主管/管理员可以整趟完成 / 차수 생성자 또는 관리자만 전체 완료 가능",
          required_creator: job.created_by || '' };
      }
    }

    // ---- 安全收尾：在岗人员尚未全部完成时禁止 finalize ----
    // 实时统计在岗 segment 数（不依赖 stored active_worker_count）
    const activeRs = await env.DB.prepare(
      "SELECT COUNT(*) as c, GROUP_CONCAT(worker_name, '、') as names FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
    ).bind(job_id).first();
    const activeCount = (activeRs && activeRs.c) || 0;
    if (activeCount > 0) {
      return { ok: false, error: "active_workers_still_working",
        message: "仍有人员正在拣货，请先让所有人完成本次拣货后再整趟完成 / 아직 작업 중인 인원이 있습니다. 모두 완료 후 다시 시도하세요",
        active_worker_count: activeCount,
        active_worker_names: (activeRs && activeRs.names) || '' };
    }

    // 1) 关闭残留 open segments（每段计算 minutes）— 此时正常无残留，仅作兜底
    const stale = await env.DB.prepare(
      "SELECT id, worker_id, joined_at FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
    ).bind(job_id).all();
    for (const s of (stale.results || [])) {
      const m = Math.max(0, Math.round(
        (new Date(t).getTime() - new Date(s.joined_at).getTime()) / 60000 * 10
      ) / 10);
      await env.DB.prepare(
        "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason='finalize' WHERE id=?"
      ).bind(t, m, s.id).run();
      // 关该 segment 名下未完成的 pwd
      await env.DB.prepare(
        "UPDATE v2_pick_worker_docs SET status='completed', finished_at=?, minutes_worked=? WHERE segment_id=? AND status='working'"
      ).bind(t, m, s.id).run();
    }

    // 2) pick_docs 全部 → completed
    await env.DB.prepare(
      "UPDATE v2_ops_job_pick_docs SET pick_status='completed', pick_finished_at=COALESCE(NULLIF(pick_finished_at,''), ?) WHERE job_id=? AND pick_status!='completed'"
    ).bind(t, job_id).run();

    // 3) 写整趟次最终结果记录
    const remark = String(body.remark || "").trim();
    const result_note = String(body.result_note || "").trim();
    const allDocs = await env.DB.prepare(
      "SELECT pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at"
    ).bind(job_id).all();
    const docNos = (allDocs.results || []).map(r => r.pick_doc_no);
    const totalsRs = await env.DB.prepare(
      `SELECT COUNT(DISTINCT worker_id) as worker_count,
              COUNT(*) as pwd_count,
              COALESCE(SUM(minutes_worked), 0) as total_minutes
       FROM v2_pick_worker_docs WHERE job_id=?`
    ).bind(job_id).first();
    const result_id = "RES-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, remark, result_json, result_lines_json, created_by, created_at)
      VALUES(?,?,?,?,?,?,?)
    `).bind(result_id, job_id, remark, JSON.stringify({
      kind: 'trip_finalize',
      pick_doc_nos: docNos,
      worker_count: (totalsRs && totalsRs.worker_count) || 0,
      total_pwd: (totalsRs && totalsRs.pwd_count) || 0,
      total_minutes: Math.round(((totalsRs && totalsRs.total_minutes) || 0) * 10) / 10,
      result_note,
      finalized_by: worker_id
    }), '[]', worker_id, t).run();

    // 4) job → completed
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(t, job_id).run();

    return {
      ok: true,
      job_id,
      finalized_at: t,
      pick_doc_count: docNos.length,
      worker_count: (totalsRs && totalsRs.worker_count) || 0,
      total_minutes: Math.round(((totalsRs && totalsRs.total_minutes) || 0) * 10) / 10
    };
  });
});

// =====================================================
// PICK DIRECT — 活跃趟次列表（含 pending 趟次，便于现场看到"待拣"）
// =====================================================
route("v2_pick_job_active_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);

  const rs = await env.DB.prepare(
    `SELECT * FROM v2_ops_jobs
     WHERE job_type='pick_direct' AND status IN ('pending','working','awaiting_close')
     AND is_temporary_interrupt=0
     ORDER BY created_at DESC LIMIT 50`
  ).all();

  const items = [];
  for (const job of (rs.results || [])) {
    const pds = await env.DB.prepare(
      `SELECT pick_doc_no, pick_status, pick_started_at, pick_finished_at
       FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at`
    ).bind(job.id).all();
    const workers = await env.DB.prepare(
      "SELECT worker_id, worker_name FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
    ).bind(job.id).all();
    const pwds = await env.DB.prepare(
      `SELECT pick_doc_no, worker_id, worker_name, status FROM v2_pick_worker_docs WHERE job_id=?`
    ).bind(job.id).all();

    const partsByDoc = {};
    for (const p of (pwds.results || [])) {
      if (!partsByDoc[p.pick_doc_no]) partsByDoc[p.pick_doc_no] = [];
      partsByDoc[p.pick_doc_no].push(p);
    }
    const docList = (pds.results || []).map(d => {
      const parts = partsByDoc[d.pick_doc_no] || [];
      const activeNames = Array.from(new Set(parts.filter(p => p.status === 'working').map(p => p.worker_name).filter(Boolean)));
      const allNames = Array.from(new Set(parts.map(p => p.worker_name).filter(Boolean)));
      return Object.assign({}, d, {
        active_picker_names: activeNames,
        all_picker_names: allNames,
        active_picker_count: activeNames.length,
        total_picker_count: allNames.length
      });
    });
    let pendingCnt = 0, workingCnt = 0, completedCnt = 0;
    for (const d of docList) {
      const st = d.pick_status || 'pending';
      if (st === 'completed') completedCnt++;
      else if (st === 'working') workingCnt++;
      else pendingCnt++;
    }
    items.push({
      id: job.id,
      display_no: job.display_no || '',
      status: job.status,
      active_worker_count: job.active_worker_count || 0,
      created_by: job.created_by || '',
      created_at: job.created_at,
      pick_doc_nos: docList.map(r => r.pick_doc_no),
      pick_docs: docList,
      pick_doc_pending_count: pendingCnt,
      pick_doc_working_count: workingCnt,
      pick_doc_completed_count: completedCnt,
      workers: (workers.results || []).map(w => ({ id: w.worker_id, name: w.worker_name }))
    });
  }
  return json({ ok: true, items });
});

// =====================================================
// PICK DIRECT — 加入趟次（已废弃；新流程必须扫描具体拣货单号）
// 保留路由仅为旧客户端兼容兜底，直接返回引导错误。
// =====================================================
route("v2_pick_job_join", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  return json({
    ok: false,
    error: "deprecated_use_start_by_docs",
    message: "新流程：实际拣货人请扫描手中的拣货单号开始拣货，不再支持直接加入趟次"
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

    // ---- Phase 1: 查询关联出库单（校验前置，不再先建后回滚） ----
    const linkedOb = await findOutboundByWorkOrder(env, work_order_no);
    const obId = linkedOb ? linkedOb.id : "";
    const obStatus = linkedOb ? (linkedOb.status || "") : "";

    // 出库单状态校验（前置于 job 创建）
    if (linkedOb) {
      if (obStatus === "completed") {
        return { ok: false, error: "bulk_order_already_completed",
          message: "该工单已完成，如需返工或追加操作，请在协同中心设为待再操作" };
      }
      if (obStatus === "cancelled") {
        return { ok: false, error: "bulk_order_cancelled",
          message: "该工单已取消，不能继续操作" };
      }
    }

    // ---- Phase 1.5: 跨任务类型互斥 ----
    // bulk_op 自身的 cross-job guard 在后面，这里先做全局互斥
    {
      const busy = await checkWorkerBusy(env, worker_id, null);
      if (busy && busy.job_type !== 'bulk_op') {
        return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };
      }
    }

    // ---- Phase 2: 查找或创建 job ----
    // 系统出库单：按 linked_outbound_order_id 查活跃 job（防不同编码裂开多条）
    // 纯手工工单号：按 related_doc_id 查
    let job = null;
    if (obId) {
      const existing = await env.DB.prepare(
        "SELECT * FROM v2_ops_jobs WHERE job_type='bulk_op' AND linked_outbound_order_id=? AND status IN ('pending','working','awaiting_close') AND is_temporary_interrupt=0 LIMIT 1"
      ).bind(obId).first();
      if (existing) job = existing;
    }
    if (!job) {
      const existing = await env.DB.prepare(
        "SELECT * FROM v2_ops_jobs WHERE job_type='bulk_op' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') AND is_temporary_interrupt=0 LIMIT 1"
      ).bind(work_order_no).first();
      if (existing) job = existing;
    }

    // Cross-job guard
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
      // 历史完成拦截
      // 系统出库单且 reopen_pending → 跳过拦截（允许再操作）
      // 系统出库单其他状态 → 按 linked_outbound_order_id 查历史
      // 纯手工工单号 → 按 related_doc_id 查历史
      if (obStatus !== "reopen_pending") {
        let lastJob = null;
        if (obId) {
          lastJob = await env.DB.prepare(
            "SELECT status FROM v2_ops_jobs WHERE job_type='bulk_op' AND linked_outbound_order_id=? ORDER BY created_at DESC LIMIT 1"
          ).bind(obId).first();
        }
        if (!lastJob) {
          lastJob = await env.DB.prepare(
            "SELECT status FROM v2_ops_jobs WHERE job_type='bulk_op' AND related_doc_id=? ORDER BY created_at DESC LIMIT 1"
          ).bind(work_order_no).first();
        }
        if (lastJob && lastJob.status === 'completed') {
          if (linkedOb) {
            return { ok: false, error: "bulk_order_already_completed",
              message: "该工单已完成，如需返工或追加操作，请在协同中心设为待再操作" };
          }
          return { ok: false, error: "bulk_work_order_already_completed",
            message: "该纯工单号已完成，不能再次操作。如需返工，请创建系统出库单或使用新工单号" };
        }
      }

      const started_from_reopen = (obStatus === "reopen_pending");
      const jobMeta = started_from_reopen ? JSON.stringify({ started_from_reopen_pending: true }) : "{}";

      job_id = "JOB-" + uid();
      is_new_job = true;
      await env.DB.prepare(`
        INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
          status, shared_result_json, linked_outbound_order_id,
          parent_job_id, is_temporary_interrupt, interrupt_type, created_by, created_at, updated_at, active_worker_count)
        VALUES(?,'order_op','bulk','bulk_op','work_order',?,'working',?,?,
          '',0,'',?,?,?,1)
      `).bind(job_id, work_order_no, jobMeta, obId, worker_id, t, t).run();
    }

    // ---- Phase 3: 创建 worker segment ----
    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    // ---- Phase 4: 出库单状态同步 ----
    if (linkedOb && (obStatus === "pending_issue" || obStatus === "issued" || obStatus === "reopen_pending")) {
      await env.DB.prepare(
        "UPDATE v2_outbound_orders SET status='working', updated_at=? WHERE id=?"
      ).bind(t, obId).run();
    }

    // ---- Phase 5: 返回结果 ----
    const ret = { ok: true, job_id, worker_seg_id: seg_id, is_new_job };
    if (linkedOb) {
      ret.linked_outbound = {
        id: obId,
        display_no: linkedOb.display_no || obId,
        customer: linkedOb.customer || "",
        destination: linkedOb.destination || "",
        po_no: linkedOb.po_no || "",
        wms_work_order_no: linkedOb.wms_work_order_no || "",
        planned_box_count: Number(linkedOb.planned_box_count || 0),
        planned_pallet_count: Number(linkedOb.planned_pallet_count || 0),
        instruction: linkedOb.instruction || "",
        status: obStatus
      };
    }
    return ret;
  });
});

// 出库单查找 helper：匹配顺序 id → display_no → wms_work_order_no
async function findOutboundByWorkOrder(env, workOrderNo) {
  if (!workOrderNo) return null;
  let row = await env.DB.prepare(
    "SELECT * FROM v2_outbound_orders WHERE id=? LIMIT 1"
  ).bind(workOrderNo).first();
  if (row) return row;
  row = await env.DB.prepare(
    "SELECT * FROM v2_outbound_orders WHERE display_no=? ORDER BY created_at DESC LIMIT 1"
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
    const leave_only = body.leave_only === true;

    const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (jobCheck && jobCheck.status === 'completed') {
      return { ok: false, error: "already_completed", message: "任务已完成，请勿重复提交" };
    }

    // Leave-only: close segments, recalc, set awaiting_close if no one left
    if (leave_only) {
      await closeAllOpenSegs(env, job_id, worker_id, t, 'leave');
      const leaveCount = await recalcActiveCount(env, job_id, t);
      if (leaveCount === 0) {
        await env.DB.prepare(
          "UPDATE v2_ops_jobs SET status='awaiting_close', updated_at=? WHERE id=? AND status='working'"
        ).bind(t, job_id).run();
      }
      return { ok: true, left: true, active_worker_count: leaveCount };
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

    // 口径联动：大货操作完成 → 通过强关联回写出库单（首次覆盖 / reopen 累加）
    const finishedJob = await env.DB.prepare(
      "SELECT linked_outbound_order_id, shared_result_json FROM v2_ops_jobs WHERE id=?"
    ).bind(job_id).first();
    const linkedObId = finishedJob ? (finishedJob.linked_outbound_order_id || "") : "";
    if (linkedObId) {
      const linkedOb = await env.DB.prepare(
        "SELECT * FROM v2_outbound_orders WHERE id=?"
      ).bind(linkedObId).first();
      if (linkedOb) {
        let jobMeta = {};
        try { jobMeta = JSON.parse(finishedJob.shared_result_json || "{}"); } catch(e) {}
        const isReopen = !!jobMeta.started_from_reopen_pending;

        const newBoxCount = isReopen
          ? Number(linkedOb.actual_box_count || 0) + resultData.total_operated_box_count
          : resultData.total_operated_box_count;
        const newPalletCount = isReopen
          ? Number(linkedOb.actual_pallet_count || 0) + resultData.pallet_count
          : resultData.pallet_count;

        await env.DB.prepare(
          "UPDATE v2_outbound_orders SET actual_box_count=?, actual_pallet_count=?, status='ready_to_ship', updated_at=? WHERE id=?"
        ).bind(newBoxCount, newPalletCount, t, linkedObId).run();
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

  // TODO(分页): 当前硬编码 LIMIT 200，与其他 v2_*_list 不同。
  //   后续如需"加载更多"，改用 pageParams(body) + LIMIT ? OFFSET ?，
  //   并调整下方 jobIds 关联查询为按页内 ids 关联。
  let sql = "SELECT * FROM v2_ops_jobs WHERE flow_stage='order_op'";
  const binds = [];
  if (job_type) { sql += " AND job_type=?"; binds.push(job_type); }
  if (start) { sql += " AND created_at>=?"; binds.push(start + "T00:00:00.000Z"); }
  if (end) { sql += " AND created_at<=?"; binds.push(end + "T23:59:59.999Z"); }
  sql += " ORDER BY created_at DESC LIMIT 200";
  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  const jobs = rs.results || [];
  if (jobs.length === 0) return json({ ok: true, items: [] });

  const jobIds = jobs.map(j => j.id);
  const placeholders = jobIds.map(() => '?').join(',');

  const [workersRs, pickDocsRs, resultsRs] = await Promise.all([
    env.DB.prepare(
      `SELECT job_id, worker_name, minutes_worked FROM v2_ops_job_workers WHERE job_id IN (${placeholders}) ORDER BY joined_at`
    ).bind(...jobIds).all(),
    env.DB.prepare(
      `SELECT job_id, pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id IN (${placeholders}) ORDER BY created_at`
    ).bind(...jobIds).all(),
    env.DB.prepare(
      `SELECT job_id, remark, result_json, created_at FROM v2_ops_job_results WHERE job_id IN (${placeholders}) ORDER BY created_at DESC`
    ).bind(...jobIds).all(),
  ]);

  const workersByJob = {};
  for (const w of (workersRs.results || [])) {
    if (!workersByJob[w.job_id]) workersByJob[w.job_id] = [];
    workersByJob[w.job_id].push(w);
  }
  const pickDocsByJob = {};
  for (const p of (pickDocsRs.results || [])) {
    if (!pickDocsByJob[p.job_id]) pickDocsByJob[p.job_id] = [];
    pickDocsByJob[p.job_id].push(p.pick_doc_no);
  }
  const latestResultByJob = {};
  for (const r of (resultsRs.results || [])) {
    if (!latestResultByJob[r.job_id]) latestResultByJob[r.job_id] = r;
  }

  const items = jobs.map(job => {
    const workerRows = workersByJob[job.id] || [];
    const names = [...new Set(workerRows.map(w => w.worker_name).filter(Boolean))];
    const totalMin = workerRows.reduce((s, w) => s + (Number(w.minutes_worked) || 0), 0);
    const pickDocs = (job.job_type === 'pick_direct') ? (pickDocsByJob[job.id] || []) : [];
    const lr = latestResultByJob[job.id];
    let resultData = null, remark = "";
    if (lr) {
      remark = lr.remark || "";
      try { resultData = JSON.parse(lr.result_json); } catch(e) {}
    }
    return {
      ...job,
      worker_names: names,
      worker_names_text: names.join(", "),
      total_minutes_worked: Math.round(totalMin),
      pick_doc_nos: pickDocs,
      result_data: resultData,
      result_remark: remark
    };
  });

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

  // 5. 出库单↔job 状态错位：出库单 completed 但仍有活跃 job，或出库单 working 但无活跃 job
  const obJobMismatch = await env.DB.prepare(`
    SELECT o.id as outbound_id, o.display_no, o.status as ob_status,
           j.id as job_id, j.status as job_status, j.job_type
    FROM v2_outbound_orders o
    JOIN v2_ops_jobs j ON j.linked_outbound_order_id = o.id
    WHERE (o.status = 'shipped' AND j.status IN ('working','awaiting_close','pending'))
       OR (o.status = 'working' AND j.status IN ('completed','cancelled'))
    ORDER BY o.created_at DESC LIMIT 50
  `).all();

  return json({
    ok: true,
    multi_open_segments: multiSegs.results || [],
    cross_job_workers: crossJob.results || [],
    orphan_open_segments: orphanSegs.results || [],
    count_drift_jobs: drifts,
    outbound_job_status_mismatch: obJobMismatch.results || []
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
// VERIFY CENTER — 扫码核对（客服上传批次 + 现场扫码核对）
// =====================================================

// 1) 上传核对批次（客服上传 Excel → 前端解析后 POST rows）
// 请求体：{ batch_no?, remark?, rows: [{ barcode, planned_box_count, customer_name, row_no? }] }
// 规则：
// - barcode/客户名必填；planned_box_count 必须是正整数
// - 同一 barcode 出现多次：合并箱数；客户名不同则报错（可能串单）
// - 批次 planned_qty = SUM(items.planned_box_count)
route("v2_verify_batch_upload", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const remark = String(body.remark || "").trim();
  const created_by = String(body.created_by || "").trim();
  let batch_no = String(body.batch_no || "").trim();
  const rawRows = Array.isArray(body.rows) ? body.rows : [];
  if (rawRows.length === 0) return err("empty_rows");

  // ---- 清洗+合并 ----
  const errors = [];
  const merged = {}; // barcode -> { barcode, planned_box_count, customer_name, row_nos:[] }
  rawRows.forEach((r, idx) => {
    const row_no = Number(r && r.row_no) || (idx + 2); // 默认从表格第 2 行起（表头 1）
    const bc = String((r && r.barcode) || "").trim();
    const cn = String((r && r.customer_name) || "").trim();
    const bcRaw = r && r.planned_box_count;
    const bc_n = typeof bcRaw === 'number' ? bcRaw : parseInt(String(bcRaw || "").trim(), 10);
    if (!bc && !cn && !bc_n) return; // 整行空 → 跳过
    if (!bc) { errors.push({ row: row_no, msg: "条码为空 / 바코드 비어있음" }); return; }
    if (!cn) { errors.push({ row: row_no, msg: "客户名为空 / 고객사 비어있음" }); return; }
    if (!Number.isFinite(bc_n) || bc_n <= 0 || !Number.isInteger(bc_n)) {
      errors.push({ row: row_no, msg: "计划箱数必须是正整数 / 계획 박스수는 양의 정수" }); return;
    }
    if (!merged[bc]) {
      merged[bc] = { barcode: bc, planned_box_count: bc_n, customer_name: cn, row_nos: [row_no] };
    } else {
      const prev = merged[bc];
      if (prev.customer_name !== cn) {
        errors.push({ row: row_no, msg: "条码 " + bc + " 在第 " + prev.row_nos.join(",") + " 行属于客户 " + prev.customer_name + "，此行却写 " + cn + "（可能串单）" });
        return;
      }
      prev.planned_box_count += bc_n;
      prev.row_nos.push(row_no);
    }
  });
  if (errors.length > 0) return json({ ok: false, error: "row_errors", errors });
  const items = Object.values(merged);
  if (items.length === 0) return err("no_valid_rows");

  return withIdem(env, body, "v2_verify_batch_upload", async () => {
    const t = now();
    const id = "VB-" + uid();
    if (!batch_no) {
      const dateStr = kstToday().replace(/-/g, '');
      batch_no = 'VBT-' + dateStr + '-' + Date.now().toString(36).slice(-4).toUpperCase();
    }
    const dup = await env.DB.prepare("SELECT id FROM v2_verify_batches WHERE batch_no=?").bind(batch_no).first();
    if (dup) return { ok: false, error: "batch_no_duplicate", message: "批次号已存在 / 배치번호 중복" };

    const planned_qty = items.reduce((s, it) => s + (it.planned_box_count || 0), 0);
    // batch 级 customer_name：单客户直写；多客户存"(多客户/다고객)"
    const distinctCu = {};
    items.forEach(it => { distinctCu[it.customer_name] = true; });
    const distinctCuList = Object.keys(distinctCu);
    const batchCustomerName = distinctCuList.length === 1 ? distinctCuList[0] : ("(多客户/다고객 " + distinctCuList.length + ")");

    await env.DB.prepare(`
      INSERT INTO v2_verify_batches(id, batch_no, customer_name, planned_qty, status,
        remark, created_by, created_at, updated_at)
      VALUES(?,?,?,?,'pending',?,?,?,?)
    `).bind(id, batch_no, batchCustomerName, planned_qty, remark, created_by, t, t).run();

    for (const it of items) {
      const item_id = "VBI-" + uid();
      await env.DB.prepare(`
        INSERT INTO v2_verify_batch_items(id, batch_id, barcode, planned_qty, planned_box_count, customer_name, created_at)
        VALUES(?,?,?,?,?,?,?)
      `).bind(item_id, id, it.barcode, it.planned_box_count, it.planned_box_count, it.customer_name, t).run();
    }

    return {
      ok: true,
      id,
      batch_no,
      item_count: items.length,
      planned_total_box_count: planned_qty,
      distinct_customer_count: distinctCuList.length
    };
  });
});

// 2) 批次列表（带扫描汇总）
route("v2_verify_batch_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const status = String(body.status || "").trim();
  const customer_name = String(body.customer_name || "").trim();
  const start_date = String(body.start_date || "").trim();
  const end_date = String(body.end_date || "").trim();

  const { limit, offset } = pageParams(body);
  let sql = "SELECT * FROM v2_verify_batches WHERE 1=1";
  const binds = [];
  if (status) { sql += " AND status=?"; binds.push(status); }
  if (customer_name) { sql += " AND customer_name LIKE ?"; binds.push('%' + customer_name + '%'); }
  if (start_date) { sql += " AND created_at >= ?"; binds.push(start_date); }
  if (end_date) { sql += " AND created_at <= ?"; binds.push(end_date + 'T23:59:59Z'); }
  sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?";
  binds.push(limit, offset);

  const rs = await env.DB.prepare(sql).bind(...binds).all();
  const batches = rs.results || [];
  if (batches.length === 0) return json({ ok: true, items: [], limit, offset });

  // 批量聚合 scan_logs：ok 数 / 异常数
  const ids = batches.map(b => b.id);
  const placeholders = ids.map(() => '?').join(',');
  const statRs = await env.DB.prepare(
    `SELECT batch_id,
            SUM(CASE WHEN scan_result='ok' THEN 1 ELSE 0 END) AS ok_count,
            SUM(CASE WHEN scan_result IN ('not_found','overflow') THEN 1 ELSE 0 END) AS abnormal_count
     FROM v2_verify_scan_logs WHERE batch_id IN (${placeholders}) GROUP BY batch_id`
  ).bind(...ids).all();
  const statMap = {};
  (statRs.results || []).forEach(r => { statMap[r.batch_id] = r; });

  const items = batches.map(b => {
    const s = statMap[b.id] || {};
    return {
      ...b,
      scanned_ok_count: Number(s.ok_count || 0),
      abnormal_count: Number(s.abnormal_count || 0)
    };
  });
  return json({ ok: true, items, limit, offset });
});

// 3) 批次详情（按"条码对应计划箱数"出每行状态 + 聚合异常）
route("v2_verify_batch_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");

  const batch = await env.DB.prepare("SELECT * FROM v2_verify_batches WHERE id=?").bind(id).first();
  if (!batch) return err("not found", 404);

  const itemsRs = await env.DB.prepare(
    "SELECT * FROM v2_verify_batch_items WHERE batch_id=? ORDER BY created_at"
  ).bind(id).all();
  const logsRs = await env.DB.prepare(
    "SELECT * FROM v2_verify_scan_logs WHERE batch_id=? ORDER BY scanned_at DESC LIMIT 500"
  ).bind(id).all();
  // 每个 barcode 已扫 ok 次数
  const okByBcRs = await env.DB.prepare(
    "SELECT barcode, COUNT(*) AS c FROM v2_verify_scan_logs WHERE batch_id=? AND scan_result='ok' GROUP BY barcode"
  ).bind(id).all();

  const itemsRaw = itemsRs.results || [];
  const logs = logsRs.results || [];
  const okByBc = {};
  (okByBcRs.results || []).forEach(r => { okByBc[r.barcode] = Number(r.c || 0); });

  // 每个 item 的状态
  let planned_total_box_count = 0, scanned_ok_total_count = 0;
  let shortage_count = 0, overflow_count_items = 0, ok_items = 0, not_scanned_count = 0;
  const items = itemsRaw.map(it => {
    const planned = Number(it.planned_box_count || it.planned_qty || 1);
    const ok = Number(okByBc[it.barcode] || 0);
    planned_total_box_count += planned;
    scanned_ok_total_count += ok;
    let st;
    if (ok === 0) { st = 'not_scanned'; not_scanned_count++; }
    else if (ok < planned) { st = 'shortage'; shortage_count++; }
    else if (ok === planned) { st = 'ok'; ok_items++; }
    else { st = 'overflow'; overflow_count_items++; }
    return {
      id: it.id,
      barcode: it.barcode,
      customer_name: it.customer_name || '',
      planned_box_count: planned,
      scanned_ok_count: ok,
      diff_count: ok - planned,
      status: st
    };
  });

  // scan_logs 统计 + 为每条 log 挂 customer_name（如果 item 匹配到）
  const itemMap = {};
  itemsRaw.forEach(it => { itemMap[it.barcode] = it; });
  let log_ok = 0, log_overflow = 0, log_not_found = 0, log_duplicate = 0;
  const palletMap = {};
  const enrichedLogs = logs.map(l => {
    if (l.scan_result === 'ok') log_ok++;
    else if (l.scan_result === 'overflow') log_overflow++;
    else if (l.scan_result === 'not_found') log_not_found++;
    else if (l.scan_result === 'duplicate') log_duplicate++;
    const p = l.pallet_no || '(未填/미기입)';
    if (!palletMap[p]) palletMap[p] = { pallet_no: p, scanned_ok_count: 0, abnormal_count: 0 };
    if (l.scan_result === 'ok') palletMap[p].scanned_ok_count++;
    else palletMap[p].abnormal_count++;
    const matched = itemMap[l.barcode];
    return { ...l, customer_name: matched ? (matched.customer_name || '') : '' };
  });
  const pallet_summary = Object.values(palletMap).sort((a, b) =>
    (b.scanned_ok_count + b.abnormal_count) - (a.scanned_ok_count + a.abnormal_count)
  );

  const abnormal_count = shortage_count + overflow_count_items + log_not_found;

  return json({
    ok: true,
    batch,
    items,
    scan_logs: enrichedLogs,
    summary: {
      planned_total_box_count,
      scanned_ok_total_count,
      // 条码级异常条数
      ok_count: ok_items,
      shortage_count,
      overflow_count: overflow_count_items,
      not_scanned_count,
      // 扫码流水级统计
      log_ok_count: log_ok,
      log_overflow_count: log_overflow,
      log_not_found_count: log_not_found,
      log_duplicate_count: log_duplicate,
      not_found_count: log_not_found,
      abnormal_count,
      diff: planned_total_box_count - scanned_ok_total_count
    },
    pallet_summary
  });
});

// 4) 批次状态变更（completed / cancelled）
route("v2_verify_batch_update_status", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const target = String(body.status || "").trim();
  const actor = String(body.actor || body.worker_id || body.created_by || "").trim();
  if (!id || !target) return err("missing id or status");
  const allowed = ['pending', 'verifying', 'completed', 'cancelled'];
  if (allowed.indexOf(target) === -1) return err("bad status");

  return withIdem(env, body, "v2_verify_batch_update_status", async () => {
    const row = await env.DB.prepare("SELECT * FROM v2_verify_batches WHERE id=?").bind(id).first();
    if (!row) return { ok: false, error: "batch not found" };
    if (row.status === 'completed' && target !== 'completed') return { ok: false, error: "already_completed" };
    if (row.status === 'cancelled' && target !== 'cancelled') return { ok: false, error: "already_cancelled" };

    const t = now();
    let sql = "UPDATE v2_verify_batches SET status=?, updated_at=?";
    const binds = [target, t];
    if (target === 'completed') { sql += ", completed_at=?, completed_by=?"; binds.push(t, actor); }
    if (target === 'cancelled') { sql += ", cancelled_at=?, cancelled_by=?"; binds.push(t, actor); }
    sql += " WHERE id=?";
    binds.push(id);
    await env.DB.prepare(sql).bind(...binds).run();
    return { ok: true, id, status: target };
  });
});

// 5) 扫码提交：现场逐条扫，每扫一次必写入 scan_logs（含托盘号）
route("v2_verify_scan_submit", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const batch_id = String(body.batch_id || "").trim();
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const pallet_no = String(body.pallet_no || "").trim();
  const barcode = String(body.barcode || "").trim();
  if (!batch_id || !barcode) return err("missing batch_id or barcode");
  if (!pallet_no) return err("missing pallet_no");
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_verify_scan_submit", async () => {
    // 校验批次状态
    const batch = await env.DB.prepare("SELECT * FROM v2_verify_batches WHERE id=?").bind(batch_id).first();
    if (!batch) return { ok: false, error: "batch_not_found" };
    if (batch.status === 'completed' || batch.status === 'cancelled') {
      return { ok: false, error: "batch_closed", message: "批次已 " + batch.status };
    }
    // 校验 job 状态
    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!job) return { ok: false, error: "job_not_found" };
    if (job.job_type !== 'verify_scan') return { ok: false, error: "job_type_mismatch" };
    if (job.status !== 'working') return { ok: false, error: "job_not_working", message: "任务不在作业中" };

    // 匹配 batch_items
    const item = await env.DB.prepare(
      "SELECT * FROM v2_verify_batch_items WHERE batch_id=? AND barcode=? LIMIT 1"
    ).bind(batch_id, barcode).first();

    // 以条码对应的 planned_box_count 为准；只在超过计划箱数时才判为 overflow
    let scan_result, message = '';
    let customer_name = '';
    let planned_box_count = 0;
    let barcode_ok_before = 0;
    if (!item) {
      scan_result = 'not_found';
      message = '条码不在本批次 / 배치에 없는 바코드';
    } else {
      customer_name = item.customer_name || '';
      planned_box_count = Math.max(1, Number(item.planned_box_count || item.planned_qty || 1));
      const okRs = await env.DB.prepare(
        "SELECT COUNT(*) AS c FROM v2_verify_scan_logs WHERE batch_id=? AND barcode=? AND scan_result='ok'"
      ).bind(batch_id, barcode).first();
      barcode_ok_before = okRs ? Number(okRs.c || 0) : 0;
      if (barcode_ok_before < planned_box_count) {
        scan_result = 'ok';
      } else {
        scan_result = 'overflow';
        message = '超出计划箱数 / 계획 박스수 초과';
      }
    }

    const t = now();
    const log_id = "VSL-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_verify_scan_logs(id, batch_id, job_id, worker_id, worker_name,
        pallet_no, barcode, scan_result, message, scanned_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)
    `).bind(log_id, batch_id, job_id, worker_id, worker_name,
        pallet_no, barcode, scan_result, message, t).run();

    // 条码级实时：当前扫码后的 ok 次数 / 差异
    const barcode_ok_now = scan_result === 'ok' ? (barcode_ok_before + 1) : barcode_ok_before;
    const diff_count = barcode_ok_now - planned_box_count;

    // 批次级汇总
    const sumRs = await env.DB.prepare(
      `SELECT
        SUM(CASE WHEN scan_result='ok' THEN 1 ELSE 0 END) AS ok_count,
        SUM(CASE WHEN scan_result='not_found' THEN 1 ELSE 0 END) AS nf_count,
        SUM(CASE WHEN scan_result='overflow' THEN 1 ELSE 0 END) AS of_count
       FROM v2_verify_scan_logs WHERE batch_id=?`
    ).bind(batch_id).first();
    const summary = {
      planned_total_box_count: batch.planned_qty || 0,
      scanned_ok_total_count: Number((sumRs && sumRs.ok_count) || 0),
      not_found_count: Number((sumRs && sumRs.nf_count) || 0),
      overflow_count: Number((sumRs && sumRs.of_count) || 0)
    };
    summary.abnormal_count = summary.not_found_count + summary.overflow_count;
    summary.diff = summary.planned_total_box_count - summary.scanned_ok_total_count;

    return {
      ok: true,
      scan_result,
      message,
      log_id,
      barcode_info: {
        barcode,
        customer_name,
        planned_box_count,
        scanned_ok_count: barcode_ok_now,
        diff_count
      },
      summary
    };
  });
});

// 6) 开始扫码核对（专用包装：更新批次状态 + 创建/加入 verify_scan job）
route("v2_verify_job_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const batch_id = String(body.batch_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!batch_id) return err("missing batch_id");
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_verify_job_start", async () => {
    const batch = await env.DB.prepare("SELECT * FROM v2_verify_batches WHERE id=?").bind(batch_id).first();
    if (!batch) return { ok: false, error: "batch_not_found" };
    if (batch.status === 'completed' || batch.status === 'cancelled') {
      return { ok: false, error: "batch_closed", message: "批次已 " + batch.status };
    }

    const t = now();

    // 多任务互斥（允许已在同 job 的本人重入）
    let existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE related_doc_type='verify_batch' AND related_doc_id=? AND job_type='verify_scan' AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(batch_id).first();

    const busy = await checkWorkerBusy(env, worker_id, existing ? existing.id : null);
    if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };

    let job_id, is_new_job = false;
    if (existing) {
      job_id = existing.id;
      const dup = await findOpenSeg(env, job_id, worker_id);
      if (!dup) {
        await env.DB.prepare(
          "UPDATE v2_ops_jobs SET active_worker_count=active_worker_count+1, status='working', updated_at=? WHERE id=?"
        ).bind(t, job_id).run();
        const seg_id = "WS-" + uid();
        await env.DB.prepare(`
          INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
          VALUES(?,?,?,?,?)
        `).bind(seg_id, job_id, worker_id, worker_name, t).run();
        // 更新批次状态（pending -> verifying）
        if (batch.status === 'pending') {
          await env.DB.prepare("UPDATE v2_verify_batches SET status='verifying', updated_at=? WHERE id=?").bind(t, batch_id).run();
        }
        return { ok: true, job_id, worker_seg_id: seg_id, is_new_job: false, batch_id };
      }
      // 已在同 job，直接返回
      return { ok: true, job_id, worker_seg_id: dup.id, is_new_job: false, already_joined: true, batch_id };
    }

    job_id = "JOB-" + uid();
    is_new_job = true;
    await env.DB.prepare(`
      INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
        status, created_by, created_at, updated_at, active_worker_count)
      VALUES(?, 'order_op', '', 'verify_scan', 'verify_batch', ?, 'working', ?, ?, ?, 1)
    `).bind(job_id, batch_id, worker_id, t, t).run();

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    if (batch.status === 'pending') {
      await env.DB.prepare("UPDATE v2_verify_batches SET status='verifying', updated_at=? WHERE id=?").bind(t, batch_id).run();
    }

    return { ok: true, job_id, worker_seg_id: seg_id, is_new_job, batch_id };
  });
});

// 7) 完成扫码核对：结束 segment（+可选关 job + 可选关批次）
route("v2_verify_job_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const complete_job = body.complete_job !== false; // 默认 true
  const complete_batch = !!body.complete_batch;     // 默认 false
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_verify_job_finish", async () => {
    const t = now();
    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!job) return { ok: false, error: "job_not_found" };
    if (job.job_type !== 'verify_scan') return { ok: false, error: "job_type_mismatch" };
    if (job.status === 'completed') return { ok: false, error: "already_completed" };

    // 关闭本人 segment
    await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');

    if (complete_job) {
      // 关闭所有 open segments
      await env.DB.prepare(
        "UPDATE v2_ops_job_workers SET left_at=?, leave_reason='job_completed' WHERE job_id=? AND left_at=''"
      ).bind(t, job_id).run();

      // 写入结果摘要
      const batch_id = job.related_doc_id || '';
      const sumRs = await env.DB.prepare(
        `SELECT
          SUM(CASE WHEN scan_result='ok' THEN 1 ELSE 0 END) AS ok_count,
          SUM(CASE WHEN scan_result='duplicate' THEN 1 ELSE 0 END) AS dup_count,
          SUM(CASE WHEN scan_result='not_found' THEN 1 ELSE 0 END) AS nf_count,
          SUM(CASE WHEN scan_result='overflow' THEN 1 ELSE 0 END) AS of_count
         FROM v2_verify_scan_logs WHERE batch_id=?`
      ).bind(batch_id).first();
      const batch = await env.DB.prepare("SELECT planned_qty FROM v2_verify_batches WHERE id=?").bind(batch_id).first();
      const summary = {
        batch_id,
        planned_qty: (batch && batch.planned_qty) || 0,
        scanned_ok_count: Number((sumRs && sumRs.ok_count) || 0),
        duplicate_count: Number((sumRs && sumRs.dup_count) || 0),
        not_found_count: Number((sumRs && sumRs.nf_count) || 0),
        overflow_count: Number((sumRs && sumRs.of_count) || 0)
      };
      const result_id = "RES-" + uid();
      await env.DB.prepare(`
        INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, created_by, created_at)
        VALUES(?,?,?,?,?,?,?,?)
      `).bind(result_id, job_id, summary.scanned_ok_count, 0,
          String(body.remark || ""), JSON.stringify(summary), worker_id, t).run();

      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
      ).bind(t, job_id).run();

      if (complete_batch && batch_id) {
        await env.DB.prepare(
          "UPDATE v2_verify_batches SET status='completed', completed_at=?, completed_by=?, updated_at=? WHERE id=?"
        ).bind(t, worker_id, t, batch_id).run();
      }
    } else {
      // 仅本人退出，若无剩余 open seg 则 awaiting_close
      const realCount = await recalcActiveCount(env, job_id, t);
      if (realCount <= 0 && job.status === 'working') {
        await env.DB.prepare("UPDATE v2_ops_jobs SET status='awaiting_close', updated_at=? WHERE id=?").bind(t, job_id).run();
      }
    }

    return { ok: true, job_id, completed: complete_job, batch_completed: complete_job && complete_batch };
  });
});

// =====================================================
// 数据看板 V1 — 单子数据 / 工时分析 / WMS 导入 / 管理看板
// 全部只读为主；WMS 导入写入独立 v2_wms_import_* 表，不触碰现场工时
// =====================================================

// 1) 单子数据 — 列表（按筛选 + 聚合 worker count / total minutes / start / end）
route("v2_dashboard_order_list", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const start_date = String(body.start_date || "").trim();
  const end_date = String(body.end_date || "").trim();
  const flow_stage = String(body.flow_stage || "").trim();
  const job_type = String(body.job_type || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const doc_no = String(body.doc_no || "").trim();

  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 100;
  if (limit > 500) limit = 500;
  let offset = parseInt(body.offset, 10);
  if (!Number.isFinite(offset) || offset < 0) offset = 0;

  let where = "WHERE 1=1";
  const binds = [];
  if (start_date) { where += " AND DATE(j.created_at)>=?"; binds.push(start_date); }
  if (end_date)   { where += " AND DATE(j.created_at)<=?"; binds.push(end_date); }
  if (flow_stage) { where += " AND j.flow_stage=?"; binds.push(flow_stage); }
  if (job_type)   { where += " AND j.job_type=?"; binds.push(job_type); }
  if (doc_no) {
    where += " AND (j.display_no LIKE ? OR j.related_doc_id LIKE ? OR j.linked_outbound_order_id LIKE ?)";
    const pat = "%" + doc_no + "%";
    binds.push(pat, pat, pat);
  }
  if (worker_name) {
    where += " AND EXISTS (SELECT 1 FROM v2_ops_job_workers w WHERE w.job_id=j.id AND w.worker_name LIKE ?)";
    binds.push("%" + worker_name + "%");
  }

  const totalRs = await env.DB.prepare(
    `SELECT COUNT(*) as c FROM v2_ops_jobs j ${where}`
  ).bind(...binds).first();
  const total = (totalRs && totalRs.c) || 0;

  const sql = `
    SELECT
      j.id, j.display_no, j.related_doc_type, j.related_doc_id, j.linked_outbound_order_id,
      j.flow_stage, j.biz_class, j.job_type, j.status, j.created_at, j.updated_at,
      (SELECT COUNT(DISTINCT w2.worker_id) FROM v2_ops_job_workers w2 WHERE w2.job_id=j.id) AS worker_count,
      (SELECT COALESCE(SUM(w2.minutes_worked),0) FROM v2_ops_job_workers w2 WHERE w2.job_id=j.id) AS total_minutes,
      (SELECT MIN(w2.joined_at) FROM v2_ops_job_workers w2 WHERE w2.job_id=j.id) AS started_at,
      (SELECT MAX(w2.left_at) FROM v2_ops_job_workers w2 WHERE w2.job_id=j.id AND w2.left_at!='') AS ended_at,
      (SELECT COALESCE(SUM(box_count),0) FROM v2_ops_job_results WHERE job_id=j.id) AS box_count_sum,
      (SELECT COALESCE(SUM(pallet_count),0) FROM v2_ops_job_results WHERE job_id=j.id) AS pallet_count_sum,
      (SELECT SUBSTR(COALESCE(GROUP_CONCAT(remark, ' | '), ''), 1, 100) FROM v2_ops_job_results WHERE job_id=j.id AND remark!='') AS result_remarks_short
    FROM v2_ops_jobs j ${where}
    ORDER BY j.created_at DESC LIMIT ? OFFSET ?`;
  const rs = await env.DB.prepare(sql).bind(...binds, limit, offset).all();
  const items = (rs.results || []).map(r => Object.assign({}, r, {
    total_minutes: round1(r.total_minutes),
  }));
  return json({ ok: true, items, total, limit, offset });
});

// 2) 单子数据 — 详情
route("v2_dashboard_order_detail", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  if (!job_id) return err("missing job_id");

  const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
  if (!job) return err("not found", 404);

  const [workersRs, resultsRs, pickDocsRs] = await Promise.all([
    env.DB.prepare(
      "SELECT id, worker_id, worker_name, joined_at, left_at, minutes_worked, leave_reason FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at ASC LIMIT 200"
    ).bind(job_id).all(),
    env.DB.prepare(
      "SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at ASC LIMIT 200"
    ).bind(job_id).all(),
    env.DB.prepare(
      "SELECT id, segment_id, worker_id, worker_name, pick_doc_no, status, started_at AS joined_at, finished_at AS left_at, minutes_worked FROM v2_pick_worker_docs WHERE job_id=? ORDER BY started_at ASC LIMIT 500"
    ).bind(job_id).all()
  ]);

  return json({
    ok: true,
    job,
    workers: workersRs.results || [],
    results: resultsRs.results || [],
    pick_worker_docs: pickDocsRs.results || []
  });
});

// 2.5) 单子数据 — 导出（一次性聚合 worker / result / pick / 关联单据；不要 N+1）
route("v2_dashboard_order_export", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const start_date = String(body.start_date || "").trim();
  const end_date = String(body.end_date || "").trim();
  const flow_stage = String(body.flow_stage || "").trim();
  const job_type = String(body.job_type || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const doc_no = String(body.doc_no || "").trim();

  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 5000;
  if (limit > 10000) limit = 10000;

  let where = "WHERE 1=1";
  const binds = [];
  if (start_date) { where += " AND DATE(j.created_at)>=?"; binds.push(start_date); }
  if (end_date)   { where += " AND DATE(j.created_at)<=?"; binds.push(end_date); }
  if (flow_stage) { where += " AND j.flow_stage=?"; binds.push(flow_stage); }
  if (job_type)   { where += " AND j.job_type=?"; binds.push(job_type); }
  if (doc_no) {
    where += " AND (j.display_no LIKE ? OR j.related_doc_id LIKE ? OR j.linked_outbound_order_id LIKE ?)";
    const pat = "%" + doc_no + "%";
    binds.push(pat, pat, pat);
  }
  if (worker_name) {
    where += " AND EXISTS (SELECT 1 FROM v2_ops_job_workers w WHERE w.job_id=j.id AND w.worker_name LIKE ?)";
    binds.push("%" + worker_name + "%");
  }

  const jobsRs = await env.DB.prepare(
    `SELECT id, display_no, related_doc_type, related_doc_id, linked_outbound_order_id,
            flow_stage, biz_class, job_type, status, created_at, updated_at
     FROM v2_ops_jobs j ${where}
     ORDER BY j.created_at DESC LIMIT ?`
  ).bind(...binds, limit).all();
  const jobs = jobsRs.results || [];
  if (jobs.length === 0) return json({ ok: true, rows: [], total: 0 });

  const jobIds = jobs.map(j => j.id);
  const inboundIds = [...new Set(jobs.filter(j => j.related_doc_type === 'inbound' && j.related_doc_id).map(j => j.related_doc_id))];
  const outboundIds = [...new Set(jobs.flatMap(j => {
    const ids = [];
    if (j.related_doc_type === 'outbound' && j.related_doc_id) ids.push(j.related_doc_id);
    if (j.linked_outbound_order_id) ids.push(j.linked_outbound_order_id);
    return ids;
  }))];
  const pickJobIds = jobs.filter(j => j.job_type === 'pick_direct').map(j => j.id);

  // ---- 批量 IN 查询 helper（D1 prepared statement 单条最多 100 bind 参数，CHUNK 取 80 留余量）----
  async function batchSelectIn(sqlTemplate, ids) {
    const out = [];
    const CHUNK = 80;
    for (let i = 0; i < ids.length; i += CHUNK) {
      const chunk = ids.slice(i, i + CHUNK);
      const placeholders = chunk.map(() => '?').join(',');
      const sql = sqlTemplate.replace('PLACEHOLDER', placeholders);
      const rs = await env.DB.prepare(sql).bind(...chunk).all();
      if (rs.results) out.push(...rs.results);
    }
    return out;
  }

  const [workersAll, resultsAll, pickAll, inboundAll, outboundAll] = await Promise.all([
    batchSelectIn(
      `SELECT id, job_id, worker_id, worker_name, joined_at, left_at, minutes_worked, leave_reason
       FROM v2_ops_job_workers WHERE job_id IN (PLACEHOLDER) ORDER BY joined_at ASC`,
      jobIds
    ),
    batchSelectIn(
      `SELECT id, job_id, box_count, pallet_count, remark, result_json, created_by, created_at
       FROM v2_ops_job_results WHERE job_id IN (PLACEHOLDER) ORDER BY created_at ASC`,
      jobIds
    ),
    pickJobIds.length > 0 ? batchSelectIn(
      `SELECT id, job_id, worker_id, worker_name, pick_doc_no, status, started_at, finished_at, minutes_worked
       FROM v2_pick_worker_docs WHERE job_id IN (PLACEHOLDER) ORDER BY started_at ASC`,
      pickJobIds
    ) : Promise.resolve([]),
    inboundIds.length > 0 ? batchSelectIn(
      `SELECT id, customer, display_no, external_inbound_no, accounted, accounted_by, accounted_at
       FROM v2_inbound_plans WHERE id IN (PLACEHOLDER)`,
      inboundIds
    ) : Promise.resolve([]),
    outboundIds.length > 0 ? batchSelectIn(
      `SELECT id, customer, display_no, destination, po_no, wms_work_order_no,
              planned_box_count, planned_pallet_count, actual_box_count, actual_pallet_count,
              accounted, accounted_by, accounted_at
       FROM v2_outbound_orders WHERE id IN (PLACEHOLDER)`,
      outboundIds
    ) : Promise.resolve([])
  ]);

  // ---- group by job_id ----
  const workersByJob = {}, resultsByJob = {}, pickByJob = {};
  workersAll.forEach(w => { (workersByJob[w.job_id] = workersByJob[w.job_id] || []).push(w); });
  resultsAll.forEach(r => { (resultsByJob[r.job_id] = resultsByJob[r.job_id] || []).push(r); });
  pickAll.forEach(p => { (pickByJob[p.job_id] = pickByJob[p.job_id] || []).push(p); });
  const inboundById = {}, outboundById = {};
  inboundAll.forEach(d => { inboundById[d.id] = d; });
  outboundAll.forEach(d => { outboundById[d.id] = d; });

  function tryParseDiff(rj) {
    if (!rj) return '';
    try {
      const o = JSON.parse(rj);
      const v = o.diff_note || o.diff_notes || o.diff || o['差异'] || o['差异说明'] || '';
      return v ? String(v) : '';
    } catch (e) { return ''; }
  }

  const out = jobs.map(j => {
    const ws = workersByJob[j.id] || [];
    const rs = resultsByJob[j.id] || [];
    const ps = pickByJob[j.id] || [];

    const workerNames = [...new Set(ws.map(w => w.worker_name).filter(Boolean))].join('、');
    const total_minutes = round1(ws.reduce((s, w) => s + (Number(w.minutes_worked) || 0), 0));
    const started_at = ws.reduce((m, w) => (!m || (w.joined_at && w.joined_at < m)) ? w.joined_at : m, '');
    const ended_at = ws.reduce((m, w) => (w.left_at && w.left_at > m) ? w.left_at : m, '');

    const box_sum = rs.reduce((s, r) => s + (Number(r.box_count) || 0), 0);
    const pallet_sum = rs.reduce((s, r) => s + (Number(r.pallet_count) || 0), 0);
    const remarks = rs.map(r => r.remark || '').filter(Boolean).join(' | ');
    const diffs = rs.map(r => tryParseDiff(r.result_json)).filter(Boolean).join(' | ');
    const resultLines = rs.map(r =>
      `[#${r.id || ''}] 箱${r.box_count||0}/板${r.pallet_count||0} ` +
      (r.remark ? `备注:${r.remark} ` : '') +
      (r.created_by ? `by ${r.created_by} ` : '') +
      (r.created_at ? `@${r.created_at}` : '')
    ).join(' || ');
    const result_json_all = JSON.stringify(rs.map(r => {
      try { return JSON.parse(r.result_json || '{}'); } catch (e) { return r.result_json || ''; }
    }));
    const result_created_by = [...new Set(rs.map(r => r.created_by).filter(Boolean))].join('、');
    const result_created_at = rs.length > 0 ? rs[rs.length - 1].created_at : '';

    // 业务关联
    let related = null;
    if (j.related_doc_type === 'inbound') related = inboundById[j.related_doc_id] || null;
    else if (j.related_doc_type === 'outbound') related = outboundById[j.related_doc_id] || null;
    const linked_ob = j.linked_outbound_order_id ? outboundById[j.linked_outbound_order_id] : null;
    const customer = (related && related.customer) || (linked_ob && linked_ob.customer) || '';
    const accounted = (related && related.accounted) ?? (linked_ob && linked_ob.accounted) ?? '';
    const accounted_by = (related && related.accounted_by) || (linked_ob && linked_ob.accounted_by) || '';
    const accounted_at = (related && related.accounted_at) || (linked_ob && linked_ob.accounted_at) || '';
    const inbound_display_no = (j.related_doc_type === 'inbound' && related) ? (related.display_no || related.external_inbound_no || '') : '';
    const outbound_display_no = (j.related_doc_type === 'outbound' && related) ? (related.display_no || '') : (linked_ob ? linked_ob.display_no || '' : '');
    const ob_for_fields = (j.related_doc_type === 'outbound' && related) ? related : linked_ob;
    const wms_work_order_no = (ob_for_fields && ob_for_fields.wms_work_order_no) || '';
    const destination = (ob_for_fields && ob_for_fields.destination) || '';
    const po_no = (ob_for_fields && ob_for_fields.po_no) || '';
    const planned_box_count = (ob_for_fields && ob_for_fields.planned_box_count) || 0;
    const planned_pallet_count = (ob_for_fields && ob_for_fields.planned_pallet_count) || 0;
    const actual_box_count = (ob_for_fields && ob_for_fields.actual_box_count) || 0;
    const actual_pallet_count = (ob_for_fields && ob_for_fields.actual_pallet_count) || 0;

    // 代发拣货
    let pick_doc_nos = '', pick_worker_summary = '';
    if (j.job_type === 'pick_direct' && ps.length > 0) {
      const docs = [...new Set(ps.map(p => p.pick_doc_no).filter(Boolean))];
      pick_doc_nos = docs.join('、');
      pick_worker_summary = docs.map(d => {
        const inDoc = ps.filter(p => p.pick_doc_no === d);
        const parts = inDoc.map(p => `${p.worker_name || p.worker_id}${p.minutes_worked ? ' ' + round1(p.minutes_worked) + '分' : ''}`);
        return `${d}: ${parts.join(' / ')}`;
      }).join('；');
    }

    return {
      job_id: j.id,
      日期: (j.created_at || '').slice(0, 10),
      单号: j.display_no || j.related_doc_id || j.linked_outbound_order_id || j.id,
      display_no: j.display_no || '',
      related_doc_id: j.related_doc_id || '',
      linked_outbound_order_id: j.linked_outbound_order_id || '',
      flow_stage: j.flow_stage || '',
      job_type: j.job_type || '',
      biz_class: j.biz_class || '',
      status: j.status || '',
      created_at: j.created_at || '',
      started_at: started_at || '',
      ended_at: ended_at || '',
      worker_count: new Set(ws.map(w => w.worker_id).filter(Boolean)).size,
      worker_names: workerNames,
      total_minutes,
      total_hours: round1(total_minutes / 60),
      // 作业结果
      result_count: rs.length,
      box_count_sum: box_sum,
      pallet_count_sum: pallet_sum,
      result_remarks: remarks,
      diff_notes: diffs,
      result_lines_json_all: resultLines,
      result_json_all,
      result_created_by,
      result_created_at: result_created_at || '',
      // 业务关联
      customer,
      accounted: accounted === '' ? '' : (accounted ? 1 : 0),
      accounted_by, accounted_at,
      outbound_display_no, inbound_display_no,
      wms_work_order_no, destination, po_no,
      planned_box_count, planned_pallet_count, actual_box_count, actual_pallet_count,
      // 代发拣货
      pick_doc_nos, pick_worker_summary
    };
  });

  return json({ ok: true, rows: out, total: out.length, truncated: jobs.length >= limit });
});

// 3) 工时分析 — 汇总（summary + by_worker + by_job_type + segments）
route("v2_dashboard_workhour_summary", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const start_date = String(body.start_date || "").trim();
  const end_date = String(body.end_date || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  const flow_stage = String(body.flow_stage || "").trim();
  const job_type = String(body.job_type || "").trim();

  let where = "WHERE 1=1";
  const binds = [];
  if (start_date) { where += " AND DATE(w.joined_at)>=?"; binds.push(start_date); }
  if (end_date)   { where += " AND DATE(w.joined_at)<=?"; binds.push(end_date); }
  if (flow_stage) { where += " AND j.flow_stage=?"; binds.push(flow_stage); }
  if (job_type)   { where += " AND j.job_type=?"; binds.push(job_type); }
  if (worker_name) { where += " AND w.worker_name LIKE ?"; binds.push("%" + worker_name + "%"); }

  // segments: 限 1000 条，避免一次拉太大
  const segSql = `
    SELECT w.worker_id, w.worker_name, w.joined_at, w.left_at, w.minutes_worked, w.leave_reason,
           j.id AS job_id, j.display_no, j.flow_stage, j.biz_class, j.job_type, j.status
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON j.id = w.job_id
    ${where}
    ORDER BY w.joined_at DESC LIMIT 1000`;
  const rs = await env.DB.prepare(segSql).bind(...binds).all();
  const rows = rs.results || [];

  const nowMs = Date.now();
  const todayKst = kstToday();
  const segments = rows.map(r => {
    const closed = !!r.left_at;
    let minutes = Number(r.minutes_worked) || 0;
    if (!closed && r.joined_at) {
      const t = new Date(r.joined_at).getTime();
      if (!isNaN(t)) minutes = Math.max(0, (nowMs - t) / 60000);
    }
    minutes = round1(minutes);
    const joinedKstDate = kstDateOf(r.joined_at);
    const crossDayActive = !closed && joinedKstDate && joinedKstDate < todayKst;
    let anomaly = 0, anomaly_reason = '';
    if (closed && minutes <= 0) { anomaly = 1; anomaly_reason = '已结束但工时为 0/负'; }
    else if (closed && minutes >= 720) { anomaly = 1; anomaly_reason = '已结束 ≥12 小时'; }
    else if (!closed && minutes >= 720) { anomaly = 1; anomaly_reason = '进行中 ≥12 小时'; }
    else if (crossDayActive) { anomaly = 1; anomaly_reason = '跨天未结束'; }
    const long_segment = (!anomaly && minutes >= 240) ? 1 : 0;
    return {
      worker_id: r.worker_id, worker_name: r.worker_name,
      joined_at: r.joined_at, left_at: r.left_at,
      minutes,
      leave_reason: r.leave_reason || '',
      active: closed ? 0 : 1,
      job_id: r.job_id, display_no: r.display_no,
      flow_stage: r.flow_stage, biz_class: r.biz_class,
      job_type: r.job_type, status: r.status,
      anomaly, anomaly_reason, long_segment
    };
  });

  const byWorkerMap = {}, byJobTypeMap = {}, jobIdSet = {};
  let total_minutes = 0, max_segment_minutes = 0, anomaly_count = 0, long_segment_count = 0;
  segments.forEach(s => {
    total_minutes += s.minutes;
    if (s.minutes > max_segment_minutes) max_segment_minutes = s.minutes;
    if (s.anomaly) anomaly_count++;
    if (s.long_segment) long_segment_count++;
    jobIdSet[s.job_id] = 1;
    const wk = s.worker_name || s.worker_id || '--';
    if (!byWorkerMap[wk]) byWorkerMap[wk] = { worker_name: wk, total_minutes: 0, job_ids: {}, max_segment_minutes: 0 };
    byWorkerMap[wk].total_minutes += s.minutes;
    byWorkerMap[wk].job_ids[s.job_id] = 1;
    if (s.minutes > byWorkerMap[wk].max_segment_minutes) byWorkerMap[wk].max_segment_minutes = s.minutes;
    const jt = s.job_type || '--';
    if (!byJobTypeMap[jt]) byJobTypeMap[jt] = { job_type: jt, total_minutes: 0, worker_ids: {}, job_ids: {} };
    byJobTypeMap[jt].total_minutes += s.minutes;
    byJobTypeMap[jt].worker_ids[s.worker_id || s.worker_name] = 1;
    byJobTypeMap[jt].job_ids[s.job_id] = 1;
  });

  const by_worker = Object.values(byWorkerMap).map(v => {
    const job_count = Object.keys(v.job_ids).length;
    return {
      worker_name: v.worker_name,
      total_minutes: round1(v.total_minutes),
      total_hours: round1(v.total_minutes / 60),
      job_count,
      avg_minutes_per_job: job_count > 0 ? round1(v.total_minutes / job_count) : 0,
      max_segment_minutes: round1(v.max_segment_minutes)
    };
  }).sort((a, b) => b.total_minutes - a.total_minutes);

  const by_job_type = Object.values(byJobTypeMap).map(v => {
    const job_count = Object.keys(v.job_ids).length;
    const worker_count = Object.keys(v.worker_ids).length;
    return {
      job_type: v.job_type,
      total_minutes: round1(v.total_minutes),
      worker_count,
      job_count,
      avg_minutes_per_worker: worker_count > 0 ? round1(v.total_minutes / worker_count) : 0
    };
  }).sort((a, b) => b.total_minutes - a.total_minutes);

  const worker_count = Object.keys(byWorkerMap).length;
  const job_count = Object.keys(jobIdSet).length;

  return json({
    ok: true,
    summary: {
      total_minutes: round1(total_minutes),
      total_hours: round1(total_minutes / 60),
      worker_count,
      job_count,
      avg_minutes_per_worker: worker_count > 0 ? round1(total_minutes / worker_count) : 0,
      max_segment_minutes: round1(max_segment_minutes),
      anomaly_count,
      long_segment_count
    },
    by_worker,
    by_job_type,
    segments,
    truncated: rows.length >= 1000
  });
});

// 4) WMS 导入 — 写入批次 + 行
route("v2_dashboard_wms_import", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const import_type = String(body.import_type || "generic").trim();
  const file_name = String(body.file_name || "").trim();
  const uploaded_by = String(body.uploaded_by || "").trim();
  const headers = Array.isArray(body.headers) ? body.headers : [];
  const rows = Array.isArray(body.rows) ? body.rows : [];
  if (rows.length === 0) return err("empty rows");
  if (rows.length > 5000) return err("too many rows; max 5000 per batch");

  return withIdem(env, body, "v2_dashboard_wms_import", async () => {
    const batch_id = "WMS-" + uid();
    const t = now();

    // 计算 date_from / date_to
    let date_from = '', date_to = '';
    rows.forEach(r => {
      const d = String(r.work_date || '').slice(0, 10);
      if (d) {
        if (!date_from || d < date_from) date_from = d;
        if (!date_to || d > date_to) date_to = d;
      }
    });

    await env.DB.prepare(`
      INSERT INTO v2_wms_import_batches(id, import_type, file_name, row_count, date_from, date_to,
        uploaded_by, status, raw_headers_json, created_at)
      VALUES(?,?,?,?,?,?,?, 'imported', ?, ?)
    `).bind(batch_id, import_type, file_name, rows.length, date_from, date_to,
            uploaded_by, JSON.stringify(headers), t).run();

    // 分批 insert（每批 50 行，避免单条 SQL 过长）
    const CHUNK = 50;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const slice = rows.slice(i, i + CHUNK);
      const stmts = slice.map(r => {
        const id = "WR-" + uid();
        return env.DB.prepare(`
          INSERT INTO v2_wms_import_rows(id, batch_id, import_type, work_date, operated_at,
            worker_name, worker_id, customer, doc_no, order_no, sku, qty, box_count,
            operation_type, raw_json, matched_job_id, matched_worker_id, match_confidence, created_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'','',0,?)
        `).bind(
          id, batch_id, import_type,
          String(r.work_date || '').slice(0, 10),
          String(r.operated_at || ''),
          String(r.worker_name || ''),
          String(r.worker_id || ''),
          String(r.customer || ''),
          String(r.doc_no || ''),
          String(r.order_no || ''),
          String(r.sku || ''),
          Number(r.qty) || 0,
          Number(r.box_count) || 0,
          String(r.operation_type || ''),
          JSON.stringify(r.raw || {}),
          t
        );
      });
      await env.DB.batch(stmts);
    }

    return { ok: true, batch_id, row_count: rows.length, date_from, date_to };
  });
});

// 5) WMS 导入 — 最近批次列表
route("v2_dashboard_wms_batches", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const { limit, offset } = pageParams(body);
  const rs = await env.DB.prepare(
    "SELECT id, import_type, file_name, row_count, date_from, date_to, uploaded_by, status, created_at FROM v2_wms_import_batches ORDER BY created_at DESC LIMIT ? OFFSET ?"
  ).bind(limit, offset).all();
  return json({ ok: true, items: rs.results || [], limit, offset });
});

// 6) WMS 导入 — 批次明细
route("v2_dashboard_wms_batch_detail", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const batch_id = String(body.batch_id || "").trim();
  if (!batch_id) return err("missing batch_id");
  const { limit, offset } = pageParams(body);
  const batch = await env.DB.prepare("SELECT * FROM v2_wms_import_batches WHERE id=?").bind(batch_id).first();
  if (!batch) return err("not found", 404);
  const rows = await env.DB.prepare(
    "SELECT * FROM v2_wms_import_rows WHERE batch_id=? ORDER BY work_date ASC, operated_at ASC LIMIT ? OFFSET ?"
  ).bind(batch_id, limit, offset).all();
  return json({ ok: true, batch, rows: rows.results || [], limit, offset });
});

// 7) 管理看板 — 工时 × WMS 复合人效（V1 简化匹配）
route("v2_dashboard_management_summary", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const start_date = String(body.start_date || "").trim();
  const end_date = String(body.end_date || "").trim();

  // import_type → job_type 候选集
  const TYPE_MAP = {
    change_order: ['change_order'],
    pack_direct: ['pack_direct'],
    pick_direct: ['pick_direct'],
    inbound: ['inbound_direct', 'inbound_bulk', 'inbound_return'],
    outbound: ['load_outbound', 'verify_scan', 'bulk_op'],
  };

  // ---- 工时段（与 workhour_summary 同口径，但聚合到 by_job_type / by_worker）----
  let where = "WHERE 1=1";
  const binds = [];
  if (start_date) { where += " AND DATE(w.joined_at)>=?"; binds.push(start_date); }
  if (end_date)   { where += " AND DATE(w.joined_at)<=?"; binds.push(end_date); }

  const segRs = await env.DB.prepare(`
    SELECT w.worker_id, w.worker_name, w.joined_at, w.left_at, w.minutes_worked,
           j.flow_stage, j.biz_class, j.job_type
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON j.id = w.job_id
    ${where}
    ORDER BY w.joined_at DESC LIMIT 5000`).bind(...binds).all();
  const segs = segRs.results || [];

  // ---- WMS 行（按 work_date 在范围内）----
  let wmsWhere = "WHERE 1=1";
  const wmsBinds = [];
  if (start_date) { wmsWhere += " AND work_date>=?"; wmsBinds.push(start_date); }
  if (end_date)   { wmsWhere += " AND work_date<=?"; wmsBinds.push(end_date); }
  const wmsRs = await env.DB.prepare(
    `SELECT import_type, worker_name, qty, box_count FROM v2_wms_import_rows ${wmsWhere} LIMIT 20000`
  ).bind(...wmsBinds).all();
  const wmsRows = wmsRs.results || [];

  // ---- 工时聚合 ----
  const nowMs = Date.now();
  let total_minutes = 0, anomaly_count = 0;
  const workerMins = {}, jobTypeMins = {}, workerJobTypeMins = {};
  segs.forEach(r => {
    const closed = !!r.left_at;
    let m = Number(r.minutes_worked) || 0;
    if (!closed && r.joined_at) {
      const t = new Date(r.joined_at).getTime();
      if (!isNaN(t)) m = Math.max(0, Math.round((nowMs - t) / 60000));
    }
    if (m > 240 || (closed && m <= 0)) anomaly_count++;
    total_minutes += m;
    const wk = r.worker_name || r.worker_id || '--';
    workerMins[wk] = (workerMins[wk] || 0) + m;
    const jt = r.job_type || '--';
    jobTypeMins[jt] = (jobTypeMins[jt] || 0) + m;
    const k = wk + '||' + jt;
    workerJobTypeMins[k] = (workerJobTypeMins[k] || 0) + m;
  });

  // ---- WMS 聚合（按 import_type 反查 job_type 候选；按 worker_name）----
  let total_qty = 0, total_boxes = 0;
  const jobTypeWms = {}; // job_type -> { qty, boxes }
  const workerWms = {};  // worker_name -> { qty, boxes }
  wmsRows.forEach(r => {
    const q = Number(r.qty) || 0, b = Number(r.box_count) || 0;
    total_qty += q;
    total_boxes += b;
    const cands = TYPE_MAP[r.import_type] || [];
    cands.forEach(jt => {
      if (!jobTypeWms[jt]) jobTypeWms[jt] = { qty: 0, boxes: 0 };
      // 平均分摊到候选集，避免重复计入
      jobTypeWms[jt].qty += q / cands.length;
      jobTypeWms[jt].boxes += b / cands.length;
    });
    if (r.worker_name) {
      if (!workerWms[r.worker_name]) workerWms[r.worker_name] = { qty: 0, boxes: 0 };
      workerWms[r.worker_name].qty += q;
      workerWms[r.worker_name].boxes += b;
    }
  });

  // ---- by_job_type ----
  const jtSet = new Set([...Object.keys(jobTypeMins), ...Object.keys(jobTypeWms)]);
  const by_job_type = [...jtSet].map(jt => {
    const mins = jobTypeMins[jt] || 0;
    const hours = Math.round(mins / 6) / 10;
    const w = jobTypeWms[jt] || { qty: 0, boxes: 0 };
    const qty = Math.round(w.qty * 10) / 10;
    const boxes = Math.round(w.boxes * 10) / 10;
    return {
      job_type: jt,
      total_minutes: mins,
      total_hours: hours,
      wms_qty: qty,
      wms_boxes: boxes,
      qty_per_hour: hours > 0 ? Math.round((qty / hours) * 10) / 10 : 0,
      boxes_per_hour: hours > 0 ? Math.round((boxes / hours) * 10) / 10 : 0
    };
  }).sort((a, b) => b.total_minutes - a.total_minutes);

  // ---- by_worker ----
  const wkSet = new Set([...Object.keys(workerMins), ...Object.keys(workerWms)]);
  const by_worker = [...wkSet].map(wk => {
    const mins = workerMins[wk] || 0;
    const hours = Math.round(mins / 6) / 10;
    const w = workerWms[wk] || { qty: 0, boxes: 0 };
    const qty = Math.round(w.qty * 10) / 10;
    const boxes = Math.round(w.boxes * 10) / 10;
    return {
      worker_name: wk,
      total_minutes: mins,
      total_hours: hours,
      wms_qty: qty,
      wms_boxes: boxes,
      qty_per_hour: hours > 0 ? Math.round((qty / hours) * 10) / 10 : 0,
      boxes_per_hour: hours > 0 ? Math.round((boxes / hours) * 10) / 10 : 0
    };
  }).sort((a, b) => b.total_minutes - a.total_minutes);

  const total_hours = Math.round(total_minutes / 6) / 10;
  return json({
    ok: true,
    summary: {
      total_minutes,
      total_hours,
      total_qty: Math.round(total_qty * 10) / 10,
      total_boxes: Math.round(total_boxes * 10) / 10,
      qty_per_hour: total_hours > 0 ? Math.round((total_qty / total_hours) * 10) / 10 : 0,
      boxes_per_hour: total_hours > 0 ? Math.round((total_boxes / total_hours) * 10) / 10 : 0,
      worker_count: Object.keys(workerMins).length,
      anomaly_count
    },
    by_job_type,
    by_worker
  });
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
    let formData = null;
    const ct = request.headers.get("content-type") || "";

    if (request.method === "GET") {
      body = Object.fromEntries(url.searchParams);
    } else if (ct.includes("multipart/form-data")) {
      isMultipart = true;
      formData = await request.formData();
      body = { action: formData.get("action") || "", k: formData.get("k") || "" };
    } else if (ct.includes("application/json")) {
      body = await request.json().catch(() => ({}));
    } else {
      const txt = await request.text().catch(() => "");
      try { body = JSON.parse(txt); } catch { body = Object.fromEntries(new URLSearchParams(txt)); }
    }

    const action = String(body.action || "").trim();

    // Special handling for multipart upload — formData already parsed above, pass it directly
    if (action === "v2_attachment_upload" || isMultipart) {
      return await handleMultipartUpload(formData, env);
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
async function handleMultipartUpload(formData, env) {
  try {
    await ensureMigrated(env.DB);
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
