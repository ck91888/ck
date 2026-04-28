/**
 * CK Warehouse V2 — Ops App (Field Execution System)
 * Mobile-first, bilingual (zh/ko), shared tasks, interrupts
 * Badge-based entry (reuses main system badge logic)
 */

// ===== State =====
var _currentPage = "badge";
var _pageParams = {};
var _navStack = [];
var _activeJobId = null;   // current job id I'm participating in
var _activeSegId = null;    // my current worker segment
var _pollTimer = null;
var _issueFilter = "pending";
var _currentIssueId = null;
var _unloadPlanData = null;   // loaded plan detail (plan + lines)
var _unloadScanner = null;
var _currentRunId = null;
var _photoUploadCtx = {};  // { related_doc_type, attachment_category, related_doc_id }
var _badgeScanner = null;  // Html5Qrcode instance for badge scan
var _badgeModalScanner = null; // Html5Qrcode instance for badge change modal
var _startInflight = false; // in-flight guard for start actions

// ===== Action Lock — 防连点 =====
var _actionLocks = {};
function withActionLock(key, btnEl, pendingText, fn) {
  if (_actionLocks[key]) return;        // 已锁定，直接忽略
  _actionLocks[key] = true;
  var origText = '';
  var origDisabled = false;
  if (btnEl) {
    origText = btnEl.textContent;
    origDisabled = btnEl.disabled;
    btnEl.disabled = true;
    btnEl.textContent = pendingText || '提交中.../저장중...';
  }
  var restore = function() {
    _actionLocks[key] = false;
    if (btnEl) {
      btnEl.disabled = origDisabled;
      btnEl.textContent = origText;
    }
  };
  try {
    var result = fn();
    if (result && typeof result.then === 'function') {
      result.then(restore, restore);
    } else {
      restore();
    }
  } catch(e) {
    restore();
    throw e;
  }
}

// ===== Badge logic (ported from main system app.js) =====
function parseBadge(code) {
  var raw = (code || "").trim();
  var parts = raw.split("|");
  var id = (parts[0] || "").trim();
  var name = (parts[1] || "").trim();
  return { raw: raw, id: id, name: name };
}
function isDaId(id) { return /^DA-\d{6,8}-.+$/.test(id); }
function isEmpId(id) { return /^EMP-.+$/.test(id); }
function isPermanentDaId(id) { return /^DAF-.+$/.test(id); }
function isOperatorBadge(raw) {
  var p = parseBadge(raw);
  return isDaId(p.id) || isEmpId(p.id) || isPermanentDaId(p.id);
}
function badgeDisplay(raw) {
  var p = parseBadge(raw);
  return p.name ? (p.id + "｜" + p.name) : p.id;
}

// ===== Badge storage =====
function getBadge() {
  try { return localStorage.getItem(V2_OPS_BADGE_KEY) || ""; } catch(e) { return ""; }
}
function setBadge(raw) {
  try { localStorage.setItem(V2_OPS_BADGE_KEY, raw); } catch(e) {}
}
function getAuthDay() {
  try { return localStorage.getItem(V2_OPS_AUTH_DAY_KEY) || ""; } catch(e) { return ""; }
}
function setAuthDay(day) {
  try { localStorage.setItem(V2_OPS_AUTH_DAY_KEY, day); } catch(e) {}
}

// KST today (UTC+9)
function kstToday() {
  var d = new Date(Date.now() + 9 * 3600000);
  return d.toISOString().slice(0, 10);
}

// ===== Identity getters (used throughout the app) =====
// getWorkerId returns badge ID (e.g. "EMP-001"), getWorkerName returns badge name (e.g. "张三")
function getWorkerId() {
  var p = parseBadge(getBadge());
  return p.id || "";
}
function getWorkerName() {
  var p = parseBadge(getBadge());
  return p.name || p.id || "";
}

// ===== API =====
// 写操作 action 集合 — 自动注入 client_req_id 供后端幂等
var _WRITE_ACTIONS = [
  'v2_issue_create','v2_issue_handle_start','v2_issue_handle_finish',
  'v2_issue_close','v2_issue_cancel',
  'v2_outbound_order_create','v2_outbound_order_update_status',
  'v2_outbound_load_start','v2_outbound_load_finish',
  'v2_outbound_stock_op_start','v2_outbound_stock_op_finish',
  'v2_inbound_plan_create','v2_inbound_plan_update_status','v2_inbound_plan_cancel',
  'v2_inbound_dynamic_finalize',
  'v2_unplanned_unload_start','v2_unplanned_unload_join','v2_unplanned_unload_finish',
  'v2_feedback_finalize_to_inbound','v2_unload_dynamic_start',
  'v2_unload_job_start','v2_unload_job_finish',
  'v2_inbound_job_start','v2_inbound_job_finish',
  'v2_inbound_mark_completed',
  'v2_ops_job_start','v2_ops_job_leave','v2_ops_job_finish','v2_ops_job_resume',
  'v2_pick_job_start','v2_pick_job_start_by_docs','v2_pick_job_join','v2_pick_job_add_docs','v2_pick_job_finish','v2_pick_job_finalize',
  'v2_bulk_op_job_start','v2_bulk_op_job_finish',
  'v2_correction_request_create','v2_admin_dirty_data_cleanup',
  'v2_verify_batch_upload','v2_verify_batch_update_status',
  'v2_verify_job_start','v2_verify_job_finish','v2_verify_scan_submit',
  'v2_outbound_order_ack_change','v2_outbound_pickup_confirm'
];
function _genReqId(action) {
  return action + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
}
function api(params) {
  params.k = OPS_KEY;
  if (_WRITE_ACTIONS.indexOf(params.action) !== -1 && !params.client_req_id) {
    params.client_req_id = _genReqId(params.action);
  }
  return fetch(V2_API, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params)
  }).then(function(res) { return res.json(); })
    .catch(function(e) {
      return {
        ok: false,
        error: "network_error",
        network_error: true,
        message: "接口连接失败（网络或域名问题），不是业务提交失败\n네트워크/도메인 오류\n" + (e && e.message || e)
      };
    });
}

function uploadFile(formData) {
  formData.append("k", OPS_KEY);
  formData.append("action", "v2_attachment_upload");
  return fetch(V2_API, { method: "POST", body: formData })
    .then(function(res) { return res.json(); })
    .catch(function(e) {
      return {
        ok: false,
        error: "network_error",
        network_error: true,
        message: "文件上传失败（网络或域名问题）\n파일 업로드 실패\n" + (e && e.message || e)
      };
    });
}

function fileUrl(fileKey) {
  return V2_API + "/file?key=" + encodeURIComponent(fileKey);
}

// ===== Navigation =====
function goPage(name, params) {
  if (_currentPage !== "badge") {
    _navStack.push({ page: _currentPage, params: _pageParams });
    if (_navStack.length > 20) _navStack.shift();
  }
  _pageParams = params || {};
  showPage(name);
}

function goBack() {
  var prev = _navStack.pop();
  if (prev) {
    _pageParams = prev.params || {};
    showPage(prev.page);
  } else {
    showPage("home");
  }
}

function showPage(name) {
  // Stop polling
  if (_pollTimer) { clearInterval(_pollTimer); _pollTimer = null; }

  _currentPage = name;
  var pages = document.querySelectorAll(".page");
  for (var i = 0; i < pages.length; i++) {
    pages[i].classList.remove("active");
  }
  var el = document.getElementById("page-" + name);
  if (el) el.classList.add("active");

  // Page init
  if (name === "home") initHome();
  if (name === "unload") initUnload();
  if (name === "inbound") initInbound();
  if (name === "inbound_return") initInboundReturn();
  if (name === "outbound_load") initOutboundLoad();
  if (name === "outbound_stock_op") initOutboundStockOp();
  if (name === "issue_list") loadIssueList();
  if (name === "issue_detail") loadIssueDetail();
  if (name === "generic_job") initGenericJob();
  if (name === "pick_direct") initPickDirect();
  if (name === "bulk_op") initBulkOp();
  if (name === "import_delivery") initImportDelivery();
  if (name === "verify_scan") initVerifyScan();
}

// ===== Badge Entry (replaces old login) =====
function checkBadgeAuth() {
  var badge = getBadge();
  var authDay = getAuthDay();
  var today = kstToday();
  if (badge && isOperatorBadge(badge) && authDay === today) {
    updateHeaderBadge();
    // 尝试恢复到活跃任务页，无活跃任务则进首页
    resumeActiveOrHome();
    return true;
  }
  return false;
}

function resumeActiveOrHome() {
  restoreActiveJob();
  api({ action: "v2_ops_my_active_job", worker_id: getWorkerId() }).then(function(res) {
    if (res && res.ok && res.active && res.job) {
      saveActiveJob(res.job.id, res.segment ? res.segment.id : null);
      var jt = res.job.job_type || "";
      if (jt === "unload") showPage("unload");
      else if (jt === "inbound_return") showPage("inbound_return");
      else if (jt === "inbound_direct" || jt === "inbound_bulk") { _pageParams = { job_type: jt, biz_class: res.job.biz_class || "" }; showPage("inbound"); }
      else if (jt === "load_outbound") showPage("outbound_load");
      else if (jt === "outbound_stock_op") showPage("outbound_stock_op");
      else if (jt === "pick_direct") showPage("pick_direct");
      else if (jt === "bulk_op") showPage("bulk_op");
      else if (jt === "issue_handle") { _currentIssueId = res.job.related_doc_id || null; showPage("issue_detail"); }
      else if (jt === "verify_scan") { _vsBatchId = res.job.related_doc_id || ""; showPage("verify_scan"); }
      else { _pageParams = { flow_stage: res.job.flow_stage || "", biz_class: res.job.biz_class || "", job_type: jt, title: JOB_TYPE_LABEL[jt] || jt }; showPage("generic_job"); }
    } else {
      showPage("home");
    }
  }).catch(function() { showPage("home"); });
}

function applyBadge(raw) {
  raw = (raw || "").trim();
  if (!raw || !isOperatorBadge(raw)) {
    return false;
  }
  setBadge(raw);
  setAuthDay(kstToday());
  updateHeaderBadge();
  // 记录登录事件（fire-and-forget，不阻塞登录流程）
  var parsed = parseBadge(raw);
  api({
    action: "v2_ops_login_mark",
    worker_id: parsed.id,
    worker_name: parsed.name,
    page_source: "001",
    device_info: navigator.userAgent || ""
  }).catch(function() {});
  return true;
}

function updateHeaderBadge() {
  var el = document.getElementById("headerWorker");
  if (el) el.textContent = badgeDisplay(getBadge()) || "--";
}

// --- Badge scan on entry page ---
function startBadgeScan() {
  var readerEl = document.getElementById("badgeReader");
  var statusEl = document.getElementById("badgeStatus");
  var btn = document.getElementById("badgeScanBtn");
  if (!readerEl) return;

  // If scanner already running, stop it
  if (_badgeScanner) {
    try { _badgeScanner.stop(); } catch(e) {}
    _badgeScanner = null;
    readerEl.innerHTML = "";
    btn.textContent = "开始扫码 / 스캔 시작";
    return;
  }

  btn.textContent = "停止扫码 / 스캔 중지";
  statusEl.textContent = "";

  _badgeScanner = new Html5Qrcode("badgeReader", {
    formatsToSupport: [Html5QrcodeSupportedFormats.QR_CODE, Html5QrcodeSupportedFormats.CODE_128, Html5QrcodeSupportedFormats.CODE_39],
    experimentalFeatures: { useBarCodeDetectorIfSupported: true }
  });

  _badgeScanner.start(
    { facingMode: "environment" },
    { fps: 10, qrbox: { width: 250, height: 120 } },
    function(decodedText) {
      var code = decodedText.trim();
      try { code = decodeURIComponent(code); } catch(e) {}

      if (!isOperatorBadge(code)) {
        statusEl.textContent = "无效工牌 / 잘못된 명찰: " + code;
        statusEl.style.color = "#e74c3c";
        return;
      }
      // Valid badge
      try { _badgeScanner.stop(); } catch(e) {}
      _badgeScanner = null;
      readerEl.innerHTML = "";

      if (applyBadge(code)) {
        showPage("home");
      }
    },
    function() {} // ignore scan errors
  ).catch(function(e) {
    statusEl.textContent = "摄像头启动失败 / 카메라 시작 실패: " + e;
    statusEl.style.color = "#e74c3c";
    btn.textContent = "开始扫码 / 스캔 시작";
    _badgeScanner = null;
  });
}

function showBadgeManualInput() {
  var box = document.getElementById("badgeManualBox");
  if (box) box.style.display = box.style.display === "none" ? "" : "none";
}

function submitManualBadge() {
  var input = document.getElementById("badgeManualInput");
  var statusEl = document.getElementById("badgeStatus");
  var val = (input ? input.value : "").trim();

  if (!val) {
    statusEl.textContent = "请输入工牌 / 명찰을 입력하세요";
    statusEl.style.color = "#e74c3c";
    return;
  }
  if (!isOperatorBadge(val)) {
    statusEl.textContent = "无效工牌格式 / 잘못된 명찰 형식\n" + t("badge_format_hint");
    statusEl.style.color = "#e74c3c";
    return;
  }
  // Stop scanner if running
  if (_badgeScanner) {
    try { _badgeScanner.stop(); } catch(e) {}
    _badgeScanner = null;
    document.getElementById("badgeReader").innerHTML = "";
  }

  if (applyBadge(val)) {
    showPage("home");
  }
}

// --- Badge change modal (right-top entry) ---
function hasActiveJob() {
  return !!_activeJobId;
}

function showBadgeChange() {
  // 有活跃任务时禁止更换工牌
  if (hasActiveJob()) {
    alert("当前还有进行中的任务，请先返回任务并结束或离开后再更换工牌\n\n진행 중인 작업이 있습니다. 작업을 종료하거나 나간 후 명찰을 변경하세요");
    return;
  }
  var modal = document.getElementById("badgeModal");
  modal.style.display = "flex";
  var input = document.getElementById("badgeModalInput");
  if (input) input.value = getBadge();
  var statusEl = document.getElementById("badgeModalStatus");
  if (statusEl) statusEl.textContent = "当前 / 현재: " + badgeDisplay(getBadge());
}

function hideBadgeModal() {
  if (_badgeModalScanner) {
    try { _badgeModalScanner.stop(); } catch(e) {}
    _badgeModalScanner = null;
    document.getElementById("badgeModalReader").innerHTML = "";
  }
  document.getElementById("badgeModal").style.display = "none";
}

function startBadgeModalScan() {
  var readerEl = document.getElementById("badgeModalReader");
  var statusEl = document.getElementById("badgeModalStatus");
  if (!readerEl) return;

  if (_badgeModalScanner) {
    try { _badgeModalScanner.stop(); } catch(e) {}
    _badgeModalScanner = null;
    readerEl.innerHTML = "";
    return;
  }

  statusEl.textContent = "扫码中... / 스캔중...";
  _badgeModalScanner = new Html5Qrcode("badgeModalReader", {
    formatsToSupport: [Html5QrcodeSupportedFormats.QR_CODE, Html5QrcodeSupportedFormats.CODE_128, Html5QrcodeSupportedFormats.CODE_39],
    experimentalFeatures: { useBarCodeDetectorIfSupported: true }
  });

  _badgeModalScanner.start(
    { facingMode: "environment" },
    { fps: 10, qrbox: { width: 240, height: 110 } },
    function(decodedText) {
      var code = decodedText.trim();
      try { code = decodeURIComponent(code); } catch(e) {}

      if (!isOperatorBadge(code)) {
        statusEl.textContent = "无效工牌 / 잘못된 명찰: " + code;
        statusEl.style.color = "#e74c3c";
        return;
      }
      try { _badgeModalScanner.stop(); } catch(e) {}
      _badgeModalScanner = null;
      readerEl.innerHTML = "";

      applyBadge(code);
      statusEl.textContent = "已更换 / 변경됨: " + badgeDisplay(code);
      statusEl.style.color = "#27ae60";
      setTimeout(hideBadgeModal, 800);
    },
    function() {}
  ).catch(function(e) {
    statusEl.textContent = "摄像头启动失败 / 카메라 시작 실패";
    statusEl.style.color = "#e74c3c";
    _badgeModalScanner = null;
  });
}

function submitBadgeModal() {
  var input = document.getElementById("badgeModalInput");
  var statusEl = document.getElementById("badgeModalStatus");
  var val = (input ? input.value : "").trim();

  if (!val || !isOperatorBadge(val)) {
    statusEl.textContent = "无效工牌格式 / 잘못된 명찰 형식";
    statusEl.style.color = "#e74c3c";
    return;
  }

  if (_badgeModalScanner) {
    try { _badgeModalScanner.stop(); } catch(e) {}
    _badgeModalScanner = null;
    document.getElementById("badgeModalReader").innerHTML = "";
  }

  applyBadge(val);
  statusEl.textContent = "已更换 / 변경됨: " + badgeDisplay(val);
  statusEl.style.color = "#27ae60";
  setTimeout(hideBadgeModal, 800);
}

function hasOtherActiveJob(allowJobId) {
  if (!_activeJobId) return false;
  if (allowJobId && _activeJobId === allowJobId) return false;
  return true;
}

function warnActiveJob() {
  alert("当前已有进行中的任务，请先返回首页点击顶部任务条，结束或暂时离开后再开始新任务\n"
    + "이미 진행 중인 작업이 있습니다. 홈에서 상단 작업 바를 눌러 현재 작업을 완료하거나 퇴장 후 새 작업을 시작하세요");
  return false;
}

// ===== Home =====
function initHome() {
  if (!_activeJobId) localStorage.removeItem(V2_INTERRUPT_KEY);
  checkMyActiveJob();
  restoreActiveJob();
}

function restoreActiveJob() {
  try {
    var saved = JSON.parse(localStorage.getItem(V2_ACTIVE_JOB_KEY) || "null");
    if (saved) {
      _activeJobId = saved.job_id;
      _activeSegId = saved.seg_id;
    }
  } catch(e) {}
}

function saveActiveJob(jobId, segId) {
  _activeJobId = jobId;
  _activeSegId = segId;
  localStorage.setItem(V2_ACTIVE_JOB_KEY, JSON.stringify({ job_id: jobId, seg_id: segId }));
}

function clearActiveJob() {
  _activeJobId = null;
  _activeSegId = null;
  localStorage.removeItem(V2_ACTIVE_JOB_KEY);
}

async function checkMyActiveJob() {
  var bar = document.getElementById("myTaskBar");
  var label = document.getElementById("myTaskLabel");
  var meta = document.getElementById("myTaskMeta");
  if (!bar) return;

  var res = await api({ action: "v2_ops_my_active_job", worker_id: getWorkerId() });
  if (res && res.ok && res.active && res.job) {
    bar.classList.remove("hidden");
    var job = res.job;
    var typeLabel = JOB_TYPE_LABEL[job.job_type] || job.job_type;
    label.textContent = typeLabel;
    meta.textContent = "状态/상태: " + (STATUS_LABEL[job.status] || job.status) + " | " + (job.active_worker_count || 0) + "人/명 参与中";
    saveActiveJob(job.id, res.segment ? res.segment.id : null);
  } else {
    bar.classList.add("hidden");
    clearActiveJob();
  }
}

function goMyTask() {
  if (!_activeJobId) return;
  // Get job info from checkMyActiveJob's last result to determine page
  api({ action: "v2_ops_job_detail", job_id: _activeJobId }).then(function(res) {
    if (!res || !res.ok || !res.job) { goPage("home"); return; }
    var jt = res.job.job_type || "";
    if (jt === "unload") goPage("unload");
    else if (jt === "inbound_return") goPage("inbound_return");
    else if (jt.indexOf("inbound") === 0) goPage("inbound", { job_type: jt, biz_class: res.job.biz_class || "" });
    else if (jt === "load_outbound") goPage("outbound_load");
    else if (jt === "outbound_stock_op") goPage("outbound_stock_op");
    else if (jt === "pick_direct") goPage("pick_direct");
    else if (jt === "bulk_op") goPage("bulk_op");
    else if (jt === "issue_handle") goPage("issue_detail");
    else if (jt === "pickup_delivery_import") goPage("import_delivery");
    else if (jt === "verify_scan") { _vsBatchId = res.job.related_doc_id || ""; goPage("verify_scan"); }
    else goPage("generic_job", {
      flow_stage: res.job.flow_stage || "",
      biz_class: res.job.biz_class || "",
      job_type: jt,
      title: JOB_TYPE_LABEL[jt] || jt
    });
  });
}

var JOB_TYPE_LABEL = {
  unload: "卸货/하차",
  inbound_direct: "代发入库/직배송 입고",
  inbound_bulk: "大货入库/대량화물 입고",
  inbound_return: "退件入库/반품 입고",
  pick_direct: "代发拣货/직배송 피킹",
  bulk_op: "大货操作/대량화물 작업",
  pack_direct: "代发打包/직배송 포장",
  change_order: "换单操作/송장 교체",
  load_outbound: "出库装货/출고 상차",
  outbound_stock_op: "库内操作/창고 재고 작업",
  inventory: "盘点/재고조사",
  disposal: "废弃处理/폐기 처리",
  qc: "质检/품검",
  issue_handle: "问题点处理/이슈 처리",
  other_internal: "仓库整理/창고 정리",
  scan_pallet: "过机扫描/스캔",
  load_import: "装柜出货/적재 출고",
  pickup_delivery_import: "外出取/送货/외부 픽업·배송",
  verify_scan: "扫码核对/스캔 검수"
};

var STATUS_LABEL = {
  pending: "待到库/대기중",
  unloading: "卸货中（可提前理货）/하차중(입고가능)",
  unloading_putting_away: "卸货中+理货中/하차중+입고중",
  arrived_pending_putaway: "已到库待理货/입고대기",
  putting_away: "理货中/입고중",
  processing: "处理中/처리중",
  working: "作业中/작업중",
  responded: "已反馈/피드백완료",
  completed: "已入库/입고완료",
  closed: "已关闭/종료",
  cancelled: "已取消/취소됨",
  draft: "草稿/초안",
  issued: "已下发/배정됨",
  arrived: "已到货/도착",
  awaiting_close: "待收尾/마감대기",
  field_working: "现场卸货中/현장하차중",
  unloaded_pending_info: "待补充信息/정보보완대기",
  converted: "已转正/전환완료",
  verifying: "核对中/검수중",
  ready_to_ship: "待出库/출고대기",
  shipped: "已出库/출고완료",
  reopen_pending: "待再操作/재작업 대기",
  pending_issue: "待下发/배정대기",
  partially_completed: "部分入库完成/부분 입고 완료",
  // 出库库内操作型 V2
  operation_reserved: "操作预约/작업 예약",
  stock_operating: "操作中/작업중",
  pending_outbound_update: "待更新出库计划/출고 계획 업데이트 대기",
  preparing_outbound: "出库准备中/출고 준비중"
};

var ISSUE_STATUS_LABEL = {
  pending: "待处理/대기중",
  processing: "处理中/처리중",
  responded: "已反馈/피드백완료",
  rework_required: "需追加处理/추가처리필요",
  completed: "已完成/완료",
  closed: "已完成/완료",
  cancelled: "已作废/취소됨"
};

var BIZ_LABEL = {
  direct_ship: "代发/직배송",
  bulk: "大货/대량화물",
  return: "退件/반품",
  import: "进口/수입"
};

// PRIORITY_LABEL 已下线：业务规则改为 FIFO（按发布时间先后处理），UI 不再展示优先级。
// 数据库 v2_issue_tickets.priority 字段保留，后端 v2_issue_create 默认 'normal'，向后兼容旧数据。

// ===== Unload =====
var UNIT_TYPES = [
  { key: "container_large", zh: "大柜", ko: "대형 컨테이너" },
  { key: "container_small", zh: "小柜", ko: "소형 컨테이너" },
  { key: "pallet", zh: "托", ko: "팔레트" },
  { key: "carton", zh: "箱", ko: "박스" },
  { key: "cbm", zh: "方(CBM)", ko: "CBM" }
];

function unitLabel(key) {
  var u = UNIT_TYPES.find(function(t) { return t.key === key; });
  return u ? u.zh + "/" + u.ko : key;
}

// ===== 卸货页辅助（自 001/patch.js 合并） =====
function planNo(plan) { return (plan && (plan.display_no || plan.id)) || ''; }

function selectedUnloadDisplayNo() {
  var sel = document.getElementById('unloadPlanSelect');
  if (!sel || sel.selectedIndex < 1) return '';
  return sel.options[sel.selectedIndex].getAttribute('data-display-no') || '';
}

function renderUnloadPlanCard(planData, displayNo) {
  if (!planData || !planData.plan) return;
  _unloadPlanData = planData;
  var p = planData.plan;
  var lines = planData.lines || [];
  var card = document.getElementById('unloadPlanCard');
  var info = document.getElementById('unloadPlanInfo');
  var area = document.getElementById('unloadPlanLinesArea');
  if (card) card.style.display = '';
  if (info) {
    info.innerHTML = '<div><b>' + esc(displayNo || planNo(p)) + '</b> | ' +
      esc(p.plan_date) + ' | ' + esc(p.customer || '') + '</div>' +
      '<div class="muted">' + esc(p.cargo_summary || '') +
      (p.remark ? ' — ' + esc(p.remark) : '') + '</div>';
  }
  if (area) {
    if (lines.length > 0) {
      var tbl = '<table class="mini-table"><tr><th>类型/유형</th><th>计划/계획</th></tr>';
      lines.forEach(function(ln) { tbl += '<tr><td>' + unitLabel(ln.unit_type) + '</td><td>' + ln.planned_qty + '</td></tr>'; });
      tbl += '</table>';
      area.innerHTML = tbl;
    } else {
      area.innerHTML = '<span class="muted">无明细 / 명세 없음</span>';
    }
  }
}

async function previewSelectedPlan() {
  var sel = document.getElementById('unloadPlanSelect');
  var card = document.getElementById('unloadPlanCard');
  if (!sel || !card) return;
  var planId = sel.value || '';
  if (!planId) { card.style.display = 'none'; return; }
  var no = selectedUnloadDisplayNo();
  var res = await api({ action: 'v2_inbound_plan_detail', id: planId });
  if (res && res.ok && res.plan) {
    renderUnloadPlanCard(res, no || planNo(res.plan));
  } else {
    card.style.display = 'none';
  }
}

function stripDiffRequired() {
  var diffArea = document.getElementById('unloadDiffArea');
  if (!diffArea) return;
  var lbl = diffArea.querySelector('label');
  if (lbl) lbl.textContent = '现场差异说明 / 현장 차이 설명';
}

// 自 patch.js 合并：第二次 override 是最终有效逻辑（带 [现场动态]/[待补充] 标签）
// 显式 limit:200，并在客户端二次过滤 completed/cancelled，避免被默认分页截断
async function loadInboundPlans(selectId) {
  var sel = document.getElementById(selectId);
  if (!sel) return;
  var current = sel.value || '';
  var res = await api({
    action: 'v2_inbound_plan_list',
    start_date: '', end_date: '', status: '',
    limit: 200, offset: 0
  });
  var items = (res && res.ok && res.items) ? res.items.slice() : [];

  var opts = '<option value="">-- 选择入库计划/입고계획 선택 --</option>';
  var byDate = {};
  items.forEach(function(p) {
    if (p.status === 'completed' || p.status === 'cancelled') return;
    var d = p.plan_date || '';
    if (!byDate[d]) byDate[d] = [];
    byDate[d].push(p);
  });
  Object.keys(byDate).sort().reverse().forEach(function(d) {
    byDate[d].sort(function(a, b) { return String(a.created_at || '').localeCompare(String(b.created_at || '')); });
    byDate[d].forEach(function(p) {
      var no = p.display_no || p.id;
      var tag = (p.source_type === 'field_dynamic') ? '[现场动态] ' : '';
      if (p.status === 'unloaded_pending_info') tag = '[待补充] ';
      opts += '<option value="' + esc(p.id) + '" data-display-no="' + esc(no) + '">' +
        tag + '[' + esc(no) + '] ' + esc(p.customer || '') + ' - ' + esc(p.cargo_summary || '') + '</option>';
    });
  });
  sel.innerHTML = opts;
  if (current) sel.value = current;

  if (selectId === 'unloadPlanSelect') previewSelectedPlan();
}

async function initUnload() {
  _unloadPlanData = null;
  stopUnloadScan();

  // 有进行中的卸货任务 → 直接进入 working
  if (_activeJobId) {
    var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
    if (res && res.ok && res.job && res.job.job_type === "unload" && res.job.status === "working") {
      // 仅 inbound_plan 关联才回灌 plan 数据；feedback-first 流程 _unloadPlanData 保持 null
      if (res.job.related_doc_type === "inbound_plan" && res.job.related_doc_id) {
        var planRes = await api({ action: "v2_inbound_plan_detail", id: res.job.related_doc_id });
        if (planRes && planRes.ok) _unloadPlanData = planRes;
      } else {
        _unloadPlanData = null;
      }
      showUnloadWorking(res.job);
      startJobPoll("unload");
      return;
    }
  }

  showUnloadEntry();
  var interruptBanner = document.getElementById("unloadInterruptBanner");
  if (interruptBanner) interruptBanner.style.display = hasInterruptContext() ? "" : "none";
}

async function showUnloadEntry() {
  document.getElementById("unloadEntryCard").style.display = "";
  document.getElementById("unloadPlanCard").style.display = "none";
  document.getElementById("unloadWorkersCard").style.display = "none";
  document.getElementById("unloadResultCard").style.display = "none";
  await loadUnloadCandidates();
  loadUnplannedActiveList();
}

async function loadUnplannedActiveList() {
  var wrap = document.getElementById("unplannedActiveList");
  var box = document.getElementById("unplannedActiveItems");
  if (!wrap || !box) return;
  var res = await api({ action: "v2_unplanned_unload_active_list" });
  if (!res || !res.ok || !res.items || res.items.length === 0) {
    wrap.style.display = "none";
    box.innerHTML = "";
    return;
  }
  wrap.style.display = "";
  var html = "";
  res.items.forEach(function(item) {
    html += '<div style="border:1px solid #e67e22;border-radius:6px;padding:8px;margin-bottom:6px;background:#fff8f0;">';
    html += '<div style="font-weight:700;font-size:13px;">' + esc(item.display_no) + '</div>';
    html += '<div style="font-size:12px;color:#555;">';
    html += '发起: ' + esc(item.submitted_by) + ' · ' + esc(fmtTime(item.created_at));
    html += ' · 参与: ' + item.active_worker_count + '人';
    if (item.worker_names && item.worker_names.length > 0) {
      html += ' (' + esc(item.worker_names.join(', ')) + ')';
    }
    html += '</div>';
    html += '<button class="btn btn-outline btn-sm" style="margin-top:4px;" onclick="joinUnplannedUnload(\'' + esc(item.feedback_id) + '\', this)">加入卸货 / 참여</button>';
    html += '</div>';
  });
  box.innerHTML = html;
}

async function joinUnplannedUnload(feedbackId, btnEl) {
  withActionLock('joinUnplanned_' + feedbackId, btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_unplanned_unload_join",
      feedback_id: feedbackId,
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      localStorage.setItem('v2_unplanned_fb_id', res.feedback_id || '');
      _unloadPlanData = null;
      if (res.already_joined) {
        alert("已在此任务中 / 이미 참여 중: " + (res.display_no || ''));
      } else {
        alert("已加入计划外卸货: " + (res.display_no || '') + "\n계획외 하차 참여됨");
      }
      var jobRes = await api({ action: "v2_ops_job_detail", job_id: res.job_id });
      if (jobRes && jobRes.ok) showUnloadWorking(jobRes.job);
      startJobPoll("unload");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

function showUnloadWorking(job) {
  document.getElementById("unloadEntryCard").style.display = "none";
  document.getElementById("unloadWorkersCard").style.display = "";
  document.getElementById("unloadResultCard").style.display = "";

  if (_unloadPlanData && _unloadPlanData.plan) {
    renderUnloadPlanCard(_unloadPlanData, planNo(_unloadPlanData.plan));
  } else {
    document.getElementById("unloadPlanCard").style.display = "none";
  }

  buildUnloadResultForm();
  refreshUnloadWorkers();
  stripDiffRequired();
}

function buildUnloadResultForm() {
  var container = document.getElementById("unloadResultLines");
  var planLines = (_unloadPlanData && _unloadPlanData.lines) || [];

  if (planLines.length > 0) {
    // Build form based on plan lines
    var html = '<table class="mini-table"><tr><th>类型/유형</th><th>计划/계획</th><th>实际/실제</th></tr>';
    planLines.forEach(function(ln) {
      html += '<tr><td>' + unitLabel(ln.unit_type) + '</td><td>' + ln.planned_qty + '</td>' +
        '<td><input type="number" class="unload-actual-input" data-unit="' + esc(ln.unit_type) + '" ' +
        'value="0" min="0" step="0.1" style="width:70px;" onchange="checkUnloadDiff()"></td></tr>';
    });
    html += '</table>';
    container.innerHTML = html;
  } else {
    // No plan — free form with all unit types
    var html = '<table class="mini-table"><tr><th>类型/유형</th><th>实际数量/실제 수량</th></tr>';
    UNIT_TYPES.forEach(function(u) {
      html += '<tr><td>' + u.zh + '/' + u.ko + '</td>' +
        '<td><input type="number" class="unload-actual-input" data-unit="' + u.key + '" ' +
        'value="0" min="0" step="0.1" style="width:70px;" onchange="checkUnloadDiff()"></td></tr>';
    });
    html += '</table>';
    container.innerHTML = html;
  }

  checkUnloadDiff();
  updateUnloadActions();
}

function getUnloadResultLines() {
  var inputs = document.querySelectorAll(".unload-actual-input");
  var lines = [];
  for (var i = 0; i < inputs.length; i++) {
    var qty = parseFloat(inputs[i].value) || 0;
    if (qty > 0) {
      lines.push({ unit_type: inputs[i].getAttribute("data-unit"), actual_qty: qty });
    }
  }
  return lines;
}

function checkUnloadDiff() {
  var planLines = (_unloadPlanData && _unloadPlanData.lines) || [];
  var diffArea = document.getElementById("unloadDiffArea");
  if (planLines.length === 0) { diffArea.style.display = "none"; return; }

  var hasDiff = false;
  var inputs = document.querySelectorAll(".unload-actual-input");
  var actualMap = {};
  for (var i = 0; i < inputs.length; i++) {
    actualMap[inputs[i].getAttribute("data-unit")] = parseFloat(inputs[i].value) || 0;
  }
  planLines.forEach(function(ln) {
    if ((actualMap[ln.unit_type] || 0) !== (ln.planned_qty || 0)) hasDiff = true;
  });

  diffArea.style.display = hasDiff ? "" : "none";
}

async function updateUnloadActions() {
  var actionsDiv = document.getElementById("unloadActions");
  if (!_activeJobId) { actionsDiv.innerHTML = ""; return; }

  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  var workerCount = (res && res.ok && res.job) ? res.job.active_worker_count : 1;

  var html = '<button class="btn btn-outline" onclick="unloadLeave(this)">暂时离开 / 일시 퇴장</button>';
  if (workerCount <= 1) {
    html = '<button class="btn btn-success" onclick="unloadComplete(this)">完成卸货 / 하차 완료</button>' +
      '<div style="height:8px"></div>' + html;
  } else {
    html = '<div class="muted" style="margin-bottom:8px;">还有其他人参与中(' + workerCount + '人/명)，无法完成 / 다른 참여자 있음</div>' + html;
  }
  actionsDiv.innerHTML = html;
}

// biz_class 兜底映射：前端 job_type → 后端 biz_class
function _resolveBizClass() {
  var bc = _pageParams.biz_class || '';
  if (!bc) {
    var jt = _pageParams.job_type || '';
    if (jt === 'inbound_direct') bc = 'direct_ship';
    else if (jt === 'inbound_bulk') bc = 'bulk';
  }
  return bc;
}

async function loadInboundCandidates() {
  var sel = document.getElementById("inboundPlanSelect");
  if (!sel) return;
  var bc = _resolveBizClass();
  // 用 required_biz_class 让后端按 biz_task 未完成口径过滤；biz_class 同时传以兼容旧后端
  var res = await api({ action: "v2_inbound_plan_ops_candidates", scene: "putaway", biz_class: bc, required_biz_class: bc });
  var opts = '<option value="">-- 选择系统候选单 / 시스템 후보 선택 --</option>';
  if (res && res.ok && res.items) {
    res.items.forEach(function(p) {
      opts += '<option value="' + esc(p.id) + '" data-display-no="' + esc(p.display_no || '') + '">[' + esc(p.display_no || p.id) + '] ' + esc(p.customer) + ' - ' + esc(p.cargo_summary) + '</option>';
    });
  }
  sel.innerHTML = opts;
}

async function loadUnloadCandidates() {
  var sel = document.getElementById("unloadPlanSelect");
  if (!sel) return;
  // 显式 limit:200 防止下拉被默认 100 截断
  var res = await api({ action: "v2_inbound_plan_ops_candidates", scene: "unload", biz_class: "", limit: 200 });
  var opts = '<option value="">-- 选择入库计划/입고계획 선택 --</option>';
  if (res && res.ok && res.items) {
    res.items.forEach(function(p) {
      var stText = STATUS_LABEL[p.status] ? ' [' + STATUS_LABEL[p.status].split('/')[0] + ']' : '';
      var no = p.display_no || p.id;
      opts += '<option value="' + esc(p.id) + '" data-display-no="' + esc(no) + '">[' + esc(no) + ']' + stText + ' ' + esc(p.customer) + ' - ' + esc(p.cargo_summary) + '</option>';
    });
  }
  sel.innerHTML = opts;
}

function onInboundCandidateSelect() {
  var sel = document.getElementById("inboundPlanSelect");
  if (!sel || !sel.value) return;
  var opt = sel.options[sel.selectedIndex];
  var displayNo = (opt && opt.getAttribute('data-display-no')) || sel.value;
  var inp = document.getElementById("inboundCodeInput");
  if (inp) inp.value = displayNo;
  resolveInboundCode();
}

function clearInboundResolve() {
  _ibResolvedKind = '';
  _ibResolvedPlanId = '';
  _ibResolvedPlan = null;
  var inp = document.getElementById("inboundCodeInput"); if (inp) inp.value = '';
  var r = document.getElementById("ibResolveResult"); if (r) r.innerHTML = '';
  var ef = document.getElementById("ibExternalFields"); if (ef) ef.style.display = 'none';
  var sel = document.getElementById("inboundPlanSelect"); if (sel) sel.value = '';
  stopInboundScan();
}

async function resolveInboundCode(btnEl) {
  var inp = document.getElementById("inboundCodeInput");
  var code = (inp ? inp.value : '').trim();
  if (!code) { alert("请输入或扫描单号 / 번호를 입력하세요"); return; }
  var resultEl = document.getElementById("ibResolveResult");
  var extFields = document.getElementById("ibExternalFields");
  var bc = _resolveBizClass();

  if (btnEl) { btnEl.disabled = true; btnEl.textContent = '识别中...'; }
  try {
    var res = await api({ action: "v2_inbound_resolve_code", code: code, biz_class: bc });
    if (!res || !res.ok) { alert("识别失败 / 인식 실패"); return; }

    if (res.kind === 'system') {
      _ibResolvedKind = 'system';
      _ibResolvedPlanId = res.plan.id;
      _ibResolvedPlan = res.plan;
      if (resultEl) resultEl.innerHTML = '<div style="background:#e8f5e9;border-radius:6px;padding:8px;"><b>✓ 系统单</b> ' + esc(res.plan.display_no || res.plan.id) + '<br>' + esc(res.plan.customer) + ' · ' + esc(res.plan.cargo_summary || '--') + '</div>';
      if (extFields) extFields.style.display = 'none';
    } else if (res.kind === 'external') {
      _ibResolvedKind = 'external';
      _ibResolvedPlanId = '';
      _ibResolvedPlan = null;
      if (resultEl) resultEl.innerHTML = '<div style="background:#fff3e0;border-radius:6px;padding:8px;"><b>→ 外部单号</b>：' + esc(code) + '<br><span style="font-size:12px;color:#888;">请填写客户名后开始入库</span></div>';
      if (extFields) extFields.style.display = '';
    } else if (res.kind === 'biz_mismatch') {
      _ibResolvedKind = '';
      if (resultEl) resultEl.innerHTML = '<div style="background:#ffebee;border-radius:6px;padding:8px;color:#c62828;">✗ ' + esc(res.message) + '</div>';
      if (extFields) extFields.style.display = 'none';
    } else if (res.kind === 'biz_already_completed') {
      _ibResolvedKind = '';
      if (resultEl) resultEl.innerHTML = '<div style="background:#fff3e0;border-radius:6px;padding:8px;color:#e65100;">✓ 该入库单的此业务类型已完成入库 / 이 입고단의 해당 업무 유형은 이미 입고 완료됨<br><span style="font-size:12px;">' + esc(res.message || '') + '</span></div>';
      if (extFields) extFields.style.display = 'none';
    } else if (res.kind === 'status_not_allowed') {
      _ibResolvedKind = '';
      if (resultEl) resultEl.innerHTML = '<div style="background:#ffebee;border-radius:6px;padding:8px;color:#c62828;">✗ ' + esc(res.message) + '</div>';
      if (extFields) extFields.style.display = 'none';
    }
  } finally {
    if (btnEl) { btnEl.disabled = false; btnEl.textContent = '识别单号 / 번호 인식'; }
  }
}

// ===== 统一扫码器（入库页） =====
var _inboundScanner = null;
function startInboundScan() {
  if (_inboundScanner) { stopInboundScan(); return; }
  var readerEl = document.getElementById("inboundScanReader");
  if (!readerEl) return;
  readerEl.innerHTML = "";
  var btn = document.getElementById("ibScanBtn");
  try {
    _inboundScanner = new Html5Qrcode("inboundScanReader");
    _inboundScanner.start(
      { facingMode: "environment" },
      { fps: 10, qrbox: { width: 250, height: 150 } },
      function(decoded) {
        stopInboundScan();
        var code = String(decoded || "").trim();
        if (!code) return;
        var inp = document.getElementById("inboundCodeInput");
        if (inp) inp.value = code;
        resolveInboundCode();
      },
      function() {}
    ).catch(function(e) {
      alert("摄像头启动失败 / 카메라 시작 실패: " + e);
      _inboundScanner = null;
    });
    if (btn) btn.textContent = "取消扫码 / 스캔 취소";
  } catch(e) {
    alert("扫码不可用 / 스캔 불가: " + e.message);
  }
}
function stopInboundScan() {
  if (_inboundScanner) {
    try { _inboundScanner.stop(); } catch(e) {}
    _inboundScanner = null;
    var el = document.getElementById("inboundScanReader");
    if (el) el.innerHTML = "";
    var btn = document.getElementById("ibScanBtn");
    if (btn) btn.textContent = "📷 扫码";
  }
}

async function startUnload(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  var planId = document.getElementById("unloadPlanSelect").value;
  if (!planId) {
    alert("请先选择入库计划单 / 입고 계획을 먼저 선택하세요");
    return;
  }
  withActionLock('startUnload', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_unload_job_start",
      plan_id: planId,
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      biz_class: ""
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      stopUnloadScan();
      _unloadPlanData = null;
      if (planId) {
        var planRes = await api({ action: "v2_inbound_plan_detail", id: planId });
        if (planRes && planRes.ok) _unloadPlanData = planRes;
      }
      if (!res.already_joined) {
        alert(res.is_new_job ? "已创建卸货任务 / 하차 작업 생성됨" : "已加入卸货任务 / 하차 작업 참여됨");
      }
      var jobRes = await api({ action: "v2_ops_job_detail", job_id: res.job_id });
      if (jobRes && jobRes.ok) showUnloadWorking(jobRes.job);
      startJobPoll("unload");
    } else if (res && res.error === "unload_not_allowed_for_status") {
      alert("该入库计划已完成卸货，当前不能继续卸货\n이 입고계획은 이미 하차 완료되어 추가 하차 불가");
    } else if (res && res.error === "unload_status_inconsistent") {
      alert(res.message || "状态异常，请联系管理员");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function startUnloadNoPlan(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  withActionLock('startUnloadNoPlan', btnEl || null, '提交中.../저장중...', async function() {
    // 从挂起上下文带入 parent_job_id（自 patch.js 合并）
    var interruptInfo = null;
    try { interruptInfo = JSON.parse(localStorage.getItem(V2_INTERRUPT_KEY) || 'null'); } catch (e) {}
    var parentJobId = (interruptInfo && interruptInfo.parent_job_id) || '';

    // 客户端预检：避免重复创建 FB —— 如已有 active 计划外卸货，引导加入
    // （后端 existing_active_unplanned_unload 是兜底硬保障）
    if (!parentJobId) {
      try {
        var actRes = await api({ action: "v2_unplanned_unload_active_list" });
        if (actRes && actRes.ok && actRes.items && actRes.items.length > 0) {
          var item = actRes.items[0];
          var msg = "已有进行中的计划外卸货：" + (item.display_no || '') +
                    "（发起: " + (item.submitted_by || '?') + "，参与 " + (item.active_worker_count || 0) + " 人）\n" +
                    "请加入此任务而非重复创建。点确定 = 加入；取消 = 放弃。\n" +
                    "진행 중인 계획외 하차가 있습니다. 참여하세요.";
          if (confirm(msg)) {
            joinUnplannedUnload(item.feedback_id, btnEl);
          }
          return;
        }
      } catch (e) { /* 预检失败不阻塞，由后端兜底 */ }
    }

    var res = await api({
      action: "v2_unplanned_unload_start",
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      parent_job_id: parentJobId,
      interrupt_type: parentJobId ? 'unload' : ''
    });

    // 后端兜底：existing_active 自动转加入
    if (res && res.error === 'existing_active_unplanned_unload' && res.feedback_id) {
      var msg2 = (res.message || "已有进行中的计划外卸货任务") + "\n点确定加入此任务";
      if (confirm(msg2)) {
        joinUnplannedUnload(res.feedback_id, btnEl);
      }
      return;
    }

    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      localStorage.setItem('v2_unplanned_fb_id', res.feedback_id || '');
      stopUnloadScan();
      _unloadPlanData = null;
      alert("已创建计划外卸货单: " + (res.display_no || res.feedback_id) + "\n계획외 하차 작업 생성됨");
      var jobRes = await api({ action: "v2_ops_job_detail", job_id: res.job_id });
      if (jobRes && jobRes.ok) showUnloadWorking(jobRes.job);
      startJobPoll("unload");
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

function startUnloadScan() {
  if (_unloadScanner) { stopUnloadScan(); return; }
  var readerEl = document.getElementById("unloadScanReader");
  readerEl.innerHTML = "";
  try {
    _unloadScanner = new Html5Qrcode("unloadScanReader");
    _unloadScanner.start(
      { facingMode: "environment" },
      { fps: 10, qrbox: { width: 250, height: 150 } },
      async function(decoded) {
        stopUnloadScan();
        var res = await api({ action: "v2_inbound_plan_find_by_code", code: decoded });
        if (res && res.ok && res.plan) {
          var sel = document.getElementById("unloadPlanSelect");
          sel.value = res.plan.id;
          var label = res.plan.display_no || decoded;
          alert("已匹配入库计划: " + label + "\n입고계획 매칭됨: " + label);
          startUnload();
        } else {
          alert("未找到匹配入库计划 / 일치하는 입고계획 없음: " + decoded);
        }
      },
      function() {} // ignore scan errors
    ).catch(function(e) {
      alert("摄像头启动失败 / 카메라 시작 실패: " + e);
      _unloadScanner = null;
    });
  } catch(e) {
    alert("扫码不可用 / 스캔 불가: " + e.message);
  }
}

function stopUnloadScan() {
  if (_unloadScanner) {
    try { _unloadScanner.stop(); } catch(e) {}
    _unloadScanner = null;
    var el = document.getElementById("unloadScanReader");
    if (el) el.innerHTML = "";
  }
}

// (external scanner removed — replaced by unified inboundScan above)

async function unloadLeave(btnEl) {
  if (!_activeJobId) return;
  if (!confirm("确认暂时离开？/ 일시 퇴장하시겠습니까?")) return;
  withActionLock('unloadLeave', btnEl || null, '提交中.../저장중...', async function() {
    var fbId = localStorage.getItem('v2_unplanned_fb_id') || '';
    var leaveAction = fbId ? 'v2_unplanned_unload_finish' : 'v2_unload_job_finish';
    var res = await api({
      action: leaveAction,
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_only: true
    });
    if (res && res.ok) {
      localStorage.removeItem('v2_unplanned_fb_id');
      var resumed = await checkAndResumeParent();
      if (!resumed) {
        clearActiveJob();
        _unloadPlanData = null;
        goPage("home");
      }
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function unloadComplete(btnEl) {
  if (!_activeJobId) return;
  var resultLines = getUnloadResultLines();
  if (resultLines.length === 0) {
    alert("请至少填写一项实际数量 / 실제 수량을 최소 1건 입력하세요");
    return;
  }

  withActionLock('unloadComplete', btnEl || null, '提交中.../저장중...', async function() {
    var diffNote = ((document.getElementById("unloadDiffNote") || {}).value || "").trim();
    var remark = ((document.getElementById("unloadRemark") || {}).value || "").trim();

    // 自 patch.js 合并：实际数量与计划不一致但用户没填差异说明 → 自动补默认值（避免后端 diff_note_required）
    var planLines = (_unloadPlanData && _unloadPlanData.lines) || [];
    if (planLines.length > 0 && !diffNote) {
      var actualMap = {};
      resultLines.forEach(function(r) { actualMap[r.unit_type] = r.actual_qty; });
      var hasDiff = false;
      planLines.forEach(function(ln) {
        if ((actualMap[ln.unit_type] || 0) !== (ln.planned_qty || 0)) hasDiff = true;
      });
      if (hasDiff) diffNote = '现场实收数量与计划数量不一致';
    }

    var fbId = localStorage.getItem('v2_unplanned_fb_id') || '';
    var actionName = fbId ? 'v2_unplanned_unload_finish' : 'v2_unload_job_finish';

    var res = await api({
      action: actionName,
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      result_lines: resultLines,
      diff_note: diffNote,
      remark: remark,
      complete_job: true
    });

    if (res && res.ok) {
      var msg = "卸货已完成 / 하차 완료";
      if (fbId || res.feedback_id) {
        msg += "\n（现场反馈已更新，请协同中心补充信息并转正 / 현장 피드백 업데이트됨, 협업센터에서 정보 보완 후 전환 필요）";
      } else {
        msg += "\n（入库计划状态已变更为\u201C已到库待入库\u201D / 입고계획 상태가 \u201C입고대기\u201D로 변경됨）";
      }
      alert(msg);
      localStorage.removeItem('v2_unplanned_fb_id');

      var resumed = await checkAndResumeParent();
      if (!resumed) {
        clearActiveJob();
        _unloadPlanData = null;
        goPage("home");
      }
    } else if (res && res.error === "diff_note_required") {
      // 自 patch.js 合并：自动补默认差异说明 + 重试一次
      diffNote = '现场实收数量与计划数量不一致（自动补充）';
      var dnEl = document.getElementById('unloadDiffNote');
      if (dnEl) dnEl.value = diffNote;
      var retry = await api({
        action: actionName,
        job_id: _activeJobId,
        worker_id: getWorkerId(),
        result_lines: resultLines,
        diff_note: diffNote,
        remark: remark,
        complete_job: true
      });
      if (retry && retry.ok) {
        alert("卸货已完成（已自动补充差异备注）/ 하차 완료 (차이 메모 자동 추가됨)");
        localStorage.removeItem('v2_unplanned_fb_id');
        var resumedR = await checkAndResumeParent();
        if (!resumedR) { clearActiveJob(); _unloadPlanData = null; goPage("home"); }
      } else {
        alert("失败/실패: " + (retry ? (retry.message || retry.error) : "unknown"));
      }
    } else if (res && res.error === "already_completed") {
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다. 중복 제출하지 마세요");
      localStorage.removeItem('v2_unplanned_fb_id');
      var resumed2 = await checkAndResumeParent();
      if (!resumed2) { clearActiveJob(); _unloadPlanData = null; goPage("home"); }
    } else if (res && res.error === "unload_plan_status_invalid") {
      alert("当前卸货计划状态已变化，不能继续完成，请返回刷新\n하차 계획 상태가 변경되었습니다. 새로고침해 주세요");
      clearActiveJob();
      _unloadPlanData = null;
      goPage("home");
    } else if (res && res.error === "others_still_working") {
      var _n1 = res.active_worker_count || res.active_count || "?";
      alert("您已退出此任务，还有 " + _n1 + " 人继续作业\n현재 작업에서 퇴장했습니다. " + _n1 + "명이 계속 작업 중입니다");
      localStorage.removeItem('v2_unplanned_fb_id');
      var resumed3 = await checkAndResumeParent();
      if (!resumed3) { clearActiveJob(); _unloadPlanData = null; goPage("home"); }
    } else if (res && res.error === "empty_result") {
      alert(res.message || "至少填写一项实际数量");
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

function unloadGoBack() {
  if (_activeJobId) {
    if (confirm("离开将暂停当前任务 / 퇴장 시 현재 작업이 일시정지됩니다. 确认？/ 확인?")) {
      unloadLeave();
    }
  } else if (hasInterruptContext()) {
    checkAndResumeParent();
  } else {
    goPage("home");
  }
}

async function refreshUnloadWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) {
    renderWorkers("unloadWorkers", res.workers);
    updateUnloadActions();
  }
}

function startJobPoll(type) {
  if (_pollTimer) clearInterval(_pollTimer);
  _pollTimer = setInterval(function() {
    if (type === "unload") refreshUnloadWorkers();
    if (type === "load") refreshLoadWorkers();
    if (type === "inbound") refreshInboundWorkers();
    if (type === "inbound_return") refreshInboundReturnWorkers();
    if (type === "generic") refreshGenericWorkers();
    if (type === "pick") refreshPickWorkers();
    if (type === "bulk") refreshBulkWorkers();
    if (type === "import_delivery") refreshImportDeliveryWorkers();
    if (type === "verify_scan") refreshVerifyScanWorkers();
  }, 5000);
}

function renderWorkers(containerId, workers) {
  var el = document.getElementById(containerId);
  if (!el) return;
  if (!workers || workers.length === 0) {
    el.innerHTML = '<span class="muted">无 / 없음</span>';
    return;
  }
  var activeW = workers.filter(function(w) { return !w.left_at; });
  // UI 去重兜底（止血）— 根修复在后端 findOpenSeg 防重 + closeAllOpenSegs 自愈
  var seen = {};
  var deduped = [];
  activeW.forEach(function(w) {
    var wid = w.worker_id || w.id;
    if (!seen[wid]) { seen[wid] = true; deduped.push(w); }
  });
  var html = '<div style="font-size:12px;color:#666;margin-bottom:6px;">' +
    '在岗/참여중: ' + deduped.length + '人/명</div>';
  deduped.forEach(function(w) {
    html += '<span class="worker-tag">' + esc(w.worker_name || w.worker_id) + '</span>';
  });
  el.innerHTML = html;
}

// ===== Inbound (standard: direct_ship + bulk) =====
// ===== 统一入库入口：resolve 状态 =====
var _ibResolvedKind = ''; // 'system' | 'external' | ''
var _ibResolvedPlanId = '';
var _ibResolvedPlan = null; // plan summary object from resolve

async function initInbound() {
  var title = document.getElementById("inboundTitle");
  var jt = _pageParams.job_type || "inbound_direct";
  title.textContent = JOB_TYPE_LABEL[jt] || "理货入库/입고정리";

  // If already in a putaway job, show working state
  if (_activeJobId) {
    var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
    if (res && res.ok && res.job && res.job.job_type && (res.job.job_type === 'inbound_direct' || res.job.job_type === 'inbound_bulk') && res.job.status === "working") {
      document.getElementById("inboundEntryCard").style.display = "none";
      document.getElementById("inboundWorkingCard").style.display = "";
      loadInboundPlanInfo(res.job.related_doc_id);
      refreshInboundWorkers();
      startJobPoll("inbound");
      return;
    }
  }

  document.getElementById("inboundEntryCard").style.display = "";
  document.getElementById("inboundWorkingCard").style.display = "none";
  // Reset unified entry
  clearInboundResolve();
  var exCu = document.getElementById("inboundExternalCustomer"); if (exCu) exCu.value = '';
  var exRe = document.getElementById("inboundExternalRemark"); if (exRe) exRe.value = '';
  // Reset extra_ops inputs
  var eSort = document.getElementById("inboundExtraSort"); if (eSort) eSort.value = '0';
  var eLabel = document.getElementById("inboundExtraLabel"); if (eLabel) eLabel.value = '0';
  var eRepair = document.getElementById("inboundExtraRepair"); if (eRepair) eRepair.value = '0';
  var eOther = document.getElementById("inboundExtraOther"); if (eOther) eOther.value = '';
  var rNote = document.getElementById("inboundResultNote"); if (rNote) rNote.value = '';
  var rRmk = document.getElementById("inboundRemark"); if (rRmk) rRmk.value = '';
  await loadInboundCandidates();
}

async function startInbound(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  if (!_ibResolvedKind) {
    alert("请先识别单号 / 먼저 번호를 인식하세요");
    return;
  }

  var jobType = _pageParams.job_type || "inbound_direct";
  var bizClass = _resolveBizClass();

  var payload = {
    action: "v2_inbound_job_start",
    worker_id: getWorkerId(),
    worker_name: getWorkerName(),
    biz_class: bizClass,
    job_type: jobType
  };

  if (_ibResolvedKind === 'system') {
    payload.plan_id = _ibResolvedPlanId;
  } else if (_ibResolvedKind === 'external') {
    var exNo = ((document.getElementById("inboundCodeInput") || {}).value || "").trim();
    var exCu = ((document.getElementById("inboundExternalCustomer") || {}).value || "").trim();
    var exRe = ((document.getElementById("inboundExternalRemark") || {}).value || "").trim();
    if (!exNo) { alert("请输入外部入库单号 / 외부 입고번호를 입력하세요"); return; }
    if (!exCu) { alert("请输入客户名 / 고객명을 입력하세요"); return; }
    payload.external_inbound_no = exNo;
    payload.customer_name = exCu;
    payload.start_remark = exRe;
  }

  withActionLock('startInbound', btnEl || null, '提交中.../저장중...', async function() {
    stopInboundScan();
    var res = await api(payload);
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      if (!res.already_joined) {
        alert(res.is_new_job ? "已创建入库任务，状态变更为入库中 / 입고 작업 생성됨" : "已加入入库任务 / 입고 작업 참여됨");
      }
      document.getElementById("inboundEntryCard").style.display = "none";
      document.getElementById("inboundWorkingCard").style.display = "";
      loadInboundPlanInfo(res.plan_id || payload.plan_id);
      refreshInboundWorkers();
      startJobPoll("inbound");
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

var _inboundPlanData = null;

async function loadInboundPlanInfo(planId) {
  _inboundPlanData = null;
  var infoEl = document.getElementById("inboundPlanInfo");
  var linesEl = document.getElementById("inboundResultLines");
  if (!planId) {
    if (infoEl) infoEl.innerHTML = '<span class="muted">--</span>';
    if (linesEl) linesEl.innerHTML = '';
    return;
  }
  var res = await api({ action: "v2_inbound_plan_detail", id: planId });
  if (!res || !res.ok || !res.plan) return;
  _inboundPlanData = res;
  var p = res.plan;
  var lines = res.lines || [];
  // Plan info card
  if (infoEl) {
    var html = '<div><b>' + esc(p.display_no || p.id) + '</b> · ' + esc(p.customer || '--') + '</div>';
    html += '<div>' + esc(p.cargo_summary || '--') + '</div>';
    infoEl.innerHTML = html;
  }
  // Build result lines form
  var unloadNotDone = (p.status === 'unloading' || p.status === 'unloading_putting_away');
  if (linesEl) {
    if (lines.length > 0) {
      var html = '<table style="width:100%;font-size:13px;border-collapse:collapse;margin-bottom:8px;">';
      html += '<thead><tr style="background:#f5f5f5;"><th style="padding:4px 6px;text-align:left;">类型</th><th style="padding:4px 6px;">卸货实到</th><th style="padding:4px 6px;">本次入库</th></tr></thead><tbody>';
      lines.forEach(function(ln) {
        var actualQty = ln.actual_qty || 0;
        var actualDisplay = unloadNotDone ? '<span style="color:#e67e22;font-weight:700;">卸货中/하차중</span>' : String(actualQty);
        html += '<tr>';
        html += '<td style="padding:4px 6px;">' + esc(ln.unit_type || '--') + '</td>';
        html += '<td style="padding:4px 6px;text-align:center;">' + actualDisplay + '</td>';
        html += '<td style="padding:4px 6px;"><input type="number" class="input ib-putaway-input" data-unit="' + esc(ln.unit_type || '') + '" value="' + (unloadNotDone ? '' : actualQty) + '" min="0" style="width:80px;text-align:center;" placeholder="' + (unloadNotDone ? '待卸货完成' : '') + '"></td>';
        html += '</tr>';
      });
      html += '</tbody></table>';
      if (unloadNotDone) {
        html += '<div style="font-size:12px;color:#e67e22;margin-bottom:6px;">⚠ 卸货尚未完成，实到数量待更新。卸货完成后方可完成理货。<br>⚠ 하차 미완료, 실수량 미확정. 하차 완료 후 입고 완료 가능.</div>';
      }
      linesEl.innerHTML = html;
    } else {
      linesEl.innerHTML = '<div style="font-size:12px;color:#999;">无明细行，完成时仅记录备注</div>';
    }
  }
}

async function inboundLeave(btnEl) {
  if (!_activeJobId) return;
  if (!confirm("确认暂时离开？/ 일시 퇴장하시겠습니까?")) return;
  withActionLock('inboundLeave', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_inbound_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_only: true
    });
    if (res && res.ok) {
      clearActiveJob();
      goPage("home");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function finishInbound(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  // Front-end pre-check: block finish if unload not done
  if (_inboundPlanData && _inboundPlanData.plan) {
    var pStatus = _inboundPlanData.plan.status;
    if (pStatus === 'unloading' || pStatus === 'unloading_putting_away') {
      alert("卸货未完成，无法完成理货。请等待卸货结束后再完成。\n하차가 아직 완료되지 않아 입고 완료 처리할 수 없습니다. 하차 완료 후 다시 시도하세요.");
      return;
    }
  }
  withActionLock('finishInbound', btnEl || null, '提交中.../저장중...', async function() {
    var remark = (document.getElementById("inboundRemark") || {}).value || "";
    var resultNote = (document.getElementById("inboundResultNote") || {}).value || "";

    // Collect putaway result lines
    var resultLines = [];
    var inputs = document.querySelectorAll(".ib-putaway-input");
    for (var i = 0; i < inputs.length; i++) {
      var inp = inputs[i];
      var qty = Number(inp.value || 0);
      resultLines.push({ unit_type: inp.getAttribute("data-unit") || "", putaway_qty: qty });
    }

    // Collect extra_ops (额外作业量)
    var extraOps = {
      sort_qty: Number(((document.getElementById("inboundExtraSort") || {}).value) || 0) || 0,
      label_qty: Number(((document.getElementById("inboundExtraLabel") || {}).value) || 0) || 0,
      repair_box_qty: Number(((document.getElementById("inboundExtraRepair") || {}).value) || 0) || 0,
      other_op_remark: (((document.getElementById("inboundExtraOther") || {}).value) || "").trim()
    };

    var res = await api({
      action: "v2_inbound_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      remark: remark.trim(),
      result_note: resultNote.trim(),
      result_lines: resultLines,
      extra_ops: extraOps,
      complete_job: true
    });
    if (res && res.ok) {
      // Check plan status to show appropriate message
      var planAfter = null;
      try { planAfter = await api({ action: "v2_inbound_plan_detail", plan_id: _inboundPlanData && _inboundPlanData.plan ? _inboundPlanData.plan.id : "" }); } catch(e) {}
      var planStatus = (planAfter && planAfter.ok && planAfter.plan) ? planAfter.plan.status : "completed";
      if (planStatus === "completed") {
        alert("入库已完成，状态已更新为\u201C已入库\u201D\n입고 완료, 상태가 \u201C입고완료\u201D로 변경됨");
      } else if (planStatus === "unloading" || planStatus === "unloading_putting_away") {
        alert("本次理货已完成。卸货仍在进行中，如还有未理部分可后续继续理货。\n이번 입고 완료. 하차 진행 중이며, 미입고분은 이후 계속 가능합니다.");
      } else {
        alert("本次理货已完成。仍有部分货物未理完，后续可继续理货。\n이번 입고 완료. 미입고분이 있어 이후 계속 가능합니다.");
      }
      clearActiveJob();
      _inboundPlanData = null;
      goPage("home");
    } else if (res && res.error === "already_completed") {
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다. 중복 제출하지 마세요");
      clearActiveJob();
      _inboundPlanData = null;
      goPage("home");
    } else if (res && res.error === "unload_not_finished") {
      // Unload still running — user stays on current page, no logout
      alert("卸货未完成，无法完成理货。请等待卸货结束后再完成。\n하차가 아직 완료되지 않아 입고 완료 처리할 수 없습니다. 하차 완료 후 다시 시도하세요.");
    } else if (res && res.error === "inbound_plan_status_invalid") {
      alert("当前入库计划状态已变化，不能继续完成，请返回刷新\n입고계획 상태가 변경되었습니다. 새로고침해 주세요");
      clearActiveJob();
      _inboundPlanData = null;
      goPage("home");
    } else if (res && res.error === "others_still_working") {
      var _n2 = res.active_worker_count || res.active_count || "?";
      alert("您已退出此任务，还有 " + _n2 + " 人继续作业\n현재 작업에서 퇴장했습니다. " + _n2 + "명이 계속 작업 중입니다");
      clearActiveJob();
      _inboundPlanData = null;
      goPage("home");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function refreshInboundWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("inboundWorkers", res.workers);
}

// ===== Return Inbound (lightweight work-time tracking) =====
async function initInboundReturn() {
  // If already in a return inbound job, show working state
  if (_activeJobId) {
    var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
    if (res && res.ok && res.job && res.job.job_type === 'inbound_return' && res.job.status === 'working') {
      document.getElementById("inboundReturnEntryCard").style.display = "none";
      document.getElementById("inboundReturnWorkingCard").style.display = "";
      renderInboundReturnSession(res.job);
      refreshInboundReturnWorkers();
      startJobPoll("inbound_return");
      return;
    }
  }
  document.getElementById("inboundReturnEntryCard").style.display = "";
  document.getElementById("inboundReturnWorkingCard").style.display = "none";
  var cu = document.getElementById("inboundReturnCustomer"); if (cu) cu.value = '';
  var sr = document.getElementById("inboundReturnStartRemark"); if (sr) sr.value = '';
  var rn = document.getElementById("inboundReturnResultNote"); if (rn) rn.value = '';
  var rm = document.getElementById("inboundReturnRemark"); if (rm) rm.value = '';
}

function renderInboundReturnSession(job) {
  var el = document.getElementById("inboundReturnSessionInfo");
  if (!el) return;
  var html = '<div><b>任务号/작업번호:</b> ' + esc(job.id) + '</div>';
  el.innerHTML = html;
}

async function startInboundReturn(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  var customer = ((document.getElementById("inboundReturnCustomer") || {}).value || "").trim();
  var startRemark = ((document.getElementById("inboundReturnStartRemark") || {}).value || "").trim();
  withActionLock('startInboundReturn', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_inbound_job_start",
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      biz_class: "return",
      job_type: "inbound_return",
      customer_name: customer,
      start_remark: startRemark
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      alert("已开始退件入库 / 반품 입고 시작됨");
      document.getElementById("inboundReturnEntryCard").style.display = "none";
      document.getElementById("inboundReturnWorkingCard").style.display = "";
      var jobRes = await api({ action: "v2_ops_job_detail", job_id: res.job_id });
      if (jobRes && jobRes.ok) renderInboundReturnSession(jobRes.job);
      refreshInboundReturnWorkers();
      startJobPoll("inbound_return");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function inboundReturnLeave(btnEl) {
  if (!_activeJobId) return;
  if (!confirm("确认暂时离开？/ 일시 퇴장하시겠습니까?")) return;
  withActionLock('inboundReturnLeave', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_inbound_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_only: true
    });
    if (res && res.ok) {
      clearActiveJob();
      goPage("home");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function finishInboundReturn(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  withActionLock('finishInboundReturn', btnEl || null, '提交中.../저장중...', async function() {
    var remark = (document.getElementById("inboundReturnRemark") || {}).value || "";
    var resultNote = (document.getElementById("inboundReturnResultNote") || {}).value || "";
    var res = await api({
      action: "v2_inbound_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      remark: remark.trim(),
      result_note: resultNote.trim(),
      complete_job: true
    });
    if (res && res.ok) {
      alert("退件入库工时已记录 / 반품 입고 작업 시간 기록됨");
      clearActiveJob();
      goPage("home");
    } else if (res && res.error === "already_completed") {
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다");
      clearActiveJob();
      goPage("home");
    } else if (res && res.error === "others_still_working") {
      var _n3 = res.active_worker_count || res.active_count || "?";
      alert("您已退出此任务，还有 " + _n3 + " 人继续作业\n현재 작업에서 퇴장했습니다. " + _n3 + "명이 계속 작업 중입니다");
      clearActiveJob();
      goPage("home");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function refreshInboundReturnWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("inboundReturnWorkers", res.workers);
}

// ===== Import Delivery (外出取/送货) =====
async function initImportDelivery() {
  if (_activeJobId) {
    var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
    if (res && res.ok && res.job && res.job.job_type === 'pickup_delivery_import' && res.job.status === 'working') {
      document.getElementById("idEntryCard").style.display = "none";
      document.getElementById("idWorkingCard").style.display = "";
      renderImportDeliverySession(res.job);
      refreshImportDeliveryWorkers();
      startJobPoll("import_delivery");
      return;
    }
  }
  document.getElementById("idEntryCard").style.display = "";
  document.getElementById("idWorkingCard").style.display = "none";
  var d = document.getElementById("idDestination"); if (d) d.value = '';
  var p = document.getElementById("idPieceCount"); if (p) p.value = '';
  var r = document.getElementById("idRemark"); if (r) r.value = '';
}

function renderImportDeliverySession(job) {
  var el = document.getElementById("idSessionInfo");
  if (!el) return;
  var html = '<div><b>任务号/작업번호:</b> ' + esc(job.id) + '</div>';
  el.innerHTML = html;
}

async function startImportDelivery(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  withActionLock('startImportDelivery', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_import_delivery_job_start",
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      alert("已开始外出 / 외부 출발 완료");
      document.getElementById("idEntryCard").style.display = "none";
      document.getElementById("idWorkingCard").style.display = "";
      var jobRes = await api({ action: "v2_ops_job_detail", job_id: res.job_id });
      if (jobRes && jobRes.ok) renderImportDeliverySession(jobRes.job);
      refreshImportDeliveryWorkers();
      startJobPoll("import_delivery");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function finishImportDelivery(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  var dest = ((document.getElementById("idDestination") || {}).value || "").trim();
  if (!dest) { alert("请填写去向 / 목적지를 입력하세요"); return; }
  var pieceCount = parseInt((document.getElementById("idPieceCount") || {}).value || "0", 10) || 0;
  var remark = ((document.getElementById("idRemark") || {}).value || "").trim();
  withActionLock('finishImportDelivery', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_import_delivery_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      complete_job: true,
      destination_note: dest,
      estimated_piece_count: pieceCount,
      remark: remark
    });
    if (res && res.ok) {
      alert("外出任务完成 / 외부 작업 완료");
      clearActiveJob();
      goPage("home");
    } else if (res && res.error === "already_completed") {
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다");
      clearActiveJob();
      goPage("home");
    } else if (res && res.error === "others_still_working") {
      var _n = res.active_worker_count || res.active_count || "?";
      alert("您已退出此任务，还有 " + _n + " 人继续作业\n현재 작업에서 퇴장했습니다. " + _n + "명이 계속 작업 중입니다");
      clearActiveJob();
      goPage("home");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function leaveImportDelivery(btnEl) {
  if (!_activeJobId) return;
  if (!confirm("确认暂时离开？/ 일시 퇴장하시겠습니까?")) return;
  withActionLock('leaveImportDelivery', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_import_delivery_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_only: true
    });
    if (res && res.ok) {
      clearActiveJob();
      goPage("home");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function refreshImportDeliveryWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("idWorkers", res.workers);
}

// ===== Outbound Load =====
async function initOutboundLoad() {
  clearOutboundResolve();
  stopOutboundScan();

  if (_activeJobId) {
    var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
    if (res && res.ok && res.job && res.job.job_type === 'load_outbound' && res.job.status === 'working') {
      showOutboundLoadWorking();
      refreshLoadWorkers();
      startJobPoll("load");
      return;
    }
  }

  showOutboundLoadEntry();
  await loadOutboundOrders();
  startJobPoll("load");
}

function showOutboundLoadEntry() {
  document.getElementById("obLoadEntryCard").style.display = "";
  document.getElementById("obLoadActionCard").style.display = "none";
  document.getElementById("loadResultCard").style.display = "none";
  document.getElementById("loadInterruptBar").style.display = "none";
}

function showOutboundLoadWorking() {
  document.getElementById("obLoadEntryCard").style.display = "none";
  document.getElementById("obLoadActionCard").style.display = "";
  document.getElementById("loadResultCard").style.display = "";
  document.getElementById("loadInterruptBar").style.display = "";
}

async function loadOutboundOrders() {
  var sel = document.getElementById("loadOrderSelect");
  if (!sel) return;
  var res = await api({ action: "v2_outbound_order_list", start_date: "", end_date: "" });
  var opts = '<option value="">-- 选择出库单/출고단 선택 --</option>';
  if (res && res.ok && res.items) {
    res.items.forEach(function(o) {
      // 装货页只显示可装货状态：ready_to_ship / preparing_outbound（库内操作型已被客服更新）
      if (o.status !== "ready_to_ship" && o.status !== "preparing_outbound") return;
      var bizTag = o.biz_class ? '['+ o.biz_class + '] ' : '';
      opts += '<option value="' + esc(o.id) + '">[' + esc(o.status) + '] ' + bizTag + esc(o.order_date) + ' ' + esc(o.customer) + '</option>';
    });
  }
  sel.innerHTML = opts;
}

// ===== 出库装货：扫码/识别/清空 =====
var _obResolvedOrderId = "";
var _obLoadScanner = null;

async function resolveOutboundCode(btnEl) {
  var inp = document.getElementById("obLoadCodeInput");
  var code = (inp ? inp.value : '').trim();
  if (!code) { alert("请输入或扫描单号 / 번호를 입력하세요"); return; }
  var resultEl = document.getElementById("obResolveResult");

  if (btnEl) { btnEl.disabled = true; btnEl.textContent = '识别中...'; }
  try {
    var res = await api({ action: "v2_outbound_order_resolve_code", code: code });
    if (!res || !res.ok) { alert("识别失败 / 인식 실패"); return; }

    if (res.kind === 'system') {
      var o = res.order;
      _obResolvedOrderId = o.id;
      document.getElementById("loadOrderSelect").value = o.id;
      _refreshOutboundLoadMaterial();
      var modeMap = {warehouse_dispatch:'仓库代发',customer_pickup:'客户自提',milk_express:'牛奶速递',milk_pallet:'牛奶托盘',container_pickup:'整柜提货'};
      if (resultEl) resultEl.innerHTML = '<div style="background:#e8f5e9;border-radius:6px;padding:8px;">' +
        '<b>✓ </b>' + esc(o.display_no || o.id) + '<br>' +
        esc(o.customer || '--') + ' · ' + (modeMap[o.outbound_mode] || o.outbound_mode || '--') + '<br>' +
        '计划: ' + (o.planned_box_count || 0) + '箱 / ' + (o.planned_pallet_count || 0) + '托' +
        '</div>';
    } else if (res.kind === 'not_found') {
      _obResolvedOrderId = "";
      if (resultEl) resultEl.innerHTML = '<div style="background:#ffebee;border-radius:6px;padding:8px;color:#c62828;">✗ ' + esc(res.message) + '</div>';
    } else if (res.kind === 'status_not_allowed') {
      _obResolvedOrderId = "";
      if (resultEl) resultEl.innerHTML = '<div style="background:#ffebee;border-radius:6px;padding:8px;color:#c62828;">✗ ' + esc(res.message) + '</div>';
    }
  } finally {
    if (btnEl) { btnEl.disabled = false; btnEl.textContent = '识别单号 / 번호 인식'; }
  }
}

function clearOutboundResolve() {
  _obResolvedOrderId = "";
  var inp = document.getElementById("obLoadCodeInput");
  if (inp) inp.value = "";
  var resultEl = document.getElementById("obResolveResult");
  if (resultEl) resultEl.innerHTML = "";
  var matEl = document.getElementById("obLoadMaterialBox");
  if (matEl) matEl.innerHTML = "";
  document.getElementById("loadOrderSelect").value = "";
}

function startOutboundScan() {
  if (_obLoadScanner) { stopOutboundScan(); return; }
  var readerEl = document.getElementById("obLoadScanReader");
  if (!readerEl) return;
  readerEl.innerHTML = "";
  var btn = document.getElementById("obScanBtn");
  try {
    _obLoadScanner = new Html5Qrcode("obLoadScanReader");
    _obLoadScanner.start(
      { facingMode: "environment" },
      { fps: 10, qrbox: { width: 250, height: 150 } },
      function(decoded) {
        stopOutboundScan();
        var code = String(decoded || "").trim();
        if (!code) return;
        var inp = document.getElementById("obLoadCodeInput");
        if (inp) inp.value = code;
        resolveOutboundCode();
      },
      function() {}
    ).catch(function(e) {
      alert("摄像头启动失败 / 카메라 시작 실패: " + e);
      _obLoadScanner = null;
    });
    if (btn) btn.textContent = "取消扫码 / 스캔 취소";
  } catch(e) {
    alert("扫码不可用 / 스캔 불가: " + e.message);
  }
}

function stopOutboundScan() {
  if (_obLoadScanner) {
    try { _obLoadScanner.stop(); } catch(e) {}
    _obLoadScanner = null;
    var el = document.getElementById("obLoadScanReader");
    if (el) el.innerHTML = "";
    var btn = document.getElementById("obScanBtn");
    if (btn) btn.textContent = "📷 扫码";
  }
}

function onOutboundCandidateSelect() {
  var sel = document.getElementById("loadOrderSelect");
  var id = sel ? sel.value : "";
  if (!id) return;
  _obResolvedOrderId = id;
  var resultEl = document.getElementById("obResolveResult");
  var opt = sel.options[sel.selectedIndex];
  if (resultEl && opt) resultEl.innerHTML = '<div style="background:#e8f5e9;border-radius:6px;padding:8px;"><b>✓ </b>' + esc(opt.textContent) + '</div>';
  _refreshOutboundLoadMaterial();
}

async function startOutboundLoad(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  withActionLock('startOutboundLoad', btnEl || null, '提交中.../저장중...', async function() {
    var orderId = _obResolvedOrderId || document.getElementById("loadOrderSelect").value || "";
    var res = await api({
      action: "v2_outbound_load_start",
      order_id: orderId,
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      biz_class: ""
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      if (!res.already_joined) {
        alert(res.is_new_job ? "已创建装货任务 / 상차 작업 생성됨" : "已加入装货任务 / 상차 작업 참여됨");
      }
      showOutboundLoadWorking();
      refreshLoadWorkers();
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

function startLoadNoOrder() {
  _obResolvedOrderId = "";
  document.getElementById("loadOrderSelect").value = "";
  startOutboundLoad();
}

async function finishOutboundLoad(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  withActionLock('finishOutboundLoad', btnEl || null, '提交中.../저장중...', async function() {
    var box = parseInt(document.getElementById("loadBoxCount").value) || 0;
    var pallet = parseInt(document.getElementById("loadPalletCount").value) || 0;
    var remark = document.getElementById("loadRemark").value.trim();

    var res = await api({
      action: "v2_outbound_load_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      box_count: box,
      pallet_count: pallet,
      remark: remark,
      complete_job: true
    });
    if (res && res.ok) {
      alert("装货已完成 / 상차 완료");
      var resumed = await checkAndResumeParent();
      if (!resumed) {
        clearActiveJob();
        goPage("home");
      }
    } else if (res && res.error === "already_completed") {
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다. 중복 제출하지 마세요");
      var resumed2 = await checkAndResumeParent();
      if (!resumed2) {
        clearActiveJob();
        goPage("home");
      }
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function saveOutboundLoadResult(btnEl) {
  await finishOutboundLoad(btnEl);
}

function outboundLoadGoBack() {
  if (_activeJobId) {
    alert("装货进行中，请先完成或暂时离开 / 상차 진행 중입니다. 먼저 완료하거나 퇴장하세요");
    return;
  }
  if (hasInterruptContext()) {
    checkAndResumeParent();
  } else {
    goPage("outbound_menu");
  }
}

async function refreshLoadWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("loadWorkers", res.workers);
}

// ===== 出库资料展示 helper（执行系统通用） =====
async function renderOutboundMaterials(orderId, containerId) {
  var box = document.getElementById(containerId);
  if (!box) return;
  if (!orderId) { box.innerHTML = ''; return; }
  box.innerHTML = '<div class="muted" style="font-size:12px;">资料加载中... / 자료 로딩...</div>';
  try {
    var res = await api({
      action: "v2_attachment_list",
      related_doc_type: "outbound_order",
      related_doc_id: orderId
    });
    if (!res || !res.ok) { box.innerHTML = ''; return; }
    var atts = (res.items || []).filter(function(a) { return a.attachment_category === 'outbound_material'; });
    if (atts.length === 0) {
      box.innerHTML = '<div class="muted" style="font-size:12px;">暂无出库资料 / 출고 자료 없음</div>';
      return;
    }
    var html = '<div style="font-weight:700;margin-bottom:4px;">出库资料 / 출고 자료 (' + atts.length + ')</div>';
    atts.forEach(function(att) {
      var url = V2_API + "/file?key=" + encodeURIComponent(att.file_key);
      html += '<div style="padding:4px 0;border-bottom:1px solid #f0f0f0;">';
      html += esc(att.file_name) + ' ';
      html += '<a class="btn btn-outline btn-sm" href="' + esc(url) + '" download="' + esc(att.file_name) + '">下载/다운로드</a> ';
      html += '<a class="btn btn-outline btn-sm" href="' + esc(url) + '" target="_blank" rel="noopener">打开/打印·열기/인쇄</a>';
      html += '</div>';
    });
    box.innerHTML = html;
  } catch (e) {
    box.innerHTML = '<div class="muted" style="color:#c62828;font-size:12px;">资料加载失败</div>';
  }
}

// ===== 出库装货：选定单号后展示资料 =====
async function _refreshOutboundLoadMaterial() {
  // 装货页可能没有专用容器；如果有则填，没有就跳过
  var holder = document.getElementById("obLoadMaterialBox");
  if (!holder) return;
  await renderOutboundMaterials(_obResolvedOrderId, "obLoadMaterialBox");
}

// ===== 库内操作页面 =====
var _osoOrderId = "";

function initOutboundStockOp() {
  _osoOrderId = "";
  showStockOpEntry();
  loadStockOpCandidates();
  if (_activeJobId) {
    showStockOpWorking();
    refreshStockOpWorkers();
  }
  startJobPoll("oso");
}

function showStockOpEntry() {
  var e1 = document.getElementById("osoEntryCard");
  if (e1) e1.style.display = "";
  var e2 = document.getElementById("osoActionCard");
  if (e2) e2.style.display = "none";
  var e3 = document.getElementById("osoResultCard");
  if (e3) e3.style.display = "none";
}

function showStockOpWorking() {
  var e1 = document.getElementById("osoEntryCard");
  if (e1) e1.style.display = "none";
  var e2 = document.getElementById("osoActionCard");
  if (e2) e2.style.display = "";
  var e3 = document.getElementById("osoResultCard");
  if (e3) e3.style.display = "";
}

async function loadStockOpCandidates() {
  var sel = document.getElementById("osoOrderSelect");
  if (!sel) return;
  var res = await api({ action: "v2_outbound_stock_op_list" });
  var opts = '<option value="">-- 选择/선택 --</option>';
  if (res && res.ok && res.items) {
    res.items.forEach(function(o) {
      var biz = o.biz_class ? '[' + o.biz_class + '] ' : '';
      var st = o.status === 'stock_operating' ? '[操作中] ' : '[预约] ';
      opts += '<option value="' + esc(o.id) + '" data-customer="' + esc(o.customer || '') + '" data-instruction="' + esc(o.instruction || '') + '">' + st + biz + esc(o.display_no || o.id) + ' · ' + esc(o.customer || '--') + '</option>';
    });
  }
  sel.innerHTML = opts;
}

async function onStockOpOrderSelect() {
  var sel = document.getElementById("osoOrderSelect");
  var info = document.getElementById("osoOrderInfo");
  var matBox = document.getElementById("osoMaterialBox");
  _osoOrderId = sel ? sel.value : "";
  if (!_osoOrderId) {
    if (info) info.innerHTML = '--';
    if (matBox) matBox.innerHTML = '';
    return;
  }
  var opt = sel.options[sel.selectedIndex];
  var customer = opt ? opt.getAttribute('data-customer') : '';
  var instruction = opt ? opt.getAttribute('data-instruction') : '';
  if (info) {
    info.innerHTML = '<b>客户/고객:</b> ' + esc(customer || '--') +
      (instruction ? '<br><b>说明/설명:</b> ' + esc(instruction) : '');
  }
  await renderOutboundMaterials(_osoOrderId, "osoMaterialBox");
}

async function startOutboundStockOp(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  if (!_osoOrderId) { alert("请先选择出库单 / 출고단을 선택하세요"); return; }
  withActionLock('startOutboundStockOp', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_outbound_stock_op_start",
      outbound_order_id: _osoOrderId,
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      if (!res.already_joined) {
        alert(res.is_new_job ? "已创建库内操作任务 / 작업 생성됨" : "已加入库内操作任务 / 작업 참여됨");
      }
      showStockOpWorking();
      refreshStockOpWorkers();
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

async function refreshStockOpWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("osoWorkers", res.workers);
}

async function finishOutboundStockOp(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  withActionLock('finishOutboundStockOp', btnEl || null, '提交中.../저장중...', async function() {
    var box = parseInt(document.getElementById("osoBoxCount").value) || 0;
    var pallet = parseInt(document.getElementById("osoPalletCount").value) || 0;
    var remark = document.getElementById("osoRemark").value.trim();

    var res = await api({
      action: "v2_outbound_stock_op_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      box_count: box,
      pallet_count: pallet,
      remark: remark,
      complete_job: true
    });
    if (res && res.ok) {
      alert("库内操作已完成 / 작업 완료\n出库计划等待客服更新 / 출고 계획은 고객지원팀이 업데이트 예정");
      var resumed = await checkAndResumeParent();
      if (!resumed) {
        clearActiveJob();
        goPage("home");
      }
    } else if (res && res.error === "already_completed") {
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다");
      clearActiveJob();
      goPage("home");
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

async function saveOutboundStockOpResult(btnEl) {
  await finishOutboundStockOp(btnEl);
}

function outboundStockOpGoBack() {
  if (_activeJobId) {
    alert("库内操作进行中，请先完成 / 진행 중입니다. 먼저 완료하세요");
    return;
  }
  goPage("outbound_menu");
}

// ===== Issue List =====
// 取问题描述前 30 字（兼容旧记录的 issue_summary）
function _issueTitleText(it) {
  var d = (it && (it.issue_description || it.issue_summary)) || "";
  d = String(d).trim().replace(/\s+/g, ' ');
  if (d.length > 30) d = d.substring(0, 30) + "…";
  return d || "(无描述)";
}

async function loadIssueList() {
  var body = document.getElementById("issueListBody");
  if (!body) return;
  body.innerHTML = '<div class="card"><span class="muted">加载中.../로딩중...</span></div>';

  var statusMap = { pending: "pending", processing: "processing", my: "", responded: "responded" };
  var bizSel = document.getElementById("issueBizFilter");
  var bizVal = bizSel ? bizSel.value : "";

  var items = [];
  if (_issueFilter === "pending") {
    // 现场视角下 rework_required 本质也是待再次处理，并入"待处理"列表（状态标签仍区分）
    var pair = await Promise.all([
      api({ action: "v2_issue_ops_list", status: "pending", biz_class: bizVal, sort: "oldest_first" }),
      api({ action: "v2_issue_ops_list", status: "rework_required", biz_class: bizVal, sort: "oldest_first" })
    ]);
    var r1 = pair[0], r2 = pair[1];
    if (!r1 || !r1.ok || !r2 || !r2.ok) {
      body.innerHTML = '<div class="card"><span class="muted">加载失败/로딩 실패</span></div>';
      return;
    }
    // FIFO：最早发布的最先处理；priority 字段已废弃，不参与排序
    items = (r1.items || []).concat(r2.items || []);
    items.sort(function(a, b) { return (a.created_at || 0) - (b.created_at || 0); });
  } else {
    var status = statusMap[_issueFilter] || "";
    var res = await api({ action: "v2_issue_ops_list", status: status, biz_class: bizVal, sort: "oldest_first" });
    if (!res || !res.ok) {
      body.innerHTML = '<div class="card"><span class="muted">加载失败/로딩 실패</span></div>';
      return;
    }
    items = res.items || [];
    if (_issueFilter === "my") {
      items = items.filter(function(it) { return it.status === "processing" || it.status === "responded" || it.status === "rework_required"; });
    }
  }

  if (items.length === 0) {
    body.innerHTML = '<div class="card"><span class="muted">暂无数据 / 데이터 없음</span></div>';
    return;
  }

  var html = "";
  items.forEach(function(it) {
    html += '<div class="list-item" onclick="openIssue(\'' + esc(it.id) + '\')">' +
      '<div class="item-title">' +
        '<span class="st st-' + esc(it.status) + '">' + esc(ISSUE_STATUS_LABEL[it.status] || it.status) + '</span> ' +
        '<span class="biz-tag biz-' + esc(it.biz_class) + '">' + esc(BIZ_LABEL[it.biz_class] || it.biz_class) + '</span>' +
      '</div>' +
      '<div style="font-size:14px;font-weight:600;margin-top:4px;">' + esc(_issueTitleText(it)) + '</div>' +
      '<div class="item-meta">' +
        esc(it.customer || "") + (it.related_doc_no ? " · " + esc(it.related_doc_no) : "") +
        ' · ' + esc(fmtTime(it.created_at)) +
      '</div>' +
    '</div>';
  });
  body.innerHTML = html;
}

function filterIssues(filter, btn) {
  _issueFilter = filter;
  var tabs = document.querySelectorAll("#page-issue_list .tab-bar button");
  for (var i = 0; i < tabs.length; i++) tabs[i].classList.remove("active");
  if (btn) btn.classList.add("active");
  loadIssueList();
}

function openIssue(id) {
  _currentIssueId = id;
  goPage("issue_detail");
}

// ===== Issue Detail =====
async function loadIssueDetail() {
  var body = document.getElementById("issueDetailBody");
  if (!body || !_currentIssueId) return;
  body.innerHTML = '<div class="card"><span class="muted">加载中.../로딩중...</span></div>';

  var res = await api({ action: "v2_issue_detail", id: _currentIssueId });
  if (!res || !res.ok || !res.issue) {
    body.innerHTML = '<div class="card"><span class="muted">加载失败/로딩 실패</span></div>';
    return;
  }

  var it = res.issue;
  var runs = res.handle_runs || [];
  var atts = res.attachments || [];

  var html = '<div class="card">';
  html += '<div style="font-size:18px;font-weight:700;margin-bottom:8px;">' + esc(_issueTitleText(it)) + '</div>';
  html += '<div class="detail-field"><b>状态/상태:</b> <span class="st st-' + esc(it.status) + '">' + esc(ISSUE_STATUS_LABEL[it.status] || it.status) + '</span></div>';
  html += '<div class="detail-field"><b>业务/업무:</b> <span class="biz-tag biz-' + esc(it.biz_class) + '">' + esc(BIZ_LABEL[it.biz_class] || it.biz_class) + '</span></div>';
  html += '<div class="detail-field"><b>客户/고객:</b> ' + esc(it.customer) + '</div>';
  html += '<div class="detail-field"><b>关联单号/관련번호:</b> ' + esc(it.related_doc_no) + '</div>';
  html += '<div class="detail-field"><b>提出人/제출자:</b> ' + esc(it.submitted_by) + '</div>';
  html += '<div class="detail-field"><b>提出时间/제출시간:</b> ' + esc(fmtTime(it.created_at)) + '</div>';
  html += '<div class="detail-section"><b>问题描述/문제 설명:</b><div style="margin-top:4px;white-space:pre-wrap;">' + esc(it.issue_description) + '</div></div>';

  if (it.latest_feedback_text) {
    html += '<div class="detail-section"><b>最新反馈/최신 피드백:</b><div style="margin-top:4px;white-space:pre-wrap;">' + esc(it.latest_feedback_text) + '</div></div>';
  }
  if (it.rework_note) {
    html += '<div class="detail-section" style="border-left:3px solid #e65100;padding-left:8px;"><b>追加处理要求/추가처리 요청:</b><div style="margin-top:4px;white-space:pre-wrap;color:#e65100;">' + esc(it.rework_note) + '</div></div>';
  }
  if (it.total_minutes_worked > 0) {
    html += '<div class="detail-field"><b>累计工时/누적 작업시간:</b> ' + it.total_minutes_worked.toFixed(1) + ' 分钟/분</div>';
  }
  html += '</div>';

  // Attachments
  if (atts.length > 0) {
    html += '<div class="card"><div class="card-title">附件/첨부파일 (' + atts.length + ')</div><div class="photo-upload">';
    atts.forEach(function(att) {
      if (att.content_type && att.content_type.startsWith("image/")) {
        html += '<img class="photo-thumb" src="' + esc(fileUrl(att.file_key)) + '" onclick="showLightbox(\'' + esc(fileUrl(att.file_key)) + '\')">';
      } else {
        html += '<div style="font-size:12px;margin:4px 0;">' + esc(att.file_name) + '</div>';
      }
    });
    html += '</div></div>';
  }

  // Actions
  if (it.status === "pending" || it.status === "processing" || it.status === "rework_required") {
    html += '<div class="card">';
    if (it.status === "pending" || it.status === "rework_required") {
      html += '<button class="btn btn-success" onclick="handleIssueStart(this)">开始处理 / 처리 시작</button>';
    }
    if (it.status === "processing") {
      html += '<div class="detail-section"><label>反馈内容 / 피드백 내용 <span style="color:red;">*必填/필수</span></label>';
      html += '<textarea id="issueFeedback" rows="3" placeholder="输入处理结果 / 처리 결과를 입력하세요 (必填/필수)"></textarea>';
      html += '<label>上传照片 / 사진 업로드</label>';
      html += '<div class="photo-upload" id="issuePhotos"><div class="photo-add" onclick="uploadPhoto(\'issue_ticket\',\'issue_handle_photo\')">+</div></div>';
      html += '<button class="btn btn-danger mt-10" onclick="handleIssueFinish(this)">结束处理 / 처리 종료</button>';
      html += '<button class="btn btn-outline mt-10" onclick="handleIssueLeave(this)">暂时离开 / 일시 퇴장</button>';
      html += '</div>';
    }
    html += '</div>';
  }

  // Handle runs history
  if (runs.length > 0) {
    html += '<div class="card"><div class="card-title">处理记录 / 처리 이력</div>';
    runs.forEach(function(r) {
      html += '<div style="border-bottom:1px solid #f0f0f0;padding:8px 0;font-size:13px;">';
      html += '<div><b>' + esc(r.handler_name || r.handler_id) + '</b> · ' +
        '<span class="st st-' + esc(r.run_status) + '">' + esc(r.run_status) + '</span></div>';
      html += '<div class="muted">' + esc(fmtTime(r.started_at)) + ' → ' + esc(fmtTime(r.ended_at)) +
        (r.minutes_worked ? ' (' + r.minutes_worked.toFixed(1) + ' 分钟/분)' : '') + '</div>';
      if (r.feedback_text) html += '<div style="margin-top:4px;">' + esc(r.feedback_text) + '</div>';
      html += '</div>';
    });
    html += '</div>';
  }

  body.innerHTML = html;
}

async function handleIssueStart(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  withActionLock('handleIssueStart', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_issue_handle_start",
      issue_id: _currentIssueId,
      handler_id: getWorkerId(),
      handler_name: getWorkerName()
    });
    if (res && res.ok) {
      _currentRunId = res.run_id;
      saveActiveJob(res.job_id, res.worker_seg_id);
      alert("已开始处理 / 처리 시작됨");
      loadIssueDetail();
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function handleIssueFinish(btnEl) {
  var feedback = document.getElementById("issueFeedback");
  var feedbackText = feedback ? feedback.value.trim() : "";
  if (!feedbackText) {
    alert("请填写处理结果后再提交 / 처리 결과를 입력한 후 제출하세요");
    return;
  }
  withActionLock('handleIssueFinish', btnEl || null, '提交中.../저장중...', async function() {
    var detailRes = await api({ action: "v2_issue_detail", id: _currentIssueId });
    var runs = (detailRes && detailRes.handle_runs) || [];
    var workingRun = runs.find(function(r) { return r.run_status === "working"; });
    if (!workingRun) {
      alert("找不到进行中的处理记录 / 진행 중인 처리 기록을 찾을 수 없습니다");
      return;
    }

    var res = await api({
      action: "v2_issue_handle_finish",
      run_id: workingRun.id,
      feedback_text: feedbackText
    });

    if (res && res.ok) {
      alert("处理完成 / 처리 완료\n工时/작업시간: " + res.minutes_worked.toFixed(1) + " 分钟/분");
      clearActiveJob();
      loadIssueDetail();
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function handleIssueLeave(btnEl) {
  if (!_activeJobId) return;
  withActionLock('handleIssueLeave', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_ops_job_leave",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_reason: "leave"
    });
    if (res && res.ok) {
      clearActiveJob();
      alert("已暂时离开问题点处理，可稍后重新开始\n이슈 처리에서 일시 퇴장했습니다. 나중에 다시 시작할 수 있습니다");
      loadIssueDetail();
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

// ===== Pick Direct (代发拣货 — 趟次制 v2.20260427a) =====
// 流程：A 创建趟次（不计工时）→ 实际拣货人 B 扫描手中的拣货单号开始计时 → 完成本次拣货
var _pickCreateDocNos = [];      // 创建趟次时录入的单号（暂存）
var _pickStartDocNos = [];       // 实际拣货人开始时扫描的单号（暂存）
var _pickCreateScanner = null;
var _pickStartScanner = null;
var _pickWorkingDocList = [];    // 当前工作段的拣货单号
var _pickWorkingStartedAt = null; // 当前工作段开始时间戳（ms）
var _pickWorkingTripCreatedBy = '';  // 当前趟次创建人 worker_id（用于 finalize 权限判断）
var _pickStartLookupCache = {};      // pick_doc_no → v2_pick_doc_lookup 结果（用于扫码后显示参与人）
var _pickElapsedTimer = null;
var _pickMode = 'start';         // 'start' | 'create'

function initPickDirect() {
  _pickCreateDocNos = [];
  _pickStartDocNos = [];
  _pickStartLookupCache = {};
  renderPickDocList("pickCreateDocList", _pickCreateDocNos, "_pickCreateDocNos", "pickCreateDocList");
  renderPickDocList("pickStartDocList", _pickStartDocNos, "_pickStartDocNos", "pickStartDocList");
  renderPickStartLookupHint();

  var inpC = document.getElementById("pickCreateDocInput");
  if (inpC) inpC.onkeydown = function(e) { if (e.key === "Enter") { e.preventDefault(); addPickCreateDoc(); } };
  var inpS = document.getElementById("pickStartDocInput");
  if (inpS) inpS.onkeydown = function(e) { if (e.key === "Enter") { e.preventDefault(); addPickStartDoc(); } };

  if (_activeJobId) {
    enterPickWorkingSection();
  } else {
    switchPickMode('start');
    loadPickActiveList();
  }
  startJobPoll("pick");
}

function switchPickMode(mode) {
  if (_activeJobId) return; // 工作中不允许切模式
  _pickMode = (mode === 'create') ? 'create' : 'start';
  var startSec = document.getElementById("pickStartSection");
  var createSec = document.getElementById("pickCreateSection");
  var workSec = document.getElementById("pickWorkingSection");
  var btnStart = document.getElementById("pickModeStartBtn");
  var btnCreate = document.getElementById("pickModeCreateBtn");
  var hint = document.getElementById("pickModeHint");
  if (workSec) workSec.style.display = "none";
  if (_pickMode === 'create') {
    if (startSec) startSec.style.display = "none";
    if (createSec) createSec.style.display = "";
    if (btnStart) { btnStart.classList.remove("btn-primary"); btnStart.classList.add("btn-outline"); }
    if (btnCreate) { btnCreate.classList.remove("btn-outline"); btnCreate.classList.add("btn-primary"); }
    if (hint) hint.textContent = "录入拣货单号生成趟次（仅录入，不计工时）/ 피킹번호 입력하여 차수 생성 (작업시간 미기록)";
  } else {
    if (startSec) startSec.style.display = "";
    if (createSec) createSec.style.display = "none";
    if (btnStart) { btnStart.classList.remove("btn-outline"); btnStart.classList.add("btn-primary"); }
    if (btnCreate) { btnCreate.classList.remove("btn-primary"); btnCreate.classList.add("btn-outline"); }
    if (hint) hint.textContent = "扫描您手中的拣货单号开始拣货 / 가지고 계신 피킹번호를 스캔하여 시작하세요";
  }
  // 切模式时停所有扫码
  stopPickCreateScan();
  stopPickStartScan();
}

// 拣货单标签渲染：可选 removable 数组绑定（变量名）
function renderPickDocList(containerId, docs, varName, refreshContainerId) {
  var el = document.getElementById(containerId);
  if (!el) return;
  if (!docs || docs.length === 0) {
    el.innerHTML = '<span class="muted">未添加拣货单号 / 피킹번호 미추가</span>';
    return;
  }
  // start 列表删单号时同步刷新参与人提示
  var extraHook = (varName === '_pickStartDocNos') ? ';renderPickStartLookupHint()' : '';
  var html = '<div class="tag-wrap">';
  docs.forEach(function(no, i) {
    html += '<span class="doc-tag">' + esc(no);
    html += ' <span class="remove" onclick="' + varName + '.splice(' + i + ',1);renderPickDocList(\'' + (refreshContainerId || containerId) + '\',' + varName + ',\'' + varName + '\',\'' + (refreshContainerId || containerId) + '\')' + extraHook + ';">&times;</span>';
    html += '</span>';
  });
  html += '</div>';
  el.innerHTML = html;
}

// ---- 创建趟次 mode ----
function addPickCreateDoc() {
  var inp = document.getElementById("pickCreateDocInput");
  var val = (inp ? inp.value : "").trim();
  if (!val) return;
  if (_pickCreateDocNos.indexOf(val) === -1) {
    _pickCreateDocNos.push(val);
    renderPickDocList("pickCreateDocList", _pickCreateDocNos, "_pickCreateDocNos", "pickCreateDocList");
  }
  if (inp) { inp.value = ""; inp.focus(); }
}

function togglePickCreateScan() {
  if (_pickCreateScanner) { stopPickCreateScan(); return; }
  var readerEl = document.getElementById("pickCreateScanReader");
  if (!readerEl) return;
  readerEl.innerHTML = "";
  try {
    _pickCreateScanner = new Html5Qrcode("pickCreateScanReader");
    _pickCreateScanner.start(
      { facingMode: "environment" },
      { fps: 10, qrbox: { width: 250, height: 150 } },
      function(decoded) {
        var code = String(decoded || "").trim();
        if (!code) return;
        if (_pickCreateDocNos.indexOf(code) === -1) {
          _pickCreateDocNos.push(code);
          renderPickDocList("pickCreateDocList", _pickCreateDocNos, "_pickCreateDocNos", "pickCreateDocList");
        }
      },
      function() {}
    ).catch(function(e) {
      alert("摄像头启动失败 / 카메라 시작 실패: " + e);
      _pickCreateScanner = null;
    });
    var btn = document.getElementById("pickCreateScanBtn");
    if (btn) btn.textContent = "停止扫码 / 스캔 중지";
  } catch(e) {
    alert("扫码不可用 / 스캔 불가: " + e.message);
  }
}

function stopPickCreateScan() {
  if (_pickCreateScanner) {
    try { _pickCreateScanner.stop(); } catch(e) {}
    _pickCreateScanner = null;
    var el = document.getElementById("pickCreateScanReader");
    if (el) el.innerHTML = "";
    var btn = document.getElementById("pickCreateScanBtn");
    if (btn) btn.textContent = "扫码 / 스캔";
  }
}

async function submitCreatePickTrip(btnEl) {
  if (_pickCreateDocNos.length === 0) {
    alert("请先添加至少一个拣货单号\n최소 하나의 피킹번호를 추가하세요");
    return;
  }
  withActionLock('submitCreatePickTrip', btnEl || null, '创建中.../생성중...', async function() {
    var res = await api({
      action: "v2_pick_job_start",
      pick_doc_nos: _pickCreateDocNos,
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      var trip = res.trip_no || res.display_no || res.job_id;
      alert("趟次已创建：" + trip + "\n趟次已创建，不记录你的拣货工时。请把拣货单交给实际拣货人扫码开始。\n\n차수 생성됨: " + trip + "\n차수가 생성되었습니다. 작업시간은 기록되지 않으며, 피킹번호를 실제 작업자에게 전달해 스캔하여 시작하세요.");
      _pickCreateDocNos = [];
      renderPickDocList("pickCreateDocList", _pickCreateDocNos, "_pickCreateDocNos", "pickCreateDocList");
      stopPickCreateScan();
      switchPickMode('start');
      loadPickActiveList();
    } else if (res && res.error === "doc_conflict") {
      alert(res.message || "拣货单号冲突 / 피킹번호 충돌");
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

// ---- 实际拣货人扫码开始 mode ----
function addPickStartDoc() {
  var inp = document.getElementById("pickStartDocInput");
  var val = (inp ? inp.value : "").trim();
  if (!val) return;
  if (_pickStartDocNos.indexOf(val) === -1) {
    _pickStartDocNos.push(val);
    renderPickDocList("pickStartDocList", _pickStartDocNos, "_pickStartDocNos", "pickStartDocList");
    pickStartLookupOne(val);
  }
  if (inp) { inp.value = ""; inp.focus(); }
}

// 扫码/录入后查 v2_pick_doc_lookup，把"该单当前已有参与人员"显示给现场拣货人
// 三态：loading（查询中）/ ok（参与人列表）/ error（网络失败，不阻塞开始）
async function pickStartLookupOne(docNo) {
  if (!docNo) return;
  _pickStartLookupCache[docNo] = { _state: 'loading', pick_doc_no: docNo };
  renderPickStartLookupHint();
  try {
    var res = await api({ action: "v2_pick_doc_lookup", pick_doc_no: docNo });
    if (res && res.ok) {
      res._state = 'ok';
      _pickStartLookupCache[docNo] = res;
    } else {
      _pickStartLookupCache[docNo] = { _state: 'ok', found: false, pick_doc_no: docNo };
    }
  } catch (e) {
    _pickStartLookupCache[docNo] = { _state: 'error', pick_doc_no: docNo };
  }
  renderPickStartLookupHint();
}

function renderPickStartLookupHint() {
  var el = document.getElementById("pickStartLookupHint");
  if (!el) return;
  var lines = [];
  for (var i = 0; i < _pickStartDocNos.length; i++) {
    var no = _pickStartDocNos[i];
    var info = _pickStartLookupCache[no];
    if (!info) continue;
    if (info._state === 'loading') {
      lines.push('<div style="color:#999;">⏳ ' + esc(no) + ' — 查询中... / 조회 중...</div>');
      continue;
    }
    if (info._state === 'error') {
      lines.push('<div style="color:#fa8c16;">' + esc(no) + ' — 参与人查询失败，但不影响开始；开始时以后端校验为准 / 참여자 조회 실패, 시작에는 영향 없음</div>');
      continue;
    }
    if (info.found === false) {
      lines.push('<div style="color:#ff4d4f;">⚠ ' + esc(no) + ' — 系统未找到该拣货单 / 시스템에서 찾을 수 없음</div>');
      continue;
    }
    var parts = info.participants || [];
    var actives = parts.filter(function(p) { return p.status === 'working'; }).map(function(p) { return p.worker_name || p.worker_id; });
    var done = parts.filter(function(p) { return p.status === 'completed'; }).map(function(p) { return p.worker_name || p.worker_id; });
    var pieces = [];
    if (actives.length > 0) pieces.push('<span style="color:#1677ff;">当前已有参与人员/현재 작업중: ' + esc(actives.join("、")) + '</span>');
    if (done.length > 0) pieces.push('<span style="color:#888;">曾参与/이전 작업: ' + esc(done.join("、")) + '</span>');
    if (parts.length === 0) pieces.push('<span style="color:#52c41a;">尚无人参与，将由你首次开始 / 첫 작업자</span>');
    var stTip = (info.pick_status === 'completed') ? '（已完成）' : '';
    lines.push('<div>' + esc(no) + stTip + ' · ' + pieces.join(' · ') + (actives.length > 0 ? '　<span style="color:#999;">你将加入该拣货单的共同拣货 / 함께 작업합니다</span>' : '') + '</div>');
  }
  el.innerHTML = lines.join('');
}

function togglePickStartScan() {
  if (_pickStartScanner) { stopPickStartScan(); return; }
  var readerEl = document.getElementById("pickStartScanReader");
  if (!readerEl) return;
  readerEl.innerHTML = "";
  try {
    _pickStartScanner = new Html5Qrcode("pickStartScanReader");
    _pickStartScanner.start(
      { facingMode: "environment" },
      { fps: 10, qrbox: { width: 250, height: 150 } },
      function(decoded) {
        var code = String(decoded || "").trim();
        if (!code) return;
        if (_pickStartDocNos.indexOf(code) === -1) {
          _pickStartDocNos.push(code);
          renderPickDocList("pickStartDocList", _pickStartDocNos, "_pickStartDocNos", "pickStartDocList");
          pickStartLookupOne(code);
        }
      },
      function() {}
    ).catch(function(e) {
      alert("摄像头启动失败 / 카메라 시작 실패: " + e);
      _pickStartScanner = null;
    });
    var btn = document.getElementById("pickStartScanBtn");
    if (btn) btn.textContent = "停止扫码 / 스캔 중지";
  } catch(e) {
    alert("扫码不可用 / 스캔 불가: " + e.message);
  }
}

function stopPickStartScan() {
  if (_pickStartScanner) {
    try { _pickStartScanner.stop(); } catch(e) {}
    _pickStartScanner = null;
    var el = document.getElementById("pickStartScanReader");
    if (el) el.innerHTML = "";
    var btn = document.getElementById("pickStartScanBtn");
    if (btn) btn.textContent = "扫码 / 스캔";
  }
}

async function submitStartPickByDocs(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  if (_pickStartDocNos.length === 0) {
    alert("请先扫描或添加至少一个拣货单号\n최소 하나의 피킹번호를 스캔/추가하세요");
    return;
  }
  withActionLock('submitStartPickByDocs', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_pick_job_start_by_docs",
      pick_doc_nos: _pickStartDocNos,
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      _pickWorkingDocList = (res.picked_doc_nos || []).slice();
      _pickWorkingStartedAt = res.started_at ? new Date(res.started_at).getTime() : Date.now();
      _pickStartDocNos = [];
      _pickStartLookupCache = {};
      renderPickDocList("pickStartDocList", _pickStartDocNos, "_pickStartDocNos", "pickStartDocList");
      renderPickStartLookupHint();
      stopPickStartScan();
      enterPickWorkingSection(res);
    } else if (res && res.error === "cross_trip_not_allowed") {
      alert(res.message || "不能跨趟次同时拣货");
    } else if (res && res.error === "doc_not_found") {
      alert(res.message || "拣货单号未找到");
    } else if (res && res.error === "trip_already_completed") {
      alert(res.message || "趟次已完成 / 차수가 이미 완료됨");
    } else if (res && res.error === "trip_cancelled") {
      alert(res.message || "趟次已取消 / 차수가 취소됨");
    } else if (res && res.error === "trip_interrupted") {
      alert(res.message || "趟次正在临时挂起 / 차수가 임시 중단됨");
    } else if (res && res.error === "worker_has_active_job") {
      alert(res.message || "您当前已有活跃任务，请先完成或暂离");
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

function enterPickWorkingSection(startRes) {
  var startSec = document.getElementById("pickStartSection");
  var createSec = document.getElementById("pickCreateSection");
  var workSec = document.getElementById("pickWorkingSection");
  var modeBar = document.getElementById("pickModeBar");
  if (startSec) startSec.style.display = "none";
  if (createSec) createSec.style.display = "none";
  if (workSec) workSec.style.display = "";
  if (modeBar) modeBar.style.display = "none";

  // 当前拣货人
  var pickerEl = document.getElementById("pickWorkingPicker");
  if (pickerEl) pickerEl.textContent = (getWorkerName() || getWorkerId() || "--");

  // 标题 + 开始时间 + finalize 按钮显隐
  api({ action: "v2_ops_job_detail", job_id: _activeJobId }).then(function(res) {
    if (res && res.ok && res.job) {
      var t = document.getElementById("pickWorkingTitle");
      if (t) t.textContent = "趟次/차수: " + (res.job.display_no || _activeJobId);
      // 找当前 worker 自己的 open segment 作为开始时间
      var mySeg = (res.workers || []).find(function(w) {
        return w.worker_id === getWorkerId() && !w.left_at;
      });
      if (mySeg && mySeg.joined_at) {
        _pickWorkingStartedAt = new Date(mySeg.joined_at).getTime();
      } else if (!_pickWorkingStartedAt) {
        _pickWorkingStartedAt = Date.now();
      }
      var sa = document.getElementById("pickWorkingStartedAt");
      if (sa && _pickWorkingStartedAt) {
        sa.textContent = new Date(_pickWorkingStartedAt).toLocaleString();
      }
      _pickWorkingTripCreatedBy = res.job.created_by || '';
      // 计时页恒不显示整趟完成（按设计：自己还在岗就不可能整趟完成）
      // 整趟完成仅出现在「进行中趟次」列表卡片，且需 active_worker_count=0 + 当前用户=创建人
      var wrap = document.getElementById("pickFinalizeWrap");
      if (wrap) wrap.style.display = "none";
    }
  });

  // 我本次的拣货单 + 趟次全部拣货单
  refreshPickWorkingDocs();
  refreshPickWorkers();
  startPickElapsedTimer();
}

function refreshPickWorkingDocs() {
  if (!_activeJobId) return;
  api({ action: "v2_pick_job_docs_list", job_id: _activeJobId }).then(function(res) {
    if (!res || !res.ok) return;
    var docs = res.docs || [];
    // 全部拣货单（按总状态着色 + 参与人员标注）
    var tripEl = document.getElementById("pickWorkingTripDocList");
    if (tripEl) {
      if (docs.length === 0) {
        tripEl.innerHTML = '<span class="muted">无 / 없음</span>';
      } else {
        var html = '<div class="tag-wrap">';
        docs.forEach(function(d) {
          var st = d.pick_status || 'pending';
          var cls = 'doc-tag is-' + st;
          var parts = d.participants || [];
          var activeNames = parts.filter(function(p) { return p.status === 'working'; }).map(function(p) { return p.worker_name; }).filter(Boolean);
          var allNames = Array.from(new Set(parts.map(function(p) { return p.worker_name; }).filter(Boolean)));
          var hint = '';
          if (st === 'working' && activeNames.length > 0) {
            hint = '<span class="picker-hint">@' + esc(activeNames.join("、")) + '</span>';
          } else if (st === 'completed' && allNames.length > 0) {
            hint = '<span class="picker-hint">✓' + esc(allNames.join("、")) + '</span>';
          }
          html += '<span class="' + cls + '">' + esc(d.pick_doc_no) + hint + '</span>';
        });
        html += '</div>';
        tripEl.innerHTML = html;
      }
    }
    // 我本次的拣货单（用 _activeSegId 匹配 participants.segment_id）
    var myDocs = docs.filter(function(d) {
      return (d.participants || []).some(function(p) {
        return p.segment_id && p.segment_id === _activeSegId && p.status === 'working';
      });
    });
    if (myDocs.length === 0) {
      // 兜底：worker_id + working
      myDocs = docs.filter(function(d) {
        return (d.participants || []).some(function(p) {
          return p.worker_id === getWorkerId() && p.status === 'working';
        });
      });
    }
    _pickWorkingDocList = myDocs.map(function(d) { return d.pick_doc_no; });
    var myEl = document.getElementById("pickWorkingDocList");
    if (myEl) {
      if (myDocs.length === 0) {
        myEl.innerHTML = '<span class="muted">无 / 없음</span>';
      } else {
        myEl.innerHTML = '<div class="tag-wrap">' + myDocs.map(function(d) {
          return '<span class="doc-tag is-working">' + esc(d.pick_doc_no) + '</span>';
        }).join('') + '</div>';
      }
    }
    var cntEl = document.getElementById("pickWorkingDocCount");
    if (cntEl) cntEl.textContent = String(myDocs.length);
  });
}

function startPickElapsedTimer() {
  if (_pickElapsedTimer) clearInterval(_pickElapsedTimer);
  _pickElapsedTimer = setInterval(function() {
    if (!_pickWorkingStartedAt) return;
    var sec = Math.floor((Date.now() - _pickWorkingStartedAt) / 1000);
    var h = Math.floor(sec / 3600);
    var m = Math.floor((sec % 3600) / 60);
    var s = sec % 60;
    var el = document.getElementById("pickWorkingElapsed");
    if (el) el.textContent = (h > 0 ? h + "h " : "") + m + "m " + s + "s";
  }, 1000);
}

function stopPickElapsedTimer() {
  if (_pickElapsedTimer) { clearInterval(_pickElapsedTimer); _pickElapsedTimer = null; }
}

async function loadPickActiveList() {
  var el = document.getElementById("pickActiveList");
  if (!el) return;
  el.innerHTML = '<span class="muted">加载中.../로딩중...</span>';
  var res = await api({ action: "v2_pick_job_active_list" });
  if (!res || !res.ok) {
    el.innerHTML = '<span class="muted">加载失败</span>';
    return;
  }
  var items = res.items || [];
  if (items.length === 0) {
    el.innerHTML = '<span class="muted">暂无进行中趟次 / 진행중 차수 없음</span>';
    return;
  }
  var html = '';
  items.forEach(function(t) {
    var docList = t.pick_docs || [];
    var docsHtml = '<span class="muted">--</span>';
    if (docList.length > 0) {
      docsHtml = '<div class="tag-wrap">' + docList.map(function(d) {
        var st = d.pick_status || 'pending';
        var cls = 'doc-tag is-' + st;
        var activeNames = d.active_picker_names || [];
        var allNames = d.all_picker_names || [];
        var hint = '';
        if (st === 'working' && activeNames.length > 0) {
          hint = '<span class="picker-hint">@' + esc(activeNames.join("、")) + '</span>';
        } else if (st === 'completed' && allNames.length > 0) {
          hint = '<span class="picker-hint">✓' + esc(allNames.join("、")) + '</span>';
        } else if (allNames.length > 0) {
          hint = '<span class="picker-hint">' + allNames.length + '人/명</span>';
        }
        return '<span class="' + cls + '">' + esc(d.pick_doc_no) + hint + '</span>';
      }).join('') + '</div>';
    }
    var wNames = (t.workers || []).map(function(w) { return w.name || w.id; });
    var wHtml = wNames.length > 0 ? wNames.map(function(n) {
      return '<span class="worker-tag">' + esc(n) + '</span>';
    }).join('') : '<span class="muted">无 / 없음</span>';

    var stCls = t.status || 'pending';
    var stLabel = (t.status === 'working') ? '拣货中 / 피킹중'
                  : (t.status === 'awaiting_close') ? '待收尾 / 마감 대기'
                  : '待开始 / 시작 대기';
    var summary = '待:' + (t.pick_doc_pending_count || 0) + ' / 中:' + (t.pick_doc_working_count || 0) + ' / 完:' + (t.pick_doc_completed_count || 0);

    // 仅当 (无人在岗) 且 (当前 worker 是趟次创建人) 时显示 finalize 入口
    // 后端会再做一次权限校验（创建人/ADMINKEY），前端不渲染避免误点
    var noWorkerActive = (t.workers || []).length === 0;
    var isCreator = (t.created_by && t.created_by === getWorkerId());
    var canFinalize = (t.status !== 'completed') && noWorkerActive && isCreator;

    html += '<div class="trip-card">';
    html += '<div class="trip-card-header"><span class="trip-tag">' + esc(t.display_no || t.id) + '</span>';
    html += '<span class="st st-' + esc(stCls) + '">' + esc(stLabel) + '</span></div>';
    html += '<div class="trip-card-meta">拣货单 / 피킹번호 (' + esc(summary) + '):</div>' + docsHtml;
    html += '<div class="trip-card-meta" style="margin-top:6px;">在岗拣货人 / 작업중:</div><div style="margin-top:2px;">' + wHtml + '</div>';
    html += '<div class="trip-card-meta" style="margin-top:6px;color:#999;font-size:11px;">趟次创建人 / 생성자: ' + esc(t.created_by || "--") + '</div>';
    if (canFinalize) {
      html += '<div style="margin-top:8px;"><button class="btn btn-warning btn-sm" onclick="finalizePickJob(this,\'' + esc(t.id) + '\',\'' + esc(t.created_by || '') + '\')">整趟完成（仅趟次创建人）/ 차수 전체 완료 (생성자만)</button></div>';
    } else if (noWorkerActive && t.status !== 'completed') {
      html += '<div class="muted" style="margin-top:8px;font-size:11px;">仅趟次创建人可整趟完成 / 차수 생성자만 전체 완료 가능</div>';
    } else if (!noWorkerActive && t.status !== 'completed') {
      html += '<div class="muted" style="margin-top:8px;font-size:11px;">仍有人员在岗，需所有人完成本次拣货后才能整趟完成 / 작업 중 인원 있음, 모두 완료 후 마감 가능</div>';
    }
    html += '</div>';
  });
  el.innerHTML = html;
}

async function finalizePickJob(btnEl, jobId, createdBy) {
  if (!jobId) return;
  // 前端权限拦截：非创建人直接劝退（后端会再校验一次）
  if (createdBy && createdBy !== getWorkerId()) {
    alert("只有趟次创建人或主管/管理员可以整趟完成\n차수 생성자 또는 관리자만 전체 완료 가능\n\n创建人 / 생성자: " + createdBy);
    return;
  }
  if (!confirm("确认整趟完成？\n这会结束该趟次所有仍在进行中的拣货计时。\n\n전체 완료를 확인하시겠습니까?\n해당 차수의 진행 중인 모든 피킹 작업시간이 종료됩니다.")) return;
  withActionLock('finalizePickJob_' + jobId, btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_pick_job_finalize",
      job_id: jobId,
      worker_id: getWorkerId(),
      remark: (document.getElementById("pickRemark") || {}).value || "",
      result_note: (document.getElementById("pickResultNote") || {}).value || ""
    });
    if (res && res.ok) {
      var msg = "整趟次已完成 / 차수 전체 완료\n";
      msg += "拣货单/피킹번호: " + (res.pick_doc_count || 0) + " 张/장\n";
      msg += "参与人数/참여 인원: " + (res.worker_count || 0) + " 人/명\n";
      msg += "总用时/총 소요: " + (res.total_minutes || 0) + " 分钟/분";
      alert(msg);
      stopPickCreateScan();
      stopPickStartScan();
      stopPickElapsedTimer();
      if (_activeJobId === jobId) {
        clearActiveJob();
        goPage("order_op_menu");
      } else {
        loadPickActiveList();
      }
    } else if (res && res.error === "forbidden_not_creator") {
      alert((res.message || "只有趟次创建人或主管/管理员可以整趟完成") +
            (res.required_creator ? "\n\n创建人 / 생성자: " + res.required_creator : ""));
    } else if (res && res.error === "active_workers_still_working") {
      var msg = res.message || "仍有人员正在拣货，请先让所有人完成本次拣货后再整趟完成";
      if (res.active_worker_names) msg += "\n\n在岗 / 작업중: " + res.active_worker_names;
      alert(msg);
      loadPickActiveList();
    } else if (res && res.error === "missing_worker_id") {
      alert(res.message || "请提供拣货人ID / worker_id 필요");
    } else if (res && res.error === "already_completed") {
      alert("趟次已完成 / 차수 완료");
      loadPickActiveList();
    } else if (res && res.error === "already_cancelled") {
      alert("趟次已取消 / 차수 취소됨");
      loadPickActiveList();
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

async function finishPickJob(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  if (!confirm("确认完成本次拣货？\n이번 피킹을 완료하시겠습니까?")) return;
  withActionLock('finishPickJob', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_pick_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      remark: (document.getElementById("pickRemark") || {}).value || "",
      result_note: (document.getElementById("pickResultNote") || {}).value || ""
    });
    if (res && res.ok) {
      var msg = "本次拣货已完成 / 이번 피킹 완료\n用时/소요: " + (res.minutes_worked != null ? res.minutes_worked + " 分钟/분" : "--");
      if (res.finished_pick_doc_nos && res.finished_pick_doc_nos.length > 0) {
        msg += "\n本次参与的拣货单/이번 피킹번호: " + res.finished_pick_doc_nos.join(", ");
      }
      if ((res.active_worker_count || 0) > 0) {
        msg += "\n该趟次仍有 " + res.active_worker_count + " 人在拣货 / 차수 내 " + res.active_worker_count + "명 작업중";
      } else {
        msg += "\n该趟次已无人在岗，请由创建人或主管点【整趟完成】结束趟次\n작업자가 모두 종료. 생성자/관리자가 [차수 전체 완료]를 눌러 마감하세요";
      }
      alert(msg);
      stopPickCreateScan();
      stopPickStartScan();
      stopPickElapsedTimer();
      clearActiveJob();
      goPage("order_op_menu");
    } else if (res && res.error === "already_completed") {
      alert("趟次已完成 / 차수 완료");
      clearActiveJob();
      goPage("order_op_menu");
    } else if (res && res.error === "no_open_segment") {
      alert("您未在该趟次中拣货 / 이 차수에서 작업 중이 아닙니다");
      clearActiveJob();
      goPage("order_op_menu");
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

async function refreshPickWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) {
    renderWorkers("pickWorkers", res.workers);
    refreshPickWorkingDocs();
  }
}

// ===== Bulk Op (大货操作) =====
var _bulkScanner = null;

var _bulkElapsedTimer = null;
var _bulkStartedAt = null;

function _bulkSwitchState(state) {
  // state: 'idle' | 'working'
  var idle = document.getElementById("bulkStateIdle");
  var working = document.getElementById("bulkStateWorking");
  if (!idle || !working) return;
  idle.style.display = state === "idle" ? "" : "none";
  working.style.display = state === "working" ? "" : "none";
  // Clear error bar on state switch
  var errBar = document.getElementById("bulkErrorBar");
  if (errBar) { errBar.style.display = "none"; errBar.textContent = ""; }
  var subBar = document.getElementById("bulkSubmittingBar");
  if (subBar) subBar.style.display = "none";
}

function _bulkSetSubmitting(on) {
  var subBar = document.getElementById("bulkSubmittingBar");
  if (subBar) subBar.style.display = on ? "" : "none";
  // Disable all inputs + buttons during submit
  var card = document.getElementById("bulkResultCard");
  if (card) {
    var inputs = card.querySelectorAll("input, textarea");
    for (var i = 0; i < inputs.length; i++) inputs[i].disabled = on;
  }
  var leaveBtn = document.getElementById("bulkLeaveBtn");
  if (leaveBtn) leaveBtn.disabled = on;
  var finBtn = document.getElementById("bulkFinishBtn");
  if (finBtn) { finBtn.disabled = on; finBtn.textContent = on ? "提交中.../저장중..." : "完成操作 / 작업 완료"; }
}

function _bulkShowError(msg) {
  var errBar = document.getElementById("bulkErrorBar");
  if (errBar) { errBar.textContent = msg; errBar.style.display = ""; }
}

function _bulkStartElapsedTimer() {
  if (_bulkElapsedTimer) clearInterval(_bulkElapsedTimer);
  _bulkElapsedTimer = setInterval(function() {
    if (!_bulkStartedAt) return;
    var sec = Math.floor((Date.now() - _bulkStartedAt) / 1000);
    var h = Math.floor(sec / 3600);
    var m = Math.floor((sec % 3600) / 60);
    var s = sec % 60;
    var el = document.getElementById("bulkElapsed");
    if (el) el.textContent = (h > 0 ? h + "h " : "") + m + "m " + s + "s";
  }, 1000);
}

function _bulkStopElapsedTimer() {
  if (_bulkElapsedTimer) { clearInterval(_bulkElapsedTimer); _bulkElapsedTimer = null; }
}

function _bulkEnterWorkingState(workOrderNo, jobDetail) {
  _bulkSwitchState("working");
  var noEl = document.getElementById("bulkActiveOrderNo");
  if (noEl) noEl.textContent = workOrderNo || "--";
  // Start time
  _bulkStartedAt = Date.now();
  if (jobDetail && jobDetail.created_at) {
    try { _bulkStartedAt = new Date(jobDetail.created_at).getTime(); } catch(e) {}
  }
  var stEl = document.getElementById("bulkStartTime");
  if (stEl) {
    try {
      var d = new Date(_bulkStartedAt);
      var kst = new Date(d.getTime() + 9 * 3600 * 1000);
      stEl.textContent = kst.toISOString().slice(11, 16);
    } catch(e) { stEl.textContent = "--"; }
  }
  _bulkStartElapsedTimer();
  refreshBulkWorkers();
}

function initBulkOp() {
  _bulkStopElapsedTimer();
  if (_activeJobId) {
    // Resume into working state
    _bulkSwitchState("working");
    refreshBulkWorkers();
    api({ action: "v2_ops_job_detail", job_id: _activeJobId }).then(function(res) {
      if (res && res.ok && res.job) {
        _bulkEnterWorkingState(res.job.related_doc_id || "", res.job);
        // Worker count
        var wcEl = document.getElementById("bulkWorkerCount");
        if (wcEl) wcEl.textContent = (res.job.active_worker_count || 0) + " 人/명";
      }
    });
  } else {
    _bulkSwitchState("idle");
  }
  startJobPoll("bulk");
}

function startBulkScan() {
  if (_bulkScanner) { stopBulkScan(); return; }
  var readerEl = document.getElementById("bulkScanReader");
  if (!readerEl) return;
  readerEl.innerHTML = "";
  try {
    _bulkScanner = new Html5Qrcode("bulkScanReader");
    _bulkScanner.start(
      { facingMode: "environment" },
      { fps: 10, qrbox: { width: 250, height: 150 } },
      function(decoded) {
        stopBulkScan();
        var code = String(decoded || "").trim();
        if (!code) return;
        var inp = document.getElementById("bulkOrderInput");
        if (inp) inp.value = code;
      },
      function() {}
    ).catch(function(e) {
      alert("摄像头启动失败 / 카메라 시작 실패: " + e);
      _bulkScanner = null;
    });
    document.getElementById("bulkScanBtn").textContent = "停止扫码 / 스캔 중지";
  } catch(e) {
    alert("扫码不可用 / 스캔 불가: " + e.message);
  }
}

function stopBulkScan() {
  if (_bulkScanner) {
    try { _bulkScanner.stop(); } catch(e) {}
    _bulkScanner = null;
    var el = document.getElementById("bulkScanReader");
    if (el) el.innerHTML = "";
    document.getElementById("bulkScanBtn").textContent = "扫码 / 스캔";
  }
}

async function startBulkJob(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  var workOrderNo = (document.getElementById("bulkOrderInput") || {}).value.trim();
  if (!workOrderNo) {
    alert("请输入或扫描工单号\n작업지시 번호를 입력하거나 스캔하세요");
    return;
  }
  withActionLock('startBulkJob', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_bulk_op_job_start",
      work_order_no: workOrderNo,
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      // Switch to working state
      _bulkEnterWorkingState(workOrderNo, null);
      // Show linked outbound if available
      var obCard = document.getElementById("bulkLinkedObCard");
      var obBody = document.getElementById("bulkLinkedObBody");
      if (obCard && obBody && res.linked_outbound) {
        var ob = res.linked_outbound;
        var h = '';
        if (ob.display_no) h += '<div><b>出库单号/출고단번호:</b> ' + esc(ob.display_no) + '</div>';
        if (ob.status === "reopen_pending") h += '<div><span class="st st-reopen_pending">' + L("status_reopen_pending") + '</span></div>';
        if (ob.customer) h += '<div><b>客户/고객:</b> ' + esc(ob.customer) + '</div>';
        if (ob.destination) h += '<div><b>目的地/목적지:</b> ' + esc(ob.destination) + '</div>';
        if (ob.po_no) h += '<div><b>PO号/발주번호:</b> ' + esc(ob.po_no) + '</div>';
        if (ob.planned_box_count) h += '<div><b>计划箱数/계획 박스:</b> ' + ob.planned_box_count + '</div>';
        if (ob.planned_pallet_count) h += '<div><b>计划托数/계획 팔레트:</b> ' + ob.planned_pallet_count + '</div>';
        if (ob.instruction) h += '<div><b>作业说明/작업 설명:</b> ' + esc(ob.instruction) + '</div>';
        h += '<div id="bulkLinkedObMaterial" style="margin-top:6px;"></div>';
        obBody.innerHTML = h || '<span class="muted">无关联出库单</span>';
        obCard.style.display = "";
        if (ob.id) renderOutboundMaterials(ob.id, "bulkLinkedObMaterial");
      } else if (obCard) {
        obCard.style.display = "none";
      }
    } else if (res && res.error === "worker_already_in_other_bulk_job") {
      var otherNo = res.other_work_order_no || "";
      alert("当前已在其他大货工单作业中，请先退出或完成当前工单"
        + (otherNo ? "\n当前工单号: " + otherNo : "")
        + "\n현재 다른 대량화물 작업에 참여 중입니다. 먼저 퇴장하거나 완료한 후 다시 시도하세요");
    } else if (res && res.error === "bulk_order_already_completed") {
      alert("该工单已完成，如需返工或追加操作，请联系协同中心设为待再操作"
        + "\n이미 완료된 작업입니다. 재작업/추가 작업은 협업센터에서 재작업 대기로 변경 후 진행하세요");
    } else if (res && res.error === "bulk_work_order_already_completed") {
      alert("该纯工单号已完成，不能再次操作。如需返工，请创建系统出库单或使用新工单号"
        + "\n해당 작업번호는 이미 완료되어 재사용할 수 없습니다. 재작업이 필요하면 시스템 출고단을 생성하거나 새 작업번호를 사용하세요");
    } else if (res && res.error === "bulk_order_cancelled") {
      alert("该工单已取消，不能继续操作\n취소된 작업입니다. 계속 진행할 수 없습니다");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

// Bulk-op output fields (used by both pre-check and submission)
var _bulkOutputFieldIds = [
  "bulkPackedSku", "bulkPackedBox", "bulkCartonLarge", "bulkCartonSmall",
  "bulkRepairedBox", "bulkReboxed", "bulkLabelCount", "bulkTotalBox",
  "bulkPalletCount", "bulkForkliftLoc"
];

function _hasBulkOutput() {
  for (var i = 0; i < _bulkOutputFieldIds.length; i++) {
    var el = document.getElementById(_bulkOutputFieldIds[i]);
    if (el && (parseInt(el.value) || 0) > 0) return true;
  }
  var fk = document.getElementById("bulkUsedForklift");
  if (fk && fk.checked) return true;
  return false;
}

async function bulkLeave(btnEl) {
  if (!_activeJobId) return;
  if (!confirm("确认暂时离开？/ 일시 퇴장하시겠습니까?")) return;
  withActionLock('bulkLeave', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_bulk_op_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_only: true
    });
    if (res && (res.ok || res.error === "others_still_working")) {
      _bulkStopElapsedTimer();
      stopBulkScan();
      clearActiveJob();
      goPage("order_op_menu");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

async function finishBulkJob(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  if (!confirm("确认完成本次大货操作？\n이번 대량화물 작업을 완료하시겠습니까?")) return;
  withActionLock('finishBulkJob', btnEl || null, '提交中.../저장중...', async function() {
    // Enter submitting state
    _bulkSetSubmitting(true);
    var res = await api({
      action: "v2_bulk_op_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      packed_sku_count: parseInt(document.getElementById("bulkPackedSku").value) || 0,
      packed_box_count: parseInt(document.getElementById("bulkPackedBox").value) || 0,
      used_carton_large_count: parseInt(document.getElementById("bulkCartonLarge").value) || 0,
      used_carton_small_count: parseInt(document.getElementById("bulkCartonSmall").value) || 0,
      repaired_box_count: parseInt(document.getElementById("bulkRepairedBox").value) || 0,
      reboxed_count: parseInt(document.getElementById("bulkReboxed").value) || 0,
      label_count: parseInt(document.getElementById("bulkLabelCount").value) || 0,
      total_operated_box_count: parseInt(document.getElementById("bulkTotalBox").value) || 0,
      pallet_count: parseInt(document.getElementById("bulkPalletCount").value) || 0,
      used_forklift: document.getElementById("bulkUsedForklift").checked ? 1 : 0,
      forklift_location_count: parseInt(document.getElementById("bulkForkliftLoc").value) || 0,
      remark: (document.getElementById("bulkRemark") || {}).value || "",
      result_note: (document.getElementById("bulkResultNote") || {}).value || ""
    });

    if (res && res.ok) {
      // Success — exit
      _bulkStopElapsedTimer();
      alert("大货操作已完成 / 대량화물 작업 완료");
      stopBulkScan();
      clearActiveJob();
      goPage("order_op_menu");
    } else if (res && res.error === "already_completed") {
      _bulkStopElapsedTimer();
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다");
      clearActiveJob();
      goPage("order_op_menu");
    } else if (res && res.error === "others_still_working") {
      // Not last person — kicked out
      _bulkStopElapsedTimer();
      var _nb = res.active_worker_count || "?";
      alert("不能完成，已暂时退出该工单。还有 " + _nb + " 人继续作业\n완료 불가, 퇴장 처리됨. " + _nb + "명이 계속 작업 중입니다");
      stopBulkScan();
      clearActiveJob();
      goPage("order_op_menu");
    } else if (res && res.error === "missing_bulk_output") {
      // Stay on page, show inline error, don't clear inputs
      _bulkSetSubmitting(false);
      _bulkShowError("请先记录操作产出后再完成 / 작업 산출물을 먼저 기록한 후 완료하세요");
    } else {
      // Other error — stay on page
      _bulkSetSubmitting(false);
      _bulkShowError("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

async function refreshBulkWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) {
    renderWorkers("bulkWorkers", res.workers);
    // Update worker count in card
    var wcEl = document.getElementById("bulkWorkerCount");
    if (wcEl && res.job) wcEl.textContent = (res.job.active_worker_count || 0) + " 人/명";
  }
}

// ===== Generic Job — 轻量工时页统一模板 =====
var _genericJobCtx = {};
var _gjStartedAt = null;
var _gjElapsedTimer = null;

function resetGenericJobState() {
  _gjStopElapsedTimer();
  _gjStartedAt = null;
  _gjSwitchState("idle");
  _gjSetSubmitting(false);
  var wcEl = document.getElementById("gjWorkerCount");
  if (wcEl) wcEl.textContent = "0 人/명";
  renderWorkers("gjWorkers", []);
  renderWorkers("gjIdleWorkers", []);
  var elapsedEl = document.getElementById("gjElapsed");
  if (elapsedEl) elapsedEl.textContent = "--";
  var stEl = document.getElementById("gjStartTime");
  if (stEl) stEl.textContent = "--";
}

// ---- 状态切换 helper ----
function _gjSwitchState(state) {
  var idle = document.getElementById("gjStateIdle");
  var working = document.getElementById("gjStateWorking");
  if (!idle || !working) return;
  idle.style.display = state === "idle" ? "" : "none";
  working.style.display = state === "working" ? "" : "none";
  var errBar = document.getElementById("gjErrorBar");
  if (errBar) { errBar.style.display = "none"; errBar.textContent = ""; }
  var subBar = document.getElementById("gjSubmittingBar");
  if (subBar) subBar.style.display = "none";
}

function _gjSetSubmitting(on) {
  var subBar = document.getElementById("gjSubmittingBar");
  if (subBar) subBar.style.display = on ? "" : "none";
  var leaveBtn = document.getElementById("gjLeaveBtn");
  if (leaveBtn) leaveBtn.disabled = on;
  var finBtn = document.getElementById("gjFinishBtn");
  if (finBtn) { finBtn.disabled = on; finBtn.textContent = on ? "提交中.../저장중..." : "结束 / 종료"; }
}

function _gjShowError(msg) {
  var errBar = document.getElementById("gjErrorBar");
  if (errBar) { errBar.textContent = msg; errBar.style.display = ""; }
}

function _gjStartElapsedTimer() {
  if (_gjElapsedTimer) clearInterval(_gjElapsedTimer);
  _gjElapsedTimer = setInterval(function() {
    if (!_gjStartedAt) return;
    var sec = Math.floor((Date.now() - _gjStartedAt) / 1000);
    var h = Math.floor(sec / 3600);
    var m = Math.floor((sec % 3600) / 60);
    var s = sec % 60;
    var el = document.getElementById("gjElapsed");
    if (el) el.textContent = (h > 0 ? h + "h " : "") + m + "m " + s + "s";
  }, 1000);
}

function _gjStopElapsedTimer() {
  if (_gjElapsedTimer) { clearInterval(_gjElapsedTimer); _gjElapsedTimer = null; }
}

function _gjEnterWorkingState(jobDetail) {
  _gjSwitchState("working");
  var titleEl = document.getElementById("gjActiveTitle");
  if (titleEl) titleEl.textContent = _genericJobCtx.title || "--";
  var realStart = null;
  if (jobDetail && jobDetail.created_at) {
    realStart = new Date(jobDetail.created_at);
    if (isNaN(realStart.getTime())) realStart = null;
  }
  _gjStartedAt = realStart ? realStart.getTime() : Date.now();
  var stEl = document.getElementById("gjStartTime");
  if (stEl) stEl.textContent = new Date(_gjStartedAt).toLocaleTimeString();
  _gjStartElapsedTimer();
  if (jobDetail) {
    var wcEl = document.getElementById("gjWorkerCount");
    if (wcEl) wcEl.textContent = (jobDetail.active_worker_count || 0) + " 人/명";
  }
}

// ---- 初始化 ----
function initGenericJob() {
  resetGenericJobState();
  _genericJobCtx = _pageParams || {};
  var title = _genericJobCtx.title || "--";
  var el = document.getElementById("genericJobTitle");
  if (el) el.textContent = title;
  var idleTitle = document.getElementById("gjIdleTitle");
  if (idleTitle) idleTitle.textContent = title;

  if (_activeJobId) {
    api({ action: "v2_ops_job_detail", job_id: _activeJobId }).then(function(res) {
      if (res && res.ok && res.job && res.job.status !== "completed" && res.job.status !== "cancelled") {
        var jobType = res.job.job_type || "";
        var expectedType = _genericJobCtx.job_type || "";
        if (expectedType && jobType !== expectedType) {
          clearActiveJob();
          _gjSwitchState("idle");
          return;
        }
        _gjEnterWorkingState(res.job);
        var wcEl = document.getElementById("gjWorkerCount");
        if (wcEl) wcEl.textContent = (res.job.active_worker_count || 0) + " 人/명";
        renderWorkers("gjWorkers", res.workers);
      } else {
        _gjSwitchState("idle");
        if (res && res.ok && res.job && (res.job.status === "completed" || res.job.status === "cancelled")) {
          clearActiveJob();
        }
      }
    });
  } else {
    _gjSwitchState("idle");
  }
  startJobPoll("generic");
}

function goGenericBack() {
  _gjStopElapsedTimer();
  var stage = _genericJobCtx.flow_stage || "";
  if (stage === "order_op") goPage("order_op_menu");
  else if (stage === "internal") goPage("internal_menu");
  else if (stage === "import") goPage("import_menu");
  else if (stage === "outbound") goPage("outbound_menu");
  else goPage("home");
}

// ---- 开始 ----
async function startGenericJob(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  withActionLock('startGenericJob', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_ops_job_start",
      flow_stage: _genericJobCtx.flow_stage || "",
      biz_class: _genericJobCtx.biz_class || "",
      job_type: _genericJobCtx.job_type || "",
      related_doc_type: "",
      related_doc_id: "",
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      var jobRes = await api({ action: "v2_ops_job_detail", job_id: res.job_id });
      _gjEnterWorkingState(jobRes && jobRes.ok ? jobRes.job : null);
      if (jobRes && jobRes.ok) renderWorkers("gjWorkers", jobRes.workers);
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

// ---- 暂时离开 ----
async function leaveGenericJob(btnEl) {
  if (!_activeJobId) return;
  withActionLock('leaveGenericJob', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_ops_job_leave",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_reason: "leave"
    });
    if (res && res.ok) {
      resetGenericJobState();
      clearActiveJob();
      alert("您已退出当前作业，其他人仍可继续\n현재 작업에서 퇴장했습니다. 다른 작업자는 계속할 수 있습니다");
      goGenericBack();
    } else {
      alert("失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

// ---- 结束 ----
async function finishGenericJob(btnEl) {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  withActionLock('finishGenericJob', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_ops_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId()
    });
    if (res && res.ok) {
      resetGenericJobState();
      clearActiveJob();
      alert("任务已完成 / 작업 완료");
      goGenericBack();
    } else if (res && res.error === "already_completed") {
      resetGenericJobState();
      clearActiveJob();
      alert("任务已完成，请勿重复提交\n작업이 이미 완료되었습니다");
      goGenericBack();
    } else if (res && res.error === "others_still_working") {
      resetGenericJobState();
      clearActiveJob();
      alert("您已退出当前作业，还有 " + (res.active_worker_count || 0) + " 人继续作业\n현재 작업에서 퇴장했습니다. " + (res.active_worker_count || 0) + "명이 계속 작업 중");
      goGenericBack();
    } else {
      _gjShowError(res ? (res.message || res.error) : "unknown");
    }
  });
}

// ---- 刷新人员 ----
async function refreshGenericWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) {
    renderWorkers("gjWorkers", res.workers);
    renderWorkers("gjIdleWorkers", res.workers);
    var wcEl = document.getElementById("gjWorkerCount");
    if (wcEl) wcEl.textContent = (res.job.active_worker_count || 0) + " 人/명";
  }
}

// ===== Task Interrupts =====
async function interruptToUnload() {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  if (!confirm("挂起当前任务，临时去卸货？\n현재 작업을 일시정지하고 임시 하차로 이동하시겠습니까?")) return;

  withActionLock('interruptToUnload', null, null, async function() {
    localStorage.setItem(V2_INTERRUPT_KEY, JSON.stringify({
      parent_job_id: _activeJobId,
      parent_page: _currentPage,
      parent_params: _pageParams
    }));

    var res = await api({
      action: "v2_ops_job_leave",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      leave_reason: "interrupted"
    });
    if (!res || !res.ok) {
      alert("挂起失败 / 일시정지 실패: " + (res ? res.error : "unknown"));
      localStorage.removeItem(V2_INTERRUPT_KEY);
      return;
    }

    clearActiveJob();
    _unloadPlanData = null;
    _navStack = [];
    goPage("unload");
  });
}

async function interruptToLoad() {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  if (!confirm("挂起当前任务，临时去装货？\n현재 작업을 일시정지하고 임시 상차로 이동하시겠습니까?")) return;

  withActionLock('interruptToLoad', null, null, async function() {
    localStorage.setItem(V2_INTERRUPT_KEY, JSON.stringify({
      parent_job_id: _activeJobId,
      parent_page: _currentPage,
      parent_params: _pageParams
    }));

    var res = await api({
      action: "v2_ops_job_start",
      flow_stage: "outbound",
      job_type: "load_outbound",
      related_doc_type: "",
      related_doc_id: "",
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      parent_job_id: _activeJobId,
      is_temporary_interrupt: true,
      interrupt_type: "load"
    });

    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      _navStack = [];
      goPage("outbound_load");
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  });
}

function hasInterruptContext() {
  try {
    var saved = JSON.parse(localStorage.getItem(V2_INTERRUPT_KEY) || "null");
    return !!(saved && saved.parent_job_id);
  } catch(e) { return false; }
}

async function checkAndResumeParent() {
  var saved = null;
  try { saved = JSON.parse(localStorage.getItem(V2_INTERRUPT_KEY) || "null"); } catch(e) {}
  if (!saved || !saved.parent_job_id) return false;

  var doResume = confirm("临时任务已结束，是否恢复原任务？\n임시 작업이 종료되었습니다. 원래 작업으로 복귀하시겠습니까?");
  if (doResume) {
    var res = await api({
      action: "v2_ops_job_resume",
      parent_job_id: saved.parent_job_id,
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (res && res.ok) {
      saveActiveJob(saved.parent_job_id, res.worker_seg_id);
      localStorage.removeItem(V2_INTERRUPT_KEY);
      _unloadPlanData = null;
      _pageParams = saved.parent_params || {};
      showPage(saved.parent_page || "home");
      return true;
    } else {
      alert("恢复失败，返回首页 / 복귀 실패, 홈으로 이동");
    }
  }
  localStorage.removeItem(V2_INTERRUPT_KEY);
  clearActiveJob();
  _unloadPlanData = null;
  goPage("home");
  return true;
}

// ===== Photo Upload =====
function uploadPhoto(docType, category) {
  var docId = "";
  if (docType === "outbound_order") {
    docId = (document.getElementById("loadOrderSelect") || {}).value || "";
  }
  if (!docId) docId = _activeJobId || _currentIssueId || "";
  _photoUploadCtx = {
    related_doc_type: docType,
    attachment_category: category,
    related_doc_id: docId
  };
  document.getElementById("photoInput").click();
}

async function handlePhotoUpload(input) {
  if (!input.files || !input.files[0]) return;
  var file = input.files[0];
  var fd = new FormData();
  fd.append("file", file);
  fd.append("related_doc_type", _photoUploadCtx.related_doc_type || "");
  fd.append("related_doc_id", _photoUploadCtx.related_doc_id || "");
  fd.append("attachment_category", _photoUploadCtx.attachment_category || "");
  fd.append("uploaded_by", getWorkerName());

  var res = await uploadFile(fd);
  if (res && res.ok) {
    alert("上传成功 / 업로드 성공");
    // Refresh if on issue detail
    if (_currentPage === "issue_detail") loadIssueDetail();
  } else {
    alert("上传失败 / 업로드 실패: " + (res ? res.error : "unknown"));
  }
  input.value = "";
}

function showLightbox(url) {
  document.getElementById("lightboxImg").src = url;
  document.getElementById("lightbox").classList.remove("hidden");
}

// ===== Utils =====
function fmtTime(isoStr) {
  if (!isoStr) return "--";
  try {
    var d = new Date(isoStr);
    var m = d.getMonth() + 1;
    var day = d.getDate();
    var h = d.getHours();
    var min = d.getMinutes();
    return m + "-" + day + " " + (h < 10 ? "0" : "") + h + ":" + (min < 10 ? "0" : "") + min;
  } catch(e) { return isoStr; }
}

// =====================================================
// Verify Scan — 扫码核对
// =====================================================
var _vsBatchId = "";        // 当前进入作业的 batch_id
var _vsBatchList = [];      // entry 态缓存
var _vsLastPallet = "";     // 上一次填写的托盘号
var _vsSubmitInflight = false;

function initVerifyScan() {
  var entry = document.getElementById("vsEntryCard");
  var working = document.getElementById("vsWorkingCard");
  var logCard = document.getElementById("vsScanLogCard");
  var workersCard = document.getElementById("vsWorkersCard");
  var actionCard = document.getElementById("vsActionCard");

  // 已在本人 verify_scan 任务中 — 直接回 working 态
  if (_activeJobId) {
    api({ action: "v2_ops_job_detail", job_id: _activeJobId }).then(function(res) {
      if (res && res.ok && res.job && res.job.job_type === "verify_scan" && res.job.status === "working") {
        _vsBatchId = res.job.related_doc_id || "";
        entry.style.display = "none";
        working.style.display = "";
        logCard.style.display = "";
        workersCard.style.display = "";
        actionCard.style.display = "";
        renderWorkers("vsWorkers", res.workers);
        refreshVerifyScanSummary();
        refreshVerifyScanLogs();
        startJobPoll("verify_scan");
        setTimeout(function() {
          var p = document.getElementById("vsPalletInput");
          if (p && _vsLastPallet) p.value = _vsLastPallet;
        }, 50);
        return;
      }
      enterVsEntryState();
    });
    return;
  }
  enterVsEntryState();
}

function enterVsEntryState() {
  document.getElementById("vsEntryCard").style.display = "";
  document.getElementById("vsWorkingCard").style.display = "none";
  document.getElementById("vsScanLogCard").style.display = "none";
  document.getElementById("vsWorkersCard").style.display = "none";
  document.getElementById("vsActionCard").style.display = "none";
  loadVsBatches();
}

function verifyScanGoBack() {
  if (_activeJobId) {
    // 提示：有活跃任务不直接返回，而是走首页顶部任务条恢复
    goPage("home");
    return;
  }
  goPage("outbound_menu");
}

async function loadVsBatches() {
  var sel = document.getElementById("vsBatchSelect");
  if (!sel) return;
  sel.innerHTML = '<option value="">加载中.../로딩중...</option>';
  // pending + verifying 都能被接续
  var pair = await Promise.all([
    api({ action: "v2_verify_batch_list", status: "pending" }),
    api({ action: "v2_verify_batch_list", status: "verifying" })
  ]);
  var items = [];
  if (pair[0] && pair[0].ok) items = items.concat(pair[0].items || []);
  if (pair[1] && pair[1].ok) items = items.concat(pair[1].items || []);
  _vsBatchList = items;
  sel.innerHTML = '<option value="">-- 选择批次/배치 선택 --</option>';
  items.forEach(function(b) {
    var opt = document.createElement("option");
    opt.value = b.id;
    opt.textContent = b.batch_no + " · " + (b.customer_name || "--") + " · 计划/계획 " + (b.planned_qty || 0);
    sel.appendChild(opt);
  });
  onVsBatchChange();
}

function onVsBatchChange() {
  var sel = document.getElementById("vsBatchSelect");
  var info = document.getElementById("vsBatchInfo");
  if (!sel || !info) return;
  var id = sel.value;
  if (!id) { info.textContent = "--"; return; }
  var b = null;
  for (var i = 0; i < _vsBatchList.length; i++) if (_vsBatchList[i].id === id) { b = _vsBatchList[i]; break; }
  if (!b) { info.textContent = "--"; return; }
  info.innerHTML = '客户/고객: <b>' + esc(b.customer_name || "--") + '</b> · 状态/상태: <b>' + esc(b.status) +
    '</b> · 计划/계획: <b>' + (b.planned_qty || 0) + '</b> · 已扫/완료: <b>' + (b.scanned_ok_count || 0) +
    '</b> · 异常/이상: <b>' + (b.abnormal_count || 0) + '</b>';
}

function startVerifyScan(btnEl) {
  if (hasOtherActiveJob()) return warnActiveJob();
  var sel = document.getElementById("vsBatchSelect");
  var batch_id = sel ? sel.value : "";
  if (!batch_id) { alert("请先选择批次 / 배치를 선택하세요"); return; }
  withActionLock('startVerifyScan', btnEl || null, '开始中.../시작중...', async function() {
    var res = await api({
      action: "v2_verify_job_start",
      batch_id: batch_id,
      worker_id: getWorkerId(),
      worker_name: getWorkerName()
    });
    if (!res || !res.ok) {
      if (res && res.error === "worker_has_active_job") {
        alert("已有进行中的任务，请先结束 / 이미 진행 중인 작업이 있습니다");
        return;
      }
      alert((res && (res.message || res.error)) || "开始失败 / 시작 실패");
      return;
    }
    saveActiveJob(res.job_id, res.worker_seg_id);
    _vsBatchId = batch_id;
    // 进入 working 态
    document.getElementById("vsEntryCard").style.display = "none";
    document.getElementById("vsWorkingCard").style.display = "";
    document.getElementById("vsScanLogCard").style.display = "";
    document.getElementById("vsWorkersCard").style.display = "";
    document.getElementById("vsActionCard").style.display = "";
    await refreshVerifyScanSummary();
    await refreshVerifyScanLogs();
    await refreshVerifyScanWorkers();
    startJobPoll("verify_scan");
    setTimeout(function() {
      var p = document.getElementById("vsPalletInput");
      if (p) p.focus();
    }, 80);
  });
}

async function refreshVerifyScanSummary() {
  if (!_vsBatchId) return;
  var res = await api({ action: "v2_verify_batch_detail", id: _vsBatchId });
  if (!res || !res.ok) return;
  applyVsSummary(res.batch, res.summary);
}

function applyVsSummary(batch, sum) {
  var head = document.getElementById("vsBatchHeader");
  if (head && batch) {
    head.innerHTML = '<b>' + esc(batch.batch_no || "--") + '</b> · ' + esc(batch.customer_name || "--") +
      ' · <span class="st st-' + esc(batch.status) + '">' + esc(batch.status) + '</span>';
  }
  var plan = document.getElementById("vsPlanQty");
  var ok = document.getElementById("vsOkQty");
  var ab = document.getElementById("vsAbQty");
  var diff = document.getElementById("vsDiffQty");
  var planVal = (sum && (sum.planned_total_box_count != null ? sum.planned_total_box_count : sum.planned_qty)) || 0;
  var okVal = (sum && (sum.scanned_ok_total_count != null ? sum.scanned_ok_total_count : sum.scanned_ok_count)) || 0;
  var abVal = (sum && sum.abnormal_count) || 0;
  if (plan) plan.textContent = planVal;
  if (ok) ok.textContent = okVal;
  if (ab) ab.textContent = abVal;
  if (diff) {
    var d = (sum && typeof sum.diff === 'number') ? sum.diff : (planVal - okVal);
    diff.textContent = d;
  }
}

async function refreshVerifyScanLogs() {
  if (!_vsBatchId) return;
  var res = await api({ action: "v2_verify_batch_detail", id: _vsBatchId });
  if (!res || !res.ok) return;
  renderVsLogs(res.scan_logs || []);
}

function renderVsLogs(logs) {
  var body = document.getElementById("vsScanLogBody");
  if (!body) return;
  if (!logs.length) {
    body.innerHTML = '<span class="muted">暂无扫码记录 / 스캔 기록 없음</span>';
    return;
  }
  var recent = logs.slice(0, 20);
  var html = "";
  recent.forEach(function(l) {
    var cls = l.scan_result === 'ok' ? 'ok' :
              (l.scan_result === 'not_found' ? 'err' : 'warn');
    html += '<div class="scan-log-row">' +
      '<span class="tag ' + cls + '">' + esc(l.scan_result) + '</span>' +
      '<b>' + esc(l.barcode) + '</b> · P/팔: ' + esc(l.pallet_no || '--') +
      ' · ' + esc(l.worker_name || l.worker_id || '--') + ' · ' + esc(fmtTime(l.scanned_at)) +
      (l.message ? ' · ' + esc(l.message) : '') +
    '</div>';
  });
  body.innerHTML = html;
}

async function refreshVerifyScanWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("vsWorkers", res.workers);
}

function onVsBarcodeKey(ev) {
  if (ev && (ev.key === "Enter" || ev.keyCode === 13)) {
    ev.preventDefault();
    submitVsBarcode();
  }
}

async function submitVsBarcode() {
  if (_vsSubmitInflight) return;
  if (!_activeJobId || !_vsBatchId) return;
  var pEl = document.getElementById("vsPalletInput");
  var bEl = document.getElementById("vsBarcodeInput");
  var pallet_no = (pEl && pEl.value || "").trim();
  var barcode = (bEl && bEl.value || "").trim();
  if (!pallet_no) { showVsResult("warn", "请先填写托盘号 / 팔레트 번호를 입력하세요"); if (pEl) pEl.focus(); return; }
  if (!barcode) return;
  _vsSubmitInflight = true;
  try {
    var res = await api({
      action: "v2_verify_scan_submit",
      batch_id: _vsBatchId,
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      pallet_no: pallet_no,
      barcode: barcode
    });
    _vsLastPallet = pallet_no;
    if (!res || !res.ok) {
      showVsResult("err", (res && (res.message || res.error)) || "提交失败 / 제출 실패");
    } else {
      var r = res.scan_result;
      var cls = r === 'ok' ? 'ok' : (r === 'not_found' ? 'err' : 'warn');
      var info = res.barcode_info || {};
      var msg;
      if (r === 'not_found') {
        msg = '[not_found] ' + barcode + '｜不在本批次 / 배치에 없음';
      } else {
        var cu = info.customer_name || '--';
        var okN = (info.scanned_ok_count != null) ? info.scanned_ok_count : '?';
        var planN = (info.planned_box_count != null) ? info.planned_box_count : '?';
        var tag = r === 'ok' ? 'OK' : (r === 'overflow' ? '超扫/초과' : r);
        msg = '[' + tag + '] ' + barcode + '｜' + cu + '｜已扫 ' + okN + '/' + planN;
        if (r === 'overflow') msg += '（已超出计划）';
      }
      showVsResult(cls, msg);
      applyVsSummary(null, res.summary);
      refreshVerifyScanLogs();
    }
  } finally {
    _vsSubmitInflight = false;
    if (bEl) { bEl.value = ""; bEl.focus(); }
  }
}

function showVsResult(kind, text) {
  var el = document.getElementById("vsLastResult");
  if (!el) return;
  el.className = "scan-feedback result-" + kind;
  el.style.display = "";
  el.textContent = text;
}

function leaveVerifyScan(btnEl) {
  if (!_activeJobId) { goPage("home"); return; }
  withActionLock('leaveVerifyScan', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_verify_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      complete_job: false
    });
    if (!res || !res.ok) {
      alert((res && (res.message || res.error)) || "暂离失败 / 퇴장 실패");
      return;
    }
    clearActiveJob();
    goPage("home");
  });
}

async function finishVerifyScan(btnEl) {
  if (!_activeJobId) { goPage("home"); return; }
  if (_vsBatchId) {
    try {
      var detail = await api({ action: "v2_verify_batch_detail", id: _vsBatchId });
      if (detail && detail.ok && detail.summary) {
        var s = detail.summary;
        var shortage = s.shortage_count || 0;
        var overflow = s.overflow_count || 0;
        var notScanned = s.not_scanned_count || 0;
        var notFound = s.not_found_count || 0;
        if (shortage + overflow + notScanned + notFound > 0) {
          var txt = "当前核对仍有差异 / 현재 차이 있음:\n" +
            "少扫 / 부족: " + shortage + "\n" +
            "超扫 / 초과: " + overflow + "\n" +
            "未扫 / 미스캔: " + notScanned + "\n" +
            "非本批次 / 미일치: " + notFound + "\n\n" +
            "是否仍然完成核对？/ 그래도 완료하시겠습니까？";
          if (!confirm(txt)) return;
        }
      }
    } catch (e) { /* 忽略网络异常，仍允许用户完成 */ }
  }
  var doCloseBatch = confirm("是否同时将该核对批次标记为【已完成】？\n배치를 '완료'로 표시하시겠습니까？\n\n[确定/확인] = 结束任务+关闭批次\n[取消/취소] = 只结束任务，批次仍开放");
  withActionLock('finishVerifyScan', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_verify_job_finish",
      job_id: _activeJobId,
      worker_id: getWorkerId(),
      complete_job: true,
      complete_batch: !!doCloseBatch
    });
    if (!res || !res.ok) {
      alert((res && (res.message || res.error)) || "完成失败 / 완료 실패");
      return;
    }
    clearActiveJob();
    _vsBatchId = "";
    _vsLastPallet = "";
    goPage("home");
  });
}

// ===== Init =====
window.addEventListener("DOMContentLoaded", function() {
  checkBadgeAuth();
  // 自 patch.js 合并：卸货计划下拉变更时预览
  var unloadSel = document.getElementById('unloadPlanSelect');
  if (unloadSel) {
    unloadSel.addEventListener('change', previewSelectedPlan);
  }
  stripDiffRequired();
});
