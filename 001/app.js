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
function api(params) {
  params.k = OPS_KEY;
  return fetch(V2_API, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params)
  }).then(function(res) { return res.json(); })
    .catch(function(e) { return { ok: false, error: "network: " + e }; });
}

function uploadFile(formData) {
  formData.append("k", OPS_KEY);
  formData.append("action", "v2_attachment_upload");
  return fetch(V2_API, { method: "POST", body: formData })
    .then(function(res) { return res.json(); })
    .catch(function(e) { return { ok: false, error: "upload failed: " + e }; });
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
  if (name === "outbound_load") initOutboundLoad();
  if (name === "issue_list") loadIssueList();
  if (name === "issue_detail") loadIssueDetail();
  if (name === "generic_job") initGenericJob();
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
      else if (jt.indexOf("inbound") === 0) { _pageParams = { job_type: jt, biz_class: res.job.biz_class || "" }; showPage("inbound"); }
      else if (jt === "load_outbound") showPage("outbound_load");
      else if (jt === "issue_handle") { _currentIssueId = res.job.related_doc_id || null; showPage("issue_detail"); }
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

// ===== Home =====
function initHome() {
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
  localStorage.removeItem(V2_INTERRUPT_KEY);
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
    else if (jt.indexOf("inbound") === 0) goPage("inbound", { job_type: jt, biz_class: res.job.biz_class || "" });
    else if (jt === "load_outbound") goPage("outbound_load");
    else if (jt === "issue_handle") goPage("issue_detail");
    else goPage("generic_job");
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
  load_outbound: "出库装货/출고 상차",
  inventory: "盘点/재고조사",
  disposal: "废弃处理/폐기 처리",
  qc: "质检/품검",
  issue_handle: "问题点处理/이슈 처리",
  other_internal: "其他库内/기타 창고작업",
  scan_pallet: "过机扫描/스캔",
  load_import: "装柜出货/적재 출고"
};

var STATUS_LABEL = {
  pending: "待处理/대기중",
  processing: "处理中/처리중",
  working: "作业中/작업중",
  responded: "已反馈/피드백완료",
  completed: "已完成/완료",
  closed: "已关闭/종료",
  cancelled: "已取消/취소됨",
  draft: "草稿/초안",
  issued: "已下发/배정됨",
  arrived: "已到货/도착",
  awaiting_close: "待收尾/마감대기"
};

var BIZ_LABEL = {
  direct_ship: "代发/직배송",
  bulk: "大货/대량화물",
  return: "退件/반품",
  import: "进口/수입"
};

var PRIORITY_LABEL = {
  urgent: "🔴 紧急/긴급",
  high: "🟠 高/높음",
  normal: "普通/보통",
  low: "低/낮음"
};

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

async function initUnload() {
  _unloadPlanData = null;
  stopUnloadScan();

  // If we have an active unload job, jump to working state
  if (_activeJobId) {
    var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
    if (res && res.ok && res.job && res.job.job_type === "unload" && res.job.status === "working") {
      var planId = res.job.related_doc_id || "";
      if (planId) {
        var planRes = await api({ action: "v2_inbound_plan_detail", id: planId });
        if (planRes && planRes.ok) _unloadPlanData = planRes;
      }
      showUnloadWorking(res.job);
      startJobPoll("unload");
      return;
    }
  }

  // Show entry state
  showUnloadEntry();
}

async function showUnloadEntry() {
  document.getElementById("unloadEntryCard").style.display = "";
  document.getElementById("unloadPlanCard").style.display = "none";
  document.getElementById("unloadWorkersCard").style.display = "none";
  document.getElementById("unloadResultCard").style.display = "none";
  await loadInboundPlans("unloadPlanSelect");
}

function showUnloadWorking(job) {
  document.getElementById("unloadEntryCard").style.display = "none";
  document.getElementById("unloadWorkersCard").style.display = "";
  document.getElementById("unloadResultCard").style.display = "";

  // Show plan info if available
  if (_unloadPlanData && _unloadPlanData.plan) {
    var p = _unloadPlanData.plan;
    var lines = _unloadPlanData.lines || [];
    document.getElementById("unloadPlanCard").style.display = "";
    document.getElementById("unloadPlanInfo").innerHTML =
      '<div><b>' + esc(p.display_no || p.id) + '</b> | ' + esc(p.plan_date) + ' | ' + esc(p.customer) + '</div>' +
      '<div class="muted">' + esc(p.cargo_summary) + (p.remark ? ' — ' + esc(p.remark) : '') + '</div>';

    if (lines.length > 0) {
      var tbl = '<table class="mini-table"><tr><th>类型/유형</th><th>计划/계획</th></tr>';
      lines.forEach(function(ln) {
        tbl += '<tr><td>' + unitLabel(ln.unit_type) + '</td><td>' + ln.planned_qty + '</td></tr>';
      });
      tbl += '</table>';
      document.getElementById("unloadPlanLinesArea").innerHTML = tbl;
    } else {
      document.getElementById("unloadPlanLinesArea").innerHTML = '<span class="muted">无明细 / 명세 없음</span>';
    }
  } else {
    document.getElementById("unloadPlanCard").style.display = "none";
  }

  // Build result lines form
  buildUnloadResultForm();
  refreshUnloadWorkers();
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

  var html = '<button class="btn btn-outline" onclick="unloadLeave()">暂时离开 / 일시 퇴장</button>';
  if (workerCount <= 1) {
    html = '<button class="btn btn-success" onclick="unloadComplete()">完成卸货 / 하차 완료</button>' +
      '<div style="height:8px"></div>' + html;
  } else {
    html = '<div class="muted" style="margin-bottom:8px;">还有其他人参与中(' + workerCount + '人/명)，无法完成 / 다른 참여자 있음</div>' + html;
  }
  actionsDiv.innerHTML = html;
}

async function loadInboundPlans(selectId) {
  var sel = document.getElementById(selectId);
  if (!sel) return;
  var res = await api({ action: "v2_inbound_plan_list", start_date: "", end_date: "", status: "" });
  var opts = '<option value="">-- 选择入库计划/입고계획 선택 --</option>';
  if (res && res.ok && res.items) {
    res.items.forEach(function(p) {
      if (p.status === "completed" || p.status === "cancelled") return;
      opts += '<option value="' + esc(p.id) + '">[' + esc(p.display_no || p.id) + '] ' + esc(p.customer) + ' - ' + esc(p.cargo_summary) + '</option>';
    });
  }
  sel.innerHTML = opts;
}

async function startUnload() {
  if (_startInflight) return;
  _startInflight = true;
  try {
    var planId = document.getElementById("unloadPlanSelect").value;
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
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  } finally { _startInflight = false; }
}

function startUnloadNoPlan() {
  document.getElementById("unloadPlanSelect").value = "";
  startUnload();
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
      function(decoded) {
        stopUnloadScan();
        // Try to find matching plan
        var sel = document.getElementById("unloadPlanSelect");
        var found = false;
        for (var i = 0; i < sel.options.length; i++) {
          if (sel.options[i].value === decoded) {
            sel.value = decoded;
            found = true;
            break;
          }
        }
        if (found) {
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

async function unloadLeave() {
  if (!_activeJobId) return;
  if (!confirm("确认暂时离开？/ 일시 퇴장하시겠습니까?")) return;
  var res = await api({
    action: "v2_unload_job_finish",
    job_id: _activeJobId,
    worker_id: getWorkerId(),
    leave_only: true
  });
  if (res && res.ok) {
    clearActiveJob();
    _unloadPlanData = null;
    goPage("home");
  } else {
    alert("失败/실패: " + (res ? res.error : "unknown"));
  }
}

async function unloadComplete() {
  if (!_activeJobId) return;
  var resultLines = getUnloadResultLines();
  if (resultLines.length === 0) {
    alert("请至少填写一项实际数量 / 실제 수량을 최소 1건 입력하세요");
    return;
  }

  // Check diff note
  var planLines = (_unloadPlanData && _unloadPlanData.lines) || [];
  var diffNote = (document.getElementById("unloadDiffNote") || {}).value || "";
  diffNote = diffNote.trim();
  if (planLines.length > 0) {
    var hasDiff = false;
    var actualMap = {};
    resultLines.forEach(function(r) { actualMap[r.unit_type] = r.actual_qty; });
    planLines.forEach(function(ln) {
      if ((actualMap[ln.unit_type] || 0) !== (ln.planned_qty || 0)) hasDiff = true;
    });
    if (hasDiff && !diffNote) {
      alert("计划与实际有差异，请填写差异说明 / 계획과 실제에 차이가 있어 차이 설명을 입력해주세요");
      return;
    }
  }

  var remark = (document.getElementById("unloadRemark") || {}).value || "";
  var res = await api({
    action: "v2_unload_job_finish",
    job_id: _activeJobId,
    worker_id: getWorkerId(),
    result_lines: resultLines,
    diff_note: diffNote,
    remark: remark.trim(),
    complete_job: true
  });

  if (res && res.ok) {
    var msg = "卸货已完成 / 하차 완료";
    if (res.no_doc) msg += "\n（无单卸货已自动生成反馈 / 서류 없는 하차 피드백 자동 생성됨）";
    alert(msg);
    clearActiveJob();
    _unloadPlanData = null;
    goPage("home");
  } else if (res && res.error === "others_still_working") {
    alert("还有" + res.active_count + "人参与��，无法完成 / 아직 " + res.active_count + "명 참여 중, 완료 불가");
  } else if (res && res.error === "empty_result") {
    alert(res.message || "至少填写一项实际数量");
  } else if (res && res.error === "diff_note_required") {
    alert(res.message || "请填写差异说明");
  } else {
    alert("失败/실패: " + (res ? res.error : "unknown"));
  }
}

function unloadGoBack() {
  if (_activeJobId) {
    if (confirm("离开将暂停当前任务 / 퇴장 시 현재 작업이 일시정지됩니다. 确认？/ 확인?")) {
      unloadLeave();
    }
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
    if (type === "generic") refreshGenericWorkers();
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

// ===== Inbound =====
async function initInbound() {
  var title = document.getElementById("inboundTitle");
  var jt = _pageParams.job_type || "inbound_direct";
  title.textContent = JOB_TYPE_LABEL[jt] || "入库/입고";
  await loadInboundPlans("inboundPlanSelect");
  startJobPoll("inbound");
}

async function startInbound() {
  if (_startInflight) return;
  _startInflight = true;
  try {
    var planId = document.getElementById("inboundPlanSelect").value;
    var res = await api({
      action: "v2_inbound_job_start",
      plan_id: planId,
      worker_id: getWorkerId(),
      worker_name: getWorkerName(),
      biz_class: _pageParams.biz_class || "",
      job_type: _pageParams.job_type || "inbound_direct"
    });
    if (res && res.ok) {
      saveActiveJob(res.job_id, res.worker_seg_id);
      if (!res.already_joined) {
        alert(res.is_new_job ? "已创建入库任务 / 입고 작업 생성됨" : "已加入入库任务 / 입고 작업 참여됨");
      }
      refreshInboundWorkers();
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  } finally { _startInflight = false; }
}

async function finishInbound() {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  var res = await api({
    action: "v2_inbound_job_finish",
    job_id: _activeJobId,
    worker_id: getWorkerId(),
    remark: "",
    complete_job: true
  });
  if (res && res.ok) {
    alert("入库已完成 / 입고 완료");
    clearActiveJob();
    goPage("home");
  } else {
    alert("失败/실패: " + (res ? res.error : "unknown"));
  }
}

async function refreshInboundWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("inboundWorkers", res.workers);
}

// ===== Outbound Load =====
async function initOutboundLoad() {
  await loadOutboundOrders();
  startJobPoll("load");
}

async function loadOutboundOrders() {
  var sel = document.getElementById("loadOrderSelect");
  if (!sel) return;
  var res = await api({ action: "v2_outbound_order_list", start_date: "", end_date: "" });
  var opts = '<option value="">-- 选择出库单/출고단 선택 --</option>';
  if (res && res.ok && res.items) {
    res.items.forEach(function(o) {
      if (o.status === "completed" || o.status === "cancelled") return;
      opts += '<option value="' + esc(o.id) + '">[' + esc(o.status) + '] ' + esc(o.order_date) + ' ' + esc(o.customer) + '</option>';
    });
  }
  sel.innerHTML = opts;
}

async function startOutboundLoad() {
  if (_startInflight) return;
  _startInflight = true;
  try {
    var orderId = document.getElementById("loadOrderSelect").value;
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
      document.getElementById("loadResultCard").style.display = "";
      document.getElementById("loadInterruptBar").style.display = "";
      refreshLoadWorkers();
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  } finally { _startInflight = false; }
}

function startLoadNoOrder() {
  document.getElementById("loadOrderSelect").value = "";
  startOutboundLoad();
}

async function finishOutboundLoad() {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
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
    clearActiveJob();
    goPage("home");
  } else {
    alert("失败/실패: " + (res ? res.error : "unknown"));
  }
}

async function saveOutboundLoadResult() {
  await finishOutboundLoad();
}

async function refreshLoadWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("loadWorkers", res.workers);
}

// ===== Issue List =====
async function loadIssueList() {
  var body = document.getElementById("issueListBody");
  if (!body) return;
  body.innerHTML = '<div class="card"><span class="muted">加载中.../로딩중...</span></div>';

  var statusMap = { pending: "pending", processing: "processing", my: "", responded: "responded" };
  var status = statusMap[_issueFilter] || "";
  var res = await api({ action: "v2_issue_ops_list", status: status });

  if (!res || !res.ok) {
    body.innerHTML = '<div class="card"><span class="muted">加载失败/로딩 실패</span></div>';
    return;
  }

  var items = res.items || [];
  if (_issueFilter === "my") {
    // Show processing + responded
    items = items.filter(function(it) { return it.status === "processing" || it.status === "responded"; });
  }

  if (items.length === 0) {
    body.innerHTML = '<div class="card"><span class="muted">暂无数据 / 데이터 없음</span></div>';
    return;
  }

  var html = "";
  items.forEach(function(it) {
    html += '<div class="list-item" onclick="openIssue(\'' + esc(it.id) + '\')">' +
      '<div class="item-title">' +
        '<span class="st st-' + esc(it.status) + '">' + esc(STATUS_LABEL[it.status] || it.status) + '</span> ' +
        '<span class="biz-tag biz-' + esc(it.biz_class) + '">' + esc(BIZ_LABEL[it.biz_class] || it.biz_class) + '</span> ' +
        '<span class="priority-' + esc(it.priority) + '">' + esc(PRIORITY_LABEL[it.priority] || it.priority) + '</span>' +
      '</div>' +
      '<div style="font-size:14px;font-weight:600;margin-top:4px;">' + esc(it.issue_summary || "(无摘要)") + '</div>' +
      '<div class="item-meta">' +
        esc(it.customer || "") + (it.related_doc_no ? " · " + esc(it.related_doc_no) : "") +
        ' · ' + esc(it.issue_type || "") +
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
  html += '<div style="font-size:18px;font-weight:700;margin-bottom:8px;">' + esc(it.issue_summary || "(无摘要)") + '</div>';
  html += '<div class="detail-field"><b>状态/상태:</b> <span class="st st-' + esc(it.status) + '">' + esc(STATUS_LABEL[it.status] || it.status) + '</span></div>';
  html += '<div class="detail-field"><b>业务/업무:</b> <span class="biz-tag biz-' + esc(it.biz_class) + '">' + esc(BIZ_LABEL[it.biz_class] || it.biz_class) + '</span></div>';
  html += '<div class="detail-field"><b>优先级/우선순위:</b> ' + esc(PRIORITY_LABEL[it.priority] || it.priority) + '</div>';
  html += '<div class="detail-field"><b>客户/고객:</b> ' + esc(it.customer) + '</div>';
  html += '<div class="detail-field"><b>关联单号/관련번호:</b> ' + esc(it.related_doc_no) + '</div>';
  html += '<div class="detail-field"><b>类型/유형:</b> ' + esc(it.issue_type) + '</div>';
  html += '<div class="detail-field"><b>提出人/제출자:</b> ' + esc(it.submitted_by) + '</div>';
  html += '<div class="detail-field"><b>提出时间/제출시간:</b> ' + esc(fmtTime(it.created_at)) + '</div>';
  html += '<div class="detail-section"><b>问题描述/문제 설명:</b><div style="margin-top:4px;white-space:pre-wrap;">' + esc(it.issue_description) + '</div></div>';

  if (it.latest_feedback_text) {
    html += '<div class="detail-section"><b>最新反馈/최신 피드백:</b><div style="margin-top:4px;white-space:pre-wrap;">' + esc(it.latest_feedback_text) + '</div></div>';
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
  if (it.status === "pending" || it.status === "processing") {
    html += '<div class="card">';
    if (it.status === "pending") {
      html += '<button class="btn btn-success" onclick="handleIssueStart()">开始处理 / 처리 시작</button>';
    }
    if (it.status === "processing") {
      html += '<div class="detail-section"><label>反馈内容 / 피드백 내용</label>';
      html += '<textarea id="issueFeedback" rows="3" placeholder="输入处理结果 / 처리 결과를 입력하세요"></textarea>';
      html += '<label>上传照片 / 사진 업로드</label>';
      html += '<div class="photo-upload" id="issuePhotos"><div class="photo-add" onclick="uploadPhoto(\'issue_ticket\',\'feedback_photo\')">+</div></div>';
      html += '<button class="btn btn-danger mt-10" onclick="handleIssueFinish()">结束处理 / 처리 종료</button>';
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

async function handleIssueStart() {
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
}

async function handleIssueFinish() {
  if (!_currentRunId && _activeJobId) {
    // Try to find the run from job
    var jobRes = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  }

  // Find latest working run for this issue
  var detailRes = await api({ action: "v2_issue_detail", id: _currentIssueId });
  var runs = (detailRes && detailRes.handle_runs) || [];
  var workingRun = runs.find(function(r) { return r.run_status === "working"; });
  if (!workingRun) {
    alert("找不到进行中的处理记录 / 진행 중인 처리 기록을 찾을 수 없습니다");
    return;
  }

  var feedback = document.getElementById("issueFeedback");
  var feedbackText = feedback ? feedback.value.trim() : "";

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
}

// ===== Generic Job =====
var _genericJobCtx = {};
function initGenericJob() {
  var title = document.getElementById("genericJobTitle");
  _genericJobCtx = _pageParams || {};
  title.textContent = _genericJobCtx.title || "--";
  startJobPoll("generic");
}

function goGenericBack() {
  var stage = _genericJobCtx.flow_stage || "";
  if (stage === "order_op") goPage("order_op_menu");
  else if (stage === "internal") goPage("internal_menu");
  else if (stage === "import") goPage("import_menu");
  else if (stage === "outbound") goPage("outbound_menu");
  else goPage("home");
}

async function startGenericJob() {
  if (_startInflight) return;
  _startInflight = true;
  try {
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
      if (!res.already_joined) {
        alert(res.is_new_job ? "已创建任务 / 작업 생성됨" : "已加入任务 / 작업 참여됨");
      }
      refreshGenericWorkers();
    } else {
      alert("失败/실패: " + (res ? res.error : "unknown"));
    }
  } finally { _startInflight = false; }
}

async function finishGenericJob() {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  var res = await api({
    action: "v2_ops_job_finish",
    job_id: _activeJobId,
    worker_id: getWorkerId()
  });
  if (res && res.ok) {
    alert("任务已完成 / 작업 완료");
    clearActiveJob();
    goPage("home");
  } else {
    alert("失败/실패: " + (res ? res.error : "unknown"));
  }
}

async function refreshGenericWorkers() {
  if (!_activeJobId) return;
  var res = await api({ action: "v2_ops_job_detail", job_id: _activeJobId });
  if (res && res.ok) renderWorkers("genericWorkers", res.workers);
}

// ===== Task Interrupts =====
async function interruptToUnload() {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  if (!confirm("挂起当前任务，临时去卸货？\n현재 작업을 일시정지하고 임시 하차로 이동하시겠습니까?")) return;

  // Save parent info
  localStorage.setItem(V2_INTERRUPT_KEY, JSON.stringify({
    parent_job_id: _activeJobId,
    parent_page: _currentPage,
    parent_params: _pageParams
  }));

  // Start interrupt job
  var res = await api({
    action: "v2_ops_job_start",
    flow_stage: "unload",
    job_type: "unload",
    related_doc_type: "",
    related_doc_id: "",
    worker_id: getWorkerId(),
    worker_name: getWorkerName(),
    parent_job_id: _activeJobId,
    is_temporary_interrupt: true,
    interrupt_type: "unload"
  });

  if (res && res.ok) {
    saveActiveJob(res.job_id, res.worker_seg_id);
    _navStack = []; // Clear nav stack for clean return
    goPage("unload");
  } else {
    alert("失败/실패: " + (res ? res.error : "unknown"));
  }
}

async function interruptToLoad() {
  if (!_activeJobId) { alert("没有进行中的任务 / 진행 중인 작업 없음"); return; }
  if (!confirm("挂起当前任务，临时去装货？\n현재 작업을 일시정지하고 임시 상차로 이동하시겠습니까?")) return;

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
      showPage(saved.parent_page || "home");
      return true;
    }
  }
  localStorage.removeItem(V2_INTERRUPT_KEY);
  return false;
}

// ===== Photo Upload =====
function uploadPhoto(docType, category) {
  _photoUploadCtx = {
    related_doc_type: docType,
    attachment_category: category,
    related_doc_id: _activeJobId || _currentIssueId || ""
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

// ===== Init =====
window.addEventListener("DOMContentLoaded", function() {
  checkBadgeAuth();
});
