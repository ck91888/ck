/**
 * CK Warehouse V2 — Collab App (Collaboration Center)
 * Desktop+mobile, language-switchable (zh/ko), issue/outbound/inbound CRUD
 */

// ===== Action Lock — 防连点 =====
var _actionLocks = {};
function withActionLock(key, btnEl, pendingText, fn) {
  if (_actionLocks[key]) return;
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

// ===== State =====
var _currentTab = "home";
var _currentView = "home";
var _currentIssueId = null;
var _currentOutboundId = null;
var _currentInboundId = null;
var _uploadCtx = {};

// ===== API =====
function getKey() { try { return localStorage.getItem(V2_KEY_STORAGE) || ""; } catch(e) { return ""; } }
function setKey(k) { try { localStorage.setItem(V2_KEY_STORAGE, k); } catch(e) {} }
function getUser() { try { return localStorage.getItem(V2_USER_KEY) || ""; } catch(e) { return ""; } }
function setUser(u) { try { localStorage.setItem(V2_USER_KEY, u); } catch(e) {} }

// 写操作 action 集合 — 自动注入 client_req_id 供后端幂等
var _WRITE_ACTIONS = [
  'v2_issue_create','v2_issue_handle_start',
  'v2_outbound_order_create','v2_outbound_load_start',
  'v2_inbound_plan_create','v2_inbound_dynamic_finalize',
  'v2_unplanned_unload_start','v2_unplanned_unload_join',
  'v2_feedback_finalize_to_inbound','v2_unload_dynamic_start',
  'v2_unload_job_start','v2_inbound_job_start',
  'v2_inbound_mark_completed','v2_ops_job_start'
];
function _genReqId(action) {
  return action + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
}
async function api(params) {
  params.k = getKey();
  if (_WRITE_ACTIONS.indexOf(params.action) !== -1 && !params.client_req_id) {
    params.client_req_id = _genReqId(params.action);
  }
  try {
    var res = await fetch(V2_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params)
    });
    return await res.json();
  } catch(e) {
    return { ok: false, error: "network: " + e };
  }
}

async function uploadFile(formData) {
  formData.append("k", getKey());
  formData.append("action", "v2_attachment_upload");
  try {
    var res = await fetch(V2_API, { method: "POST", body: formData });
    return await res.json();
  } catch(e) {
    return { ok: false, error: "upload failed: " + e };
  }
}

function fileUrl(fileKey) {
  return V2_API + "/file?key=" + encodeURIComponent(fileKey);
}

function kstToday() {
  var d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.toISOString().slice(0, 10);
}

// ===== Login =====
function doLogin() {
  var k = document.getElementById("loginKey").value.trim();
  var errEl = document.getElementById("loginErr");
  if (!k) { errEl.textContent = "请输入访问码 / 액세스 코드를 입력하세요"; return; }
  errEl.textContent = "验证中... / 확인중...";
  errEl.style.color = "#666";
  setKey(k);
  // 用需要 auth 的接口验证 key 有效性（v2_issue_list 需要 ADMINKEY/VIEWKEY）
  fetch(V2_API, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "v2_issue_list", status: "pending", k: k })
  }).then(function(res) {
    if (!res.ok && res.status === 404) {
      // Worker 不存在或未部署
      errEl.style.color = "#e74c3c";
      errEl.textContent = "后端服务未部署 (HTTP " + res.status + ")\n서버가 배포되지 않았습니다";
      localStorage.removeItem(V2_KEY_STORAGE);
      return null;
    }
    return res.json();
  }).then(function(data) {
    if (!data) return; // already handled above
    if (data && data.ok) {
      errEl.textContent = "";
      showMain();
    } else if (data && data.error === "unauthorized") {
      errEl.style.color = "#e74c3c";
      errEl.textContent = "访问码错误 / 액세스 코드 오류";
      localStorage.removeItem(V2_KEY_STORAGE);
    } else {
      errEl.style.color = "#e74c3c";
      errEl.textContent = "服务异常: " + (data ? data.error : "unknown") + "\n서버 오류";
      localStorage.removeItem(V2_KEY_STORAGE);
    }
  }).catch(function(e) {
    errEl.style.color = "#e74c3c";
    errEl.textContent = "网络异常 / 네트워크 오류: " + e.message;
    localStorage.removeItem(V2_KEY_STORAGE);
  });
}

function doLogout() {
  localStorage.removeItem(V2_KEY_STORAGE);
  document.getElementById("page-main").classList.remove("active");
  document.getElementById("page-login").classList.add("active");
}

function showMain() {
  document.getElementById("page-login").classList.remove("active");
  document.getElementById("page-main").classList.add("active");
  var userName = getUser();
  document.getElementById("userBadge").textContent = userName || "(未设置)";
  applyLang();
  goTab("home");
  // 首次进入且未设置显示名时，提示设置
  if (!userName) {
    setTimeout(promptUserName, 500);
  }
}

function promptUserName() {
  var current = getUser();
  var name = prompt(
    getLang() === "ko"
      ? "표시 이름을 입력하세요 (작업 기록에 사용됩니다):"
      : "请输入你的显示名（用于操作记录）:",
    current || ""
  );
  if (name !== null) {
    name = name.trim();
    if (name) {
      setUser(name);
      document.getElementById("userBadge").textContent = name;
    }
  }
}

function checkAutoLogin() {
  if (getKey()) {
    // 有 key，先快速验证还是否有效
    fetch(V2_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "v2_issue_list", status: "pending", k: getKey() })
    }).then(function(res) { return res.json(); })
      .then(function(data) {
        if (data && data.ok) {
          showMain();
        } else {
          // key 已失效，回到登录页
          localStorage.removeItem(V2_KEY_STORAGE);
          var errEl = document.getElementById("loginErr");
          if (errEl) {
            errEl.style.color = "#e74c3c";
            errEl.textContent = "访问码已失效，请重新输入 / 액세스 코드가 만료되었습니다";
          }
        }
      }).catch(function() {
        // 网络异常，仍然尝试进入（离线容忍）
        showMain();
      });
  }
}

// ===== Language =====
function toggleLang() {
  var cur = getLang();
  setLang(cur === "zh" ? "ko" : "zh");
  applyLang();
  // Refresh current view
  if (_currentTab === "home") loadDashboard();
  if (_currentTab === "issue") loadIssueList();
  if (_currentTab === "outbound") loadOutboundList();
  if (_currentTab === "inbound") loadInboundList();
}

function applyLang() {
  var lang = getLang();
  document.getElementById("mainTitle").textContent = L("app_title");
  document.getElementById("mainSub").textContent = L("app_subtitle");
  document.getElementById("langBtn").textContent = L("lang_switch");
  document.getElementById("logoutBtn").textContent = L("logout");
  document.getElementById("loginTitle").textContent = L("login_title");
  document.getElementById("loginBtn").textContent = L("login_btn");
  document.getElementById("loginKey").placeholder = L("login_placeholder");
  document.getElementById("btnNewIssue").textContent = L("new_issue");
  document.getElementById("btnNewOutbound").textContent = L("new_outbound");
  document.getElementById("btnNewInbound").textContent = L("new_inbound");

  // Tabs
  var tabMap = { home: "tab_home", issue: "tab_issue", outbound: "tab_outbound",
    inbound: "tab_inbound", feedback: "tab_feedback", check: "tab_check" };
  var tabs = document.querySelectorAll("#mainTabs button");
  tabs.forEach(function(btn) {
    var key = btn.getAttribute("data-tab");
    if (key && tabMap[key]) btn.textContent = L(tabMap[key]);
  });
}

// ===== Status/Biz helpers =====
function stLabel(status) {
  return L("status_" + status) || status;
}

function feedbackTitleText(fb) {
  var title = fb.title || "";
  var dn = fb.display_no || "";
  // Strip duplicate display_no prefix from title (legacy data compat)
  if (dn && title.indexOf(dn) === 0) {
    title = title.slice(dn.length).replace(/^\s+/, "");
  }
  return title || "(无标题)";
}

function inboundStatusLabel(status) {
  var map = {pending:'待到库',unloading:'卸货中',arrived_pending_putaway:'已到库待入库',putting_away:'入库中',completed:'已入库',cancelled:'已取消'};
  return map[status] || stLabel(status);
}

function bizLabel(biz) {
  return L("biz_" + biz) || biz;
}

function priLabel(pri) {
  return L("priority_" + pri) || pri;
}

function fmtTime(isoStr) {
  if (!isoStr) return "--";
  try {
    var d = new Date(isoStr);
    var m = d.getMonth() + 1;
    var day = d.getDate();
    var h = d.getHours();
    var min = d.getMinutes();
    return m + "-" + day + " " + (h < 10 ? "0" : "") + h + ":" + (min < 10 ? "0" : "") + min;
  } catch(e) { return String(isoStr); }
}

// ===== Navigation =====
function goTab(tab, btn) {
  _currentTab = tab;
  // Highlight tab
  var tabs = document.querySelectorAll("#mainTabs button");
  tabs.forEach(function(b) { b.classList.remove("active"); });
  if (btn) btn.classList.add("active");
  else {
    tabs.forEach(function(b) { if (b.getAttribute("data-tab") === tab) b.classList.add("active"); });
  }

  // Show corresponding view
  goView(tab);

  // Load data
  if (tab === "home") loadDashboard();
  if (tab === "issue") loadIssueList();
  if (tab === "outbound") loadOutboundList();
  if (tab === "inbound") loadInboundList();
  if (tab === "feedback") loadFeedbackList();
  if (tab === "order_ops") loadOrderOpsList();
}

function goView(name) {
  _currentView = name;
  var views = document.querySelectorAll(".view");
  views.forEach(function(v) { v.style.display = "none"; });
  var el = document.getElementById("view-" + name);
  if (el) el.style.display = "";
}

// ===== Dashboard =====
async function loadDashboard() {
  var grid = document.getElementById("dashGrid");
  if (!grid) return;
  grid.innerHTML = '<div class="dash-card"><span class="muted">' + L("loading") + '</span></div>';

  // Parallel fetch
  var [issues, outbounds, inbounds, upcoming, feedbacks] = await Promise.all([
    api({ action: "v2_issue_list", status: "" }),
    api({ action: "v2_outbound_order_list", start_date: "", end_date: "" }),
    api({ action: "v2_inbound_plan_list", start_date: "", end_date: "" }),
    api({ action: "v2_inbound_plan_list_upcoming" }),
    api({ action: "v2_feedback_list", feedback_type: "", status: "" })
  ]);

  var issueItems = (issues && issues.items) || [];
  var obItems = (outbounds && outbounds.items) || [];
  var ibItems = ((inbounds && inbounds.items) || []).filter(function(p) { return p.source_type !== 'return_session'; });

  var pendingIssues = issueItems.filter(function(i) { return i.status === "pending" || i.status === "processing"; });
  var pendingOb = obItems.filter(function(o) { return o.status === "draft" || o.status === "issued" || o.status === "working"; });
  var pendingIb = ibItems.filter(function(p) { return p.status === "pending" || p.status === "unloading" || p.status === "arrived_pending_putaway" || p.status === "putting_away"; });
  var fbItems = (feedbacks && feedbacks.items) || [];
  var activeFb = fbItems.filter(function(f) { return f.status === "field_working" || f.status === "unloaded_pending_info"; });

  var html = '';

  // Issue card
  html += '<div class="dash-card" onclick="goTab(\'issue\')">';
  html += '<div class="d-title">⚠️ ' + L("today_issues") + '</div>';
  if (pendingIssues.length > 0) {
    html += '<div class="d-count">' + pendingIssues.length + '</div>';
    pendingIssues.slice(0, 3).forEach(function(i) {
      html += '<div style="font-size:12px;padding:2px 0;">' +
        '<span class="st st-' + esc(i.status) + '">' + esc(stLabel(i.status)) + '</span> ' +
        esc(i.issue_summary || i.customer || "--") + '</div>';
    });
  } else {
    html += '<div class="d-empty">' + L("no_data") + '</div>';
  }
  html += '</div>';

  // Outbound card
  html += '<div class="dash-card" onclick="goTab(\'outbound\')">';
  html += '<div class="d-title">🚚 ' + L("today_outbound") + '</div>';
  if (pendingOb.length > 0) {
    html += '<div class="d-count">' + pendingOb.length + '</div>';
    pendingOb.slice(0, 3).forEach(function(o) {
      html += '<div style="font-size:12px;padding:2px 0;">' +
        '<span class="st st-' + esc(o.status) + '">' + esc(stLabel(o.status)) + '</span> ' +
        esc(o.customer || "--") + ' ' + esc(o.order_date || "") + '</div>';
    });
  } else {
    html += '<div class="d-empty">' + L("no_data") + '</div>';
  }
  html += '</div>';

  // Inbound card
  html += '<div class="dash-card" onclick="goTab(\'inbound\')">';
  html += '<div class="d-title">📦 ' + L("today_inbound") + '</div>';
  if (pendingIb.length > 0) {
    html += '<div class="d-count">' + pendingIb.length + '</div>';
    pendingIb.slice(0, 3).forEach(function(p) {
      html += '<div style="font-size:12px;padding:2px 0;">' +
        '<span class="st st-' + esc(p.status) + '">' + esc(inboundStatusLabel(p.status)) + '</span> ' +
        esc(p.customer || "--") + ' ' + esc(p.cargo_summary || "") + '</div>';
    });
  } else {
    html += '<div class="d-empty">' + L("no_data") + '</div>';
  }
  html += '</div>';

  // Feedback card
  var fbStMap = {field_working:'现场卸货中',unloaded_pending_info:'已卸货待补充信息',converted:'已转正'};
  html += '<div class="dash-card" onclick="goTab(\'feedback\')">';
  html += '<div class="d-title">💬 ' + L("today_feedback") + '</div>';
  if (activeFb.length > 0) {
    html += '<div class="d-count">' + activeFb.length + '</div>';
    activeFb.slice(0, 3).forEach(function(f) {
      html += '<div style="font-size:12px;padding:2px 0;">' +
        '<span class="st st-' + esc(f.status) + '">' + esc(fbStMap[f.status] || stLabel(f.status)) + '</span> ' +
        '[' + esc(f.display_no || f.id) + '] ' + esc(feedbackTitleText(f)) + '</div>';
    });
  } else {
    html += '<div class="d-empty">' + L("no_data") + '</div>';
  }
  html += '</div>';

  // Upcoming inbound card (next 3 working days)
  var upcomingItems = ((upcoming && upcoming.items) || []).filter(function(p) { return p.source_type !== 'return_session'; });
  var upcomingDates = (upcoming && upcoming.dates) || [];
  html += '<div class="dash-card" onclick="goTab(\'inbound\')">';
  html += '<div class="d-title">📅 ' + L("upcoming_inbound") + '</div>';
  if (upcomingItems.length > 0) {
    html += '<div class="d-count">' + upcomingItems.length + '</div>';
    // Group by date
    var byDate = {};
    upcomingItems.forEach(function(p) {
      var d = p.plan_date || "?";
      if (!byDate[d]) byDate[d] = [];
      byDate[d].push(p);
    });
    upcomingDates.forEach(function(d) {
      if (!byDate[d]) return;
      html += '<div style="font-size:11px;font-weight:700;margin-top:4px;">' + esc(d) + '</div>';
      byDate[d].forEach(function(p) {
        html += '<div style="font-size:12px;padding:2px 0;">' +
          '<span class="st st-' + esc(p.status) + '">' + esc(inboundStatusLabel(p.status)) + '</span> ' +
          esc(p.customer || "--") + ' ' + esc(p.cargo_summary || "") + '</div>';
      });
    });
  } else {
    html += '<div class="d-empty">' + L("no_data") + '</div>';
  }
  html += '</div>';

  grid.innerHTML = html;
}

// ===== Issue List =====
async function loadIssueList() {
  var body = document.getElementById("issueListBody");
  if (!body) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var status = document.getElementById("issueFilterStatus").value;
  var biz = document.getElementById("issueFilterBiz").value;
  var res = await api({ action: "v2_issue_list", status: status, biz_class: biz });

  if (!res || !res.ok) {
    body.innerHTML = '<div class="card muted">加载失败</div>';
    return;
  }

  var items = res.items || [];
  if (items.length === 0) {
    body.innerHTML = '<div class="card muted">' + L("no_data") + '</div>';
    return;
  }

  var html = '<div class="card">';
  items.forEach(function(it) {
    html += '<div class="list-item" onclick="openIssueDetail(\'' + esc(it.id) + '\')">';
    html += '<div class="item-title">';
    html += '<span class="st st-' + esc(it.status) + '">' + esc(stLabel(it.status)) + '</span> ';
    html += '<span class="biz-tag biz-' + esc(it.biz_class) + '">' + esc(bizLabel(it.biz_class)) + '</span> ';
    if (it.priority === "urgent" || it.priority === "high") {
      html += '<span class="priority-' + esc(it.priority) + '">' + esc(priLabel(it.priority)) + '</span> ';
    }
    html += esc(it.issue_summary || "(无摘要)");
    html += '</div>';
    html += '<div class="item-meta">';
    html += esc(it.customer || "") + ' · ' + esc(it.issue_type || "") + ' · ' + esc(it.submitted_by || "") + ' · ' + esc(fmtTime(it.created_at));
    if (it.total_minutes_worked > 0) html += ' · ' + it.total_minutes_worked.toFixed(1) + L("minutes");
    html += '</div>';
    html += '</div>';
  });
  html += '</div>';
  body.innerHTML = html;
}

// ===== Issue Create =====
async function submitIssue(btnEl) {
  var summary = document.getElementById("ic-summary").value.trim();
  if (!summary) { alert("请填写问题摘要"); return; }
  withActionLock('submitIssue', btnEl || null, '提交中.../저장중...', async function() {
    var biz = document.getElementById("ic-biz").value;
    var customer = document.getElementById("ic-customer").value.trim();
    var docno = document.getElementById("ic-docno").value.trim();
    var itype = document.getElementById("ic-type").value.trim();
    var desc = document.getElementById("ic-desc").value.trim();
    var priority = document.getElementById("ic-priority").value;

    var res = await api({
      action: "v2_issue_create",
      biz_class: biz,
      customer: customer,
      related_doc_no: docno,
      issue_type: itype,
      issue_summary: summary,
      issue_description: desc,
      priority: priority,
      submitted_by: getUser()
    });

    if (res && res.ok) {
      alert("已创建: " + res.id);
      document.getElementById("ic-customer").value = "";
      document.getElementById("ic-docno").value = "";
      document.getElementById("ic-type").value = "";
      document.getElementById("ic-summary").value = "";
      document.getElementById("ic-desc").value = "";
      goTab("issue");
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

// ===== Issue Detail =====
function openIssueDetail(id) {
  _currentIssueId = id;
  goView("issue_detail");
  loadIssueDetail();
}

async function loadIssueDetail() {
  var body = document.getElementById("issueDetailBody");
  if (!body || !_currentIssueId) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var res = await api({ action: "v2_issue_detail", id: _currentIssueId });
  if (!res || !res.ok || !res.issue) {
    body.innerHTML = '<div class="card muted">加载失败</div>';
    return;
  }

  var it = res.issue;
  var runs = res.handle_runs || [];
  var atts = res.attachments || [];

  var html = '<div class="card">';
  html += '<div style="font-size:18px;font-weight:700;margin-bottom:8px;">' + esc(it.issue_summary) + '</div>';
  html += '<div class="detail-field"><b>' + L("status") + ':</b> <span class="st st-' + esc(it.status) + '">' + esc(stLabel(it.status)) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("biz_class") + ':</b> <span class="biz-tag biz-' + esc(it.biz_class) + '">' + esc(bizLabel(it.biz_class)) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("priority") + ':</b> <span class="priority-' + esc(it.priority) + '">' + esc(priLabel(it.priority)) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("customer") + ':</b> ' + esc(it.customer) + '</div>';
  html += '<div class="detail-field"><b>' + L("related_doc_no") + ':</b> ' + esc(it.related_doc_no) + '</div>';
  html += '<div class="detail-field"><b>' + L("issue_type") + ':</b> ' + esc(it.issue_type) + '</div>';
  html += '<div class="detail-field"><b>' + L("submitted_by") + ':</b> ' + esc(it.submitted_by) + '</div>';
  html += '<div class="detail-field"><b>' + L("created_at") + ':</b> ' + esc(fmtTime(it.created_at)) + '</div>';
  html += '<div class="detail-section"><b>' + L("issue_description") + ':</b>';
  html += '<div style="margin-top:4px;white-space:pre-wrap;">' + esc(it.issue_description) + '</div></div>';

  // Latest feedback
  if (it.latest_feedback_text) {
    html += '<div class="detail-section"><b>' + L("latest_feedback") + ':</b>';
    html += '<div style="margin-top:4px;white-space:pre-wrap;color:#1565c0;">' + esc(it.latest_feedback_text) + '</div></div>';
  }

  // Work time
  if (it.total_minutes_worked > 0) {
    html += '<div class="detail-field"><b>' + L("total_work_time") + ':</b> ' + it.total_minutes_worked.toFixed(1) + ' ' + L("minutes") + '</div>';
  }
  html += '</div>';

  // Attachments
  if (atts.length > 0) {
    html += '<div class="card"><div class="card-title">' + L("attachments") + ' (' + atts.length + ')</div>';
    html += '<div class="att-grid">';
    atts.forEach(function(att) {
      if (att.content_type && att.content_type.startsWith("image/")) {
        html += '<img class="att-thumb" src="' + esc(fileUrl(att.file_key)) + '" onclick="showLightbox(\'' + esc(fileUrl(att.file_key)) + '\')">';
      } else {
        html += '<div style="font-size:12px;">' + esc(att.file_name) + '</div>';
      }
    });
    html += '</div></div>';
  }

  // Handle runs
  if (runs.length > 0) {
    html += '<div class="card"><div class="card-title">' + L("handle_history") + '</div>';
    runs.forEach(function(r) {
      html += '<div style="border-bottom:1px solid #f0f0f0;padding:8px 0;font-size:13px;">';
      html += '<div><b>' + esc(r.handler_name || r.handler_id) + '</b> · ';
      html += '<span class="st st-' + esc(r.run_status) + '">' + esc(r.run_status) + '</span></div>';
      html += '<div class="muted">' + esc(fmtTime(r.started_at)) + ' → ' + esc(fmtTime(r.ended_at));
      if (r.minutes_worked) html += ' (' + r.minutes_worked.toFixed(1) + ' ' + L("minutes") + ')';
      html += '</div>';
      if (r.feedback_text) html += '<div style="margin-top:4px;color:#1565c0;">' + esc(r.feedback_text) + '</div>';
      html += '</div>';
    });
    html += '</div>';
  }

  // Actions
  if (it.status !== "closed" && it.status !== "cancelled") {
    html += '<div class="card">';
    if (it.status === "responded" || it.status === "pending" || it.status === "processing") {
      html += '<button class="btn btn-success" onclick="closeIssue(this)">' + L("close_issue") + '</button> ';
    }
    html += '<button class="btn btn-danger" onclick="cancelIssue(this)">' + L("cancel_issue") + '</button>';
    html += '<div style="margin-top:10px;"><label>' + L("attachments") + '</label>';
    html += '<div class="att-grid" id="issueDetailAtts">';
    html += '<div class="att-upload" onclick="doUpload(\'issue_ticket\',\'' + esc(it.id) + '\',\'issue_photo\')">+</div>';
    html += '</div></div>';
    html += '</div>';
  }

  body.innerHTML = html;
}

async function closeIssue(btnEl) {
  if (!confirm(L("confirm") + "?")) return;
  withActionLock('closeIssue', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({ action: "v2_issue_close", id: _currentIssueId });
    if (res && res.ok) {
      alert(L("status_closed"));
      loadIssueDetail();
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

async function cancelIssue(btnEl) {
  if (!confirm(L("confirm") + "?")) return;
  withActionLock('cancelIssue', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({ action: "v2_issue_cancel", id: _currentIssueId });
    if (res && res.ok) {
      alert(L("status_cancelled"));
      loadIssueDetail();
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

// ===== Outbound List =====
async function loadOutboundList() {
  var body = document.getElementById("outboundListBody");
  if (!body) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var start = document.getElementById("obFilterStart").value;
  var end = document.getElementById("obFilterEnd").value;
  var status = document.getElementById("obFilterStatus").value;

  var res = await api({ action: "v2_outbound_order_list", start_date: start, end_date: end, status: status });
  if (!res || !res.ok) {
    body.innerHTML = '<div class="card muted">加载失败</div>';
    return;
  }

  var items = res.items || [];
  if (items.length === 0) {
    body.innerHTML = '<div class="card muted">' + L("no_data") + '</div>';
    return;
  }

  var html = '<div class="card">';
  items.forEach(function(o) {
    html += '<div class="list-item" onclick="openOutboundDetail(\'' + esc(o.id) + '\')">';
    html += '<div class="item-title">';
    html += '<span class="st st-' + esc(o.status) + '">' + esc(stLabel(o.status)) + '</span> ';
    html += '<span class="biz-tag biz-' + esc(o.biz_class) + '">' + esc(bizLabel(o.biz_class)) + '</span> ';
    html += esc(o.customer || "--");
    html += '</div>';
    html += '<div class="item-meta">' + esc(o.order_date || "") + ' · ' + esc(o.operation_mode || "") + ' · ' + esc(o.outbound_mode || "") + ' · ' + esc(fmtTime(o.created_at)) + '</div>';
    html += '</div>';
  });
  html += '</div>';
  body.innerHTML = html;
}

// ===== Outbound Create =====
var _obLineCount = 0;

function addOutboundLine() {
  _obLineCount++;
  var tbody = document.getElementById("ocLinesBody");
  var tr = document.createElement("tr");
  tr.id = "oc-line-" + _obLineCount;
  tr.innerHTML = '<td><input type="text" id="ocl-wms-' + _obLineCount + '"></td>' +
    '<td><input type="text" id="ocl-sku-' + _obLineCount + '"></td>' +
    '<td><input type="number" id="ocl-qty-' + _obLineCount + '" value="0"></td>' +
    '<td><button class="btn btn-outline btn-sm" onclick="this.parentElement.parentElement.remove()">×</button></td>';
  tbody.appendChild(tr);
}

async function submitOutbound(btnEl) {
  var customer = document.getElementById("oc-customer").value.trim();
  if (!customer) { alert("请填写客户名"); return; }
  withActionLock('submitOutbound', btnEl || null, '提交中.../저장중...', async function() {
    var date = document.getElementById("oc-date").value || kstToday();
    var biz = document.getElementById("oc-biz").value;
    var opmode = document.getElementById("oc-opmode").value.trim();
    var outmode = document.getElementById("oc-outmode").value.trim();
    var instruction = document.getElementById("oc-instruction").value.trim();
    var remark = document.getElementById("oc-remark").value.trim();

    // Collect lines
    var lines = [];
    var rows = document.getElementById("ocLinesBody").querySelectorAll("tr");
    rows.forEach(function(tr) {
      var id = tr.id.replace("oc-line-", "");
      var wms = (document.getElementById("ocl-wms-" + id) || {}).value || "";
      var sku = (document.getElementById("ocl-sku-" + id) || {}).value || "";
      var qty = parseInt((document.getElementById("ocl-qty-" + id) || {}).value) || 0;
      if (wms || sku || qty > 0) lines.push({ wms_order_no: wms, sku: sku, quantity: qty });
    });

    var res = await api({
      action: "v2_outbound_order_create",
      order_date: date,
      customer: customer,
      biz_class: biz,
      operation_mode: opmode,
      outbound_mode: outmode,
      instruction: instruction,
      remark: remark,
      created_by: getUser(),
      lines: lines
    });

    if (res && res.ok) {
      alert("已创建: " + res.id);
      document.getElementById("oc-customer").value = "";
      document.getElementById("oc-opmode").value = "";
      document.getElementById("oc-outmode").value = "";
      document.getElementById("oc-instruction").value = "";
      document.getElementById("oc-remark").value = "";
      document.getElementById("ocLinesBody").innerHTML = "";
      _obLineCount = 0;
      goTab("outbound");
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

// ===== Outbound Detail =====
function openOutboundDetail(id) {
  _currentOutboundId = id;
  goView("outbound_detail");
  loadOutboundDetail();
}

async function loadOutboundDetail() {
  var body = document.getElementById("outboundDetailBody");
  if (!body || !_currentOutboundId) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var res = await api({ action: "v2_outbound_order_detail", id: _currentOutboundId });
  if (!res || !res.ok || !res.order) {
    body.innerHTML = '<div class="card muted">加载失败</div>';
    return;
  }

  var o = res.order;
  var lines = res.lines || [];
  var jobs = res.jobs || [];
  var atts = res.attachments || [];

  var html = '<div class="card">';
  html += '<div style="font-size:16px;font-weight:700;margin-bottom:8px;">' + esc(o.id) + '</div>';
  html += '<div class="detail-field"><b>' + L("status") + ':</b> <span class="st st-' + esc(o.status) + '">' + esc(stLabel(o.status)) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("order_date") + ':</b> ' + esc(o.order_date) + '</div>';
  html += '<div class="detail-field"><b>' + L("customer") + ':</b> ' + esc(o.customer) + '</div>';
  html += '<div class="detail-field"><b>' + L("biz_class") + ':</b> <span class="biz-tag biz-' + esc(o.biz_class) + '">' + esc(bizLabel(o.biz_class)) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("operation_mode") + ':</b> ' + esc(o.operation_mode) + '</div>';
  html += '<div class="detail-field"><b>' + L("outbound_mode") + ':</b> ' + esc(o.outbound_mode) + '</div>';
  if (o.instruction) html += '<div class="detail-section"><b>' + L("instruction") + ':</b><div style="margin-top:4px;white-space:pre-wrap;">' + esc(o.instruction) + '</div></div>';
  if (o.remark) html += '<div class="detail-field"><b>' + L("remark") + ':</b> ' + esc(o.remark) + '</div>';
  html += '<div class="detail-field"><b>' + L("submitted_by") + ':</b> ' + esc(o.created_by) + ' · ' + esc(fmtTime(o.created_at)) + '</div>';
  html += '</div>';

  // Lines
  if (lines.length > 0) {
    html += '<div class="card"><div class="card-title">明细行 (' + lines.length + ')</div>';
    html += '<table class="line-table"><thead><tr><th>' + L("wms_order_no") + '</th><th>' + L("sku") + '</th><th>' + L("quantity") + '</th></tr></thead><tbody>';
    lines.forEach(function(ln) {
      html += '<tr><td>' + esc(ln.wms_order_no) + '</td><td>' + esc(ln.sku) + '</td><td>' + (ln.quantity || 0) + '</td></tr>';
    });
    html += '</tbody></table></div>';
  }

  // Jobs (field execution)
  if (jobs.length > 0) {
    html += '<div class="card"><div class="card-title">现场执行记录</div>';
    jobs.forEach(function(j) {
      var result = {};
      try { result = JSON.parse(j.shared_result_json || "{}"); } catch(e) {}
      html += '<div style="border-bottom:1px solid #f0f0f0;padding:8px 0;font-size:13px;">';
      html += '<span class="st st-' + esc(j.status) + '">' + esc(stLabel(j.status)) + '</span> ';
      html += esc(j.job_type) + ' · ' + esc(fmtTime(j.created_at));
      if (result.box_count) html += ' · 箱:' + result.box_count;
      if (result.pallet_count) html += ' · 托:' + result.pallet_count;
      if (result.remark) html += ' · ' + esc(result.remark);
      html += '</div>';
    });
    html += '</div>';
  }

  // Attachments
  if (atts.length > 0) {
    html += '<div class="card"><div class="card-title">' + L("attachments") + ' (' + atts.length + ')</div>';
    html += '<div class="att-grid">';
    atts.forEach(function(att) {
      if (att.content_type && att.content_type.startsWith("image/")) {
        html += '<img class="att-thumb" src="' + esc(fileUrl(att.file_key)) + '" onclick="showLightbox(\'' + esc(fileUrl(att.file_key)) + '\')">';
      } else {
        html += '<div style="font-size:12px;">' + esc(att.file_name) + '</div>';
      }
    });
    html += '</div></div>';
  }

  // Status actions
  if (o.status !== "completed" && o.status !== "cancelled") {
    html += '<div class="card">';
    if (o.status === "draft") {
      html += '<button class="btn btn-primary" onclick="updateObStatus(\'issued\', this)">' + L("status_issued") + '</button> ';
    }
    if (o.status === "issued" || o.status === "working") {
      html += '<button class="btn btn-success" onclick="updateObStatus(\'completed\', this)">' + L("status_completed") + '</button> ';
    }
    html += '<button class="btn btn-danger" onclick="updateObStatus(\'cancelled\', this)">' + L("status_cancelled") + '</button>';
    html += '</div>';
  }

  body.innerHTML = html;
}

async function updateObStatus(status, btnEl) {
  if (!confirm(L("confirm") + "?")) return;
  withActionLock('updateObStatus_' + status, btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({ action: "v2_outbound_order_update_status", id: _currentOutboundId, status: status });
    if (res && res.ok) {
      loadOutboundDetail();
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

// ===== Inbound List =====
async function loadInboundList() {
  var body = document.getElementById("inboundListBody");
  if (!body) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var start = document.getElementById("ibFilterStart").value;
  var end = document.getElementById("ibFilterEnd").value;
  var status = document.getElementById("ibFilterStatus").value;

  var res = await api({ action: "v2_inbound_plan_list", start_date: start, end_date: end, status: status });
  if (!res || !res.ok) {
    body.innerHTML = '<div class="card muted">加载失败</div>';
    return;
  }

  var items = (res.items || []).filter(function(p) { return p.source_type !== 'return_session'; });
  if (items.length === 0) {
    body.innerHTML = '<div class="card muted">' + L("no_data") + '</div>';
    return;
  }

  var html = '<div class="card">';
  items.forEach(function(p) {
    html += '<div class="list-item" onclick="openInboundDetail(\'' + esc(p.id) + '\')">';
    html += '<div class="item-title">';
    html += '<span class="st st-' + esc(p.status) + '">' + esc(inboundStatusLabel(p.status)) + '</span> ';
    html += '<span class="biz-tag biz-' + esc(p.biz_class) + '">' + esc(bizLabel(p.biz_class)) + '</span> ';
    html += esc(p.display_no || p.id) + ' · ' + esc(p.customer || "--") + ' · ' + esc(p.cargo_summary || "");
    html += '</div>';
    html += '<div class="item-meta">' + esc(p.plan_date || "") + ' · ' + esc(p.expected_arrival || "") + ' · ' + esc(fmtTime(p.created_at)) + '</div>';
    html += '</div>';
  });
  html += '</div>';
  body.innerHTML = html;
}

// ===== Inbound Create =====
// Plan line helpers
function addIbcLine() {
  var tbody = document.getElementById("ibcLinesBody");
  var tr = document.createElement("tr");
  tr.innerHTML = '<td><select class="ibc-line-type">' +
    '<option value="container_large">' + L("unit_container_large") + '</option>' +
    '<option value="container_small">' + L("unit_container_small") + '</option>' +
    '<option value="pallet">' + L("unit_pallet") + '</option>' +
    '<option value="carton">' + L("unit_carton") + '</option>' +
    '<option value="cbm">' + L("unit_cbm") + '</option>' +
    '</select></td>' +
    '<td><input type="number" class="ibc-line-qty" value="0" min="0" step="0.1" style="width:70px;"></td>' +
    '<td><input type="text" class="ibc-line-remark" style="width:100px;"></td>' +
    '<td><button class="btn btn-outline btn-sm" onclick="this.closest(\'tr\').remove()">×</button></td>';
  tbody.appendChild(tr);
}

function getIbcLines() {
  var rows = document.querySelectorAll("#ibcLinesBody tr");
  var lines = [];
  for (var i = 0; i < rows.length; i++) {
    var type = rows[i].querySelector(".ibc-line-type").value;
    var qty = parseFloat(rows[i].querySelector(".ibc-line-qty").value) || 0;
    var remark = rows[i].querySelector(".ibc-line-remark").value.trim();
    if (qty > 0) lines.push({ unit_type: type, planned_qty: qty, remark: remark });
  }
  return lines;
}

function toggleIbcAutoOb() {
  var checked = document.getElementById("ibc-auto-ob").checked;
  document.getElementById("ibcAutoObFields").style.display = checked ? "" : "none";
}

async function submitInbound(btnEl) {
  var customer = document.getElementById("ibc-customer").value.trim();
  if (!customer) { alert(L("customer") + " " + L("required") + "!"); return; }
  withActionLock('submitInbound', btnEl || null, '提交中.../저장중...', async function() {
    var date = document.getElementById("ibc-date").value || kstToday();
    var biz = document.getElementById("ibc-biz").value;
    var cargo = document.getElementById("ibc-cargo").value.trim();
    var arrival = document.getElementById("ibc-arrival").value.trim();
    var purpose = document.getElementById("ibc-purpose").value.trim();
    var remark = document.getElementById("ibc-remark").value.trim();
    var lines = getIbcLines();
    var autoOb = document.getElementById("ibc-auto-ob").checked;

    var payload = {
      action: "v2_inbound_plan_create",
      plan_date: date,
      customer: customer,
      biz_class: biz,
      cargo_summary: cargo,
      expected_arrival: arrival,
      purpose: purpose,
      remark: remark,
      lines: lines,
      created_by: getUser()
    };

    if (autoOb) {
      payload.auto_create_outbound = true;
      payload.ob_operation_mode = (document.getElementById("ibc-ob-opmode") || {}).value || "";
      payload.ob_outbound_mode = (document.getElementById("ibc-ob-outmode") || {}).value || "";
      payload.ob_instruction = (document.getElementById("ibc-ob-instruction") || {}).value || "";
    }

    var res = await api(payload);

    if (res && res.ok) {
      var msg = L("success") + ": " + res.id;
      if (res.outbound_id) msg += "\n" + L("auto_create_outbound") + ": " + res.outbound_id;
      alert(msg);
      document.getElementById("ibc-customer").value = "";
      document.getElementById("ibc-cargo").value = "";
      document.getElementById("ibc-arrival").value = "";
      document.getElementById("ibc-purpose").value = "";
      document.getElementById("ibc-remark").value = "";
      document.getElementById("ibcLinesBody").innerHTML = "";
      document.getElementById("ibc-auto-ob").checked = false;
      document.getElementById("ibcAutoObFields").style.display = "none";
      goTab("inbound");
    } else {
      alert(L("error") + ": " + (res ? res.error : "unknown"));
    }
  });
}

// ===== Inbound Detail =====
function openInboundDetail(id) {
  _currentInboundId = id;
  goView("inbound_detail");
  loadInboundDetail();
}

var UNIT_TYPE_LABELS = {
  container_large: "unit_container_large",
  container_small: "unit_container_small",
  pallet: "unit_pallet",
  carton: "unit_carton",
  cbm: "unit_cbm"
};

function unitTypeLabel(key) {
  return L(UNIT_TYPE_LABELS[key] || key);
}

async function loadInboundDetail() {
  var body = document.getElementById("inboundDetailBody");
  if (!body || !_currentInboundId) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var res = await api({ action: "v2_inbound_plan_detail", id: _currentInboundId });
  if (!res || !res.ok || !res.plan) {
    body.innerHTML = '<div class="card muted">' + L("error") + '</div>';
    return;
  }

  var p = res.plan;
  var lines = res.lines || [];
  var jobs = res.jobs || [];
  var atts = res.attachments || [];

  // --- Basic info: two-column grid ---
  var isDynamic = (p.source_type === 'field_dynamic');
  var isFromFeedback = (p.source_type === 'from_feedback');
  var isExternal = (p.source_type === 'external_inbound');
  var isReturnSession = (p.source_type === 'return_session');
  var html = '<div class="card">';
  html += '<div style="font-size:16px;font-weight:700;margin-bottom:10px;">';
  if (isFromFeedback) html += '<span style="background:#8e24aa;color:#fff;font-size:11px;padding:2px 6px;border-radius:3px;margin-right:6px;">反馈转正</span>';
  else if (isDynamic) html += '<span style="background:#ff9800;color:#fff;font-size:11px;padding:2px 6px;border-radius:3px;margin-right:6px;">旧动态单</span>';
  else if (isExternal) html += '<span style="background:#2196f3;color:#fff;font-size:11px;padding:2px 6px;border-radius:3px;margin-right:6px;">外部WMS入库</span>';
  else if (isReturnSession) html += '<span style="background:#f39c12;color:#fff;font-size:11px;padding:2px 6px;border-radius:3px;margin-right:6px;">退件入库会话</span>';
  html += esc(p.display_no || p.id) + '</div>';
  if (isExternal && p.external_inbound_no) {
    html += '<div style="font-size:12px;color:#1565c0;margin-bottom:8px;">外部 WMS 入库单号：<b>' + esc(p.external_inbound_no) + '</b></div>';
  }
  if (isReturnSession) {
    html += '<div style="background:#fff8e1;border-left:3px solid #f39c12;padding:6px 10px;margin-bottom:8px;font-size:12px;">退件入库 · 仅记录工时 · 入库数量由外部 WMS 数据对账</div>';
  }
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 16px;font-size:13px;">';
  html += '<div><b>' + L("status") + ':</b> <span class="st st-' + esc(p.status) + '">' + esc(inboundStatusLabel(p.status)) + '</span></div>';
  html += '<div><b>' + L("biz_class") + ':</b> <span class="biz-tag biz-' + esc(p.biz_class) + '">' + esc(bizLabel(p.biz_class)) + '</span></div>';
  html += '<div><b>' + L("plan_date") + ':</b> ' + esc(p.plan_date) + '</div>';
  html += '<div><b>' + L("customer") + ':</b> ' + esc(p.customer) + '</div>';
  html += '<div><b>' + L("cargo_summary") + ':</b> ' + esc(p.cargo_summary) + '</div>';
  html += '<div><b>' + L("expected_arrival") + ':</b> ' + esc(p.expected_arrival) + '</div>';
  if (p.purpose) html += '<div style="grid-column:1/-1;"><b>' + L("purpose") + ':</b> ' + esc(p.purpose) + '</div>';
  if (p.remark) html += '<div style="grid-column:1/-1;"><b>' + L("remark") + ':</b> ' + esc(p.remark) + '</div>';
  html += '<div style="grid-column:1/-1;"><b>' + L("submitted_by") + ':</b> ' + esc(p.created_by) + ' · ' + esc(fmtTime(p.created_at)) + '</div>';
  if (p.manual_completed_by) {
    html += '<div style="grid-column:1/-1;"><b>直接完结人:</b> ' + esc(p.manual_completed_by) + ' · ' + esc(fmtTime(p.manual_completed_at)) + '</div>';
  }
  html += '</div></div>';

  // --- Plan lines ---
  if (lines.length > 0) {
    var hasPutaway = lines.some(function(ln) { return (ln.putaway_qty || 0) > 0; });
    html += '<div class="card"><div class="card-title">' + L("plan_lines") + '</div>';
    html += '<table class="line-table"><thead><tr><th>' + L("biz_class") + '</th><th>' + L("planned_qty") + '</th><th>' + L("actual_qty") + '</th>';
    if (hasPutaway) html += '<th>实际入库</th><th>差异</th>';
    else html += '<th>' + L("diff") + '</th>';
    html += '</tr></thead><tbody>';
    lines.forEach(function(ln) {
      var actualQty = ln.actual_qty || 0;
      html += '<tr><td>' + esc(unitTypeLabel(ln.unit_type)) + '</td><td>' + ln.planned_qty + '</td><td>' + actualQty + '</td>';
      if (hasPutaway) {
        var pQty = ln.putaway_qty || 0;
        var diff2 = pQty - actualQty;
        var diffStr2 = diff2 === 0 ? "-" : (diff2 > 0 ? "+" + diff2 : "" + diff2);
        var diffClass2 = diff2 !== 0 ? ' style="color:#e74c3c;font-weight:700;"' : '';
        html += '<td>' + pQty + '</td><td' + diffClass2 + '>' + diffStr2 + '</td>';
      } else {
        var diff = actualQty - (ln.planned_qty || 0);
        var diffStr = diff === 0 ? "-" : (diff > 0 ? "+" + diff : "" + diff);
        var diffClass = diff !== 0 ? ' style="color:#e74c3c;font-weight:700;"' : '';
        html += '<td' + diffClass + '>' + diffStr + '</td>';
      }
      html += '</tr>';
    });
    html += '</tbody></table></div>';
  }

  // --- Jobs: structured execution records (only if jobs exist) ---
  if (jobs.length > 0) {
    html += '<div class="card"><div class="card-title">现场执行记录</div>';
    jobs.forEach(function(j, idx) {
      if (idx > 0) html += '<div style="border-top:1px solid #eee;margin:8px 0;"></div>';
      var jobTypeMap = {unload:'卸货',inbound_direct:'代发入库',inbound_bulk:'大货入库',inbound_return:'退件入库',load:'装货',sort:'分拣',check:'核对',other:'其他'};
      var jobLabel = jobTypeMap[j.job_type] || j.job_type || '--';
      var isInboundJob = (j.job_type || '').indexOf('inbound') === 0;
      var isReturnJob = (j.job_type === 'inbound_return') || j.is_return === true;
      html += '<div style="font-size:13px;line-height:1.8;">';
      html += '<div><span class="st st-' + esc(j.status) + '">' + esc(stLabel(j.status)) + '</span> <b>' + esc(jobLabel) + '</b></div>';
      if (isReturnJob) {
        html += '<div style="background:#fff8e1;border-left:3px solid #f39c12;padding:4px 8px;margin:4px 0;font-size:12px;">退件入库 · 仅记录工时 · 入库数量由外部 WMS 数据对账</div>';
      }
      html += '<div>参与人员：' + esc(j.worker_names_text || j.created_by || '--') + '</div>';
      html += '<div>完成时间：' + esc(j.completed_at ? fmtTime(j.completed_at) : '--') + '</div>';
      html += '<div>用时：' + (j.total_minutes_worked || 0) + L("minutes") + '</div>';
      // Result lines — unload shows actual_qty, standard inbound shows putaway_qty, return inbound shows nothing
      var rl = j.result_lines || [];
      if (!isReturnJob) {
        if (rl.length > 0) {
          var parts = [];
          rl.forEach(function(r) {
            var qty = isInboundJob ? (r.putaway_qty || 0) : (r.actual_qty || 0);
            parts.push(unitTypeLabel(r.unit_type) + ' ' + qty);
          });
          html += '<div>' + (isInboundJob ? '实际入库：' : '实际结果：') + esc(parts.join(' / ')) + '</div>';
        } else if (j.status === 'completed') {
          html += '<div>' + (isInboundJob ? '实际入库：' : '实际结果：') + '--</div>';
        }
      }
      // Extra ops — standard inbound only
      if (!isReturnJob && isInboundJob && j.extra_ops) {
        var eo = j.extra_ops;
        var eoParts = [];
        if (Number(eo.sort_qty || 0) > 0) eoParts.push('理货 ' + eo.sort_qty);
        if (Number(eo.label_qty || 0) > 0) eoParts.push('贴标 ' + eo.label_qty);
        if (Number(eo.repair_box_qty || 0) > 0) eoParts.push('修补箱 ' + eo.repair_box_qty);
        if (eoParts.length > 0) {
          html += '<div>额外作业：' + esc(eoParts.join(' / ')) + '</div>';
        }
        if (eo.other_op_remark) html += '<div>其他操作说明：' + esc(eo.other_op_remark) + '</div>';
      }
      if (j.diff_note) html += '<div>现场差异说明：' + esc(j.diff_note) + '</div>';
      if (j.result_note) html += '<div>' + (isReturnJob ? '工作说明：' : '入库说明：') + esc(j.result_note) + '</div>';
      if (j.remark) html += '<div>其他备注：' + esc(j.remark) + '</div>';
      html += '</div>';
    });
    html += '</div>';
  }

  // --- Attachments ---
  if (atts.length > 0) {
    html += '<div class="card"><div class="card-title">' + L("attachments") + ' (' + atts.length + ')</div>';
    html += '<div class="att-grid">';
    atts.forEach(function(att) {
      if (att.content_type && att.content_type.startsWith("image/")) {
        html += '<img class="att-thumb" src="' + esc(fileUrl(att.file_key)) + '" onclick="showLightbox(\'' + esc(fileUrl(att.file_key)) + '\')">';
      }
    });
    html += '</div></div>';
  }

  // --- Finalize form for dynamic plans awaiting info ---
  if (isDynamic && p.status === 'unloaded_pending_info') {
    html += '<div class="card"><div class="card-title">补充信息并转正</div>';
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px;">';
    html += '<div><label><b>' + L("customer") + '</b></label><input id="dynCustomer" class="input" value="' + esc(p.customer === '待补充' ? '' : p.customer) + '" placeholder="客户名称"></div>';
    html += '<div><label><b>' + L("biz_class") + '</b></label><select id="dynBiz" class="input"><option value="">--</option><option value="direct_ship"' + (p.biz_class === 'direct_ship' ? ' selected' : '') + '>' + bizLabel('direct_ship') + '</option><option value="bulk"' + (p.biz_class === 'bulk' ? ' selected' : '') + '>' + bizLabel('bulk') + '</option><option value="return"' + (p.biz_class === 'return' ? ' selected' : '') + '>' + bizLabel('return') + '</option><option value="import"' + (p.biz_class === 'import' ? ' selected' : '') + '>' + bizLabel('import') + '</option></select></div>';
    html += '<div style="grid-column:1/-1;"><label><b>' + L("cargo_summary") + '</b></label><input id="dynCargo" class="input" value="' + esc(p.cargo_summary) + '"></div>';
    html += '<div><label><b>' + L("expected_arrival") + '</b></label><input id="dynArrival" class="input" value="' + esc(p.expected_arrival) + '"></div>';
    html += '<div><label><b>' + L("purpose") + '</b></label><input id="dynPurpose" class="input" value="' + esc(p.purpose) + '"></div>';
    html += '<div style="grid-column:1/-1;"><label><b>' + L("remark") + '</b></label><input id="dynRemark" class="input" value="' + esc(p.remark) + '"></div>';
    html += '</div>';
    html += '<div style="margin-top:10px;"><button class="btn btn-success" onclick="finalizeDynamicPlan(this)">确认转正为入库单</button></div>';
    html += '</div>';
  }

  // --- Actions ---
  html += '<div class="card">';
  html += '<button class="btn btn-outline btn-sm" onclick="printIbQr()">' + L("print") + '</button> ';
  if (p.status === "arrived_pending_putaway") {
    html += '<button class="btn btn-success" onclick="markInboundCompleted(this)">文员直接完成入库 / 직접 입고 완료</button> ';
  }
  if (p.status === "pending" || p.status === "arrived_pending_putaway") {
    html += '<button class="btn btn-danger btn-sm" onclick="cancelInboundPlan(this)">' + L("status_cancelled") + '</button>';
  }
  html += '</div>';

  body.innerHTML = html;
}

// QR helper using qrcode-generator (loaded as ../shared/qrcode.min.js)
function buildInboundQrHtml(text, cellSize) {
  var qr = qrcode(0, 'M');
  qr.addData(String(text || ''));
  qr.make();
  return qr.createSvgTag({ cellSize: cellSize || 4, margin: 0, scalable: true });
}

function printIbQr() {
  var qrEl = document.getElementById("ibDetailQr");
  if (!qrEl) return;
  var win = window.open("", "_blank");
  win.document.write('<html><head><title>' + _currentInboundId + '</title></head><body style="text-align:center;padding:40px;">' +
    '<h2>' + esc(_currentInboundId) + '</h2>' + qrEl.innerHTML +
    '<script>window.print();<\/script></body></html>');
  win.document.close();
}

async function cancelInboundPlan(btnEl) {
  var reason = prompt("取消原因（可选）/ 취소 사유(선택):", "");
  if (reason === null) return;
  if (!confirm("确认取消此入库计划？\n이 입고계획을 취소하시겠습니까?")) return;
  withActionLock('cancelInboundPlan', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_inbound_plan_cancel",
      inbound_plan_id: _currentInboundId,
      operator_name: getUser(),
      reason: reason.trim()
    });
    if (res && res.ok) {
      alert("已取消 / 취소됨");
      loadInboundDetail();
    } else if (res && res.error === "active_job_exists") {
      alert("当前仍有进行中的现场任务，不能取消\n현재 진행 중인 현장 작업이 있어 취소 불가");
    } else if (res && res.error === "cancel_not_allowed") {
      alert(res.message || "当前状态不允许取消");
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

async function markInboundCompleted(btnEl) {
  if (!_currentInboundId) return;
  var remark = prompt("入库完成备注（可选）/ 입고 완료 메모(선택):", "");
  if (remark === null) return;
  if (!confirm("确认将此入库计划标记为\u201C已入库\u201D？\n이 입고계획을 \u201C입고완료\u201D로 변경하시겠습니까?")) return;
  withActionLock('markInboundCompleted', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_inbound_mark_completed",
      inbound_plan_id: _currentInboundId,
      operator_name: getUser(),
      remark: remark
    });
    if (res && res.ok) {
      alert("已标记为已入库（文员直接完结）/ 입고완료로 변경됨(직접 완결)");
      loadInboundDetail();
    } else if (res && res.error === "inbound_job_still_active") {
      alert("当前仍有进行中的入库任务，不能直接完结\n현재 진행 중인 입고 작업이 있어 직접 완결 불가");
    } else {
      alert(L("error") + ": " + (res ? res.error : "unknown"));
    }
  });
}

async function finalizeDynamicPlan(btnEl) {
  var customer = (document.getElementById("dynCustomer") || {}).value || "";
  if (!customer.trim()) { alert("请填写客户名称"); return; }
  if (!confirm("确认将此动态单转正为正式入库单？")) return;
  withActionLock('finalizeDynamicPlan', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_inbound_dynamic_finalize",
      id: _currentInboundId,
      customer: customer.trim(),
      biz_class: (document.getElementById("dynBiz") || {}).value || "",
      cargo_summary: (document.getElementById("dynCargo") || {}).value || "",
      expected_arrival: (document.getElementById("dynArrival") || {}).value || "",
      purpose: (document.getElementById("dynPurpose") || {}).value || "",
      remark: (document.getElementById("dynRemark") || {}).value || ""
    });
    if (res && res.ok) {
      alert("已转正为入库单: " + (res.display_no || res.id));
      loadInboundDetail();
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

// ===== Feedback Module =====
var _currentFeedbackId = null;

async function loadFeedbackList() {
  var body = document.getElementById("feedbackListBody");
  if (!body) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var fbType = (document.getElementById("fbFilterType") || {}).value || "";
  var fbStatus = (document.getElementById("fbFilterStatus") || {}).value || "";
  var res = await api({ action: "v2_feedback_list", feedback_type: fbType, status: fbStatus });

  if (!res || !res.ok) {
    body.innerHTML = '<div class="card muted">' + L("error") + '</div>';
    return;
  }

  var items = res.items || [];
  if (items.length === 0) {
    body.innerHTML = '<div class="card muted">' + L("no_data") + '</div>';
    return;
  }

  var html = '<div class="card">';
  items.forEach(function(fb) {
    var typeLabel = fb.feedback_type === "unplanned_unload" ? "计划外卸货" : fb.feedback_type === "unload_no_doc" ? L("feedback_unload_no_doc") : (fb.feedback_type || L("feedback_general"));
    var statusLabel = fb.status === "open" ? L("status_open") : fb.status === "converted" ? L("status_converted") : fb.status === "field_working" ? "现场卸货中" : fb.status === "unloaded_pending_info" ? "待补充信息" : stLabel(fb.status);
    var fbDisplayNo = fb.display_no || fb.id;
    html += '<div class="list-item" onclick="openFeedbackDetail(\'' + esc(fb.id) + '\')">';
    html += '<div class="item-title">';
    html += '<span class="st st-' + esc(fb.status) + '">' + esc(statusLabel) + '</span> ';
    html += '<span class="biz-tag">' + esc(typeLabel) + '</span> ';
    html += '[' + esc(fbDisplayNo) + '] ' + esc(feedbackTitleText(fb));
    html += '</div>';
    html += '<div class="item-meta">' + esc(fb.submitted_by || "") + ' · ' + esc(fmtTime(fb.created_at)) + '</div>';
    html += '</div>';
  });
  html += '</div>';
  body.innerHTML = html;
}

function openFeedbackDetail(id) {
  _currentFeedbackId = id;
  goView("feedback_detail");
  loadFeedbackDetail();
}

async function loadFeedbackDetail() {
  var body = document.getElementById("feedbackDetailBody");
  if (!body || !_currentFeedbackId) return;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var res = await api({ action: "v2_feedback_detail", id: _currentFeedbackId });
  if (!res || !res.ok || !res.feedback) {
    body.innerHTML = '<div class="card muted">' + L("error") + '</div>';
    return;
  }

  var fb = res.feedback;
  var jobResults = res.job_results || [];
  var feedbackResultLines = res.feedback_result_lines || [];
  var isUnplanned = (fb.feedback_type === "unplanned_unload");
  var typeLabel = isUnplanned ? "计划外卸货" : fb.feedback_type === "unload_no_doc" ? L("feedback_unload_no_doc") : (fb.feedback_type || L("feedback_general"));
  var statusLabel = fb.status === "open" ? L("status_open") : fb.status === "converted" ? L("status_converted") : fb.status === "field_working" ? "现场卸货中" : fb.status === "unloaded_pending_info" ? "已卸货·待补充信息" : stLabel(fb.status);

  var fbDisplayNo = fb.display_no || fb.id;
  var html = '<div class="card">';
  html += '<div style="font-size:16px;font-weight:700;margin-bottom:4px;">' + esc(fbDisplayNo) + '</div>';
  if (fb.display_no) {
    html += '<div style="font-size:11px;color:#999;margin-bottom:8px;">ID: ' + esc(fb.id) + '</div>';
  }
  html += '<div class="detail-field"><b>' + L("status") + ':</b> <span class="st st-' + esc(fb.status) + '">' + esc(statusLabel) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("feedback_type") + ':</b> ' + esc(typeLabel) + '</div>';
  html += '<div class="detail-field"><b>标题:</b> ' + esc(feedbackTitleText(fb)) + '</div>';
  html += '<div class="detail-field"><b>内容:</b> ' + esc(fb.content) + '</div>';
  html += '<div class="detail-field"><b>' + L("submitted_by") + ':</b> ' + esc(fb.submitted_by) + ' · ' + esc(fmtTime(fb.created_at)) + '</div>';
  if (fb.completed_at) {
    html += '<div class="detail-field"><b>完成时间:</b> ' + esc(fmtTime(fb.completed_at)) + '</div>';
  }
  if (fb.completed_by) {
    html += '<div class="detail-field"><b>完成人:</b> ' + esc(fb.completed_by) + '</div>';
  }
  if (fb.diff_note) {
    html += '<div class="detail-field"><b>现场差异说明:</b> ' + esc(fb.diff_note) + '</div>';
  }
  if (fb.remark) {
    html += '<div class="detail-field"><b>备注:</b> ' + esc(fb.remark) + '</div>';
  }
  if (fb.inbound_plan_id) {
    html += '<div class="detail-field"><b>已转正入库计划:</b> <a href="#" onclick="openInboundDetail(\'' + esc(fb.inbound_plan_id) + '\');return false;">' + esc(fb.inbound_plan_id) + '</a></div>';
  }
  html += '</div>';

  // Unload result lines (from feedback itself — unplanned_unload flow)
  if (feedbackResultLines.length > 0) {
    html += '<div class="card"><div class="card-title">卸货结果明细</div>';
    html += '<table class="line-table"><thead><tr><th>类型</th><th>实际数量</th></tr></thead><tbody>';
    feedbackResultLines.forEach(function(r) {
      html += '<tr><td>' + esc(unitTypeLabel(r.unit_type)) + '</td><td>' + (r.actual_qty || 0) + '</td></tr>';
    });
    html += '</tbody></table></div>';
  }

  // Legacy job results (for old unload_no_doc feedbacks)
  if (jobResults.length > 0 && feedbackResultLines.length === 0) {
    html += '<div class="card"><div class="card-title">卸货结果</div>';
    jobResults.forEach(function(jr) {
      html += '<div style="font-size:13px;padding:4px 0;">';
      try {
        var rl = JSON.parse(jr.result_lines_json || "[]");
        rl.forEach(function(r) { html += unitTypeLabel(r.unit_type) + ': ' + r.actual_qty + ' '; });
      } catch(e) {}
      if (jr.diff_note) html += '<br>' + L("diff_note") + ': ' + esc(jr.diff_note);
      html += '</div>';
    });
    html += '</div>';
  }

  // "补充并转正" form — for unplanned_unload with status unloaded_pending_info
  if (fb.status === "unloaded_pending_info" && (isUnplanned || fb.feedback_type === "unload_no_doc")) {
    html += '<div class="card"><div class="card-title">补充信息并转正为入库计划</div>';
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px;">';
    html += '<div><label><b>' + L("customer") + ' *</b></label><input id="fb-conv-customer" class="input" placeholder="客户名称"></div>';
    html += '<div><label><b>' + L("biz_class") + '</b></label><select id="fb-conv-biz" class="input"><option value="">--</option><option value="direct_ship">' + bizLabel("direct_ship") + '</option><option value="bulk">' + bizLabel("bulk") + '</option><option value="return">' + bizLabel("return") + '</option><option value="import">' + bizLabel("import") + '</option></select></div>';
    var defaultCargo = feedbackResultLines.map(function(r) { return unitTypeLabel(r.unit_type) + ' ' + (r.actual_qty || 0); }).join(' / ');
    html += '<div style="grid-column:1/-1;"><label><b>' + L("cargo_summary") + '</b></label><input id="fb-conv-cargo" class="input" value="' + esc(defaultCargo || fb.title || '') + '"></div>';
    html += '<div><label><b>' + L("expected_arrival") + '</b></label><input id="fb-conv-arrival" class="input" placeholder="预计到达"></div>';
    html += '<div><label><b>' + L("purpose") + '</b></label><input id="fb-conv-purpose" class="input" placeholder="入库目的"></div>';
    html += '<div style="grid-column:1/-1;"><label><b>' + L("remark") + '</b></label><input id="fb-conv-remark" class="input" value="' + esc(fb.remark || '') + '"></div>';
    html += '</div>';

    // Show editable lines table
    if (feedbackResultLines.length > 0) {
      html += '<div style="margin-top:10px;"><b>卸货明细（将作为入库计划明细）:</b>';
      html += '<table class="line-table"><thead><tr><th>类型</th><th>数量</th></tr></thead><tbody id="fb-conv-lines">';
      feedbackResultLines.forEach(function(r, idx) {
        html += '<tr><td>' + esc(unitTypeLabel(r.unit_type)) + '<input type="hidden" class="fb-line-unit" value="' + esc(r.unit_type) + '"></td>';
        html += '<td><input type="number" class="fb-line-qty" value="' + (r.actual_qty || 0) + '" min="0" step="0.1" style="width:80px;"></td></tr>';
      });
      html += '</tbody></table></div>';
    }

    html += '<div style="margin-top:12px;"><button class="btn btn-success" onclick="convertFeedbackToInbound(this)">确认转正为入库计划</button></div>';
    html += '</div>';
  }

  // Legacy convert action (for old open unload_no_doc)
  if (fb.status === "open" && fb.feedback_type === "unload_no_doc") {
    html += '<div class="card"><div class="card-title">' + L("convert_to_inbound") + '</div>';
    html += '<div class="form-group"><label>' + L("customer") + '</label><input id="fb-conv-customer" type="text"></div>';
    html += '<div class="form-group"><label>' + L("biz_class") + '</label>';
    html += '<select id="fb-conv-biz"><option value="direct_ship">' + L("biz_direct_ship") + '</option><option value="bulk">' + L("biz_bulk") + '</option><option value="return">' + L("biz_return") + '</option><option value="import">' + L("biz_import") + '</option></select></div>';
    html += '<div class="form-group"><label>' + L("cargo_summary") + '</label><input id="fb-conv-cargo" type="text" value="' + esc(fb.title || "") + '"></div>';
    html += '<button class="btn btn-primary" onclick="convertFeedbackToInbound(this)">' + L("convert_to_inbound") + '</button>';
    html += '</div>';
  }

  body.innerHTML = html;
}

async function convertFeedbackToInbound(btnEl) {
  var customer = (document.getElementById("fb-conv-customer") || {}).value || "";
  if (!customer.trim()) { alert(L("customer") + " " + L("required") + "!"); return; }
  if (!confirm("确认将此反馈转正为正式入库计划？")) return;
  withActionLock('convertFeedbackToInbound', btnEl || null, '提交中.../저장중...', async function() {
    var biz = (document.getElementById("fb-conv-biz") || {}).value || "";
    var cargo = (document.getElementById("fb-conv-cargo") || {}).value || "";

    // Collect lines from the editable table
    var lines = [];
    var unitEls = document.querySelectorAll(".fb-line-unit");
    var qtyEls = document.querySelectorAll(".fb-line-qty");
    for (var i = 0; i < unitEls.length; i++) {
      var qty = parseFloat(qtyEls[i].value) || 0;
      if (qty > 0) {
        lines.push({ unit_type: unitEls[i].value, actual_qty: qty, planned_qty: qty });
      }
    }

    var res = await api({
      action: "v2_feedback_finalize_to_inbound",
      feedback_id: _currentFeedbackId,
      customer: customer.trim(),
      biz_class: biz,
      cargo_summary: cargo.trim(),
      expected_arrival: (document.getElementById("fb-conv-arrival") || {}).value || "",
      purpose: (document.getElementById("fb-conv-purpose") || {}).value || "",
      remark: (document.getElementById("fb-conv-remark") || {}).value || "",
      lines: lines,
      created_by: getUser()
    });

    if (res && res.ok) {
      alert(L("success") + ": " + (res.display_no || res.inbound_plan_id));
      _currentInboundId = res.inbound_plan_id;
      goView("inbound_detail");
      loadInboundDetail();
    } else {
      alert(L("error") + ": " + (res ? res.error : "unknown"));
    }
  });
}

// ===== File Upload =====
function doUpload(docType, docId, category) {
  _uploadCtx = { related_doc_type: docType, related_doc_id: docId, attachment_category: category };
  document.getElementById("fileInput").click();
}

async function handleFileUpload(input) {
  if (!input.files || !input.files[0]) return;
  var file = input.files[0];
  var fd = new FormData();
  fd.append("file", file);
  fd.append("related_doc_type", _uploadCtx.related_doc_type || "");
  fd.append("related_doc_id", _uploadCtx.related_doc_id || "");
  fd.append("attachment_category", _uploadCtx.attachment_category || "");
  fd.append("uploaded_by", getUser());

  var res = await uploadFile(fd);
  if (res && res.ok) {
    alert("上传成功");
    // Refresh current detail
    if (_currentView === "issue_detail") loadIssueDetail();
    if (_currentView === "outbound_detail") loadOutboundDetail();
    if (_currentView === "inbound_detail") loadInboundDetail();
  } else {
    alert("上传失败: " + (res ? res.error : "unknown"));
  }
  input.value = "";
}

function showLightbox(url) {
  document.getElementById("lightboxImg").src = url;
  document.getElementById("lightbox").classList.add("show");
}

// ===== Order Ops (按单操作) =====
var _orderOpsJobType = {
  pick_direct: "代发拣货",
  bulk_op: "大货操作"
};
var _orderOpsStatus = {
  pending: "待开始",
  working: "作业中",
  awaiting_close: "待收尾",
  completed: "已完成"
};
var _flowStageLabel = {
  order_op: "按单操作",
  inbound: "入库",
  outbound: "出库",
  internal: "库内作业"
};
var _leaveReasonLabel = {
  finished: "正常完成",
  job_completed: "任务完成",
  interrupted: "中断离开",
  left: "主动离开",
  "": "--"
};

async function loadOrderOpsList() {
  var body = document.getElementById("orderOpsListBody");
  if (!body) return;
  body.innerHTML = '<span class="muted">' + L("loading") + '</span>';

  var job_type = (document.getElementById("orderOpsTypeFilter") || {}).value || "";
  var start = (document.getElementById("orderOpsStartDate") || {}).value || "";
  var end = (document.getElementById("orderOpsEndDate") || {}).value || "";

  var res = await api({ action: "v2_order_ops_job_list", job_type: job_type, start_date: start, end_date: end });
  if (!res || !res.ok) {
    body.innerHTML = '<span class="muted">加载失败</span>';
    return;
  }

  var items = res.items || [];
  if (items.length === 0) {
    body.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    return;
  }

  var html = '<table class="simple-table"><thead><tr>';
  html += '<th>类型</th><th>趟次/工单</th><th>关联单号</th><th>参与人员</th><th>工时</th><th>状态</th><th>创建时间</th><th>操作</th>';
  html += '</tr></thead><tbody>';

  items.forEach(function(j) {
    var typeText = _orderOpsJobType[j.job_type] || j.job_type;

    // Trip/work order column
    var tripHtml = '--';
    if (j.display_no) {
      tripHtml = '<span class="trip-tag">' + esc(j.display_no) + '</span>';
    } else if (j.related_doc_id) {
      tripHtml = '<span class="doc-tag">' + esc(j.related_doc_id) + '</span>';
    }

    // Doc nos column (pick docs or --)
    var docHtml = '<span class="muted">--</span>';
    if (j.job_type === "pick_direct" && j.pick_doc_nos && j.pick_doc_nos.length > 0) {
      docHtml = '<div class="tag-wrap">' + j.pick_doc_nos.map(function(d) {
        return '<span class="doc-tag">' + esc(d) + '</span>';
      }).join('') + '</div>';
    } else if (j.related_doc_id && j.display_no) {
      docHtml = '<span class="doc-tag">' + esc(j.related_doc_id) + '</span>';
    }

    var stText = _orderOpsStatus[j.status] || j.status;
    var stClass = 'st st-' + j.status;

    html += '<tr>';
    html += '<td>' + esc(typeText) + '</td>';
    html += '<td class="col-trip">' + tripHtml + '</td>';
    html += '<td class="col-doc">' + docHtml + '</td>';
    html += '<td class="col-people">' + esc(j.worker_names_text || "--") + '</td>';
    html += '<td class="col-num">' + (j.total_minutes_worked || 0) + '分</td>';
    html += '<td><span class="' + stClass + '">' + esc(stText) + '</span></td>';
    html += '<td class="col-time">' + esc(fmtTime(j.created_at)) + '</td>';
    html += '<td class="col-op"><a href="#" onclick="openOrderOpsDetail(\'' + esc(j.id) + '\');return false;">详情</a></td>';
    html += '</tr>';
  });

  html += '</tbody></table>';
  body.innerHTML = html;
}

var _currentOrderOpsId = null;
function openOrderOpsDetail(id) {
  _currentOrderOpsId = id;
  goView("order_ops_detail");
  loadOrderOpsDetail();
}

async function loadOrderOpsDetail() {
  var body = document.getElementById("orderOpsDetailBody");
  if (!body || !_currentOrderOpsId) return;
  body.innerHTML = '<span class="muted">' + L("loading") + '</span>';

  var res = await api({ action: "v2_ops_job_detail", job_id: _currentOrderOpsId });
  if (!res || !res.ok || !res.job) {
    body.innerHTML = '<span class="muted">加载失败或未找到</span>';
    return;
  }

  var j = res.job;
  var workers = res.workers || [];
  var results = res.results || [];
  var typeText = _orderOpsJobType[j.job_type] || j.job_type;
  var stText = _orderOpsStatus[j.status] || j.status;

  // === Card 1: Basic Info ===
  var flowLabel = _flowStageLabel[j.flow_stage] || j.flow_stage || "--";
  var html = '<div class="card">';
  html += '<div class="card-title">' + esc(typeText);
  if (j.display_no) html += ' · <span style="font-family:monospace;color:#2f54eb;">' + esc(j.display_no) + '</span>';
  html += '</div>';
  html += '<div class="detail-grid">';
  html += '<div class="detail-field"><b>状态:</b> <span class="st st-' + esc(j.status) + '">' + esc(stText) + '</span></div>';
  html += '<div class="detail-field"><b>业务分类:</b> ' + esc(bizLabel(j.biz_class)) + '</div>';
  if (j.display_no) {
    html += '<div class="detail-field"><b>趟次号:</b> <span class="trip-tag">' + esc(j.display_no) + '</span></div>';
  }
  html += '<div class="detail-field"><b>作业阶段:</b> ' + esc(flowLabel) + '</div>';
  if (j.related_doc_id) {
    html += '<div class="detail-field"><b>关联单号:</b> <span class="doc-tag">' + esc(j.related_doc_id) + '</span></div>';
  }
  html += '<div class="detail-field"><b>创建人:</b> ' + esc(j.created_by || "--") + '</div>';
  html += '<div class="detail-field"><b>创建时间:</b> ' + esc(fmtTime(j.created_at)) + '</div>';
  html += '</div></div>';

  // === Card 2: Pick Docs (if applicable) ===
  if (j.job_type === 'pick_direct') {
    html += '<div class="card"><div class="card-title">拣货单号</div><div id="orderOpsPickDocs"><span class="muted">加载中...</span></div></div>';
  }

  // === Card 3: Workers ===
  html += '<div class="card"><div class="card-title">参与人员</div>';
  if (workers.length > 0) {
    html += '<table class="simple-table"><thead><tr>';
    html += '<th>姓名</th><th>加入时间</th><th>离开时间</th><th>工时</th><th>离开原因</th>';
    html += '</tr></thead><tbody>';
    workers.forEach(function(w) {
      var reasonText = _leaveReasonLabel[w.leave_reason] || w.leave_reason || "--";
      html += '<tr>';
      html += '<td>' + esc(w.worker_name || "--") + '</td>';
      html += '<td class="col-time">' + esc(fmtTime(w.joined_at)) + '</td>';
      html += '<td class="col-time">' + (w.left_at ? esc(fmtTime(w.left_at)) : '<span class="st st-working">进行中</span>') + '</td>';
      html += '<td class="col-num">' + (w.minutes_worked ? Math.round(w.minutes_worked) + '分' : "--") + '</td>';
      html += '<td>' + esc(reasonText) + '</td>';
      html += '</tr>';
    });
    html += '</tbody></table>';
  } else {
    html += '<span class="muted">无参与人员记录</span>';
  }
  html += '</div>';

  // === Card 4: Results ===
  if (results.length > 0) {
    html += '<div class="card"><div class="card-title">操作结果</div>';
    results.forEach(function(r) {
      html += '<div style="border-bottom:1px solid #f0f0f0;padding:10px 0;">';
      html += '<div style="font-size:12px;color:#888;margin-bottom:6px;">记录时间: ' + esc(fmtTime(r.created_at)) + ' · 记录人: ' + esc(r.created_by || "--") + '</div>';
      if (r.remark) html += '<div class="detail-field"><b>备注:</b> ' + esc(r.remark) + '</div>';
      if (r.result_json) {
        try {
          var rd = JSON.parse(r.result_json);
          if (j.job_type === 'pick_direct' && rd.pick_doc_nos && rd.pick_doc_nos.length > 0) {
            html += '<div class="detail-field"><b>拣货单:</b> <div class="tag-wrap" style="margin-top:4px;">' +
              rd.pick_doc_nos.map(function(d) { return '<span class="doc-tag">' + esc(d) + '</span>'; }).join('') +
              '</div></div>';
          }
          if (rd.result_note) html += '<div class="detail-field"><b>结果说明:</b> ' + esc(rd.result_note) + '</div>';
          // Bulk op fields — structured grid
          if (j.job_type === 'bulk_op') {
            var fields = [
              ["品数", rd.packed_sku_count], ["打包箱数", rd.packed_box_count],
              ["大纸箱", rd.used_carton_large_count], ["小纸箱", rd.used_carton_small_count],
              ["修补箱数", rd.repaired_box_count], ["换箱数", rd.reboxed_count],
              ["标签数", rd.label_count], ["操作总箱数", rd.total_operated_box_count],
              ["打托数", rd.pallet_count], ["使用叉车", rd.used_forklift ? "是" : "否"],
              ["叉车货位数", rd.forklift_location_count]
            ];
            var validFields = fields.filter(function(f) { return f[1] !== undefined && f[1] !== 0 && f[1] !== "否"; });
            if (validFields.length > 0) {
              html += '<div class="detail-field" style="margin-top:6px;"><b>作业产出:</b></div>';
              html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:4px 12px;font-size:12px;margin-top:4px;">';
              validFields.forEach(function(f) {
                html += '<div><span style="color:#888;">' + esc(f[0]) + ':</span> <b>' + esc(String(f[1])) + '</b></div>';
              });
              html += '</div>';
            }
          }
        } catch(e) {}
      }
      html += '</div>';
    });
    html += '</div>';
  }

  body.innerHTML = html;

  // Load pick docs if applicable
  if (j.job_type === 'pick_direct') {
    var pdRes = await api({ action: "v2_pick_job_docs_list", job_id: j.id });
    var pdEl = document.getElementById("orderOpsPickDocs");
    if (pdEl && pdRes && pdRes.ok && pdRes.docs) {
      if (pdRes.docs.length > 0) {
        pdEl.innerHTML = '<div class="tag-wrap">' + pdRes.docs.map(function(d) {
          return '<span class="doc-tag">' + esc(d.pick_doc_no) + '</span>';
        }).join('') + '</div>';
      } else {
        pdEl.innerHTML = '<span class="muted">无拣货单号</span>';
      }
    }
  }
}

// ===== Init =====
window.addEventListener("DOMContentLoaded", function() {
  // Set default dates
  var today = kstToday();
  var el;
  el = document.getElementById("oc-date"); if (el) el.value = today;
  el = document.getElementById("ibc-date"); if (el) el.value = today;

  checkAutoLogin();
});
