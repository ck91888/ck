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
  'v2_issue_create','v2_issue_handle_start','v2_issue_handle_finish',
  'v2_issue_close','v2_issue_cancel','v2_issue_rework',
  'v2_outbound_order_create','v2_outbound_order_update_status',
  'v2_outbound_load_start','v2_outbound_load_finish',
  'v2_inbound_plan_create','v2_inbound_plan_update_status','v2_inbound_plan_cancel',
  'v2_inbound_dynamic_finalize',
  'v2_unplanned_unload_start','v2_unplanned_unload_join','v2_unplanned_unload_finish',
  'v2_feedback_finalize_to_inbound','v2_unload_dynamic_start',
  'v2_unload_job_start','v2_unload_job_finish',
  'v2_inbound_job_start','v2_inbound_job_finish',
  'v2_inbound_mark_completed',
  'v2_ops_job_start','v2_ops_job_leave','v2_ops_job_finish','v2_ops_job_resume',
  'v2_pick_job_start','v2_pick_job_join','v2_pick_job_add_docs','v2_pick_job_finish',
  'v2_bulk_op_job_start','v2_bulk_op_job_finish',
  'v2_correction_request_create','v2_admin_dirty_data_cleanup',
  'v2_verify_batch_upload','v2_verify_batch_update_status',
  'v2_inbound_plan_mark_accounted','v2_outbound_order_mark_accounted',
  'v2_inbound_plan_delete','v2_outbound_order_delete',
  'v2_outbound_order_update_ship_plan',
  'v2_outbound_stock_op_start','v2_outbound_stock_op_finish',
  'v2_feedback_delete',
  'v2_issue_mark_accounting_required','v2_issue_mark_accounted',
  'v2_inbound_plan_update','v2_outbound_order_update','v2_outbound_order_ack_change',
  'v2_outbound_pickup_confirm'
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
    // TypeError: Failed to fetch / NetworkError — 不是业务错误，是网络/域名问题
    return {
      ok: false,
      error: "network_error",
      network_error: true,
      message: "接口连接失败（网络或域名问题），不是业务提交失败\n네트워크 오류 (서버 응답 없음)\n" + (e && e.message || e)
    };
  }
}

async function uploadFile(formData) {
  formData.append("k", getKey());
  formData.append("action", "v2_attachment_upload");
  try {
    var res = await fetch(V2_API, { method: "POST", body: formData });
    return await res.json();
  } catch(e) {
    return {
      ok: false,
      error: "network_error",
      network_error: true,
      message: "文件上传失败（网络或域名问题）\n파일 업로드 실패 (네트워크 오류)\n" + (e && e.message || e)
    };
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
  // 轻量鉴权：只验 ADMINKEY/VIEWKEY，不查任何业务表
  fetch(V2_API, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "v2_auth_check", k: k })
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
    // 轻量探测，避免开页就触发业务 SQL
    fetch(V2_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "v2_auth_check", k: getKey() })
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
      }).catch(function(e) {
        // 网络/域名异常：不要进入主页，否则用户会误以为系统正常但所有提交都失败
        var errEl = document.getElementById("loginErr");
        if (errEl) {
          errEl.style.color = "#e74c3c";
          errEl.textContent = "后端接口连接失败，请检查网络或接口域名\n백엔드 연결 실패, 네트워크/도메인 확인\n(" + (e && e.message || e) + ")";
        }
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
  if (_currentTab === "order_ops") loadOrderOpsList();
  // order_ops_detail 是 view 而非 tab，必须用 _currentView 判断
  if (_currentView === "order_ops_detail" && _currentOrderOpsJobId) loadOrderOpsDetail(_currentOrderOpsJobId);
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
  var btnNC = document.getElementById("btnNewCheck");
  if (btnNC) btnNC.textContent = L("new_check");

  // Tabs
  var tabMap = { home: "tab_home", issue: "tab_issue", outbound: "tab_outbound",
    inbound: "tab_inbound", feedback: "tab_feedback", check: "tab_check",
    order_ops: "tab_order_ops" };
  var tabs = document.querySelectorAll("#mainTabs button");
  tabs.forEach(function(btn) {
    var key = btn.getAttribute("data-tab");
    if (key && tabMap[key]) btn.textContent = L(tabMap[key]);
  });

  // Generic data-i18n pass (for order_ops view + future views)
  var els = document.querySelectorAll("[data-i18n]");
  els.forEach(function(el) {
    var key = el.getAttribute("data-i18n");
    if (!key) return;
    if (el.tagName === "INPUT" || el.tagName === "TEXTAREA") {
      el.placeholder = L(key);
    } else {
      el.textContent = L(key);
    }
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
  var lang = getLang();
  if (lang === 'ko') {
    var map = {pending:'대기중',unloading:'하차중(입고가능)',unloading_putting_away:'하차중+입고중',arrived_pending_putaway:'입고대기',putting_away:'입고중',partially_completed:'부분 입고 완료',completed:'입고완료',cancelled:'취소됨'};
  } else {
    var map = {pending:'待到库',unloading:'卸货中（可提前理货）',unloading_putting_away:'卸货中+理货中',arrived_pending_putaway:'已到库待理货',putting_away:'理货中',partially_completed:'部分入库完成',completed:'已入库',cancelled:'已取消'};
  }
  return map[status] || stLabel(status);
}

// 入库业务类型 → 现场入库操作文案
function inboundBizTaskLabel(biz) {
  var lang = getLang();
  var zh = { direct_ship: '代发入库', bulk: '大货入库', return: '退件入库', change_order: '换单入库' };
  var ko = { direct_ship: '직배송 입고', bulk: '대량화물 입고', return: '반품 입고', change_order: '송장교체 입고' };
  return (lang === 'ko' ? ko : zh)[biz] || biz;
}

function bizLabel(biz) {
  return L("biz_" + biz) || biz;
}

function outModeLabel(mode) {
  return L("outmode_" + mode) || mode;
}

// priLabel: UI 不再展示优先级；保留函数定义只为兼容外部潜在引用，返回原值
function priLabel(pri) { return pri; }

// 列表外层是否记帐 tag（入库计划/出库作业单共用）
function accountTag(o) {
  // 字段未下发（旧接口/旧缓存）→ 不显示，避免把未知态误标为"未记帐"
  if (!o || o.accounted == null) return '';
  if (Number(o.accounted) === 1) {
    return '<span class="account-tag accounted">' + esc(L("accounted_yes")) + '</span> ';
  }
  return '<span class="account-tag unaccounted">' + esc(L("accounted_no")) + '</span> ';
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

  // Show corresponding view（goView 内部会自动 updateActionsBarByView）
  goView(tab);

  // Load data
  if (tab === "home") loadDashboard();
  if (tab === "issue") loadIssueList();
  if (tab === "outbound") loadOutboundList();
  if (tab === "inbound") loadInboundList();
  if (tab === "feedback") loadFeedbackList();
  if (tab === "order_ops") loadOrderOpsList();
  if (tab === "check") loadVerifyList();
}

// 按当前 view 动态显示"+新建"按钮（详情/创建/feedback/home/order_ops 全部隐藏）
function updateActionsBarByView(view) {
  var map = {
    issue:    'btnNewIssue',
    outbound: 'btnNewOutbound',
    inbound:  'btnNewInbound',
    check:    'btnNewCheck'
    // 其余 view（home / feedback / order_ops / *_create / *_detail）→ 全部隐藏
  };
  var target = map[view] || null;
  ['btnNewIssue','btnNewOutbound','btnNewInbound','btnNewCheck'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el) el.style.display = (id === target) ? '' : 'none';
  });
}

function goView(name) {
  _currentView = name;
  var views = document.querySelectorAll(".view");
  views.forEach(function(v) { v.style.display = "none"; });
  var el = document.getElementById("view-" + name);
  if (el) el.style.display = "";
  updateActionsBarByView(name);
}

// TODO(分页 — list 接口能力盘点 2026-04-27):
//   v2_issue_list / v2_outbound_order_list / v2_inbound_plan_list /
//   v2_feedback_list / v2_verify_batch_list ✓ 已支持 limit/offset。
//   v2_order_ops_job_list ✗ 当前硬编码 LIMIT 200，需先改后端再前端分页。
//   后续做"加载更多"时：维护 _xxxOffset 累加，列表底部按钮调
//   loadXxxList({append:true}) 即可，无需改后端（除 order_ops 外）。

// ===== Dashboard =====
async function loadDashboard() {
  var grid = document.getElementById("dashGrid");
  if (!grid) return;
  grid.innerHTML = '<div class="dash-card"><span class="muted">' + L("loading") + '</span></div>';

  // 单次聚合：替代原先 5 个 list 接口 Promise.all
  var summary = await api({ action: "v2_dashboard_summary" });
  if (!summary || !summary.ok) {
    var errMsg = (summary && (summary.error || summary.message)) || "网络异常 / 네트워크 오류";
    console.error("[dashboard_summary]", summary);
    grid.innerHTML =
      '<div class="dash-card"><div style="color:#e74c3c;font-weight:700;">今日看板加载失败 / 대시보드 로딩 실패</div>' +
      '<div style="font-size:12px;color:#666;margin-top:6px;">原因: ' + esc(String(errMsg)) + '</div>' +
      '<button class="btn btn-outline btn-sm" style="margin-top:8px;" onclick="loadDashboard()">重试 / 재시도</button>' +
      '</div>';
    return;
  }

  // 后端已按"待处理"状态过滤，前端不再二次 filter
  var pendingIssues = (summary.issues && summary.issues.items) || [];
  var pendingOb = (summary.outbounds && summary.outbounds.items) || [];
  var pendingIb = (summary.inbounds && summary.inbounds.items) || [];
  var activeFb = (summary.feedbacks && summary.feedbacks.items) || [];
  var upcoming = summary.upcoming || { items: [], dates: [] };

  // 优先用后端 count（items 仅是前 3 条预览，length ≠ 实际总数）
  // TODO(分页): 主要 list 接口（v2_inbound_plan_list / v2_outbound_order_list /
  //   v2_issue_list / v2_field_feedback_list）已支持 limit/offset，
  //   后续可在列表底部加"加载更多"按钮，本期先保持单页渲染。
  function pickCount(section, fallback) {
    if (section && section.count != null) {
      var n = Number(section.count);
      if (Number.isFinite(n)) return n;
    }
    return fallback.length;
  }
  var issuesCount  = pickCount(summary.issues,    pendingIssues);
  var obCount      = pickCount(summary.outbounds, pendingOb);
  var ibCount      = pickCount(summary.inbounds,  pendingIb);
  var fbCount      = pickCount(summary.feedbacks, activeFb);

  var html = '';

  // Issue card
  html += '<div class="dash-card" onclick="goTab(\'issue\')">';
  html += '<div class="d-title">⚠️ ' + L("today_issues") + '</div>';
  if (issuesCount > 0) {
    html += '<div class="d-count">' + issuesCount + '</div>';
    pendingIssues.slice(0, 3).forEach(function(i) {
      html += '<div style="font-size:12px;padding:2px 0;">' +
        '<span class="st st-' + esc(i.status) + '">' + esc(stLabel(i.status)) + '</span> ' +
        esc(issueTitleText(i) || i.customer || "--") + '</div>';
    });
  } else {
    html += '<div class="d-empty">' + L("no_data") + '</div>';
  }
  html += '</div>';

  // Outbound card
  var ackPendingObs = pendingOb.filter(function(o) { return Number(o.warehouse_ack_required) === 1; });
  html += '<div class="dash-card" onclick="goTab(\'outbound\')">';
  html += '<div class="d-title">🚚 ' + L("today_outbound") + '</div>';
  if (obCount > 0) {
    html += '<div class="d-count">' + obCount + '</div>';
    if (ackPendingObs.length > 0) {
      html += '<div style="background:#ffebee;border-left:3px solid #c62828;padding:4px 6px;font-size:12px;color:#c62828;font-weight:700;margin-bottom:4px;">⚠ 已变更待仓库确认 (' + ackPendingObs.length + (ackPendingObs.length >= 3 ? '+' : '') + ')</div>';
    }
    pendingOb.slice(0, 3).forEach(function(o) {
      html += '<div style="font-size:12px;padding:2px 0;">' +
        '<span class="st st-' + esc(o.status) + '">' + esc(stLabel(o.status)) + '</span> ' +
        (Number(o.warehouse_ack_required) === 1 ? '<span class="st" style="background:#ffebee;color:#c62828;">⚠待确认</span> ' : '') +
        accountTag(o) +
        esc(o.customer || "--") + ' ' + esc(o.order_date || "") + '</div>';
    });
  } else {
    html += '<div class="d-empty">' + L("no_data") + '</div>';
  }
  html += '</div>';

  // Inbound card
  html += '<div class="dash-card" onclick="goTab(\'inbound\')">';
  html += '<div class="d-title">📦 ' + L("today_inbound") + '</div>';
  if (ibCount > 0) {
    html += '<div class="d-count">' + ibCount + '</div>';
    pendingIb.slice(0, 3).forEach(function(p) {
      html += '<div style="font-size:12px;padding:2px 0;">' +
        '<span class="st st-' + esc(p.status) + '">' + esc(inboundStatusLabel(p.status)) + '</span> ' +
        accountTag(p) +
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
  if (fbCount > 0) {
    html += '<div class="d-count">' + fbCount + '</div>';
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
  var upcomingItems = (upcoming.items || []).filter(function(p) { return p.source_type !== 'return_session'; });
  var upcomingDates = upcoming.dates || [];
  // 后端 upcoming.count 已基于 source_type != 'return_session' 过滤口径；优先使用，回退到客户端 length
  var upcomingCount = upcomingItems.length;
  if (upcoming && upcoming.count != null) {
    var _uc = Number(upcoming.count);
    if (Number.isFinite(_uc)) upcomingCount = _uc;
  }
  html += '<div class="dash-card" onclick="goTab(\'inbound\')">';
  html += '<div class="d-title">📅 ' + L("upcoming_inbound") + '</div>';
  if (upcomingCount > 0) {
    html += '<div class="d-count">' + upcomingCount + '</div>';
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
          accountTag(p) +
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
  var acctSel = document.getElementById("issueFilterAccounting");
  var acct = acctSel ? acctSel.value : "";
  var apiParams = { action: "v2_issue_list", status: status, biz_class: biz };
  if (acct === 'required') apiParams.accounting_required = 1;
  else if (acct === 'unpaid') { apiParams.accounting_required = 1; apiParams.accounted = 0; }
  else if (acct === 'paid') apiParams.accounted = 1;
  var res = await api(apiParams);

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
    if (Number(it.accounted) === 1) {
      html += '<span class="biz-tag" style="background:#c8e6c9;color:#1b5e20;">已记帐</span> ';
    } else if (Number(it.accounting_required) === 1) {
      html += '<span class="biz-tag" style="background:#ffe0b2;color:#bf360c;">需记帐</span> ';
    }
    html += esc(issueTitleText(it));
    html += '</div>';
    html += '<div class="item-meta">';
    html += esc(it.customer || "") + ' · ' + esc(it.submitted_by || "") + ' · ' + esc(fmtTime(it.created_at));
    if (it.total_minutes_worked > 0) html += ' · ' + it.total_minutes_worked.toFixed(1) + L("minutes");
    html += '</div>';
    html += '</div>';
  });
  html += '</div>';
  body.innerHTML = html;
}

// ===== Issue Create =====
async function submitIssue(btnEl) {
  var desc = document.getElementById("ic-desc").value.trim();
  if (!desc) { alert("请填写问题描述 / 문제 설명을 입력하세요"); return; }
  var customer = document.getElementById("ic-customer").value.trim();
  if (!customer) { alert("请填写客户 / 고객을 입력하세요"); return; }

  withActionLock('submitIssue', btnEl || null, '提交中.../저장중...', async function() {
    var biz = document.getElementById("ic-biz").value;
    var docno = document.getElementById("ic-docno").value.trim();

    var res = await api({
      action: "v2_issue_create",
      biz_class: biz,
      customer: customer,
      related_doc_no: docno,
      issue_description: desc,
      priority: "normal", // 优先级字段已下线，固定 normal 兼容后端
      submitted_by: getUser()
    });

    if (res && res.ok) {
      alert("已创建: " + res.id);
      document.getElementById("ic-customer").value = "";
      document.getElementById("ic-docno").value = "";
      document.getElementById("ic-desc").value = "";
      goTab("issue");
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

// 取问题描述前 30 字作为标题（兼容旧记录的 issue_summary）
function issueTitleText(it) {
  var d = (it && (it.issue_description || it.issue_summary)) || "";
  d = String(d).trim().replace(/\s+/g, ' ');
  if (d.length > 30) d = d.substring(0, 30) + "…";
  return d || "(无描述)";
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
  html += '<div style="font-size:18px;font-weight:700;margin-bottom:8px;">' + esc(issueTitleText(it)) + '</div>';
  html += '<div class="detail-field"><b>' + L("status") + ':</b> <span class="st st-' + esc(it.status) + '">' + esc(stLabel(it.status)) + '</span>';
  if (Number(it.accounted) === 1) {
    html += ' <span class="biz-tag" style="background:#c8e6c9;color:#1b5e20;">已记帐</span>';
  } else if (Number(it.accounting_required) === 1) {
    html += ' <span class="biz-tag" style="background:#ffe0b2;color:#bf360c;">需记帐</span>';
  }
  html += '</div>';
  html += '<div class="detail-field"><b>' + L("biz_class") + ':</b> <span class="biz-tag biz-' + esc(it.biz_class) + '">' + esc(bizLabel(it.biz_class)) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("customer") + ':</b> ' + esc(it.customer) + '</div>';
  html += '<div class="detail-field"><b>' + L("related_doc_no") + ':</b> ' + esc(it.related_doc_no) + '</div>';
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

  // P1-5: 处理图片/附件（含 issue_handle_photo / feedback_photo / issue_photo / issue_attachment 等所有 issue_ticket 下附件）
  if (atts.length > 0) {
    html += '<div class="card"><div class="card-title">处理图片/附件 / 처리 사진·첨부 (' + atts.length + ')</div>';
    html += '<div class="att-grid">';
    atts.forEach(function(att) {
      var ct = att.content_type || '';
      var url = fileUrl(att.file_key);
      var who = (att.uploaded_by ? esc(att.uploaded_by) : '') + (att.created_at ? ' · ' + esc(fmtTime(att.created_at)) : '');
      if (ct.indexOf('image/') === 0) {
        html += '<div class="att-cell" style="text-align:center;">';
        html += '<img class="att-thumb" src="' + esc(url) + '" onclick="showLightbox(\'' + esc(url) + '\')" style="cursor:zoom-in;">';
        if (who) html += '<div class="muted" style="font-size:10px;">' + who + '</div>';
        html += '</div>';
      } else {
        var isPdf = ct.indexOf('pdf') !== -1 || (att.file_name && /\.pdf$/i.test(att.file_name));
        var icon = isPdf ? '📄' : '📎';
        html += '<div class="att-cell" style="display:inline-block;padding:8px 10px;border:1px solid #e0e0e0;border-radius:6px;background:#fafafa;margin:4px;">';
        html += '<a href="' + esc(url) + '" target="_blank" download="' + esc(att.file_name || '') + '" style="font-size:12px;color:#1565c0;text-decoration:none;">';
        html += icon + ' ' + esc(att.file_name || '附件') + '</a>';
        if (who) html += '<div class="muted" style="font-size:10px;margin-top:2px;">' + who + '</div>';
        html += '</div>';
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

  // Rework note
  if (it.rework_note) {
    html += '<div class="card" style="border-left:3px solid #e65100;">';
    html += '<div class="card-title" style="color:#e65100;">' + L("rework_note") + '</div>';
    html += '<div style="white-space:pre-wrap;">' + esc(it.rework_note) + '</div>';
    html += '</div>';
  }

  // Actions
  if (it.status !== "completed" && it.status !== "closed" && it.status !== "cancelled") {
    html += '<div class="card">';
    if (it.status === "responded" || it.status === "rework_required") {
      html += '<button class="btn btn-success" onclick="completeIssue(this)">' + L("complete_issue") + '</button> ';
      html += '<button class="btn btn-warning" onclick="reworkIssue(this)">' + L("rework_issue") + '</button> ';
    }
    if (it.status === "pending" || it.status === "processing") {
      html += '<button class="btn btn-success" onclick="completeIssue(this)">' + L("complete_issue") + '</button> ';
    }
    html += '<button class="btn btn-danger" onclick="cancelIssue(this)">' + L("cancel_issue") + '</button>';
    html += '<div style="margin-top:10px;"><label>' + L("attachments") + '</label>';
    html += '<div class="att-grid" id="issueDetailAtts">';
    html += '<div class="att-upload" onclick="doUpload(\'issue_ticket\',\'' + esc(it.id) + '\',\'issue_photo\')">+</div>';
    html += '</div></div>';
    html += '</div>';
  }

  // P1-6：记帐操作（已完成/已关闭后可标记需记帐）
  var canMarkAcct = (it.status === 'completed' || it.status === 'closed');
  var hasAcctData = Number(it.accounting_required) === 1 || Number(it.accounted) === 1;
  if (canMarkAcct || hasAcctData) {
    html += '<div class="card"><div class="card-title">记帐 / 기장</div>';
    if (Number(it.accounting_required) === 1) {
      html += '<div class="detail-field"><b>提示记帐:</b> ' + esc(it.accounting_required_by || '') + ' · ' + esc(fmtTime(it.accounting_required_at)) + '</div>';
      if (it.accounting_note) {
        html += '<div class="detail-field"><b>记帐备注:</b> <span style="white-space:pre-wrap;">' + esc(it.accounting_note) + '</span></div>';
      }
    }
    if (Number(it.accounted) === 1) {
      html += '<div class="detail-field"><b>已记帐:</b> ' + esc(it.accounted_by || '') + ' · ' + esc(fmtTime(it.accounted_at)) + '</div>';
    }
    if (canMarkAcct && Number(it.accounted) !== 1) {
      if (Number(it.accounting_required) !== 1) {
        html += '<button class="btn btn-warning" onclick="markIssueAccountingRequired(this)">提示记帐 / 기장 알림</button>';
      } else {
        html += '<button class="btn btn-success" onclick="markIssueAccounted(this)">标记已记帐 / 기장 완료</button> ';
        html += '<button class="btn btn-outline" onclick="markIssueAccountingRequired(this)">修改记帐备注</button>';
      }
    }
    html += '</div>';
  }

  body.innerHTML = html;
}

async function markIssueAccountingRequired(btnEl) {
  var note = prompt('记帐备注（如：额外贴标 50 箱、换箱 12 个、补操作费等）:');
  if (note === null) return;
  withActionLock('markIssueAccountingRequired', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_issue_mark_accounting_required",
      id: _currentIssueId,
      accounting_note: note,
      by: getUser()
    });
    if (res && res.ok) {
      alert('已标记需记帐 / 기장 알림 완료');
      loadIssueDetail();
    } else {
      alert('失败: ' + (res ? (res.message || res.error) : 'unknown'));
    }
  });
}

async function markIssueAccounted(btnEl) {
  if (!confirm('确认已记帐？此操作记录由 ' + getUser() + ' 完成。')) return;
  withActionLock('markIssueAccounted', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({ action: "v2_issue_mark_accounted", id: _currentIssueId, by: getUser() });
    if (res && res.ok) {
      alert('已记帐 / 기장 완료');
      loadIssueDetail();
    } else {
      alert('失败: ' + (res ? (res.message || res.error) : 'unknown'));
    }
  });
}

async function completeIssue(btnEl) {
  if (!confirm(L("confirm_complete_issue") + "?")) return;
  withActionLock('completeIssue', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({ action: "v2_issue_close", id: _currentIssueId });
    if (res && res.ok) {
      alert(L("status_completed"));
      loadIssueDetail();
    } else {
      alert("失败: " + (res ? res.error : "unknown"));
    }
  });
}

async function reworkIssue(btnEl) {
  var note = prompt(L("rework_prompt"));
  if (!note || !note.trim()) return;
  withActionLock('reworkIssue', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({ action: "v2_issue_rework", id: _currentIssueId, rework_note: note.trim() });
    if (res && res.ok) {
      alert(L("rework_sent"));
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
  var btn = document.getElementById("obQueryBtn");
  if (btn) btn.disabled = true;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var start = document.getElementById("obFilterStart").value;
  var end = document.getElementById("obFilterEnd").value;
  var status = document.getElementById("obFilterStatus").value;
  var accountedSel = document.getElementById("outboundFilterAccounted");
  var accounted = accountedSel ? accountedSel.value : "";
  var bizSel = document.getElementById("obFilterBizClass");
  var biz_class = bizSel ? bizSel.value : "";
  var usesSel = document.getElementById("obFilterUsesStockOp");
  var uses_stock_operation = usesSel ? usesSel.value : "";
  var matSel = document.getElementById("obFilterHasMaterial");
  var has_material = matSel ? matSel.value : "";
  var custEl = document.getElementById("obFilterCustomer");
  var customer_keyword = custEl ? (custEl.value || '').trim() : "";

  var res;
  try {
    res = await api({
      action: "v2_outbound_order_list",
      start_date: start, end_date: end, status: status, accounted: accounted,
      biz_class: biz_class,
      customer_keyword: customer_keyword,
      uses_stock_operation: uses_stock_operation,
      has_material: has_material,
      limit: 50
    });
  } finally {
    if (btn) btn.disabled = false;
  }
  if (!res || !res.ok) {
    var errMsg = (res && (res.message || res.error)) || '网络异常 / 네트워크 오류';
    body.innerHTML = '<div class="card muted" style="color:#c62828;">加载失败 / 로딩 실패: ' + esc(String(errMsg)) + '</div>';
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
    if (o.biz_class) {
      html += '<span class="st" style="background:#e3f2fd;color:#1565c0;">[' + esc(bizLabel(o.biz_class)) + ']</span> ';
    }
    if (Number(o.uses_stock_operation) === 1) {
      html += '<span class="st" style="background:#fff3e0;color:#e65100;">[' + esc(L("uses_stock_operation")) + ']</span> ';
    }
    if (Number(o.warehouse_ack_required) === 1) {
      html += '<span class="st" style="background:#ffebee;color:#c62828;">⚠ 已变更待确认</span> ';
    }
    var matCount = Number(o.material_count || 0);
    if (matCount > 0) {
      html += '<span class="st" style="background:#e8f5e9;color:#2e7d32;">[' + esc(L("outbound_material_uploaded")) + ' ' + matCount + ']</span> ';
    } else {
      html += '<span class="st" style="background:#f5f5f5;color:#888;">[' + esc(L("outbound_material_missing")) + ']</span> ';
    }
    html += accountTag(o);
    html += esc(o.display_no || o.id) + ' · ' + esc(o.customer || "--");
    html += '</div>';
    // 优先显示预计出库时间；为空 fallback 到作业单日期
    var dateLabel = '';
    if (o.expected_ship_at) {
      dateLabel = '预计出库 ' + esc(o.expected_ship_at);
    } else {
      dateLabel = '作业单日期 ' + esc(o.order_date || '--');
    }
    var meta = dateLabel;
    if (o.destination) meta += ' · ' + esc(o.destination);
    if (o.wms_work_order_no) meta += ' · ' + esc(o.wms_work_order_no);
    if (o.outbound_mode) meta += ' · ' + esc(outModeLabel(o.outbound_mode));
    if (o.planned_box_count) meta += ' · ' + L("planned_prefix") + o.planned_box_count + L("unit_box");
    if (o.planned_pallet_count) meta += ' · ' + o.planned_pallet_count + L("unit_pallet");
    meta += ' · ' + esc(fmtTime(o.created_at));
    if (o.accounted == 1 && (o.accounted_by || o.accounted_at)) {
      meta += ' · ' + L("accounted_by_short") + ': ' + esc(o.accounted_by || "") + (o.accounted_at ? ' ' + esc(fmtTime(o.accounted_at)) : '');
    }
    html += '<div class="item-meta">' + meta + '</div>';
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
  tr.innerHTML = '<td><input type="text" id="ocl-sku-' + _obLineCount + '"></td>' +
    '<td><input type="number" id="ocl-qty-' + _obLineCount + '" value="0"></td>' +
    '<td><button class="btn btn-outline btn-sm" onclick="this.parentElement.parentElement.remove()">×</button></td>';
  tbody.appendChild(tr);
}

// 出库新建表单的附件累积（HTML <input multiple> 二次选择会替换上次选择，
// 因此必须用 JS 数组累积，避免"上传两次只保留一次"的视觉错觉）
var _obCreateMaterials = [];

function _ocMaterialsKey(f) {
  return (f && f.name || '') + '|' + (f && f.size || 0) + '|' + (f && f.lastModified || 0);
}

function _renderOcMaterialsList() {
  var box = document.getElementById('ocMaterialsList');
  if (!box) return;
  if (_obCreateMaterials.length === 0) {
    box.innerHTML = '<div class="muted" style="font-size:12px;">尚未选择任何文件 / 파일 미선택</div>';
    return;
  }
  var html = '<div style="font-size:12px;margin-top:4px;">已选文件 / 선택된 파일 (' + _obCreateMaterials.length + ')：</div>';
  html += '<ul style="font-size:12px;margin:4px 0 0 0;padding-left:18px;list-style:disc;">';
  _obCreateMaterials.forEach(function(f, idx) {
    var sizeKb = Math.round(((f && f.size) || 0) / 1024);
    html += '<li style="margin:2px 0;">' + esc(f.name || '?') + ' <span class="muted">(' + sizeKb + ' KB)</span> '
         + '<a href="javascript:void(0);" onclick="removeOcMaterial(' + idx + ')" style="color:#c62828;">移除 / 제거</a></li>';
  });
  html += '</ul>';
  box.innerHTML = html;
}

function onOcMaterialsChange(inputEl) {
  if (!inputEl || !inputEl.files) return;
  var seen = {};
  _obCreateMaterials.forEach(function(f) { seen[_ocMaterialsKey(f)] = 1; });
  var newFiles = Array.prototype.slice.call(inputEl.files);
  var added = 0;
  for (var i = 0; i < newFiles.length; i++) {
    var key = _ocMaterialsKey(newFiles[i]);
    if (seen[key]) continue;
    _obCreateMaterials.push(newFiles[i]);
    seen[key] = 1;
    added++;
  }
  // 立刻清空 input.value，使再次选择"同名文件/同一组文件"也能触发 change
  inputEl.value = "";
  _renderOcMaterialsList();
}

function removeOcMaterial(idx) {
  if (idx < 0 || idx >= _obCreateMaterials.length) return;
  _obCreateMaterials.splice(idx, 1);
  _renderOcMaterialsList();
}

function clearOcMaterials() {
  _obCreateMaterials = [];
  _renderOcMaterialsList();
}

async function submitOutbound(btnEl) {
  var customer = document.getElementById("oc-customer").value.trim();
  if (!customer) { alert(L("customer") + "!"); return; }
  var bizSel = document.getElementById("oc-biz-class");
  var biz_class = bizSel ? bizSel.value.trim() : "";
  if (!biz_class) { alert(L("biz_class") + "!"); return; }
  var outmodeCheck = document.getElementById("oc-outmode").value.trim();
  if (!outmodeCheck) { alert(L("select_outbound_mode")); return; }
  withActionLock('submitOutbound', btnEl || null, '提交中.../저장중...', async function() {
    var destination = document.getElementById("oc-destination").value.trim();
    var po_no = document.getElementById("oc-po").value.trim();
    var wms_wo = document.getElementById("oc-wms-wo").value.trim();
    var outmode = document.getElementById("oc-outmode").value.trim();
    var instruction = document.getElementById("oc-instruction").value.trim();
    var plannedBox = parseInt(document.getElementById("oc-planned-box").value) || 0;
    var plannedPallet = parseInt(document.getElementById("oc-planned-pallet").value) || 0;
    var usesEl = document.getElementById("oc-uses-stock-op");
    var uses_stock_operation = usesEl && usesEl.value === "1" ? 1 : 0;
    var expEl = document.getElementById("oc-expected-ship-at");
    var expected_ship_at = expEl ? expEl.value.trim() : "";
    var reqEl = document.getElementById("oc-outbound-requirement");
    var outbound_requirement = reqEl ? reqEl.value.trim() : "";

    // 作业单日期（order_date）自动取自预计出库时间的日期部分；都没有就用今日
    // hidden 输入仍保留，用户/调试端可手动覆盖
    var dateEl = document.getElementById("oc-date");
    var manualDate = dateEl ? dateEl.value.trim() : "";
    var date = manualDate || (expected_ship_at ? expected_ship_at.slice(0, 10) : "") || kstToday();

    var matFiles = _obCreateMaterials.slice(0);

    // Collect lines
    var lines = [];
    var rows = document.getElementById("ocLinesBody").querySelectorAll("tr");
    rows.forEach(function(tr) {
      var id = tr.id.replace("oc-line-", "");
      var sku = (document.getElementById("ocl-sku-" + id) || {}).value || "";
      var qty = parseInt((document.getElementById("ocl-qty-" + id) || {}).value) || 0;
      if (sku || qty > 0) lines.push({ sku: sku, quantity: qty });
    });

    var res = await api({
      action: "v2_outbound_order_create",
      order_date: date,
      customer: customer,
      biz_class: biz_class,
      uses_stock_operation: uses_stock_operation,
      destination: destination,
      po_no: po_no,
      wms_work_order_no: wms_wo,
      outbound_mode: outmode,
      instruction: instruction,
      expected_ship_at: expected_ship_at,
      outbound_requirement: outbound_requirement,
      planned_box_count: plannedBox,
      planned_pallet_count: plannedPallet,
      created_by: getUser(),
      lines: lines
    });

    if (!res || !res.ok) {
      alert("失败: " + (res ? (res.message || res.error) : "unknown"));
      return;
    }

    var newOrderId = res.id;
    var displayNo = res.display_no || res.id;

    // 上传出库资料（创建成功后再上传，related_doc_id=order_id）
    var failedNames = [];
    if (matFiles.length > 0 && newOrderId) {
      for (var i = 0; i < matFiles.length; i++) {
        try {
          var ok = await uploadOutboundMaterial(newOrderId, matFiles[i]);
          if (!ok) failedNames.push(matFiles[i].name || '?');
        } catch (e) {
          failedNames.push((matFiles[i] && matFiles[i].name) || '?');
        }
      }
    }

    if (failedNames.length > 0) {
      alert("出库单已创建：" + displayNo + "\n但部分资料上传失败，请进入详情补传 / 출고단 생성됨, 일부 자료 업로드 실패:\n" + failedNames.join("\n"));
    } else {
      alert("已创建 / 생성됨: " + displayNo);
    }

    // 清空表单
    document.getElementById("oc-customer").value = "";
    if (bizSel) bizSel.value = "";
    if (usesEl) usesEl.value = "0";
    document.getElementById("oc-destination").value = "";
    document.getElementById("oc-po").value = "";
    document.getElementById("oc-wms-wo").value = "";
    document.getElementById("oc-outmode").value = "";
    document.getElementById("oc-instruction").value = "";
    if (expEl) expEl.value = "";
    if (reqEl) reqEl.value = "";
    var matEl = document.getElementById("oc-materials");
    if (matEl) matEl.value = "";
    clearOcMaterials();
    document.getElementById("oc-planned-box").value = "0";
    document.getElementById("oc-planned-pallet").value = "0";
    document.getElementById("ocLinesBody").innerHTML = "";
    _obLineCount = 0;
    goTab("outbound");
  });
}

// 出库资料文件上传 helper（复用 uploadFile）
async function uploadOutboundMaterial(orderId, file) {
  var fd = new FormData();
  fd.append("file", file);
  fd.append("related_doc_type", "outbound_order");
  fd.append("related_doc_id", orderId);
  fd.append("attachment_category", "outbound_material");
  fd.append("uploaded_by", getUser() || "");
  var resp = await uploadFile(fd);
  return resp && resp.ok;
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

  // Cache order data for printing
  window._currentOutboundOrderCache = o;
  window._currentOutboundLinesCache = lines;

  var obDisplayNo = o.display_no || o.id;
  var usesStockOp = Number(o.uses_stock_operation) === 1;
  var html = '<div class="card">';
  html += '<div style="font-size:16px;font-weight:700;margin-bottom:8px;">' + esc(obDisplayNo) + '</div>';
  html += '<div class="detail-field"><b>' + L("status") + ':</b> <span class="st st-' + esc(o.status) + '">' + esc(stLabel(o.status)) + '</span>';
  if (o.biz_class) {
    html += ' <span class="st" style="background:#e3f2fd;color:#1565c0;">[' + esc(bizLabel(o.biz_class)) + ']</span>';
  }
  if (usesStockOp) {
    html += ' <span class="st" style="background:#fff3e0;color:#e65100;">[' + esc(L("uses_stock_operation")) + ']</span>';
  }
  if (Number(o.revision_no || 0) > 0) {
    html += ' <span class="st" style="background:#fff3e0;color:#ef6c00;">已修改 #' + Number(o.revision_no) + '</span>';
  }
  if (Number(o.warehouse_ack_required) === 1) {
    html += ' <span class="st" style="background:#ffebee;color:#c62828;">⚠ 待仓库确认变更 / 창고 확인 대기</span>';
  }
  html += '</div>';
  if (Number(o.revision_no || 0) > 0 && o.last_modified_by) {
    html += '<div class="detail-field muted" style="font-size:12px;"><b>最近修改 / 최근 수정:</b> ' + esc(o.last_modified_by) + (o.last_modified_at ? ' · ' + esc(fmtTime(o.last_modified_at)) : '') + '</div>';
  }
  if (Number(o.warehouse_ack_required) === 0 && o.warehouse_ack_by) {
    html += '<div class="detail-field muted" style="font-size:12px;"><b>仓库已确认 / 창고 확인됨:</b> ' + esc(o.warehouse_ack_by) + (o.warehouse_ack_at ? ' · ' + esc(fmtTime(o.warehouse_ack_at)) : '') + '</div>';
  }
  html += '<div class="detail-field"><b>' + L("order_date") + ':</b> ' + esc(o.order_date) + '</div>';
  html += '<div class="detail-field"><b>' + L("customer") + ':</b> ' + esc(o.customer) + '</div>';
  if (o.destination) html += '<div class="detail-field"><b>' + L("destination") + ':</b> ' + esc(o.destination) + '</div>';
  if (o.po_no) html += '<div class="detail-field"><b>' + L("po_no") + ':</b> ' + esc(o.po_no) + '</div>';
  if (o.wms_work_order_no) html += '<div class="detail-field"><b>' + L("wms_work_order_no") + ':</b> ' + esc(o.wms_work_order_no) + '</div>';
  html += '<div class="detail-field"><b>' + L("outbound_mode") + ':</b> ' + esc(outModeLabel(o.outbound_mode)) + '</div>';
  if (o.expected_ship_at) html += '<div class="detail-field"><b>' + L("expected_ship_at") + ':</b> ' + esc(o.expected_ship_at) + '</div>';
  if (o.outbound_requirement) html += '<div class="detail-section"><b>' + L("outbound_requirement") + ':</b><div style="margin-top:4px;white-space:pre-wrap;">' + esc(o.outbound_requirement) + '</div></div>';
  if (o.instruction) html += '<div class="detail-section"><b>' + L("instruction") + ':</b><div style="margin-top:4px;white-space:pre-wrap;">' + esc(o.instruction) + '</div></div>';
  html += '<div class="detail-field"><b>' + L("planned_box_pallet") + ':</b> ' + (o.planned_box_count || 0) + L("unit_box") + ' / ' + (o.planned_pallet_count || 0) + L("unit_pallet") + '</div>';
  html += '<div class="detail-field"><b>' + L("actual_box_pallet") + ':</b> ' + (o.actual_box_count || 0) + L("unit_box") + ' / ' + (o.actual_pallet_count || 0) + L("unit_pallet") + '</div>';
  html += '<div class="detail-field"><b>' + L("submitted_by") + ':</b> ' + esc(o.created_by) + ' · ' + esc(fmtTime(o.created_at)) + '</div>';
  if (o.source_inbound_plan_id) {
    html += '<div class="detail-field"><b>来源入库计划 / 출처 입고:</b> <a href="javascript:void(0);" onclick="openInboundDetail(\'' + esc(o.source_inbound_plan_id) + '\')">查看入库单 / 입고단 보기</a></div>';
  }
  html += '</div>';

  // 库内操作型：pending_outbound_update 提示 + 仓库反馈结果 + 更新出库计划按钮
  if (usesStockOp) {
    var stockOpStatus = o.stock_operation_status || '';
    var stockResult = null;
    try { stockResult = JSON.parse(o.stock_operation_result_json || "null"); } catch(e) {}
    if (stockOpStatus === 'completed' || stockResult) {
      var stockOpHeadline;
      if (o.status === 'pending_outbound_update') {
        stockOpHeadline = (getLang() === 'ko'
          ? '창고 재고 작업 완료, 고객센터의 출고 계획 업데이트 대기중'
          : '库内操作已完成，等待客服更新出库计划');
      } else if (o.status === 'preparing_outbound') {
        stockOpHeadline = (getLang() === 'ko'
          ? '창고 재고 작업 완료, 출고 준비중'
          : '库内操作已完成，出库准备中');
      } else if (o.status === 'shipped') {
        stockOpHeadline = (getLang() === 'ko'
          ? '창고 재고 작업 완료, 출고완료'
          : '库内操作已完成，已出库');
      } else {
        stockOpHeadline = (getLang() === 'ko' ? '창고 재고 작업 완료' : '库内操作已完成');
      }
      html += '<div class="card" style="border-left:4px solid #ef6c00;">';
      html += '<div class="card-title" style="color:#ef6c00;">' + esc(stockOpHeadline) + '</div>';
      if (o.stock_operation_completed_by) {
        html += '<div class="detail-field"><b>' + esc(L("submitted_by")) + ':</b> ' + esc(o.stock_operation_completed_by) + ' · ' + esc(fmtTime(o.stock_operation_completed_at)) + '</div>';
      }
      if (stockResult) {
        html += '<div class="detail-field"><b>' + esc(L("actual_box_pallet")) + ':</b> ' +
          Number(stockResult.total_box_count || 0) + esc(L("unit_box")) + ' / ' +
          Number(stockResult.total_pallet_count || 0) + esc(L("unit_pallet")) + '</div>';
        if (stockResult.last_remark) {
          html += '<div class="detail-field"><b>' + esc(L("remark")) + ':</b> ' + esc(stockResult.last_remark) + '</div>';
        }
        if (Array.isArray(stockResult.results) && stockResult.results.length > 0) {
          html += '<details style="margin-top:6px;"><summary style="cursor:pointer;color:#666;">明细 (' + stockResult.results.length + ')</summary>';
          html += '<table class="line-table"><thead><tr><th>箱</th><th>托</th><th>备注</th><th>提交人</th><th>时间</th></tr></thead><tbody>';
          stockResult.results.forEach(function(r) {
            html += '<tr><td>' + Number(r.box_count || 0) + '</td><td>' + Number(r.pallet_count || 0) + '</td><td>' + esc(r.remark || '') + '</td><td>' + esc(r.created_by || '') + '</td><td>' + esc(fmtTime(r.created_at)) + '</td></tr>';
          });
          html += '</tbody></table></details>';
        }
      }
      html += '</div>';
    }
    if (o.status === 'pending_outbound_update') {
      html += '<div class="card" style="border-left:4px solid #c62828;background:#fff8f6;">';
      html += '<div style="font-weight:700;color:#c62828;">' + esc(L("update_ship_plan_hint")) + '</div>';
      html += '<div style="margin-top:8px;">';
      html += '<button class="btn btn-primary" onclick="openShipPlanForm()">' + esc(L("update_ship_plan")) + '</button>';
      html += '</div>';
      html += '</div>';
    }
  }

  // Lines
  if (lines.length > 0) {
    html += '<div class="card"><div class="card-title">明细行 (' + lines.length + ')</div>';
    html += '<table class="line-table"><thead><tr><th>' + L("sku") + '</th><th>' + L("quantity") + '</th></tr></thead><tbody>';
    lines.forEach(function(ln) {
      html += '<tr><td>' + esc(ln.sku) + '</td><td>' + (ln.quantity || 0) + '</td></tr>';
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
      html += esc(orderOpsJobTypeText(j.job_type)) + ' · ' + esc(fmtTime(j.created_at));
      if (result.box_count) html += ' · 箱:' + result.box_count;
      if (result.pallet_count) html += ' · 托:' + result.pallet_count;
      if (result.remark) html += ' · ' + esc(result.remark);
      html += '</div>';
    });
    html += '</div>';
  }

  // 出库资料（attachment_category = 'outbound_material'）
  var outboundMaterials = atts.filter(function(a) { return a.attachment_category === 'outbound_material'; });
  html += '<div class="card"><div class="card-title">' + esc(L("outbound_materials")) + ' (' + outboundMaterials.length + ')</div>';
  if (outboundMaterials.length === 0) {
    html += '<div class="muted">' + esc(L("outbound_materials_empty")) + '</div>';
  } else {
    html += '<table class="line-table"><thead><tr><th>文件名</th><th>上传人</th><th>时间</th><th>操作</th></tr></thead><tbody>';
    outboundMaterials.forEach(function(att) {
      var url = fileUrl(att.file_key);
      var ct = (att.content_type || '').toLowerCase();
      var fn = (att.file_name || '').toLowerCase();
      var isPdf = ct.indexOf('pdf') !== -1 || fn.endsWith('.pdf');
      var isImg = ct.startsWith('image/');
      var openLabel = isPdf ? L("open_print") : (isImg ? L("open_print") : L("download_print"));
      html += '<tr>';
      html += '<td>' + esc(att.file_name) + '</td>';
      html += '<td>' + esc(att.uploaded_by || '--') + '</td>';
      html += '<td>' + esc(fmtTime(att.created_at)) + '</td>';
      html += '<td>';
      html += '<a class="btn btn-outline btn-sm" href="' + esc(url) + '" download="' + esc(att.file_name) + '">' + esc(L("download")) + '</a> ';
      html += '<a class="btn btn-outline btn-sm" href="' + esc(url) + '" target="_blank" rel="noopener">' + esc(openLabel) + '</a>';
      html += '</td>';
      html += '</tr>';
    });
    html += '</tbody></table>';
  }
  // 客服在详情页补传按钮
  html += '<div style="margin-top:8px;">';
  html += '<input id="ob-detail-upload-input" type="file" multiple accept=".xlsx,.xls,.csv,.pdf,.jpg,.jpeg,.png" style="display:none;" onchange="uploadOutboundMaterialsFromDetail(this)">';
  html += '<button class="btn btn-outline btn-sm" onclick="document.getElementById(\'ob-detail-upload-input\').click()">+ ' + esc(L("outbound_materials")) + '</button>';
  html += '</div>';
  html += '</div>';

  // Attachments — grouped by category（车辆照片 / 其它非出库资料）
  var vehiclePhotos = atts.filter(function(a) { return a.attachment_category === 'vehicle_photo' || a.attachment_category === 'load_vehicle_photo'; });
  var otherAtts = atts.filter(function(a) { return a.attachment_category !== 'vehicle_photo' && a.attachment_category !== 'load_vehicle_photo' && a.attachment_category !== 'outbound_material'; });
  if (vehiclePhotos.length > 0) {
    html += '<div class="card"><div class="card-title">' + L("vehicle_photos") + ' (' + vehiclePhotos.length + ')</div>';
    html += '<div class="att-grid">';
    vehiclePhotos.forEach(function(att) {
      html += '<img class="att-thumb" src="' + esc(fileUrl(att.file_key)) + '" onclick="showLightbox(\'' + esc(fileUrl(att.file_key)) + '\')">';
    });
    html += '</div></div>';
  }
  if (otherAtts.length > 0) {
    html += '<div class="card"><div class="card-title">' + L("attachments") + ' (' + otherAtts.length + ')</div>';
    html += '<div class="att-grid">';
    otherAtts.forEach(function(att) {
      if (att.content_type && att.content_type.startsWith("image/")) {
        html += '<img class="att-thumb" src="' + esc(fileUrl(att.file_key)) + '" onclick="showLightbox(\'' + esc(fileUrl(att.file_key)) + '\')">';
      } else {
        html += '<div style="font-size:12px;">' + esc(att.file_name) + '</div>';
      }
    });
    html += '</div></div>';
  }

  // P1-9 提货信息卡片（仅未冻结状态可编辑；已冻结仅展示）
  var FROZEN_OB2 = ['shipped', 'completed', 'cancelled'];
  var pickupCanEdit = FROZEN_OB2.indexOf(o.status) === -1;
  var hasPickupAny = o.pickup_vehicle_no || o.pickup_driver_name || o.pickup_driver_phone ||
                     o.pickup_person_name || o.pickup_company || o.pickup_time || o.pickup_note;
  html += '<div class="card">';
  html += '<div class="card-title">📋 提货信息 / 픽업 정보';
  if (Number(o.pickup_confirm_required) === 1) {
    html += ' <span class="st" style="background:#fff3e0;color:#e65100;">⚠ 待仓库确认 / 창고 확인 대기</span>';
  } else if (o.pickup_confirmed_by) {
    html += ' <span class="st" style="background:#e8f5e9;color:#2e7d32;">✓ 仓库已确认 / 창고 확인됨</span>';
  }
  html += '</div>';
  if (hasPickupAny) {
    html += '<div style="font-size:13px;line-height:1.7;">';
    if (o.pickup_vehicle_no) html += '<div><b>车牌 / 차번:</b> ' + esc(o.pickup_vehicle_no) + '</div>';
    if (o.pickup_driver_name || o.pickup_driver_phone) {
      html += '<div><b>司机 / 기사:</b> ' + esc(o.pickup_driver_name || '--');
      if (o.pickup_driver_phone) html += ' · ' + esc(o.pickup_driver_phone);
      html += '</div>';
    }
    if (o.pickup_person_name || o.pickup_company) {
      html += '<div><b>提货人 / 픽업 담당자:</b> ' + esc(o.pickup_person_name || '--');
      if (o.pickup_company) html += ' (' + esc(o.pickup_company) + ')';
      html += '</div>';
    }
    if (o.pickup_time) html += '<div><b>提货时间 / 픽업 시간:</b> ' + esc(o.pickup_time) + '</div>';
    if (o.pickup_note) html += '<div><b>备注 / 비고:</b> ' + esc(o.pickup_note) + '</div>';
    html += '</div>';
    if (Number(o.pickup_confirm_required) === 0 && o.pickup_confirmed_by) {
      html += '<div class="muted" style="font-size:11px;margin-top:6px;">已确认: ' + esc(o.pickup_confirmed_by) + (o.pickup_confirmed_at ? ' · ' + esc(fmtTime(o.pickup_confirmed_at)) : '') + '</div>';
    }
  } else {
    html += '<div class="muted">暂无提货信息 / 픽업 정보 없음</div>';
  }
  if (pickupCanEdit) {
    html += '<div style="margin-top:10px;"><button class="btn btn-outline btn-sm" onclick="openPickupEditForm()">编辑提货信息 / 픽업 정보 편집</button></div>';
  }
  html += '</div>';

  // 记账标记
  html += renderAccountedCard({
    accounted: Number(o.accounted || 0),
    accounted_by: o.accounted_by || '',
    accounted_at: o.accounted_at || '',
    onMark: "markOutboundAccounted(1, this)",
    onUnmark: "markOutboundAccounted(0, this)"
  });

  // Print + Status actions
  html += '<div class="card">';
  html += '<button class="btn btn-outline btn-sm" onclick="printOutboundOrder()">' + L("print") + '</button> ';
  // P1-8：未冻结状态下允许修改
  var FROZEN_OB = ['shipped', 'completed', 'cancelled'];
  if (FROZEN_OB.indexOf(o.status) === -1) {
    html += '<button class="btn btn-primary" onclick="openOutboundEditForm()">修改出库单 / 출고단 수정</button> ';
  }
  if (o.status !== "shipped" && o.status !== "cancelled") {
    html += '<button class="btn btn-danger" onclick="updateObStatus(\'cancelled\', this)">' + L("status_cancelled") + '</button>';
  }
  if (o.status === "ready_to_ship") {
    html += '<button class="btn btn-primary" onclick="updateObStatus(\'reopen_pending\', this)">' + L("set_reopen_pending") + '</button>';
  }
  if (o.status === "reopen_pending") {
    html += '<button class="btn btn-danger" onclick="updateObStatus(\'cancelled\', this)">' + L("status_cancelled") + '</button>';
  }
  // 已取消单 → 显示删除
  if (o.status === "cancelled") {
    html += '<button class="btn btn-danger" style="background:#c62828;border-color:#c62828;" onclick="deleteOutboundOrder(this)">🗑 ' + L("delete_outbound") + '</button>';
  }
  html += '</div>';

  body.innerHTML = html;
}

// 统一记账卡片渲染（入库/出库共用）
function renderAccountedCard(opts) {
  var a = opts.accounted === 1;
  var html = '<div class="card">';
  html += '<div class="card-title">记账标记 / 기장 표시</div>';
  if (a) {
    html += '<div style="color:#2e7d32;font-weight:700;">● 已记账 / 기장 완료</div>';
    html += '<div class="muted" style="font-size:13px;margin-top:4px;">操作人: ' + esc(opts.accounted_by || '--') + ' · ' + esc(fmtTime(opts.accounted_at)) + '</div>';
    html += '<div style="margin-top:8px;"><button class="btn btn-outline btn-sm" onclick="' + opts.onUnmark + '">取消记账 / 기장 취소</button></div>';
  } else {
    html += '<div style="color:#e67e22;font-weight:700;">● 未记账 / 미기장</div>';
    html += '<div style="margin-top:8px;"><button class="btn btn-primary btn-sm" onclick="' + opts.onMark + '">标记已记账 / 기장 완료 표시</button></div>';
  }
  html += '</div>';
  return html;
}

async function markOutboundAccounted(accounted, btnEl) {
  var op = getUser();
  if (accounted === 1 && !op) { alert("请先在右上角设置你的显示名"); return; }
  var msg = accounted === 1 ? "确认标记为【已记账】？" : "确认取消记账？";
  if (!confirm(msg)) return;
  withActionLock('markOutboundAccounted', btnEl || null, '提交中...', async function() {
    var res = await api({
      action: "v2_outbound_order_mark_accounted",
      id: _currentOutboundId,
      accounted: accounted,
      operator_name: op
    });
    if (!res || !res.ok) {
      alert("失败: " + (res ? (res.message || res.error) : "unknown"));
      return;
    }
    loadOutboundDetail();
  });
}

async function updateObStatus(status, btnEl) {
  if (!confirm(L("confirm") + "?")) return;
  withActionLock('updateObStatus_' + status, btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({ action: "v2_outbound_order_update_status", id: _currentOutboundId, status: status });
    if (res && res.ok) {
      loadOutboundDetail();
    } else {
      alert("失败: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

// ===== 库内操作完成后客服更新出库计划 =====
function openShipPlanForm() {
  var o = window._currentOutboundOrderCache || {};
  var html = '';
  html += '<div class="card" id="ob-shipplan-card">';
  html += '<div class="card-title">' + esc(L("update_ship_plan")) + '</div>';
  html += '<div class="form-group"><label>' + esc(L("expected_ship_at")) + '</label>';
  html += '<input id="sp-expected-ship-at" type="datetime-local" value="' + esc(o.expected_ship_at || "") + '"></div>';
  html += '<div class="form-group"><label>' + esc(L("outbound_mode")) + '</label>';
  html += '<select id="sp-outbound-mode">';
  html += '<option value="">--</option>';
  ['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'].forEach(function(m) {
    var sel = (o.outbound_mode === m) ? ' selected' : '';
    html += '<option value="' + m + '"' + sel + '>' + esc(outModeLabel(m)) + '</option>';
  });
  html += '</select></div>';
  html += '<div class="form-group"><label>' + esc(L("destination")) + '</label>';
  html += '<input id="sp-destination" type="text" value="' + esc(o.destination || "") + '"></div>';
  html += '<div class="form-group"><label>' + esc(L("outbound_requirement")) + '</label>';
  html += '<textarea id="sp-outbound-requirement" rows="3">' + esc(o.outbound_requirement || "") + '</textarea></div>';
  html += '<div class="form-group"><label>' + esc(L("remark")) + '</label>';
  html += '<textarea id="sp-remark" rows="2"></textarea></div>';
  html += '<button class="btn btn-primary" onclick="submitShipPlanUpdate(this)">' + esc(L("save")) + '</button> ';
  html += '<button class="btn btn-outline" onclick="document.getElementById(\'ob-shipplan-card\').remove()">' + esc(L("cancel")) + '</button>';
  html += '</div>';
  // 注入到详情顶部
  var body = document.getElementById("outboundDetailBody");
  if (body) {
    var wrap = document.createElement("div");
    wrap.innerHTML = html;
    body.insertBefore(wrap.firstChild, body.firstChild);
    wrap.firstChild && window.scrollTo({top: 0, behavior: 'smooth'});
  }
}

async function submitShipPlanUpdate(btnEl) {
  if (!_currentOutboundId) return;
  if (!confirm(L("confirm_update_ship_plan"))) return;
  var expEl = document.getElementById("sp-expected-ship-at");
  var modeEl = document.getElementById("sp-outbound-mode");
  var destEl = document.getElementById("sp-destination");
  var reqEl = document.getElementById("sp-outbound-requirement");
  var remarkEl = document.getElementById("sp-remark");
  var expected_ship_at = expEl ? expEl.value.trim() : "";
  var outbound_mode = modeEl ? modeEl.value.trim() : "";
  var destination = destEl ? destEl.value.trim() : "";
  var outbound_requirement = reqEl ? reqEl.value.trim() : "";
  var remark = remarkEl ? remarkEl.value.trim() : "";
  if (!expected_ship_at && !outbound_requirement) {
    alert(L("expected_ship_at") + " / " + L("outbound_requirement") + " 至少填一项");
    return;
  }
  withActionLock('submitShipPlanUpdate', btnEl || null, '提交中.../저장중...', async function() {
    var res = await api({
      action: "v2_outbound_order_update_ship_plan",
      id: _currentOutboundId,
      expected_ship_at: expected_ship_at,
      outbound_mode: outbound_mode,
      destination: destination,
      outbound_requirement: outbound_requirement,
      remark: remark
    });
    if (res && res.ok) {
      alert("已更新 / 업데이트 완료");
      loadOutboundDetail();
    } else {
      alert("失败: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

// 详情页补传出库资料
async function uploadOutboundMaterialsFromDetail(inputEl) {
  if (!_currentOutboundId) return;
  var files = inputEl && inputEl.files ? Array.prototype.slice.call(inputEl.files) : [];
  if (files.length === 0) return;
  var failed = 0;
  for (var i = 0; i < files.length; i++) {
    var ok = await uploadOutboundMaterial(_currentOutboundId, files[i]);
    if (!ok) failed++;
  }
  inputEl.value = "";
  if (failed > 0) alert("部分文件上传失败: " + failed + " 个");
  else alert("上传成功 / 업로드 완료");
  loadOutboundDetail();
}

// ===== Outbound Print =====
function printOutboundOrder() {
  var o = window._currentOutboundOrderCache;
  var lines = window._currentOutboundLinesCache || [];
  if (!o) { alert("无数据"); return; }

  var displayNo = o.display_no || o.id;

  // QR code
  var qrHtml = '';
  try { qrHtml = buildInboundQrHtml(displayNo, 3); } catch(e) { qrHtml = ''; }

  // Outbound mode label
  var modeText = outModeLabel(o.outbound_mode);

  // Lines table
  var linesHtml = '';
  if (lines.length > 0) {
    linesHtml = '<table><thead><tr><th>SKU</th><th>' + L("quantity") + '</th></tr></thead><tbody>';
    lines.forEach(function(ln) {
      linesHtml += '<tr><td>' + esc(ln.sku || '') + '</td><td>' + (ln.quantity || 0) + '</td></tr>';
    });
    linesHtml += '</tbody></table>';
  }

  var win = window.open('', '_blank');
  var html = '<!doctype html><html><head><meta charset="utf-8"/><title>' + esc(displayNo) + '</title>' +
    '<style>' +
    'body{font-family:"Microsoft YaHei","Helvetica Neue",Arial,sans-serif;margin:20px 30px;color:#000;}' +
    '.print-header{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:3px solid #000;padding-bottom:10px;margin-bottom:14px;}' +
    '.print-title{font-size:22px;font-weight:900;}' +
    '.print-sub{font-size:13px;color:#333;margin-top:4px;}' +
    '.qr-box{text-align:center;flex-shrink:0;margin-left:20px;}' +
    '.qr-box svg{width:100px;height:100px;}' +
    '.qr-label{font-size:10px;color:#666;margin-top:2px;line-height:1.3;}' +
    '.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px 24px;font-size:13px;margin-bottom:14px;}' +
    '.info-grid .label{font-weight:700;}' +
    'table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px;}' +
    'th,td{border:1px solid #333;padding:5px 6px;text-align:left;}' +
    'th{background:#eee;font-weight:700;}' +
    '.count-grid{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:4px 16px;font-size:13px;margin-bottom:14px;border:1px solid #999;padding:8px 12px;}' +
    '.count-grid .label{font-weight:700;}' +
    '.count-grid .val{font-size:18px;font-weight:900;}' +
    '.sig-row{display:flex;gap:40px;margin-top:30px;font-size:13px;}' +
    '.sig-item{flex:1;}' +
    '.sig-line{border-bottom:1px solid #333;height:30px;margin-top:4px;}' +
    '.footer{margin-top:16px;font-size:11px;color:#888;text-align:center;border-top:1px dashed #ccc;padding-top:6px;}' +
    '@media print{@page{size:A4;margin:15mm 20mm;} body{margin:0;}}' +
    '</style></head><body>' +

    // Header
    '<div class="print-header">' +
      '<div>' +
        '<div class="print-title">出库作业单</div>' +
        '<div class="print-sub">CK 仓储</div>' +
      '</div>' +
      '<div class="qr-box">' + qrHtml + '<div class="qr-label">' + esc(displayNo) + '</div></div>' +
    '</div>' +

    // Info grid
    '<div class="info-grid">' +
      '<div><span class="label">出库单号：</span>' + esc(displayNo) + '</div>' +
      '<div><span class="label">日期：</span>' + esc(o.order_date || '') + '</div>' +
      '<div><span class="label">客户：</span>' + esc(o.customer || '') + '</div>' +
      '<div><span class="label">出库模式：</span>' + esc(modeText) + '</div>' +
      (o.destination ? '<div><span class="label">目的地：</span>' + esc(o.destination) + '</div>' : '') +
      (o.po_no ? '<div><span class="label">PO号/발주번호：</span>' + esc(o.po_no) + '</div>' : '') +
      (o.wms_work_order_no ? '<div><span class="label">WMS工单号：</span>' + esc(o.wms_work_order_no) + '</div>' : '') +
      '<div><span class="label">提出人：</span>' + esc(o.created_by || '') + '</div>' +
      (o.instruction ? '<div style="grid-column:1/-1;"><span class="label">作业说明：</span>' + esc(o.instruction) + '</div>' : '') +
    '</div>' +

    // Box/pallet counts
    '<div class="count-grid">' +
      '<div><div class="label">计划箱数</div><div class="val">' + (o.planned_box_count || 0) + '</div></div>' +
      '<div><div class="label">计划托数</div><div class="val">' + (o.planned_pallet_count || 0) + '</div></div>' +
      '<div><div class="label">实际箱数</div><div class="val">' + (o.actual_box_count || 0) + '</div></div>' +
      '<div><div class="label">实际托数</div><div class="val">' + (o.actual_pallet_count || 0) + '</div></div>' +
    '</div>' +

    // Lines table
    linesHtml +

    // Signature row
    '<div class="sig-row">' +
      '<div class="sig-item"><span class="label">制单人：</span>' + esc(o.created_by || '') + '<div class="sig-line"></div></div>' +
      '<div class="sig-item"><span class="label">仓库确认：</span><div class="sig-line"></div></div>' +
      '<div class="sig-item"><span class="label">客户签收：</span><div class="sig-line"></div></div>' +
      '<div class="sig-item"><span class="label">日期：</span><div class="sig-line"></div></div>' +
    '</div>' +

    '<div class="footer">Printed from CK Warehouse V2</div>' +
    '<script>window.onload=function(){window.print();}<\/script>' +
    '</body></html>';
  win.document.write(html);
  win.document.close();

  if (o.status === "pending_issue") {
    api({ action: "v2_outbound_order_update_status", id: o.id, status: "issued" }).then(function(res) {
      if (res && res.ok) loadOutboundDetail();
    });
  }
}

// ===== Inbound List =====
async function loadInboundList() {
  var body = document.getElementById("inboundListBody");
  if (!body) return;
  var btn = document.getElementById("ibQueryBtn");
  if (btn) btn.disabled = true;
  body.innerHTML = '<div class="card muted">' + L("loading") + '</div>';

  var start = document.getElementById("ibFilterStart").value;
  var end = document.getElementById("ibFilterEnd").value;
  var status = document.getElementById("ibFilterStatus").value;
  var accountedSel = document.getElementById("inboundFilterAccounted");
  var accounted = accountedSel ? accountedSel.value : "";
  var bizSel = document.getElementById("ibFilterBizClass");
  var biz_class = bizSel ? bizSel.value : "";
  var custEl = document.getElementById("ibFilterCustomer");
  var customer_keyword = custEl ? (custEl.value || '').trim() : "";

  var res;
  try {
    res = await api({
      action: "v2_inbound_plan_list",
      start_date: start, end_date: end, status: status, accounted: accounted,
      biz_class: biz_class,
      customer_keyword: customer_keyword,
      limit: 50
    });
  } finally {
    if (btn) btn.disabled = false;
  }
  if (!res || !res.ok) {
    var errMsg = (res && (res.message || res.error)) || '网络异常 / 네트워크 오류';
    body.innerHTML = '<div class="card muted" style="color:#c62828;">加载失败 / 로딩 실패: ' + esc(String(errMsg)) + '</div>';
    return;
  }

  var items = (res.items || []).filter(function(p) { return p.source_type !== 'return_session'; });
  if (items.length === 0) {
    body.innerHTML = '<div class="card muted">' + L("no_data") + '</div>';
    return;
  }
  // plan_date 倒序，同日按 created_at 倒序（最新计划在上）
  items.sort(function(a, b) {
    var da = String(a.plan_date || ''), db = String(b.plan_date || '');
    if (da !== db) return db.localeCompare(da);
    return String(b.created_at || '').localeCompare(String(a.created_at || ''));
  });

  var html = '<div class="card">';
  items.forEach(function(p) {
    var dynTag = '';
    if (p.source_type === 'from_feedback') {
      dynTag = '<span style="background:#8e24aa;color:#fff;font-size:10px;padding:1px 4px;border-radius:2px;margin-right:4px;">反馈转正</span>';
    } else if (p.source_type === 'field_dynamic') {
      dynTag = '<span style="background:#ff9800;color:#fff;font-size:10px;padding:1px 4px;border-radius:2px;margin-right:4px;">动态</span>';
    }
    html += '<div class="list-item" onclick="openInboundDetail(\'' + esc(p.id) + '\')">';
    html += '<div class="item-title">';
    html += '<span class="st st-' + esc(p.status) + '">' + esc(inboundStatusLabel(p.status)) + '</span> ';
    html += accountTag(p);
    html += dynTag;
    // 多业务类型 tag（兼容老数据：列表后端注入 biz_classes，缺则回退 biz_class 单值）
    var bizArr = (p.biz_classes && p.biz_classes.length) ? p.biz_classes : (p.biz_class ? [p.biz_class] : []);
    for (var bi = 0; bi < bizArr.length; bi++) {
      html += '<span class="biz-tag biz-' + esc(bizArr[bi]) + '" style="margin-right:4px;">' + esc(bizLabel(bizArr[bi])) + '</span>';
    }
    html += ' ' + esc(p.display_no || p.id) + ' · ' + esc(p.customer || "--") + ' · ' + esc(p.cargo_summary || "");
    html += '</div>';
    var ibMeta = esc(p.plan_date || "") + ' · ' + esc(p.expected_arrival || "") + ' · ' + esc(fmtTime(p.created_at));
    // line summary：箱/托/件 — 仅显示有数量的项
    var lineSum = p.line_summary || {};
    var lineParts = [];
    var unitOrder = ['carton','pallet','container_large','container_small','cbm'];
    for (var ui = 0; ui < unitOrder.length; ui++) {
      var ut = unitOrder[ui];
      var q = Number(lineSum[ut] || 0);
      if (q > 0) lineParts.push(unitTypeLabel(ut) + ' ' + (Number.isInteger(q) ? q : q.toFixed(1)));
    }
    if (lineParts.length > 0) ibMeta += ' · ' + lineParts.join(' / ');
    if (Number(p.related_outbound_count || 0) > 0) {
      ibMeta += ' · 关联出库 ' + Number(p.related_outbound_count) + ' 单';
    }
    if (p.accounted == 1 && (p.accounted_by || p.accounted_at)) {
      ibMeta += ' · ' + L("accounted_by_short") + ': ' + esc(p.accounted_by || "") + (p.accounted_at ? ' ' + esc(fmtTime(p.accounted_at)) : '');
    }
    html += '<div class="item-meta">' + ibMeta + '</div>';
    // 未完成入库类型醒目提示（仅当多业务类型 + 至少一个 pending 时显示）
    var missing = p.missing_biz_classes || p.pending_biz_classes || [];
    if (bizArr.length > 1 && missing.length > 0 && p.status !== 'completed' && p.status !== 'cancelled') {
      var missText = missing.map(function(b) { return inboundBizTaskLabel(b); }).join('、');
      html += '<div style="font-size:12px;color:#c62828;background:#ffebee;border-left:3px solid #c62828;padding:3px 6px;margin-top:4px;">⚠️ 未完成入库类型 / 미완료 입고 유형: ' + esc(missText) + '</div>';
    }
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

// P1-4 关联出库计划（多行）
var _ibcLinkObSeq = 0;

function toggleIbcLinkOb() {
  var checked = document.getElementById("ibc-link-ob").checked;
  var panel = document.getElementById("ibcLinkObPanel");
  if (panel) panel.style.display = checked ? "" : "none";
  // 第一次打开：自动加一行
  if (checked) {
    var rows = document.getElementById("ibcLinkObRows");
    if (rows && !rows.children.length) addIbcLinkObRow();
  }
}

function addIbcLinkObRow() {
  _ibcLinkObSeq++;
  var seq = _ibcLinkObSeq;
  var rows = document.getElementById("ibcLinkObRows");
  if (!rows) return;
  // 默认带过来：客户用入库的客户（提交时再读取，无需此处填）
  var div = document.createElement("div");
  div.id = "ibcLinkOb-row-" + seq;
  div.style.cssText = "border:1px solid #d0d0d0;border-radius:6px;padding:8px;margin-bottom:8px;background:#fff;";
  var html = '';
  html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">';
  html += '<div style="font-weight:700;font-size:13px;color:#1565c0;">出库计划 #' + seq + '</div>';
  html += '<button type="button" class="btn btn-outline btn-sm" onclick="removeIbcLinkObRow(' + seq + ')" style="color:#c62828;">移除</button>';
  html += '</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:13px;">';
  html += '<div><label><b>客户</b><span style="color:#888;font-weight:normal;font-size:11px;"> (留空=入库客户)</span></label><input id="lob-customer-' + seq + '" class="input" placeholder="留空则使用入库的客户"></div>';
  html += '<div><label><b>业务分类 *</b></label><select id="lob-biz-' + seq + '" class="input">';
  html += '<option value="">--</option>';
  html += '<option value="direct_ship">代发 / 직배송</option>';
  html += '<option value="bulk" selected>大货 / 대량화물</option>';
  html += '<option value="return">退件 / 반품</option>';
  html += '</select></div>';
  html += '<div><label><b>出库模式 *</b></label><select id="lob-mode-' + seq + '" class="input">';
  html += '<option value="">--</option>';
  html += '<option value="warehouse_dispatch">仓库叫车 / 창고 차량 배차</option>';
  html += '<option value="customer_pickup">客户自提 / 고객 자가 픽업</option>';
  html += '<option value="milk_express">牛奶快递 / 밀크런 택배</option>';
  html += '<option value="milk_pallet">牛奶托盘 / 밀크런 팔레트</option>';
  html += '<option value="container_pickup">柜子提货 / 컨테이너 픽업</option>';
  html += '</select></div>';
  html += '<div><label><b>是否库内操作</b></label><select id="lob-uses-' + seq + '" class="input">';
  html += '<option value="0">否：普通出库</option>';
  html += '<option value="1">是：先库内操作</option>';
  html += '</select></div>';
  html += '<div><label><b>预计出库时间</b></label><input id="lob-expected-' + seq + '" class="input" type="datetime-local"></div>';
  html += '<div><label><b>计划箱数</b></label><input id="lob-box-' + seq + '" class="input" type="number" min="0" value="0"></div>';
  html += '<div><label><b>计划托数</b></label><input id="lob-pallet-' + seq + '" class="input" type="number" min="0" value="0"></div>';
  html += '<div style="grid-column:1/-1;"><label><b>出库要求</b></label><textarea id="lob-req-' + seq + '" class="input" rows="2"></textarea></div>';
  html += '<div style="grid-column:1/-1;"><label><b>备注</b></label><textarea id="lob-remark-' + seq + '" class="input" rows="1"></textarea></div>';
  html += '</div>';
  div.innerHTML = html;
  rows.appendChild(div);
}

function removeIbcLinkObRow(seq) {
  var el = document.getElementById("ibcLinkOb-row-" + seq);
  if (el) el.parentNode.removeChild(el);
}

function getIbcLinkObRows() {
  var rows = document.querySelectorAll("#ibcLinkObRows > div[id^='ibcLinkOb-row-']");
  var out = [];
  for (var i = 0; i < rows.length; i++) {
    var seq = rows[i].id.replace("ibcLinkOb-row-", "");
    out.push({
      seq: seq,
      customer: ((document.getElementById("lob-customer-" + seq) || {}).value || "").trim(),
      biz_class: ((document.getElementById("lob-biz-" + seq) || {}).value || "").trim(),
      outbound_mode: ((document.getElementById("lob-mode-" + seq) || {}).value || "").trim(),
      uses_stock_operation: Number((document.getElementById("lob-uses-" + seq) || {}).value || 0),
      expected_ship_at: ((document.getElementById("lob-expected-" + seq) || {}).value || "").trim(),
      planned_box_count: Number((document.getElementById("lob-box-" + seq) || {}).value || 0),
      planned_pallet_count: Number((document.getElementById("lob-pallet-" + seq) || {}).value || 0),
      outbound_requirement: ((document.getElementById("lob-req-" + seq) || {}).value || "").trim(),
      remark: ((document.getElementById("lob-remark-" + seq) || {}).value || "").trim()
    });
  }
  return out;
}

function getIbcBizClasses() {
  var nodes = document.querySelectorAll('#ibc-biz-classes .ibc-biz-chk');
  var arr = [];
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].checked) arr.push(nodes[i].value);
  }
  return arr;
}

async function submitInbound(btnEl) {
  var customer = document.getElementById("ibc-customer").value.trim();
  if (!customer) { alert("请填写客户 / 고객을 입력하세요"); return; }
  // 业务类型至少选一个
  var bizArr = getIbcBizClasses();
  if (bizArr.length === 0) { alert("请至少选择一个业务类型（代发/大货/退件）/ 업무 유형을 1개 이상 선택하세요"); return; }
  // 严格校验：必须至少有一行 planned_qty>0 的明细
  var linesPre = getIbcLines();
  if (linesPre.length === 0) { alert("请至少填写一行货物明细 / 화물 명세를 1건 이상 입력하세요"); return; }

  // P1-4 关联出库计划行 — 校验
  var linkOb = document.getElementById("ibc-link-ob");
  var linkObOn = !!(linkOb && linkOb.checked);
  var linkObRows = linkObOn ? getIbcLinkObRows() : [];
  if (linkObOn) {
    if (linkObRows.length === 0) { alert("已勾选'关联出库计划'但未添加任何行 / 출고 계획이 없습니다"); return; }
    for (var li = 0; li < linkObRows.length; li++) {
      var r = linkObRows[li];
      if (!r.biz_class) { alert("出库计划 #" + r.seq + " 缺少业务分类 / 업무 분류 필수"); return; }
      if (!r.outbound_mode) { alert("出库计划 #" + r.seq + " 缺少出库模式 / 출고 모드 필수"); return; }
    }
  }

  withActionLock('submitInbound', btnEl || null, '提交中.../저장중...', async function() {
    var date = document.getElementById("ibc-date").value || kstToday();
    var biz_classes = bizArr;
    var biz = biz_classes[0]; // 兼容旧字段
    var cargo = document.getElementById("ibc-cargo").value.trim();
    var arrival = document.getElementById("ibc-arrival").value.trim();
    var purpose = document.getElementById("ibc-purpose").value.trim();
    var remark = document.getElementById("ibc-remark").value.trim();
    var lines = linesPre;
    // 货物摘要为空时，自动用明细生成
    if (!cargo) {
      cargo = lines.map(function(ln) { return unitTypeLabel(ln.unit_type) + ' ' + ln.planned_qty; }).join(' / ');
    }

    // 第一步：创建入库计划（不再用 auto_create_outbound 单条）
    var ibRes = await api({
      action: "v2_inbound_plan_create",
      plan_date: date,
      customer: customer,
      biz_class: biz,
      biz_classes: biz_classes,
      cargo_summary: cargo,
      expected_arrival: arrival,
      purpose: purpose,
      remark: remark,
      lines: lines,
      created_by: getUser()
    });

    if (!ibRes || !ibRes.ok) {
      alert("失败 / 실패: " + (ibRes ? (ibRes.message || ibRes.error) : "unknown"));
      return;
    }
    var planId = ibRes.id;
    var planDispNo = ibRes.display_no || planId;

    // 第二步：循环创建关联出库单
    var createdObs = [];
    var failedObs = [];
    for (var i = 0; i < linkObRows.length; i++) {
      var row = linkObRows[i];
      var obRes = await api({
        action: "v2_outbound_order_create",
        order_date: date,
        customer: row.customer || customer,
        biz_class: row.biz_class,
        outbound_mode: row.outbound_mode,
        uses_stock_operation: row.uses_stock_operation,
        expected_ship_at: row.expected_ship_at,
        planned_box_count: row.planned_box_count,
        planned_pallet_count: row.planned_pallet_count,
        outbound_requirement: row.outbound_requirement,
        remark: row.remark,
        source_inbound_plan_id: planId,
        created_by: getUser()
      });
      if (obRes && obRes.ok) {
        createdObs.push(obRes.display_no || obRes.id);
      } else {
        failedObs.push("#" + row.seq + ": " + ((obRes && (obRes.message || obRes.error)) || 'unknown'));
      }
    }

    var msg = "已创建入库计划 / 입고 계획 생성: " + planDispNo;
    if (createdObs.length > 0) msg += "\n关联出库 / 연결된 출고 (" + createdObs.length + "): " + createdObs.join(", ");
    if (failedObs.length > 0) msg += "\n出库创建失败 / 출고 생성 실패:\n" + failedObs.join("\n");
    alert(msg);

    // 重置表单
    document.getElementById("ibc-customer").value = "";
    document.getElementById("ibc-cargo").value = "";
    document.getElementById("ibc-arrival").value = "";
    document.getElementById("ibc-purpose").value = "";
    document.getElementById("ibc-remark").value = "";
    document.getElementById("ibcLinesBody").innerHTML = "";
    if (linkOb) linkOb.checked = false;
    var panel = document.getElementById("ibcLinkObPanel");
    if (panel) panel.style.display = "none";
    var rowsHolder = document.getElementById("ibcLinkObRows");
    if (rowsHolder) rowsHolder.innerHTML = "";
    var chks = document.querySelectorAll('#ibc-biz-classes .ibc-biz-chk');
    for (var ci = 0; ci < chks.length; ci++) chks[ci].checked = false;
    ensureFirstIbcLine();
    goTab("inbound");
  });
}

// 确保入库创建表单始终存在至少一行明细
function ensureFirstIbcLine() {
  var tbody = document.getElementById("ibcLinesBody");
  if (tbody && !tbody.children.length && typeof addIbcLine === "function") addIbcLine();
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
  // 缓存 plan 给 printIbQr / 二维码 A4 单据使用
  window._currentInboundPlanCache = p;
  window._currentInboundPretty = p.display_no || p.id;
  window._currentInboundPlan = p; // P1-7 修改弹窗用

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
  // 多业务类型 tag
  var detailBizArr = (res.biz_classes && res.biz_classes.length) ? res.biz_classes :
                     ((p.biz_classes && p.biz_classes.length) ? p.biz_classes : (p.biz_class ? [p.biz_class] : []));
  var bizTagsHtml = '';
  for (var dbi = 0; dbi < detailBizArr.length; dbi++) {
    bizTagsHtml += '<span class="biz-tag biz-' + esc(detailBizArr[dbi]) + '" style="margin-right:4px;">' + esc(bizLabel(detailBizArr[dbi])) + '</span>';
  }
  html += '<div><b>' + L("biz_class") + ':</b> ' + (bizTagsHtml || '--') + '</div>';
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

  // --- P1-4 关联出库单（按 source_inbound_plan_id 反查）---
  var linkedObs = res.linked_outbound_orders || [];
  if (linkedObs.length > 0) {
    html += '<div class="card">';
    html += '<div class="card-title">关联出库单 / 연결된 출고단 (' + linkedObs.length + ')</div>';
    html += '<table class="line-table"><thead><tr><th>单号</th><th>状态</th><th>客户</th><th>业务</th><th>出库模式</th><th>预计出库</th><th>箱/托</th></tr></thead><tbody>';
    linkedObs.forEach(function(ob) {
      html += '<tr style="cursor:pointer;" onclick="openOutboundDetail(\'' + esc(ob.id) + '\')">';
      html += '<td><a href="javascript:void(0);" onclick="event.stopPropagation();openOutboundDetail(\'' + esc(ob.id) + '\')">' + esc(ob.display_no || ob.id) + '</a></td>';
      html += '<td><span class="st st-' + esc(ob.status) + '">' + esc(stLabel(ob.status)) + '</span>';
      if (Number(ob.uses_stock_operation) === 1) html += ' <span class="st" style="background:#fff3e0;color:#e65100;font-size:10px;">[库内操作]</span>';
      html += '</td>';
      html += '<td>' + esc(ob.customer || '--') + '</td>';
      html += '<td>' + (ob.biz_class ? '<span class="biz-tag biz-' + esc(ob.biz_class) + '">' + esc(bizLabel(ob.biz_class)) + '</span>' : '--') + '</td>';
      html += '<td>' + esc(outModeLabel(ob.outbound_mode) || '--') + '</td>';
      html += '<td>' + esc(ob.expected_ship_at || '--') + '</td>';
      html += '<td>' + Number(ob.planned_box_count || 0) + '/' + Number(ob.planned_pallet_count || 0) + '</td>';
      html += '</tr>';
    });
    html += '</tbody></table>';
    html += '</div>';
  }

  // --- 入库类型执行状态卡片 ---
  var bizTasks = res.biz_tasks || [];
  if (!isReturnSession && bizTasks.length > 0) {
    var pendingTasks = bizTasks.filter(function(t) { return t.status !== 'completed'; });
    var headerColor = pendingTasks.length === 0 ? '#1b5e20' : '#c62828';
    html += '<div class="card">';
    html += '<div class="card-title" style="display:flex;align-items:center;gap:8px;">入库类型执行状态 / 입고 유형별 작업 상태';
    if (pendingTasks.length > 0) {
      var missText2 = pendingTasks.map(function(t) { return inboundBizTaskLabel(t.biz_class); }).join('、');
      html += '<span style="font-size:11px;color:#fff;background:' + headerColor + ';padding:2px 6px;border-radius:3px;">未完成 / 미완료: ' + esc(missText2) + '</span>';
    } else {
      html += '<span style="font-size:11px;color:#fff;background:' + headerColor + ';padding:2px 6px;border-radius:3px;">全部完成 / 전체 완료</span>';
    }
    html += '</div>';
    html += '<table class="line-table"><thead><tr>';
    html += '<th>业务类型 / 업무</th><th>对应操作 / 작업</th><th>状态 / 상태</th><th>完成人 / 완료자</th><th>完成时间 / 완료시간</th><th>工时(分钟) / 작업시간</th><th>作业</th>';
    html += '</tr></thead><tbody>';
    bizTasks.forEach(function(t) {
      var stClass = (t.status === 'completed') ? 'st-completed' : 'st-pending';
      var stText = (t.status === 'completed') ? (getLang() === 'ko' ? '완료' : '已完成') : (getLang() === 'ko' ? '미완료' : '未完成');
      html += '<tr>';
      html += '<td><span class="biz-tag biz-' + esc(t.biz_class) + '">' + esc(bizLabel(t.biz_class)) + '</span></td>';
      html += '<td>' + esc(inboundBizTaskLabel(t.biz_class)) + '</td>';
      html += '<td><span class="st ' + stClass + '">' + esc(stText) + '</span></td>';
      // 完成人优先显示 worker_names（姓名串），fallback 到 completed_by（worker_id）
      var doneBy = t.worker_names || t.completed_by || '';
      html += '<td>' + esc(doneBy || '--') + '</td>';
      html += '<td>' + esc(t.completed_at ? fmtTime(t.completed_at) : '--') + '</td>';
      html += '<td>' + (t.total_minutes ? Math.round(t.total_minutes) : '--') + '</td>';
      if (t.job_id) {
        html += '<td><a href="javascript:void(0)" onclick="event.stopPropagation();openOrderOpsDetail(\'' + esc(t.job_id) + '\')">查看 / 보기</a></td>';
      } else {
        html += '<td>--</td>';
      }
      html += '</tr>';
    });
    html += '</tbody></table>';
    html += '</div>';
  }

  // --- Plan lines ---
  var unloadNotDone002 = (p.status === 'unloading' || p.status === 'unloading_putting_away');
  if (lines.length > 0) {
    var hasPutaway = lines.some(function(ln) { return (ln.putaway_qty || 0) > 0; });
    html += '<div class="card"><div class="card-title">' + L("plan_lines") + '</div>';
    html += '<table class="line-table"><thead><tr><th>' + L("biz_class") + '</th><th>' + L("planned_qty") + '</th><th>' + L("actual_qty") + '</th>';
    if (hasPutaway) html += '<th>实际入库</th><th>差异</th>';
    else html += '<th>' + L("diff") + '</th>';
    html += '</tr></thead><tbody>';
    lines.forEach(function(ln) {
      var actualQty = ln.actual_qty || 0;
      var actualDisplay = (unloadNotDone002 && actualQty === 0) ? '<span style="color:#e67e22;">卸货中</span>' : String(actualQty);
      html += '<tr><td>' + esc(unitTypeLabel(ln.unit_type)) + '</td><td>' + ln.planned_qty + '</td><td>' + actualDisplay + '</td>';
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
      var jobLabel = orderOpsJobTypeText(j.job_type);
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
    html += '<div><label><b>' + L("biz_class") + '</b></label><select id="dynBiz" class="input"><option value="">--</option><option value="direct_ship"' + (p.biz_class === 'direct_ship' ? ' selected' : '') + '>' + bizLabel('direct_ship') + '</option><option value="bulk"' + (p.biz_class === 'bulk' ? ' selected' : '') + '>' + bizLabel('bulk') + '</option><option value="return"' + (p.biz_class === 'return' ? ' selected' : '') + '>' + bizLabel('return') + '</option><option value="change_order"' + (p.biz_class === 'change_order' ? ' selected' : '') + '>' + bizLabel('change_order') + '</option><option value="import"' + (p.biz_class === 'import' ? ' selected' : '') + '>' + bizLabel('import') + '</option></select></div>';
    html += '<div style="grid-column:1/-1;"><label><b>' + L("cargo_summary") + '</b></label><input id="dynCargo" class="input" value="' + esc(p.cargo_summary) + '"></div>';
    html += '<div><label><b>' + L("expected_arrival") + '</b></label><input id="dynArrival" class="input" value="' + esc(p.expected_arrival) + '"></div>';
    html += '<div><label><b>' + L("purpose") + '</b></label><input id="dynPurpose" class="input" value="' + esc(p.purpose) + '"></div>';
    html += '<div style="grid-column:1/-1;"><label><b>' + L("remark") + '</b></label><input id="dynRemark" class="input" value="' + esc(p.remark) + '"></div>';
    html += '</div>';
    html += '<div style="margin-top:10px;"><button class="btn btn-success" onclick="finalizeDynamicPlan(this)">确认转正为入库单</button></div>';
    html += '</div>';
  }

  // 记账标记
  html += renderAccountedCard({
    accounted: Number(p.accounted || 0),
    accounted_by: p.accounted_by || '',
    accounted_at: p.accounted_at || '',
    onMark: "markInboundAccounted(1, this)",
    onUnmark: "markInboundAccounted(0, this)"
  });

  // --- Actions ---
  html += '<div class="card">';
  html += '<button class="btn btn-outline btn-sm" onclick="printIbQr()">' + L("print") + '</button> ';
  if (p.status === "arrived_pending_putaway" || p.status === "putting_away" || p.status === "partially_completed") {
    html += '<button class="btn btn-success" onclick="markInboundCompleted(this)">文员直接完成入库 / 직접 입고 완료</button> ';
  }
  // P1-7：未到库（pending）才能修改
  if (p.status === "pending") {
    html += '<button class="btn btn-primary" onclick="openInboundEditForm()">修改入库计划 / 입고 계획 수정</button> ';
  }
  if (p.status === "pending" || p.status === "arrived_pending_putaway") {
    html += '<button class="btn btn-danger btn-sm" onclick="cancelInboundPlan(this)">' + L("status_cancelled") + '</button>';
  }
  // 已取消计划 → 显示删除
  if (p.status === "cancelled") {
    html += '<button class="btn btn-danger" style="background:#c62828;border-color:#c62828;" onclick="deleteInboundPlan(this)">🗑 ' + L("delete_inbound") + '</button>';
  }
  html += '</div>';

  body.innerHTML = html;
}

// P1-7：未到库入库计划 — 修改弹窗
function openInboundEditForm() {
  var p = window._currentInboundPlan;
  if (!p || p.status !== 'pending') {
    alert('仅未到库（待到库）状态的入库单可以修改');
    return;
  }
  var bizList = [];
  try { bizList = JSON.parse(p.biz_classes_json || '[]'); } catch(e) {}
  if (bizList.length === 0 && p.biz_class) bizList = [p.biz_class];
  var bizCheck = function(v) { return bizList.indexOf(v) !== -1 ? ' checked' : ''; };

  var html = '';
  html += '<div class="modal-overlay" id="ibEditOverlay" onclick="closeInboundEditForm(event)">';
  html += '<div class="modal-box" onclick="event.stopPropagation();" style="max-width:560px;">';
  html += '<div style="font-size:16px;font-weight:700;margin-bottom:12px;">修改入库计划 / 입고 계획 수정</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px;">';
  html += '<div><label><b>计划日期</b></label><input id="ib-edit-date" class="input" type="date" value="' + esc(p.plan_date || '') + '"></div>';
  html += '<div><label><b>客户 *</b></label><input id="ib-edit-customer" class="input" value="' + esc(p.customer || '') + '"></div>';
  html += '<div style="grid-column:1/-1;"><label><b>业务类型（多选） *</b></label><div style="padding:6px 0;">';
  html += '<label style="margin-right:14px;"><input type="checkbox" class="ib-edit-biz" value="direct_ship"' + bizCheck('direct_ship') + '> 代发 / 직배송</label>';
  html += '<label style="margin-right:14px;"><input type="checkbox" class="ib-edit-biz" value="bulk"' + bizCheck('bulk') + '> 大货 / 대량화물</label>';
  html += '<label style="margin-right:14px;"><input type="checkbox" class="ib-edit-biz" value="return"' + bizCheck('return') + '> 退件 / 반품</label>';
  html += '<label style="margin-right:14px;"><input type="checkbox" class="ib-edit-biz" value="change_order"' + bizCheck('change_order') + '> 换单 / 송장교체</label>';
  html += '</div></div>';
  html += '<div><label><b>预计到达</b></label><input id="ib-edit-arrival" class="input" value="' + esc(p.expected_arrival || '') + '"></div>';
  html += '<div><label><b>入库目的</b></label><input id="ib-edit-purpose" class="input" value="' + esc(p.purpose || '') + '"></div>';
  html += '<div style="grid-column:1/-1;"><label><b>货物概要</b></label><input id="ib-edit-cargo" class="input" value="' + esc(p.cargo_summary || '') + '"></div>';
  html += '<div style="grid-column:1/-1;"><label><b>备注</b></label><textarea id="ib-edit-remark" class="input" rows="2">' + esc(p.remark || '') + '</textarea></div>';
  html += '</div>';
  html += '<div style="margin-top:14px;text-align:right;">';
  html += '<button class="btn btn-outline" onclick="closeInboundEditForm()">取消</button> ';
  html += '<button class="btn btn-primary" onclick="submitInboundEdit(this)">保存</button>';
  html += '</div></div></div>';
  var div = document.createElement('div');
  div.innerHTML = html;
  document.body.appendChild(div.firstChild);
}

function closeInboundEditForm(ev) {
  if (ev && ev.target && ev.target.id !== 'ibEditOverlay') return;
  var el = document.getElementById('ibEditOverlay');
  if (el) el.parentNode.removeChild(el);
}

async function submitInboundEdit(btnEl) {
  var customer = (document.getElementById('ib-edit-customer') || {}).value || '';
  if (!customer.trim()) { alert('请填写客户'); return; }
  var bizList = [];
  document.querySelectorAll('.ib-edit-biz:checked').forEach(function(el) { bizList.push(el.value); });
  if (bizList.length === 0) { alert('请至少选择一个业务类型'); return; }
  withActionLock('submitInboundEdit', btnEl || null, '保存中.../저장중...', async function() {
    var res = await api({
      action: "v2_inbound_plan_update",
      id: _currentInboundId,
      plan_date: (document.getElementById('ib-edit-date') || {}).value || '',
      customer: customer.trim(),
      biz_classes: bizList,
      expected_arrival: (document.getElementById('ib-edit-arrival') || {}).value || '',
      purpose: (document.getElementById('ib-edit-purpose') || {}).value || '',
      cargo_summary: (document.getElementById('ib-edit-cargo') || {}).value || '',
      remark: (document.getElementById('ib-edit-remark') || {}).value || ''
    });
    if (res && res.ok) {
      alert('已保存 / 저장됨');
      closeInboundEditForm();
      loadInboundDetail();
    } else {
      alert('失败/실패: ' + (res ? (res.message || res.error) : 'unknown'));
    }
  });
}

// ===== P1-8 出库单修改（非冻结状态）=====
function openOutboundEditForm() {
  var o = window._currentOutboundOrderCache;
  if (!o) { alert('订单未加载 / 주문 미로드'); return; }
  var FROZEN = ['shipped', 'completed', 'cancelled'];
  if (FROZEN.indexOf(o.status) !== -1) {
    alert('已出库/完成/取消的出库单不能修改 / 출고완료/완료/취소된 단은 수정 불가');
    return;
  }
  var bizSel = function(v) { return o.biz_class === v ? ' selected' : ''; };
  var modeSel = function(v) { return o.outbound_mode === v ? ' selected' : ''; };
  var usesSel = function(v) { return Number(o.uses_stock_operation || 0) === v ? ' selected' : ''; };

  var html = '';
  html += '<div class="modal-overlay" id="obEditOverlay" onclick="closeOutboundEditForm(event)">';
  html += '<div class="modal-box" onclick="event.stopPropagation();" style="max-width:640px;">';
  html += '<div style="font-size:16px;font-weight:700;margin-bottom:12px;">修改出库单 / 출고단 수정';
  if (Number(o.revision_no || 0) > 0) html += ' <span class="muted" style="font-size:12px;font-weight:400;">(当前版本 #' + Number(o.revision_no) + ')</span>';
  html += '</div>';
  html += '<div style="background:#fff8e1;border-left:3px solid #ef6c00;padding:8px;margin-bottom:10px;font-size:12px;color:#6d4c00;">⚠ 保存后会通知仓库重新确认 / 저장 후 창고 재확인 필요</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px;">';
  html += '<div><label><b>单据日期</b></label><input id="ob-edit-date" class="input" type="date" value="' + esc(o.order_date || '') + '"></div>';
  html += '<div><label><b>客户 *</b></label><input id="ob-edit-customer" class="input" value="' + esc(o.customer || '') + '"></div>';
  html += '<div><label><b>业务分类 *</b></label><select id="ob-edit-biz" class="input">';
  html += '<option value=""' + (o.biz_class ? '' : ' selected') + '>--</option>';
  html += '<option value="direct_ship"' + bizSel('direct_ship') + '>代发 / 직배송</option>';
  html += '<option value="bulk"' + bizSel('bulk') + '>大货 / 대량화물</option>';
  html += '<option value="return"' + bizSel('return') + '>退件 / 반품</option>';
  html += '</select></div>';
  html += '<div><label><b>出库模式</b></label><select id="ob-edit-outmode" class="input">';
  html += '<option value="">--</option>';
  ['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'].forEach(function(m) {
    html += '<option value="' + m + '"' + modeSel(m) + '>' + esc(outModeLabel(m)) + '</option>';
  });
  html += '</select></div>';
  html += '<div><label><b>是否使用现货</b></label><select id="ob-edit-uses-stock" class="input">';
  html += '<option value="0"' + usesSel(0) + '>否：普通出库</option>';
  html += '<option value="1"' + usesSel(1) + '>是：先库内操作</option>';
  html += '</select></div>';
  html += '<div><label><b>预计出库时间</b></label><input id="ob-edit-expected" class="input" type="datetime-local" value="' + esc(o.expected_ship_at || '') + '"></div>';
  html += '<div><label><b>目的地</b></label><input id="ob-edit-destination" class="input" value="' + esc(o.destination || '') + '"></div>';
  html += '<div><label><b>PO号</b></label><input id="ob-edit-po" class="input" value="' + esc(o.po_no || '') + '"></div>';
  html += '<div><label><b>WMS工单号</b></label><input id="ob-edit-wms" class="input" value="' + esc(o.wms_work_order_no || '') + '"></div>';
  html += '<div><label><b>计划箱数</b></label><input id="ob-edit-box" class="input" type="number" min="0" value="' + Number(o.planned_box_count || 0) + '"></div>';
  html += '<div><label><b>计划托数</b></label><input id="ob-edit-pallet" class="input" type="number" min="0" value="' + Number(o.planned_pallet_count || 0) + '"></div>';
  html += '<div style="grid-column:1/-1;"><label><b>出库要求</b></label><textarea id="ob-edit-req" class="input" rows="2">' + esc(o.outbound_requirement || '') + '</textarea></div>';
  html += '<div style="grid-column:1/-1;"><label><b>作业说明</b></label><textarea id="ob-edit-instruction" class="input" rows="2">' + esc(o.instruction || '') + '</textarea></div>';
  html += '<div style="grid-column:1/-1;"><label><b>备注</b></label><textarea id="ob-edit-remark" class="input" rows="2">' + esc(o.remark || '') + '</textarea></div>';
  html += '</div>';
  html += '<div style="margin-top:14px;text-align:right;">';
  html += '<button class="btn btn-outline" onclick="closeOutboundEditForm()">取消 / 취소</button> ';
  html += '<button class="btn btn-primary" onclick="submitOutboundEdit(this)">保存 / 저장</button>';
  html += '</div></div></div>';
  var div = document.createElement('div');
  div.innerHTML = html;
  document.body.appendChild(div.firstChild);
}

function closeOutboundEditForm(ev) {
  if (ev && ev.target && ev.target.id !== 'obEditOverlay') return;
  var el = document.getElementById('obEditOverlay');
  if (el) el.parentNode.removeChild(el);
}

async function submitOutboundEdit(btnEl) {
  var customer = (document.getElementById('ob-edit-customer') || {}).value || '';
  if (!customer.trim()) { alert('请填写客户 / 고객을 입력하세요'); return; }
  var biz = (document.getElementById('ob-edit-biz') || {}).value || '';
  if (!biz) { alert('请选择业务分类 / 업무 분류를 선택하세요'); return; }
  var by = getUser();
  if (!by) { alert('请先在右上角设置你的显示名 / 사용자명을 먼저 설정하세요'); return; }
  withActionLock('submitOutboundEdit', btnEl || null, '保存中.../저장중...', async function() {
    var res = await api({
      action: "v2_outbound_order_update",
      id: _currentOutboundId,
      by: by,
      order_date: (document.getElementById('ob-edit-date') || {}).value || '',
      customer: customer.trim(),
      biz_class: biz,
      outbound_mode: (document.getElementById('ob-edit-outmode') || {}).value || '',
      uses_stock_operation: Number((document.getElementById('ob-edit-uses-stock') || {}).value || 0),
      expected_ship_at: (document.getElementById('ob-edit-expected') || {}).value || '',
      destination: (document.getElementById('ob-edit-destination') || {}).value || '',
      po_no: (document.getElementById('ob-edit-po') || {}).value || '',
      wms_work_order_no: (document.getElementById('ob-edit-wms') || {}).value || '',
      planned_box_count: Number((document.getElementById('ob-edit-box') || {}).value || 0),
      planned_pallet_count: Number((document.getElementById('ob-edit-pallet') || {}).value || 0),
      outbound_requirement: (document.getElementById('ob-edit-req') || {}).value || '',
      instruction: (document.getElementById('ob-edit-instruction') || {}).value || '',
      remark: (document.getElementById('ob-edit-remark') || {}).value || ''
    });
    if (res && res.ok) {
      alert('已保存（版本 #' + Number(res.revision_no || 0) + '），等待仓库确认 / 저장됨, 창고 확인 대기');
      closeOutboundEditForm();
      loadOutboundDetail();
    } else {
      alert('失败/실패: ' + (res ? (res.message || res.error) : 'unknown'));
    }
  });
}

// ===== P1-9 提货信息编辑（独立模态，触发 pickup_confirm_required）=====
function openPickupEditForm() {
  var o = window._currentOutboundOrderCache;
  if (!o) { alert('订单未加载 / 주문 미로드'); return; }
  var FROZEN = ['shipped', 'completed', 'cancelled'];
  if (FROZEN.indexOf(o.status) !== -1) {
    alert('已出库/完成/取消的出库单不能编辑提货信息');
    return;
  }
  var html = '';
  html += '<div class="modal-overlay" id="pkEditOverlay" onclick="closePickupEditForm(event)">';
  html += '<div class="modal-box" onclick="event.stopPropagation();" style="max-width:560px;">';
  html += '<div style="font-size:16px;font-weight:700;margin-bottom:12px;">编辑提货信息 / 픽업 정보 편집</div>';
  html += '<div style="background:#fff8e1;border-left:3px solid #ef6c00;padding:8px;margin-bottom:10px;font-size:12px;color:#6d4c00;">⚠ 保存后需仓库现场再次确认 / 저장 후 창고 재확인 필요</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px;">';
  html += '<div><label><b>车牌 / 차번</b></label><input id="pk-vehicle" class="input" value="' + esc(o.pickup_vehicle_no || '') + '"></div>';
  html += '<div><label><b>司机姓名 / 기사명</b></label><input id="pk-driver-name" class="input" value="' + esc(o.pickup_driver_name || '') + '"></div>';
  html += '<div><label><b>司机电话 / 기사 전화</b></label><input id="pk-driver-phone" class="input" value="' + esc(o.pickup_driver_phone || '') + '"></div>';
  html += '<div><label><b>提货人 / 픽업 담당자</b></label><input id="pk-person" class="input" value="' + esc(o.pickup_person_name || '') + '"></div>';
  html += '<div><label><b>所属公司 / 회사</b></label><input id="pk-company" class="input" value="' + esc(o.pickup_company || '') + '"></div>';
  html += '<div><label><b>提货时间 / 픽업 시간</b></label><input id="pk-time" class="input" type="datetime-local" value="' + esc(o.pickup_time || '') + '"></div>';
  html += '<div style="grid-column:1/-1;"><label><b>备注 / 비고</b></label><textarea id="pk-note" class="input" rows="2">' + esc(o.pickup_note || '') + '</textarea></div>';
  html += '</div>';
  html += '<div style="margin-top:14px;text-align:right;">';
  html += '<button class="btn btn-outline" onclick="closePickupEditForm()">取消 / 취소</button> ';
  html += '<button class="btn btn-primary" onclick="submitPickupEdit(this)">保存 / 저장</button>';
  html += '</div></div></div>';
  var div = document.createElement('div');
  div.innerHTML = html;
  document.body.appendChild(div.firstChild);
}

function closePickupEditForm(ev) {
  if (ev && ev.target && ev.target.id !== 'pkEditOverlay') return;
  var el = document.getElementById('pkEditOverlay');
  if (el) el.parentNode.removeChild(el);
}

async function submitPickupEdit(btnEl) {
  var by = getUser();
  if (!by) { alert('请先在右上角设置你的显示名 / 사용자명을 먼저 설정하세요'); return; }
  withActionLock('submitPickupEdit', btnEl || null, '保存中.../저장중...', async function() {
    var res = await api({
      action: "v2_outbound_order_update",
      id: _currentOutboundId,
      by: by,
      pickup_vehicle_no: (document.getElementById('pk-vehicle') || {}).value || '',
      pickup_driver_name: (document.getElementById('pk-driver-name') || {}).value || '',
      pickup_driver_phone: (document.getElementById('pk-driver-phone') || {}).value || '',
      pickup_person_name: (document.getElementById('pk-person') || {}).value || '',
      pickup_company: (document.getElementById('pk-company') || {}).value || '',
      pickup_time: (document.getElementById('pk-time') || {}).value || '',
      pickup_note: (document.getElementById('pk-note') || {}).value || ''
    });
    if (res && res.ok) {
      alert('已保存，等待仓库确认 / 저장됨, 창고 확인 대기');
      closePickupEditForm();
      loadOutboundDetail();
    } else {
      alert('失败/실패: ' + (res ? (res.message || res.error) : 'unknown'));
    }
  });
}

// ===== 删除已取消的入库计划 =====
async function deleteInboundPlan(btnEl) {
  if (!_currentInboundId) return;
  if (!confirm(L("confirm_delete_inbound"))) return;
  withActionLock('deleteInboundPlan', btnEl || null, '删除中.../삭제 중...', async function() {
    var res = await api({ action: "v2_inbound_plan_delete", id: _currentInboundId });
    if (res && res.ok) {
      alert(L("delete_success"));
      _currentInboundId = null;
      goTab("inbound");
      loadInboundList();
    } else if (res && res.error === "only_cancelled_can_delete") {
      alert(res.message || "只能删除已取消的入库计划");
    } else if (res && res.error === "active_job_exists") {
      alert(res.message || "仍有进行中的现场任务，不能删除");
    } else if (res && res.error === "has_ops_history_cannot_delete") {
      alert(res.message || "该入库计划存在已完成作业历史，不允许删除");
    } else {
      alert(L("error") + ": " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

// ===== 删除已取消的出库作业单 =====
async function deleteOutboundOrder(btnEl) {
  if (!_currentOutboundId) return;
  if (!confirm(L("confirm_delete_outbound"))) return;
  withActionLock('deleteOutboundOrder', btnEl || null, '删除中.../삭제 중...', async function() {
    var res = await api({ action: "v2_outbound_order_delete", id: _currentOutboundId });
    if (res && res.ok) {
      alert(L("delete_success"));
      _currentOutboundId = null;
      goTab("outbound");
      loadOutboundList();
    } else if (res && res.error === "only_cancelled_can_delete") {
      alert(res.message || "只能删除已取消的出库作业单");
    } else if (res && res.error === "active_job_exists") {
      alert(res.message || "仍有进行中的现场任务，不能删除");
    } else if (res && res.error === "has_ops_history_cannot_delete") {
      alert(res.message || "该出库作业单存在已完成作业历史，不允许删除");
    } else {
      alert(L("error") + ": " + (res ? (res.message || res.error) : "unknown"));
    }
  });
}

async function markInboundAccounted(accounted, btnEl) {
  var op = getUser();
  if (accounted === 1 && !op) { alert("请先在右上角设置你的显示名"); return; }
  var msg = accounted === 1 ? "确认标记为【已记账】？" : "确认取消记账？";
  if (!confirm(msg)) return;
  withActionLock('markInboundAccounted', btnEl || null, '提交中...', async function() {
    var res = await api({
      action: "v2_inbound_plan_mark_accounted",
      id: _currentInboundId,
      accounted: accounted,
      operator_name: op
    });
    if (!res || !res.ok) {
      alert("失败: " + (res ? (res.message || res.error) : "unknown"));
      return;
    }
    loadInboundDetail();
  });
}

// QR helper using qrcode-generator (loaded as ../shared/qrcode.min.js)
function buildInboundQrHtml(text, cellSize) {
  var qr = qrcode(0, 'M');
  qr.addData(String(text || ''));
  qr.make();
  return qr.createSvgTag({ cellSize: cellSize || 4, margin: 0, scalable: true });
}

// 入库计划单 A4 打印：左上抬头 + 右上小二维码 + 双列信息 + 明细表 + 签字行
function printIbQr() {
  var displayNo = window._currentInboundPretty || _currentInboundId || '';
  var planId = _currentInboundId || '';
  var plan = window._currentInboundPlanCache || {};

  var qrHtml = '';
  try { qrHtml = buildInboundQrHtml(displayNo || planId, 3); }
  catch (e) { qrHtml = '<div style="color:red;font-size:10px;">QR error</div>'; }

  var detailBody = document.getElementById('inboundDetailBody');
  var tables = detailBody ? detailBody.querySelectorAll('table.line-table') : [];
  var linesHtml = tables.length ? tables[0].outerHTML : '';

  var bizMap = { direct_ship: '直发/직배송', bulk: '大货/대량', return_op: '退件/반품', inventory_op: '库内/창고' };
  var bizText = bizMap[plan.biz_class] || plan.biz_class || '';

  var win = window.open('', '_blank');
  var html = '<!doctype html><html><head><meta charset="utf-8"/><title>' + esc(displayNo) + '</title>' +
    '<style>' +
    'body{font-family:"Microsoft YaHei","Helvetica Neue",Arial,sans-serif;margin:20px 30px;color:#000;}' +
    '.print-header{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:3px solid #000;padding-bottom:10px;margin-bottom:14px;}' +
    '.print-title{font-size:22px;font-weight:900;}' +
    '.print-sub{font-size:13px;color:#333;margin-top:4px;}' +
    '.qr-box{text-align:center;flex-shrink:0;margin-left:20px;}' +
    '.qr-box svg{width:100px;height:100px;}' +
    '.qr-label{font-size:10px;color:#666;margin-top:2px;line-height:1.3;}' +
    '.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px 24px;font-size:13px;margin-bottom:14px;}' +
    '.info-grid .label{font-weight:700;}' +
    'table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px;}' +
    'th,td{border:1px solid #333;padding:5px 6px;text-align:left;}' +
    'th{background:#eee;font-weight:700;}' +
    '.sig-row{display:flex;gap:40px;margin-top:30px;font-size:13px;}' +
    '.sig-item{flex:1;}' +
    '.sig-line{border-bottom:1px solid #333;height:30px;margin-top:4px;}' +
    '.footer{margin-top:16px;font-size:11px;color:#888;text-align:center;border-top:1px dashed #ccc;padding-top:6px;}' +
    '@media print{@page{size:A4;margin:15mm 20mm;} body{margin:0;}}' +
    '</style></head><body>' +
    '<div class="print-header">' +
      '<div>' +
        '<div class="print-title">入库计划单</div>' +
        '<div class="print-sub">CK 仓储</div>' +
      '</div>' +
      '<div class="qr-box">' + qrHtml + '<div class="qr-label">' + esc(displayNo) + '</div></div>' +
    '</div>' +
    '<div class="info-grid">' +
      '<div><span class="label">入库单号：</span>' + esc(displayNo) + '</div>' +
      '<div><span class="label">货物摘要：</span>' + esc(plan.cargo_summary || '') + '</div>' +
      '<div><span class="label">计划日期：</span>' + esc(plan.plan_date || '') + '</div>' +
      '<div><span class="label">预计到达：</span>' + esc(plan.expected_arrival || '--') + '</div>' +
      '<div><span class="label">客户：</span>' + esc(plan.customer || '') + '</div>' +
      '<div><span class="label">提出人：</span>' + esc(plan.created_by || '') + '</div>' +
      '<div><span class="label">业务分类：</span>' + esc(bizText) + '</div>' +
      (plan.remark ? '<div><span class="label">备注：</span>' + esc(plan.remark) + '</div>' : '') +
      (plan.purpose ? '<div style="grid-column:1/-1;"><span class="label">入库目的：</span>' + esc(plan.purpose) + '</div>' : '') +
    '</div>' +
    linesHtml +
    '<div class="sig-row">' +
      '<div class="sig-item"><span class="label">制单人：</span>' + esc(plan.created_by || '') + '<div class="sig-line"></div></div>' +
      '<div class="sig-item"><span class="label">仓库确认：</span><div class="sig-line"></div></div>' +
      '<div class="sig-item"><span class="label">客户签收：</span><div class="sig-line"></div></div>' +
      '<div class="sig-item"><span class="label">日期：</span><div class="sig-line"></div></div>' +
    '</div>' +
    '<div class="footer">Printed from CK Warehouse V2</div>' +
    '<script>window.onload=function(){window.print();}<\/script>' +
    '</body></html>';
  win.document.write(html);
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
    } else if (res && res.error === "biz_tasks_pending") {
      alert(res.message || "该入库计划还有未完成的入库类型，请先在现场完成对应入库操作。");
    } else {
      alert(L("error") + ": " + (res ? (res.message || res.error) : "unknown"));
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
    html += '<div><label><b>' + L("biz_class") + '</b></label><select id="fb-conv-biz" class="input"><option value="">--</option><option value="direct_ship">' + bizLabel("direct_ship") + '</option><option value="bulk">' + bizLabel("bulk") + '</option><option value="return">' + bizLabel("return") + '</option><option value="change_order">' + bizLabel("change_order") + '</option><option value="import">' + bizLabel("import") + '</option></select></div>';
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
    html += '<select id="fb-conv-biz"><option value="direct_ship">' + L("biz_direct_ship") + '</option><option value="bulk">' + L("biz_bulk") + '</option><option value="return">' + L("biz_return") + '</option><option value="change_order">' + L("biz_change_order") + '</option><option value="import">' + L("biz_import") + '</option></select></div>';
    html += '<div class="form-group"><label>' + L("cargo_summary") + '</label><input id="fb-conv-cargo" type="text" value="' + esc(fb.title || "") + '"></div>';
    html += '<button class="btn btn-primary" onclick="convertFeedbackToInbound(this)">' + L("convert_to_inbound") + '</button>';
    html += '</div>';
  }

  // 删除现场反馈（未转正即可删；converted / 已生成正式入库计划 / 进行中作业 都会被后端拦截）
  if (fb.status !== 'converted' && !fb.inbound_plan_id) {
    html += '<div class="card"><div class="card-title" style="color:#c0392b;">删除反馈 / 피드백 삭제</div>';
    html += '<div class="muted" style="font-size:12px;margin-bottom:8px;">用于清理误操作 / 测试数据。' +
            '已转正、已生成正式入库计划、或仍有进行中作业的反馈无法删除。</div>';
    html += '<button class="btn btn-danger" onclick="deleteFeedback(this)">删除反馈 / 피드백 삭제</button>';
    html += '</div>';
  }

  body.innerHTML = html;
}

async function deleteFeedback(btnEl) {
  if (!_currentFeedbackId) return;
  if (!confirm("确认删除该现场反馈？删除后不可恢复。\n해당 현장 피드백을 삭제하시겠습니까? 삭제 후 복구할 수 없습니다.")) return;
  withActionLock('deleteFeedback', btnEl || null, '删除中.../삭제중...', async function() {
    var res = await api({ action: "v2_feedback_delete", id: _currentFeedbackId });
    if (res && res.ok) {
      alert("已删除 / 삭제됨");
      goTab("feedback");
      if (typeof loadFeedbackList === 'function') loadFeedbackList();
    } else {
      alert("删除失败/실패: " + (res ? (res.message || res.error) : "unknown"));
    }
  });
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
var _currentOrderOpsJobId = null;
function orderOpsJobTypeText(t) {
  var k = "order_ops_job_type_" + t;
  var v = L(k);
  return v === k ? (t || "--") : v;
}
function orderOpsStatusText(s) {
  var k = "order_ops_status_" + s;
  var v = L(k);
  return v === k ? (s || "--") : v;
}
function orderOpsFlowText(f) {
  var k = "order_ops_flow_" + f;
  var v = L(k);
  return v === k ? (f || "--") : v;
}
function orderOpsLeaveReasonText(r) {
  if (!r) return "--";
  var k = "order_ops_leave_reason_" + r;
  var v = L(k);
  return v === k ? r : v;
}

async function loadOrderOpsList() {
  var body = document.getElementById("orderOpsListBody");
  if (!body) return;
  body.innerHTML = '<span class="muted">' + L("loading") + '</span>';

  var searchBtn = document.getElementById("orderOpsSearchBtn");
  var btnText = searchBtn ? searchBtn.textContent : "";
  if (searchBtn) { searchBtn.disabled = true; searchBtn.textContent = L("loading"); }

  var job_type = (document.getElementById("orderOpsTypeFilter") || {}).value || "";
  var start = (document.getElementById("orderOpsStartDate") || {}).value || "";
  var end = (document.getElementById("orderOpsEndDate") || {}).value || "";

  var res = await api({ action: "v2_order_ops_job_list", job_type: job_type, start_date: start, end_date: end });
  if (searchBtn) { searchBtn.disabled = false; searchBtn.textContent = btnText; }
  if (!res || !res.ok) {
    body.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    return;
  }

  var items = res.items || [];
  if (items.length === 0) {
    body.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    return;
  }

  var html = '<table class="simple-table"><thead><tr>';
  html += '<th>' + L("order_ops_col_type") + '</th>';
  html += '<th>' + L("order_ops_col_trip_or_work") + '</th>';
  html += '<th>' + L("order_ops_col_doc") + '</th>';
  html += '<th>' + L("order_ops_col_people") + '</th>';
  html += '<th>' + L("total_work_time") + '</th>';
  html += '<th>' + L("order_ops_col_status") + '</th>';
  html += '<th>' + L("created_at") + '</th>';
  html += '<th>' + L("order_ops_col_ops") + '</th>';
  html += '</tr></thead><tbody>';

  items.forEach(function(j) {
    var typeText = orderOpsJobTypeText(j.job_type);

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

    var stText = orderOpsStatusText(j.status);
    var stClass = 'st st-' + j.status;

    html += '<tr>';
    html += '<td>' + esc(typeText) + '</td>';
    html += '<td class="col-trip">' + tripHtml + '</td>';
    html += '<td class="col-doc">' + docHtml + '</td>';
    html += '<td class="col-people">' + esc(j.worker_names_text || "--") + '</td>';
    html += '<td class="col-num">' + (j.total_minutes_worked || 0) + L("minutes") + '</td>';
    html += '<td><span class="' + stClass + '">' + esc(stText) + '</span></td>';
    html += '<td class="col-time">' + esc(fmtTime(j.created_at)) + '</td>';
    html += '<td class="col-op"><a href="#" onclick="openOrderOpsDetail(\'' + esc(j.id) + '\');return false;">' + L("order_ops_view_detail") + '</a></td>';
    html += '</tr>';
  });

  html += '</tbody></table>';
  body.innerHTML = html;
}

function openOrderOpsDetail(id) {
  _currentOrderOpsJobId = id;
  goView("order_ops_detail");
  loadOrderOpsDetail(id);
}

async function loadOrderOpsDetail(id) {
  var body = document.getElementById("orderOpsDetailBody");
  var jobId = id || _currentOrderOpsJobId;
  if (!body || !jobId) return;
  _currentOrderOpsJobId = jobId;
  body.innerHTML = '<span class="muted">' + L("loading") + '</span>';

  var res = await api({ action: "v2_ops_job_detail", job_id: jobId });
  if (!res || !res.ok || !res.job) {
    body.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    return;
  }

  var j = res.job;
  var workers = res.workers || [];
  var results = res.results || [];
  var typeText = orderOpsJobTypeText(j.job_type);
  var stText = orderOpsStatusText(j.status);

  // === Card 1: Basic Info ===
  var flowLabel = orderOpsFlowText(j.flow_stage);
  var html = '<div class="card">';
  html += '<div class="card-title">' + esc(typeText);
  if (j.display_no) html += ' · <span style="font-family:monospace;color:#2f54eb;">' + esc(j.display_no) + '</span>';
  html += '</div>';
  html += '<div class="detail-grid">';
  html += '<div class="detail-field"><b>' + L("order_ops_field_status") + ':</b> <span class="st st-' + esc(j.status) + '">' + esc(stText) + '</span></div>';
  html += '<div class="detail-field"><b>' + L("order_ops_field_biz_class") + ':</b> ' + esc(bizLabel(j.biz_class)) + '</div>';
  if (j.display_no) {
    var tripLabel = (j.job_type === 'bulk_op') ? L("order_ops_field_work_order") : L("order_ops_field_trip_no");
    html += '<div class="detail-field"><b>' + tripLabel + ':</b> <span class="trip-tag">' + esc(j.display_no) + '</span></div>';
  }
  html += '<div class="detail-field"><b>' + L("order_ops_field_job_type") + ':</b> ' + esc(flowLabel) + '</div>';
  if (j.related_doc_id) {
    html += '<div class="detail-field"><b>' + L("related_doc_no") + ':</b> <span class="doc-tag">' + esc(j.related_doc_id) + '</span></div>';
  }
  html += '<div class="detail-field"><b>' + L("order_ops_field_creator") + ':</b> ' + esc(j.created_by || "--") + '</div>';
  html += '<div class="detail-field"><b>' + L("order_ops_field_started_at") + ':</b> ' + esc(fmtTime(j.created_at)) + '</div>';
  html += '</div></div>';

  // === Card 2: Pick Docs (if applicable) ===
  if (j.job_type === 'pick_direct') {
    html += '<div class="card"><div class="card-title">' + L("order_ops_detail_pick_docs") + '</div><div id="orderOpsPickDocs"><span class="muted">' + L("loading") + '</span></div></div>';
    html += '<div class="card"><div class="card-title">' + L("order_ops_detail_pwd_by_doc") + '</div><div id="orderOpsPickByDoc"><span class="muted">' + L("loading") + '</span></div></div>';
    html += '<div class="card"><div class="card-title">' + L("order_ops_detail_pwd_by_worker") + '</div><div id="orderOpsPickByWorker"><span class="muted">' + L("loading") + '</span></div></div>';
  }

  // === Card 3: Workers ===
  html += '<div class="card"><div class="card-title">' + L("order_ops_detail_workers") + '</div>';
  if (workers.length > 0) {
    html += '<table class="simple-table"><thead><tr>';
    html += '<th>' + L("submitted_by") + '</th>';
    html += '<th>' + L("order_ops_field_started_at") + '</th>';
    html += '<th>' + L("order_ops_leave_reason_leave") + '</th>';
    html += '<th>' + L("total_work_time") + '</th>';
    html += '<th>' + L("remark") + '</th>';
    html += '</tr></thead><tbody>';
    workers.forEach(function(w) {
      var reasonText = orderOpsLeaveReasonText(w.leave_reason);
      html += '<tr>';
      html += '<td>' + esc(w.worker_name || "--") + '</td>';
      html += '<td class="col-time">' + esc(fmtTime(w.joined_at)) + '</td>';
      html += '<td class="col-time">' + (w.left_at ? esc(fmtTime(w.left_at)) : '<span class="st st-working">' + L("order_ops_status_working") + '</span>') + '</td>';
      html += '<td class="col-num">' + (w.minutes_worked ? Math.round(w.minutes_worked) + L("minutes") : "--") + '</td>';
      html += '<td>' + esc(reasonText) + '</td>';
      html += '</tr>';
    });
    html += '</tbody></table>';
  } else {
    html += '<span class="muted">' + L("no_data") + '</span>';
  }
  html += '</div>';

  // === Card 4: Results ===
  if (results.length > 0) {
    html += '<div class="card"><div class="card-title">' + L("order_ops_detail_results") + '</div>';
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
          if (j.job_type === 'pickup_delivery_import') {
            var pdFields = [
              ["目的地/목적지", rd.destination_note],
              ["大概件数/대략 수량", rd.estimated_piece_count]
            ];
            var pdValid = pdFields.filter(function(f) { return f[1] !== undefined && f[1] !== "" && f[1] !== 0; });
            if (pdValid.length > 0) {
              html += '<div class="detail-field" style="margin-top:6px;"><b>外出记录:</b></div>';
              html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:4px 12px;font-size:12px;margin-top:4px;">';
              pdValid.forEach(function(f) {
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

  // Load pick docs + breakdown if applicable
  if (j.job_type === 'pick_direct') {
    var bkRes = await api({ action: "v2_pick_job_breakdown", job_id: j.id });
    renderPickBreakdown(bkRes);
  }
}

function renderPickBreakdown(bkRes) {
  var pdEl = document.getElementById("orderOpsPickDocs");
  var byDocEl = document.getElementById("orderOpsPickByDoc");
  var byWorkerEl = document.getElementById("orderOpsPickByWorker");
  if (!bkRes || !bkRes.ok) {
    if (pdEl) pdEl.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    if (byDocEl) byDocEl.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    if (byWorkerEl) byWorkerEl.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    return;
  }
  var docsView = bkRes.docs_view || [];
  var workersView = bkRes.workers_view || [];

  // 顶部拣货单号 tag 列表（按总状态着色）
  if (pdEl) {
    if (docsView.length === 0) {
      pdEl.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    } else {
      pdEl.innerHTML = '<div class="tag-wrap">' + docsView.map(function(d) {
        var st = d.pick_status || 'pending';
        return '<span class="doc-tag is-' + esc(st) + '">' + esc(d.pick_doc_no) + '</span>';
      }).join('') + '</div>';
    }
  }

  // 按单视角：每张单 + 参与人员表格
  if (byDocEl) {
    if (docsView.length === 0) {
      byDocEl.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    } else {
      var dh = '';
      docsView.forEach(function(d) {
        var st = d.pick_status || 'pending';
        var stText = (st === 'completed') ? '已完成/완료' : (st === 'working') ? '拣货中/피킹중' : '待拣/대기';
        dh += '<div style="border-bottom:1px solid #f0f0f0;padding:10px 0;">';
        dh += '<div style="margin-bottom:6px;"><span class="doc-tag is-' + esc(st) + '">' + esc(d.pick_doc_no) + '</span> ';
        dh += '<span class="st st-' + esc(st) + '" style="margin-left:6px;">' + esc(stText) + '</span> ';
        dh += '<span class="muted" style="margin-left:6px;font-size:12px;">' + L("order_ops_detail_pwd_total_pickers") + ': ' + (d.participants || []).length + '</span>';
        dh += '</div>';
        var parts = d.participants || [];
        if (parts.length === 0) {
          dh += '<span class="muted">' + L("no_data") + '</span>';
        } else {
          dh += '<table class="simple-table"><thead><tr>';
          dh += '<th>' + L("order_ops_detail_pwd_picker") + '</th>';
          dh += '<th>' + L("order_ops_detail_pwd_started_at") + '</th>';
          dh += '<th>' + L("order_ops_detail_pwd_finished_at") + '</th>';
          dh += '<th>' + L("order_ops_detail_pwd_minutes") + '</th>';
          dh += '<th>' + L("order_ops_detail_pwd_status") + '</th>';
          dh += '</tr></thead><tbody>';
          parts.forEach(function(p) {
            var pst = p.status || 'working';
            var pstText = (pst === 'completed') ? '已完成/완료' : '进行中/진행중';
            dh += '<tr>';
            dh += '<td>' + esc(p.worker_name || p.worker_id || '--') + '</td>';
            dh += '<td class="col-time">' + esc(fmtTime(p.started_at)) + '</td>';
            dh += '<td class="col-time">' + (p.finished_at ? esc(fmtTime(p.finished_at)) : '<span class="st st-working">--</span>') + '</td>';
            dh += '<td class="col-num">' + (p.minutes_worked != null ? Math.round(p.minutes_worked * 10) / 10 : '--') + '</td>';
            dh += '<td><span class="st st-' + esc(pst === 'completed' ? 'completed' : 'working') + '">' + esc(pstText) + '</span></td>';
            dh += '</tr>';
          });
          dh += '</tbody></table>';
        }
        dh += '</div>';
      });
      byDocEl.innerHTML = dh;
    }
  }

  // 按人视角：每人 + total_minutes + 参与的单
  if (byWorkerEl) {
    if (workersView.length === 0) {
      byWorkerEl.innerHTML = '<span class="muted">' + L("no_data") + '</span>';
    } else {
      var wh = '';
      workersView.forEach(function(w) {
        wh += '<div style="border-bottom:1px solid #f0f0f0;padding:10px 0;">';
        wh += '<div style="margin-bottom:6px;"><b>' + esc(w.worker_name || w.worker_id || '--') + '</b> ';
        wh += '<span class="muted" style="margin-left:8px;font-size:12px;">' + L("order_ops_detail_pwd_total_minutes") + ': <b style="color:#1677ff;">' + (w.total_minutes || 0) + '</b></span></div>';
        var docs = w.pick_doc_nos || [];
        if (docs.length > 0) {
          wh += '<div class="detail-field"><b>' + L("order_ops_detail_pwd_picked_docs") + ':</b> <div class="tag-wrap" style="margin-top:4px;">';
          wh += docs.map(function(d) { return '<span class="doc-tag">' + esc(d) + '</span>'; }).join('');
          wh += '</div></div>';
        }
        var segs = w.segments || [];
        if (segs.length > 0) {
          wh += '<div class="detail-field" style="margin-top:6px;"><b>' + L("order_ops_detail_pwd_segments") + ':</b></div>';
          wh += '<table class="simple-table"><thead><tr>';
          wh += '<th>' + L("order_ops_detail_pwd_started_at") + '</th>';
          wh += '<th>' + L("order_ops_detail_pwd_finished_at") + '</th>';
          wh += '<th>' + L("order_ops_detail_pwd_minutes") + '</th>';
          wh += '<th>' + L("order_ops_detail_pwd_picked_docs") + '</th>';
          wh += '</tr></thead><tbody>';
          segs.forEach(function(s) {
            wh += '<tr>';
            wh += '<td class="col-time">' + esc(fmtTime(s.joined_at)) + '</td>';
            wh += '<td class="col-time">' + (s.left_at ? esc(fmtTime(s.left_at)) : '<span class="st st-working">--</span>') + '</td>';
            wh += '<td class="col-num">' + (s.minutes_worked != null ? Math.round(s.minutes_worked * 10) / 10 : '--') + '</td>';
            wh += '<td>' + (s.pick_doc_nos || []).map(function(d) { return '<span class="doc-tag" style="margin-right:2px;">' + esc(d) + '</span>'; }).join('') + '</td>';
            wh += '</tr>';
          });
          wh += '</tbody></table>';
        }
        wh += '</div>';
      });
      byWorkerEl.innerHTML = wh;
    }
  }
}

// =====================================================
// Verify Center — 核对中心
// =====================================================
var _currentVerifyBatchId = null;

async function loadVerifyList() {
  var body = document.getElementById("checkListBody");
  if (!body) return;
  body.innerHTML = '<span class="muted">加载中...</span>';
  var status = (document.getElementById("checkFilterStatus") || {}).value || "";
  var customer = (document.getElementById("checkFilterCustomer") || {}).value || "";
  var res = await api({ action: "v2_verify_batch_list", status: status, customer_name: customer });
  if (!res || !res.ok) { body.innerHTML = '<span class="muted">加载失败</span>'; return; }
  var items = res.items || [];
  if (items.length === 0) {
    body.innerHTML = '<div class="card"><span class="muted">暂无批次</span></div>';
    return;
  }
  var html = '<div class="card"><div class="card-title">核对批次 <span class="count">共 ' + items.length + ' 条</span></div>';
  items.forEach(function(b) {
    var diff = (b.planned_qty || 0) - (b.scanned_ok_count || 0);
    html += '<div class="list-item" onclick="openVerifyBatch(\'' + esc(b.id) + '\')">' +
      '<div class="item-title">' +
        '<span class="st st-' + esc(b.status) + '">' + esc(verifyStatusLabel(b.status)) + '</span> ' +
        esc(b.batch_no || '--') + ' · ' + esc(b.customer_name || '--') +
      '</div>' +
      '<div class="item-meta">' +
        '计划/계획 ' + (b.planned_qty || 0) +
        ' · 正确/정상 ' + (b.scanned_ok_count || 0) +
        ' · 异常/이상 ' + (b.abnormal_count || 0) +
        ' · 差异/차이 ' + diff +
        ' · ' + esc(b.created_at || '').slice(0, 16).replace('T', ' ') +
      '</div>' +
    '</div>';
  });
  html += '</div>';
  body.innerHTML = html;
}

function verifyStatusLabel(s) {
  return ({
    pending: "待核对/대기",
    verifying: "核对中/검수중",
    completed: "已完成/완료",
    cancelled: "已作废/취소"
  })[s] || s;
}

function openVerifyBatch(id) {
  _currentVerifyBatchId = id;
  goView("check_detail");
  loadVerifyDetail();
}

async function loadVerifyDetail() {
  var body = document.getElementById("checkDetailBody");
  if (!body || !_currentVerifyBatchId) return;
  body.innerHTML = '<span class="muted">加载中...</span>';
  var res = await api({ action: "v2_verify_batch_detail", id: _currentVerifyBatchId });
  if (!res || !res.ok) { body.innerHTML = '<span class="muted">加载失败</span>'; return; }
  var b = res.batch;
  var s = res.summary || {};
  var items = res.items || [];
  var logs = res.scan_logs || [];
  var pallets = res.pallet_summary || [];

  var canClose = (b.status === 'pending' || b.status === 'verifying');
  var html = '';

  // 批次头
  html += '<div class="card">';
  html += '<div class="card-title">' + esc(b.batch_no) + ' · ' + esc(b.customer_name) +
    ' <span class="st st-' + esc(b.status) + '">' + esc(verifyStatusLabel(b.status)) + '</span></div>';
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;font-size:13px;">' +
    '<div>计划/계획: <b>' + (s.planned_total_box_count || 0) + ' 箱</b></div>' +
    '<div>已扫/스캔: <b style="color:#2e7d32;">' + (s.scanned_ok_total_count || 0) + '</b></div>' +
    '<div>差异/차이: <b style="color:#c62828;">' + (s.diff || 0) + '</b></div>' +
    '<div>完成条码/완료: <b style="color:#2e7d32;">' + (s.ok_count || 0) + '</b></div>' +
    '<div>少扫条码/부족: <b style="color:#e67e22;">' + (s.shortage_count || 0) + '</b></div>' +
    '<div>超扫条码/초과: <b style="color:#e65100;">' + (s.overflow_count || 0) + '</b></div>' +
    '<div>未扫条码/미스캔: <b>' + (s.not_scanned_count || 0) + '</b></div>' +
    '<div>非本批次扫码/미일치: <b style="color:#c62828;">' + (s.not_found_count || 0) + '</b></div>' +
  '</div>';
  if (b.remark) html += '<div style="margin-top:8px;font-size:13px;"><b>备注:</b> ' + esc(b.remark) + '</div>';
  html += '<div style="margin-top:10px;">';
  if (canClose) {
    html += '<button class="btn btn-primary btn-sm" onclick="updateVerifyBatch(\'' + esc(b.id) + '\',\'completed\',this)">标记已完成</button> ';
    html += '<button class="btn btn-outline btn-sm" onclick="updateVerifyBatch(\'' + esc(b.id) + '\',\'cancelled\',this)">作废批次</button>';
  } else {
    html += '<span class="muted">批次已关闭，不可变更</span>';
  }
  html += '</div></div>';

  // 按条码核对表格
  html += '<div class="card"><div class="card-title">按条码核对 <span class="count">' + items.length + ' 条</span></div>';
  if (!items.length) html += '<span class="muted">无条码</span>';
  else {
    html += '<div style="max-height:360px;overflow:auto;">';
    html += '<table class="verify-item-table" style="width:100%;border-collapse:collapse;font-size:13px;">' +
      '<thead><tr style="background:#fafafa;">' +
        '<th style="text-align:left;padding:6px;">条码</th>' +
        '<th style="text-align:left;padding:6px;">客户名</th>' +
        '<th style="text-align:right;padding:6px;">计划箱数</th>' +
        '<th style="text-align:right;padding:6px;">已扫箱数</th>' +
        '<th style="text-align:right;padding:6px;">差异</th>' +
        '<th style="text-align:left;padding:6px;">状态</th>' +
      '</tr></thead><tbody>';
    items.forEach(function(it) {
      var stCls = ({ ok: 'st-completed', shortage: 'st-issued', overflow: 'st-rework_required', not_scanned: 'st-pending' })[it.status] || '';
      var stLabel = ({ ok: '完成/완료', shortage: '少扫/부족', overflow: '超扫/초과', not_scanned: '未扫/미스캔' })[it.status] || it.status;
      html += '<tr style="border-top:1px solid #f0f0f0;">' +
        '<td style="padding:6px;font-family:monospace;">' + esc(it.barcode) + '</td>' +
        '<td style="padding:6px;">' + esc(it.customer_name || '--') + '</td>' +
        '<td style="padding:6px;text-align:right;">' + (it.planned_box_count || 0) + '</td>' +
        '<td style="padding:6px;text-align:right;">' + (it.scanned_ok_count || 0) + '</td>' +
        '<td style="padding:6px;text-align:right;' + (it.diff_count !== 0 ? 'color:#c62828;font-weight:700;' : '') + '">' + (it.diff_count || 0) + '</td>' +
        '<td style="padding:6px;"><span class="st ' + stCls + '">' + esc(stLabel) + '</span></td>' +
      '</tr>';
    });
    html += '</tbody></table></div>';
  }
  html += '</div>';

  // 按托盘汇总
  html += '<div class="card"><div class="card-title">按托盘汇总 <span class="count">' + pallets.length + ' 托</span></div>';
  if (!pallets.length) html += '<span class="muted">尚无现场扫码</span>';
  else {
    pallets.forEach(function(p) {
      html += '<div class="list-item">' +
        '<div class="item-title">' + esc(p.pallet_no) + '</div>' +
        '<div class="item-meta">正确 ' + p.scanned_ok_count + ' · 异常 ' + p.abnormal_count + '</div>' +
      '</div>';
    });
  }
  html += '</div>';

  // 异常记录（只展示非 ok 的扫码流水）
  var abnormalLogs = logs.filter(function(l) { return l.scan_result !== 'ok'; });
  html += '<div class="card"><div class="card-title">异常记录 <span class="count">' + abnormalLogs.length + ' 条</span></div>';
  if (!abnormalLogs.length) html += '<span class="muted">暂无异常</span>';
  else {
    html += '<div style="max-height:260px;overflow:auto;">';
    abnormalLogs.forEach(function(l) {
      var tagCls = l.scan_result === 'not_found' ? 'st-cancelled' : 'st-rework_required';
      var typeLabel = ({ not_found: '不在本批次', overflow: '超扫', duplicate: '重复(legacy)' })[l.scan_result] || l.scan_result;
      html += '<div style="padding:6px 0;border-bottom:1px solid #f0f0f0;font-size:13px;">' +
        '<span class="st ' + tagCls + '">' + esc(typeLabel) + '</span> ' +
        '<b>' + esc(l.barcode) + '</b>' +
        (l.customer_name ? ' · 客户 ' + esc(l.customer_name) : '') +
        ' · 托盘 ' + esc(l.pallet_no || '--') +
        ' · ' + esc(l.worker_name || l.worker_id || '--') +
        ' · ' + esc((l.scanned_at || '').slice(0, 16).replace('T', ' ')) +
        (l.message ? '<div class="muted" style="margin-left:8px;">' + esc(l.message) + '</div>' : '') +
      '</div>';
    });
    html += '</div>';
  }
  html += '</div>';

  // 完整扫码流水
  html += '<div class="card"><div class="card-title">扫码流水 <span class="count">最近 ' + Math.min(100, logs.length) + ' 条</span></div>';
  if (!logs.length) html += '<span class="muted">暂无扫码记录</span>';
  else {
    html += '<div style="max-height:300px;overflow:auto;">';
    logs.slice(0, 100).forEach(function(l) {
      var tagCls = l.scan_result === 'ok' ? 'st-completed' :
                   (l.scan_result === 'not_found' ? 'st-cancelled' : 'st-issued');
      html += '<div style="padding:6px 0;border-bottom:1px solid #f0f0f0;font-size:13px;">' +
        '<span class="st ' + tagCls + '">' + esc(l.scan_result) + '</span> ' +
        '<b>' + esc(l.barcode) + '</b>' +
        (l.customer_name ? ' · ' + esc(l.customer_name) : '') +
        ' · 托盘 ' + esc(l.pallet_no || '--') +
        ' · ' + esc(l.worker_name || l.worker_id || '--') +
        ' · ' + esc((l.scanned_at || '').slice(0, 16).replace('T', ' ')) +
      '</div>';
    });
    html += '</div>';
  }
  html += '</div>';

  body.innerHTML = html;
}

// ===== Excel 模板下载 =====
function downloadVerifyTemplate() {
  if (typeof XLSX === 'undefined') {
    alert("Excel 库未加载，请检查网络后刷新页面\nExcel 라이브러리 미로딩, 페이지 새로고침");
    return;
  }
  var data = [
    ["条码", "计划箱数", "客户名"],
    ["BC-001", 3, "A客户"],
    ["BC-002", 1, "A客户"],
    ["BC-003", 5, "B客户"]
  ];
  var ws = XLSX.utils.aoa_to_sheet(data);
  ws['!cols'] = [{ wch: 20 }, { wch: 12 }, { wch: 20 }];
  var wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "核对批次模板");
  var fname = "核对批次模板_verify_batch_template.xlsx";
  try {
    XLSX.writeFile(wb, fname);
  } catch (e) {
    alert("模板生成失败：" + (e && e.message || e));
  }
}

// ===== Excel 文件解析 + 预览 =====
var _vcParsedRows = null; // [{ barcode, planned_box_count, customer_name, row_no }]

var _VC_HEADER_ALIASES = {
  barcode: ["条码", "出库条码", "barcode", "bar code"],
  planned_box_count: ["计划箱数", "箱数", "planned_box_count", "box count", "box_count"],
  customer_name: ["客户名", "客户", "customer_name", "customer", "고객사", "고객명"]
};

function _vcFindHeaderIdx(header, aliases) {
  var norm = header.map(function(h) { return String(h || "").trim().toLowerCase(); });
  for (var i = 0; i < aliases.length; i++) {
    var a = aliases[i].toLowerCase();
    var idx = norm.indexOf(a);
    if (idx >= 0) return idx;
  }
  return -1;
}

function onVerifyFilePick(input) {
  var submitBtn = document.getElementById("vc-submit-btn");
  if (submitBtn) submitBtn.disabled = true;
  _vcParsedRows = null;
  var file = input && input.files && input.files[0];
  var previewBox = document.getElementById("vc-preview");
  var sumEl = document.getElementById("vc-preview-summary");
  var errEl = document.getElementById("vc-preview-errors");
  var tblEl = document.getElementById("vc-preview-table");
  if (errEl) { errEl.style.display = "none"; errEl.innerHTML = ""; }
  if (!file) { if (previewBox) previewBox.style.display = "none"; return; }
  if (typeof XLSX === 'undefined') {
    alert("Excel 解析库未加载（SheetJS CDN 未就绪），请检查网络后刷新");
    return;
  }

  var reader = new FileReader();
  reader.onload = function(e) {
    try {
      var data = new Uint8Array(e.target.result);
      var wb = XLSX.read(data, { type: 'array' });
      var sheet = wb.Sheets[wb.SheetNames[0]];
      if (!sheet) throw new Error("文件无工作表");
      var rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "", blankrows: false });
      if (!rows.length) throw new Error("文件为空");
      var header = rows[0].map(String);
      var bcIdx = _vcFindHeaderIdx(header, _VC_HEADER_ALIASES.barcode);
      var bxIdx = _vcFindHeaderIdx(header, _VC_HEADER_ALIASES.planned_box_count);
      var cuIdx = _vcFindHeaderIdx(header, _VC_HEADER_ALIASES.customer_name);
      if (bcIdx < 0 || bxIdx < 0 || cuIdx < 0) {
        var miss = [];
        if (bcIdx < 0) miss.push("条码");
        if (bxIdx < 0) miss.push("计划箱数");
        if (cuIdx < 0) miss.push("客户名");
        throw new Error("表头缺少：" + miss.join("、"));
      }

      var parsed = [];
      var errors = [];
      for (var i = 1; i < rows.length; i++) {
        var r = rows[i];
        if (!r) continue;
        var bc = String(r[bcIdx] || "").trim();
        var bxRaw = r[bxIdx];
        var cu = String(r[cuIdx] || "").trim();
        if (!bc && !cu && !bxRaw) continue; // 空行
        var bxN = typeof bxRaw === 'number' ? bxRaw : parseInt(String(bxRaw || "").trim(), 10);
        var rowNo = i + 1; // Excel 行号（含表头）
        if (!bc) errors.push({ row: rowNo, msg: "条码为空" });
        if (!cu) errors.push({ row: rowNo, msg: "客户名为空" });
        if (!Number.isFinite(bxN) || bxN <= 0 || !Number.isInteger(bxN)) errors.push({ row: rowNo, msg: "计划箱数非正整数: " + bxRaw });
        parsed.push({ barcode: bc, planned_box_count: bxN, customer_name: cu, row_no: rowNo });
      }

      // 合并预检 + 客户冲突检查
      var merged = {};
      var mergeErrors = [];
      parsed.forEach(function(p) {
        if (!p.barcode || !p.customer_name || !(p.planned_box_count > 0)) return;
        if (!merged[p.barcode]) merged[p.barcode] = { barcode: p.barcode, planned_box_count: p.planned_box_count, customer_name: p.customer_name, rows: [p.row_no] };
        else {
          var prev = merged[p.barcode];
          if (prev.customer_name !== p.customer_name) {
            mergeErrors.push({ row: p.row_no, msg: "条码 " + p.barcode + " 在第 " + prev.rows.join(",") + " 行属于 " + prev.customer_name + "，第 " + p.row_no + " 行却写 " + p.customer_name + "（可能串单）" });
          } else {
            prev.planned_box_count += p.planned_box_count;
            prev.rows.push(p.row_no);
          }
        }
      });
      var mergedList = Object.keys(merged).map(function(k) { return merged[k]; });

      var distinctCu = {};
      var totalBox = 0;
      mergedList.forEach(function(m) { distinctCu[m.customer_name] = true; totalBox += m.planned_box_count; });

      // 渲染预览
      sumEl.innerHTML =
        '总行数/줄수: <b>' + parsed.length + '</b> · ' +
        '去重后条码/바코드: <b>' + mergedList.length + '</b> · ' +
        '计划总箱数/계획 박스: <b>' + totalBox + '</b> · ' +
        '客户数/고객수: <b>' + Object.keys(distinctCu).length + '</b>';

      var allErrors = errors.concat(mergeErrors);
      if (allErrors.length) {
        var errHtml = '<b>发现 ' + allErrors.length + ' 条问题：</b><br>';
        allErrors.slice(0, 30).forEach(function(e) { errHtml += '第 ' + e.row + ' 行: ' + esc(e.msg) + '<br>'; });
        if (allErrors.length > 30) errHtml += '...（仅展示前 30 条）';
        errEl.innerHTML = errHtml;
        errEl.style.display = "";
      }

      // 预览表格
      var preview = mergedList.slice(0, 10);
      var tblHtml = '<table style="width:100%;border-collapse:collapse;font-size:13px;">' +
        '<thead><tr style="background:#fafafa;">' +
          '<th style="text-align:left;padding:4px;">条码</th>' +
          '<th style="text-align:left;padding:4px;">客户名</th>' +
          '<th style="text-align:right;padding:4px;">计划箱数</th>' +
        '</tr></thead><tbody>';
      preview.forEach(function(m) {
        tblHtml += '<tr style="border-top:1px solid #f0f0f0;">' +
          '<td style="padding:4px;font-family:monospace;">' + esc(m.barcode) + '</td>' +
          '<td style="padding:4px;">' + esc(m.customer_name) + '</td>' +
          '<td style="padding:4px;text-align:right;">' + m.planned_box_count + '</td>' +
        '</tr>';
      });
      tblHtml += '</tbody></table>';
      if (mergedList.length > 10) tblHtml += '<div class="muted" style="padding:4px;font-size:12px;">...以下省略 ' + (mergedList.length - 10) + ' 行</div>';
      tblEl.innerHTML = tblHtml;

      previewBox.style.display = "";
      if (!allErrors.length && mergedList.length > 0) {
        _vcParsedRows = parsed; // 原始行（后端会重新合并校验）
        if (submitBtn) submitBtn.disabled = false;
      }
    } catch (ex) {
      errEl.innerHTML = '<b>解析失败：</b>' + esc(ex.message || String(ex));
      errEl.style.display = "";
      previewBox.style.display = "";
      sumEl.innerHTML = '';
      tblEl.innerHTML = '';
    }
  };
  reader.readAsArrayBuffer(file);
}

function submitVerifyBatch(btnEl) {
  if (!_vcParsedRows || _vcParsedRows.length === 0) {
    alert("请先上传并通过预览的 Excel 文件");
    return;
  }
  var batch_no = (document.getElementById("vc-batch_no") || {}).value.trim();
  var remark = (document.getElementById("vc-remark") || {}).value.trim();
  withActionLock('submitVerifyBatch', btnEl || null, '创建中...', async function() {
    var res = await api({
      action: "v2_verify_batch_upload",
      batch_no: batch_no,
      remark: remark,
      rows: _vcParsedRows,
      created_by: getUser() || "cs"
    });
    if (!res || !res.ok) {
      if (res && res.error === 'row_errors' && Array.isArray(res.errors)) {
        var msg = "后端校验未通过：\n";
        res.errors.slice(0, 10).forEach(function(e) { msg += "第 " + e.row + " 行: " + e.msg + "\n"; });
        alert(msg);
      } else {
        alert((res && (res.message || res.error)) || "创建失败");
      }
      return;
    }
    alert("批次已创建：" + res.batch_no + "\n去重后条码 " + res.item_count + " 条 · 计划总箱数 " + res.planned_total_box_count + " 箱 · 客户 " + res.distinct_customer_count + " 个");
    // 重置
    ["vc-batch_no","vc-remark"].forEach(function(id) {
      var el = document.getElementById(id); if (el) el.value = "";
    });
    var f = document.getElementById("vc-file"); if (f) f.value = "";
    _vcParsedRows = null;
    var pv = document.getElementById("vc-preview"); if (pv) pv.style.display = "none";
    goTab("check");
  });
}

function updateVerifyBatch(id, target, btnEl) {
  var label = target === 'completed' ? '标记为已完成' : '作废此批次';
  if (!confirm(label + "？")) return;
  withActionLock('updateVerifyBatch_' + id, btnEl || null, '提交中...', async function() {
    var res = await api({
      action: "v2_verify_batch_update_status",
      id: id,
      status: target,
      actor: getUser() || "cs"
    });
    if (!res || !res.ok) {
      alert((res && (res.message || res.error)) || "操作失败");
      return;
    }
    loadVerifyDetail();
  });
}

// ===== Init =====
window.addEventListener("DOMContentLoaded", function() {
  var today = kstToday();
  var el;
  el = document.getElementById("oc-date"); if (el) el.value = today;
  el = document.getElementById("ibc-date"); if (el) el.value = today;

  var d7 = new Date(new Date().getTime() + 9*3600000 - 7*86400000);
  var weekAgo = d7.toISOString().slice(0, 10);
  el = document.getElementById("orderOpsStartDate"); if (el && !el.value) el.value = weekAgo;
  el = document.getElementById("orderOpsEndDate"); if (el && !el.value) el.value = today;

  // 入库创建表单：进入时确保至少一行明细，点击"+ 入库计划"后再补一行
  ensureFirstIbcLine();
  var btnNI = document.getElementById("btnNewInbound");
  if (btnNI) btnNI.addEventListener("click", function() { setTimeout(ensureFirstIbcLine, 30); });

  checkAutoLogin();
});
