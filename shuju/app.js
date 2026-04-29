// CK 仓库数据看板 — app.js
// 现有: realtime / live docs / correction
// 新增: 单子数据 orders / 工时分析 workhours / WMS 导入 wms / 管理看板 management

var _currentTab = "realtime";
var _refreshTimer = null;

// ===== 通用 utils =====
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, function(c) {
    return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c];
  });
}
function getKey() { return localStorage.getItem(SHUJU_KEY_STORAGE) || ""; }
function setKey(k) { localStorage.setItem(SHUJU_KEY_STORAGE, k); }

async function api(params) {
  params.k = getKey();
  try {
    var res = await fetch(V2_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params)
    });
    return await res.json();
  } catch(e) { return { ok: false, error: e.message }; }
}

function fmtTime(isoStr) {
  if (!isoStr) return "--";
  try {
    var d = new Date(isoStr);
    if (isNaN(d.getTime())) return isoStr;
    var kst = new Date(d.getTime() + 9 * 3600 * 1000);
    return kst.toISOString().slice(5, 16).replace("T", " ");
  } catch(e) { return isoStr; }
}
function minutesSince(isoStr) {
  if (!isoStr) return "--";
  var t = new Date(isoStr).getTime();
  if (isNaN(t)) return "--";
  return Math.round((Date.now() - t) / 60000);
}
function round1(n) {
  n = Number(n);
  if (!isFinite(n)) return 0;
  return Math.round(n * 10) / 10;
}
function fmtMinutes(m) {
  m = round1(m);
  if (m < 60) return m + " 分";
  var h = Math.floor(m / 60);
  var r = round1(m - h * 60);
  if (r <= 0) return h + " 小时";
  return h + " 小时 " + r + " 分";
}
function fmtNumber(n) {
  n = Number(n) || 0;
  return round1(n).toLocaleString();
}
function defaultDateRangeToday() {
  var d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.toISOString().slice(0, 10);
}
function setBtnLoading(btn, loading, originalText) {
  if (!btn) return;
  if (loading) {
    btn.dataset._origText = btn.textContent;
    btn.disabled = true;
    btn.textContent = "加载中...";
  } else {
    btn.disabled = false;
    btn.textContent = originalText || btn.dataset._origText || btn.textContent;
  }
}

// CSV 导出
function exportCsv(filename, rows) {
  if (!rows || rows.length === 0) { alert("无数据可导出"); return; }
  var keys = Object.keys(rows[0]);
  var lines = [keys.join(",")];
  rows.forEach(function(r) {
    lines.push(keys.map(function(k) {
      var v = r[k] == null ? "" : String(r[k]);
      if (v.indexOf(',') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0) {
        v = '"' + v.replace(/"/g, '""') + '"';
      }
      return v;
    }).join(","));
  });
  var bom = "﻿";
  var blob = new Blob([bom + lines.join("\r\n")], { type: "text/csv;charset=utf-8" });
  var url = URL.createObjectURL(blob);
  var a = document.createElement("a");
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  setTimeout(function() { URL.revokeObjectURL(url); a.remove(); }, 200);
}

function statusTag(s) {
  var cls = "tag-gray";
  if (s === "working") cls = "tag-green";
  else if (s === "awaiting_close") cls = "tag-orange";
  else if (s === "pending") cls = "tag-blue";
  else if (s === "completed") cls = "tag-purple";
  return '<span class="tag ' + cls + '">' + esc(statusLabel(s)) + '</span>';
}

// ===== Login / Auth =====
function doLogin() {
  var key = document.getElementById("loginKey").value.trim();
  if (!key) return;
  setKey(key);
  api({ action: "v2_auth_check" }).then(function(res) {
    if (res && res.ok) {
      showApp();
    } else {
      var el = document.getElementById("loginError");
      el.style.display = "";
      el.textContent = "访问码错误或网络异常";
    }
  });
}
function doLogout() {
  localStorage.removeItem(SHUJU_KEY_STORAGE);
  location.reload();
}
function checkAuth() {
  var k = getKey();
  if (!k) return;
  api({ action: "v2_auth_check" }).then(function(res) {
    if (res && res.ok) showApp();
  });
}
function showApp() {
  document.getElementById("loginWrap").style.display = "none";
  document.getElementById("appWrap").style.display = "";
  startClock();
  initFilterSelects();
  // 首次默认日期
  setDefaultDateInputs();
  loadRealtime();
  loadLiveDocs();
  _refreshTimer = setInterval(function() {
    if (_currentTab === "realtime") {
      loadRealtime();
      loadLiveDocs();
    }
  }, 30000);
}
function startClock() {
  function tick() {
    var d = new Date(Date.now() + 9 * 3600 * 1000);
    var el = document.getElementById("headerClock");
    if (el) el.textContent = d.toISOString().slice(0,10) + " " + d.toISOString().slice(11,19) + " KST";
  }
  tick();
  setInterval(tick, 1000);
}

function initFilterSelects() {
  // 单子数据 / 工时分析 共用 flow_stage 与 job_type select
  ['ordersFilterFlow', 'whFilterFlow'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el && !el.dataset._init) {
      el.innerHTML = FLOW_STAGE_OPTIONS.map(function(o) {
        return '<option value="' + esc(o.value) + '">' + esc(o.label) + '</option>';
      }).join('');
      el.dataset._init = '1';
    }
  });
  ['ordersFilterJobType', 'whFilterJobType'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el && !el.dataset._init) {
      el.innerHTML = JOB_TYPE_OPTIONS.map(function(o) {
        return '<option value="' + esc(o.value) + '">' + esc(o.label) + '</option>';
      }).join('');
      el.dataset._init = '1';
    }
  });
  // P2-12: 单子数据 状态筛选下拉
  var statusEl = document.getElementById('ordersFilterStatus');
  if (statusEl && !statusEl.dataset._init && typeof STATUS_OPTIONS !== 'undefined') {
    statusEl.innerHTML = STATUS_OPTIONS.map(function(o) {
      return '<option value="' + esc(o.value) + '">' + esc(o.label) + '</option>';
    }).join('');
    statusEl.dataset._init = '1';
  }
}

function setDefaultDateInputs() {
  var today = defaultDateRangeToday();
  ['ordersFilterStart', 'ordersFilterEnd', 'whFilterStart', 'whFilterEnd',
   'mgmtFilterStart', 'mgmtFilterEnd'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el && !el.value) el.value = today;
  });
}

function switchTab(tab, btn) {
  _currentTab = tab;
  var tabs = document.querySelectorAll(".tab-bar button");
  for (var i = 0; i < tabs.length; i++) tabs[i].classList.remove("active");
  if (btn) btn.classList.add("active");
  ["realtime","orders","workhours","wms","management"].forEach(function(t) {
    var el = document.getElementById("tab-" + t);
    if (el) el.style.display = (t === tab) ? "" : "none";
  });
  if (tab === "realtime") { loadRealtime(); loadLiveDocs(); }
  else if (tab === "orders" && !window._ordersLoaded) { window._ordersLoaded = true; loadOrders(); }
  else if (tab === "workhours" && !window._workhoursLoaded) { window._workhoursLoaded = true; loadWorkhours(); }
  else if (tab === "wms" && !window._wmsLoaded) { window._wmsLoaded = true; loadWmsBatches(); }
  else if (tab === "management" && !window._managementLoaded) { window._managementLoaded = true; loadManagement(); }
}

// ===== Realtime overview =====
var _liveWorkersCache = [];
var _liveBizFilter = '';
async function loadRealtime() {
  var res = await api({ action: "v2_dashboard_realtime_overview" });
  if (!res || !res.ok) return;

  document.getElementById("statActiveWorkers").textContent = res.current_active_workers || 0;
  document.getElementById("statTodayLogins").textContent = res.today_login_workers || 0;
  document.getElementById("statActiveJobs").textContent = res.current_active_jobs || 0;
  document.getElementById("statActiveDocs").textContent = res.current_active_docs || 0;

  _liveWorkersCache = res.worker_live_status || [];
  renderLiveWorkers();

  var breakdown = res.biz_breakdown || [];
  var grid = document.getElementById("bizBreakdown");
  if (breakdown.length === 0) {
    grid.innerHTML = '<div class="muted" style="padding:20px;text-align:center;">当前无活跃业务</div>';
  } else {
    var bhtml = "";
    breakdown.forEach(function(b) {
      var jt = b.job_type || '';
      var label = jobTypeLabel(b.job_type) || flowLabel(b.flow_stage);
      var sel = (jt === _liveBizFilter) ? ' biz-item-active' : '';
      bhtml += '<div class="biz-item' + sel + '" style="cursor:pointer;" onclick="filterLiveByJobType(\'' + esc(jt) + '\')">';
      bhtml += '<div class="biz-name">' + esc(label) + '</div>';
      bhtml += '<div class="biz-nums"><b>' + (b.worker_count || 0) + '</b> 人 / <b>' + (b.job_count || 0) + '</b> 个任务</div>';
      bhtml += '<div class="muted" style="font-size:11px;margin-top:2px;">点击查看详情</div>';
      bhtml += '</div>';
    });
    grid.innerHTML = bhtml;
  }
}

function filterLiveByJobType(jt) {
  _liveBizFilter = (_liveBizFilter === jt) ? '' : jt;
  renderLiveWorkers();
  // 同步业务卡片高亮
  var cards = document.querySelectorAll('#bizBreakdown .biz-item');
  cards.forEach(function(c) { c.classList.remove('biz-item-active'); });
  if (_liveBizFilter) {
    cards.forEach(function(c) {
      if (c.getAttribute('onclick') && c.getAttribute('onclick').indexOf("'" + _liveBizFilter + "'") !== -1) {
        c.classList.add('biz-item-active');
      }
    });
  }
}

function renderLiveWorkers() {
  var tbody = document.getElementById("workerLiveBody");
  if (!tbody) return;
  var rows = _liveWorkersCache;
  if (_liveBizFilter) {
    rows = rows.filter(function(w) { return w.job_type === _liveBizFilter; });
  }
  // 筛选条 + 重置按钮
  var hint = document.getElementById('workerLiveFilterHint');
  if (hint) {
    if (_liveBizFilter) {
      hint.innerHTML = '已筛选业务：<b>' + esc(jobTypeLabel(_liveBizFilter)) + '</b>（' + rows.length + ' 人） <a href="#" onclick="filterLiveByJobType(\'\');return false;">清除筛选</a>';
      hint.style.display = '';
    } else {
      hint.innerHTML = '';
      hint.style.display = 'none';
    }
  }
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" class="muted" style="text-align:center;">' +
      (_liveBizFilter ? '该业务下当前无在岗人员' : '当前没有在岗人员') + '</td></tr>';
    return;
  }
  var html = "";
  rows.forEach(function(w) {
    html += "<tr>";
    html += "<td><b>" + esc(w.worker_name) + "</b></td>";
    html += "<td>" + esc(bizLabel(w.biz_class)) + "</td>";
    html += "<td>" + esc(flowLabel(w.flow_stage)) + "</td>";
    html += "<td>" + esc(jobTypeLabel(w.job_type)) + "</td>";
    html += "<td>" + esc(w.display_no || w.related_doc_id || "--") + "</td>";
    html += "<td>" + esc(fmtTime(w.joined_at)) + "</td>";
    html += "<td>" + minutesSince(w.joined_at) + "</td>";
    html += "<td>" + statusTag(w.job_status) + "</td>";
    html += '<td><button class="row-action warn" onclick="markAnomaly(\'worker\',\'' + esc(w.worker_id) + '\')">标记异常</button></td>';
    html += "</tr>";
  });
  tbody.innerHTML = html;
}

async function loadLiveDocs() {
  var res = await api({ action: "v2_dashboard_live_docs" });
  var tbody = document.getElementById("liveDocsBody");
  if (!res || !res.ok) {
    tbody.innerHTML = '<tr><td colspan="8" class="muted" style="text-align:center;">加载失败</td></tr>';
    return;
  }
  var docs = res.docs || [];
  if (docs.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="muted" style="text-align:center;">当前没有进行中的单子</td></tr>';
    return;
  }
  var docFlows = {};
  docs.forEach(function(d) {
    var key = d.related_doc_id || d.job_id;
    if (!docFlows[key]) docFlows[key] = [];
    docFlows[key].push(d.flow_stage);
  });
  var html = "";
  docs.forEach(function(d) {
    var key = d.related_doc_id || d.job_id;
    var flows = docFlows[key] || [];
    var isParallel = flows.indexOf("unload") !== -1 && flows.indexOf("inbound") !== -1;
    html += "<tr>";
    html += "<td><b>" + esc(d.display_no || d.related_doc_id || d.job_id) + "</b></td>";
    html += "<td>" + esc(bizLabel(d.biz_class)) + "</td>";
    html += "<td>" + esc(flowLabel(d.flow_stage)) + (isParallel ? ' <span class="tag tag-orange" style="font-size:10px;">并行</span>' : '') + "</td>";
    html += "<td>" + esc(d.worker_names || "--") + "</td>";
    html += "<td>" + esc(fmtTime(d.created_at)) + "</td>";
    html += "<td>" + minutesSince(d.created_at) + "</td>";
    html += "<td>" + statusTag(d.status) + "</td>";
    html += '<td><button class="row-action warn" onclick="markAnomaly(\'doc\',\'' + esc(d.job_id) + '\')">标记异常</button>';
    html += ' <button class="row-action" onclick="requestCorrection(\'' + esc(d.job_id) + '\')">发起修正</button></td>';
    html += "</tr>";
  });
  tbody.innerHTML = html;
}

async function markAnomaly(type, id) {
  var reason = prompt("请填写异常说明（将记入修正申请表）:");
  if (!reason || !reason.trim()) return;
  var reporter = prompt("您的姓名/工号:");
  if (!reporter || !reporter.trim()) return;
  var res = await api({
    action: "v2_correction_request_create",
    type: type, target_id: id, reporter: reporter.trim(), reason: reason.trim()
  });
  if (res && res.ok) alert("已提交修正申请 #" + res.id + "\n主管将另行处理，不会直接修改业务数据");
  else alert("提交失败：" + (res ? (res.message || res.error) : "unknown"));
}
async function requestCorrection(jobId) {
  var reason = prompt("请填写修正诉求（工时/人员/结果 等）:");
  if (!reason || !reason.trim()) return;
  var reporter = prompt("您的姓名/工号:");
  if (!reporter || !reporter.trim()) return;
  var res = await api({
    action: "v2_correction_request_create",
    type: "job", target_id: jobId, reporter: reporter.trim(), reason: reason.trim()
  });
  if (res && res.ok) alert("已提交修正申请 #" + res.id);
  else alert("提交失败：" + (res ? (res.message || res.error) : "unknown"));
}

// =====================================================
// 单子数据 (orders)
// =====================================================
var _ordersItems = [];

function ordersFilterParams() {
  return {
    start_date: document.getElementById("ordersFilterStart").value || '',
    end_date: document.getElementById("ordersFilterEnd").value || '',
    flow_stage: document.getElementById("ordersFilterFlow").value || '',
    job_type: document.getElementById("ordersFilterJobType").value || '',
    status: (document.getElementById("ordersFilterStatus") || {}).value || '',
    worker_name: document.getElementById("ordersFilterWorker").value.trim(),
    doc_no: document.getElementById("ordersFilterDoc").value.trim(),
    limit: 200, offset: 0
  };
}

async function loadOrders(btn) {
  var tbody = document.getElementById("ordersBody");
  tbody.innerHTML = '<tr><td colspan="12" class="muted" style="text-align:center;">加载中...</td></tr>';
  setBtnLoading(btn, true);
  var params = ordersFilterParams();
  params.action = "v2_dashboard_order_list";
  var res = await api(params);
  setBtnLoading(btn, false, "查询");
  if (!res || !res.ok) {
    tbody.innerHTML = '<tr><td colspan="12" class="muted" style="text-align:center;">加载失败: ' + esc(res && (res.message || res.error)) + '</td></tr>';
    return;
  }
  _ordersItems = res.items || [];
  document.getElementById("ordersTotal").textContent = (res.total || 0) + ' 条 (显示前 ' + _ordersItems.length + ')';
  if (_ordersItems.length === 0) {
    tbody.innerHTML = '<tr><td colspan="12" class="muted" style="text-align:center;">暂无数据</td></tr>';
    return;
  }
  var html = '';
  _ordersItems.forEach(function(j) {
    var resultSummary = j.result_summary || '';
    if (!resultSummary) resultSummary = '<span class="muted">--</span>';
    html += '<tr>';
    html += '<td>' + esc((j.created_at || '').slice(0, 10)) + '</td>';
    html += '<td><b>' + esc(j.display_no || j.related_doc_id || j.id) + '</b></td>';
    html += '<td>' + esc(flowLabel(j.flow_stage)) + '</td>';
    html += '<td>' + esc(jobTypeLabel(j.job_type)) + '</td>';
    html += '<td>' + esc(bizLabel(j.biz_class)) + '</td>';
    html += '<td>' + statusTag(j.status) + '</td>';
    html += '<td>' + (j.worker_count || 0) + '</td>';
    html += '<td>' + fmtMinutes(j.total_minutes) + '</td>';
    html += '<td>' + esc(fmtTime(j.started_at)) + '</td>';
    html += '<td>' + esc(fmtTime(j.ended_at)) + '</td>';
    html += '<td>' + (resultSummary.indexOf('<span') === 0 ? resultSummary : esc(resultSummary)) + '</td>';
    html += '<td><button class="row-action" onclick="openOrderDetail(\'' + esc(j.id) + '\')">详情</button></td>';
    html += '</tr>';
  });
  tbody.innerHTML = html;
}

async function exportOrders(btn) {
  setBtnLoading(btn, true);
  try {
    var params = ordersFilterParams();
    params.action = "v2_dashboard_order_export";
    params.limit = 10000;
    delete params.offset;
    var res = await api(params);
    if (!res || !res.ok) {
      var msg = res ? String(res.message || res.error || '') : 'unknown';
      if (msg.indexOf('too many SQL variables') >= 0) {
        alert('导出失败：本次数据量较大，系统正在按小批量查询修复。请刷新后重试。');
      } else {
        alert('导出失败: ' + msg);
      }
      return;
    }
    var rows = res.rows || [];
    if (rows.length === 0) { alert("无数据可导出"); return; }
    var csvRows = rows.map(function(r) {
    return {
      日期: r['日期'] || '',
      单号: r['单号'] || '',
      客户: r.customer || '',
      业务阶段: flowLabel(r.flow_stage),
      任务类型: jobTypeLabel(r.job_type),
      业务分类: bizLabel(r.biz_class),
      状态: statusLabel(r.status),
      是否记账: r.accounted === '' ? '' : (r.accounted ? '已记账' : '未记账'),
      记账人: r.accounted_by || '',
      记账时间: r.accounted_at || '',
      参与人数: r.worker_count || 0,
      参与人员: r.worker_names || '',
      总分钟: r.total_minutes || 0,
      总小时: r.total_hours || 0,
      开始时间: r.started_at || '',
      结束时间: r.ended_at || '',
      作业结果摘要: r.result_summary || '',
      作业备注: r.result_notes || '',
      差异说明: r.diff_notes || '',
      作业明细行数: r.result_lines_count || 0,
      作业明细说明: r.readable_result_lines || '',
      箱数合计: r.box_count_sum || 0,
      板数合计: r.pallet_count_sum || 0,
      打包SKU数: r.packed_sku_count_sum || 0,
      打包箱数: r.packed_box_count_sum || 0,
      总操作箱数: r.total_operated_box_count_sum || 0,
      贴标数: r.label_count_sum || 0,
      修箱数: r.repaired_box_count_sum || 0,
      换箱数: r.reboxed_count_sum || 0,
      使用大纸箱数: r.used_carton_large_count_sum || 0,
      使用小纸箱数: r.used_carton_small_count_sum || 0,
      核对OK数: r.verify_ok_count_sum || 0,
      核对NG数: r.verify_ng_count_sum || 0,
      拣货单号: r.pick_doc_nos || '',
      拣货人员明细: r.pick_worker_summary || '',
      结果提交人: r.result_submitters || '',
      结果提交时间: r.result_submitted_at || '',
      出库单号: r.outbound_display_no || '',
      入库单号: r.inbound_display_no || '',
      WMS工单号: r.wms_work_order_no || '',
      目的地: r.destination || '',
      PO号: r.po_no || '',
      计划箱数: r.planned_box_count || 0,
      计划板数: r.planned_pallet_count || 0,
      实际箱数: r.actual_box_count || 0,
      实际板数: r.actual_pallet_count || 0,
      是否库内操作: (r.uses_stock_operation === 1 || r.uses_stock_operation === '1') ? '是' : '否',
      出库单状态: r.outbound_status || '',
      预计出库时间: r.expected_ship_at || '',
      出库要求: r.outbound_requirement || '',
      出库作业说明: r.ob_instruction || '',
      出库备注: r.ob_remark || '',
      提货备注: r.ob_pickup_note || '',
      入库备注: r.inbound_remark || '',
      入库是否手动完成: Number(r.inbound_force_completed || 0) === 1 ? '是' : '否',
      入库手动完成人: r.inbound_force_completed_by || '',
      入库手动完成时间: r.inbound_force_completed_at || '',
      入库手动完成原因: r.inbound_force_complete_reason || '',
      库内操作子状态: r.stock_op_status || '',
      库内操作完成时间: r.stock_op_completed_at || '',
      库内操作完成人: r.stock_op_completed_by || '',
      出库资料数: r.material_count || 0,
      job_id: r.job_id,
      原始结果JSON: r.raw_result_json_compact || ''
    };
  });
    var startD = (params.start_date || '').replace(/-/g, '');
    var endD = (params.end_date || '').replace(/-/g, '');
    var fname = 'orders_export_' + (startD || 'all') + '_' + (endD || 'all') + '.csv';
    exportCsv(fname, csvRows);
    if (res.truncated) alert('已达单次导出上限 10000 行，部分数据可能被截断。请缩小日期或筛选范围。');
  } catch (e) {
    alert('导出失败: ' + (e && e.message || e));
  } finally {
    setBtnLoading(btn, false, "导出明细 CSV");
  }
}

async function openOrderDetail(jobId) {
  var modal = document.getElementById("orderDetailModal");
  var body = document.getElementById("orderDetailBody");
  body.innerHTML = '<div class="muted">加载中...</div>';
  modal.style.display = '';
  var res = await api({ action: "v2_dashboard_order_detail", job_id: jobId });
  if (!res || !res.ok) {
    body.innerHTML = '<div class="muted">加载失败</div>';
    return;
  }
  var j = res.job || {};
  var html = '';
  html += '<h3>基本信息</h3>';
  html += '<table class="data-table"><tr><th>job_id</th><td>' + esc(j.id) + '</td>';
  html += '<th>display_no</th><td>' + esc(j.display_no || '--') + '</td></tr>';
  html += '<tr><th>业务阶段</th><td>' + esc(flowLabel(j.flow_stage)) + '</td>';
  html += '<th>任务类型</th><td>' + esc(jobTypeLabel(j.job_type)) + '</td></tr>';
  html += '<tr><th>业务分类</th><td>' + esc(bizLabel(j.biz_class)) + '</td>';
  html += '<th>状态</th><td>' + statusTag(j.status) + '</td></tr>';
  html += '<tr><th>related_doc_id</th><td>' + esc(j.related_doc_id || '--') + '</td>';
  html += '<th>linked_outbound_order_id</th><td>' + esc(j.linked_outbound_order_id || '--') + '</td></tr>';
  html += '<tr><th>created_at</th><td>' + esc(fmtTime(j.created_at)) + '</td>';
  html += '<th>updated_at</th><td>' + esc(fmtTime(j.updated_at)) + '</td></tr>';
  html += '</table>';

  // 入库手动完成标
  if (Number(res.inbound_force_completed || 0) === 1) {
    html += '<h3 style="margin-top:14px;color:#6a1b9a;">入库手动完成（不计入现场工时）</h3>';
    html += '<table class="data-table">';
    html += '<tr><th>手动完成人</th><td>' + esc(res.inbound_force_completed_by || '--') + '</td>';
    html += '<th>手动完成时间</th><td>' + esc(res.inbound_force_completed_at || '--') + '</td></tr>';
    html += '<tr><th>原因</th><td colspan="3" style="white-space:pre-wrap;">' + esc(res.inbound_force_complete_reason || '--') + '</td></tr>';
    html += '</table>';
  }

  // 关联单据备注（入库备注 / 出库要求 / 出库作业说明 / 出库备注 / 提货备注）
  var hasAnyRemark = res.inbound_remark || res.outbound_requirement || res.ob_instruction || res.ob_remark || res.ob_pickup_note;
  if (hasAnyRemark) {
    html += '<h3 style="margin-top:14px;">关联单据备注</h3>';
    html += '<table class="data-table">';
    if (res.inbound_remark) html += '<tr><th>入库备注</th><td colspan="3" style="white-space:pre-wrap;">' + esc(res.inbound_remark) + '</td></tr>';
    if (res.outbound_requirement) html += '<tr><th>出库要求</th><td colspan="3" style="white-space:pre-wrap;">' + esc(res.outbound_requirement) + '</td></tr>';
    if (res.ob_instruction) html += '<tr><th>出库作业说明</th><td colspan="3" style="white-space:pre-wrap;">' + esc(res.ob_instruction) + '</td></tr>';
    if (res.ob_remark) html += '<tr><th>出库备注</th><td colspan="3" style="white-space:pre-wrap;">' + esc(res.ob_remark) + '</td></tr>';
    if (res.ob_pickup_note) html += '<tr><th>提货备注</th><td colspan="3" style="white-space:pre-wrap;">' + esc(res.ob_pickup_note) + '</td></tr>';
    html += '</table>';
  }

  // 参与人员
  var workers = res.workers || [];
  html += '<h3 style="margin-top:14px;">参与人员 (' + workers.length + ')</h3>';
  if (workers.length === 0) html += '<div class="muted">无</div>';
  else {
    html += '<table class="data-table"><thead><tr><th>员工</th><th>开始</th><th>结束</th><th>分钟</th><th>离开原因</th></tr></thead><tbody>';
    workers.forEach(function(w) {
      html += '<tr><td>' + esc(w.worker_name || w.worker_id) + '</td>';
      html += '<td>' + esc(fmtTime(w.joined_at)) + '</td>';
      html += '<td>' + esc(fmtTime(w.left_at)) + '</td>';
      html += '<td>' + (w.minutes_worked || 0) + '</td>';
      html += '<td>' + esc(w.leave_reason || '--') + '</td></tr>';
    });
    html += '</tbody></table>';
  }

  // 作业结果 — 业务可读
  var results = res.results || [];
  var p = res.parsed || {};
  if (results.length > 0 || p.result_summary) {
    html += '<h3 style="margin-top:14px;">作业结果（业务摘要）</h3>';
    html += '<table class="data-table">';
    html += '<tr><th>作业结果摘要</th><td colspan="3">' + esc(p.result_summary || '--') + '</td></tr>';
    html += '<tr><th>箱数合计</th><td>' + (p.box_count_sum || 0) + '</td>';
    html += '<th>板数合计</th><td>' + (p.pallet_count_sum || 0) + '</td></tr>';
    html += '<tr><th>打包SKU数</th><td>' + (p.packed_sku_count_sum || 0) + '</td>';
    html += '<th>打包箱数</th><td>' + (p.packed_box_count_sum || 0) + '</td></tr>';
    html += '<tr><th>贴标数</th><td>' + (p.label_count_sum || 0) + '</td>';
    html += '<th>总操作箱数</th><td>' + (p.total_operated_box_count_sum || 0) + '</td></tr>';
    html += '<tr><th>修箱数</th><td>' + (p.repaired_box_count_sum || 0) + '</td>';
    html += '<th>换箱数</th><td>' + (p.reboxed_count_sum || 0) + '</td></tr>';
    html += '<tr><th>使用大纸箱</th><td>' + (p.used_carton_large_count_sum || 0) + '</td>';
    html += '<th>使用小纸箱</th><td>' + (p.used_carton_small_count_sum || 0) + '</td></tr>';
    if ((p.verify_ok_count_sum || 0) + (p.verify_ng_count_sum || 0) > 0) {
      html += '<tr><th>核对OK</th><td>' + (p.verify_ok_count_sum || 0) + '</td>';
      html += '<th>核对NG</th><td>' + (p.verify_ng_count_sum || 0) + '</td></tr>';
    }
    html += '<tr><th>作业备注</th><td colspan="3">' + esc(p.result_notes || '--') + '</td></tr>';
    html += '<tr><th>差异说明</th><td colspan="3">' + esc(p.diff_notes || '--') + '</td></tr>';
    html += '<tr><th>作业明细 (' + (p.result_lines_count || 0) + ' 行)</th><td colspan="3" style="white-space:pre-wrap;">' + esc(p.readable_result_lines || '--') + '</td></tr>';
    html += '<tr><th>结果提交人</th><td>' + esc(p.result_submitters || '--') + '</td>';
    html += '<th>结果提交时间</th><td>' + esc(p.result_submitted_at || '--') + '</td></tr>';
    html += '</table>';

    html += '<details style="margin-top:8px;"><summary class="muted" style="cursor:pointer;">原始JSON（排查用）— ' + results.length + ' 条</summary>';
    html += '<table class="data-table" style="margin-top:6px;"><thead><tr><th>箱</th><th>板</th><th>备注</th><th>diff_note</th><th>result_json</th><th>result_lines_json</th><th>提交人</th><th>提交时间</th></tr></thead><tbody>';
    results.forEach(function(r) {
      var rj = r.result_json == null ? '' : String(r.result_json);
      if (rj.length > 200) rj = rj.slice(0, 200) + '...';
      var rl = r.result_lines_json == null ? '' : String(r.result_lines_json);
      if (rl.length > 200) rl = rl.slice(0, 200) + '...';
      html += '<tr><td>' + (r.box_count || 0) + '</td>';
      html += '<td>' + (r.pallet_count || 0) + '</td>';
      html += '<td>' + esc(r.remark || '--') + '</td>';
      html += '<td>' + esc(r.diff_note || '--') + '</td>';
      html += '<td><code style="font-size:11px;">' + esc(rj) + '</code></td>';
      html += '<td><code style="font-size:11px;">' + esc(rl) + '</code></td>';
      html += '<td>' + esc(r.created_by || '--') + '</td>';
      html += '<td>' + esc(fmtTime(r.created_at)) + '</td></tr>';
    });
    html += '</tbody></table></details>';
  }

  // pick_worker_docs (代发拣货特有)
  var pwd = res.pick_worker_docs || [];
  if (pwd.length > 0) {
    html += '<h3 style="margin-top:14px;">代发拣货分组 — 按拣货单</h3>';
    var byDoc = {};
    pwd.forEach(function(p) {
      var k = p.pick_doc_no;
      if (!byDoc[k]) byDoc[k] = [];
      byDoc[k].push(p);
    });
    html += '<table class="data-table"><thead><tr><th>拣货单号</th><th>参与人</th></tr></thead><tbody>';
    Object.keys(byDoc).forEach(function(d) {
      var ws = byDoc[d].map(function(p) {
        return esc(p.worker_name || p.worker_id) + (p.left_at ? '(已完成 ' + (p.minutes_worked||0) + '分)' : '(进行中)');
      }).join('、');
      html += '<tr><td><b>' + esc(d) + '</b></td><td>' + ws + '</td></tr>';
    });
    html += '</tbody></table>';

    html += '<h3 style="margin-top:14px;">代发拣货分组 — 按人员</h3>';
    var byWk = {};
    pwd.forEach(function(p) {
      var k = p.worker_name || p.worker_id;
      if (!byWk[k]) byWk[k] = [];
      byWk[k].push(p);
    });
    html += '<table class="data-table"><thead><tr><th>员工</th><th>参与了哪些拣货单</th></tr></thead><tbody>';
    Object.keys(byWk).forEach(function(w) {
      var ds = byWk[w].map(function(p) { return esc(p.pick_doc_no); }).join('、');
      html += '<tr><td><b>' + esc(w) + '</b></td><td>' + ds + '</td></tr>';
    });
    html += '</tbody></table>';
  }

  body.innerHTML = html;
}

function closeOrderDetail() {
  document.getElementById("orderDetailModal").style.display = 'none';
}

// =====================================================
// 工时分析 (workhours)
// =====================================================
var _whSummary = null;

function whFilterParams() {
  return {
    start_date: document.getElementById("whFilterStart").value || '',
    end_date: document.getElementById("whFilterEnd").value || '',
    worker_name: document.getElementById("whFilterWorker").value.trim(),
    flow_stage: document.getElementById("whFilterFlow").value || '',
    job_type: document.getElementById("whFilterJobType").value || ''
  };
}

async function loadWorkhours(btn) {
  setBtnLoading(btn, true);
  var params = whFilterParams();
  params.action = "v2_dashboard_workhour_summary";
  var res = await api(params);
  setBtnLoading(btn, false, "查询");
  if (!res || !res.ok) {
    document.getElementById("whSummaryRow").innerHTML = '<div class="muted">加载失败</div>';
    return;
  }
  _whSummary = res;
  var s = res.summary || {};

  // 顶部统计卡
  var cards = [
    { label: '总工时', value: fmtMinutes(s.total_minutes || 0), sub: round1(s.total_hours) + ' 小时' },
    { label: '参与人数', value: s.worker_count || 0 },
    { label: '任务数', value: s.job_count || 0 },
    { label: '人均工时', value: fmtMinutes(s.avg_minutes_per_worker || 0) },
    { label: '最长单段', value: fmtMinutes(s.max_segment_minutes || 0) },
    { label: '异常段', value: s.anomaly_count || 0, sub: '≥12小时 / ≤0分 / 跨天未结束' },
    { label: '长工时提醒', value: s.long_segment_count || 0, sub: '≥240分 但未达异常' }
  ];
  var ch = '';
  cards.forEach(function(c) {
    ch += '<div class="stat-card"><div class="stat-label">' + esc(c.label) + '</div>';
    ch += '<div class="stat-value">' + esc(c.value) + '</div>';
    if (c.sub) ch += '<div class="stat-sub">' + esc(c.sub) + '</div>';
    ch += '</div>';
  });
  document.getElementById("whSummaryRow").innerHTML = ch;

  // 表 1: 按员工
  var bw = res.by_worker || [];
  var tb1 = '';
  if (bw.length === 0) tb1 = '<tr><td colspan="5" class="muted" style="text-align:center;">暂无数据</td></tr>';
  else bw.forEach(function(w) {
    tb1 += '<tr><td><b>' + esc(w.worker_name) + '</b></td>';
    tb1 += '<td>' + round1(w.total_minutes) + '</td>';
    tb1 += '<td>' + round1(w.total_hours) + '</td>';
    tb1 += '<td>' + (w.job_count || 0) + '</td>';
    tb1 += '<td>' + round1(w.avg_minutes_per_job) + '</td>';
    tb1 += '<td>' + round1(w.max_segment_minutes) + '</td></tr>';
  });
  document.getElementById("whByWorkerBody").innerHTML = tb1;

  // 表 2: 按任务类型
  var bj = res.by_job_type || [];
  var tb2 = '';
  if (bj.length === 0) tb2 = '<tr><td colspan="5" class="muted" style="text-align:center;">暂无数据</td></tr>';
  else bj.forEach(function(j) {
    tb2 += '<tr><td><b>' + esc(jobTypeLabel(j.job_type)) + '</b></td>';
    tb2 += '<td>' + round1(j.total_minutes) + '</td>';
    tb2 += '<td>' + (j.worker_count || 0) + '</td>';
    tb2 += '<td>' + (j.job_count || 0) + '</td>';
    tb2 += '<td>' + round1(j.avg_minutes_per_worker) + '</td></tr>';
  });
  document.getElementById("whByJobTypeBody").innerHTML = tb2;

  // 表 3: 段明细 (前 200 条)
  var segs = res.segments || [];
  var tb3 = '';
  if (segs.length === 0) tb3 = '<tr><td colspan="8" class="muted" style="text-align:center;">暂无数据</td></tr>';
  else segs.slice(0, 200).forEach(function(s) {
    var anomalyTag = s.anomaly
      ? ' <span class="tag tag-red" style="font-size:10px;" title="' + esc(s.anomaly_reason || '') + '">异常</span>'
      : '';
    var longTag = s.long_segment ? ' <span class="tag tag-orange" style="font-size:10px;">长工时</span>' : '';
    var activeTag = s.active ? ' <span class="tag tag-green" style="font-size:10px;">在岗</span>' : '';
    tb3 += '<tr><td>' + esc(s.worker_name) + '</td>';
    tb3 += '<td>' + esc((s.joined_at || '').slice(0, 10)) + '</td>';
    tb3 += '<td>' + esc(jobTypeLabel(s.job_type)) + '</td>';
    tb3 += '<td>' + esc(s.display_no || s.job_id) + '</td>';
    tb3 += '<td>' + esc(fmtTime(s.joined_at)) + '</td>';
    tb3 += '<td>' + esc(fmtTime(s.left_at)) + '</td>';
    tb3 += '<td>' + round1(s.minutes || 0) + activeTag + longTag + anomalyTag + '</td>';
    tb3 += '<td>' + statusTag(s.status) + '</td></tr>';
  });
  document.getElementById("whSegmentsBody").innerHTML = tb3;
  if (res.truncated) {
    document.getElementById("whSegmentsHint").textContent = '* 段数已截断为 1000 条，请缩小日期/筛选范围';
  } else {
    document.getElementById("whSegmentsHint").textContent = '';
  }
}

function exportWorkhoursSegments() {
  if (!_whSummary) { alert("请先查询"); return; }
  var rows = (_whSummary.segments || []).map(function(s) {
    return {
      员工: s.worker_name, 日期: (s.joined_at || '').slice(0, 10),
      任务类型: jobTypeLabel(s.job_type), 任务号: s.display_no || s.job_id,
      开始: fmtTime(s.joined_at), 结束: fmtTime(s.left_at),
      分钟: round1(s.minutes), 状态: statusLabel(s.status),
      在岗: s.active, 异常: s.anomaly ? 1 : 0, 异常原因: s.anomaly_reason || '',
      长工时: s.long_segment ? 1 : 0
    };
  });
  exportCsv("workhour_segments_" + defaultDateRangeToday() + ".csv", rows);
}

// =====================================================
// WMS 导入 (wms)
// =====================================================
var _wmsHeaders = [];
var _wmsRows = [];      // normalized rows ready to upload
var _wmsRawRows = [];   // 原始解析行
var _wmsFieldMap = {};  // 标准字段 → 原始表头

function normalizeHeader(h) {
  return String(h || '').trim().toLowerCase().replace(/\s+/g, '');
}

function detectFieldMap(headers) {
  var map = {};
  var normHeaders = headers.map(normalizeHeader);
  Object.keys(WMS_FIELD_ALIASES).forEach(function(stdField) {
    var aliases = WMS_FIELD_ALIASES[stdField];
    for (var i = 0; i < normHeaders.length; i++) {
      var h = normHeaders[i];
      for (var k = 0; k < aliases.length; k++) {
        var a = String(aliases[k]).toLowerCase().replace(/\s+/g, '');
        if (h === a) { map[stdField] = headers[i]; return; }
      }
    }
  });
  return map;
}

function normalizeWmsRows(rawRows, headerMap, importType) {
  return rawRows.map(function(row) {
    var out = { import_type: importType, raw: row };
    Object.keys(headerMap).forEach(function(stdField) {
      var origHeader = headerMap[stdField];
      var v = row[origHeader];
      if (v == null) v = '';
      out[stdField] = v;
    });
    // 数字字段
    out.qty = Number(out.qty) || 0;
    out.box_count = Number(out.box_count) || 0;
    // 日期：尽量挤成 YYYY-MM-DD
    if (out.work_date) {
      var s = String(out.work_date).trim();
      // Excel 序列号
      if (/^\d{4,6}$/.test(s) && Number(s) > 30000) {
        var d = new Date((Number(s) - 25569) * 86400 * 1000);
        s = d.toISOString().slice(0, 10);
      } else {
        s = s.replace(/\//g, '-').slice(0, 10);
      }
      out.work_date = s;
    }
    return out;
  });
}

async function parseExcelOrCsv(file) {
  return new Promise(function(resolve, reject) {
    var reader = new FileReader();
    reader.onload = function(e) {
      try {
        var data = new Uint8Array(e.target.result);
        if (typeof XLSX === 'undefined') return reject(new Error('SheetJS 未加载'));
        var wb = XLSX.read(data, { type: 'array', cellDates: true });
        var sheetName = wb.SheetNames[0];
        var sheet = wb.Sheets[sheetName];
        var rows = XLSX.utils.sheet_to_json(sheet, { defval: '', raw: false });
        var headers = [];
        if (rows.length > 0) headers = Object.keys(rows[0]);
        else {
          // 退化：用 sheet_to_json header:1
          var arr = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
          if (arr.length > 0) headers = arr[0];
        }
        resolve({ headers: headers, rows: rows });
      } catch(err) { reject(err); }
    };
    reader.onerror = reject;
    reader.readAsArrayBuffer(file);
  });
}

async function onWmsFilePick(input) {
  var file = input.files[0];
  if (!file) return;
  var status = document.getElementById("wmsParseStatus");
  status.textContent = '解析中...';
  try {
    var parsed = await parseExcelOrCsv(file);
    _wmsHeaders = parsed.headers;
    _wmsRawRows = parsed.rows;
    _wmsFieldMap = detectFieldMap(_wmsHeaders);
    var importType = document.getElementById("wmsImportType").value;
    _wmsRows = normalizeWmsRows(_wmsRawRows, _wmsFieldMap, importType);
    document.getElementById("wmsFileName").textContent = file.name + ' (' + _wmsRawRows.length + ' 行)';
    renderWmsPreview();
    status.textContent = '';
    document.getElementById("wmsImportBtn").disabled = (_wmsRows.length === 0);
  } catch(e) {
    status.textContent = '解析失败: ' + e.message;
    _wmsRows = [];
    document.getElementById("wmsImportBtn").disabled = true;
  }
}

function renderWmsPreview() {
  // 1) headers + map
  var mapHtml = '<table class="data-table"><thead><tr><th>标准字段</th><th>识别到的表头</th></tr></thead><tbody>';
  Object.keys(WMS_FIELD_ALIASES).forEach(function(f) {
    var got = _wmsFieldMap[f] || '<span style="color:#bbb;">未识别</span>';
    mapHtml += '<tr><td><b>' + esc(f) + '</b></td><td>' + got + '</td></tr>';
  });
  mapHtml += '</tbody></table>';
  document.getElementById("wmsFieldMap").innerHTML = mapHtml;

  // 2) preview top 20
  var preview = _wmsRows.slice(0, 20);
  if (preview.length === 0) {
    document.getElementById("wmsPreviewBody").innerHTML = '<tr><td colspan="9" class="muted" style="text-align:center;">暂无数据</td></tr>';
    return;
  }
  var html = '';
  preview.forEach(function(r) {
    html += '<tr>';
    html += '<td>' + esc(r.work_date || '') + '</td>';
    html += '<td>' + esc(r.operated_at || '') + '</td>';
    html += '<td>' + esc(r.worker_name || '') + '</td>';
    html += '<td>' + esc(r.doc_no || '') + '</td>';
    html += '<td>' + esc(r.customer || '') + '</td>';
    html += '<td>' + esc(r.sku || '') + '</td>';
    html += '<td>' + (r.qty || 0) + '</td>';
    html += '<td>' + (r.box_count || 0) + '</td>';
    html += '<td>' + esc(r.operation_type || '') + '</td>';
    html += '</tr>';
  });
  document.getElementById("wmsPreviewBody").innerHTML = html;
}

function onWmsImportTypeChange() {
  if (_wmsRawRows.length > 0) {
    var importType = document.getElementById("wmsImportType").value;
    _wmsRows = normalizeWmsRows(_wmsRawRows, _wmsFieldMap, importType);
    renderWmsPreview();
  }
}

async function submitWmsImport(btn) {
  if (_wmsRows.length === 0) { alert('请先选择文件'); return; }
  if (_wmsRows.length > 5000) { alert('单次最多导入 5000 行，请分批'); return; }
  var importType = document.getElementById("wmsImportType").value;
  var uploadedBy = (document.getElementById("wmsUploadedBy").value || '').trim();
  if (!uploadedBy) { alert('请填写导入人姓名'); return; }
  var fileName = document.getElementById("wmsFileName").textContent.split(' (')[0] || '';
  setBtnLoading(btn, true);
  var res = await api({
    action: "v2_dashboard_wms_import",
    import_type: importType,
    file_name: fileName,
    uploaded_by: uploadedBy,
    headers: _wmsHeaders,
    rows: _wmsRows,
    client_req_id: 'WMS-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8)
  });
  setBtnLoading(btn, false, "导入");
  if (res && res.ok) {
    alert('导入成功 batch_id=' + res.batch_id + '，共 ' + res.row_count + ' 行');
    _wmsRows = []; _wmsRawRows = []; _wmsHeaders = []; _wmsFieldMap = {};
    document.getElementById("wmsFile").value = '';
    document.getElementById("wmsFileName").textContent = '';
    document.getElementById("wmsPreviewBody").innerHTML = '';
    document.getElementById("wmsFieldMap").innerHTML = '';
    document.getElementById("wmsImportBtn").disabled = true;
    loadWmsBatches();
  } else {
    alert('导入失败: ' + (res ? (res.message || res.error) : 'unknown'));
  }
}

async function loadWmsBatches() {
  var tbody = document.getElementById("wmsBatchesBody");
  tbody.innerHTML = '<tr><td colspan="8" class="muted" style="text-align:center;">加载中...</td></tr>';
  var res = await api({ action: "v2_dashboard_wms_batches", limit: 100, offset: 0 });
  if (!res || !res.ok) {
    tbody.innerHTML = '<tr><td colspan="8" class="muted" style="text-align:center;">加载失败</td></tr>';
    return;
  }
  var items = res.items || [];
  if (items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="muted" style="text-align:center;">尚无导入批次</td></tr>';
    return;
  }
  var html = '';
  items.forEach(function(b) {
    html += '<tr>';
    html += '<td>' + esc(fmtTime(b.created_at)) + '</td>';
    html += '<td>' + esc(importTypeLabel(b.import_type)) + '</td>';
    html += '<td>' + esc(b.file_name || '--') + '</td>';
    html += '<td>' + (b.row_count || 0) + '</td>';
    html += '<td>' + esc(b.date_from || '--') + ' ~ ' + esc(b.date_to || '--') + '</td>';
    html += '<td>' + esc(b.uploaded_by || '--') + '</td>';
    html += '<td>' + esc(b.status || '--') + '</td>';
    html += '<td><button class="row-action" onclick="openWmsBatchDetail(\'' + esc(b.id) + '\')">查看</button></td>';
    html += '</tr>';
  });
  tbody.innerHTML = html;
}

async function openWmsBatchDetail(batchId) {
  var modal = document.getElementById("wmsBatchModal");
  var body = document.getElementById("wmsBatchBody");
  body.innerHTML = '<div class="muted">加载中...</div>';
  modal.style.display = '';
  var res = await api({ action: "v2_dashboard_wms_batch_detail", batch_id: batchId, limit: 200, offset: 0 });
  if (!res || !res.ok) {
    body.innerHTML = '<div class="muted">加载失败</div>';
    return;
  }
  var b = res.batch || {};
  var rows = res.rows || [];
  var html = '<h3>批次信息</h3>';
  html += '<div class="muted">' + esc(b.id) + ' · ' + esc(importTypeLabel(b.import_type)) +
    ' · ' + esc(b.file_name) + ' · ' + (b.row_count || 0) + ' 行 · ' + esc(b.uploaded_by) + '</div>';
  html += '<h3 style="margin-top:14px;">前 ' + rows.length + ' 行明细</h3>';
  html += '<table class="data-table"><thead><tr><th>日期</th><th>时间</th><th>员工</th><th>单号</th><th>客户</th><th>SKU</th><th>数量</th><th>箱数</th></tr></thead><tbody>';
  rows.forEach(function(r) {
    html += '<tr><td>' + esc(r.work_date) + '</td><td>' + esc(r.operated_at) + '</td>';
    html += '<td>' + esc(r.worker_name) + '</td><td>' + esc(r.doc_no) + '</td>';
    html += '<td>' + esc(r.customer) + '</td><td>' + esc(r.sku) + '</td>';
    html += '<td>' + (r.qty || 0) + '</td><td>' + (r.box_count || 0) + '</td></tr>';
  });
  html += '</tbody></table>';
  body.innerHTML = html;
}

function closeWmsBatchDetail() {
  document.getElementById("wmsBatchModal").style.display = 'none';
}

// =====================================================
// 管理看板 (management)
// =====================================================
async function loadManagement(btn) {
  setBtnLoading(btn, true);
  var start = document.getElementById("mgmtFilterStart").value || '';
  var end = document.getElementById("mgmtFilterEnd").value || '';
  var res = await api({ action: "v2_dashboard_management_summary", start_date: start, end_date: end });
  setBtnLoading(btn, false, "查询");
  if (!res || !res.ok) {
    document.getElementById("mgmtSummaryRow").innerHTML = '<div class="muted">加载失败</div>';
    return;
  }
  var s = res.summary || {};
  var cards = [
    { label: '总工时', value: s.total_hours + ' h', sub: s.total_minutes + ' 分' },
    { label: 'WMS 总数量', value: fmtNumber(s.total_qty) },
    { label: 'WMS 总箱数', value: fmtNumber(s.total_boxes) },
    { label: '综合件/小时', value: s.qty_per_hour },
    { label: '综合箱/小时', value: s.boxes_per_hour },
    { label: '活跃员工', value: s.worker_count || 0 },
    { label: '异常工时段', value: s.anomaly_count || 0 }
  ];
  var ch = '';
  cards.forEach(function(c) {
    ch += '<div class="stat-card"><div class="stat-label">' + esc(c.label) + '</div>';
    ch += '<div class="stat-value">' + esc(c.value) + '</div>';
    if (c.sub) ch += '<div class="stat-sub">' + esc(c.sub) + '</div>';
    ch += '</div>';
  });
  document.getElementById("mgmtSummaryRow").innerHTML = ch;

  var bj = res.by_job_type || [];
  var tb1 = '';
  if (bj.length === 0) tb1 = '<tr><td colspan="6" class="muted" style="text-align:center;">暂无数据</td></tr>';
  else bj.forEach(function(j) {
    tb1 += '<tr><td><b>' + esc(jobTypeLabel(j.job_type)) + '</b></td>';
    tb1 += '<td>' + (j.total_hours || 0) + '</td>';
    tb1 += '<td>' + fmtNumber(j.wms_qty) + '</td>';
    tb1 += '<td>' + fmtNumber(j.wms_boxes) + '</td>';
    tb1 += '<td>' + (j.qty_per_hour || 0) + '</td>';
    tb1 += '<td>' + (j.boxes_per_hour || 0) + '</td></tr>';
  });
  document.getElementById("mgmtByJobTypeBody").innerHTML = tb1;

  var bw = res.by_worker || [];
  var tb2 = '';
  if (bw.length === 0) tb2 = '<tr><td colspan="6" class="muted" style="text-align:center;">暂无数据</td></tr>';
  else bw.forEach(function(w) {
    tb2 += '<tr><td><b>' + esc(w.worker_name) + '</b></td>';
    tb2 += '<td>' + (w.total_hours || 0) + '</td>';
    tb2 += '<td>' + fmtNumber(w.wms_qty) + '</td>';
    tb2 += '<td>' + fmtNumber(w.wms_boxes) + '</td>';
    tb2 += '<td>' + (w.qty_per_hour || 0) + '</td>';
    tb2 += '<td>' + (w.boxes_per_hour || 0) + '</td></tr>';
  });
  document.getElementById("mgmtByWorkerBody").innerHTML = tb2;
}

// ===== Init =====
checkAuth();
window.addEventListener("DOMContentLoaded", function() {
  var keyEl = document.getElementById("loginKey");
  if (keyEl) keyEl.addEventListener("keydown", function(e) {
    if (e.key === "Enter") doLogin();
  });
});
