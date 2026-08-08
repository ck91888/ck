/* CK Warehouse V2 — 003 Consumables & Assets */
var S003 = {
  lang: localStorage.getItem(V2_003_LANG_KEY) || 'zh',
  key: '', role: '', badge: '', operatorId: '', operatorName: '',
  locations: [], currentMaterial: null, currentAsset: null, ledgerRows: [],
  badgeScanner: null, itemScanner: null, currentView: 'dashboard', busy: false
};

function E(id) { return document.getElementById(id); }
function esc(v) { return String(v == null ? '' : v).replace(/[&<>'"]/g, function(c) { return ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'})[c]; }); }
function jsq(v) { return String(v == null ? '' : v).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); }
function T(key) { return (LANG[S003.lang] && LANG[S003.lang][key]) || (LANG.zh && LANG.zh[key]) || key; }
function val(id) { return (E(id) && E(id).value || '').trim(); }
function num(id) { var n = Number(E(id) && E(id).value); return Number.isFinite(n) ? n : 0; }
function kstToday() { return new Date(Date.now() + 9 * 3600000).toISOString().slice(0, 10); }
function fmtTime(iso) { if (!iso) return '--'; try { return new Date(iso).toLocaleString(S003.lang === 'ko' ? 'ko-KR' : 'zh-CN', { timeZone:'Asia/Seoul', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' }); } catch(e) { return iso; } }
function fmtDate(iso) { if (!iso) return '--'; return String(iso).slice(0, 10); }
function fmtQty(n) { n = Number(n) || 0; return Number.isInteger(n) ? String(n) : String(Math.round(n * 100) / 100); }
function fmtMoney(n, currency) { n = Number(n) || 0; if (!n) return '--'; return n.toLocaleString() + ' ' + (currency || 'KRW'); }
function reqId(prefix) { return (prefix || '003') + '-' + Date.now().toString(36) + '-' + crypto.getRandomValues(new Uint32Array(1))[0].toString(36); }
function isAdmin() { return S003.role === 'admin'; }
function canOperate() { return S003.role === 'admin' || S003.role === 'operator'; }

function applyI18n() {
  document.documentElement.lang = S003.lang === 'ko' ? 'ko' : 'zh';
  document.querySelectorAll('[data-i18n]').forEach(function(el) { el.textContent = T(el.getAttribute('data-i18n')); });
  document.querySelectorAll('[data-i18n-placeholder]').forEach(function(el) { el.placeholder = T(el.getAttribute('data-i18n-placeholder')); });
  if (S003.role) updateHeader();
}

function toggleLang() {
  S003.lang = S003.lang === 'zh' ? 'ko' : 'zh';
  localStorage.setItem(V2_003_LANG_KEY, S003.lang);
  applyI18n();
  refreshCurrentView();
}

function refreshCurrentView() {
  if (!S003.role) return;
  if (S003.currentView === 'dashboard') loadDashboard();
  else if (S003.currentView === 'materials') loadMaterials();
  else if (S003.currentView === 'material-detail' && S003.currentMaterial) renderMaterialDetail(S003.currentMaterial);
  else if (S003.currentView === 'assets') loadAssets();
  else if (S003.currentView === 'asset-detail' && S003.currentAsset) renderAssetDetail(S003.currentAsset);
  else if (S003.currentView === 'ledger') loadLedger();
  else if (S003.currentView === 'settings') loadLocations(true);
}

async function api(action, data, options) {
  var body = Object.assign({}, data || {}, { action: action, k: (options && options.key) || S003.key });
  var res;
  try {
    res = await fetch(V2_API, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
  } catch(e) { throw new Error('network_error'); }
  var json;
  try { json = await res.json(); } catch(e) { throw new Error('invalid_response'); }
  if (!res.ok || !json.ok) throw new Error(json.error || json.message || ('http_' + res.status));
  return json;
}

function errorText(code) {
  var zh = {
    unauthorized:'访问码错误或已失效', unauthorized_admin_only:'此操作仅限管理员', network_error:'网络连接失败，请稍后重试',
    invalid_response:'服务器返回异常', not_found:'没有找到对应记录', duplicate_item_code:'编号已被使用', duplicate_barcode:'条码已被使用', duplicate_serial_no:'序列号已被使用',
    insufficient_stock:'库存不足，不能提交', material_inactive:'该耗材已停用', stock_changed_retry:'库存刚被其他人修改，请刷新后重试',
    asset_changed_retry:'物品状态刚被修改，请刷新后重试', asset_not_available:'该物品当前不可领用', asset_not_assigned:'该物品当前没有被领用',
    not_current_keeper:'只有当前领用人或管理员可以归还', asset_unavailable:'该物品已报废或遗失', asset_not_in_repair:'该物品不在维修中',
    operator_required:'缺少操作人信息', positive_qty_required:'数量必须大于 0', valid_counted_qty_required:'请输入正确的实盘数量',
    name_category_unit_required:'请填写名称、分类和单位', name_category_required:'请填写名称和分类', warehouse_and_location_required:'请填写仓库和位置编码'
  };
  var ko = {
    unauthorized:'접근 코드가 올바르지 않습니다', unauthorized_admin_only:'관리자만 가능한 작업입니다', network_error:'네트워크 연결 실패',
    invalid_response:'서버 응답 오류', not_found:'해당 기록을 찾을 수 없습니다', duplicate_item_code:'이미 사용 중인 번호입니다', duplicate_barcode:'이미 사용 중인 바코드입니다', duplicate_serial_no:'이미 사용 중인 시리얼 번호입니다',
    insufficient_stock:'재고가 부족합니다', material_inactive:'사용 중지된 소모품입니다', stock_changed_retry:'재고가 변경되었습니다. 새로고침 후 다시 시도하세요',
    asset_changed_retry:'비품 상태가 변경되었습니다. 다시 시도하세요', asset_not_available:'현재 수령할 수 없는 비품입니다', asset_not_assigned:'현재 사용 중인 비품이 아닙니다',
    not_current_keeper:'현재 사용자 또는 관리자만 반납할 수 있습니다', asset_unavailable:'폐기 또는 분실 처리된 비품입니다', asset_not_in_repair:'수리 중인 비품이 아닙니다',
    operator_required:'작업자 정보가 없습니다', positive_qty_required:'수량은 0보다 커야 합니다', valid_counted_qty_required:'올바른 실사 수량을 입력하세요',
    name_category_unit_required:'명칭·분류·단위를 입력하세요', name_category_required:'명칭과 분류를 입력하세요', warehouse_and_location_required:'창고와 위치 코드를 입력하세요'
  };
  return (S003.lang === 'ko' ? ko[code] : zh[code]) || code || T('failed');
}

function toast(message, isError) {
  var el = E('toast');
  el.textContent = message;
  el.className = 'toast show' + (isError ? ' error' : '');
  clearTimeout(toast._timer);
  toast._timer = setTimeout(function() { el.className = 'toast'; }, 2600);
}

function setPage(id) {
  ['entryPage','badgePage','adminLoginPage','appWrap'].forEach(function(x) { E(x).classList.toggle('hidden', x !== id); });
}

function showEntry() { stopBadgeScanner(); setPage('entryPage'); }

async function openFieldEntry() {
  var key = localStorage.getItem(V2_003_OPS_KEY_STORAGE) || '';
  if (!key) {
    key = prompt(S003.lang === 'ko' ? '현장 접근 코드를 입력하세요' : '请输入现场访问码') || '';
    key = key.trim();
  }
  if (!key) return;
  try {
    var auth = await api('v2_003_auth_check', {}, { key: key });
    if (auth.role !== 'operator' && auth.role !== 'admin') throw new Error('unauthorized');
    S003.key = key;
    S003.role = auth.role;
    localStorage.setItem(V2_003_OPS_KEY_STORAGE, key);
  } catch(e) {
    localStorage.removeItem(V2_003_OPS_KEY_STORAGE);
    toast(errorText(e.message), true);
    return;
  }
  var badge = localStorage.getItem(V2_003_BADGE_KEY) || '';
  var day = localStorage.getItem(V2_003_BADGE_DAY_KEY) || '';
  if (badge && day === kstToday() && applyBadge(badge, false)) { bootApp(); return; }
  setPage('badgePage');
}

function openAdminLogin() {
  E('adminLoginError').textContent = '';
  E('adminKeyInput').value = localStorage.getItem(V2_003_AUTH_KEY) || '';
  setPage('adminLoginPage');
  setTimeout(function() { E('adminKeyInput').focus(); }, 80);
}

async function doAdminLogin(event) {
  event.preventDefault();
  var key = val('adminKeyInput');
  if (!key) return;
  E('adminLoginError').textContent = '';
  try {
    var auth = await api('v2_003_auth_check', {}, { key:key });
    S003.key = key; S003.role = auth.role;
    localStorage.setItem(V2_003_AUTH_KEY, key);
    localStorage.setItem(V2_003_ROLE_KEY, auth.role);
    S003.operatorId = auth.role === 'admin' ? 'ADMIN' : 'VIEWER';
    S003.operatorName = localStorage.getItem(V2_003_ADMIN_NAME_KEY) || (auth.role === 'admin' ? '管理员' : T('viewer_mode'));
    bootApp();
  } catch(e) { E('adminLoginError').textContent = errorText(e.message); }
}

function parseBadge(raw) {
  raw = String(raw || '').trim();
  var parts = raw.split('|');
  if (parts.length < 2) return null;
  var id = parts.shift().trim();
  var name = parts.join('|').trim();
  if (!id || !name || !(/^(EMP-|DA-|DAF-)/.test(id))) return null;
  return { raw:raw, id:id, name:name };
}

function applyBadge(raw, persist) {
  var badge = parseBadge(raw);
  if (!badge) return false;
  S003.badge = badge.raw; S003.operatorId = badge.id; S003.operatorName = badge.name;
  if (persist !== false) {
    localStorage.setItem(V2_003_BADGE_KEY, badge.raw);
    localStorage.setItem(V2_003_BADGE_DAY_KEY, kstToday());
  }
  return true;
}

function manualBadge() {
  var raw = prompt(S003.lang === 'ko' ? '형식: EMP-001|이름' : '格式：EMP-001|姓名');
  if (!raw) return;
  if (!applyBadge(raw, true)) { E('badgeError').textContent = S003.lang === 'ko' ? '명찰 형식이 올바르지 않습니다' : '工牌格式不正确'; return; }
  bootApp();
}

async function startBadgeScan() {
  E('badgeError').textContent = '';
  if (S003.badgeScanner) { stopBadgeScanner(); return; }
  try {
    S003.badgeScanner = new Html5Qrcode('badgeReader');
    await S003.badgeScanner.start({ facingMode:'environment' }, { fps:8, qrbox:{width:240,height:160} }, function(text) {
      if (!applyBadge(text, true)) { E('badgeError').textContent = S003.lang === 'ko' ? '명찰 QR이 아닙니다' : '不是有效工牌二维码'; return; }
      stopBadgeScanner(); bootApp();
    }, function() {});
    E('badgeScanBtn').textContent = T('close');
  } catch(e) { E('badgeError').textContent = S003.lang === 'ko' ? '카메라를 열 수 없습니다. 직접 입력하세요.' : '无法打开摄像头，请使用手动输入'; stopBadgeScanner(); }
}

function stopBadgeScanner() {
  if (!S003.badgeScanner) return;
  var scanner = S003.badgeScanner; S003.badgeScanner = null;
  Promise.resolve(scanner.stop()).catch(function() {}).finally(function() {
    try { scanner.clear(); } catch(e) {}
    if (E('badgeScanBtn')) E('badgeScanBtn').textContent = T('scan_badge');
  });
}

function bootApp() {
  stopBadgeScanner();
  setPage('appWrap');
  document.querySelectorAll('.admin-only').forEach(function(el) { el.classList.toggle('hidden', !isAdmin()); });
  E('settingsTabBtn').classList.toggle('hidden', !isAdmin());
  updateHeader();
  applyI18n();
  loadLocations(false);
  goView('dashboard');
}

function updateHeader() {
  var mode = isAdmin() ? T('admin_mode') : (S003.role === 'operator' ? T('field_mode') : T('viewer_mode'));
  E('headerMode').textContent = mode;
  E('operatorChip').textContent = S003.operatorName || mode;
  E('operatorChip').title = isAdmin() ? T('change_name') : '';
}

function changeAdminName() {
  if (!isAdmin()) return;
  var name = prompt(T('change_name'), S003.operatorName || '管理员');
  if (!name || !name.trim()) return;
  S003.operatorName = name.trim().slice(0, 50);
  localStorage.setItem(V2_003_ADMIN_NAME_KEY, S003.operatorName);
  updateHeader();
}

function logout003() {
  S003.key = ''; S003.role = ''; S003.operatorId = ''; S003.operatorName = '';
  localStorage.removeItem(V2_003_AUTH_KEY); localStorage.removeItem(V2_003_ROLE_KEY);
  closeScanner(); showEntry();
}

function goView(name, btn) {
  if (name === 'settings' && !isAdmin()) name = 'dashboard';
  document.querySelectorAll('.view').forEach(function(v) { v.classList.toggle('active', v.id === 'view-' + name); });
  S003.currentView = name;
  if (btn) {
    document.querySelectorAll('#mainTabs button').forEach(function(b) { b.classList.remove('active'); });
    btn.classList.add('active');
  } else if (['dashboard','materials','assets','ledger','settings'].includes(name)) {
    document.querySelectorAll('#mainTabs button').forEach(function(b) { b.classList.toggle('active', b.getAttribute('data-view') === name); });
  }
  window.scrollTo({ top:0, behavior:'smooth' });
  if (name === 'dashboard') loadDashboard();
  if (name === 'materials') loadMaterials();
  if (name === 'assets') loadAssets();
  if (name === 'ledger') loadLedger();
  if (name === 'settings') loadLocations(true);
}

function operatorPayload() { return { operator_id:S003.operatorId, operator_name:S003.operatorName }; }

async function loadDashboard() {
  E('dashboardStats').innerHTML = '<div class="empty">' + esc(T('loading')) + '</div>';
  try {
    var res = await api('v2_003_dashboard');
    var s = res.summary || {};
    var stats = [
      [T('material_count'), s.material_count || 0, ''], [T('low_stock'), s.low_stock_count || 0, (s.low_stock_count || 0) ? 'alert' : 'good'],
      [T('asset_count'), s.asset_count || 0, ''], [T('assigned_assets'), s.assigned_asset_count || 0, ''],
      [T('repair_assets'), s.repair_asset_count || 0, (s.repair_asset_count || 0) ? 'alert' : ''],
      [T('today_records'), (s.material_txn_today || 0) + (s.asset_txn_today || 0), 'good']
    ];
    E('dashboardStats').innerHTML = stats.map(function(x) { return '<div class="stat-card '+x[2]+'"><div class="stat-label">'+esc(x[0])+'</div><div class="stat-value">'+esc(x[1])+'</div></div>'; }).join('');
    E('recentActivity').innerHTML = (res.recent || []).length ? res.recent.map(activityHtml).join('') : '<div class="empty">'+esc(T('no_data'))+'</div>';
  } catch(e) { E('dashboardStats').innerHTML = '<div class="empty">'+esc(errorText(e.message))+'</div>'; }
}

function activityHtml(r) {
  var action = r.kind === 'material' ? txnLabel(r.txn_type) : assetActionLabel(r.action_type);
  var extra = r.kind === 'material' ? ((Number(r.qty_delta) > 0 ? '+' : '') + fmtQty(r.qty_delta) + ' ' + (r.unit || '')) : assetStatusLabel(r.status_after);
  return '<div class="activity-item"><div class="activity-dot '+esc(r.kind)+'">'+(r.kind === 'material' ? '耗' : '物')+'</div><div class="activity-main"><b>'+esc(action)+' · '+esc(r.item_name || r.item_code)+'</b><small>'+esc(r.operator_name || '--')+' · '+esc(extra)+'</small></div><div class="activity-time">'+esc(fmtTime(r.created_at))+'</div></div>';
}

function fillDatalist(id, values) { E(id).innerHTML = (values || []).map(function(v) { return '<option value="'+esc(v)+'"></option>'; }).join(''); }

async function loadLocations(renderSettings) {
  try {
    var res = await api('v2_003_location_list', { include_inactive: isAdmin() ? 1 : 0 });
    S003.locations = res.items || [];
    var warehouses = Array.from(new Set(S003.locations.map(function(x) { return x.warehouse_name; }).filter(Boolean)));
    fillDatalist('warehouseList', warehouses);
    fillDatalist('locationList', S003.locations.map(function(x) { return x.location_code; }).filter(Boolean));
    if (renderSettings) renderLocations();
  } catch(e) { if (renderSettings) E('locationListBody').innerHTML = '<div class="empty">'+esc(errorText(e.message))+'</div>'; }
}

function renderLocations() {
  var body = E('locationListBody');
  if (!S003.locations.length) { body.innerHTML = '<div class="empty">'+esc(T('no_data'))+'</div>'; return; }
  body.innerHTML = S003.locations.map(function(x) {
    return '<div class="settings-row"><b>'+esc(x.warehouse_name)+'</b><span>'+esc(x.location_code)+'</span><span>'+esc(x.location_name || '--')+'</span><button class="btn mini soft" onclick="editLocation(\''+jsq(x.id)+'\')">'+esc(T('edit'))+'</button></div>';
  }).join('');
}

function populateStaticLists() {
  fillDatalist('materialCategoryList', MATERIAL_CATEGORIES); fillDatalist('assetCategoryList', ASSET_CATEGORIES); fillDatalist('materialUnitList', MATERIAL_UNITS);
  E('materialCategory').innerHTML = '<option value="">'+esc(T('all_categories'))+'</option>' + MATERIAL_CATEGORIES.map(function(x){return '<option value="'+esc(x)+'">'+esc(x)+'</option>';}).join('');
  E('assetCategory').innerHTML = '<option value="">'+esc(T('all_categories'))+'</option>' + ASSET_CATEGORIES.map(function(x){return '<option value="'+esc(x)+'">'+esc(x)+'</option>';}).join('');
}

async function loadMaterials() {
  E('materialList').innerHTML = '<div class="empty">'+esc(T('loading'))+'</div>';
  try {
    var res = await api('v2_003_material_list', { search:val('materialSearch'), category:val('materialCategory'), status:val('materialStatus'), low_stock_only:E('lowStockOnly').checked ? 1 : 0, limit:200 });
    E('materialList').innerHTML = res.items.length ? res.items.map(materialCardHtml).join('') : '<div class="empty">'+esc(T('no_data'))+'</div>';
  } catch(e) { E('materialList').innerHTML = '<div class="empty">'+esc(errorText(e.message))+'</div>'; }
}

function materialCardHtml(m) {
  var name = S003.lang === 'ko' && m.name_ko ? m.name_ko : m.name_zh;
  return '<article class="item-card '+(Number(m.low_stock) ? 'low' : '')+'" onclick="openMaterialDetail(\''+jsq(m.id)+'\')"><div class="item-head"><div class="item-title"><b>'+esc(name)+'</b><small>'+esc(m.material_code)+(m.spec ? ' · '+esc(m.spec) : '')+'</small></div><div class="stock-number">'+esc(fmtQty(m.current_qty))+'<small>'+esc(m.unit)+'</small></div></div><div class="item-meta"><span class="tag blue">'+esc(m.category)+'</span>'+(Number(m.low_stock) ? '<span class="tag red">'+esc(T('low_stock_badge'))+'</span>' : '')+'<span>⌖ '+esc([m.warehouse_name,m.location_code].filter(Boolean).join(' / ') || '--')+'</span><span>'+esc(T('min_stock'))+': '+esc(fmtQty(m.min_qty))+'</span></div></article>';
}

async function openMaterialDetail(id) {
  goView('material-detail');
  E('materialDetail').innerHTML = '<div class="empty">'+esc(T('loading'))+'</div>';
  try { var res = await api('v2_003_material_detail', { id:id }); S003.currentMaterial = res; renderMaterialDetail(res); }
  catch(e) { E('materialDetail').innerHTML = '<div class="empty">'+esc(errorText(e.message))+'</div>'; }
}

function renderMaterialDetail(res) {
  var m = res.item || {}; var name = S003.lang === 'ko' && m.name_ko ? m.name_ko : m.name_zh;
  var actions = '';
  if (canOperate()) {
    actions += '<button class="btn warning" onclick="openMaterialTxn(\'issue\')">'+esc(T('issue'))+'</button>';
    actions += '<button class="btn warning" onclick="openMaterialTxn(\'use\')">'+esc(T('use'))+'</button>';
    actions += '<button class="btn soft" onclick="openMaterialTxn(\'return\')">'+esc(T('return_item'))+'</button>';
  }
  if (isAdmin()) {
    actions += '<button class="btn success" onclick="openMaterialTxn(\'inbound\')">'+esc(T('inbound'))+'</button>';
    actions += '<button class="btn soft" onclick="openMaterialTxn(\'stocktake\')">'+esc(T('stocktake'))+'</button>';
    actions += '<button class="btn soft" onclick="openMaterialTxn(\'adjust\')">'+esc(T('adjust'))+'</button>';
    actions += '<button class="btn soft" onclick="openMaterialForm(S003.currentMaterial.item)">'+esc(T('edit'))+'</button>';
  }
  var info = [
    [T('category'),m.category],[T('spec'),m.spec],[T('warehouse'),m.warehouse_name],[T('location'),m.location_code],
    [T('min_stock'),fmtQty(m.min_qty)+' '+(m.unit||'')],[T('cost'),fmtMoney(m.unit_cost,m.currency)],[T('supplier'),m.supplier],[T('status'),m.status === 'active' ? T('active') : T('inactive')]
  ];
  var history = (res.transactions || []).length ? (res.transactions || []).map(materialTxnHtml).join('') : '<div class="empty">'+esc(T('no_data'))+'</div>';
  E('materialDetail').innerHTML = '<div class="detail-hero"><div class="hero-top"><div><h1>'+esc(name)+'</h1><p>'+esc(m.material_code)+(m.barcode ? ' · '+esc(m.barcode) : '')+'</p></div><div class="hero-qty">'+esc(fmtQty(m.current_qty))+'<small> '+esc(m.unit)+'</small></div></div><div class="detail-actions">'+actions+'</div></div><div class="panel"><div class="info-grid">'+info.map(infoCell).join('')+'</div>'+(m.note ? '<p class="item-meta">'+esc(T('note'))+': '+esc(m.note)+'</p>' : '')+'</div><div class="panel"><div class="panel-head"><h2>'+esc(T('transaction_history'))+'</h2></div><div class="timeline">'+history+'</div></div>';
}

function infoCell(x) { return '<div class="info-cell"><small>'+esc(x[0])+'</small><b>'+esc(x[1] || '--')+'</b></div>'; }
function txnLabel(t) { return ({opening:S003.lang==='ko'?'기초재고':'期初库存',inbound:T('inbound'),issue:T('issue'),use:T('use'),return:T('return_item'),adjust:T('adjust'),stocktake:T('stocktake')})[t] || t; }
function materialTxnHtml(t) { var delta = Number(t.qty_delta)||0; return '<div class="timeline-row"><div class="timeline-action">'+esc(txnLabel(t.txn_type))+' <span class="tag '+(delta<0?'orange':'green')+'">'+(delta>0?'+':'')+esc(fmtQty(delta))+'</span></div><div class="timeline-detail">'+esc(t.operator_name||'--')+(t.recipient_name?' → '+esc(t.recipient_name):'')+'<small>'+esc([t.purpose,t.related_doc_no,t.note].filter(Boolean).join(' · ')||'--')+' · '+esc(fmtQty(t.qty_before))+' → '+esc(fmtQty(t.qty_after))+'</small></div><div class="timeline-time">'+esc(fmtTime(t.created_at))+'</div></div>'; }

function clearMaterialForm() { ['mfId','mfCode','mfBarcode','mfNameZh','mfNameKo','mfCategory','mfSpec','mfUnit','mfWarehouse','mfLocation','mfCost','mfSupplier','mfNote'].forEach(function(id){E(id).value='';}); E('mfOpening').value='0'; E('mfMin').value='0'; E('mfStatus').value='active'; }
function openMaterialForm(item) {
  clearMaterialForm();
  if (item) {
    E('materialFormTitle').textContent = T('edit'); E('mfId').value=item.id; E('mfCode').value=item.material_code||''; E('mfBarcode').value=item.barcode||'';
    E('mfNameZh').value=item.name_zh||''; E('mfNameKo').value=item.name_ko||''; E('mfCategory').value=item.category||''; E('mfSpec').value=item.spec||'';
    E('mfUnit').value=item.unit||''; E('mfWarehouse').value=item.warehouse_name||''; E('mfLocation').value=item.location_code||''; E('mfMin').value=item.min_qty||0;
    E('mfCost').value=item.unit_cost||''; E('mfSupplier').value=item.supplier||''; E('mfStatus').value=item.status||'active'; E('mfNote').value=item.note||''; E('mfOpeningWrap').classList.add('hidden');
  } else { E('materialFormTitle').textContent=T('add_material'); E('mfOpeningWrap').classList.remove('hidden'); }
  goView('material-edit');
}

async function saveMaterial(event) {
  event.preventDefault(); if (S003.busy) return; S003.busy=true;
  var data = Object.assign(operatorPayload(), { id:val('mfId'), material_code:val('mfCode'), barcode:val('mfBarcode'), name_zh:val('mfNameZh'), name_ko:val('mfNameKo'), category:val('mfCategory'), spec:val('mfSpec'), unit:val('mfUnit'), warehouse_name:val('mfWarehouse'), location_code:val('mfLocation'), opening_qty:num('mfOpening'), min_qty:num('mfMin'), unit_cost:num('mfCost'), currency:'KRW', supplier:val('mfSupplier'), status:val('mfStatus'), note:val('mfNote') });
  try { var res=await api('v2_003_material_save',data); toast(T('success')); await loadLocations(false); openMaterialDetail(res.id); }
  catch(e){ toast(errorText(e.message),true); } finally { S003.busy=false; }
}

function openMaterialTxn(type) {
  var m=S003.currentMaterial&&S003.currentMaterial.item; if(!m)return;
  E('mtMaterialId').value=m.id; E('mtType').value=type; E('mtTitle').textContent=txnLabel(type); E('mtItemInfo').textContent=(S003.lang==='ko'&&m.name_ko?m.name_ko:m.name_zh)+' · '+fmtQty(m.current_qty)+' '+m.unit;
  ['mtQty','mtPurpose','mtDoc','mtNote'].forEach(function(id){E(id).value='';}); E('mtCost').value=m.unit_cost||''; E('mtSupplier').value=m.supplier||''; E('mtRecipient').value=S003.operatorName||'';
  E('mtQty').min=type==='adjust'?'':'0.01'; E('mtQtyLabel').textContent=type==='stocktake'?T('counted_qty'):(type==='adjust'?T('delta_qty'):T('qty'));
  E('mtInboundFields').classList.toggle('hidden',type!=='inbound'); E('mtPeopleFields').classList.toggle('hidden',['inbound','adjust','stocktake'].includes(type));
  E('materialTxnModal').classList.remove('hidden'); setTimeout(function(){E('mtQty').focus();},80);
}

async function submitMaterialTxn(event) {
  event.preventDefault(); if(S003.busy)return; S003.busy=true;
  var type=val('mtType'), qty=num('mtQty'); var data=Object.assign(operatorPayload(),{material_id:val('mtMaterialId'),txn_type:type,recipient_name:val('mtRecipient'),purpose:val('mtPurpose'),related_doc_no:val('mtDoc'),unit_cost:num('mtCost'),supplier:val('mtSupplier'),note:val('mtNote'),client_req_id:reqId('mtx')});
  if(type==='adjust')data.delta=qty; else if(type==='stocktake')data.counted_qty=qty; else data.qty=qty;
  if(type==='issue'&&val('mtRecipient')===S003.operatorName)data.recipient_id=S003.operatorId;
  try { await api('v2_003_material_txn',data); closeModal('materialTxnModal'); toast(T('success')); openMaterialDetail(data.material_id); }
  catch(e){toast(errorText(e.message),true);} finally{S003.busy=false;}
}

async function loadAssets() {
  E('assetList').innerHTML='<div class="empty">'+esc(T('loading'))+'</div>';
  try { var res=await api('v2_003_asset_list',{search:val('assetSearch'),category:val('assetCategory'),status:val('assetStatus'),limit:200}); E('assetList').innerHTML=res.items.length?res.items.map(assetCardHtml).join(''):'<div class="empty">'+esc(T('no_data'))+'</div>'; }
  catch(e){E('assetList').innerHTML='<div class="empty">'+esc(errorText(e.message))+'</div>';}
}

function assetStatusLabel(s){return ({available:T('available'),assigned:T('assigned'),repair:T('repair'),retired:T('retired'),lost:T('lost')})[s]||s;}
function statusTagClass(s){return ({available:'green',assigned:'blue',repair:'orange',retired:'gray',lost:'red'})[s]||'gray';}
function assetCardHtml(a){var name=S003.lang==='ko'&&a.name_ko?a.name_ko:a.name_zh;return '<article class="item-card" onclick="openAssetDetail(\''+jsq(a.id)+'\')"><div class="item-head"><div class="item-title"><b>'+esc(name)+'</b><small>'+esc(a.asset_code)+(a.model?' · '+esc(a.model):'')+'</small></div><span class="tag '+statusTagClass(a.status)+'">'+esc(assetStatusLabel(a.status))+'</span></div><div class="item-meta"><span class="tag blue">'+esc(a.category)+'</span><span>⌖ '+esc([a.warehouse_name,a.location_code].filter(Boolean).join(' / ')||'--')+'</span>'+(a.keeper_name?'<span>◎ '+esc(a.keeper_name)+'</span>':'')+'</div></article>';}

async function openAssetDetail(id){goView('asset-detail');E('assetDetail').innerHTML='<div class="empty">'+esc(T('loading'))+'</div>';try{var res=await api('v2_003_asset_detail',{id:id});S003.currentAsset=res;renderAssetDetail(res);}catch(e){E('assetDetail').innerHTML='<div class="empty">'+esc(errorText(e.message))+'</div>';}}

function assetActionLabel(a){return ({create:S003.lang==='ko'?'등록':'建档',edit:T('edit'),assign:T('assign'),return:T('return_asset'),transfer:T('transfer'),repair_start:T('repair_start'),repair_done:T('repair_done'),retire:T('retire'),lost:T('mark_lost')})[a]||a;}
function renderAssetDetail(res){
  var a=res.item||{},name=S003.lang==='ko'&&a.name_ko?a.name_ko:a.name_zh,actions='';
  if(canOperate()&&a.status==='available')actions+='<button class="btn success" onclick="openAssetAction(\'assign\')">'+esc(T('assign'))+'</button>';
  if(canOperate()&&a.status==='assigned')actions+='<button class="btn soft" onclick="openAssetAction(\'return\')">'+esc(T('return_asset'))+'</button>';
  if(isAdmin()){
    actions+='<button class="btn soft" onclick="openAssetForm(S003.currentAsset.item)">'+esc(T('edit'))+'</button><button class="btn soft" onclick="openAssetAction(\'transfer\')">'+esc(T('transfer'))+'</button>';
    if(a.status!=='repair'&&!['retired','lost'].includes(a.status))actions+='<button class="btn warning" onclick="openAssetAction(\'repair_start\')">'+esc(T('repair_start'))+'</button>';
    if(a.status==='repair')actions+='<button class="btn success" onclick="openAssetAction(\'repair_done\')">'+esc(T('repair_done'))+'</button>';
    if(!['retired','lost'].includes(a.status))actions+='<button class="btn danger" onclick="openAssetAction(\'retire\')">'+esc(T('retire'))+'</button><button class="btn danger" onclick="openAssetAction(\'lost\')">'+esc(T('mark_lost'))+'</button>';
    actions+='<button class="btn soft" onclick="printAssetQr()">'+esc(T('print_qr'))+'</button><button class="btn soft" onclick="E(\'assetPhotoInput\').click()">'+esc(T('upload_photo'))+'</button>';
  }
  var info=[[T('category'),a.category],[T('brand'),a.brand],[T('model'),a.model],[T('serial_no'),a.serial_no],[T('warehouse'),a.warehouse_name],[T('location'),a.location_code],[T('keeper'),a.keeper_name||'--'],[T('status'),assetStatusLabel(a.status)],[T('purchase_date'),fmtDate(a.purchase_date)],[T('purchase_cost'),fmtMoney(a.purchase_cost,a.currency)],[T('supplier'),a.supplier],[T('warranty_until'),fmtDate(a.warranty_until)]];
  var photos=(res.attachments||[]).length?'<div class="panel"><div class="panel-head"><h2>'+esc(T('photos'))+'</h2></div><div class="photo-grid">'+res.attachments.map(function(p){return '<a href="'+esc(fileUrl(p.file_key))+'" target="_blank"><img src="'+esc(fileUrl(p.file_key))+'" alt="'+esc(p.file_name)+'"></a>';}).join('')+'</div></div>':'';
  var history=(res.transactions||[]).length?res.transactions.map(assetTxnHtml).join(''):'<div class="empty">'+esc(T('no_data'))+'</div>';
  E('assetDetail').innerHTML='<div class="detail-hero"><div class="hero-top"><div><h1>'+esc(name)+'</h1><p>'+esc(a.asset_code)+(a.barcode?' · '+esc(a.barcode):'')+'</p></div><div><span class="tag '+statusTagClass(a.status)+'">'+esc(assetStatusLabel(a.status))+'</span></div></div><div class="detail-actions">'+actions+'</div></div><div class="panel"><div class="info-grid">'+info.map(infoCell).join('')+'</div>'+(a.note?'<p class="item-meta">'+esc(T('note'))+': '+esc(a.note)+'</p>':'')+'</div>'+photos+'<div class="panel"><div class="panel-head"><h2>'+esc(T('asset_history'))+'</h2></div><div class="timeline">'+history+'</div></div>';
}

function assetTxnHtml(t){var move=[t.to_warehouse,t.to_location].filter(Boolean).join(' / '),keeper=t.to_keeper_name||'';return '<div class="timeline-row"><div class="timeline-action">'+esc(assetActionLabel(t.action_type))+'</div><div class="timeline-detail">'+esc(t.operator_name||'--')+'<small>'+esc([keeper,move,t.related_doc_no,t.note].filter(Boolean).join(' · ')||'--')+'</small></div><div class="timeline-time">'+esc(fmtTime(t.created_at))+'</div></div>';}

function clearAssetForm(){['afId','afCode','afBarcode','afNameZh','afNameKo','afCategory','afBrand','afModel','afSerial','afWarehouse','afLocation','afPurchaseDate','afCost','afSupplier','afWarranty','afNote'].forEach(function(id){E(id).value='';});}
function openAssetForm(item){clearAssetForm();if(item){E('assetFormTitle').textContent=T('edit');E('afId').value=item.id;E('afCode').value=item.asset_code||'';E('afBarcode').value=item.barcode||'';E('afNameZh').value=item.name_zh||'';E('afNameKo').value=item.name_ko||'';E('afCategory').value=item.category||'';E('afBrand').value=item.brand||'';E('afModel').value=item.model||'';E('afSerial').value=item.serial_no||'';E('afWarehouse').value=item.warehouse_name||'';E('afLocation').value=item.location_code||'';E('afPurchaseDate').value=item.purchase_date||'';E('afCost').value=item.purchase_cost||'';E('afSupplier').value=item.supplier||'';E('afWarranty').value=item.warranty_until||'';E('afNote').value=item.note||'';}else E('assetFormTitle').textContent=T('add_asset');goView('asset-edit');}

async function saveAsset(event){event.preventDefault();if(S003.busy)return;S003.busy=true;var data=Object.assign(operatorPayload(),{id:val('afId'),asset_code:val('afCode'),barcode:val('afBarcode'),name_zh:val('afNameZh'),name_ko:val('afNameKo'),category:val('afCategory'),brand:val('afBrand'),model:val('afModel'),serial_no:val('afSerial'),warehouse_name:val('afWarehouse'),location_code:val('afLocation'),purchase_date:val('afPurchaseDate'),purchase_cost:num('afCost'),currency:'KRW',supplier:val('afSupplier'),warranty_until:val('afWarranty'),note:val('afNote')});try{var res=await api('v2_003_asset_save',data);toast(T('success'));await loadLocations(false);openAssetDetail(res.id);}catch(e){toast(errorText(e.message),true);}finally{S003.busy=false;}}

function openAssetAction(type){var a=S003.currentAsset&&S003.currentAsset.item;if(!a)return;if(type==='retire'&&!confirm(T('confirm_retire')))return;if(type==='lost'&&!confirm(T('confirm_lost')))return;E('aaAssetId').value=a.id;E('aaType').value=type;E('aaTitle').textContent=assetActionLabel(type);E('aaItemInfo').textContent=(S003.lang==='ko'&&a.name_ko?a.name_ko:a.name_zh)+' · '+a.asset_code;E('aaKeeper').value=type==='assign'?(S003.operatorName||''):'';E('aaWarehouse').value=a.warehouse_name||'';E('aaLocation').value=a.location_code||'';E('aaDoc').value='';E('aaNote').value='';E('aaKeeperFields').classList.toggle('hidden',type!=='assign');E('aaLocationFields').classList.toggle('hidden',!['return','transfer','repair_done'].includes(type));E('assetActionModal').classList.remove('hidden');}

async function submitAssetAction(event){event.preventDefault();if(S003.busy)return;S003.busy=true;var type=val('aaType'),keeper=val('aaKeeper');var data=Object.assign(operatorPayload(),{asset_id:val('aaAssetId'),action_type:type,to_keeper_name:keeper,warehouse_name:val('aaWarehouse'),location_code:val('aaLocation'),related_doc_no:val('aaDoc'),note:val('aaNote'),client_req_id:reqId('atx')});if(type==='assign'&&keeper===S003.operatorName)data.to_keeper_id=S003.operatorId;try{await api('v2_003_asset_action',data);closeModal('assetActionModal');toast(T('success'));openAssetDetail(data.asset_id);}catch(e){toast(errorText(e.message),true);}finally{S003.busy=false;}}

function fileUrl(key){return V2_API+'/file?key='+encodeURIComponent(key||'');}
async function uploadAssetPhoto(input){var a=S003.currentAsset&&S003.currentAsset.item,file=input.files&&input.files[0];if(!a||!file)return;var fd=new FormData();fd.append('action','v2_attachment_upload');fd.append('k',S003.key);fd.append('file',file);fd.append('related_doc_type','asset');fd.append('related_doc_id',a.id);fd.append('attachment_category','asset_photo');fd.append('uploaded_by',S003.operatorName);try{var res=await fetch(V2_API,{method:'POST',body:fd});var json=await res.json();if(!res.ok||!json.ok)throw new Error(json.error||'upload_failed');toast(T('photo_uploaded'));openAssetDetail(a.id);}catch(e){toast(errorText(e.message),true);}finally{input.value='';}}

function printAssetQr(){var a=S003.currentAsset&&S003.currentAsset.item;if(!a)return;var qr=qrcode(0,'M');qr.addData(a.asset_code);qr.make();var svg=qr.createSvgTag({cellSize:6,margin:0,scalable:true});var name=S003.lang==='ko'&&a.name_ko?a.name_ko:a.name_zh;var w=window.open('','_blank');w.document.write('<!doctype html><html><head><meta charset="utf-8"><title>'+esc(a.asset_code)+'</title><style>body{font-family:Arial,sans-serif;text-align:center;margin:24px}.label{width:70mm;border:1px solid #aaa;padding:7mm;margin:auto}.qr svg{width:42mm;height:42mm}h1{font-size:18px;margin:8px 0 3px}p{margin:2px;font-size:12px}@media print{@page{margin:5mm}body{margin:0}.label{border:0}}</style></head><body><div class="label"><div class="qr">'+svg+'</div><h1>'+esc(name)+'</h1><p>'+esc(a.asset_code)+'</p><p>CK Warehouse</p></div><script>window.onload=function(){window.print()}<\/script></body></html>');w.document.close();}

async function loadLedger(){E('ledgerList').innerHTML='<div class="empty">'+esc(T('loading'))+'</div>';try{var res=await api('v2_003_ledger_list',{kind:val('ledgerKind'),search:val('ledgerSearch'),start_date:val('ledgerStart'),end_date:val('ledgerEnd'),limit:200});S003.ledgerRows=res.items||[];renderLedger();}catch(e){E('ledgerList').innerHTML='<div class="empty">'+esc(errorText(e.message))+'</div>';}}
function renderLedger(){if(!S003.ledgerRows.length){E('ledgerList').innerHTML='<div class="empty">'+esc(T('no_data'))+'</div>';return;}var head='<table><thead><tr><th>'+esc(T('time'))+'</th><th>'+esc(T('category'))+'</th><th>'+esc(T('asset_name'))+'</th><th>'+esc(T('status'))+'</th><th>'+esc(T('qty'))+'</th><th>'+esc(T('operator'))+'</th><th>'+esc(T('recipient'))+'</th><th>'+esc(T('warehouse'))+'</th><th>'+esc(T('note'))+'</th></tr></thead><tbody>';var rows=S003.ledgerRows.map(function(r){var action=r.kind==='material'?txnLabel(r.action_type):assetActionLabel(r.action_type);var qty=r.kind==='material'?((Number(r.qty_delta)>0?'+':'')+fmtQty(r.qty_delta)+' '+(r.unit||'')):'--';return '<tr><td>'+esc(fmtTime(r.created_at))+'</td><td>'+esc(r.kind==='material'?T('materials'):T('assets'))+'</td><td><b>'+esc(r.item_name)+'</b><br><small>'+esc(r.item_code)+'</small></td><td>'+esc(action)+'</td><td>'+esc(qty)+'</td><td>'+esc(r.operator_name||'--')+'</td><td>'+esc(r.recipient_name||'--')+'</td><td>'+esc([r.warehouse_name,r.location_code].filter(Boolean).join('/')||'--')+'</td><td>'+esc([r.purpose,r.related_doc_no,r.note].filter(Boolean).join(' · ')||'--')+'</td></tr>';}).join('');E('ledgerList').innerHTML=head+rows+'</tbody></table>';}

function csvCell(v){v=String(v==null?'':v);return '"'+v.replace(/"/g,'""')+'"';}
function exportLedgerCsv(){if(!S003.ledgerRows.length){toast(T('no_data'),true);return;}var rows=[['时间','类别','编号','名称','操作','数量变化','操作前','操作后','操作人','领用人','仓库','位置','关联单号','备注']];S003.ledgerRows.forEach(function(r){rows.push([fmtTime(r.created_at),r.kind,r.item_code,r.item_name,r.action_type,r.qty_delta,r.qty_before,r.qty_after,r.operator_name,r.recipient_name,r.warehouse_name,r.location_code,r.related_doc_no,[r.purpose,r.note].filter(Boolean).join(' · ')]);});var csv='\ufeff'+rows.map(function(row){return row.map(csvCell).join(',');}).join('\r\n');var blob=new Blob([csv],{type:'text/csv;charset=utf-8'});var url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download='CK-003-ledger-'+kstToday()+'.csv';a.click();setTimeout(function(){URL.revokeObjectURL(url);},500);}

function openLocationModal(){E('locId').value='';E('locWarehouse').value='';E('locCode').value='';E('locName').value='';E('locActive').checked=true;E('locationModal').classList.remove('hidden');}
function editLocation(id){var x=S003.locations.find(function(v){return v.id===id;});if(!x)return;E('locId').value=x.id;E('locWarehouse').value=x.warehouse_name;E('locCode').value=x.location_code;E('locName').value=x.location_name||'';E('locActive').checked=Number(x.active)!==0;E('locationModal').classList.remove('hidden');}
async function saveLocation(event){event.preventDefault();if(S003.busy)return;S003.busy=true;var data=Object.assign(operatorPayload(),{id:val('locId'),warehouse_name:val('locWarehouse'),location_code:val('locCode'),location_name:val('locName'),active:E('locActive').checked?1:0});try{await api('v2_003_location_save',data);closeModal('locationModal');toast(T('success'));loadLocations(true);}catch(e){toast(errorText(e.message),true);}finally{S003.busy=false;}}

function closeModal(id){E(id).classList.add('hidden');}
function openScanner(){E('scannerModal').classList.remove('hidden');E('scannerError').textContent='';E('manualItemCode').value='';}
function closeScanner(){stopItemScanner();E('scannerModal').classList.add('hidden');}
async function startItemScan(){if(S003.itemScanner){stopItemScanner();return;}E('scannerError').textContent='';try{S003.itemScanner=new Html5Qrcode('itemReader');await S003.itemScanner.start({facingMode:'environment'},{fps:10,qrbox:{width:250,height:180}},function(text){lookupCode(text);},function(){});E('itemScanBtn').textContent=T('close');}catch(e){E('scannerError').textContent=S003.lang==='ko'?'카메라를 열 수 없습니다. 번호를 직접 입력하세요.':'无法打开摄像头，请手动输入编号';stopItemScanner();}}
function stopItemScanner(){if(!S003.itemScanner)return;var s=S003.itemScanner;S003.itemScanner=null;Promise.resolve(s.stop()).catch(function(){}).finally(function(){try{s.clear();}catch(e){}if(E('itemScanBtn'))E('itemScanBtn').textContent=T('start_scan');});}
function lookupManualCode(){lookupCode(val('manualItemCode'));}
async function lookupCode(code){code=String(code||'').trim();if(!code)return;try{var res=await api('v2_003_lookup',{code:code});closeScanner();if(res.item.kind==='asset')openAssetDetail(res.item.id);else openMaterialDetail(res.item.id);}catch(e){E('scannerError').textContent=errorText(e.message);}}

window.addEventListener('DOMContentLoaded',function(){populateStaticLists();applyI18n();showEntry();E('ledgerStart').value=kstToday();E('ledgerEnd').value=kstToday();});
