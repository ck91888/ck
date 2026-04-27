// CK 仓库数据看板 — config.js
// 常量 / label 字典 / WMS 字段映射

var V2_API = "https://api.ck91888.cn";
var SHUJU_KEY_STORAGE = "ck_v2_shuju_k";

// ---- Label 字典 ----
var JOB_TYPE_LABELS = {
  unload: "卸货",
  inbound_direct: "代发入库",
  inbound_bulk: "大货入库",
  inbound_return: "退件入库",
  load_outbound: "出库装货",
  pick_direct: "代发拣货",
  bulk_op: "大货操作",
  pack_direct: "代发打包",
  change_order: "换单操作",
  inventory: "盘点",
  disposal: "废弃处理",
  qc: "质检",
  other_internal: "其他库内",
  scan_pallet: "过机扫描",
  load_import: "装柜出货",
  pickup_delivery_import: "外出取/送货",
  verify_scan: "扫码核对",
  issue_handle: "问题件处理"
};
var FLOW_LABELS = {
  unload: "卸货",
  inbound: "入库",
  order_op: "按单操作",
  outbound: "出库",
  internal: "库内操作",
  import: "进口专区",
  issue: "问题件"
};
var BIZ_LABELS = {
  direct_ship: "代发", bulk: "大货", return: "退件", import: "进口"
};
var STATUS_LABELS = {
  working: "作业中", awaiting_close: "待收尾", pending: "待开始",
  completed: "已完成", cancelled: "已取消"
};
var IMPORT_TYPE_LABELS = {
  change_order: "换单",
  pack_direct: "代发打包",
  pick_direct: "代发拣货",
  inbound: "入库",
  outbound: "出库",
  generic: "其他"
};

function jobTypeLabel(jt) { return JOB_TYPE_LABELS[jt] || jt || "--"; }
function flowLabel(f) { return FLOW_LABELS[f] || f || "--"; }
function bizLabel(b) { return BIZ_LABELS[b] || b || ""; }
function statusLabel(s) { return STATUS_LABELS[s] || s || "--"; }
function importTypeLabel(t) { return IMPORT_TYPE_LABELS[t] || t || "--"; }

// ---- 筛选项配置（前端 select 渲染用）----
var FLOW_STAGE_OPTIONS = [
  { value: '', label: '全部业务阶段' },
  { value: 'unload', label: '卸货' },
  { value: 'inbound', label: '入库' },
  { value: 'order_op', label: '按单操作' },
  { value: 'outbound', label: '出库' },
  { value: 'internal', label: '库内操作' },
  { value: 'import', label: '进口专区' },
  { value: 'issue', label: '问题件' }
];
var JOB_TYPE_OPTIONS = [
  { value: '', label: '全部任务类型' },
  { value: 'unload', label: '卸货' },
  { value: 'inbound_direct', label: '代发入库' },
  { value: 'inbound_bulk', label: '大货入库' },
  { value: 'inbound_return', label: '退件入库' },
  { value: 'pick_direct', label: '代发拣货' },
  { value: 'bulk_op', label: '大货操作' },
  { value: 'pack_direct', label: '代发打包' },
  { value: 'change_order', label: '换单操作' },
  { value: 'load_outbound', label: '出库装货' },
  { value: 'verify_scan', label: '扫码核对' },
  { value: 'inventory', label: '盘点' },
  { value: 'disposal', label: '废弃处理' },
  { value: 'qc', label: '质检' },
  { value: 'other_internal', label: '其他库内' },
  { value: 'scan_pallet', label: '过机扫描' },
  { value: 'load_import', label: '装柜出货' },
  { value: 'pickup_delivery_import', label: '外出取/送货' },
  { value: 'issue_handle', label: '问题件处理' }
];

// ---- WMS 字段别名识别（不区分大小写、空格 / 全角已 trim 后比对）----
// key = 标准化字段；value = 表头别名小写数组
var WMS_FIELD_ALIASES = {
  work_date:   ['date', '日期', '작업일', 'work_date', 'workdate', '作业日期'],
  operated_at: ['time', '时间', '작업시간', 'operated_at', 'operatedat', '操作时间', 'datetime'],
  worker_name: ['worker', 'worker_name', 'workername', '操作人', '작업자', '담당자', '员工'],
  worker_id:   ['worker_id', 'workerid', '工号', '员工号'],
  doc_no:      ['doc_no', 'docno', 'order_no', 'orderno', '单号', '订单号', '작업번호', '송장번호', '운송장번호'],
  order_no:    ['original_order_no', '原单号', '原订单号'],
  customer:    ['customer', '客户', '고객', '客户名称'],
  qty:         ['qty', 'quantity', '数量', '수량'],
  box_count:   ['box_count', 'boxes', '箱数', '박스', 'box', 'cartons'],
  sku:         ['sku', 'SKU', 'item', '商品编码', '货号'],
  operation_type: ['operation_type', '操作类型', '类型', 'op_type']
};

// ---- import_type → job_type 候选（与后端 management TYPE_MAP 对齐）----
var WMS_TYPE_TO_JOB_TYPES = {
  change_order: ['change_order'],
  pack_direct: ['pack_direct'],
  pick_direct: ['pick_direct'],
  inbound: ['inbound_direct', 'inbound_bulk', 'inbound_return'],
  outbound: ['load_outbound', 'verify_scan', 'bulk_op']
};
