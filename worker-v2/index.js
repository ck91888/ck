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

// 输出可读 KST 时间（YYYY-MM-DD HH:mm:ss），用于导出 CSV
function fmtKst(iso) {
  if (!iso) return '';
  const t = new Date(iso).getTime();
  if (isNaN(t)) return String(iso);
  const s = new Date(t + 9 * 3600 * 1000).toISOString();
  return s.slice(0, 10) + ' ' + s.slice(11, 19);
}

// 业务预约时间格式化（expected_ship_at / pickup_time 这种 datetime-local 文本）
// 不做 +9 时区换算，仅 T→空格、截到 16 位
// 输入 "2026-05-04T09:22" → "2026-05-04 09:22"
function fmtBusinessDateTime(s) {
  if (!s) return '';
  const str = String(s);
  return str.replace('T', ' ').slice(0, 16);
}

// 出库单字段标签（中文/韩文）— change_log diff 渲染用
const OUTBOUND_FIELD_LABELS = {
  customer:               { zh: '客户',           ko: '고객' },
  biz_class:              { zh: '业务分类',       ko: '업무 분류' },
  destination:            { zh: '目的地',         ko: '목적지' },
  po_no:                  { zh: 'PO号',           ko: 'PO번호' },
  wms_work_order_no:      { zh: 'WMS工单号',      ko: 'WMS 작업번호' },
  outbound_mode:          { zh: '出库模式',       ko: '출고 모드' },
  instruction:            { zh: '作业说明',       ko: '작업 설명' },
  remark:                 { zh: '备注',           ko: '비고' },
  planned_box_count:      { zh: '计划箱数',       ko: '계획 박스' },
  planned_pallet_count:   { zh: '计划托数',       ko: '계획 팔레트' },
  expected_ship_at:       { zh: '预计出库日期',   ko: '출고 예정일' },
  outbound_requirement:   { zh: '出库要求',       ko: '출고 요구사항' },
  uses_stock_operation:   { zh: '是否库内操作',   ko: '창고 내 작업 여부' },
  pickup_vehicle_no:      { zh: '车牌',           ko: '차번' },
  pickup_driver_name:     { zh: '司机',           ko: '기사' },
  pickup_driver_phone:    { zh: '司机电话',       ko: '기사 전화' },
  pickup_person_name:     { zh: '提货人',         ko: '픽업 담당자' },
  pickup_company:         { zh: '提货公司',       ko: '픽업 회사' },
  pickup_time:            { zh: '提货时间',       ko: '픽업 시간' },
  pickup_note:            { zh: '提货备注',       ko: '픽업 비고' },
  order_date:             { zh: '订单日期',       ko: '주문일' },
  operation_mode:         { zh: '操作方式',       ko: '작업 방식' }
};

// 比较 outbound 单字段值 — 数值字段以数值比较，其他统一字符串比较
function _outboundValEqual(field, oldVal, newVal) {
  const numericFields = ['planned_box_count', 'planned_pallet_count', 'uses_stock_operation'];
  if (numericFields.indexOf(field) !== -1) {
    return Number(oldVal || 0) === Number(newVal || 0);
  }
  return String(oldVal == null ? '' : oldVal) === String(newVal == null ? '' : newVal);
}

function _outboundValForDisplay(field, val) {
  if (val == null || val === '') return '--';
  if (field === 'uses_stock_operation') return Number(val) === 1 ? '是' : '否';
  return String(val);
}

// 构建 outbound 修改 diff
// 返回 { diff: { field: { from, to } }, summary: '字段：旧 → 新；...', changedFields: [...] }
function buildOutboundDiff(oldRow, newValues, editableFields) {
  const diff = {};
  const summaryParts = [];
  const changedFields = [];
  for (const f of editableFields) {
    if (newValues[f] === undefined) continue;
    const oldVal = oldRow ? oldRow[f] : null;
    const newVal = newValues[f];
    if (_outboundValEqual(f, oldVal, newVal)) continue;
    diff[f] = { from: oldVal == null ? '' : oldVal, to: newVal == null ? '' : newVal };
    changedFields.push(f);
    const lbl = (OUTBOUND_FIELD_LABELS[f] && OUTBOUND_FIELD_LABELS[f].zh) || f;
    summaryParts.push(lbl + '：' + _outboundValForDisplay(f, oldVal) + ' → ' + _outboundValForDisplay(f, newVal));
  }
  return { diff, summary: summaryParts.join('；'), changedFields };
}

// 写入一条出库修改日志（在事务外调用即可，调用方负责升 revision）
async function insertOutboundChangeLog(env, params) {
  const { order_id, revision_no, change_type, changed_by, diff, summary, t } = params;
  const log_id = uid();
  await env.DB.prepare(`
    INSERT INTO v2_outbound_order_change_logs(
      id, order_id, revision_no, change_type, changed_by, changed_at,
      diff_json, summary_text, warehouse_ack_required, warehouse_ack_by, warehouse_ack_at, ack_source, created_at
    ) VALUES(?,?,?,?,?,?,?,?,1,'','','',?)
  `).bind(
    log_id, order_id, revision_no, change_type || 'order_update',
    changed_by || '', t,
    JSON.stringify(diff || {}), summary || '',
    t
  ).run();
  return log_id;
}

// KST 日期 → UTC 范围 [startUtc, endUtc)
// 输入 "2026-04-27" → { startUtc: "2026-04-26T15:00:00.000Z", endUtc: "2026-04-27T15:00:00.000Z" }
function kstDayRangeUtc(dateStr) {
  if (!dateStr || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return null;
  const startKst = new Date(dateStr + 'T00:00:00.000+09:00');
  if (isNaN(startKst.getTime())) return null;
  const endKst = new Date(startKst.getTime() + 24 * 3600 * 1000);
  return { startUtc: startKst.toISOString(), endUtc: endKst.toISOString() };
}

// D1 prepared statement 单条最多约 100 bind 参数 → CHUNK=80
async function batchSelectInGlobal(env, sqlTemplate, ids) {
  const out = [];
  if (!ids || ids.length === 0) return out;
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

// 解析 v2_ops_job_results → 业务可读摘要 + 各项数量累加
// 输入：job_type, results rows
// 输出：result_summary / 累加字段 / raw_result_json_compact
function parseOpsResultForExport(job_type, resultRows) {
  const out = {
    result_summary: '',
    result_notes: '',
    diff_notes: '',
    box_count_sum: 0,
    pallet_count_sum: 0,
    // 入库理货数量（按 unit_type 分桶 + 总和）
    putaway_qty_sum: 0,
    putaway_carton_qty_sum: 0,
    putaway_pallet_qty_sum: 0,
    // 卸货 / 通用实际数量（fallback：line 没有 putaway_qty 但有 actual_qty）
    actual_qty_sum: 0,
    actual_carton_qty_sum: 0,
    actual_pallet_qty_sum: 0,
    // 入库 extra_ops（理货顺手做的）
    sort_qty_sum: 0,           // 单独整理数量
    repair_box_qty_sum: 0,     // extra_ops.repair_box_qty（与 repaired_box_count 区分写入端）
    packed_sku_count_sum: 0,
    packed_box_count_sum: 0,
    total_operated_box_count_sum: 0,
    label_count_sum: 0,
    repaired_box_count_sum: 0,
    reboxed_count_sum: 0,
    used_carton_large_count_sum: 0,
    used_carton_small_count_sum: 0,
    verify_ok_count_sum: 0,
    verify_ng_count_sum: 0,
    result_submitters: '',
    result_submitted_at: '',
    readable_result_lines: '',
    result_lines_count: 0,
    raw_result_json_compact: ''
  };
  const rows = resultRows || [];
  const remarks = [], diffs = [], rawObjs = [], readableLines = [], submitters = new Set();
  let lastCreatedAt = '';

  // 累加单行 line 中的 putaway_qty / actual_qty 到分桶（unit_type 兼容 carton/pallet/box）
  const aggregateLine = (item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item)) return;
    const unit = String(item.unit_type || item.unit || '').toLowerCase();
    const isCarton = (unit === 'carton' || unit === 'box' || unit === 'cartons' || unit === 'boxes');
    const isPallet = (unit === 'pallet' || unit === 'pallets' || unit === 'plt' || unit === 'tray');
    const pq = Number(item.putaway_qty);
    const aq = Number(item.actual_qty);
    if (Number.isFinite(pq) && pq > 0) {
      out.putaway_qty_sum += pq;
      if (isCarton) out.putaway_carton_qty_sum += pq;
      else if (isPallet) out.putaway_pallet_qty_sum += pq;
    }
    if (Number.isFinite(aq) && aq > 0) {
      out.actual_qty_sum += aq;
      if (isCarton) out.actual_carton_qty_sum += aq;
      else if (isPallet) out.actual_pallet_qty_sum += aq;
    }
    // 行内附带的 label_count / repaired_box_count 等也尝试累加
    const ln = Number(item.label_count); if (Number.isFinite(ln) && ln > 0) out.label_count_sum += ln;
    const rb = Number(item.repaired_box_count); if (Number.isFinite(rb) && rb > 0) out.repaired_box_count_sum += rb;
    const rx = Number(item.reboxed_count); if (Number.isFinite(rx) && rx > 0) out.reboxed_count_sum += rx;
  };

  // result_lines_json 单行 → 可读文本
  const renderLine = (item) => {
    if (item == null) return '';
    if (typeof item !== 'object') return String(item);
    if (Array.isArray(item)) return item.join(' / ');
    const id = item.sku || item.barcode || item.item || item.product_name || item.name || '';
    const parts = [];
    if (id) parts.push('SKU ' + id);
    const planned = item.planned_qty ?? item.plan_qty ?? item['计划'] ?? null;
    const actual = item.actual_qty ?? item.qty ?? item['实际'] ?? null;
    const putaway = item.putaway_qty ?? item['入库'] ?? null;
    const diff = item.diff_qty ?? item.diff ?? null;
    if (item.unit_type) {
      const u = String(item.unit_type).toLowerCase();
      const unitLbl = (u === 'carton' || u === 'box') ? '箱' : (u === 'pallet' ? '托' : item.unit_type);
      parts.push(unitLbl);
    }
    if (planned != null && planned !== '') parts.push('计划 ' + planned);
    if (actual != null && actual !== '') parts.push('实际 ' + actual);
    if (putaway != null && putaway !== '') parts.push('理货 ' + putaway);
    if (diff != null && diff !== '') parts.push('差异 ' + diff);
    if (item.box_count != null && item.box_count !== '') parts.push('箱 ' + item.box_count);
    if (item.pallet_count != null && item.pallet_count !== '') parts.push('板 ' + item.pallet_count);
    if (item.remark) parts.push('备注 ' + item.remark);
    if (parts.length === 0) {
      try { return JSON.stringify(item); } catch (e) { return ''; }
    }
    return parts.join(' / ');
  };

  // 从 result_json 顶层兼容老 shorthand：{carton:4}/{pallet:3}/{box_count:4}
  const aggregateShorthand = (rj) => {
    if (!rj || typeof rj !== 'object' || Array.isArray(rj)) return;
    const c = Number(rj.carton);
    if (Number.isFinite(c) && c > 0) {
      out.putaway_qty_sum += c;
      out.putaway_carton_qty_sum += c;
    }
    const p = Number(rj.pallet);
    if (Number.isFinite(p) && p > 0) {
      out.putaway_qty_sum += p;
      out.putaway_pallet_qty_sum += p;
    }
  };

  rows.forEach(r => {
    out.box_count_sum += Number(r.box_count) || 0;
    out.pallet_count_sum += Number(r.pallet_count) || 0;
    if (r.remark) remarks.push(String(r.remark));
    if (r.diff_note) diffs.push(String(r.diff_note));
    if (r.created_by) submitters.add(String(r.created_by));
    if (r.created_at && r.created_at > lastCreatedAt) lastCreatedAt = r.created_at;

    let rj = null;
    if (r.result_json) {
      try { rj = JSON.parse(r.result_json); } catch (e) { rj = null; }
    }
    if (rj && typeof rj === 'object' && !Array.isArray(rj)) {
      const addNum = (k, target) => {
        const v = Number(rj[k]);
        if (Number.isFinite(v)) out[target] += v;
      };
      addNum('packed_sku_count', 'packed_sku_count_sum');
      addNum('packed_box_count', 'packed_box_count_sum');
      addNum('total_operated_box_count', 'total_operated_box_count_sum');
      addNum('label_count', 'label_count_sum');
      addNum('repaired_box_count', 'repaired_box_count_sum');
      addNum('reboxed_count', 'reboxed_count_sum');
      addNum('used_carton_large_count', 'used_carton_large_count_sum');
      addNum('used_carton_small_count', 'used_carton_small_count_sum');
      addNum('verify_ok_count', 'verify_ok_count_sum');
      addNum('verify_ng_count', 'verify_ng_count_sum');
      if (!r.pallet_count && Number(rj.pallet_count) > 0) {
        out.pallet_count_sum += Number(rj.pallet_count);
      }
      // 入库 extra_ops（理货顺手贴标/修箱/整理）— 与 packing 端的 *_count 区分但归入同一摘要桶
      if (rj.extra_ops && typeof rj.extra_ops === 'object') {
        const eo = rj.extra_ops;
        const sq = Number(eo.sort_qty); if (Number.isFinite(sq) && sq > 0) out.sort_qty_sum += sq;
        const lq = Number(eo.label_qty); if (Number.isFinite(lq) && lq > 0) out.label_count_sum += lq;
        const rq = Number(eo.repair_box_qty);
        if (Number.isFinite(rq) && rq > 0) {
          out.repair_box_qty_sum += rq;
          out.repaired_box_count_sum += rq;
        }
        if (eo.other_op_remark) remarks.push(String(eo.other_op_remark));
      }
      // result_json 顶层"单行式" e.g. {unit_type:'carton', putaway_qty:4}
      if (rj.unit_type) aggregateLine(rj);
      // result_json 内嵌 result_lines / lines
      const innerLines = Array.isArray(rj.result_lines) ? rj.result_lines
                       : Array.isArray(rj.lines) ? rj.lines
                       : null;
      if (innerLines) {
        innerLines.forEach(aggregateLine);
        if (out.result_lines_count === 0 && innerLines.length > 0) {
          // 仅当 result_lines_json 不存在时才用 inner 计数（避免重复）
          if (!r.result_lines_json) out.result_lines_count += innerLines.length;
        }
      }
      // 老 shorthand：{carton:4}/{pallet:3}
      aggregateShorthand(rj);
      const diffVal = rj.diff_note || rj.diff_notes || rj.diff || rj['差异'] || rj['差异说明'] || '';
      if (diffVal) diffs.push(String(diffVal));
      rawObjs.push(rj);
    } else if (r.result_json) {
      rawObjs.push(r.result_json);
    }

    // 解析 result_lines_json
    if (r.result_lines_json) {
      let lines = null;
      try { lines = JSON.parse(r.result_lines_json); } catch (e) { lines = null; }
      const arr = Array.isArray(lines)
        ? lines
        : (lines && Array.isArray(lines.lines)) ? lines.lines
        : (lines && Array.isArray(lines.result_lines)) ? lines.result_lines
        : (lines ? [lines] : []);
      out.result_lines_count += arr.length;
      arr.forEach(item => {
        aggregateLine(item);
        const txt = renderLine(item);
        if (txt) readableLines.push(txt);
      });
    }
  });

  out.result_notes = remarks.join(' | ');
  // 去重 diff_notes
  out.diff_notes = [...new Set(diffs.filter(Boolean))].join(' | ');
  out.result_submitters = [...submitters].join('、');
  out.result_submitted_at = fmtKst(lastCreatedAt);

  let linesStr = readableLines.join(' || ');
  if (linesStr.length > 2000) linesStr = linesStr.slice(0, 2000) + '...已截断';
  out.readable_result_lines = linesStr;

  let rawStr = '';
  try { rawStr = JSON.stringify(rawObjs); } catch (e) { rawStr = ''; }
  if (rawStr.length > 1000) rawStr = rawStr.slice(0, 1000) + '...已截断';
  out.raw_result_json_compact = rawStr;

  // 生成业务摘要
  const parts = [];
  const isInboundJob = (job_type || '').indexOf('inbound') === 0; // inbound_direct / inbound_bulk / inbound_change_order / inbound_return
  if (job_type === 'pack_direct') {
    if (out.packed_sku_count_sum > 0) parts.push('打包SKU ' + out.packed_sku_count_sum);
    if (out.packed_box_count_sum > 0) parts.push('打包箱 ' + out.packed_box_count_sum);
    if (out.label_count_sum > 0) parts.push('贴标 ' + out.label_count_sum);
    if (out.total_operated_box_count_sum > 0) parts.push('总操作箱 ' + out.total_operated_box_count_sum);
    if (out.pallet_count_sum > 0) parts.push('托盘 ' + out.pallet_count_sum);
    if (out.repaired_box_count_sum > 0) parts.push('修箱 ' + out.repaired_box_count_sum);
    if (out.reboxed_count_sum > 0) parts.push('换箱 ' + out.reboxed_count_sum);
    if (out.used_carton_large_count_sum > 0) parts.push('大纸箱 ' + out.used_carton_large_count_sum);
    if (out.used_carton_small_count_sum > 0) parts.push('小纸箱 ' + out.used_carton_small_count_sum);
  } else if (job_type === 'change_order') {
    parts.push('仅计时，无数量结果');
  } else if (job_type === 'verify_scan') {
    if (out.verify_ok_count_sum > 0) parts.push('核对OK ' + out.verify_ok_count_sum);
    if (out.verify_ng_count_sum > 0) parts.push('核对NG ' + out.verify_ng_count_sum);
    if (out.box_count_sum > 0) parts.push('箱 ' + out.box_count_sum);
    if (out.pallet_count_sum > 0) parts.push('板 ' + out.pallet_count_sum);
  } else if (isInboundJob) {
    // 入库类：理货箱数/理货托数 优先；无 putaway 时 fallback actual
    if (out.putaway_carton_qty_sum > 0) parts.push('理货箱数 ' + out.putaway_carton_qty_sum);
    if (out.putaway_pallet_qty_sum > 0) parts.push('理货托数 ' + out.putaway_pallet_qty_sum);
    if (out.putaway_qty_sum > 0 && out.putaway_carton_qty_sum === 0 && out.putaway_pallet_qty_sum === 0) {
      parts.push('理货数量 ' + out.putaway_qty_sum);
    }
    if (out.putaway_qty_sum === 0) {
      if (out.actual_carton_qty_sum > 0) parts.push('实际箱数 ' + out.actual_carton_qty_sum);
      if (out.actual_pallet_qty_sum > 0) parts.push('实际托数 ' + out.actual_pallet_qty_sum);
    }
    if (out.sort_qty_sum > 0) parts.push('整理 ' + out.sort_qty_sum);
    if (out.label_count_sum > 0) parts.push('贴标 ' + out.label_count_sum);
    if (out.repaired_box_count_sum > 0) parts.push('修箱 ' + out.repaired_box_count_sum);
    if (out.box_count_sum > 0) parts.push('箱 ' + out.box_count_sum);
    if (out.pallet_count_sum > 0) parts.push('板 ' + out.pallet_count_sum);
  } else if (job_type === 'unload') {
    // 卸货：以行内 actual_qty 为准；按 unit 分桶
    if (out.actual_carton_qty_sum > 0) parts.push('卸货箱数 ' + out.actual_carton_qty_sum);
    if (out.actual_pallet_qty_sum > 0) parts.push('卸货托数 ' + out.actual_pallet_qty_sum);
    if (out.actual_qty_sum > 0 && out.actual_carton_qty_sum === 0 && out.actual_pallet_qty_sum === 0) {
      parts.push('卸货数量 ' + out.actual_qty_sum);
    }
    if (out.box_count_sum > 0) parts.push('箱 ' + out.box_count_sum);
    if (out.pallet_count_sum > 0) parts.push('板 ' + out.pallet_count_sum);
  } else {
    // 默认：出库/库内/拣货 等
    if (out.box_count_sum > 0) parts.push('箱 ' + out.box_count_sum);
    if (out.pallet_count_sum > 0) parts.push('板 ' + out.pallet_count_sum);
    if (out.actual_qty_sum > 0 && out.box_count_sum === 0 && out.pallet_count_sum === 0) {
      parts.push('实际数量 ' + out.actual_qty_sum);
    }
  }

  // 任意一个数量字段 > 0 都不应再判定"无数量结果"
  const hasAnyQty = (
    out.box_count_sum > 0 ||
    out.pallet_count_sum > 0 ||
    out.putaway_qty_sum > 0 ||
    out.putaway_carton_qty_sum > 0 ||
    out.putaway_pallet_qty_sum > 0 ||
    out.actual_qty_sum > 0 ||
    out.actual_carton_qty_sum > 0 ||
    out.actual_pallet_qty_sum > 0 ||
    out.sort_qty_sum > 0 ||
    out.repair_box_qty_sum > 0 ||
    out.packed_sku_count_sum > 0 ||
    out.packed_box_count_sum > 0 ||
    out.total_operated_box_count_sum > 0 ||
    out.label_count_sum > 0 ||
    out.repaired_box_count_sum > 0 ||
    out.reboxed_count_sum > 0 ||
    out.used_carton_large_count_sum > 0 ||
    out.used_carton_small_count_sum > 0 ||
    out.verify_ok_count_sum > 0 ||
    out.verify_ng_count_sum > 0
  );

  if (parts.length === 0) {
    if (out.result_notes && job_type !== 'change_order') {
      out.result_summary = '备注：' + out.result_notes;
    } else if (job_type === 'change_order') {
      out.result_summary = '仅计时，无数量结果';
    } else if (!hasAnyQty) {
      out.result_summary = '仅记录工时，无数量结果';
    } else {
      out.result_summary = '';
    }
  } else {
    out.result_summary = parts.join('，');
    if (out.result_notes && job_type !== 'change_order') {
      out.result_summary += '；备注：' + out.result_notes;
    }
  }

  if (out.result_summary.length > 500) {
    out.result_summary = out.result_summary.slice(0, 500) + '...';
  }
  return out;
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

// "只取日期"归一化：支持 YYYY-MM-DD / YYYY/MM/DD / datetime-local (YYYY-MM-DDTHH:MM) / ISO 等
// 设计原则：
//   - 优先按 KST 解析（仓库业务日历日 = KST 自然日）
//   - 输入若已是 YYYY-MM-DD 直接返回；datetime-local 仅截前 10 位（已是本地含义）
//   - 仅 ISO UTC 或带 Z 才走 +9 小时换算
//   - 解析失败的兜底：截前 10 位
function normalizeDateOnly(v) {
  if (v == null) return '';
  const s = String(v).trim();
  if (!s) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{4}\/\d{2}\/\d{2}$/.test(s)) return s.replace(/\//g, '-');
  // datetime-local 格式 YYYY-MM-DDTHH:MM(:SS)，无时区信息 → 直接截前 10 位（已是 KST 文本）
  if (/^\d{4}-\d{2}-\d{2}T/.test(s) && !/[zZ]$/.test(s) && !/[+\-]\d{2}:?\d{2}$/.test(s.slice(11))) {
    return s.slice(0, 10);
  }
  // ISO / 带时区 → 转 KST 日期
  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    const k = new Date(d.getTime() + 9 * 3600000);
    return k.toISOString().slice(0, 10);
  }
  return s.slice(0, 10);
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

// 列表分页元信息：所有 list 接口统一在 return json 时附加
// 用法：return json({ ok:true, items, ...pageMeta(total, limit, offset) })
function pageMeta(total, limit, offset) {
  const _total = Math.max(0, Number(total) || 0);
  const _limit = Math.max(1, Number(limit) || 50);
  const _offset = Math.max(0, Number(offset) || 0);
  const page = Math.floor(_offset / _limit) + 1;
  const page_count = Math.max(1, Math.ceil(_total / _limit));
  return { total: _total, limit: _limit, offset: _offset, page, page_count };
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

// 关闭某 job 下所有未退出的 worker segment（不限当前 worker，job 完成时统一收口）
// 返回关闭数量；自动重算 active_worker_count（必为 0）
async function closeOpenWorkerSegmentsForJob(env, jobId, t, reason) {
  if (!jobId) return 0;
  const closeAt = t || now();
  const closeReason = String(reason || 'job_completed_auto_close');
  const segs = await env.DB.prepare(
    "SELECT id, joined_at FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
  ).bind(jobId).all();
  const rows = segs.results || [];
  for (const seg of rows) {
    const joinedMs = Date.parse(seg.joined_at || '');
    const leftMs = Date.parse(closeAt);
    let minutes = 0;
    if (Number.isFinite(joinedMs) && Number.isFinite(leftMs)) {
      minutes = Math.max(0, Math.round((leftMs - joinedMs) / 60000 * 10) / 10);
    }
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason=? WHERE id=?"
    ).bind(closeAt, minutes, closeReason, seg.id).run();
  }
  if (rows.length > 0) {
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(closeAt, jobId).run();
  }
  return rows.length;
}

// 按 v2_ops_job_workers 汇总实际工时（人时累加，多人并行不抵扣）
// - closed segment：直接用 minutes_worked；若为 0/缺失则用 left_at-joined_at 兜底
// - open segment：用 fallbackEndAt - joined_at 计算（不写库）；如果传 closeOpen=true 则同步关闭
// 返回 { total_minutes, worker_names, worker_count, segments }
async function sumJobWorkerMinutes(env, jobId, fallbackEndAt, closeOpen) {
  if (!jobId) return { total_minutes: 0, worker_names: '', worker_count: 0, segments: [] };
  const fallback = fallbackEndAt || now();
  const rs = await env.DB.prepare(
    "SELECT id, worker_id, worker_name, joined_at, left_at, minutes_worked, leave_reason FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at ASC"
  ).bind(jobId).all();
  const rows = rs.results || [];
  const segments = [];
  const nameSet = new Set();
  const idSet = new Set();
  let total = 0;
  for (const r of rows) {
    const closed = !!(r.left_at && String(r.left_at).length > 0);
    const joinedMs = Date.parse(r.joined_at || '');
    let leftAt = closed ? r.left_at : fallback;
    let leftMs = Date.parse(leftAt);
    let minutes = 0;
    if (closed) {
      const stored = Number(r.minutes_worked);
      if (Number.isFinite(stored) && stored > 0) {
        minutes = stored;
      } else if (Number.isFinite(joinedMs) && Number.isFinite(leftMs)) {
        minutes = Math.max(0, Math.round((leftMs - joinedMs) / 60000 * 10) / 10);
      }
    } else {
      // open segment：按 fallback 计算；可选地写回 DB
      if (Number.isFinite(joinedMs) && Number.isFinite(leftMs)) {
        minutes = Math.max(0, Math.round((leftMs - joinedMs) / 60000 * 10) / 10);
      }
      if (closeOpen) {
        await env.DB.prepare(
          "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason=COALESCE(NULLIF(leave_reason,''), 'auto_close') WHERE id=?"
        ).bind(leftAt, minutes, r.id).run();
      }
    }
    minutes = Math.max(0, minutes);
    total += minutes;
    if (r.worker_name) nameSet.add(r.worker_name);
    if (r.worker_id) idSet.add(r.worker_id);
    segments.push({
      id: r.id,
      worker_id: r.worker_id || '',
      worker_name: r.worker_name || '',
      joined_at: r.joined_at || '',
      left_at: closed ? (r.left_at || '') : '',
      open: !closed,
      minutes_worked: minutes,
      leave_reason: r.leave_reason || ''
    });
  }
  return {
    total_minutes: Math.round(total * 10) / 10,
    worker_names: Array.from(nameSet).join('、'),
    worker_count: idSet.size,
    segments
  };
}

// ===== 任务类型分类：仅记工时 vs 需要产出数据 =====
// 仅记工时：最后一人退出后立即 completed，无需任何 result_json
const TIME_ONLY_JOB_TYPES = new Set([
  'inbound_return',     // 退件入库（轻量工时）
  'pack_direct',        // 代发打包
  'change_order',       // 换单操作
  'other_internal',     // 仓库整理
  'disposal',           // 废弃处理
  'qc',                 // 质检
  'inventory',          // 盘点
  'scan_pallet',        // 过机扫描
  'verify_scan',        // 扫码核对（仅记工时；超过 48h 由 cleanup 自动结束）
  'issue_handle'        // 问题点处理 — 由 v2_issue_handle_finish 收尾，不参与本逻辑（兜底归类时间型）
]);
// 需要产出数据：最后一人退出且无 result → awaiting_close；有 result → completed
const RESULT_REQUIRED_JOB_TYPES = new Set([
  'unload', 'unplanned_unload',
  'load_outbound', 'outbound_stock_op',
  'inbound_direct', 'inbound_bulk', 'inbound_change_order',
  'bulk_op', 'pick_direct',
  'load_import', 'pickup_delivery_import'
]);
// 扫码核对超时阈值（毫秒）— 48 小时
const VERIFY_SCAN_TIMEOUT_MS = 48 * 3600 * 1000;
function isTimeOnlyJobType(jobType) {
  return TIME_ONLY_JOB_TYPES.has(String(jobType || ''));
}
function isResultRequiredJobType(jobType) {
  return RESULT_REQUIRED_JOB_TYPES.has(String(jobType || ''));
}

// 一次性收尾：当某 job 的 open worker = 0 时调用，按类型决定 completed 或 awaiting_close
// 已 completed/cancelled 的 job 不动；result_summary/finished_at 视情况写入
async function autoCloseJobIfNoOpenWorkers(env, jobId, t) {
  if (!jobId) return null;
  const closeAt = t || now();
  const job = await env.DB.prepare(
    "SELECT id, status, job_type FROM v2_ops_jobs WHERE id=?"
  ).bind(jobId).first();
  if (!job) return null;
  if (job.status === 'completed' || job.status === 'cancelled') return job;

  const openCnt = await env.DB.prepare(
    "SELECT COUNT(*) AS c FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
  ).bind(jobId).first();
  const openCount = Number((openCnt && openCnt.c) || 0);
  if (openCount > 0) return job;

  if (isTimeOnlyJobType(job.job_type)) {
    const defaultSummary = (job.job_type === 'verify_scan')
      ? '扫码核对：仅记录工时，无数量结果'
      : '仅记录工时，无数量结果';
    await env.DB.prepare(`
      UPDATE v2_ops_jobs
         SET status='completed', finished_at=?, updated_at=?, active_worker_count=0,
             result_summary=COALESCE(NULLIF(result_summary,''), ?)
       WHERE id=?
    `).bind(closeAt, closeAt, defaultSummary, jobId).run();
    // verify_scan 联动：把对应批次推进到 completed（如未关闭）
    if (job.job_type === 'verify_scan') {
      await _completeVerifyBatchIfLinked(env, jobId, closeAt, 'auto_complete_via_job');
    }
    return Object.assign({}, job, { status: 'completed', finished_at: closeAt });
  }

  // 需要产出 / 未识别类型：按 ops_job_results 是否存在分流
  const resCnt = await env.DB.prepare(
    "SELECT COUNT(*) AS c FROM v2_ops_job_results WHERE job_id=?"
  ).bind(jobId).first();
  const hasResult = Number((resCnt && resCnt.c) || 0) > 0;
  if (hasResult) {
    await env.DB.prepare(`
      UPDATE v2_ops_jobs
         SET status='completed', finished_at=COALESCE(NULLIF(finished_at,''), ?), updated_at=?, active_worker_count=0
       WHERE id=? AND status NOT IN ('completed','cancelled')
    `).bind(closeAt, closeAt, jobId).run();
    return Object.assign({}, job, { status: 'completed', finished_at: closeAt });
  }
  // 无产出 → awaiting_close（等待管理员手动补录）
  await env.DB.prepare(`
    UPDATE v2_ops_jobs
       SET status='awaiting_close', updated_at=?, active_worker_count=0,
           result_summary=COALESCE(NULLIF(result_summary,''), '待补充产出数据')
     WHERE id=? AND status NOT IN ('completed','cancelled')
  `).bind(closeAt, jobId).run();
  return Object.assign({}, job, { status: 'awaiting_close' });
}

// 入库计划状态自愈：unloading/unloading_putting_away/putting_away 等需要 active job
// 的状态，若实际没有 active job，根据现存记录推回到一个一致状态。返回 {repaired, old_status, new_status, reason}
async function repairInboundPlanWorkState(env, planId, reason) {
  if (!planId) return { repaired: false };
  const plan = await env.DB.prepare(
    "SELECT id, display_no, status FROM v2_inbound_plans WHERE id=?"
  ).bind(planId).first();
  if (!plan) return { repaired: false, error: "plan_not_found" };
  const oldStatus = plan.status;
  // 仅修复需要 active job 但缺失的几种状态
  const repairTargets = ['unloading', 'unloading_putting_away', 'putting_away'];
  if (repairTargets.indexOf(oldStatus) === -1) {
    return { repaired: false, old_status: oldStatus, new_status: oldStatus, reason: 'no_repair_needed' };
  }

  // 查 active unload / putaway job
  const activeUnload = await env.DB.prepare(
    `SELECT id FROM v2_ops_jobs
       WHERE related_doc_type='inbound_plan' AND related_doc_id=?
         AND job_type='unload' AND status IN ('pending','working','awaiting_close')
       LIMIT 1`
  ).bind(planId).first();
  const activePutaway = await env.DB.prepare(
    `SELECT id FROM v2_ops_jobs
       WHERE related_doc_type='inbound_plan' AND related_doc_id=?
         AND job_type IN ('inbound_direct','inbound_bulk','inbound_change_order')
         AND status IN ('pending','working','awaiting_close')
       LIMIT 1`
  ).bind(planId).first();
  const hasUnloadCompleted = await env.DB.prepare(
    `SELECT id FROM v2_ops_jobs
       WHERE related_doc_type='inbound_plan' AND related_doc_id=?
         AND job_type='unload' AND status='completed'
       LIMIT 1`
  ).bind(planId).first();
  const hasPutawayCompleted = await env.DB.prepare(
    `SELECT id FROM v2_ops_jobs
       WHERE related_doc_type='inbound_plan' AND related_doc_id=?
         AND job_type IN ('inbound_direct','inbound_bulk','inbound_change_order')
         AND status='completed'
       LIMIT 1`
  ).bind(planId).first();

  // 所有要求业务是否完成
  let allBizDone = false;
  try {
    const tasks = await listInboundPlanBizTasks(env, planId);
    if (tasks && tasks.length > 0) {
      allBizDone = tasks.every(t => t.status === 'completed');
    }
  } catch (e) { /* ignore */ }

  let newStatus = oldStatus;
  let repairReason = reason || '';

  if (oldStatus === 'unloading') {
    if (activeUnload) return { repaired: false, old_status: oldStatus, new_status: oldStatus, reason: 'active_unload_present' };
    // unloading 但无 active unload
    if (allBizDone) {
      newStatus = 'completed';
      repairReason = 'all_biz_done_repaired_to_completed';
    } else if (activePutaway) {
      newStatus = 'putting_away';
      repairReason = 'unload_missing_but_putaway_active';
    } else if (hasUnloadCompleted) {
      newStatus = 'arrived_pending_putaway';
      repairReason = 'unload_missing_repaired_to_arrived_pending_putaway';
    } else {
      newStatus = 'pending';
      repairReason = 'unload_status_without_job_repaired_to_pending';
    }
  } else if (oldStatus === 'unloading_putting_away') {
    if (activeUnload) return { repaired: false, old_status: oldStatus, new_status: oldStatus, reason: 'active_unload_present' };
    if (allBizDone) {
      newStatus = 'completed';
      repairReason = 'all_biz_done_repaired_to_completed';
    } else if (activePutaway) {
      newStatus = 'putting_away';
      repairReason = 'unload_missing_with_active_putaway';
    } else if (hasUnloadCompleted || hasPutawayCompleted) {
      newStatus = 'arrived_pending_putaway';
      repairReason = 'unload_missing_repaired_to_arrived_pending_putaway';
    } else {
      newStatus = 'pending';
      repairReason = 'unload_putaway_status_without_jobs_repaired_to_pending';
    }
  } else if (oldStatus === 'putting_away') {
    if (activePutaway) return { repaired: false, old_status: oldStatus, new_status: oldStatus, reason: 'active_putaway_present' };
    if (allBizDone) {
      newStatus = 'completed';
      repairReason = 'all_biz_done_repaired_to_completed';
    } else {
      newStatus = 'arrived_pending_putaway';
      repairReason = 'putaway_missing_repaired_to_arrived_pending_putaway';
    }
  }

  if (newStatus === oldStatus) {
    return { repaired: false, old_status: oldStatus, new_status: oldStatus, reason: 'no_change' };
  }

  const t = now();
  const sets = ["status=?", "updated_at=?"];
  const binds = [newStatus, t];
  if (newStatus === 'completed') {
    sets.push("manual_completed_at=COALESCE(NULLIF(manual_completed_at,''), ?)");
    binds.push(t);
  }
  binds.push(planId);
  await env.DB.prepare(
    "UPDATE v2_inbound_plans SET " + sets.join(', ') + " WHERE id=?"
  ).bind(...binds).run();

  return {
    repaired: true,
    old_status: oldStatus,
    new_status: newStatus,
    reason: repairReason,
    display_no: plan.display_no || ''
  };
}

// verify_scan job 关闭时联动其挂的核对批次（v2_verify_batches）→ completed
// 仅当当前 batch.status 不是 completed/cancelled 才推进，避免覆盖人工标记
async function _completeVerifyBatchIfLinked(env, jobId, closeAt, source) {
  if (!jobId) return false;
  const job = await env.DB.prepare(
    "SELECT job_type, related_doc_id FROM v2_ops_jobs WHERE id=?"
  ).bind(jobId).first();
  if (!job || job.job_type !== 'verify_scan' || !job.related_doc_id) return false;
  const batch = await env.DB.prepare(
    "SELECT id, status FROM v2_verify_batches WHERE id=?"
  ).bind(job.related_doc_id).first();
  if (!batch) return false;
  if (batch.status === 'completed' || batch.status === 'cancelled') return false;
  await env.DB.prepare(
    "UPDATE v2_verify_batches SET status='completed', completed_at=COALESCE(NULLIF(completed_at,''), ?), completed_by=COALESCE(NULLIF(completed_by,''), ?), updated_at=? WHERE id=?"
  ).bind(closeAt, source || 'auto', closeAt, batch.id).run();
  return true;
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

  // ---- v2.20260428a：入库计划业务类型多选 ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN biz_classes_json TEXT DEFAULT '[]'`,
  `CREATE TABLE IF NOT EXISTS v2_inbound_plan_biz_tasks (
    id TEXT PRIMARY KEY,
    plan_id TEXT DEFAULT '',
    biz_class TEXT DEFAULT '',
    job_type TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    job_id TEXT DEFAULT '',
    started_at TEXT DEFAULT '',
    completed_at TEXT DEFAULT '',
    completed_by TEXT DEFAULT '',
    worker_names TEXT DEFAULT '',
    total_minutes REAL DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_inbound_biz_task_unique ON v2_inbound_plan_biz_tasks(plan_id, biz_class)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_biz_task_plan ON v2_inbound_plan_biz_tasks(plan_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_biz_task_biz_status ON v2_inbound_plan_biz_tasks(biz_class, status)`,

  // ---- v2.20260428b：出库作业单业务分类 + 库内操作型 + 出库资料 ----
  `ALTER TABLE v2_outbound_orders ADD COLUMN uses_stock_operation INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN stock_operation_status TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN stock_operation_job_id TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN stock_operation_completed_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN stock_operation_completed_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN stock_operation_result_json TEXT DEFAULT '{}'`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN expected_ship_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN outbound_requirement TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN outbound_docs_required INTEGER DEFAULT 0`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_biz_status_date ON v2_outbound_orders(biz_class, status, order_date, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_stock_status_date ON v2_outbound_orders(uses_stock_operation, status, order_date, created_at)`,

  // ---- v2.20260430a：测试反馈批量修复 ----
  // P1-4：入库计划→关联出库单 反链（source_inbound_plan_id 已在 v2.20260423 加入）
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_source_inbound ON v2_outbound_orders(source_inbound_plan_id)`,

  // P1-6：问题点提示记帐
  `ALTER TABLE v2_issue_tickets ADD COLUMN accounting_required INTEGER DEFAULT 0`,
  `ALTER TABLE v2_issue_tickets ADD COLUMN accounting_required_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_issue_tickets ADD COLUMN accounting_required_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_issue_tickets ADD COLUMN accounting_note TEXT DEFAULT ''`,
  `ALTER TABLE v2_issue_tickets ADD COLUMN accounted INTEGER DEFAULT 0`,
  `ALTER TABLE v2_issue_tickets ADD COLUMN accounted_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_issue_tickets ADD COLUMN accounted_at TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_issue_accounting_required ON v2_issue_tickets(accounting_required, accounted, status, created_at)`,

  // P1-8：出库单修改 + 仓库确认
  `ALTER TABLE v2_outbound_orders ADD COLUMN revision_no INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN last_modified_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN last_modified_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN warehouse_ack_required INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN warehouse_ack_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN warehouse_ack_at TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_warehouse_ack ON v2_outbound_orders(warehouse_ack_required, status)`,

  // P1-9：出库提货信息
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_vehicle_no TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_driver_name TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_driver_phone TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_person_name TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_company TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_time TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_note TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_confirm_required INTEGER DEFAULT 0`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_confirmed_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_outbound_orders ADD COLUMN pickup_confirmed_at TEXT DEFAULT ''`,

  // ---- v2.20260430d：列表查询性能 + 业务/客户筛选索引（2026-04-29） ----
  // 入库
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_status_date ON v2_inbound_plans(status, plan_date, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_customer ON v2_inbound_plans(customer)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_biz_status ON v2_inbound_plans(biz_class, status, plan_date)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_biz_task_biz_plan ON v2_inbound_plan_biz_tasks(biz_class, plan_id)`,
  // 出库
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_status_date ON v2_outbound_orders(status, order_date, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_customer ON v2_outbound_orders(customer)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_outbound_biz_status ON v2_outbound_orders(biz_class, status, order_date)`,
  // 附件
  `CREATE INDEX IF NOT EXISTS idx_v2_attachments_doc_cat ON v2_attachments(related_doc_type, related_doc_id, attachment_category)`,

  // ---- v2.20260430f：入库计划手动强制完成（force complete，区分现场完成 vs 文员强制完成） ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN force_completed INTEGER DEFAULT 0`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN force_completed_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN force_completed_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN force_complete_reason TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plan_biz_tasks ADD COLUMN completion_source TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plan_biz_tasks ADD COLUMN completion_note TEXT DEFAULT ''`,

  // ---- v2.20260430g：问题点客服追加处理需求历史（保留多轮，不覆盖） ----
  `CREATE TABLE IF NOT EXISTS v2_issue_rework_requests (
    id TEXT PRIMARY KEY,
    issue_id TEXT DEFAULT '',
    request_note TEXT DEFAULT '',
    requested_by TEXT DEFAULT '',
    status TEXT DEFAULT 'open',
    created_at TEXT,
    resolved_at TEXT DEFAULT '',
    resolved_by TEXT DEFAULT '',
    related_run_id TEXT DEFAULT ''
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_issue_rework_issue ON v2_issue_rework_requests(issue_id, created_at)`,

  // ---- v2.20260430h：出库单修改明细日志 + 仓库确认链路 ----
  `CREATE TABLE IF NOT EXISTS v2_outbound_order_change_logs (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    revision_no INTEGER DEFAULT 0,
    change_type TEXT DEFAULT 'order_update',
    changed_by TEXT DEFAULT '',
    changed_at TEXT,
    diff_json TEXT DEFAULT '{}',
    summary_text TEXT DEFAULT '',
    warehouse_ack_required INTEGER DEFAULT 1,
    warehouse_ack_by TEXT DEFAULT '',
    warehouse_ack_at TEXT DEFAULT '',
    ack_source TEXT DEFAULT '',
    created_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ob_change_logs_order ON v2_outbound_order_change_logs(order_id, revision_no)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ob_change_logs_ack ON v2_outbound_order_change_logs(warehouse_ack_required, changed_at)`,

  // ---- v2.20260508a：手动收尾 / 修改产出 / cleanup 标记 / customer 字段 ----
  `ALTER TABLE v2_ops_jobs ADD COLUMN customer TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN finished_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN result_summary TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN manual_finalized INTEGER DEFAULT 0`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN manual_finalized_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN manual_finalized_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN manual_finalize_reason TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN result_corrected INTEGER DEFAULT 0`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN result_corrected_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN result_corrected_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN result_correct_reason TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_jobs ADD COLUMN cleanup_note TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_results ADD COLUMN source TEXT DEFAULT ''`,
  `ALTER TABLE v2_ops_job_results ADD COLUMN previous_result_id TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_jobs_customer ON v2_ops_jobs(customer) WHERE customer != ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_ops_jobs_status_type ON v2_ops_jobs(status, job_type, created_at)`,

  // ---- v2.20260526c：入库计划 + 现场反馈 软删除（误转正回滚） ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN is_deleted INTEGER DEFAULT 0`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN deleted_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN deleted_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN delete_reason TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN is_deleted INTEGER DEFAULT 0`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN deleted_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN deleted_by TEXT DEFAULT ''`,
  `ALTER TABLE v2_field_feedbacks ADD COLUMN delete_reason TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_is_deleted ON v2_inbound_plans(is_deleted, status)`,

  // ---- v2.20260609a：入库计划冗余字段：卸货完成时间/卸货完成人（用于"按卸货完成日期搜索"） ----
  `ALTER TABLE v2_inbound_plans ADD COLUMN unload_completed_at TEXT DEFAULT ''`,
  `ALTER TABLE v2_inbound_plans ADD COLUMN unload_completed_by TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_inbound_plans_unload_completed_at ON v2_inbound_plans(unload_completed_at)`,

  // ---- v2.20260808a：003 耗材与物品管理 ----
  `CREATE TABLE IF NOT EXISTS v2_003_locations (
    id TEXT PRIMARY KEY,
    warehouse_name TEXT DEFAULT '',
    location_code TEXT DEFAULT '',
    location_name TEXT DEFAULT '',
    active INTEGER DEFAULT 1,
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_location_unique
    ON v2_003_locations(warehouse_name, location_code)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_location_active
    ON v2_003_locations(active, warehouse_name, location_code)`,

  `CREATE TABLE IF NOT EXISTS v2_003_materials (
    id TEXT PRIMARY KEY,
    material_code TEXT DEFAULT '',
    barcode TEXT DEFAULT '',
    name_zh TEXT DEFAULT '',
    name_ko TEXT DEFAULT '',
    category TEXT DEFAULT '',
    spec TEXT DEFAULT '',
    unit TEXT DEFAULT '',
    warehouse_name TEXT DEFAULT '',
    location_code TEXT DEFAULT '',
    current_qty REAL DEFAULT 0,
    min_qty REAL DEFAULT 0,
    unit_cost REAL DEFAULT 0,
    currency TEXT DEFAULT 'KRW',
    supplier TEXT DEFAULT '',
    status TEXT DEFAULT 'active',
    note TEXT DEFAULT '',
    stock_version INTEGER DEFAULT 0,
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_by TEXT DEFAULT '',
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_material_code
    ON v2_003_materials(material_code) WHERE material_code != ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_material_barcode
    ON v2_003_materials(barcode) WHERE barcode != ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_material_status_category
    ON v2_003_materials(status, category, name_zh)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_material_low_stock
    ON v2_003_materials(status, current_qty, min_qty)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_material_location
    ON v2_003_materials(warehouse_name, location_code)`,

  `CREATE TABLE IF NOT EXISTS v2_003_material_txns (
    id TEXT PRIMARY KEY,
    material_id TEXT DEFAULT '',
    txn_type TEXT DEFAULT '',
    qty_delta REAL DEFAULT 0,
    qty_before REAL DEFAULT 0,
    qty_after REAL DEFAULT 0,
    warehouse_name TEXT DEFAULT '',
    location_code TEXT DEFAULT '',
    recipient_id TEXT DEFAULT '',
    recipient_name TEXT DEFAULT '',
    purpose TEXT DEFAULT '',
    related_doc_no TEXT DEFAULT '',
    unit_cost REAL DEFAULT 0,
    supplier TEXT DEFAULT '',
    note TEXT DEFAULT '',
    operator_id TEXT DEFAULT '',
    operator_name TEXT DEFAULT '',
    created_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_mtxn_material_time
    ON v2_003_material_txns(material_id, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_mtxn_type_time
    ON v2_003_material_txns(txn_type, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_mtxn_operator_time
    ON v2_003_material_txns(operator_id, created_at)`,

  `CREATE TABLE IF NOT EXISTS v2_003_assets (
    id TEXT PRIMARY KEY,
    asset_code TEXT DEFAULT '',
    barcode TEXT DEFAULT '',
    name_zh TEXT DEFAULT '',
    name_ko TEXT DEFAULT '',
    category TEXT DEFAULT '',
    brand TEXT DEFAULT '',
    model TEXT DEFAULT '',
    serial_no TEXT DEFAULT '',
    warehouse_name TEXT DEFAULT '',
    location_code TEXT DEFAULT '',
    keeper_id TEXT DEFAULT '',
    keeper_name TEXT DEFAULT '',
    status TEXT DEFAULT 'available',
    purchase_date TEXT DEFAULT '',
    purchase_cost REAL DEFAULT 0,
    currency TEXT DEFAULT 'KRW',
    supplier TEXT DEFAULT '',
    warranty_until TEXT DEFAULT '',
    note TEXT DEFAULT '',
    asset_version INTEGER DEFAULT 0,
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_by TEXT DEFAULT '',
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_asset_code
    ON v2_003_assets(asset_code) WHERE asset_code != ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_asset_barcode
    ON v2_003_assets(barcode) WHERE barcode != ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_asset_serial
    ON v2_003_assets(serial_no) WHERE serial_no != ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_asset_status_category
    ON v2_003_assets(status, category, name_zh)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_asset_keeper
    ON v2_003_assets(keeper_id, status)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_asset_location
    ON v2_003_assets(warehouse_name, location_code)`,

  `CREATE TABLE IF NOT EXISTS v2_003_asset_txns (
    id TEXT PRIMARY KEY,
    asset_id TEXT DEFAULT '',
    action_type TEXT DEFAULT '',
    status_before TEXT DEFAULT '',
    status_after TEXT DEFAULT '',
    from_warehouse TEXT DEFAULT '',
    from_location TEXT DEFAULT '',
    to_warehouse TEXT DEFAULT '',
    to_location TEXT DEFAULT '',
    from_keeper_id TEXT DEFAULT '',
    from_keeper_name TEXT DEFAULT '',
    to_keeper_id TEXT DEFAULT '',
    to_keeper_name TEXT DEFAULT '',
    related_doc_no TEXT DEFAULT '',
    note TEXT DEFAULT '',
    operator_id TEXT DEFAULT '',
    operator_name TEXT DEFAULT '',
    created_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_atxn_asset_time
    ON v2_003_asset_txns(asset_id, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_atxn_type_time
    ON v2_003_asset_txns(action_type, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_atxn_operator_time
    ON v2_003_asset_txns(operator_id, created_at)`,

  // ---- v2.20260808b：003 采购申请、发货与到货收货 ----
  `CREATE TABLE IF NOT EXISTS v2_003_purchase_orders (
    id TEXT PRIMARY KEY,
    order_no TEXT DEFAULT '',
    status TEXT DEFAULT 'requested',
    urgency TEXT DEFAULT 'normal',
    warehouse_name TEXT DEFAULT '',
    request_reason TEXT DEFAULT '',
    requested_by_id TEXT DEFAULT '',
    requested_by_name TEXT DEFAULT '',
    purchaser_name TEXT DEFAULT '',
    supplier TEXT DEFAULT '',
    purchase_channel TEXT DEFAULT '',
    platform_order_no TEXT DEFAULT '',
    expected_date TEXT DEFAULT '',
    currency TEXT DEFAULT 'KRW',
    total_amount REAL DEFAULT 0,
    note TEXT DEFAULT '',
    has_discrepancy INTEGER DEFAULT 0,
    closed_reason TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_po_no
    ON v2_003_purchase_orders(order_no) WHERE order_no != ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_po_status_time
    ON v2_003_purchase_orders(status, updated_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_po_requester
    ON v2_003_purchase_orders(requested_by_id, created_at)`,

  `CREATE TABLE IF NOT EXISTS v2_003_purchase_order_lines (
    id TEXT PRIMARY KEY,
    order_id TEXT DEFAULT '',
    material_id TEXT DEFAULT '',
    requested_qty REAL DEFAULT 0,
    ordered_qty REAL DEFAULT 0,
    received_qty REAL DEFAULT 0,
    unit_cost REAL DEFAULT 0,
    note TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_pol_order_material
    ON v2_003_purchase_order_lines(order_id, material_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_pol_material
    ON v2_003_purchase_order_lines(material_id, created_at)`,

  `CREATE TABLE IF NOT EXISTS v2_003_purchase_shipments (
    id TEXT PRIMARY KEY,
    shipment_no TEXT DEFAULT '',
    order_id TEXT DEFAULT '',
    delivery_method TEXT DEFAULT 'express',
    tracking_no TEXT DEFAULT '',
    supplier TEXT DEFAULT '',
    expected_date TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    received_at TEXT DEFAULT '',
    received_by TEXT DEFAULT '',
    note TEXT DEFAULT '',
    created_by TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_shipment_no
    ON v2_003_purchase_shipments(shipment_no) WHERE shipment_no != ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_shipment_tracking
    ON v2_003_purchase_shipments(tracking_no) WHERE tracking_no != ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_shipment_status_method
    ON v2_003_purchase_shipments(status, delivery_method, expected_date)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_shipment_order
    ON v2_003_purchase_shipments(order_id, created_at)`,

  `CREATE TABLE IF NOT EXISTS v2_003_purchase_shipment_items (
    id TEXT PRIMARY KEY,
    shipment_id TEXT DEFAULT '',
    order_line_id TEXT DEFAULT '',
    material_id TEXT DEFAULT '',
    expected_qty REAL DEFAULT 0,
    received_qty REAL DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_psi_shipment_line
    ON v2_003_purchase_shipment_items(shipment_id, order_line_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_psi_material
    ON v2_003_purchase_shipment_items(material_id, created_at)`,

  `CREATE TABLE IF NOT EXISTS v2_003_purchase_receipts (
    id TEXT PRIMARY KEY,
    receipt_no TEXT DEFAULT '',
    shipment_id TEXT DEFAULT '',
    order_id TEXT DEFAULT '',
    delivery_method TEXT DEFAULT '',
    tracking_no TEXT DEFAULT '',
    has_discrepancy INTEGER DEFAULT 0,
    discrepancy_note TEXT DEFAULT '',
    received_by_id TEXT DEFAULT '',
    received_by_name TEXT DEFAULT '',
    warehouse_name TEXT DEFAULT '',
    received_at TEXT,
    created_at TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_receipt_no
    ON v2_003_purchase_receipts(receipt_no) WHERE receipt_no != ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_003_receipt_shipment
    ON v2_003_purchase_receipts(shipment_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_receipt_order_time
    ON v2_003_purchase_receipts(order_id, received_at)`,

  `CREATE TABLE IF NOT EXISTS v2_003_purchase_receipt_items (
    id TEXT PRIMARY KEY,
    receipt_id TEXT DEFAULT '',
    shipment_item_id TEXT DEFAULT '',
    order_line_id TEXT DEFAULT '',
    material_id TEXT DEFAULT '',
    expected_qty REAL DEFAULT 0,
    received_qty REAL DEFAULT 0,
    difference_qty REAL DEFAULT 0,
    warehouse_name TEXT DEFAULT '',
    location_code TEXT DEFAULT '',
    note TEXT DEFAULT '',
    created_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_pri_receipt
    ON v2_003_purchase_receipt_items(receipt_id, material_id)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_pri_material_time
    ON v2_003_purchase_receipt_items(material_id, created_at)`,

  // ---- v2.20260808c：003 部门归属、部门消耗统计 ----
  `ALTER TABLE v2_003_material_txns ADD COLUMN department TEXT DEFAULT ''`,
  `ALTER TABLE v2_003_assets ADD COLUMN keeper_department TEXT DEFAULT ''`,
  `ALTER TABLE v2_003_asset_txns ADD COLUMN from_department TEXT DEFAULT ''`,
  `ALTER TABLE v2_003_asset_txns ADD COLUMN to_department TEXT DEFAULT ''`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_mtxn_department_time
    ON v2_003_material_txns(department, txn_type, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_v2_003_asset_keeper_department
    ON v2_003_assets(keeper_department, status)`,
];

// 每次发布迁移变化时手动 +1（patch 段），冷启动只比对一次字符串即可跳过整段 MIGRATIONS
const CURRENT_SCHEMA_VERSION = 'v2.20260808c';

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
        WHERE status IN ('pending_issue','issued','working','ready_to_ship','preparing_outbound','operation_reserved','stock_operating','pending_outbound_update')`
    ).first(),
    env.DB.prepare(
      `SELECT * FROM v2_outbound_orders
        WHERE status IN ('pending_issue','issued','working','ready_to_ship','preparing_outbound','operation_reserved','stock_operating','pending_outbound_update')
        ORDER BY (CASE WHEN expected_ship_at IS NOT NULL AND expected_ship_at != '' THEN substr(expected_ship_at,1,10) ELSE order_date END) ASC,
                 created_at ASC LIMIT 3`
    ).all(),

    // ---- inbounds（待执行入库 / 多业务计划部分完成也算"待执行"）----
    env.DB.prepare(
      `SELECT COUNT(*) AS c FROM v2_inbound_plans
        WHERE source_type != 'return_session'
          AND COALESCE(is_deleted,0)=0
          AND status IN ('pending','unloading','unloading_putting_away','arrived_pending_putaway','putting_away','partially_completed')`
    ).first(),
    env.DB.prepare(
      `SELECT * FROM v2_inbound_plans
        WHERE source_type != 'return_session'
          AND COALESCE(is_deleted,0)=0
          AND status IN ('pending','unloading','unloading_putting_away','arrived_pending_putaway','putting_away','partially_completed')
        ORDER BY plan_date ASC, created_at ASC LIMIT 3`
    ).all(),

    // ---- feedbacks（现场反馈进行中；软删除/转正回滚的反馈不统计）----
    env.DB.prepare(
      `SELECT COUNT(*) AS c FROM v2_field_feedbacks
        WHERE status IN ('field_working','unloaded_pending_info')
          AND COALESCE(is_deleted,0)=0`
    ).first(),
    env.DB.prepare(
      `SELECT * FROM v2_field_feedbacks
        WHERE status IN ('field_working','unloaded_pending_info')
          AND COALESCE(is_deleted,0)=0
        ORDER BY created_at ASC LIMIT 3`
    ).all(),

    // ---- upcoming（未来 3 工作日入库计划，按日期分组）----
    env.DB.prepare(
      `SELECT * FROM v2_inbound_plans
        WHERE plan_date>=? AND plan_date<=?
          AND source_type != 'return_session'
          AND COALESCE(is_deleted,0)=0
          AND status NOT IN ('completed','cancelled','deleted')
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
  // 模糊搜索：customer_q（客户名）/ related_doc_q（关联单号 + 描述 + 摘要）
  const customer_q = String(body.customer_q || body.q_customer || "").trim();
  const related_doc_q = String(body.related_doc_q || body.q_related_doc || "").trim();
  const { limit, offset } = pageParams(body);
  // 构造 WHERE 子句（COUNT 与 SELECT 共用，binds 也共用顺序）
  let where = " WHERE 1=1";
  const binds = [];
  if (status) { where += " AND status=?"; binds.push(status); }
  if (biz_class) { where += " AND biz_class=?"; binds.push(biz_class); }
  // P1-6：记帐筛选
  if (body.accounting_required === 1 || body.accounting_required === '1') {
    where += " AND accounting_required=1";
  }
  if (body.accounted === 1 || body.accounted === '1') {
    where += " AND accounted=1";
  } else if (body.accounted === 0 || body.accounted === '0') {
    where += " AND accounting_required=1 AND accounted=0";
  }
  if (customer_q) {
    where += " AND COALESCE(customer,'') LIKE ?";
    binds.push("%" + customer_q + "%");
  }
  if (related_doc_q) {
    // v2_issue_tickets 真实存在的列：related_doc_no / issue_summary / issue_description / customer
    where += " AND (COALESCE(related_doc_no,'') LIKE ? OR COALESCE(issue_summary,'') LIKE ? OR COALESCE(issue_description,'') LIKE ?)";
    const kw = "%" + related_doc_q + "%";
    binds.push(kw, kw, kw);
  }
  const orderBy = sort === "oldest_first" ? " ORDER BY created_at ASC" : " ORDER BY created_at DESC";
  const countSql = "SELECT COUNT(*) AS c FROM v2_issue_tickets" + where;
  const listSql = "SELECT * FROM v2_issue_tickets" + where + orderBy + " LIMIT ? OFFSET ?";
  try {
    const countRow = binds.length > 0
      ? await env.DB.prepare(countSql).bind(...binds).first()
      : await env.DB.prepare(countSql).first();
    const total = Number((countRow && countRow.c) || 0);
    const rs = await env.DB.prepare(listSql).bind(...binds, limit, offset).all();
    return json({ ok: true, items: rs.results || [], ...pageMeta(total, limit, offset) });
  } catch (e) {
    // 让前端拿到真实错误而不是只看到"加载失败"
    return json({
      ok: false,
      error: "issue_list_failed",
      detail: "v2_issue_list SQL failed",
      message: String((e && e.message) || e),
      sql_preview: listSql,
      bind_count: binds.length
    }, 500);
  }
});

route("v2_issue_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const row = await env.DB.prepare("SELECT * FROM v2_issue_tickets WHERE id=?").bind(id).first();
  if (!row) return err("not found", 404);

  // 处理轮次（升序，便于 timeline；前端需要倒序时自己反转）
  const runsRs = await env.DB.prepare(
    "SELECT * FROM v2_issue_handle_runs WHERE issue_id=? ORDER BY started_at ASC"
  ).bind(id).all();
  const runs = runsRs.results || [];
  const runIds = runs.map(r => r.id).filter(Boolean);
  const jobIds = runs.map(r => r.job_id).filter(Boolean);

  // 客服追加处理需求（升序）
  const rwksRs = await env.DB.prepare(
    "SELECT * FROM v2_issue_rework_requests WHERE issue_id=? ORDER BY created_at ASC"
  ).bind(id).all();
  const reworks = rwksRs.results || [];

  // 三类附件查询（issue_ticket / issue_handle_run / ops_job）+ 旧数据兼容（issue / job / issue_handle）
  // 注意：旧 BUG 把照片写到 related_doc_type='issue_ticket' related_doc_id=job_id，这里也要捞回来
  const allAtts = [];
  // 1) issue_ticket / issue / issue_handle 同 issue_id
  {
    const r = await env.DB.prepare(
      "SELECT * FROM v2_attachments WHERE related_doc_type IN ('issue_ticket','issue','issue_handle') AND related_doc_id=?"
    ).bind(id).all();
    for (const a of (r.results || [])) allAtts.push(a);
  }
  // 2) issue_handle_run + run_ids
  if (runIds.length > 0) {
    const rows2 = await batchSelectInGlobal(env,
      "SELECT * FROM v2_attachments WHERE related_doc_type IN ('issue_handle_run','issue_handle') AND related_doc_id IN (PLACEHOLDER)",
      runIds);
    for (const a of rows2) allAtts.push(a);
  }
  // 3) ops_job / job + job_ids（attachment_category='issue_handle_photo' 或 'issue_attachment'，为兼容老数据放宽）
  if (jobIds.length > 0) {
    const rows3 = await batchSelectInGlobal(env,
      "SELECT * FROM v2_attachments WHERE related_doc_type IN ('ops_job','job','issue_ticket') AND related_doc_id IN (PLACEHOLDER)",
      jobIds);
    for (const a of rows3) {
      // 旧 bug 路径：related_doc_type='issue_ticket' 且 related_doc_id=job_id（不会和 issue_id=ISS-xxx 冲突，因 job_id 是 JOB-xxx）
      allAtts.push(a);
    }
  }
  // 去重（按 id）
  const seenAtt = {};
  const attachmentsAll = [];
  for (const a of allAtts) {
    if (seenAtt[a.id]) continue;
    seenAtt[a.id] = 1;
    attachmentsAll.push(a);
  }
  // 排序：created_at 升序
  attachmentsAll.sort((a, b) => String(a.created_at || '').localeCompare(String(b.created_at || '')));

  // 主附件（issue_ticket/issue/issue_handle 且 related_doc_id == issue.id）
  const ticketAtts = attachmentsAll.filter(a =>
    (a.related_doc_type === 'issue_ticket' || a.related_doc_type === 'issue' || a.related_doc_type === 'issue_handle')
    && a.related_doc_id === id
  );

  // 处理轮次附件归属：按 run_id (issue_handle_run/issue_handle) 和 job_id (ops_job/job/issue_ticket-orphan)
  const attsByRun = {};
  for (const a of attachmentsAll) {
    if ((a.related_doc_type === 'issue_handle_run' || a.related_doc_type === 'issue_handle') && a.related_doc_id) {
      (attsByRun[a.related_doc_id] = attsByRun[a.related_doc_id] || []).push(a);
    }
  }
  const attsByJob = {};
  for (const a of attachmentsAll) {
    const isJobLike = (a.related_doc_type === 'ops_job' || a.related_doc_type === 'job');
    // 旧 bug：issue_ticket 类型但 id 是 JOB-xxx
    const isOrphanIssueTicket = (a.related_doc_type === 'issue_ticket' && a.related_doc_id && /^JOB-/.test(a.related_doc_id));
    if (isJobLike || isOrphanIssueTicket) {
      (attsByJob[a.related_doc_id] = attsByJob[a.related_doc_id] || []).push(a);
    }
  }
  // 把附件挂到对应 run 上 + 拉取 segment 详情、计算实际工时/自然跨度
  const runsEnriched = [];
  for (const r of runs) {
    const list = [];
    if (r.id && attsByRun[r.id]) list.push(...attsByRun[r.id]);
    if (r.job_id && attsByJob[r.job_id]) list.push(...attsByJob[r.job_id]);
    // 同 run 内去重
    const seen = {};
    const dedup = [];
    for (const a of list) { if (!seen[a.id]) { seen[a.id] = 1; dedup.push(a); } }

    // 实际工时 = v2_ops_job_workers segment 累加；run.minutes_worked 兜底
    let actual_minutes = Number(r.minutes_worked || 0);
    let segments = [];
    let worker_names = r.handler_name || '';
    if (r.job_id) {
      const sum = await sumJobWorkerMinutes(env, r.job_id, r.ended_at || now(), false);
      segments = sum.segments;
      if (segments.length > 0) {
        actual_minutes = sum.total_minutes;
        if (sum.worker_names) worker_names = sum.worker_names;
      }
    }
    // 自然跨度：started_at → ended_at（未完成则到 now）
    let natural_span_minutes = 0;
    const sMs = Date.parse(r.started_at || '');
    const eMs = Date.parse(r.ended_at || now());
    if (Number.isFinite(sMs) && Number.isFinite(eMs)) {
      natural_span_minutes = Math.max(0, Math.round((eMs - sMs) / 60000 * 10) / 10);
    }

    runsEnriched.push(Object.assign({}, r, {
      attachments: dedup,
      segments,
      actual_minutes,
      natural_span_minutes,
      worker_names
    }));
  }

  // ---- timeline（升序）----
  const timeline = [];
  // 1) 创建
  timeline.push({
    type: 'issue_created',
    title: '客服提出问题 / 고객지원 문제 제기',
    content: row.issue_description || '',
    user: row.submitted_by || '',
    at: row.created_at || '',
    attachments: ticketAtts
  });
  // 2) 每轮处理（开始 + 结束分两条；如未完成只有开始）
  for (const r of runsEnriched) {
    timeline.push({
      type: 'handle_started',
      title: '仓库开始处理 / 창고 처리 시작',
      content: '',
      user: r.handler_name || r.handler_id || '',
      at: r.started_at || '',
      attachments: []
    });
    if (r.run_status === 'completed' && r.ended_at) {
      timeline.push({
        type: 'handle_finished',
        title: '仓库处理完成 / 창고 처리 완료',
        content: r.feedback_text || '',
        user: r.handler_name || r.handler_id || '',
        at: r.ended_at || '',
        minutes_worked: Number(r.actual_minutes || r.minutes_worked || 0),
        actual_minutes: Number(r.actual_minutes || r.minutes_worked || 0),
        natural_span_minutes: Number(r.natural_span_minutes || 0),
        started_at: r.started_at || '',
        worker_names: r.worker_names || '',
        segments: r.segments || [],
        attachments: r.attachments || []
      });
    }
  }
  // 3) 客服追加请求
  for (const w of reworks) {
    timeline.push({
      type: 'rework_requested',
      title: '客服追加处理需求 / 고객지원 추가 처리 요청',
      content: w.request_note || '',
      user: w.requested_by || '',
      at: w.created_at || '',
      rework_status: w.status || '',
      attachments: []
    });
  }
  // 4) 记账提示 / 已记账
  if (row.accounting_required_at) {
    timeline.push({
      type: 'accounting_required',
      title: '提示需记帐 / 기장 알림',
      content: row.accounting_note || '',
      user: row.accounting_required_by || '',
      at: row.accounting_required_at,
      attachments: []
    });
  }
  if (row.accounted_at) {
    timeline.push({
      type: 'accounted',
      title: '已记帐 / 기장 완료',
      content: '',
      user: row.accounted_by || '',
      at: row.accounted_at,
      attachments: []
    });
  }
  // 5) 关闭：用 issue.updated_at 作为 closed 时间（仅当 status=completed 且无后续事件）
  if (row.status === 'completed' || row.status === 'closed') {
    timeline.push({
      type: 'issue_closed',
      title: '问题完成 / 문제 완료',
      content: '',
      user: '',
      at: row.updated_at || '',
      attachments: []
    });
  }
  if (row.status === 'cancelled') {
    timeline.push({
      type: 'issue_cancelled',
      title: '问题已作废 / 문제 취소',
      content: '',
      user: '',
      at: row.updated_at || '',
      attachments: []
    });
  }
  // timeline 升序
  timeline.sort((a, b) => String(a.at || '').localeCompare(String(b.at || '')));

  return json({
    ok: true,
    issue: row,
    item: row,                  // 别名，方便新前端
    handle_runs: runsEnriched,  // 已附 attachments
    runs: runsEnriched,         // 别名
    reworks,
    attachments: ticketAtts,    // 主附件（issue_ticket）
    all_attachments: attachmentsAll,  // 全部附件（含 run/job）
    timeline
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

// P1-6：标记需要记帐
route("v2_issue_mark_accounting_required", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_issue_mark_accounting_required", async () => {
    const row = await env.DB.prepare("SELECT id, status FROM v2_issue_tickets WHERE id=?").bind(id).first();
    if (!row) return { ok: false, error: "not_found" };
    const t = now();
    const by = String(body.by || body.actor || "");
    const note = String(body.accounting_note || "");
    await env.DB.prepare(
      "UPDATE v2_issue_tickets SET accounting_required=1, accounted=0, accounting_required_by=?, accounting_required_at=?, accounting_note=?, updated_at=? WHERE id=?"
    ).bind(by, t, note, t, id).run();
    return { ok: true, id };
  });
});

// P1-6：标记已记帐
route("v2_issue_mark_accounted", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_issue_mark_accounted", async () => {
    const row = await env.DB.prepare("SELECT accounting_required FROM v2_issue_tickets WHERE id=?").bind(id).first();
    if (!row) return { ok: false, error: "not_found" };
    if (Number(row.accounting_required) !== 1) {
      return { ok: false, error: "not_marked_required", message: "请先标记需要记帐" };
    }
    const t = now();
    const by = String(body.by || body.actor || "");
    await env.DB.prepare(
      "UPDATE v2_issue_tickets SET accounted=1, accounted_by=?, accounted_at=?, updated_at=? WHERE id=?"
    ).bind(by, t, t, id).run();
    return { ok: true, id };
  });
});

route("v2_issue_rework", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const rework_note = String(body.rework_note || "").trim();
  if (!id) return err("missing id");
  if (!rework_note) return err("missing rework_note");

  return withIdem(env, body, "v2_issue_rework", async () => {
    const issue = await env.DB.prepare("SELECT id, status FROM v2_issue_tickets WHERE id=?").bind(id).first();
    if (!issue) return { ok: false, error: "not_found" };
    if (issue.status === 'cancelled') return { ok: false, error: "cancelled_cannot_rework" };
    const t = now();
    const requested_by = String(body.requested_by || body.by || "").trim();
    const rework_id = "RWK-" + uid();
    // 关联最新一轮 working/completed run（方便追溯客服是针对哪一轮发的追加）
    const lastRun = await env.DB.prepare(
      "SELECT id FROM v2_issue_handle_runs WHERE issue_id=? ORDER BY started_at DESC LIMIT 1"
    ).bind(id).first();
    await env.DB.prepare(`
      INSERT INTO v2_issue_rework_requests
        (id, issue_id, request_note, requested_by, status, created_at, related_run_id)
      VALUES(?,?,?,?, 'open', ?, ?)
    `).bind(rework_id, id, rework_note, requested_by, t, lastRun ? lastRun.id : '').run();
    // 同时回写 issue.rework_note（兼容旧前端：详情页只显示最新一条）
    await env.DB.prepare(
      "UPDATE v2_issue_tickets SET status='rework_required', rework_note=?, updated_at=? WHERE id=?"
    ).bind(rework_note, t, id).run();
    return { ok: true, rework_id };
  });
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

// 暂时离开后继续处理：在原 job 内新建一段 segment，工时按段累加
route("v2_issue_handle_resume", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const issue_id = String(body.issue_id || "").trim();
  const handler_id = String(body.handler_id || "").trim();
  const handler_name = String(body.handler_name || "").trim();
  if (!issue_id || !handler_id) return err("missing issue_id or handler_id");

  return withIdem(env, body, "v2_issue_handle_resume", async () => {
    // 找当前 worker 的 working run
    const run = await env.DB.prepare(
      "SELECT * FROM v2_issue_handle_runs WHERE issue_id=? AND handler_id=? AND run_status='working' ORDER BY started_at DESC LIMIT 1"
    ).bind(issue_id, handler_id).first();
    if (!run) return { ok: false, error: "no_working_run", message: "未找到进行中的处理记录，请重新开始" };
    if (!run.job_id) return { ok: false, error: "run_missing_job_id" };

    // 已经有 open segment 直接返回（幂等）
    const existing = await findOpenSeg(env, run.job_id, handler_id);
    if (existing) {
      return { ok: true, run_id: run.id, job_id: run.job_id, worker_seg_id: existing.id, already_open: true };
    }

    // 检查 worker 是否在其它 job 忙（允许在同一 job_id）
    const busy = await checkWorkerBusy(env, handler_id, run.job_id);
    if (busy) return { ok: false, error: "worker_busy", busy_job_type: busy.job_type };

    const t = now();

    // 确保 job 仍处于 working
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='working', updated_at=? WHERE id=? AND status!='completed'"
    ).bind(t, run.job_id).run();

    const worker_seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(worker_seg_id, run.job_id, handler_id, handler_name || run.handler_name || '', t).run();

    await recalcActiveCount(env, run.job_id, t);

    return { ok: true, run_id: run.id, job_id: run.job_id, worker_seg_id, resumed: true };
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
  if (run.run_status === "completed") {
    return json({ ok: true, already_completed: true, error: "already_completed", message: "处理已完成，请勿重复提交", run_id, job_id: run.job_id || '' });
  }

  const t = now();

  // 关闭该 job 下所有 open segment（逐段算 left_at-joined_at），再按 segment 汇总实际工时
  // 严禁用 finish_time - run.started_at 作为工时（会把暂时离开/跨天等待都算入）
  let minutes = 0;
  if (run.job_id) {
    const sum = await sumJobWorkerMinutes(env, run.job_id, t, true);
    minutes = Math.round((sum.total_minutes || 0) * 10) / 10;

    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', updated_at=? WHERE id=?"
    ).bind(t, run.job_id).run();
  }

  // Update run
  await env.DB.prepare(`
    UPDATE v2_issue_handle_runs SET ended_at=?, minutes_worked=?, feedback_text=?, run_status='completed' WHERE id=?
  `).bind(t, minutes, feedback_text, run_id).run();

  // Update issue：按所有完成轮次的实际 minutes_worked 汇总
  const allRuns = await env.DB.prepare(
    "SELECT minutes_worked FROM v2_issue_handle_runs WHERE issue_id=? AND run_status='completed'"
  ).bind(run.issue_id).all();
  const totalMin = Math.round((allRuns.results || []).reduce((s, r) => s + (Number(r.minutes_worked) || 0), 0) * 10) / 10;

  await env.DB.prepare(`
    UPDATE v2_issue_tickets SET status='responded', latest_feedback_text=?, total_minutes_worked=?, updated_at=? WHERE id=?
  `).bind(feedback_text, totalMin, t, run.issue_id).run();

  // 关闭尚未 resolved 的客服追加请求（本轮处理视为对最近追加的回应）
  await env.DB.prepare(`
    UPDATE v2_issue_rework_requests
       SET status='resolved', resolved_at=?, resolved_by=?
     WHERE issue_id=? AND status='open'
  `).bind(t, run.handler_name || run.handler_id || '', run.issue_id).run();

  return json({ ok: true, minutes_worked: minutes, total_minutes: totalMin, run_id, job_id: run.job_id || '' });
});

// =====================================================
// OUTBOUND ORDERS — Collab side
// =====================================================
route("v2_outbound_order_create", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  // 业务分类必填（direct_ship/bulk/return）
  const VALID_BIZ = ['direct_ship','bulk','return'];
  const biz_class = String(body.biz_class || "").trim();
  if (!biz_class || VALID_BIZ.indexOf(biz_class) === -1) {
    return err("biz_class 必须是 direct_ship/bulk/return / 업무 분류는 direct_ship/bulk/return 중 하나여야 합니다");
  }
  const uses_stock_operation = (Number(body.uses_stock_operation) === 1) ? 1 : 0;
  return withIdem(env, body, "v2_outbound_order_create", async () => {
    const outbound_mode = String(body.outbound_mode || "").trim();
    const VALID_MODES = ['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'];
    if (!outbound_mode || !VALID_MODES.includes(outbound_mode)) return { ok: false, error: "invalid outbound_mode" };
    const id = "OB-" + uid();
    const t = now();
    // 预计出库 → 统一存"只日期" YYYY-MM-DD（即便客户传旧格式 datetime-local 也归一）
    const expected_ship_at = normalizeDateOnly(body.expected_ship_at);
    const _esaDate = expected_ship_at; // 已是 YYYY-MM-DD 或 ''
    const order_date = String(body.order_date || _esaDate || kstToday());
    const display_no = await nextOutboundDisplayNo(env, order_date);
    // 库内操作型：初始状态 operation_reserved；普通：pending_issue
    const initStatus = uses_stock_operation === 1 ? 'operation_reserved' : 'pending_issue';
    const initStockOpStatus = uses_stock_operation === 1 ? 'reserved' : '';
    const outbound_requirement = String(body.outbound_requirement || "").trim();
    const source_inbound_plan_id = String(body.source_inbound_plan_id || "").trim();
    await env.DB.prepare(`
      INSERT INTO v2_outbound_orders(id, order_date, customer, biz_class, operation_mode,
        outbound_mode, instruction, remark, status, source_inbound_plan_id, created_by, created_at, updated_at,
        destination, po_no, wms_work_order_no,
        planned_box_count, planned_pallet_count, actual_box_count, actual_pallet_count, display_no,
        uses_stock_operation, stock_operation_status, expected_ship_at, outbound_requirement)
      VALUES(?,?,?,?,'',?,?,'',?,?,?,?,?,?,?,?,?,?,0,0,?,?,?,?,?)
    `).bind(
      id,
      order_date,
      String(body.customer || ""),
      biz_class,
      outbound_mode,
      String(body.instruction || ""),
      initStatus,
      source_inbound_plan_id,
      String(body.created_by || ""),
      t, t,
      String(body.destination || ""),
      String(body.po_no || ""),
      String(body.wms_work_order_no || ""),
      Number(body.planned_box_count || 0),
      Number(body.planned_pallet_count || 0),
      display_no,
      uses_stock_operation,
      initStockOpStatus,
      expected_ship_at,
      outbound_requirement
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
  // date_basis：'expected_ship_at'（默认，按预计出库日期）/ 'order_date'（兼容旧调用）
  const date_basis = String(body.date_basis || "expected_ship_at").trim();
  const status = String(body.status || "").trim();
  const accounted = String(body.accounted == null ? "" : body.accounted).trim();
  const biz_class = String(body.biz_class || "").trim();
  const customer_keyword = String(body.customer_keyword || "").trim();
  const usesStockRaw = String(body.uses_stock_operation == null ? "" : body.uses_stock_operation).trim();
  const hasMaterialRaw = String(body.has_material == null ? "" : body.has_material).trim();
  const { limit, offset } = pageParams(body);
  // 按 date_basis 选择字段：expected_ship_at 派生 date_key（非空时取其日期，否则 fallback order_date）
  const _dateExpr = (date_basis === 'order_date')
    ? "order_date"
    : "(CASE WHEN expected_ship_at IS NOT NULL AND expected_ship_at != '' THEN substr(expected_ship_at,1,10) ELSE order_date END)";
  let where = " WHERE 1=1";
  const binds = [];
  if (start) { where += " AND " + _dateExpr + ">=?"; binds.push(start); }
  if (end)   { where += " AND " + _dateExpr + "<=?"; binds.push(end); }
  if (status) { where += " AND status=?"; binds.push(status); }
  else { where += " AND status != 'cancelled'"; } // 默认"全部状态"排除已取消，仅当显式筛 cancelled 才返回
  if (accounted === "1") { where += " AND accounted=1"; }
  else if (accounted === "0") { where += " AND (accounted IS NULL OR accounted=0)"; }
  if (biz_class) { where += " AND biz_class=?"; binds.push(biz_class); }
  if (customer_keyword) { where += " AND customer LIKE ?"; binds.push('%' + customer_keyword + '%'); }
  if (usesStockRaw === "1") { where += " AND uses_stock_operation=1"; }
  else if (usesStockRaw === "0") { where += " AND (uses_stock_operation IS NULL OR uses_stock_operation=0)"; }
  if (hasMaterialRaw === "1") {
    where += " AND EXISTS (SELECT 1 FROM v2_attachments a WHERE a.related_doc_type='outbound_order' AND a.related_doc_id=v2_outbound_orders.id AND a.attachment_category='outbound_material')";
  } else if (hasMaterialRaw === "0") {
    where += " AND NOT EXISTS (SELECT 1 FROM v2_attachments a WHERE a.related_doc_type='outbound_order' AND a.related_doc_id=v2_outbound_orders.id AND a.attachment_category='outbound_material')";
  }
  const countRow = binds.length > 0
    ? await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_outbound_orders" + where).bind(...binds).first()
    : await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_outbound_orders" + where).first();
  const total = Number((countRow && countRow.c) || 0);
  const listSql = "SELECT * FROM v2_outbound_orders" + where + " ORDER BY " + _dateExpr + " DESC, created_at DESC LIMIT ? OFFSET ?";
  const rs = await env.DB.prepare(listSql).bind(...binds, limit, offset).all();
  const items = rs.results || [];
  // 注入 material_count（CHUNK=80 防 D1 too many SQL variables）
  if (items.length > 0) {
    const ids = items.map(o => o.id);
    const matRows = await batchSelectInGlobal(env,
      `SELECT related_doc_id AS id, COUNT(*) AS c FROM v2_attachments
        WHERE related_doc_type='outbound_order' AND attachment_category='outbound_material'
          AND related_doc_id IN (PLACEHOLDER) GROUP BY related_doc_id`,
      ids);
    const map = {};
    for (const r of matRows) map[r.id] = Number(r.c || 0);
    for (const it of items) it.material_count = Number(map[it.id] || 0);

    // 注入 latest_change_summary（每单最新一条 change_log 摘要，用于列表 hover/小字显示）
    const ackIds = items.filter(o => Number(o.warehouse_ack_required) === 1).map(o => o.id);
    if (ackIds.length > 0) {
      const sumRows = await batchSelectInGlobal(env,
        `SELECT order_id, summary_text, revision_no FROM v2_outbound_order_change_logs
          WHERE order_id IN (PLACEHOLDER) AND warehouse_ack_required=1
          ORDER BY revision_no DESC, changed_at DESC`,
        ackIds);
      const sumMap = {};
      for (const r of sumRows) {
        if (!sumMap[r.order_id]) sumMap[r.order_id] = r.summary_text || '';
      }
      for (const it of items) {
        if (sumMap[it.id]) it.latest_change_summary = sumMap[it.id];
      }
    }
  }
  return json({ ok: true, items, ...pageMeta(total, limit, offset) });
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
  // 注入 material_count
  const materialCount = (orderAtts.results || []).filter(a => a.attachment_category === 'outbound_material').length;
  row.material_count = materialCount;

  // 修改日志（按 revision_no DESC）
  const changeLogRs = await env.DB.prepare(
    `SELECT id, revision_no, change_type, changed_by, changed_at,
            diff_json, summary_text, warehouse_ack_required, warehouse_ack_by, warehouse_ack_at, ack_source
       FROM v2_outbound_order_change_logs
      WHERE order_id=?
      ORDER BY revision_no DESC, changed_at DESC
      LIMIT 50`
  ).bind(id).all();
  const change_logs = (changeLogRs.results || []).map(r => {
    let diff = {};
    try { diff = JSON.parse(r.diff_json || '{}'); } catch (e) {}
    return {
      id: r.id,
      revision_no: Number(r.revision_no || 0),
      change_type: r.change_type || 'order_update',
      changed_by: r.changed_by || '',
      changed_at: r.changed_at || '',
      diff,
      summary_text: r.summary_text || '',
      warehouse_ack_required: Number(r.warehouse_ack_required || 0),
      warehouse_ack_by: r.warehouse_ack_by || '',
      warehouse_ack_at: r.warehouse_ack_at || '',
      ack_source: r.ack_source || ''
    };
  });
  const pending_change_logs = change_logs.filter(x => x.warehouse_ack_required === 1);
  const latest_change_log = change_logs.length > 0 ? change_logs[0] : null;

  return json({
    ok: true,
    order: row,
    lines: lines.results || [],
    jobs: jobs.results || [],
    attachments: allAtts,
    change_logs,
    pending_change_logs,
    latest_change_log
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
    // 库内操作型出库单状态
    "operation_reserved":      ["cancelled"],
    "stock_operating":         ["cancelled"],
    "pending_outbound_update": ["cancelled"],
    "preparing_outbound":      ["reopen_pending", "cancelled"],
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

// ===== 出库作业单：客服更新预计出库计划（库内操作完成后） =====
route("v2_outbound_order_update_ship_plan", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_outbound_order_update_ship_plan", async () => {
    const order = await env.DB.prepare("SELECT * FROM v2_outbound_orders WHERE id=?").bind(id).first();
    if (!order) return { ok: false, error: "not_found", message: "出库单不存在" };
    const cur = String(order.status || "");
    const ALLOWED_FROM = ['pending_outbound_update', 'operation_reserved', 'issued', 'preparing_outbound'];
    if (ALLOWED_FROM.indexOf(cur) === -1) {
      return { ok: false, error: "invalid_status",
        message: "当前状态（" + cur + "）不允许更新出库计划 / 현재 상태에서는 출고 계획을 업데이트할 수 없습니다" };
    }
    const VALID_MODES = ['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'];
    const expected_ship_at = normalizeDateOnly(body.expected_ship_at);
    const outbound_mode = String(body.outbound_mode || "").trim();
    const destination = String(body.destination || "").trim();
    const outbound_requirement = String(body.outbound_requirement || "").trim();
    const remark = String(body.remark || "").trim();
    if (outbound_mode && VALID_MODES.indexOf(outbound_mode) === -1) {
      return { ok: false, error: "invalid_outbound_mode" };
    }
    const t = now();
    // 仅更新"提供且非空"的字段；空串保留原值
    const provided = {};
    if (expected_ship_at) provided.expected_ship_at = expected_ship_at;
    if (outbound_mode) provided.outbound_mode = outbound_mode;
    if (destination) provided.destination = destination;
    if (outbound_requirement) provided.outbound_requirement = outbound_requirement;
    if (remark) provided.remark = remark;

    const diffResult = buildOutboundDiff(order, provided, [
      'expected_ship_at','outbound_mode','destination','outbound_requirement','remark'
    ]);

    const sets = ["updated_at=?"];
    const binds = [t];
    for (const k of diffResult.changedFields) {
      sets.push(k + "=?");
      binds.push(provided[k]);
    }
    // expected_ship_at 变化时同步 order_date（display_no 沿用旧值）
    if (diffResult.changedFields.indexOf('expected_ship_at') !== -1 && expected_ship_at.length >= 10) {
      sets.push("order_date=?");
      binds.push(expected_ship_at.slice(0, 10));
    }
    // 库内操作型 + pending_outbound_update → preparing_outbound（流程必经路径，与 diff 无关）
    let nextStatus = cur;
    if (Number(order.uses_stock_operation) === 1 && cur === 'pending_outbound_update') {
      nextStatus = 'preparing_outbound';
      sets.push("status=?");
      binds.push(nextStatus);
    }
    // 有 diff 时同步把 warehouse_ack_required 标记，写 change_log
    const by = String(body.by || body.actor || body.modified_by || "");
    if (diffResult.changedFields.length > 0) {
      sets.push("revision_no=COALESCE(revision_no,0)+1");
      sets.push("last_modified_by=?"); binds.push(by);
      sets.push("last_modified_at=?"); binds.push(t);
      sets.push("warehouse_ack_required=1");
      sets.push("warehouse_ack_by=''");
      sets.push("warehouse_ack_at=''");
    }
    binds.push(id);
    await env.DB.prepare(
      "UPDATE v2_outbound_orders SET " + sets.join(", ") + " WHERE id=?"
    ).bind(...binds).run();

    if (diffResult.changedFields.length > 0) {
      const newRevision = Number(order.revision_no || 0) + 1;
      await insertOutboundChangeLog(env, {
        order_id: id,
        revision_no: newRevision,
        change_type: 'ship_plan',
        changed_by: by,
        diff: diffResult.diff,
        summary: diffResult.summary,
        t
      });
    }

    return { ok: true, status: nextStatus, no_change: diffResult.changedFields.length === 0 };
  });
});

// P1-8：未出库完成的出库单 — 修改（产生 warehouse_ack 提示 + 写 change_log）
route("v2_outbound_order_update", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_outbound_order_update", async () => {
    const order = await env.DB.prepare("SELECT * FROM v2_outbound_orders WHERE id=?").bind(id).first();
    if (!order) return { ok: false, error: "not_found" };
    const FROZEN = ['shipped', 'completed', 'cancelled'];
    if (FROZEN.indexOf(order.status) !== -1) {
      return { ok: false, error: "frozen_status_cannot_edit", message: "已出库/完成/取消的出库单不能修改：" + order.status };
    }
    const t = now();
    const by = String(body.by || body.actor || body.modified_by || "");

    const editable = [
      'order_date', 'customer', 'biz_class', 'operation_mode', 'outbound_mode',
      'destination', 'po_no', 'wms_work_order_no',
      'expected_ship_at', 'outbound_requirement', 'instruction', 'remark',
      'planned_box_count', 'planned_pallet_count', 'uses_stock_operation',
      // 提货信息（P1-9 一并支持）
      'pickup_vehicle_no','pickup_driver_name','pickup_driver_phone',
      'pickup_person_name','pickup_company','pickup_time','pickup_note'
    ];

    // 先做 diff 对比，只把真正变化的字段写库
    const diffResult = buildOutboundDiff(order, body, editable);
    if (diffResult.changedFields.length === 0) {
      return { ok: true, id, revision_no: Number(order.revision_no || 0), no_change: true };
    }

    const sets = [];
    const binds = [];
    let pickupTouched = false;
    let expectedShipTouched = false;
    let orderDateExplicit = false;
    for (const k of diffResult.changedFields) {
      sets.push(k + "=?");
      const v = body[k];
      if (k.startsWith('planned_') || k === 'uses_stock_operation') {
        binds.push(Number(v || 0));
      } else if (k === 'expected_ship_at') {
        // 预计出库 → 只存日期
        binds.push(normalizeDateOnly(v));
      } else {
        binds.push(String(v == null ? '' : v));
      }
      if (k.indexOf('pickup_') === 0) pickupTouched = true;
      if (k === 'expected_ship_at') expectedShipTouched = true;
      if (k === 'order_date') orderDateExplicit = true;
    }
    // 修改 expected_ship_at 但未显式覆盖 order_date → 同步派生 order_date
    // 历史 display_no 不重写（避免引用混乱）；下次新单按新日期生成
    if (expectedShipTouched && !orderDateExplicit) {
      const _esa = String(body.expected_ship_at || '').trim();
      if (_esa.length >= 10) {
        sets.push("order_date=?");
        binds.push(_esa.slice(0, 10));
      }
    }
    const newRevision = Number(order.revision_no || 0) + 1;
    sets.push("revision_no=?"); binds.push(newRevision);
    sets.push("last_modified_by=?"); binds.push(by);
    sets.push("last_modified_at=?"); binds.push(t);
    sets.push("warehouse_ack_required=1");
    sets.push("warehouse_ack_by=''");
    sets.push("warehouse_ack_at=''");
    if (pickupTouched) {
      sets.push("pickup_confirm_required=1");
      sets.push("pickup_confirmed_by=''");
      sets.push("pickup_confirmed_at=''");
    }
    sets.push("updated_at=?"); binds.push(t);
    binds.push(id);
    await env.DB.prepare(
      "UPDATE v2_outbound_orders SET " + sets.join(", ") + " WHERE id=?"
    ).bind(...binds).run();

    // 写入修改明细日志（仅当有变化时）
    await insertOutboundChangeLog(env, {
      order_id: id,
      revision_no: newRevision,
      change_type: pickupTouched && diffResult.changedFields.every(f => f.indexOf('pickup_') === 0)
        ? 'pickup_update'
        : 'order_update',
      changed_by: by,
      diff: diffResult.diff,
      summary: diffResult.summary,
      t
    });

    return { ok: true, id, revision_no: newRevision, summary_text: diffResult.summary };
  });
});

// P1-8：仓库确认已查看出库单变更（同步清掉 change_logs 中所有 pending 记录）
route("v2_outbound_order_ack_change", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_outbound_order_ack_change", async () => {
    const order = await env.DB.prepare("SELECT id, warehouse_ack_required, revision_no FROM v2_outbound_orders WHERE id=?").bind(id).first();
    if (!order) return { ok: false, error: "not_found" };
    const alreadyAcked = Number(order.warehouse_ack_required) !== 1;
    const t = now();
    const worker = String(body.worker_name || body.by || "");
    const ack_source = String(body.source || '').trim() || 'warehouse';

    if (!alreadyAcked) {
      await env.DB.prepare(
        "UPDATE v2_outbound_orders SET warehouse_ack_required=0, warehouse_ack_by=?, warehouse_ack_at=?, updated_at=? WHERE id=?"
      ).bind(worker, t, t, id).run();
    }
    // 清掉该 order_id 所有 pending change_logs（即便主表已 ack 也确保日志同步）
    const updRs = await env.DB.prepare(
      "UPDATE v2_outbound_order_change_logs SET warehouse_ack_required=0, warehouse_ack_by=?, warehouse_ack_at=?, ack_source=? WHERE order_id=? AND warehouse_ack_required=1"
    ).bind(worker, t, ack_source, id).run();
    const acked_count = (updRs && updRs.meta && Number(updRs.meta.changes || 0)) || 0;

    return {
      ok: true,
      id,
      already_acked: alreadyAcked,
      acked_revision_no: Number(order.revision_no || 0),
      acked_count
    };
  });
});

// P1-9：仓库确认已查看提货信息
route("v2_outbound_pickup_confirm", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_outbound_pickup_confirm", async () => {
    const order = await env.DB.prepare("SELECT id, pickup_confirm_required FROM v2_outbound_orders WHERE id=?").bind(id).first();
    if (!order) return { ok: false, error: "not_found" };
    const t = now();
    const worker = String(body.worker_name || body.by || "");
    await env.DB.prepare(
      "UPDATE v2_outbound_orders SET pickup_confirm_required=0, pickup_confirmed_by=?, pickup_confirmed_at=?, updated_at=? WHERE id=?"
    ).bind(worker, t, t, id).run();
    return { ok: true, id };
  });
});

// 只读诊断：列出 substr(expected_ship_at,1,10) != order_date 的出库单
// 用途：客服反馈"4-30 筛出 5-4 的单"前的 audit；不修改任何数据
route("v2_outbound_order_diag_date_mismatch", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 200;
  if (limit > 1000) limit = 1000;
  const include_frozen = body.include_frozen === true || body.include_frozen === 1 || body.include_frozen === "1";
  let sql = `
    SELECT id, display_no, order_date, expected_ship_at, status, customer, created_at
    FROM v2_outbound_orders
    WHERE expected_ship_at IS NOT NULL AND expected_ship_at != ''
      AND substr(expected_ship_at,1,10) != order_date`;
  if (!include_frozen) {
    sql += " AND status NOT IN ('shipped','completed','cancelled')";
  }
  sql += " ORDER BY created_at DESC LIMIT ?";
  const rs = await env.DB.prepare(sql).bind(limit).all();
  const rows = rs.results || [];
  return json({
    ok: true,
    count: rows.length,
    truncated: rows.length >= limit,
    items: rows.map(r => ({
      id: r.id,
      display_no: r.display_no || '',
      order_date: r.order_date || '',
      expected_ship_at: fmtBusinessDateTime(r.expected_ship_at),
      expected_ship_date: String(r.expected_ship_at || '').slice(0, 10),
      status: r.status || '',
      customer: r.customer || '',
      created_at: fmtKst(r.created_at)
    }))
  });
});

// 只读 → 行动：把 expected_ship_at 非空的单 order_date 同步为 expected_ship_at 日期
// 限制：仅对未 shipped/completed/cancelled、且没有现场 job 的单生效；display_no 不重写
// 必须显式传 confirm:true 才执行；返回每条的 before/after
route("v2_outbound_order_admin_realign_order_date", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  if (body.confirm !== true && body.confirm !== "true" && body.confirm !== 1 && body.confirm !== "1") {
    return err("missing confirm:true (dry-run protection)");
  }
  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 100;
  if (limit > 500) limit = 500;
  // 没有进行中 job 的单 = NOT EXISTS in v2_ops_jobs(linked_outbound_order_id 或 related_doc_id)
  const rs = await env.DB.prepare(`
    SELECT id, display_no, order_date, expected_ship_at, status, customer
    FROM v2_outbound_orders o
    WHERE expected_ship_at IS NOT NULL AND expected_ship_at != ''
      AND substr(expected_ship_at,1,10) != order_date
      AND status NOT IN ('shipped','completed','cancelled')
      AND NOT EXISTS (
        SELECT 1 FROM v2_ops_jobs j
        WHERE (j.linked_outbound_order_id = o.id
            OR (j.related_doc_type IN ('outbound','outbound_order') AND j.related_doc_id = o.id))
          AND j.status IN ('pending','working','awaiting_close')
      )
    ORDER BY created_at DESC LIMIT ?
  `).bind(limit).all();
  const rows = rs.results || [];
  const t = now();
  const out = [];
  for (const r of rows) {
    const newDate = String(r.expected_ship_at).slice(0, 10);
    await env.DB.prepare(
      "UPDATE v2_outbound_orders SET order_date=?, updated_at=? WHERE id=?"
    ).bind(newDate, t, r.id).run();
    out.push({
      id: r.id,
      display_no: r.display_no || '',
      before_order_date: r.order_date || '',
      after_order_date: newDate,
      expected_ship_at: fmtBusinessDateTime(r.expected_ship_at),
      status: r.status,
      customer: r.customer || ''
    });
  }
  return json({ ok: true, updated: out.length, items: out });
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

  const loadableStatuses = ['issued', 'working', 'ready_to_ship', 'preparing_outbound'];
  if (order.status === 'shipped') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "该出库单已出库，不能再装货 / 이미 출고 완료되어 상차할 수 없습니다" });
  }
  if (order.status === 'cancelled') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "该出库单已取消 / 해당 출고단은 취소되었습니다" });
  }
  if (order.status === 'pending_issue') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "该出库单尚未下发，请先打印下发 / 아직 배정되지 않았습니다. 먼저 인쇄하세요" });
  }
  if (order.status === 'operation_reserved' || order.status === 'stock_operating') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "该出库单为库内操作型，仓库操作中尚不能正式装货 / 창고 재고 작업 중이라 상차할 수 없습니다" });
  }
  if (order.status === 'pending_outbound_update') {
    return json({ ok: true, kind: 'status_not_allowed', order, message: "库内操作已完成，待客服更新出库计划 / 창고 작업 완료, 출고 계획 업데이트 대기" });
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
        // 装货开始时把 order 推到 ready_to_ship（兼容 ready_to_ship/preparing_outbound/issued/working）
        await env.DB.prepare(
          "UPDATE v2_outbound_orders SET status='ready_to_ship', updated_at=? WHERE id=? AND status IN ('ready_to_ship','preparing_outbound','issued','working')"
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

  // 终态幂等保护：已完成的 job — finish 是幂等操作，返回 ok:true + already_completed:true
  // 同时兜底关闭可能残留的 open segment（即首次完成时漏关也能在重提交时修复）
  const jobCheck = await env.DB.prepare("SELECT status FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
  if (jobCheck && jobCheck.status === 'completed') {
    const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, now(), 'already_completed_cleanup');
    return json({ ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" });
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
      // 防御性收口：关闭所有遗留 open segment（多人任务即使 realCount=0 也确保 left_at 全部写入）
      await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');
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
// OUTBOUND STOCK OPERATION — 库内操作型出库单（仓库现货 → 库内操作 → 客服更新出库计划）
// =====================================================
// 列出待执行库内操作的出库单（仓库现场入口用）
route("v2_outbound_stock_op_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const status = String(body.status || "").trim();
  let sql = "SELECT * FROM v2_outbound_orders WHERE uses_stock_operation=1";
  const binds = [];
  if (status) {
    sql += " AND status=?";
    binds.push(status);
  } else {
    sql += " AND status IN ('operation_reserved','stock_operating')";
  }
  sql += " ORDER BY order_date ASC, created_at ASC LIMIT 200";
  const rs = await env.DB.prepare(sql).bind(...binds).all();
  return json({ ok: true, items: rs.results || [] });
});

// 仓库开始库内操作（创建/复用 ops_jobs，绑定 worker，推 order.status）
route("v2_outbound_stock_op_start", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const order_id = String(body.outbound_order_id || body.order_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!order_id) return err("missing outbound_order_id");
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_outbound_stock_op_start", async () => {
    const order = await env.DB.prepare("SELECT * FROM v2_outbound_orders WHERE id=?").bind(order_id).first();
    if (!order) return { ok: false, error: "not_found", message: "出库单不存在" };
    if (Number(order.uses_stock_operation) !== 1) {
      return { ok: false, error: "not_stock_op_order", message: "该出库单不是库内操作型" };
    }
    const cur = String(order.status || "");
    if (cur !== 'operation_reserved' && cur !== 'stock_operating') {
      return { ok: false, error: "invalid_status",
        message: "当前状态（" + cur + "）不允许开始库内操作" };
    }

    const t = now();
    // 复用已存在的 working job
    let job = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE job_type='outbound_stock_op' AND related_doc_type='outbound_order' AND related_doc_id=? AND status IN ('pending','working') LIMIT 1"
    ).bind(order_id).first();

    const busy = await checkWorkerBusy(env, worker_id, job ? job.id : null);
    if (busy) return { ok: false, error: "worker_has_active_job",
      active_job_id: busy.job_id, active_job_type: busy.job_type };

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
        VALUES(?, 'outbound', ?, 'outbound_stock_op', 'outbound_order', ?, 'working', ?, ?, ?, 1)
      `).bind(job_id, String(order.biz_class || ""), order_id, worker_id, t, t).run();
    }

    const seg_id = "WS-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_workers(id, job_id, worker_id, worker_name, joined_at)
      VALUES(?,?,?,?,?)
    `).bind(seg_id, job_id, worker_id, worker_name, t).run();

    // 同步 order：status=stock_operating, stock_operation_status=working, stock_operation_job_id
    await env.DB.prepare(
      "UPDATE v2_outbound_orders SET status='stock_operating', stock_operation_status='working', stock_operation_job_id=?, updated_at=? WHERE id=?"
    ).bind(job_id, t, order_id).run();

    return { ok: true, job_id, worker_seg_id: seg_id, is_new_job };
  });
});

// 仓库完成库内操作（写 result，关闭 segs，推 order.status='pending_outbound_update'）
route("v2_outbound_stock_op_finish", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const worker_name = String(body.worker_name || "").trim();
  if (!job_id) return err("missing job_id");

  return withIdem(env, body, "v2_outbound_stock_op_finish", async () => {
    const t = now();
    const box_count = Number(body.box_count || 0);
    const pallet_count = Number(body.pallet_count || 0);
    const remark = String(body.remark || "");
    const result_lines_json = String(body.result_lines_json || "");
    const extraResultJson = String(body.result_json || "");

    // 终态幂等保护 — finish 幂等：返回 ok:true + already_completed:true；兜底关闭遗留 open segment
    const jobCheck = await env.DB.prepare("SELECT status, related_doc_id FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!jobCheck) return { ok: false, error: "job_not_found" };
    if (jobCheck.status === 'completed') {
      const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
      return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" };
    }
    const order_id = jobCheck.related_doc_id || "";

    // 关闭该 worker 全部 open segs
    if (worker_id) await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');
    const realCount = await recalcActiveCount(env, job_id, t);

    // 写 ops_job_results（库内操作明细）
    const result_id = "RES-" + uid();
    let resultJsonStr = '';
    if (extraResultJson) {
      resultJsonStr = extraResultJson;
    } else {
      const resultObj = { box_count, pallet_count, remark };
      if (result_lines_json) {
        try { resultObj.result_lines = JSON.parse(result_lines_json); } catch (e) { /* ignore parse */ }
      }
      resultJsonStr = JSON.stringify(resultObj);
    }
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, created_by, created_at)
      VALUES(?,?,?,?,?,?,?,?)
    `).bind(result_id, job_id, box_count, pallet_count, remark, resultJsonStr, worker_id, t).run();

    const complete_job = body.complete_job !== false; // 默认完成

    if (complete_job && realCount <= 0) {
      // 防御性收口：关闭所有遗留 open segment
      await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');
      // 关闭 job
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', shared_result_json=?, updated_at=? WHERE id=?"
      ).bind(resultJsonStr, t, job_id).run();

      // 汇总该 job 全部 results 到 order.stock_operation_result_json
      const allRes = await env.DB.prepare(
        "SELECT box_count, pallet_count, remark, result_json, created_by, created_at FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at ASC"
      ).bind(job_id).all();
      let sumBox = 0, sumPallet = 0;
      const lines = [];
      for (const r of (allRes.results || [])) {
        sumBox += Number(r.box_count || 0);
        sumPallet += Number(r.pallet_count || 0);
        lines.push({
          box_count: Number(r.box_count || 0),
          pallet_count: Number(r.pallet_count || 0),
          remark: r.remark || "",
          created_by: r.created_by || "",
          created_at: r.created_at || ""
        });
      }
      const summary = {
        total_box_count: sumBox,
        total_pallet_count: sumPallet,
        last_box_count: box_count,
        last_pallet_count: pallet_count,
        last_remark: remark,
        results: lines
      };

      // 推 order.status = pending_outbound_update + 写完成时间/人
      if (order_id) {
        await env.DB.prepare(
          `UPDATE v2_outbound_orders
           SET status='pending_outbound_update',
               stock_operation_status='completed',
               stock_operation_completed_at=?,
               stock_operation_completed_by=?,
               stock_operation_result_json=?,
               updated_at=?
           WHERE id=?`
        ).bind(t, (worker_name || worker_id || ''), JSON.stringify(summary), t, order_id).run();
      }

      return { ok: true, result_id, status: 'completed' };
    }

    // 还有人在做
    if (complete_job) {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='awaiting_close', updated_at=? WHERE id=?"
      ).bind(t, job_id).run();
    }
    return { ok: true, result_id, status: 'pending' };
  });
});

// =====================================================
// INBOUND PLANS — Collab side
// =====================================================
route("v2_inbound_plan_create", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  // 业务类型多选：必须至少 1 项合法 (direct_ship/bulk/return)
  const bizNorm = normalizeInboundBizClasses(body);
  if (bizNorm.list.length === 0) {
    return err("biz_classes 至少选择一个业务类型（代发/大货/退件）/ 업무 유형을 1개 이상 선택하세요");
  }
  return withIdem(env, body, "v2_inbound_plan_create", async () => {
    const id = "IB-" + uid();
    const t = now();
    const plan_date = String(body.plan_date || kstToday());
    const customer = String(body.customer || "");
    const biz_class = bizNorm.primary; // 兼容旧字段，存第一个
    const biz_classes_json = JSON.stringify(bizNorm.list);
    const created_by = String(body.created_by || "");
    const display_no = await nextDisplayNo(env, plan_date);

    await env.DB.prepare(`
      INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, biz_classes_json, cargo_summary,
        expected_arrival, purpose, remark, status, created_by, created_at, updated_at, display_no)
      VALUES(?,?,?,?,?,?,?,?,?,'pending',?,?,?,?)
    `).bind(
      id, plan_date,
      customer, biz_class, biz_classes_json,
      String(body.cargo_summary || ""),
      normalizeDateOnly(body.expected_arrival),
      String(body.purpose || ""),
      String(body.remark || ""),
      created_by, t, t, display_no
    ).run();

    // 为每个业务类型创建一条 biz_task（pending）
    for (const biz of bizNorm.list) {
      const taskId = "IBT-" + uid();
      await env.DB.prepare(`
        INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
          (id, plan_id, biz_class, job_type, status, created_at, updated_at)
        VALUES(?,?,?,?, 'pending', ?, ?)
      `).bind(taskId, id, biz, mapInboundBizToJobType(biz), t, t).run();
    }

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

// ===================================================================
// 入库计划「业务类型多选 + 多类型完成判定」helpers
// 一张计划可同时含 direct_ship/bulk/return 多个业务，每个业务一条 task。
// 整张计划只有所有 task 都 completed 才能视为完成；部分完成进入 partially_completed。
// ===================================================================

// 入库业务分类 — 注意：本 change_order 是【入库换单】（biz_class=change_order，
// 对应 job_type=inbound_change_order），与按单/出库已有的 job_type=change_order
// （换单操作）是不同概念，禁止互相复用。
const INBOUND_BIZ_VALID = ['direct_ship', 'bulk', 'return', 'change_order'];
const INBOUND_BIZ_TO_JOB_TYPE = {
  direct_ship: 'inbound_direct',
  bulk: 'inbound_bulk',
  return: 'inbound_return',
  change_order: 'inbound_change_order'
};
const INBOUND_JOB_TYPE_TO_BIZ = {
  inbound_direct: 'direct_ship',
  inbound_bulk: 'bulk',
  inbound_return: 'return',
  inbound_change_order: 'change_order'
};
function mapInboundBizToJobType(biz) { return INBOUND_BIZ_TO_JOB_TYPE[biz] || ''; }
function mapInboundJobTypeToBiz(jt) { return INBOUND_JOB_TYPE_TO_BIZ[jt] || ''; }

// 从请求体中规整出 biz_classes 数组：优先 body.biz_classes，回退 body.biz_class
// 返回 { list, primary }，list 仅含合法 enum 且去重，primary 为第一个值
function normalizeInboundBizClasses(body) {
  let raw = body && body.biz_classes;
  let arr = [];
  if (Array.isArray(raw)) arr = raw.slice();
  else if (typeof raw === 'string' && raw) arr = raw.split(',');
  if (arr.length === 0 && body && body.biz_class) arr = [String(body.biz_class)];
  const seen = {};
  const list = [];
  for (let i = 0; i < arr.length; i++) {
    const v = String(arr[i] || '').trim();
    if (!v) continue;
    if (INBOUND_BIZ_VALID.indexOf(v) === -1) continue;
    if (seen[v]) continue;
    seen[v] = 1;
    list.push(v);
  }
  return { list, primary: list[0] || '' };
}

// 解析 plan 行的 biz_classes（兼容老数据）
function extractPlanBizClasses(plan) {
  if (!plan) return [];
  let arr = [];
  try {
    const raw = plan.biz_classes_json;
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) arr = parsed.filter(Boolean).map(String);
    }
  } catch (e) { /* ignore */ }
  if (arr.length === 0 && plan.biz_class) {
    const v = String(plan.biz_class);
    if (INBOUND_BIZ_VALID.indexOf(v) !== -1) arr = [v];
  }
  // 去重 + 仅保留合法值
  const seen = {};
  const out = [];
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];
    if (INBOUND_BIZ_VALID.indexOf(v) === -1) continue;
    if (seen[v]) continue;
    seen[v] = 1;
    out.push(v);
  }
  return out;
}

// 懒加载创建 biz_tasks：plan 第一次被读到 / 操作时确保 task 行齐全
// 旧已 completed 的计划：自动把 task 标 completed，避免显示"未完成"
async function ensureInboundPlanBizTasks(env, plan) {
  if (!plan || !plan.id) return [];
  // return_session 不是协同中心口径，不生成 biz_task
  if (plan.source_type === 'return_session') return [];
  const list = extractPlanBizClasses(plan);
  if (list.length === 0) return [];
  const existing = await env.DB.prepare(
    "SELECT biz_class FROM v2_inbound_plan_biz_tasks WHERE plan_id=?"
  ).bind(plan.id).all();
  const has = {};
  for (const r of (existing.results || [])) has[r.biz_class] = 1;
  const missing = list.filter(b => !has[b]);
  if (missing.length === 0) return list;
  const t = now();
  const planCompleted = (plan.status === 'completed');
  for (const biz of missing) {
    const id = "IBT-" + uid();
    const jt = mapInboundBizToJobType(biz);
    if (planCompleted) {
      await env.DB.prepare(`
        INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
          (id, plan_id, biz_class, job_type, status, completed_at, completed_by, created_at, updated_at)
        VALUES(?,?,?,?, 'completed', ?, ?, ?, ?)
      `).bind(id, plan.id, biz, jt,
              plan.manual_completed_at || plan.updated_at || t,
              plan.manual_completed_by || '(legacy)',
              t, t).run();
    } else {
      await env.DB.prepare(`
        INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
          (id, plan_id, biz_class, job_type, status, created_at, updated_at)
        VALUES(?,?,?,?, 'pending', ?, ?)
      `).bind(id, plan.id, biz, jt, t, t).run();
    }
  }
  return list;
}

// 拉取 plan 的所有 biz_task（建议先 ensure）
async function listInboundPlanBizTasks(env, plan_id) {
  const rs = await env.DB.prepare(
    "SELECT * FROM v2_inbound_plan_biz_tasks WHERE plan_id=? ORDER BY biz_class"
  ).bind(plan_id).all();
  return rs.results || [];
}

// 把某个业务类型的 task 标完成（idempotent — 已 completed 不重复写）
async function markInboundBizTaskCompleted(env, plan_id, biz_class, payload) {
  if (!plan_id || !biz_class) return;
  const t = now();
  const row = await env.DB.prepare(
    "SELECT id, status FROM v2_inbound_plan_biz_tasks WHERE plan_id=? AND biz_class=?"
  ).bind(plan_id, biz_class).first();
  if (!row) {
    // 计划当时没生成 task（极端兼容）：直接补一条 completed
    await env.DB.prepare(`
      INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
        (id, plan_id, biz_class, job_type, status, job_id, started_at, completed_at, completed_by, worker_names, total_minutes, created_at, updated_at)
      VALUES(?,?,?,?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      "IBT-" + uid(), plan_id, biz_class, mapInboundBizToJobType(biz_class),
      String((payload && payload.job_id) || ''),
      String((payload && payload.started_at) || t),
      t, String((payload && payload.completed_by) || ''),
      String((payload && payload.worker_names) || ''),
      Number((payload && payload.total_minutes) || 0),
      t, t
    ).run();
    return;
  }
  if (row.status === 'completed') return;
  await env.DB.prepare(`
    UPDATE v2_inbound_plan_biz_tasks
       SET status='completed', job_id=?, started_at=COALESCE(NULLIF(started_at,''), ?),
           completed_at=?, completed_by=?, worker_names=?, total_minutes=?, updated_at=?
     WHERE id=?
  `).bind(
    String((payload && payload.job_id) || ''),
    String((payload && payload.started_at) || t),
    t,
    String((payload && payload.completed_by) || ''),
    String((payload && payload.worker_names) || ''),
    Number((payload && payload.total_minutes) || 0),
    t,
    row.id
  ).run();
}

// 重新计算计划总状态：综合 (a) 卸货是否还在 (b) biz_task 完成度 (c) 已有的 putaway 口径
// 仅在 (1) 业务 task 模型存在 且 (2) 卸货已结束 的情况下，由 task 模型决定 completed/partially_completed
// 卸货中或无 biz_task 的老数据，沿用旧的 status 推进规则（由调用方传入 fallbackStatus）
async function recalcInboundPlanCompletion(env, plan_id, t, opts) {
  const ts = t || now();
  const plan = await env.DB.prepare(
    "SELECT id, status, source_type FROM v2_inbound_plans WHERE id=?"
  ).bind(plan_id).first();
  if (!plan) return null;
  if (plan.status === 'cancelled') return plan.status;
  if (plan.source_type === 'return_session') return plan.status;

  const activeUnload = await env.DB.prepare(
    "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status IN ('pending','working') LIMIT 1"
  ).bind(plan_id).first();
  const otherInbound = await env.DB.prepare(
    "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type IN ('inbound_direct','inbound_bulk','inbound_return','inbound_change_order') AND status IN ('pending','working') LIMIT 1"
  ).bind(plan_id).first();

  // 卸货还在 → 不进入完成态。若仍有理货并行 → unloading_putting_away；否则 unloading
  if (activeUnload) {
    const next = otherInbound ? 'unloading_putting_away' : 'unloading';
    if (plan.status !== next) {
      await env.DB.prepare(
        "UPDATE v2_inbound_plans SET status=?, updated_at=? WHERE id=?"
      ).bind(next, ts, plan_id).run();
    }
    return next;
  }

  const tasks = await listInboundPlanBizTasks(env, plan_id);
  if (tasks.length > 0) {
    const completedCnt = tasks.filter(x => x.status === 'completed').length;
    const allCompleted = (completedCnt === tasks.length);
    const someCompleted = (completedCnt > 0);
    let next;
    if (allCompleted) {
      // 所有 biz 已完成 → 整单 completed
      next = 'completed';
    } else if (someCompleted) {
      // 至少一个完成、还有未完成 → 部分入库完成
      next = 'partially_completed';
    } else if (otherInbound) {
      next = 'putting_away';
    } else {
      next = 'arrived_pending_putaway';
    }
    if (plan.status !== next) {
      await env.DB.prepare(
        "UPDATE v2_inbound_plans SET status=?, updated_at=? WHERE id=?"
      ).bind(next, ts, plan_id).run();
    }
    return next;
  }

  // 老数据无 biz_task：沿用 putaway_qty 口径
  const completion = await checkPlanFullyCompleted(env, plan_id);
  let next;
  if (completion.allDone) next = 'completed';
  else if (otherInbound) next = 'putting_away';
  else next = 'arrived_pending_putaway';
  if (plan.status !== next) {
    await env.DB.prepare(
      "UPDATE v2_inbound_plans SET status=?, updated_at=? WHERE id=?"
    ).bind(next, ts, plan_id).run();
  }
  return next;
}

route("v2_inbound_plan_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const start = String(body.start_date || "").trim();
  const end = String(body.end_date || "").trim();
  const status = String(body.status || "").trim();
  const accounted = String(body.accounted == null ? "" : body.accounted).trim();
  // 执行系统过滤：required_biz_class 限定该业务类型仍未完成的计划（in: direct_ship/bulk/return）
  const required_biz_class = String(body.required_biz_class || "").trim();
  // 协同中心筛选：biz_class 仅按"包含该业务"过滤（含 legacy + json + biz_task）；customer_keyword 模糊搜索
  const biz_class_filter = String(body.biz_class || "").trim();
  const customer_keyword = String(body.customer_keyword || "").trim();
  // 卸货完成日期（KST 自然日 → UTC 范围 [from, toExclusive)）
  const unload_done_from = String(body.unload_done_date_from || "").trim();
  const unload_done_to = String(body.unload_done_date_to || "").trim();
  const VALID_BIZ = ['direct_ship','bulk','return'];
  const { limit, offset } = pageParams(body);

  // 排除退件入库会话：return_session 不属于正式入库计划口径
  // 软删除单：列表/导出永远排除（即便用户显式筛 cancelled 也不显示 deleted）
  let where = " WHERE source_type != 'return_session' AND COALESCE(is_deleted,0)=0";
  const binds = [];
  if (start) { where += " AND plan_date>=?"; binds.push(start); }
  if (end) { where += " AND plan_date<=?"; binds.push(end); }
  if (status) { where += " AND status=?"; binds.push(status); }
  else { where += " AND status != 'cancelled'"; } // 默认"全部状态"排除已取消，仅当显式筛 cancelled 才返回
  if (accounted === "1") { where += " AND accounted=1"; }
  else if (accounted === "0") { where += " AND (accounted IS NULL OR accounted=0)"; }
  // 卸货完成日期筛选（KST 日历日 → UTC ISO 字段 unload_completed_at 半开区间）
  if (unload_done_from) {
    const r = kstDayRangeUtc(unload_done_from);
    if (r) { where += " AND unload_completed_at >= ?"; binds.push(r.startUtc); }
  }
  if (unload_done_to) {
    const r = kstDayRangeUtc(unload_done_to);
    if (r) { where += " AND unload_completed_at < ?"; binds.push(r.endUtc); }
  }
  // 业务分类筛选：兼容 (a) biz_classes_json 包含 (b) v2_inbound_plan_biz_tasks 存在 (c) 旧数据 plan.biz_class
  if (biz_class_filter && VALID_BIZ.indexOf(biz_class_filter) !== -1) {
    where += " AND ("
        +    "biz_class=?"
        +    " OR biz_classes_json LIKE ?"
        +    " OR EXISTS (SELECT 1 FROM v2_inbound_plan_biz_tasks t WHERE t.plan_id=v2_inbound_plans.id AND t.biz_class=?)"
        +  ")";
    binds.push(biz_class_filter, '%"' + biz_class_filter + '"%', biz_class_filter);
  }
  // 客户名模糊搜索
  if (customer_keyword) {
    where += " AND customer LIKE ?";
    binds.push('%' + customer_keyword + '%');
  }
  // 注：required_biz_class 只在 001 执行端使用、不参与协同中心分页；后置过滤在 JS 中完成，
  //     COUNT 仅按 WHERE 子句口径，对协同中心场景准确，对 001 候选场景不显示 pager 不影响业务。
  const countRow = await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_inbound_plans" + where).bind(...binds).first();
  const total = Number((countRow && countRow.c) || 0);
  const listSql = "SELECT * FROM v2_inbound_plans" + where + " ORDER BY plan_date DESC, created_at DESC LIMIT ? OFFSET ?";
  const rs = await env.DB.prepare(listSql).bind(...binds, limit, offset).all();
  const rows = rs.results || [];

  if (rows.length === 0) return json({ ok: true, items: [], ...pageMeta(total, limit, offset) });

  // ===== 批量 enrichment（消除 N+1）=====
  const planIds = rows.map(p => p.id);

  // 1) biz_tasks（按 plan_id 分组）
  const taskRows = await batchSelectInGlobal(env,
    "SELECT plan_id, biz_class, status FROM v2_inbound_plan_biz_tasks WHERE plan_id IN (PLACEHOLDER) ORDER BY biz_class",
    planIds);
  const tasksByPlan = {};
  for (const r of taskRows) {
    if (!tasksByPlan[r.plan_id]) tasksByPlan[r.plan_id] = [];
    tasksByPlan[r.plan_id].push({ biz_class: r.biz_class, status: r.status });
  }

  // 2) lines summary（unit_type 维度合计）
  const lineRows = await batchSelectInGlobal(env,
    "SELECT plan_id, unit_type, SUM(planned_qty) AS qty FROM v2_inbound_plan_lines WHERE plan_id IN (PLACEHOLDER) GROUP BY plan_id, unit_type",
    planIds);
  const linesByPlan = {};
  for (const r of lineRows) {
    if (!linesByPlan[r.plan_id]) linesByPlan[r.plan_id] = {};
    linesByPlan[r.plan_id][r.unit_type || ''] = Number(r.qty || 0);
  }

  // 3) 关联出库计数
  const linkRows = await batchSelectInGlobal(env,
    "SELECT source_inbound_plan_id AS plan_id, COUNT(*) AS c FROM v2_outbound_orders WHERE source_inbound_plan_id IN (PLACEHOLDER) GROUP BY source_inbound_plan_id",
    planIds);
  const linkedObByPlan = {};
  for (const r of linkRows) linkedObByPlan[r.plan_id] = Number(r.c || 0);

  // 4) 附件计数（仅 inbound_plan）
  const attRows = await batchSelectInGlobal(env,
    "SELECT related_doc_id AS plan_id, COUNT(*) AS c FROM v2_attachments WHERE related_doc_type='inbound_plan' AND related_doc_id IN (PLACEHOLDER) GROUP BY related_doc_id",
    planIds);
  const attCountByPlan = {};
  for (const r of attRows) attCountByPlan[r.plan_id] = Number(r.c || 0);
  // 4b) 入库明细资料计数（attachment_category='inbound_material'）
  const matRows = await batchSelectInGlobal(env,
    "SELECT related_doc_id AS plan_id, COUNT(*) AS c FROM v2_attachments WHERE related_doc_type='inbound_plan' AND attachment_category='inbound_material' AND related_doc_id IN (PLACEHOLDER) GROUP BY related_doc_id",
    planIds);
  const materialCountByPlan = {};
  for (const r of matRows) materialCountByPlan[r.plan_id] = Number(r.c || 0);

  // 5) 物理卸货是否完成（同一 plan 仅一次卸货 → 任意 unload job completed = 卸货已完成）
  const unloadDoneRows = await batchSelectInGlobal(env,
    "SELECT related_doc_id AS plan_id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND job_type='unload' AND status='completed' AND related_doc_id IN (PLACEHOLDER) GROUP BY related_doc_id",
    planIds);
  const unloadDoneByPlan = {};
  for (const r of unloadDoneRows) unloadDoneByPlan[r.plan_id] = 1;

  // ===== 装配 + 后置过滤 =====
  const items = [];
  for (const p of rows) {
    const tasks = tasksByPlan[p.id] || [];
    const biz_classes = extractPlanBizClasses(p);
    const completed = tasks.filter(x => x.status === 'completed').map(x => x.biz_class);
    const pending = tasks.filter(x => x.status !== 'completed').map(x => x.biz_class);
    if (required_biz_class) {
      // 只返回包含该 biz 且 该 biz task 仍未完成 的计划
      if (biz_classes.indexOf(required_biz_class) === -1) continue;
      if (pending.indexOf(required_biz_class) === -1) continue;
    }
    // 卸货完成口径：(a) 有 completed unload job  或  (b) 整单 status 已越过卸货阶段
    const postUnloadStatuses = ['arrived_pending_putaway','putting_away','partially_completed','completed'];
    const unload_completed = unloadDoneByPlan[p.id]
      ? 1
      : (postUnloadStatuses.indexOf(p.status) !== -1 ? 1 : 0);
    items.push({
      ...p,
      biz_classes,
      biz_task_summary: tasks,
      completed_biz_classes: completed,
      pending_biz_classes: pending,
      missing_biz_classes: pending,
      unload_completed,
      line_summary: linesByPlan[p.id] || {},
      related_outbound_count: linkedObByPlan[p.id] || 0,
      attachment_count: attCountByPlan[p.id] || 0,
      inbound_material_count: materialCountByPlan[p.id] || 0
    });
  }
  return json({ ok: true, items, ...pageMeta(total, limit, offset) });
});

route("v2_inbound_plan_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const row = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
  if (!row) return err("not found", 404);
  // 退件入库会话不属于正式入库计划口径，协同中心不应打开
  if (row.source_type === 'return_session') return err("not found", 404);
  await ensureInboundPlanBizTasks(env, row);
  const biz_tasks = await listInboundPlanBizTasks(env, id);
  const biz_classes = extractPlanBizClasses(row);
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

  // biz_tasks 增补 missing_biz_classes（前端"未完成入库类型"用）
  const completed_biz_classes = biz_tasks.filter(x => x.status === 'completed').map(x => x.biz_class);
  const pending_biz_classes = biz_tasks.filter(x => x.status !== 'completed').map(x => x.biz_class);

  // 物理卸货摘要：一张计划只允许一次到仓卸货（按 plan_id 聚合 unload 类 job）
  const unloadJobs = enrichedJobs.filter(j => j.job_type === 'unload');
  const completedUnloadJobs = unloadJobs.filter(j => j.status === 'completed');
  const activeUnloadJobs = unloadJobs.filter(j => ['pending','working','awaiting_close'].indexOf(j.status) !== -1);
  let unload_status_text = 'pending'; // pending / unloading / completed
  if (completedUnloadJobs.length > 0) unload_status_text = 'completed';
  else if (activeUnloadJobs.length > 0) unload_status_text = 'unloading';
  // 取最后一次完成的卸货 job 作为代表
  const lastCompletedUnload = completedUnloadJobs.length > 0
    ? completedUnloadJobs.reduce((m, j) => (!m || (j.completed_at && j.completed_at > m.completed_at) ? j : m), null)
    : null;
  const unload_summary = {
    status: unload_status_text,
    completed: unload_status_text === 'completed',
    job_id: lastCompletedUnload ? lastCompletedUnload.id : (activeUnloadJobs[0] ? activeUnloadJobs[0].id : ''),
    worker_names: lastCompletedUnload ? (lastCompletedUnload.worker_names_text || '') : '',
    completed_at: lastCompletedUnload ? (lastCompletedUnload.completed_at || '') : '',
    total_minutes: lastCompletedUnload ? Math.round(lastCompletedUnload.total_minutes_worked || 0) : 0,
    result_lines: lastCompletedUnload ? (lastCompletedUnload.result_lines || []) : [],
    diff_note: lastCompletedUnload ? (lastCompletedUnload.diff_note || '') : ''
  };

  // P1-4：关联出库单（source_inbound_plan_id 反查）
  const linkedObRs = await env.DB.prepare(
    "SELECT id, display_no, status, customer, biz_class, outbound_mode, expected_ship_at, planned_box_count, planned_pallet_count, order_date, uses_stock_operation FROM v2_outbound_orders WHERE source_inbound_plan_id=? ORDER BY created_at ASC"
  ).bind(id).all();

  return json({
    ok: true,
    plan: { ...row, biz_classes },
    biz_tasks,
    biz_classes,
    completed_biz_classes,
    pending_biz_classes,
    missing_biz_classes: pending_biz_classes,
    unload_summary,
    lines: planLines.results || [],
    jobs: enrichedJobs,
    attachments: atts.results || [],
    inbound_materials: (atts.results || []).filter(a => a.attachment_category === 'inbound_material'),
    linked_outbound_orders: linkedObRs.results || []
  });
});

// Find inbound plan by display_no or id (for QR scan)
route("v2_inbound_plan_find_by_code", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const code = String(body.code || "").trim();
  if (!code) return err("missing code");
  // 先查"是否存在但已被软删除"——给现场一个友好提示，而不是笼统的 not_found
  const probeSel = "SELECT id, display_no, status, is_deleted FROM v2_inbound_plans WHERE (display_no=? OR id=?) ORDER BY created_at DESC LIMIT 1";
  const probe = await env.DB.prepare(probeSel).bind(code, code).first();
  if (probe && Number(probe.is_deleted || 0) === 1) {
    return json({
      ok: false,
      error: "plan_deleted",
      message: "该入库计划已删除，请联系办公室\n삭제된 입고계획입니다. 사무실에 문의하세요.",
      plan_id: probe.id,
      display_no: probe.display_no || ''
    }, 404);
  }
  // Prefer display_no, fallback to id（仍保留 cancelled 排除以兼容老语义）
  let row = await env.DB.prepare(
    "SELECT id, display_no, status, customer, cargo_summary, biz_class, biz_classes_json, source_type, manual_completed_at, manual_completed_by, updated_at FROM v2_inbound_plans WHERE display_no=? AND status!='cancelled' AND COALESCE(is_deleted,0)=0"
  ).bind(code).first();
  if (!row) {
    row = await env.DB.prepare(
      "SELECT id, display_no, status, customer, cargo_summary, biz_class, biz_classes_json, source_type, manual_completed_at, manual_completed_by, updated_at FROM v2_inbound_plans WHERE id=? AND status!='cancelled' AND COALESCE(is_deleted,0)=0"
    ).bind(code).first();
  }
  if (!row) return err("not found", 404);
  await ensureInboundPlanBizTasks(env, row);
  const tasks = await listInboundPlanBizTasks(env, row.id);
  const biz_classes = extractPlanBizClasses(row);
  return json({
    ok: true,
    plan: { ...row, biz_classes },
    biz_classes,
    biz_task_summary: tasks.map(x => ({ biz_class: x.biz_class, status: x.status })),
    pending_biz_classes: tasks.filter(x => x.status !== 'completed').map(x => x.biz_class),
    completed_biz_classes: tasks.filter(x => x.status === 'completed').map(x => x.biz_class)
  });
});

// ===== Ops candidates: filtered list for putaway/unload scene =====
route("v2_inbound_plan_ops_candidates", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const scene = String(body.scene || "").trim();
  // biz_class 旧字段保留兼容；优先用 required_biz_class（新口径，按 biz_classes_json + 未完成 biz_task 过滤）
  const biz_class = String(body.biz_class || "").trim();
  const required_biz_class = String(body.required_biz_class || biz_class).trim();
  const keyword = String(body.keyword || "").trim();
  const limit = Math.min(Number(body.limit) || 100, 200);

  let statusFilter;
  if (scene === 'putaway') {
    // partially_completed 也属于"还能继续入库"的状态
    statusFilter = "('unloading','unloading_putting_away','arrived_pending_putaway','putting_away','partially_completed')";
  } else if (scene === 'unload') {
    statusFilter = "('pending','unloading','unloading_putting_away')";
  } else {
    return err("scene must be putaway or unload");
  }

  let sql = `SELECT id, display_no, external_inbound_no, customer, cargo_summary, status, biz_class, biz_classes_json, plan_date, source_type, manual_completed_at, manual_completed_by, updated_at
    FROM v2_inbound_plans WHERE status IN ${statusFilter} AND source_type != 'return_session' AND COALESCE(is_deleted,0)=0`;
  const binds = [];
  if (keyword) {
    sql += " AND (display_no LIKE ? OR external_inbound_no LIKE ? OR customer LIKE ?)";
    const kw = "%" + keyword + "%";
    binds.push(kw, kw, kw);
  }
  sql += " ORDER BY created_at DESC LIMIT ?";
  binds.push(limit * 3); // 多取一些用于过滤后再截断

  const stmt = env.DB.prepare(sql);
  const rs = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
  const rows = rs.results || [];

  // 候选自愈：unload 场景下，若 plan 状态为 unloading/unloading_putting_away 但无
  // active unload job → 调 repair；若仍不是 unload 可继续状态则跳过
  if (scene === 'unload') {
    for (const p of rows) {
      if (p.status === 'unloading' || p.status === 'unloading_putting_away') {
        const hasActive = await env.DB.prepare(
          `SELECT id FROM v2_ops_jobs
             WHERE related_doc_type='inbound_plan' AND related_doc_id=?
               AND job_type='unload' AND status IN ('pending','working','awaiting_close') LIMIT 1`
        ).bind(p.id).first();
        if (!hasActive) {
          const r = await repairInboundPlanWorkState(env, p.id, 'candidates_auto_repair');
          if (r && r.repaired) {
            p.status = r.new_status;
            p._repair_reason = r.reason;
          }
        }
      }
    }
  }

  // 按 biz_classes_json 与 biz_task 过滤，仅返回该 biz 仍未完成的计划
  // 注意：unload 场景代表"整张计划的物理卸货"，与业务类型无关 —— 不按 biz_class 过滤
  const items = [];
  for (const p of rows) {
    // 修复后若不再属于 unload 候选状态 → 跳过
    if (scene === 'unload' && ['pending','unloading','unloading_putting_away'].indexOf(p.status) === -1) continue;
    if (scene !== 'unload' && required_biz_class) {
      await ensureInboundPlanBizTasks(env, p);
      const biz_classes = extractPlanBizClasses(p);
      if (biz_classes.indexOf(required_biz_class) === -1) {
        // 老数据兼容：如果 plan.biz_class 命中但不在 list（例如 import），跳过
        if (p.biz_class !== required_biz_class) continue;
      }
      const tasks = await listInboundPlanBizTasks(env, p.id);
      const pending = tasks.filter(x => x.status !== 'completed').map(x => x.biz_class);
      // 如果存在 task：要求 required_biz_class 仍 pending
      // 如果无 task（极旧老数据）：放行（沿用旧 biz_class= 比对）
      if (tasks.length > 0 && pending.indexOf(required_biz_class) === -1) continue;
    }
    items.push({
      ...p,
      biz_classes: extractPlanBizClasses(p)
    });
    if (items.length >= limit) break;
  }
  return json({ ok: true, items });
});

// ===== Resolve inbound code: identify system plan vs external no =====
route("v2_inbound_resolve_code", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const code = String(body.code || "").trim();
  const biz_class = String(body.biz_class || "").trim();
  if (!code) return err("missing code");

  // 先探测：是否存在但已被软删除（"误转正回滚"产物）→ 现场扫码给清晰提示
  const probeDel = await env.DB.prepare(
    "SELECT id, display_no, is_deleted FROM v2_inbound_plans WHERE (display_no=? OR external_inbound_no=? OR id=?) AND COALESCE(is_deleted,0)=1 ORDER BY created_at DESC LIMIT 1"
  ).bind(code, code, code).first();
  if (probeDel) {
    return json({ ok: true, kind: 'plan_deleted',
      message: '该入库计划已删除，请联系办公室\n삭제된 입고계획입니다. 사무실에 문의하세요.',
      plan_id: probeDel.id, display_no: probeDel.display_no || '' });
  }
  // Try to find system plan by display_no, external_inbound_no, or id
  const SEL = "SELECT id, display_no, external_inbound_no, status, customer, cargo_summary, biz_class, biz_classes_json, plan_date, source_type, manual_completed_at, manual_completed_by, updated_at FROM v2_inbound_plans";
  let plan = await env.DB.prepare(SEL + " WHERE display_no=? AND status!='cancelled' AND COALESCE(is_deleted,0)=0").bind(code).first();
  if (!plan) {
    plan = await env.DB.prepare(SEL + " WHERE external_inbound_no=? AND status!='cancelled' AND COALESCE(is_deleted,0)=0 ORDER BY created_at DESC LIMIT 1").bind(code).first();
  }
  if (!plan) {
    plan = await env.DB.prepare(SEL + " WHERE id=? AND status!='cancelled' AND COALESCE(is_deleted,0)=0").bind(code).first();
  }

  if (!plan) {
    // Not a system plan → treat as external inbound number
    return json({ ok: true, kind: 'external', code });
  }

  await ensureInboundPlanBizTasks(env, plan);
  const biz_classes = extractPlanBizClasses(plan);
  const tasks = await listInboundPlanBizTasks(env, plan.id);
  const pending = tasks.filter(x => x.status !== 'completed').map(x => x.biz_class);
  const completed = tasks.filter(x => x.status === 'completed').map(x => x.biz_class);
  const planEnriched = { ...plan, biz_classes };

  // Found system plan — check biz_class match
  if (biz_class) {
    const inList = biz_classes.indexOf(biz_class) !== -1;
    const legacyMatch = (plan.biz_class === biz_class);
    if (!inList && !legacyMatch) {
      const have = biz_classes.length ? biz_classes.join('/') : (plan.biz_class || '');
      return json({ ok: true, kind: 'biz_mismatch', plan: planEnriched, message: "该入库单不属于当前业务（" + have + "），不能在此页面开始入库" });
    }
    // 该 biz 已完成 → 不允许重复操作
    if (tasks.length > 0 && completed.indexOf(biz_class) !== -1) {
      return json({ ok: true, kind: 'biz_already_completed', plan: planEnriched, message: "该入库单的此业务类型已完成入库" });
    }
  }

  // Check status is putaway-able（partially_completed 也允许继续，因为还有未完成的 biz）
  const putawayStatuses = ['unloading', 'unloading_putting_away', 'arrived_pending_putaway', 'putting_away', 'partially_completed'];
  if (putawayStatuses.indexOf(plan.status) === -1) {
    return json({ ok: true, kind: 'status_not_allowed', plan: planEnriched, message: "该系统入库单当前状态（" + plan.status + "）不可开始入库" });
  }

  return json({
    ok: true,
    kind: 'system',
    plan: planEnriched,
    biz_classes,
    pending_biz_classes: pending,
    completed_biz_classes: completed
  });
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
    "SELECT * FROM v2_inbound_plans WHERE plan_date>=? AND plan_date<=? AND status NOT IN ('completed','cancelled','deleted') AND source_type != 'return_session' AND COALESCE(is_deleted,0)=0 ORDER BY plan_date ASC, created_at ASC"
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
  // 禁止通过此接口直接设 completed —— 必须走 v2_inbound_mark_completed（含 biz_task 校验）
  if (status === 'completed') return err("请使用 v2_inbound_mark_completed 完成入库计划（需所有业务类型已完成）");
  await env.DB.prepare(
    "UPDATE v2_inbound_plans SET status=?, updated_at=? WHERE id=?"
  ).bind(status, now(), id).run();
  return json({ ok: true });
});

// P1-7：未到库入库单 — 修改
// 仅 status=pending（未到库）允许修改；已开工/完成的拒
route("v2_inbound_plan_update", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_inbound_plan_update", async () => {
    const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
    if (!plan) return { ok: false, error: "not_found" };
    if (plan.status !== 'pending') {
      return { ok: false, error: "cannot_edit_after_started", message: "已开工/完成的入库单不能修改：" + plan.status };
    }
    // 关联的已 active/completed inbound job → 拒（兜底）
    const anyJob = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND status IN ('working','pending','awaiting_close','completed') LIMIT 1"
    ).bind(id).first();
    if (anyJob) {
      return { ok: false, error: "has_jobs_cannot_edit", message: "该入库单已有现场作业关联，不能修改" };
    }

    const t = now();
    const bizNorm = normalizeInboundBizClasses(body);
    if (bizNorm.list.length === 0) {
      return { ok: false, error: "biz_classes_required", message: "请至少选择一个业务类型" };
    }
    const biz_class = bizNorm.primary;
    const biz_classes_json = JSON.stringify(bizNorm.list);

    await env.DB.prepare(
      `UPDATE v2_inbound_plans SET plan_date=?, customer=?, biz_class=?, biz_classes_json=?,
        cargo_summary=?, expected_arrival=?, purpose=?, remark=?, updated_at=? WHERE id=?`
    ).bind(
      String(body.plan_date || plan.plan_date),
      String(body.customer || plan.customer),
      biz_class, biz_classes_json,
      String(body.cargo_summary != null ? body.cargo_summary : (plan.cargo_summary || "")),
      normalizeDateOnly(body.expected_arrival != null ? body.expected_arrival : (plan.expected_arrival || "")),
      String(body.purpose != null ? body.purpose : (plan.purpose || "")),
      String(body.remark != null ? body.remark : (plan.remark || "")),
      t, id
    ).run();

    // biz_tasks 同步：pending 可增删；completed 不允许删
    const existing = await env.DB.prepare(
      "SELECT id, biz_class, status FROM v2_inbound_plan_biz_tasks WHERE plan_id=?"
    ).bind(id).all();
    const existSet = {};
    for (const r of (existing.results || [])) existSet[r.biz_class] = r;
    const targetSet = {};
    for (const b of bizNorm.list) targetSet[b] = true;
    // 删除：existing 中 target 没有，且 status=pending → 删
    for (const biz of Object.keys(existSet)) {
      const er = existSet[biz];
      if (!targetSet[biz]) {
        if (er.status === 'completed') {
          return { ok: false, error: "completed_biz_cannot_remove", message: "业务类型 " + biz + " 已有完成记录，不能移除" };
        }
        await env.DB.prepare("DELETE FROM v2_inbound_plan_biz_tasks WHERE id=?").bind(er.id).run();
      }
    }
    // 新增：target 中 existing 没有 → INSERT pending
    for (const biz of bizNorm.list) {
      if (!existSet[biz]) {
        await env.DB.prepare(
          `INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks (id, plan_id, biz_class, job_type, status, created_at, updated_at)
           VALUES(?,?,?,?, 'pending', ?, ?)`
        ).bind("IBT-" + uid(), id, biz, mapInboundBizToJobType(biz), t, t).run();
      }
    }

    // lines 全量替换（如果传了）
    if (Array.isArray(body.lines)) {
      await env.DB.prepare("DELETE FROM v2_inbound_plan_lines WHERE plan_id=?").bind(id).run();
      for (let i = 0; i < body.lines.length; i++) {
        const ln = body.lines[i];
        await env.DB.prepare(
          `INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, remark)
           VALUES(?,?,?,?,?,?)`
        ).bind("IPL-" + uid(), id, i + 1, String(ln.unit_type || ""), Number(ln.planned_qty || 0), String(ln.remark || "")).run();
      }
    }

    return { ok: true, id, biz_classes: bizNorm.list };
  });
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

// ===== 入库计划：删除（仅 cancelled，且无任何 ops 历史） =====
route("v2_inbound_plan_delete", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || body.inbound_plan_id || "").trim();
  if (!id) return err("missing id");

  return withIdem(env, body, "v2_inbound_plan_delete", async () => {
    const plan = await env.DB.prepare("SELECT id, status FROM v2_inbound_plans WHERE id=?").bind(id).first();
    if (!plan) return { ok: false, error: "not_found" };
    if (plan.status !== 'cancelled') {
      return { ok: false, error: "only_cancelled_can_delete", message: "只能删除已取消的入库计划，请先取消" };
    }
    // 在制 / 待续作业 → 拒（按规范要求二次校验）
    const activeJob = await env.DB.prepare(
      "SELECT id, job_type FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(id).first();
    if (activeJob) {
      return { ok: false, error: "active_job_exists", message: "仍有进行中的现场任务，不能删除" };
    }
    // 已存在 completed ops 历史 → 拒，避免误删正式数据
    const histJob = await env.DB.prepare(
      "SELECT id, job_type FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND status='completed' LIMIT 1"
    ).bind(id).first();
    if (histJob) {
      return { ok: false, error: "has_ops_history_cannot_delete", message: "该入库计划存在已完成作业历史，不允许删除" };
    }

    let deleted = { lines: 0, biz_tasks: 0, attachments: 0, plan: 0 };
    const linesRs = await env.DB.prepare("DELETE FROM v2_inbound_plan_lines WHERE plan_id=?").bind(id).run();
    deleted.lines = (linesRs.meta && linesRs.meta.changes) || 0;
    const tasksRs = await env.DB.prepare("DELETE FROM v2_inbound_plan_biz_tasks WHERE plan_id=?").bind(id).run();
    deleted.biz_tasks = (tasksRs.meta && tasksRs.meta.changes) || 0;
    const attsRs = await env.DB.prepare(
      "DELETE FROM v2_attachments WHERE related_doc_type='inbound_plan' AND related_doc_id=?"
    ).bind(id).run();
    deleted.attachments = (attsRs.meta && attsRs.meta.changes) || 0;
    const planRs = await env.DB.prepare("DELETE FROM v2_inbound_plans WHERE id=?").bind(id).run();
    deleted.plan = (planRs.meta && planRs.meta.changes) || 0;

    return { ok: true, id, deleted };
  });
});

// ===== 入库计划：删除"现场反馈转正"误入库（软删除）=====
// 用途：仓库现场没识别出已有正式入库计划 → 走了"反馈 → 转正"路径产生重复入库单 → 此处回滚
// 与 v2_inbound_plan_delete 不同：
//   - 接受 ops 历史存在（保留工时不丢，仅把入库计划本身打 deleted）
//   - 不接受非 from_feedback 来源
//   - 已记帐默认拒绝（accounted_plan_cannot_delete）
//   - 联动把源 v2_field_feedbacks 标记为 cancelled+deleted（避免被再次转正）
route("v2_inbound_plan_delete_converted_feedback", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || body.inbound_plan_id || "").trim();
  const by = String(body.by || body.operator || "").trim();
  const reason = String(body.reason || "").trim();
  if (!id) return err("missing id");

  return withIdem(env, body, "v2_inbound_plan_delete_converted_feedback", async () => {
    const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
    if (!plan) return { ok: false, error: "not_found", message: "入库计划不存在" };
    if (Number(plan.is_deleted || 0) === 1) {
      return { ok: false, error: "already_deleted", message: "该入库计划已被删除" };
    }
    // 来源校验：必须是现场反馈转正
    const isFromFeedback = (plan.source_type === 'from_feedback')
                        || (plan.source_type === 'field_feedback')
                        || !!plan.source_feedback_id;
    if (!isFromFeedback) {
      return { ok: false, error: "not_from_feedback",
        message: '仅"现场反馈转正"的入库计划才能用此入口删除，请走标准取消/删除流程' };
    }
    // 已记帐拦截：默认不允许；管理员需先取消记帐
    if (Number(plan.accounted || 0) === 1) {
      return { ok: false, error: "accounted_plan_cannot_delete",
        message: "已记帐入库计划不能直接删除，请先取消记帐或由管理员处理" };
    }
    // 在制作业 → 拒（保护现场正在进行的任务）
    const activeJob = await env.DB.prepare(
      "SELECT id, job_type FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(id).first();
    if (activeJob) {
      return { ok: false, error: "active_job_exists",
        message: "仍有进行中的现场任务（" + (activeJob.job_type || '') + "），请先让现场结束/离开后再删除" };
    }

    const t = now();
    // 1) 软删除入库计划本身
    await env.DB.prepare(
      `UPDATE v2_inbound_plans
          SET is_deleted=1, deleted_at=?, deleted_by=?, delete_reason=?,
              status='deleted', updated_at=?
        WHERE id=?`
    ).bind(t, by, reason || '误转正入库单，回滚', t, id).run();

    // 2) 联动源现场反馈：标记 deleted+cancelled，清理转正关联，避免被再次转正
    let feedback_updated = 0;
    const fbId = plan.source_feedback_id || '';
    if (fbId) {
      const fbRow = await env.DB.prepare("SELECT id, status FROM v2_field_feedbacks WHERE id=?").bind(fbId).first();
      if (fbRow) {
        await env.DB.prepare(
          `UPDATE v2_field_feedbacks
              SET status='cancelled', is_deleted=1, deleted_at=?, deleted_by=?,
                  delete_reason='误转正入库计划已删除' || (CASE WHEN ?='' THEN '' ELSE '：'||? END),
                  inbound_plan_id='', updated_at=?
            WHERE id=?`
        ).bind(t, by, reason, reason, t, fbId).run();
        feedback_updated = 1;
      }
    }

    return {
      ok: true,
      id,
      deleted_at: t,
      deleted_by: by,
      delete_reason: reason,
      source_feedback_id: fbId,
      feedback_updated
    };
  });
});

// ===== 出库作业单：删除（仅 cancelled，且无任何 ops 历史） =====
route("v2_outbound_order_delete", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || body.outbound_order_id || "").trim();
  if (!id) return err("missing id");

  return withIdem(env, body, "v2_outbound_order_delete", async () => {
    const ord = await env.DB.prepare("SELECT id, status FROM v2_outbound_orders WHERE id=?").bind(id).first();
    if (!ord) return { ok: false, error: "not_found" };
    if (ord.status !== 'cancelled') {
      return { ok: false, error: "only_cancelled_can_delete", message: "只能删除已取消的出库作业单，请先取消" };
    }
    // 在制 → 拒
    const activeJob = await env.DB.prepare(
      "SELECT id, job_type FROM v2_ops_jobs WHERE (related_doc_type='outbound_order' AND related_doc_id=?) OR linked_outbound_order_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(id, id).first();
    if (activeJob) {
      return { ok: false, error: "active_job_exists", message: "仍有进行中的现场任务，不能删除" };
    }
    // 已 completed 的 ops 历史 → 拒
    const histJob = await env.DB.prepare(
      "SELECT id, job_type FROM v2_ops_jobs WHERE ((related_doc_type='outbound_order' AND related_doc_id=?) OR linked_outbound_order_id=?) AND status='completed' LIMIT 1"
    ).bind(id, id).first();
    if (histJob) {
      return { ok: false, error: "has_ops_history_cannot_delete", message: "该出库作业单存在已完成作业历史，不允许删除" };
    }

    let deleted = { lines: 0, attachments: 0, order: 0 };
    const linesRs = await env.DB.prepare("DELETE FROM v2_outbound_order_lines WHERE order_id=?").bind(id).run();
    deleted.lines = (linesRs.meta && linesRs.meta.changes) || 0;
    const attsRs = await env.DB.prepare(
      "DELETE FROM v2_attachments WHERE related_doc_type='outbound_order' AND related_doc_id=?"
    ).bind(id).run();
    deleted.attachments = (attsRs.meta && attsRs.meta.changes) || 0;
    const ordRs = await env.DB.prepare("DELETE FROM v2_outbound_orders WHERE id=?").bind(id).run();
    deleted.order = (ordRs.meta && ordRs.meta.changes) || 0;

    return { ok: true, id, deleted };
  });
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
    // 兼容多业务类型：优先 biz_classes，回退 biz_class
    const bizNorm = normalizeInboundBizClasses({ biz_classes: body.biz_classes, biz_class: body.biz_class || plan.biz_class });
    const biz_class = bizNorm.primary || String(body.biz_class || plan.biz_class || "").trim();
    const biz_classes_json = bizNorm.list.length > 0 ? JSON.stringify(bizNorm.list) : (plan.biz_classes_json || '[]');
    const cargo_summary = String(body.cargo_summary || plan.cargo_summary || "").trim();
    const expected_arrival = normalizeDateOnly(body.expected_arrival || plan.expected_arrival || "");
    const purpose = String(body.purpose || plan.purpose || "").trim();
    const remark = String(body.remark || plan.remark || "").trim();

    await env.DB.prepare(`
      UPDATE v2_inbound_plans SET customer=?, biz_class=?, biz_classes_json=?, cargo_summary=?,
        expected_arrival=?, purpose=?, remark=?, status='completed',
        needs_info_update=0, updated_at=? WHERE id=?
    `).bind(customer, biz_class, biz_classes_json, cargo_summary, expected_arrival, purpose, remark, t, id).run();

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

    // 同步 biz_task：写完毕（与 status='completed' 对齐）
    const updatedPlan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
    await ensureInboundPlanBizTasks(env, updatedPlan);
    for (const biz of extractPlanBizClasses(updatedPlan)) {
      await markInboundBizTaskCompleted(env, id, biz, {
        completed_by: '(legacy_dynamic_finalize)'
      });
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
  // 新建批次时携带的可选信息（同一时段允许多批同时存在）
  const cargo_summary = String(body.cargo_summary || body.title || "").trim();
  const customer = String(body.customer || "").trim();
  const vehicle_info = String(body.vehicle_info || "").trim();
  const driver_info = String(body.driver_info || "").trim();
  const source_info = String(body.source_info || "").trim();
  const remark_in = String(body.remark || "").trim();
  if (!worker_id) return err("missing worker_id");

  return withIdem(env, body, "v2_unplanned_unload_start", async () => {
    // 仅检查当前 worker 是否在其它任务（与 parent_job_id 兼容）；
    // 允许同时存在多批不同客户/不同车辆的计划外卸货 — 不再因"已有 active unplanned_unload"而阻断
    if (!parent_job_id) {
      const busy = await checkWorkerBusy(env, worker_id, null);
      if (busy) return { ok: false, error: "worker_has_active_job", active_job_id: busy.job_id, active_job_type: busy.job_type };
    }

    const t = now();
    const fb_id = "FB-" + uid();
    const fb_display_no = await nextFeedbackDisplayNo(env, kstToday(), 'XCXH');

    // 标题：优先使用现场录入的"货物说明"，否则给默认占位
    const fb_title = cargo_summary || "计划外到货-现场卸货中";
    // content：拼接 客户/车辆/司机/来源 多行结构（便于详情/列表展示）
    const contentParts = [];
    if (customer) contentParts.push("客户/고객: " + customer);
    if (vehicle_info) contentParts.push("车辆/차량: " + vehicle_info);
    if (driver_info) contentParts.push("司机/기사: " + driver_info);
    if (source_info) contentParts.push("来源/출처: " + source_info);
    if (contentParts.length === 0) contentParts.push("现场操作人员发起计划外卸货");
    const fb_content = contentParts.join("\n");

    await env.DB.prepare(`
      INSERT INTO v2_field_feedbacks(id, feedback_type, related_doc_type, related_doc_id,
        title, content, submitted_by, status, parent_job_id, interrupt_type, display_no, remark, created_at, updated_at)
      VALUES(?,'unplanned_unload','ops_job','',?,?,?,'field_working',?,?,?,?,?,?)
    `).bind(fb_id,
        fb_title,
        fb_content,
        worker_name || worker_id,
        parent_job_id, interrupt_type, fb_display_no, remark_in, t, t).run();

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
        const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
        // feedback 兜底 — 如果首次完成时未推进 feedback 状态，已完成 job 的 feedback 也强制推进
        const _job = await env.DB.prepare("SELECT related_doc_type, related_doc_id FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
        if (_job && _job.related_doc_type === 'field_feedback' && _job.related_doc_id) {
          await env.DB.prepare(
            "UPDATE v2_field_feedbacks SET status='unloaded_pending_info', updated_at=? WHERE id=? AND status='field_working'"
          ).bind(t, _job.related_doc_id).run();
        }
        return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" };
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

    // 防御性收口 — 关闭所有遗留 open segment（多人卸货必须确保所有人 left_at 写入）
    await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');

    const sharedResult = JSON.stringify({ result_lines, diff_note, remark });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', shared_result_json=?, active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(sharedResult, t, job_id).run();

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
// 返回所有进行中的批次（不只是第一条）；含 cargo/customer/remark 供前端列表展示
route("v2_unplanned_unload_active_list", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const fbs = await env.DB.prepare(
    "SELECT * FROM v2_field_feedbacks WHERE feedback_type='unplanned_unload' AND status='field_working' ORDER BY created_at DESC LIMIT 100"
  ).all();
  const items = [];
  const _now = now();
  for (const fb of (fbs.results || [])) {
    const jobId = fb.related_doc_id || '';
    // 防御性自愈：如果反馈关联的 job 已 completed，但 feedback.status 仍 field_working
    // → 强制把 feedback 推进到 unloaded_pending_info；不再加入"当前计划外卸货中"列表
    if (jobId) {
      const jobRow = await env.DB.prepare(
        "SELECT status FROM v2_ops_jobs WHERE id=?"
      ).bind(jobId).first();
      if (jobRow && jobRow.status === 'completed') {
        // 顺手关掉残留 open segment 并推进 feedback
        await closeOpenWorkerSegmentsForJob(env, jobId, _now, 'feedback_self_heal');
        await env.DB.prepare(
          "UPDATE v2_field_feedbacks SET status='unloaded_pending_info', updated_at=? WHERE id=? AND status='field_working'"
        ).bind(_now, fb.id).run();
        continue;
      }
    }
    let activeCount = 0, workerNames = [], totalWorkerCount = 0, allWorkerNames = [];
    if (jobId) {
      const ws = await env.DB.prepare(
        "SELECT worker_id, worker_name, left_at FROM v2_ops_job_workers WHERE job_id=?"
      ).bind(jobId).all();
      const rows = ws.results || [];
      const activeRows = rows.filter(r => !r.left_at);
      activeCount = activeRows.length;
      workerNames = activeRows.map(r => r.worker_name || r.worker_id);
      const distinctIds = new Set();
      const distinctNames = [];
      for (const r of rows) {
        const k = r.worker_id || r.worker_name;
        if (k && !distinctIds.has(k)) {
          distinctIds.add(k);
          distinctNames.push(r.worker_name || r.worker_id);
        }
      }
      totalWorkerCount = distinctIds.size;
      allWorkerNames = distinctNames;
    }
    // 解析 content 中的 客户/车辆/司机/来源 提示（不强约束格式，仅尽力解析）
    const contentStr = String(fb.content || '');
    const _extract = (label) => {
      const m = contentStr.match(new RegExp("^" + label + "[/／/]?[^:：]*[:：]\\s*(.+)$", "m"));
      return m ? m[1].trim() : '';
    };
    const customer = _extract("客户");
    const vehicle_info = _extract("车辆");
    const driver_info = _extract("司机");
    const source_info = _extract("来源");

    items.push({
      feedback_id: fb.id,
      display_no: fb.display_no || fb.id,
      title: fb.title || '',
      cargo_summary: fb.title || '',
      customer,
      vehicle_info,
      driver_info,
      source_info,
      content: contentStr,
      remark: fb.remark || '',
      submitted_by: fb.submitted_by || '',
      created_by: fb.submitted_by || '',
      created_at: fb.created_at || '',
      started_at: fb.created_at || '',
      job_id: jobId,
      related_job_id: jobId,
      active_worker_count: activeCount,
      worker_count: totalWorkerCount,
      worker_names: workerNames,
      all_worker_names: allWorkerNames,
      parent_job_id: fb.parent_job_id || ''
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

    // 业务类型：兼容多选（biz_classes 数组）+ 老单值 biz_class
    const bizNorm = normalizeInboundBizClasses({ biz_classes: body.biz_classes, biz_class: body.biz_class });
    const biz_class = bizNorm.primary || String(body.biz_class || "").trim();
    const biz_classes_json = bizNorm.list.length > 0 ? JSON.stringify(bizNorm.list) : '[]';
    const cargo_summary = String(body.cargo_summary || "").trim();
    const expected_arrival = normalizeDateOnly(body.expected_arrival);
    const purpose = String(body.purpose || "").trim();
    const remark = String(body.remark || "").trim();
    const created_by = String(body.created_by || "").trim();

    await env.DB.prepare(`
      INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, biz_classes_json, cargo_summary,
        expected_arrival, purpose, remark, status, source_feedback_id, created_by, created_at, updated_at, display_no, source_type)
      VALUES(?,?,?,?,?,?,?,?,?,'arrived_pending_putaway',?,?,?,?,?,'from_feedback')
    `).bind(plan_id, plan_date, customer, biz_class, biz_classes_json, cargo_summary,
        expected_arrival, purpose, remark,
        feedback_id, created_by, t, t, display_no).run();

    // 初始化 biz_tasks（pending）
    for (const biz of bizNorm.list) {
      await env.DB.prepare(`
        INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
          (id, plan_id, biz_class, job_type, status, created_at, updated_at)
        VALUES(?,?,?,?, 'pending', ?, ?)
      `).bind("IBT-" + uid(), plan_id, biz, mapInboundBizToJobType(biz), t, t).run();
    }

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
      INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, biz_classes_json, cargo_summary,
        expected_arrival, purpose, remark, status, created_by, created_at, updated_at, display_no, source_type, needs_info_update)
      VALUES(?,?,'待补充','','[]','现场无单卸货','','','','field_working',?,?,?,?,'field_dynamic',1)
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
  if (!plan_id) return err("missing_inbound_plan", 400);

  return withIdem(env, body, "v2_unload_job_start", async () => {
    const t = now();

    const plan = await env.DB.prepare("SELECT status FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
    if (!plan) return { ok: false, error: "plan not found" };
    // 已经卸过货的状态：不允许再次创建卸货任务（除非管理员强制重开）
    const unloadDoneStatuses = ['arrived_pending_putaway', 'putting_away', 'partially_completed', 'completed'];
    const forceReopen = body.force_reopen === true;
    if (unloadDoneStatuses.indexOf(plan.status) !== -1 && !forceReopen) {
      return {
        ok: false,
        error: "unload_already_completed",
        current_status: plan.status,
        message: "该入库计划已完成卸货，请进入对应业务类型入库操作 / 이 입고계획은 하차가 완료되어 해당 업무 입고 작업으로 진입하세요"
      };
    }
    const unloadAllowed = ['pending', 'unloading', 'unloading_putting_away'];
    if (unloadAllowed.indexOf(plan.status) === -1) {
      return { ok: false, error: "unload_not_allowed_for_status", message: "当前状态不可继续卸货 / 현재 상태에서 하차 불가", current_status: plan.status };
    }

    let job = null;
    const existing = await env.DB.prepare(
      "SELECT * FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status IN ('pending','working') LIMIT 1"
    ).bind(plan_id).first();
    if (existing) job = existing;

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
      const plan2 = await env.DB.prepare("SELECT status FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
      if (plan2 && (plan2.status === 'unloading' || plan2.status === 'unloading_putting_away')) {
        // 自愈：状态为卸货中但无 active unload job — 推回到一致状态
        const repair = await repairInboundPlanWorkState(env, plan_id, 'start_unload_detected_missing_active_job');
        if (repair && repair.repaired && repair.new_status === 'pending') {
          // 修复后允许重新开始卸货：fall through 创建新 job
        } else if (repair && repair.repaired) {
          return {
            ok: false,
            error: "unload_status_repaired",
            repaired: true,
            old_status: repair.old_status,
            new_status: repair.new_status,
            reason: repair.reason,
            message: ({
              arrived_pending_putaway: '系统已修复：卸货已完成，等待理货 / 하차 완료, 입고 정리 대기',
              putting_away: '系统已修复：理货进行中，无需重新卸货 / 입고 정리 진행 중',
              completed: '系统已修复：该入库单已完成 / 입고가 완료되었습니다'
            }[repair.new_status]) || '系统已自动修复该入库单状态，请重新选择 / 상태를 자동 복구했습니다. 다시 선택해주세요'
          };
        } else {
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
      await env.DB.prepare(
        "UPDATE v2_inbound_plans SET status='unloading', updated_at=? WHERE id=? AND status='pending'"
      ).bind(t, plan_id).run();
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
      const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
      return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" };
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

    // 4f. Complete job — 先关闭所有遗留 open segment，再标记完成
    await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');
    const sharedResult = JSON.stringify({ box_count, pallet_count, remark, result_lines, diff_note, has_diff: hasDiff });
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', shared_result_json=?, active_worker_count=0, updated_at=? WHERE id=?"
    ).bind(sharedResult, t, job_id).run();

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
        // 动态计划：同样写入 unload_completed_at / unload_completed_by，方便协同中心按卸货完成日期搜索
        const _wkRows = await env.DB.prepare(
          "SELECT DISTINCT worker_name FROM v2_ops_job_workers WHERE job_id=? AND worker_name != ''"
        ).bind(job_id).all();
        const _names = (_wkRows.results || []).map(w => w.worker_name).filter(Boolean);
        const _namesText = _names.length ? _names.join(", ") : (worker_id || "");
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='unloaded_pending_info', cargo_summary=?, unload_completed_at=COALESCE(NULLIF(unload_completed_at,''), ?), unload_completed_by=COALESCE(NULLIF(unload_completed_by,''), ?), updated_at=? WHERE id=?"
        ).bind(cargoSummary || "现场无单卸货", t, _namesText, t, plan_id).run();
        return { ok: true, result_id, dynamic_plan: true, plan_id };
      } else {
        // 物理卸货已完成 → 冗余字段 unload_completed_at/_by 写入入库计划主表，
        // 供协同中心"按卸货完成日期搜索"的索引扫描；仅在首次写入时落地（避免重复完成覆盖）
        const planRow2 = await env.DB.prepare(
          "SELECT unload_completed_at FROM v2_inbound_plans WHERE id=?"
        ).bind(plan_id).first();
        if (!planRow2 || !planRow2.unload_completed_at) {
          const workerRows = await env.DB.prepare(
            "SELECT DISTINCT worker_name FROM v2_ops_job_workers WHERE job_id=? AND worker_name != ''"
          ).bind(job_id).all();
          const names = (workerRows.results || []).map(w => w.worker_name).filter(Boolean);
          const namesText = names.length ? names.join(", ") : (worker_id || "");
          await env.DB.prepare(
            "UPDATE v2_inbound_plans SET unload_completed_at=?, unload_completed_by=?, updated_at=? WHERE id=?"
          ).bind(t, namesText, t, plan_id).run();
        }
        // 物理卸货已完成 → 由 recalcInboundPlanCompletion 综合 biz_task 完成度 + active 入库任务
        // 推断整单状态：completed / partially_completed / putting_away / arrived_pending_putaway
        // 不再按业务类型分别要求卸货 —— 一次卸货代表整张 plan 的物理卸货完成
        await ensureInboundPlanBizTasks(env, await env.DB.prepare(
          "SELECT id, status, biz_class, biz_classes_json, source_type FROM v2_inbound_plans WHERE id=?"
        ).bind(plan_id).first());
        await recalcInboundPlanCompletion(env, plan_id, t);
      }
    }

    if (!plan_id || plan_id === "") {
      // 兜底：同一个 job 只允许一条 unload_no_doc FB，防止异常路径下重复 INSERT
      const existingFb = await env.DB.prepare(
        "SELECT id FROM v2_field_feedbacks WHERE related_doc_type='ops_job' AND related_doc_id=? AND feedback_type='unload_no_doc' LIMIT 1"
      ).bind(job_id).first();
      if (existingFb) {
        return { ok: true, result_id, feedback_id: existingFb.id, no_doc: true, fb_reused: true };
      }
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

  const VALID_JOB_TYPES = ['inbound_direct', 'inbound_bulk', 'inbound_return', 'inbound_change_order'];
  if (VALID_JOB_TYPES.indexOf(job_type) === -1) return err("invalid job_type: " + job_type);
  const isReturn = (job_type === 'inbound_return');
  // 入库换单（biz_class=change_order）走标准计划路径，与代发/大货同流程
  const isStandard = (job_type === 'inbound_direct' || job_type === 'inbound_bulk' || job_type === 'inbound_change_order');

  if (isReturn && biz_class && biz_class !== 'return') {
    return err("biz_class mismatch for inbound_return: " + biz_class);
  }
  if (isStandard) {
    const expectedBiz = mapInboundJobTypeToBiz(job_type);
    if (biz_class !== expectedBiz) {
      return err("biz_class must be " + expectedBiz + " for " + job_type + ", got: " + biz_class);
    }
  }

  return withIdem(env, body, "v2_inbound_job_start", async () => {
    let plan_id = String(body.plan_id || "").trim();
    const t = now();
    const today = kstToday();

    // ===== Path A: start from existing system inbound plan (standard only) =====
    if (plan_id && isStandard) {
      const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
      if (!plan) return { ok: false, error: "plan not found" };
      const inboundStartAllowed = ['unloading', 'unloading_putting_away', 'arrived_pending_putaway', 'putting_away', 'partially_completed'];
      if (inboundStartAllowed.indexOf(plan.status) === -1) {
        return { ok: false, error: "plan_status_invalid", message: "当前状态不可开始理货 / 현재 상태에서 입고 불가, current: " + plan.status };
      }
      // 业务类型校验：当前 biz 必须在计划的 biz_classes_json 内（或老数据 biz_class 单值匹配）
      const biz_classes = extractPlanBizClasses(plan);
      const inList = biz_classes.indexOf(biz_class) !== -1;
      const legacyMatch = (plan.biz_class === biz_class);
      if (!inList && !legacyMatch) {
        const have = biz_classes.length ? biz_classes.join('/') : (plan.biz_class || '');
        return { ok: false, error: "biz_class_mismatch", message: "plan biz_classes mismatch: plan=" + have + " req=" + biz_class };
      }
      // 该 biz 是否已完成 → 拒绝
      await ensureInboundPlanBizTasks(env, plan);
      const taskRow = await env.DB.prepare(
        "SELECT status FROM v2_inbound_plan_biz_tasks WHERE plan_id=? AND biz_class=?"
      ).bind(plan_id, biz_class).first();
      if (taskRow && taskRow.status === 'completed') {
        return { ok: false, error: "biz_task_already_completed", message: "该入库计划的此业务类型已完成入库" };
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
        // 复用计划：补 biz_task（若该 biz 缺）
        const planRow = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
        if (planRow) {
          await ensureInboundPlanBizTasks(env, planRow);
          const tt = await env.DB.prepare(
            "SELECT status FROM v2_inbound_plan_biz_tasks WHERE plan_id=? AND biz_class=?"
          ).bind(plan_id, biz_class).first();
          if (!tt) {
            await env.DB.prepare(`
              INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
                (id, plan_id, biz_class, job_type, status, created_at, updated_at)
              VALUES(?,?,?,?, 'pending', ?, ?)
            `).bind("IBT-" + uid(), plan_id, biz_class, mapInboundBizToJobType(biz_class), t, t).run();
          } else if (tt.status === 'completed') {
            return { ok: false, error: "biz_task_already_completed", message: "该入库单的此业务类型已完成入库" };
          }
        }
      } else {
        plan_id = "IB-" + uid();
        const display_no = await nextDisplayNo(env, today);
        const biz_classes_json = JSON.stringify([biz_class]);
        await env.DB.prepare(`
          INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, biz_classes_json, cargo_summary,
            expected_arrival, purpose, remark, status, created_by, created_at, updated_at,
            display_no, source_type, external_inbound_no)
          VALUES(?,?,?,?,?,'外部WMS入库单','','',?, 'putting_away',?,?,?,?,'external_inbound',?)
        `).bind(plan_id, today, customer_name, biz_class, biz_classes_json, start_remark, worker_id, t, t, display_no, external_inbound_no).run();
        await env.DB.prepare(`
          INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
            (id, plan_id, biz_class, job_type, status, created_at, updated_at)
          VALUES(?,?,?,?, 'pending', ?, ?)
        `).bind("IBT-" + uid(), plan_id, biz_class, job_type, t, t).run();
      }
    }
    // ===== Path C: return inbound lightweight session =====
    else if (isReturn) {
      if (!plan_id) {
        plan_id = "IB-" + uid();
        const display_no = await nextDisplayNo(env, today);
        const biz_classes_json = JSON.stringify(['return']);
        await env.DB.prepare(`
          INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, biz_classes_json, cargo_summary,
            expected_arrival, purpose, remark, status, created_by, created_at, updated_at,
            display_no, source_type)
          VALUES(?,?,?,'return',?,'退件入库会话','','',?, 'putting_away',?,?,?,?,'return_session')
        `).bind(plan_id, today, customer_name || '未指定', biz_classes_json, start_remark, worker_id, t, t, display_no).run();
        // return_session 不在协同中心列表口径，不强制 biz_task；但补一条便于完成时统一收口
        await env.DB.prepare(`
          INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
            (id, plan_id, biz_class, job_type, status, created_at, updated_at)
          VALUES(?,?,?,?, 'pending', ?, ?)
        `).bind("IBT-" + uid(), plan_id, 'return', 'inbound_return', t, t).run();
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
        const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
        return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" };
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
        // partially_completed 也允许：还有别的 biz 未完成，但本 biz 自己可以完成
        const inboundFinishAllowed = ['putting_away', 'partially_completed'];
        if (planCheck && inboundFinishAllowed.indexOf(planCheck.status) === -1) {
          return { ok: false, error: "inbound_plan_status_invalid", message: "当前入库计划状态不允许完成入库（当前: " + planCheck.status + "）" };
        }
      }
    }

    await closeAllOpenSegs(env, job_id, worker_id, t, leave_only ? 'leave' : 'finished');
    const realCount = await recalcActiveCount(env, job_id, t);

    if (leave_only) {
      // 退件入库等仅工时型：最后一人离开就 completed，不再残留 awaiting_close
      if (realCount <= 0) await autoCloseJobIfNoOpenWorkers(env, job_id, t);
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

      // 防御性收口：先关闭所有遗留 open segment，再标记完成
      await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
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

        // 标记该业务类型的 biz_task 完成
        const biz_for_task = mapInboundJobTypeToBiz(jobRow.job_type) || jobRow.biz_class || '';
        if (biz_for_task) {
          // 汇总参与人员 + 总分钟，写入 biz_task
          const wkRs = await env.DB.prepare(
            "SELECT worker_name, minutes_worked, joined_at FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at"
          ).bind(job_id).all();
          const wkRows = wkRs.results || [];
          const names = [...new Set(wkRows.map(w => w.worker_name).filter(Boolean))].join('、');
          const total_min = Math.round(wkRows.reduce((s, w) => s + (Number(w.minutes_worked) || 0), 0));
          const startedAt = wkRows.reduce((m, w) => (!m || (w.joined_at && w.joined_at < m)) ? w.joined_at : m, '');
          await markInboundBizTaskCompleted(env, pid, biz_for_task, {
            job_id,
            started_at: startedAt || jobRow.created_at || t,
            completed_by: worker_id,
            worker_names: names,
            total_minutes: total_min
          });
        }

        // 重新计算计划总状态
        await recalcInboundPlanCompletion(env, pid, t);
      }
      if (isReturnJob && jobRow.related_doc_id) {
        // return_session：保留原有"完成即整单 completed"语义
        await env.DB.prepare(
          "UPDATE v2_inbound_plans SET status='completed', updated_at=? WHERE id=? AND source_type='return_session'"
        ).bind(t, jobRow.related_doc_id).run();
        // 同步 biz_task（若存在）
        await markInboundBizTaskCompleted(env, jobRow.related_doc_id, 'return', {
          job_id,
          started_at: jobRow.created_at || t,
          completed_by: worker_id
        });
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
        const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
        return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" };
      }
    }

    await closeAllOpenSegs(env, job_id, worker_id, t, leave_only ? 'leave' : 'finished');
    const realCount = await recalcActiveCount(env, job_id, t);

    if (leave_only) {
      if (realCount <= 0) await autoCloseJobIfNoOpenWorkers(env, job_id, t);
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

      // 防御性收口：先关闭所有遗留 open segment，再标记完成
      await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
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

    const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(plan_id).first();
    if (!plan) return { ok: false, error: "plan not found" };
    const markCompletedAllowed = ['arrived_pending_putaway', 'putting_away', 'partially_completed'];
    if (markCompletedAllowed.indexOf(plan.status) === -1) {
      return { ok: false, error: "status_invalid", message: "only arrived_pending_putaway/putting_away/partially_completed can be marked completed, current: " + plan.status };
    }

    const activeJob = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type LIKE 'inbound%' AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(plan_id).first();
    if (activeJob) {
      return { ok: false, error: "inbound_job_still_active", message: "当前仍有进行中的入库任务，不能直接完结" };
    }

    // 多业务类型限制：仍有未完成的 biz_task → 拒绝
    await ensureInboundPlanBizTasks(env, plan);
    const tasks = await listInboundPlanBizTasks(env, plan_id);
    const pending = tasks.filter(x => x.status !== 'completed').map(x => x.biz_class);
    if (pending.length > 0) {
      const biz_label_zh = { direct_ship: '代发入库', bulk: '大货入库', return: '退件入库', change_order: '换单入库' };
      const biz_label_ko = { direct_ship: '직배송 입고', bulk: '대량화물 입고', return: '반품 입고', change_order: '송장교체 입고' };
      const zhList = pending.map(b => biz_label_zh[b] || b).join('、');
      const koList = pending.map(b => biz_label_ko[b] || b).join(', ');
      return {
        ok: false,
        error: "biz_tasks_pending",
        pending_biz_classes: pending,
        message: "该入库计划还有未完成的入库类型：" + zhList + "。请现场完成对应入库操作后再完成整单。\n해당 입고 계획에 미완료 입고 유형이 있습니다: " + koList + ". 현장에서 해당 작업을 완료한 후 전체 입고 완료 처리하세요."
      };
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
// 入库计划：手动强制完成（外部 WMS 已完结 / 仅登记单据；不生成现场工时）
//   - 允许：pending / arrived_pending_putaway / putting_away / partially_completed
//   - 禁止：completed / cancelled / 任意 active inbound job 存在的计划
//   - 自动把所有未完成 biz_task 标 completed（completion_source='manual_force'）
// =====================================================
route("v2_inbound_plan_force_complete", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  const reason = String(body.reason || "").trim();
  if (!reason) return err("reason_required");
  const operator = String(body.operator_name || body.created_by || "").trim() || '(unknown)';
  const actual_box_count = body.actual_box_count == null ? null : Number(body.actual_box_count);
  const actual_pallet_count = body.actual_pallet_count == null ? null : Number(body.actual_pallet_count);

  return withIdem(env, body, "v2_inbound_plan_force_complete", async () => {
    const plan = await env.DB.prepare("SELECT * FROM v2_inbound_plans WHERE id=?").bind(id).first();
    if (!plan) return { ok: false, error: "not_found" };
    if (plan.status === 'completed') return { ok: false, error: "already_completed" };
    if (plan.status === 'cancelled') return { ok: false, error: "cancelled_cannot_complete" };

    const ALLOWED = ['pending', 'arrived_pending_putaway', 'putting_away', 'partially_completed'];
    if (ALLOWED.indexOf(plan.status) === -1) {
      return { ok: false, error: "status_invalid", message: "current status: " + plan.status };
    }

    // active job 存在 → 不允许（避免和现场作业冲突）
    const activeJob = await env.DB.prepare(
      "SELECT id, job_type FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(id).first();
    if (activeJob) {
      return { ok: false, error: "active_job_cannot_force_complete", active_job_id: activeJob.id, active_job_type: activeJob.job_type };
    }

    const t = now();
    // 确保 biz_tasks 行齐全
    await ensureInboundPlanBizTasks(env, plan);
    // 把所有未完成 biz_task 标 completed（manual_force 来源）
    await env.DB.prepare(`
      UPDATE v2_inbound_plan_biz_tasks
        SET status='completed', completed_at=?, completed_by=?, worker_names=?,
            total_minutes=COALESCE(total_minutes,0),
            completion_source='manual_force', completion_note=?, updated_at=?
        WHERE plan_id=? AND status!='completed'
    `).bind(t, operator, operator, reason, t, id).run();

    // 实际数量写入 v2_inbound_plan_lines（按 unit_type=carton/pallet 分别 upsert）
    // v2_inbound_plans 主表无 actual_box_count / actual_pallet_count 字段
    async function upsertPlanLineActual(unit_type, qty) {
      if (qty == null || !Number.isFinite(qty)) return;
      const existed = await env.DB.prepare(
        "SELECT id FROM v2_inbound_plan_lines WHERE plan_id=? AND unit_type=? LIMIT 1"
      ).bind(id, unit_type).first();
      if (existed) {
        await env.DB.prepare(
          "UPDATE v2_inbound_plan_lines SET actual_qty=? WHERE id=?"
        ).bind(qty, existed.id).run();
      } else if (qty > 0) {
        const maxRow = await env.DB.prepare(
          "SELECT MAX(line_no) AS m FROM v2_inbound_plan_lines WHERE plan_id=?"
        ).bind(id).first();
        const nextLineNo = (maxRow && Number(maxRow.m) ? Number(maxRow.m) : 0) + 1;
        await env.DB.prepare(
          "INSERT INTO v2_inbound_plan_lines(id, plan_id, line_no, unit_type, planned_qty, actual_qty, remark) VALUES(?,?,?,?,?,?,?)"
        ).bind(uid(), id, nextLineNo, unit_type, 0, qty, 'manual_force_complete').run();
      }
    }
    await upsertPlanLineActual('carton', actual_box_count);
    await upsertPlanLineActual('pallet', actual_pallet_count);

    // 更新入库计划：force_completed=1 + status=completed（仅写真实存在字段）
    await env.DB.prepare(
      "UPDATE v2_inbound_plans SET status='completed', updated_at=?, force_completed=1, force_completed_by=?, force_completed_at=?, force_complete_reason=?, manual_completed_by=?, manual_completed_at=? WHERE id=?"
    ).bind(t, operator, t, reason, operator, t, id).run();

    return { ok: true, status: 'completed', force_completed: 1, completed_at: t, completed_by: operator };
  });
});

// =====================================================
// 入库计划：批量把 partially_completed 强制设为 completed
//   - 仅 ADMINKEY；不产生现场工时；幂等
//   - 跳过条件：active inbound/unload job 存在 / 已 completed / 已 cancelled
//   - 业务类型 biz_task 全部 admin_force_complete；写 force_completed_*
//   - 关联出库单：仅统计已有 source_inbound_plan_id=plan.id 数量；不重复生成
//     （现系统出库单只在入库计划创建时通过 auto_create_outbound / link_ob 生成，
//      没有"完成时再生成"语义，所以本路由不补生成；如未生成请到协同中心手工创建）
// =====================================================
route("v2_admin_force_complete_partial_inbounds", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const dryRun = body.dry_run === true;
  const reason = String(body.reason || "历史多业务类型入库残留，管理员批量设为已入库").trim();
  const operator = String(body.operator_name || body.created_by || "ADMIN").trim();

  // 兼容多种历史拼写
  const targetStatuses = ['partially_completed', 'partial_completed', 'partially_done'];
  const placeholder = targetStatuses.map(() => '?').join(',');
  const rs = await env.DB.prepare(
    "SELECT * FROM v2_inbound_plans WHERE status IN (" + placeholder + ") ORDER BY created_at ASC LIMIT 10000"
  ).bind(...targetStatuses).all();
  const rows = rs.results || [];

  let checked_count = 0;
  let completed_count = 0;
  let skipped_active_job_count = 0;
  let generated_outbound_count = 0;
  let already_linked_outbound_count = 0;
  const examples = [];
  const t = now();

  for (const plan of rows) {
    checked_count++;

    // active job 探测
    const activeJob = await env.DB.prepare(
      "SELECT id, job_type FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(plan.id).first();
    if (activeJob) {
      skipped_active_job_count++;
      if (examples.length < 50) examples.push({
        display_no: plan.display_no,
        plan_id: plan.id,
        old_status: plan.status,
        new_status: plan.status,
        skipped_reason: "active_job: " + activeJob.job_type + " #" + activeJob.id
      });
      continue;
    }

    // 已存在的关联出库单（幂等判定）
    const linkedRs = await env.DB.prepare(
      "SELECT id, display_no FROM v2_outbound_orders WHERE source_inbound_plan_id=?"
    ).bind(plan.id).all();
    const linkedRows = linkedRs.results || [];
    already_linked_outbound_count += linkedRows.length;

    // dry-run 模式：只汇报
    if (dryRun) {
      await ensureInboundPlanBizTasks(env, plan);
      const tasks = await listInboundPlanBizTasks(env, plan.id);
      const pendingBiz = tasks.filter(x => x.status !== 'completed').map(x => x.biz_class);
      completed_count++;
      if (examples.length < 50) examples.push({
        display_no: plan.display_no,
        plan_id: plan.id,
        old_status: plan.status,
        new_status: 'completed',
        would_complete_biz_tasks: pendingBiz,
        existing_linked_outbound_count: linkedRows.length
      });
      continue;
    }

    // 实跑：补 biz_task + 写 force_completed_*
    await ensureInboundPlanBizTasks(env, plan);
    const tasksBefore = await listInboundPlanBizTasks(env, plan.id);
    const pendingBiz = tasksBefore.filter(x => x.status !== 'completed').map(x => x.biz_class);

    await env.DB.prepare(`
      UPDATE v2_inbound_plan_biz_tasks
         SET status='completed',
             completed_at=?,
             completed_by=?,
             worker_names=COALESCE(NULLIF(worker_names,''), ?),
             total_minutes=COALESCE(total_minutes, 0),
             completion_source='admin_force_complete',
             completion_note=?,
             updated_at=?
       WHERE plan_id=? AND status!='completed'
    `).bind(t, operator, operator, reason, t, plan.id).run();

    await env.DB.prepare(
      "UPDATE v2_inbound_plans SET status='completed', updated_at=?, force_completed=1, force_completed_by=?, force_completed_at=?, force_complete_reason=?, manual_completed_by=COALESCE(NULLIF(manual_completed_by,''), ?), manual_completed_at=COALESCE(NULLIF(manual_completed_at,''), ?) WHERE id=?"
    ).bind(t, operator, t, reason, operator, t, plan.id).run();

    completed_count++;

    await env.DB.prepare(`
      INSERT INTO v2_admin_cleanup_logs(id, operator, action_type, target_job_id, target_worker_id, reason, detail_json, created_at)
      VALUES(?, ?, 'force_complete_partial_inbound', '', '', ?, ?, ?)
    `).bind("CLN-" + uid(), operator, reason,
            JSON.stringify({
              plan_id: plan.id, display_no: plan.display_no,
              old_status: plan.status,
              completed_biz_tasks: pendingBiz,
              existing_linked_outbound_count: linkedRows.length
            }),
            t).run();

    if (examples.length < 50) examples.push({
      display_no: plan.display_no,
      plan_id: plan.id,
      old_status: plan.status,
      new_status: 'completed',
      completed_biz_tasks: pendingBiz,
      existing_linked_outbound_count: linkedRows.length
    });
  }

  return json({
    ok: true,
    dry_run: dryRun,
    checked_count,
    completed_count,
    skipped_active_job_count,
    generated_outbound_count, // 现系统无"完成时再生成出库单"语义，恒 0
    already_linked_outbound_count,
    examples
  });
});

// =====================================================
// 入库计划/出库作业单：协同中心导出 CSV（最大 10000 行）
// =====================================================
const _STATUS_LABEL_ZH = {
  // inbound
  pending: '未到货', unloading: '卸货中', unloading_putting_away: '卸货中+理货中',
  arrived_pending_putaway: '已到库待理货', putting_away: '理货中',
  partially_completed: '部分入库完成', processing: '处理中', completed: '已完成',
  unloaded_pending_info: '已卸货·待补充信息',
  // outbound
  pending_issue: '待下发', issued: '已下发', working: '操作中', ready_to_ship: '待出库',
  operation_reserved: '操作预约', stock_operating: '操作中(库内)',
  pending_outbound_update: '待更新出库计划', preparing_outbound: '出库准备中',
  shipped: '已出库', reopen_pending: '待再操作', cancelled: '已取消'
};
const _BIZ_LABEL_ZH = {
  direct_ship: '代发', bulk: '大货', return: '退件', change_order: '换单', import: '进口'
};
const _INBOUND_BIZ_LABEL_ZH = {
  direct_ship: '代发入库', bulk: '大货入库', return: '退件入库', change_order: '换单入库'
};
function _statusLabelZh(s) { return _STATUS_LABEL_ZH[s] || (s || ''); }
function _bizListLabelZh(arr) {
  return (arr || []).map(b => _BIZ_LABEL_ZH[b] || b).join('+');
}

route("v2_inbound_plan_export", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const start = String(body.start_date || "").trim();
  const end = String(body.end_date || "").trim();
  const status = String(body.status || "").trim();
  const accounted = String(body.accounted == null ? "" : body.accounted).trim();
  const biz_class_filter = String(body.biz_class || "").trim();
  const customer_keyword = String(body.customer_keyword || "").trim();
  const unload_done_from = String(body.unload_done_date_from || "").trim();
  const unload_done_to = String(body.unload_done_date_to || "").trim();
  const VALID_BIZ = ['direct_ship','bulk','return','change_order'];
  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 5000;
  if (limit > 10000) limit = 10000;

  let sql = "SELECT * FROM v2_inbound_plans WHERE source_type != 'return_session' AND COALESCE(is_deleted,0)=0";
  const binds = [];
  if (start) { sql += " AND plan_date>=?"; binds.push(start); }
  if (end) { sql += " AND plan_date<=?"; binds.push(end); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  else { sql += " AND status != 'cancelled'"; }
  if (accounted === "1") { sql += " AND accounted=1"; }
  else if (accounted === "0") { sql += " AND (accounted IS NULL OR accounted=0)"; }
  if (unload_done_from) {
    const r = kstDayRangeUtc(unload_done_from);
    if (r) { sql += " AND unload_completed_at >= ?"; binds.push(r.startUtc); }
  }
  if (unload_done_to) {
    const r = kstDayRangeUtc(unload_done_to);
    if (r) { sql += " AND unload_completed_at < ?"; binds.push(r.endUtc); }
  }
  if (biz_class_filter && VALID_BIZ.indexOf(biz_class_filter) !== -1) {
    sql += " AND ("
        +    "biz_class=?"
        +    " OR biz_classes_json LIKE ?"
        +    " OR EXISTS (SELECT 1 FROM v2_inbound_plan_biz_tasks t WHERE t.plan_id=v2_inbound_plans.id AND t.biz_class=?)"
        +  ")";
    binds.push(biz_class_filter, '%"' + biz_class_filter + '"%', biz_class_filter);
  }
  if (customer_keyword) {
    sql += " AND customer LIKE ?";
    binds.push('%' + customer_keyword + '%');
  }
  sql += " ORDER BY plan_date DESC, created_at DESC LIMIT ?";
  binds.push(limit);
  const rs = await env.DB.prepare(sql).bind(...binds).all();
  const rows = rs.results || [];
  if (rows.length === 0) return json({ ok: true, rows: [], truncated: false });

  const planIds = rows.map(p => p.id);

  const taskRows = await batchSelectInGlobal(env,
    `SELECT plan_id, biz_class, status, completed_by, worker_names, completed_at, total_minutes, completion_source
     FROM v2_inbound_plan_biz_tasks WHERE plan_id IN (PLACEHOLDER) ORDER BY biz_class`,
    planIds);
  const tasksByPlan = {};
  for (const r of taskRows) {
    if (!tasksByPlan[r.plan_id]) tasksByPlan[r.plan_id] = [];
    tasksByPlan[r.plan_id].push(r);
  }
  const linkRows = await batchSelectInGlobal(env,
    `SELECT id, source_inbound_plan_id, display_no FROM v2_outbound_orders WHERE source_inbound_plan_id IN (PLACEHOLDER)`,
    planIds);
  const linkByPlan = {};
  for (const r of linkRows) {
    if (!linkByPlan[r.source_inbound_plan_id]) linkByPlan[r.source_inbound_plan_id] = [];
    linkByPlan[r.source_inbound_plan_id].push(r.display_no || r.id);
  }
  const attRows = await batchSelectInGlobal(env,
    `SELECT related_doc_id AS plan_id, COUNT(*) AS c FROM v2_attachments
       WHERE related_doc_type='inbound_plan' AND related_doc_id IN (PLACEHOLDER) GROUP BY related_doc_id`,
    planIds);
  const attByPlan = {};
  for (const r of attRows) attByPlan[r.plan_id] = Number(r.c || 0);
  // 入库明细资料：数量 + 文件名（attachment_category='inbound_material'）
  const matFileRows = await batchSelectInGlobal(env,
    `SELECT related_doc_id AS plan_id, file_name FROM v2_attachments
       WHERE related_doc_type='inbound_plan' AND attachment_category='inbound_material'
         AND related_doc_id IN (PLACEHOLDER) ORDER BY created_at ASC`,
    planIds);
  const materialFilesByPlan = {};
  for (const r of matFileRows) {
    if (!materialFilesByPlan[r.plan_id]) materialFilesByPlan[r.plan_id] = [];
    materialFilesByPlan[r.plan_id].push(r.file_name || '');
  }
  // 入库实际数量来源是 v2_inbound_plan_lines（按 unit_type='carton'/'pallet' 分行）
  // v2_inbound_plans 主表无 actual_box_count / actual_pallet_count 字段
  const planLineRows = await batchSelectInGlobal(env,
    `SELECT plan_id, unit_type, SUM(planned_qty) AS pq, SUM(actual_qty) AS aq, SUM(putaway_qty) AS pu
       FROM v2_inbound_plan_lines WHERE plan_id IN (PLACEHOLDER) GROUP BY plan_id, unit_type`,
    planIds);
  const linesByPlan = {};
  for (const r of planLineRows) {
    if (!linesByPlan[r.plan_id]) linesByPlan[r.plan_id] = {};
    linesByPlan[r.plan_id][r.unit_type || ''] = {
      pq: Number(r.pq || 0),
      aq: Number(r.aq || 0),
      pu: Number(r.pu || 0)
    };
  }

  const out = rows.map(p => {
    const bizArr = extractPlanBizClasses(p);
    const tasks = tasksByPlan[p.id] || [];
    const completedBiz = tasks.filter(t => t.status === 'completed').map(t => t.biz_class);
    const pendingBiz = tasks.filter(t => t.status !== 'completed').map(t => t.biz_class);
    // 入库类型执行状态明细：代发入库=已完成/EMP-xxx/2026-04-30 10:20；大货入库=未完成
    const taskDetail = tasks.map(t => {
      const lbl = _INBOUND_BIZ_LABEL_ZH[t.biz_class] || t.biz_class;
      if (t.status === 'completed') {
        const who = t.worker_names || t.completed_by || '';
        const at = t.completed_at ? fmtKst(t.completed_at) : '';
        const src = t.completion_source === 'manual_force' ? '(手动)' : '';
        return `${lbl}=已完成${src}/${who}/${at}`;
      } else {
        return `${lbl}=未完成`;
      }
    }).join('；');
    const linkedNos = linkByPlan[p.id] || [];
    const lp = linesByPlan[p.id] || {};
    const cartonAgg = lp.carton || { pq: 0, aq: 0, pu: 0 };
    const palletAgg = lp.pallet || { pq: 0, aq: 0, pu: 0 };
    // 实际数量优先取 putaway（实际入库），否则取卸货 actual
    const actualBox = cartonAgg.pu || cartonAgg.aq;
    const actualPallet = palletAgg.pu || palletAgg.aq;

    return {
      入库单号: p.display_no || p.id,
      外部WMS单号: p.external_inbound_no || '',
      计划日期: p.plan_date || '',
      客户: p.customer || '',
      状态: _statusLabelZh(p.status),
      业务分类: _bizListLabelZh(bizArr),
      已完成入库类型: completedBiz.map(b => _BIZ_LABEL_ZH[b] || b).join('+'),
      未完成入库类型: pendingBiz.map(b => _BIZ_LABEL_ZH[b] || b).join('+'),
      入库类型执行状态明细: taskDetail,
      货物摘要: p.cargo_summary || '',
      用途: p.purpose || '',
      预计到达日期: normalizeDateOnly(p.expected_arrival),
      卸货完成时间: p.unload_completed_at ? fmtKst(p.unload_completed_at) : '',
      卸货人员: p.unload_completed_by || '',
      备注: p.remark || '',
      创建人: p.created_by || '',
      创建时间: fmtKst(p.created_at),
      计划箱数: cartonAgg.pq,
      计划托数: palletAgg.pq,
      实际箱数: actualBox,
      实际托数: actualPallet,
      是否记账: p.accounted == 1 ? '已记账' : '未记账',
      记账人: p.accounted_by || '',
      记账时间: p.accounted_at ? fmtKst(p.accounted_at) : '',
      关联出库单数量: linkedNos.length,
      关联出库单号: linkedNos.join('；'),
      入库明细数量: (materialFilesByPlan[p.id] || []).length,
      入库明细文件名: (materialFilesByPlan[p.id] || []).join('；'),
      附件数量: attByPlan[p.id] || 0,
      是否手动完成: Number(p.force_completed || 0) === 1 ? '是' : '否',
      手动完成人: p.force_completed_by || '',
      手动完成时间: p.force_completed_at ? fmtKst(p.force_completed_at) : '',
      手动完成原因: p.force_complete_reason || '',
      文员直接完结人: p.manual_completed_by || '',
      文员直接完结时间: p.manual_completed_at ? fmtKst(p.manual_completed_at) : '',
      来源类型: p.source_type || '',
      plan_id: p.id
    };
  });
  return json({ ok: true, rows: out, truncated: rows.length >= limit });
});

route("v2_outbound_order_export", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const start = String(body.start_date || "").trim();
  const end = String(body.end_date || "").trim();
  const date_basis = String(body.date_basis || "expected_ship_at").trim();
  const status = String(body.status || "").trim();
  const accounted = String(body.accounted == null ? "" : body.accounted).trim();
  const biz_class = String(body.biz_class || "").trim();
  const customer_keyword = String(body.customer_keyword || "").trim();
  const usesStockRaw = String(body.uses_stock_operation == null ? "" : body.uses_stock_operation).trim();
  const hasMaterialRaw = String(body.has_material == null ? "" : body.has_material).trim();
  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 5000;
  if (limit > 10000) limit = 10000;

  let sql = "SELECT * FROM v2_outbound_orders WHERE 1=1";
  const binds = [];
  const _dateExpr = (date_basis === 'order_date')
    ? "order_date"
    : "(CASE WHEN expected_ship_at IS NOT NULL AND expected_ship_at != '' THEN substr(expected_ship_at,1,10) ELSE order_date END)";
  if (start) { sql += " AND " + _dateExpr + ">=?"; binds.push(start); }
  if (end)   { sql += " AND " + _dateExpr + "<=?"; binds.push(end); }
  if (status) { sql += " AND status=?"; binds.push(status); }
  else { sql += " AND status != 'cancelled'"; }
  if (accounted === "1") { sql += " AND accounted=1"; }
  else if (accounted === "0") { sql += " AND (accounted IS NULL OR accounted=0)"; }
  if (biz_class) { sql += " AND biz_class=?"; binds.push(biz_class); }
  if (customer_keyword) { sql += " AND customer LIKE ?"; binds.push('%' + customer_keyword + '%'); }
  if (usesStockRaw === "1") { sql += " AND uses_stock_operation=1"; }
  else if (usesStockRaw === "0") { sql += " AND (uses_stock_operation IS NULL OR uses_stock_operation=0)"; }
  if (hasMaterialRaw === "1") {
    sql += " AND EXISTS (SELECT 1 FROM v2_attachments a WHERE a.related_doc_type='outbound_order' AND a.related_doc_id=v2_outbound_orders.id AND a.attachment_category='outbound_material')";
  } else if (hasMaterialRaw === "0") {
    sql += " AND NOT EXISTS (SELECT 1 FROM v2_attachments a WHERE a.related_doc_type='outbound_order' AND a.related_doc_id=v2_outbound_orders.id AND a.attachment_category='outbound_material')";
  }
  sql += " ORDER BY " + _dateExpr + " DESC, created_at DESC LIMIT ?";
  binds.push(limit);
  const rs = await env.DB.prepare(sql).bind(...binds).all();
  const rows = rs.results || [];
  if (rows.length === 0) return json({ ok: true, rows: [], truncated: false });

  const ids = rows.map(o => o.id);
  // 资料：count + 文件名串
  const matRows = await batchSelectInGlobal(env,
    `SELECT related_doc_id AS id, file_name FROM v2_attachments
       WHERE related_doc_type='outbound_order' AND attachment_category='outbound_material'
         AND related_doc_id IN (PLACEHOLDER)
       ORDER BY created_at ASC`,
    ids);
  const matNamesByOb = {}, matCountByOb = {};
  for (const r of matRows) {
    matCountByOb[r.id] = (matCountByOb[r.id] || 0) + 1;
    if (!matNamesByOb[r.id]) matNamesByOb[r.id] = [];
    matNamesByOb[r.id].push(r.file_name || '');
  }
  // 来源入库计划 display_no
  const inboundIds = [...new Set(rows.map(r => r.source_inbound_plan_id).filter(Boolean))];
  const inboundDispNoById = {};
  if (inboundIds.length > 0) {
    const ibRows = await batchSelectInGlobal(env,
      `SELECT id, display_no FROM v2_inbound_plans WHERE id IN (PLACEHOLDER)`,
      inboundIds);
    for (const r of ibRows) inboundDispNoById[r.id] = r.display_no || r.id;
  }

  const _OUTMODE_ZH = {
    warehouse_dispatch: '仓库叫车', customer_pickup: '客户自提',
    milk_express: '牛奶快递', milk_pallet: '牛奶托盘', container_pickup: '柜子提货'
  };

  const out = rows.map(o => {
    let stockResult = null;
    try { stockResult = JSON.parse(o.stock_operation_result_json || 'null'); } catch (e) {}
    const stockResultSummary = stockResult
      ? '箱:' + Number(stockResult.total_box_count || 0) + ' / 托:' + Number(stockResult.total_pallet_count || 0) + (stockResult.last_remark ? ' / ' + stockResult.last_remark : '')
      : '';
    return {
      出库单号: o.display_no || o.id,
      作业单日期: o.order_date || '',
      预计出库日期: normalizeDateOnly(o.expected_ship_at),
      客户: o.customer || '',
      状态: _statusLabelZh(o.status),
      业务分类: _BIZ_LABEL_ZH[o.biz_class] || o.biz_class || '',
      是否库内操作: Number(o.uses_stock_operation || 0) === 1 ? '是' : '否',
      出库模式: _OUTMODE_ZH[o.outbound_mode] || o.outbound_mode || '',
      目的地: o.destination || '',
      PO号: o.po_no || '',
      WMS工单号: o.wms_work_order_no || '',
      作业说明: o.instruction || '',
      出库要求: o.outbound_requirement || '',
      备注: o.remark || '',
      计划箱数: Number(o.planned_box_count || 0),
      计划托数: Number(o.planned_pallet_count || 0),
      实际箱数: Number(o.actual_box_count || 0),
      实际托数: Number(o.actual_pallet_count || 0),
      库内操作状态: o.stock_operation_status || '',
      库内操作完成人: o.stock_operation_completed_by || '',
      库内操作完成时间: o.stock_operation_completed_at ? fmtKst(o.stock_operation_completed_at) : '',
      库内操作结果: stockResultSummary,
      提货车辆: o.pickup_vehicle_no || '',
      司机姓名: o.pickup_driver_name || '',
      司机电话: o.pickup_driver_phone || '',
      提货人: o.pickup_person_name || '',
      提货公司: o.pickup_company || '',
      预计提货时间: o.pickup_time || '',
      提货备注: o.pickup_note || '',
      提货信息确认人: o.pickup_confirmed_by || '',
      提货信息确认时间: o.pickup_confirmed_at ? fmtKst(o.pickup_confirmed_at) : '',
      是否待仓库确认: Number(o.warehouse_ack_required || 0) === 1 ? '是' : '否',
      变更确认人: o.warehouse_ack_by || '',
      变更确认时间: o.warehouse_ack_at ? fmtKst(o.warehouse_ack_at) : '',
      最后修改人: o.last_modified_by || '',
      最后修改时间: o.last_modified_at ? fmtKst(o.last_modified_at) : '',
      版本号: Number(o.revision_no || 0),
      是否记账: o.accounted == 1 ? '已记账' : '未记账',
      记账人: o.accounted_by || '',
      记账时间: o.accounted_at ? fmtKst(o.accounted_at) : '',
      出库资料数量: matCountByOb[o.id] || 0,
      出库资料文件名: (matNamesByOb[o.id] || []).join('；'),
      来源入库计划: inboundDispNoById[o.source_inbound_plan_id] || '',
      创建人: o.created_by || '',
      创建时间: fmtKst(o.created_at),
      order_id: o.id
    };
  });
  return json({ ok: true, rows: out, truncated: rows.length >= limit });
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
      // 仅工时型：直接 completed；需要产出且无 result：awaiting_close
      await autoCloseJobIfNoOpenWorkers(env, job_id, t);
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
      const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
      return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" };
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

    // 防御性收口：再关一次（避免 closeAllOpenSegs 之后还有别人 open）
    await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', active_worker_count=0, updated_at=? WHERE id=?"
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

  // 检测该员工的"跨天未退出"或"任务已完成但未退出"的脏段
  // 任务已完成的脏段直接自动关闭（系统数据残留，无需员工/主管干预）
  // 跨天未退出仍提示员工/主管
  const today = kstToday();
  const allOpen = await env.DB.prepare(
    `SELECT w.id AS segment_id, w.job_id, w.joined_at, w.worker_name,
            j.status AS job_status, j.job_type, j.flow_stage, j.related_doc_id, j.updated_at AS job_updated_at
       FROM v2_ops_job_workers w
       JOIN v2_ops_jobs j ON j.id = w.job_id
      WHERE w.worker_id=? AND w.left_at=''
      ORDER BY w.joined_at ASC`
  ).bind(worker_id).all();
  const stale_segments = [];
  const auto_cleaned_segments = [];
  const _t = now();
  for (const r of (allOpen.results || [])) {
    let joinedKstDate = '';
    if (r.joined_at) {
      const jms = Date.parse(r.joined_at);
      if (Number.isFinite(jms)) {
        const dt = new Date(jms + 9 * 3600 * 1000);
        joinedKstDate = dt.getUTCFullYear() + '-' +
          String(dt.getUTCMonth() + 1).padStart(2, '0') + '-' +
          String(dt.getUTCDate()).padStart(2, '0');
      }
    }
    if (r.job_status === 'completed') {
      // 自动收口：用 job.updated_at 兜底（最接近完成时刻），否则用 now
      const closeAt = r.job_updated_at || _t;
      const joinedMs = Date.parse(r.joined_at || '');
      const leftMs = Date.parse(closeAt);
      let minutes = 0;
      if (Number.isFinite(joinedMs) && Number.isFinite(leftMs)) {
        minutes = Math.max(0, Math.round((leftMs - joinedMs) / 60000 * 10) / 10);
      }
      await env.DB.prepare(
        "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason='auto_cleanup_completed_job' WHERE id=?"
      ).bind(closeAt, minutes, r.segment_id).run();
      // 同步重算 active_worker_count（防御性，仍应为 0）
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET active_worker_count=0, updated_at=? WHERE id=?"
      ).bind(_t, r.job_id).run();
      auto_cleaned_segments.push({
        segment_id: r.segment_id,
        job_id: r.job_id,
        job_type: r.job_type || '',
        related_doc_id: r.related_doc_id || ''
      });
      continue;
    }
    if (joinedKstDate && joinedKstDate !== today) {
      stale_segments.push({
        segment_id: r.segment_id,
        job_id: r.job_id,
        joined_at: r.joined_at || '',
        job_status: r.job_status || '',
        job_type: r.job_type || '',
        flow_stage: r.flow_stage || '',
        related_doc_id: r.related_doc_id || '',
        stale_reason: '跨天未退出'
      });
    }
  }

  if (!seg) return json({ ok: true, active: false, stale_segments, auto_cleaned_segments });

  const t = now();
  await recalcActiveCount(env, seg.job_id, t);
  const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(seg.job_id).first();
  return json({ ok: true, active: true, segment: seg, job, stale_segments, auto_cleaned_segments });
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
// 入库/出库资料的统一类型/大小白名单（图片类附件如车辆/卸货照片走另一通道）
const ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024; // 20MB
const ATTACHMENT_MATERIAL_MIME = {
  "application/pdf": 1,
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": 1,
  "application/vnd.ms-excel": 1,
  "text/csv": 1,
  "image/jpeg": 1,
  "image/png": 1
};
const ATTACHMENT_MATERIAL_EXT = ["pdf","xlsx","xls","csv","jpg","jpeg","png"];
function _isMaterialCategory(cat) {
  return cat === "outbound_material" || cat === "inbound_material";
}
function _materialFileAllowed(file) {
  const name = String(file && file.name || "").toLowerCase();
  const dot = name.lastIndexOf(".");
  const ext = dot >= 0 ? name.slice(dot + 1) : "";
  const mime = String(file && file.type || "").toLowerCase();
  if (ATTACHMENT_MATERIAL_MIME[mime]) return true;
  if (ATTACHMENT_MATERIAL_EXT.indexOf(ext) !== -1) return true;
  return false;
}

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

  // 资料类附件（入库/出库明细）的类型 + 大小校验
  if (_isMaterialCategory(attachment_category)) {
    if (!_materialFileAllowed(file)) {
      return err("unsupported_file_type: 仅支持 PDF / Excel / CSV / 图片文件 (PDF, Excel, CSV, image)", 400);
    }
    if (file.size && file.size > ATTACHMENT_MAX_BYTES) {
      return err("file_too_large: 单文件最大 20MB / 단일 파일 최대 20MB", 400);
    }
  }

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

  // 完整附件对象供前端立即渲染（避免再请求列表）
  const attachment = {
    id,
    related_doc_type,
    related_doc_id,
    attachment_category,
    file_name: file.name,
    file_key: fileKey,
    file_size: file.size,
    content_type: file.type,
    mime_type: file.type,
    uploaded_by,
    created_at: t
  };
  return json({ ok: true, id, file_key: fileKey, attachment });
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

// 删除附件 — 仅协同中心写权限（ADMINKEY/VIEWKEY 共用 isAuth）；
// 出库资料联动出库单变更：revision_no+1 / warehouse_ack_required=1 / 写 change_log
route("v2_attachment_delete", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_attachment_delete", async () => {
    const att = await env.DB.prepare("SELECT * FROM v2_attachments WHERE id=?").bind(id).first();
    if (!att) return { ok: false, error: "not_found", message: "附件不存在或已被删除" };

    // 已 frozen 的出库单不允许删除其资料
    if (att.related_doc_type === 'outbound_order' && att.attachment_category === 'outbound_material') {
      const order = await env.DB.prepare(
        "SELECT id, status FROM v2_outbound_orders WHERE id=?"
      ).bind(att.related_doc_id).first();
      const FROZEN = ['shipped', 'completed', 'cancelled'];
      if (order && FROZEN.indexOf(order.status) !== -1) {
        return { ok: false, error: "frozen_status_cannot_edit",
          message: "已出库/完成/取消的出库单不能删除资料：" + order.status };
      }
    }

    // 删 R2（失败不阻断 DB 删除——文件孤悬可后续清理；DB 删除失败才视为整体失败）
    let r2_deleted = false;
    try {
      if (att.file_key && env.R2_BUCKET) {
        await env.R2_BUCKET.delete(att.file_key);
        r2_deleted = true;
      }
    } catch (e) { /* swallow */ }

    await env.DB.prepare("DELETE FROM v2_attachments WHERE id=?").bind(id).run();

    return {
      ok: true,
      deleted: true,
      id,
      file_key: att.file_key || '',
      related_doc_type: att.related_doc_type || '',
      related_doc_id: att.related_doc_id || '',
      attachment_category: att.attachment_category || '',
      r2_deleted
    };
  });
});

// 出库资料变化触发仓库重新确认 — revision+1 / warehouse_ack_required=1 / 写 change_log
// 调用方：客服在修改出库单弹窗里删除/新增了出库资料之后调用本 action 收口
// summary 例："出库资料变更：删除 1 个，新增 2 个"
route("v2_outbound_order_mark_material_changed", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  const summary = String(body.summary || "出库资料变更").trim();
  const by = String(body.by || body.modified_by || "");
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_outbound_order_mark_material_changed", async () => {
    const order = await env.DB.prepare("SELECT id, status, revision_no FROM v2_outbound_orders WHERE id=?").bind(id).first();
    if (!order) return { ok: false, error: "not_found" };
    const FROZEN = ['shipped', 'completed', 'cancelled'];
    if (FROZEN.indexOf(order.status) !== -1) {
      return { ok: false, error: "frozen_status_cannot_edit",
        message: "已出库/完成/取消的出库单不能修改：" + order.status };
    }
    const t = now();
    const newRevision = Number(order.revision_no || 0) + 1;
    await env.DB.prepare(`
      UPDATE v2_outbound_orders
         SET revision_no=?, last_modified_by=?, last_modified_at=?,
             warehouse_ack_required=1, warehouse_ack_by='', warehouse_ack_at='',
             updated_at=?
       WHERE id=?
    `).bind(newRevision, by, t, t, id).run();

    await insertOutboundChangeLog(env, {
      order_id: id,
      revision_no: newRevision,
      change_type: 'material_update',
      changed_by: by,
      diff: body.diff || {},
      summary,
      t
    });

    return { ok: true, id, revision_no: newRevision, summary_text: summary };
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
  // 软删除的反馈（如误转正回滚）一律不显示
  let where = " WHERE COALESCE(is_deleted,0)=0";
  const binds = [];
  if (feedback_type) { where += " AND feedback_type=?"; binds.push(feedback_type); }
  if (status) { where += " AND status=?"; binds.push(status); }
  const countRow = binds.length > 0
    ? await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_field_feedbacks" + where).bind(...binds).first()
    : await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_field_feedbacks" + where).first();
  const total = Number((countRow && countRow.c) || 0);
  const listSql = "SELECT * FROM v2_field_feedbacks" + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?";
  const rs = await env.DB.prepare(listSql).bind(...binds, limit, offset).all();
  return json({ ok: true, items: rs.results || [], ...pageMeta(total, limit, offset) });
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
  const bizNorm = normalizeInboundBizClasses({ biz_classes: body.biz_classes, biz_class: body.biz_class });
  const biz_class = bizNorm.primary || String(body.biz_class || "");
  const biz_classes_json = bizNorm.list.length > 0 ? JSON.stringify(bizNorm.list) : '[]';
  const created_by = String(body.created_by || "");
  const display_no = await nextDisplayNo(env, plan_date);

  await env.DB.prepare(`
    INSERT INTO v2_inbound_plans(id, plan_date, customer, biz_class, biz_classes_json, cargo_summary,
      expected_arrival, purpose, remark, status, source_feedback_id, created_by, created_at, updated_at, display_no)
    VALUES(?,?,?,?,?,?,?,?,?,'pending',?,?,?,?,?)
  `).bind(
    id, plan_date, customer, biz_class, biz_classes_json,
    String(body.cargo_summary || fb.title || ""),
    normalizeDateOnly(body.expected_arrival),
    String(body.purpose || ""),
    String(body.remark || fb.content || ""),
    feedback_id, created_by, t, t, display_no
  ).run();

  // 初始化 biz_tasks（pending）
  for (const biz of bizNorm.list) {
    await env.DB.prepare(`
      INSERT OR IGNORE INTO v2_inbound_plan_biz_tasks
        (id, plan_id, biz_class, job_type, status, created_at, updated_at)
      VALUES(?,?,?,?, 'pending', ?, ?)
    `).bind("IBT-" + uid(), id, biz, mapInboundBizToJobType(biz), t, t).run();
  }

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

// ===== 现场反馈：删除（仅误操作脏数据，ADMINKEY only） =====
route("v2_feedback_delete", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized", 401);
  const id = String(body.id || body.feedback_id || "").trim();
  if (!id) return err("missing id");

  return withIdem(env, body, "v2_feedback_delete", async () => {
    const fb = await env.DB.prepare(
      "SELECT id, status, feedback_type, inbound_plan_id FROM v2_field_feedbacks WHERE id=?"
    ).bind(id).first();
    if (!fb) return { ok: false, error: "not_found" };

    // 已转正 → 拒（formal inbound/outbound 数据不能因删反馈而连带丢失）
    if (fb.status === 'converted') {
      return { ok: false, error: "converted_cannot_delete", message: "已转正的反馈不能删除 / 정식 전환된 피드백은 삭제 불가" };
    }
    if (fb.inbound_plan_id) {
      return { ok: false, error: "has_converted_doc_cannot_delete", message: "该反馈已生成正式入库计划，不能删除 / 정식 입고 계획이 생성되어 삭제 불가" };
    }

    // 仅允许这些状态删除（unloaded_pending_info 现已在列）
    const allowDelete = ['open', 'field_working', 'unloaded_pending_info', 'cancelled'];
    if (allowDelete.indexOf(fb.status) === -1) {
      return { ok: false, error: "status_not_allowed", message: "当前状态不允许删除：" + fb.status };
    }

    // 有进行中的关联 job → 拒（不能在作业中误删）
    const activeJob = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='field_feedback' AND related_doc_id=? AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(id).first();
    if (activeJob) {
      return { ok: false, error: "active_job_cannot_delete", message: "该反馈有关联进行中任务，不能删除 / 진행 중 작업이 있어 삭제 불가" };
    }

    // unloaded_pending_info / open / field_working(无 active) / cancelled：
    // 反馈未转正，关联的 cancelled / completed unplanned_unload job 仅是反馈生命周期内部数据
    // → 连同 workers / results / job 一并清理，避免悬挂记录
    const relatedJobsRs = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='field_feedback' AND related_doc_id=?"
    ).bind(id).all();
    const relatedJobIds = (relatedJobsRs.results || []).map(r => r.id);

    let deleted = { feedback: 0, attachments: 0, jobs: 0, job_workers: 0, job_results: 0 };

    // 反馈关联 job 通常 1 条；逐个清 workers/results 再删 job 主体（小循环不影响性能）
    for (const jobId of relatedJobIds) {
      const wRs = await env.DB.prepare(
        "DELETE FROM v2_ops_job_workers WHERE job_id=?"
      ).bind(jobId).run();
      deleted.job_workers += (wRs.meta && wRs.meta.changes) || 0;

      const rRs = await env.DB.prepare(
        "DELETE FROM v2_ops_job_results WHERE job_id=?"
      ).bind(jobId).run();
      deleted.job_results += (rRs.meta && rRs.meta.changes) || 0;

      const jRs = await env.DB.prepare(
        "DELETE FROM v2_ops_jobs WHERE id=?"
      ).bind(jobId).run();
      deleted.jobs += (jRs.meta && jRs.meta.changes) || 0;
    }

    const attsRs = await env.DB.prepare(
      "DELETE FROM v2_attachments WHERE related_doc_type='field_feedback' AND related_doc_id=?"
    ).bind(id).run();
    deleted.attachments = (attsRs.meta && attsRs.meta.changes) || 0;

    const fbRs = await env.DB.prepare("DELETE FROM v2_field_feedbacks WHERE id=?").bind(id).run();
    deleted.feedback = (fbRs.meta && fbRs.meta.changes) || 0;

    return { ok: true, id, deleted };
  });
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
        const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
        return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "趟次已完成" };
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
      const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
      return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "趟次已完成" };
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
      // customer：linkedOb 优先；前端 body.customer 兜底（非系统单时由 001 现场录入）
      const initCustomer = String((linkedOb && linkedOb.customer) || body.customer || '').trim();
      await env.DB.prepare(`
        INSERT INTO v2_ops_jobs(id, flow_stage, biz_class, job_type, related_doc_type, related_doc_id,
          status, shared_result_json, linked_outbound_order_id,
          parent_job_id, is_temporary_interrupt, interrupt_type,
          customer,
          created_by, created_at, updated_at, active_worker_count)
        VALUES(?,'order_op','bulk','bulk_op','work_order',?,'working',?,?,
          '',0,'',?,?,?,?,1)
      `).bind(job_id, work_order_no, jobMeta, obId, initCustomer, worker_id, t, t).run();
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
      const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
      return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "任务已完成" };
    }

    // Leave-only: close segments, recalc; bulk_op 是结果型 → 无人后 awaiting_close（autoClose 兜底）
    if (leave_only) {
      await closeAllOpenSegs(env, job_id, worker_id, t, 'leave');
      const leaveCount = await recalcActiveCount(env, job_id, t);
      if (leaveCount === 0) await autoCloseJobIfNoOpenWorkers(env, job_id, t);
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

    // 4. Last person — 客户必填校验（非系统单时）
    const jobBefore = await env.DB.prepare(
      "SELECT customer, linked_outbound_order_id FROM v2_ops_jobs WHERE id=?"
    ).bind(job_id).first();
    const inCustomer = String(body.customer || "").trim();
    const isSystemDoc = !!(jobBefore && jobBefore.linked_outbound_order_id);
    const finalCustomer = inCustomer || (jobBefore && jobBefore.customer) || "";
    if (!isSystemDoc && !finalCustomer) {
      return { ok: false, error: "missing_customer",
        message: "请填写客户名称 / 고객명을 입력하세요" };
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
      result_note: String(body.result_note || ""),
      customer: finalCustomer
    };

    const result_id = "RES-" + uid();
    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, remark, result_json, result_lines_json, created_by, created_at)
      VALUES(?,?,?,?,?,?,?)
    `).bind(result_id, job_id, String(body.remark || ""), JSON.stringify(resultData), '[]', worker_id, t).run();

    // 防御性收口：先关闭所有遗留 open segment，再标记完成
    await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET status='completed', finished_at=?, active_worker_count=0, customer=COALESCE(NULLIF(?, ''), customer), updated_at=? WHERE id=?"
    ).bind(t, finalCustomer, t, job_id).run();

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
  const { limit, offset } = pageParams(body);

  let where = " WHERE flow_stage='order_op'";
  const binds = [];
  if (job_type) { where += " AND job_type=?"; binds.push(job_type); }
  // created_at 为 UTC ISO；按 KST 日历日筛选
  const _stR = kstDayRangeUtc(start);
  const _enR = kstDayRangeUtc(end);
  if (_stR) { where += " AND created_at>=?"; binds.push(_stR.startUtc); }
  if (_enR) { where += " AND created_at<?";  binds.push(_enR.endUtc); }
  // COUNT（同 WHERE）+ 分页 SELECT
  const countRow = binds.length > 0
    ? await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_ops_jobs" + where).bind(...binds).first()
    : await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_ops_jobs" + where).first();
  const total = Number((countRow && countRow.c) || 0);
  const listSql = "SELECT * FROM v2_ops_jobs" + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?";
  const rs = await env.DB.prepare(listSql).bind(...binds, limit, offset).all();
  const jobs = rs.results || [];
  if (jobs.length === 0) return json({ ok: true, items: [], ...pageMeta(total, limit, offset) });

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

  return json({ ok: true, items, ...pageMeta(total, limit, offset) });
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

  // 1. 当前在岗人数 = distinct worker_id with open segments AND job 仍处于活跃状态
  // 已 completed/cancelled 的 job 即使有残留 left_at='' 也不算在岗
  const activeWorkers = await env.DB.prepare(
    `SELECT COUNT(DISTINCT w.worker_id) as c
       FROM v2_ops_job_workers w
       JOIN v2_ops_jobs j ON j.id = w.job_id
      WHERE w.left_at='' AND j.status IN ('pending','working','awaiting_close')`
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
  //    包含 segment_id (w.id) 用于管理员强制退出定位；is_stale / stale_reason 标记异常段
  const liveWorkers = await env.DB.prepare(`
    SELECT w.id AS segment_id, w.worker_id, w.worker_name, w.joined_at, w.job_id,
           j.flow_stage, j.biz_class, j.job_type, j.related_doc_type, j.related_doc_id,
           j.display_no as job_display_no, j.status as job_status, j.updated_at as job_updated_at,
           p.display_no as plan_display_no
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON w.job_id = j.id
    LEFT JOIN v2_inbound_plans p ON j.related_doc_type='inbound_plan' AND j.related_doc_id = p.id
    WHERE w.left_at=''
    ORDER BY j.job_type ASC, w.worker_name ASC, w.joined_at ASC
  `).all();
  // Inject unified display_no + 异常判定（is_stale / stale_reason / minutes_open）
  // priority: plan_display_no > job_display_no > related_doc_id > job_id
  const _nowMs = Date.parse(now());
  const liveWorkerRows = (liveWorkers.results || []).map(function(r) {
    r.display_no = r.plan_display_no || r.job_display_no || r.related_doc_id || r.job_id || '';
    let minutesOpen = 0;
    let joinedKstDate = '';
    if (r.joined_at) {
      const jms = Date.parse(r.joined_at);
      if (Number.isFinite(jms)) {
        minutesOpen = Math.max(0, Math.floor((_nowMs - jms) / 60000));
        // KST 日期：joined_at 是 UTC ISO，加 9h 后取 UTC 年月日即 KST 年月日
        const dt = new Date(jms + 9 * 3600 * 1000);
        const yy = dt.getUTCFullYear();
        const mm = String(dt.getUTCMonth() + 1).padStart(2, '0');
        const dd = String(dt.getUTCDate()).padStart(2, '0');
        joinedKstDate = yy + '-' + mm + '-' + dd;
      }
    }
    let is_stale = 0;
    let stale_reason = '';
    // 异常优先级：完成未退出 > 跨天未退出 > 超长 12h
    if (r.job_status === 'completed') {
      is_stale = 1; stale_reason = '任务已完成但人员未退出';
    } else if (joinedKstDate && joinedKstDate !== today) {
      is_stale = 1; stale_reason = '跨天未退出';
    } else if (minutesOpen >= 720) {
      is_stale = 1; stale_reason = '超长未退出';
    }
    r.minutes_open = minutesOpen;
    r.is_stale = is_stale;
    r.stale_reason = stale_reason;
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
    ORDER BY
      CASE j.flow_stage
        WHEN 'unload' THEN 1
        WHEN 'inbound' THEN 2
        WHEN 'order_op' THEN 3
        WHEN 'outbound' THEN 4
        WHEN 'internal' THEN 5
        WHEN 'import' THEN 6
        WHEN 'issue' THEN 7
        ELSE 99
      END ASC,
      j.created_at ASC
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

// P2-10：执行系统简版实时看板
// 今日上岗 = 今日 KST 有 joined_at 的 distinct worker
// 当前在岗 = 有 open segment（left_at=''）且关联 job 在 working/pending/awaiting_close
// 不在岗 = 今日上岗 - 当前在岗
route("v2_ops_realtime_board", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const today = kstToday();
  const range = kstDayRangeUtc(today);

  // 当前在岗 — open seg + active job
  const activeRs = await env.DB.prepare(`
    SELECT w.worker_id, w.worker_name, w.joined_at, w.job_id,
           j.job_type, j.flow_stage, j.related_doc_id, j.related_doc_type,
           j.display_no AS job_display_no, j.status AS job_status,
           p.display_no AS plan_display_no
    FROM v2_ops_job_workers w
    JOIN v2_ops_jobs j ON w.job_id = j.id
    LEFT JOIN v2_inbound_plans p ON j.related_doc_type='inbound_plan' AND j.related_doc_id = p.id
    WHERE w.left_at='' AND j.status IN ('working','pending','awaiting_close')
    ORDER BY j.job_type ASC, w.worker_name ASC, w.joined_at ASC
  `).all();
  const active_workers = (activeRs.results || []).map(function(r) {
    return {
      worker_id: r.worker_id,
      worker_name: r.worker_name,
      job_type: r.job_type,
      flow_stage: r.flow_stage,
      display_no: r.plan_display_no || r.job_display_no || r.related_doc_id || r.job_id || '',
      job_id: r.job_id,
      joined_at: r.joined_at
    };
  });

  // 今日上岗 — distinct worker
  const todayJoinRs = await env.DB.prepare(`
    SELECT DISTINCT worker_id, worker_name
    FROM v2_ops_job_workers
    WHERE joined_at >= ? AND joined_at < ?
  `).bind(range.startUtc, range.endUtc).all();
  const todayWorkers = todayJoinRs.results || [];
  const activeIds = {};
  active_workers.forEach(function(w) { activeIds[w.worker_id] = true; });

  // 不在岗 = 今日已上岗 - 当前在岗；带最后任务+最后离岗时间
  const offWorkers = [];
  for (const tw of todayWorkers) {
    if (activeIds[tw.worker_id]) continue;
    const last = await env.DB.prepare(`
      SELECT w.left_at, w.job_id, j.job_type, j.flow_stage, j.related_doc_id, j.display_no AS job_display_no,
             p.display_no AS plan_display_no
      FROM v2_ops_job_workers w
      JOIN v2_ops_jobs j ON w.job_id = j.id
      LEFT JOIN v2_inbound_plans p ON j.related_doc_type='inbound_plan' AND j.related_doc_id = p.id
      WHERE w.worker_id=? AND w.joined_at >= ? AND w.joined_at < ? AND w.left_at != ''
      ORDER BY w.left_at DESC LIMIT 1
    `).bind(tw.worker_id, range.startUtc, range.endUtc).first();
    offWorkers.push({
      worker_id: tw.worker_id,
      worker_name: tw.worker_name,
      last_job_type: (last && last.job_type) || '',
      last_flow_stage: (last && last.flow_stage) || '',
      last_display_no: (last && (last.plan_display_no || last.job_display_no || last.related_doc_id)) || '',
      last_left_at: (last && last.left_at) || ''
    });
  }

  return json({
    ok: true,
    today_worker_count: todayWorkers.length,
    active_worker_count: active_workers.length,
    off_worker_count: offWorkers.length,
    active_workers: active_workers,
    off_workers: offWorkers
  });
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
      AND j.job_type IN ('bulk_op','inbound_direct','inbound_bulk','inbound_change_order','inbound_return','unload')
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
// 历史脏数据修复：按 v2_ops_job_workers 重算 issue_handle_runs.minutes_worked
// 仅 ADMINKEY；默认 dry_run=true，仅返回差异列表
// =====================================================
route("v2_admin_recalc_issue_work_minutes", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const issue_id = String(body.issue_id || "").trim();
  const run_id = String(body.run_id || "").trim();
  const dry_run = body.dry_run === false ? false : true; // 默认 true
  const limit = Math.min(Math.max(parseInt(body.limit || 200, 10) || 200, 1), 2000);

  let runsRs;
  if (run_id) {
    runsRs = await env.DB.prepare(
      "SELECT * FROM v2_issue_handle_runs WHERE id=?"
    ).bind(run_id).all();
  } else if (issue_id) {
    runsRs = await env.DB.prepare(
      "SELECT * FROM v2_issue_handle_runs WHERE issue_id=? ORDER BY started_at ASC"
    ).bind(issue_id).all();
  } else {
    runsRs = await env.DB.prepare(
      "SELECT * FROM v2_issue_handle_runs WHERE run_status='completed' ORDER BY started_at DESC LIMIT ?"
    ).bind(limit).all();
  }
  const runs = runsRs.results || [];

  const diffs = [];
  const issueIds = new Set();
  for (const r of runs) {
    let new_minutes = Number(r.minutes_worked || 0);
    if (r.job_id) {
      const sum = await sumJobWorkerMinutes(env, r.job_id, r.ended_at || now(), false);
      if ((sum.segments || []).length > 0) {
        new_minutes = sum.total_minutes;
      }
    }
    const old_minutes = Number(r.minutes_worked || 0);
    const diff = Math.round((new_minutes - old_minutes) * 10) / 10;
    diffs.push({
      run_id: r.id,
      issue_id: r.issue_id,
      job_id: r.job_id || '',
      handler_name: r.handler_name || r.handler_id || '',
      started_at: r.started_at || '',
      ended_at: r.ended_at || '',
      old_minutes,
      new_minutes,
      diff
    });
    if (r.issue_id) issueIds.add(r.issue_id);
  }

  if (!dry_run) {
    const t = now();
    for (const d of diffs) {
      if (Math.abs(d.diff) < 0.05) continue;
      await env.DB.prepare(
        "UPDATE v2_issue_handle_runs SET minutes_worked=? WHERE id=?"
      ).bind(d.new_minutes, d.run_id).run();
    }
    // 重算每个 issue 的 total_minutes_worked
    const issueTotals = [];
    for (const iid of issueIds) {
      const rrs = await env.DB.prepare(
        "SELECT minutes_worked FROM v2_issue_handle_runs WHERE issue_id=? AND run_status='completed'"
      ).bind(iid).all();
      const total = Math.round((rrs.results || []).reduce((s, r) => s + (Number(r.minutes_worked) || 0), 0) * 10) / 10;
      await env.DB.prepare(
        "UPDATE v2_issue_tickets SET total_minutes_worked=?, updated_at=? WHERE id=?"
      ).bind(total, t, iid).run();
      issueTotals.push({ issue_id: iid, total_minutes: total });
    }
    return json({ ok: true, dry_run: false, updated: diffs.length, diffs, issue_totals: issueTotals });
  }

  return json({ ok: true, dry_run: true, count: diffs.length, diffs, issue_count: issueIds.size });
});




// =====================================================
// 管理员强制退出 — 关闭 v2_ops_job_workers 中遗留的未关闭参与段
//   只 close 工时段，不删除原始数据，不创建 ops_job_results
//   仅 ADMINKEY 允许；OPSKEY/VIEWKEY 拒绝
// =====================================================
// UTC ISO → "YYYY-MM-DD HH:mm" KST 显示串
function _kstFmt(iso) {
  if (!iso) return '';
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return '';
  const d = new Date(ms + 9 * 3600000);
  return d.toISOString().slice(0, 16).replace('T', ' ');
}

route("v2_admin_force_worker_leave", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const job_id = String(body.job_id || "").trim();
  const worker_id = String(body.worker_id || "").trim();
  const segment_id = String(body.segment_id || "").trim();
  const reason = String(body.reason || "").trim();
  const operator_name = String(body.operator_name || "").trim();
  const close_mode = String(body.close_mode || "now").trim();
  // 兼容两个字段：force_left_at（推荐）/ leave_at（旧）
  const customLeaveAt = String(body.force_left_at || body.leave_at || "").trim();

  if (!job_id) return err("missing job_id");
  if (!worker_id) return err("missing worker_id");
  if (!reason) return err("missing reason");
  if (!operator_name) return err("missing operator_name");

  return withIdem(env, body, "v2_admin_force_worker_leave", async () => {
    // 定位 open segment：优先 segment_id，否则 job_id+worker_id 最新一条
    let seg = null;
    if (segment_id) {
      seg = await env.DB.prepare(
        "SELECT * FROM v2_ops_job_workers WHERE id=? AND left_at=''"
      ).bind(segment_id).first();
    } else {
      seg = await env.DB.prepare(
        "SELECT * FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at='' ORDER BY joined_at DESC LIMIT 1"
      ).bind(job_id, worker_id).first();
    }
    if (!seg) return { ok: false, error: "already_closed", message: "未找到未关闭参与段（可能已退出）" };

    // 选择退出时间（内部统一用 camelCase 变量，避免与 JSON 字段名 left_at 混淆）
    const job = await env.DB.prepare("SELECT id, status, updated_at FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    let leaveAt = now();
    if (close_mode === "custom") {
      if (!customLeaveAt) return { ok: false, error: "missing_leave_at", message: "close_mode=custom 时必须传 force_left_at" };
      leaveAt = customLeaveAt;
    } else if (close_mode === "job_completed_at") {
      leaveAt = (job && job.updated_at) ? job.updated_at : now();
    }

    const leaveMs = Date.parse(leaveAt);
    const joinMs = Date.parse(seg.joined_at || "");
    if (!Number.isFinite(leaveMs)) {
      return { ok: false, error: "invalid_leave_at", message: "leave_at 解析失败：" + leaveAt };
    }
    if (!Number.isFinite(joinMs)) {
      return { ok: false, error: "invalid_joined_at", message: "joined_at 解析失败：" + (seg.joined_at || '') };
    }
    // 退出时间不能早于开始时间——静默 max(0,...) 会掩盖填错时间
    if (leaveMs < joinMs) {
      return {
        ok: false,
        error: "force_left_before_joined",
        message: "强制退出时间不能早于开始时间",
        joined_at: seg.joined_at || "",
        joined_at_kst: _kstFmt(seg.joined_at || ""),
        left_at: leaveAt,
        left_at_kst: _kstFmt(leaveAt)
      };
    }

    const minutesWorked = Math.round(((leaveMs - joinMs) / 60000) * 10) / 10;

    const fullReason = "admin_force_leave: " + reason + " | operator=" + operator_name;
    await env.DB.prepare(
      "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason=? WHERE id=?"
    ).bind(leaveAt, minutesWorked, fullReason, seg.id).run();

    // 重算 active_worker_count；若 job 还在 working/pending 且无 open，可降级 awaiting_close
    const t = now();
    const real = await recalcActiveCount(env, job_id, t);
    if (job && real === 0) {
      const cur = String(job.status || "");
      if (cur === "working" || cur === "pending") {
        await env.DB.prepare(
          "UPDATE v2_ops_jobs SET status='awaiting_close', updated_at=? WHERE id=?"
        ).bind(t, job_id).run();
      }
      // completed 不动
    }

    return {
      ok: true,
      closed_segment_id: seg.id,
      worker_id: seg.worker_id || worker_id,
      worker_name: seg.worker_name || "",
      job_id,
      joined_at: seg.joined_at || "",
      joined_at_kst: _kstFmt(seg.joined_at || ""),
      left_at: leaveAt,
      left_at_kst: _kstFmt(leaveAt),
      minutes_worked: minutesWorked
    };
  });
});

// =====================================================
// 一次性脏数据清理：v2_ops_jobs.status='completed' 但 v2_ops_job_workers.left_at='' 的残留段
// 原因：历史 finish action 未统一关闭所有人员段；老脏数据不会自动消失
// 仅 ADMINKEY 可调；自动按 job.updated_at（首选）或 now 兜底关闭
// =====================================================
route("v2_admin_cleanup_completed_open_segments", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const dryRun = body.dry_run === true;
  const t = now();
  const rs = await env.DB.prepare(`
    SELECT w.id AS segment_id, w.job_id, w.worker_id, w.worker_name, w.joined_at,
           j.status AS job_status, j.job_type, j.related_doc_id, j.updated_at AS job_updated_at
      FROM v2_ops_job_workers w
      JOIN v2_ops_jobs j ON j.id = w.job_id
     WHERE w.left_at='' AND j.status='completed'
     ORDER BY w.joined_at ASC
  `).all();
  const rows = rs.results || [];
  const examples = [];
  const touchedJobIds = new Set();
  let cleaned = 0;
  for (const r of rows) {
    const closeAt = r.job_updated_at || t;
    const joinedMs = Date.parse(r.joined_at || '');
    const leftMs = Date.parse(closeAt);
    let minutes = 0;
    if (Number.isFinite(joinedMs) && Number.isFinite(leftMs)) {
      minutes = Math.max(0, Math.round((leftMs - joinedMs) / 60000 * 10) / 10);
    }
    if (!dryRun) {
      await env.DB.prepare(
        "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason='admin_cleanup_completed_job' WHERE id=?"
      ).bind(closeAt, minutes, r.segment_id).run();
      touchedJobIds.add(r.job_id);
    }
    cleaned++;
    if (examples.length < 20) {
      examples.push({
        segment_id: r.segment_id,
        job_id: r.job_id,
        job_type: r.job_type,
        related_doc_id: r.related_doc_id || '',
        worker_id: r.worker_id || '',
        worker_name: r.worker_name || '',
        joined_at: r.joined_at || '',
        will_close_at: closeAt,
        will_minutes: minutes
      });
    }
  }
  // 同步把这些 job 的 active_worker_count 修为 0
  if (!dryRun) {
    for (const jid of touchedJobIds) {
      await env.DB.prepare(
        "UPDATE v2_ops_jobs SET active_worker_count=0, updated_at=? WHERE id=?"
      ).bind(t, jid).run();
    }
  }
  // 兜底：把所有"反馈仍 field_working 但关联 job 已 completed"的反馈推进到 unloaded_pending_info
  let feedbackHealed = 0;
  if (!dryRun) {
    const fbRs = await env.DB.prepare(`
      SELECT fb.id AS fb_id
        FROM v2_field_feedbacks fb
        JOIN v2_ops_jobs j ON j.id = fb.related_doc_id
       WHERE fb.feedback_type='unplanned_unload' AND fb.status='field_working' AND j.status='completed'
    `).all();
    for (const fb of (fbRs.results || [])) {
      await env.DB.prepare(
        "UPDATE v2_field_feedbacks SET status='unloaded_pending_info', updated_at=? WHERE id=? AND status='field_working'"
      ).bind(t, fb.fb_id).run();
      feedbackHealed++;
    }
  }
  return json({
    ok: true,
    dry_run: dryRun,
    cleaned_count: cleaned,
    affected_job_count: touchedJobIds.size,
    feedback_healed_count: feedbackHealed,
    examples
  });
});

// =====================================================
// 一次性 job status 收尾：pending/working/awaiting_close 但已无 open worker 的，
// 按 isTimeOnlyJobType 分流；ADMINKEY；支持 dry_run
// =====================================================
// 协同中心：单条入库计划状态自愈
route("v2_inbound_plan_repair_state", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");
  return withIdem(env, body, "v2_inbound_plan_repair_state", async () => {
    const r = await repairInboundPlanWorkState(env, id, String(body.reason || 'manual_repair'));
    if (!r) return { ok: false, error: "not_found" };
    return { ok: true, ...r };
  });
});

// 一次性扫描所有 unloading/unloading_putting_away/putting_away 入库计划，无 active job 的修复
route("v2_admin_cleanup_inbound_plan_states", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const dryRun = body.dry_run === true;
  const rs = await env.DB.prepare(`
    SELECT id, display_no, status
      FROM v2_inbound_plans
     WHERE status IN ('unloading','unloading_putting_away','putting_away')
     ORDER BY created_at ASC
     LIMIT 5000
  `).all();
  const rows = rs.results || [];
  let checked = 0, repaired = 0, kept = 0;
  const examples = [];
  for (const p of rows) {
    checked++;
    // 干跑：先模拟（不写库）：实际还是会被 repairInboundPlanWorkState 写入
    // 简化：dryRun 时跳过 helper，直接探测应转目标
    if (dryRun) {
      const hasActiveUnload = await env.DB.prepare(
        `SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status IN ('pending','working','awaiting_close') LIMIT 1`
      ).bind(p.id).first();
      const hasActivePutaway = await env.DB.prepare(
        `SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type IN ('inbound_direct','inbound_bulk','inbound_change_order') AND status IN ('pending','working','awaiting_close') LIMIT 1`
      ).bind(p.id).first();
      if (p.status === 'unloading' && !hasActiveUnload) {
        repaired++;
        if (examples.length < 30) examples.push({ id: p.id, display_no: p.display_no, old_status: p.status, would_repair: true });
      } else if (p.status === 'unloading_putting_away' && !hasActiveUnload) {
        repaired++;
        if (examples.length < 30) examples.push({ id: p.id, display_no: p.display_no, old_status: p.status, would_repair: true });
      } else if (p.status === 'putting_away' && !hasActivePutaway) {
        repaired++;
        if (examples.length < 30) examples.push({ id: p.id, display_no: p.display_no, old_status: p.status, would_repair: true });
      } else {
        kept++;
      }
      continue;
    }
    const r = await repairInboundPlanWorkState(env, p.id, 'cleanup_inbound_states');
    if (r && r.repaired) {
      repaired++;
      if (examples.length < 30) examples.push({
        id: p.id, display_no: p.display_no,
        old_status: r.old_status, new_status: r.new_status, reason: r.reason
      });
    } else {
      kept++;
    }
  }
  return json({ ok: true, dry_run: dryRun, checked_count: checked, repaired_count: repaired, kept_count: kept, examples });
});

// 卸货范围修正：处理"多业务类型 plan 被分别要求卸货" 历史脏数据
// 规则：一张计划只需一次到仓卸货；卸完即整单进入业务入库阶段
// 扫描所有 plan，遇到以下情况修复：
//   (a) 已有 completed unload job，但 plan.status 仍是 pending/unloading/unloading_putting_away
//       → 根据 biz_task 完成度回算（completed/partially_completed/putting_away/arrived_pending_putaway）
//   (b) plan 有多 biz_classes，存在某 biz 已 completed，但 plan.status 没体现 partially_completed/completed
//       → 回算
// 干跑模式 dry_run=true 仅返回 would_repair examples，不写库
route("v2_admin_cleanup_inbound_unload_scope", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const dryRun = body.dry_run === true;
  const t = now();
  // 扫描所有非 cancelled / 非 completed / 非 return_session 计划
  const rs = await env.DB.prepare(`
    SELECT id, display_no, status, biz_class, biz_classes_json, source_type, updated_at
      FROM v2_inbound_plans
     WHERE status NOT IN ('cancelled')
       AND (source_type IS NULL OR source_type != 'return_session')
     ORDER BY created_at ASC
     LIMIT 10000
  `).all();
  const rows = rs.results || [];
  let checked_count = 0, repaired_count = 0, kept_count = 0;
  const examples = [];

  for (const p of rows) {
    checked_count++;

    const biz_classes = extractPlanBizClasses(p);
    if (biz_classes.length === 0) { kept_count++; continue; }

    // 已完成的 unload job 数 + 各 biz_task 状态
    const unloadDone = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status='completed' LIMIT 1"
    ).bind(p.id).first();
    const activeUnload = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(p.id).first();
    const activePutaway = await env.DB.prepare(
      "SELECT id FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type IN ('inbound_direct','inbound_bulk','inbound_change_order') AND status IN ('pending','working','awaiting_close') LIMIT 1"
    ).bind(p.id).first();

    // 确保 biz_task 行齐全
    if (!dryRun) await ensureInboundPlanBizTasks(env, p);
    const tasks = await listInboundPlanBizTasks(env, p.id);
    const completedCnt = tasks.filter(x => x.status === 'completed').length;
    const taskCnt = tasks.length;
    const allCompleted = (taskCnt > 0 && completedCnt === taskCnt);
    const someCompleted = (completedCnt > 0 && completedCnt < taskCnt);

    // 推断目标状态
    let target = p.status;
    if (activeUnload) {
      target = activePutaway ? 'unloading_putting_away' : 'unloading';
    } else if (unloadDone || ['arrived_pending_putaway','putting_away','partially_completed','completed'].indexOf(p.status) !== -1) {
      // 物理卸货已完成
      if (allCompleted) target = 'completed';
      else if (someCompleted) target = 'partially_completed';
      else if (activePutaway) target = 'putting_away';
      else target = 'arrived_pending_putaway';
    } else {
      target = 'pending';
    }

    if (target === p.status) { kept_count++; continue; }

    if (dryRun) {
      repaired_count++;
      if (examples.length < 50) examples.push({
        id: p.id, display_no: p.display_no, biz_classes,
        old_status: p.status, would_set_status: target,
        unload_done: !!unloadDone, active_unload: !!activeUnload, active_putaway: !!activePutaway,
        biz_task_completed: completedCnt, biz_task_total: taskCnt
      });
      continue;
    }

    const sets = ["status=?", "updated_at=?"];
    const binds = [target, t];
    if (target === 'completed') {
      sets.push("manual_completed_at=COALESCE(NULLIF(manual_completed_at,''), ?)");
      binds.push(t);
    }
    binds.push(p.id);
    await env.DB.prepare(
      "UPDATE v2_inbound_plans SET " + sets.join(', ') + " WHERE id=?"
    ).bind(...binds).run();
    repaired_count++;
    if (examples.length < 50) examples.push({
      id: p.id, display_no: p.display_no, biz_classes,
      old_status: p.status, new_status: target,
      unload_done: !!unloadDone, biz_task_completed: completedCnt, biz_task_total: taskCnt
    });

    // 审计日志
    await env.DB.prepare(`
      INSERT INTO v2_admin_cleanup_logs(id, operator, action_type, target_job_id, target_worker_id, reason, detail_json, created_at)
      VALUES(?, ?, 'cleanup_inbound_unload_scope', '', '', ?, ?, ?)
    `).bind("CLN-" + uid(), String(body.operator || 'admin'),
            'plan_status: ' + p.status + ' -> ' + target,
            JSON.stringify({ plan_id: p.id, display_no: p.display_no, biz_classes,
                            unload_done: !!unloadDone, biz_task_completed: completedCnt, biz_task_total: taskCnt }),
            t).run();
  }

  return json({ ok: true, dry_run: dryRun, checked_count, repaired_count, kept_count, examples });
});

// 把历史入库计划的 unload_completed_at / unload_completed_by 从已完成的 unload job 回填
// — 仅处理 unload_completed_at 为空、且存在 status='completed' 的 unload job 的 plan
// — 取最后一个 completed unload job 的 updated_at 作为完成时间
route("v2_admin_backfill_inbound_unload_completed_at", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const dryRun = body.dry_run === true;
  const t = now();
  const rs = await env.DB.prepare(`
    SELECT id, display_no, status FROM v2_inbound_plans
     WHERE (unload_completed_at IS NULL OR unload_completed_at='')
       AND (source_type IS NULL OR source_type != 'return_session')
       AND COALESCE(is_deleted,0)=0
     ORDER BY created_at ASC
     LIMIT 10000
  `).all();
  const rows = rs.results || [];
  let checked_count = 0, updated_count = 0;
  const examples = [];
  for (const p of rows) {
    checked_count++;
    // 取最后一个 completed unload job（更精确的"卸货完成时间"= updated_at）
    const job = await env.DB.prepare(
      "SELECT id, updated_at FROM v2_ops_jobs WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND job_type='unload' AND status='completed' ORDER BY updated_at DESC LIMIT 1"
    ).bind(p.id).first();
    if (!job) continue;
    // 汇总工人名（卸货人员）
    const wkRs = await env.DB.prepare(
      "SELECT DISTINCT worker_name FROM v2_ops_job_workers WHERE job_id=? AND worker_name != ''"
    ).bind(job.id).all();
    const names = (wkRs.results || []).map(w => w.worker_name).filter(Boolean);
    const namesText = names.join(", ");
    const finishedAt = job.updated_at || t;
    if (dryRun) {
      updated_count++;
      if (examples.length < 50) examples.push({
        id: p.id, display_no: p.display_no, would_set_unload_completed_at: finishedAt, would_set_unload_completed_by: namesText
      });
      continue;
    }
    await env.DB.prepare(
      "UPDATE v2_inbound_plans SET unload_completed_at=?, unload_completed_by=?, updated_at=? WHERE id=?"
    ).bind(finishedAt, namesText, t, p.id).run();
    updated_count++;
    if (examples.length < 50) examples.push({
      id: p.id, display_no: p.display_no, unload_completed_at: finishedAt, unload_completed_by: namesText
    });
  }
  return json({ ok: true, dry_run: dryRun, checked_count, updated_count, examples });
});

route("v2_admin_cleanup_job_statuses", async (body, env) => {
  if (!isAdmin(body, env)) return err("unauthorized_admin_only", 401);
  const dryRun = body.dry_run === true;
  const t = now();
  const tMs = Date.parse(t);
  const rs = await env.DB.prepare(`
    SELECT id, job_type, status, finished_at, updated_at, created_at
      FROM v2_ops_jobs
     WHERE status IN ('pending','working','awaiting_close')
     ORDER BY created_at ASC
     LIMIT 5000
  `).all();
  const rows = rs.results || [];
  let checked_count = 0;
  let completed_time_only_count = 0;
  let awaiting_close_count = 0;
  let completed_with_result_count = 0;
  let kept_active_count = 0;
  let scan_verify_completed_count = 0;
  let scan_verify_timeout_closed_count = 0;
  let verify_batch_completed_count = 0;
  const examples = [];
  for (const j of rows) {
    checked_count++;
    const openCnt = await env.DB.prepare(
      "SELECT COUNT(*) AS c FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
    ).bind(j.id).first();
    const openCount = Number((openCnt && openCnt.c) || 0);

    // 取 worker 最后 left_at 作为 finished_at 兜底
    const lastSeg = await env.DB.prepare(
      "SELECT left_at FROM v2_ops_job_workers WHERE job_id=? AND left_at!='' ORDER BY left_at DESC LIMIT 1"
    ).bind(j.id).first();
    const fallbackFinishedAt = j.finished_at || (lastSeg && lastSeg.left_at) || j.updated_at || j.created_at || t;

    // ===== 扫码核对专项：仅工时 + 48h 强制结束 =====
    if (j.job_type === 'verify_scan') {
      const baseMs = Date.parse(j.created_at || j.updated_at || t);
      const timedOut = Number.isFinite(baseMs) && Number.isFinite(tMs) && (tMs - baseMs >= VERIFY_SCAN_TIMEOUT_MS);
      // open_worker=0 → 直接 completed；open_worker>0 但已超 48h → 关 segment + completed
      // open_worker>0 且未超时 → 保留 working
      if (openCount === 0) {
        scan_verify_completed_count++;
        const action = 'scan_verify_completed_time_only';
        if (!dryRun) {
          await env.DB.prepare(`
            UPDATE v2_ops_jobs
               SET status='completed', finished_at=?, updated_at=?, active_worker_count=0,
                   result_summary=COALESCE(NULLIF(result_summary,''), '扫码核对：仅记录工时，无数量结果'),
                   cleanup_note='auto_completed_scan_verify_no_open_workers'
             WHERE id=?
          `).bind(fallbackFinishedAt, t, j.id).run();
          if (await _completeVerifyBatchIfLinked(env, j.id, fallbackFinishedAt, 'cleanup_no_open_workers')) verify_batch_completed_count++;
        }
        if (examples.length < 30) examples.push({ id: j.id, job_type: j.job_type, old_status: j.status, action, finished_at: fallbackFinishedAt });
        continue;
      }
      if (timedOut) {
        scan_verify_timeout_closed_count++;
        const action = 'scan_verify_timeout_closed_48h';
        // cutoff 时间：base_time + 48h；若已超过 now 则取 now
        const cutoffMs = Math.min(tMs, baseMs + VERIFY_SCAN_TIMEOUT_MS);
        const cutoffIso = new Date(cutoffMs).toISOString();
        if (!dryRun) {
          // 关闭所有 open segment，按 joined_at 到 cutoff 计算分钟
          const openSegs = await env.DB.prepare(
            "SELECT id, joined_at FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
          ).bind(j.id).all();
          for (const s of (openSegs.results || [])) {
            const jms = Date.parse(s.joined_at || '');
            const mins = (Number.isFinite(jms))
              ? Math.max(0, Math.round((cutoffMs - jms) / 60000 * 10) / 10) : 0;
            await env.DB.prepare(
              "UPDATE v2_ops_job_workers SET left_at=?, minutes_worked=?, leave_reason='auto_closed_scan_verify_after_48h' WHERE id=?"
            ).bind(cutoffIso, mins, s.id).run();
          }
          await env.DB.prepare(`
            UPDATE v2_ops_jobs
               SET status='completed', finished_at=?, updated_at=?, active_worker_count=0,
                   result_summary='扫码核对：仅记录工时，超过48小时自动结束',
                   cleanup_note='auto_closed_scan_verify_after_48h'
             WHERE id=?
          `).bind(cutoffIso, t, j.id).run();
          if (await _completeVerifyBatchIfLinked(env, j.id, cutoffIso, 'cleanup_timeout_48h')) verify_batch_completed_count++;
        }
        if (examples.length < 30) examples.push({ id: j.id, job_type: j.job_type, old_status: j.status, action, finished_at: cutoffIso });
        continue;
      }
      // 未超时且仍有人 → 保持 working
      kept_active_count++;
      continue;
    }
    // ===== 非 verify_scan：仍在岗就跳过 =====
    if (openCount > 0) { kept_active_count++; continue; }

    let action = '';
    if (isTimeOnlyJobType(j.job_type)) {
      completed_time_only_count++;
      action = 'completed_time_only';
      if (!dryRun) {
        await env.DB.prepare(`
          UPDATE v2_ops_jobs
             SET status='completed', finished_at=?, updated_at=?, active_worker_count=0,
                 result_summary=COALESCE(NULLIF(result_summary,''), '仅记录工时，无数量结果'),
                 cleanup_note='auto_completed_time_only_no_open_workers'
           WHERE id=?
        `).bind(fallbackFinishedAt, t, j.id).run();
      }
    } else {
      // 需要产出 / 未识别类型：按 result 是否存在分流
      const resCnt = await env.DB.prepare(
        "SELECT COUNT(*) AS c FROM v2_ops_job_results WHERE job_id=?"
      ).bind(j.id).first();
      const hasResult = Number((resCnt && resCnt.c) || 0) > 0;
      if (hasResult) {
        completed_with_result_count++;
        action = 'completed_with_result';
        if (!dryRun) {
          await env.DB.prepare(`
            UPDATE v2_ops_jobs
               SET status='completed', finished_at=COALESCE(NULLIF(finished_at,''), ?), updated_at=?, active_worker_count=0,
                   cleanup_note='auto_completed_with_existing_result'
             WHERE id=?
          `).bind(fallbackFinishedAt, t, j.id).run();
        }
      } else {
        awaiting_close_count++;
        action = 'awaiting_close';
        if (!dryRun) {
          await env.DB.prepare(`
            UPDATE v2_ops_jobs
               SET status='awaiting_close', updated_at=?, active_worker_count=0,
                   result_summary=COALESCE(NULLIF(result_summary,''), '待补充产出数据'),
                   cleanup_note='awaiting_manual_result_close'
             WHERE id=?
          `).bind(t, j.id).run();
        }
      }
    }

    if (examples.length < 30) {
      examples.push({ id: j.id, job_type: j.job_type, old_status: j.status, action, finished_at: fallbackFinishedAt });
    }
  }
  return json({
    ok: true,
    dry_run: dryRun,
    checked_count,
    kept_active_count,
    completed_time_only_count,
    awaiting_close_count,
    completed_with_result_count,
    scan_verify_completed_count,
    scan_verify_timeout_closed_count,
    verify_batch_completed_count,
    examples
  });
});

// =====================================================
// 数据看板：手动补产出并完成（awaiting_close → completed）
// =====================================================
route("v2_ops_job_manual_finalize", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const reason = String(body.reason || "").trim();
  const operator = String(body.by || body.operator || "ADMIN").trim();
  const customer = String(body.customer || "").trim();
  const result_note = String(body.result_note || "").trim();
  const result_json_in = body.result_json || {};
  if (!job_id) return err("missing job_id");
  if (!reason) return err("missing reason", 400);

  return withIdem(env, body, "v2_ops_job_manual_finalize", async () => {
    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!job) return { ok: false, error: "not_found" };
    if (job.status === 'completed') return { ok: false, error: "already_completed", message: "任务已完成，请走『修改产出』" };
    if (job.status === 'cancelled') return { ok: false, error: "cancelled" };

    const openCnt = await env.DB.prepare(
      "SELECT COUNT(*) AS c FROM v2_ops_job_workers WHERE job_id=? AND left_at=''"
    ).bind(job_id).first();
    if (Number((openCnt && openCnt.c) || 0) > 0) {
      return { ok: false, error: "active_worker_exists_cannot_finalize",
        message: "仍有人员在岗，请先让所有人退出再补录产出" };
    }

    const t = now();
    const result_id = "RES-" + uid();
    const resultObj = (typeof result_json_in === 'object' && result_json_in) ? result_json_in : {};
    if (customer) resultObj.customer = customer;
    if (result_note) resultObj.result_note = result_note;
    const summary = String(body.result_summary || _summarizeResultObj(resultObj) || '手动补录').slice(0, 200);

    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, created_by, created_at, source)
      VALUES(?,?,?,?,?,?,?,?,'manual_finalize')
    `).bind(
      result_id, job_id,
      Number(resultObj.box_count || 0), Number(resultObj.pallet_count || 0),
      String(resultObj.remark || result_note || ''),
      JSON.stringify(resultObj),
      operator, t
    ).run();

    const sets = [
      "status='completed'", "finished_at=?", "updated_at=?", "active_worker_count=0",
      "result_summary=?", "manual_finalized=1", "manual_finalized_by=?",
      "manual_finalized_at=?", "manual_finalize_reason=?"
    ];
    const binds = [t, t, summary, operator, t, reason];
    if (customer) { sets.push("customer=?"); binds.push(customer); }
    binds.push(job_id);
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET " + sets.join(', ') + " WHERE id=?"
    ).bind(...binds).run();

    return { ok: true, job_id, result_id, status: 'completed', summary, finished_at: t };
  });
});

// 简单摘要：取常见数量字段累加
function _summarizeResultObj(r) {
  if (!r || typeof r !== 'object') return '';
  const parts = [];
  const map = [
    ['box_count', '箱'], ['pallet_count', '托'], ['cbm_count', 'CBM'],
    ['container_large_count', '大柜'], ['container_small_count', '小柜'],
    ['operated_box_count', '处理箱'], ['label_count', '标签'],
    ['reboxed_count', '换箱'], ['repaired_box_count', '修箱'],
    ['sku_count', 'SKU'], ['packed_box_count', '打包箱']
  ];
  for (const [k, l] of map) {
    if (Number(r[k] || 0) > 0) parts.push(l + ' ' + Number(r[k]));
  }
  return parts.join(' / ');
}

// =====================================================
// 数据看板：修改已完成 job 的产出（保留历史 / source='manual_correction'）
// =====================================================
route("v2_ops_job_result_update", async (body, env) => {
  if (!isAuth(body, env)) return err("unauthorized", 401);
  const job_id = String(body.job_id || "").trim();
  const reason = String(body.reason || "").trim();
  const operator = String(body.by || body.operator || "ADMIN").trim();
  const customer = String(body.customer || "").trim();
  const result_note = String(body.result_note || "").trim();
  const result_json_in = body.result_json || {};
  if (!job_id) return err("missing job_id");
  if (!reason) return err("missing reason", 400);

  return withIdem(env, body, "v2_ops_job_result_update", async () => {
    const job = await env.DB.prepare("SELECT * FROM v2_ops_jobs WHERE id=?").bind(job_id).first();
    if (!job) return { ok: false, error: "not_found" };
    if (job.status !== 'completed') {
      return { ok: false, error: "only_completed_can_correct",
        message: "仅已完成任务可修改产出，待收尾请走『补充产出并完成』" };
    }

    const t = now();
    // 取最新 result 作为 previous_result_id，便于审计
    const prev = await env.DB.prepare(
      "SELECT id FROM v2_ops_job_results WHERE job_id=? ORDER BY created_at DESC LIMIT 1"
    ).bind(job_id).first();
    const previous_result_id = prev ? prev.id : '';

    const result_id = "RES-" + uid();
    const resultObj = (typeof result_json_in === 'object' && result_json_in) ? result_json_in : {};
    if (customer) resultObj.customer = customer;
    if (result_note) resultObj.result_note = result_note;
    const summary = String(body.result_summary || _summarizeResultObj(resultObj) || '管理员修正').slice(0, 200);

    await env.DB.prepare(`
      INSERT INTO v2_ops_job_results(id, job_id, box_count, pallet_count, remark, result_json, created_by, created_at, source, previous_result_id)
      VALUES(?,?,?,?,?,?,?,?,'manual_correction',?)
    `).bind(
      result_id, job_id,
      Number(resultObj.box_count || 0), Number(resultObj.pallet_count || 0),
      String(resultObj.remark || result_note || ''),
      JSON.stringify(resultObj),
      operator, t,
      previous_result_id
    ).run();

    const sets = [
      "result_summary=?", "result_corrected=1", "result_corrected_by=?",
      "result_corrected_at=?", "result_correct_reason=?", "updated_at=?"
    ];
    const binds = [summary, operator, t, reason, t];
    if (customer) { sets.push("customer=?"); binds.push(customer); }
    binds.push(job_id);
    await env.DB.prepare(
      "UPDATE v2_ops_jobs SET " + sets.join(', ') + " WHERE id=?"
    ).bind(...binds).run();

    return { ok: true, job_id, result_id, previous_result_id, summary };
  });
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
  let where = " WHERE 1=1";
  const binds = [];
  if (status) { where += " AND status=?"; binds.push(status); }
  if (customer_name) { where += " AND customer_name LIKE ?"; binds.push('%' + customer_name + '%'); }
  // created_at 为 UTC ISO；按 KST 日历日筛选
  const _stR2 = kstDayRangeUtc(start_date);
  const _enR2 = kstDayRangeUtc(end_date);
  if (_stR2) { where += " AND created_at >= ?"; binds.push(_stR2.startUtc); }
  if (_enR2) { where += " AND created_at < ?";  binds.push(_enR2.endUtc); }
  const countRow = binds.length > 0
    ? await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_verify_batches" + where).bind(...binds).first()
    : await env.DB.prepare("SELECT COUNT(*) AS c FROM v2_verify_batches" + where).first();
  const total = Number((countRow && countRow.c) || 0);
  const listSql = "SELECT * FROM v2_verify_batches" + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?";
  const rs = await env.DB.prepare(listSql).bind(...binds, limit, offset).all();
  const batches = rs.results || [];
  if (batches.length === 0) return json({ ok: true, items: [], ...pageMeta(total, limit, offset) });

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
  return json({ ok: true, items, ...pageMeta(total, limit, offset) });
});

// 3) 批次详情（按"条码对应计划箱数"出每行状态 + 聚合异常）
route("v2_verify_batch_detail", async (body, env) => {
  if (!isOpsAuth(body, env)) return err("unauthorized", 401);
  const id = String(body.id || "").trim();
  if (!id) return err("missing id");

  const batch = await env.DB.prepare("SELECT * FROM v2_verify_batches WHERE id=?").bind(id).first();
  if (!batch) return err("not found", 404);

  // 默认拉满 5000 条流水，足够覆盖单次核对场景；如果后续超大批次再做 chunk
  const logsLimit = Math.max(500, Math.min(20000, Number(body.scan_logs_limit || 5000)));
  const itemsRs = await env.DB.prepare(
    "SELECT * FROM v2_verify_batch_items WHERE batch_id=? ORDER BY created_at"
  ).bind(id).all();
  const logsRs = await env.DB.prepare(
    "SELECT * FROM v2_verify_scan_logs WHERE batch_id=? ORDER BY scanned_at DESC LIMIT ?"
  ).bind(id, logsLimit).all();
  // 每个 barcode 已扫 ok 次数
  const okByBcRs = await env.DB.prepare(
    "SELECT barcode, COUNT(*) AS c FROM v2_verify_scan_logs WHERE batch_id=? AND scan_result='ok' GROUP BY barcode"
  ).bind(id).all();
  // 流水真实总条数（用于前端判断是否截断）
  const totalLogsRs = await env.DB.prepare(
    "SELECT COUNT(*) AS c FROM v2_verify_scan_logs WHERE batch_id=?"
  ).bind(id).first();
  const total_scan_logs = Number((totalLogsRs && totalLogsRs.c) || 0);

  const itemsRaw = itemsRs.results || [];
  const logs = logsRs.results || [];
  const okByBc = {};
  (okByBcRs.results || []).forEach(r => { okByBc[r.barcode] = Number(r.c || 0); });

  // 预聚合：每个 barcode 的托盘集合 / 最后扫描时间&人
  const barcodeMeta = {};
  for (const l of logs) {
    if (!l || !l.barcode) continue;
    const m = barcodeMeta[l.barcode] || (barcodeMeta[l.barcode] = {
      pallets: new Set(),
      pallet_scanned: {},  // pallet_no -> ok_count
      last_scanned_at: '', last_scanned_by: ''
    });
    if (l.pallet_no) m.pallets.add(l.pallet_no);
    if (l.scan_result === 'ok') {
      const p = l.pallet_no || '';
      m.pallet_scanned[p] = (m.pallet_scanned[p] || 0) + 1;
    }
    // logs 已按 scanned_at DESC，第一条即最近
    if (!m.last_scanned_at) {
      m.last_scanned_at = l.scanned_at || '';
      m.last_scanned_by = l.worker_name || l.worker_id || '';
    }
  }

  // 每个 item 的状态（计划内条码）
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
    const meta = barcodeMeta[it.barcode] || null;
    return {
      id: it.id,
      barcode: it.barcode,
      customer_name: it.customer_name || '',
      planned_box_count: planned,
      scanned_ok_count: ok,
      diff_count: ok - planned,
      status: st,
      pallet_numbers: meta ? Array.from(meta.pallets).sort() : [],
      last_scanned_at: meta ? meta.last_scanned_at : '',
      last_scanned_by: meta ? meta.last_scanned_by : ''
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
    if (!palletMap[p]) palletMap[p] = {
      pallet_no: p, scanned_ok_count: 0, abnormal_count: 0,
      not_found_count: 0, overflow_count: 0, duplicate_count: 0
    };
    if (l.scan_result === 'ok') palletMap[p].scanned_ok_count++;
    else {
      palletMap[p].abnormal_count++;
      if (l.scan_result === 'not_found') palletMap[p].not_found_count++;
      else if (l.scan_result === 'overflow') palletMap[p].overflow_count++;
      else if (l.scan_result === 'duplicate') palletMap[p].duplicate_count++;
    }
    const matched = itemMap[l.barcode];
    return { ...l, customer_name: matched ? (matched.customer_name || '') : '' };
  });
  // 异常托盘排前面；同档按总扫描数降序
  const pallet_summary = Object.values(palletMap).sort((a, b) => {
    if (b.abnormal_count !== a.abnormal_count) return b.abnormal_count - a.abnormal_count;
    return (b.scanned_ok_count + b.abnormal_count) - (a.scanned_ok_count + a.abnormal_count);
  });

  // 计划外（非本批次）条码合成行 — 让"按条码核对"也能看到 not_found
  const extraMap = {};
  for (const l of enrichedLogs) {
    if (l.scan_result !== 'not_found') continue;
    if (!l.barcode) continue;
    const k = l.barcode;
    if (!extraMap[k]) {
      extraMap[k] = {
        id: 'EXTRA-' + k,
        barcode: k,
        customer_name: '',
        planned_box_count: 0,
        scanned_ok_count: 0,
        not_found_count: 0,
        diff_count: 0,
        status: 'not_in_batch',
        pallet_numbers: new Set(),
        last_scanned_at: l.scanned_at || '',
        last_scanned_by: l.worker_name || l.worker_id || ''
      };
    }
    extraMap[k].not_found_count++;
    if (l.pallet_no) extraMap[k].pallet_numbers.add(l.pallet_no);
  }
  const extra_items = Object.values(extraMap).map(x => Object.assign({}, x, {
    pallet_numbers: Array.from(x.pallet_numbers).sort(),
    diff_count: x.not_found_count  // 视为正向超扫量，便于前端展示
  }));
  const not_in_batch_count = extra_items.length;

  const abnormal_count = shortage_count + overflow_count_items + log_not_found + not_scanned_count;

  return json({
    ok: true,
    batch,
    items,
    extra_items,
    scan_logs: enrichedLogs,
    total_scan_logs,
    scan_logs_truncated: total_scan_logs > enrichedLogs.length,
    summary: {
      planned_total_box_count,
      scanned_ok_total_count,
      // 条码级异常条数
      ok_count: ok_items,
      shortage_count,
      overflow_count: overflow_count_items,
      not_scanned_count,
      not_in_batch_count,
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
    if (job.status === 'completed') {
      const cleaned = await closeOpenWorkerSegmentsForJob(env, job_id, t, 'already_completed_cleanup');
      return { ok: true, already_completed: true, error: "already_completed", cleaned_open_segments: cleaned, message: "核对任务已完成" };
    }

    // 关闭本人 segment
    await closeAllOpenSegs(env, job_id, worker_id, t, 'finished');

    if (complete_job) {
      // 防御性收口 — 关闭所有遗留 open segment
      await closeOpenWorkerSegmentsForJob(env, job_id, t, 'job_completed');

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
  const status = String(body.status || "").trim();

  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 100;
  if (limit > 500) limit = 500;
  let offset = parseInt(body.offset, 10);
  if (!Number.isFinite(offset) || offset < 0) offset = 0;

  let where = "WHERE 1=1";
  const binds = [];
  const startRange = kstDayRangeUtc(start_date);
  const endRange = kstDayRangeUtc(end_date);
  if (startRange) { where += " AND j.created_at >= ?"; binds.push(startRange.startUtc); }
  if (endRange)   { where += " AND j.created_at < ?"; binds.push(endRange.endUtc); }
  if (flow_stage) { where += " AND j.flow_stage=?"; binds.push(flow_stage); }
  if (job_type)   { where += " AND j.job_type=?"; binds.push(job_type); }
  if (status)     { where += " AND j.status=?"; binds.push(status); }
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
      (SELECT MAX(w2.left_at) FROM v2_ops_job_workers w2 WHERE w2.job_id=j.id AND w2.left_at!='') AS ended_at
    FROM v2_ops_jobs j ${where}
    ORDER BY j.created_at DESC LIMIT ? OFFSET ?`;
  const rs = await env.DB.prepare(sql).bind(...binds, limit, offset).all();
  const baseItems = rs.results || [];
  if (baseItems.length === 0) return json({ ok: true, items: [], total, limit, offset });

  // 批量取 results → 调 parseOpsResultForExport 生成业务摘要
  const jobIds = baseItems.map(j => j.id);
  const resultsAll = await batchSelectInGlobal(env,
    `SELECT job_id, box_count, pallet_count, remark, diff_note, result_json, result_lines_json, created_by, created_at FROM v2_ops_job_results WHERE job_id IN (PLACEHOLDER)`,
    jobIds);
  const resultsByJob = {};
  resultsAll.forEach(r => { (resultsByJob[r.job_id] = resultsByJob[r.job_id] || []).push(r); });

  const items = baseItems.map(j => {
    const parsed = parseOpsResultForExport(j.job_type, resultsByJob[j.id] || []);
    return Object.assign({}, j, {
      total_minutes: round1(j.total_minutes),
      box_count_sum: parsed.box_count_sum,
      pallet_count_sum: parsed.pallet_count_sum,
      packed_box_count_sum: parsed.packed_box_count_sum,
      total_operated_box_count_sum: parsed.total_operated_box_count_sum,
      label_count_sum: parsed.label_count_sum,
      result_summary: parsed.result_summary,
      result_remarks_short: (parsed.result_notes || '').slice(0, 100)
    });
  });
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

  const results = resultsRs.results || [];
  const parsed = parseOpsResultForExport(job.job_type, results);

  // 关联单据的备注（让单子详情能直接看到客服备注 / 出库要求 等）
  let inbound_remark = '';
  let inbound_force_completed = 0, inbound_force_completed_by = '', inbound_force_completed_at = '', inbound_force_complete_reason = '';
  let outbound_requirement = '', ob_instruction = '', ob_remark = '', ob_pickup_note = '';
  const isInboundLink = (t) => (t === 'inbound_plan' || t === 'inbound');
  const isOutboundRelType = (t) => (t === 'outbound' || t === 'outbound_order');
  if (isInboundLink(job.related_doc_type) && job.related_doc_id) {
    const ib = await env.DB.prepare(
      "SELECT remark, force_completed, force_completed_by, force_completed_at, force_complete_reason FROM v2_inbound_plans WHERE id=?"
    ).bind(job.related_doc_id).first();
    if (ib) {
      inbound_remark = ib.remark || '';
      inbound_force_completed = Number(ib.force_completed) === 1 ? 1 : 0;
      inbound_force_completed_by = ib.force_completed_by || '';
      inbound_force_completed_at = ib.force_completed_at ? fmtKst(ib.force_completed_at) : '';
      inbound_force_complete_reason = ib.force_complete_reason || '';
    }
  }
  const obId = (isOutboundRelType(job.related_doc_type) && job.related_doc_id) ? job.related_doc_id : (job.linked_outbound_order_id || '');
  if (obId) {
    const ob = await env.DB.prepare(
      "SELECT outbound_requirement, instruction, remark, pickup_note FROM v2_outbound_orders WHERE id=?"
    ).bind(obId).first();
    if (ob) {
      outbound_requirement = ob.outbound_requirement || '';
      ob_instruction = ob.instruction || '';
      ob_remark = ob.remark || '';
      ob_pickup_note = ob.pickup_note || '';
    }
  }

  return json({
    ok: true,
    job,
    workers: workersRs.results || [],
    results,
    pick_worker_docs: pickDocsRs.results || [],
    parsed,
    inbound_remark,
    inbound_force_completed,
    inbound_force_completed_by,
    inbound_force_completed_at,
    inbound_force_complete_reason,
    outbound_requirement,
    ob_instruction,
    ob_remark,
    ob_pickup_note
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
  const status = String(body.status || "").trim();

  let limit = parseInt(body.limit, 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 5000;
  if (limit > 10000) limit = 10000;

  let where = "WHERE 1=1";
  const binds = [];
  const startRange = kstDayRangeUtc(start_date);
  const endRange = kstDayRangeUtc(end_date);
  if (startRange) { where += " AND j.created_at >= ?"; binds.push(startRange.startUtc); }
  if (endRange)   { where += " AND j.created_at < ?"; binds.push(endRange.endUtc); }
  if (flow_stage) { where += " AND j.flow_stage=?"; binds.push(flow_stage); }
  if (job_type)   { where += " AND j.job_type=?"; binds.push(job_type); }
  if (status)     { where += " AND j.status=?"; binds.push(status); }
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
  // 兼容历史 related_doc_type 写法：实际写入是 'inbound_plan'，旧导出按 'inbound' 过滤导致永远空
  const isInboundLink = (t) => (t === 'inbound_plan' || t === 'inbound');
  const inboundIds = [...new Set(jobs.filter(j => isInboundLink(j.related_doc_type) && j.related_doc_id).map(j => j.related_doc_id))];
  const outboundIds = [...new Set(jobs.flatMap(j => {
    const ids = [];
    // 兼容 'outbound' 和 'outbound_order' 两种 related_doc_type 写法
    if ((j.related_doc_type === 'outbound' || j.related_doc_type === 'outbound_order') && j.related_doc_id) ids.push(j.related_doc_id);
    if (j.linked_outbound_order_id) ids.push(j.linked_outbound_order_id);
    return ids;
  }))];
  const pickJobIds = jobs.filter(j => j.job_type === 'pick_direct').map(j => j.id);

  const [workersAll, resultsAll, pickAll, inboundAll, outboundAll] = await Promise.all([
    batchSelectInGlobal(env,
      `SELECT id, job_id, worker_id, worker_name, joined_at, left_at, minutes_worked, leave_reason
       FROM v2_ops_job_workers WHERE job_id IN (PLACEHOLDER) ORDER BY joined_at ASC`,
      jobIds),
    batchSelectInGlobal(env,
      `SELECT id, job_id, box_count, pallet_count, remark, diff_note, result_json, result_lines_json, created_by, created_at
       FROM v2_ops_job_results WHERE job_id IN (PLACEHOLDER) ORDER BY created_at ASC`,
      jobIds),
    batchSelectInGlobal(env,
      `SELECT id, job_id, worker_id, worker_name, pick_doc_no, status, started_at, finished_at, minutes_worked
       FROM v2_pick_worker_docs WHERE job_id IN (PLACEHOLDER) ORDER BY started_at ASC`,
      pickJobIds),
    batchSelectInGlobal(env,
      `SELECT id, customer, display_no, external_inbound_no, biz_class, biz_classes_json, accounted, accounted_by, accounted_at,
              remark, force_completed, force_completed_by, force_completed_at, force_complete_reason
       FROM v2_inbound_plans WHERE id IN (PLACEHOLDER)`,
      inboundIds),
    batchSelectInGlobal(env,
      `SELECT id, customer, display_no, destination, po_no, wms_work_order_no,
              planned_box_count, planned_pallet_count, actual_box_count, actual_pallet_count,
              accounted, accounted_by, accounted_at,
              biz_class, status, uses_stock_operation, expected_ship_at, outbound_requirement,
              instruction, remark, pickup_note,
              stock_operation_status, stock_operation_completed_at, stock_operation_completed_by
       FROM v2_outbound_orders WHERE id IN (PLACEHOLDER)`,
      outboundIds)
  ]);

  const workersByJob = {}, resultsByJob = {}, pickByJob = {};
  workersAll.forEach(w => { (workersByJob[w.job_id] = workersByJob[w.job_id] || []).push(w); });
  resultsAll.forEach(r => { (resultsByJob[r.job_id] = resultsByJob[r.job_id] || []).push(r); });
  pickAll.forEach(p => { (pickByJob[p.job_id] = pickByJob[p.job_id] || []).push(p); });
  const inboundById = {}, outboundById = {};
  inboundAll.forEach(d => { inboundById[d.id] = d; });
  outboundAll.forEach(d => { outboundById[d.id] = d; });

  // 入库计划业务类型 / 完成 / 未完成（多业务类型 V1.2）
  const inboundBizTasksAll = inboundIds.length > 0 ? await batchSelectInGlobal(env,
    `SELECT plan_id, biz_class, status FROM v2_inbound_plan_biz_tasks WHERE plan_id IN (PLACEHOLDER)`,
    inboundIds) : [];
  const inboundBizTasksByPlan = {};
  inboundBizTasksAll.forEach(t => { (inboundBizTasksByPlan[t.plan_id] = inboundBizTasksByPlan[t.plan_id] || []).push(t); });

  // 出库资料份数（attachment_category='outbound_material'）
  const outboundMaterialCntMap = {};
  if (outboundIds.length > 0) {
    const matRows = await batchSelectInGlobal(env,
      `SELECT related_doc_id AS id, COUNT(*) AS c FROM v2_attachments
        WHERE related_doc_type='outbound_order' AND attachment_category='outbound_material'
          AND related_doc_id IN (PLACEHOLDER) GROUP BY related_doc_id`,
      outboundIds);
    matRows.forEach(r => { outboundMaterialCntMap[r.id] = Number(r.c || 0); });
  }

  const out = jobs.map(j => {
    const ws = workersByJob[j.id] || [];
    const rs = resultsByJob[j.id] || [];
    const ps = pickByJob[j.id] || [];

    const workerNames = [...new Set(ws.map(w => w.worker_name).filter(Boolean))].join('、');
    const total_minutes = round1(ws.reduce((s, w) => s + (Number(w.minutes_worked) || 0), 0));
    const started_at_iso = ws.reduce((m, w) => (!m || (w.joined_at && w.joined_at < m)) ? w.joined_at : m, '');
    const ended_at_iso = ws.reduce((m, w) => (w.left_at && w.left_at > m) ? w.left_at : m, '');

    // 解析 result_json → 业务可读摘要 + 各项累加
    const parsed = parseOpsResultForExport(j.job_type, rs);

    // 代发拣货：拣货单号 + 拣货人员明细 + 摘要补充
    let pick_doc_nos = '', pick_worker_summary = '';
    if (j.job_type === 'pick_direct' && ps.length > 0) {
      const docs = [...new Set(ps.map(p => p.pick_doc_no).filter(Boolean))];
      pick_doc_nos = docs.join('、');
      pick_worker_summary = docs.map(d => {
        const inDoc = ps.filter(p => p.pick_doc_no === d);
        const parts = inDoc.map(p => `${p.worker_name || p.worker_id}${p.minutes_worked ? ' ' + round1(p.minutes_worked) + '分' : ''}`);
        return `${d}: ${parts.join(' / ')}`;
      }).join('；');
      // pick_direct 摘要：拣货单 X 张 + 工时为主
      const pickSummary = '拣货单 ' + docs.length + ' 张';
      if (parsed.result_summary && parsed.result_summary.indexOf('仅记录工时') < 0 && parsed.result_summary.indexOf('仅计时') < 0) {
        parsed.result_summary = pickSummary + '；' + parsed.result_summary;
      } else {
        parsed.result_summary = pickSummary;
      }
    }

    // 业务关联
    const isOutboundRelType = (t) => (t === 'outbound' || t === 'outbound_order');
    let related = null;
    if (isInboundLink(j.related_doc_type)) related = inboundById[j.related_doc_id] || null;
    else if (isOutboundRelType(j.related_doc_type)) related = outboundById[j.related_doc_id] || null;
    const linked_ob = j.linked_outbound_order_id ? outboundById[j.linked_outbound_order_id] : null;
    const customer = (related && related.customer) || (linked_ob && linked_ob.customer) || '';
    const accounted = (related && related.accounted) ?? (linked_ob && linked_ob.accounted) ?? '';
    const accounted_by = (related && related.accounted_by) || (linked_ob && linked_ob.accounted_by) || '';
    const accounted_at_iso = (related && related.accounted_at) || (linked_ob && linked_ob.accounted_at) || '';
    const inbound_display_no = (isInboundLink(j.related_doc_type) && related) ? (related.display_no || related.external_inbound_no || '') : '';
    const outbound_display_no = (isOutboundRelType(j.related_doc_type) && related) ? (related.display_no || '') : (linked_ob ? linked_ob.display_no || '' : '');

    // 入库计划业务类型 / 完成 / 未完成
    let inbound_biz_classes = '', inbound_completed_biz = '', inbound_pending_biz = '';
    if (isInboundLink(j.related_doc_type) && related) {
      let bizArr = [];
      try {
        const raw = related.biz_classes_json;
        if (raw) { const parsedArr = JSON.parse(raw); if (Array.isArray(parsedArr)) bizArr = parsedArr.filter(Boolean).map(String); }
      } catch (e) { /* ignore */ }
      if (bizArr.length === 0 && related.biz_class) bizArr = [String(related.biz_class)];
      const bizMapZh = { direct_ship: '代发', bulk: '大货', return: '退件', import: '进口' };
      inbound_biz_classes = bizArr.map(b => bizMapZh[b] || b).join('+');
      const tasks = inboundBizTasksByPlan[related.id] || [];
      if (tasks.length > 0) {
        const doneArr = tasks.filter(t => t.status === 'completed').map(t => bizMapZh[t.biz_class] || t.biz_class);
        const pendArr = tasks.filter(t => t.status !== 'completed').map(t => bizMapZh[t.biz_class] || t.biz_class);
        inbound_completed_biz = doneArr.join('+');
        inbound_pending_biz = pendArr.join('+');
      }
    }
    // 出库作业单（含 related_doc_type='outbound_order'）
    const outboundRelated = (isOutboundRelType(j.related_doc_type) && j.related_doc_id) ? outboundById[j.related_doc_id] : null;
    const ob_for_fields = outboundRelated || linked_ob;
    // ob_for_fields 仅可用于 outbound 路径，避免 isInboundLink 时把 inbound_plan 当 outbound 取字段
    const wms_work_order_no = (ob_for_fields && ob_for_fields.wms_work_order_no) || '';
    const destination = (ob_for_fields && ob_for_fields.destination) || '';
    const po_no = (ob_for_fields && ob_for_fields.po_no) || '';
    const planned_box_count = (ob_for_fields && ob_for_fields.planned_box_count) || 0;
    const planned_pallet_count = (ob_for_fields && ob_for_fields.planned_pallet_count) || 0;
    const actual_box_count = (ob_for_fields && ob_for_fields.actual_box_count) || 0;
    const actual_pallet_count = (ob_for_fields && ob_for_fields.actual_pallet_count) || 0;
    // 出库扩展字段（库内操作型 + 资料）
    const ob_uses_stock_operation = (ob_for_fields && Number(ob_for_fields.uses_stock_operation) === 1) ? 1 : 0;
    const ob_outbound_status = (ob_for_fields && ob_for_fields.status) || '';
    // expected_ship_at 是 datetime-local 文本（已是本地预约时间），不做 +9 换算，仅 T→空格
    const ob_expected_ship_at = normalizeDateOnly(ob_for_fields && ob_for_fields.expected_ship_at);
    const ob_outbound_requirement = (ob_for_fields && ob_for_fields.outbound_requirement) || '';
    const ob_instruction = (ob_for_fields && ob_for_fields.instruction) || '';
    const ob_remark = (ob_for_fields && ob_for_fields.remark) || '';
    const ob_pickup_note = (ob_for_fields && ob_for_fields.pickup_note) || '';
    const inbound_remark = (isInboundLink(j.related_doc_type) && related && related.remark) ? related.remark : '';
    const inbound_force_completed = (isInboundLink(j.related_doc_type) && related && Number(related.force_completed) === 1) ? 1 : 0;
    const inbound_force_completed_by = (isInboundLink(j.related_doc_type) && related) ? (related.force_completed_by || '') : '';
    const inbound_force_completed_at = (isInboundLink(j.related_doc_type) && related && related.force_completed_at) ? fmtKst(related.force_completed_at) : '';
    const inbound_force_complete_reason = (isInboundLink(j.related_doc_type) && related) ? (related.force_complete_reason || '') : '';
    const ob_stock_op_status = (ob_for_fields && ob_for_fields.stock_operation_status) || '';
    const ob_stock_op_completed_at = (ob_for_fields && ob_for_fields.stock_operation_completed_at) ? fmtKst(ob_for_fields.stock_operation_completed_at) : '';
    const ob_stock_op_completed_by = (ob_for_fields && ob_for_fields.stock_operation_completed_by) || '';
    const ob_material_count = ob_for_fields ? Number(outboundMaterialCntMap[ob_for_fields.id] || 0) : 0;

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
      created_at: fmtKst(j.created_at),
      started_at: fmtKst(started_at_iso),
      ended_at: fmtKst(ended_at_iso),
      worker_count: new Set(ws.map(w => w.worker_id).filter(Boolean)).size,
      worker_names: workerNames,
      total_minutes,
      total_hours: round1(total_minutes / 60),
      // 作业结果（业务可读摘要 + 解析后的各项累加）
      result_count: rs.length,
      result_summary: parsed.result_summary,
      result_notes: parsed.result_notes,
      diff_notes: parsed.diff_notes,
      box_count_sum: parsed.box_count_sum,
      pallet_count_sum: parsed.pallet_count_sum,
      putaway_qty_sum: parsed.putaway_qty_sum,
      putaway_carton_qty_sum: parsed.putaway_carton_qty_sum,
      putaway_pallet_qty_sum: parsed.putaway_pallet_qty_sum,
      actual_qty_sum: parsed.actual_qty_sum,
      actual_carton_qty_sum: parsed.actual_carton_qty_sum,
      actual_pallet_qty_sum: parsed.actual_pallet_qty_sum,
      sort_qty_sum: parsed.sort_qty_sum,
      packed_sku_count_sum: parsed.packed_sku_count_sum,
      packed_box_count_sum: parsed.packed_box_count_sum,
      total_operated_box_count_sum: parsed.total_operated_box_count_sum,
      label_count_sum: parsed.label_count_sum,
      repaired_box_count_sum: parsed.repaired_box_count_sum,
      reboxed_count_sum: parsed.reboxed_count_sum,
      used_carton_large_count_sum: parsed.used_carton_large_count_sum,
      used_carton_small_count_sum: parsed.used_carton_small_count_sum,
      verify_ok_count_sum: parsed.verify_ok_count_sum,
      verify_ng_count_sum: parsed.verify_ng_count_sum,
      result_submitters: parsed.result_submitters,
      result_submitted_at: parsed.result_submitted_at,
      readable_result_lines: parsed.readable_result_lines,
      result_lines_count: parsed.result_lines_count,
      raw_result_json_compact: parsed.raw_result_json_compact,
      // 业务关联
      customer,
      accounted: accounted === '' ? '' : (accounted ? 1 : 0),
      accounted_by,
      accounted_at: fmtKst(accounted_at_iso),
      outbound_display_no, inbound_display_no,
      wms_work_order_no, destination, po_no,
      planned_box_count, planned_pallet_count, actual_box_count, actual_pallet_count,
      // 出库扩展（库内操作 / 资料）
      uses_stock_operation: ob_uses_stock_operation,
      outbound_status: ob_outbound_status,
      expected_ship_at: ob_expected_ship_at,
      outbound_requirement: ob_outbound_requirement,
      ob_instruction,
      ob_remark,
      ob_pickup_note,
      inbound_remark,
      inbound_force_completed,
      inbound_force_completed_by,
      inbound_force_completed_at,
      inbound_force_complete_reason,
      stock_op_status: ob_stock_op_status,
      stock_op_completed_at: ob_stock_op_completed_at,
      stock_op_completed_by: ob_stock_op_completed_by,
      material_count: ob_material_count,
      // 入库计划业务类型 / 完成 / 未完成（多业务类型 V1.2）
      inbound_biz_classes,
      inbound_completed_biz,
      inbound_pending_biz,
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
  const startRange = kstDayRangeUtc(start_date);
  const endRange = kstDayRangeUtc(end_date);
  if (startRange) { where += " AND w.joined_at >= ?"; binds.push(startRange.startUtc); }
  if (endRange)   { where += " AND w.joined_at < ?"; binds.push(endRange.endUtc); }
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
    inbound: ['inbound_direct', 'inbound_bulk', 'inbound_return', 'inbound_change_order'],
    outbound: ['load_outbound', 'verify_scan', 'bulk_op'],
  };

  // ---- 工时段（与 workhour_summary 同口径，但聚合到 by_job_type / by_worker）----
  let where = "WHERE 1=1";
  const binds = [];
  const startRange = kstDayRangeUtc(start_date);
  const endRange = kstDayRangeUtc(end_date);
  if (startRange) { where += " AND w.joined_at >= ?"; binds.push(startRange.startUtc); }
  if (endRange)   { where += " AND w.joined_at < ?"; binds.push(endRange.endUtc); }

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
// 003 - Consumables & warehouse assets / 耗材与物品管理
// =====================================================
function v003Id(prefix) {
  return String(prefix || 'V003') + '-' + crypto.randomUUID();
}

function v003Text(value, maxLen = 200) {
  return String(value == null ? '' : value).trim().slice(0, maxLen);
}

function v003Number(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function v003Changes(result) {
  return Number(result && result.meta && result.meta.changes) || 0;
}

function v003Operator(body) {
  return {
    id: v003Text(body.operator_id || body.worker_id || '', 80),
    name: v003Text(body.operator_name || body.worker_name || body.created_by || '', 120)
  };
}

function v003RequireOperator(body) {
  const op = v003Operator(body);
  if (!op.id && !op.name) return null;
  if (!op.id) op.id = op.name;
  if (!op.name) op.name = op.id;
  return op;
}

const V003_DEPARTMENTS = ['代发', '大货', '进口'];

function v003Department(value) {
  const department = v003Text(value, 20);
  return V003_DEPARTMENTS.includes(department) ? department : '';
}

// 003 现场端按用户要求不再使用访问码。现场权限只对 003 的明确白名单接口开放，
// 并且每个请求都必须带当前工牌解析出的操作人信息；管理员接口仍只认 ADMINKEY。
function v003IsPublicField(body) {
  if (String(body.k || '').trim()) return false;
  const op = v003RequireOperator(body);
  return !!(op && /^(EMP-|DA-|DAF-)/.test(op.id) && op.name);
}

function v003CanField(body, env) {
  return isAdmin(body, env) || isOpsKey(body, env) || v003IsPublicField(body);
}

function v003CanRead(body, env) {
  return isOpsAuth(body, env) || v003IsPublicField(body);
}

async function v003NextCode(env, tableName, fieldName, prefix) {
  const day = kstToday().replace(/-/g, '');
  const base = prefix + '-' + day + '-';
  const allowed = {
    v2_003_materials: 'material_code',
    v2_003_assets: 'asset_code'
  };
  if (allowed[tableName] !== fieldName) throw new Error('invalid code target');
  const row = await env.DB.prepare(
    `SELECT ${fieldName} AS code FROM ${tableName} WHERE ${fieldName} LIKE ? ORDER BY ${fieldName} DESC LIMIT 1`
  ).bind(base + '%').first();
  const last = row && row.code ? parseInt(String(row.code).split('-').pop(), 10) || 0 : 0;
  return base + String(last + 1).padStart(3, '0');
}

route('v2_003_auth_check', async (body, env) => {
  if (isAdmin(body, env)) return json({ ok: true, role: 'admin' });
  if (isOpsKey(body, env)) return json({ ok: true, role: 'operator' });
  if (isAuth(body, env)) return json({ ok: true, role: 'viewer' });
  return err('unauthorized', 401);
});

route('v2_003_dashboard', async (body, env) => {
  if (!isOpsAuth(body, env)) return err('unauthorized', 401);
  const range = kstDayRangeUtc(kstToday());
  const [materials, low, assets, assigned, repair, mToday, aToday, recentM, recentA] = await Promise.all([
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_materials WHERE status='active'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_materials WHERE status='active' AND min_qty>0 AND current_qty<=min_qty").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_assets WHERE status!='retired'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_assets WHERE status='assigned'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_assets WHERE status='repair'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_material_txns WHERE created_at>=? AND created_at<?")
      .bind(range.startUtc, range.endUtc).first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_asset_txns WHERE created_at>=? AND created_at<?")
      .bind(range.startUtc, range.endUtc).first(),
    env.DB.prepare(`SELECT t.*, m.material_code AS item_code, m.name_zh AS item_name, m.unit
      FROM v2_003_material_txns t JOIN v2_003_materials m ON m.id=t.material_id
      ORDER BY t.created_at DESC LIMIT 8`).all(),
    env.DB.prepare(`SELECT t.*, a.asset_code AS item_code, a.name_zh AS item_name
      FROM v2_003_asset_txns t JOIN v2_003_assets a ON a.id=t.asset_id
      ORDER BY t.created_at DESC LIMIT 8`).all()
  ]);
  const recent = [];
  (recentM.results || []).forEach(r => recent.push({ kind: 'material', ...r }));
  (recentA.results || []).forEach(r => recent.push({ kind: 'asset', ...r }));
  recent.sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
  return json({
    ok: true,
    summary: {
      material_count: Number(materials && materials.c) || 0,
      low_stock_count: Number(low && low.c) || 0,
      asset_count: Number(assets && assets.c) || 0,
      assigned_asset_count: Number(assigned && assigned.c) || 0,
      repair_asset_count: Number(repair && repair.c) || 0,
      material_txn_today: Number(mToday && mToday.c) || 0,
      asset_txn_today: Number(aToday && aToday.c) || 0
    },
    recent: recent.slice(0, 10)
  });
});

route('v2_003_location_list', async (body, env) => {
  if (!v003CanRead(body, env)) return err('unauthorized', 401);
  const includeInactive = isAdmin(body, env) && String(body.include_inactive || '') === '1';
  const rs = await env.DB.prepare(
    `SELECT * FROM v2_003_locations ${includeInactive ? '' : 'WHERE active=1'}
     ORDER BY location_code, location_name`
  ).all();
  return json({ ok: true, items: rs.results || [] });
});

route('v2_003_location_save', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const warehouse = '';
  const code = v003Text(body.location_code, 80);
  const name = v003Text(body.location_name, 120);
  if (!code) return err('location_required');
  const active = String(body.active) === '0' || body.active === false ? 0 : 1;
  const id = v003Text(body.id, 100);
  const op = v003RequireOperator(body) || { id: 'ADMIN', name: '管理员' };
  const t = now();
  if (id) {
    const result = await env.DB.prepare(`UPDATE v2_003_locations
      SET warehouse_name=?, location_code=?, location_name=?, active=?, updated_at=? WHERE id=?`)
      .bind(warehouse, code, name, active, t, id).run();
    if (!v003Changes(result)) return err('not_found', 404);
    return json({ ok: true, id });
  }
  const newId = v003Id('LOC');
  await env.DB.prepare(`INSERT INTO v2_003_locations
    (id, warehouse_name, location_code, location_name, active, created_by, created_at, updated_at)
    VALUES(?,?,?,?,?,?,?,?)`)
    .bind(newId, warehouse, code, name, active, op.name, t, t).run();
  return json({ ok: true, id: newId });
});

route('v2_003_material_list', async (body, env) => {
  if (!v003CanRead(body, env)) return err('unauthorized', 401);
  const { limit, offset } = pageParams(body);
  const search = v003Text(body.search, 120);
  const category = v003Text(body.category, 80);
  let status = v003Text(body.status, 30);
  const lowOnly = String(body.low_stock_only || '') === '1';
  if (!isAdmin(body, env)) status = 'active';
  const where = ['1=1'];
  const binds = [];
  if (search) {
    where.push('(material_code LIKE ? OR barcode LIKE ? OR name_zh LIKE ? OR name_ko LIKE ? OR spec LIKE ?)');
    for (let i = 0; i < 5; i++) binds.push('%' + search + '%');
  }
  if (category) { where.push('category=?'); binds.push(category); }
  if (status) { where.push('status=?'); binds.push(status); }
  if (lowOnly) where.push("status='active' AND min_qty>0 AND current_qty<=min_qty");
  const sqlWhere = 'WHERE ' + where.join(' AND ');
  const [count, rows] = await Promise.all([
    env.DB.prepare(`SELECT COUNT(*) AS c FROM v2_003_materials ${sqlWhere}`).bind(...binds).first(),
    env.DB.prepare(`SELECT *, CASE WHEN status='active' AND min_qty>0 AND current_qty<=min_qty THEN 1 ELSE 0 END AS low_stock
      FROM v2_003_materials ${sqlWhere}
      ORDER BY low_stock DESC, category, name_zh, material_code LIMIT ? OFFSET ?`)
      .bind(...binds, limit, offset).all()
  ]);
  return json({ ok: true, items: rows.results || [], ...pageMeta(count && count.c, limit, offset) });
});

route('v2_003_material_detail', async (body, env) => {
  if (!v003CanRead(body, env)) return err('unauthorized', 401);
  const id = v003Text(body.id, 100);
  if (!id) return err('missing_id');
  const item = await env.DB.prepare(`SELECT *, CASE WHEN status='active' AND min_qty>0 AND current_qty<=min_qty
    THEN 1 ELSE 0 END AS low_stock FROM v2_003_materials WHERE id=?`).bind(id).first();
  if (!item) return err('not_found', 404);
  const txns = await env.DB.prepare(`SELECT * FROM v2_003_material_txns
    WHERE material_id=? ORDER BY created_at DESC LIMIT 100`).bind(id).all();
  return json({ ok: true, item, transactions: txns.results || [] });
});

route('v2_003_material_save', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const id = v003Text(body.id, 100);
  const nameZh = v003Text(body.name_zh, 160);
  const nameKo = v003Text(body.name_ko, 160);
  const category = v003Text(body.category, 80);
  const unit = v003Text(body.unit, 40);
  if (!nameZh || !category || !unit) return err('name_category_unit_required');
  const barcode = v003Text(body.barcode, 120);
  const op = v003RequireOperator(body) || { id: 'ADMIN', name: '管理员' };
  const status = ['active', 'inactive'].includes(String(body.status)) ? String(body.status) : 'active';
  const data = {
    material_code: v003Text(body.material_code, 80), barcode,
    name_zh: nameZh, name_ko: nameKo, category,
    spec: v003Text(body.spec, 160), unit,
    location_code: v003Text(body.location_code, 80),
    min_qty: Math.max(0, v003Number(body.min_qty)),
    unit_cost: Math.max(0, v003Number(body.unit_cost)),
    currency: v003Text(body.currency || 'KRW', 12) || 'KRW',
    supplier: v003Text(body.supplier, 160), status,
    note: v003Text(body.note, 1000)
  };
  if (data.material_code) {
    const dup = await env.DB.prepare('SELECT id FROM v2_003_materials WHERE material_code=? AND id!=? LIMIT 1')
      .bind(data.material_code, id || '').first();
    if (dup) return err('duplicate_item_code');
  }
  if (barcode) {
    const dup = await env.DB.prepare('SELECT id FROM v2_003_materials WHERE barcode=? AND id!=? LIMIT 1')
      .bind(barcode, id || '').first();
    if (dup) return err('duplicate_barcode');
  }
  const t = now();
  if (id) {
    const result = await env.DB.prepare(`UPDATE v2_003_materials SET
      material_code=COALESCE(NULLIF(?,''), material_code), barcode=?, name_zh=?, name_ko=?, category=?, spec=?, unit=?,
      location_code=?, min_qty=?, unit_cost=?, currency=?, supplier=?, status=?, note=?,
      updated_by=?, updated_at=? WHERE id=?`)
      .bind(data.material_code, data.barcode, data.name_zh, data.name_ko, data.category, data.spec, data.unit,
        data.location_code, data.min_qty, data.unit_cost, data.currency, data.supplier,
        data.status, data.note, op.name, t, id).run();
    if (!v003Changes(result)) return err('not_found', 404);
    return json({ ok: true, id });
  }
  const newId = v003Id('MAT');
  const code = data.material_code || await v003NextCode(env, 'v2_003_materials', 'material_code', 'HC');
  const opening = Math.max(0, v003Number(body.opening_qty));
  const stmts = [env.DB.prepare(`INSERT INTO v2_003_materials
    (id, material_code, barcode, name_zh, name_ko, category, spec, unit, warehouse_name, location_code,
     current_qty, min_qty, unit_cost, currency, supplier, status, note, stock_version,
     created_by, created_at, updated_by, updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?)`)
    .bind(newId, code, data.barcode, data.name_zh, data.name_ko, data.category, data.spec, data.unit,
      '', data.location_code, opening, data.min_qty, data.unit_cost, data.currency,
      data.supplier, data.status, data.note, op.name, t, op.name, t)];
  if (opening > 0) {
    stmts.push(env.DB.prepare(`INSERT INTO v2_003_material_txns
      (id, material_id, txn_type, qty_delta, qty_before, qty_after, warehouse_name, location_code,
       recipient_id, recipient_name, purpose, related_doc_no, unit_cost, supplier, note,
       operator_id, operator_name, created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .bind(v003Id('MTX'), newId, 'opening', opening, 0, opening, '', data.location_code,
        '', '', '期初库存', '', data.unit_cost, data.supplier, data.note, op.id, op.name, t));
  }
  await env.DB.batch(stmts);
  return json({ ok: true, id: newId, material_code: code });
});

async function v003PrepareMaterialImport(body, env) {
  const source = Array.isArray(body.rows) ? body.rows : [];
  if (!source.length) return { errors: [{ row: 0, error: 'bulk_import_empty' }], plans: [] };
  if (source.length > 300) return { errors: [{ row: 0, error: 'bulk_import_limit' }], plans: [] };

  const existingRs = await env.DB.prepare(`SELECT id, material_code, barcode, name_zh, name_ko, category,
    spec, unit, location_code, current_qty, min_qty, unit_cost, currency, supplier, status, note
    FROM v2_003_materials`).all();
  const existing = existingRs.results || [];
  const byCode = new Map(existing.filter(x => x.material_code).map(x => [String(x.material_code), x]));
  const byBarcode = new Map(existing.filter(x => x.barcode).map(x => [String(x.barcode), x]));
  const usedCodes = new Set();
  const usedBarcodes = new Set();
  const usedTargets = new Set();
  const errors = [];
  const plans = [];
  const dayPrefix = 'HC-' + kstToday().replace(/-/g, '') + '-';
  let sequence = existing.reduce((max, x) => {
    const code = String(x.material_code || '');
    if (!code.startsWith(dayPrefix)) return max;
    return Math.max(max, parseInt(code.slice(dayPrefix.length), 10) || 0);
  }, 0);
  const hasValue = (raw, key) => raw && raw[key] != null && String(raw[key]).trim() !== '';

  for (let i = 0; i < source.length; i++) {
    const raw = source[i] || {};
    const row = Math.max(1, Math.trunc(v003Number(raw.row_no, i + 2)));
    let code = v003Text(raw.material_code, 80);
    const barcode = v003Text(raw.barcode, 120);
    const codeTarget = code ? byCode.get(code) : null;
    const barcodeTarget = barcode ? byBarcode.get(barcode) : null;
    if (codeTarget && barcodeTarget && codeTarget.id !== barcodeTarget.id) {
      errors.push({ row, error: 'bulk_code_barcode_conflict' });
      continue;
    }
    const target = codeTarget || barcodeTarget || null;
    if (target && usedTargets.has(target.id)) {
      errors.push({ row, error: 'bulk_duplicate_target' });
      continue;
    }
    if (code && usedCodes.has(code)) {
      errors.push({ row, error: 'bulk_duplicate_code' });
      continue;
    }
    if (barcode && usedBarcodes.has(barcode)) {
      errors.push({ row, error: 'bulk_duplicate_barcode' });
      continue;
    }

    const nameZh = v003Text(raw.name_zh, 160);
    const category = v003Text(raw.category, 80);
    const unit = v003Text(raw.unit, 40);
    if (!nameZh || !category || !unit) {
      errors.push({ row, error: 'name_category_unit_required' });
      continue;
    }
    const status = hasValue(raw, 'status') ? v003Text(raw.status, 30) : (target && target.status) || 'active';
    if (!['active', 'inactive'].includes(status)) {
      errors.push({ row, error: 'bulk_invalid_status' });
      continue;
    }
    const opening = hasValue(raw, 'opening_qty') ? v003Number(raw.opening_qty, NaN) : 0;
    const minQty = hasValue(raw, 'min_qty') ? v003Number(raw.min_qty, NaN) : v003Number(target && target.min_qty);
    const unitCost = hasValue(raw, 'unit_cost') ? v003Number(raw.unit_cost, NaN) : v003Number(target && target.unit_cost);
    if (![opening, minQty, unitCost].every(Number.isFinite) || opening < 0 || minQty < 0 || unitCost < 0) {
      errors.push({ row, error: 'bulk_invalid_number' });
      continue;
    }
    if (!code) {
      if (target && target.material_code) code = String(target.material_code);
      else {
        do { sequence++; code = dayPrefix + String(sequence).padStart(3, '0'); }
        while (byCode.has(code) || usedCodes.has(code));
      }
    }
    const conflictingCode = byCode.get(code);
    if (conflictingCode && (!target || conflictingCode.id !== target.id)) {
      errors.push({ row, error: 'duplicate_item_code' });
      continue;
    }
    const conflictingBarcode = barcode ? byBarcode.get(barcode) : null;
    if (conflictingBarcode && (!target || conflictingBarcode.id !== target.id)) {
      errors.push({ row, error: 'duplicate_barcode' });
      continue;
    }

    const keep = (key, fallback = '') => hasValue(raw, key) ? v003Text(raw[key], key === 'note' ? 1000 : 160) : v003Text(target && target[key], key === 'note' ? 1000 : 160);
    plans.push({
      row,
      action: target ? 'update' : 'create',
      id: target ? target.id : v003Id('MAT'),
      material_code: code,
      barcode: barcode || v003Text(target && target.barcode, 120),
      name_zh: nameZh,
      name_ko: keep('name_ko'),
      category,
      spec: keep('spec'),
      unit,
      location_code: keep('location_code'),
      opening_qty: target ? 0 : opening,
      min_qty: minQty,
      unit_cost: unitCost,
      currency: 'KRW',
      supplier: keep('supplier'),
      status,
      note: keep('note')
    });
    usedCodes.add(code);
    if (barcode) usedBarcodes.add(barcode);
    if (target) usedTargets.add(target.id);
  }
  return { errors, plans };
}

route('v2_003_material_bulk_import', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const prepared = await v003PrepareMaterialImport(body, env);
  if (prepared.errors.length) return json({ ok: false, error: 'bulk_import_invalid', errors: prepared.errors }, 400);
  const created = prepared.plans.filter(x => x.action === 'create').length;
  const updated = prepared.plans.length - created;
  if (String(body.dry_run || '') === '1') {
    return json({ ok: true, dry_run: true, created_count: created, updated_count: updated,
      items: prepared.plans.map(x => ({ row: x.row, action: x.action, material_code: x.material_code,
        name_zh: x.name_zh, category: x.category, unit: x.unit, opening_qty: x.opening_qty })) });
  }
  const op = v003RequireOperator(body) || { id: 'ADMIN', name: '管理员' };
  return withIdem(env, body, 'v2_003_material_bulk_import', async () => {
    const t = now();
    const statements = [];
    for (const item of prepared.plans) {
      if (item.action === 'update') {
        statements.push(env.DB.prepare(`UPDATE v2_003_materials SET material_code=?, barcode=?, name_zh=?, name_ko=?,
          category=?, spec=?, unit=?, location_code=?, min_qty=?, unit_cost=?, currency=?, supplier=?, status=?, note=?,
          updated_by=?, updated_at=? WHERE id=?`)
          .bind(item.material_code, item.barcode, item.name_zh, item.name_ko, item.category, item.spec, item.unit,
            item.location_code, item.min_qty, item.unit_cost, item.currency, item.supplier, item.status, item.note,
            op.name, t, item.id));
      } else {
        statements.push(env.DB.prepare(`INSERT INTO v2_003_materials
          (id, material_code, barcode, name_zh, name_ko, category, spec, unit, warehouse_name, location_code,
           current_qty, min_qty, unit_cost, currency, supplier, status, note, stock_version,
           created_by, created_at, updated_by, updated_at)
          VALUES(?,?,?,?,?,?,?,?,'',?,?,?,?,?,?,?,?,0,?,?,?,?)`)
          .bind(item.id, item.material_code, item.barcode, item.name_zh, item.name_ko, item.category, item.spec,
            item.unit, item.location_code, item.opening_qty, item.min_qty, item.unit_cost, item.currency,
            item.supplier, item.status, item.note, op.name, t, op.name, t));
        if (item.opening_qty > 0) {
          statements.push(env.DB.prepare(`INSERT INTO v2_003_material_txns
            (id, material_id, txn_type, qty_delta, qty_before, qty_after, warehouse_name, location_code,
             recipient_id, recipient_name, purpose, related_doc_no, unit_cost, supplier, note,
             operator_id, operator_name, department, created_at)
            VALUES(?,?,?,?,?,?,'',?,'','','期初库存',?,?,?,?,?,?,?,?)`)
            .bind(v003Id('MTX'), item.id, 'opening', item.opening_qty, 0, item.opening_qty,
              item.location_code, '', item.unit_cost, item.supplier, item.note, op.id, op.name, '', t));
        }
      }
    }
    await env.DB.batch(statements);
    return { ok: true, created_count: created, updated_count: updated, total: prepared.plans.length };
  });
});

route('v2_003_material_txn', async (body, env) => {
  if (!v003CanField(body, env)) return err('unauthorized', 401);
  const txnType = v003Text(body.txn_type, 30);
  const allowed = ['inbound', 'issue', 'use', 'return', 'adjust', 'stocktake'];
  if (!allowed.includes(txnType)) return err('invalid_txn_type');
  if (['inbound', 'adjust', 'stocktake'].includes(txnType) && !isAdmin(body, env)) {
    return err('unauthorized_admin_only', 401);
  }
  const id = v003Text(body.material_id, 100);
  if (!id) return err('missing_material_id');
  const op = v003RequireOperator(body);
  if (!op) return err('operator_required');
  const department = ['issue', 'use', 'return'].includes(txnType) ? v003Department(body.department) : '';
  const recipientName = v003Text(body.recipient_name, 120);
  if (['issue', 'use', 'return'].includes(txnType) && !department) return err('department_required');
  if (['issue', 'use', 'return'].includes(txnType) && !recipientName) return err('recipient_required');
  const rawQty = v003Number(body.qty, NaN);
  if (!['adjust', 'stocktake'].includes(txnType) && (!Number.isFinite(rawQty) || rawQty <= 0)) {
    return err('positive_qty_required');
  }
  return withIdem(env, body, 'v2_003_material_txn', async () => {
    for (let attempt = 0; attempt < 4; attempt++) {
      const item = await env.DB.prepare('SELECT * FROM v2_003_materials WHERE id=?').bind(id).first();
      if (!item) throw new Error('not_found');
      if (item.status !== 'active' && !isAdmin(body, env)) throw new Error('material_inactive');
      const before = v003Number(item.current_qty);
      let delta = 0;
      if (txnType === 'inbound' || txnType === 'return') delta = rawQty;
      else if (txnType === 'issue' || txnType === 'use') delta = -rawQty;
      else if (txnType === 'adjust') delta = v003Number(body.delta, NaN);
      else if (txnType === 'stocktake') {
        const counted = v003Number(body.counted_qty, NaN);
        if (!Number.isFinite(counted) || counted < 0) throw new Error('valid_counted_qty_required');
        delta = counted - before;
      }
      if (!Number.isFinite(delta) || Math.abs(delta) > 1000000000) throw new Error('invalid_qty');
      const after = Math.round((before + delta) * 10000) / 10000;
      if (after < 0) throw new Error('insufficient_stock');
      const version = Number(item.stock_version) || 0;
      const t = now();
      const location = v003Text(body.location_code || item.location_code, 80);
      const cost = Math.max(0, v003Number(body.unit_cost, item.unit_cost));
      const supplier = v003Text(body.supplier || item.supplier, 160);
      const txId = v003Id('MTX');
      const results = await env.DB.batch([
        env.DB.prepare(`UPDATE v2_003_materials SET current_qty=?, location_code=?,
          unit_cost=?, supplier=?, stock_version=stock_version+1, updated_by=?, updated_at=?
          WHERE id=? AND stock_version=?`)
          .bind(after, location, cost, supplier, op.name, t, id, version),
        env.DB.prepare(`INSERT INTO v2_003_material_txns
          (id, material_id, txn_type, qty_delta, qty_before, qty_after, warehouse_name, location_code,
           recipient_id, recipient_name, purpose, related_doc_no, unit_cost, supplier, note,
           operator_id, operator_name, department, created_at)
          SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? FROM v2_003_materials
          WHERE id=? AND stock_version=?`)
          .bind(txId, id, txnType, delta, before, after, '', location,
            v003Text(body.recipient_id, 80), recipientName,
            v003Text(body.purpose, 240), v003Text(body.related_doc_no, 120), cost, supplier,
            v003Text(body.note, 1000), op.id, op.name, department, t, id, version + 1)
      ]);
      if (v003Changes(results[0]) === 1 && v003Changes(results[1]) === 1) {
        return { ok: true, id: txId, qty_before: before, qty_delta: delta, qty_after: after };
      }
    }
    throw new Error('stock_changed_retry');
  });
});

route('v2_003_department_usage', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const department = v003Text(body.department, 20);
  if (department && !v003Department(department)) return err('department_required');
  const startRange = kstDayRangeUtc(v003Text(body.start_date, 10));
  const endRange = kstDayRangeUtc(v003Text(body.end_date, 10));
  const where = ["t.txn_type IN ('issue','use','return')", "t.department IN ('代发','大货','进口')"];
  const binds = [];
  if (department) { where.push('t.department=?'); binds.push(department); }
  if (startRange) { where.push('t.created_at>=?'); binds.push(startRange.startUtc); }
  if (endRange) { where.push('t.created_at<?'); binds.push(endRange.endUtc); }
  const rs = await env.DB.prepare(`SELECT t.department, m.id AS material_id, m.material_code,
      m.name_zh, m.name_ko, m.category, m.unit,
      ROUND(SUM(CASE WHEN t.txn_type='issue' THEN -t.qty_delta ELSE 0 END),4) AS issue_qty,
      ROUND(SUM(CASE WHEN t.txn_type='use' THEN -t.qty_delta ELSE 0 END),4) AS use_qty,
      ROUND(SUM(CASE WHEN t.txn_type='return' THEN t.qty_delta ELSE 0 END),4) AS return_qty,
      COUNT(*) AS record_count
    FROM v2_003_material_txns t JOIN v2_003_materials m ON m.id=t.material_id
    WHERE ${where.join(' AND ')}
    GROUP BY t.department, m.id, m.material_code, m.name_zh, m.name_ko, m.category, m.unit
    ORDER BY CASE t.department WHEN '代发' THEN 1 WHEN '大货' THEN 2 ELSE 3 END,
      use_qty DESC, issue_qty DESC, m.category, m.name_zh`).bind(...binds).all();
  const items = rs.results || [];
  const summary = V003_DEPARTMENTS.map(name => {
    const rows = items.filter(x => x.department === name);
    return {
      department: name,
      material_count: rows.length,
      record_count: rows.reduce((sum, x) => sum + v003Number(x.record_count), 0)
    };
  });
  return json({ ok: true, departments: V003_DEPARTMENTS, summary, items });
});

route('v2_003_asset_list', async (body, env) => {
  if (!v003CanRead(body, env)) return err('unauthorized', 401);
  const { limit, offset } = pageParams(body);
  const search = v003Text(body.search, 120);
  const category = v003Text(body.category, 80);
  const keeper = v003Text(body.keeper, 120);
  const status = v003Text(body.status, 30);
  const where = ['1=1'];
  const binds = [];
  if (search) {
    where.push('(asset_code LIKE ? OR barcode LIKE ? OR name_zh LIKE ? OR name_ko LIKE ? OR serial_no LIKE ? OR model LIKE ?)');
    for (let i = 0; i < 6; i++) binds.push('%' + search + '%');
  }
  if (category) { where.push('category=?'); binds.push(category); }
  if (keeper) { where.push('(keeper_name LIKE ? OR keeper_id LIKE ?)'); binds.push('%' + keeper + '%', '%' + keeper + '%'); }
  if (status) { where.push('status=?'); binds.push(status); }
  const sqlWhere = 'WHERE ' + where.join(' AND ');
  const [count, rows] = await Promise.all([
    env.DB.prepare(`SELECT COUNT(*) AS c FROM v2_003_assets ${sqlWhere}`).bind(...binds).first(),
    env.DB.prepare(`SELECT * FROM v2_003_assets ${sqlWhere}
      ORDER BY CASE status WHEN 'repair' THEN 0 WHEN 'lost' THEN 1 ELSE 2 END, category, name_zh, asset_code
      LIMIT ? OFFSET ?`).bind(...binds, limit, offset).all()
  ]);
  return json({ ok: true, items: rows.results || [], ...pageMeta(count && count.c, limit, offset) });
});

route('v2_003_asset_detail', async (body, env) => {
  if (!v003CanRead(body, env)) return err('unauthorized', 401);
  const id = v003Text(body.id, 100);
  if (!id) return err('missing_id');
  const [item, txns, attachments] = await Promise.all([
    env.DB.prepare('SELECT * FROM v2_003_assets WHERE id=?').bind(id).first(),
    env.DB.prepare('SELECT * FROM v2_003_asset_txns WHERE asset_id=? ORDER BY created_at DESC LIMIT 100').bind(id).all(),
    env.DB.prepare("SELECT * FROM v2_attachments WHERE related_doc_type='asset' AND related_doc_id=? ORDER BY created_at DESC")
      .bind(id).all()
  ]);
  if (!item) return err('not_found', 404);
  return json({ ok: true, item, transactions: txns.results || [], attachments: attachments.results || [] });
});

route('v2_003_asset_save', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const id = v003Text(body.id, 100);
  const nameZh = v003Text(body.name_zh, 160);
  const category = v003Text(body.category, 80);
  if (!nameZh || !category) return err('name_category_required');
  const barcode = v003Text(body.barcode, 120);
  const serial = v003Text(body.serial_no, 160);
  const op = v003RequireOperator(body) || { id: 'ADMIN', name: '管理员' };
  const assetCode = v003Text(body.asset_code, 80);
  if (assetCode) {
    const dup = await env.DB.prepare('SELECT id FROM v2_003_assets WHERE asset_code=? AND id!=? LIMIT 1')
      .bind(assetCode, id || '').first();
    if (dup) return err('duplicate_item_code');
  }
  if (barcode) {
    const dup = await env.DB.prepare('SELECT id FROM v2_003_assets WHERE barcode=? AND id!=? LIMIT 1')
      .bind(barcode, id || '').first();
    if (dup) return err('duplicate_barcode');
  }
  if (serial) {
    const dup = await env.DB.prepare('SELECT id FROM v2_003_assets WHERE serial_no=? AND id!=? LIMIT 1')
      .bind(serial, id || '').first();
    if (dup) return err('duplicate_serial_no');
  }
  const data = {
    asset_code: assetCode, barcode,
    name_zh: nameZh, name_ko: v003Text(body.name_ko, 160), category,
    brand: v003Text(body.brand, 100), model: v003Text(body.model, 120), serial_no: serial,
    location_code: v003Text(body.location_code, 80),
    purchase_date: normalizeDateOnly(body.purchase_date),
    purchase_cost: Math.max(0, v003Number(body.purchase_cost)),
    currency: v003Text(body.currency || 'KRW', 12) || 'KRW',
    supplier: v003Text(body.supplier, 160), warranty_until: normalizeDateOnly(body.warranty_until),
    note: v003Text(body.note, 1000)
  };
  const t = now();
  if (id) {
    const existing = await env.DB.prepare('SELECT * FROM v2_003_assets WHERE id=?').bind(id).first();
    if (!existing) return err('not_found', 404);
    const result = await env.DB.batch([
      env.DB.prepare(`UPDATE v2_003_assets SET asset_code=COALESCE(NULLIF(?,''),asset_code), barcode=?,
        name_zh=?, name_ko=?, category=?, brand=?, model=?, serial_no=?, location_code=?,
        purchase_date=?, purchase_cost=?, currency=?, supplier=?, warranty_until=?, note=?,
        asset_version=asset_version+1, updated_by=?, updated_at=? WHERE id=?`)
        .bind(data.asset_code, data.barcode, data.name_zh, data.name_ko, data.category, data.brand, data.model,
          data.serial_no, data.location_code, data.purchase_date, data.purchase_cost,
          data.currency, data.supplier, data.warranty_until, data.note, op.name, t, id),
      env.DB.prepare(`INSERT INTO v2_003_asset_txns
        (id, asset_id, action_type, status_before, status_after, from_warehouse, from_location,
         to_warehouse, to_location, from_keeper_id, from_keeper_name, to_keeper_id, to_keeper_name,
         related_doc_no, note, operator_id, operator_name, created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .bind(v003Id('ATX'), id, 'edit', existing.status, existing.status,
          existing.warehouse_name, existing.location_code, '', data.location_code,
          existing.keeper_id, existing.keeper_name, existing.keeper_id, existing.keeper_name,
          '', '修改物品资料', op.id, op.name, t)
    ]);
    if (!v003Changes(result[0])) return err('not_found', 404);
    return json({ ok: true, id });
  }
  const newId = v003Id('AST');
  const code = data.asset_code || await v003NextCode(env, 'v2_003_assets', 'asset_code', 'WP');
  await env.DB.batch([
    env.DB.prepare(`INSERT INTO v2_003_assets
      (id, asset_code, barcode, name_zh, name_ko, category, brand, model, serial_no,
       warehouse_name, location_code, keeper_id, keeper_name, status, purchase_date, purchase_cost,
       currency, supplier, warranty_until, note, asset_version, created_by, created_at, updated_by, updated_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,'','','available',?,?,?,?,?,?,0,?,?,?,?)`)
      .bind(newId, code, data.barcode, data.name_zh, data.name_ko, data.category, data.brand, data.model,
        data.serial_no, '', data.location_code, data.purchase_date, data.purchase_cost,
        data.currency, data.supplier, data.warranty_until, data.note, op.name, t, op.name, t),
    env.DB.prepare(`INSERT INTO v2_003_asset_txns
      (id, asset_id, action_type, status_before, status_after, from_warehouse, from_location,
       to_warehouse, to_location, from_keeper_id, from_keeper_name, to_keeper_id, to_keeper_name,
       related_doc_no, note, operator_id, operator_name, created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .bind(v003Id('ATX'), newId, 'create', '', 'available', '', '', '', data.location_code,
        '', '', '', '', '', data.note, op.id, op.name, t)
  ]);
  return json({ ok: true, id: newId, asset_code: code });
});

route('v2_003_asset_action', async (body, env) => {
  if (!v003CanField(body, env)) return err('unauthorized', 401);
  const action = v003Text(body.action_type, 30);
  const allowed = ['assign', 'return', 'transfer', 'repair_start', 'repair_done', 'retire', 'lost'];
  if (!allowed.includes(action)) return err('invalid_action_type');
  if (!['assign', 'return', 'transfer'].includes(action) && !isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const id = v003Text(body.asset_id, 100);
  const op = v003RequireOperator(body);
  if (!id) return err('missing_asset_id');
  if (!op) return err('operator_required');
  return withIdem(env, body, 'v2_003_asset_action', async () => {
    for (let attempt = 0; attempt < 4; attempt++) {
      const item = await env.DB.prepare('SELECT * FROM v2_003_assets WHERE id=?').bind(id).first();
      if (!item) throw new Error('not_found');
      if (['retired', 'lost'].includes(item.status)) throw new Error('asset_unavailable');
      let status = item.status;
      let location = v003Text(body.location_code || item.location_code, 80);
      let keeperId = item.keeper_id || '';
      let keeperName = item.keeper_name || '';
      let keeperDepartment = v003Department(item.keeper_department);
      if (action === 'assign') {
        if (item.status !== 'available') throw new Error('asset_not_available');
        keeperId = v003Text(body.to_keeper_id || body.recipient_id || op.id, 80);
        keeperName = v003Text(body.to_keeper_name || body.recipient_name || op.name, 120);
        if (!keeperId && !keeperName) throw new Error('keeper_required');
        keeperDepartment = v003Department(body.department);
        if (!keeperDepartment) throw new Error('department_required');
        status = 'assigned';
      } else if (action === 'return') {
        if (item.status !== 'assigned') throw new Error('asset_not_assigned');
        if (!isAdmin(body, env) && item.keeper_id && item.keeper_id !== op.id) throw new Error('not_current_keeper');
        keeperId = '';
        keeperName = '';
        keeperDepartment = '';
        status = 'available';
      } else if (action === 'transfer') {
        if (!isAdmin(body, env) && item.keeper_id && item.keeper_id !== op.id) throw new Error('not_current_keeper');
        if (!location) throw new Error('location_required');
      } else if (action === 'repair_start') {
        status = 'repair';
      } else if (action === 'repair_done') {
        if (item.status !== 'repair') throw new Error('asset_not_in_repair');
        status = 'available';
        keeperId = '';
        keeperName = '';
        keeperDepartment = '';
      } else if (action === 'retire') {
        status = 'retired';
        keeperId = '';
        keeperName = '';
      } else if (action === 'lost') {
        status = 'lost';
      }
      const version = Number(item.asset_version) || 0;
      const t = now();
      const txId = v003Id('ATX');
      const results = await env.DB.batch([
        env.DB.prepare(`UPDATE v2_003_assets SET status=?, location_code=?,
          keeper_id=?, keeper_name=?, keeper_department=?, asset_version=asset_version+1, updated_by=?, updated_at=?
          WHERE id=? AND asset_version=?`)
          .bind(status, location, keeperId, keeperName, keeperDepartment, op.name, t, id, version),
        env.DB.prepare(`INSERT INTO v2_003_asset_txns
          (id, asset_id, action_type, status_before, status_after, from_warehouse, from_location,
           to_warehouse, to_location, from_keeper_id, from_keeper_name, to_keeper_id, to_keeper_name,
           related_doc_no, note, operator_id, operator_name, from_department, to_department, created_at)
          SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? FROM v2_003_assets
          WHERE id=? AND asset_version=?`)
          .bind(txId, id, action, item.status, status, item.warehouse_name, item.location_code,
            '', location, item.keeper_id, item.keeper_name, keeperId, keeperName,
            v003Text(body.related_doc_no, 120), v003Text(body.note, 1000), op.id, op.name,
            v003Department(item.keeper_department), keeperDepartment, t, id, version + 1)
      ]);
      if (v003Changes(results[0]) === 1 && v003Changes(results[1]) === 1) {
        return { ok: true, id: txId, status, keeper_id: keeperId, keeper_name: keeperName,
          keeper_department: keeperDepartment };
      }
    }
    throw new Error('asset_changed_retry');
  });
});

route('v2_003_lookup', async (body, env) => {
  if (!v003CanRead(body, env)) return err('unauthorized', 401);
  const code = v003Text(body.code, 160);
  if (!code) return err('missing_code');
  const [material, asset] = await Promise.all([
    env.DB.prepare(`SELECT id, material_code AS code, barcode, name_zh, name_ko, 'material' AS kind
      FROM v2_003_materials WHERE material_code=? OR barcode=? LIMIT 1`).bind(code, code).first(),
    env.DB.prepare(`SELECT id, asset_code AS code, barcode, name_zh, name_ko, 'asset' AS kind
      FROM v2_003_assets WHERE asset_code=? OR barcode=? LIMIT 1`).bind(code, code).first()
  ]);
  const item = asset || material;
  if (!item) return err('not_found', 404);
  return json({ ok: true, item });
});

route('v2_003_ledger_list', async (body, env) => {
  if (!v003CanRead(body, env)) return err('unauthorized', 401);
  const { limit, offset } = pageParams(body);
  const kind = v003Text(body.kind, 20);
  const search = v003Text(body.search, 120);
  const fieldRequest = !isAdmin(body, env) && (isOpsKey(body, env) || v003IsPublicField(body));
  const fieldOperatorId = fieldRequest ? v003Text(body.operator_id, 80) : '';
  if (fieldRequest && !fieldOperatorId) return err('operator_required');
  const action = v003Text(body.txn_type || body.action_type, 30);
  const department = v003Department(body.department);
  const startRange = kstDayRangeUtc(v003Text(body.start_date, 10));
  const endRange = kstDayRangeUtc(v003Text(body.end_date, 10));

  async function materialRows() {
    const where = ['1=1']; const binds = [];
    if (fieldRequest) { where.push('t.operator_id=?'); binds.push(fieldOperatorId); }
    if (search) { where.push('(m.material_code LIKE ? OR m.name_zh LIKE ? OR t.operator_name LIKE ? OR t.recipient_name LIKE ?)'); for (let i=0;i<4;i++) binds.push('%'+search+'%'); }
    if (action) { where.push('t.txn_type=?'); binds.push(action); }
    if (department) { where.push('t.department=?'); binds.push(department); }
    if (startRange) { where.push('t.created_at>=?'); binds.push(startRange.startUtc); }
    if (endRange) { where.push('t.created_at<?'); binds.push(endRange.endUtc); }
    const w = 'WHERE ' + where.join(' AND ');
    const [count, rows] = await Promise.all([
      env.DB.prepare(`SELECT COUNT(*) AS c FROM v2_003_material_txns t JOIN v2_003_materials m ON m.id=t.material_id ${w}`).bind(...binds).first(),
      env.DB.prepare(`SELECT 'material' AS kind, t.id, t.created_at, t.txn_type AS action_type,
        m.id AS item_id, m.material_code AS item_code, m.name_zh AS item_name, m.unit,
        t.qty_delta, t.qty_before, t.qty_after, t.operator_id, t.operator_name,
        t.recipient_id, t.recipient_name, t.department, t.warehouse_name, t.location_code,
        t.related_doc_no, t.note, t.purpose
        FROM v2_003_material_txns t JOIN v2_003_materials m ON m.id=t.material_id ${w}
        ORDER BY t.created_at DESC LIMIT ? OFFSET ?`).bind(...binds, limit, offset).all()
    ]);
    return { total: Number(count && count.c) || 0, rows: rows.results || [] };
  }

  async function assetRows() {
    const where = ['1=1']; const binds = [];
    if (fieldRequest) { where.push('t.operator_id=?'); binds.push(fieldOperatorId); }
    if (search) { where.push('(a.asset_code LIKE ? OR a.name_zh LIKE ? OR t.operator_name LIKE ? OR t.to_keeper_name LIKE ?)'); for (let i=0;i<4;i++) binds.push('%'+search+'%'); }
    if (action) { where.push('t.action_type=?'); binds.push(action); }
    if (department) {
      where.push("(CASE WHEN t.action_type='return' THEN t.from_department ELSE t.to_department END)=?");
      binds.push(department);
    }
    if (startRange) { where.push('t.created_at>=?'); binds.push(startRange.startUtc); }
    if (endRange) { where.push('t.created_at<?'); binds.push(endRange.endUtc); }
    const w = 'WHERE ' + where.join(' AND ');
    const [count, rows] = await Promise.all([
      env.DB.prepare(`SELECT COUNT(*) AS c FROM v2_003_asset_txns t JOIN v2_003_assets a ON a.id=t.asset_id ${w}`).bind(...binds).first(),
      env.DB.prepare(`SELECT 'asset' AS kind, t.id, t.created_at, t.action_type, t.status_after,
        a.id AS item_id, a.asset_code AS item_code, a.name_zh AS item_name, '' AS unit,
        0 AS qty_delta, 0 AS qty_before, 0 AS qty_after, t.operator_id, t.operator_name,
        t.to_keeper_id AS recipient_id, t.to_keeper_name AS recipient_name,
        CASE WHEN t.action_type='return' THEN t.from_department ELSE t.to_department END AS department,
        t.to_warehouse AS warehouse_name, t.to_location AS location_code,
        t.related_doc_no, t.note, '' AS purpose
        FROM v2_003_asset_txns t JOIN v2_003_assets a ON a.id=t.asset_id ${w}
        ORDER BY t.created_at DESC LIMIT ? OFFSET ?`).bind(...binds, limit, offset).all()
    ]);
    return { total: Number(count && count.c) || 0, rows: rows.results || [] };
  }

  if (kind === 'material') {
    const r = await materialRows();
    return json({ ok: true, items: r.rows, ...pageMeta(r.total, limit, offset) });
  }
  if (kind === 'asset') {
    const r = await assetRows();
    return json({ ok: true, items: r.rows, ...pageMeta(r.total, limit, offset) });
  }
  const [m, a] = await Promise.all([materialRows(), assetRows()]);
  const items = m.rows.concat(a.rows)
    .sort((x, y) => String(y.created_at).localeCompare(String(x.created_at)))
    .slice(0, limit);
  return json({ ok: true, items, ...pageMeta(m.total + a.total, limit, offset) });
});

// =====================================================
// 003 - Purchasing & receiving / 采购与到货
// =====================================================
function v003HumanNo(prefix) {
  const day = kstToday().replace(/-/g, '');
  return String(prefix || 'NO') + '-' + day + '-' + crypto.randomUUID().slice(0, 6).toUpperCase();
}

function v003Lines(value, max = 60) {
  return Array.isArray(value) ? value.slice(0, max) : [];
}

function v003PurchaseStatus(status) {
  const allowed = ['requested', 'purchasing', 'shipped', 'partial_received', 'completed', 'cancelled'];
  return allowed.includes(String(status || '')) ? String(status) : '';
}

async function v003PurchaseDetail(env, orderId) {
  const order = await env.DB.prepare('SELECT * FROM v2_003_purchase_orders WHERE id=?').bind(orderId).first();
  if (!order) return null;
  const [lines, shipments, shipmentItems, attachments, receipts] = await Promise.all([
    env.DB.prepare(`SELECT l.*, m.material_code, m.name_zh, m.name_ko, m.spec, m.unit,
        COALESCE((SELECT SUM(si.expected_qty) FROM v2_003_purchase_shipment_items si
          JOIN v2_003_purchase_shipments s ON s.id=si.shipment_id
          WHERE si.order_line_id=l.id AND s.status!='cancelled'),0) AS scheduled_qty
      FROM v2_003_purchase_order_lines l
      JOIN v2_003_materials m ON m.id=l.material_id
      WHERE l.order_id=? ORDER BY m.category, m.name_zh`).bind(orderId).all(),
    env.DB.prepare(`SELECT s.*,
        (SELECT COUNT(*) FROM v2_003_purchase_shipment_items si WHERE si.shipment_id=s.id) AS item_count,
        (SELECT COUNT(*) FROM v2_attachments a WHERE a.related_doc_type='material_shipment'
          AND a.related_doc_id=s.id AND a.attachment_category='arrival_photo') AS photo_count
      FROM v2_003_purchase_shipments s WHERE s.order_id=? ORDER BY s.created_at DESC`).bind(orderId).all(),
    env.DB.prepare(`SELECT si.*, m.material_code, m.name_zh, m.name_ko, m.spec, m.unit
      FROM v2_003_purchase_shipment_items si
      JOIN v2_003_materials m ON m.id=si.material_id
      WHERE si.shipment_id IN (SELECT id FROM v2_003_purchase_shipments WHERE order_id=?)
      ORDER BY m.category, m.name_zh`).bind(orderId).all(),
    env.DB.prepare(`SELECT * FROM v2_attachments
      WHERE related_doc_type='material_shipment'
        AND related_doc_id IN (SELECT id FROM v2_003_purchase_shipments WHERE order_id=?)
      ORDER BY created_at DESC`).bind(orderId).all(),
    env.DB.prepare(`SELECT * FROM v2_003_purchase_receipts WHERE order_id=? ORDER BY received_at DESC`)
      .bind(orderId).all()
  ]);
  return {
    order,
    lines: lines.results || [],
    shipments: shipments.results || [],
    shipment_items: shipmentItems.results || [],
    attachments: attachments.results || [],
    receipts: receipts.results || []
  };
}

route('v2_003_purchase_summary', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const [requested, purchasing, waiting, partial, discrepancy, completed] = await Promise.all([
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_purchase_orders WHERE status='requested'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_purchase_orders WHERE status='purchasing'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_purchase_orders WHERE status='shipped'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_purchase_orders WHERE status='partial_received'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_purchase_orders WHERE has_discrepancy=1 AND status!='cancelled'").first(),
    env.DB.prepare("SELECT COUNT(*) AS c FROM v2_003_purchase_orders WHERE status='completed'").first()
  ]);
  return json({ ok: true, summary: {
    requested: Number(requested && requested.c) || 0,
    purchasing: Number(purchasing && purchasing.c) || 0,
    waiting: Number(waiting && waiting.c) || 0,
    partial: Number(partial && partial.c) || 0,
    discrepancy: Number(discrepancy && discrepancy.c) || 0,
    completed: Number(completed && completed.c) || 0
  }});
});

route('v2_003_purchase_order_list', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const { limit, offset } = pageParams(body);
  const status = v003PurchaseStatus(body.status);
  const search = v003Text(body.search, 120);
  const where = ['1=1'];
  const binds = [];
  if (status) { where.push('o.status=?'); binds.push(status); }
  if (String(body.discrepancy_only || '') === '1') where.push('o.has_discrepancy=1');
  if (search) {
    where.push(`(o.order_no LIKE ? OR o.requested_by_name LIKE ? OR o.purchaser_name LIKE ?
      OR o.supplier LIKE ? OR o.platform_order_no LIKE ?
      OR EXISTS (SELECT 1 FROM v2_003_purchase_shipments s WHERE s.order_id=o.id AND s.tracking_no LIKE ?))`);
    for (let i = 0; i < 6; i++) binds.push('%' + search + '%');
  }
  const w = 'WHERE ' + where.join(' AND ');
  const [count, rows] = await Promise.all([
    env.DB.prepare(`SELECT COUNT(*) AS c FROM v2_003_purchase_orders o ${w}`).bind(...binds).first(),
    env.DB.prepare(`SELECT o.*,
        COALESCE((SELECT SUM(l.requested_qty) FROM v2_003_purchase_order_lines l WHERE l.order_id=o.id),0) AS requested_total,
        COALESCE((SELECT SUM(l.ordered_qty) FROM v2_003_purchase_order_lines l WHERE l.order_id=o.id),0) AS ordered_total,
        COALESCE((SELECT SUM(l.received_qty) FROM v2_003_purchase_order_lines l WHERE l.order_id=o.id),0) AS received_total,
        (SELECT COUNT(*) FROM v2_003_purchase_order_lines l WHERE l.order_id=o.id) AS line_count,
        (SELECT COUNT(*) FROM v2_003_purchase_order_lines l WHERE l.order_id=o.id
          AND l.ordered_qty>0 AND l.received_qty>=l.ordered_qty) AS completed_line_count,
        (SELECT COUNT(*) FROM v2_003_purchase_shipments s WHERE s.order_id=o.id AND s.status='pending') AS pending_shipment_count
      FROM v2_003_purchase_orders o ${w}
      ORDER BY CASE o.status WHEN 'requested' THEN 0 WHEN 'partial_received' THEN 1 WHEN 'shipped' THEN 2
        WHEN 'purchasing' THEN 3 WHEN 'completed' THEN 4 ELSE 5 END, o.updated_at DESC
      LIMIT ? OFFSET ?`).bind(...binds, limit, offset).all()
  ]);
  return json({ ok: true, items: rows.results || [], ...pageMeta(count && count.c, limit, offset) });
});

route('v2_003_purchase_order_detail', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const id = v003Text(body.id, 100);
  if (!id) return err('missing_id');
  const detail = await v003PurchaseDetail(env, id);
  if (!detail) return err('not_found', 404);
  return json({ ok: true, ...detail });
});

route('v2_003_purchase_request_create', async (body, env) => {
  if (!v003CanField(body, env)) return err('unauthorized', 401);
  const op = v003RequireOperator(body);
  if (!op) return err('operator_required');
  const input = v003Lines(body.lines);
  if (!input.length) return err('purchase_lines_required');
  const seen = new Set();
  const lines = [];
  for (const raw of input) {
    const materialId = v003Text(raw && raw.material_id, 100);
    const qty = v003Number(raw && raw.requested_qty, NaN);
    if (!materialId || seen.has(materialId) || !Number.isFinite(qty) || qty <= 0) return err('invalid_purchase_line');
    seen.add(materialId);
    const material = await env.DB.prepare("SELECT id FROM v2_003_materials WHERE id=? AND status='active'")
      .bind(materialId).first();
    if (!material) return err('material_not_found');
    lines.push({ material_id: materialId, requested_qty: qty, note: v003Text(raw.note, 500) });
  }
  return withIdem(env, body, 'v2_003_purchase_request_create', async () => {
    const orderId = v003Id('PO');
    const orderNo = v003HumanNo('CG');
    const t = now();
    const urgency = ['normal', 'urgent'].includes(String(body.urgency)) ? String(body.urgency) : 'normal';
    const statements = [env.DB.prepare(`INSERT INTO v2_003_purchase_orders
      (id, order_no, status, urgency, warehouse_name, request_reason, requested_by_id, requested_by_name,
       purchaser_name, supplier, purchase_channel, platform_order_no, expected_date, currency, total_amount,
       note, has_discrepancy, closed_reason, created_at, updated_at)
      VALUES(?,?,'requested',?,?,?,?,?,'','','','','','KRW',0,?,0,'',?,?)`)
      .bind(orderId, orderNo, urgency, '', v003Text(body.request_reason, 500),
        op.id, op.name, v003Text(body.note, 1000), t, t)];
    for (const line of lines) {
      statements.push(env.DB.prepare(`INSERT INTO v2_003_purchase_order_lines
        (id, order_id, material_id, requested_qty, ordered_qty, received_qty, unit_cost, note, created_at, updated_at)
        VALUES(?,?,?,?,0,0,0,?,?,?)`)
        .bind(v003Id('POL'), orderId, line.material_id, line.requested_qty, line.note, t, t));
    }
    await env.DB.batch(statements);
    return { ok: true, id: orderId, order_no: orderNo };
  });
});

route('v2_003_purchase_order_update', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const id = v003Text(body.id, 100);
  const op = v003RequireOperator(body) || { id: 'ADMIN', name: '管理员' };
  const order = await env.DB.prepare('SELECT * FROM v2_003_purchase_orders WHERE id=?').bind(id).first();
  if (!order) return err('not_found', 404);
  if (['completed', 'cancelled'].includes(order.status)) return err('purchase_order_closed');
  const existingRs = await env.DB.prepare(`SELECT l.*,
      COALESCE((SELECT SUM(si.expected_qty) FROM v2_003_purchase_shipment_items si
        JOIN v2_003_purchase_shipments s ON s.id=si.shipment_id
        WHERE si.order_line_id=l.id AND s.status!='cancelled'),0) AS scheduled_qty
    FROM v2_003_purchase_order_lines l WHERE l.order_id=?`).bind(id).all();
  const existing = existingRs.results || [];
  const changes = new Map(v003Lines(body.lines).map(x => [v003Text(x && x.id, 100), x]));
  let total = 0;
  let orderedCount = 0;
  const t = now();
  const statements = [];
  for (const line of existing) {
    const raw = changes.get(line.id) || {};
    const orderedQty = v003Number(raw.ordered_qty, line.ordered_qty || line.requested_qty);
    const unitCost = Math.max(0, v003Number(raw.unit_cost, line.unit_cost));
    if (!Number.isFinite(orderedQty) || orderedQty < 0 || orderedQty < v003Number(line.scheduled_qty)) {
      return err('ordered_qty_below_shipped');
    }
    if (orderedQty > 0) orderedCount++;
    total += orderedQty * unitCost;
    statements.push(env.DB.prepare(`UPDATE v2_003_purchase_order_lines
      SET ordered_qty=?, unit_cost=?, note=?, updated_at=? WHERE id=? AND order_id=?`)
      .bind(orderedQty, unitCost, v003Text(raw.note != null ? raw.note : line.note, 500), t, line.id, id));
  }
  if (!orderedCount) return err('ordered_qty_required');
  statements.push(env.DB.prepare(`UPDATE v2_003_purchase_orders SET status='purchasing', purchaser_name=?,
      supplier=?, purchase_channel=?, platform_order_no=?, expected_date=?, currency=?, total_amount=?, note=?, updated_at=?
      WHERE id=?`)
    .bind(op.name, v003Text(body.supplier, 160), v003Text(body.purchase_channel, 100),
      v003Text(body.platform_order_no, 160), normalizeDateOnly(body.expected_date),
      v003Text(body.currency || 'KRW', 12) || 'KRW', Math.round(total * 100) / 100,
      v003Text(body.note != null ? body.note : order.note, 1000), t, id));
  await env.DB.batch(statements);
  return json({ ok: true, id, status: 'purchasing', total_amount: Math.round(total * 100) / 100 });
});

route('v2_003_purchase_shipment_create', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const orderId = v003Text(body.order_id, 100);
  const method = ['express', 'supplier'].includes(String(body.delivery_method)) ? String(body.delivery_method) : '';
  const tracking = v003Text(body.tracking_no, 180);
  if (!method) return err('delivery_method_required');
  if (method === 'express' && !tracking) return err('tracking_no_required');
  const order = await env.DB.prepare('SELECT * FROM v2_003_purchase_orders WHERE id=?').bind(orderId).first();
  if (!order) return err('not_found', 404);
  if (['completed', 'cancelled'].includes(order.status)) return err('purchase_order_closed');
  if (tracking) {
    const duplicate = await env.DB.prepare('SELECT id FROM v2_003_purchase_shipments WHERE tracking_no=? LIMIT 1')
      .bind(tracking).first();
    if (duplicate) return err('duplicate_tracking_no');
  }
  const lineRs = await env.DB.prepare(`SELECT l.*,
      COALESCE((SELECT SUM(si.expected_qty) FROM v2_003_purchase_shipment_items si
        JOIN v2_003_purchase_shipments s ON s.id=si.shipment_id
        WHERE si.order_line_id=l.id AND s.status!='cancelled'),0) AS scheduled_qty
    FROM v2_003_purchase_order_lines l WHERE l.order_id=?`).bind(orderId).all();
  const lineMap = new Map((lineRs.results || []).map(x => [x.id, x]));
  const rawItems = v003Lines(body.items);
  const items = [];
  const seen = new Set();
  for (const raw of rawItems) {
    const lineId = v003Text(raw && raw.order_line_id, 100);
    const qty = v003Number(raw && raw.expected_qty, NaN);
    const line = lineMap.get(lineId);
    if (!line || seen.has(lineId) || !Number.isFinite(qty) || qty <= 0) return err('invalid_shipment_line');
    if (v003Number(line.scheduled_qty) + qty > v003Number(line.ordered_qty) + 0.0001) return err('shipment_qty_exceeds_ordered');
    seen.add(lineId);
    items.push({ line_id: lineId, material_id: line.material_id, qty });
  }
  if (!items.length) return err('shipment_lines_required');
  return withIdem(env, body, 'v2_003_purchase_shipment_create', async () => {
    const shipmentId = v003Id('SHP');
    const shipmentNo = v003HumanNo(method === 'express' ? 'KD' : 'SC');
    const op = v003RequireOperator(body) || { id: 'ADMIN', name: '管理员' };
    const t = now();
    const statements = [env.DB.prepare(`INSERT INTO v2_003_purchase_shipments
      (id, shipment_no, order_id, delivery_method, tracking_no, supplier, expected_date, status,
       received_at, received_by, note, created_by, created_at, updated_at)
      VALUES(?,?,?,?,?,?,?,'pending','','',?,?,?,?)`)
      .bind(shipmentId, shipmentNo, orderId, method, tracking,
        v003Text(body.supplier || order.supplier, 160), normalizeDateOnly(body.expected_date),
        v003Text(body.note, 1000), op.name, t, t)];
    for (const item of items) {
      statements.push(env.DB.prepare(`INSERT INTO v2_003_purchase_shipment_items
        (id, shipment_id, order_line_id, material_id, expected_qty, received_qty, created_at, updated_at)
        VALUES(?,?,?,?,?,0,?,?)`)
        .bind(v003Id('PSI'), shipmentId, item.line_id, item.material_id, item.qty, t, t));
    }
    statements.push(env.DB.prepare(`UPDATE v2_003_purchase_orders
      SET status=CASE WHEN status='partial_received' THEN status ELSE 'shipped' END, updated_at=? WHERE id=?`)
      .bind(t, orderId));
    await env.DB.batch(statements);
    return { ok: true, id: shipmentId, shipment_no: shipmentNo };
  });
});

async function v003ReceivingDetail(env, shipment) {
  const [items, attachments, receipt] = await Promise.all([
    env.DB.prepare(`SELECT si.*, l.unit_cost, l.requested_qty, l.ordered_qty, l.received_qty AS order_received_qty,
        m.material_code, m.name_zh, m.name_ko, m.spec, m.unit, m.warehouse_name, m.location_code
      FROM v2_003_purchase_shipment_items si
      JOIN v2_003_purchase_order_lines l ON l.id=si.order_line_id
      JOIN v2_003_materials m ON m.id=si.material_id
      WHERE si.shipment_id=? ORDER BY m.category, m.name_zh`).bind(shipment.id).all(),
    env.DB.prepare(`SELECT * FROM v2_attachments WHERE related_doc_type='material_shipment'
      AND related_doc_id=? ORDER BY created_at DESC`).bind(shipment.id).all(),
    env.DB.prepare('SELECT * FROM v2_003_purchase_receipts WHERE shipment_id=?').bind(shipment.id).first()
  ]);
  return { shipment, items: items.results || [], attachments: attachments.results || [], receipt: receipt || null };
}

route('v2_003_receiving_pending', async (body, env) => {
  if (!v003CanField(body, env)) return err('unauthorized', 401);
  const method = ['express', 'supplier'].includes(String(body.delivery_method)) ? String(body.delivery_method) : '';
  const binds = [];
  let where = "WHERE s.status='pending'";
  if (method) { where += ' AND s.delivery_method=?'; binds.push(method); }
  const rows = await env.DB.prepare(`SELECT s.*, o.order_no, o.warehouse_name, o.urgency,
      (SELECT COUNT(*) FROM v2_003_purchase_shipment_items si WHERE si.shipment_id=s.id) AS item_count,
      (SELECT COUNT(*) FROM v2_attachments a WHERE a.related_doc_type='material_shipment'
        AND a.related_doc_id=s.id AND a.attachment_category='arrival_photo') AS photo_count
    FROM v2_003_purchase_shipments s JOIN v2_003_purchase_orders o ON o.id=s.order_id
    ${where} ORDER BY CASE o.urgency WHEN 'urgent' THEN 0 ELSE 1 END,
      CASE WHEN s.expected_date='' THEN 1 ELSE 0 END, s.expected_date, s.created_at`).bind(...binds).all();
  return json({ ok: true, items: rows.results || [] });
});

route('v2_003_receiving_lookup', async (body, env) => {
  if (!v003CanField(body, env)) return err('unauthorized', 401);
  const code = v003Text(body.code, 180);
  if (!code) return err('missing_code');
  const shipment = await env.DB.prepare(`SELECT s.*, o.order_no, o.warehouse_name AS requested_warehouse,
      o.requested_by_name, o.purchaser_name, o.purchase_channel, o.platform_order_no
    FROM v2_003_purchase_shipments s JOIN v2_003_purchase_orders o ON o.id=s.order_id
    WHERE s.tracking_no=? OR s.shipment_no=? LIMIT 1`).bind(code, code).first();
  if (!shipment) return err('shipment_not_found', 404);
  const detail = await v003ReceivingDetail(env, shipment);
  return json({ ok: true, ...detail });
});

route('v2_003_receipt_confirm', async (body, env) => {
  if (!v003CanField(body, env)) return err('unauthorized', 401);
  const shipmentId = v003Text(body.shipment_id, 100);
  const op = v003RequireOperator(body);
  if (!shipmentId) return err('missing_shipment_id');
  if (!op) return err('operator_required');
  const shipment = await env.DB.prepare(`SELECT s.*, o.order_no, o.supplier AS order_supplier
    FROM v2_003_purchase_shipments s JOIN v2_003_purchase_orders o ON o.id=s.order_id WHERE s.id=?`)
    .bind(shipmentId).first();
  if (!shipment) return err('shipment_not_found', 404);
  if (shipment.status !== 'pending') {
    const receipt = await env.DB.prepare('SELECT id, receipt_no FROM v2_003_purchase_receipts WHERE shipment_id=?')
      .bind(shipmentId).first();
    return json({ ok: true, duplicate: true, receipt: receipt || null, shipment_status: shipment.status });
  }
  if (shipment.delivery_method === 'supplier') {
    const photo = await env.DB.prepare(`SELECT id FROM v2_attachments WHERE related_doc_type='material_shipment'
      AND related_doc_id=? AND attachment_category='arrival_photo' LIMIT 1`).bind(shipmentId).first();
    if (!photo) return err('arrival_photo_required');
  }
  const itemRs = await env.DB.prepare(`SELECT si.*, l.unit_cost, m.current_qty, m.stock_version,
      m.warehouse_name AS current_warehouse, m.location_code AS current_location
    FROM v2_003_purchase_shipment_items si
    JOIN v2_003_purchase_order_lines l ON l.id=si.order_line_id
    JOIN v2_003_materials m ON m.id=si.material_id WHERE si.shipment_id=?`).bind(shipmentId).all();
  const shipmentItems = itemRs.results || [];
  if (!shipmentItems.length) return err('shipment_lines_required');
  const rawMap = new Map(v003Lines(body.items).map(x => [v003Text(x && x.shipment_item_id, 100), x]));
  const receiptItems = [];
  let hasDiscrepancy = false;
  for (const item of shipmentItems) {
    const raw = rawMap.get(item.id);
    if (!raw) return err('receipt_lines_incomplete');
    const qty = v003Number(raw.received_qty, NaN);
    if (!Number.isFinite(qty) || qty < 0 || qty > 1000000000) return err('invalid_received_qty');
    const location = v003Text(raw.location_code || body.location_code || item.current_location, 80);
    if (qty > 0 && !location) return err('putaway_location_required');
    const difference = Math.round((qty - v003Number(item.expected_qty)) * 10000) / 10000;
    if (Math.abs(difference) > 0.0001) hasDiscrepancy = true;
    receiptItems.push({
      ...item,
      received_qty: qty,
      difference_qty: difference,
      warehouse_name: '',
      location_code: location,
      note: v003Text(raw.note, 500)
    });
  }
  const discrepancyNote = v003Text(body.discrepancy_note, 1000);
  if (discrepancyNote) hasDiscrepancy = true;
  const receiptId = v003Id('REC');
  const receiptNo = v003HumanNo('SH');
  const t = now();
  const statements = [];
  statements.push(env.DB.prepare(`INSERT INTO v2_003_purchase_receipts
    (id, receipt_no, shipment_id, order_id, delivery_method, tracking_no, has_discrepancy,
     discrepancy_note, received_by_id, received_by_name, warehouse_name, received_at, created_at)
    SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM v2_003_purchase_shipments s
    WHERE s.id=? AND s.status='pending'
      AND NOT EXISTS (SELECT 1 FROM v2_003_purchase_receipts r WHERE r.shipment_id=s.id)`)
    .bind(receiptId, receiptNo, shipmentId, shipment.order_id, shipment.delivery_method, shipment.tracking_no,
      hasDiscrepancy ? 1 : 0, discrepancyNote, op.id, op.name, '', t, t, shipmentId));
  for (const item of receiptItems) {
    const receiptItemId = v003Id('RCI');
    statements.push(env.DB.prepare(`INSERT INTO v2_003_purchase_receipt_items
      (id, receipt_id, shipment_item_id, order_line_id, material_id, expected_qty, received_qty,
       difference_qty, warehouse_name, location_code, note, created_at)
      SELECT ?,?,?,?,?,?,?,?,?,?,?,? WHERE EXISTS
        (SELECT 1 FROM v2_003_purchase_receipts WHERE id=? AND shipment_id=?)`)
      .bind(receiptItemId, receiptId, item.id, item.order_line_id, item.material_id, item.expected_qty,
        item.received_qty, item.difference_qty, item.warehouse_name, item.location_code, item.note, t,
        receiptId, shipmentId));
    statements.push(env.DB.prepare(`UPDATE v2_003_purchase_shipment_items SET received_qty=?, updated_at=?
      WHERE id=? AND EXISTS (SELECT 1 FROM v2_003_purchase_receipts WHERE id=?)`)
      .bind(item.received_qty, t, item.id, receiptId));
    if (item.received_qty > 0) {
      statements.push(env.DB.prepare(`UPDATE v2_003_materials SET current_qty=current_qty+?,
        location_code=?, unit_cost=?, supplier=?, stock_version=stock_version+1,
        updated_by=?, updated_at=? WHERE id=?
        AND EXISTS (SELECT 1 FROM v2_003_purchase_receipts WHERE id=?)`)
        .bind(item.received_qty, item.location_code, Math.max(0, v003Number(item.unit_cost)),
          v003Text(shipment.supplier || shipment.order_supplier, 160), op.name, t, item.material_id, receiptId));
      statements.push(env.DB.prepare(`INSERT INTO v2_003_material_txns
        (id, material_id, txn_type, qty_delta, qty_before, qty_after, warehouse_name, location_code,
         recipient_id, recipient_name, purpose, related_doc_no, unit_cost, supplier, note,
         operator_id, operator_name, created_at)
        SELECT ?,m.id,'purchase_inbound',?,m.current_qty-?,m.current_qty,?,?,?,?,?,?,?,?,?,?,?,?
        FROM v2_003_materials m WHERE m.id=?
          AND EXISTS (SELECT 1 FROM v2_003_purchase_receipts WHERE id=?)`)
        .bind(v003Id('MTX'), item.received_qty, item.received_qty, item.warehouse_name, item.location_code,
          '', '', '采购到货', receiptNo, Math.max(0, v003Number(item.unit_cost)),
          v003Text(shipment.supplier || shipment.order_supplier, 160), item.note, op.id, op.name, t,
          item.material_id, receiptId));
    }
  }
  statements.push(env.DB.prepare(`UPDATE v2_003_purchase_order_lines SET
    received_qty=COALESCE((SELECT SUM(ri.received_qty) FROM v2_003_purchase_receipt_items ri
      WHERE ri.order_line_id=v2_003_purchase_order_lines.id),0), updated_at=?
    WHERE order_id=? AND EXISTS (SELECT 1 FROM v2_003_purchase_receipts WHERE id=?)`)
    .bind(t, shipment.order_id, receiptId));
  statements.push(env.DB.prepare(`UPDATE v2_003_purchase_shipments SET status=?, received_at=?, received_by=?, updated_at=?
    WHERE id=? AND status='pending' AND EXISTS (SELECT 1 FROM v2_003_purchase_receipts WHERE id=?)`)
    .bind(hasDiscrepancy ? 'discrepancy' : 'received', t, op.name, t, shipmentId, receiptId));
  statements.push(env.DB.prepare(`UPDATE v2_003_purchase_orders SET
    has_discrepancy=CASE WHEN ?=1 THEN 1 ELSE has_discrepancy END,
    status=CASE
      WHEN (SELECT COALESCE(SUM(received_qty),0) FROM v2_003_purchase_order_lines WHERE order_id=id)
        >= (SELECT COALESCE(SUM(ordered_qty),0) FROM v2_003_purchase_order_lines WHERE order_id=id)
       AND (SELECT COALESCE(SUM(ordered_qty),0) FROM v2_003_purchase_order_lines WHERE order_id=id)>0
       AND NOT EXISTS (SELECT 1 FROM v2_003_purchase_order_lines WHERE order_id=id AND ordered_qty<=0)
      THEN 'completed' ELSE 'partial_received' END,
    updated_at=? WHERE id=? AND EXISTS (SELECT 1 FROM v2_003_purchase_receipts WHERE id=?)`)
    .bind(hasDiscrepancy ? 1 : 0, t, shipment.order_id, receiptId));
  const results = await env.DB.batch(statements);
  if (!v003Changes(results[0])) {
    const existing = await env.DB.prepare('SELECT id, receipt_no FROM v2_003_purchase_receipts WHERE shipment_id=?')
      .bind(shipmentId).first();
    return json({ ok: true, duplicate: true, receipt: existing || null });
  }
  const updatedOrder = await env.DB.prepare('SELECT status, has_discrepancy FROM v2_003_purchase_orders WHERE id=?')
    .bind(shipment.order_id).first();
  return json({ ok: true, id: receiptId, receipt_no: receiptNo, has_discrepancy: hasDiscrepancy,
    order_status: updatedOrder && updatedOrder.status });
});

route('v2_003_purchase_order_close', async (body, env) => {
  if (!isAdmin(body, env)) return err('unauthorized_admin_only', 401);
  const id = v003Text(body.id, 100);
  const mode = String(body.mode || '');
  const reason = v003Text(body.reason, 1000);
  if (!reason) return err('close_reason_required');
  const order = await env.DB.prepare('SELECT * FROM v2_003_purchase_orders WHERE id=?').bind(id).first();
  if (!order) return err('not_found', 404);
  if (['completed', 'cancelled'].includes(order.status)) return json({ ok: true, id, status: order.status });
  const status = mode === 'cancel' ? 'cancelled' : 'completed';
  const t = now();
  const statements = [env.DB.prepare(`UPDATE v2_003_purchase_orders SET status=?,
    has_discrepancy=CASE WHEN ?='completed' THEN 1 ELSE has_discrepancy END,
    closed_reason=?, updated_at=? WHERE id=?`).bind(status, status, reason, t, id)];
  if (status === 'cancelled') {
    statements.push(env.DB.prepare("UPDATE v2_003_purchase_shipments SET status='cancelled', updated_at=? WHERE order_id=? AND status='pending'")
      .bind(t, id));
  }
  await env.DB.batch(statements);
  return json({ ok: true, id, status });
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
    if (!formData) return err("invalid multipart form");
    const k = formData.get("k") || "";
    const file = formData.get("file");
    if (!file) return err("missing file");

    const related_doc_type = v003Text(formData.get("related_doc_type"), 80);
    const related_doc_id = v003Text(formData.get("related_doc_id"), 120);
    const attachment_category = v003Text(formData.get("attachment_category"), 80);
    const uploaded_by = v003Text(formData.get("uploaded_by"), 120);
    const fieldBody = {
      k,
      operator_id: v003Text(formData.get("operator_id"), 80),
      operator_name: v003Text(formData.get("operator_name") || uploaded_by, 120)
    };
    const publicArrival = attachment_category === 'arrival_photo' && related_doc_type === 'material_shipment'
      && v003IsPublicField(fieldBody);
    if (!isOpsAuth(fieldBody, env) && !publicArrival) return err("unauthorized", 401);
    if (!related_doc_type || !related_doc_id) return err("missing attachment target");
    if (attachment_category === 'arrival_photo') {
      if (related_doc_type !== 'material_shipment') return err('invalid_arrival_photo_target');
      if (!String(file.type || '').startsWith('image/')) return err('image_required');
      if (Number(file.size) > 15 * 1024 * 1024) return err('file_too_large');
      const shipment = await env.DB.prepare("SELECT id FROM v2_003_purchase_shipments WHERE id=? AND status='pending'")
        .bind(related_doc_id).first();
      if (!shipment) return err('shipment_not_pending');
    }

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
