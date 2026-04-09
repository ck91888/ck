/**
 * CK Warehouse V2 — Ops Config & i18n
 * Shared config for field execution system (/ck/001/)
 */

var V2_API = "https://ck-v2-api.ck91888.workers.dev";

// ===== OPS 专用访问码（第一轮轻量访问控制，非安全密钥）=====
// 部署时替换为实际值，并在 Cloudflare Workers 设置对应 OPSKEY secret
var OPS_KEY = "ck003";

// ===== localStorage keys =====
var V2_OPS_BADGE_KEY = "ck_v2_ops_badge";       // 工牌原始值 e.g. "EMP-001|张三"
var V2_OPS_AUTH_DAY_KEY = "ck_v2_ops_auth_day";  // 当天确认日期 e.g. "2026-04-07"
var V2_ACTIVE_JOB_KEY = "ck_v2_active_job";
var V2_INTERRUPT_KEY = "ck_v2_interrupt_parent";

// ===== i18n =====
// Ops defaults to bilingual (zh / ko shown together)
// Format: { zh: "中文", ko: "한국어" }
var I18N = {
  // -- Common --
  app_title: { zh: "现场执行系统", ko: "현장 실행 시스템" },
  app_subtitle: { zh: "CK 仓库 · 仁川", ko: "CK 창고 · 인천" },
  home: { zh: "首页", ko: "홈" },
  back: { zh: "返回", ko: "뒤로" },
  confirm: { zh: "确认", ko: "확인" },
  cancel: { zh: "取消", ko: "취소" },
  save: { zh: "保存", ko: "저장" },
  close: { zh: "关闭", ko: "닫기" },
  submit: { zh: "提交", ko: "제출" },
  loading: { zh: "加载中...", ko: "로딩중..." },
  refresh: { zh: "刷新", ko: "새로고침" },
  start: { zh: "开始", ko: "시작" },
  finish: { zh: "结束", ko: "종료" },
  remark: { zh: "备注", ko: "비고" },
  time: { zh: "时间", ko: "시간" },
  status: { zh: "状态", ko: "상태" },
  operator: { zh: "操作员", ko: "작업자" },
  no_data: { zh: "暂无数据", ko: "데이터 없음" },
  success: { zh: "成功", ko: "성공" },
  error: { zh: "失败", ko: "실패" },
  required: { zh: "必填", ko: "필수" },
  upload_photo: { zh: "上传照片", ko: "사진 업로드" },
  view_photo: { zh: "查看照片", ko: "사진 보기" },
  my_task: { zh: "我的当前任务", ko: "내 현재 작업" },
  go_collab: { zh: "去协同中心", ko: "협업센터 이동" },

  // -- Menu items --
  menu_unload: { zh: "到仓卸货", ko: "하차" },
  menu_inbound: { zh: "入库", ko: "입고" },
  menu_order_op: { zh: "按单操作", ko: "주문별 작업" },
  menu_outbound: { zh: "出库作业", ko: "출고 작업" },
  menu_internal: { zh: "库内操作", ko: "창고 내 작업" },
  menu_issue: { zh: "问题点处理", ko: "이슈 처리" },
  menu_import: { zh: "进口专区", ko: "수입 전용" },

  // -- Unload --
  unload_title: { zh: "到仓卸货", ko: "하차" },
  scan_inbound_doc: { zh: "扫/选入库单", ko: "입고서류 스캔/선택" },
  no_doc_temp: { zh: "无单临时卸货", ko: "서류 없이 임시 하차" },
  unload_result: { zh: "卸货结果", ko: "하차 결과" },

  // -- Inbound --
  inbound_title: { zh: "入库", ko: "입고" },
  inbound_direct: { zh: "代发入库", ko: "직배송 입고" },
  inbound_bulk: { zh: "大货入库", ko: "대량화물 입고" },
  inbound_return: { zh: "退件入库", ko: "반품 입고" },

  // -- Order operation --
  order_op_title: { zh: "按单操作", ko: "주문별 작업" },
  pick_direct: { zh: "代发拣货", ko: "직배송 피킹" },
  bulk_op: { zh: "大货操作", ko: "대량화물 작업" },

  // -- Outbound --
  outbound_title: { zh: "出库作业", ko: "출고 작업" },
  pack_direct: { zh: "代发打包", ko: "직배송 포장" },
  load_outbound: { zh: "出库装货", ko: "출고 상차" },
  box_count: { zh: "箱数", ko: "박스 수" },
  pallet_count: { zh: "托数", ko: "팔레트 수" },
  vehicle_photo: { zh: "提货车辆照片", ko: "차량 사진" },

  // -- Internal --
  internal_title: { zh: "库内操作", ko: "창고 내 작업" },
  inventory: { zh: "盘点", ko: "재고조사" },
  disposal: { zh: "废弃处理", ko: "폐기 처리" },
  qc: { zh: "质检", ko: "품검" },
  other_internal: { zh: "其他库内作业", ko: "기타 창고작업" },

  // -- Issue handling --
  issue_title: { zh: "问题点处理", ko: "이슈 처리" },
  issue_pending: { zh: "待处理", ko: "대기중" },
  issue_processing: { zh: "处理中", ko: "처리중" },
  issue_my: { zh: "我处理的", ko: "내 처리건" },
  issue_done: { zh: "已完成", ko: "완료" },
  issue_start_handle: { zh: "开始处理", ko: "처리 시작" },
  issue_finish_handle: { zh: "结束处理", ko: "처리 종료" },
  issue_feedback: { zh: "反馈内容", ko: "피드백 내용" },
  priority_urgent: { zh: "紧急", ko: "긴급" },
  priority_high: { zh: "高", ko: "높음" },
  priority_normal: { zh: "普通", ko: "보통" },
  priority_low: { zh: "低", ko: "낮음" },

  // -- Import --
  import_title: { zh: "进口专区", ko: "수입 전용" },

  // -- Biz class --
  biz_direct_ship: { zh: "代发", ko: "직배송" },
  biz_bulk: { zh: "大货", ko: "대량화물" },
  biz_return: { zh: "退件", ko: "반품" },
  biz_import: { zh: "进口", ko: "수입" },

  // -- Status --
  status_pending: { zh: "待处理", ko: "대기중" },
  status_processing: { zh: "处理中", ko: "처리중" },
  status_working: { zh: "作业中", ko: "작업중" },
  status_responded: { zh: "已反馈", ko: "피드백완료" },
  status_completed: { zh: "已完成", ko: "완료" },
  status_closed: { zh: "已关闭", ko: "종료" },
  status_cancelled: { zh: "已取消", ko: "취소됨" },
  status_draft: { zh: "草稿", ko: "초안" },
  status_issued: { zh: "已下发", ko: "배정됨" },
  status_arrived: { zh: "已到货", ko: "도착" },
  status_field_working: { zh: "现场卸货中", ko: "현장 하차중" },
  status_unloaded_pending_info: { zh: "已卸货·待补充", ko: "하차완료·정보보완" },
  status_awaiting_close: { zh: "待收尾", ko: "마감대기" },

  // -- Interrupt --
  interrupt_unload: { zh: "挂起并临时卸货", ko: "일시정지 후 임시 하차" },
  interrupt_load: { zh: "挂起并临时装货", ko: "일시정지 후 임시 상차" },
  resume_task: { zh: "恢复原任务", ko: "원래 작업 복귀" },
  back_home: { zh: "返回首页", ko: "홈으로" },

  // -- Multi-person --
  active_workers: { zh: "当前参与人员", ko: "현재 참여 인원" },
  worker_count: { zh: "人", ko: "명" },
  join_task: { zh: "加入任务", ko: "작업 참여" },
  existing_task_found: { zh: "已有任务进行中，是否加入？", ko: "진행 중인 작업이 있습니다. 참여하시겠습니까?" },

  // -- Badge / Entry --
  badge_scan_title: { zh: "请扫描工牌开始", ko: "명찰을 스캔하여 시작하세요" },
  badge_scan_btn: { zh: "开始扫码", ko: "스캔 시작" },
  badge_manual_btn: { zh: "手动输入", ko: "수동 입력" },
  badge_change: { zh: "更换工牌", ko: "명찰 변경" },
  badge_invalid: { zh: "无效工牌格式", ko: "잘못된 명찰 형식" },
  badge_format_hint: { zh: "格式: EMP-001|张三 / DA-20260407-01|名字 / DAF-01|名字", ko: "형식: EMP-001|이름 / DA-20260407-01|이름 / DAF-01|이름" },
  badge_ok: { zh: "工牌已识别", ko: "명찰 인식됨" },
  worker_setup: { zh: "设置操作员", ko: "작업자 설정" },

  // -- Unit types --
  unit_container_large: { zh: "大柜", ko: "대형 컨테이너" },
  unit_container_small: { zh: "小柜", ko: "소형 컨테이너" },
  unit_pallet: { zh: "托", ko: "팔레트" },
  unit_carton: { zh: "箱", ko: "박스" },
  unit_cbm: { zh: "方(CBM)", ko: "CBM" },
  planned_qty: { zh: "计划数量", ko: "계획 수량" },
  actual_qty: { zh: "实际数量", ko: "실제 수량" },
  diff: { zh: "差异", ko: "차이" },
  diff_note: { zh: "差异说明", ko: "차이 설명" },
  complete_unload: { zh: "完成卸货", ko: "하차 완료" },
  leave_temp: { zh: "暂时离开", ko: "일시 퇴장" },
  no_doc_unload: { zh: "无单临时卸货", ko: "서류 없이 임시 하차" },
  scan_plan: { zh: "扫码选单", ko: "QR 스캔" },
};

// ===== Bilingual display helper =====
// For ops: show "中文 / 한국어" by default
function t(key) {
  var entry = I18N[key];
  if (!entry) return key;
  return entry.zh + " / " + entry.ko;
}

// Single language
function tz(key) {
  var entry = I18N[key];
  return entry ? entry.zh : key;
}

function tk(key) {
  var entry = I18N[key];
  return entry ? entry.ko : key;
}

// HTML-safe
function esc(s) {
  return String(s || "").replace(/[&<>"']/g, function(c) {
    return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c];
  });
}
