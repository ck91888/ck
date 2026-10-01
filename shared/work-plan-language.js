/* Office work-plan labels use the existing language preference and applyLang pass.
 * Only explicitly bound interface copy is translated; business values are untouched. */
(function(){
'use strict';
if(!location.pathname.startsWith('/002/')||!window.LANG)return;
const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
// Append entries to preserve the keys used by static template labels.
const pairs=[
 ['作业计划','작업 계획'],['＋库存作业计划','＋재고 작업 계획'],['新建作业计划','작업 계획 등록'],['从入库计划创建作业计划','입고 계획에서 작업 계획 등록'],
 ['刷新 / 새로고침','새로고침'],['保存 / 저장','저장'],['关闭 / 닫기','닫기'],['打开 / 열기','열기'],['上一页','이전'],['下一页','다음'],['查看作业单 / 열기','작업서 보기'],
 ['暂无作业计划。入库作业请在入库计划中填写；库存作业可单独新增。','작업 계획이 없습니다. 입고 작업은 입고 계획에서 등록하고, 재고 작업은 별도로 추가하세요.'],
 ['部门 / 부서','부서'],['大货 / 대량','대량화물'],['代发 / 출고대행','출고대행'],['进口 / 수입','수입'],
 ['作业名称','작업명'],['客户','고객'],['关联来源','연결 출처'],['库内库存','창고 재고'],['入库计划','입고 계획'],['出库计划','출고 계획'],
 ['供应链系统单号（库内库存必填）','외부 시스템 번호 (재고 작업 필수)'],['供应链系统单号','외부 시스템 번호'],['箱唛／货物范围','박스 마크·화물 범위'],
 ['本作业计划数量（同步出库必填）','작업 계획 수량 (출고 예약 시 필수)'],['计划数量（同步出库必填）','계획 수량 (출고 예약 시 필수)'],['计划单位','계획 단위'],
 ['关联单据（可下拉选择）','연결 문서 (목록 선택 가능)'],['作业类型 / 작업 종류','작업 종류'],['需操作／加工','작업·가공 필요'],['直接转发（无加工）','작업 없이 전달'],
 ['操作要求','작업 지시'],['接单负责人','접수 담당자'],['要求完成时间','완료 기한'],['货物位置','화물 위치'],['完成期限','완료 기한'],['已有计划时的追加原因','기존 계획에 추가하는 사유'],
 ['查看关联现场任务','연결된 현장 작업 보기'],['打印作业单','작업서 인쇄'],['修改作业要求','작업 지시 수정'],['上传打托明细 Excel','팔레트 명세 Excel 업로드'],
 ['下载打托明细模板 / 양식 다운로드','팔레트 명세 양식 다운로드'],['← 返回本批作业指令','← 해당 화물 작업 목록'],
 ['可先保存作业，反馈明细后再安排出库 / 출고 예약 없이 작업 등록 가능','출고 예약 없이 작업 계획을 등록하고, 명세 전달 후 출고를 예약할 수 있습니다.'],['＋客户已约出库，填写计划（可选）','＋출고 예약 입력 (선택)'],
 ['待安排','배정 대기'],['已分配','배정 완료'],['作业中','작업 중'],['已暂停','일시 중지'],['待审核','검수 대기'],['待整改','재작업 대기'],['审核通过','검수 완료'],['待客户安排','고객 예약 대기'],['已关联出库','출고 연결됨'],['已关闭','종료'],['已作废','취소됨'],
 ['作业资料 / 작업 자료','작업 자료'],['资料 / 자료','자료'],['类型','종류'],['上传人 · 时间','등록자·시간'],['下载 / 다운로드','다운로드'],['历史来源资料 · 원본 자료','기존 문서 자료'],
 ['作业说明／明细 · 작업 자료','작업 설명·명세'],['托唛 · 팔레트 라벨','팔레트 라벨'],['出库单 · 출고 서류','출고 서류'],['产品条码标签 · 상품 바코드','상품 바코드 라벨'],
 ['资料类型 / 자료 종류','자료 종류'],['选择文件 / 파일 선택','파일 선택'],['上传资料 / 업로드','자료 업로드'],['撤下 / 해제','해제'],['每个文件不超过20MB，可连续追加。 / 파일당 20MB 이하','파일당 20MB 이하, 추가 업로드 가능'],
 ['出库计划和现场执行共用这些资料。 / 출고 계획·현장 작업에서 같은 자료를 사용합니다.','출고 계획과 현장 작업에서 같은 자료를 사용합니다.'],
 ['出库安排 / 출고 예약','출고 예약'],['可以提前预约；完成审核后才能装货。 / 사전 예약 가능, 작업 확인 후 상차','사전 예약이 가능하며, 작업 검수 완료 후 상차할 수 있습니다.'],['安排出库 / 출고 예약','출고 예약'],
 ['直接转发 / 작업 없이 전달','작업 없이 전달'],['可发货数量 / 출고 가능 수량','출고 가능 수량'],['确认收货，可安排发货 / 출고 준비 확인','입고·출고 준비 확인'],
 ['关联作业计划 / 연결 작업 계획','연결 작업 계획'],['入库计划／库内库存 → 作业计划 → 出库计划','입고 계획·창고 재고 → 작업 계획 → 출고 계획'],['查找 / 검색','검색'],['打开作业计划 / 작업 계획','작업 계획 열기'],['查看要求与资料 / 자료 보기','작업 지시·자료 보기'],
 ['暂无作业资料。托唛、出库单、产品条码等在这里统一上传。','작업 자료가 없습니다. 팔레트 라벨, 출고 서류, 상품 바코드를 이곳에 등록하세요.'],
 ['随本入库计划建立作业计划（可选、多条）','입고와 연결된 작업 계획 (선택·복수 가능)'],['＋添加作业计划','＋작업 계획 추가'],['删除此计划','이 계획 삭제'],['已预约出库（可选）','출고 예약 (선택)'],
 ['出库日期','출고일'],['分配数量','배정 수량'],['出库方式','출고 방식'],['目的地','목적지'],['运输／提货备注（选填）','운송·픽업 메모 (선택)'],['删除此出库计划','이 출고 계획 삭제'],
 ['箱','박스'],['件','개'],['托','팔레트'],
 ["代发理货上架","직배송 검수·적치"],
 ["大货理货上架","대량화물 검수·적치"],
 ["直进直出","입고 후 직출"],
 ["整托退回","팔레트 반송"],
 ["换单","송장 교체"],
 ["外部入库单号（可多个）","외부 입고번호（복수 가능）"],
 ["按部门填写单号，现场逐单完成；所有部门的关联单号全部完成后，整单才入库完成。","부서별 입고번호를 입력하세요. 모든 부서의 입고번호가 완료되어야 전체 입고가 완료됩니다."],
 ["本批总作业明细","입고 건 전체 작업 명세"],
 ["本入库计划下所有作业共用，点文件名或“下载”即可获取。","이 입고계획의 모든 작업에서 공유합니다. 파일명 또는 다운로드를 누르세요."],
 ["暂未上传总作业明细","전체 작업 명세가 없습니다"],
 ["选择总作业明细（可多选）","전체 작업 명세 선택（복수 가능）"],
 ["上传总作业明细","전체 명세 업로드"],
 ["支持 Excel、CSV、PDF、图片，每个文件不超过 20MB。","Excel, CSV, PDF, 이미지 · 파일당 최대 20MB"],
 ['调整出库数量与单位','출고 수량·단위 변경'],['待操作货量','작업 대상 수량'],
 ['打托或换包装后，可单独填写预计出库成果；例如 28 箱打成 2 托。原作业货量保留，已有预约须逐单核对。','팔레트 작업이나 재포장 후 예상 출고 산출을 별도로 입력합니다. 예: 28박스 → 2팔레트. 기존 작업 수량을 유지하며 예약별 수량을 확인하세요.'],
 ['预计可出库总数量','예상 출고 가능 총수량'],['出库成果单位','출고 산출 단위'],['预约数量','예약 수량'],['调整说明','변경 설명'],['保存出库数量与单位','출고 수량·단위 저장'],
 ['客服提供给仓库的作业资料','고객 담당자가 창고에 제공하는 작업 자료'],['托唛、出库单和产品条码标签由客服上传，仓库在这里下载。','팔레트 라벨, 출고 서류, 상품 바코드 라벨은 고객 담당자가 올리고 창고에서 내려받습니다.'],['暂无客服作业资料','고객 담당자 자료가 없습니다'],['上传客服资料','고객 담당자 자료 업로드'],
 ['仓库反馈给客服的作业说明／明细','창고에서 고객 담당자에게 전달하는 작업 설명·명세'],['现场上传作业说明或明细；办公室收到仓库反馈后也可代录，客服在这里查看下载。','현장에서 작업 설명이나 명세를 올립니다. 사무실에서도 창고 피드백을 대신 등록할 수 있으며 고객 담당자가 여기에서 확인합니다.'],['暂无仓库反馈','창고 작업 피드백이 없습니다'],['上传作业说明／明细','작업 설명·명세 업로드'],['代录仓库反馈','창고 피드백 대리 등록']
, ['新增库存作业计划','재고 작업 계획 등록'],['库存来源','재고 출처'],['关联货物','연결 화물'],['作业要求与货量','작업 지시·화물 수량'],['出库预约（可选）','출고 예약 (선택)'],
 ['供应链系统单号（必填）','외부 시스템 번호 (필수)'],['供应链系统单号（选填）','외부 시스템 번호 (선택)'],['箱唛／货物范围（选填）','박스 마크·화물 범위 (선택)'],['货物位置（选填）','화물 위치 (선택)'],['本计划货量（已知则填）','작업 전 수량 (확인 시 입력)'],['货量单位','화물 수량 단위'],['请选择单位','단위를 선택하세요'],['要求完成时间（选填）','완료 기한 (선택)'],['追加作业原因（必填）','추가 작업 사유 (필수)'],
 ['使用已有库存；入库作业请从入库计划建立，人员在现场派工时选择。','기존 재고 작업입니다. 입고 작업은 입고계획에서 등록하고 작업자는 현장 배정 시 선택하세요.'],['沿用原单据客户和来源，人员在现场派工时选择。','기존 문서의 고객·출처를 사용합니다. 작업자는 현장에서 배정하세요.'],['填写作业前的货量；未预约可暂不填。直接转发或同时预约出库时，数量和单位必填。','작업 전 화물 수량입니다. 미예약 시 비워둘 수 있습니다. 직접 전달·출고 예약 시 수량과 단위가 필수입니다.']
];
const keys=new Map();pairs.forEach(([source,ko],i)=>{const key='ck_plan_'+i;keys.set(source,key);LANG.zh[key]=source.replace(/\s*(?:\/|·)\s*[가-힣].*$/u,'');LANG.ko[key]=ko;});
const key=value=>keys.get(value),text=value=>key(value)?L(key(value)):value;
const attrs=value=>key(value)?' data-i18n="'+key(value)+'"':'';
const html=value=>key(value)?'<span'+attrs(value)+'>'+escape(text(value))+'</span>':escape(value);
const bind=(el,value)=>{const k=key(value);if(k)el.dataset.i18n=k;else delete el.dataset.i18n;el.textContent=text(value);return el;};
function counts(el,total,page,pages){el.dataset.planCounts=JSON.stringify([total,page,pages]);renderCount(el);}
function renderCount(el){const [total,page,pages]=JSON.parse(el.dataset.planCounts);el.textContent=getLang()==='ko'?`총 ${total}건 · ${page} / ${pages} 페이지`:`共 ${total} 批 · 第 ${page} / ${pages} 页`;}
const original=window.applyLang;window.applyLang=function(...args){const result=original.apply(this,args);document.querySelectorAll('[data-plan-counts]').forEach(renderCount);const korean=getLang()==='ko';document.querySelectorAll('[data-ck-batch-print]').forEach(el=>{const count=el.dataset.ckBatchPrint;el.textContent=korean?`이번 입고 작업 계획서 일괄 인쇄 (${count}건)`:`批量打印本批作业单（${count}份）`;});document.querySelectorAll('[data-ck-batch-print-hint]').forEach(el=>{el.textContent=korean?'인쇄 시 “PDF로 저장”을 선택하면 한 파일로 저장됩니다. 작업 계획서마다 새 페이지에서 시작하며 취소된 건은 제외합니다.':'打印时选择“保存为 PDF”，得到一个文件；每张作业单另起一页。已取消的作业单不打印。';});return result;};
window.CKPlanCopy={text,html,attrs,bind,counts};
// Newly opened panels inherit the current language without remounting forms.
// The observer only touches our explicit static labels, never entered values.
function translate(root){if(root.nodeType!==1)return;const items=root.matches('[data-i18n^="ck_plan_"]')?[root]:[];items.push(...root.querySelectorAll('[data-i18n^="ck_plan_"]'));for(const el of items){
 // Older templates contain ordinal keys. Resolve their original visible caption
 // before translating, so adding/removing a label cannot turn an action into a unit.
 const caption=el.textContent.trim();if(caption!==L(el.dataset.i18n)){const resolved=keys.get(caption)||[...keys.values()].find(k=>LANG.zh[k]===caption||LANG.ko[k]===caption);if(resolved)el.dataset.i18n=resolved;}
 const value=L(el.dataset.i18n);if(el.textContent!==value)el.textContent=value;
}}
new MutationObserver(changes=>{for(const change of changes)for(const node of change.addedNodes)translate(node);}).observe(document.body,{childList:true,subtree:true});
})();
