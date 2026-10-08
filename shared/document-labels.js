(function(){
 'use strict';
 const e=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const ko=()=>window.getLang?.()==='ko',copy=(zh,kr)=>ko()?kr:zh;
 const number=x=>x?.display_no||copy('单号待补充','번호 확인 필요');
 const source=x=>x?.source_type==='inventory'?copy('库内库存','창고 재고'):(x?.source_display_no||copy('来源单号待补充','원본 번호 확인 필요'));
 // These helpers return plain text. HTML callers must escape at the rendering boundary.
 // Business references are not interchangeable with internal job primary keys.
 const text=v=>typeof v==='string'||typeof v==='number'?String(v).trim():'';
 const uuid='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
 const internalToken=new RegExp('\\b(?:(?:SOPJOB|JOB(?:-(?:LD|CB))?)-'+uuid+'|JOB-[a-z0-9]{8,10}-[a-z0-9]{6,10})\\b','gi');
 const internalValue=new RegExp('^(?:(?:(?:JOB(?:-(?:LD|CB))?|SOPJOB|NEED|TASK|DISPATCH)-)?'+uuid+'|JOB-[a-z0-9]{8,10}-[a-z0-9]{6,10})$','i');
 const parse=v=>{if(v&&typeof v==='object')return v;try{const p=JSON.parse(v||'{}');return p&&typeof p==='object'?p:{};}catch{return {};}};
 const data=x=>x&&typeof x==='object'?{...(x.job||{}),...x}:{};
 const internal=(v,x={})=>internalValue.test(v)||[x.id,x.job_id,x.jobId,x.current_job_id].some(id=>id&&v===String(id)&&/^(?:JOB|SOPJOB)-/i.test(v));
 const business=(v,x)=>{const parts=(Array.isArray(v)?v:[v]).flatMap(v=>text(v).split('、')).filter(Boolean),safe=parts.filter(v=>!internal(v,x)&&!new RegExp(internalToken.source,'i').test(v));return safe.join('、');};
 const types={unload:['卸货','하차'],unplanned_unload:['临时卸货','임시 하차'],inbound_direct:['代发入库','출고대행 입고'],inbound_bulk:['大货入库','대량 입고'],inbound_return:['退件入库','반품 입고'],inbound_change_order:['换单入库','송장 교체 입고'],pick_direct:['代发拣货','출고대행 피킹'],bulk_op:['大货操作','대량 작업'],pack_direct:['代发打包','출고대행 포장'],change_order:['换单操作','송장 교체'],load_outbound:['出库装货','출고 상차'],outbound_stock_op:['库内操作','창고 내 작업'],inventory:['盘点','재고 조사'],disposal:['废弃处理','폐기 처리'],qc:['质检','품질 검사'],issue_handle:['问题点处理','문제 처리'],other_internal:['仓库整理','창고 정리'],scan_pallet:['过机扫描','스캔 작업'],load_import:['进口装柜','수입 컨테이너 적재'],pickup_delivery_import:['进口取送货','수입 픽업·배송'],verify_scan:['扫码核对','스캔 확인'],courier_receiving:['快递收货','택배 수령']};
 const departments={bulk:['大货','대량'],direct_ship:['代发','출고대행'],import:['进口','수입'],other:['其他','기타']};
 function jobType(value){const x=data(value),key=typeof value==='string'?value:x.job_type||x.jobType||x.last_job_type;return types[key]?copy(...types[key]):copy('作业','작업');}
 function jobDepartment(value){const x=data(value),key=typeof value==='string'?value:x.job_department||x.labor_department||x.department||x.assigned_department;return departments[key]?copy(...departments[key]):'';}
 function jobStart(x){const raw=text(x.job_started_at||x.started_at||x.start||x.job_created_at||x.created_at||x.joined_at);if(!raw)return '';const d=new Date(raw);if(!Number.isFinite(d.getTime()))return '';const local=/(?:Z|[+-]\d{2}:?\d{2})$/i.test(raw)?new Date(d.getTime()+9*3600000).toISOString():raw;return local.slice(0,16).replace('T',' ')+' KST';}
 function jobFallback(x){
  const key=x.job_type||x.jobType||x.last_job_type,defaults=types[key]||['作业','작업'];
  const rawTitle=business(x.job_title||x.title,x),defaultTitles=[...defaults,defaults.join(' / '),key,...(key==='pack_direct'?['代发打包 / 직배송 포장']:[])];
  const title=defaultTitles.includes(rawTitle)?'':rawTitle;
  return [...new Set([jobType(x),title,jobDepartment(x),jobStart(x)].filter(Boolean))].join(' · ');
 }
 function jobLabel(value){
  const x=data(value);
  const canonical=Object.hasOwn(x,'display_business_no')||x.has_business_reference===false;
  const primary=business(canonical?x.display_business_no:x.business_no,x);if(primary)return primary;
  // Source-less records retain legacy RW aliases for lookup. Their primary label
  // uses structured metadata so switching language does not retain a Chinese type.
  if(canonical&&(x.job_type||x.jobType||x.job_title||x.job_department||x.job_started_at))return jobFallback(x);
  const label=business(x.job_label||x.jobLabel,x);if(label)return label;
  for(const key of (canonical?[]:['work_plan_no','work_order_no','external_work_order_no','external_inbound_no','inbound_external_no','pick_doc_nos','pick_doc_no','display_no','no','jobNo','job_no','trip_no','inbound_plan_no','outbound_plan_no'])){const v=business(x[key],x);if(v)return v;}
  return jobFallback(x);
 }
 function jobDispatcher(value){
  const x=data(value);
  // Canonical values come from sop_records task/dispatch ownership. Never infer
  // the dispatcher from a creator, logged-in operator, or crew lead.
  if(Object.hasOwn(x,'dispatcher_name')||Object.hasOwn(x,'dispatcherName'))return text(x.dispatcher_name||x.dispatcherName)||'未记录 / 미기록';
  const dispatch=x.dispatch,kind=x.kind||x.task_kind||x.assignment_kind;
  if(dispatch&&(!dispatch.kind||['task','dispatch'].includes(dispatch.kind))){const state=parse(dispatch.state);return text(state.owner)||'未记录 / 미기록';}
  if(['task','dispatch'].includes(kind)){const state=parse(x.state);return text(x.owner||state.owner)||'未记录 / 미기록';}
  return '未记录 / 미기록';
 }
 const jobSummary=x=>jobLabel(x)+' · '+copy('派审员','배정·검수 담당자')+'：'+jobDispatcher(x);
 const leaveReasons={finished:['已结束','종료'],job_completed:['作业已完成','작업 완료'],dispatcher_finish:['派审员已结束作业','배정·검수 담당자가 작업 종료'],completed:['作业已完成','작업 완료'],job_finished:['作业已完成','작업 완료'],manual_leave:['人员离开作业','작업 이탈'],leave:['人员离开作业','작업 이탈'],removed:['调整人员','인원 변경'],people_removed:['调整人员','인원 변경'],dispatcher_change:['派审员调整人员','배정·검수 담당자가 인원 변경'],operation_finished:['操作完成，待审核','작업 완료 · 검수 대기'],issue_feedback:['问题处理已反馈','문제 처리 보고 완료'],finalize:['作业已完成','작업 완료'],auto_close:['计时已关闭，待核实','시간 기록 종료 · 확인 필요'],auto_cleanup_completed_job:['已完成作业，计时已关闭','완료 작업 시간 기록 종료'],admin_cleanup:['管理员关闭计时','관리자가 시간 기록 종료'],admin_cleanup_completed_job:['管理员关闭已完成作业计时','관리자가 완료 작업 시간 기록 종료'],auto_closed_scan_verify_after_48h:['超时核对作业已关闭，待核实','확인 작업 시간 초과 종료 · 확인 필요'],dispatcher_remove:['派审员调整人员','배정·검수 담당자가 인원 변경'],dispatcher_people:['派审员调整人员','배정·검수 담당자가 인원 변경'],reassigned:['重新分配作业','작업 재배정'],borrowed:['借调装卸','상하차 지원'],crew_borrow:['借调装卸','상하차 지원'],crew_return:['返回原作业','원작업 복귀'],break_start:['开始休息','휴식 시작'],attendance_break:['开始休息','휴식 시작'],checkout:['已下班','퇴근'],attendance_checkout:['已下班','퇴근'],job_cancelled:['作业已取消','작업 취소'],cancelled:['作业已取消','작업 취소'],paused:['作业已暂停','작업 일시 중지'],pause:['作业已暂停','작업 일시 중지']};
 function jobLeaveReason(value){const v=text(value);if(!v)return '';if(leaveReasons[v])return copy(...leaveReasons[v]);if(v.startsWith('borrow:'))return copy('借调装卸','상하차 지원');if(v.startsWith('attendance_stale:'))return copy('跨日计时已关闭，待核实','이전 근무 시간 기록 종료 · 확인 필요');if(v==='attendance:checkout'||v==='attendance:verified_departure')return copy('已下班，作业计时结束','퇴근 · 작업 시간 기록 종료');if(v.startsWith('attendance:break'))return copy('开始休息','휴식 시작');if(v.startsWith('pause:'))return copy('作业已暂停','작업 일시 중지')+(v.slice(6)?' · '+jobLeaveReason(v.slice(6)):'');if(/^[a-z][a-z0-9]*(?:[_:.-][a-z0-9_-]+)+$/i.test(v)||new RegExp('(?:SOPJOB|JOB)-'+uuid,'i').test(v))return copy('已结束，待核实','종료 · 확인 필요');return v;}
 function jobHistoryText(value,context){const v=typeof value==='object'?JSON.stringify(value,null,2):String(value??''),x=data(context),ids=[x.id,x.job_id,x.jobId,x.current_job_id].filter(Boolean).map(String);return v.replace(internalToken,id=>ids.includes(id)?jobLabel(x):copy('关联作业（详情未记录）','연결 작업（상세 미기록）'));}
 window.CKDocumentLabels={number,source,jobLabel,jobDispatcher,jobSummary,jobType,jobDepartment,jobLeaveReason,jobHistoryText};

 function page(x){
  if(!x?.id||!x.display_no)throw Error(copy('请刷新作业计划后再打印','작업 계획을 새로고침 후 인쇄하세요'));
  if(x.status==='cancelled'||x.source_cancelled)throw Error(copy('已取消的作业计划不能打印','취소된 작업 계획서는 인쇄할 수 없습니다'));
  qrcode.stringToBytes=qrcode.stringToBytesFuncs['UTF-8'];
  const code=qrcode(0,'M');
  code.addData('CKWORK|'+x.id+'|'+(x.requirement_version||1));
  code.make();
  const qr=code.createSvgTag({cellSize:3,margin:12,scalable:true});
  const sourceTitle=x.source_type==='inbound'?copy('来源入库计划','원본 입고 계획'):x.source_type==='outbound'?copy('来源出库计划','원본 출고 계획'):copy('货物来源','화물 출처');
  return `<section class="sheet"><header><div><h1>${copy('CK 仓库作业计划','CK 창고 작업 계획서')}</h1><p>${copy('客户','고객')}：${e(x.customer)}</p><p class="number">${copy('作业计划号','작업 계획번호')}：${e(number(x))}</p><span class="version">${copy('要求版本','요구사항 버전')} ${e(x.requirement_version||1)}</span></div>${qr}</header><p><b>${sourceTitle}：${e(source(x))}</b></p>${x.supply_chain_no?'<p>'+copy('供应链单号','공급망 번호')+'：'+e(x.supply_chain_no)+'</p>':''}<h2>${e(x.title)}</h2>${x.deadline?'<p>'+copy('要求完成日期','완료 예정일')+'：'+e(window.CKWorkDate?.day(x.deadline)||'—')+'</p>':''}<p>${copy('货物范围','화물 범위')}：${e(x.scope_text||copy('见要求','요구사항 참조'))}</p><pre>${e(x.instructions)}</pre><p>${copy('卸货时已完成的打托、分货，请交接说明，后续只执行剩余内容。','하차 중 완료한 팔레트·분류 작업은 인계하고 남은 작업만 진행하세요.')}</p><div class="line">${copy('派审员','담당자')}：________　${copy('操作人员','작업자')}：________________</div><div class="line">${copy('完成明细','완료 내역')}：____________________________________</div><div class="line">${copy('审核','검수')}：________　${copy('日期','날짜')}：________________</div></section>`;
 }
 function print(items,title){
  if(!items.length)throw Error(copy('本批没有可打印的作业单','인쇄할 작업 계획서가 없습니다'));
  // Render before opening the window so an invalid record never leaves a blank print tab.
  const pages=items.map(page).join('');
  const w=window.open('','_blank');if(!w)throw Error(copy('请允许浏览器打开打印窗口','인쇄 창을 허용하세요'));
  const html=`<!doctype html><html lang="${ko()?'ko':'zh'}"><head><meta charset="utf-8"><title>${e(title)} ${copy('作业单','작업 계획서')}</title><style>@page{size:A4;margin:15mm}*{box-sizing:border-box}body{font:16px/1.7 'Microsoft YaHei','Malgun Gothic',sans-serif;color:#111;margin:0;background:#eef2f1}.sheet{width:210mm;min-height:297mm;margin:20px auto;padding:15mm;background:#fff;box-shadow:0 2px 12px #ccd4d0}header{display:flex;justify-content:space-between;align-items:center;border-bottom:3px solid #111;padding-bottom:14px;gap:16px}svg{width:38mm;height:38mm;flex-shrink:0}h1{font-size:25px}h2{font-size:22px}pre{font:inherit;white-space:pre-wrap;overflow-wrap:anywhere;border:1px solid #aaa;padding:18px}.number{font-size:19px;font-weight:700}.version{font-size:13px;color:#444}.line{border-bottom:1px solid #aaa;padding:20px 0}@media print{body{background:#fff}.sheet{width:auto;min-height:0;margin:0;padding:0;box-shadow:none}.sheet:not(:last-child){break-after:page;page-break-after:always}header,pre{break-inside:avoid}}</style></head><body>${pages}</body></html>`;
  w.document.write(html);w.document.close();w.focus();w.print();
 }
 window.CKNeedPrint=x=>window.CKSession?.request
  ?window.CKSession.request('sop_get',{id:x.id}).then(r=>print([r.record],number(r.record)))
  :print([x],number(x));
 const printBatch=g=>print((g?.items||[]).filter(x=>x.status!=='cancelled'&&!x.source_cancelled),g?.display_no||copy('本批','이번 일괄'));
 window.CKNeedBatchPrint=g=>{
  if(!window.CKSession?.request)return printBatch(g);
  const query=g.key?{group_key:g.key}:g.source_type&&g.source_id?{source_type:g.source_type,source_id:g.source_id}:{need_id:g.items?.[0]?.id};
  return window.CKSession.request('sop_need_groups',query).then(r=>{
   const fresh=(r.items||[]).find(x=>g.key?x.key===g.key:x.items?.some(i=>i.id===g.items?.[0]?.id));
   return printBatch(fresh);
  });
 };
})();
