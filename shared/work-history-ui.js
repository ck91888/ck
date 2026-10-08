(function(){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const text=(zh,ko)=>window.getLang?.()==='ko'?ko:zh;
const visible=(value,context)=>window.CKDocumentLabels?.jobHistoryText?window.CKDocumentLabels.jobHistoryText(String(value??''),context):String(value??'');
const isJob=record=>record?.kind==='task'||record?.kind==='dispatch'||!!record?.job_type;
const isSopJob=record=>['task','dispatch'].includes(record?.kind||record?.task_kind);
const historyContext=(record,state)=>{const x={...record,...state};if(record?.id){x.id=record.id;for(const key of ['job_id','jobId','current_job_id'])if(x[key]&&x[key]!==record.id)delete x[key];}return x;};
const day=value=>{const raw=String(value??'').trim();if(!raw)return '';const parsed=new Date(raw);if(!Number.isFinite(parsed.getTime()))return '';return /(?:Z|[+-]\d{2}:\d{2})$/.test(raw)?new Date(parsed.getTime()+9*3600000).toISOString().slice(0,10):raw.slice(0,10);};
const time=value=>{const d=value?new Date(value):new Date(NaN);return Number.isFinite(d.getTime())?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Seoul',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(d)+' KST':text('时间未记录','시간 미기록');};
window.CKWorkDate={day,time};
const actions={sop_task_create:['创建派工任务','배정 작업 생성'],sop_task_dispatch:['分配现场任务','현장 작업 배정'],sop_task_start:['开始作业','작업 시작'],sop_task_people:['调整参与人员','작업 인원 변경'],sop_task_pause:['暂停作业','작업 일시 중지'],sop_task_finish:['提交作业成果','작업 산출 제출'],sop_task_review:['审核作业成果','작업 산출 검수'],sop_task_complete_review:['审核通过并完成','검수 통과·완료'],sop_task_delegate:['授权任务代办','작업 대리 권한 부여'],sop_task_cancel:['作废任务','작업 취소'],sop_native_start:['派工并开始','배정 및 시작'],sop_native_people:['调整参与人员','작업 인원 변경'],sop_need_create:['创建作业计划','작업 계획 생성'],sop_need_from_outbound:['建立关联作业计划','연결 작업 계획 생성'],sop_need_update:['修改作业要求','작업 지시 수정'],sop_work_material_upload:['上传作业资料','작업 자료 업로드'],sop_work_material_remove:['移除作业资料','작업 자료 제거'],sop_batch_work_material_upload:['上传本批总作业明细','전체 작업 명세 업로드'],sop_batch_work_material_remove:['移除本批总作业明细','전체 작업 명세 제거'],sop_batch_work_material_restore:['恢复本批总作业明细','전체 작업 명세 복원'],sop_need_schedule:['安排出库计划','출고 계획 예약'],sop_need_schedule_batch:['安排多条出库计划','복수 출고 계획 예약'],sop_need_forward_ready:['确认收货及可发货数量','입고·출고 가능 수량 확인'],sop_need_shipping_basis:['调整出库数量与单位','출고 수량·단위 변경'],sop_need_details:['更新作业成果明细','작업 산출 명세 변경'],sop_need_forward:['确认明细已转发客户','고객에게 명세 전달 확인'],sop_need_plan_quantity:['调整预约出库数量','출고 예약 수량 변경'],sop_need_cancel:['作废作业计划','작업 계획 취소'],v2_inbound_plan_bind_external:['补充或更正外部入库单号','외부 입고번호 보완·수정'],v2_inbound_plan_confirm_issue:['确认入库计划已打印下发','입고계획 인쇄·배포 확인']};
const states={pending:['待安排','배정 대기'],assigned:['已分配','배정 완료'],working:['作业中','작업 중'],paused:['已暂停','일시 중지'],awaiting_review:['待审核','검수 대기'],rework:['待整改','재작업 대기'],completed:['审核通过','검수 완료'],waiting_customer:['待客户安排','고객 예약 대기'],linked:['已关联出库','출고 연결됨'],closed:['已关闭','종료'],cancelled:['已作废','취소됨']};
const fields={job_id:['关联任务','연결 작업'],task_id:['关联任务','연결 작업'],current_job_id:['当前任务','현재 작업'],job_type:['作业类型','작업 종류'],dispatcher_name:['派审员','배정·검수 담당자'],leave_reason:['结束原因','종료 사유'],title:['作业名称','작업명'],customer:['客户','고객'],instructions:['作业要求','작업 지시'],scope_text:['货物范围','화물 범위'],owner:['负责人','담당자'],location:['货物位置','화물 위치'],deadline:['要求完成日期','완료 예정일'],status:['进度','진행 상태'],planned_quantity:['作业货量','작업 수량'],planned_unit:['货量单位','수량 단위'],operation_kind:['作业类型','작업 종류'],result:['审核成果','검수 산출'],shipping_basis:['预计可出库成果','예상 출고 산출'],links:['出库安排','출고 예약'],details:['成果明细','산출 명세'],forwarded:['明细转发客户','고객 명세 전달'],last_material_change:['资料变更','자료 변경'],requirement_version:['作业要求版本','작업 지시 버전'],material_version:['资料版本','자료 버전'],external_inbound_no:['外部入库单号','외부 입고번호'],bulk_external_inbound_no:['大货外部入库单号','대량화물 입고번호'],revision:['版本','버전']};
const parse=value=>{if(value&&typeof value==='object')return value;try{const x=JSON.parse(value||'{}');return x&&typeof x==='object'?x:{};}catch{return {};}};
const unit=v=>({箱:['箱','박스'],件:['件','개'],托:['托','팔레트'],单:['单','건'],批:['批','회']}[v]?text(...{箱:['箱','박스'],件:['件','개'],托:['托','팔레트'],单:['单','건'],批:['批','회']}[v]):String(v||''));
function value(key,v,context){
 if(key==='dispatcher_name'||(key==='owner'&&isSopJob(context)))return window.CKDocumentLabels?.jobDispatcher?.({dispatcher_name:v})||text('未记录','미기록');
 if(v==null||v==='')return text('未填写','미입력');
 if(['job_id','task_id','current_job_id'].includes(key))return visible(v,String(v)===String(context?.id||'')?context:undefined);
 if(key==='job_type')return window.CKDocumentLabels?.jobType?.(v)||text('作业','작업');
 if(key==='leave_reason')return window.CKDocumentLabels?.jobLeaveReason?.(v)||visible(v,context);
 if(key==='deadline')return day(v)||text('未填写','미입력');
 if(key==='status')return states[v]?text(...states[v]):text('未识别状态','미확인 상태');
 if(key==='operation_kind')return v==='direct_forward'?text('直接转发，无加工','작업 없이 전달'):v==='operation'?text('需操作／加工','작업·가공 필요'):text('未识别类型','미확인 종류');
 if(key==='planned_unit')return unit(v);
 if(key==='forwarded')return v?text('已转发','전달 완료'):text('未转发','미전달');
 if(key==='last_material_change'){const a={upload:['上传','업로드'],remove:['移除','제거'],restore:['恢复','복원'],batch_upload:['上传总明细','전체 명세 업로드'],batch_remove:['移除总明细','전체 명세 제거'],batch_restore:['恢复总明细','전체 명세 복원']}[v.action];return (a?text(...a):text('更新资料','자료 변경'))+' · '+String(v.file_name||text('文件名未记录','파일명 미기록'));}
 if(['result','shipping_basis'].includes(key))return [v.quantity!=null?String(v.quantity)+' '+unit(v.unit):'',String(v.description||v.reason||'')].filter(Boolean).join(' · ')||text('内容未记录','내용 미기록');
 if(key==='details')return [String(v.filename||''),v.version?'V'+v.version:'',v.rows?.length!=null?v.rows.length+' '+text('行','행'):''].filter(Boolean).join(' · ')||text('内容未记录','내용 미기록');
 if(key==='links')return Array.isArray(v)?v.map(x=>(x.display_no||text('关联出库计划','연결 출고 계획'))+' · '+(x.quantity??text('数量未记录','수량 미기록'))+' '+unit(x.unit)).join('；')||text('未安排','미예약'):text('未安排','미예약');
 return ['string','number','boolean'].includes(typeof v)?String(v):text('内容未记录','내용 미기록');
}
function eventHtml(event,record){
 const before=parse(event.before_json),after=parse(event.after_json),changed=Object.keys(fields).filter(k=>JSON.stringify(before[k])!==JSON.stringify(after[k]));
 const name=actions[event.action]?text(...actions[event.action]):text('更新作业记录','작업 기록 변경');
 const context=historyContext(record,{...before,...after}),title=visible(after.title||before.title||record?.title||'',context),summary=isJob(context)?window.CKDocumentLabels?.jobSummary?.(context):'',object=[summary,title].filter((v,i,all)=>v&&all.indexOf(v)===i).join(' · ')||text('作业计划','작업 계획');
 const fieldName=key=>key==='owner'&&isSopJob(context)?text('派审员','배정·검수 담당자'):text(...fields[key]);
 return '<article class="ck-audit-event"><p><strong>'+esc(event.actor_name||text('操作人未记录','작업자 미기록'))+'</strong> · '+esc(time(event.created_at))+'</p><p>'+esc(name)+' · '+esc(object)+'</p><details><summary>'+text('查看关键变化','주요 변경 보기')+'</summary>'+(changed.length?changed.map(k=>'<div class="ck-audit-change"><strong>'+esc(fieldName(k))+'</strong><div><span>'+text('修改前','변경 전')+'：</span>'+esc(visible(value(k,before[k],historyContext(record,before)),historyContext(record,before)))+'</div><div><span>'+text('修改后','변경 후')+'：</span>'+esc(visible(value(k,after[k],historyContext(record,after)),historyContext(record,after)))+'</div></div>').join(''):'<p>'+text('无可显示的业务变化明细','표시할 업무 변경 내역이 없습니다')+'</p>')+'</details></article>';
}
const records=new WeakMap();
function render(host,events,record){const opened=[...host.querySelectorAll('.ck-audit-event>details')].map(x=>x.open);host.innerHTML='<summary>'+text('操作历史（最近100次）','작업 이력（최근100건）')+'</summary>'+events.map(x=>eventHtml(x,record)).join('');[...host.querySelectorAll('.ck-audit-event>details')].forEach((x,i)=>x.open=!!opened[i]);}
window.CKWorkHistory=(host,events,record)=>{host.className='card ck-work-audit';records.set(host,{events,record});render(host,events,record);};
const previous=window.applyLang;if(typeof previous==='function')window.applyLang=function(...args){const out=previous.apply(this,args);document.querySelectorAll('.ck-work-audit').forEach(host=>{const data=records.get(host);if(data)render(host,data.events,data.record);});return out;};
})();
