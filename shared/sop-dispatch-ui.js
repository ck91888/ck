(function(){
 'use strict';
 window.CKNativeTaskButton=function(job){
  const b=document.createElement('button');b.type='button';b.className='btn btn-outline ck-native-task';
  const title=document.createElement('strong'),info=document.createElement('span'),crew=document.createElement('small');
  title.textContent=window.CKDocumentLabels.jobLabel(job);
  info.textContent=[window.CKDocumentLabels.jobHistoryText(job.title,job)||window.CKDocumentLabels.jobType(job),job.customer].filter(Boolean).join(' · ');
  crew.textContent='作业人员 / 작업자: '+((job.workers||[]).map(w=>w.name).join('、')||(job.status==='assigned'?'待开始 / 시작 대기':'暂无人员 / 인원 없음'));
  const dispatcher=document.createElement('small');dispatcher.className='ck-task-dispatcher';dispatcher.textContent='派审员 / 배정·검수 담당자：'+window.CKDocumentLabels.jobDispatcher(job);b.append(title,info,dispatcher,crew);b.onclick=async()=>{b.disabled=true;try{await CKOpenNativeJob(job);}finally{b.disabled=false;}};return b;
 };
 const startActions=new Set(['v2_unload_job_start','v2_unplanned_unload_start','v2_inbound_job_start','v2_import_delivery_job_start','v2_outbound_load_start','v2_outbound_stock_op_start','v2_issue_handle_start','v2_pick_job_start','v2_pick_job_start_by_docs','v2_bulk_op_job_start','v2_ops_job_start','v2_verify_job_start']);
 window.CKViewRestingJob=function(detail){
  const dialog=document.createElement('dialog');dialog.className='ck-workflow';dialog.innerHTML='<h2>本人休息中的任务 / 휴식 중인 내 작업</h2><p data-summary></p><p>查看不会恢复计时。结束本人休息后，仅在原任务仍允许且仍属本任务人员时恢复。整个任务暂停时仍须派审员恢复任务。 / 조회만으로 작업시간이 재개되지 않습니다.</p><p role="alert"></p><div class="ck-buttons"><button data-return>结束本人休息 / 내 휴식 종료</button><button data-close>关闭 / 닫기</button></div>';
  dialog.querySelector('[data-summary]').textContent=window.CKDocumentLabels.jobSummary(detail);document.body.append(dialog);dialog.showModal();
  const close=()=>{dialog.close();dialog.remove();};dialog.querySelector('[data-close]').onclick=close;dialog.oncancel=e=>{e.preventDefault();close();};
  const request={job_id:detail.job.id,client_req_id:crypto.randomUUID()},button=dialog.querySelector('[data-return]');button.onclick=async()=>{if(button.disabled)return;button.disabled=true;try{await CKSession.request('sop_native_rest_return',request);close();await window.loadDispatchTasks?.();goPage('home');}catch(e){dialog.querySelector('[role=alert]').textContent=e.message;button.disabled=false;}};
 };
 window.CKInstallDispatch=function(){
  window.CKChooseNativeStaff=async payload=>{const {startDepartment,departments}=await import('/shared/labor-department.js');return chooseStaff(startDepartment(payload),departments,payload);};
  let lead=null;try{lead=JSON.parse(sessionStorage.getItem('ck_test_active_lead')||'null');}catch{}
  window.getWorkerId=()=>lead?.id||'';window.getWorkerName=()=>lead?.name||'';window.getBadge=()=>lead?lead.id+'|'+lead.name:'';
  window.CKSetNativeLead=function(person){if(person){lead=person;sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));}};
  window.CKOpenNativeJob=async function(job){
   try{

    if(job.task_kind==='task'){window.CKClearNativeJob();goPage('bulk_op',{task:job.id,external:false});return;}
     if(job.task_kind==='legacy')await CKSession.request('sop_native_adopt',{job_id:job.id,client_req_id:crypto.randomUUID()});
     const r=await api({action:'v2_ops_job_detail',job_id:job.id});
    if(r?.ok&&r.can_view_resting&&!r.can_manage_dispatch){window.CKViewRestingJob(r);return;}
    if(!r?.ok||!r.can_manage_dispatch)throw Error(r?.error||'你已不在此任务中，请联系派工人 / 배정 담당자에게 문의하세요');
    if(!['pending','working','awaiting_close','paused'].includes(r.job.status))throw Error('任务已结束，请刷新列表 / 작업 종료, 목록을 새로고침하세요');
    if(job.job_type==='courier_receiving')return window.CKOpenCourierBatch(job.id);
    const state=JSON.parse(r.dispatch.state),crew=r.workers.filter(w=>!w.left_at).map(w=>({id:w.worker_id,name:w.worker_name}));
    // Viewing a resting crew must not create a work segment or widen access.
    // The server authorization above still decides who may open this job.
    const assigned=Array.isArray(state.workers)?state.workers:[];
    const previous=r.workers.find(w=>w.worker_id===state.lead_id)||r.workers[0];
    lead=crew.find(w=>w.id===state.lead_id)||crew[0]||state.last_lead||assigned.find(w=>w.id===state.lead_id)||(previous?{id:previous.worker_id,name:previous.worker_name}:null);
    if(!lead)throw Error('任务人员信息缺失，请联系管理员 / 작업자 정보를 확인하세요');
    sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));
    if(r.job.job_type==='unload'&&r.job.related_doc_type==='field_feedback')localStorage.setItem('v2_unplanned_fb_id',r.job.related_doc_id);
    else localStorage.removeItem('v2_unplanned_fb_id');
    saveActiveJob(job.id,null);if(r.job.job_type==='bulk_op'){goPage('bulk_op',{task:job.id,external:true,detail:r});return;}if(r.job.job_type==='issue_handle')window._currentIssueId=r.job.related_doc_id;goMyTask();
   }catch(e){alert(e.message);}
  };
  window.CKClearNativeJob=function(){lead=null;sessionStorage.removeItem('ck_test_active_lead');clearActiveJob();};
  // The manager dispatches several crews; the backend still checks every worker's occupancy.
  window.hasOtherActiveJob=()=>false;
  window.checkMyActiveJob=async()=>{};window.restoreActiveJob=()=>{};
  const original=window.api;const retries=new Map();
  window.api=async function(body){
   if(!startActions.has(body.action))return original(body);
   // Freeze document numbers before the staff camera can start.
   body=structuredClone(body);
   const codes=[...(Array.isArray(body.pick_doc_nos)?body.pick_doc_nos:body.pick_doc_nos?String(body.pick_doc_nos).split(','):[]),body.external_inbound_no,body.work_order_no].filter(Boolean);
   const scanError=codes.map(x=>window.CKDocumentCode?.documentCodeError(x)).find(Boolean);
   if(scanError)return {ok:false,error:scanError};
   await window.stopAllManagedQrScanners?.();
   const fingerprint=JSON.stringify(Object.fromEntries(Object.entries(body).filter(([k])=>!['client_req_id','worker_id','worker_name','handler_id','handler_name'].includes(k)).sort(([a],[b])=>a.localeCompare(b))));
   let pending=retries.get(fingerprint);
   if(!pending){const {startDepartment,departments}=await import('/shared/labor-department.js');const staff=await chooseStaff(startDepartment(body),departments,body);if(!staff)return {ok:false,error:'已取消派工'};pending={payload:{...body,client_req_id:body.client_req_id||crypto.randomUUID()},staff};retries.set(fingerprint,pending);}
   const staff=pending.staff;
   try{
    const r=await CKSession.request('sop_native_start',{payload:pending.payload,...staff});
    retries.delete(fingerprint);lead=r.lead||staff.workers.find(x=>x.id===staff.lead_id);sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));
    return r;
   }catch(e){if(e.businessError&&!e.message.includes('任务已建立'))retries.delete(fingerprint);return {ok:false,error:e.message};}
  };
 };
 // Native job dispatch and courier dispatch share this actual dialog and picker.
 window.CKNativePeopleDialog=function(options){
  let resolve;const promise=new Promise(r=>resolve=r),dialog=document.createElement('dialog');dialog.className='ck-workflow'+(options.className?' '+options.className:'');
  dialog.innerHTML='<form><h2>'+options.title+'</h2><p>'+options.description+'</p>'+(options.beforeFields||'')+CKPeopleFields()+(options.afterFields||'')+'<p role="alert"></p><div class="ck-buttons"><button type="submit">'+options.submitLabel+'</button><button type="button" data-cancel'+(options.cancelLight?' class="light"':'')+'>'+ (options.cancelLabel||'取消')+'</button></div></form>';
  document.body.append(dialog);const form=dialog.querySelector('form'),error=dialog.querySelector('[role=alert]'),button=form.querySelector('[type=submit]'),cancel=dialog.querySelector('[data-cancel]');dialog.showModal();
  const picker=CKPeoplePicker(form,{workers:options.workers||[],leadId:options.leadId||'',allowEmpty:options.allowEmpty||false,error,borrowContext:options.borrowContext||null});let done=false,busy=false;
  const update=()=>{if(done)return;let valid=true;if(options.disableUntilValid)try{picker.read();}catch{valid=false;}button.disabled=busy||!valid;cancel.disabled=busy;};
  const observer=new MutationObserver(update);observer.observe(form,{subtree:true,childList:true,characterData:true});form.addEventListener('change',update);update();
  const close=async value=>{if(done)return;done=true;observer.disconnect();await picker.destroy();dialog.close();dialog.remove();resolve(value);};
  cancel.onclick=()=>close(null);dialog.oncancel=e=>{e.preventDefault();if(!busy)close(null);};
  form.onsubmit=async e=>{e.preventDefault();if(button.disabled||done)return;busy=true;update();try{await picker.ready();if(done)return;const result=await options.onSubmit(picker.read(),form);await close(result);}catch(e){if(!done)error.textContent=e.message;}finally{busy=false;update();}};
  return {dialog,form,picker,promise,close};
 };
 function chooseStaff(department,departments,payload){
  const borrowable=['v2_unload_job_start','v2_unplanned_unload_start','v2_outbound_load_start'].includes(payload?.action)||payload?.action==='v2_ops_job_start'&&payload.job_type==='load_outbound';
  return CKNativePeopleDialog({title:'负责人分配本次作业人员',description:'扫描实际操作人员工牌；确认后按原业务流程开始计时。',beforeFields:'<label>本次用工部门 / 작업 부서<select name="labor_department" required><option value="">请选择 / 선택하세요</option>'+Object.entries(departments).map(([k,v])=>'<option value="'+k+'" '+(k===department?'selected':'')+'>'+v+'</option>').join('')+'</select></label>',afterFields:'<label>预计操作分钟<input name="minutes" type="number" min="1" step="1" value="30" required></label>',submitLabel:'确认人员并开始',borrowContext:borrowable?{payload}:null,onSubmit:(staff,form)=>({...staff,labor_department:form.elements.labor_department.value,estimated_minutes:Number(form.elements.minutes.value)})}).promise;
 }
})();
