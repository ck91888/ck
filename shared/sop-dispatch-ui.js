(function(){
 'use strict';
 const startActions=new Set(['v2_unload_job_start','v2_unplanned_unload_start','v2_inbound_job_start','v2_import_delivery_job_start','v2_outbound_load_start','v2_outbound_stock_op_start','v2_issue_handle_start','v2_pick_job_start','v2_pick_job_start_by_docs','v2_bulk_op_job_start','v2_ops_job_start','v2_verify_job_start']);
 window.CKInstallDispatch=function(){
  let lead=null;try{lead=JSON.parse(sessionStorage.getItem('ck_test_active_lead')||'null');}catch{}
  window.getWorkerId=()=>lead?.id||'';window.getWorkerName=()=>lead?.name||'';window.getBadge=()=>lead?lead.id+'|'+lead.name:'';
  window.CKSetNativeLead=function(person){if(person){lead=person;sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));}};
  window.CKOpenNativeJob=async function(job){
   try{
    const r=await api({action:'v2_ops_job_detail',job_id:job.id});
    if(!r?.ok||!r.can_manage_dispatch)throw Error(r?.error||'你已不在此任务中，请联系派工人 / 배정 담당자에게 문의하세요');
    if(!['pending','working','awaiting_close'].includes(r.job.status))throw Error('任务已结束，请刷新列表 / 작업 종료, 목록을 새로고침하세요');
    const state=JSON.parse(r.dispatch.state),crew=r.workers.filter(w=>!w.left_at).map(w=>({id:w.worker_id,name:w.worker_name}));
    lead=crew.find(w=>w.id===state.lead_id)||crew[0]||state.last_lead;
    if(!lead)throw Error('任务人员信息缺失，请联系管理员 / 작업자 정보를 확인하세요');
    sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));
    if(r.job.job_type==='unload'&&r.job.related_doc_type==='field_feedback')localStorage.setItem('v2_unplanned_fb_id',r.job.related_doc_id);
    else localStorage.removeItem('v2_unplanned_fb_id');
    saveActiveJob(job.id,null);if(r.job.job_type==='issue_handle')window._currentIssueId=r.job.related_doc_id;goMyTask();
   }catch(e){alert(e.message);}
  };
  window.CKClearNativeJob=function(){lead=null;sessionStorage.removeItem('ck_test_active_lead');clearActiveJob();};
  // The manager dispatches several crews; the backend still checks every worker's occupancy.
  window.hasOtherActiveJob=()=>false;
  window.checkMyActiveJob=async()=>{};window.restoreActiveJob=()=>{};
  const original=window.api;const retries=new Map();
  window.api=async function(body){
   if(!startActions.has(body.action))return original(body);
   const fingerprint=JSON.stringify(Object.fromEntries(Object.entries(body).filter(([k])=>!['client_req_id','worker_id','worker_name','handler_id','handler_name'].includes(k)).sort(([a],[b])=>a.localeCompare(b))));
   let pending=retries.get(fingerprint);
   if(!pending){const {startDepartment,departments}=await import('/shared/labor-department.js');const staff=await chooseStaff(startDepartment(body),departments);if(!staff)return {ok:false,error:'已取消派工'};pending={payload:{...body,client_req_id:body.client_req_id||crypto.randomUUID()},staff};retries.set(fingerprint,pending);}
   const staff=pending.staff;
   try{
    const r=await CKSession.request('sop_native_start',{payload:pending.payload,...staff});
    retries.delete(fingerprint);lead=r.lead||staff.workers.find(x=>x.id===staff.lead_id);sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));
    return r;
   }catch(e){if(e.businessError&&!e.message.includes('任务已建立'))retries.delete(fingerprint);return {ok:false,error:e.message};}
  };
 };
 function chooseStaff(department,departments){return new Promise(resolve=>{
  const dialog=document.createElement('dialog');dialog.className='ck-workflow';dialog.style.cssText='width:min(620px,94vw);max-height:90vh;overflow:auto;padding:20px;border:1px solid #ccd6e0;border-radius:8px';
  dialog.innerHTML='<form><h2>负责人分配本次作业人员</h2><p>扫描实际操作人员工牌；确认后按原业务流程开始计时。</p><label>本次用工部门 / 작업 부서<select name="labor_department" required><option value="">请选择 / 선택하세요</option>'+Object.entries(departments).map(([k,v])=>'<option value="'+k+'" '+(k===department?'selected':'')+'>'+v+'</option>').join('')+'</select></label>'+CKPeopleFields()+'<label>预计操作分钟<input name="minutes" type="number" min="1" step="1" value="30" required></label><p role="alert"></p><button type="submit">确认人员并开始</button> <button type="button" data-cancel>取消</button></form>';
  document.body.append(dialog);const form=dialog.querySelector('form'),error=dialog.querySelector('[role=alert]');dialog.showModal();
  const picker=CKPeoplePicker(form,{error});let done=false;
  const finish=async value=>{if(done)return;done=true;await picker.destroy();dialog.close();dialog.remove();resolve(value);};
  dialog.querySelector('[data-cancel]').onclick=()=>finish(null);dialog.oncancel=e=>{e.preventDefault();finish(null);};
  form.onsubmit=e=>{e.preventDefault();try{const staff=picker.read();finish({...staff,labor_department:form.elements.labor_department.value,estimated_minutes:Number(form.elements.minutes.value)});}catch(e){error.textContent=e.message;}};
 });}
})();
