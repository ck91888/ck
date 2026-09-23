(function(){
 'use strict';
 window.CKInstallNativeLifecycle=function(){
  const $=id=>document.getElementById(id),leaveNames=['unloadLeave','inboundLeave','inboundReturnLeave','leaveImportDelivery','handleIssueLeave','bulkLeave','leaveGenericJob','leaveVerifyScan'];
  let current=null,editing=false;
  const bar=document.createElement('section');bar.className='ck-dispatch-controls';bar.hidden=true;
  bar.innerHTML='<div><b>派审员管理 / 담당자 관리</b><p data-crew></p></div><div class="ck-buttons"><button type="button" data-people>调整人员 / 인원 변경</button><button type="button" data-rest>人员与休息 / 인원·휴식</button><button type="button" data-home>返回首页，任务继续 / 작업 유지·홈으로</button></div>';
  bar.querySelector('[data-people]').onclick=()=>editPeople();bar.querySelector('[data-rest]').onclick=()=>CKOpenFieldLabor();bar.querySelector('[data-home]').onclick=()=>goPage('home');
  function accept(r){
   if(!r?.can_manage_dispatch||r.job?.id!==window._activeJobId)return;
   current=r;const page=$('page-'+window._currentPage);if(!page||!['pending','working','awaiting_close'].includes(r.job.status))return;
   const state=JSON.parse(r.dispatch?.state||'{}');window.CKSetNativeLead?.(state.workers?.find(w=>w.id===state.lead_id)||state.last_lead);
   if(bar.parentElement!==page){const top=page.querySelector('.topbar');if(top)top.after(bar);else page.prepend(bar);}bar.hidden=false;
   bar.querySelector('[data-crew]').textContent='正在作业 / 작업 중: '+(r.workers.filter(w=>!w.left_at).map(w=>w.worker_name).join('、')||'暂无人员 / 없음');
   const pick=$('pickWorkingPicker');if(r.job.job_type==='pick_direct'&&pick)pick.textContent=r.workers.filter(w=>!w.left_at).map(w=>w.worker_name).join('、')||'—';
  }
  const originalApi=window.api;window.api=async function(body){const r=await originalApi(body);if(body.action==='v2_ops_job_detail'&&r?.ok)accept(r);return r;};
  const originalPage=window.showPage;window.showPage=function(...args){bar.hidden=true;current=null;if(['home','labor','issue_list'].includes(args[0])||String(args[0]).endsWith('_menu'))CKClearNativeJob();return originalPage(...args);};
  async function detail(){const id=window._activeJobId;if(!id)throw Error('请先打开正在进行的任务 / 진행 중인 작업을 여세요');const r=await api({action:'v2_ops_job_detail',job_id:id});if(!r?.ok||!r.can_manage_dispatch)throw Error(r?.error||'无法调整该任务 / 인원 변경 불가');return r;}
  async function editPeople(destination=''){
   if(editing)return;editing=true;let dialog,picker;
   try{
    const r=await detail(),jobId=r.job.id,record=JSON.parse(r.dispatch.state),workers=r.workers.filter(w=>!w.left_at).map(w=>({id:w.worker_id,name:w.worker_name}));
    dialog=document.createElement('dialog');dialog.className='ck-workflow ck-native-people';dialog.innerHTML='<form><h2>调整本任务人员 / 작업 인원 변경</h2><p>移除的人结束本段计时，新增的人从保存时开始；留在任务中的人员连续计时。全部移除后任务保留待收尾。</p><p class="ck-muted">삭제한 인원은 종료, 추가한 인원은 저장 시 시작합니다.</p>'+CKPeopleFields()+'<p role="alert"></p><div class="ck-buttons"><button type="submit">保存人员 / 인원 저장</button><button type="button" data-cancel>取消 / 취소</button></div></form>';
    document.body.append(dialog);dialog.showModal();const form=dialog.querySelector('form'),error=dialog.querySelector('[role=alert]');picker=CKPeoplePicker(form,{workers,leadId:record.lead_id,error,allowEmpty:true});let requestId=null,fingerprint='';
    const close=async()=>{await picker.destroy();dialog.close();dialog.remove();editing=false;};
    dialog.querySelector('[data-cancel]').onclick=close;dialog.oncancel=e=>{e.preventDefault();close();};
    form.onsubmit=async e=>{e.preventDefault();const button=e.submitter;button.disabled=true;try{
     const staff=picker.read(),key=JSON.stringify(staff);if(key!==fingerprint){requestId=crypto.randomUUID();fingerprint=key;}
     const saved=await CKSession.request('sop_native_people',{job_id:jobId,revision:r.dispatch.revision,client_req_id:requestId,...staff});
     window.CKSetNativeLead?.(saved.lead);await close();if(destination){CKClearNativeJob();goPage(destination);return;}const refreshed=await api({action:'v2_ops_job_detail',job_id:jobId});
     if(window._activeJobId===jobId){for(const id of ['unloadWorkers','inboundWorkers','inboundReturnWorkers','importDeliveryWorkers','loadWorkers','osoWorkers','pickWorkers','bulkWorkers','gjWorkers','vsWorkers'])if($(id))renderWorkers(id,refreshed.workers);window.dispatchEvent(new CustomEvent('ck-native-people-changed',{detail:refreshed}));if(refreshed.job.job_type==='pick_direct')refreshPickWorkingDocs();}
    }catch(e){error.textContent=e.message;button.disabled=false;}};
   }catch(e){editing=false;dialog?.remove();alert(e.message);}
  }
  window.CKEditNativePeople=editPeople;
  for(const name of leaveNames){if(!window[name])continue;window[name]=()=>editPeople();document.querySelectorAll('[onclick^="'+name+'("]').forEach(b=>b.textContent='调整人员 / 인원 변경');}
  for(const name of ['outboundLoadGoBack','outboundStockOpGoBack','unloadGoBack','verifyScanGoBack'])window[name]=()=>goPage('home');
  document.querySelector('#pickCreateSection .card-title').textContent='新建拣货并派工 / 피킹 생성·배정';
  const createHint=document.querySelector('#pickCreateSection .card-title').nextElementSibling;if(createHint)createHint.textContent='扫描本趟拣货单后分配人员，确认即开始实际操作员计时。 / 피킹번호 스캔 후 인원 배정과 동시에 작업시간을 기록합니다.';
  window.interruptToUnload=()=>editPeople('unload');window.interruptToLoad=()=>editPeople('outbound_load');
  document.querySelectorAll('[onclick="interruptToUnload()"]').forEach(b=>b.textContent='调人去卸货 / 하차 인원 조정');
  document.querySelectorAll('[onclick="interruptToLoad()"]').forEach(b=>b.textContent='调人去装货 / 상차 인원 조정');
  const switchMode=window.switchPickMode;window.switchPickMode=function(mode){switchMode(mode);$('pickModeHint').textContent='扫描拣货单，分配操作人员后开始计时。 / 피킹번호 스캔 후 작업자를 배정합니다.';};
  $('pickModeCreateBtn').textContent='新建拣货趟次 / 피킹 차수 생성';
  document.querySelector('[onclick="finishPickJob(this)"]').textContent='确认并结束本趟拣货 / 확인 후 차수 종료';
  window.submitCreatePickTrip=async function(button){
   if(!_pickCreateDocNos.length){alert('请先添加拣货单号 / 피킹번호를 추가하세요');return;}
   return withActionLock('submitCreatePickTrip',button,'提交中 / 저장 중',async()=>{
    const r=await api({action:'v2_pick_job_start',pick_doc_nos:_pickCreateDocNos});
    if(!r?.ok){alert(r?.message||r?.error||'创建失败 / 생성 실패');return;}
    saveActiveJob(r.job_id,r.worker_seg_id);_pickCreateDocNos=[];stopPickCreateScan();enterPickWorkingSection();startJobPoll('pick');
   });
  };
  const oldFinish=window.finishPickJob;
  window.finishPickJob=async function(button){
   const r=await detail();if(!r.can_manage_dispatch)return oldFinish(button);
   return withActionLock('finishPickJob',button,'提交中 / 저장 중',async()=>{
    const result=await api({action:'v2_pick_job_finalize',job_id:r.job.id,worker_id:getWorkerId(),remark:$('pickRemark').value,result_note:$('pickResultNote').value,client_req_id:crypto.randomUUID()});
    if(!result?.ok){alert(result?.message||result?.error||'结束失败 / 종료 실패');return;}
    stopPickCreateScan();stopPickStartScan();stopPickElapsedTimer();CKClearNativeJob();alert('本趟拣货已完成，全部参与人员结束计时。 / 차수 완료, 참여 인원의 작업시간이 종료되었습니다.');goPage('home');
   });
  };
  window.loadPickActiveList=async function(){
   const host=$('pickActiveList');if(!host)return;host.textContent='加载中 / 로딩 중';
   try{const r=await CKSession.request('sop_dispatch_list'),jobs=r.items.filter(j=>j.job_type==='pick_direct');host.replaceChildren();for(const job of jobs){const b=document.createElement('button');b.type='button';b.className='btn btn-outline';b.textContent=(job.display_no||job.title||job.id)+' · '+job.workers.map(w=>w.name).join('、')+' · 继续管理 / 작업 관리';b.onclick=()=>CKOpenNativeJob(job);host.append(b);}if(!jobs.length)host.textContent='暂无进行中拣货 / 진행 중인 피킹 없음';}catch(e){host.textContent=e.message;}
  };
  // Opening an existing dispatch must never call the old worker-join route.
  // The signed-in dispatcher may already be a member of this exact crew.
  window.joinUnplannedUnload=async function(feedbackId,button){
   if(button)button.disabled=true;
   try{
    const r=await CKSession.request('sop_dispatch_list');
    const job=r.items.find(j=>j.job_type==='unload'&&j.source_type==='field_feedback'&&j.source_id===feedbackId);
    if(!job)throw Error('你已不在此任务中，请联系派工人 / 배정 담당자에게 문의하세요');
    await CKOpenNativeJob(job);
   }catch(e){alert(e.message);}finally{if(button)button.disabled=false;}
  };
  window.loadUnplannedActiveList=async function(){
   const wrap=$('unplannedActiveList'),box=$('unplannedActiveItems'),hint=$('unplannedActiveHint');if(!wrap||!box)return;
   wrap.style.display='';box.textContent='加载中 / 로딩 중';
   if(hint){hint.style.display='';hint.textContent='本人派工或正在参与的任务可直接继续；需要增减人员请进入任务后调整。 / 배정했거나 참여 중인 작업을 열어 인원을 변경하세요.';}
   try{
    const [all,mine]=await Promise.all([api({action:'v2_unplanned_unload_active_list'}),CKSession.request('sop_dispatch_list')]);
    if(!all?.ok)throw Error(all?.error||'加载失败 / 로딩 실패');
    box.replaceChildren();wrap.style.display=all.items?.length?'':'none';
    for(const item of all.items||[]){
     const job=mine.items.find(j=>j.job_type==='unload'&&j.source_type==='field_feedback'&&j.source_id===item.feedback_id);
     const row=document.createElement('section');row.className='ck-unplanned-resume';
     const title=document.createElement('b');title.textContent=item.display_no||item.feedback_id;
     const info=document.createElement('p');info.textContent='派工 / 배정: '+(job?.owner||item.submitted_by||'—')+' · '+(item.cargo_summary||'')+' · '+(item.worker_names||[]).join('、');
     row.append(title,info);
     if(job){const button=document.createElement('button');button.type='button';button.className='btn btn-outline';button.textContent='继续作业 / 작업 계속';button.onclick=()=>joinUnplannedUnload(item.feedback_id,button);row.append(button);}
     else{const note=document.createElement('small');note.textContent='由原派工人管理；加入人员请联系派工人 / 인원 추가는 배정 담당자에게 요청하세요';row.append(note);}
     box.append(row);
    }
   }catch(e){box.textContent=e.message;}
  };
 };
})();
