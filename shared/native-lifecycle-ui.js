(function(){
 'use strict';
 window.CKToggleNativePause=async function(detail,onSaved){
  if(!detail?.can_manage_dispatch)throw Error('请由本任务派审员操作 / 담당자가 처리하세요');
  const state=JSON.parse(detail.dispatch.state),paused=detail.job.status==='paused',id=detail.job.id;
  let request=null;
  if(paused){
   const workers=state.paused_workers||[];
   return CKNativePeopleDialog({title:'恢复整个任务 / 전체 작업 재개',description:'只选择现在实际到位的人员；休息未结束、已签退或在其他任务的人不能恢复计时。 / 실제 복귀 인원만 선택하세요.',workers,leadId:workers.some(w=>w.id===state.lead_id)?state.lead_id:workers[0]?.id||'',submitLabel:'确认人员并恢复 / 인원 확인·재개',onSubmit:async staff=>{const key=JSON.stringify(staff);if(request?.key!==key)request={key,body:{job_id:id,revision:detail.dispatch.revision,client_req_id:crypto.randomUUID(),...staff}};const r=await CKSession.request('sop_native_resume',request.body);await onSaved?.(r);return r;}}).promise;
  }
  const dialog=document.createElement('dialog');dialog.className='ck-workflow';dialog.innerHTML='<form><h2>暂停整个任务 / 전체 작업 중지</h2><p>停止本任务所有人的有效作业计时，保留原任务与人员分配。个人休息或借调请使用人员入口。 / 전체 인원 작업시간 중지. 개인 휴식·지원은 인원 메뉴를 이용하세요.</p><label>暂停原因 / 중지 사유<textarea name="reason" required maxlength="500"></textarea></label><p role="alert"></p><div class="ck-buttons"><button type="submit">确认暂停 / 중지 확인</button><button type="button" data-cancel>取消 / 취소</button></div></form>';
  document.body.append(dialog);dialog.showModal();let busy=false;
  const close=()=>{if(!busy){dialog.close();dialog.remove();}};dialog.querySelector('[data-cancel]').onclick=close;dialog.oncancel=e=>{e.preventDefault();close();};
  dialog.querySelector('form').onsubmit=async e=>{e.preventDefault();if(busy)return;busy=true;const button=dialog.querySelector('[type=submit]');button.disabled=true;
   try{const reason=dialog.querySelector('textarea').value.trim();if(request?.reason!==reason)request={job_id:id,revision:detail.dispatch.revision,client_req_id:crypto.randomUUID(),reason};const r=await CKSession.request('sop_native_pause',request);await onSaved?.(r);busy=false;close();}catch(x){dialog.querySelector('[role=alert]').textContent=x.message;}finally{busy=false;button.disabled=false;}};
 };
 window.CKInstallNativeLifecycle=function(){
  const $=id=>document.getElementById(id),leaveNames=['unloadLeave','inboundLeave','inboundReturnLeave','leaveImportDelivery','handleIssueLeave','bulkLeave','leaveGenericJob','leaveVerifyScan'];
  let current=null,editing=false,finishedJob=null;
  const bar=document.createElement('section');bar.className='ck-dispatch-controls';bar.hidden=true;
  bar.innerHTML='<div><b>派审员管理 / 담당자 관리</b><p data-job></p><p data-crew></p></div><div class="ck-buttons"><button type="button" data-people>调整人员 / 인원 변경</button><button type="button" data-rest>个人休息／恢复 / 개인 휴식·복귀</button><button type="button" data-pause>暂停整个任务 / 전체 작업 중지</button><button type="button" data-home>返回首页，任务继续 / 작업 유지·홈으로</button></div>';
  bar.querySelector('[data-pause]').onclick=async()=>{try{await CKToggleNativePause(await detail(),()=>detail());}catch(e){alert(e.message);}};bar.querySelector('[data-people]').onclick=()=>editPeople();bar.querySelector('[data-rest]').onclick=()=>CKOpenFieldLabor();bar.querySelector('[data-home]').onclick=()=>goPage('home');
  function accept(r){
   if(!r?.can_manage_dispatch||r.job?.id!==window._activeJobId)return;
   current=r;const page=$('page-'+window._currentPage);if(!page||!['pending','working','awaiting_close','paused'].includes(r.job.status))return;
   const state=JSON.parse(r.dispatch?.state||'{}');
   bar.querySelector('[data-pause]').textContent=r.job.status==='paused'?'恢复整个任务 / 전체 작업 재개':'暂停整个任务 / 전체 작업 중지';bar.querySelector('[data-people]').disabled=r.job.status==='paused';window.CKSetNativeLead?.(state.workers?.find(w=>w.id===state.lead_id)||state.last_lead);
   if(bar.parentElement!==page){const top=page.querySelector('.topbar');if(top)top.after(bar);else page.prepend(bar);}bar.hidden=false;
   bar.querySelector('[data-job]').textContent=window.CKDocumentLabels.jobSummary(r)+(r.job.status==='paused'?' · 已暂停 / 중지: '+(state.pause_reason||''):'');
   bar.querySelector('[data-crew]').textContent='正在作业 / 작업 중: '+(r.workers.filter(w=>!w.left_at).map(w=>w.worker_name).join('、')||'暂无人员 / 없음')+((r.borrowed_out||[]).length?' · 借出 / 지원 중: '+r.borrowed_out.map(b=>b.worker_name).join('、'):'');
   // The same response updates both the shared controls and original operation panels.
   for(const id of ['unloadWorkers','inboundWorkers','inboundReturnWorkers','idWorkers','loadWorkers','osoWorkers','pickWorkers','bulkWorkers','gjWorkers','gjIdleWorkers','vsWorkers'])if($(id))renderWorkers(id,r.workers);
   if($('gjWorkerCount'))$('gjWorkerCount').textContent=r.job.active_worker_count+' 人/명';
   if(window._currentPage==='generic_job'){
    const card=$('gjActiveTitle')?.closest('.card'),status=card?.querySelector('div[style*="flex-wrap"]>span>span');
    if(status)status.textContent=r.job.status==='paused'?'已暂停 / 중지':r.job.active_worker_count?'作业中 / 작업 중':'当前无人计时 / 현재 작업 인원 없음';
    if(card?.lastElementChild)card.lastElementChild.textContent='仅实际在岗人员累计有效工时；休息和整任务暂停不计入。 / 실제 작업 구간만 합산합니다.';
    for(const id of ['gjFinishBtn','gjLeaveBtn'])if($(id))$(id).disabled=r.job.status==='paused';
   }
   const pick=$('pickWorkingPicker');if(r.job.job_type==='pick_direct'&&pick)pick.textContent=r.workers.filter(w=>!w.left_at).map(w=>w.worker_name).join('、')||'—';
  }
  const originalApi=window.api;window.api=async function(body){const managed=current?.job?.id===body.job_id&&current.can_manage_dispatch;const r=await originalApi(body);if(body.action==='v2_ops_job_detail'&&r?.ok)accept(r);
   if(managed&&r?.ok&&!body.leave_only&&/_(finish|finalize)$/.test(body.action)){
    const end=await originalApi({action:'v2_ops_job_detail',job_id:body.job_id});
    if(end?.job?.status==='completed'){finishedJob=body.job_id;setTimeout(()=>{if(finishedJob)window.showPage('home');},0);}
   }return r;};
  const originalPage=window.showPage;window.showPage=function(...args){if(finishedJob){finishedJob=null;args=['home'];window._navStack=[];window._pageParams={};CKClearNativeJob();}bar.hidden=true;current=null;if(['home','labor','issue_list'].includes(args[0])||String(args[0]).endsWith('_menu'))CKClearNativeJob();const result=originalPage(...args),id=window._activeJobId;
   if(id)setTimeout(()=>{if(window._activeJobId===id&&!current)detail().catch(()=>{});},0);return result;};
  const originalElapsed=window._gjStartElapsedTimer;
  if(originalElapsed)window._gjStartElapsedTimer=function(){window._gjStopElapsedTimer();const tick=()=>{if(!current||current.job.id!==window._activeJobId)return;const el=$('gjElapsed');if(!el)return;el.previousElementSibling.textContent='有效人分钟 / 작업 인분:';el.textContent=current.workers.reduce((sum,w)=>sum+(w.left_at?Number(w.minutes_worked)||0:Math.max(0,(Date.now()-Date.parse(w.joined_at))/60000)),0).toFixed(1);};tick();window._gjElapsedTimer=setInterval(tick,1000);};
  async function detail(){const id=window._activeJobId;if(!id)throw Error('请先打开正在进行的任务 / 진행 중인 작업을 여세요');const r=await api({action:'v2_ops_job_detail',job_id:id});if(!r?.ok||!r.can_manage_dispatch)throw Error(r?.error||'无法调整该任务 / 인원 변경 불가');return r;}
  async function editPeople(destination=''){
   if(destination){window.CKClearNativeJob();goPage(destination);return;}
   if(editing)return;editing=true;let dialog,picker;
   try{
    await window.stopAllManagedQrScanners?.();
    const r=await detail(),jobId=r.job.id,record=JSON.parse(r.dispatch.state),workers=r.workers.filter(w=>!w.left_at).map(w=>({id:w.worker_id,name:w.worker_name}));
    for(const b of r.borrowed_out||[])if(!workers.some(w=>w.id===b.worker_id))workers.push({id:b.worker_id,name:b.worker_name,borrowed:true});
    dialog=document.createElement('dialog');dialog.className='ck-workflow ck-native-people';dialog.innerHTML='<form><h2>调整本任务人员 / 작업 인원 변경</h2><p>移除的人结束本段计时，新增的人从保存时开始；留在任务中的人员连续计时。全部移除后任务保留待收尾。</p><p class="ck-muted">삭제한 인원은 종료, 추가한 인원은 저장 시 시작합니다.</p>'+CKPeopleFields()+'<p role="alert"></p><div class="ck-buttons"><button type="submit">保存人员 / 인원 저장</button><button type="button" data-cancel>取消 / 취소</button></div></form>';
    document.body.append(dialog);dialog.showModal();const form=dialog.querySelector('form'),error=dialog.querySelector('[role=alert]');picker=CKPeoplePicker(form,{workers,leadId:record.lead_id,error,allowEmpty:true,borrowContext:['unload','load_outbound'].includes(r.job.job_type)?{job_id:jobId}:null});let requestId=null,fingerprint='';
    const close=async()=>{await picker.destroy();dialog.close();dialog.remove();editing=false;};
    dialog.querySelector('[data-cancel]').onclick=close;dialog.oncancel=e=>{e.preventDefault();close();};
    form.onsubmit=async e=>{e.preventDefault();const button=e.submitter;button.disabled=true;try{
     await picker.ready();const staff=picker.read(),key=JSON.stringify(staff);if(key!==fingerprint){requestId=crypto.randomUUID();fingerprint=key;}
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
  window.interruptToUnload=()=>{CKClearNativeJob();goPage('unload');};window.interruptToLoad=()=>{CKClearNativeJob();goPage('outbound_load');};
  document.querySelectorAll('[onclick="interruptToUnload()"],[onclick="interruptToLoad()"]').forEach(b=>b.remove());
  const switchMode=window.switchPickMode;window.switchPickMode=function(){switchMode('start');$('pickModeBar').hidden=true;};
  document.querySelector('#pickStartSection .card-title').textContent='扫描拣货单并派工 / 피킹번호 스캔·배정';
  document.querySelector('#pickStartSection .card-title').nextElementSibling.textContent='可连续扫描多个外部拣货单号，点“分配人员并开始”后再扫工牌。确认后开始本趟计时。 / 외부 피킹번호를 스캔한 뒤 인원을 배정하세요.';
  document.querySelector('[onclick="submitStartPickByDocs(this)"]').textContent='分配人员并开始 / 인원 배정·시작';
  const lookupHint=window.renderPickStartLookupHint;window.renderPickStartLookupHint=function(){
   const el=$('pickStartLookupHint');if(!el)return;el.replaceChildren();
   for(const no of _pickStartDocNos){const info=_pickStartLookupCache[no],line=document.createElement('div');
    line.textContent=no+' · '+(!info||info._state==='loading'?'识别中 / 확인 중':info._state==='error'?'暂未取得状态，开始前会再次校验 / 시작 시 재확인':!info.found?'新拣货单，待派工 / 새 피킹번호':info.pick_status==='completed'?'已完成，请核对单号 / 완료된 번호':'已登记，请继续原趟次 / 기존 작업 계속');el.append(line);
   }
  };
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
   try{const r=await CKSession.request('sop_dispatch_list'),jobs=r.items.filter(j=>j.job_type==='pick_direct');host.replaceChildren();for(const job of jobs)host.append(CKNativeTaskButton(job));if(!jobs.length)host.textContent='暂无进行中拣货 / 진행 중인 피킹 없음';}catch(e){host.textContent=e.message;}
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
     const title=document.createElement('b');title.textContent=window.CKDocumentLabels.jobLabel({...item,...(job||{}),job_type:'unplanned_unload'});
     const info=document.createElement('p');info.textContent='派审员 / 배정·검수 담당자：'+window.CKDocumentLabels.jobDispatcher(job||item)+' · '+(item.cargo_summary||'')+' · '+(item.worker_names||[]).join('、');
     row.append(title,info);
     if(job){const button=document.createElement('button');button.type='button';button.className='btn btn-outline';button.textContent='继续作业 / 작업 계속';button.onclick=()=>joinUnplannedUnload(item.feedback_id,button);row.append(button);}
     else{const note=document.createElement('small');note.textContent='由原派工人管理；加入人员请联系派工人 / 인원 추가는 배정 담당자에게 요청하세요';row.append(note);}
     box.append(row);
    }
   }catch(e){box.textContent=e.message;}
  };
 };
})();
