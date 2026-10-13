(function(){
 'use strict';
 const $=id=>document.getElementById(id),bi=(zh,ko)=>'<span>'+zh+'</span><small lang="ko">'+ko+'</small>';
 window.CKInstallCheckWorkbench=function(){
  let jobId='',batchId='',detail=null,batch=null,check=null,logs=[],queue=[],busy=false,blocked=false,restored='',loading=0,finishRequest=null,scanEpoch=0;
  const key=id=>'ck-verify-queue:'+CKSession.user.id+':'+id;
  $('page-verify_scan').classList.add('ck-check-workbench');
  $('vsEntryCard').insertAdjacentHTML('beforeend','<p id="vsEntryError" role="alert"></p>');
  $('vsWorkingCard').innerHTML='<div class="card-title">'+bi('当前批次','현재 배치')+'</div><div id="vsBatchHeader"></div><div class="ck-check-stats">'+[['vsPlanQty','计划','계획'],['vsOkQty','正确','정상'],['vsAbQty','异常','이상'],['vsDiffQty','差异','차이']].map(([id,zh,ko])=>'<div>'+bi(zh,ko)+'<strong id="'+id+'">—</strong></div>').join('')+'</div><p class="ck-check-caption">批次累计数量，包含此前记录；不代表个人产量。<br><span lang="ko">이전 기록을 포함한 배치 합계이며 개인 실적이 아닙니다.</span></p><div class="ck-check-inputs"><label for="vsOperator">'+bi('扫码操作员','스캔 작업자')+'<select id="vsOperator"></select></label><label for="vsPalletInput">'+bi('当前托盘号 · 必填','현재 팔레트 번호 · 필수')+'<input id="vsPalletInput" autocomplete="off" maxlength="256" placeholder="扫描或输入托盘号 / 팔레트 번호"></label><label for="vsBarcodeInput">'+bi('扫描出库条码','출고 바코드 스캔')+'<input id="vsBarcodeInput" autocomplete="off" spellcheck="false" maxlength="256" placeholder="扫码后 Enter / 스캔 후 Enter"></label></div><div class="ck-check-scan-actions"><button id="vsSubmit" type="button" class="btn btn-primary">'+bi('提交条码','바코드 제출')+'</button><button id="vsRefresh" type="button" class="btn btn-outline">'+bi('刷新批次','배치 새로고침')+'</button></div><p id="vsState" role="status"></p><p class="ck-check-caption">Enter 后依序提交，可继续扫描；每条保留提交时的托盘与人员。失败时队列暂停，请重试首条。<br><span lang="ko">Enter 순서대로 제출합니다. 오류 시 첫 항목을 재시도하세요.</span></p><div id="vsLastResult" role="status" aria-live="polite"></div><section id="vsQueue" hidden><h3>'+bi('待提交队列','제출 대기열')+'</h3><ol id="vsQueueItems"></ol><button type="button" id="vsRetry" class="btn btn-primary">'+bi('重试首条','첫 항목 재시도')+'</button></section>';
  $('vsActionCard').innerHTML='<h3>'+bi('本次作业交接','이번 작업 인계')+'</h3><p id="vsHandoverHint"></p><form id="vsFinishForm"><label>'+bi('交接说明','인계 내용')+'<textarea name="remark" maxlength="2000" placeholder="接收人、位置与待处理事项 / 인수자·위치·미완료 사항"></textarea></label><label id="vsCloseBatchLabel"><input type="checkbox" name="complete_batch">'+bi('同时关闭旧批次','기존 배치도 종료')+'</label><p role="alert" id="vsFinishError"></p><button type="submit" id="vsFinish" class="btn btn-primary">'+bi('结束本次核对作业','이번 검수 작업 종료')+'</button></form>';
  const start=$('vsEntryCard').querySelector('[onclick^="startVerifyScan"]');start.innerHTML=bi('扫工牌派工并开始','명찰 스캔·배정·시작');
  function persist(){if(!jobId)return;try{if(queue.length)sessionStorage.setItem(key(jobId),JSON.stringify(queue));else sessionStorage.removeItem(key(jobId));}catch{show('err','队列保存失败，请保留此页并完成提交 / 대기열을 완료한 뒤 이동하세요');}}
  function show(type,message){const el=$('vsLastResult');el.className='ck-check-result '+type;el.textContent=message;el.hidden=!message;}
  function queuePaint(){
   $('vsQueue').hidden=!queue.length;$('vsQueueItems').replaceChildren();
   queue.forEach((item,i)=>{const li=document.createElement('li');li.textContent=(i===0?(blocked?'待重试 / 재시도: ':busy?'提交中 / 제출 중: ':'待提交 / 대기: '):'排队 / 대기: ')+item.barcode+' · '+item.pallet_no+' · '+item.worker_name;$('vsQueueItems').append(li);});
   $('vsRetry').hidden=!blocked;$('vsRetry').disabled=busy;
   const working=detail?.job.status==='working'&&detail?.workers.some(w=>!w.left_at);
   $('vsSubmit').disabled=!working;$('vsBarcodeInput').readOnly=!working;
   $('vsFinish').disabled=!!queue.length||busy||detail?.job.status==='paused';
   $('vsState').textContent=blocked?'本条未确认，后续条码保留在队列 / 오류 항목 재시도 필요':busy?'正在提交，队列 '+queue.length+' 条 / 제출 중: '+queue.length:working?'可连续扫码 / 연속 스캔 가능':'任务暂停或无人计时，请恢复作业 / 작업 재개 필요';
  }
  function renderLogs(){
   const body=$('vsScanLogBody');body.replaceChildren();
   if(!logs.length){body.textContent='暂无扫码记录 / 스캔 기록 없음';return;}
   for(const log of logs.slice(0,20)){
    const row=document.createElement('article');row.className='ck-check-log';
    const status=document.createElement('span');status.className='ck-check-status '+(log.scan_result==='ok'?'ok':'warn');status.textContent=({ok:'正确 / 정상',overflow:'超量 / 초과',not_found:'清单外 / 목록 외',duplicate:'重复 / 중복'})[log.scan_result]||log.scan_result;
    const code=document.createElement('strong');code.textContent=log.barcode;
    const meta=document.createElement('p');meta.textContent='托盘 / 팔레트: '+(log.pallet_no||'—')+' · '+(log.worker_name||log.worker_id||'—')+' · '+fmtTime(log.scanned_at);
    row.append(status,code,meta);body.append(row);
   }
  }
  function applyBatch(r){
   batch=r.batch;check=r.check;logs=r.scan_logs||[];
   $('vsBatchHeader').textContent=(batch.batch_no||'—')+' · '+(check?.ship_date||batch.customer_name||'')+' · '+(batch.status==='completed'?'已关闭 / 종료':'核对中 / 검수 중');
   const summary=check?{planned_total_box_count:check.summary.planned,scanned_ok_total_count:check.summary.scanned,abnormal_count:check.summary.unresolved,diff:check.summary.planned-check.summary.scanned}:r.summary;
   window.applyVsSummary(null,summary);renderLogs();
   $('vsCloseBatchLabel').hidden=!!check;
   $('vsHandoverHint').textContent=check?'结束本次作业会停止人员计时并保留清单。保存轮次与最终交接由经理或复核人员在核对中心办理。 / 작업 종료 후 목록은 유지되며 승인 담당자가 최종 인계합니다.':'结束后保留所有扫码历史；可选择同时关闭该旧批次。 / 종료 후에도 스캔 기록이 유지됩니다.';
  }
  function applyDetail(r){
   detail=r;const select=$('vsOperator'),old=select.value,crew=r.workers.filter(w=>!w.left_at);select.replaceChildren();
   for(const w of crew){if([...select.options].some(x=>x.value===w.worker_id))continue;const opt=document.createElement('option');opt.value=w.worker_id;opt.textContent=w.worker_name+' · '+w.worker_id;select.append(opt);}
   if([...select.options].some(x=>x.value===old))select.value=old;
   else if([...select.options].some(x=>x.value===CKSession.user.badge))select.value=CKSession.user.badge;
   else if([...select.options].some(x=>x.value===getWorkerId()))select.value=getWorkerId();
   if(!crew.length){const opt=document.createElement('option');opt.textContent='暂无在岗人员 / 작업 인원 없음';opt.value='';select.append(opt);}
   renderWorkers('vsWorkers',r.workers);queuePaint();
  }
  async function refresh(){
   const id=jobId,bid=batchId,epoch=scanEpoch;if(!id)return;
   const [d,r]=await Promise.all([api({action:'v2_ops_job_detail',job_id:id}),api({action:'v2_verify_batch_detail',id:bid,scan_logs_limit:500})]);
   if(id!==jobId)return;if(!d?.ok||!r?.ok)throw Error(d?.error||r?.error||'读取失败 / 조회 실패');
   if(!d.can_manage_dispatch)throw Error('任务权限或人员已变化，请从首页重新打开 / 홈에서 작업을 다시 여세요');
   applyDetail(d);if(epoch===scanEpoch)applyBatch(r);
  }
  async function drain(){
   if(busy||blocked||!queue.length)return;busy=true;queuePaint();
   const item=queue[0];
   try{
    const r=await api({...item,action:'v2_verify_scan_submit'});
    if(!r?.ok)throw Error(r?.message||r?.error||'提交失败 / 제출 실패');
    queue.shift();scanEpoch++;persist();logs=[r.log,...logs.filter(x=>x.id!==r.log_id)].filter(Boolean);renderLogs();window.applyVsSummary(null,r.summary);
    show(r.scan_result==='ok'?'ok':'warn',item.barcode+' · '+item.pallet_no+' · '+r.message);
    if(restored===item.client_req_id){$('vsBarcodeInput').value='';restored='';}
    if(!document.activeElement||document.activeElement===document.body||document.activeElement===document.documentElement||document.activeElement===$('vsBarcodeInput')||document.activeElement===$('vsSubmit')||document.activeElement===$('vsRetry'))$('vsBarcodeInput').focus();
   }catch(e){
    blocked=true;show('err',item.barcode+' · '+item.pallet_no+'：'+e.message+'；未确认成功，请重试 / 재시도 필요');
    if(!$('vsBarcodeInput').value){$('vsBarcodeInput').value=item.barcode;restored=item.client_req_id;$('vsBarcodeInput').focus();$('vsBarcodeInput').select();}
   }finally{busy=false;queuePaint();if(!blocked&&queue.length)void drain();}
  }
  function submit(){
   const input=$('vsBarcodeInput');if(restored&&queue[0]?.client_req_id===restored){retry();return;}
   const barcode=input.value.trim(),pallet=$('vsPalletInput').value.trim(),worker=$('vsOperator').value;
   if(!barcode)return;
   if(!pallet){show('err','请先填写托盘号；条码已保留 / 팔레트 번호를 입력하세요');$('vsPalletInput').focus();return;}
   if(!worker||detail?.job.status!=='working'){show('err','请先恢复任务并确认在岗人员 / 작업과 인원을 확인하세요');return;}
   if(queue.length>=100){show('err','队列已满，请停止扫描并等待或重试；输入条码已保留 / 대기열을 먼저 처리하세요');return;}
   const w=detail.workers.find(w=>w.worker_id===worker&&!w.left_at);if(!w)return;
   queue.push({client_req_id:crypto.randomUUID(),job_id:jobId,batch_id:batchId,worker_id:worker,worker_name:w.worker_name,pallet_no:pallet,barcode});persist();input.value='';restored='';_vsLastPallet=pallet;queuePaint();void drain();input.focus();
  }
  function retry(){if(busy||!queue.length)return;blocked=false;void drain();}
  $('vsBarcodeInput').oninput=()=>{restored='';};$('vsBarcodeInput').onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();submit();}};
  $('vsPalletInput').onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();$('vsBarcodeInput').focus();}};
  $('vsSubmit').onclick=submit;$('vsRetry').onclick=retry;$('vsRefresh').onclick=()=>refresh().catch(e=>show('err',e.message));
  window.submitVsBarcode=submit;window.onVsBarcodeKey=e=>{if(e.key==='Enter'){e.preventDefault();submit();}};
  window.renderVsLogs=items=>{logs=items;renderLogs();};
  window.refreshVerifyScanSummary=()=>refresh().catch(e=>show('err',e.message));window.refreshVerifyScanLogs=()=>Promise.resolve();
  window.refreshVerifyScanWorkers=async()=>{const id=jobId;if(!id||window._currentPage!=='verify_scan')return;const r=await api({action:'v2_ops_job_detail',job_id:id});if(id!==jobId)return;if(r?.ok)applyDetail(r);else show('err',r?.error||'人员读取失败 / 인원 조회 실패');};
  window.loadVsBatches=async()=>{
   const token=++loading,sel=$('vsBatchSelect'),prior=sel.value;sel.disabled=true;start.disabled=true;$('vsEntryError').textContent='';sel.replaceChildren(new Option('加载中 / 로딩 중',''));
   try{
    const items=[];for(const status of ['pending','verifying']){let offset=0;while(true){const r=await api({action:'v2_verify_batch_list',status,limit:200,offset});if(!r?.ok)throw Error(r?.error||'批次读取失败 / 배치 조회 실패');items.push(...r.items);offset+=r.items.length;if(!r.items.length||offset>=r.total)break;}}
    if(token!==loading)return;_vsBatchList=items;sel.replaceChildren(new Option(items.length?'选择批次 / 배치 선택':'暂无待核对批次 / 대기 배치 없음',''));
    for(const item of items)sel.append(new Option(item.batch_no+' · '+(item.customer_name||'')+' · '+item.planned_qty,item.id));if(items.some(x=>x.id===prior))sel.value=prior;sel.disabled=false;start.disabled=!items.length;onVsBatchChange();
   }catch(e){if(token!==loading)return;sel.replaceChildren(new Option('读取失败 / 조회 실패',''));$('vsEntryError').textContent=e.message+'；请点击刷新重试 / 새로고침하세요';}
  };
  window.startVerifyScan=async button=>{
   const id=$('vsBatchSelect').value;if(!id){$('vsEntryError').textContent='请选择核对批次 / 배치를 선택하세요';return;}
   button.disabled=true;$('vsEntryError').textContent='';
   try{
    const openExisting=async()=>{
     const found=await CKSession.request('sop_verify_active',{batch_id:id}),job=found.active;
     if(!job)return false;
     if(job.can_takeover){
      if(!confirm('原派审员已离岗或休息。确认接班此核对任务？原人员、工时和扫码记录保留；接班后请扫描当前在岗工牌调整人员。 / 기존 기록은 유지됩니다. 현재 작업자를 다시 배정하세요.'))return true;
      await CKSession.request('sop_verify_takeover',{job_id:job.id,revision:job.revision,client_req_id:crypto.randomUUID()});
     }else if(!job.can_manage)throw Error('此核对任务仍由原派审员管理，请联系本部门负责人 / 기존 담당자에게 문의하세요');
     await CKOpenNativeJob({id:job.id,job_type:'verify_scan'});return true;
    };
    if(await openExisting())return;
    const r=await api({action:'v2_verify_job_start',batch_id:id});if(!r?.ok){if(/已有在途任务/.test(r?.error||'')&&await openExisting())return;throw Error(r?.error||'派工失败 / 배정 실패');}
    saveActiveJob(r.job_id,r.worker_seg_id);_vsBatchId=id;await window.initVerifyScan();
   }catch(e){$('vsEntryError').textContent=e.message;}finally{button.disabled=false;}
  };
  window.initVerifyScan=async()=>{
   if(!window._activeJobId){jobId='';batchId='';detail=null;enterVsEntryState();return;}
   const id=window._activeJobId;
   try{
    const r=await api({action:'v2_ops_job_detail',job_id:id});if(!r?.ok||r.job?.job_type!=='verify_scan'||!r.can_manage_dispatch)throw Error(r?.error||'请从首页打开自己的核对任务 / 홈에서 검수 작업을 여세요');
    if(id!==window._activeJobId)return;
    if(jobId!==id){jobId=id;batchId=r.job.related_doc_id;_vsBatchId=batchId;queue=[];try{queue=JSON.parse(sessionStorage.getItem(key(id))||'[]');}catch{}blocked=queue.length>0;busy=false;restored='';finishRequest=null;$('vsPalletInput').value=queue.at(-1)?.pallet_no||'';$('vsBarcodeInput').value='';show('','');$('vsFinishForm').reset();}
    $('vsEntryCard').style.display='none';for(const name of ['vsWorkingCard','vsScanLogCard','vsWorkersCard','vsActionCard'])$(name).style.display='';
    applyDetail(r);await refresh();startJobPoll('verify_scan');if(queue.length)show('warn','上次未确认的条码已保留，请重试首条 / 이전 대기 항목을 재시도하세요');else $('vsPalletInput').focus();
   }catch(e){$('vsEntryError').textContent=e.message;}
  };
  $('vsFinishForm').onsubmit=async e=>{
   e.preventDefault();if(queue.length||busy)return;const form=e.currentTarget,button=$('vsFinish');button.disabled=true;$('vsFinishError').textContent='';
   try{const data={job_id:jobId,complete_job:true,complete_batch:!check&&form.elements.complete_batch.checked,remark:form.elements.remark.value.trim()},signature=JSON.stringify(data);if(finishRequest?.signature!==signature)finishRequest={signature,body:{...data,client_req_id:crypto.randomUUID()}};
    const r=await api({action:'v2_verify_job_finish',...finishRequest.body});if(!r?.ok)throw Error(r?.error||'结束失败 / 종료 실패');CKClearNativeJob();jobId='';batchId='';_vsBatchId='';_vsLastPallet='';goPage('home');
   }catch(x){$('vsFinishError').textContent=x.message;button.disabled=false;}
  };
  const originalApi=window.api;window.api=async function(body){const r=await originalApi(body);if(body.action==='v2_ops_job_detail'&&r?.ok&&body.job_id===jobId)applyDetail(r);return r;};
  const originalPage=window.showPage;window.showPage=function(name,...args){if(window._currentPage==='verify_scan'&&name!=='verify_scan'&&queue.length){show('err','请先处理待提交队列，再离开此页 / 대기열 처리 후 이동하세요');return;}return originalPage(name,...args);};
  window.addEventListener('beforeunload',e=>{if(queue.length){persist();e.preventDefault();e.returnValue='';}});
  window.addEventListener('ck-native-people-changed',e=>{if(e.detail.job.id===jobId)applyDetail(e.detail);});
 };
})();
