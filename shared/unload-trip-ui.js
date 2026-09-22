(function(){
'use strict';
window.CKInstallUnloadTrip=function(){
 const $=id=>document.getElementById(id),esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const request=async body=>{const r=await window.api(body);if(!r?.ok)throw Error(r?.message||r?.error||'请求失败 / 요청 실패');return r;};
 let selected=new Map(),candidates=[],queryVersion=0,queue=Promise.resolve(),pendingScans=0,starting=false,scanner=null,lastScan='',lastScanAt=0,currentJob=null,finishRequest=null;
 try{selected=new Map(JSON.parse(sessionStorage.getItem('ck_unload_selection')||'[]'));}catch{}
 const entry=document.createElement('section');entry.id='ck-trip-entry';entry.className='ck-trip';
 entry.innerHTML=`<h3>本车入库计划 / 차량 입고계획</h3><p class="ck-trip-help">连续扫描入库计划单，全部加入后统一开始卸货。<br>입고계획을 연속 스캔한 후 한 번에 하차를 시작하세요.</p>
 <form id="ck-trip-scan" class="ck-trip-scan"><label for="ck-trip-code">扫描或输入入库单号 / 입고번호 스캔</label><div><input id="ck-trip-code" autocomplete="off" placeholder="扫码后自动加入 / 스캔 후 자동 추가"><button type="submit">加入 / 추가</button></div></form>
 <button type="button" id="ck-trip-camera" class="btn btn-outline btn-sm">相机连续扫码 / 카메라 연속 스캔</button><div id="ck-trip-reader"></div>
 <p id="ck-trip-message" role="status" aria-live="polite"></p><div class="ck-trip-heading"><b id="ck-trip-count"></b><span>同车统一派工 / 차량 단위 배정</span></div><ul id="ck-trip-selected"></ul>
 <details id="ck-trip-lookup"><summary>搜索或勾选入库计划 / 계획 검색·선택</summary><form id="ck-trip-search" class="ck-trip-search"><input aria-label="搜索客户或单号 / 고객·번호 검색" placeholder="客户、计划单号、外部入库单号"><button type="submit">搜索 / 검색</button></form><div id="ck-trip-candidates"></div><p class="ck-trip-help">显示最近50张候选计划；可搜索其他单据，扫码直接匹配全部计划。 / 최근 50개 표시, 검색·스캔으로 다른 계획 추가</p></details>
 <button id="ck-trip-start" class="btn btn-success" type="button">开始本车卸货 / 차량 하차 시작</button><button id="ck-trip-unplanned" class="btn btn-outline btn-sm" type="button">新建临时卸货 / 임시 하차 생성</button>`;
 const entryCard=$('unloadEntryCard');for(const child of [...entryCard.children])if(!['unplannedActiveList','unplannedUnloadFormCard'].includes(child.id))child.hidden=true;
 entryCard.prepend(entry);
 const working=document.createElement('section');working.className='card ck-trip';working.id='ck-trip-working';working.hidden=true;$('page-unload').append(working);
 const message=(text,error=false)=>{$('ck-trip-message').textContent=text;$('ck-trip-message').classList.toggle('ck-trip-error',error);};
 const name=p=>(p.display_no||p.id)+' · '+(p.customer||'—');
 const saveSelection=()=>sessionStorage.setItem('ck_unload_selection',JSON.stringify([...selected]));
 function renderSelected(){
  $('ck-trip-count').textContent='已选 '+selected.size+' 张 / '+selected.size+'개 선택';
  $('ck-trip-selected').innerHTML=selected.size?[...selected.values()].map(p=>`<li><div><strong>${esc(name(p))}</strong><small>${esc(p.cargo_summary||'')}</small></div><button type="button" data-remove="${esc(p.id)}" aria-label="移除 ${esc(p.display_no||p.id)}">移除 / 제외</button></li>`).join(''):'<li class="ck-trip-empty">尚未加入计划，扫描第一张入库单 / 첫 입고계획을 스캔하세요</li>';
  $('ck-trip-start').disabled=!selected.size||starting||pendingScans>0;
  $('ck-trip-candidates').querySelectorAll('input[type=checkbox]').forEach(c=>{c.checked=selected.has(c.value);});
 }
 function add(p){
  if(selected.has(p.id)){message('本车已加入 '+name(p)+'，不会重复添加 / 이미 추가된 계획입니다');return;}
  if(selected.size>=20)throw Error('一车最多选择20张计划 / 최대 20개 선택');
  selected.set(p.id,p);saveSelection();renderSelected();message('已加入 '+name(p)+'，请继续扫描 / 추가 완료, 계속 스캔하세요');
 }
 function enqueue(code){
  code=String(code||'').trim();if(!code||starting)return;
  pendingScans++;renderSelected();
  queue=queue.then(async()=>{const r=await request({action:'v2_inbound_plan_find_by_code',code,scene:'unload_trip'});add(r.plan);}).catch(e=>message(e.message,true)).finally(()=>{pendingScans--;renderSelected();});
 }
 $('ck-trip-scan').onsubmit=e=>{e.preventDefault();const code=$('ck-trip-code').value;$('ck-trip-code').value='';enqueue(code);$('ck-trip-code').focus();};
 $('ck-trip-selected').onclick=e=>{const b=e.target.closest('[data-remove]');if(!b||starting)return;selected.delete(b.dataset.remove);saveSelection();renderSelected();message('已从本车移除 / 차량 목록에서 제외했습니다');$('ck-trip-code').focus();};
 async function loadCandidates(){
  const version=++queryVersion;try{
   const keyword=$('ck-trip-search').querySelector('input').value.trim();
   const r=await request({action:'v2_inbound_plan_ops_candidates',scene:'unload',keyword,limit:50});if(version!==queryVersion)return;
   candidates=(r.items||[]).filter(p=>p.status==='pending');
   $('ck-trip-candidates').innerHTML=candidates.map(p=>`<label class="ck-trip-option"><input type="checkbox" value="${esc(p.id)}" ${selected.has(p.id)?'checked':''}><span><strong>${esc(name(p))}</strong><small>${esc(p.cargo_summary||'')}</small></span></label>`).join('')||'<p>暂无匹配的待卸货计划 / 하차 대기 계획이 없습니다</p>';
  }catch(e){message(e.message,true);}
 }
 $('ck-trip-candidates').onchange=e=>{const p=candidates.find(p=>p.id===e.target.value);if(!p||starting)return;try{if(e.target.checked)add(p);else{selected.delete(p.id);saveSelection();renderSelected();}}catch(error){e.target.checked=false;message(error.message,true);}};
 $('ck-trip-search').onsubmit=e=>{e.preventDefault();loadCandidates();};
 $('ck-trip-lookup').ontoggle=()=>{if($('ck-trip-lookup').open)loadCandidates();};
 async function stopCamera(){const old=scanner;scanner=null;if(old){await old.stop().catch(()=>{});try{old.clear();}catch{}}$('ck-trip-camera').textContent='相机连续扫码 / 카메라 연속 스캔';}
 $('ck-trip-camera').onclick=async()=>{
  if(scanner){await stopCamera();return;}
  try{const s=new Html5Qrcode('ck-trip-reader');scanner=s;await startManagedQrScanner(s,'ck-trip-reader',{fps:10,qrbox:{width:250,height:150}},code=>{const now=Date.now();if(code===lastScan&&now-lastScanAt<2500)return;lastScan=code;lastScanAt=now;enqueue(code);},()=>{});$('ck-trip-camera').textContent='停止相机 / 카메라 중지';}catch(e){await stopCamera();message('相机启动失败 / 카메라 오류: '+e.message,true);}
 };
 $('ck-trip-unplanned').onclick=()=>{stopCamera();openUnplannedUnloadForm();};
 const originalInit=window.initUnload,originalEntry=window.showUnloadEntry,originalActions=window.updateUnloadActions;
 window.showUnloadEntry=async function(){currentJob=null;working.hidden=true;await originalEntry();renderSelected();$('ck-trip-code').focus();};
 window.loadUnloadCandidates=loadCandidates;
 window.initUnload=async function(){
  working.hidden=true;
  if(_activeJobId){
   try{const r=await request({action:'v2_ops_job_detail',job_id:_activeJobId});
    if(r.job.related_doc_type==='field_feedback')localStorage.setItem('v2_unplanned_fb_id',r.job.related_doc_id);else localStorage.removeItem('v2_unplanned_fb_id');
    if(r.unload_plans?.length&&r.job.status==='working'){await showTrip(r);return;}
   }catch(e){message(e.message,true);}
  }
  return originalInit();
 };
 // Dispatchers finish the assigned team together, including legacy temporary unloads.
 window.updateUnloadActions=async function(){
  const id=_activeJobId;if(!id)return originalActions();
  const r=await request({action:'v2_ops_job_detail',job_id:id});if(_activeJobId!==id)return;
  if(!r.can_manage_dispatch)return originalActions();
  if(r.job.related_doc_type==='field_feedback')localStorage.setItem('v2_unplanned_fb_id',r.job.related_doc_id);else localStorage.removeItem('v2_unplanned_fb_id');
  $('unloadActions').innerHTML='<p class="ck-trip-help">由派审员核对实收后统一结束，所有参与人员同步结束计时。 / 담당자 확인 후 참여 인원의 작업시간을 함께 종료합니다.</p><button class="btn btn-success" id="ck-unload-team-finish">确认产出并结束卸货 / 확인 후 하차 종료</button><button class="btn btn-outline" id="ck-unload-keep-working">返回首页，任务继续 / 작업 유지·홈으로</button>';
  $('ck-unload-team-finish').onclick=e=>unloadComplete(e.currentTarget);$('ck-unload-keep-working').onclick=()=>goPage('home');
 };
 $('ck-trip-start').onclick=async()=>{
  if(starting||!selected.size||pendingScans)return;starting=true;renderSelected();await stopCamera();
  try{const r=await request({action:'v2_unload_job_start',plan_ids:[...selected.keys()],worker_id:getWorkerId(),worker_name:getWorkerName(),biz_class:''});
   saveActiveJob(r.job_id,r.worker_seg_id);localStorage.removeItem('v2_unplanned_fb_id');selected.clear();saveSelection();
   await window.initUnload();
  }catch(e){message(e.message,true);}finally{starting=false;renderSelected();}
 };
 async function showTrip(r){
  currentJob=r;finishRequest=null;await stopCamera();if(_pollTimer){clearInterval(_pollTimer);_pollTimer=null;}
  ['unloadEntryCard','unloadPlanCard','unloadWorkersCard','unloadResultCard'].forEach(id=>$(id).style.display='none');
  working.hidden=false;
  const workers=[...new Set(r.workers.filter(w=>!w.left_at).map(w=>w.worker_name))];
  working.innerHTML=`<div class="ck-trip-heading"><h3>本车卸货 · ${r.unload_plans.length} 张计划 / 차량 하차</h3><span class="ck-trip-tag">作业中 / 진행 중</span></div><p>操作人员 / 작업자：${workers.map(esc).join('、')}</p><p class="ck-trip-help">本车统一计时，完成时分别核对各计划的实收。 / 차량 단위로 시간 기록, 계획별 실제 수량 확인</p>
  <form id="ck-trip-result">${r.unload_plans.map(({plan:p,lines,needs})=>`<section class="ck-trip-plan"><h4>${esc(name(p))}</h4><p>${esc(p.cargo_summary||'')}</p>${needs.length?'<div class="ck-trip-needs"><b>卸货时的作业要求 / 하차 시 작업 요청</b>'+needs.map(n=>'<p><strong>'+esc(n.title)+'</strong> '+esc(n.scope_text||'')+'<br>'+esc(n.instructions||'')+'</p>').join('')+'</div>':''}<div class="ck-trip-quantities">${lines.map(l=>l.unit_type==='courier'?'<p class="ck-trip-help">快递 '+esc(l.planned_qty)+' 件：请逐件扫码收货，收齐后自动更新。 / 택배는 개별 송장 스캔</p>':`<label>${esc(window.unitLabel?.(l.unit_type)||l.unit_type)} <small>计划 / 계획 ${esc(l.planned_qty)}</small><input type="number" min="0" max="100000000" step="any" required inputmode="decimal" data-plan="${esc(p.id)}" data-line="${esc(l.id)}" aria-label="${esc(p.display_no)} ${esc(window.unitLabel?.(l.unit_type)||l.unit_type)} 实收" placeholder="实收 / 실제"></label>`).join('')}</div><label class="ck-trip-note">差异说明（选填） / 차이 설명(선택)<input data-diff="${esc(p.id)}" maxlength="1000"></label></section>`).join('')}
  <label>备注（选填） / 비고(선택)<input name="remark" maxlength="2000"></label><p id="ck-trip-finish-message" role="status"></p><button id="ck-trip-finish" class="btn btn-success" type="submit" ${r.can_manage_dispatch?'':'disabled'}>确认实收并结束本车卸货 / 확인 후 차량 하차 종료</button></form><button id="ck-trip-back" class="btn btn-outline" type="button">返回首页，任务继续 / 작업 유지·홈으로</button>`;
  $('ck-trip-back').onclick=()=>goPage('home');
  $('ck-trip-result').onsubmit=async e=>{
   e.preventDefault();const button=$('ck-trip-finish');if(button.disabled)return;button.disabled=true;
   try{
    if(!finishRequest)finishRequest={action:'v2_unload_job_finish',job_id:r.job.id,worker_id:getWorkerId(),complete_job:true,client_req_id:crypto.randomUUID(),remark:new FormData(e.target).get('remark'),plan_results:r.unload_plans.map(({plan})=>({plan_id:plan.id,lines:[...working.querySelectorAll('[data-plan]')].filter(x=>x.dataset.plan===plan.id).map(x=>({line_id:x.dataset.line,actual_qty:Number(x.value)})),diff_note:[...working.querySelectorAll('[data-diff]')].find(x=>x.dataset.diff===plan.id).value}))};
    await request(finishRequest);finishRequest=null;window.CKClearNativeJob?.();working.innerHTML='<h3>本车卸货已完成 / 차량 하차 완료</h3><p>各计划实收已保存，所有参与人员已结束计时。 / 실제 수량 저장 및 작업시간 종료 완료</p><button id="ck-trip-done" class="btn btn-success">返回首页 / 홈으로</button>';$('ck-trip-done').onclick=()=>goPage('home');
   }catch(error){if(!(error instanceof TypeError))finishRequest=null;$('ck-trip-finish-message').textContent=error.message;button.disabled=false;}
  };
 }
 const showPage=window.showPage;window.showPage=function(name,...args){if(name!=='unload')stopCamera();return showPage(name,...args);};
 window.unloadGoBack=()=>goPage('home');
 renderSelected();
};
})();
