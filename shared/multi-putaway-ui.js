(function(){
'use strict';
window.CKInstallMultiPutaway=function(){
 const $=id=>document.getElementById(id),active=()=>window._pageParams?.job_type==='inbound_direct_putaway';
 const list=document.createElement('section');list.id='ck-putaway-selected';list.className='ck-putaway-selected';list.hidden=true;
 $('inboundPlanSelect').parentElement.after(list);
 let selected=[],busy=false,detail=null,cancelRequest=null,lookup=0;
 $('inboundCodeInput').parentElement.classList.add('ck-putaway-scan-row');
 const start=$('inboundEntryCard').querySelector('[onclick="startInbound(this)"]'),identify=$('inboundEntryCard').querySelector('[onclick="resolveInboundCode(this)"]');
 function render(){
  $('inboundEntryCard').classList.toggle('ck-multi-putaway-entry',active());
  list.hidden=!active();start.textContent=active()?'开始共同上架 / 공동 적치 시작':'开始入库 / 입고 시작';identify.textContent=active()?'识别并加入 / 인식·추가':'识别单号 / 번호 인식';
  if(!active())return;
  list.innerHTML='<h3>已选入库单 / 선택한 입고번호（'+selected.length+'）</h3><p>连续扫码、输入或选择加入；一次派工，数量和工时按整任务记录。 / 연속 스캔·입력·선택, 한 번 배정. 수량·시간은 작업 전체 기준.</p>';
  for(const [i,p] of selected.entries()){
   const row=document.createElement('div');row.className='ck-putaway-member';
   const info=document.createElement('div'),no=document.createElement('strong'),hint=document.createElement('small');no.textContent=p.selected_external_inbound_no;hint.textContent=(p.source_type==='direct_tally_session'?'独立单号 / 독립 입고번호':(p.display_no||p.id)+' · '+(p.customer||''))+' · 理货完成 / 검수 완료';info.append(no,hint);
   const remove=document.createElement('button');remove.type='button';remove.textContent='移除 / 삭제';remove.disabled=busy;remove.setAttribute('aria-label',p.selected_external_inbound_no+' 移除 / 삭제');remove.onclick=()=>{selected.splice(i,1);render();};row.append(info,remove);list.append(row);
  }
  if(!selected.length){const empty=document.createElement('p');empty.textContent='尚未加入入库单 / 입고번호를 추가하세요';list.append(empty);}
 }
 const clear=window.clearInboundResolve;window.clearInboundResolve=function(){lookup++;if(!busy)selected=[];clear();render();};
 const init=window.initInbound;window.initInbound=async function(...args){await init(...args);render();};
 const resolve=window.resolveInboundCode;window.resolveInboundCode=async function(...args){
  if(!active())return resolve(...args);if(busy)return;
  const ticket=++lookup,input=$('inboundCodeInput').value.trim();
  await resolve(...args);
  if(ticket!==lookup||!active()||$('inboundCodeInput').value.trim()!==input){render();return;}
  const p=window._ibResolvedPlan,ref=p?.inbound_progress?.items.find(x=>x.external_no===p.selected_external_inbound_no);
  if(['system','independent'].includes(window._ibResolvedKind)&&p?.id){
   if(ref?.status!=='tallied_pending_putaway'){$('ibResolveResult').textContent='此单不在待上架状态，请继续原任务 / 적치 대기 상태가 아닙니다';window._ibResolvedKind='';render();return;}
   const duplicate=selected.some(x=>x.id===p.id&&x.selected_external_inbound_no===p.selected_external_inbound_no);
   if(!duplicate&&selected.length>=50){$('ibResolveResult').textContent='每次最多 50 单 / 최대 50개';render();return;}
   if(!duplicate)selected.push(p);
   $('ibResolveResult').textContent=p.selected_external_inbound_no+(duplicate?' 已在列表中 / 이미 선택됨':' 已加入 / 추가됨');
   $('inboundCodeInput').value='';$('inboundPlanSelect').value='';$('inboundCodeInput').focus();
  }
  render();
 };
 $('inboundCodeInput').addEventListener('keydown',e=>{if(active()&&e.key==='Enter'){e.preventDefault();window.resolveInboundCode(identify);}});
 const begin=window.startInbound;window.startInbound=function(button){
  if(!active())return begin(button);if(busy)return;
  if(!selected.length){alert('请先加入已完成理货的入库单 / 검수 완료 입고번호를 추가하세요');return;}
  if($('inboundCodeInput').value.trim()){alert('还有单号未加入，请先识别或清空输入 / 입력한 번호를 먼저 추가하거나 지우세요');return;}
  if(hasOtherActiveJob())return warnActiveJob();
  return withActionLock('startInbound',button,'提交中 / 저장 중',async()=>{
   busy=true;lookup++;render();
   try{
    stopInboundScan();
    const r=await api({action:'v2_inbound_job_start',job_type:'inbound_direct_putaway',biz_class:'direct_ship',putaway_refs:selected.map(p=>({plan_id:p.id,external_inbound_no:p.selected_external_inbound_no}))});
    if(!r?.ok){alert(r?.message||r?.error||'上架派工失败 / 적치 배정 실패');return;}
    saveActiveJob(r.job_id,r.worker_seg_id);selected=[];$('inboundEntryCard').style.display='none';$('inboundWorkingCard').style.display='';
    await loadInboundPlanInfo(r.plan_id);await refreshInboundWorkers();startJobPoll('inbound');
   }finally{busy=false;render();}
  });
 };
 const members=document.createElement('section');members.id='ck-putaway-members';members.className='ck-putaway-selected';members.hidden=true;$('inboundPlanInfoCard').before(members);
 const cancel=document.createElement('button');cancel.type='button';cancel.id='ck-putaway-cancel';cancel.className='btn btn-outline';cancel.textContent='取消本次上架 / 이번 적치 취소';cancel.hidden=true;$('inboundWorkingCard').append(cancel);
 const original=window.api;window.api=async function(body){const r=await original(body);if(body.action==='v2_ops_job_detail'&&r?.ok&&body.job_id===window._activeJobId){
  detail=r;const refs=r.putaway_refs||[],show=r.job.job_type==='inbound_direct_putaway';members.hidden=!show;$('inboundWorkingCard').classList.toggle('ck-multi-putaway-working',show&&refs.length>1);cancel.hidden=!show||!r.can_manage_dispatch||['completed','cancelled'].includes(r.job.status);
  if(show){members.innerHTML='<h3>本次共同上架 / 이번 공동 적치</h3>';for(const ref of refs){const row=document.createElement('p');row.textContent=ref.external_inbound_no+' · '+(ref.display_no||'独立单号 / 독립 입고번호')+(ref.customer?' · '+ref.customer:'')+' · '+(ref.status==='completed'?'已入库 / 입고 완료':'上架中 / 적치 중');members.append(row);}if(!refs.length){const p=document.createElement('p');p.textContent=r.job.inbound_external_no||r.job.business_no;members.append(p);}const note=document.createElement('p');note.textContent='货位数、叉车及人员工时按本次任务记录一次，未按单分摊。 / 위치 수·지게차·인원 시간은 작업 전체 기준, 입고번호별 배분 없음.';members.append(note);}
 }return r;};
 cancel.onclick=async()=>{
  if(!detail?.can_manage_dispatch)return;
  const d=document.createElement('dialog');d.className='ck-workflow';d.innerHTML='<form><h2>取消本次上架 / 이번 적치 취소</h2><p>停止人员计时，保留理货和本次任务历史，全部关联单回到待上架。 / 작업시간 종료, 검수·작업 이력 유지, 전체 입고번호 적치 대기 복귀.</p><label>取消原因 / 취소 사유<textarea required maxlength="500"></textarea></label><p role="alert"></p><button type="submit">确认取消上架 / 적치 취소 확인</button><button type="button" data-close>返回 / 돌아가기</button></form>';document.body.append(d);d.showModal();let saving=false;
  const close=()=>{if(!saving){d.close();d.remove();}};d.querySelector('[data-close]').onclick=close;d.oncancel=e=>{e.preventDefault();close();};
  d.querySelector('form').onsubmit=async e=>{e.preventDefault();if(saving)return;saving=true;const btn=d.querySelector('[type=submit]');btn.disabled=true;try{const reason=d.querySelector('textarea').value.trim(),key=JSON.stringify([detail.job.id,detail.dispatch.revision,reason]);if(cancelRequest?.key!==key)cancelRequest={key,body:{job_id:detail.job.id,revision:detail.dispatch.revision,reason,client_req_id:crypto.randomUUID()}};await CKSession.request('sop_putaway_cancel',cancelRequest.body);saving=false;close();CKClearNativeJob();goPage('home');}catch(error){d.querySelector('[role=alert]').textContent=error.message;}finally{saving=false;btn.disabled=false;}};
 };
 $('ck-putaway-workload').querySelector('p').textContent='本次任务整体货位数（不按单复制） / 작업 전체 적치 위치 수 (입고번호별 복제 없음)';
 render();
};
})();
