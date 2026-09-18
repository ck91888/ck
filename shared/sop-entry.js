(function(){
 'use strict';
 if(!window.CK_SOP_ROLLOUT?.enabled)return;
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const link=(tab,source,id)=>'../sop/?tab='+encodeURIComponent(tab)+(source?'&source='+encodeURIComponent(source)+'&source_id='+encodeURIComponent(id||''):'');
 const tabs=document.getElementById('mainTabs');
 if(tabs){
  const inbound=tabs.querySelector('[data-tab="inbound"]'),outbound=tabs.querySelector('[data-tab="outbound"]');
  tabs.insertBefore(inbound,outbound);outbound.textContent='出库计划 / 출고 계획';
  const b=document.createElement('button');b.textContent='作业需求 / 작업 요청';b.onclick=()=>location.href=link('need');tabs.insertBefore(b,outbound);
 }
 const home=document.querySelector('.home-grid');if(home){const b=document.createElement('div');b.className='home-btn accent';b.textContent='负责人派工 / 담당자 배정';b.onclick=()=>location.href=link('task');home.prepend(b);}
 const dataTabs=document.querySelector('.tab-bar');if(!tabs&&dataTabs){const b=document.createElement('button');b.textContent='调度与日报 / 배정·일일보고';b.onclick=()=>location.href=link('dashboard');dataTabs.append(b);}
 // Wrap detail rendering, always refer to the canonical record instead of a second instruction box.
 const oldApi=window.api;
 if(oldApi){window.api=async function(params){const r=await oldApi(params);
  if(r?.ok&&params.action==='v2_outbound_order_detail'&&r.sop_needs?.length){
   // Keep the legacy data value intact. Canonical requirements are shown in the linked panel.
   setTimeout(()=>addSource('outboundDetailBody','outbound',params.id,r.sop_needs),0);
  }else if(r?.ok&&params.action==='v2_outbound_order_detail')setTimeout(()=>addSource('outboundDetailBody','outbound',params.id,[]),0);
  if(r?.ok&&params.action==='v2_inbound_plan_detail')setTimeout(()=>addSource('inboundDetailBody','inbound',params.id,[]),0);
  if(r?.ok&&params.action==='v2_issue_detail')setTimeout(()=>addSource('issueDetailBody','issue',params.id,[]),0);
  if(r?.ok&&params.action==='v2_verify_batch_detail')setTimeout(()=>addSource('verifyDetailBody','check',params.id,[]),0);
  return r;};}
 function addSource(target,type,id,needs){const body=document.getElementById(target);if(!body||body.querySelector('[data-sop-source]'))return;
  const box=document.createElement('div');box.className='card';box.dataset.sopSource='true';
  box.innerHTML='<b>'+(needs.length?'关联作业（统一记录）':'负责人工作台')+'</b><p>'+needs.map(n=>esc(n.title)+' · '+esc(n.status)).join('<br>')+'</p>';
  const a=document.createElement('a');a.className='btn btn-outline';a.href=link(type==='issue'?'issue':type==='check'?'check':'need',type,id);a.textContent=needs.length?'查看关联作业 / 연결 작업':'创建或关联作业 / 작업 연결';box.append(a);body.prepend(box);
 }
})();
