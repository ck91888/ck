(function(){
'use strict';
const labels={direct_ship:['理货上架','검수·적치'],bulk:['直进直出','입고 후 직출'],return:['整托退回','팔레트 반송'],change_order:['换单','송장 교체']};
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
window.CKInboundLabel=key=>labels[key]?.join(' / ')||key;
function externalField(id,value=''){
 const box=document.createElement('div');box.className='form-group ck-external-field';box.innerHTML='<label for="'+id+'">外部系统入库单号 / 외부 시스템 입고번호 <span aria-hidden="true">*</span></label><input id="'+id+'" type="text" maxlength="120" autocomplete="off" value="'+esc(value)+'" placeholder="客服填写自有系统的入库单号 / 외부 입고번호"><p class="muted">现场理货扫描此单号关联本计划 / 현장에서 이 번호로 입고계획을 찾습니다</p>';return box;
}
function choices(host,selector,id,value=''){
 if(!host)return;host.classList.add('ck-inbound-choices');
 for(const input of host.querySelectorAll(selector)){const label=input.closest('label');label.classList.add('ck-inbound-choice');label.replaceChildren(input);const text=document.createElement('span');text.innerHTML='<strong>'+esc(labels[input.value][0])+'</strong><small>'+esc(labels[input.value][1])+'</small>';label.append(text);}
 const field=externalField(id,value);host.after(field);
 const update=()=>{const on=[...host.querySelectorAll(selector)].some(x=>x.checked&&x.value==='direct_ship');field.hidden=!on;field.querySelector('input').required=on;};
 host.addEventListener('change',update);update();
}
function selectField(select,id,value=''){
 if(!select)return;for(const option of [...select.options]){if(labels[option.value])option.textContent=CKInboundLabel(option.value);else if(option.value)option.remove();}
 const field=externalField(id,value);select.closest('div').after(field);field.style.gridColumn='1/-1';const update=()=>{field.hidden=select.value!=='direct_ship';};select.addEventListener('change',update);update();
}
window.CKInstallInboundFlow=function(app){
 if(!window.CK_SOP_ROLLOUT?.staging)return;
 if(app==='002'){
  window.inboundBizTaskLabel=window.CKInboundLabel;
  const statusLabel=window.inboundStatusLabel;window.inboundStatusLabel=status=>status==='completed'?'已入库 / 입고 완료':statusLabel(status);
  document.querySelector('#ibFilterStatus option[value="completed"]').textContent='已入库 / 입고 완료';
  choices(document.getElementById('ibc-biz-classes'),'.ibc-biz-chk','ck-ibc-external');
  const note=document.getElementById('ibc-biz-classes').nextElementSibling.nextElementSibling;
  if(note)note.textContent='理货上架需完成实际理货；其余类型卸货完成后自动入库 / 검수·적치 외 유형은 하차 완료 시 자동 입고';
  const create=document.getElementById('view-inbound_create');create.classList.add('ck-inbound-form');
  for(const id of ['ibc-date','ibc-customer'])document.getElementById(id).closest('.form-group').classList.add('ck-inbound-half');
  for(const opt of document.getElementById('ibFilterBizClass').options)if(labels[opt.value])opt.textContent=CKInboundLabel(opt.value);
  const edit=window.openInboundEditForm;window.openInboundEditForm=function(){edit();const first=document.querySelector('#ibEditOverlay .ib-edit-biz');if(first)choices(first.closest('label').parentElement,'.ib-edit-biz','ck-ib-edit-external',window._currentInboundPlan?.external_inbound_no||'');};
  const detail=window.loadInboundDetail;window.loadInboundDetail=async function(...args){await detail(...args);const p=window._currentInboundPlan,host=document.getElementById('inboundDetailBody');if(!p||!host)return;
   const box=document.createElement('section');box.className='ck-inbound-reference';box.innerHTML='<strong>外部系统入库单号 / 외부 입고번호</strong><span>'+esc(p.external_inbound_no||'未填写 / 미등록')+'</span>';
   if(!['completed','cancelled'].includes(p.status)&&p.source_type!=='return_session'){
    const button=document.createElement('button');button.type='button';button.className='btn btn-outline btn-sm';button.textContent='填写／更正单号 / 번호 수정';box.append(button);
    button.onclick=()=>{button.hidden=true;const form=document.createElement('form');form.append(externalField('ck-bind-external',p.external_inbound_no||''));form.insertAdjacentHTML('beforeend','<button type="submit" class="btn btn-primary">保存单号 / 저장</button><button type="button" class="btn btn-outline">取消 / 취소</button><p role="alert"></p>');box.append(form);form.querySelector('[type=button]').onclick=()=>{form.remove();button.hidden=false;};form.onsubmit=async e=>{e.preventDefault();const save=form.querySelector('[type=submit]');save.disabled=true;try{await CKSession.request('v2_inbound_plan_bind_external',{id:p.id,previous_code:p.external_inbound_no||'',external_inbound_no:form.querySelector('input').value,client_req_id:crypto.randomUUID()});await loadInboundDetail();}catch(err){form.querySelector('[role=alert]').textContent=err.message;save.disabled=false;}};};
   }host.prepend(box);selectField(document.getElementById('dynBiz'),'ck-dyn-external',p.external_inbound_no||'');
  };
  const feedback=window.loadFeedbackDetail;window.loadFeedbackDetail=async function(...args){await feedback(...args);selectField(document.getElementById('fb-conv-biz'),'ck-fb-external');};
  const original=window.api;window.api=async function(body){
   const fields={v2_inbound_plan_create:'ck-ibc-external',v2_inbound_plan_update:'ck-ib-edit-external',v2_feedback_finalize_to_inbound:'ck-fb-external',v2_feedback_convert_to_inbound:'ck-fb-external',v2_inbound_dynamic_finalize:'ck-dyn-external'};
   if(fields[body.action]){const input=document.getElementById(fields[body.action]);if(input)body.external_inbound_no=input.value.trim();}
   const result=await original(body);
   if(body.action==='v2_inbound_plan_create'&&result?.ok){const input=document.getElementById('ck-ibc-external');input.value='';input.closest('.ck-external-field').hidden=true;}
   return result;
  };
 }else if(app==='001'){
  for(const button of document.querySelectorAll('#page-inbound_menu button'))if(button.getAttribute('onclick')?.includes('inbound_change_order'))button.remove();
  const code=document.getElementById('inboundCodeInput');code.placeholder='扫描外部系统入库单号 / 외부 입고번호 스캔';code.addEventListener('input',()=>{window._ibResolvedKind='';window._ibResolvedPlanId='';window._ibResolvedPlan=null;});
  document.querySelector('#inboundEntryCard>label').textContent='扫描外部系统入库单号 / 외부 입고번호 스캔';
  const resolve=window.resolveInboundCode;window.resolveInboundCode=async function(...args){window._ibResolvedKind='';window._ibResolvedPlanId='';window._ibResolvedPlan=null;await resolve(...args);if(window._ibResolvedKind==='system'){const p=window._ibResolvedPlan;document.getElementById('ibResolveResult').innerHTML='<div class="ck-inbound-match"><strong>已关联入库计划 / 입고계획 연결됨</strong><div>外部入库单号：'+esc(p.external_inbound_no||'—')+'</div><div>'+esc(p.display_no||p.id)+' · '+esc(p.customer)+'</div><div>'+esc(p.cargo_summary||'')+'</div></div>';}};
  window.loadInboundCandidates=async function(){const sel=document.getElementById('inboundPlanSelect'),bc=_resolveBizClass();const r=await api({action:'v2_inbound_plan_ops_candidates',scene:'putaway',biz_class:bc,required_biz_class:bc});sel.innerHTML='<option value="">选择待理货计划 / 입고계획 선택</option>';for(const p of r?.items||[]){const o=document.createElement('option');o.value=p.id;o.dataset.code=p.external_inbound_no||p.display_no||p.id;o.textContent=(p.external_inbound_no||'未填外部号')+' · '+p.customer+' · '+(p.display_no||p.id);sel.append(o);}};
  window.onInboundCandidateSelect=function(){const s=document.getElementById('inboundPlanSelect');if(!s.value)return;code.value=s.selectedOptions[0].dataset.code;resolveInboundCode();};
 }
};
})();
