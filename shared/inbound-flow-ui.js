(function(){
'use strict';
const labels={direct_ship:['代发理货上架','직배송 검수·적치'],bulk_putaway:['大货理货上架','대량화물 검수·적치'],bulk:['直进直出','입고 후 직출'],return:['整托退回','팔레트 반송'],change_order:['换单','송장 교체']};
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const copy=(zh,ko)=>window.getLang?.()==='ko'?ko:zh;
const codes=value=>[...new Set(String(value||'').normalize('NFKC').split(/[\n\r,;，；\t]+/).map(x=>x.trim()).filter(Boolean))];
const classes=p=>p.biz_classes||JSON.parse(p.biz_classes_json||'[]');
window.CKInboundLabel=key=>labels[key]?(location.pathname.startsWith('/001/')?labels[key].join(' / '):labels[key][window.getLang?.()==='ko'?1:0]):key;
const mark=(zh,ko)=>window.CKPlanCopy?CKPlanCopy.html(zh):esc(copy(zh,ko));
function externalField(id,value='',bulkValue=''){
 const box=document.createElement('div');box.className='form-group ck-external-field';
 const bulk=codes(bulkValue),direct=codes(value).filter(x=>!bulk.includes(x));
  box.innerHTML=['direct_ship','bulk_putaway'].map((biz,i)=>'<div data-reference-dept="'+biz+'"><label for="'+id+(i?'-bulk':'')+'">'+mark(...labels[biz])+' · '+mark('外部入库单号（可多个）','외부 입고번호（복수 가능）')+'</label><textarea data-reference="'+biz+'" id="'+id+(i?'-bulk':'')+'" rows="2" maxlength="6050" autocomplete="off" placeholder="'+copy('可暂缺，后期在原单补充；每行一个或逗号分隔','나중에 원래 계획에서 보완 가능; 한 줄에 한 번호 또는 쉼표 구분')+'">'+esc((i?bulk:direct).join('\n'))+'</textarea></div>').join('')+'<p class="muted">'+copy('可先建单、接收货物，外部号待补充；实际理货开工前须按部门补齐。所有关联单号逐单完成后，整单才入库完成。','계획 생성·입고 수령 후 외부 입고번호 보완 가능. 검수 시작 전 부서별 번호를 입력하고 모든 번호를 완료해야 전체 입고가 완료됩니다.')+'</p>';return box;
}
 function showFields(field,biz){field.hidden=!biz.some(x=>['direct_ship','bulk_putaway'].includes(x));for(const group of field.querySelectorAll('[data-reference-dept]')){group.hidden=!biz.includes(group.dataset.referenceDept);group.querySelector('textarea').required=false;}}
function fieldData(field){
 const value=biz=>{const input=field.querySelector('[data-reference="'+biz+'"]');return input&&!input.parentElement.hidden?codes(input.value):[];};
 const direct=value('direct_ship'),bulk=value('bulk_putaway');
 if(direct.some(x=>bulk.includes(x)))throw Error(copy('两部门不能使用同一外部入库单号，请分开填写','동일 입고번호를 두 부서에 지정할 수 없습니다'));
 return {external_inbound_no:[...direct,...bulk].join('\n'),direct_external_inbound_no:direct.join('\n'),bulk_external_inbound_no:bulk.join('\n')};
}
 function referenceNotice(p){
  const all=codes(p?.external_inbound_no),bulk=codes(p?.bulk_external_inbound_no),missing=classes(p||{}).filter(b=>['direct_ship','bulk_putaway'].includes(b)&&!(b==='bulk_putaway'?bulk:all.filter(x=>!bulk.includes(x))).length);
  let html=missing.length?'<p class="ck-reference-pending"><strong>'+copy('外部入库单号待补充','외부 입고번호 보완 대기')+'</strong>：'+missing.map(b=>esc(CKInboundLabel(b))).join('、')+'</p>':'';
  for(const x of p?.external_reference_history||[]){const t=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Seoul',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(new Date(x.created_at));html+='<p class="ck-reference-supplemented"><strong>'+copy('外部入库单号已补充','외부 입고번호 보완 완료')+'</strong> · '+esc(CKInboundLabel(x.biz_class))+' · '+esc(x.actor_name)+' · '+esc(t)+' KST</p>';}
  return html;
 }
window.CKInboundReferenceNotice=referenceNotice;
const issueRequests=new Map();
window.CKInboundIssueTag=p=>{const s=p?.issue_state;if(!s)return '';const label={unissued:['未下发','미배포'],issued:['已打印下发','인쇄·배포 완료'],needs_reissue:['内容已更新，需重新打印下发','변경됨 · 재인쇄·배포 필요'],cancelled:['已取消，不可下发','취소됨 · 배포 불가']}[s.state];return '<span class="ck-inbound-issue-status" data-issue-state="'+esc(s.state)+'">'+esc(copy(...label))+'</span>';};
function issuePanel(host,p){
 const state=p.issue_state;if(!state)return;
 const panel=document.createElement('section');panel.className='ck-inbound-issue';panel.innerHTML=CKInboundIssueTag(p)+'<p>'+copy('点击详情底部“打印”即记录下发；取消打印仍保留下发记录。','상세 하단의 인쇄를 누르면 배포가 기록됩니다. 인쇄를 취소해도 기록은 유지됩니다.')+'</p>'+(state.confirmed_at?'<p>'+esc(state.actor_name)+' · '+esc(new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Seoul',dateStyle:'short',timeStyle:'medium',hour12:false}).format(new Date(state.confirmed_at)))+' KST</p>':'');
 const status=document.createElement('p');status.setAttribute('role','status');panel.append(status);
 host.prepend(panel);
}
window.CKInboundRecordPrintIssue=async p=>{if(!p.issue_state||p.issue_state.state==='cancelled')throw Error(copy('当前计划不能打印下发，请刷新后重试','현재 계획은 인쇄·배포할 수 없습니다. 새로고침 후 다시 시도하세요.'));const key=p.id+':'+p.issue_state.revision;if(!issueRequests.has(key))issueRequests.set(key,crypto.randomUUID());return CKSession.request('v2_inbound_plan_confirm_issue',{id:p.id,revision:p.issue_state.revision,client_req_id:issueRequests.get(key)});};
window.CKInboundPrintIssued=async(p,result)=>{if(window._currentInboundId!==p.id)return;const host=document.getElementById('view-inbound_detail');if(host?.style.display!=='none'){if(window._currentInboundPlan?.id===p.id)window._currentInboundPlan.issue_state=result.issue_state;await loadInboundDetail();}else if(document.getElementById('view-inbound')?.style.display!=='none')await loadInboundList();};
 function progressHtml(p){const progress=p?.inbound_progress,notice=referenceNotice(p);if(!progress?.total)return notice;
 const state={pending:copy('待理货','대기'),working:copy('理货中','작업 중'),completed:copy('已入库','완료')};
  return notice+'<div class="ck-inbound-progress"><strong>'+copy('外部单理货进度','입고 진행')+'：'+progress.completed+' / '+progress.total+'</strong><table class="line-table"><thead><tr><th>'+copy('理货部门','입고 부서')+'</th><th>'+copy('外部入库单号','입고번호')+'</th><th>'+copy('状态','상태')+'</th></tr></thead><tbody>'+progress.items.map(x=>'<tr><td>'+esc(CKInboundLabel(x.biz_class||'direct_ship'))+'</td><td>'+esc(x.external_no)+'</td><td>'+state[x.status]+'</td></tr>').join('')+'</tbody></table></div>';
}
function choices(host,selector,id,p={}){
 if(!host)return;host.classList.add('ck-inbound-choices');
 if(!host.querySelector('[value="bulk_putaway"]')){const label=document.createElement('label'),input=document.createElement('input');input.type='checkbox';input.className=selector.slice(1);input.value='bulk_putaway';input.checked=classes(p).includes('bulk_putaway');label.append(input);host.querySelector('[value="direct_ship"]').closest('label').after(label);}
 for(const input of host.querySelectorAll(selector)){const label=input.closest('label');label.classList.add('ck-inbound-choice');label.replaceChildren(input);const text=document.createElement('span');if(window.CKPlanCopy)CKPlanCopy.bind(text,labels[input.value][0]);else text.textContent=CKInboundLabel(input.value);label.append(text);}
 const field=externalField(id,p.external_inbound_no,p.bulk_external_inbound_no);host.after(field);
 const update=()=>showFields(field,[...host.querySelectorAll(selector)].filter(x=>x.checked).map(x=>x.value));host.addEventListener('change',update);update();
}
function selectField(select,id,p={}){
 if(!select)return;if(!select.querySelector('[value="bulk_putaway"]'))select.add(new Option(CKInboundLabel('bulk_putaway'),'bulk_putaway'),select.querySelector('[value="bulk"]'));
 for(const option of [...select.options]){if(labels[option.value]){if(window.CKPlanCopy)CKPlanCopy.bind(option,labels[option.value][0]);else option.textContent=CKInboundLabel(option.value);}else if(option.value)option.remove();}
 const field=externalField(id,p.external_inbound_no,p.bulk_external_inbound_no);select.closest('div').after(field);field.style.gridColumn='1/-1';const update=()=>showFields(field,[select.value]);select.addEventListener('change',update);update();
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
  document.getElementById('ibc-date').closest('.form-group').hidden=true;
  const customerGroup=document.getElementById('ibc-customer').closest('.form-group'),arrivalGroup=document.getElementById('ibc-arrival').closest('.form-group');
  customerGroup.classList.add('ck-inbound-half');arrivalGroup.classList.add('ck-inbound-half');customerGroup.after(arrivalGroup);
  document.querySelector('#view-inbound .filter-label b').textContent='建单日期 / 등록일';
  document.getElementById('ibFilterBizClass').add(new Option(CKInboundLabel('bulk_putaway'),'bulk_putaway'));
  for(const opt of document.getElementById('ibFilterBizClass').options)if(labels[opt.value]){if(window.CKPlanCopy)CKPlanCopy.bind(opt,labels[opt.value][0]);else opt.textContent=CKInboundLabel(opt.value);}
  const edit=window.openInboundEditForm;window.openInboundEditForm=function(){edit();document.getElementById('ib-edit-date').parentElement.hidden=true;const first=document.querySelector('#ibEditOverlay .ib-edit-biz');if(first)choices(first.closest('label').parentElement,'.ib-edit-biz','ck-ib-edit-external',window._currentInboundPlan||{});};
  const detail=window.loadInboundDetail;window.loadInboundDetail=async function(...args){await detail(...args);const p=window._currentInboundPlan,host=document.getElementById('inboundDetailBody');if(!p||!host)return;
   const box=document.createElement('section');box.className='ck-inbound-reference';box.innerHTML=progressHtml(p)||'<strong>外部系统入库单号 / 외부 입고번호</strong><span>'+esc(p.external_inbound_no||'未填写 / 미등록')+'</span>';
   if(!['completed','cancelled'].includes(p.status)&&p.source_type!=='return_session'){
    const button=document.createElement('button');button.type='button';button.className='btn btn-outline btn-sm';button.textContent='填写／更正单号 / 번호 수정';box.append(button);
    button.onclick=()=>{button.hidden=true;const form=document.createElement('form');const fields=externalField('ck-bind-external',p.external_inbound_no,p.bulk_external_inbound_no);showFields(fields,classes(p));form.append(fields);form.insertAdjacentHTML('beforeend','<button type="submit" class="btn btn-primary">保存单号 / 저장</button><button type="button" class="btn btn-outline">取消 / 취소</button><p role="alert"></p>');box.append(form);form.querySelector('[type=button]').onclick=()=>{form.remove();button.hidden=false;};let pending=null;form.addEventListener('input',()=>{pending=null;});form.onsubmit=async e=>{e.preventDefault();const save=form.querySelector('[type=submit]');save.disabled=true;try{if(!pending)pending={id:p.id,previous_code:p.external_inbound_no||'',previous_bulk_code:p.bulk_external_inbound_no||'',...fieldData(fields),client_req_id:crypto.randomUUID()};await CKSession.request('v2_inbound_plan_bind_external',pending);await loadInboundDetail();}catch(err){form.querySelector('[role=alert]').textContent=err.message;save.disabled=false;}};};
   }host.prepend(box);issuePanel(host,p);selectField(document.getElementById('dynBiz'),'ck-dyn-external',p);
  };
  const feedback=window.loadFeedbackDetail;window.loadFeedbackDetail=async function(...args){await feedback(...args);selectField(document.getElementById('fb-conv-biz'),'ck-fb-external');};
  const original=window.api;window.api=async function(body){
   const fields={v2_inbound_plan_create:'ck-ibc-external',v2_inbound_plan_update:'ck-ib-edit-external',v2_feedback_finalize_to_inbound:'ck-fb-external',v2_feedback_convert_to_inbound:'ck-fb-external',v2_inbound_dynamic_finalize:'ck-dyn-external'};
   if(fields[body.action]){const input=document.getElementById(fields[body.action]);if(input)Object.assign(body,fieldData(input.closest('.ck-external-field')));}
   const result=await original(body);
   if(body.action==='v2_inbound_plan_create'&&result?.ok){const input=document.getElementById('ck-ibc-external');input.closest('.ck-external-field').querySelectorAll('textarea').forEach(x=>x.value='');input.closest('.ck-external-field').hidden=true;}
   return result;
  };
 }else if(app==='001'){
  JOB_TYPE_LABEL.inbound_direct=CKInboundLabel('direct_ship');JOB_TYPE_LABEL.inbound_bulk=CKInboundLabel('bulk_putaway');
  for(const button of document.querySelectorAll('#page-inbound_menu button')){const action=button.getAttribute('onclick')||'';if(action.includes('inbound_change_order'))button.remove();else if(action.includes('inbound_direct'))button.textContent=CKInboundLabel('direct_ship');else if(action.includes('inbound_bulk'))button.textContent=CKInboundLabel('bulk_putaway');}
  const code=document.getElementById('inboundCodeInput');code.placeholder='扫描外部系统入库单号 / 외부 입고번호 스캔';code.addEventListener('input',()=>{window._ibResolvedKind='';window._ibResolvedPlanId='';window._ibResolvedPlan=null;});
  document.querySelector('#inboundEntryCard>label').textContent='扫描外部系统入库单号 / 외부 입고번호 스캔';
  const resolve=window.resolveInboundCode;window.resolveInboundCode=async function(...args){window._ibResolvedKind='';window._ibResolvedPlanId='';window._ibResolvedPlan=null;await resolve(...args);if(window._ibResolvedKind==='system'){const p=window._ibResolvedPlan;document.getElementById('ibResolveResult').innerHTML='<div class="ck-inbound-match"><strong>本次外部入库单 / 이번 입고번호：'+esc(p.selected_external_inbound_no||'—')+'</strong><div>已关联 '+esc(p.display_no||p.id)+' · '+esc(p.customer)+'</div><div>'+esc(p.cargo_summary||'')+'</div>'+progressHtml(p)+'</div>';}};
  window.loadInboundCandidates=async function(){const sel=document.getElementById('inboundPlanSelect'),bc=_resolveBizClass();const r=await api({action:'v2_inbound_plan_ops_candidates',scene:'putaway',biz_class:bc,required_biz_class:bc});sel.innerHTML='<option value="">选择本次外部入库单 / 작업할 입고번호 선택</option>';for(const p of r?.items||[]){const refs=p.inbound_progress?.items||[{external_no:p.external_inbound_no||p.display_no||p.id,status:'pending'}];for(const ref of refs.filter(x=>x.status!=='completed')){const o=document.createElement('option');o.value=p.id+'::'+ref.external_no;o.dataset.code=ref.external_no;o.textContent=ref.external_no+' · '+p.customer+' · '+(p.display_no||p.id)+(ref.status==='working'?' · 理货中':'');sel.append(o);}}};
  window.onInboundCandidateSelect=function(){const s=document.getElementById('inboundPlanSelect');if(!s.value)return;code.value=s.selectedOptions[0].dataset.code;resolveInboundCode();};
   const planInfo=window.loadInboundPlanInfo;window.loadInboundPlanInfo=async function(...args){const rendered=await planInfo(...args);if(!rendered?.rendered||rendered.jobId!==window._activeJobId||rendered.data!==window._inboundPlanData)return rendered;const data=rendered.data,p=data.plan;const job=(data.jobs||[]).find(x=>x.id===window._activeJobId),current=job?.inbound_external_no||p.external_inbound_nos?.[0]||'';const box=document.createElement('section');box.className='ck-inbound-match';box.innerHTML='<strong>本次外部入库单 / 이번 입고번호：'+esc(current)+'</strong>'+progressHtml(p);document.getElementById('inboundPlanInfo').prepend(box);
   if(p.inbound_progress?.total>1){document.querySelectorAll('.ib-putaway-input').forEach(x=>{x.value='';x.placeholder='本单实际数';});const note=document.createElement('p');note.className='muted';note.textContent='以下填写本次外部入库单的实际数量，请勿重复填写整批数量 / 이번 입고번호의 실제 수량만 입력하세요';document.getElementById('inboundResultLines').prepend(note);}
    return rendered;
   };
 }
};
})();
