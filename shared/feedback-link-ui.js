(function(){
'use strict';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const label=u=>window.unitTypeLabel?unitTypeLabel(u):u;
const qty=n=>Number(n||0).toLocaleString('zh-CN',{maximumFractionDigits:4});
window.CKInstallFeedbackLink=function(app){
 if(app!=='002'||!window.CK_SOP_ROLLOUT?.staging)return;
 const processed=document.querySelector('#fbFilterStatus option[value="converted"]');if(processed)processed.textContent='已处理（关联／新建）';
 const office=()=>['manager','service','reviewer'].includes(window.CKSession?.user?.role)&&window.CKSession?.scope!=='field';
 const createView=document.getElementById('view-inbound_create'),customerInput=document.getElementById('ibc-customer'),customerPlaceholder=customerInput.placeholder;
 let createGeneration=0,createSource=null,detailGeneration=0;
 function clearSource(){createGeneration++;createSource=null;createView.querySelector('[data-feedback-create-source]')?.remove();customerInput.placeholder=customerPlaceholder;}
 function newForm(){clearSource();window.resetInboundCreateForm();if(window._createSubmissionIntents)delete window._createSubmissionIntents.v2_inbound_plan_create;}
 const currentCreate=snapshot=>snapshot.generation===createGeneration&&createView.style.display!=='none';
 window.CKInboundCreate={
  capture(){return {generation:createGeneration,references:window.CKInboundCreateReferences?.()||{},feedback_context:createSource?{feedback_id:createSource.feedback_id,feedback_version:createSource.feedback_version,job_version:createSource.job_version}:null};},
  isCurrent:currentCreate,
  complete(snapshot,id){if(!currentCreate(snapshot))return;const linked=!!snapshot.feedback_context;newForm();if(linked)openInboundDetail(id);else goTab('inbound');}
 };
 const previousView=window.goView;window.goView=function(name,...args){if(name==='inbound_create')newForm();else if(createView.style.display!=='none')clearSource();return previousView(name,...args);};
 const cancelCreate=()=>{const id=createSource?.feedback_id;newForm();if(id)openFeedbackDetail(id);else goTab('inbound');};
 createView.querySelector('.nav-back').onclick=cancelCreate;
 async function createFromFeedback(fb,button,canApply,error){
  if(button.disabled||!office())return;button.disabled=true;error.textContent='正在读取原卸货记录 / 원래 하차 기록 확인 중…';
  try{
   const r=await CKSession.request('sop_feedback_link_create_preview',{feedback_id:fb.id});if(!canApply())return;
   if(r.existing_link){openInboundDetail(r.existing_link.plan_id);return;}
   const source=r.source,units=['container_large','container_small','pallet','carton','cbm'];
   if(!source||source.feedback_id!==fb.id||!source.feedback_version||!source.job_version||!Array.isArray(source.actual_totals)||!source.actual_totals.some(x=>Number(x.actual_qty)>0)||source.actual_totals.some(x=>!units.includes(x.unit_type)||!Number.isFinite(x.actual_qty)||x.actual_qty<0))throw Error('原卸货数量或单位需先核实 / 원래 수량·단위를 먼저 확인하세요');
   goView('inbound_create');createSource=source;
   const tbody=document.getElementById('ibcLinesBody');tbody.replaceChildren();
   for(const line of source.actual_totals){addIbcLine();const row=tbody.lastElementChild;row.querySelector('.ibc-line-type').value=line.unit_type;row.querySelector('.ibc-line-qty').value=line.actual_qty;}
   document.getElementById('ibc-cargo').value=source.cargo_summary||source.actual_totals.map(x=>label(x.unit_type)+' '+qty(x.actual_qty)).join(' / ');
   document.getElementById('ibc-remark').value=[source.content,source.remark].filter((value,index,list)=>value&&list.indexOf(value)===index).join('\n');
   customerInput.placeholder='请核实并填写客户 / 고객을 확인하고 입력하세요';
   const banner=document.createElement('section');banner.className='ck-feedback-create-source';banner.dataset.feedbackCreateSource='';
   banner.innerHTML='<div><strong>来自计划外卸货 / 현장 하차에서 생성</strong><b>'+esc(source.display_no||fb.display_no||fb.id)+'</b></div><p>原卸货实收 / 기존 실제 하차: '+esc(source.actual_totals.map(x=>label(x.unit_type)+' '+qty(x.actual_qty)).join(' · '))+'</p>'+(source.completed_at?'<p>原完成时间 / 기존 완료 시간: '+esc(window.fmtTime?fmtTime(source.completed_at):source.completed_at)+'</p>':'')+'<p>请核实客户并选择业务分类。原客户／唛头等文字保留在备注；原卸货人员、工时、实收和照片随保存关联。 / 고객·입고 유형을 확인하세요. 고객·마크 원문은 비고에 보존하며 기존 작업 기록과 사진을 연결합니다.</p><button type="button" class="btn btn-outline" data-feedback-create-cancel>取消建单，返回原卸货 / 취소·원래 하차로</button>';
   banner.querySelector('button').onclick=cancelCreate;createView.querySelector('.card').prepend(banner);customerInput.focus();
  }catch(e){if(canApply())error.textContent=e.message;}
  finally{if(canApply())button.disabled=false;}
 }
 let feedback=null,planData=null,feedbackItems=[];
 const apiOriginal=window.api;window.api=async function(body){const r=await apiOriginal(body);if(r?.ok&&body.action==='v2_feedback_detail'&&body.id===window._currentFeedbackId)feedback=r;if(r?.ok&&body.action==='v2_inbound_plan_detail')planData=r;if(r?.ok&&body.action==='v2_feedback_list')feedbackItems=r.items||[];return r;};
 const listOriginal=window.loadFeedbackList;window.loadFeedbackList=async function(...args){await listOriginal(...args);const host=document.getElementById('feedbackListBody');if(!host)return;for(const row of host.querySelectorAll('.list-item')){const f=feedbackItems.find(f=>row.getAttribute('onclick')?.includes("'"+f.id+"'"));if(f?.linked_plan_no){row.querySelector('.st').textContent='已关联 / 연결 완료';const note=document.createElement('span');note.textContent=' · '+f.linked_plan_no;row.querySelector('.item-meta').append(note);}}};
 const detail=window.loadFeedbackDetail;window.loadFeedbackDetail=async function(...args){const detailToken=++detailGeneration;await detail(...args);const data=feedback,fb=data?.feedback,host=document.getElementById('feedbackDetailBody');if(detailToken!==detailGeneration||!host||!fb||fb.id!==window._currentFeedbackId||document.getElementById('view-feedback_detail').style.display==='none')return;
  const conversion=document.getElementById('fb-conv-customer')?.closest('.card');conversion?.remove();
  const link=data.existing_plan_link||(fb.inbound_plan_id?{plan_id:fb.inbound_plan_id,display_no:fb.inbound_plan_id}:null);
  if(link){
   const panel=document.createElement('section');panel.className='card ck-feedback-link';panel.innerHTML='<strong>已关联入库计划 / 입고계획 연결 완료</strong><p>'+esc(link.display_no)+(link.customer?' · '+esc(link.customer):'')+'</p>'+(link.linked_at?'<p class="muted">'+esc(link.linked_name)+' · '+esc(window.fmtTime?fmtTime(link.linked_at):link.linked_at)+'</p>':'')+'<button type="button" class="btn btn-primary" data-feedback-create-open>查看入库计划 / 입고계획 보기</button>';panel.querySelector('button').onclick=()=>openInboundDetail(link.plan_id);host.prepend(panel);
   for(const b of host.querySelectorAll('.detail-field b'))if(b.textContent==='已转正入库计划:')b.textContent='已关联入库计划:';
   const status=host.querySelector('.st-converted');if(status)status.textContent='已关联 / 연결 완료';
   return;
  }
  if(!office()||!['unplanned_unload','unload_no_doc'].includes(fb.feedback_type)||!['unloaded_pending_info','open'].includes(fb.status)||fb.inbound_plan_id)return;
  const choices=document.createElement('section');choices.className='card ck-feedback-choices';choices.innerHTML='<h3>补齐入库计划 / 입고계획 연결</h3><p>尚未建单可新建；已有计划请核对后关联。 / 계획이 없으면 새로 만들고, 있으면 기존 계획을 연결하세요.</p><div><button type="button" class="btn btn-primary" data-feedback-create>新建入库计划 / 입고계획 새로 만들기</button><button type="button" class="btn btn-outline" data-feedback-existing>关联已有入库计划 / 기존 입고계획 연결</button></div><p role="status" aria-live="polite"></p>';
  const panel=document.createElement('section');panel.className='card ck-feedback-link';panel.innerHTML='<h3>关联已有入库计划 / 기존 입고계획 연결</h3><p class="muted">已卸完、现场未识别计划时，在这里补关联。原卸货人员、工时和产出一起保留。<br>하차 완료 후 기존 계획을 연결합니다. 작업 기록은 그대로 유지됩니다.</p><form class="ck-feedback-search"><label for="ck-feedback-keyword">查找计划 / 계획 검색</label><div><input id="ck-feedback-keyword" autocomplete="off" placeholder="客户、计划号或外部入库单号 / 고객·입고번호"><button type="submit" class="btn btn-outline">查找 / 검색</button></div></form><p class="ck-feedback-message" role="status" aria-live="polite"></p><div class="ck-feedback-candidates"></div><div class="ck-feedback-preview"></div>';
  host.firstElementChild.after(choices,panel);
  const form=panel.querySelector('form'),input=form.querySelector('input'),message=panel.querySelector('[role=status]'),list=panel.querySelector('.ck-feedback-candidates'),preview=panel.querySelector('.ck-feedback-preview');let generation=0,selected=null,timer;
  const current=()=>panel.isConnected&&window._currentFeedbackId===fb.id&&document.getElementById('view-feedback_detail').style.display!=='none';
  choices.querySelector('[data-feedback-create]').onclick=e=>createFromFeedback(fb,e.currentTarget,current,choices.querySelector('[role=status]'));
  choices.querySelector('[data-feedback-existing]').onclick=()=>{panel.scrollIntoView({behavior:'smooth',block:'start'});input.focus();};
  async function choose(id){const token=++generation;message.textContent='正在核对明细 / 확인 중…';preview.replaceChildren();selected=null;try{const r=await CKSession.request('sop_feedback_link_preview',{feedback_id:fb.id,plan_id:id});if(!current()||token!==generation)return;selected=r;message.textContent='';renderPreview();}catch(e){if(current()&&token===generation)message.textContent=e.message;}}
  async function search(offset=0){const token=++generation;selected=null;preview.replaceChildren();message.textContent='正在查找 / 검색 중…';list.replaceChildren();try{
   const r=await CKSession.request('sop_feedback_link_candidates',{feedback_id:fb.id,keyword:input.value,offset});if(!current()||token!==generation)return;
   message.textContent=r.items.length?'仅显示尚未卸货的计划 / 하차 전 계획만 표시됩니다':'没有找到可关联计划。可换客户名或单号查找；已卸货的计划不能重复关联。 / 연결 가능한 계획이 없습니다.';
   for(const p of r.items){const row=document.createElement('button');row.type='button';row.className='ck-feedback-candidate';row.innerHTML='<span><strong>'+esc(p.display_no||p.id)+'</strong><b>'+esc(p.customer)+'</b><small>'+esc(p.expected_arrival||'到达日期未填 / 예정일 미등록')+'</small></span><span>'+esc(p.lines.map(l=>label(l.unit_type)+' '+qty(l.planned_qty)).join(' · ')||p.cargo_summary||'—')+'</span><span class="muted">核对 / 확인</span>';row.onclick=()=>choose(p.id);list.append(row);}
   if(offset||r.has_more){const nav=document.createElement('div');nav.className='ck-feedback-pages';for(const [text,target,enabled] of [['上一页 / 이전',offset-20,offset>0],['下一页 / 다음',offset+20,r.has_more]]){const b=document.createElement('button');b.type='button';b.className='btn btn-outline btn-sm';b.textContent=text;b.disabled=!enabled;b.onclick=()=>search(target);nav.append(b);}list.append(nav);}
  }catch(e){if(current()&&token===generation)message.textContent=e.message;}}
  function renderPreview(){const r=selected,p=r.plan;preview.innerHTML='<div class="ck-feedback-selected"><strong>'+esc(p.customer)+' · '+esc(p.display_no)+'</strong><button type="button" class="btn btn-outline btn-sm">收起 / 닫기</button></div><p>本次卸货实收 / 실제 하차: '+esc(r.actual_totals.map(l=>label(l.unit_type)+' '+qty(l.actual_qty)).join(' · '))+'</p><div class="ck-feedback-table"><table class="line-table"><thead><tr><th>类型 / 유형</th><th>原计划 / 계획</th><th>本次实收 / 실제</th></tr></thead><tbody>'+r.lines.map((l,i)=>'<tr><td>'+esc(label(l.unit_type))+(l.id?'':' <small>新增实收行</small>')+'</td><td>'+qty(l.planned_qty)+'</td><td><input aria-label="'+esc(label(l.unit_type))+' 本次实收 '+(i+1)+'" type="number" min="0" max="100000000" step="any" data-row="'+i+'" value="'+(l.actual_qty===null?'':esc(l.actual_qty))+'"'+(l.actual_qty===null?'':' readonly')+' required></td></tr>').join('')+'</tbody></table></div><p class="muted">'+(r.lines.some(l=>l.actual_qty===null)?'同类型有多行，请分配原实收数量；合计保持不变。<br>동일 유형의 행별 실제 수량을 배분하세요.<br>':'')+(r.needs_putaway?'关联后仍需逐单完成理货上架 / 연결 후 외부 입고번호별 검수·적치가 필요합니다':'关联后按原计划规则更新入库状态 / 기존 계획에 따라 입고 상태를 반영합니다')+'</p><label class="ck-feedback-confirm"><input type="checkbox">确认是同一批货 / 같은 화물임을 확인했습니다</label><button type="button" class="btn btn-primary ck-feedback-save">确认关联 / 연결 확정</button><p role="alert"></p>';
   preview.querySelector('.ck-feedback-selected button').onclick=()=>{++generation;selected=null;preview.replaceChildren();};
   const save=preview.querySelector('.ck-feedback-save'),error=preview.querySelector('[role=alert]');save.onclick=async()=>{
    if(!preview.querySelector('[type=checkbox]').checked){error.textContent='请先核对客户、单号及数量，并确认是同一批货 / 동일 화물인지 확인하세요';return;}
    const values=[...preview.querySelectorAll('[data-row]')];if(values.some(el=>!el.reportValidity()))return;
    save.disabled=true;error.textContent='关联中 / 연결 중…';
    const controls=[...panel.querySelectorAll('button,input')];controls.forEach(el=>el.disabled=true);
    try{await CKSession.request('sop_feedback_link_save',{feedback_id:fb.id,plan_id:p.id,feedback_version:r.feedback_version,plan_version:r.plan_version,job_version:r.job_version,lines:r.lines.map((l,i)=>({line_id:l.id,unit_type:l.unit_type,actual_qty:Number(values[i].value)}))});if(current())await window.loadFeedbackDetail();}
    catch(e){if(current()){error.textContent=e.message;controls.forEach(el=>el.disabled=false);}}
   };
  }
  form.onsubmit=e=>{e.preventDefault();clearTimeout(timer);search();};input.oninput=()=>{clearTimeout(timer);timer=setTimeout(()=>{if(current())search();},300);};await search();
 };
 const inbound=window.loadInboundDetail;window.loadInboundDetail=async function(...args){await inbound(...args);const link=planData?.existing_feedback_link,host=document.getElementById('inboundDetailBody');if(!link||!host||link.plan_id!==window._currentInboundId)return;const row=document.createElement('section');row.className='ck-feedback-origin';row.innerHTML='<span>原计划外卸货 / 현장 하차: <strong>'+esc(link.feedback_no||link.feedback_id)+'</strong> · '+esc(link.linked_name)+'</span><button type="button" class="btn btn-outline btn-sm">查看现场反馈 / 피드백 보기</button>';row.querySelector('button').onclick=()=>openFeedbackDetail(link.feedback_id);host.prepend(row);};
};
})();
