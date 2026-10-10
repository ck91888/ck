(function(){
'use strict';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const label=u=>window.unitTypeLabel?unitTypeLabel(u):u;
const qty=n=>Number(n||0).toLocaleString('zh-CN',{maximumFractionDigits:4});
window.CKInstallFeedbackLink=function(app){
 if(app!=='002'||!window.CK_SOP_ROLLOUT?.staging)return;
 const processed=document.querySelector('#fbFilterStatus option[value="converted"]');if(processed)processed.textContent='已处理（关联／新建）';
 let feedback=null,planData=null,feedbackItems=[];
 const apiOriginal=window.api;window.api=async function(body){const r=await apiOriginal(body);if(r?.ok&&body.action==='v2_feedback_detail')feedback=r;if(r?.ok&&body.action==='v2_inbound_plan_detail')planData=r;if(r?.ok&&body.action==='v2_feedback_list')feedbackItems=r.items||[];return r;};
 const listOriginal=window.loadFeedbackList;window.loadFeedbackList=async function(...args){await listOriginal(...args);const host=document.getElementById('feedbackListBody');if(!host)return;for(const row of host.querySelectorAll('.list-item')){const f=feedbackItems.find(f=>row.getAttribute('onclick')?.includes("'"+f.id+"'"));if(f?.linked_plan_no){row.querySelector('.st').textContent='已关联 / 연결 완료';const note=document.createElement('span');note.textContent=' · '+f.linked_plan_no;row.querySelector('.item-meta').append(note);}}};
 const detail=window.loadFeedbackDetail;window.loadFeedbackDetail=async function(...args){await detail(...args);const data=feedback,fb=data?.feedback,host=document.getElementById('feedbackDetailBody');if(!host||!fb||fb.id!==window._currentFeedbackId)return;
  const link=data.existing_plan_link;
  if(link){
   const panel=document.createElement('section');panel.className='card ck-feedback-link';panel.innerHTML='<strong>已关联已有入库计划 / 기존 입고계획 연결 완료</strong><p>'+esc(link.display_no)+' · '+esc(link.customer)+'</p><p class="muted">'+esc(link.linked_name)+' · '+esc(window.fmtTime?fmtTime(link.linked_at):link.linked_at)+'</p><button type="button" class="btn btn-primary">查看入库计划 / 입고계획 보기</button>';panel.querySelector('button').onclick=()=>openInboundDetail(link.plan_id);host.prepend(panel);
   for(const b of host.querySelectorAll('.detail-field b'))if(b.textContent==='已转正入库计划:')b.textContent='已关联入库计划:';
   const status=host.querySelector('.st-converted');if(status)status.textContent='已关联 / 연결 완료';
   return;
  }
  if(!['unplanned_unload','unload_no_doc'].includes(fb.feedback_type)||!['unloaded_pending_info','open'].includes(fb.status)||fb.inbound_plan_id)return;
  // Existing plans are the first choice; creating a genuinely missing plan remains available.
  const conversion=document.getElementById('fb-conv-customer')?.closest('.card');if(conversion){const fold=document.createElement('details');fold.className='ck-feedback-new';fold.innerHTML='<summary>确实没有计划？补充信息并新建 / 기존 계획이 없으면 신규 등록</summary>';conversion.before(fold);fold.append(conversion);}
  const panel=document.createElement('section');panel.className='card ck-feedback-link';panel.innerHTML='<h3>关联已有入库计划 / 기존 입고계획 연결</h3><p class="muted">已卸完、现场未识别计划时，在这里补关联。原卸货人员、工时和产出一起保留。<br>하차 완료 후 기존 계획을 연결합니다. 작업 기록은 그대로 유지됩니다.</p><form class="ck-feedback-search"><label for="ck-feedback-keyword">查找计划 / 계획 검색</label><div><input id="ck-feedback-keyword" autocomplete="off" placeholder="客户、计划号或外部入库单号 / 고객·입고번호"><button type="submit" class="btn btn-outline">查找 / 검색</button></div></form><p class="ck-feedback-message" role="status" aria-live="polite"></p><div class="ck-feedback-candidates"></div><div class="ck-feedback-preview"></div>';
  host.firstElementChild.after(panel);
  const form=panel.querySelector('form'),input=form.querySelector('input'),message=panel.querySelector('[role=status]'),list=panel.querySelector('.ck-feedback-candidates'),preview=panel.querySelector('.ck-feedback-preview');let generation=0,selected=null,timer;
  const current=()=>panel.isConnected&&window._currentFeedbackId===fb.id;
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
