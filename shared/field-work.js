/* Work plans and external work orders share one field workspace. */
(function(){
 'use strict';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const states={pending:'待派工 / 배정 대기',assigned:'已安排 / 배정 완료',working:'作业中 / 작업 중',paused:'已暂停 / 일시 중지',rework:'待整改 / 재작업',awaiting_close:'已暂停 · 待收尾 / 일시 중지·마감 대기',awaiting_review:'待审核 / 검수 대기',completed:'已完成 / 완료',waiting_customer:'等待客户安排',linked:'成果已关联出库',cancelled:'已取消'};
 const counts=[['packed_sku_count','品数 / 품목수'],['packed_count','打包箱数 / 포장박스'],['used_carton_large_count','大纸箱 / 대형박스'],['used_carton_small_count','小纸箱 / 소형박스'],['repaired_box_count','修补箱数 / 수리박스'],['reboxed_count','换箱数 / 교체박스'],['label_count','标签数 / 라벨수'],['operated_box_count','操作总箱数 / 총 박스'],['pallet_count','打托数 / 팔레트'],['forklift_location_count','叉车货位数 / 지게차 위치']];
 window.CKFieldWork=function(root,initial='',options={}){
  root.className='ck-bulk';
  const external=!!options.external,api=(action,data={})=>CKSession.request(action,data),$=name=>root.querySelector('[data-'+name+']');
  let picker=null,scan=null,current=null,closed=false,loadSequence=0,requestId='',requestSignature='';
  async function cleanup(){if(picker){await picker.destroy();picker=null;}if(scan){const old=scan;scan=null;await old.stop().catch(()=>{});}}
  function error(x){if(!closed&&$('error')){$('error').textContent=x.message;$('error').hidden=false;}}
  async function write(action,data){
   const signature=JSON.stringify({action,data});
   if(signature!==requestSignature||!requestId){requestSignature=signature;requestId=crypto.randomUUID();}
   const body={...data,client_req_id:requestId};
   if(action==='sop_native_start')body.payload={...data.payload,client_req_id:requestId};
   try{const r=await api(action,body);requestId='';return r;}catch(x){if(x.businessError)requestId='';throw x;}
  }
  function nativeState(r){
   if(!r.can_manage_dispatch||!r.dispatch||r.job?.job_type!=='bulk_op')throw Error('无法操作此任务，请联系原派审员 / 담당자에게 문의하세요');
   const j=r.job,d=JSON.parse(r.dispatch.state),live=r.workers.filter(w=>!w.left_at),saved=r.results[0];
   let result=saved?JSON.parse(saved.result_json||'{}'):null;
   if(result)result={...result,packed_count:result.packed_box_count,operated_box_count:result.total_operated_box_count,description:result.description||saved.remark||''};
   return {external:true,native:r,need:{display_no:j.business_no||j.display_no||j.related_doc_id,title:'外部作业 / 외부 작업',customer:j.customer||'',instructions:'按纸质作业单核对本次操作要求。 / 인쇄된 작업서를 확인하세요.'},
    source:j.outbound_plan_no?{number:j.outbound_plan_no}:null,segments:r.workers,
    task:{id:j.id,revision:r.dispatch.revision,status:j.status,lead_id:d.lead_id,workers:live.map(w=>({id:w.worker_id,name:w.worker_name})),owner:d.owner,started_at:j.created_at,result,location:''},last_lead:d.last_lead||d.workers.find(w=>w.id===d.lead_id)};
  }
  async function resolve(code,detail){
   if(closed)return;
   const sequence=++loadSequence;await cleanup();if(closed||sequence!==loadSequence)return;
   let next;
   if(!external)next=await api('sop_field_resolve',{code});
   else{
    code=String(code||'').trim();const invalid=window.CKDocumentCode?.documentCodeError(code);if(invalid)throw Error(invalid);
    if(/^(CKWORK\||NEED-|SOPJOB-|(?:ZY|RW)-\d{8}-\d+)/i.test(code))throw Error('这是作业计划单，请从需求作业单入口打开 / 작업 계획서 탭을 사용하세요');
    if(detail)next=nativeState(detail);
    else if(code===current?.task?.id)next=nativeState(await api('v2_ops_job_detail',{job_id:code}));
    else{
     const r=await api('sop_dispatch_list',{external_code:code});
     next=r.items.length?nativeState(await api('v2_ops_job_detail',{job_id:r.items[0].id})):{external:true,need:{display_no:code,title:'外部作业 / 외부 작업',customer:'',instructions:'按纸质作业单核对本次操作要求。 / 인쇄된 작업서를 확인하세요.'},task:null,segments:[]};
    }
   }
   if(closed||sequence!==loadSequence)return;current=next;options.onChange?.(next.task);draw();
  }
  async function home(){
   const sequence=++loadSequence;await cleanup();if(closed||sequence!==loadSequence)return;
   current=null;options.onChange?.(null);
   root.innerHTML=`<section class="ck-scan"><span class="ck-step">01 / SCAN WORK ORDER</span><h2>${external?'扫描外部作业单':'扫描作业计划单'}</h2><p>${external?'외부 작업서를 스캔하세요.':'작업 계획서를 스캔하세요.'}</p><form data-scanform><label for="ck-work-code">作业单号 / 작업 번호</label><div class="ck-scan-row"><input id="ck-work-code" data-code autocomplete="off" placeholder="${external?'扫描或输入外部作业单号':'扫码枪扫描或输入编号'}" required><button class="ck-primary" style="width:auto;margin:0" type="submit">打开</button></div></form><div class="ck-actions" style="margin-top:14px"><button data-camera class="ck-small">相机扫码 / 카메라</button></div><div id="ck-work-camera"></div><p data-error class="ck-error" role="alert" hidden></p></section><section><div class="ck-section-heading"><h3>进行中的作业 / 진행 중</h3><button class="ck-small" data-refresh>刷新</button></div><div data-tasks>正在读取…</div></section>`;
   $('scanform').onsubmit=async ev=>{ev.preventDefault();const b=ev.submitter||ev.target.querySelector('button[type=submit]');if(b?.disabled)return;if(b)b.disabled=true;try{await resolve($('code').value.trim());}catch(x){error(x);}finally{if(b)b.disabled=false;}};
   $('refresh').onclick=loadTasks;
   $('camera').onclick=async()=>{
    if(scan){await cleanup();$('camera').textContent='相机扫码 / 카메라';return;}
    try{scan=new Html5Qrcode('ck-work-camera');let reading=false;await scan.start({facingMode:'environment'},{fps:8,qrbox:220},async code=>{if(reading)return;reading=true;try{await resolve(code);}catch(x){error(x);reading=false;}},()=>{});$('camera').textContent='停止扫码 / 스캔 중지';}
    catch(x){error(Error('无法打开相机，请使用扫码枪或输入编号 / 카메라 사용 불가'));}
   };
   $('code').focus();await loadTasks();
  }
  async function loadTasks(){
   const sequence=loadSequence,host=$('tasks');if(!host)return;
   try{
    let items=[];
    if(external){const r=await api('sop_dispatch_list');items=r.items.filter(t=>t.task_kind==='dispatch'&&t.job_type==='bulk_op').map(t=>({...t,display_no:t.business_no,title:t.title||'外部作业 / 외부 작업'}));}
    else{let offset=0,more=true;while(more){const r=await api('sop_list',{kind:'task',offset});items.push(...r.items);more=r.more;offset+=50;if(offset>=1000)break;}items=items.filter(t=>t.department==='bulk'&&!['completed','cancelled'].includes(t.status));}
    if(closed||sequence!==loadSequence||$('tasks')!==host)return;
    host.innerHTML=items.map(t=>`<button class="ck-task-link" data-task="${esc(t.id)}"><b class="ck-task-number">${esc(t.work_plan_no||t.display_no||'单号待补充')}</b><span class="ck-task-customer">客户 / 고객：${esc(t.customer||'—')}</span><span class="ck-task-title">${esc(t.title)}</span><small>${states[t.status]||esc(t.status)} · ${esc((t.workers||[]).map(w=>w.name).join('、'))}</small></button>`).join('')||'<p class="ck-muted">暂无进行中的作业 / 진행 중인 작업 없음</p>';
    host.querySelectorAll('[data-task]').forEach(b=>b.onclick=()=>resolve(b.dataset.task).catch(error));
   }catch(x){if(!closed&&sequence===loadSequence&&$('tasks')===host)host.textContent=x.message;}
  }
  function draw(){
   const {need:n,task:t,source,segments,changed}=current,title=n?.title||t?.title||'',state=t?.status||n?.status||'pending',live=segments.filter(s=>!s.left_at);
   root.innerHTML=`<div class="ck-actions"><button data-back class="ck-small">← 扫描其他作业单 / 다른 작업</button><button data-refresh class="ck-small">刷新 / 새로고침</button><button data-home class="ck-small">返回首页，任务继续 / 작업 유지·홈으로</button></div><section class="ck-work-sheet"><span class="ck-state">${states[state]||esc(state)}</span><p class="ck-work-number">作业单号 / 작업 번호：<b>${esc(n?.display_no||t?.work_plan_no||t?.display_no||'单号待补充')}</b></p><h2>${esc(title)}</h2><div class="ck-work-meta"><div>客户 / 고객<b>${esc(n?.customer||source?.customer||'—')}</b></div><div>货物来源 / 화물 출처<b>${esc(source?.number||(external?'外部作业单 / 외부 작업서':window.CKDocumentLabels.source(n)))}</b></div><div>货物范围 / 화물 범위<b>${esc(n?.scope_text||'见作业要求')}</b></div><div>作业位置 / 작업 위치<b>${esc(t?.location||n?.location||'待确认')}</b></div></div><div class="ck-instructions">${esc(n?.instructions||title)}</div>${changed?'<p class="ck-warning">纸单版本已更新，请按本页最新要求核对后派工。</p>':''}</section><div data-content></div><p data-error class="ck-error" role="alert" hidden></p>`;
   $('back').onclick=()=>home().catch(error);$('refresh').onclick=()=>resolve(t?.id||n.id||n.display_no).catch(error);$('home').onclick=()=>window.goPage('home');
   if(external&&!t){
    people('派工并开始 / 배정 및 시작',[],async staff=>{
     const r=await write('sop_native_start',{payload:{action:'v2_bulk_op_job_start',work_order_no:n.display_no,customer:$('people').elements.customer.value.trim()},...staff,labor_department:'bulk',estimated_minutes:Number($('people').elements.minutes.value)});
     await resolve(r.job_id);
    },false,'<label>客户 / 고객<input name="customer" required></label><label>预计操作分钟 / 예상 작업시간<input name="minutes" type="number" min="1" step="1" value="30" required></label>');return;
   }
   if(!t&&n?.operation_kind==='direct_forward'&&n.status==='pending'){
    $('content').innerHTML='<section class="chain-forward"><h3>直接转发 / 작업 없이 전달</h3><p>收货后确认可发货数量，不记录加工工时。</p><form data-forward><label>可发货数量 / 출고 가능 수량<input data-forward-quantity type="number" min="1" step="1" required value="'+esc(n.planned_quantity)+'"></label><button>确认可发货 / 출고 준비 확인</button></form></section>';
    $('forward').onsubmit=async ev=>{ev.preventDefault();const submit=ev.submitter||ev.target.querySelector('button[type=submit],button:not([type])');if(submit.disabled)return;submit.disabled=true;try{await write('sop_need_forward_ready',{id:n.id,revision:n.revision,quantity:Number($('forward-quantity').value)});await resolve(n.id);}catch(x){error(x);submit.disabled=false;}};return;
   }
   if(!t&&n?.status==='pending'){people('派工并开始 / 배정 및 시작',[],async staff=>{const r=await write('sop_task_dispatch',{department:n.department,title:n.title,need_id:n.id,job_type:'bulk_op',estimated_minutes:30,location:n.location||'',...staff});await resolve(r.id);});return;}
   if(!t){$('content').innerHTML='<p class="ck-muted">此作业已完成或暂不能派工，请联系订单处理组核对。</p>';return;}
   $('content').innerHTML=`<section class="ck-work-sheet"><span class="ck-step">02 / PEOPLE & EXECUTION</span><h3>参与人员 / 참여 인원</h3><p>${live.length?live.map(w=>esc(w.worker_name)).join(' · '):'当前无人计时 / 현재 작업 인원 없음'}</p><p class="ck-muted">${t.started_at?'开始 '+CKAttendance.at(t.started_at):'人员到位后开始记录工时'} · 负责人 ${esc(t.owner)}</p><div class="ck-actions" data-actions></div><div data-editor></div></section>`;
   const action=(label,fn)=>{const b=document.createElement('button');b.className='ck-small';b.textContent=label;b.onclick=async()=>{b.disabled=true;try{await fn();}catch(x){error(x);}finally{b.disabled=false;}};$('actions').append(b);};
   if(external&&['pending','working','awaiting_close'].includes(t.status)){
    action(live.length?'调整人员 / 인원 변경':'核对人员并开始 / 인원 확인·시작',()=>people('保存人员 / 인원 저장',t.workers,async staff=>{await write('sop_native_people',{job_id:t.id,revision:t.revision,...staff,reason:$('reason').value});await resolve(t.id);},true));
    action('登记休息 / 휴식 등록',()=>window.CKOpenFieldLabor?.());
    if(live.length)action('暂停作业 / 작업 중지',()=>pause(t));
    action('填写产出并审核结束 / 산출·검수 완료',()=>finish(t));return;
   }
   if(['assigned','paused','rework'].includes(t.status))action('核对人员并开始 / 인원 확인·시작',()=>people('开始作业 / 작업 시작',t.workers,async staff=>{let revision=t.revision;if(JSON.stringify(staff.workers)!==JSON.stringify(t.workers)||staff.lead_id!==t.lead_id){const r=await write('sop_task_people',{id:t.id,revision,...staff,reason:'开工前核对到位人员'});revision=r.revision;}await write('sop_task_start',{id:t.id,revision});await resolve(t.id);}));
   if(t.status==='working'){
    action('调整人员 / 인원 변경',()=>people('保存人员 / 인원 저장',live.map(w=>({id:w.worker_id,name:w.worker_name})),async staff=>{await write('sop_task_people',{id:t.id,revision:t.revision,...staff,reason:$('reason').value});await resolve(t.id);},true));
    action('登记休息 / 휴식 등록',()=>window.CKOpenFieldLabor?.());action('暂停作业 / 작업 중지',()=>pause(t));action('填写产出并审核结束 / 산출·검수 완료',()=>finish(t));
   }else if(t.status==='awaiting_review')action('审核原有产出 / 검수',()=>{
    $('editor').innerHTML=`${CKResultSummary(t.result)}${t.result?.location?`<p>${esc(t.result.location)}</p>`:''}${CKResultPhotos(t.result)}<form data-review><label>审核结论<select name="decision"><option value="pass">通过 / 통과</option><option value="return">退回整改 / 재작업</option></select></label><label>审核说明<textarea name="reason" required></textarea></label><button class="ck-primary">保存审核 / 검수 저장</button></form>`;
    $('review').onsubmit=async ev=>{ev.preventDefault();const submit=ev.submitter||ev.target.querySelector('button[type=submit],button:not([type])');if(submit.disabled)return;submit.disabled=true;try{await write('sop_task_review',{id:t.id,revision:t.revision,...Object.fromEntries(new FormData(ev.target))});await resolve(t.id);}catch(x){error(x);submit.disabled=false;}};
   });
   else if(t.status==='completed')$('editor').innerHTML=`<h3>已审核完成 / 검수 완료</h3>${CKResultSummary(t.result)}${CKResultPhotos(t.result)}`;
  }
  function people(label,workers,save,adjust=false,fields=''){
   if(picker)picker.destroy();const host=$('editor')||$('content');
   host.innerHTML=`<form data-people class="ck-work-sheet"><span class="ck-step">02 / SCAN STAFF</span><h3>扫描操作员工牌 / 작업자 명찰</h3>${fields}${CKPeopleFields()}${adjust?'<label>调整原因 / 변경 사유<input data-reason required></label>':''}<button class="ck-primary" type="submit">${label}</button></form>`;
   picker=CKPeoplePicker($('people'),{workers,leadId:current.task?.lead_id,error:$('error'),allowEmpty:external&&adjust});
   $('people').onsubmit=async ev=>{ev.preventDefault();const submit=ev.submitter||ev.target.querySelector('button[type=submit],button:not([type])');if(submit.disabled)return;submit.disabled=true;try{await save(picker.read());}catch(x){error(x);submit.disabled=false;}};
  }
  function pause(t){
   if(picker){picker.destroy();picker=null;}
   $('editor').innerHTML='<form data-pause><label>暂停原因 / 중지 사유<textarea name="reason" required></textarea></label><button class="ck-primary">确认暂停 / 중지</button></form>';
   $('pause').onsubmit=async ev=>{ev.preventDefault();const submit=ev.submitter||ev.target.querySelector('button[type=submit],button:not([type])');if(submit.disabled)return;submit.disabled=true;try{const reason=new FormData(ev.target).get('reason');await write(external?'sop_native_people':'sop_task_pause',external?{job_id:t.id,revision:t.revision,workers:[],lead_id:'',reason}:{id:t.id,revision:t.revision,reason});await resolve(t.id);}catch(x){error(x);submit.disabled=false;}};
  }
  function finish(t){
   if(picker){picker.destroy();picker=null;}
   const n=current.need,shipping=current.shipping,unit=shipping?.unit||n?.shipping_basis?.unit||n?.planned_unit||n?.unit||'箱',booked=shipping?.links?.filter(l=>l.phase==='planned')||[],linked=external&&!!current.native.job.linked_outbound_order_id,leadId=t.lead_id||current.last_lead?.id;
   const planned=external?'':`<div class="ck-instructions"><p>待操作货量 / 작업 대상: ${esc(n?.planned_quantity||'—')} ${esc(n?.planned_unit||'')}</p><p>预计可出库成果 / 예상 출고 산출: ${esc(shipping?.quantity||n?.planned_quantity||'—')} ${esc(unit)}</p>${booked.map(l=>`<p>已预约出库 / 출고 예약: ${esc(shipping.orders.find(o=>o.id===l.outbound_id)?.display_no||l.outbound_id)} · <b>${esc(l.quantity)} ${esc(l.unit)}</b></p>`).join('')}<p>按实际完成的成果填写。打托后按托记录成果，操作总箱数填写实际处理的箱数。 / 실제 산출을 입력하세요.</p></div><div class="ck-counter-grid"><label>实际成果数量 / 실제 완료 수량<input type="number" name="quantity" min="1" step="1" required></label><label>成果单位 / 단위<select name="unit">${['箱','件','托','单','批'].map(u=>`<option ${u===unit?'selected':''}>${u}</option>`).join('')}</select></label></div>`;
   $('editor').innerHTML=`<form data-finish><h3>审核并记录本次产出 / 검수·산출</h3><p class="ck-muted">核对现场实际完成内容后填写。本次产出整单记录一次，个人工时按参与时段计算。</p>${external?`<label>客户 / 고객<input name="customer" value="${esc(n.customer)}" required ${linked?'readonly':''}></label>`:''}${planned}<details open><summary>操作产出与耗材 / 작업 산출·자재</summary><div class="ck-counter-grid">${counts.map(([key,label])=>`<label>${label}<input name="${key}" type="number" min="0" step="1" value="0"></label>`).join('')}</div></details><label style="display:flex;align-items:center;gap:10px"><input name="used_forklift" type="checkbox" style="width:auto;margin:0">使用叉车 / 지게차 사용</label><label>附加操作（选填）/ 추가 작업 (선택)<textarea name="description" placeholder="产出之外的补充操作 / 추가 작업 내용"></textarea></label><div data-location-photos></div><label>审核说明 / 검수 내용<input name="reason" placeholder="核对数量、包装及货位后的结论" required></label><button class="ck-primary">审核通过并结束任务 / 검수 통과·종료</button></form>`;
   const photos=CKLocationPhotoPicker($('location-photos'),t,{type:external?'ops_job':'sop_task'});
   $('finish').onsubmit=async ev=>{
    ev.preventDefault();const submit=ev.submitter||ev.target.querySelector('button[type=submit],button:not([type])');if(submit.disabled)return;submit.disabled=true;
    try{
     const result=Object.fromEntries(new FormData(ev.target)),reason=result.reason;delete result.reason;
     for(const [key] of counts){result[key]=Number(result[key]);if(!Number.isSafeInteger(result[key])||result[key]<0)throw Error('工耗数量必须为非负整数 / 산출은 0 이상의 정수로 입력하세요');}
     if(booked.some(l=>l.unit!==result.unit))throw Error('本次成果单位与出库预约不一致，请联系办公室调整出库数量与单位。 / 출고 예약 단위를 확인하세요.');
     const reserved=booked.reduce((sum,l)=>sum+Number(l.quantity),0);if(reserved>Number(result.quantity))throw Error('已预约 '+reserved+' '+result.unit+'，本次填写 '+result.quantity+' '+result.unit+'，请核实实际成果或调整预约。 / 예약 수량을 확인하세요.');
     result.used_forklift=!!result.used_forklift;result.location_photos=await photos.read();
     if(external)await write('v2_bulk_op_job_finish',{...result,job_id:t.id,worker_id:leadId,complete_job:true,packed_box_count:result.packed_count,total_operated_box_count:result.operated_box_count,remark:result.description,result_note:reason});
     else await write('sop_task_complete_review',{id:t.id,revision:t.revision,result,decision:'pass',reason});
     await resolve(t.id);
    }catch(x){error(x);submit.disabled=false;}
   };
  }
  (initial?resolve(initial,options.detail):home()).catch(error);
  return {destroy(){closed=true;loadSequence++;cleanup();},open:resolve};
 };
})();
