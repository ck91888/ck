(function(){
'use strict';
const enabled=()=>!!window.CK_SOP_ROLLOUT?.workChain;
const fieldContext=flag=>flag||window.CKSession?.user?.scope==='field'||window.location.pathname.startsWith('/001/');
const e=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const api=(action,data={})=>CKSession.request(action,data);
const kinds={work_material:'作业说明／明细 · 작업 자료',pallet_label:'托唛 · 팔레트 라벨',shipping_document:'出库单 · 출고 서류',product_label:'产品条码标签 · 상품 바코드'};
const fileUrl=f=>window.SOP_API+'?action=v2_attachment_get&file_key='+encodeURIComponent(f.file_key);
const button=(label,fn)=>{const b=document.createElement('button');b.type='button';if(window.CKPlanCopy)CKPlanCopy.bind(b,label);else b.textContent=label;b.onclick=async()=>{b.disabled=true;try{await fn();}catch(x){alert(x.message);}finally{b.disabled=false;}};return b;};
function viewLink(need){return '/002/?need='+encodeURIComponent(need.id)+'&individual=1';}
function shippingBasisEditor(host,need,shipping,onChange){
 if(need.result||need.operation_kind==='direct_forward'||['closed','cancelled'].includes(need.status)||!['manager','service'].includes(CKSession.user?.role))return;
 const copy=value=>window.CKPlanCopy?CKPlanCopy.html(value):e(value), links=shipping.links.filter(l=>shipping.shipping_plans.some(o=>o.id===l.outbound_id&&o.status!=='cancelled'));
 const section=document.createElement('details');section.className='chain-basis';
 section.innerHTML='<summary>'+copy('调整出库数量与单位')+'</summary><p>'+copy('待操作货量')+'：<b>'+e(need.planned_quantity)+' '+e(need.planned_unit)+'</b></p><p class="muted">'+copy('打托或换包装后，可单独填写预计出库成果；例如 28 箱打成 2 托。原作业货量保留，已有预约须逐单核对。')+'</p><form><div class="chain-basis-grid"><label>'+copy('预计可出库总数量')+'<input name="quantity" type="number" min="1" step="1" required value="'+e(shipping.schedule_quantity)+'"></label><label>'+copy('出库成果单位')+'<select name="unit">'+['箱','件','托','单','批'].map(u=>'<option value="'+u+'" '+(shipping.schedule_unit===u?'selected':'')+'>'+e(u)+'</option>').join('')+'</select></label></div>'+links.map(l=>'<label class="chain-booking">'+e(shipping.shipping_plans.find(o=>o.id===l.outbound_id)?.display_no||l.outbound_id)+' · '+copy('预约数量')+' <span data-booking-unit>'+e(shipping.schedule_unit)+'</span><input data-allocation="'+e(l.outbound_id)+'" type="number" min="1" step="1" required value="'+e(l.quantity)+'"></label>').join('')+'<label>'+copy('调整说明')+'<input name="reason" required></label><button type="submit">'+copy('保存出库数量与单位')+'</button><p role="status"></p></form>';
 host.append(section);const form=section.querySelector('form');form.elements.unit.onchange=()=>section.querySelectorAll('[data-booking-unit]').forEach(el=>el.textContent=form.elements.unit.value);
 let request='',signature='';form.onsubmit=async ev=>{ev.preventDefault();const submit=ev.submitter,status=form.querySelector('[role=status]');submit.disabled=true;status.textContent='';try{const body={id:need.id,revision:shipping.revision,quantity:Number(form.elements.quantity.value),unit:form.elements.unit.value,reason:form.elements.reason.value,allocations:[...form.querySelectorAll('[data-allocation]')].map(el=>({outbound_id:el.dataset.allocation,quantity:Number(el.value)}))};const next=JSON.stringify(body);if(next!==signature){signature=next;request=crypto.randomUUID();}await api('sop_need_shipping_basis',{...body,client_req_id:request});await onChange?.();}catch(x){status.textContent=x.message;}finally{submit.disabled=false;}};
}
async function allocation(need){const r=await api('sop_work_need_search',{search:need.id});return r.items.find(n=>n.id===need.id);}
const copy=(zh,ko)=>window.CKPlanCopy?CKPlanCopy.html(zh):e(window.getLang?.()==='ko'?ko:zh);
function filesTable(files,empty='暂无作业资料。托唛、出库单、产品条码等在这里统一上传。'){return files.length?'<div class="chain-scroll"><table class="chain-files"><thead><tr><th><span data-i18n="ck_plan_59">资料 / 자료</span></th><th><span data-i18n="ck_plan_60">类型</span></th><th><span data-i18n="ck_plan_61">上传人 · 时间</span></th><th></th></tr></thead><tbody>'+files.map(f=>'<tr><td><a href="'+e(fileUrl(f))+'" target="_blank" rel="noopener">'+e(f.file_name)+'</a>'+(f.historical?'<small><span data-i18n="ck_plan_63">历史来源资料 · 원본 자료</span></small>':'')+'</td><td>'+(window.CKPlanCopy?CKPlanCopy.html((f.batch?'本批总作业明细':kinds[f.material_kind])||'打托／货物明细'):e((f.batch?(window.getLang?.()==='ko'?'입고 건 전체 작업 명세':'本批总作业明细'):kinds[f.material_kind])||'打托／货物明细'))+'</td><td>'+e(f.uploaded_by)+'<small>'+e(new Date(f.created_at).toLocaleString('zh-CN',{timeZone:'Asia/Seoul',hour12:false}))+'</small></td><td><a href="'+e(fileUrl(f))+'" download="'+e(f.file_name)+'"><span data-i18n="ck_plan_62">下载 / 다운로드</span></a><span data-remove-file="'+e(f.id)+'"></span></td></tr>').join('')+'</tbody></table></div>':'<p class="muted">'+copy(empty,empty==='暂无仓库反馈'?'창고 작업 피드백이 없습니다':empty==='暂无客服作业资料'?'고객 담당자 자료가 없습니다':'작업 자료가 없습니다')+'</p>';}
async function materials(host,need,{field=false,onChange=()=>{},items=null}={}){
 if(fieldContext(field)){host.replaceChildren();return;}
 host.className='chain-materials';host.innerHTML='<h3><span data-i18n="ck_plan_58">作业资料 / 작업 자료</span></h3><p>正在读取…</p>';
 const r=items?{items,revision:need.revision}:await api('sop_work_materials',{id:need.id});if(!host.isConnected)return;
 let revision=r.revision;
 const feedback=r.items.filter(f=>f.material_kind==='work_material'),office=r.items.filter(f=>f.material_kind!=='work_material');
 host.innerHTML='<section class="chain-material-group" data-direction="office"><h3>'+copy('客服提供给仓库的作业资料','고객 담당자가 창고에 제공하는 작업 자료')+'</h3><p class="muted">'+copy('托唛、出库单和产品条码标签由客服上传，仓库在这里下载。','팔레트 라벨, 출고 서류, 상품 바코드 라벨은 고객 담당자가 올리고 창고에서 내려받습니다.')+'</p>'+filesTable(office,'暂无客服作业资料')+'</section><section class="chain-material-group" data-direction="field"><h3>'+copy('仓库反馈给客服的作业说明／明细','창고에서 고객 담당자에게 전달하는 작업 설명·명세')+'</h3><p class="muted">'+copy('现场上传作业说明或明细；办公室收到仓库反馈后也可代录，客服在这里查看下载。','현장에서 작업 설명이나 명세를 올립니다. 사무실에서도 창고 피드백을 대신 등록할 수 있으며 고객 담당자가 여기에서 확인합니다.')+'</p>'+filesTable(feedback,'暂无仓库反馈')+'</section>';
 const role=CKSession.user?.role,writable=!['closed','cancelled'].includes(need.status);
 const canOffice=writable&&!field&&['manager','service'].includes(role);
 const canFeedback=writable&&(field?['manager','dispatcher','reviewer'].includes(role):['manager','service'].includes(role));
 host.querySelectorAll('[data-remove-file]').forEach(span=>{const f=r.items.find(f=>f.id===span.dataset.removeFile);if(!f||f.historical||f.batch||f.id===need.details?.attachment_id||!(f.material_kind==='work_material'?canFeedback:canOffice))return;span.append(button('撤下 / 해제',async()=>{if(!confirm('撤下这份资料？历史记录仍会保留。 / 이 자료를 해제할까요?'))return;await api('sop_work_material_remove',{id:need.id,revision,attachment_id:f.id,client_req_id:crypto.randomUUID()});await onChange();if(host.isConnected)await materials(host,need,{field,onChange});}));});
 function addUpload(direction){
  const feedbackForm=direction==='field',form=document.createElement('form');form.className='chain-upload';
  form.innerHTML=(feedbackForm?'':'<label><span data-i18n="ck_plan_68">资料类型 / 자료 종류</span><select data-kind>'+Object.entries(kinds).filter(([k])=>k!=='work_material').map(([k,v])=>'<option'+(window.CKPlanCopy?CKPlanCopy.attrs(v):'')+' value="'+k+'">'+(window.CKPlanCopy?CKPlanCopy.text(v):v)+'</option>').join('')+'</select></label>')+'<label><span data-i18n="ck_plan_69">选择文件 / 파일 선택</span><input data-files type="file" multiple accept=".pdf,.xlsx,.xls,.csv,.jpg,.jpeg,.png,.webp" required></label><button type="submit">'+(feedbackForm?field?copy('上传作业说明／明细','작업 설명·명세 업로드'):copy('代录仓库反馈','창고 피드백 대리 등록'):copy('上传客服资料','고객 담당자 자료 업로드'))+'</button><p class="muted"><span data-i18n="ck_plan_72">每个文件不超过20MB，可连续追加。 / 파일당 20MB 이하</span></p><p data-status role="status"></p>';
  host.querySelector('[data-direction="'+direction+'"]').append(form);
  let pending=[];
  form.querySelector('[data-files]').onchange=()=>{pending=[];};
  form.onsubmit=async ev=>{ev.preventDefault();const submit=form.querySelector('[type=submit]'),status=form.querySelector('[data-status]');submit.disabled=true;
   if(!pending.length)pending=Array.from(form.querySelector('[data-files]').files).map(file=>({file,request:crypto.randomUUID(),kind:feedbackForm?'work_material':form.querySelector('[data-kind]').value,done:false}));
   try{for(const item of pending.filter(x=>!x.done)){status.textContent='正在上传 / 업로드 중：'+item.file.name;const data=new FormData();for(const [k,v] of Object.entries({action:'v2_attachment_upload',related_doc_type:'sop_need',related_doc_id:need.id,attachment_category:'work_material',material_kind:item.kind,revision,client_req_id:item.request}))data.set(k,v);data.set('file',item.file);const response=await fetch(window.SOP_API,{method:'POST',credentials:'include',body:data}),result=await response.json();if(!result.ok)throw Error(result.error||'上传失败');revision=result.revision;item.done=true;}
    status.textContent='资料已上传 / 업로드 완료';pending=[];await onChange();if(host.isConnected)await materials(host,need,{field,onChange});
   }catch(x){status.textContent=x.message+'。已成功的文件会保留，请刷新确认后重试。';}finally{submit.disabled=false;}
  };
 }
 if(canOffice)addUpload('office');
 if(canFeedback)addUpload('field');
}
async function mountNeed(host,need,options={}){
 if(!enabled()||fieldContext(options.field))return;host.classList.add('chain-need');
 const docs=document.createElement('section');host.append(docs);const materialLoad=materials(docs,need,options);
 if(options.field)return materialLoad;
 const allocationLoad=allocation(need);
 const plan=document.createElement('section');plan.className='chain-schedule';plan.innerHTML='<h3><span data-i18n="ck_plan_74">出库安排 / 출고 예약</span></h3><p class="muted"><span data-i18n="ck_plan_75">可以提前预约；完成审核后才能装货。 / 사전 예약 가능, 작업 확인 후 상차</span></p>';host.append(plan);
 const [shipping]=await Promise.all([allocationLoad,materialLoad]);if(!host.isConnected)return;
 if(!['closed','cancelled'].includes(need.status)){
  const a=shipping;if(a){const info=document.createElement('p');info.textContent='可安排 '+a.remaining+' '+a.schedule_unit+' / 예약 가능';plan.append(info);const link=document.createElement('a');link.className='btn btn-primary';link.href='/002/?create_outbound=1&need='+encodeURIComponent(need.id);if(window.CKPlanCopy)CKPlanCopy.bind(link,'安排出库 / 출고 예약');else link.textContent='安排出库 / 출고 예약';plan.append(link);}
 }
 for(const l of need.links||[]){const row=document.createElement('p');row.innerHTML='<a href="/002/?outbound='+encodeURIComponent(l.outbound_id)+'">'+e(shipping?.shipping_plans?.find(o=>o.id===l.outbound_id)?.display_no||l.outbound_id)+'</a> · '+e(shipping?.shipping_plans?.find(o=>o.id===l.outbound_id)?.expected_ship_at||'')+' · '+e(shipping?.shipping_plans?.find(o=>o.id===l.outbound_id)?.status==='cancelled'?'已取消 · ':'')+e(l.quantity)+' '+e(l.unit);plan.append(row);}
 if(shipping)shippingBasisEditor(plan,need,shipping,options.onChange);
 if(need.operation_kind==='direct_forward'&&need.status==='pending'){
  const section=document.createElement('section');section.className='chain-forward';section.innerHTML='<h3><span data-i18n="ck_plan_77">直接转发 / 작업 없이 전달</span></h3><p>收货后核对可发货数量；此确认不生成加工工时。</p><label><span data-i18n="ck_plan_78">可发货数量 / 출고 가능 수량</span><input type="number" min="1" step="1" value="'+e(need.planned_quantity)+'"></label>';section.append(button('确认收货，可安排发货 / 출고 준비 확인',async()=>{await api('sop_need_forward_ready',{id:need.id,revision:need.revision,quantity:Number(section.querySelector('input').value),client_req_id:crypto.randomUUID()});await options.onChange?.();}));host.prepend(section);
 }
}
let selected=null,pickerRun=0,pickerRoot=null;
async function outboundPicker(preselect=''){
 if(!enabled())return;const root=document.getElementById('view-outbound_create'),card=root.querySelector('.card');
 if(!pickerRoot){pickerRoot=document.createElement('section');pickerRoot.className='chain-picker';card.querySelector('.card-title').after(pickerRoot);}
 selected=null;pickerRoot.innerHTML='<h3><span data-i18n="ck_plan_80">关联作业计划 / 연결 작업 계획</span></h3><p class="muted"><span data-i18n="ck_plan_81">入库计划／库内库存 → 作业计划 → 出库计划</span></p><form data-search-form class="chain-search"><input data-search placeholder="客户、作业名称或单号 / 고객·작업명·번호" aria-label="查找作业计划"><button><span data-i18n="ck_plan_82">查找 / 검색</span></button></form><div data-results></div><div data-selection hidden></div>';
 if(window.CKPlanCopy)CKPlanCopy.bind(card.querySelector('.card-title'),'安排出库 / 출고 예약');else card.querySelector('.card-title').textContent='安排出库 / 출고 예약';
 for(const id of ['oc-customer','oc-biz-class','oc-uses-stock-op','oc-wms-wo','oc-instruction','oc-materials','oc-planned-box','oc-planned-pallet'])document.getElementById(id).closest('.form-group').hidden=true;
 const lines=document.getElementById('ocLinesTable');lines.hidden=true;lines.previousElementSibling.hidden=true;lines.nextElementSibling.hidden=true;document.getElementById('ocLinesBody').replaceChildren();
 const req=document.getElementById('oc-outbound-requirement');req.previousElementSibling.removeAttribute('data-i18n');req.previousElementSibling.textContent='运输／提货备注（选填）/ 운송·픽업 메모';req.placeholder='车辆、提货交接等；加工要求和标签请在作业计划维护';
 const date=document.getElementById('oc-expected-ship-at');date.required=true;date.nextElementSibling.textContent='按客户预约填写 / 고객 예약일';window.CKOutboundForm?.sync();
 async function choose(n){selected=n;const box=pickerRoot.querySelector('[data-selection]');box.hidden=false;box.innerHTML='<div class="chain-selected"><strong>'+e(n.customer)+' · '+e(n.title)+'</strong><a href="'+e(viewLink(n))+'" target="_blank"><span data-i18n="ck_plan_84">查看要求与资料 / 자료 보기</span></a><p>'+e(n.scope_text||n.instructions)+'</p><small>'+e(n.source_type==='inbound'?'来源入库计划：'+window.CKDocumentLabels.source(n):'来源：库内库存')+'</small></div><label>本次出库数量 / 출고 수량 · '+e(n.schedule_unit||n.result?.unit||n.planned_unit||'未填单位')+'<input id="ck-link-quantity" type="number" min="1" step="1" required max="'+e(n.remaining)+'" value="'+e(n.remaining)+'"></label><p class="muted">剩余可安排 '+e(n.remaining)+' '+e(n.schedule_unit)+'。'+(n.result?'作业已审核。':'作业完成前可预约，暂不可装货。')+'</p>';document.getElementById('oc-customer').value=n.customer;document.getElementById('oc-biz-class').value=n.department==='import'?'bulk':n.department;document.getElementById('oc-instruction').value=n.instructions;document.getElementById('oc-uses-stock-op').value='0';document.getElementById('oc-wms-wo').value=n.supply_chain_no||'';pickerRoot.querySelector('[data-results]').replaceChildren();}
 async function search(offset=0){const run=++pickerRun,results=pickerRoot.querySelector('[data-results]');results.textContent='正在查找…';try{const r=await api('sop_work_need_search',{search:pickerRoot.querySelector('[data-search]').value.trim(),offset});if(run!==pickerRun)return;results.replaceChildren();for(const n of r.items){const b=button((n.display_no?n.display_no+' · ':'')+n.customer+' · '+n.title+' · 可安排 '+n.remaining+' '+n.schedule_unit,()=>choose(n));b.className='chain-choice';results.append(b);}if(!r.items.length)results.innerHTML='<p>没有可安排的作业计划，请先从入库计划或库存建立需求。直接转发也需要一条需求。</p><a href="/002/?tab=need"><span data-i18n="ck_plan_83">打开作业计划 / 작업 계획</span></a>';const pages=document.createElement('div');pages.className='chain-search';if(offset)pages.append(button('上一页',()=>search(Math.max(0,offset-30))));if(r.more)pages.append(button('下一页',()=>search(offset+30)));results.append(pages);}catch(x){results.textContent=x.message;}}
 pickerRoot.querySelector('[data-search-form]').onsubmit=ev=>{ev.preventDefault();search();};
 if(preselect){const r=await api('sop_get',{id:preselect}),a=await allocation(r.record);if(a)await choose(a);else await search();}else await search();
}
function prepareOutbound(body){
 if(!enabled())return body;
 if(!selected||!document.getElementById('view-outbound_create')||document.getElementById('view-outbound_create').style.display==='none')throw Error('请先选择关联作业计划');
 const quantity=Number(document.getElementById('ck-link-quantity')?.value),unit=selected.schedule_unit;
 if(!Number.isSafeInteger(quantity)||quantity<1||quantity>selected.remaining)throw Error('请输入剩余可安排范围内的出库数量');
 if(!body.expected_ship_at)throw Error('请填写出库日期');
 return Object.assign(body,{sop_existing_need_id:selected.id,sop_need_revision:selected.revision,sop_link_quantity:quantity,customer:selected.customer,biz_class:selected.department==='import'?'bulk':selected.department,instruction:selected.instructions,uses_stock_operation:0,lines:[],planned_box_count:unit==='箱'?quantity:0,planned_pallet_count:unit==='托'?quantity:0});
}
function hideFileControls(root,type){
 if(!enabled()||!root)return;
 const selectors=type==='inbound'?'[id^="ib-"][type="file"],button[onclick*="InboundMaterial"],button[onclick*="ib-detail-material-input"],button[onclick*="ib-edit-material-input"]':'[id^="ob-"][type="file"],button[onclick*="OutboundMaterial"],button[onclick*="ob-detail-upload-input"],button[onclick*="ob-edit-material-input"]';
 root.querySelectorAll(selectors).forEach(x=>x.hidden=true);
 const edit=document.getElementById(type==='inbound'?'ib-edit-materials':'ob-edit-materials');if(edit&&root.contains(edit)){edit.parentElement.hidden=true;}
 if(type==='outbound')for(const id of ['ob-edit-customer','ob-edit-biz','ob-edit-uses-stock','ob-edit-wms','ob-edit-instruction','ob-edit-box','ob-edit-pallet']){const x=document.getElementById(id);if(x&&root.contains(x)){if(x.tagName==='SELECT')x.disabled=true;else x.readOnly=true;x.parentElement.hidden=true;}}
}
function installOffice(){
 if(!enabled())return;
 LANG.zh.new_outbound='+ 出库计划';LANG.ko.new_outbound='+ 출고 계획';document.getElementById('btnNewOutbound').textContent=LANG.zh.new_outbound;document.getElementById('obFilterUsesStockOp').hidden=true;
 const file=document.getElementById('ibc-materials');file.closest('.form-group').hidden=true;
 const old=document.getElementById('ibc-link-ob');old.checked=false;old.closest('.form-group').hidden=true;document.getElementById('ibcLinkObPanel').style.display='none';
 const wrap=(name,after)=>{const original=window[name];if(typeof original!=='function')return;window[name]=function(...args){const r=original.apply(this,args);if(r?.then)return r.then(v=>{after();return v;});after();return r;};};
 wrap('openInboundEditForm',()=>hideFileControls(document.body,'inbound'));wrap('openOutboundEditForm',()=>hideFileControls(document.body,'outbound'));
}
async function batchMaterials(host,group,{field=false}={}){
 if(fieldContext(field)){host.replaceChildren();return;}
 if(!enabled()||group.source_type!=='inbound'||!group.items?.length)return;
 const text=(zh,ko)=>window.CKPlanCopy?CKPlanCopy.html(zh):e(window.getLang?.()==='ko'?ko:zh);
 const need=group.items[0];
 const r=await api('sop_batch_work_materials',{id:need.id});if(!host.isConnected)return;
 host.className='chain-batch-materials';host.innerHTML='<h3>'+text('本批总作业明细','입고 건 전체 작업 명세')+'</h3><p class="muted">'+text('本入库计划下所有作业共用，点文件名或“下载”即可获取。','이 입고계획의 모든 작업에서 공유합니다. 파일명 또는 다운로드를 누르세요.')+'</p><div data-batch-files>'+ (r.items.length?filesTable(r.items):'<p class="muted">'+text('暂未上传总作业明细','전체 작업 명세가 없습니다')+'</p>')+'</div>';
  if(field||!['manager','service'].includes(CKSession.user?.role))return;
  if(r.can_manage){
   const removed=r.removed_items||[];
   if(removed.length){const bin=document.createElement('details');bin.className='chain-removed-materials';bin.innerHTML='<summary>'+text('已移除资料（可恢复）','제거한 자료（복구 가능）')+' · '+removed.length+'</summary>'+filesTable(removed);host.append(bin);}
   const changedLabel=window.getLang?.()==='ko'?'변경: ':'操作：';
   for(const f of [...r.items,...removed]){
    const slot=[...host.querySelectorAll('[data-remove-file]')].find(x=>x.dataset.removeFile===f.id);if(!slot)continue;
    const restore=!!f.removed;let pending=null;
    const action=button(restore?'恢复':'移除',async()=>{
     if(!pending){const message=restore?(window.getLang?.()==='ko'?'이 입고계획의 모든 연결 작업에 이 자료를 복구합니다. 계속하시겠습니까?':'将此资料恢复到本入库计划的全部关联作业，是否继续？'):(window.getLang?.()==='ko'?'이 자료를 입고계획의 모든 연결 작업에서 제거합니다. 파일은 보관되며 복구할 수 있습니다. 계속하시겠습니까?':'将此资料从本入库计划的全部关联作业中移除，文件保留且可恢复，是否继续？');if(!confirm(message))return;pending={id:need.id,attachment_id:f.id,material_revision:f.material_revision,client_req_id:crypto.randomUUID()};}
     await api(restore?'sop_batch_work_material_restore':'sop_batch_work_material_remove',pending);await batchMaterials(host,group,{field});
    });action.dataset.batchMaterialAction=restore?'restore':'remove';slot.append(action);
    if(f.changed_at){const audit=document.createElement('small');audit.textContent=changedLabel+(f.changed_by||'')+' · '+(window.CKWorkDate?CKWorkDate.time(f.changed_at):new Date(f.changed_at).toLocaleString('zh-CN',{timeZone:'Asia/Seoul',hour12:false}));slot.append(audit);}
   }
  }
 const form=document.createElement('form');form.className='chain-upload';form.innerHTML='<label>'+text('选择总作业明细（可多选）','전체 작업 명세 선택（복수 가능）')+'<input type="file" multiple required accept=".xlsx,.xls,.csv,.pdf,.jpg,.jpeg,.png,.webp"></label><button type="submit">'+text('上传总作业明细','전체 명세 업로드')+'</button><p class="muted">'+text('支持 Excel、CSV、PDF、图片，每个文件不超过 20MB。','Excel, CSV, PDF, 이미지 · 파일당 최대 20MB')+'</p><p role="status"></p>';host.append(form);
 let pending=[];const input=form.querySelector('input');input.onchange=()=>{pending=[];};
 form.onsubmit=async event=>{event.preventDefault();const submit=form.querySelector('button'),status=form.querySelector('[role=status]');submit.disabled=true;input.disabled=true;
  if(!pending.length)pending=[...input.files].map(file=>({file,request:crypto.randomUUID(),done:false}));
  try{for(const item of pending.filter(x=>!x.done)){status.textContent=(window.getLang?.()==='ko'?'업로드 중: ':'正在上传：')+item.file.name;const data=new FormData();for(const [k,v] of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:group.source_id,need_id:need.id,attachment_category:'batch_work_material',client_req_id:item.request}))data.set(k,v);data.set('file',item.file);const response=await fetch(window.SOP_API,{method:'POST',credentials:'include',body:data}),result=await response.json();if(!result.ok)throw Error(result.error||'上传失败');item.done=true;}
   await batchMaterials(host,group,{field});
  }catch(error){status.textContent=error.message;}finally{submit.disabled=false;input.disabled=false;}
 };
}
window.CKWorkChain={enabled,mountNeed,materials,batchMaterials,outboundPicker,prepareOutbound,hideFileControls,installOffice};
})();
