(function(){
'use strict';
const e=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const field=(name,label,type='text')=>`<label>${label}<input data-work="${name}" type="${type}"></label>`;
const planLabel=text=>window.CKPlanCopy?CKPlanCopy.html(text):e(text);
window.CKWorkFields=function(host){
  host.innerHTML='<h3><span data-i18n="ck_plan_86">随本入库计划建立作业计划（可选、多条）</span></h3><p>可以先保存作业计划，不必安排出库。例如：先打托并反馈明细，客户确认后再补出库计划。资料可在下方按箱唛分组、选择文件，与本次入库一起保存；直接转发沿用整单需求。<br>출고 예약 없이 작업 계획을 먼저 등록할 수 있습니다.</p><div data-work-list></div><button type="button" data-add-work>'+planLabel('新增')+'</button>';
 const list=host.querySelector('[data-work-list]'),drafts=new Map();
 const changed=()=>host.dispatchEvent(new Event('ck-work-change'));
 function add(){const row=document.createElement('fieldset');row.dataset.workRow='';row.innerHTML=field('title','作业名称')+'<label>业务<select data-work="department"><option value="bulk">大货</option><option value="direct_ship">代发</option></select></label>'+field('scope_text','箱唛／货物范围（文字）')+'<label><span data-i18n="ck_plan_29">作业类型 / 작업 종류</span><select data-work="operation_kind"><option value="operation">需操作／加工 · 작업 필요</option><option value="direct_forward">直接转发（无加工）· 작업 없이 전달</option></select></label><label>客服作业要求<textarea data-work="instructions" placeholder="例如：BD001～015打托，反馈明细后等待客户预约出库"></textarea></label>'+field('supply_chain_no','供应链系统单号（有则填写）')+field('planned_quantity','本作业计划数量','number')+'<label>单位<select data-work="planned_unit"><option>箱</option><option>件</option><option>托</option></select></label><p class="ck-muted" style="grid-column:1/-1">暂无出库预约时直接保存；作业完成、反馈明细后再安排出库。<br>출고 미정: 작업 완료 및 명세 전달 후 예약</p><div data-outbounds></div><button type="button" data-add-ob><span data-i18n="ck_plan_46">＋客户已约出库，填写计划（可选）</span></button><button type="button" data-remove>'+planLabel('删除此计划')+'</button>';
 list.append(row);if(window.CKCargoGroups&&window.CK_SOP_ROLLOUT?.workChain){const groupHost=document.createElement('section');groupHost.className='ck-plan-wide';groupHost.style.gridColumn='1/-1';row.append(groupHost);const draft=CKCargoGroups.create(groupHost);drafts.set(row,draft);const sync=()=>{const direct=row.querySelector('[data-work=operation_kind]').value==='direct_forward';groupHost.hidden=direct;groupHost.querySelectorAll('input,textarea,button,select').forEach(x=>x.disabled=direct);};row.querySelector('[data-work=operation_kind]').addEventListener('change',sync);draft.ready.then(sync);}
 row.querySelector('[data-remove]').onclick=()=>{drafts.get(row)?.destroy();drafts.delete(row);row.remove();changed();};row.querySelector('[data-add-ob]').onclick=()=>addOutbound(row.querySelector('[data-outbounds]'));list.append(row);changed();return row;}
 host.querySelector('[data-add-work]').onclick=add;
 return {add,clear:()=>{for(const d of drafts.values())d.destroy();drafts.clear();list.replaceChildren();changed();},prepare:async values=>{const entries=[...list.children];entries.forEach(row=>row.disabled=true);try{for(let i=0;i<entries.length;i++)if(values[i].cargo_groups)await drafts.get(entries[i]).prepare(values[i].cargo_groups);if(entries.some(r=>!r.isConnected))throw Error('已取消创建');return values;}finally{entries.forEach(row=>row.disabled=false);}},read:()=>Array.from(list.children).map(row=>{const data={};row.querySelectorAll('[data-work]').forEach(x=>data[x.dataset.work]=x.value.trim());if(!data.title||!data.instructions)throw Error('请填写每条作业的名称和文字要求');data.outbounds=readOutbounds(row);if(data.operation_kind!=='direct_forward'&&drafts.has(row)){const groups=drafts.get(row).read();if(groups){if(data.outbounds.length)throw Error('分组作业请在审核后按组安排出库');data.cargo_groups=groups;}}return data;})};
};
// These controls are shared by inline bookings and the standalone outbound form.
const outboundModes=['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'];
const bookingCopy={
 ck_booking_destination:['目的地','목적지'],ck_booking_po:['PO号','발주번호'],ck_booking_mode:['出库模式','출고 모드'],
 ck_booking_date:['预计出库日期','출고 예정일'],ck_booking_quantity:['本次出库数量','출고 수량'],
 ck_booking_remark:['运输／提货备注（选填）','운송·픽업 메모 (선택)'],ck_booking_choose:['请选择出库模式','출고 모드를 선택하세요'],
 ck_booking_destination_hint:['如：美国FBA仓、客户门店','예: 미국 FBA 창고, 고객 매장'],ck_booking_po_hint:['客户PO编号','고객 발주번호'],
 ck_booking_remark_hint:['车辆、提货交接等；加工要求和标签请在作业计划维护','차량·픽업 인계 메모. 작업 지시와 라벨은 작업 계획에서 관리하세요.'],
 ck_booking_incomplete:['请补齐预计出库日期、本次出库数量和出库模式；尚未预约则移除此出库项。','출고 예정일, 수량, 출고 모드를 입력하세요. 미정이면 이 예약 항목을 삭제하세요.']
};
if(window.LANG)for(const [key,[zh,ko]] of Object.entries(bookingCopy)){LANG.zh[key]=zh;LANG.ko[key]=ko;}
const bookingText=key=>window.L?L(key):(bookingCopy[key]?.join(' / ')||key);
const bookingLabel=key=>'<span data-i18n="'+key+'">'+e(bookingText(key))+'</span>';
function modeOptions(){return '<option value="" data-i18n="ck_booking_choose">'+e(bookingText('ck_booking_choose'))+'</option>'+outboundModes.map(value=>'<option value="'+value+'" data-i18n="outmode_'+value+'">'+e(bookingText('outmode_'+value))+'</option>').join('');}
const outboundFields=[['destination','ck_booking_destination','text','ck_booking_destination_hint','oc-destination'],['po_no','ck_booking_po','text','ck_booking_po_hint','oc-po'],['outbound_mode','ck_booking_mode','select','','oc-outmode'],['expected_ship_at','ck_booking_date','date','','oc-expected-ship-at'],['outbound_requirement','ck_booking_remark','textarea','ck_booking_remark_hint','oc-outbound-requirement']];
function bookingControl([name,key,type,hint]){const attributes=' data-ob="'+name+'"'+(hint?' data-i18n="'+hint+'" placeholder="'+e(bookingText(hint))+'"':'');const control=type==='select'?'<select'+attributes+'>'+modeOptions()+'</select>':type==='textarea'?'<textarea'+attributes+' rows="2"></textarea>':'<input'+attributes+' type="'+type+'">';return '<label>'+bookingLabel(key)+control+'</label>';}
function syncOutboundFields(){for(const [name,key,type,hint,id] of outboundFields){const input=document.getElementById(id);if(!input)continue;const label=input.previousElementSibling;if(label?.tagName==='LABEL'){label.dataset.i18n=key;label.textContent=bookingText(key);}if(hint){input.dataset.i18n=hint;input.placeholder=bookingText(hint);}if(type==='select'){const selected=input.value;input.innerHTML=modeOptions();input.value=selected;}}}
window.CKOutboundForm={sync:syncOutboundFields};if(typeof document!=='undefined')syncOutboundFields();
function addOutbound(host,context={}){const row=document.createElement('fieldset');row.dataset.outboundRow='';row.innerHTML='<legend>'+planLabel('已预约出库（可选）')+'</legend><p class="ck-plan-wide muted">请按客户已确认的预约填写；尚未预约，请移除此项。<br>고객이 확정한 예약을 입력하세요. 미정이면 이 항목을 삭제하세요.</p><label>'+bookingLabel('ck_booking_quantity')+' <span data-ob-unit></span><input data-ob="quantity" type="number" min="1" step="1"></label>'+[outboundFields[3],outboundFields[2],outboundFields[0],outboundFields[1],outboundFields[4]].map(bookingControl).join('')+'<p class="ck-plan-wide muted">出库数量沿用本计划货量单位；打托后成果单位不同，可先保存作业，反馈成果后再安排出库。<br>작업 전 화물 단위를 사용합니다. 포장 후 단위가 다르면 결과 확인 후 출고를 예약하세요.</p><button type="button">'+planLabel('删除此出库计划')+'</button>';const source=host.closest('[data-work-row]'),unit=()=>context.unit?.()||source?.querySelector('[data-work="planned_unit"]')?.value||'';const sync=()=>{row.querySelector('[data-ob-unit]').textContent=unit()?'（'+unit()+'）':'（先选择货量单位 / 단위 선택）';};const editor=context.unitControl||source?.querySelector('[data-work="planned_unit"]')||host;editor?.addEventListener('change',sync);row.querySelector('button').onclick=()=>{editor?.removeEventListener('change',sync);row.remove();};host.append(row);sync();}
function readOutbounds(host){return Array.from(host.querySelectorAll('[data-outbound-row]')).flatMap(row=>{const data={};row.querySelectorAll('[data-ob]').forEach(x=>data[x.dataset.ob]=x.value.trim());if(!Object.values(data).some(v=>v!==''))return [];if(!data.expected_ship_at||!Number.isSafeInteger(Number(data.quantity))||Number(data.quantity)<1||!outboundModes.includes(data.outbound_mode))throw Error(bookingText('ck_booking_incomplete'));return [data];});}
window.CKOptionalOutbounds=function(host,context={}){host.innerHTML='<p>'+planLabel('可先保存作业，反馈明细后再安排出库 / 출고 예약 없이 작업 등록 가능')+'</p><div data-outbounds></div><button type="button">'+planLabel('＋客户已约出库，填写计划（可选）')+'</button>';host.querySelector('button').onclick=()=>addOutbound(host.querySelector('[data-outbounds]'),context);return ()=>readOutbounds(host);};
// Ignore only untouched defaults; preserve every user-entered legacy shipping draft.
window.CKHasStandaloneOutbound=function(row){return Object.entries(row).some(([key,value])=>{
 if(key==='seq')return false;
 if(key==='biz_class')return value&&value!=='bulk';
 if(['uses_stock_operation','planned_box_count','planned_pallet_count'].includes(key))return Number(value)!==0;
 return String(value??'').trim()!=='';
});};
window.CKConnectInboundOutbounds=function(host){
 const checkbox=document.getElementById('ibc-link-ob'),panel=document.getElementById('ibcLinkObPanel');
 if(!checkbox||!panel)return;
 if(window.CK_SOP_ROLLOUT?.workChain){checkbox.checked=false;checkbox.closest('.form-group').hidden=true;panel.style.display='none';return;}
 const group=checkbox.closest('.form-group'),label=checkbox.closest('label');
 for(const node of Array.from(label.childNodes))if(node!==checkbox)node.remove();
 label.append(document.createTextNode(' 客户已有预约：另建出库计划（可选）/ 출고 예약 있음'));
 const notice=document.createElement('p');notice.className='ck-muted';notice.hidden=true;
 notice.textContent='下方保留了之前填写的出库资料。若属于某条作业，请填到该需求下；若暂不安排出库，取消勾选即可。资料不会自动归到某一需求。';group.append(notice);
 const sync=()=>{
  const hasWork=host.querySelector('[data-work-list]').children.length>0;
  const hasDraft=getIbcLinkObRows().some(CKHasStandaloneOutbound);
  if(hasWork&&!hasDraft)checkbox.checked=false;
  group.hidden=hasWork&&!hasDraft;
  panel.style.display=checkbox.checked?'':'none';
  notice.hidden=!(hasWork&&hasDraft&&checkbox.checked);
 };
 host.addEventListener('ck-work-change',sync);checkbox.addEventListener('change',sync);
 panel.addEventListener('input',sync);panel.addEventListener('change',sync);panel.addEventListener('click',sync);
 sync();
};
const stateLabel={pending:'待安排',assigned:'已分配',working:'作业中',paused:'已暂停',awaiting_review:'待审核',rework:'待整改',completed:'已审核',waiting_customer:'待客户安排',linked:'已关联出库',closed:'已关闭',cancelled:'已取消·不得执行'};
window.CKWorkNeedsTable=function(items){return '<table class="line-table ck-work-table"><thead><tr><th>序号</th><th>货物范围／数量</th><th>作业要求／分货依据</th><th>进度</th></tr></thead><tbody>'+items.map((x,i)=>'<tr'+(x.status==='cancelled'?' class="ck-cancelled"':'')+'><td>'+(i+1)+'<div class="ck-work-number">'+e(x.display_no||'')+'</div></td><td><strong>'+e(x.scope_text||'范围见右侧要求')+'</strong><div>'+e(x.planned_quantity?x.planned_quantity+' '+(x.planned_unit||''):'数量见要求')+'</div></td><td><strong>'+e(x.title)+'</strong><div class="ck-instruction-text">'+e(x.instructions)+'</div>'+(x.deadline?'<div>'+planLabel('要求完成日期')+'：'+e(window.CKWorkDate?.day(x.deadline)||'—')+'</div>':'')+(x.location?'<div>位置：'+e(x.location)+'</div>':'')+'</td><td>'+e(stateLabel[x.status]||x.status)+'<div>版本 '+e(x.revision||1)+'</div></td></tr>').join('')+'</tbody></table>';};
window.CKDownloadWorkTemplate=function(){const a=document.createElement('a');a.href='/templates/pallet-details.xlsx';a.download='CK打托明细模板.xlsx';document.body.append(a);a.click();a.remove();};
window.CKUploadWorkDetails=async function(need,file){
 if(!file||!/\.xlsx?$/i.test(file.name))throw Error('请选择打托明细 Excel 文件');if(file.size>20*1024*1024)throw Error('文件不能超过20MB');
 const book=XLSX.read(await file.arrayBuffer(),{type:'array'}),grid=XLSX.utils.sheet_to_json(book.Sheets[book.SheetNames[0]],{header:1,raw:false,defval:''});
 const header=grid.shift()||[];const expected=['托盘号','箱唛/条码','数量（箱唛时数量可不填默认1）','效期（非必填）','批次号（非必填）','备注（非必填）'];
 if(!expected.every((x,i)=>String(header[i]||'').trim()===x))throw Error('表头与打托明细模板不一致，请使用原模板');
 const rows=grid.filter(r=>r.some(v=>String(v).trim())).map((r,i)=>{if(!r[0]||!r[1])throw Error('第'+(i+2)+'行缺少托盘号或箱唛/条码');const quantity=r[2]===''?1:Number(r[2]);if(!Number.isSafeInteger(quantity)||quantity<1)throw Error('第'+(i+2)+'行数量无效');return {pallet:String(r[0]),mark:String(r[1]),quantity,expiry:r[3]||'',batch:r[4]||'',remark:r[5]||''};});
 if(!rows.length||rows.length>10000)throw Error('明细须为1至10000行');
 if(!confirm('共'+rows.length+'行、'+new Set(rows.map(r=>r.pallet)).size+'个托盘。上传为新版本？数量不会自动改写审核成果。'))return null;
 const form=new FormData();form.append('action','v2_attachment_upload');form.append('file',file);form.append('related_doc_type','sop_need');form.append('related_doc_id',need.id);form.append('attachment_category','inbound_material');form.append('uploaded_by',CKSession.user.name);
 const response=await fetch('/api',{method:'POST',credentials:'same-origin',body:form}),uploaded=await response.json();if(!uploaded.ok)throw Error(uploaded.error||uploaded.message||'上传失败');
 const attachment=uploaded.attachment||uploaded;
 return CKSession.request('sop_need_details',{id:need.id,revision:need.revision,client_req_id:crypto.randomUUID(),filename:file.name,attachment_id:attachment.id,rows});
};
})();
