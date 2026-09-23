(function(){
'use strict';
const e=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const field=(name,label,type='text')=>`<label>${label}<input data-work="${name}" type="${type}"></label>`;
window.CKWorkFields=function(host){
 host.innerHTML='<h3>随本入库计划建立作业需求（可选、多条）</h3><p>可以先保存作业需求，不必安排出库。例如：先打托并反馈明细，客户确认后再补出库计划。没有作业要求可留空，后续追加。<br>출고 예약 없이 작업 요청을 먼저 등록할 수 있습니다.</p><div data-work-list></div><button type="button" data-add-work>＋添加作业需求</button>';
 const list=host.querySelector('[data-work-list]');
 const changed=()=>host.dispatchEvent(new Event('ck-work-change'));
 function add(){const row=document.createElement('fieldset');row.dataset.workRow='';row.innerHTML=field('title','作业名称')+'<label>业务<select data-work="department"><option value="bulk">大货</option><option value="direct_ship">代发</option></select></label>'+field('scope_text','箱唛／货物范围（文字）')+'<label>客服作业要求<textarea data-work="instructions" placeholder="例如：BD001～015打托，反馈明细后等待客户预约出库"></textarea></label>'+field('supply_chain_no','供应链系统单号（有则填写）')+field('planned_quantity','本作业计划数量','number')+'<label>单位<select data-work="planned_unit"><option>箱</option><option>件</option><option>托</option></select></label><p class="ck-muted" style="grid-column:1/-1">暂无出库预约时直接保存；作业完成、反馈明细后再安排出库。<br>출고 미정: 작업 완료 및 명세 전달 후 예약</p><div data-outbounds></div><button type="button" data-add-ob>＋客户已约出库，填写计划（可选）</button><button type="button" data-remove>删除此需求</button>';
 row.querySelector('[data-remove]').onclick=()=>{row.remove();changed();};row.querySelector('[data-add-ob]').onclick=()=>addOutbound(row.querySelector('[data-outbounds]'));list.append(row);changed();return row;}
 host.querySelector('[data-add-work]').onclick=add;
 return {add,clear:()=>{list.replaceChildren();changed();},read:()=>Array.from(list.children).map(row=>{const data={};row.querySelectorAll('[data-work]').forEach(x=>data[x.dataset.work]=x.value.trim());if(!data.title||!data.instructions)throw Error('请填写每条作业的名称和文字要求');data.outbounds=readOutbounds(row);return data;})};
};
function addOutbound(host){const row=document.createElement('fieldset');row.dataset.outboundRow='';row.innerHTML='<legend>已预约出库（可选）</legend><p style="grid-column:1/-1">尚未预约可留空或移除此项，不影响保存作业需求。</p><label>出库日期<input data-ob="expected_ship_at" type="date"></label><label>分配数量<input data-ob="quantity" type="number" min="1"></label><label>出库方式<select data-ob="outbound_mode"><option value="customer_pickup">客户自提</option><option value="warehouse_dispatch">仓库安排</option><option value="milk_express">快递</option><option value="milk_pallet">托盘运输</option><option value="container_pickup">集装箱</option></select></label><label>目的地<input data-ob="destination"></label><label>出库要求<textarea data-ob="outbound_requirement"></textarea></label><button type="button">删除此出库计划</button>';row.querySelector('button').onclick=()=>row.remove();host.append(row);}
function readOutbounds(host){return Array.from(host.querySelectorAll('[data-outbound-row]')).flatMap(row=>{const data={};row.querySelectorAll('[data-ob]').forEach(x=>data[x.dataset.ob]=x.value.trim());const filled=Object.entries(data).some(([k,v])=>k==='outbound_mode'?v&&v!=='customer_pickup':v!=='');if(!filled)return [];if(!data.expected_ship_at||!Number.isSafeInteger(Number(data.quantity))||Number(data.quantity)<1)throw Error('已填写部分出库资料，请补齐出库日期和分配数量；尚未预约则移除此出库项，作业需求仍可保存。 / 출고일과 수량을 입력하거나 미정인 출고 항목을 삭제하세요.');return [data];});}
window.CKOptionalOutbounds=function(host){host.innerHTML='<p>可先保存作业，反馈明细后再安排出库 / 출고 예약 없이 작업 등록 가능</p><div data-outbounds></div><button type="button">＋客户已约出库，填写计划（可选）</button>';host.querySelector('button').onclick=()=>addOutbound(host.querySelector('[data-outbounds]'));return ()=>readOutbounds(host);};
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
window.CKWorkNeedsTable=function(items){return '<table class="line-table ck-work-table"><thead><tr><th>序号</th><th>货物范围／数量</th><th>作业要求／分货依据</th><th>进度</th></tr></thead><tbody>'+items.map((x,i)=>'<tr'+(x.status==='cancelled'?' class="ck-cancelled"':'')+'><td>'+(i+1)+'</td><td><strong>'+e(x.scope_text||'范围见右侧要求')+'</strong><div>'+e(x.planned_quantity?x.planned_quantity+' '+(x.planned_unit||''):'数量见要求')+'</div></td><td><strong>'+e(x.title)+'</strong><div class="ck-instruction-text">'+e(x.instructions)+'</div>'+(x.location?'<div>位置：'+e(x.location)+'</div>':'')+'</td><td>'+e(stateLabel[x.status]||x.status)+'<div>版本 '+e(x.revision||1)+'</div></td></tr>').join('')+'</tbody></table>';};
window.CKPrintWorkGroup=function(g){const w=window.open('','_blank');if(!w)throw Error('请允许打开打印窗口');let qr='';try{if(g.source_type==='inbound'&&window.buildInboundQrHtml)qr=buildInboundQrHtml(g.display_no,3);}catch{}
 w.document.write('<!doctype html><html><head><meta charset="utf-8"><title>'+e(g.display_no)+' 作业指令</title><style>body{font:14px/1.6 sans-serif;margin:24px;color:#111}header{display:flex;justify-content:space-between;border-bottom:2px solid #111}svg{width:90px;height:90px}table{width:100%;border-collapse:collapse}th,td{border:1px solid #777;padding:8px;text-align:left;vertical-align:top;overflow-wrap:anywhere}th{background:#eee}.ck-instruction-text{white-space:pre-wrap}.ck-cancelled{color:#888}thead{display:table-header-group}tr{break-inside:avoid}@media print{@page{size:A4;margin:15mm}body{margin:0}}</style></head><body><header><h1>整批作业指令</h1>'+qr+'</header><h2>'+e(g.display_no)+'</h2><p>来源：'+e(g.source_type==='inbound'?'入库计划':'库存／原业务单据')+'　客户：'+e(g.customer)+'　货物：'+e(g.cargo_summary||'见作业明细')+'</p>'+CKWorkNeedsTable(g.items)+'<p>请按货物范围及要求卸货分货；范围不明或实物不符，先报告负责人确认。</p><p>打印时间：'+new Date().toLocaleString('zh-CN',{timeZone:'Asia/Seoul'})+'（韩国时间）</p></body></html>');w.document.close();w.focus();w.print();};
window.CKNeedPrint=function(x){const w=window.open('','_blank');if(!w)throw Error('请允许浏览器打开打印窗口');w.document.write('<!doctype html><html><head><title>作业单</title><style>body{font:18px sans-serif;padding:24px}pre{white-space:pre-wrap;line-height:1.7}td,th{border:1px solid #aaa;padding:10px}table{border-collapse:collapse;width:100%}</style></head><body><h1>CK 仓库作业单</h1><p>'+e(x.id)+'　客户：'+e(x.customer)+'</p><p>来源：'+e(x.source_id||'库内库存')+'　供应链单号：'+e(x.supply_chain_no||'—')+'</p><h2>'+e(x.title)+'</h2><p>货物范围：'+e(x.scope_text||'见要求')+'</p><pre>'+e(x.instructions)+'</pre><p>主操作员：________　协助人员：________</p><p>实际完成明细／位置：________________</p><p>派审员审核：________　日期：________</p></body></html>');w.document.close();w.focus();w.print();};
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
