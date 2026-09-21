(function(){
'use strict';
const e=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const field=(name,label,type='text')=>`<label>${label}<input data-work="${name}" type="${type}"></label>`;
window.CKWorkFields=function(host){
 host.innerHTML='<h3>随本入库计划建立作业需求（可选、多条）</h3><p>要求由客服直接填写文字。没有作业要求可留空，后续在入库详情追加。每条需求可同时安排多个出库计划；入库代发等不出库的操作不必填出库。</p><div data-work-list></div><button type="button" data-add-work>＋添加作业需求</button>';
 const list=host.querySelector('[data-work-list]');
 function add(){const row=document.createElement('fieldset');row.dataset.workRow='';row.innerHTML=field('title','作业名称')+'<label>业务<select data-work="department"><option value="bulk">大货</option><option value="direct_ship">代发</option></select></label>'+field('scope_text','箱唛／货物范围（文字）')+'<label>客服作业要求<textarea data-work="instructions" placeholder="例如：BD0921001–15打托，完成后反馈打托明细"></textarea></label>'+field('supply_chain_no','供应链系统单号（有则填写）')+field('planned_quantity','本作业计划数量','number')+'<label>单位<select data-work="planned_unit"><option>箱</option><option>件</option><option>托</option></select></label><div data-outbounds></div><button type="button" data-add-ob>＋同时新增出库计划</button><button type="button" data-remove>删除此需求</button>';
 row.querySelector('[data-remove]').onclick=()=>row.remove();row.querySelector('[data-add-ob]').onclick=()=>addOutbound(row.querySelector('[data-outbounds]'));list.append(row);return row;}
 host.querySelector('[data-add-work]').onclick=add;
 return {add,clear:()=>list.replaceChildren(),read:()=>Array.from(list.children).map(row=>{const data={};row.querySelectorAll('[data-work]').forEach(x=>data[x.dataset.work]=x.value.trim());if(!data.title||!data.instructions)throw Error('请填写每条作业的名称和文字要求');data.outbounds=readOutbounds(row);return data;})};
};
function addOutbound(host){const row=document.createElement('fieldset');row.dataset.outboundRow='';row.innerHTML='<legend>关联出库计划</legend><label>出库日期<input data-ob="expected_ship_at" type="date" required></label><label>分配数量<input data-ob="quantity" type="number" min="1" required></label><label>出库方式<select data-ob="outbound_mode"><option value="customer_pickup">客户自提</option><option value="warehouse_dispatch">仓库安排</option><option value="milk_express">快递</option><option value="milk_pallet">托盘运输</option><option value="container_pickup">集装箱</option></select></label><label>目的地<input data-ob="destination"></label><label>出库要求<textarea data-ob="outbound_requirement"></textarea></label><button type="button">删除此出库计划</button>';row.querySelector('button').onclick=()=>row.remove();host.append(row);}
function readOutbounds(host){return Array.from(host.querySelectorAll('[data-outbound-row]')).map(row=>{const data={};row.querySelectorAll('[data-ob]').forEach(x=>data[x.dataset.ob]=x.value.trim());if(!data.expected_ship_at||Number(data.quantity)<1)throw Error('请填写出库日期与分配数量');return data;});}
window.CKOptionalOutbounds=function(host){host.innerHTML='<div data-outbounds></div><button type="button">＋同时新增出库计划（可选）</button>';host.querySelector('button').onclick=()=>addOutbound(host.querySelector('[data-outbounds]'));return ()=>readOutbounds(host);};
window.CKNeedPrint=function(x){const w=window.open('','_blank');if(!w)throw Error('请允许浏览器打开打印窗口');w.document.write('<!doctype html><html><head><title>作业单</title><style>body{font:18px sans-serif;padding:24px}pre{white-space:pre-wrap;line-height:1.7}td,th{border:1px solid #aaa;padding:10px}table{border-collapse:collapse;width:100%}</style></head><body><h1>CK 仓库作业单</h1><p>'+e(x.id)+'　客户：'+e(x.customer)+'</p><p>来源：'+e(x.source_id||'库内库存')+'　供应链单号：'+e(x.supply_chain_no||'—')+'</p><h2>'+e(x.title)+'</h2><p>货物范围：'+e(x.scope_text||'见要求')+'</p><pre>'+e(x.instructions)+'</pre><p>主操作员：________　协助人员：________</p><p>实际完成明细／位置：________________</p><p>派审员审核：________　日期：________</p></body></html>');w.document.close();w.focus();w.print();};
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
