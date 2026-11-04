import {exportRange,nextExportDate,koreanTime,financeWorkbookData,LABOR_EXPORT_LIMITS} from './labor-export-data.js';
const identity=()=>{const u=window.CKSession?.user;return u?JSON.stringify([u.id,u.scope,u.role,u.operation_context?.id||'']):'';};
export async function collectLaborExport(request,range,{valid=()=>true,progress=()=>{}}={}){
 const rows=[],seen=new Set();let cursor={date:range.start,offset:0},asOf='',snapshot='',pages=0;
 while(cursor){
  if(!valid())throw Error('导出已取消或登录身份变化，未生成文件');
  const r=await request({start_date:range.start,end_date:range.end,agency:range.agency||'',keyword:range.keyword||'',cursor,as_of:asOf||undefined,snapshot:cursor.offset?snapshot:undefined});
  if(!valid())throw Error('导出已取消或登录身份变化，未生成文件');
  if(!r.ok||r.start!==range.start||r.end!==range.end||r.date!==cursor.date||r.filters?.agency!==(range.agency||'')||r.filters?.keyword!==(range.keyword||'')||!Array.isArray(r.items)||r.items.length>LABOR_EXPORT_LIMITS.page||!Number.isSafeInteger(r.total)||r.total<cursor.offset+r.items.length||!r.snapshot||!Number.isFinite(Date.parse(r.asOf))||asOf&&asOf!==r.asOf)throw Error(r.error||'导出分页校验失败，未生成文件，请重试');
  asOf=r.asOf;snapshot=r.snapshot;
  for(const item of r.items){const key=item.date+'|'+item.personKey;if(item.date!==cursor.date||!item.personKey||seen.has(key))throw Error('导出人员重复或日期不一致，未生成文件');seen.add(key);rows.push(item);}
  if(rows.length>LABOR_EXPORT_LIMITS.rows)throw Error('超过20000个人日，未生成部分文件；请缩小日期范围或按公司筛选');
  const expected=cursor.offset+r.items.length<r.total?{date:cursor.date,offset:cursor.offset+r.items.length}:cursor.date<range.end?{date:nextExportDate(cursor.date),offset:0}:null,next=r.next;
  if(JSON.stringify(next)!==JSON.stringify(expected)||next&&next.date===cursor.date&&!r.items.length)throw Error('导出分页不完整或未前进，未生成文件');
  progress({date:r.date,rows:rows.length,pages:++pages});cursor=next;
 }
 return {rows,meta:{...range,asOf}};
}
export function mountLaborFinanceExport(root,{date,today,agencies,alive,visible}){
 let disposed=false,busy=false,controller,run=0;const button=root.querySelector('[data-finance-export]');
 const dialog=document.createElement('dialog');dialog.className='ck-editor';dialog.dataset.financeExportDialog='';dialog.setAttribute('aria-label','日当工时表按日期范围导出');
 dialog.innerHTML='<form><h2>日当工时表导出 / 근무시간 내보내기</h2><p>韩国自然日，每人每天一行；小时保留2位小数，附全部上下班明细。未签退显示未下班，不推算缺失工时。</p><label>开始日期（包含）<input type="date" name="start" required></label><label>结束日期（包含）<input type="date" name="end" required></label><label>导出范围<select name="mode"><option value="all">全部日当（不受页面筛选及分页限制）</option><option value="filter">按公司、姓名或工牌筛选</option></select></label><fieldset data-finance-filters hidden><label>人力公司<select name="agency"><option value="">全部公司</option></select></label><label>姓名或工牌<input name="keyword" maxlength="80"></label></fieldset><p data-finance-status role="status" aria-live="polite"></p><p data-finance-error class="ck-error" role="alert" hidden></p><p class="ck-muted">每次最多366天、20000个人日；超限或任何一页失败不下载部分文件。部门按实际作业，派审、休息及待核实分列；原始核对CSV仍保留。</p><div class="ck-actions"><button type="button" data-finance-close>关闭 / 닫기</button><button type="submit">导出 Excel / 내보내기</button></div></form>';
 for(const agency of agencies){const option=document.createElement('option');option.value=option.textContent=agency;dialog.querySelector('[name=agency]').append(option);}root.append(dialog);
 const form=dialog.querySelector('form'),status=dialog.querySelector('[data-finance-status]'),error=dialog.querySelector('[data-finance-error]'),close=dialog.querySelector('[data-finance-close]'),submit=form.querySelector('[type=submit]');
 const valid=()=>!disposed&&alive()&&visible()&&dialog.open;
 const cancel=()=>{run++;controller?.abort();busy=false;submit.disabled=false;form.querySelectorAll('input,select').forEach(n=>n.disabled=false);close.textContent='关闭 / 닫기';};
 const hide=()=>{cancel();dialog.close();};close.onclick=hide;dialog.oncancel=event=>{event.preventDefault();hide();};dialog.addEventListener('close',()=>{if(!dialog.open)cancel();});
 button.onclick=()=>{if(!alive()||!visible())return;form.elements.start.value=date().slice(0,8)+'01';form.elements.end.value=date();form.elements.mode.value='all';form.elements.agency.value='';form.elements.keyword.value='';form.querySelector('fieldset').hidden=true;error.hidden=true;status.textContent='全部日当；不继承页面的管理部门、当前作业或搜索筛选。';dialog.showModal();form.elements.start.focus();};
 form.elements.mode.onchange=()=>{form.querySelector('fieldset').hidden=form.elements.mode.value==='all';};
 form.onsubmit=async event=>{
  event.preventDefault();if(busy||!valid())return;
  error.hidden=true;let range;try{range=exportRange(form.elements.start.value,form.elements.end.value);if(range.end>today)throw Error('结束日期不能晚于今天（韩国时间）');range.agency=form.elements.mode.value==='all'?'':form.elements.agency.value;range.keyword=form.elements.mode.value==='all'?'':form.elements.keyword.value.normalize('NFC').trim();}catch(x){error.hidden=false;error.textContent=x.message;return;}
  busy=true;const token=++run,owner=identity();const currentController=new AbortController();controller=currentController;const signal=currentController.signal,owns=()=>valid()&&token===run&&!signal.aborted&&!!owner&&identity()===owner;
  submit.disabled=true;form.querySelectorAll('input,select').forEach(n=>n.disabled=true);close.textContent='取消导出 / 취소';status.textContent='正在读取全部符合条件的人员…';
  const request=async data=>{const timeout=setTimeout(()=>currentController.abort(),60000);try{const response=await fetch(window.SOP_API||'/api',{method:'POST',credentials:'same-origin',signal,headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_attendance_daily_export',...data})});const r=await response.json();if(!response.ok||!r.ok)throw Error(r.error||'工时导出读取失败');return r;}finally{clearTimeout(timeout);}};
  try{
   const result=await collectLaborExport(request,range,{valid:owns,progress:p=>{if(owns())status.textContent=`已读取至 ${p.date}，共 ${p.rows} 个人日；正在生成完整文件…`;}});
   if(!owns())return;const XLSX=window.CKEnsureExcel?await window.CKEnsureExcel():window.XLSX;if(!owns())return;if(!XLSX?.utils||!XLSX?.writeFile)throw Error('Excel组件尚未加载，请检查网络后重试');
   const data=financeWorkbookData(result.rows,result.meta),book=XLSX.utils.book_new();
   for(const [key,title] of [['daily','每日工时'],['spans','出勤时段'],['notes','说明']]){const sheet=XLSX.utils.aoa_to_sheet(data[key]);sheet['!cols']=data[key][0].map((_,i)=>({wch:key==='notes'?(i?105:28):i===1?20:i===2?26:24}));sheet['!autofilter']={ref:sheet['!ref']};for(let row=1;row<data[key].length;row++)for(const col of data.hourColumns[key]||[]){const cell=sheet[XLSX.utils.encode_cell({r:row,c:col})];if(cell)cell.z='0.00';}XLSX.utils.book_append_sheet(book,sheet,title);}
   if(!owns())return;XLSX.writeFile(book,`CK_日当每日工时_${range.start}_${range.end}.xlsx`);status.textContent=`已导出 ${result.rows.length} 个人日（${range.start} 至 ${range.end}，含首尾），统计截止 ${koreanTime(result.meta.asOf)}（韩国时间）；详见Excel说明页。`;
  }catch(x){if(valid()&&token===run&&identity()===owner){error.hidden=false;error.textContent=x.name==='AbortError'?'读取超时或已取消，未生成部分文件；请缩小范围或重试':x.message;status.textContent='未生成文件。';}}
  finally{if(valid()&&token===run&&identity()!==owner)hide();if(valid()&&token===run&&identity()===owner){busy=false;submit.disabled=false;form.querySelectorAll('input,select').forEach(n=>n.disabled=false);close.textContent='关闭 / 닫기';}}
 };
 return {cancel,hide,destroy(){disposed=true;cancel();dialog.remove();button.onclick=null;}};
}
