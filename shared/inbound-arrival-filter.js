(function(){
'use strict';
const $=id=>document.getElementById(id);
window.CKInboundArrivalFilter={
 params(){return {expected_arrival_from:$('ibFilterArrivalFrom')?.value||'',expected_arrival_to:$('ibFilterArrivalTo')?.value||''};},
 install(){
 const bar=document.querySelector('#view-inbound .filter-bar');if(!bar||bar.querySelector('[data-inbound-dates]'))return;
 bar.classList.add('ck-inbound-filters');
 const dates=document.createElement('div');dates.dataset.inboundDates='';
 function group(title,ko,ids,newDate=false){
  const field=document.createElement('div');field.className='ck-date-filter-group';field.innerHTML='<div class="ck-date-filter-heading"><span>'+title+'<small>'+ko+'</small></span><div class="ck-date-filter-actions"></div></div><div class="ck-date-pair"></div>';
  const pair=field.querySelector('.ck-date-pair');ids.forEach((id,i)=>{if(i){const separator=document.createElement('span');separator.textContent='～';separator.setAttribute('aria-hidden','true');pair.append(separator);}const input=newDate?document.createElement('input'):$(id);input.id=id;input.type='date';input.setAttribute('aria-label',title+(i?'结束日期 / 종료일':'起始日期 / 시작일'));pair.append(input);});dates.append(field);return field;
 }
 const oldLabels=[...bar.querySelectorAll('.filter-label')].slice(0,2);
 group('建单日期','등록일',['ibFilterStart','ibFilterEnd']);
 const arrival=group('预计到达日期','입고 예정일',['ibFilterArrivalFrom','ibFilterArrivalTo'],true);
 group('卸货完成日期','하차 완료일',['ibFilterUnloadDoneFrom','ibFilterUnloadDoneTo']);
 oldLabels.forEach(n=>n.remove());bar.prepend(dates);
 const tools=document.createElement('div');tools.className='ck-inbound-filter-tools';for(const n of [...bar.childNodes])if(n!==dates)tools.append(n);bar.append(tools);
 const clear=document.createElement('button');clear.type='button';clear.dataset.clearArrival='';clear.textContent='清空 / 초기화';clear.title='只清空预计到达日期 / 예정일만 초기화';arrival.querySelector('.ck-date-filter-actions').append(clear);
 const help=document.createElement('details');help.className='ck-arrival-help';help.innerHTML='<summary aria-label="日期筛选说明 / 날짜 도움말" title="日期筛选说明 / 날짜 도움말">?</summary><div class="ck-arrival-help-panel">韩国业务日期；两端相同查单日，单边为起止范围。筛选时排除未填预计日期，清空后重新显示。<br>한국 날짜: 같은 날짜는 하루, 한쪽은 시작·종료 범위. 미등록 예정일은 필터 해제 시 표시됩니다.</div>';arrival.querySelector('.ck-date-filter-actions').append(help);
 const error=document.createElement('p');error.className='ck-arrival-filter-error';error.setAttribute('role','alert');error.hidden=true;dates.after(error);
 const query=()=>{const {expected_arrival_from:from,expected_arrival_to:to}=this.params();error.hidden=!(from&&to&&from>to);error.textContent=error.hidden?'':'预计到达起始日期不能晚于结束日期 / 시작일은 종료일보다 늦을 수 없습니다';if(error.hidden){resetPager('inbound');loadInboundList();}};
 for(const id of ['ibFilterArrivalFrom','ibFilterArrivalTo'])$(id).onchange=query;
 clear.onclick=()=>{$('ibFilterArrivalFrom').value='';$('ibFilterArrivalTo').value='';query();};
 const reset=document.createElement('button');reset.type='button';reset.className='btn btn-outline btn-sm';reset.dataset.resetInbound='';reset.textContent='重置 / 초기화';$('ibQueryBtn').after(reset);reset.onclick=()=>{for(const id of ['ibFilterStart','ibFilterEnd','ibFilterArrivalFrom','ibFilterArrivalTo','ibFilterUnloadDoneFrom','ibFilterUnloadDoneTo','ibFilterStatus','ibFilterBizClass','ibFilterCustomer','inboundFilterAccounted'])$(id).value='';help.open=false;query();};
 $('ibExportBtn').textContent='导出 CSV / CSV';$('ibExportBtn').title='导出入库计划 CSV / 입고 계획 CSV 내보내기';
 }
};
})();
