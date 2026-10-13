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
  const field=document.createElement('fieldset');field.innerHTML='<legend>'+title+'<small>'+ko+'</small></legend><div class="ck-date-pair"></div>';
  const pair=field.querySelector('div');ids.forEach((id,i)=>{const label=document.createElement('label');label.textContent=i?'至 / 종료':'从 / 시작';const input=newDate?document.createElement('input'):$(id);input.id=id;input.type='date';input.setAttribute('aria-label',title+(i?'结束日期 / 종료일':'起始日期 / 시작일'));label.append(input);pair.append(label);});dates.append(field);return field;
 }
 const oldLabels=[...bar.querySelectorAll('.filter-label')].slice(0,2);
 group('建单日期','등록일',['ibFilterStart','ibFilterEnd']);
 const arrival=group('预计到达日期','입고 예정일',['ibFilterArrivalFrom','ibFilterArrivalTo'],true);
 group('卸货完成日期','하차 완료일',['ibFilterUnloadDoneFrom','ibFilterUnloadDoneTo']);
 oldLabels.forEach(n=>n.remove());bar.prepend(dates);
 const tools=document.createElement('div');tools.className='ck-inbound-filter-tools';for(const n of [...bar.childNodes])if(n!==dates)tools.append(n);bar.append(tools);
 const clear=document.createElement('button');clear.type='button';clear.dataset.clearArrival='';clear.textContent='清空预计日期 / 예정일 초기화';arrival.append(clear);
 const note=document.createElement('p');note.className='ck-arrival-filter-note';note.textContent='韩国业务日期；两端相同查单日，单边为起止范围。筛选时排除未填预计日期，清空后重新显示。 / 한국 날짜: 같은 날짜는 하루, 한쪽은 시작·종료 범위. 미등록 예정일은 필터 해제 시 표시됩니다.';dates.after(note);
 const error=document.createElement('p');error.className='ck-arrival-filter-error';error.setAttribute('role','alert');error.hidden=true;note.after(error);
 const query=()=>{const {expected_arrival_from:from,expected_arrival_to:to}=this.params();error.hidden=!(from&&to&&from>to);error.textContent=error.hidden?'':'预计到达起始日期不能晚于结束日期 / 시작일은 종료일보다 늦을 수 없습니다';if(error.hidden){resetPager('inbound');loadInboundList();}};
 for(const id of ['ibFilterArrivalFrom','ibFilterArrivalTo'])$(id).onchange=query;
 clear.onclick=()=>{$('ibFilterArrivalFrom').value='';$('ibFilterArrivalTo').value='';query();};
 }
};
})();
