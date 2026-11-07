(function(){
 'use strict';
 const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const labels=[['order_count','订单数量','주문 수','本次任务覆盖的订单数，不是产品件数或拣货单张数','이번 작업의 주문 수 (상품 수량·피킹 전표 수 아님)'],['product_quantity','产品总件数','총 상품 수량','本次所有产品的件数合计','이번 작업의 전체 상품 수량'],['sku_count','SKU种类数','SKU 종류 수','不同SKU的种类数，不是件数','서로 다른 SKU 수, 상품 개수 아님'],['location_count','涉及货位数','피킹 로케이션 수','本次涉及的不同货位数，不是库存数','이번 작업의 로케이션 수, 재고 수량 아님']];
 const bi=(zh,ko)=>'<span>'+zh+'</span><small lang="ko">'+ko+'</small>';
 function fields(docs=[]){return '<fieldset class="ck-pick-workload"><legend>'+bi('本次拣货计划工作量','이번 피킹 계획 작업량')+'</legend><p class="ck-pick-scope">'+bi('整次任务填写一次，包含本次全部拣货单。','이번 작업의 모든 피킹 전표를 합쳐 한 번만 입력하세요.')+'</p><p class="ck-pick-docs">'+bi('本次任务包含 '+docs.length+' 张拣货单','이번 작업 피킹 전표 '+docs.length+'건')+'<span>'+docs.map(esc).join(' · ')+'</span></p><div class="ck-pick-inputs">'+labels.map(([key,zh,ko,hint,khint])=>'<label>'+bi(zh,ko)+'<input name="pick_'+key+'" type="text" inputmode="numeric" pattern="[0-9]+" required autocomplete="off" aria-describedby="pick-hint-'+key+'"><span class="ck-pick-hint" id="pick-hint-'+key+'">'+bi(hint,khint)+'</span></label>').join('')+'<fieldset class="ck-pick-forklift"><legend>'+bi('是否叉车配货','지게차 사용 여부')+'</legend><div class="ck-pick-choices"><label><input type="radio" name="pick_forklift" value="yes" required>'+bi('是','예')+'</label><label><input type="radio" name="pick_forklift" value="no" required>'+bi('否','아니요')+'</label></div><span class="ck-pick-hint">'+bi('请选择是或否','예 또는 아니요를 선택하세요')+'</span></fieldset></div></fieldset>';}
 function read(form){
  const result={};
  for(const [key,zh,ko] of labels){const raw=form.elements['pick_'+key].value.trim(),n=Number(raw);if(!/^\d+$/.test(raw)||!Number.isSafeInteger(n)||n<1)throw Error(zh+'须为正整数 / '+ko+': 양의 정수를 입력하세요');result[key]=n;}
  const choice=form.querySelector('[name=pick_forklift]:checked')?.value;if(!['yes','no'].includes(choice))throw Error('请选择是否叉车配货 / 지게차 사용 여부를 선택하세요');result.forklift=choice==='yes';return result;
 }
 function summary(value){
  const valid=value&&labels.filter(([key])=>key!=='order_count').every(([key])=>Number.isSafeInteger(value[key])&&value[key]>0)&&typeof value.forklift==='boolean';
  return '<section class="ck-pick-plan" data-pick-workload><h4>'+bi('任务计划工作量（非个人产量）','작업 계획량 (개인 실적 아님)')+'</h4>'+(valid?'<dl>'+labels.map(([key,zh,ko])=>'<div><dt>'+bi(zh,ko)+'</dt><dd>'+(Number.isSafeInteger(value[key])&&value[key]>0?esc(value[key]):bi('未记录','미기록'))+'</dd></div>').join('')+'<div><dt>'+bi('叉车配货','지게차 사용')+'</dt><dd>'+bi(value.forklift?'是':'否',value.forklift?'예':'아니요')+'</dd></div></dl>':'<p class="ck-pick-unrecorded">'+bi('未记录','미기록')+'</p>')+'<p class="ck-pick-hint">'+bi('开工时计划，供整任务参考；与结束实际数量分别保留。','시작 시 계획량입니다. 작업 전체 참고용이며 종료 실적과 별도로 보관합니다.')+'</p></section>';
 }
 window.CKPickWorkload={fields,read,summary};
})();
