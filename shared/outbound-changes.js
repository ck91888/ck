/* Both office and field screens render the same human-readable change notice. */
(function(){'use strict';
 const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const types={material_update:['作业资料更新','작업 자료 변경'],requirement_update:['作业要求更新','작업 지시 변경'],shipping_adjustment:['出库数量／单位调整','출고 수량·단위 변경'],order_update:['出库安排修改','출고 예약 변경']};
 function title(log){let type=log.change_type;if(type==='material_update'&&/出库数量|单位/.test(log.summary_text||''))type='shipping_adjustment';const pair=types[type]||types.order_update;return pair[window.getLang?.()==='ko'?1:0];}
 function diff(data,label,display){
  const fields=Object.keys(data||{}).filter(k=>{const v=data[k];return v&&typeof v==='object'&&(Object.hasOwn(v,'from')||Object.hasOwn(v,'to'))&&v.from!==v.to;});
  if(!fields.length)return '';
  return '<table class="line-table" style="margin-top:6px;font-size:12px"><thead><tr><th>修改内容 / 변경 항목</th><th>修改前 / 이전</th><th>修改后 / 이후</th></tr></thead><tbody>'+fields.map(k=>'<tr><td>'+escape(k==='shipping_quantity'?'出库数量与单位 / 출고 수량·단위':label(k))+'</td><td>'+escape(display(k,data[k].from))+'</td><td>'+escape(display(k,data[k].to))+'</td></tr>').join('')+'</tbody></table>';
 }
 function summary(log){return '<p class="ck-change-summary"><b>'+escape(title(log))+'</b>'+(log.summary_text?'：'+escape(log.summary_text):'')+'</p>';}
 window.CKOutboundChanges={title,diff,summary};
})();
