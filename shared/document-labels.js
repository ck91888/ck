(function(){
 'use strict';
 const e=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const ko=()=>window.getLang?.()==='ko',copy=(zh,kr)=>ko()?kr:zh;
 const number=x=>x?.display_no||copy('单号待补充','번호 확인 필요');
 const source=x=>x?.source_type==='inventory'?copy('库内库存','창고 재고'):(x?.source_display_no||copy('来源单号待补充','원본 번호 확인 필요'));
 window.CKDocumentLabels={number,source};
 // A single print implementation is shared by office and field entry scripts.
 window.CKNeedPrint=function(x){
  if(!x.display_no)throw Error(copy('请刷新作业计划后再打印','작업 계획을 새로고침 후 인쇄하세요'));
  const w=window.open('','_blank');if(!w)throw Error(copy('请允许浏览器打开打印窗口','인쇄 창을 허용하세요'));
  let qr='';try{qrcode.stringToBytes=qrcode.stringToBytesFuncs['UTF-8'];const code=qrcode(0,'M');code.addData('CKWORK|'+x.id+'|'+(x.requirement_version||1));code.make();qr=code.createSvgTag({cellSize:3,margin:12,scalable:true});}catch(error){w.close();throw error;}
  const sourceTitle=x.source_type==='inbound'?copy('来源入库计划','원본 입고 계획'):x.source_type==='outbound'?copy('来源出库计划','원본 출고 계획'):copy('货物来源','화물 출처');
  const html=`<!doctype html><html lang="${ko()?'ko':'zh'}"><head><meta charset="utf-8"><title>${e(number(x))} ${copy('作业计划','작업 계획서')}</title><style>@page{size:A4;margin:15mm}body{font:16px/1.7 'Microsoft YaHei','Malgun Gothic',sans-serif;color:#111;margin:24px}header{display:flex;justify-content:space-between;align-items:center;border-bottom:3px solid #111;padding-bottom:14px}svg{width:38mm;height:38mm;flex-shrink:0}h1{font-size:25px}h2{font-size:22px}pre{font:inherit;white-space:pre-wrap;border:1px solid #aaa;padding:18px}.number{font-size:19px;font-weight:700}.version{font-size:13px;color:#444}.line{border-bottom:1px solid #aaa;padding:20px 0}@media print{body{margin:0}header,pre{break-inside:avoid}}</style></head><body><header><div><h1>${copy('CK 仓库作业计划','CK 창고 작업 계획서')}</h1><p>${copy('客户','고객')}：${e(x.customer)}</p><p class="number">${copy('作业计划号','작업 계획번호')}：${e(number(x))}</p><span class="version">${copy('要求版本','요구사항 버전')} ${e(x.requirement_version||1)}</span></div>${qr}</header><p><b>${sourceTitle}：${e(source(x))}</b></p>${x.supply_chain_no?'<p>'+copy('供应链单号','공급망 번호')+'：'+e(x.supply_chain_no)+'</p>':''}<h2>${e(x.title)}</h2><p>${copy('货物范围','화물 범위')}：${e(x.scope_text||copy('见要求','요구사항 참조'))}</p><pre>${e(x.instructions)}</pre><p>${copy('卸货时已完成的打托、分货，请交接说明，后续只执行剩余内容。','하차 중 완료한 팔레트·분류 작업은 인계하고 남은 작업만 진행하세요.')}</p><div class="line">${copy('派审员','담당자')}：________　${copy('操作人员','작업자')}：________________</div><div class="line">${copy('完成明细','완료 내역')}：____________________________________</div><div class="line">${copy('审核','검수')}：________　${copy('日期','날짜')}：________________</div></body></html>`;
  w.document.write(html);w.document.close();w.focus();w.print();
 };
})();
