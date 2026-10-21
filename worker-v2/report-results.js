// Read-only result normalization. A row's alternate JSON encodings are one source.
import {effectiveResults} from './effective-results.js';
import {operationExport,operationSummary,readOperation} from '../shared/internal-operations.js';
const parse=x=>{try{return typeof x==='string'?JSON.parse(x):x;}catch{return null;}};
const numeric=x=>(typeof x==='number'||typeof x==='string'&&x.trim()!=='')&&Number.isFinite(Number(x))&&Number(x)>=0;
const lines=x=>Array.isArray(x)?x:Array.isArray(x?.result_lines)?x.result_lines:Array.isArray(x?.lines)?x.lines:[];
const units={carton:'箱',cartons:'箱',box:'箱',boxes:'箱',pallet:'托',pallets:'托',plt:'托',tray:'托'};
const fields={box_count:['箱','箱数'],pallet_count:['托','托盘'],packed_sku_count:['种','打包SKU'],packed_box_count:['箱','打包箱'],total_operated_box_count:['箱','总操作箱'],label_count:['张','贴标'],repaired_box_count:['箱','修箱'],reboxed_count:['箱','换箱'],used_carton_large_count:['个','大纸箱'],used_carton_small_count:['个','小纸箱'],verify_ok_count:['次','核对OK'],verify_ng_count:['次','核对NG'],location_count:['货位','货位数']};
export function parseOpsResultForExport(job_type,resultRows=[],job={}){
 const out={result_notes:'',diff_notes:'',result_submitters:'',result_submitted_at:'',readable_result_lines:'',result_lines_count:0,putaway_forklift:null},metrics=new Map(),warnings=new Set(),readable=[],remarks=[],diffs=[],submitters=new Set();
 for(const key of [...Object.keys(fields),'putaway_qty','putaway_carton_qty','putaway_pallet_qty','actual_qty','actual_carton_qty','actual_pallet_qty','sort_qty','repair_box_qty'])out[key+'_sum']=null;
 const add=(key,value,unit,label,phase='finish')=>{if(!numeric(value))return;out[key+'_sum']=(out[key+'_sum']??0)+Number(value);if(unit){const id=key+'|'+unit+'|'+phase,old=metrics.get(id);metrics.set(id,{metric:key,unit,label,phase,quantity:(old?.quantity||0)+Number(value),scope:'whole_task'});}};
 const rows=effectiveResults(resultRows);
 for(const r of rows){
  const p=parse(r.result_json)||{},outer=lines(parse(r.result_lines_json)),inner=lines(p),chosen=outer.length?outer:inner;
  if(outer.length&&inner.length&&JSON.stringify(outer)!==JSON.stringify(inner))warnings.add('两个明细来源不一致：采用 result_lines_json，原始记录保留待核');
  if(r.result_json&&!parse(r.result_json))warnings.add('result_json 无法解析');
  if(r.result_lines_json&&!parse(r.result_lines_json))warnings.add('result_lines_json 无法解析');
  if(r.remark)remarks.push(r.remark);if(r.diff_note||p.diff_note)diffs.push(r.diff_note||p.diff_note);if(r.created_by)submitters.add(r.created_by);
  if(r.created_at>out.result_submitted_at)out.result_submitted_at=r.created_at;
  const rowMetrics=new Set(),lineUnits=new Set();
  for(const line of chosen)if(numeric(line.actual_qty)||numeric(line.putaway_qty))lineUnits.add(units[line.unit_type||line.unit]||line.unit_type||line.unit||'');
  if(numeric(p.carton))lineUnits.add('箱');if(numeric(p.pallet))lineUnits.add('托');
  for(const line of chosen.length?chosen:(p.unit_type?[p]:[])){
   const unit=units[line.unit_type||line.unit]||line.unit_type||line.unit||'';
   for(const key of ['putaway_qty','actual_qty'])if(numeric(line[key])){
    add(key,line[key],unit||'单位未记录',key==='putaway_qty'?'理货':'实际');rowMetrics.add(key);
    if(unit==='箱'||unit==='托')add(key.replace('_qty',unit==='箱'?'_carton_qty':'_pallet_qty'),line[key]);
    if(!unit)warnings.add('数量单位未记录');
   }
   for(const key of ['label_count','repaired_box_count','reboxed_count'])if(numeric(line[key])){add(key,line[key],...fields[key]);rowMetrics.add(key);}
   readable.push(JSON.stringify(line));out.result_lines_count++;
  }
  for(const [key,[unit,label]] of Object.entries(fields)){
   if(rowMetrics.has(key))continue;
   let value=p[key];
   if(key==='box_count'||key==='pallet_count'){
    // SQL DEFAULT 0 alone is not evidence that a quantity was recorded.
    value=numeric(p[key])?p[key]:Number(r[key])>0?r[key]:null;
   }
   if(key==='verify_ok_count')value=p.task_scanned_ok_count??p.verify_ok_count??p.scanned_ok_count;
   if(key==='verify_ng_count')value=p.task_abnormal_count??p.verify_ng_count??p.abnormal_count;
   add(key,value,(key==='box_count'||key==='pallet_count')&&lineUnits.has(unit)?'':unit,label);
  }
  if(typeof p.used_forklift==='boolean')out.putaway_forklift=p.used_forklift;
  if(!rowMetrics.has('putaway_qty'))for(const key of ['carton','pallet'])if(numeric(p[key])){add('putaway_qty',p[key],units[key],'理货');add('putaway_'+key+'_qty',p[key]);}
  const eo=p.extra_ops||{};
  add('sort_qty',eo.sort_qty,eo.sort_unit||'单位未记录', '整理');add('repair_box_qty',eo.repair_box_qty);
  if(!rowMetrics.has('label_count')&&!numeric(p.label_count))add('label_count',eo.label_qty,...fields.label_count);
  if(!rowMetrics.has('repaired_box_count')&&!numeric(p.repaired_box_count))add('repaired_box_count',eo.repair_box_qty,...fields.repaired_box_count);
  const outputs=Array.isArray(p.group_outputs)&&p.group_outputs.length?p.group_outputs:Array.isArray(p.process_results)&&p.process_results.length?p.process_results:[p];
  for(const item of outputs)if(numeric(item.quantity)&&item.unit)add('output_'+(item.process_type||item.operation_type||'quantity'),item.quantity,item.unit,item.process_type||'完成量');
 }
 const op=operationExport(job);Object.assign(out,op);
 const operation=readOperation(job);
 if(operation){for(const phase of ['start','finish'])for(const [key,unit,label] of [['inventory_count','单位未记录','盘点数量'],['pick_count','单位未记录','废弃拣货数量'],['location_count','货位','库内货位'],['qc_count','单位未记录','质检数量'],['return_order_count','单','退件入库单数'],['return_product_count','产品','入库产品数']]){
  const v=operation[phase]?.[key];if(numeric(v))add('internal_'+key,v,unit,label,phase);
 }}
 out.quantity_metrics=[...metrics.values()];out.quantity_state=out.quantity_metrics.length?'recorded':'missing';out.quantity_warnings=[...warnings];
 out.result_notes=remarks.join(' | ');out.diff_notes=[...new Set(diffs)].join(' | ');out.result_submitters=[...submitters].join('、');out.readable_result_lines=readable.join(' || ');
 // Preserve every original encoding, including corrections, without truncation.
 out.raw_result_json_compact=JSON.stringify(resultRows.map(r=>({id:r.id,source:r.source,previous_result_id:r.previous_result_id,created_at:r.created_at,result_json:r.result_json,result_lines_json:r.result_lines_json,box_count:r.box_count,pallet_count:r.pallet_count})));
 out.result_summary=out.quantity_metrics.map(m=>m.label+' '+m.quantity+'（'+(m.metric==='location_count'?'整任务':m.unit+' / 整任务')+'）'+(m.phase==='start'?' 开始快照':'')).join('，')||'数量未记录（不等于零产出）';
 if(out.putaway_forklift!==null)out.result_summary+=' · 是否使用叉车 '+(out.putaway_forklift?'是':'否');
 if(operation||op.internal_record_phase)out.result_summary=operationSummary(job)+'；'+out.result_summary;
 if(remarks.length)out.result_summary+='；备注：'+out.result_notes;
 if(warnings.size)out.result_summary+='；'+[...warnings].join('；');
 return out;
}
