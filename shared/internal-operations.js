// Shared by the Worker and field forms. Zero is a recorded quantity; absence is not.
export const internalTypes=['inventory','disposal','qc','other_internal','inbound_return'];
export const isInternal=type=>internalTypes.includes(type);
export const inventoryLabels={blind:'盲盘 / 블라인드 재고조사',precise:'精盘 / 정밀 재고조사'};
export const metricLabels={return_order_count:'该批次退件入库单数 / 이번 반품 입고 전표 수',return_product_count:'入库产品数 / 입고 상품 수',inventory_count:'盘点数量 / 재고조사 수량',pick_count:'拣货数量 / 피킹 수량',location_count:'货位数量 / 위치 수',qc_count:'质检数量 / 품검 수량',used_forklift:'是否使用叉车 / 지게차 사용',organizing_content:'整理内容 / 정리 내용'};
const fail=m=>{throw Error(m);};
const fields=(type,mode,phase)=>type==='inbound_return'&&phase==='finish'?['return_order_count','return_product_count']:type==='inventory'&&(mode==='precise'?'start':'finish')===phase?['inventory_count','location_count','used_forklift']:type==='disposal'&&phase==='start'?['pick_count','location_count','used_forklift']:type==='qc'&&phase==='finish'?['qc_count']:type==='other_internal'&&phase==='finish'?['organizing_content']:[];
export const operationFields=fields;
export function readOperation(job){
 try{const data=JSON.parse(job?.shared_result_json||'{}').internal_operation;return data?.version===1&&data.job_type===job.job_type?data:null;}catch{return null;}
}
export function validateOperationInput(type,mode,phase,input={}){
 if(!input||typeof input!=='object'||Array.isArray(input))fail('库内操作数据无效 / 작업 입력 오류');
 const allowed=fields(type,mode,phase),out={};
 for(const key of Object.keys(input))if(!allowed.includes(key))fail('此字段不应在当前时点填写 / 현재 단계에서 입력할 수 없습니다');
 for(const key of allowed){
  const value=input[key],label=metricLabels[key];
  if(key==='used_forklift'){if(typeof value!=='boolean')fail('请选择是否使用叉车：是或否 / 지게차 사용 여부를 선택하세요');out[key]=value;}
  else if(key==='organizing_content'){
   if(typeof value!=='string'||!value.trim()||value.trim().length>2000)fail('整理内容必填，不能只有空白，最多2000字 / 정리 내용을 입력하세요 (최대 2000자)');out[key]=value.trim();
  }else{
   if(!['number','string'].includes(typeof value)||String(value).trim()===''||!Number.isSafeInteger(Number(value))||Number(value)<0||Number(value)>1e8)fail(label+'必填，请填写 0–100000000 的整数 / 0 이상의 정수를 입력하세요');
   out[key]=Number(value);
  }
 }
 return out;
}
export function validateOperationStart(type,body){
 if(!isInternal(type))return null;
 const mode=body.inventory_type;
 if(type==='inventory'&&!Object.hasOwn(inventoryLabels,mode))fail('请选择盘点类型：盲盘或精盘 / 재고조사 종류를 선택하세요');
 if(type!=='inventory'&&mode!=null)fail('只有盘点可选择盘点类型 / 재고조사 종류 오류');
 return {version:1,job_type:type,...(type==='inventory'?{inventory_type:mode}:{}),start:validateOperationInput(type,mode,'start',body.internal_metrics)};
}
export function validateOperationFinish(job,body){
 const data=readOperation(job);
 if(!data)return null; // Pre-release jobs keep their original contract, without invented facts.
 if(['inventory_type','shared_result','result_json','box_count','pallet_count'].some(k=>body[k]!=null))fail('请仅填写本阶段要求的数据，盘点类型和开始记录不可改写 / 현재 단계의 항목만 입력하세요');
 return {...data,finish:validateOperationInput(job.job_type,data.inventory_type,'finish',body.internal_metrics)};
}
export function operationSummary(job){
 if(!isInternal(job?.job_type))return '';
 const data=readOperation(job);
 if(!data)return '旧版任务：'+(job.job_type==='inventory'?'盘点类型及数量未记录':'专项数据未记录')+' / 기존 작업 · 미기록';
 const parts=[data.inventory_type?inventoryLabels[data.inventory_type]:job.job_type==='inbound_return'?'退件入库结果 / 반품 입고 결과':'库内操作 / 창고 내부 작업'];
 for(const phase of ['start','finish']){
  const snapshot=data[phase];if(!snapshot)continue;
  const values=Object.keys(metricLabels).filter(k=>Object.hasOwn(snapshot,k)).map(k=>metricLabels[k]+'：'+(k==='used_forklift'?(snapshot[k]?'是 / 예':'否 / 아니요'):snapshot[k]));
  if(values.length)parts.push((phase==='start'?'开始填写 / 시작 기록':'结束填写 / 종료 기록')+' · '+values.join(' · '));
 }
 return parts.join('；');
}
export function operationExport(job){
 if(!isInternal(job?.job_type))return {};
 const d=readOperation(job),phase=fields(job.job_type,d?.inventory_type,'start').length?'start':'finish',m=d?.[phase]||{};
 return {internal_record_version:d?.version??null,inventory_type:d?.inventory_type??null,internal_record_phase:d?(fields(job.job_type,d.inventory_type,'start').length?'start':'finish'):'legacy',
  return_order_count:m.return_order_count??null,return_product_count:m.return_product_count??null,inventory_count:m.inventory_count??null,disposal_pick_count:m.pick_count??null,internal_location_count:m.location_count??null,internal_used_forklift:m.used_forklift??null,qc_count:m.qc_count??null,organizing_content:m.organizing_content??null,
  internal_recorded_at:m.recorded_at??null,internal_recorded_by:m.recorded_by??null,internal_operation_json:d?JSON.stringify(d):''};
}
