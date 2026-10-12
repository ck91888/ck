// A start-time plan belongs to one picking trip, never to each worker or result row.
export function validatePickWorkload(value){
 const labels={product_quantity:'产品总件数 / 총 상품 수량',sku_count:'SKU种类数 / SKU 종류 수',location_count:'涉及货位数 / 피킹 로케이션 수'};
 if(!value||typeof value!=='object'||Array.isArray(value))throw Error('请填写本次拣货计划工作量 / 이번 피킹의 계획 작업량을 입력하세요');
 const clean={};
 for(const [key,label] of Object.entries(labels)){
  if(typeof value[key]!=='number'||!Number.isSafeInteger(value[key])||value[key]<1)throw Error(label+'必须为正整数 / 양의 정수를 입력하세요');
  clean[key]=value[key];
 }
 if(typeof value.forklift!=='boolean')throw Error('请选择是否叉车配货：是或否 / 지게차 사용 여부를 선택하세요');
 return {...clean,forklift:value.forklift};
}
export function readPickWorkload(value){
 try{return {...validatePickWorkload(value),basis:'planned_at_start',scope:'whole_task',pick_doc_nos:Array.isArray(value.pick_doc_nos)?value.pick_doc_nos:[],recorded_at:value.recorded_at||'',recorded_by:value.recorded_by||''};}catch{return null;}
}
export async function pickWorkloadContext(env,jobId){
 const record=await env.DB.prepare("SELECT state FROM sop_records WHERE id=? AND kind='dispatch'").bind(jobId).first();
 const plan=record?readPickWorkload(JSON.parse(record.state).planned_pick_workload):null;
 const started=await env.DB.prepare('SELECT id FROM v2_ops_job_workers WHERE job_id=? LIMIT 1').bind(jobId).first();
 const docs=(await env.DB.prepare('SELECT pick_doc_no FROM v2_ops_job_pick_docs WHERE job_id=? ORDER BY created_at,id').bind(jobId).all()).results||[];
 return {planned_pick_workload:plan,pick_workload_required:!plan&&!started,pick_workload_started:!!started,task_pick_doc_nos:docs.map(d=>d.pick_doc_no)};
}
export function pickWorkloadSnapshot(value,user,time,docs){return {...validatePickWorkload(value),basis:'planned_at_start',scope:'whole_task',pick_doc_nos:[...docs],recorded_at:time,recorded_by_id:user.id,recorded_by:user.name};}
