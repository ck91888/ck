// One vehicle is one labor job. Links carry each plan's receipt, never extra jobs.
import {inboundFlowEnabled, inboundCode} from './inbound-flow.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results||[];
const fail=message=>{throw Error(message);};
const stamp=()=>new Date().toISOString();
const parse=value=>{try{return JSON.parse(value||'null');}catch{return null;}};
export async function tripPlans(env,jobId){
 if(!inboundFlowEnabled(env))return [];
 const links=await rows(env,'SELECT * FROM ck_unload_plan_links WHERE job_id=? ORDER BY position',jobId);
 const out=[];
 for(const link of links){
  const plan=await q(env,'SELECT * FROM v2_inbound_plans WHERE id=?',link.plan_id).first();
  const lines=await rows(env,'SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY line_no',link.plan_id);
  const needs=await rows(env,"SELECT id,state FROM sop_records WHERE kind='need' AND json_extract(state,'$.source_id')=? ORDER BY updated_at",link.plan_id);
  out.push({plan,lines,needs:needs.map(n=>({id:n.id,...parse(n.state)})),result:parse(link.result_json)});
 }
 return out;
}
export async function findUnloadPlan(env,value){
 let code=inboundCode(value);
 if(!code||code.length>500)fail('请扫描入库计划单号 / 입고계획 번호를 스캔하세요');
 // Printed QR links and raw plan/external IDs resolve locally, without visiting a URL.
 if(/^https?:\/\//i.test(code)){const u=new URL(code);code=u.searchParams.get('inbound')||u.searchParams.get('plan_id')||u.searchParams.get('id')||code;}
 const plans=await rows(env,"SELECT * FROM v2_inbound_plans WHERE (display_no=? OR id=? OR instr(char(10)||COALESCE(external_inbound_no,'')||char(10),char(10)||?||char(10))>0) AND COALESCE(is_deleted,0)=0 AND status!='cancelled' AND source_type!='return_session' LIMIT 2",code,code,code);
 if(plans.length!==1)fail(plans.length?'单号匹配多张计划，请核对 / 여러 계획과 일치합니다':'未找到对应入库计划 / 입고계획을 찾을 수 없습니다');
 await available(env,plans[0]);
 return {ok:true,plan:plans[0]};
}
async function available(env,plan){
 const no=plan.display_no||plan.id;
 if(plan.is_deleted||plan.status!=='pending')fail(no+' 已开始作业或已收货，不能加入新车次 / 이미 작업 중이거나 입고된 계획입니다');
 const existing=await q(env,"SELECT id FROM v2_inbound_plan_jobs WHERE plan_id=? AND job_type='unload' AND status IN ('pending','working','awaiting_close','completed') LIMIT 1",plan.id).first();
 if(existing)fail(no+' 已有卸货任务，请从进行中的任务继续 / 진행 중 작업에서 계속하세요');
}
export async function startUnloadTrip(env,body){
 if(!inboundFlowEnabled(env))fail('整车卸货未启用');
 const ids=body.plan_ids;
 if(!Array.isArray(ids)||!ids.length||ids.length>20||ids.some(x=>typeof x!=='string'||!x||x.length>120)||new Set(ids).size!==ids.length)fail('请选择1～20张不同入库计划 / 서로 다른 계획 1~20개를 선택하세요');
 const request=String(body.client_req_id||'');if(!request||request.length>100)fail('缺少请求编号，请刷新重试');
 const fingerprint=JSON.stringify([...ids].sort());
 const prior=await q(env,'SELECT * FROM ck_unload_trips WHERE request_id=?',request).first();
 if(prior){if(prior.plan_ids_json!==fingerprint||prior.owner_id!==env.SOP_REQUEST_USER?.id)fail('请求已变更，请重新开始');return {ok:true,job_id:prior.job_id,is_new_job:false};}
 const plans=[];
 for(const id of ids){
  const p=await q(env,'SELECT * FROM v2_inbound_plans WHERE id=?',id).first();
  if(!p||p.source_type==='return_session')fail('入库计划不存在 / 입고계획이 없습니다');
  await available(env,p);
  const lines=await rows(env,'SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY line_no',id);
  if(!lines.length)fail((p.display_no||id)+' 请先补充货物明细 / 화물 명세를 먼저 입력하세요');
  plans.push({p,lines});
 }
 const t=stamp(),jobId='JOB-UL-'+crypto.randomUUID(),segId='WS-'+crypto.randomUUID();
 const worker=String(body.worker_id||''),name=String(body.worker_name||'');if(!worker)fail('请选择操作人员 / 작업자를 선택하세요');
  const statements=[
  q(env,'INSERT INTO ck_unload_trips(job_id,request_id,plan_ids_json,owner_id,created_at) VALUES(?,?,?,?,?)',jobId,request,fingerprint,env.SOP_REQUEST_USER?.id||'',t),
  q(env,"INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count) VALUES(?,'unload',?,'unload','inbound_plan',?,'working',?,?,?,1)",jobId,body.biz_class||'',ids[0],worker,t,t)
  ];
 statements.push(q(env,'INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES(?,?,?,?)',request,'v2_unload_job_start',JSON.stringify({ok:true,job_id:jobId,worker_seg_id:segId,is_new_job:true}),t));
 plans.forEach(({p,lines},i)=>statements.push(q(env,'INSERT INTO ck_unload_plan_links(job_id,plan_id,position,plan_version,lines_snapshot) VALUES(?,?,?,?,?)',jobId,p.id,i,p.updated_at,JSON.stringify(lines.map(l=>({id:l.id,unit_type:l.unit_type,planned_qty:l.planned_qty}))))));
 for(const id of ids)statements.push(q(env,"UPDATE v2_inbound_plans SET status='unloading',updated_at=? WHERE id=?",t,id));
 statements.push(q(env,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)',segId,jobId,worker,name,t));
 try{await env.DB.batch(statements);}catch(e){
  const replay=await q(env,'SELECT * FROM ck_unload_trips WHERE request_id=?',request).first();
  if(replay&&replay.plan_ids_json===fingerprint&&replay.owner_id===env.SOP_REQUEST_USER?.id)return {ok:true,job_id:replay.job_id,is_new_job:false};
  if(/unload_plan_changed|unload_plan_busy/.test(e.message))fail('其中一张计划刚被修改或已开始卸货，请刷新本车清单 / 계획이 변경되었거나 작업이 시작되었습니다');
  throw e;
 }
 return {ok:true,job_id:jobId,worker_seg_id:segId,is_new_job:true,plan_count:ids.length};
}
export async function finishUnloadTrip(env,body,{ensureTasks,recalc,syncCourier}){
 const trip=await q(env,'SELECT * FROM ck_unload_trips WHERE job_id=?',String(body.job_id||'')).first();
 if(!trip)return null;
 if(!env.SOP_GROUP_FINISH)fail('请由本车派审员完成卸货 / 배정 담당자가 하차를 완료하세요');
 const job=await q(env,'SELECT * FROM v2_ops_jobs WHERE id=?',trip.job_id).first();
 const plans=await tripPlans(env,trip.job_id);
 if(job.status!=='completed'){
  if(job.status!=='working'||!body.complete_job||body.leave_only)fail('请从整车卸货页面填写各计划实收数量 / 차량 하차 화면에서 실제 수량을 입력하세요');
  const results=body.plan_results;
  if(!Array.isArray(results)||results.length!==plans.length||new Set(results.map(r=>r.plan_id)).size!==plans.length||results.some(r=>!plans.some(p=>p.plan.id===r.plan_id)))fail('请逐张填写本车全部计划的实收数量 / 모든 계획의 실제 수량을 입력하세요');
  const prepared=[],totals=new Map();
  for(const {plan,lines} of plans){
   if(plan.is_deleted||!['unloading','unloading_putting_away','putting_away'].includes(plan.status))fail('计划状态已变化，请刷新 / 계획 상태가 변경되었습니다');
   const input=results.find(r=>r.plan_id===plan.id),expected=lines.filter(l=>l.unit_type!=='courier');
   if(!Array.isArray(input.lines)||input.lines.length!==expected.length||new Set(input.lines.map(l=>l.line_id)).size!==expected.length)fail((plan.display_no||plan.id)+' 实收明细不完整 / 실제 수량이 누락되었습니다');
   const actual=expected.map(l=>{const r=input.lines.find(x=>x.line_id===l.id);const qty=r?.actual_qty;if(typeof qty!=='number'||!Number.isFinite(qty)||qty<0||qty>1e8)fail('实收数量必须为非负数 / 올바른 수량을 입력하세요');return {line_id:l.id,unit_type:l.unit_type,actual_qty:qty};});
   if(expected.length&&!actual.some(l=>l.actual_qty>0))fail((plan.display_no||plan.id)+' 请填写实际到货数量 / 실제 도착 수량을 입력하세요');
   const hasDiff=expected.some(l=>actual.find(x=>x.line_id===l.id).actual_qty!==l.planned_qty);
   const own={plan_id:plan.id,result_lines:actual,has_diff:hasDiff,diff_note:String(input.diff_note||'').trim().slice(0,1000)};
   prepared.push({plan,own});
   actual.forEach(l=>totals.set(l.unit_type,(totals.get(l.unit_type)||0)+l.actual_qty));
  }
  const t=stamp(),resultLines=[...totals].map(([unit_type,actual_qty])=>({unit_type,actual_qty}));
  const box=(totals.get('carton')||0)+(totals.get('box')||0),pallet=totals.get('pallet')||0,remark=String(body.remark||'').trim().slice(0,2000);
  const output={box_count:box,pallet_count:pallet,remark,result_lines:resultLines,plan_results:prepared.map(x=>x.own),has_diff:prepared.some(x=>x.own.has_diff)};
  const workers=await rows(env,'SELECT worker_name FROM v2_ops_job_workers WHERE job_id=?',trip.job_id),names=[...new Set(workers.map(w=>w.worker_name).filter(Boolean))].join('、');
  // A deterministic result ID makes concurrent completion transactional and idempotent.
  const statements=[q(env,'INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,result_lines_json,diff_note,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)','R-'+trip.job_id,trip.job_id,box,pallet,remark,JSON.stringify(output),JSON.stringify(resultLines),'',env.SOP_REQUEST_USER?.name||'',t),
   q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*1440,1)),leave_reason='complete' WHERE job_id=? AND left_at=''",t,t,trip.job_id),
   q(env,"UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,shared_result_json=?,updated_at=? WHERE id=?",JSON.stringify(output),t,trip.job_id),
   q(env,'UPDATE ck_unload_trips SET finished_at=? WHERE job_id=?',t,trip.job_id)];
  for(const {plan,own} of prepared){
   for(const l of own.result_lines)statements.push(q(env,'UPDATE v2_inbound_plan_lines SET actual_qty=? WHERE id=? AND plan_id=?',l.actual_qty,l.line_id,plan.id));
   statements.push(q(env,'UPDATE ck_unload_plan_links SET result_json=? WHERE job_id=? AND plan_id=?',JSON.stringify(own),trip.job_id,plan.id));
   statements.push(q(env,"UPDATE v2_inbound_plans SET status='arrived_pending_putaway',unload_completed_at=?,unload_completed_by=?,updated_at=? WHERE id=?",t,names,t,plan.id));
  }
  try{await env.DB.batch(statements);}catch(e){if(!(await q(env,'SELECT finished_at FROM ck_unload_trips WHERE job_id=?',trip.job_id).first())?.finished_at)throw e;}
 }
 // Also reconcile on retries after a network interruption. Receipt and labor are already atomic.
 const finished=(await q(env,'SELECT finished_at FROM ck_unload_trips WHERE job_id=?',trip.job_id).first()).finished_at;
 for(const {plan} of plans){await ensureTasks(env,plan);await syncCourier(env,plan.id,recalc);await recalc(env,plan.id,finished);}
 return {ok:true,job_id:trip.job_id,already_completed:job.status==='completed',plan_count:plans.length};
}
