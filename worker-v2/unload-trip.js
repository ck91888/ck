// One vehicle is one labor job. Links carry each plan's receipt, never extra jobs.
import {inboundFlowEnabled, inboundCode} from './inbound-flow.js';
import {staffedDispatchStatements} from './crew-borrow.js';
import {courierProgress,courierProgressSQL} from './courier.js';
import {ensureSchema} from './schema-ready.js';
import {nativeCompletionGuard,completionConflict,completionChanged} from './native-completion-guard.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results||[];
const fail=message=>{throw Error(message);};
const stamp=()=>new Date().toISOString();
const parse=value=>{try{return JSON.parse(value||'null');}catch{return null;}};
export async function tripPlans(env,jobId){
 if(!inboundFlowEnabled(env))return [];
 const links=await rows(env,'SELECT * FROM ck_unload_plan_links WHERE job_id=? ORDER BY position',jobId);
 if(!links.length)return [];
 const ids=JSON.stringify(links.map(l=>l.plan_id));
 const [plans,lines,needs]=await env.DB.batch([
  q(env,'SELECT * FROM v2_inbound_plans WHERE id IN (SELECT value FROM json_each(?))',ids),
  q(env,'SELECT * FROM v2_inbound_plan_lines WHERE plan_id IN (SELECT value FROM json_each(?)) ORDER BY line_no',ids),
  q(env,"SELECT id,state FROM sop_records WHERE kind='need' AND json_extract(state,'$.source_id') IN (SELECT value FROM json_each(?)) ORDER BY updated_at",ids)
 ]);
 const work=needs.results.map(n=>({id:n.id,...parse(n.state)}));
 return links.map(link=>({plan:plans.results.find(p=>p.id===link.plan_id)||null,lines:lines.results.filter(l=>l.plan_id===link.plan_id),needs:work.filter(n=>n.source_id===link.plan_id),result:parse(link.result_json)}));
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
export async function startUnloadTrip(env,body,staff=null){
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
 if(staff){statements.push(...staffedDispatchStatements(env,{...body,job_type:'unload',source_type:'inbound_plan',source_id:ids[0]},staff,jobId,t,{ok:true,job_id:jobId,worker_seg_id:segId,is_new_job:true}));statements.push(q(env,'UPDATE v2_ops_jobs SET active_worker_count=? WHERE id=?',staff.workers.length,jobId));}
 else statements.push(q(env,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)',segId,jobId,worker,name,t));
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
   // Existing single-plan clients send unit-based receipts rather than a per-plan wrapper.
   const results=body.plan_results||(plans.length===1&&Array.isArray(body.result_lines)?[{plan_id:plans[0].plan.id,diff_note:body.diff_note||'',lines:plans[0].lines.filter(l=>l.unit_type!=='courier').map(l=>({line_id:l.id,actual_qty:body.result_lines.find(r=>r.unit_type===l.unit_type)?.actual_qty}))}]:undefined);
  if(!Array.isArray(results)||results.length!==plans.length||new Set(results.map(r=>r.plan_id)).size!==plans.length||results.some(r=>!plans.some(p=>p.plan.id===r.plan_id)))fail('请逐张填写本车全部计划的实收数量 / 모든 계획의 실제 수량을 입력하세요');
  const prepared=[],totals=new Map(),checks=[],checkId=crypto.randomUUID();
  for(const {plan,lines} of plans){
   if(!plan||plan.is_deleted||['cancelled','deleted'].includes(plan.status))fail('计划状态已变化，请刷新 / 계획 상태가 변경되었습니다');
   const courierOnly=lines.length>0&&lines.every(line=>line.unit_type==='courier'),progress=courierOnly?await courierProgress(env,plan.id):null;
   // Parcel scans may advance a plan while its unload job is system-closed.
   // Existing arrival evidence is authoritative; closing labor must not rewind it.
   const preserveArrival=!!(courierOnly&&progress?.total&&progress.total===progress.received&&plan.unload_completed_at&&!plan.force_completed&&!plan.manual_completed_at);
   if(!['unloading','unloading_putting_away','putting_away'].includes(plan.status)&&!(preserveArrival&&['arrived_pending_putaway','tallied_pending_putaway','partially_completed','completed'].includes(plan.status)))fail('计划状态已变化，请刷新 / 계획 상태가 변경되었습니다');
   const lineSnapshot=JSON.stringify(lines.map(l=>({id:l.id,unit_type:l.unit_type,planned_qty:l.planned_qty,actual_qty:l.actual_qty,putaway_qty:l.putaway_qty})));
   checks.push(q(env,`INSERT INTO ck_unload_finish_checks(id,allowed) SELECT ?,CASE WHEN
    EXISTS(SELECT 1 FROM v2_ops_jobs j JOIN ck_unload_trips tr ON tr.job_id=j.id JOIN ck_unload_plan_links lk ON lk.job_id=j.id JOIN v2_inbound_plans p ON p.id=lk.plan_id WHERE j.id=? AND j.status='working' AND tr.finished_at='' AND p.id=? AND p.status=? AND p.updated_at IS ? AND p.is_deleted IS ? AND p.unload_completed_at IS ? AND p.unload_completed_by IS ? AND p.force_completed IS ? AND p.manual_completed_at IS ?)
    AND (SELECT COUNT(*) FROM v2_inbound_plan_lines WHERE plan_id=?)=?
    AND NOT EXISTS(SELECT 1 FROM json_each(?) expected LEFT JOIN v2_inbound_plan_lines l ON l.id=json_extract(expected.value,'$.id') AND l.plan_id=? WHERE l.id IS NULL OR l.unit_type IS NOT json_extract(expected.value,'$.unit_type') OR l.planned_qty IS NOT json_extract(expected.value,'$.planned_qty') OR l.actual_qty IS NOT json_extract(expected.value,'$.actual_qty') OR l.putaway_qty IS NOT json_extract(expected.value,'$.putaway_qty'))
    THEN 1 ELSE 0 END`,checkId+':'+plan.id,trip.job_id,plan.id,plan.status,plan.updated_at,plan.is_deleted,plan.unload_completed_at,plan.unload_completed_by,plan.force_completed,plan.manual_completed_at,plan.id,lines.length,lineSnapshot,plan.id));
   if(preserveArrival)checks.push(q(env,`INSERT INTO ck_unload_finish_checks(id,allowed) SELECT ?,CASE WHEN (SELECT json_group_array(json_object('tracking_no',tracking_no,'receipt_id',receipt_id,'received_at',received_at)) FROM (${courierProgressSQL}))=? THEN 1 ELSE 0 END`,checkId+':courier:'+plan.id,plan.id,plan.id,JSON.stringify(progress.items.map(x=>({tracking_no:x.tracking_no,receipt_id:x.receipt_id,received_at:x.received_at})))));
   const input=results.find(r=>r.plan_id===plan.id),expected=lines.filter(l=>l.unit_type!=='courier');
   if(!Array.isArray(input.lines)||input.lines.length!==expected.length||new Set(input.lines.map(l=>l.line_id)).size!==expected.length)fail((plan.display_no||plan.id)+' 实收明细不完整 / 실제 수량이 누락되었습니다');
   const actual=expected.map(l=>{const r=input.lines.find(x=>x.line_id===l.id);const qty=r?.actual_qty;if(typeof qty!=='number'||!Number.isFinite(qty)||qty<0||qty>1e8)fail('实收数量必须为非负数 / 올바른 수량을 입력하세요');return {line_id:l.id,unit_type:l.unit_type,actual_qty:qty};});
   if(expected.length&&!actual.some(l=>l.actual_qty>0))fail((plan.display_no||plan.id)+' 请填写实际到货数量 / 실제 도착 수량을 입력하세요');
   const hasDiff=expected.some(l=>actual.find(x=>x.line_id===l.id).actual_qty!==l.planned_qty);
   const own={plan_id:plan.id,result_lines:actual,has_diff:hasDiff,diff_note:String(input.diff_note||'').trim().slice(0,1000)};
   prepared.push({plan,own,preserveArrival});
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
  for(const {plan,own,preserveArrival} of prepared){
   for(const l of own.result_lines)statements.push(q(env,'UPDATE v2_inbound_plan_lines SET actual_qty=? WHERE id=? AND plan_id=?',l.actual_qty,l.line_id,plan.id));
   statements.push(q(env,'UPDATE ck_unload_plan_links SET result_json=? WHERE job_id=? AND plan_id=?',JSON.stringify(own),trip.job_id,plan.id));
   if(!preserveArrival)statements.push(q(env,"UPDATE v2_inbound_plans SET status='arrived_pending_putaway',unload_completed_at=?,unload_completed_by=?,updated_at=? WHERE id=?",t,names,t,plan.id));
  }
  await ensureSchema(env.DB,'unload-finish-checks-v1',()=>q(env,'CREATE TABLE IF NOT EXISTS ck_unload_finish_checks(id TEXT PRIMARY KEY,allowed INTEGER NOT NULL CONSTRAINT ck_unload_finish_checks CHECK(allowed=1))').run());
  statements.push(q(env,'DELETE FROM ck_unload_finish_checks WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(prepared.flatMap(x=>[checkId+':'+x.plan.id,...(x.preserveArrival?[checkId+':courier:'+x.plan.id]:[])]))));
  try{await env.DB.batch([...await nativeCompletionGuard(env,body),...checks,...statements]);}catch(e){if(completionConflict.test(e.message||''))throw Error(completionChanged);if(!(await q(env,'SELECT finished_at FROM ck_unload_trips WHERE job_id=?',trip.job_id).first())?.finished_at){if(/ck_unload_finish_checks/.test(e.message||''))fail('计划或快递收货记录已变化，请刷新后核对 / 계획·택배 기록 변경 · 새로고침 후 확인하세요');throw e;}}
 }
 // Also reconcile on retries after a network interruption. Receipt and labor are already atomic.
 const finished=(await q(env,'SELECT finished_at FROM ck_unload_trips WHERE job_id=?',trip.job_id).first()).finished_at;
 for(const {plan} of plans){await ensureTasks(env,plan);await syncCourier(env,plan.id,recalc);await recalc(env,plan.id,finished);}
 return {ok:true,job_id:trip.job_id,already_completed:job.status==='completed',plan_count:plans.length};
}
