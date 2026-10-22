import {readPickWorkload} from './pick-workload.js';
// Read-only employee workday facts. Account/person IDs identify dispatchers;
// exact EMP badges identify participants. Names never establish identity.
import {attendanceReport} from './attendance-time.js';
import {jobNumbers,jobDisplayMetadata} from './document-numbers.js';
import {effectiveResults} from './effective-results.js';
import {ensureSchema} from './schema-ready.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results||[];
const parse=value=>{try{return typeof value==='string'?JSON.parse(value):value||{};}catch{return {};}};
const starts=new Set(['sop_native_start','sop_task_dispatch','sop_task_create']);
const reviews=new Set(['sop_task_review','sop_task_complete_review','sop_cargo_group_review']);
const docActions=new Set(['sop_need_create','sop_need_from_outbound','v2_outbound_order_batch_create','sop_cargo_outbounds_create','sop_cargo_reservation_create']);
const active=new Set(['pending','assigned','working','paused','awaiting_close','awaiting_review','rework']);
const unique=list=>{const seen=new Map();for(const row of list){const key=row.job_id||row.id;if(!seen.has(key))seen.set(key,row);}return [...seen.values()];};
const minutes=totals=>totals.bulk+totals.direct_ship+totals.import+totals.other;
const quantity=(value,unit,label)=>unit&&Number.isFinite(Number(value))&&Number(value)>0?{quantity:Number(value),unit,label}:null;
const cleanQuantities=values=>values.filter(Boolean);
const recordedQuantity=(value,unit,label)=>unit&&value!==null&&value!==undefined&&value!==''&&Number.isFinite(Number(value))&&Number(value)>=0?{quantity:Number(value),unit,label}:null;
const unitLabel=value=>({carton:'箱',box:'箱',pallet:'托',bag:'袋',courier:'快递件'}[value]||String(value||''));
const iso=value=>Number.isFinite(value)?new Date(value).toISOString():'';

export function employeeWorkdayAccess(env){
 const user=env.SOP_REQUEST_USER;
 if(!user||!['manager','dispatcher','reviewer','viewer'].includes(user.role)||['field','kiosk'].includes(user.scope)||env.SOP_ACCESS_CONTROL==='true'&&user.scope!=='office')throw Error('仅办公室可查看职员作业明细 / 사무실에서만 직원 작업 내역을 조회할 수 있습니다');
}

function personalSegments(record,segments,asOf,date){
 const from=Math.max(Date.parse(date+'T00:00:00+09:00'),Date.parse(record.inAt));
 const to=Math.min(Date.parse(date+'T00:00:00+09:00')+86400000,Date.parse(asOf),record.outAt?Date.parse(record.outAt):Infinity);
 return segments.filter(s=>s.badgeId===record.badgeId).map(s=>{
  const a=Math.max(from,Date.parse(s.start)),z=Math.min(to,s.end?Date.parse(s.end):Date.parse(asOf));
  return {...s,worker_id:s.sourceWorkerId||s.badgeId,clipped_start:iso(a),clipped_end:iso(z),minutes:Number.isFinite(a)&&Number.isFinite(z)?Math.max(0,(z-a)/60000):0};
 }).filter(s=>s.minutes>0);
}

function reviewFact(event){
 const after=parse(event.after_json),before=parse(event.before_json);
 if(event.action!=='sop_cargo_group_review'){
  const review=after.review||{};
  if(!['pass','return'].includes(review.decision))return null;
  return {request_id:event.request_id,job_id:event.job_id,at:event.created_at,action:event.action,decision:review.decision,round:review.round||after.round||null,note:review.reason||'',group_ids:[]};
 }
 // The need event carries the reviewed output history. Its paired task event
 // proves the job identity, and is never counted as a second review.
 const facts=[],previous=new Map();
 const visit=(data,fn)=>{for(const group of data.cargo_groups?.groups||[]){for(const output of [...(group.outputs||[]),...(group.processes||[]).flatMap(p=>[...(p.result_history||[]),...(p.result?[p.result]:[])])])fn(group,output);}};
 visit(before,(group,output)=>previous.set(group.id+'|'+output.id+'|'+output.version,JSON.stringify(output.review||null)));
 visit(after,(group,output)=>{const r=output.review;if(r?.actor_id===event.actor_id&&Date.parse(r.at)===Date.parse(event.created_at)&&['pass','return'].includes(r.decision)&&previous.get(group.id+'|'+output.id+'|'+output.version)!==JSON.stringify(r))facts.push({group_id:group.id,decision:r.decision,note:r.reason||''});});
 const decisions=new Set(facts.map(x=>x.decision));if(!facts.length||decisions.size!==1)return null;
 return {request_id:event.request_id,job_id:event.job_id,at:event.created_at,action:event.action,decision:facts[0].decision,round:null,note:facts[0].note,group_ids:[...new Set(facts.map(x=>x.group_id))]};
}

function resultFact(row){
 const result=parse(row.result_json),storedLines=parse(row.result_lines_json),lines=Array.isArray(storedLines)&&storedLines.length?storedLines:Array.isArray(result.result_lines)?result.result_lines:[],quantities=[];
 for(const line of lines){const unit=unitLabel(line.unit_type||line.unit);if(unit)quantities.push(recordedQuantity(line.actual_qty??line.putaway_qty??line.quantity,unit,'整任务实收/完成'));}
 if(!quantities.some(x=>x?.unit==='箱'))quantities.push(quantity(row.box_count,'箱','整任务实收/完成'));
 if(!quantities.some(x=>x?.unit==='托'))quantities.push(quantity(Number(row.pallet_count)||result.pallet_count,'托','整任务实收/完成'));
 for(const [key,unit,label] of [['packed_box_count','箱','整任务打包'],['total_operated_box_count','箱','整任务操作'],['label_count','张','整任务贴标'],['repaired_box_count','箱','整任务修箱'],['reboxed_count','箱','整任务换箱']])quantities.push(quantity(result[key],unit,label));
 for(const output of Array.isArray(result.group_outputs)&&result.group_outputs.length?result.group_outputs:Array.isArray(result.process_results)?result.process_results:[])quantities.push(quantity(output.quantity,output.unit,'整任务分组成果'));
 if(result.quantity&&result.unit&&!quantities.some(x=>x?.unit===result.unit&&x.quantity===Number(result.quantity)))quantities.push(quantity(result.quantity,result.unit,'整任务成果'));
 if(result.kind==='check_workbench')quantities.push(recordedQuantity(result.task_scanned_ok_count,'次','本次核对成功扫描（整任务）'));
 if(Array.isArray(result.pick_doc_nos))quantities.push(quantity(new Set(result.pick_doc_nos).size,'单','整趟拣货单'));
 return {id:row.id,at:row.created_at,scope:'whole_task',quantities:cleanQuantities(quantities),result_lines:lines,note:row.remark||result.description||result.result_note||''};
}

// Query count is independent of employee count. All ID sets use one JSON bind.
export async function employeeWorkdays(env,{date,asOf,items,validSegments,detailPersonId=''}){
 const tables=new Set((await rows(env,"SELECT name FROM sqlite_master WHERE type='table'")).map(x=>x.name));
 if(tables.has('sop_events'))await ensureSchema(env.DB,'employee-workday-event-index-v1',()=>env.DB.prepare('CREATE INDEX IF NOT EXISTS ck_employee_workday_actor_time ON sop_events(actor_id,julianday(created_at))').run());
 const identities=items.length?await rows(env,"SELECT p.id,p.badge_id FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id WHERE p.id IN (SELECT value FROM json_each(?)) AND p.badge_id='EMP-'||e.employee_no AND p.kind='permanent'",JSON.stringify(items.map(x=>x.record.personId))):[];
 const identityIds=new Set(identities.filter(p=>items.some(item=>item.record.personId===p.id&&item.record.badgeId===p.badge_id)).map(p=>p.id));
 const ids=JSON.stringify([...identityIds]),from=new Date(date+'T00:00:00+09:00').toISOString(),until=new Date(Date.parse(from)+86400000).toISOString();
 const hasEvents=tables.has('sop_events')&&tables.has('sop_records'),events=hasEvents&&items.length?await rows(env,`SELECT e.*,s.kind AS record_kind,s.state AS record_state,
 CASE WHEN e.action='sop_cargo_group_review' AND s.kind='need' THEN mirror.record_id ELSE j.id END AS job_id,j.status AS current_status
 FROM sop_events e JOIN sop_records s ON s.id=e.record_id
 LEFT JOIN sop_events mirror ON e.action='sop_cargo_group_review' AND s.kind='need' AND mirror.request_id=e.request_id||'-task' AND mirror.action=e.action AND mirror.actor_id=e.actor_id
 LEFT JOIN sop_records mirror_task ON mirror_task.id=mirror.record_id AND mirror_task.kind='task' AND json_extract(mirror_task.state,'$.need_id')=s.id
 LEFT JOIN v2_ops_jobs j ON j.id=CASE WHEN e.action='sop_cargo_group_review' AND s.kind='need' AND mirror_task.id IS NOT NULL THEN mirror.record_id ELSE e.record_id END
 WHERE e.actor_id IN (SELECT value FROM json_each(?)) AND julianday(e.created_at)<=julianday(?) AND (
 (julianday(e.created_at)>=julianday(?) AND julianday(e.created_at)<julianday(?) AND
 ((e.action IN ('sop_native_start','sop_task_dispatch','sop_task_create') AND e.expected_revision=0 AND s.kind IN ('task','dispatch') AND j.id IS NOT NULL)
 OR (e.action IN ('sop_task_review','sop_task_complete_review') AND s.kind='task' AND j.id IS NOT NULL)
 OR (e.action='sop_cargo_group_review' AND s.kind='need' AND mirror_task.id IS NOT NULL AND j.id IS NOT NULL)
 OR (e.action IN ('sop_need_create','sop_need_from_outbound','v2_outbound_order_batch_create','sop_cargo_outbounds_create','sop_cargo_reservation_create') AND s.kind='need')))
 OR (julianday(e.created_at)<julianday(?) AND e.action IN ('sop_native_start','sop_task_dispatch','sop_task_create') AND e.expected_revision=0 AND s.kind IN ('task','dispatch') AND j.status IN ('pending','assigned','working','paused','awaiting_close','awaiting_review','rework')))
 ORDER BY julianday(e.created_at),e.request_id`,ids,asOf,from,until,from):[];
 const audits=tables.has('ck_plan_operation_audit')&&items.length?await rows(env,`SELECT * FROM ck_plan_operation_audit WHERE kind='create' AND actor_id IN (SELECT value FROM json_each(?)) AND julianday(created_at)>=julianday(?) AND julianday(created_at)<julianday(?) AND julianday(created_at)<=julianday(?)`,ids,from,until,asOf):[];
 const ownByPerson=new Map(items.map(item=>[item.record.personId,personalSegments(item.record,validSegments.get(item.record.personId)||[],asOf,date)]));
 const jobIds=[...new Set([...events.map(e=>e.job_id),...[...ownByPerson.values()].flat().map(s=>s.jobId)].filter(Boolean))];
 let jobs=jobIds.length?await rows(env,hasEvents?"SELECT j.*,s.state AS dispatch_state FROM v2_ops_jobs j LEFT JOIN sop_records s ON s.id=j.id AND s.kind='dispatch' WHERE j.id IN (SELECT value FROM json_each(?))":'SELECT * FROM v2_ops_jobs WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(jobIds)):[];
 if(tables.has('sop_document_numbers')&&hasEvents)jobs=await jobNumbers(env,jobs);
 const customers=await jobCustomers(env,jobs,tables);
 const byJob=new Map(jobs.map(j=>[j.id,{...jobDisplayMetadata(j),...(j.job_type==='office_order_processing'?{time_only:true,quantity_state:'time_only',result_summary:'仅记录工时，无数量结果'}:{}),...(j.job_type==='pick_direct'?{planned_pick_workload:readPickWorkload(parse(j.dispatch_state).planned_pick_workload)}:{}),display_no:j.display_no||'',customer:(customers.get(j.id)||[]).join('、'),customers:customers.get(j.id)||[],id:j.id,job_id:j.id,status:j.status,completed_at:j.finished_at||'',dispatched_at:'',minutes:0,segments:[],results:[]}]));
 const results=detailPersonId&&jobIds.length&&tables.has('v2_ops_job_results')?effectiveResults(await rows(env,'SELECT * FROM v2_ops_job_results WHERE job_id IN (SELECT value FROM json_each(?)) ORDER BY created_at,id',JSON.stringify(jobIds))):[];
 for(const r of results)byJob.get(r.job_id)?.results.push(resultFact(r));

 const candidates=[];
 const add=(actor,type,id,at,proof={})=>{if(id)candidates.push({actor,type,id,created_at:at,...proof});};
 for(const e of events){const after=parse(e.after_json),before=parse(e.before_json),result=parse(e.result_json);
  if(docActions.has(e.action)){
   if(['sop_need_create','sop_need_from_outbound'].includes(e.action)&&e.expected_revision===0)add(e.actor_id,'need',e.record_id,e.created_at);
   if(['v2_outbound_order_batch_create','sop_cargo_outbounds_create'].includes(e.action))for(const ob of result.outbounds||[])add(e.actor_id,'outbound',ob.id,e.created_at);
   if(['sop_need_create','sop_need_from_outbound','sop_cargo_reservation_create'].includes(e.action)){const old=new Set((before.links||[]).map(x=>x.outbound_id));for(const link of after.links||[])if(!old.has(link.outbound_id))add(e.actor_id,'outbound',link.outbound_id,e.created_at,{sameCreatedAt:true,creationAt:e.action==='sop_cargo_reservation_create'?(link.at||e.created_at):(after.created_at||link.at||e.created_at)});}
  }
  if(e.action==='sop_native_start'&&Date.parse(e.created_at)>=Date.parse(from)&&after.source_type==='field_feedback')add(e.actor_id,'feedback',after.source_id,e.created_at,{job_id:e.job_id,sameCreatedAt:true});
 }
 for(const e of audits)if(['inbound','outbound'].includes(e.doc_type))add(e.actor_id,e.doc_type,e.doc_id,e.created_at);
 const documents=await createdDocuments(env,candidates,tables);
 const details=new Map();
 for(const item of items){
  const personId=item.record.personId,identityKnown=identityIds.has(personId),ownEvents=events.filter(e=>e.actor_id===personId),created=unique(ownEvents.filter(e=>starts.has(e.action)&&e.expected_revision===0&&byJob.has(e.job_id)).map(e=>({...byJob.get(e.job_id),dispatched_at:e.created_at})));
  const dispatched=created.filter(j=>Date.parse(j.dispatched_at)>=Date.parse(from)),carryover=created.filter(j=>Date.parse(j.dispatched_at)<Date.parse(from)&&active.has(j.status));
  const ownSegments=ownByPerson.get(personId),own=unique(ownSegments.map(s=>({...(byJob.get(s.jobId)||{...jobDisplayMetadata(s),id:s.jobId,job_id:s.jobId,status:s.jobStatus}),segments:ownSegments.filter(x=>x.jobId===s.jobId)}))).map(j=>({...j,minutes:minutes(attendanceReport(item.record,j.segments,asOf,date)),minutesScope:'job_interval_reference'}));
  const reviewEvents=ownEvents.filter(e=>reviews.has(e.action)),reviewed=reviewEvents.map(reviewFact).filter(Boolean).map(r=>({...byJob.get(r.job_id),...r}));
  const docs=documents.filter(d=>d.actor===personId).map(({actor,...doc})=>doc),inProgress=dispatched.filter(j=>active.has(j.status)).length,completed=dispatched.filter(j=>j.status==='completed').length,cancelled=dispatched.filter(j=>j.status==='cancelled').length;
  const notes=['派工与创建只统计可精确关联职员 ID 的记录；共享办公室账号和手填姓名不归入个人。 / 직원 ID가 확인된 기록만 집계합니다.', '参与完成指当日实际参与的作业目前已完成；整任务成果不代表个人产量。 / 참여 작업의 현재 완료 상태이며 개인 생산량이 아닙니다.'];
  if(!hasEvents)notes.push('历史派工与审核审计表未记录，相关统计暂不可用。 / 배정·검수 감사 기록이 없습니다.');
  if(!identityKnown)notes.push('职员资料与当天工牌关联不一致，身份待核实。 / 직원 정보와 당일 명찰의 연결을 확인하세요.');
  notes.push('各任务参与时段可能交叉，明细参考分钟不可相加；总实作分钟已按签到、休息与冲突裁剪。 / 작업별 참고 시간은 합산하지 마세요.');
  const unresolved=reviewEvents.length-reviewed.length;if(unresolved)notes.push('部分审核记录缺少可核对的结论，未计入审核统计。 / 확인 가능한 검수 결과가 없는 기록은 제외합니다.');
  const workday={dispatched:{total:hasEvents?dispatched.length:null,inProgress:hasEvents?inProgress:null,completed:hasEvents?completed:null,cancelled:hasEvents?cancelled:null,other:hasEvents?dispatched.length-inProgress-completed-cancelled:null,carryover:hasEvents?carryover.length:null},own:{participated:own.length,completed:own.filter(j=>j.status==='completed').length,minutes:minutes(item.totals),conflictMinutes:item.totals.conflict},reviewed:{total:hasEvents?new Set(reviewed.map(r=>r.job_id)).size:null,operationCount:hasEvents?reviewed.length:null,passed:hasEvents?reviewed.filter(r=>r.decision==='pass').length:null,returned:hasEvents?reviewed.filter(r=>r.decision==='return').length:null},documents:{total:hasEvents||tables.has('ck_plan_operation_audit')?docs.length:null},coverage:{identity:identityKnown?'exact':'unresolved',officeActorsUnmapped:true,dispatchAuditAvailable:hasEvents,unresolvedReviewOperations:unresolved,notes}};
  workday.officeDuty={minutes:minutes(attendanceReport(item.record,ownSegments.filter(s=>byJob.get(s.jobId)?.job_type==='office_order_processing'),asOf,date)),quantity_state:'time_only'};
  item.workday=workday;
  if(personId===detailPersonId)details.set(personId,{ok:true,date,asOf,record:item.record,status:item.status,currentJobs:item.currentJobs,workday,breaks:item.breaks,...(item.totals.accounting?{attendanceAccounting:item.totals.accounting}:{}),sections:{dispatched,carryover,own,reviewed,documents:docs},flags:item.totals.flags});
 }
 return details;
}

export async function jobCustomers(env,jobs,tables){
 const result=new Map(jobs.map(j=>[j.id,new Set(j.customer?[j.customer]:[])]));if(!jobs.length)return new Map();
 const selects=[];
 if(tables.has('sop_records'))selects.push("SELECT s.id AS job_id,COALESCE(NULLIF(json_extract(s.state,'$.customer'),''),json_extract(n.state,'$.customer')) AS customer FROM sop_records s LEFT JOIN sop_records n ON n.id=json_extract(s.state,'$.need_id') WHERE s.kind IN ('task','dispatch')");
 if(tables.has('v2_inbound_plans'))selects.push("SELECT j.id AS job_id,p.customer FROM v2_ops_jobs j JOIN v2_inbound_plans p ON p.id=j.related_doc_id AND j.related_doc_type IN ('inbound','inbound_plan')");
 if(tables.has('v2_outbound_orders'))selects.push(`SELECT j.id AS job_id,p.customer FROM v2_ops_jobs j JOIN v2_outbound_orders p ON (p.id=j.related_doc_id AND j.related_doc_type IN ('outbound','outbound_order')) ${jobs.some(j=>Object.hasOwn(j,'linked_outbound_order_id'))?' OR p.id=j.linked_outbound_order_id':''}`);
 if(tables.has('ck_unload_plan_links')&&tables.has('v2_inbound_plans'))selects.push('SELECT l.job_id,p.customer FROM ck_unload_plan_links l JOIN v2_inbound_plans p ON p.id=l.plan_id');
 if(tables.has('ck_load_order_links')&&tables.has('v2_outbound_orders'))selects.push('SELECT l.job_id,p.customer FROM ck_load_order_links l JOIN v2_outbound_orders p ON p.id=l.order_id');
 if(tables.has('ck_putaway_links')&&tables.has('v2_inbound_plans'))selects.push('SELECT l.job_id,p.customer FROM ck_putaway_links l JOIN v2_inbound_plans p ON p.id=l.plan_id');
 // workerd limits compound SELECTs to five terms. Keep each relationship source
 // separate: a fixed batch (at most six reads), never a query per task or UNION.
 // Customer labels are a set; this query never aggregates task work or quantities.
 const ids=JSON.stringify([...new Set(jobs.map(j=>j.id))]);
 const batches=selects.length?await env.DB.batch(selects.map(sql=>env.DB.prepare(`SELECT job_id,customer FROM (${sql}) WHERE job_id IN (SELECT value FROM json_each(?)) AND COALESCE(customer,'')!='' GROUP BY job_id,customer ORDER BY job_id,customer LIMIT 20001`).bind(ids))):[];
 for(const batch of batches){
  if(batch.success===false||batch.error)throw Error('客户关联读取失败；未返回部分统计');
  const found=batch.results||[];if(found.length>20000)throw Error('单来源客户关联超过安全上限20000条；未返回部分统计');
  for(const row of found)result.get(row.job_id)?.add(row.customer);
 }
 return new Map([...result].map(([id,names])=>[id,[...names]]));
}

async function createdDocuments(env,candidates,tables){
 const keys=[...new Set(candidates.map(x=>x.id))],ids=JSON.stringify(keys),known=new Map();if(!keys.length)return [];
 const requests=[];
 if(tables.has('sop_records'))requests.push(['need',`SELECT s.id,${tables.has('sop_document_numbers')?'n.display_no':"'' AS display_no"},json_extract(s.state,'$.status') AS status,json_extract(s.state,'$.customer') AS customer,json_extract(s.state,'$.planned_quantity') AS quantity,json_extract(s.state,'$.planned_unit') AS unit,json_extract(s.state,'$.created_at') AS source_created_at FROM sop_records s ${tables.has('sop_document_numbers')?'LEFT JOIN sop_document_numbers n ON n.record_id=s.id':''} WHERE s.kind='need' AND s.id IN (SELECT value FROM json_each(?))`]);
 if(tables.has('v2_inbound_plans'))requests.push(['inbound','SELECT id,display_no,status,customer,created_at AS source_created_at FROM v2_inbound_plans WHERE id IN (SELECT value FROM json_each(?))']);
 if(tables.has('v2_outbound_orders'))requests.push(['outbound','SELECT id,display_no,status,customer,created_at AS source_created_at,planned_box_count,planned_pallet_count FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))']);
 if(tables.has('v2_field_feedbacks'))requests.push(['feedback','SELECT id,display_no,status,created_at AS source_created_at,related_doc_id AS source_job_id FROM v2_field_feedbacks WHERE id IN (SELECT value FROM json_each(?))']);
 const result=await env.DB.batch(requests.map(([,sql])=>q(env,sql,ids)));
 requests.forEach(([type],i)=>{for(const row of result[i].results||[])known.set(type+'|'+row.id,{...row,type,customer:row.customer||'',quantities:cleanQuantities([quantity(row.quantity,row.unit,'当前计划参考'),quantity(row.planned_box_count,'箱','当前计划参考'),quantity(row.planned_pallet_count,'托','当前计划参考')]),reference:'current'});});
 if(tables.has('v2_inbound_plan_lines'))for(const line of await rows(env,'SELECT plan_id,unit_type,planned_qty FROM v2_inbound_plan_lines WHERE plan_id IN (SELECT value FROM json_each(?))',ids)){const doc=known.get('inbound|'+line.plan_id),value=quantity(line.planned_qty,unitLabel(line.unit_type),'当前计划参考');if(doc&&value)doc.quantities.push(value);}
 const found=new Map();for(const candidate of candidates){const doc=known.get(candidate.type+'|'+candidate.id),key=candidate.actor+'|'+candidate.type+'|'+candidate.id;if(doc&&(!candidate.sameCreatedAt||Date.parse(doc.source_created_at)===Date.parse(candidate.creationAt||candidate.created_at))&&(!candidate.job_id||candidate.job_id===doc.source_job_id)&&!found.has(key))found.set(key,{...doc,actor:candidate.actor,created_at:candidate.created_at});}
 return [...found.values()];
}
